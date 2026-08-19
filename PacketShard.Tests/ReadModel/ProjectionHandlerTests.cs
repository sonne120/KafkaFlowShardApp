using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PacketShard.Read;
using StackExchange.Redis;
using Testcontainers.PostgreSql;
using Testcontainers.Redis;
using Xunit;

namespace PacketShard.Tests.ReadModel;

[Trait("Category", "Infrastructure")]
public sealed class ProjectionHandlerTests : IAsyncLifetime
{
    private PostgreSqlContainer _pg = null!;
    private RedisContainer _redis = null!;
    private NpgsqlDataSource? _dataSource;
    private ConnectionMultiplexer _mux = null!;

    public async Task InitializeAsync()
    {
        _pg = new PostgreSqlBuilder()
            .WithImage(await ReadModelImage.EnsureAsync())
            .WithDatabase("readmodel")
            .WithUsername("postgres")
            .WithPassword("postgres")
            .Build();

        _redis = new RedisBuilder().WithImage("redis:7").Build();

        await Task.WhenAll(_pg.StartAsync(), _redis.StartAsync());
        await WaitForInitSqlAsync();

        var options = ConfigurationOptions.Parse(_redis.GetConnectionString());
        options.AllowAdmin = true;
        _mux = await ConnectionMultiplexer.ConnectAsync(options);
    }

    public async Task DisposeAsync()
    {
        await _mux.DisposeAsync();
        if (_dataSource is not null)
            await _dataSource.DisposeAsync();

        await Task.WhenAll(_pg.DisposeAsync().AsTask(), _redis.DisposeAsync().AsTask());
    }

    //the crash point 
    [Fact]
    public async Task Crash_after_postgres_commit_before_redis_mark_is_absorbed_by_dedup()
    {
        var value = KafkaValue("tx-1", proto: "TCP");
        var record = PacketRecord.TryParse(value)!;

        Assert.True(await CreateStore().ProjectAsync(record, default));


        Assert.False(await Redis.IsProcessedAsync("tx-1"));

        var outcome = await CreateHandler().HandleAsync(value, default);

        Assert.Equal(ProjectionOutcome.Duplicate, outcome);
        Assert.Equal(1L, await ScalarAsync<long>("SELECT count(*) FROM packet_ledger"));
        Assert.Equal(1L, await ScalarAsync<long>(
            "SELECT packet_count FROM packet_stats_by_proto WHERE proto='TCP'"));

        // And the retry finishes the job the crash interrupted.
        Assert.True(await Redis.IsProcessedAsync("tx-1"));
    }

    [Fact]
    public async Task Crash_before_postgres_commit_leaves_no_redis_marker()
    {
        var record = PacketRecord.TryParse(KafkaValue("tx-1"))! with { Payload = "{ not json" };

        await Assert.ThrowsAsync<PostgresException>(
            () => CreateStore().ProjectAsync(record, default));

        Assert.Equal(0L, await ScalarAsync<long>("SELECT count(*) FROM packet_ledger"));
        Assert.False(await Redis.IsProcessedAsync("tx-1"));

        // Nothing was acked, so redelivery of the intact event still projects normally.
        Assert.Equal(ProjectionOutcome.Projected, await CreateHandler().HandleAsync(KafkaValue("tx-1"), default));
        Assert.Equal(1L, await ScalarAsync<long>("SELECT count(*) FROM packet_ledger"));
    }

    [Fact]
    public async Task Marker_is_never_set_when_the_postgres_write_fails()
    {

        await ExecuteAsync("DROP TABLE packet_ledger CASCADE");

        await Assert.ThrowsAsync<PostgresException>(
            () => CreateHandler().HandleAsync(KafkaValue("tx-1"), default));

        Assert.False(await Redis.IsProcessedAsync("tx-1"));
    }

    //the fast path itself
    [Fact]
    public async Task Second_delivery_takes_the_redis_fast_path()
    {
        var handler = CreateHandler();
        var value = KafkaValue("tx-1");

        Assert.Equal(ProjectionOutcome.Projected, await handler.HandleAsync(value, default));

        Assert.Equal(ProjectionOutcome.FastPathSkip, await handler.HandleAsync(value, default));

        Assert.Equal(1L, await ScalarAsync<long>("SELECT count(*) FROM packet_ledger"));
    }

    [Fact]
    public async Task Lost_redis_markers_fall_back_to_postgres_dedup()
    {

        var handler = CreateHandler();
        var value = KafkaValue("tx-1", proto: "DNS");

        Assert.Equal(ProjectionOutcome.Projected, await handler.HandleAsync(value, default));

        await _mux.GetServers()[0].FlushDatabaseAsync();
        Assert.False(await Redis.IsProcessedAsync("tx-1"));

        Assert.Equal(ProjectionOutcome.Duplicate, await handler.HandleAsync(value, default));
        Assert.Equal(1L, await ScalarAsync<long>("SELECT count(*) FROM packet_ledger"));
        Assert.Equal(1L, await ScalarAsync<long>(
            "SELECT packet_count FROM packet_stats_by_proto WHERE proto='DNS'"));
    }

    // events with nothing to project

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("{ not json }")]
    public async Task Unprojectable_event_is_skipped_and_touches_nothing(string? value)
    {
        Assert.Equal(ProjectionOutcome.Skipped, await CreateHandler().HandleAsync(value, default));
        Assert.Equal(0L, await ScalarAsync<long>("SELECT count(*) FROM packet_ledger"));
    }

    [Fact]
    public async Task Delete_event_is_skipped_and_leaves_the_ledger_untouched()
    {
        var handler = CreateHandler();
        Assert.Equal(ProjectionOutcome.Projected, await handler.HandleAsync(KafkaValue("tx-1"), default));

        Assert.Equal(ProjectionOutcome.Skipped,
            await handler.HandleAsync(KafkaValue("tx-1", op: "d"), default));

        Assert.Equal(1L, await ScalarAsync<long>("SELECT count(*) FROM packet_ledger"));
    }

    //helpers

    private RedisFastPath Redis => new(_mux, TimeSpan.FromMinutes(5));

    private ReadModelStore CreateStore() =>
        new(_dataSource ??= new NpgsqlDataSourceBuilder(_pg.GetConnectionString()).Build(),
            NullLogger<ReadModelStore>.Instance);

    private ProjectionHandler CreateHandler() =>
        new(CreateStore(), Redis, NullLogger<ProjectionHandler>.Instance);

    private static string KafkaValue(
        string tx,
        string clientId = "c1",
        long version = 1,
        string proto = "TCP",
        string op = "c") =>
        $$"""
        {
          "_id": "679b0f1a2c3d4e5f60718293",
          "__op": "{{op}}",
          "transaction_id": "{{tx}}",
          "client_id": "{{clientId}}",
          "version": {{version}},
          "proto": "{{proto}}",
          "source_ip": "10.0.0.1",
          "dest_ip": "10.0.0.2",
          "source_port": 51000,
          "dest_port": 443,
          "storedAt": 1767268800000
        }
        """;

    private async Task ExecuteAsync(string sql)
    {
        await using var cmd = (_dataSource ??=
            new NpgsqlDataSourceBuilder(_pg.GetConnectionString()).Build()).CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<T> ScalarAsync<T>(string sql)
    {
        await using var cmd = (_dataSource ??=
            new NpgsqlDataSourceBuilder(_pg.GetConnectionString()).Build()).CreateCommand(sql);

        var value = await cmd.ExecuteScalarAsync();
        return value is null or DBNull ? default! : (T)value;
    }

    private async Task WaitForInitSqlAsync()
    {
        const string probe = """
            SELECT to_regclass('packet_ledger')         IS NOT NULL
               AND to_regclass('client_state')          IS NOT NULL
               AND to_regclass('packet_stats_by_proto') IS NOT NULL
            """;

        var deadline = DateTime.UtcNow.AddMinutes(2);
        while (true)
        {
            try
            {
                if (await ScalarAsync<bool>(probe))
                    return;
            }
            catch (NpgsqlException)
            {
                // Server not accepting connections yet.
            }

            if (DateTime.UtcNow > deadline)
                throw new TimeoutException(
                    "postgres/init.sql did not finish within 2 minutes; the read-model schema is missing.");

            await Task.Delay(TimeSpan.FromMilliseconds(250));
        }
    }
}
