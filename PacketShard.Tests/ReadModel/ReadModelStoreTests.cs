using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json.Linq;
using Npgsql;
using PacketShard.Read;
using Testcontainers.PostgreSql;
using Xunit;

namespace PacketShard.Tests.ReadModel;

[Trait("Category", "Infrastructure")]
public sealed class ReadModelStoreTests : IAsyncLifetime
{
    private PostgreSqlContainer _pg = null!;
    private NpgsqlDataSource? _dataSource;

    public async Task InitializeAsync()
    {
        _pg = new PostgreSqlBuilder()
            .WithImage(await ReadModelImage.EnsureAsync())
            .WithDatabase("readmodel")   // init.sql runs against POSTGRES_DB
            .WithUsername("postgres")
            .WithPassword("postgres")
            .Build();

        await _pg.StartAsync();
        await WaitForInitSqlAsync();
    }

    public async Task DisposeAsync()
    {
        if (_dataSource is not null)
            await _dataSource.DisposeAsync();

        await _pg.DisposeAsync();
    }

    //dedup (exactly-once)
    [Fact]
    public async Task Duplicate_is_absorbed_and_immv_counts_once()
    {
        var store = CreateStore(_pg.GetConnectionString());
        var record = TestRecord(tx: "tx-1", proto: "TCP");

        Assert.True(await store.ProjectAsync(record, default));    // first delivery → insert
        Assert.False(await store.ProjectAsync(record, default));   // second delivery → absorbed

        Assert.Equal(1L, await ScalarAsync<long>("SELECT count(*) FROM packet_ledger"));
        Assert.Equal(1L, await ScalarAsync<long>(
            "SELECT packet_count FROM packet_stats_by_proto WHERE proto='TCP'"));
    }

    [Fact]
    public async Task Concurrent_duplicates_insert_exactly_one_row()
    {

        var store = CreateStore(_pg.GetConnectionString());
        var record = TestRecord(tx: "tx-race", proto: "UDP");

        var results = await Task.WhenAll(
            Enumerable.Range(0, 8).Select(_ => store.ProjectAsync(record, default)));

        Assert.Equal(1, results.Count(inserted => inserted));
        Assert.Equal(1L, await ScalarAsync<long>("SELECT count(*) FROM packet_ledger"));
        Assert.Equal(1L, await ScalarAsync<long>(
            "SELECT packet_count FROM packet_stats_by_proto WHERE proto='UDP'"));
    }

    // ordering (version guard)
    [Fact]
    public async Task Stale_version_does_not_overwrite_client_state()
    {
        var store = CreateStore(_pg.GetConnectionString());
        await store.ProjectAsync(TestRecord("tx-a", version: 5, proto: "HTTPS"), default);
        await store.ProjectAsync(TestRecord("tx-b", version: 3, proto: "TCP"), default); 

        Assert.Equal(5L, await ScalarAsync<long>(
            "SELECT version FROM client_state WHERE client_id='c1'"));
        Assert.Equal("HTTPS", await ScalarAsync<string>(
            "SELECT last_proto FROM client_state WHERE client_id='c1'"));

        Assert.Equal(2L, await ScalarAsync<long>("SELECT count(*) FROM packet_ledger"));
    }

    [Fact]
    public async Task Newer_version_advances_client_state()
    {
        var store = CreateStore(_pg.GetConnectionString());
        await store.ProjectAsync(TestRecord("tx-a", version: 1, proto: "TCP"), default);
        await store.ProjectAsync(TestRecord("tx-b", version: 9, proto: "DNS"), default);

        Assert.Equal(9L, await ScalarAsync<long>(
            "SELECT version FROM client_state WHERE client_id='c1'"));
        Assert.Equal("DNS", await ScalarAsync<string>(
            "SELECT last_proto FROM client_state WHERE client_id='c1'"));
    }

    [Fact]
    public async Task Equal_version_leaves_client_state_unchanged()
    {
        var store = CreateStore(_pg.GetConnectionString());
        await store.ProjectAsync(TestRecord("tx-a", version: 4, proto: "HTTPS"), default);
        await store.ProjectAsync(TestRecord("tx-b", version: 4, proto: "TCP"), default);

        Assert.Equal("HTTPS", await ScalarAsync<string>(
            "SELECT last_proto FROM client_state WHERE client_id='c1'"));
    }

    [Fact]
    public async Task Different_clients_keep_independent_state()
    {
        var store = CreateStore(_pg.GetConnectionString());
        await store.ProjectAsync(TestRecord("tx-a", clientId: "c1", version: 10), default);
        await store.ProjectAsync(TestRecord("tx-b", clientId: "c2", version: 2), default);

        Assert.Equal(10L, await ScalarAsync<long>(
            "SELECT version FROM client_state WHERE client_id='c1'"));
        Assert.Equal(2L, await ScalarAsync<long>(
            "SELECT version FROM client_state WHERE client_id='c2'"));
    }

    //IMMV (pg_ivm)
    [Fact]
    public async Task Immv_tracks_counts_and_time_bounds_without_refresh()
    {
        var store = CreateStore(_pg.GetConnectionString());
        var t0 = new DateTimeOffset(2026, 2, 1, 9, 0, 0, TimeSpan.Zero);

        await store.ProjectAsync(TestRecord("tx-1", proto: "TCP", storedAt: t0), default);
        await store.ProjectAsync(TestRecord("tx-2", proto: "TCP", storedAt: t0.AddMinutes(2)), default);
        await store.ProjectAsync(TestRecord("tx-3", proto: "TCP", storedAt: t0.AddMinutes(1)), default);
        await store.ProjectAsync(TestRecord("tx-4", proto: "UDP", storedAt: t0.AddSeconds(30)), default);
        await store.ProjectAsync(TestRecord("tx-5", proto: "UDP", storedAt: t0.AddSeconds(90)), default);

        // Read straight out of the IMMV — no REFRESH MATERIALIZED VIEW anywhere in this test.
        Assert.Equal(2L, await ScalarAsync<long>("SELECT count(*) FROM packet_stats_by_proto"));

        Assert.Equal(3L, await ScalarAsync<long>(
            "SELECT packet_count FROM packet_stats_by_proto WHERE proto='TCP'"));
        Assert.Equal(t0.UtcDateTime, await ScalarAsync<DateTime>(
            "SELECT first_seen FROM packet_stats_by_proto WHERE proto='TCP'"));
        Assert.Equal(t0.AddMinutes(2).UtcDateTime, await ScalarAsync<DateTime>(
            "SELECT last_seen FROM packet_stats_by_proto WHERE proto='TCP'"));

        Assert.Equal(2L, await ScalarAsync<long>(
            "SELECT packet_count FROM packet_stats_by_proto WHERE proto='UDP'"));
        Assert.Equal(t0.AddSeconds(30).UtcDateTime, await ScalarAsync<DateTime>(
            "SELECT first_seen FROM packet_stats_by_proto WHERE proto='UDP'"));
        Assert.Equal(t0.AddSeconds(90).UtcDateTime, await ScalarAsync<DateTime>(
            "SELECT last_seen FROM packet_stats_by_proto WHERE proto='UDP'"));
    }

    //column mapping
    [Fact]
    public async Task New_record_is_projected_with_every_column()
    {
        var store = CreateStore(_pg.GetConnectionString());
        var storedAt = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

        Assert.True(await store.ProjectAsync(
            TestRecord("tx-1", clientId: "c1", version: 7, proto: "HTTPS", storedAt: storedAt),
            default));

        Assert.Equal("c1", await ScalarAsync<string>("SELECT client_id FROM packet_ledger"));
        Assert.Equal(7L, await ScalarAsync<long>("SELECT version FROM packet_ledger"));
        Assert.Equal("HTTPS", await ScalarAsync<string>("SELECT proto FROM packet_ledger"));
        Assert.Equal("10.0.0.1", await ScalarAsync<string>("SELECT source_ip FROM packet_ledger"));
        Assert.Equal("10.0.0.2", await ScalarAsync<string>("SELECT dest_ip FROM packet_ledger"));
        Assert.Equal(51000, await ScalarAsync<int>("SELECT source_port FROM packet_ledger"));
        Assert.Equal(443, await ScalarAsync<int>("SELECT dest_port FROM packet_ledger"));
        Assert.Equal(storedAt.UtcDateTime, await ScalarAsync<DateTime>("SELECT stored_at FROM packet_ledger"));
    }

    [Fact]
    public async Task Missing_timestamp_is_stored_as_null()
    {
        var store = CreateStore(_pg.GetConnectionString());

        await store.ProjectAsync(TestRecord("tx-1") with { StoredAt = null }, default);

        Assert.True(await ScalarAsync<bool>("SELECT stored_at IS NULL FROM packet_ledger"));
    }

    [Fact]
    public async Task Payload_lands_as_queryable_jsonb()
    {
        var store = CreateStore(_pg.GetConnectionString());
        await store.ProjectAsync(TestRecord("tx-1", proto: "DNS"), default);

        Assert.Equal("DNS", await ScalarAsync<string>("SELECT payload ->> 'proto' FROM packet_ledger"));
        Assert.Equal("tx-1", await ScalarAsync<string>("SELECT payload ->> 'transaction_id' FROM packet_ledger"));
    }

    //failure atomicity
    [Fact]
    public async Task Failed_write_throws_and_leaves_no_partial_state()
    {
        var store = CreateStore(_pg.GetConnectionString());
        var record = TestRecord("tx-1") with { Payload = "{ this is not json" };

        await Assert.ThrowsAsync<PostgresException>(() => store.ProjectAsync(record, default));

        Assert.Equal(0L, await ScalarAsync<long>("SELECT count(*) FROM packet_ledger"));
        Assert.Equal(0L, await ScalarAsync<long>("SELECT count(*) FROM client_state"));
    }

    //Debezium end-to-end

    [Fact]
    public async Task Debezium_event_flows_from_kafka_value_to_read_model()
    {
        const string kafkaValue = """
            {
              "_id": "679b0f1a2c3d4e5f60718293",
              "__op": "c",
              "transaction_id": "tx-debezium",
              "client_id": "c1",
              "version": 42,
              "proto": "HTTPS",
              "source_ip": "192.168.1.10",
              "dest_ip": "93.184.216.34",
              "source_port": 51514,
              "dest_port": 443,
              "storedAt": 1767268800000
            }
            """;

        var record = PacketRecord.TryParse(kafkaValue);
        Assert.NotNull(record);

        var store = CreateStore(_pg.GetConnectionString());
        Assert.True(await store.ProjectAsync(record!, default));

        Assert.Equal(42L, await ScalarAsync<long>("SELECT version FROM packet_ledger"));
        Assert.Equal(51514, await ScalarAsync<int>("SELECT source_port FROM packet_ledger"));
        Assert.Equal(
            DateTimeOffset.FromUnixTimeMilliseconds(1767268800000).UtcDateTime,
            await ScalarAsync<DateTime>("SELECT stored_at FROM packet_ledger"));

        // The untouched document is still in the payload, so the read side can serve it to clients.
        Assert.Equal("679b0f1a2c3d4e5f60718293",
            await ScalarAsync<string>("SELECT payload ->> '_id' FROM packet_ledger"));

        Assert.Equal(42L, await ScalarAsync<long>(
            "SELECT version FROM client_state WHERE client_id='c1'"));
    }

    //helpers
    private ReadModelStore CreateStore(string connectionString) =>
        new(_dataSource ??= new NpgsqlDataSourceBuilder(connectionString).Build(),
            NullLogger<ReadModelStore>.Instance);

    private static PacketRecord TestRecord(
        string tx,
        string clientId = "c1",
        long version = 1,
        string proto = "TCP",
        DateTimeOffset? storedAt = null) =>
        new(
            TransactionId: tx,
            ClientId: clientId,
            Version: version,
            Proto: proto,
            SourceIp: "10.0.0.1",
            DestIp: "10.0.0.2",
            SourcePort: 51000,
            DestPort: 443,
            StoredAt: storedAt ?? new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
            Payload: new JObject
            {
                ["transaction_id"] = tx,
                ["client_id"] = clientId,
                ["version"] = version,
                ["proto"] = proto
            }.ToString(Newtonsoft.Json.Formatting.None));

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
