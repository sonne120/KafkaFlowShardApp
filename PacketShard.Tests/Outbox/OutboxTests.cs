using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using PacketShard.Outbox;
using Xunit;

namespace PacketShard.Tests.Outbox;

[Trait("Category", "Infrastructure")]
public sealed class OutboxTests : IClassFixture<MySqlOutboxFixture>, IAsyncLifetime
{
    private readonly MySqlOutboxFixture _fixture;

    public OutboxTests(MySqlOutboxFixture fixture) => _fixture = fixture;

    public Task InitializeAsync() => _fixture.ResetAsync();

    public Task DisposeAsync() => Task.CompletedTask;

    //schema
    [Fact]
    public async Task Initializer_creates_the_table_and_the_reservation_procedure()
    {
        Assert.Equal(1, await _fixture.ScalarAsync<long>(
            "SELECT count(*) FROM information_schema.tables " +
            "WHERE table_schema = DATABASE() AND table_name = 'Outbox'"));

        Assert.Equal(1, await _fixture.ScalarAsync<long>(
            "SELECT count(*) FROM information_schema.routines " +
            "WHERE routine_schema = DATABASE() AND routine_name = 'GetDataFromTempTable'"));
    }

    [Fact]
    public async Task Initializer_is_idempotent_across_restarts()
    {
        await _fixture.InitializeSchemaAsync();
        await _fixture.InitializeSchemaAsync();

        Assert.Equal(1, await _fixture.ScalarAsync<long>(
            "SELECT count(*) FROM information_schema.routines " +
            "WHERE routine_schema = DATABASE() AND routine_name = 'GetDataFromTempTable'"));
    }

    //writing

    [Fact]
    public async Task AddAsync_persists_payload_topic_partition_key_and_metadata()
    {
        await AddAsync(
            new TestPacket { TransactionId = "tx-1", Proto = "UDP", SourceIp = "10.0.0.1" },
            topic: "SnapshotTopic",
            partitionBy: packet => packet.SourceIp,
            metadata: new Dictionary<string, string> { ["origin"] = "srv_ingest" });

        var row = Assert.Single(await _fixture.RowsAsync());
        Assert.Contains("\"TransactionId\":\"tx-1\"", row.RawData);
        Assert.Equal("SnapshotTopic", row.Topic);
        Assert.Equal("10.0.0.1", row.PartitionBy);      // becomes the Kafka partition key
        Assert.Contains("origin", row.Metadata);
        Assert.StartsWith(typeof(TestPacket).FullName, row.MessageType);
        Assert.False(row.IsProcessed);
        Assert.False(row.IsProcessing);
        Assert.Null(row.ReservedAt);
    }

    [Fact]
    public async Task AddAsync_without_a_partition_key_leaves_it_null()
    {
        await AddAsync(new TestPacket { TransactionId = "tx-1" }, partitionBy: null);

        Assert.Null(Assert.Single(await _fixture.RowsAsync()).PartitionBy);
    }

    //reserving

    [Fact]
    public async Task ReserveAsync_returns_pending_rows_and_marks_them_processing()
    {
        await AddManyAsync(3);

        var reserved = await ReserveAsync(top: 10, TimeSpan.FromSeconds(30));

        Assert.Equal(3, reserved.Length);
        Assert.All(await _fixture.RowsAsync(), row =>
        {
            Assert.True(row.IsProcessing);
            Assert.NotNull(row.ReservedAt);
            Assert.NotNull(row.ExpiredAt);
            Assert.False(row.IsProcessed);   // reserved is not processed
        });
    }

    [Fact]
    public async Task ReserveAsync_honours_the_batch_limit_and_takes_the_oldest_first()
    {
        await AddManyAsync(5);

        var reserved = await ReserveAsync(top: 2, TimeSpan.FromSeconds(30));

        Assert.Equal(2, reserved.Length);
        Assert.Equal(new[] { "tx-0", "tx-1" }, reserved.Select(TransactionIdOf).OrderBy(id => id));
    }

    [Fact]
    public async Task Reserved_rows_are_invisible_to_the_next_worker()
    {
        await AddManyAsync(2);

        var first = await ReserveAsync(top: 10, TimeSpan.FromSeconds(30));
        var second = await ReserveAsync(top: 10, TimeSpan.FromSeconds(30));

        Assert.Equal(2, first.Length);
        Assert.Empty(second);   // no double-publish
    }

    [Fact]
    public async Task Abandoned_reservations_return_to_the_pool_once_they_expire()
    {
        await AddManyAsync(1);

        Assert.Single(await ReserveAsync(top: 10, TimeSpan.FromSeconds(1)));
        Assert.Empty(await ReserveAsync(top: 10, TimeSpan.FromSeconds(1)));

        await Task.Delay(TimeSpan.FromSeconds(2));

        Assert.Single(await ReserveAsync(top: 10, TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public async Task Reservation_timeout_longer_than_a_minute_is_honoured()
    {
        await AddManyAsync(1);

        await ReserveAsync(top: 10, TimeSpan.FromMinutes(2));

        var heldForSeconds = await _fixture.ScalarAsync<long>(
            "SELECT TIMESTAMPDIFF(SECOND, ReservedAt, ExpiredAt) FROM Outbox LIMIT 1");

        Assert.Equal(120, heldForSeconds);
    }

    [Fact]
    public async Task Sequential_messages_are_left_for_a_different_path()
    {
        // The reservation procedure filters IsSequential = 0 — ordered streams are deliberately
        // not drained by the parallel relay.
        await AddAsync(new TestPacket { TransactionId = "tx-seq" }, isSequential: true);
        await AddAsync(new TestPacket { TransactionId = "tx-par" }, isSequential: false);

        var reserved = await ReserveAsync(top: 10, TimeSpan.FromSeconds(30));

        Assert.Equal("tx-par", TransactionIdOf(Assert.Single(reserved)));
    }

    [Fact]
    public async Task Concurrent_workers_never_reserve_the_same_row()
    {
        // srv_pub runs three replicas against one outbox. Each worker calls ReserveAsync concurrently, 
        // and the procedure must ensure
        const int rows = 40;
        await AddManyAsync(rows);

        var batches = await Task.WhenAll(
            Enumerable.Range(0, 4).Select(_ => ReserveAsync(top: rows, TimeSpan.FromSeconds(30))));

        var claimed = batches.SelectMany(batch => batch.Select(record => record.Id)).ToList();

        Assert.Equal(rows, claimed.Count);                 // nothing lost
        Assert.Equal(rows, claimed.Distinct().Count());    // nothing claimed twice
    }

    //completing

    [Fact]
    public async Task MarkAsProcessedAsync_takes_rows_out_of_circulation()
    {
        await AddManyAsync(2);

        await using (var scope = _fixture.CreateScope())
        {
            var (outbox, unitOfWork) = Resolve(scope);
            await using var tx = await unitOfWork.BeginTransactionAsync(default);
            var reserved = await outbox.ReserveAsync(10, TimeSpan.FromSeconds(30), default);
            await outbox.MarkAsProcessedAsync(reserved, default);
            await tx.CommitAsync(default);
        }

        Assert.All(await _fixture.RowsAsync(), row => Assert.True(row.IsProcessed));
        Assert.Empty(await ReserveAsync(top: 10, TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task DeleteProcessedAsync_removes_processed_rows_and_spares_the_rest()
    {
        await AddManyAsync(3);

        await using (var scope = _fixture.CreateScope())
        {
            var (outbox, unitOfWork) = Resolve(scope);
            await using var tx = await unitOfWork.BeginTransactionAsync(default);
            var reserved = await outbox.ReserveAsync(2, TimeSpan.FromSeconds(30), default);
            await outbox.MarkAsProcessedAsync(reserved, default);
            await tx.CommitAsync(default);
        }

        await using (var scope = _fixture.CreateScope())
        {
            var (outbox, unitOfWork) = Resolve(scope);
            await using var tx = await unitOfWork.BeginTransactionAsync(default);
            await outbox.DeleteProcessedAsync(default);
            await tx.CommitAsync(default);
        }

        var remaining = await _fixture.RowsAsync();
        Assert.Single(remaining);
        Assert.False(remaining[0].IsProcessed);
    }

    [Fact]
    public async Task Outbox_operations_outside_a_transaction_are_refused()
    {
        // Reserve/mark/delete all read the ambient transaction, which is how they avoid race conditions
        await using var scope = _fixture.CreateScope();
        var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => outbox.ReserveAsync(10, TimeSpan.FromSeconds(30), default));
    }

    //helpers

    private static (IOutbox Outbox, IUnitOfWork UnitOfWork) Resolve(AsyncServiceScope scope) =>
        (scope.ServiceProvider.GetRequiredService<IOutbox>(),
         scope.ServiceProvider.GetRequiredService<IUnitOfWork>());

    private async Task AddAsync(
        TestPacket packet,
        string topic = "SnapshotTopic",
        Func<TestPacket, string>? partitionBy = null,
        bool isSequential = false,
        Dictionary<string, string>? metadata = null)
    {
        await using var scope = _fixture.CreateScope();
        await scope.ServiceProvider.GetRequiredService<IOutbox>()
            .AddAsync(packet, topic, partitionBy, isSequential, metadata, CancellationToken.None);
    }

    private async Task AddManyAsync(int count)
    {
        for (var i = 0; i < count; i++)
            await AddAsync(new TestPacket { TransactionId = $"tx-{i}", Proto = "UDP" });
    }

    private async Task<ImmutableArray<OutboxRecord>> ReserveAsync(int top, TimeSpan timeout)
    {
        await using var scope = _fixture.CreateScope();
        var (outbox, unitOfWork) = Resolve(scope);
        await using var tx = await unitOfWork.BeginTransactionAsync(default);
        var reserved = await outbox.ReserveAsync(top, timeout, default);
        await tx.CommitAsync(default);
        return reserved;
    }

    private static string TransactionIdOf(OutboxRecord record) =>
        Newtonsoft.Json.Linq.JObject.Parse(record.JsonRawData).Value<string>("TransactionId")!;
}

public sealed class TestPacket
{
    public string TransactionId { get; set; } = default!;
    public string Proto { get; set; } = "UDP";
    public string SourceIp { get; set; } = "10.0.0.1";
}
