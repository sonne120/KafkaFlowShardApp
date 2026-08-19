using Microsoft.Extensions.DependencyInjection;
using PacketShard.Outbox;
using Xunit;

namespace PacketShard.Tests.Outbox;

[Trait("Category", "Infrastructure")]
public sealed class RelayTests : IClassFixture<MySqlOutboxFixture>, IAsyncLifetime
{
    private readonly MySqlOutboxFixture _fixture;

    public RelayTests(MySqlOutboxFixture fixture) => _fixture = fixture;

    public Task InitializeAsync() => _fixture.ResetAsync();

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task PublishAsync_hands_every_pending_row_to_kafka_and_marks_it_processed()
    {
        await AddAsync(new TestPacket { TransactionId = "tx-1", SourceIp = "10.0.0.1" });
        await AddAsync(new TestPacket { TransactionId = "tx-2", SourceIp = "10.0.0.2" });

        await PublishAsync();

        var sent = _fixture.Kafka.Sent;
        Assert.Equal(2, sent.Count);
        Assert.All(sent, message =>
        {
            Assert.Equal("SnapshotTopic", message.Topic);
            Assert.StartsWith(typeof(TestPacket).FullName, message.PayloadType);
            Assert.NotEqual(default, message.Created);
        });

        // PartitionBy travels through as the Kafka key — that is what keeps a client's packets
        // on one partition, which the read side's version guard depends on.
        Assert.Equal(new[] { "10.0.0.1", "10.0.0.2" }, sent.Select(m => m.Key).OrderBy(k => k));

        Assert.All(await _fixture.RowsAsync(), row => Assert.True(row.IsProcessed));
    }

    [Fact]
    public async Task PublishAsync_carries_metadata_through_to_the_message()
    {
        await AddAsync(
            new TestPacket { TransactionId = "tx-1" },
            metadata: new Dictionary<string, string> { ["origin"] = "srv_ingest" });

        await PublishAsync();

        var message = Assert.Single(_fixture.Kafka.Sent);
        Assert.NotNull(message.Metadata);
        Assert.Equal("srv_ingest", message.Metadata!["origin"]);
    }

    [Fact]
    public async Task Failed_publish_rolls_back_and_leaves_the_row_pending()
    {
        // The at-least-once guarantee in one test is that if the relay crashes after committing 
        // to Postgres but before marking the row processed in Redis, the next relay run will see
        await AddAsync(new TestPacket { TransactionId = "tx-1" });
        _fixture.Kafka.FailWith = new InvalidOperationException("kafka unreachable");

        await Assert.ThrowsAsync<InvalidOperationException>(PublishAsync);

        var row = Assert.Single(await _fixture.RowsAsync());
        Assert.False(row.IsProcessed);
        Assert.False(row.IsProcessing); 
        Assert.Empty(_fixture.Kafka.Sent);

 
        _fixture.Kafka.FailWith = null;
        await PublishAsync();

        Assert.Single(_fixture.Kafka.Sent);
        Assert.True(Assert.Single(await _fixture.RowsAsync()).IsProcessed);
    }

    [Fact]
    public async Task PublishAsync_on_an_empty_outbox_is_a_no_op()
    {
        await PublishAsync();

        Assert.Empty(_fixture.Kafka.Sent);
        Assert.Empty(await _fixture.RowsAsync());
    }

    [Fact]
    public async Task Republishing_after_a_successful_drain_sends_nothing_twice()
    {
        await AddAsync(new TestPacket { TransactionId = "tx-1" });

        await PublishAsync();
        await PublishAsync();

        Assert.Single(_fixture.Kafka.Sent);
    }

    [Fact]
    public async Task CleanupAsync_reclaims_only_what_has_been_published()
    {
        await AddAsync(new TestPacket { TransactionId = "tx-published" });
        await PublishAsync();
        await AddAsync(new TestPacket { TransactionId = "tx-still-pending" });

        await using (var scope = _fixture.CreateScope())
            await scope.ServiceProvider.GetRequiredService<IRelay>().CleanupAsync(default);

        var remaining = Assert.Single(await _fixture.RowsAsync());
        Assert.Contains("tx-still-pending", remaining.RawData);
    }

    //helpers
    private async Task PublishAsync()
    {
        await using var scope = _fixture.CreateScope();
        await scope.ServiceProvider.GetRequiredService<IRelay>().PublishAsync(CancellationToken.None);
    }

    private async Task AddAsync(TestPacket packet, Dictionary<string, string>? metadata = null)
    {
        await using var scope = _fixture.CreateScope();
        await scope.ServiceProvider.GetRequiredService<IOutbox>().AddAsync(
            packet, "SnapshotTopic", p => p.SourceIp, false, metadata, CancellationToken.None);
    }
}
