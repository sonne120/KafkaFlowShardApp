using System.Collections.Immutable;
using Grpc.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using PacketShard.Ingest.Grpc;
using PacketShard.Ingest.Services;
using PacketShard.Outbox;
using PacketShard.Shared;
using Xunit;

namespace PacketShard.Tests.Ingest;

[Trait("Category", "Unit")]
public sealed class PacketIngestServiceTests
{
    [Fact]
    public async Task Send_writes_one_outbox_row_and_reports_it_accepted()
    {
        var outbox = new RecordingOutbox();
        var service = CreateService(outbox);

        var reply = await service.Send(Packet(), Context());

        Assert.True(reply.Ok);
        Assert.Equal(1, reply.Accepted);
        Assert.Single(outbox.Writes);
    }

    [Fact]
    public async Task Send_maps_the_request_onto_the_snapshot_message()
    {
        var outbox = new RecordingOutbox();

        await CreateService(outbox).Send(Packet(proto: "UDP", sourceIp: "10.0.0.1", destIp: "10.0.0.2"), Context());

        var packet = Assert.IsType<SnapshotMessage>(Assert.Single(outbox.Writes).Data);
        Assert.Equal("UDP", packet.proto);
        Assert.Equal("10.0.0.1", packet.source_ip);
        Assert.Equal("10.0.0.2", packet.dest_ip);
        Assert.Equal(51000, packet.source_port);
        Assert.Equal(443, packet.dest_port);
        Assert.False(string.IsNullOrWhiteSpace(packet.transaction_id));  // the read-model dedup key
    }

    [Fact]
    public async Task Each_packet_gets_its_own_transaction_id()
    {
        // Two identical requests must not collide on the read side's UNIQUE key constraint, so each must
        //  get a different transaction_id.
        var outbox = new RecordingOutbox();
        var service = CreateService(outbox);

        await service.Send(Packet(), Context());
        await service.Send(Packet(), Context());

        var ids = outbox.Writes.Select(w => ((SnapshotMessage)w.Data).transaction_id).ToList();
        Assert.Equal(2, ids.Distinct().Count());
    }

    [Fact]
    public async Task Client_id_falls_back_to_unknown_when_the_source_ip_is_missing()
    {
        // client_id is NOT NULL in the read model, so an empty source ip cannot pass through.
        var outbox = new RecordingOutbox();

        await CreateService(outbox).Send(Packet(sourceIp: ""), Context());

        Assert.Equal("unknown", ((SnapshotMessage)Assert.Single(outbox.Writes).Data).client_id);
    }

    [Fact]
    public async Task Dest_ip_becomes_the_kafka_partition_key()
    {
        var outbox = new RecordingOutbox();

        await CreateService(outbox).Send(Packet(destIp: "10.0.0.9"), Context());

        Assert.Equal("10.0.0.9", Assert.Single(outbox.Writes).PartitionKey);
    }

    [Fact]
    public async Task Proto_is_attached_as_metadata_and_the_row_is_not_sequential()
    {
        var outbox = new RecordingOutbox();

        await CreateService(outbox).Send(Packet(proto: "ARP"), Context());

        var write = Assert.Single(outbox.Writes);
        Assert.Equal("ARP", write.Metadata!["proto"]);
        // IsSequential rows are skipped by the relay's reservation query; packets must not be.
        Assert.False(write.IsSequential);
    }

    [Fact]
    public async Task Topic_comes_from_configuration_with_a_default()
    {
        var configured = new RecordingOutbox();
        await CreateService(configured, topic: "CustomTopic").Send(Packet(), Context());
        Assert.Equal("CustomTopic", Assert.Single(configured.Writes).Topic);

        var defaulted = new RecordingOutbox();
        await CreateService(defaulted, topic: null).Send(Packet(), Context());
        Assert.Equal("SnapshotTopic", Assert.Single(defaulted.Writes).Topic);
    }

    //streaming
    [Fact]
    public async Task SendStream_writes_every_packet_and_counts_them()
    {
        var outbox = new RecordingOutbox();
        var stream = new FakeStream(Enumerable.Range(0, 5).Select(i => Packet(sourceIp: $"10.0.0.{i}")));

        var reply = await CreateService(outbox).SendStream(stream, Context());

        Assert.True(reply.Ok);
        Assert.Equal(5, reply.Accepted);
        Assert.Equal(5, outbox.Writes.Count);
    }

    [Fact]
    public async Task SendStream_flushes_across_the_batch_boundary_without_losing_packets()
    {

        var outbox = new RecordingOutbox();
        var stream = new FakeStream(Enumerable.Range(0, 250).Select(_ => Packet()));

        var reply = await CreateService(outbox).SendStream(stream, Context());

        Assert.Equal(250, reply.Accepted);
        Assert.Equal(250, outbox.Writes.Count);
    }

    [Fact]
    public async Task SendStream_with_no_packets_accepts_nothing()
    {
        var outbox = new RecordingOutbox();

        var reply = await CreateService(outbox).SendStream(new FakeStream([]), Context());

        Assert.True(reply.Ok);
        Assert.Equal(0, reply.Accepted);
        Assert.Empty(outbox.Writes);
    }

    [Fact]
    public async Task A_failing_outbox_write_surfaces_to_the_caller()
    {
        // The client must learn the packet was not durably stored, rather than get an Ok that
        // durability never backed.
        var outbox = new RecordingOutbox { FailWith = new InvalidOperationException("mysql down") };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateService(outbox).Send(Packet(), Context()));
    }

    //helpers
    private static PacketIngestService CreateService(IOutbox outbox, string? topic = "SnapshotTopic")
    {
        var settings = new Dictionary<string, string?>();
        if (topic is not null)
            settings["Topic"] = topic;

        return new PacketIngestService(
            new SingleScopeFactory(outbox),
            new ConfigurationBuilder().AddInMemoryCollection(settings).Build(),
            NullLogger<PacketIngestService>.Instance);
    }

    private static PacketRequest Packet(
        string proto = "UDP", string sourceIp = "10.0.0.1", string destIp = "10.0.0.2") =>
        new()
        {
            SourcePort = 51000,
            DestPort = 443,
            SourceIp = sourceIp,
            DestIp = destIp,
            SourceMac = "aa:bb:cc:dd:ee:ff",
            DestMac = "ff:ee:dd:cc:bb:aa",
            Proto = proto
        };

    private static ServerCallContext Context(CancellationToken cancellationToken = default) =>
        new TestCallContext(cancellationToken);

    private sealed class TestCallContext(CancellationToken cancellationToken) : ServerCallContext
    {
        protected override string MethodCore => "Send";
        protected override string HostCore => "localhost";
        protected override string PeerCore => "ipv4:127.0.0.1:0";
        protected override DateTime DeadlineCore => DateTime.UtcNow.AddMinutes(1);
        protected override Metadata RequestHeadersCore { get; } = new();
        protected override CancellationToken CancellationTokenCore { get; } = cancellationToken;
        protected override Metadata ResponseTrailersCore { get; } = new();
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore { get; } = new(null, new Dictionary<string, List<AuthProperty>>());

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options)
            => throw new NotSupportedException();

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders)
            => Task.CompletedTask;
    }

    private sealed record OutboxWrite(
        object Data, string Topic, string? PartitionKey, bool IsSequential, Dictionary<string, string>? Metadata);

    private sealed class RecordingOutbox : IOutbox
    {
        public List<OutboxWrite> Writes { get; } = new();
        public Exception? FailWith { get; init; }

        public Task AddAsync<T>(T data, string topic, Func<T, string>? partitionBy, bool isSequential,
            Dictionary<string, string>? metadata, CancellationToken cancellationToken) where T : class
        {
            if (FailWith is not null)
                return Task.FromException(FailWith);

            Writes.Add(new OutboxWrite(data, topic, partitionBy?.Invoke(data), isSequential, metadata));
            return Task.CompletedTask;
        }

        public Task<ImmutableArray<OutboxRecord>> ReserveAsync(int top, TimeSpan reservationTimeout, CancellationToken ct)
            => throw new NotSupportedException("ingest never reserves");

        public Task MarkAsProcessedAsync(ImmutableArray<OutboxRecord> data, CancellationToken ct)
            => throw new NotSupportedException("ingest never marks");

        public Task DeleteProcessedAsync(CancellationToken ct)
            => throw new NotSupportedException("ingest never cleans up");
    }

    private sealed class SingleScopeFactory(IOutbox outbox) : IServiceScopeFactory, IServiceScope, IServiceProvider
    {
        public IServiceScope CreateScope() => this;
        public IServiceProvider ServiceProvider => this;
        public object? GetService(Type serviceType) => serviceType == typeof(IOutbox) ? outbox : null;
        public void Dispose() { }
    }

    private sealed class FakeStream(IEnumerable<PacketRequest> packets) : IAsyncStreamReader<PacketRequest>
    {
        private readonly IEnumerator<PacketRequest> _packets = packets.GetEnumerator();

        public PacketRequest Current { get; private set; } = default!;

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            if (!_packets.MoveNext())
                return Task.FromResult(false);

            Current = _packets.Current;
            return Task.FromResult(true);
        }
    }
}
