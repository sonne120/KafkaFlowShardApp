using Grpc.Core;
using KafkaFlowShardApp.Ingest.Grpc;
using KafkaFlowShardApp.Outbox;
using KafkaFlowShardApp.Shared;

namespace KafkaFlowShardApp.Ingest.Services;

/// <summary>
/// gRPC ingestion endpoint. Each accepted packet is written to the same MySQL outbox that
/// srv_pub uses — so client-sent packets flow through the identical outbox -> Kafka ->
/// MasterNode -> shards -> read-model pipeline. The outbox give us the durable, no-dual-write
/// guarantee; this service just maps the wire message into a SnapshotMessage and stores it.
/// </summary>
public sealed class PacketIngestService : PacketIngest.PacketIngestBase
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<PacketIngestService> _logger;
    private readonly string _topic;

    public PacketIngestService(
        IServiceScopeFactory scopeFactory,
        IConfiguration configuration,
        ILogger<PacketIngestService> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        _topic = configuration["Topic"] ?? "SnapshotTopic";
    }

    public override async Task<IngestReply> Send(PacketRequest request, ServerCallContext context)
    {
        using var scope = _scopeFactory.CreateScope();
        var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();
        await WriteAsync(outbox, request, context.CancellationToken);
        return new IngestReply { Ok = true, Accepted = 1, Message = "stored" };
    }

    public override async Task<IngestReply> SendStream(
        IAsyncStreamReader<PacketRequest> requestStream, ServerCallContext context)
    {
        // One scope (one DB context) for the whole stream — each packet is its own outbox row.
        using var scope = _scopeFactory.CreateScope();
        var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();

        var accepted = 0;
        await foreach (var request in requestStream.ReadAllAsync(context.CancellationToken))
        {
            await WriteAsync(outbox, request, context.CancellationToken);
            accepted++;
        }

        _logger.LogInformation("Ingest stream stored {Count} packet(s)", accepted);
        return new IngestReply { Ok = true, Accepted = accepted, Message = $"stored {accepted}" };
    }

    private async Task WriteAsync(IOutbox outbox, PacketRequest request, CancellationToken ct)
    {
        var packet = new SnapshotMessage
        {
            transaction_id = Guid.NewGuid().ToString(),
            client_id = string.IsNullOrWhiteSpace(request.SourceIp) ? "unknown" : request.SourceIp,
            version = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            source_port = request.SourcePort,
            dest_port = request.DestPort,
            source_ip = request.SourceIp,
            dest_ip = request.DestIp,
            source_mac = request.SourceMac,
            dest_mac = request.DestMac,
            proto = request.Proto
        };

        await outbox.AddAsync(
            data: packet,
            topic: _topic,
            partitionBy: p => p.dest_ip,
            isSequential: false,
            metadata: new Dictionary<string, string> { { "proto", packet.proto } },
            cancellationToken: ct);

        _logger.LogInformation("Ingested {Proto} {SrcIp}:{SrcPort} -> {DstIp}:{DstPort}",
            packet.proto, packet.source_ip, packet.source_port, packet.dest_ip, packet.dest_port);
    }
}
