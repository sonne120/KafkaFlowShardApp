using Grpc.Core;
using KafkaFlowShardApp.Ingest.Grpc;
using KafkaFlowShardApp.Outbox;
using KafkaFlowShardApp.Shared;

namespace KafkaFlowShardApp.Ingest.Services;


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
        var packet = MapRequestToSnapshot(request);
        await WritePacketAsync(outbox, packet, context.CancellationToken);
        return new IngestReply { Ok = true, Accepted = 1, Message = "stored" };
    }

    public override async Task<IngestReply> SendStream(
        IAsyncStreamReader<PacketRequest> requestStream, ServerCallContext context)
    {
  
        using var scope = _scopeFactory.CreateScope();
        var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();

        var accepted = 0;
        var batch = new List<SnapshotMessage>();

        const int maxBatchSize = 100;

        await foreach (var request in requestStream.ReadAllAsync(context.CancellationToken))
        {
            batch.Add(MapRequestToSnapshot(request));
            if (batch.Count >= maxBatchSize)
            {
                await WriteBatchAsync(outbox, batch, context.CancellationToken);
                accepted += batch.Count;
                batch.Clear();
            }
        }

        if (batch.Count > 0)
        {
            await WriteBatchAsync(outbox, batch, context.CancellationToken);
            accepted += batch.Count;
        }

        _logger.LogInformation("Ingest stream stored {Count} packet(s)", accepted);
        return new IngestReply { Ok = true, Accepted = accepted, Message = $"stored {accepted}" };
    }


    private async Task WriteBatchAsync(IOutbox outbox, IReadOnlyCollection<SnapshotMessage> packets, CancellationToken ct)
    {
 
        foreach (var packet in packets)
        {
            await WritePacketAsync(outbox, packet, ct);
        }
        _logger.LogInformation("Wrote batch of {Count} packets to outbox", packets.Count);
    }

    private async Task WritePacketAsync(IOutbox outbox, SnapshotMessage packet, CancellationToken ct)
    {
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

    private static SnapshotMessage MapRequestToSnapshot(PacketRequest request)
    {
        return new SnapshotMessage
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
    }
}
