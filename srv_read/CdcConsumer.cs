using Confluent.Kafka;

namespace PacketShard.Read;

/// <summary>
/// Consumes Debezium MongoDB change events (one topic per shard, matched by regex) and projects
/// them into the Postgres read model. Manual offset commit — we only ack once the durable write
/// has happened, so delivery is at-least-once and projection is idempotent.
///
/// Per-message flow (the crash-safe order we agreed on):
///   redis fast-path  →  Postgres commit (UNIQUE dedup + version guard)  →  redis mark  →  ack
/// </summary>
public sealed class CdcConsumer : BackgroundService
{
    private readonly ReadModelStore _store;
    private readonly RedisFastPath _redis;
    private readonly ILogger<CdcConsumer> _logger;
    private readonly IConsumer<string, string> _consumer;
    private readonly string _topicPattern;

    public CdcConsumer(
        IConfiguration config,
        ReadModelStore store,
        RedisFastPath redis,
        ILogger<CdcConsumer> logger)
    {
        _store = store;
        _redis = redis;
        _logger = logger;

        // Debezium topics look like "pcap.https.pcap.packets" — match all shards with a regex.
        // librdkafka treats a subscription starting with "^" as a regex.
        _topicPattern = config["CdcTopicPattern"] ?? "^pcap\\..*packets$";

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = config["KafkaServer"] ?? "kafka:9093",
            GroupId = config["ConsumerGroup"] ?? "ReadModelGroup",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            // Regex subscriptions only pick up newly-created topics on a metadata refresh.
            // The Debezium topics are created after this service starts, so refresh often
            // (default is 5 min) to discover them quickly on a cold start.
            TopicMetadataRefreshIntervalMs = 10_000
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Run the blocking consume loop off the host startup thread.
        return Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);
    }

    private async Task ConsumeLoop(CancellationToken ct)
    {
        _consumer.Subscribe(_topicPattern);
        _logger.LogInformation("Read-model consumer subscribed to {Pattern}", _topicPattern);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, string> result;
                try
                {
                    result = _consumer.Consume(ct);
                }
                catch (ConsumeException e) when (!e.Error.IsFatal)
                {
                    _logger.LogWarning(e, "Non-fatal consume error");
                    continue;
                }

                if (result?.Message is null)
                    continue;

                try
                {
                    await HandleAsync(result, ct);
                    _consumer.Commit(result);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    // Leave the offset uncommitted: the message is redelivered and reprocessed.
                    // ON CONFLICT DO NOTHING makes the retry a no-op, so this is safe.
                    _logger.LogError(ex, "Projection failed; offset left uncommitted for retry");
                    await Task.Delay(TimeSpan.FromSeconds(2), ct);
                }
            }
        }
        finally
        {
            _consumer.Close();
        }
    }

    private async Task HandleAsync(ConsumeResult<string, string> result, CancellationToken ct)
    {
        var record = PacketRecord.TryParse(result.Message.Value);
        if (record is null)
            return; // tombstone / delete / no business key — ack and move on.

        // 1. Fast-path: skip the Postgres round-trip for known duplicates.
        if (await _redis.IsProcessedAsync(record.TransactionId))
        {
            _logger.LogDebug("Fast-path skip (seen) tx={Tx}", record.TransactionId);
            return;
        }

        // 2. Authoritative durable write (dedup + version guard in one transaction).
        var inserted = await _store.ProjectAsync(record, ct);

        // 3. Mark in Redis ONLY after the commit succeeded.
        await _redis.MarkProcessedAsync(record.TransactionId);

        if (inserted)
            _logger.LogInformation("Projected {Proto} tx={Tx} client={Client}",
                record.Proto, record.TransactionId, record.ClientId);
        else
            _logger.LogDebug("Postgres dedup absorbed tx={Tx}", record.TransactionId);
    }

    public override void Dispose()
    {
        _consumer.Dispose();
        base.Dispose();
    }
}
