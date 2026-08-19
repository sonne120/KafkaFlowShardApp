namespace PacketShard.Read;

public enum ProjectionOutcome
{
    Skipped,

    FastPathSkip,

    Projected,

    Duplicate
}

///   1. redis fast-path  — read-only; a known duplicate never costs a Postgres round-trip
///   2. Postgres COMMIT  — the durable, authoritative write (dedup + version guard, one tx)
///   3. redis mark       — set ONLY after the commit
///   4. kafka ack        — the caller commits the offset, last

public sealed class ProjectionHandler
{
    private readonly ReadModelStore _store;
    private readonly RedisFastPath _redis;
    private readonly ILogger<ProjectionHandler> _logger;

    public ProjectionHandler(ReadModelStore store, RedisFastPath redis, ILogger<ProjectionHandler> logger)
    {
        _store = store;
        _redis = redis;
        _logger = logger;
    }

    public async Task<ProjectionOutcome> HandleAsync(string? kafkaValue, CancellationToken ct)
    {
        var record = PacketRecord.TryParse(kafkaValue);
        if (record is null)
            return ProjectionOutcome.Skipped;

        // 1. Fast-path: skip the Postgres round-trip for known duplicates.
        if (await _redis.IsProcessedAsync(record.TransactionId))
        {
            _logger.LogDebug("Fast-path skip (seen) tx={Tx}", record.TransactionId);
            return ProjectionOutcome.FastPathSkip;
        }

        // 2. Authoritative durable write (dedup + version guard in one transaction).
        var inserted = await _store.ProjectAsync(record, ct);

        // 3. Mark in Redis ONLY after the commit succeeded.
        await _redis.MarkProcessedAsync(record.TransactionId);

        if (inserted)
        {
            _logger.LogInformation("Projected {Proto} tx={Tx} client={Client}",
                record.Proto, record.TransactionId, record.ClientId);
            return ProjectionOutcome.Projected;
        }

        _logger.LogDebug("Postgres dedup absorbed tx={Tx}", record.TransactionId);
        return ProjectionOutcome.Duplicate;
    }
}
