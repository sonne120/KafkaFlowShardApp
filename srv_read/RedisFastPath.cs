using StackExchange.Redis;

namespace PacketShard.Read;

/// <summary>
/// Redis is a *fast-path filter*, never the source of truth. It shaves the obvious duplicates
/// before they cost a Postgres round-trip, but Postgres' UNIQUE constraint is what actually
/// guarantees correctness. The ordering we settled on:
///
///     1. <see cref="IsProcessedAsync"/>  — read-only check; if seen, ack &amp; skip
///     2. Postgres COMMIT                  — the durable, authoritative write
///     3. <see cref="MarkProcessedAsync"/> — set the marker ONLY after the commit
///     4. Kafka commit (ack)
///
/// Crash between (2) and (3)/(4): on retry Redis still says "not seen", we re-INSERT, and
/// ON CONFLICT DO NOTHING absorbs it. No silent loss. The marker carries a TTL only to cap
/// memory — if it expires, the long tail is still caught by Postgres, never by Redis alone.
/// </summary>
public sealed class RedisFastPath
{
    private const string KeyPrefix = "rm:tx:";
    private readonly IDatabase _db;
    private readonly TimeSpan _ttl;

    public RedisFastPath(IConnectionMultiplexer mux, TimeSpan ttl)
    {
        _db = mux.GetDatabase();
        _ttl = ttl;
    }

    public Task<bool> IsProcessedAsync(string transactionId) =>
        _db.KeyExistsAsync(KeyPrefix + transactionId);

    public Task MarkProcessedAsync(string transactionId) =>
        _db.StringSetAsync(KeyPrefix + transactionId, "1", _ttl);
}
