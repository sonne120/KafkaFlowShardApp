using Newtonsoft.Json.Linq;

namespace KafkaFlowShardApp.Read;

/// <summary>
/// A read-model row parsed out of a Debezium MongoDB change event.
///
/// The connector runs the <c>ExtractNewDocumentState</c> SMT, so the Kafka value is the
/// *flattened* document (the Mongo doc's fields at the top level) rather than the raw
/// <c>after</c> JSON string. We keep the whole document as <see cref="Payload"/> (landed in
/// Postgres as <c>jsonb</c>) and pull out the few fields the read model indexes on.
/// </summary>
public sealed record PacketRecord(
    string TransactionId,
    string ClientId,
    long Version,
    string Proto,
    string SourceIp,
    string DestIp,
    int SourcePort,
    int DestPort,
    DateTimeOffset? StoredAt,
    string Payload)
{
    /// <summary>
    /// Parse a flattened Debezium value. Returns null for tombstones / delete events / records
    /// that carry no business key (those are acked and skipped — nothing to project).
    /// </summary>
    public static PacketRecord? TryParse(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        JObject doc;
        try
        {
            doc = JObject.Parse(value);
        }
        catch
        {
            return null;
        }

        // Debezium adds "__op" via ExtractNewDocumentState; deletes carry no document body.
        var op = doc.Value<string>("__op");
        if (op == "d")
            return null;

        var transactionId = doc.Value<string>("transaction_id");
        if (string.IsNullOrWhiteSpace(transactionId))
            return null;

        return new PacketRecord(
            TransactionId: transactionId,
            ClientId: doc.Value<string>("client_id") ?? string.Empty,
            Version: doc.Value<long?>("version") ?? 0,
            Proto: doc.Value<string>("proto") ?? "OTHER",
            SourceIp: doc.Value<string>("source_ip") ?? string.Empty,
            DestIp: doc.Value<string>("dest_ip") ?? string.Empty,
            SourcePort: doc.Value<int?>("source_port") ?? 0,
            DestPort: doc.Value<int?>("dest_port") ?? 0,
            StoredAt: ParseStoredAt(doc),
            Payload: doc.ToString(Newtonsoft.Json.Formatting.None));
    }

    private static DateTimeOffset? ParseStoredAt(JObject doc)
    {
        // Debezium can emit dates as epoch millis (number) or ISO string depending on config.
        var token = doc["storedAt"];
        if (token is null || token.Type == JTokenType.Null)
            return null;

        if (token.Type == JTokenType.Integer)
            return DateTimeOffset.FromUnixTimeMilliseconds(token.Value<long>());

        return DateTimeOffset.TryParse(token.Value<string>(), out var parsed) ? parsed : null;
    }
}
