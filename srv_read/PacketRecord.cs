using System.Globalization;
using Newtonsoft.Json.Linq;

namespace PacketShard.Read;

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

        // Newtonsoft's reader converts an ISO-8601 string into a Date token before we ever see
        // it, so what arrives here is a DateTime, not the original text. Reading it back through
        // Value<string>() renders it with the current culture and *without* an offset
        // ("01/01/2026 12:00:00"), and DateTimeOffset.TryParse then re-applies the host's local
        // offset — silently shifting every timestamp by the server's UTC offset for that date.
        if (token is JValue { Value: DateTimeOffset offset })
            return offset;

        if (token is JValue { Value: DateTime dateTime })
            return dateTime.Kind == DateTimeKind.Unspecified
                ? new DateTimeOffset(dateTime, TimeSpan.Zero)   // no offset supplied: Debezium emits UTC
                : new DateTimeOffset(dateTime.ToUniversalTime(), TimeSpan.Zero);

        // Still a string, so Newtonsoft did not recognise it as a date. Parse culture-independently
        // and treat a missing offset as UTC rather than as the server's local time.
        return DateTimeOffset.TryParse(
            token.Value<string>(),
            CultureInfo.InvariantCulture,
            DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal,
            out var parsed)
            ? parsed
            : null;
    }
}
