using System.Globalization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace PacketShard.Read;

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
    public static PacketRecord? TryParse(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        JObject doc;
        try
        {
            doc = ParseDocument(value);
        }
        catch
        {
            return null;
        }

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

    private static JObject ParseDocument(string value)
    {
        using var reader = new JsonTextReader(new StringReader(value))
        {
            DateParseHandling = DateParseHandling.None,
        };

        var doc = JObject.Load(reader);

        if (reader.Read())
            throw new JsonReaderException("Additional text found after the JSON document.");

        return doc;
    }

    private static DateTimeOffset? ParseStoredAt(JObject doc)
    {
        var token = doc["storedAt"];
        if (token is null || token.Type == JTokenType.Null)
            return null;

        if (token.Type == JTokenType.Integer)
            return DateTimeOffset.FromUnixTimeMilliseconds(token.Value<long>());

        return DateTimeOffset.TryParse(
            token.Value<string>(),
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out var parsed)
            ? parsed
            : null;
    }
}
