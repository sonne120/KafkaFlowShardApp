using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace PacketShard.Master;

public sealed class PacketDocument
{
    [BsonId]
    public ObjectId InternalId { get; set; }

    [BsonElement("packetId")]
    public string PacketId { get; set; } = Guid.NewGuid().ToString();

    // CDC / read-model keys — Debezium ships these to the read side.
    public string transaction_id { get; set; } = Guid.NewGuid().ToString();
    public string client_id { get; set; } = string.Empty;
    public long version { get; set; }

    public int source_port { get; set; }
    public int dest_port { get; set; }
    public string source_ip { get; set; } = string.Empty;
    public string dest_ip { get; set; } = string.Empty;
    public string source_mac { get; set; } = string.Empty;
    public string dest_mac { get; set; } = string.Empty;
    public string proto { get; set; } = string.Empty;

    [BsonElement("storedAt")]
    public DateTime StoredAt { get; set; } = DateTime.UtcNow;
}
