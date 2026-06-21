using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace KafkaFlowShardApp.Master;

public sealed class PacketDocument
{
    [BsonId]
    public ObjectId InternalId { get; set; }

    [BsonElement("packetId")]
    public string PacketId { get; set; } = Guid.NewGuid().ToString();

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
