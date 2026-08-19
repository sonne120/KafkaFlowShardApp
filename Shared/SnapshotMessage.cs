namespace PacketShard.Shared;

public sealed class SnapshotMessage
{
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
}
