namespace KafkaFlowShardApp.Shared;

public sealed class SnapshotMessage
{
    // CDC / read-model keys (carried end-to-end so they land in the Mongo doc).
    // transaction_id: cross-system business key -> read-model dedup (UNIQUE in Postgres).
    // client_id:      shard/partition identity (= source_ip) -> Kafka partition key for ordering.
    // version:        monotonic stamp -> version-guard for any last-value read view.
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
