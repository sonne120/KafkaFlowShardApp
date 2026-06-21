using KafkaFlowShardApp.Shared;
using Microsoft.Extensions.Configuration;

namespace KafkaFlowShardApp.Master;

public sealed class ShardMap
{
    private readonly Dictionary<ProtocolType, string> _connectionStrings;

    public string DatabaseName { get; }
    public string CollectionName { get; }

    public ShardMap(IConfiguration configuration)
    {
        DatabaseName = configuration["Shards:Database"] ?? "pcap";
        CollectionName = configuration["Shards:Collection"] ?? "packets";

        _connectionStrings = new Dictionary<ProtocolType, string>
        {
            [ProtocolType.Https] = configuration["Shards:Https"] ?? "mongodb://localhost:27018",
            [ProtocolType.Tcp]   = configuration["Shards:Tcp"]   ?? "mongodb://localhost:27019",
            [ProtocolType.Udp]   = configuration["Shards:Udp"]   ?? "mongodb://localhost:27020",
            [ProtocolType.Arp]   = configuration["Shards:Arp"]   ?? "mongodb://localhost:27021",
            [ProtocolType.Other] = configuration["Shards:Other"] ?? "mongodb://localhost:27022",
        };
    }

    public IReadOnlyDictionary<ProtocolType, string> ConnectionStrings => _connectionStrings;

    public string ConnectionStringFor(ProtocolType protocol) => _connectionStrings[protocol];
}
