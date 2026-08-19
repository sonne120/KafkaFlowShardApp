using Microsoft.Extensions.Configuration;
using PacketShard.Master;
using PacketShard.Shared;
using Xunit;

namespace PacketShard.Tests.MasterNode;

[Trait("Category", "Unit")]
public sealed class ShardMapTests
{
    [Fact]
    public void Every_protocol_has_a_shard()
    {
        var map = new ShardMap(Config());

        Assert.Equal(
            Enum.GetValues<ProtocolType>().ToHashSet(),
            map.ConnectionStrings.Keys.ToHashSet());
    }

    [Fact]
    public void Each_shard_gets_a_distinct_node()
    {
        // Five protocols, five MongoDB instances
        var map = new ShardMap(Config());

        Assert.Equal(5, map.ConnectionStrings.Values.Distinct().Count());
    }

    [Fact]
    public void Configuration_overrides_the_defaults()
    {
        var map = new ShardMap(Config(new Dictionary<string, string?>
        {
            ["Shards:Udp"] = "mongodb://udp-host:27020",
            ["Shards:Database"] = "custom_db",
            ["Shards:Collection"] = "custom_packets"
        }));

        Assert.Equal("mongodb://udp-host:27020", map.ConnectionStringFor(ProtocolType.Udp));
        Assert.Equal("custom_db", map.DatabaseName);
        Assert.Equal("custom_packets", map.CollectionName);
    }

    [Fact]
    public void Defaults_apply_when_nothing_is_configured()
    {
        var map = new ShardMap(Config());

        Assert.Equal("pcap", map.DatabaseName);
        Assert.Equal("packets", map.CollectionName);
        Assert.All(map.ConnectionStrings.Values, cs => Assert.StartsWith("mongodb://", cs));
    }

    [Fact]
    public void ConnectionStringFor_agrees_with_the_map()
    {
        var map = new ShardMap(Config());

        Assert.All(Enum.GetValues<ProtocolType>(),
            protocol => Assert.Equal(map.ConnectionStrings[protocol], map.ConnectionStringFor(protocol)));
    }

    private static IConfiguration Config(Dictionary<string, string?>? values = null) =>
        new ConfigurationBuilder().AddInMemoryCollection(values ?? new Dictionary<string, string?>()).Build();
}
