using PacketShard.Shared;
using Xunit;

namespace PacketShard.Tests.Shared;

[Trait("Category", "Unit")]
public sealed class ProtocolTypeTests
{
    [Theory]
    [InlineData("HTTPS", ProtocolType.Https)]
    [InlineData("TLS", ProtocolType.Https)]     
    [InlineData("SSL", ProtocolType.Https)]    
    [InlineData("TCP", ProtocolType.Tcp)]
    [InlineData("UDP", ProtocolType.Udp)]
    [InlineData("ARP", ProtocolType.Arp)]
    public void Known_protocols_map_to_their_shard(string proto, ProtocolType expected)
    {
        Assert.Equal(expected, proto.ToProtocolType());
    }

    [Theory]
    [InlineData("https")]
    [InlineData("Https")]
    [InlineData("  udp  ")]
    [InlineData("\tTCP\n")]
    public void Casing_and_surrounding_whitespace_are_ignored(string proto)
    {
        Assert.NotEqual(ProtocolType.Other, proto.ToProtocolType());
    }

    [Theory]
    [InlineData("ICMP")]
    [InlineData("DNS")]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void Anything_unrecognised_falls_through_to_the_other_shard(string? proto)
    {
        Assert.Equal(ProtocolType.Other, proto.ToProtocolType());
    }

    [Fact]
    public void Every_protocol_value_is_reachable_from_some_string()
    {
        var reachable = new[] { "HTTPS", "TCP", "UDP", "ARP", "anything-else" }
            .Select(p => p.ToProtocolType())
            .ToHashSet();

        Assert.Equal(Enum.GetValues<ProtocolType>().ToHashSet(), reachable);
    }
}
