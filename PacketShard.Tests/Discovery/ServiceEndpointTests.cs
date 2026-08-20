using PacketShard.ServiceDiscovery;
using Xunit;

namespace PacketShard.Tests.Discovery;

[Trait("Category", "Unit")]
public sealed class ServiceEndpointTests
{
    [Theory]
    [InlineData("http://srv_ingest-1:8080", "srv_ingest-1", 8080, "http")]
    [InlineData("https://gateway:5001", "gateway", 5001, "https")]
    [InlineData("tcp://masternode:8000", "masternode", 8000, "tcp")]
    [InlineData("  http://spaced:80  ", "spaced", 80, "http")]
    public void A_full_uri_keeps_its_scheme(string value, string host, int port, string scheme)
    {
        Assert.True(ServiceEndpoint.TryParse(value, out var endpoint));

        Assert.Equal(new ServiceEndpoint(host, port, scheme), endpoint);
    }

    [Fact]
    public void A_bare_host_and_port_is_assumed_to_be_tcp()
    {
        Assert.True(ServiceEndpoint.TryParse("masternode:8000", out var endpoint));

        Assert.Equal(new ServiceEndpoint("masternode", 8000, "tcp"), endpoint);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("masternode")]          // no port
    [InlineData("masternode:0")]        // port 0 is not dialable
    [InlineData("masternode:nonsense")]
    public void Anything_without_a_usable_port_is_rejected(string? value)
    {
        Assert.False(ServiceEndpoint.TryParse(value, out _));
    }

    [Fact]
    public void ToAddress_round_trips_through_TryParse()
    {
        var endpoint = new ServiceEndpoint("srv-read", 8080, "http");

        Assert.True(ServiceEndpoint.TryParse(endpoint.ToAddress(), out var parsed));
        Assert.Equal(endpoint, parsed);
    }
}
