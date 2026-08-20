using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using PacketShard.ServiceDiscovery;
using Xunit;

namespace PacketShard.Tests.Discovery;

[Trait("Category", "Unit")]
public sealed class DnsServiceDirectoryTests
{
    [Fact]
    public async Task Records_are_expanded_into_one_endpoint_per_address()
    {
        var lookup = await Directory().ResolveAsync("localhost", 8080, default);

        Assert.NotEmpty(lookup.Endpoints);
        Assert.All(lookup.Endpoints, endpoint =>
        {
            Assert.Equal(8080, endpoint.Port);
            Assert.Equal("http", endpoint.Scheme);
        });
        Assert.Contains(lookup.Endpoints, endpoint => endpoint.Host == "127.0.0.1");
    }

    [Fact]
    public async Task Without_a_port_there_is_nothing_to_dial()
    {
        Assert.Empty((await Directory().ResolveAsync("localhost", null, default)).Endpoints);
        Assert.Empty((await Directory().ResolveAsync("localhost", 0, default)).Endpoints);
    }

    [Fact]
    public async Task A_name_that_does_not_resolve_yields_nothing_rather_than_throwing()
    {
        var lookup = await Directory().ResolveAsync("no-such-service", 8080, default);

        Assert.Empty(lookup.Endpoints);
    }

    [Fact]
    public async Task The_suffix_is_appended_to_form_the_record_looked_up()
    {
        var scoped = Directory(suffix: "packetshard.local");

        Assert.Empty((await scoped.ResolveAsync("localhost", 8080, default)).Endpoints);
    }

    [Fact]
    public async Task A_watch_is_a_refresh_interval_because_DNS_cannot_push()
    {
        var directory = Directory(refresh: TimeSpan.FromMilliseconds(150));

        var started = DateTime.UtcNow;
        await directory.WaitForChangeAsync("localhost", 0, default);

        Assert.True(DateTime.UtcNow - started >= TimeSpan.FromMilliseconds(100));
    }

    private static IServiceDirectory Directory(string suffix = "", TimeSpan? refresh = null) =>
        new DnsServiceDirectory(
            Options.Create(new DiscoveryOptions
            {
                Provider = DiscoveryProvider.Dns,
                Dns = new DnsOptions
                {
                    Suffix = suffix,
                    Refresh = refresh ?? TimeSpan.FromSeconds(10),
                    Scheme = "http"
                }
            }),
            NullLogger<DnsServiceDirectory>.Instance);
}
