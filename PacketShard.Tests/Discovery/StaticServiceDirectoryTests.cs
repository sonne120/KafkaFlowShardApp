using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using PacketShard.ServiceDiscovery;
using Xunit;

namespace PacketShard.Tests.Discovery;

[Trait("Category", "Unit")]
public sealed class StaticServiceDirectoryTests
{
    [Fact]
    public async Task A_fallback_entry_expands_into_one_endpoint_per_address()
    {
        var directory = Directory(("srv-ingest", "http://srv_ingest-1:8080,http://srv_ingest-2:8080,http://srv_ingest-3:8080"));

        var lookup = await directory.ResolveAsync("srv-ingest", default);

        Assert.Equal(
            new[] { "http://srv_ingest-1:8080", "http://srv_ingest-2:8080", "http://srv_ingest-3:8080" },
            lookup.Endpoints.Select(e => e.ToAddress()));
    }

    [Fact]
    public async Task Service_names_are_matched_without_regard_to_case()
    {
        var directory = Directory(("srv-read", "http://srv_read:8080"));

        Assert.Single((await directory.ResolveAsync("SRV-READ", default)).Endpoints);
    }

    [Fact]
    public async Task An_unmapped_service_resolves_to_nothing_rather_than_throwing()
    {
        var directory = Directory(("srv-read", "http://srv_read:8080"));

        Assert.Empty((await directory.ResolveAsync("srv-ingest", default)).Endpoints);
    }

    [Fact]
    public async Task Malformed_addresses_are_skipped_and_the_rest_survive()
    {
        var directory = Directory(("srv-ingest", "http://good:8080, nonsense , http://also-good:8081"));

        var lookup = await directory.ResolveAsync("srv-ingest", default);

        Assert.Equal(new[] { "http://good:8080", "http://also-good:8081" }, lookup.Endpoints.Select(e => e.ToAddress()));
    }

    [Fact]
    public async Task A_fixed_list_never_reports_a_change()
    {
        var directory = Directory(("srv-read", "http://srv_read:8080"));

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        var watch = directory.WaitForChangeAsync("srv-read", 0, cts.Token);

        // The only way out is the caller giving up — a static list has nothing to report.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => watch);
    }

    private static IServiceDirectory Directory(params (string Service, string Addresses)[] services)
    {
        var options = new ConsulOptions();
        foreach (var (service, addresses) in services)
            options.Fallback[service] = addresses;

        return new StaticServiceDirectory(Options.Create(options), NullLogger<StaticServiceDirectory>.Instance);
    }
}
