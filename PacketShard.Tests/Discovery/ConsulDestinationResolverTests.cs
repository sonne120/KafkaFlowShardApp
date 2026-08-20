using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using PacketShard.LoadBalancer;
using PacketShard.ServiceDiscovery;
using Xunit;
using Yarp.ReverseProxy.Configuration;

namespace PacketShard.Tests.Discovery;

[Trait("Category", "Unit")]
public sealed class ConsulDestinationResolverTests
{
    [Fact]
    public async Task A_plain_address_is_left_exactly_as_configured()
    {
        var resolver = Resolver(new FakeDirectory());

        var resolved = await resolver.ResolveDestinationsAsync(
            Destinations(("read-1", "http://srv_read:8080")), default);

        Assert.Equal(new[] { "read-1" }, resolved.Destinations.Keys);
        Assert.Equal("http://srv_read:8080", resolved.Destinations["read-1"].Address);
    }

    [Fact]
    public async Task Nothing_to_discover_means_nothing_to_watch()
    {
        var resolver = Resolver(new FakeDirectory());

        var resolved = await resolver.ResolveDestinationsAsync(
            Destinations(("read-1", "http://srv_read:8080")), default);

        // A change token here would have YARP re-resolving a config that cannot change.
        Assert.Null(resolved.ChangeToken);
    }

    [Fact]
    public async Task A_consul_address_becomes_one_destination_per_instance()
    {
        var directory = new FakeDirectory
        {
            ["srv-ingest"] = Lookup(index: 7, "http://ingest-a:8080", "http://ingest-b:8080")
        };

        var resolved = await Resolver(directory).ResolveDestinationsAsync(
            Destinations(("ingest", "consul://srv-ingest")), default);

        Assert.Equal(new[] { "ingest[0]", "ingest[1]" }, resolved.Destinations.Keys.Order());
        Assert.Equal("http://ingest-a:8080", resolved.Destinations["ingest[0]"].Address);
        Assert.Equal("http://ingest-b:8080", resolved.Destinations["ingest[1]"].Address);
    }

    [Fact]
    public async Task The_instance_scheme_from_the_catalog_is_honoured()
    {
        var directory = new FakeDirectory
        {
            ["srv-ingest"] = new ServiceLookup([new ServiceEndpoint("ingest-a", 8443, "https")], 1)
        };

        var resolved = await Resolver(directory).ResolveDestinationsAsync(
            Destinations(("ingest", "consul://srv-ingest")), default);

        Assert.Equal("https://ingest-a:8443", resolved.Destinations["ingest[0]"].Address);
    }

    [Fact]
    public async Task Everything_but_the_address_is_carried_onto_each_instance()
    {
        var directory = new FakeDirectory
        {
            ["srv-ingest"] = Lookup(index: 1, "http://ingest-a:8080", "http://ingest-b:8080")
        };

        var template = new DestinationConfig
        {
            Address = "consul://srv-ingest",
            Metadata = new Dictionary<string, string> { ["zone"] = "eu-west" }
        };

        var resolved = await Resolver(directory).ResolveDestinationsAsync(
            new Dictionary<string, DestinationConfig> { ["ingest"] = template }, default);

        Assert.All(resolved.Destinations.Values, destination =>
            Assert.Equal("eu-west", destination.Metadata!["zone"]));
    }

    [Fact]
    public async Task A_service_with_no_healthy_instance_resolves_to_no_destinations()
    {
        var directory = new FakeDirectory { ["srv-ingest"] = ServiceLookup.Empty };

        var resolved = await Resolver(directory).ResolveDestinationsAsync(
            Destinations(("ingest", "consul://srv-ingest")), default);

        Assert.Empty(resolved.Destinations);

        // Still watched: the cluster has to recover on its own when instances come back.
        Assert.NotNull(resolved.ChangeToken);
    }

    [Fact]
    public async Task Discovered_and_hardcoded_destinations_coexist_in_one_cluster()
    {
        var directory = new FakeDirectory { ["srv-ingest"] = Lookup(index: 1, "http://ingest-a:8080") };

        var resolved = await Resolver(directory).ResolveDestinationsAsync(
            Destinations(("ingest", "consul://srv-ingest"), ("pinned", "http://legacy-ingest:8080")), default);

        Assert.Equal(new[] { "ingest[0]", "pinned" }, resolved.Destinations.Keys.Order());
    }

    [Fact]
    public async Task The_change_token_trips_when_the_directory_reports_a_change()
    {
        var directory = new FakeDirectory { ["srv-ingest"] = Lookup(index: 3, "http://ingest-a:8080") };

        var resolved = await Resolver(directory).ResolveDestinationsAsync(
            Destinations(("ingest", "consul://srv-ingest")), default);

        Assert.NotNull(resolved.ChangeToken);
        Assert.False(resolved.ChangeToken!.HasChanged);

        // The watch is armed at the index the snapshot was read at, so YARP is only called back
        // for state it has not already seen.
        Assert.Equal(3UL, await directory.WatchedIndex.Task.WaitAsync(TimeSpan.FromSeconds(5)));

        directory.ReportChange();

        var fired = await SpinUntilAsync(() => resolved.ChangeToken.HasChanged);
        Assert.True(fired);
    }

    //helpers
    private static ConsulDestinationResolver Resolver(IServiceDirectory directory) =>
        new(directory, new TestLifetime(), NullLogger<ConsulDestinationResolver>.Instance);

    private static Dictionary<string, DestinationConfig> Destinations(params (string Key, string Address)[] destinations) =>
        destinations.ToDictionary(d => d.Key, d => new DestinationConfig { Address = d.Address });

    private static ServiceLookup Lookup(ulong index, params string[] addresses) =>
        new([.. addresses.Select(a => { ServiceEndpoint.TryParse(a, out var e); return e; })], index);

    private static async Task<bool> SpinUntilAsync(Func<bool> condition)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            if (condition())
                return true;
            await Task.Delay(50);
        }
        return false;
    }

    private sealed class FakeDirectory : IServiceDirectory
    {
        private readonly Dictionary<string, ServiceLookup> _lookups = new(StringComparer.OrdinalIgnoreCase);
        private readonly TaskCompletionSource _changed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<ulong> WatchedIndex { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ServiceLookup this[string serviceName]
        {
            set => _lookups[serviceName] = value;
        }

        public void ReportChange() => _changed.TrySetResult();

        public ValueTask<ServiceLookup> ResolveAsync(string serviceName, CancellationToken cancellationToken) =>
            ValueTask.FromResult(_lookups.TryGetValue(serviceName, out var lookup) ? lookup : ServiceLookup.Empty);

        public async Task WaitForChangeAsync(string serviceName, ulong index, CancellationToken cancellationToken)
        {
            WatchedIndex.TrySetResult(index);
            await _changed.Task.WaitAsync(cancellationToken);
        }
    }

    private sealed class TestLifetime : IHostApplicationLifetime
    {
        private readonly CancellationTokenSource _stopping = new();

        public CancellationToken ApplicationStarted => CancellationToken.None;
        public CancellationToken ApplicationStopping => _stopping.Token;
        public CancellationToken ApplicationStopped => CancellationToken.None;

        public void StopApplication() => _stopping.Cancel();
    }
}
