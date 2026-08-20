using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace PacketShard.ServiceDiscovery;

public sealed class StaticServiceDirectory : IServiceDirectory
{
    private readonly IReadOnlyDictionary<string, ServiceLookup> _services;
    private readonly ILogger<StaticServiceDirectory> _logger;

    public StaticServiceDirectory(IOptions<ConsulOptions> options, ILogger<StaticServiceDirectory> logger)
    {
        _logger = logger;
        _services = options.Value.Fallback.ToDictionary(
            entry => entry.Key,
            entry => new ServiceLookup(Parse(entry.Value), 0),
            StringComparer.OrdinalIgnoreCase);
    }

    public ValueTask<ServiceLookup> ResolveAsync(string serviceName, CancellationToken cancellationToken)
    {
        if (_services.TryGetValue(serviceName, out var lookup))
            return ValueTask.FromResult(lookup);

        _logger.LogWarning("No Consul:Fallback entry for {Service}; resolving it to nothing", serviceName);
        return ValueTask.FromResult(ServiceLookup.Empty);
    }

    public Task WaitForChangeAsync(string serviceName, ulong index, CancellationToken cancellationToken)
        => Task.Delay(Timeout.Infinite, cancellationToken);

    private static List<ServiceEndpoint> Parse(string addresses)
    {
        var endpoints = new List<ServiceEndpoint>();
        foreach (var address in addresses.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (ServiceEndpoint.TryParse(address, out var endpoint))
                endpoints.Add(endpoint);
        }
        return endpoints;
    }
}
