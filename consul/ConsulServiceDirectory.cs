using System.Collections.Concurrent;
using Consul;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace PacketShard.ServiceDiscovery;

public sealed class ConsulServiceDirectory : IServiceDirectory
{
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    private readonly IConsulClient _client;
    private readonly ILogger<ConsulServiceDirectory> _logger;
    private readonly TimeSpan _watchTimeout;

    private readonly ConcurrentDictionary<string, ServiceLookup> _lastKnownGood = new(StringComparer.OrdinalIgnoreCase);

    public ConsulServiceDirectory(
        IConsulClient client,
        IOptions<ConsulOptions> options,
        ILogger<ConsulServiceDirectory> logger)
    {
        _client = client;
        _logger = logger;
        _watchTimeout = options.Value.WatchTimeout;
    }

    public async ValueTask<ServiceLookup> ResolveAsync(string serviceName, CancellationToken cancellationToken)
    {
        try
        {
            var result = await _client.Health.Service(
                serviceName, tag: null, passingOnly: true, new QueryOptions(), cancellationToken);

            var lookup = new ServiceLookup(ToEndpoints(result.Response), result.LastIndex);
            _lastKnownGood[serviceName] = lookup;

            _logger.LogDebug("Consul: {Service} has {Count} healthy instance(s) at index {Index}",
                serviceName, lookup.Endpoints.Count, lookup.Index);

            return lookup;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            if (_lastKnownGood.TryGetValue(serviceName, out var cached))
            {
                _logger.LogWarning(ex, "Consul lookup for {Service} failed; keeping the last known {Count} instance(s)",
                    serviceName, cached.Endpoints.Count);
                return cached;
            }

            _logger.LogError(ex, "Consul lookup for {Service} failed and nothing is cached yet", serviceName);
            return ServiceLookup.Empty;
        }
    }

    public async Task WaitForChangeAsync(string serviceName, ulong index, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var options = new QueryOptions { WaitIndex = index, WaitTime = _watchTimeout };
                var result = await _client.Health.Service(
                    serviceName, tag: null, passingOnly: true, options, cancellationToken);

                if (result.LastIndex != index)
                    return;
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Consul watch on {Service} failed; re-resolving in {Delay}", serviceName, RetryDelay);
                await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false);
                return;
            }
        }
    }

    private static List<ServiceEndpoint> ToEndpoints(ServiceEntry[]? entries)
    {
        var endpoints = new List<ServiceEndpoint>(entries?.Length ?? 0);
        if (entries is null)
            return endpoints;

        foreach (var entry in entries)
        {
            var host = string.IsNullOrWhiteSpace(entry.Service.Address) ? entry.Node.Address : entry.Service.Address;
            if (string.IsNullOrWhiteSpace(host) || entry.Service.Port <= 0)
                continue;

            var scheme = entry.Service.Meta is not null && entry.Service.Meta.TryGetValue("scheme", out var s) && !string.IsNullOrWhiteSpace(s)
                ? s
                : "http";

            endpoints.Add(new ServiceEndpoint(host, entry.Service.Port, scheme));
        }

        endpoints.Sort(static (a, b) =>
        {
            var byHost = string.CompareOrdinal(a.Host, b.Host);
            return byHost != 0 ? byHost : a.Port.CompareTo(b.Port);
        });

        return endpoints;
    }
}
