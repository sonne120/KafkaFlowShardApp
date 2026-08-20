using Microsoft.Extensions.Primitives;
using PacketShard.ServiceDiscovery;
using Yarp.ReverseProxy.Configuration;
using Yarp.ReverseProxy.ServiceDiscovery;

namespace PacketShard.LoadBalancer;

public sealed class ServiceDestinationResolver : IDestinationResolver
{
    public const string UriScheme = "discover";

    private readonly IServiceDirectory _directory;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly ILogger<ServiceDestinationResolver> _logger;

    public ServiceDestinationResolver(
        IServiceDirectory directory,
        IHostApplicationLifetime lifetime,
        ILogger<ServiceDestinationResolver> logger)
    {
        _directory = directory;
        _lifetime = lifetime;
        _logger = logger;
    }

    public async ValueTask<ResolvedDestinationCollection> ResolveDestinationsAsync(
        IReadOnlyDictionary<string, DestinationConfig> destinations,
        CancellationToken cancellationToken)
    {
        var resolved = new Dictionary<string, DestinationConfig>(StringComparer.OrdinalIgnoreCase);
        List<(string Service, ulong Index)>? watches = null;

        foreach (var (key, destination) in destinations)
        {
            if (!TryReadService(destination.Address, out var serviceName, out var port))
            {
                resolved[key] = destination;
                continue;
            }

            var lookup = await _directory.ResolveAsync(serviceName, port, cancellationToken);

            for (var i = 0; i < lookup.Endpoints.Count; i++)
                resolved[$"{key}[{i}]"] = destination with { Address = lookup.Endpoints[i].ToAddress() };

            if (lookup.Endpoints.Count == 0)
                _logger.LogWarning("No live instance of {Service}; {Destination} resolves to nothing", serviceName, key);
            else
                _logger.LogInformation("{Destination} -> {Count} instance(s) of {Service}: {Addresses}",
                    key, lookup.Endpoints.Count, serviceName, string.Join(", ", lookup.Endpoints.Select(e => e.ToAddress())));

            (watches ??= []).Add((serviceName, lookup.Index));
        }

        if (watches is null)
            return new ResolvedDestinationCollection(resolved, changeToken: null);

        var changed = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.ApplicationStopping);
        _ = WatchAsync(watches, changed);

        return new ResolvedDestinationCollection(resolved, new CancellationChangeToken(changed.Token));
    }

    private async Task WatchAsync(IReadOnlyList<(string Service, ulong Index)> watches, CancellationTokenSource changed)
    {
        var pending = watches.Select(watch => WatchOneAsync(watch.Service, watch.Index, changed.Token)).ToArray();

        try
        {
            await Task.WhenAny(pending);

            if (!changed.IsCancellationRequested)
                await changed.CancelAsync();

            await Task.WhenAll(pending);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Discovery watch supervision faulted; destinations hold until the next config reload");
        }
        finally
        {
            changed.Dispose();
        }
    }

    private async Task WatchOneAsync(string serviceName, ulong index, CancellationToken cancellationToken)
    {
        try
        {
            await _directory.WaitForChangeAsync(serviceName, index, cancellationToken);

            if (!cancellationToken.IsCancellationRequested)
                _logger.LogDebug("Re-resolving destinations for {Service}", serviceName);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Watch on {Service} failed; re-resolving", serviceName);
        }
    }

    private static bool TryReadService(string? address, out string serviceName, out int? port)
    {
        serviceName = "";
        port = null;

        if (string.IsNullOrWhiteSpace(address) ||
            !Uri.TryCreate(address, UriKind.Absolute, out var uri) ||
            !string.Equals(uri.Scheme, UriScheme, StringComparison.OrdinalIgnoreCase) ||
            string.IsNullOrEmpty(uri.Host))
        {
            return false;
        }

        serviceName = uri.Host;
        port = uri.Port > 0 ? uri.Port : null;
        return true;
    }
}
