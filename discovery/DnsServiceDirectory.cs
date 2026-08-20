using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace PacketShard.ServiceDiscovery;

public sealed class DnsServiceDirectory : IServiceDirectory
{
    private readonly ILogger<DnsServiceDirectory> _logger;
    private readonly DnsOptions _options;

    public DnsServiceDirectory(IOptions<DiscoveryOptions> options, ILogger<DnsServiceDirectory> logger)
    {
        _options = options.Value.Dns;
        _logger = logger;
    }

    public async ValueTask<ServiceLookup> ResolveAsync(string serviceName, int? port, CancellationToken cancellationToken)
    {
        if (port is null or <= 0)
        {
            _logger.LogError("No port given for {Service}; a DNS lookup cannot supply one — write the destination as discover://{Service}:<port>",
                serviceName, serviceName);
            return ServiceLookup.Empty;
        }

        var host = string.IsNullOrWhiteSpace(_options.Suffix) ? serviceName : $"{serviceName}.{_options.Suffix}";

        IPAddress[] addresses;
        try
        {
            addresses = await Dns.GetHostAddressesAsync(host, cancellationToken);
        }
        catch (Exception ex) when (ex is SocketException or ArgumentException)
        {
            _logger.LogWarning(ex, "DNS lookup for {Host} failed; resolving it to nothing until the next refresh", host);
            return ServiceLookup.Empty;
        }

        var endpoints = addresses
            .Where(address => address.AddressFamily == AddressFamily.InterNetwork)
            .Select(address => address.ToString())
            .OrderBy(address => address, StringComparer.Ordinal)
            .Select(address => new ServiceEndpoint(address, port.Value, _options.Scheme))
            .ToList();

        _logger.LogDebug("DNS: {Host} resolved to {Count} address(es)", host, endpoints.Count);
        return new ServiceLookup(endpoints, 0);
    }

    public Task WaitForChangeAsync(string serviceName, ulong index, CancellationToken cancellationToken)
        => Task.Delay(_options.Refresh, cancellationToken);
}
