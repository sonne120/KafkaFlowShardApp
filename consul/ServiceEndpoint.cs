using System.Globalization;

namespace PacketShard.ServiceDiscovery;

public readonly record struct ServiceEndpoint(string Host, int Port, string Scheme)
{
    public string ToAddress() => $"{Scheme}://{Host}:{Port}";

    public static bool TryParse(string? value, out ServiceEndpoint endpoint)
    {
        endpoint = default;
        if (string.IsNullOrWhiteSpace(value))
            return false;

        value = value.Trim();

        if (Uri.TryCreate(value, UriKind.Absolute, out var uri) && !string.IsNullOrEmpty(uri.Host) && uri.Port > 0)
        {
            endpoint = new ServiceEndpoint(uri.Host, uri.Port, uri.Scheme);
            return true;
        }

        var separator = value.LastIndexOf(':');
        if (separator > 0 &&
            int.TryParse(value[(separator + 1)..], NumberStyles.Integer, CultureInfo.InvariantCulture, out var port) &&
            port > 0)
        {
            endpoint = new ServiceEndpoint(value[..separator], port, "tcp");
            return true;
        }

        return false;
    }
}
public sealed record ServiceLookup(IReadOnlyList<ServiceEndpoint> Endpoints, ulong Index)
{
    public static readonly ServiceLookup Empty = new([], 0);
}
