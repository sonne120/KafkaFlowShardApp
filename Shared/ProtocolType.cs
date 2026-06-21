namespace KafkaFlowShardApp.Shared;

public enum ProtocolType
{
    Https,
    Tcp,
    Udp,
    Arp,
    Other
}

public static class ProtocolTypeExtensions
{
    public static ProtocolType ToProtocolType(this string? proto) => proto?.Trim().ToUpperInvariant() switch
    {
        "HTTPS" or "TLS" or "SSL" => ProtocolType.Https,
        "TCP" => ProtocolType.Tcp,
        "UDP" => ProtocolType.Udp,
        "ARP" => ProtocolType.Arp,
        _ => ProtocolType.Other
    };
}
