namespace PacketShard.ServiceDiscovery;

public enum DiscoveryProvider
{
    // Static: the fixed addresses in DiscoveryOptions.Fallback. No infrastructure.
    // Consul: the agent's health endpoint, watched with blocking queries. Used by docker-compose.
    // Dns:    plain DNS A records, re-read on a timer. Used on AWS, where Cloud Map is the registry.
    Static,
    Consul,
    Dns
}

public sealed class DiscoveryOptions
{
    public const string SectionName = "Discovery";

    public DiscoveryProvider Provider { get; set; } = DiscoveryProvider.Static;

    public Dictionary<string, string> Fallback { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    public ConsulOptions Consul { get; set; } = new();

    public DnsOptions Dns { get; set; } = new();

    public ServiceRegistrationOptions Service { get; set; } = new();
}

public sealed class ConsulOptions
{
    public string Address { get; set; } = "http://consul:8500";

    public string? Token { get; set; }

    public TimeSpan WatchTimeout { get; set; } = TimeSpan.FromMinutes(2);
}

public sealed class DnsOptions
{
    public string Suffix { get; set; } = "";

    public TimeSpan Refresh { get; set; } = TimeSpan.FromSeconds(10);

    public string Scheme { get; set; } = "http";
}
public sealed class ServiceRegistrationOptions
{
    public bool Enabled { get; set; } = true;

    public string Name { get; set; } = "";
    public string? Id { get; set; }

    public string? Address { get; set; }
    public bool UseIpAddress { get; set; }

    public int Port { get; set; }

    public string Scheme { get; set; } = "http";

    public string[] Tags { get; set; } = [];

    public string? HealthPath { get; set; }

    public int? HealthPort { get; set; }

    public bool TcpCheck { get; set; }

    public TimeSpan CheckInterval { get; set; } = TimeSpan.FromSeconds(10);

    public TimeSpan CheckTimeout { get; set; } = TimeSpan.FromSeconds(3);

    public TimeSpan DeregisterCriticalAfter { get; set; } = TimeSpan.FromMinutes(1);
}
