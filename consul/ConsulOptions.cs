namespace PacketShard.ServiceDiscovery;

public sealed class ConsulOptions
{
    public const string SectionName = "Consul";

    public bool Enabled { get; set; }
    public string Address { get; set; } = "http://consul:8500";

    public string? Token { get; set; }

    public TimeSpan WatchTimeout { get; set; } = TimeSpan.FromMinutes(2);
    public Dictionary<string, string> Fallback { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    public ConsulServiceOptions Service { get; set; } = new();
}

public sealed class ConsulServiceOptions
{
    public bool Enabled { get; set; } = true;

    public string Name { get; set; } = "";

    public string? Id { get; set; }

    /// <summary>
    /// Where other services should reach this instance. Explicit wins over everything; leave it
    /// unset and <see cref="UseIpAddress"/> decides between this container's IP and its hostname.
    /// </summary>
    public string? Address { get; set; }

    /// <summary>
    /// Advertise the container's own IP rather than its hostname.
    ///
    /// This is what makes Consul's DNS useful: it can only answer with an A record for an address
    /// it holds as an IP. Registered under a hostname, a lookup of srv-ingest.service.consul comes
    /// back as a CNAME to that hostname — resolvable only by Docker's embedded DNS, and a dead end
    /// for anything else. The IP is re-read on every start, so a container that comes back on a
    /// different address re-registers with the new one.
    /// </summary>
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
