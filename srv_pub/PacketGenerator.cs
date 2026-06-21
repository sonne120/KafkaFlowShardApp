using KafkaFlowShardApp.Shared;

namespace KafkaFlowShardApp.Pub;

public sealed class PacketGenerator
{
    private static readonly string[] Protocols =
    {
        "HTTPS", "TCP", "UDP", "ARP", "ICMP", "DNS"
    };

    private readonly Random _random = new();

    public SnapshotMessage Next()
    {
        var proto = Protocols[_random.Next(Protocols.Length)];

        return new SnapshotMessage
        {
            source_port = _random.Next(1024, 65535),
            dest_port = DestPortFor(proto),
            source_ip = RandomIp(),
            dest_ip = RandomIp(),
            source_mac = RandomMac(),
            dest_mac = RandomMac(),
            proto = proto
        };
    }

    private int DestPortFor(string proto) => proto switch
    {
        "HTTPS" => 443,
        "DNS" => 53,
        "ARP" => 0,
        _ => _random.Next(1, 1024)
    };

    private string RandomIp() =>
        $"{_random.Next(1, 255)}.{_random.Next(0, 255)}.{_random.Next(0, 255)}.{_random.Next(1, 255)}";

    private string RandomMac() =>
        string.Join(":", Enumerable.Range(0, 6).Select(_ => _random.Next(0, 256).ToString("x2")));
}
