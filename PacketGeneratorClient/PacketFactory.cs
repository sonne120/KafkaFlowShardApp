using PacketShard.Ingest.Grpc;

namespace PacketShard.PacketGeneratorClient;

public sealed class PacketFactory
{
    private static readonly string[] Protocols = { "HTTPS", "TCP", "UDP", "ARP", "ICMP", "DNS" };
    private readonly Random _random = new();

    public PacketRequest Next()
    {
        var proto = Protocols[_random.Next(Protocols.Length)];
        return new PacketRequest
        {
            SourcePort = _random.Next(1024, 65535),
            DestPort = DestPortFor(proto),
            SourceIp = RandomIp(),
            DestIp = RandomIp(),
            SourceMac = RandomMac(),
            DestMac = RandomMac(),
            Proto = proto
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
