using Grpc.Net.Client;
using PacketShard.Ingest.Grpc;

// Console packet generator — the cross-platform twin of the WPF client. Generates random
// packets and streams them through the LoadBalancer over gRPC.
//
// Usage:
//   dotnet run --project PacketGeneratorConsole -- [--url http://localhost:5001]
//                                                   [--count 50] [--ssl] [--loop] [--delay 1000]
//
//   --url    LoadBalancer address (default http://localhost:5001)
//   --count  packets per batch (default 50)
//   --ssl    use TLS (default OFF — plaintext h2c). SSL toggle, false position.
//   --loop   keep sending batches until Ctrl+C
//   --delay  ms between batches when looping (default 1000)

var url = GetOption(args, "--url") ?? "http://localhost:5001";
var count = int.TryParse(GetOption(args, "--count"), out var c) ? c : 50;
var delay = int.TryParse(GetOption(args, "--delay"), out var d) ? d : 1000;
var useSsl = HasFlag(args, "--ssl");
var loop = HasFlag(args, "--loop");

// SSL toggle (false position by default): with TLS off we speak plaintext HTTP/2 (h2c),
// which gRPC only permits once this switch is set.
if (!useSsl)
    AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

using var channel = GrpcChannel.ForAddress(url);
var client = new PacketIngest.PacketIngestClient(channel);
var factory = new PacketFactory();

Console.WriteLine($"Target {url}  (SSL={(useSsl ? "on" : "off")})  count={count}  loop={loop}");

var batch = 0;
do
{
    try
    {
        using var call = client.SendStream(cancellationToken: cts.Token);
        for (var i = 0; i < count; i++)
        {
            var packet = factory.Next();
            await call.RequestStream.WriteAsync(packet, cts.Token);
            Console.WriteLine($"→ {packet.Proto,-5} {packet.SourceIp}:{packet.SourcePort} -> {packet.DestIp}:{packet.DestPort}");
        }
        await call.RequestStream.CompleteAsync();

        var reply = await call;
        Console.WriteLine($"✓ batch {++batch}: server accepted {reply.Accepted} ({reply.Message})");
    }
    catch (OperationCanceledException)
    {
        break;
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"✗ {ex.Message}");
        if (!loop) return 1;
    }

    if (loop && !cts.IsCancellationRequested)
        try { await Task.Delay(delay, cts.Token); } catch (OperationCanceledException) { break; }
}
while (loop && !cts.IsCancellationRequested);

Console.WriteLine("Done.");
return 0;

static string? GetOption(string[] args, string name)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
}

static bool HasFlag(string[] args, string name) => Array.IndexOf(args, name) >= 0;

// Random packet builder — mirrors the server-side generator.
internal sealed class PacketFactory
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
