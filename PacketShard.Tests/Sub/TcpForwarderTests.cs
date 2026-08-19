using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using PacketShard.Shared;
using PacketShard.Sub;
using Xunit;

namespace PacketShard.Tests.Sub;

[Trait("Category", "Unit")]
public sealed class TcpForwarderTests
{
    [Fact]
    public async Task Handshake_sends_the_hash_of_the_api_key_never_the_key_itself()
    {
        using var server = new FakeMasterNode("API Key authenticated. You can now send messages.");

        using var forwarder = Create(server, apiKey: "valid_api_key_1");
        Assert.True(await forwarder.EnsureConnectedAsync(default));

        var handshake = await server.NextLineAsync();
        Assert.Equal(ApiKeyHasher.Hash("valid_api_key_1"), handshake);
        Assert.DoesNotContain("valid_api_key_1", handshake);
    }

    [Fact]
    public async Task A_rejected_api_key_fails_the_connection()
    {
        using var server = new FakeMasterNode("Invalid API Key");

        using var forwarder = Create(server);

        Assert.False(await forwarder.EnsureConnectedAsync(default));
    }

    [Fact]
    public async Task An_accepted_packet_reports_true()
    {
        using var server = new FakeMasterNode("API Key authenticated.", "Ok");

        using var forwarder = Create(server);

        Assert.True(await forwarder.SendAsync("""{"proto":"UDP"}""", default));
        await server.NextLineAsync();                                   // the handshake
        Assert.Equal("""{"proto":"UDP"}""", await server.NextLineAsync());
    }

    [Fact]
    public async Task A_rejected_packet_reports_false_not_null()
    {
        using var server = new FakeMasterNode("API Key authenticated.", "Rejected: filtered");

        using var forwarder = Create(server);

        Assert.False(await forwarder.SendAsync("{}", default));
    }

    [Fact]
    public async Task An_unreachable_master_node_reports_null_not_false()
    {
        using var forwarder = new TcpForwarder(
            Config(host: "127.0.0.1", port: UnusedPort(), apiKey: "valid_api_key_1"),
            NullLogger<TcpForwarder>.Instance);

        Assert.Null(await forwarder.SendAsync("{}", default));
    }

    [Fact]
    public async Task IsConnected_is_false_before_the_handshake_and_after_a_disconnect()
    {
        using var server = new FakeMasterNode("API Key authenticated.");

        using var forwarder = Create(server);
        Assert.False(forwarder.IsConnected);

        await forwarder.EnsureConnectedAsync(default);
        Assert.True(forwarder.IsConnected);

        forwarder.Disconnect();
        Assert.False(forwarder.IsConnected);
    }

    //helpers
    private static TcpForwarder Create(FakeMasterNode server, string apiKey = "valid_api_key_1") =>
        new(Config("127.0.0.1", server.Port, apiKey), NullLogger<TcpForwarder>.Instance);

    private static IConfiguration Config(string host, int port, string apiKey) =>
        new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MasterNode:Host"] = host,
            ["MasterNode:Port"] = port.ToString(),
            ["apiKey"] = apiKey
        }).Build();

    private static int UnusedPort()
    {
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }

    private sealed class FakeMasterNode : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _cts = new();
        private readonly Channel<string> _received = Channel.CreateUnbounded<string>();

        public int Port { get; }

        public FakeMasterNode(params string[] replies)
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _ = Task.Run(() => PumpAsync(new Queue<string>(replies), _cts.Token));
        }

        private async Task PumpAsync(Queue<string> replies, CancellationToken ct)
        {
            try
            {
                using var client = await _listener.AcceptTcpClientAsync(ct);
                await using var stream = client.GetStream();
                using var reader = new StreamReader(stream, Encoding.UTF8);

                while (!ct.IsCancellationRequested)
                {
                    var line = await reader.ReadLineAsync(ct);
                    if (line is null)
                        break;

                    await _received.Writer.WriteAsync(line, ct);

                    if (replies.Count > 0)
                        await stream.WriteAsync(Encoding.UTF8.GetBytes(replies.Dequeue()), ct);
                }
            }
            catch (Exception ex)
            {
                _received.Writer.TryComplete(ex);
            }
        }

        public async Task<string> NextLineAsync()
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            return await _received.Reader.ReadAsync(timeout.Token);
        }

        public void Dispose()
        {
            _cts.Cancel();
            _listener.Stop();
            _cts.Dispose();
        }
    }
}
