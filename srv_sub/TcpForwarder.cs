using System.Net.Sockets;
using System.Text;
using PacketShard.ServiceDiscovery;
using PacketShard.Shared;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace PacketShard.Sub;

public sealed class TcpForwarder : IDisposable
{
    private readonly IConfiguration _configuration;
    private readonly IServiceDirectory _directory;
    private readonly ILogger<TcpForwarder> _logger;
    private readonly string _host;
    private readonly int _port;
    private readonly string? _serviceName;
    private readonly string _apiKey;

    private TcpClient? _client;
    private NetworkStream? _stream;
    private int _instance;

    public TcpForwarder(IConfiguration configuration, IServiceDirectory directory, ILogger<TcpForwarder> logger)
    {
        _configuration = configuration;
        _directory = directory;
        _logger = logger;
        _host = configuration["MasterNode:Host"] ?? "localhost";
        _port = int.TryParse(configuration["MasterNode:Port"], out var p) ? p : 8000;
        _apiKey = configuration["apiKey"] ?? "valid_api_key_1";

        // Set MasterNode:Service to look the shard router up by name instead of dialling one fixed
        // host; leave it unset and the configured host:port is used exactly as before.
        var serviceName = configuration["MasterNode:Service"];
        _serviceName = string.IsNullOrWhiteSpace(serviceName) ? null : serviceName;
    }

    public bool IsConnected => _client?.Connected == true && _stream is not null;

    public async Task<bool> EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        if (IsConnected)
            return true;

        var endpoint = await ResolveAsync(cancellationToken);
        if (endpoint is null)
            return false;

        var (host, port, _) = endpoint.Value;

        try
        {
            Disconnect();
            _client = new TcpClient();
            await _client.ConnectAsync(host, port, cancellationToken);
            _stream = _client.GetStream();

            return await SendApiKeyAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to connect to MasterNode at {Host}:{Port}", host, port);
            Disconnect();
            return false;
        }
    }

    /// <summary>
    /// Picks the instance to dial. Every reconnect advances to the next one, so the node that just
    /// dropped us is not the first one tried again — and an instance Consul has stopped reporting
    /// healthy is not tried at all.
    /// </summary>
    private async Task<ServiceEndpoint?> ResolveAsync(CancellationToken cancellationToken)
    {
        if (_serviceName is null)
            return new ServiceEndpoint(_host, _port, "tcp");

        var lookup = await _directory.ResolveAsync(_serviceName, cancellationToken);
        if (lookup.Endpoints.Count == 0)
        {
            _logger.LogWarning("No healthy instance of {Service}; the batch stays uncommitted and is retried", _serviceName);
            return null;
        }

        var index = (int)((uint)Interlocked.Increment(ref _instance) % (uint)lookup.Endpoints.Count);
        return lookup.Endpoints[index];
    }

    private async Task<bool> SendApiKeyAsync(CancellationToken cancellationToken)
    {
        var hash = ApiKeyHasher.Hash(_apiKey);
        var bytes = Encoding.UTF8.GetBytes(hash + "\n");
        await _stream!.WriteAsync(bytes, cancellationToken);

        var response = await ReadResponseAsync(cancellationToken);
        _logger.LogInformation("MasterNode handshake: {Response}", response);

        return !response.Contains("Invalid API Key");
    }

    /// <summary>
    /// true  = MasterNode replied "Ok" (saved);
    /// false = MasterNode replied but rejected the packet (counts as a failed attempt);
    /// null  = could not deliver / transient infra error (do NOT count as an attempt).
    /// </summary>
    public async Task<bool?> SendAsync(string payload, CancellationToken cancellationToken)
    {
        if (!await EnsureConnectedAsync(cancellationToken))
            return null;

        try
        {
            var bytes = Encoding.UTF8.GetBytes(payload + "\n");
            await _stream!.WriteAsync(bytes, cancellationToken);

            var response = await ReadResponseAsync(cancellationToken);
            _logger.LogInformation("MasterNode response: {Response}", response);
            return response.Contains("Ok");
        }
        catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
        {
            _logger.LogWarning(ex, "TCP send failed; dropping connection for reconnect on next message.");
            Disconnect();
            return null;
        }
    }

    private async Task<string> ReadResponseAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[1024];
        var read = await _stream!.ReadAsync(buffer, cancellationToken);
        return Encoding.UTF8.GetString(buffer, 0, read);
    }

    public void Disconnect()
    {
        _stream?.Dispose();
        _client?.Close();
        _stream = null;
        _client = null;
    }

    public void Dispose() => Disconnect();
}
