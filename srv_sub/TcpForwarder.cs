using System.Net.Sockets;
using System.Text;
using KafkaFlowShardApp.Shared;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace KafkaFlowShardApp.Sub;

public sealed class TcpForwarder : IDisposable
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<TcpForwarder> _logger;
    private readonly string _host;
    private readonly int _port;
    private readonly string _apiKey;

    private TcpClient? _client;
    private NetworkStream? _stream;

    public TcpForwarder(IConfiguration configuration, ILogger<TcpForwarder> logger)
    {
        _configuration = configuration;
        _logger = logger;
        _host = configuration["MasterNode:Host"] ?? "localhost";
        _port = int.TryParse(configuration["MasterNode:Port"], out var p) ? p : 8000;
        _apiKey = configuration["apiKey"] ?? "valid_api_key_1";
    }

    public bool IsConnected => _client?.Connected == true && _stream is not null;

    public async Task<bool> EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        if (IsConnected)
            return true;

        try
        {
            Disconnect();
            _client = new TcpClient();
            await _client.ConnectAsync(_host, _port, cancellationToken);
            _stream = _client.GetStream();

            return await SendApiKeyAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to connect to MasterNode at {Host}:{Port}", _host, _port);
            Disconnect();
            return false;
        }
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

    public async Task<bool> SendAsync(string payload, CancellationToken cancellationToken)
    {
        if (!await EnsureConnectedAsync(cancellationToken))
            return false;

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
            return false;
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
