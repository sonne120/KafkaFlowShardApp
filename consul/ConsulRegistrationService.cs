using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using Consul;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace PacketShard.ServiceDiscovery;

public sealed class ConsulRegistrationService : IHostedService
{
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan DeregisterTimeout = TimeSpan.FromSeconds(5);

    private readonly IConsulClient _client;
    private readonly ConsulServiceOptions _options;
    private readonly ILogger<ConsulRegistrationService> _logger;
    private readonly CancellationTokenSource _stopping = new();

    private AgentServiceRegistration? _registration;
    private Task _registering = Task.CompletedTask;

    public ConsulRegistrationService(
        IConsulClient client,
        IOptions<ConsulOptions> options,
        ILogger<ConsulRegistrationService> logger)
    {
        _client = client;
        _options = options.Value.Service;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_options.Name) || _options.Port <= 0)
        {
            _logger.LogWarning("Consul:Service:Name and :Port are not both set — this process will not register");
            return Task.CompletedTask;
        }

        _registration = BuildRegistration();

        // Registering runs in the background, on the same reasoning as the outbox schema init:
        // Consul may still be electing a leader when we boot, and an unregistered service is a
        // temporary discovery gap, not a reason to hold up the process.
        _registering = RegisterWithRetryAsync(_registration, _stopping.Token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync();

        // Await the loop so a shutdown mid-retry cannot register *after* we deregister.
        try
        {
            await _registering;
        }
        catch (OperationCanceledException)
        {
            // expected
        }

        if (_registration is null)
            return;

        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(DeregisterTimeout);

            await _client.Agent.ServiceDeregister(_registration.ID, timeout.Token);
            _logger.LogInformation("Deregistered {ServiceId} from Consul", _registration.ID);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not deregister {ServiceId}; the agent will drop it after {Grace}",
                _registration.ID, _options.DeregisterCriticalAfter);
        }
    }

    private async Task RegisterWithRetryAsync(AgentServiceRegistration registration, CancellationToken cancellationToken)
    {
        for (var attempt = 1; !cancellationToken.IsCancellationRequested; attempt++)
        {
            try
            {
                await _client.Agent.ServiceRegister(registration, cancellationToken);
                _logger.LogInformation(
                    "Registered {ServiceId} with Consul as {ServiceName} at {Address}:{Port}",
                    registration.ID, registration.Name, registration.Address, registration.Port);
                return;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Consul registration attempt {Attempt} failed; retrying in {Delay}", attempt, RetryDelay);
                await Task.Delay(RetryDelay, cancellationToken);
            }
        }
    }

    /// <summary>
    /// Explicit configuration wins; otherwise this container's IP if asked for, and its hostname
    /// as the last resort.
    /// </summary>
    private string ResolveAdvertisedAddress()
    {
        if (!string.IsNullOrWhiteSpace(_options.Address))
            return _options.Address;

        if (!_options.UseIpAddress)
            return Dns.GetHostName();

        if (TryFindLocalIpAddress(out var ip))
            return ip;

        _logger.LogWarning(
            "Consul:Service:UseIpAddress is set but no non-loopback IPv4 address was found; advertising the hostname instead");
        return Dns.GetHostName();
    }

    private static bool TryFindLocalIpAddress(out string address)
    {
        // Inside a container the hostname maps to its address on the primary network — exactly the
        // one other containers reach it on, and the right answer when there is more than one.
        try
        {
            foreach (var candidate in Dns.GetHostAddresses(Dns.GetHostName()))
            {
                if (candidate.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(candidate))
                {
                    address = candidate.ToString();
                    return true;
                }
            }
        }
        catch (SocketException)
        {
            // The hostname does not resolve; read the interfaces directly instead.
        }

        foreach (var nic in NetworkInterface.GetAllNetworkInterfaces())
        {
            if (nic.OperationalStatus != OperationalStatus.Up || nic.NetworkInterfaceType == NetworkInterfaceType.Loopback)
                continue;

            foreach (var unicast in nic.GetIPProperties().UnicastAddresses)
            {
                if (unicast.Address.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(unicast.Address))
                {
                    address = unicast.Address.ToString();
                    return true;
                }
            }
        }

        address = "";
        return false;
    }

    private AgentServiceRegistration BuildRegistration()
    {
        var address = ResolveAdvertisedAddress();

        // The id is keyed on the hostname rather than on the advertised address, so it stays
        // stable and readable when that address is an IP the container may not get back.
        var id = string.IsNullOrWhiteSpace(_options.Id)
            ? $"{_options.Name}-{Dns.GetHostName()}-{_options.Port}"
            : _options.Id;

        var checkPort = _options.HealthPort ?? _options.Port;

        var check = _options.TcpCheck
            ? new AgentServiceCheck { TCP = $"{address}:{checkPort}" }
            : new AgentServiceCheck
            {
                HTTP = $"{_options.Scheme}://{address}:{checkPort}{_options.HealthPath ?? "/health"}",
                TLSSkipVerify = true
            };

        check.Name = $"{_options.Name} liveness";
        check.Interval = _options.CheckInterval;
        check.Timeout = _options.CheckTimeout;
        check.DeregisterCriticalServiceAfter = _options.DeregisterCriticalAfter;

        check.Status = HealthStatus.Critical;

        return new AgentServiceRegistration
        {
            ID = id,
            Name = _options.Name,
            Address = address,
            Port = _options.Port,
            Tags = _options.Tags,
            Meta = new Dictionary<string, string> { ["scheme"] = _options.Scheme },
            Check = check
        };
    }
}
