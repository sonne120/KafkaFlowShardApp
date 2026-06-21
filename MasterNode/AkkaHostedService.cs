using Akka.Actor;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaFlowShardApp.Master;

public sealed class AkkaHostedService : IHostedService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<AkkaHostedService> _logger;
    private ActorSystem? _system;

    public AkkaHostedService(IConfiguration configuration, ILogger<AkkaHostedService> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var port = int.TryParse(_configuration["Tcp:Port"], out var p) ? p : 8000;
        var validApiKeys = _configuration.GetSection("ApiKeys").Get<string[]>()
                           ?? new[] { "valid_api_key_1", "valid_api_key_2" };

        var shardMap = new ShardMap(_configuration);

        _system = ActorSystem.Create("masternode");
        var authActor = _system.ActorOf(AuthActor.Props(validApiKeys), "auth");
        var shardRouter = _system.ActorOf(ShardRouterActor.Props(shardMap), "shard-router");
        _system.ActorOf(MainServerActor.Props(authActor, shardRouter, port), "server");

        _logger.LogInformation("MasterNode starting on TCP port {Port} with 5 shard nodes", port);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_system is not null)
            await _system.Terminate();
    }
}
