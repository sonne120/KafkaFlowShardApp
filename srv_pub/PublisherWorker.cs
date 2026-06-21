using KafkaFlowShardApp.Outbox;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaFlowShardApp.Pub;

public sealed class PublisherWorker : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly PacketGenerator _generator;
    private readonly ILogger<PublisherWorker> _logger;
    private readonly string _topic;
    private readonly int _batchSize;
    private readonly TimeSpan _interval;

    public PublisherWorker(
        IServiceScopeFactory scopeFactory,
        PacketGenerator generator,
        IConfiguration configuration,
        ILogger<PublisherWorker> logger)
    {
        _scopeFactory = scopeFactory;
        _generator = generator;
        _logger = logger;
        _topic = configuration["Topic"] ?? "SnapshotTopic";
        _batchSize = int.TryParse(configuration["Publisher:BatchSize"], out var b) ? b : 5;
        _interval = TimeSpan.FromMilliseconds(
            int.TryParse(configuration["Publisher:IntervalMs"], out var i) ? i : 2000);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await InitializeOutboxAsync(stoppingToken);
        _logger.LogInformation("srv_pub started: {Batch} packet(s) every {Interval}", _batchSize, _interval);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = _scopeFactory.CreateScope();
                var outbox = scope.ServiceProvider.GetRequiredService<IOutbox>();

                for (var n = 0; n < _batchSize; n++)
                {
                    var packet = _generator.Next();
                    await outbox.AddAsync(
                        data: packet,
                        topic: _topic,
                        partitionBy: p => p.dest_ip,
                        isSequential: false,
                        metadata: new Dictionary<string, string> { { "proto", packet.proto } },
                        cancellationToken: stoppingToken);

                    _logger.LogInformation("Wrote {Proto} {SrcIp}:{SrcPort} -> {DstIp}:{DstPort} to outbox",
                        packet.proto, packet.source_ip, packet.source_port, packet.dest_ip, packet.dest_port);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed writing batch to outbox");
            }

            try
            {
                await Task.Delay(_interval, stoppingToken);
            }
            catch (TaskCanceledException)
            {
                break;
            }
        }
    }

    private async Task InitializeOutboxAsync(CancellationToken stoppingToken)
    {
        for (var attempt = 1; attempt <= 20 && !stoppingToken.IsCancellationRequested; attempt++)
        {
            try
            {
                using var scope = _scopeFactory.CreateScope();
                var initializer = scope.ServiceProvider.GetRequiredService<IOutboxInitializer>();
                await initializer.InitializeAsync(stoppingToken);
                _logger.LogInformation("Outbox schema initialized");
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Outbox init attempt {Attempt} failed; retrying in 3s (waiting for MySQL)", attempt);
                await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
            }
        }
    }
}
