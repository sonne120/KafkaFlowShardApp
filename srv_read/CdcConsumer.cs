using Confluent.Kafka;

namespace PacketShard.Read;

public sealed class CdcConsumer : BackgroundService
{
    private readonly ProjectionHandler _handler;
    private readonly ILogger<CdcConsumer> _logger;
    private readonly IConsumer<string, string> _consumer;
    private readonly string _topicPattern;

    public CdcConsumer(
        IConfiguration config,
        ProjectionHandler handler,
        ILogger<CdcConsumer> logger)
    {
        _handler = handler;
        _logger = logger;

    
        _topicPattern = config["CdcTopicPattern"] ?? "^pcap\\..*packets$";

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = config["KafkaServer"] ?? "kafka:9093",
            GroupId = config["ConsumerGroup"] ?? "ReadModelGroup",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,

            TopicMetadataRefreshIntervalMs = 10_000
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);
    }

    private async Task ConsumeLoop(CancellationToken ct)
    {
        _consumer.Subscribe(_topicPattern);
        _logger.LogInformation("Read-model consumer subscribed to {Pattern}", _topicPattern);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, string> result;
                try
                {
                    result = _consumer.Consume(ct);
                }
                catch (ConsumeException e) when (!e.Error.IsFatal)
                {
                    _logger.LogWarning(e, "Non-fatal consume error");
                    continue;
                }

                if (result?.Message is null)
                    continue;

                try
                {
                    await _handler.HandleAsync(result.Message.Value, ct);
                    _consumer.Commit(result);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Projection failed; offset left uncommitted for retry");
                    await Task.Delay(TimeSpan.FromSeconds(2), ct);
                }
            }
        }
        finally
        {
            _consumer.Close();
        }
    }

    public override void Dispose()
    {
        _consumer.Dispose();
        base.Dispose();
    }
}
