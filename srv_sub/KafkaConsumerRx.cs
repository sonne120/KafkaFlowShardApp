using System.Reactive.Linq;
using Confluent.Kafka;
using KafkaFlowShardApp.Shared;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaFlowShardApp.Sub;

public sealed class KafkaConsumerRx : BackgroundService
{
    private readonly int _maxConsumeBatchSize = 100;
    private readonly string _topic;
    private readonly ISerializer _serializer;
    private readonly TcpForwarder _forwarder;
    private readonly ILogger<KafkaConsumerRx> _logger;
    private readonly IConsumer<string, string> _consumer;

    public KafkaConsumerRx(
        IConfiguration configuration,
        ILogger<KafkaConsumerRx> logger,
        ISerializer serializer,
        TcpForwarder forwarder)
    {
        _topic = configuration["Topic"] ?? "SnapshotTopic";
        _serializer = serializer;
        _forwarder = forwarder;
        _logger = logger;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = configuration["KafkaServer"],
            GroupId = configuration["ConsumerGroup"] ?? "ConsumerGroup",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _forwarder.EnsureConnectedAsync(stoppingToken);

        StartKafkaObservable(stoppingToken).Subscribe(
            onNext: _ => { },
            onError: ex => _logger.LogError(ex, "Kafka stream error"),
            onCompleted: () => _logger.LogInformation("Kafka stream completed"));

        await Task.Delay(Timeout.Infinite, stoppingToken).ContinueWith(_ => { });
    }

    private IObservable<ConsumeResult<string, string>> StartKafkaObservable(CancellationToken stoppingToken)
    {
        return Observable.Create<ConsumeResult<string, string>>(observer =>
        {
            _consumer.Subscribe(_topic);

            _ = Task.Run(async () =>
            {
                try
                {
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        try
                        {
                            var batch = _consumer.ConsumeBatch(TimeSpan.FromSeconds(5), _maxConsumeBatchSize, stoppingToken);
                            if (batch.Count == 0)
                                continue;

                            foreach (var result in batch)
                            {
                                var processed = await ProcessMessage(result, stoppingToken);

                                if (processed)
                                {
                                    _consumer.Commit(result);
                                    observer.OnNext(result);
                                }
                            }
                        }
                        catch (ConsumeException e) when (!e.Error.IsFatal)
                        {
                            _logger.LogError(e, "Non-fatal consume error");
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                        catch (Exception e)
                        {
                            _logger.LogError(e, "Error consuming message");
                            observer.OnError(e);
                            break;
                        }
                    }

                    observer.OnCompleted();
                }
                finally
                {
                    _consumer.Close();
                }
            }, stoppingToken);

            return () => _consumer.Close();
        });
    }

    private async Task<bool> ProcessMessage(ConsumeResult<string, string> result, CancellationToken stoppingToken)
    {
        try
        {
            var envelope = _serializer.Deserialize<PacketMessage>(result.Message.Value);

            if (string.IsNullOrWhiteSpace(envelope.Payload))
            {
                _logger.LogWarning("Received message with empty Payload. Skipping (will commit).");
                return true;
            }

            _logger.LogInformation("Forwarding {Proto} packet to MasterNode", envelope.PayloadType);
            return await _forwarder.SendAsync(envelope.Payload, stoppingToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing message");
            return false;
        }
    }

    public override void Dispose()
    {
        _forwarder.Dispose();
        _consumer.Dispose();
        base.Dispose();
    }
}
