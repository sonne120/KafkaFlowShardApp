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
    private readonly int _maxAttempts;
    private readonly ISerializer _serializer;
    private readonly TcpForwarder _forwarder;
    private readonly DeadLetterProducer _deadLetter;
    private readonly ILogger<KafkaConsumerRx> _logger;
    private readonly IConsumer<string, string> _consumer;

    public KafkaConsumerRx(
        IConfiguration configuration,
        ILogger<KafkaConsumerRx> logger,
        ISerializer serializer,
        TcpForwarder forwarder,
        DeadLetterProducer deadLetter)
    {
        _topic = configuration["Topic"] ?? "SnapshotTopic";
        _maxAttempts = int.TryParse(configuration["MaxAttempts"], out var m) ? m : 3;
        _serializer = serializer;
        _forwarder = forwarder;
        _deadLetter = deadLetter;
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
        await _deadLetter.EnsureTopicsAsync();
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
                                var verdict = await ProcessMessage(result, stoppingToken);

                                if (verdict == true)
                                {
                                    _consumer.Commit(result);
                                    observer.OnNext(result);
                                }
                                else if (verdict == null)
                                {
                                    // rewind to this offset and retry after a short pause.
                                    _logger.LogWarning("MasterNode unreachable; retrying offset {Offset}", result.TopicPartitionOffset);
                                    _consumer.Seek(result.TopicPartitionOffset);
                                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                                    break;
                                }
                                else
                                {
                                    // Rejected by MasterNode: count the attempt, retry or dead-letter.
                                    await RouteFailureAsync(result);
                                    _consumer.Commit(result);
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

    // true  = handled (commit); false = rejected/poison (count attempt -> retry or dead-letter);
    // null  = MasterNode unreachable (don't commit, retry after short delay)
    private async Task<bool?> ProcessMessage(ConsumeResult<string, string> result, CancellationToken stoppingToken)
    {
        PacketMessage envelope;
        try
        {
            envelope = _serializer.Deserialize<PacketMessage>(result.Message.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Malformed message; routing to dead-letter");
            return false;
        }

        if (string.IsNullOrWhiteSpace(envelope.Payload))
        {
            _logger.LogWarning("Received message with empty Payload. Skipping (will commit).");
            return true;
        }

        _logger.LogInformation("Forwarding {Proto} packet to MasterNode", envelope.PayloadType);
        return await _forwarder.SendAsync(envelope.Payload, stoppingToken);
    }


    private async Task RouteFailureAsync(ConsumeResult<string, string> result)
    {
        var attempts = DeadLetterProducer.GetAttempts(result) + 1;
        var key = result.Message.Key;
        var value = result.Message.Value;

        if (attempts >= _maxAttempts)
        {
            await _deadLetter.ProduceAsync(_deadLetter.DeadLetterTopic, key, value, attempts, "max attempts exceeded");
            _logger.LogWarning("Dead-lettered after {Attempts} attempt(s) -> {Topic}", attempts, _deadLetter.DeadLetterTopic);
        }
        else
        {
            await _deadLetter.ProduceAsync(_topic, key, value, attempts);
            _logger.LogWarning("Rejected; retry {Attempts}/{Max} re-queued -> {Topic}", attempts, _maxAttempts, _topic);
        }
    }

    public override void Dispose()
    {
        _forwarder.Dispose();
        _deadLetter.Dispose();
        _consumer.Dispose();
        base.Dispose();
    }
}
