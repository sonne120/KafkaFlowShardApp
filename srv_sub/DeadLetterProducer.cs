using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace PacketShard.Sub;

public sealed class DeadLetterProducer : IDisposable
{
    public const string AttemptsHeader = "attempts";
    public const string ReasonHeader = "x-failure-reason";

    private readonly ILogger<DeadLetterProducer> _logger;
    private readonly IProducer<string, string> _producer;
    private readonly string _bootstrap;
    private readonly int _partitions;

    public string RetryTopic { get; }
    public string DeadLetterTopic { get; }

    public DeadLetterProducer(IConfiguration configuration, ILogger<DeadLetterProducer> logger)
    {
        _logger = logger;
        _bootstrap = configuration["KafkaServer"] ?? "localhost:9092";
        RetryTopic = configuration["RetryTopic"] ?? "5sdelay";
        DeadLetterTopic = configuration["DeadletterTopic"] ?? "deadletter";
        _partitions = int.TryParse(configuration["TopicPartitions"], out var p) ? p : 5;

        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = _bootstrap,
            EnableDeliveryReports = true,
            MessageTimeoutMs = 10000
        }).Build();
    }

    public async Task EnsureTopicsAsync()
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _bootstrap }).Build();
        foreach (var topic in new[] { RetryTopic, DeadLetterTopic })
        {
            try
            {
                await admin.CreateTopicsAsync(new[]
                {
                    new TopicSpecification { Name = topic, ReplicationFactor = 1, NumPartitions = _partitions }
                });
                _logger.LogInformation("Created topic {Topic}", topic);
            }
            catch (CreateTopicsException e)
            {
                _logger.LogInformation("Topic {Topic}: {Reason}", topic, e.Results[0].Error.Reason);
            }
        }
    }

    public async Task ProduceAsync(string topic, string? key, string value, int attempts, string? reason = null)
    {
        var headers = new Headers
        {
            { AttemptsHeader, Encoding.UTF8.GetBytes(attempts.ToString()) }
        };
        if (reason is not null)
            headers.Add(ReasonHeader, Encoding.UTF8.GetBytes(reason));

        await _producer.ProduceAsync(topic, new Message<string, string>
        {
            Key = key ?? Guid.NewGuid().ToString(),
            Value = value,
            Headers = headers
        });
    }

    /// <summary>Reads the current attempt count from the message headers (0 if absent).</summary>
    public static int GetAttempts(ConsumeResult<string, string> result)
    {
        if (result.Message.Headers is not null &&
            result.Message.Headers.TryGetLastBytes(AttemptsHeader, out var bytes) &&
            int.TryParse(Encoding.UTF8.GetString(bytes), out var n))
        {
            return n;
        }
        return 0;
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
    }
}
