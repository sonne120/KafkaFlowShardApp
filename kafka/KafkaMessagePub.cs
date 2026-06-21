using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Collections.Immutable;
using System.Text;

namespace KafkaFlowShardApp.Kafka;

public class KafkaMessagePub : IKafkaMessagePub
{
    private readonly ISerializer _serializer;
    private readonly ILogger<KafkaMessagePub> _logger;
    private readonly IProducer<string, string> _producer;
    private readonly string _defaultKey = Guid.NewGuid().ToString();
    private readonly ITopicRepository _topicRepository;
    private readonly IConfiguration _configuration;
    private readonly ProducerConfig _producerConfig;

    public KafkaMessagePub(IConfiguration configuration,
                           ISerializer serializer,
                           ILogger<KafkaMessagePub> logger,
                           ITopicRepository topicRepository)
    {
        _configuration = configuration;
        _topicRepository = topicRepository;
        _serializer = serializer;
        _logger = logger;
        _producerConfig = new ProducerConfig
        {
            BootstrapServers = _configuration["KafkaServer"],
            LingerMs = 200,
            BatchSize = 10 * 1024,
            MessageTimeoutMs = 10000,
            EnableDeliveryReports = true,
            MessageSendMaxRetries = 3,
            RetryBackoffMs = 1000,
        };

        _producer = new ProducerBuilder<string, string>(_producerConfig).Build();
        BeginProduction();
    }

    private async Task BeginProduction()
    {
        await _topicRepository.TryCreateTopic(_configuration["Topic"]);
        await _topicRepository.TryCreateTopic(_configuration["RetryTopic"]);
        await _topicRepository.TryCreateTopic(_configuration["DeadletterTopic"]);
    }

    public Task SendAsync(ImmutableArray<Message> messages, CancellationToken cancellationToken)
    {
        foreach (var message in messages)
        {
            try
            {
                var kafkaPayload = _serializer.Serialize(message);

                _producer.Produce(
                    message.Topic,
                    new Message<string, string> { Key = message.Key ?? _defaultKey, Value = kafkaPayload, Headers = PrepareHeaders(message.Metadata, message.Created, message.PayloadType) }, report =>
                    {
                        if (report.Status != PersistenceStatus.Persisted)
                            _logger.LogError("Failed kafka message producing with Key {Key}, Error: {error}", report.Message.Key, report.Error.Code);
                    });

                _logger.LogInformation("Message sent to Kafka, Id: {Id}, Topic: {Topic}", message.Key, message.Topic);
            }
            catch (ProduceException<Null, string> ex)
            {
                throw new Exception($"Failed to send message to Kafka, Id: {message.Key}, Topic: {message.Topic}", ex);
            }
        }

        _producer.Flush(cancellationToken);
        return Task.CompletedTask;
    }

    private Headers PrepareHeaders(Dictionary<string, string>? metadata, DateTimeOffset messageTimestamp, string messageType)
    {
        var headers = new Headers
        {
            { "timestamp", BitConverter.GetBytes(messageTimestamp.ToUnixTimeMilliseconds()) }, { "type", Encoding.UTF8.GetBytes(messageType) }
        };

        if (metadata is null)
            return headers;

        foreach (var (key, value) in metadata)
            headers.Add(key, Encoding.UTF8.GetBytes(value));

        return headers;
    }
}
