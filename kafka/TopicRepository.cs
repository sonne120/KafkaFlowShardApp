using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Configuration;

namespace KafkaFlowShardApp.Kafka;

public class TopicRepository : ITopicRepository
{
    private readonly IConfiguration _configuration;

    public TopicRepository(IConfiguration configuration)
    {
        _configuration = configuration;
    }

    public async Task TryCreateTopic(string topicName)
    {
        if (string.IsNullOrWhiteSpace(topicName))
            return;

        var adminConfig = new AdminClientConfig
        {
            BootstrapServers = _configuration["KafkaServer"]
        };

        var partitions = int.TryParse(_configuration["TopicPartitions"], out var p) ? p : 5;

        using var adminClient = new AdminClientBuilder(adminConfig).Build();
        try
        {
            await adminClient.CreateTopicsAsync(new TopicSpecification[]
            {
                new TopicSpecification
                {
                    Name = topicName,
                    ReplicationFactor = 1,
                    NumPartitions = partitions
                }
            });
        }
        catch (CreateTopicsException e)
        {
            Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
        }
    }
}
