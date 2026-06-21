namespace KafkaFlowShardApp.Kafka;

public interface ITopicRepository
{
    Task TryCreateTopic(string topicName);
}
