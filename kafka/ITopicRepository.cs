namespace PacketShard.Kafka;

public interface ITopicRepository
{
    Task TryCreateTopic(string topicName);
}
