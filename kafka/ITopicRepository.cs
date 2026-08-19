namespace PacketShard.Kafka;

public interface ITopicRepository
{
    /// <summary>
    /// No-ops on a null or blank name: topic keys are optional configuration, and a service
    /// that does not use a retry or dead-letter topic simply leaves them unset.
    /// </summary>
    Task TryCreateTopic(string? topicName);
}
