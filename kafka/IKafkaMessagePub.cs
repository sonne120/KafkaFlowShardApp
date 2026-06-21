using System.Collections.Immutable;

namespace KafkaFlowShardApp.Kafka;

public interface IKafkaMessagePub
{
    Task SendAsync(ImmutableArray<Message> messages, CancellationToken cancellationToken);
}
