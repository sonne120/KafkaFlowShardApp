using System.Collections.Immutable;

namespace PacketShard.Kafka;

public interface IKafkaMessagePub
{
    Task SendAsync(ImmutableArray<Message> messages, CancellationToken cancellationToken);
}
