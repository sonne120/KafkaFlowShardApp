namespace PacketShard.Outbox;

public interface IOutboxInitializer
{
    Task InitializeAsync(CancellationToken cancellationToken);
}
