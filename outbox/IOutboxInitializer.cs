namespace KafkaFlowShardApp.Outbox;

public interface IOutboxInitializer
{
    Task InitializeAsync(CancellationToken cancellationToken);
}
