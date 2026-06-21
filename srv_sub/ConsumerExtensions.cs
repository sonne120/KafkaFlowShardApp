using Confluent.Kafka;

namespace KafkaFlowShardApp.Sub;

public static class ConsumerExtensions
{
    public static IReadOnlyList<ConsumeResult<TKey, TValue>> ConsumeBatch<TKey, TValue>(
        this IConsumer<TKey, TValue> consumer,
        TimeSpan consumeTimeout,
        int maxBatchSize,
        CancellationToken stoppingToken)
    {
        var first = consumer.Consume(consumeTimeout);
        if (first?.Message is null)
            return Array.Empty<ConsumeResult<TKey, TValue>>();

        var batch = new List<ConsumeResult<TKey, TValue>> { first };

        while (batch.Count < maxBatchSize && !stoppingToken.IsCancellationRequested)
        {
            var next = consumer.Consume(TimeSpan.Zero);
            if (next?.Message is null)
                break;

            batch.Add(next);
        }

        return batch;
    }
}
