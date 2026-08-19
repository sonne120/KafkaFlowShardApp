using Confluent.Kafka;
using PacketShard.Sub;
using Xunit;

namespace PacketShard.Tests.Sub;

[Trait("Category", "Unit")]
public sealed class ConsumerExtensionsTests
{
    [Fact]
    public void An_empty_topic_yields_an_empty_batch()
    {
        var consumer = new ScriptedConsumer();

        Assert.Empty(consumer.ConsumeBatch(TimeSpan.FromMilliseconds(10), 10, default));
    }

    [Fact]
    public void A_single_buffered_message_is_returned_on_its_own()
    {
        var consumer = new ScriptedConsumer(Result("a"), null);

        var batch = consumer.ConsumeBatch(TimeSpan.FromMilliseconds(10), 10, default);

        Assert.Equal(new[] { "a" }, batch.Select(r => r.Message.Value));
    }

    [Fact]
    public void Everything_already_buffered_is_drained_into_one_batch()
    {
        var consumer = new ScriptedConsumer(Result("a"), Result("b"), Result("c"), null);

        var batch = consumer.ConsumeBatch(TimeSpan.FromMilliseconds(10), 10, default);

        Assert.Equal(new[] { "a", "b", "c" }, batch.Select(r => r.Message.Value));
    }

    [Fact]
    public void The_batch_stops_at_maxBatchSize_and_leaves_the_rest_buffered()
    {
        var consumer = new ScriptedConsumer(Result("a"), Result("b"), Result("c"), Result("d"));

        var batch = consumer.ConsumeBatch(TimeSpan.FromMilliseconds(10), maxBatchSize: 2, default);

        Assert.Equal(new[] { "a", "b" }, batch.Select(r => r.Message.Value));
        Assert.Equal(2, consumer.Consumed);   // it stopped asking 
    } 

    [Fact]
    public void A_result_with_no_message_ends_the_batch()
    {
        var consumer = new ScriptedConsumer(Result("a"), Eof(), Result("c"));

        var batch = consumer.ConsumeBatch(TimeSpan.FromMilliseconds(10), 10, default);

        Assert.Equal(new[] { "a" }, batch.Select(r => r.Message.Value));
    }

    [Fact]
    public void A_leading_eof_yields_an_empty_batch()
    {
        Assert.Empty(new ScriptedConsumer(Eof()).ConsumeBatch(TimeSpan.FromMilliseconds(10), 10, default));
    }

    [Fact]
    public void Cancellation_stops_the_drain_after_the_first_message()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var consumer = new ScriptedConsumer(Result("a"), Result("b"), Result("c"));

        var batch = consumer.ConsumeBatch(TimeSpan.FromMilliseconds(10), 10, cts.Token);

        Assert.Equal(new[] { "a" }, batch.Select(r => r.Message.Value));
    }

    // helpers
    private static ConsumeResult<string, string> Result(string value) => new()
    {
        Message = new Message<string, string> { Key = "k", Value = value },
        TopicPartitionOffset = new TopicPartitionOffset("SnapshotTopic", 0, Offset.Beginning)
    };

    private static ConsumeResult<string, string> Eof() => new() { IsPartitionEOF = true };

    private sealed class ScriptedConsumer(params ConsumeResult<string, string>?[] script)
        : IConsumer<string, string>
    {
        private int _index;

        public int Consumed => _index;

        public ConsumeResult<string, string> Consume(TimeSpan timeout) =>
            _index < script.Length ? script[_index++]! : null!;

        public ConsumeResult<string, string> Consume(int millisecondsTimeout) => Consume(TimeSpan.Zero);
        public ConsumeResult<string, string> Consume(CancellationToken cancellationToken = default) => Consume(TimeSpan.Zero);

        private static T Unused<T>() => throw new NotSupportedException("ConsumeBatch does not use this member.");

        public Handle Handle => Unused<Handle>();
        public string Name => Unused<string>();
        public string MemberId => Unused<string>();
        public List<TopicPartition> Assignment => Unused<List<TopicPartition>>();
        public List<string> Subscription => Unused<List<string>>();
        public IConsumerGroupMetadata ConsumerGroupMetadata => Unused<IConsumerGroupMetadata>();

        public int AddBrokers(string brokers) => Unused<int>();
        public void SetSaslCredentials(string username, string password) => Unused<bool>();
        public void Subscribe(IEnumerable<string> topics) => Unused<bool>();
        public void Subscribe(string topic) => Unused<bool>();
        public void Unsubscribe() => Unused<bool>();
        public void Assign(TopicPartition partition) => Unused<bool>();
        public void Assign(TopicPartitionOffset partition) => Unused<bool>();
        public void Assign(IEnumerable<TopicPartitionOffset> partitions) => Unused<bool>();
        public void Assign(IEnumerable<TopicPartition> partitions) => Unused<bool>();
        public void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions) => Unused<bool>();
        public void IncrementalAssign(IEnumerable<TopicPartition> partitions) => Unused<bool>();
        public void IncrementalUnassign(IEnumerable<TopicPartition> partitions) => Unused<bool>();
        public void Unassign() => Unused<bool>();
        public void StoreOffset(ConsumeResult<string, string> result) => Unused<bool>();
        public void StoreOffset(TopicPartitionOffset offset) => Unused<bool>();
        public List<TopicPartitionOffset> Commit() => Unused<List<TopicPartitionOffset>>();
        public void Commit(IEnumerable<TopicPartitionOffset> offsets) => Unused<bool>();
        public void Commit(ConsumeResult<string, string> result) => Unused<bool>();
        public void Seek(TopicPartitionOffset tpo) => Unused<bool>();
        public void Pause(IEnumerable<TopicPartition> partitions) => Unused<bool>();
        public void Resume(IEnumerable<TopicPartition> partitions) => Unused<bool>();
        public List<TopicPartitionOffset> Committed(TimeSpan timeout) => Unused<List<TopicPartitionOffset>>();
        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout) => Unused<List<TopicPartitionOffset>>();
        public Offset Position(TopicPartition partition) => Unused<Offset>();
        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestamps, TimeSpan timeout) => Unused<List<TopicPartitionOffset>>();
        public WatermarkOffsets GetWatermarkOffsets(TopicPartition tp) => Unused<WatermarkOffsets>();
        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition tp, TimeSpan timeout) => Unused<WatermarkOffsets>();
        public void Close() => Unused<bool>();
        public void Dispose() { }
    }
}
