using Akka.Actor;
using PacketShard.Shared;

namespace PacketShard.Master;

public sealed class ShardRouterActor : ReceiveActor
{
    private readonly Dictionary<ProtocolType, IActorRef> _writers = new();

    public ShardRouterActor(IReadOnlyDictionary<ProtocolType, Props> shardProps)
    {
        foreach (var (protocol, props) in shardProps)
        {
            _writers[protocol] = Context.ActorOf(props, $"shard-{protocol}".ToLowerInvariant());
        }

        Receive<WriteToShard>(write => _writers[write.Protocol].Forward(write));
    }

    public static Props Props(ShardMap shardMap) => Props(ShardPropsFor(shardMap));

    public static Props Props(IReadOnlyDictionary<ProtocolType, Props> shardProps) =>
        Akka.Actor.Props.Create(() => new ShardRouterActor(shardProps));

    private static IReadOnlyDictionary<ProtocolType, Props> ShardPropsFor(ShardMap shardMap) =>
        shardMap.ConnectionStrings.ToDictionary(
            shard => shard.Key,
            shard => ShardWriterActor.Props(
                shard.Key, shard.Value, shardMap.DatabaseName, shardMap.CollectionName));
}
