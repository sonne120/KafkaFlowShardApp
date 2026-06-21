using Akka.Actor;
using KafkaFlowShardApp.Shared;

namespace KafkaFlowShardApp.Master;

public sealed class ShardRouterActor : ReceiveActor
{
    private readonly Dictionary<ProtocolType, IActorRef> _writers = new();

    public ShardRouterActor(ShardMap shardMap)
    {
        foreach (var (protocol, connectionString) in shardMap.ConnectionStrings)
        {
            _writers[protocol] = Context.ActorOf(
                ShardWriterActor.Props(protocol, connectionString, shardMap.DatabaseName, shardMap.CollectionName),
                $"shard-{protocol}".ToLowerInvariant());
        }

        Receive<WriteToShard>(write => _writers[write.Protocol].Forward(write));
    }

    public static Props Props(ShardMap shardMap) =>
        Akka.Actor.Props.Create(() => new ShardRouterActor(shardMap));
}
