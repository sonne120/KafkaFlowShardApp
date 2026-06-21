using System.Net;
using Akka.Actor;
using Akka.IO;

namespace KafkaFlowShardApp.Master;

public sealed class MainServerActor : ReceiveActor
{
    public MainServerActor(IActorRef authActor, IActorRef shardRouter, int port)
    {
        Context.System.Tcp().Tell(new Tcp.Bind(Self, new IPEndPoint(IPAddress.Any, port)));

        Receive<Tcp.Bound>(bound => Console.WriteLine($"MasterNode listening on {bound.LocalAddress}"));

        Receive<Tcp.CommandFailed>(failed =>
        {
            Console.WriteLine($"TCP bind failed: {failed.Cmd}");
            Context.Stop(Self);
        });

        Receive<Tcp.Connected>(connected =>
        {
            Console.WriteLine($"Connection from {connected.RemoteAddress}");
            var handler = Context.ActorOf(ConnectionHandler.Props(Sender, authActor, shardRouter));
            Sender.Tell(new Tcp.Register(handler));
        });
    }

    public static Props Props(IActorRef authActor, IActorRef shardRouter, int port) =>
        Akka.Actor.Props.Create(() => new MainServerActor(authActor, shardRouter, port));
}
