using System.Text;
using Akka.Actor;
using Akka.IO;
using KafkaFlowShardApp.Shared;
using Newtonsoft.Json;

namespace KafkaFlowShardApp.Master;

public sealed class ConnectionHandler : ReceiveActor
{
    private readonly IActorRef _connection;
    private readonly IActorRef _authActor;
    private readonly IActorRef _shardRouter;

    private sealed record AuthResult(bool IsAuthenticated);
    private sealed record WriteResult(string Result);

    public ConnectionHandler(IActorRef connection, IActorRef authActor, IActorRef shardRouter)
    {
        _connection = connection;
        _authActor = authActor;
        _shardRouter = shardRouter;

        Unauthenticated();
    }

    private void Unauthenticated()
    {
        Receive<Tcp.Received>(received =>
        {
            var apiKeyHash = Encoding.UTF8.GetString(received.Data.ToArray()).Trim();
            _authActor.Ask<bool>(new AuthActor.Authenticate(apiKeyHash))
                      .PipeTo(Self, success: ok => new AuthResult(ok));
        });

        Receive<AuthResult>(result =>
        {
            if (!result.IsAuthenticated)
            {
                Send("Invalid API Key");
                _connection.Tell(Tcp.Close.Instance);
                return;
            }

            Send("API Key authenticated. You can now send messages.");
            Become(Authenticated);
        });

        Receive<Tcp.ConnectionClosed>(_ => Context.Stop(Self));
    }

    private void Authenticated()
    {
        Receive<Tcp.Received>(received =>
        {
            var text = Encoding.UTF8.GetString(received.Data.ToArray());

            foreach (var line in text.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            {
                var protocol = DetermineProtocol(line);
                _shardRouter.Ask<string>(new WriteToShard(protocol, line))
                            .PipeTo(Self, success: r => new WriteResult(r));
            }
        });

        Receive<WriteResult>(result => Send(result.Result));

        Receive<Tcp.ConnectionClosed>(_ => Context.Stop(Self));
    }

    private static ProtocolType DetermineProtocol(string json)
    {
        try
        {
            var snapshot = JsonConvert.DeserializeObject<SnapshotMessage>(json);
            return snapshot?.proto.ToProtocolType() ?? ProtocolType.Other;
        }
        catch
        {
            return ProtocolType.Other;
        }
    }

    private void Send(string message) =>
        _connection.Tell(Tcp.Write.Create(ByteString.FromBytes(Encoding.UTF8.GetBytes(message))));

    public static Props Props(IActorRef connection, IActorRef authActor, IActorRef shardRouter) =>
        Akka.Actor.Props.Create(() => new ConnectionHandler(connection, authActor, shardRouter));
}
