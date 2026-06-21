using Akka.Actor;
using KafkaFlowShardApp.Shared;

namespace KafkaFlowShardApp.Master;

public sealed class AuthActor : ReceiveActor
{
    public sealed record Authenticate(string ApiKeyHash);

    private readonly HashSet<string> _validHashes;

    public AuthActor(IEnumerable<string> validApiKeys)
    {
        _validHashes = validApiKeys.Select(ApiKeyHasher.Hash).ToHashSet();

        Receive<Authenticate>(auth => Sender.Tell(_validHashes.Contains(auth.ApiKeyHash)));
    }

    public static Props Props(IEnumerable<string> validApiKeys) =>
        Akka.Actor.Props.Create(() => new AuthActor(validApiKeys));
}
