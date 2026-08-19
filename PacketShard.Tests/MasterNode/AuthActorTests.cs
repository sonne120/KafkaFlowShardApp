using Akka.TestKit.Xunit2;
using PacketShard.Master;
using PacketShard.Shared;
using Xunit;

namespace PacketShard.Tests.MasterNode;

[Trait("Category", "Unit")]
public sealed class AuthActorTests : TestKit
{
    private const string ValidKey = "valid_api_key_1";

    [Fact]
    public void Known_key_hash_is_accepted()
    {
        var auth = Sys.ActorOf(AuthActor.Props(new[] { ValidKey, "valid_api_key_2" }));

        auth.Tell(new AuthActor.Authenticate(ApiKeyHasher.Hash(ValidKey)), TestActor);

        Assert.True(ExpectMsg<bool>());
    }

    [Fact]
    public void Unknown_key_hash_is_rejected()
    {
        var auth = Sys.ActorOf(AuthActor.Props(new[] { ValidKey }));

        auth.Tell(new AuthActor.Authenticate(ApiKeyHasher.Hash("not-a-real-key")), TestActor);

        Assert.False(ExpectMsg<bool>());
    }

    [Fact]
    public void Plaintext_key_is_rejected_even_when_the_key_itself_is_valid()
    {
        var auth = Sys.ActorOf(AuthActor.Props(new[] { ValidKey }));

        auth.Tell(new AuthActor.Authenticate(ValidKey), TestActor);

        Assert.False(ExpectMsg<bool>());
    }

    [Fact]
    public void Actor_with_no_configured_keys_rejects_everything()
    {
        var auth = Sys.ActorOf(AuthActor.Props(Array.Empty<string>()));

        auth.Tell(new AuthActor.Authenticate(ApiKeyHasher.Hash(ValidKey)), TestActor);

        Assert.False(ExpectMsg<bool>());
    }
}
