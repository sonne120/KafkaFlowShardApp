using Akka.Actor;
using Akka.TestKit;
using Akka.TestKit.TestActors;
using Akka.TestKit.Xunit2;
using PacketShard.Master;
using PacketShard.Shared;
using Xunit;

namespace PacketShard.Tests.MasterNode;

[Trait("Category", "Unit")]
public sealed class ShardRouterActorTests : TestKit
{
    [Theory]
    [InlineData(ProtocolType.Udp)]
    [InlineData(ProtocolType.Tcp)]
    [InlineData(ProtocolType.Https)]
    [InlineData(ProtocolType.Arp)]
    [InlineData(ProtocolType.Other)]
    public void Packet_reaches_its_own_shard_and_no_other(ProtocolType protocol)
    {
        var shards = ProbePerProtocol();
        var router = Sys.ActorOf(ShardRouterActor.Props(PropsFor(shards)));

        router.Tell(new WriteToShard(protocol, $$"""{"proto":"{{protocol}}"}"""));

        var delivered = shards[protocol].ExpectMsg<WriteToShard>();
        Assert.Equal(protocol, delivered.Protocol);

        foreach (var (other, probe) in shards.Where(shard => shard.Key != protocol))
        {
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
            Assert.NotEqual(protocol, other);
        }
    }

    [Fact]
    public void Router_forwards_so_the_shard_replies_to_the_original_sender()
    {
        var shards = ProbePerProtocol();
        var router = Sys.ActorOf(ShardRouterActor.Props(PropsFor(shards)));
        var origin = CreateTestProbe();

        router.Tell(new WriteToShard(ProtocolType.Udp, "{}"), origin.Ref);

        shards[ProtocolType.Udp].ExpectMsg<WriteToShard>();
        Assert.Equal(origin.Ref, shards[ProtocolType.Udp].LastSender);

        // And the round-trip closes the loop: the shard's reply goes back to the original sender, 
        // not the router.
        shards[ProtocolType.Udp].Reply("Ok");
        origin.ExpectMsg("Ok");
    }

    [Fact]
    public async Task Router_starts_one_child_per_configured_shard()
    {
        var shards = ProbePerProtocol();
        var router = Sys.ActorOf(ShardRouterActor.Props(PropsFor(shards)), "shard-router");
        Assert.NotNull(router);

        foreach (var protocol in shards.Keys)
        {
            var child = Sys.ActorSelection($"/user/shard-router/shard-{protocol}".ToLowerInvariant());
            Assert.NotNull(await child.ResolveOne(TimeSpan.FromSeconds(3)));
        }
    }

    private Dictionary<ProtocolType, TestProbe> ProbePerProtocol() =>
        Enum.GetValues<ProtocolType>().ToDictionary(protocol => protocol, _ => CreateTestProbe());

    private static IReadOnlyDictionary<ProtocolType, Props> PropsFor(
        IReadOnlyDictionary<ProtocolType, TestProbe> shards) =>
        shards.ToDictionary(shard => shard.Key, shard => ForwardActor.Props(shard.Value.Ref));
}
