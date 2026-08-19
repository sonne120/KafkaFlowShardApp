using System.Text;
using Akka.Actor;
using Akka.IO;
using Akka.TestKit;
using Akka.TestKit.TestActors;
using Akka.TestKit.Xunit2;
using PacketShard.Master;
using PacketShard.Shared;
using Xunit;

namespace PacketShard.Tests.MasterNode;

[Trait("Category", "Unit")]
public sealed class ConnectionHandlerTests : TestKit
{
    [Fact]
    public void Invalid_api_key_is_rejected_and_the_connection_is_closed()
    {
        var (handler, connection, auth, router) = Handler();

        handler.Tell(Received("wrong-key-hash"));

        auth.ExpectMsg<AuthActor.Authenticate>();
        auth.Reply(false);

        Assert.Equal("Invalid API Key", ExpectWrittenText(connection));
        connection.ExpectMsg<Tcp.Close>();

        // Nothing may reach a shard on a rejected connection.
        router.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
    }

    [Fact]
    public void Packets_sent_before_authentication_never_reach_a_shard()
    {
        var (handler, connection, auth, router) = Handler();

        handler.Tell(Received(UdpPacket()));

        auth.ExpectMsg<AuthActor.Authenticate>();
        auth.Reply(false);

        Assert.Equal("Invalid API Key", ExpectWrittenText(connection));
        router.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
    }

    [Fact]
    public void Valid_api_key_is_accepted_and_opens_the_authenticated_state()
    {
        var (handler, connection, auth, _) = Handler();

        handler.Tell(Received(ApiKeyHasher.Hash("valid_api_key_1")));

        auth.ExpectMsg<AuthActor.Authenticate>();
        auth.Reply(true);

        Assert.Equal("API Key authenticated. You can now send messages.", ExpectWrittenText(connection));
        connection.ExpectNoMsg(TimeSpan.FromMilliseconds(100));
    }

    [Fact]
    public void Udp_packet_is_classified_and_routed_to_the_udp_shard()
    {

        var shards = ProbePerProtocol();
        var router = Sys.ActorOf(ShardRouterActor.Props(PropsFor(shards)));
        var (handler, connection, auth, _) = Handler(router);

        Authenticate(handler, auth, connection);

        handler.Tell(Received(UdpPacket(sourceIp: "10.0.0.7")));

        var routed = shards[ProtocolType.Udp].ExpectMsg<WriteToShard>();
        Assert.Equal(ProtocolType.Udp, routed.Protocol);
        Assert.Contains("10.0.0.7", routed.Json);

        foreach (var (_, probe) in shards.Where(shard => shard.Key != ProtocolType.Udp))
            probe.ExpectNoMsg(TimeSpan.FromMilliseconds(100));

        shards[ProtocolType.Udp].Reply("Ok");
        Assert.Equal("Ok", ExpectWrittenText(connection));
    }

    [Theory]
    [InlineData("UDP", ProtocolType.Udp)]
    [InlineData("TCP", ProtocolType.Tcp)]
    [InlineData("HTTPS", ProtocolType.Https)]
    [InlineData("TLS", ProtocolType.Https)]   // alias
    [InlineData("ARP", ProtocolType.Arp)]
    [InlineData("ICMP", ProtocolType.Other)]  // unknown proto falls through to Other
    public void Packet_is_classified_by_its_proto_field(string proto, ProtocolType expected)
    {
        var (handler, connection, auth, router) = Handler();
        Authenticate(handler, auth, connection);

        handler.Tell(Received(UdpPacket(proto: proto)));

        Assert.Equal(expected, router.ExpectMsg<WriteToShard>().Protocol);
    }

    [Fact]
    public void Unparseable_payload_is_routed_to_the_other_shard_rather_than_dropped()
    {
        var (handler, connection, auth, router) = Handler();
        Authenticate(handler, auth, connection);

        handler.Tell(Received("this is not json"));

        Assert.Equal(ProtocolType.Other, router.ExpectMsg<WriteToShard>().Protocol);
    }

    [Fact]
    public void Newline_batched_packets_are_routed_one_per_line()
    {
        var (handler, connection, auth, router) = Handler();
        Authenticate(handler, auth, connection);

        handler.Tell(Received(
            UdpPacket(proto: "UDP") + "\n" + UdpPacket(proto: "TCP") + "\n" + UdpPacket(proto: "ARP")));

        var routed = Enumerable.Range(0, 3).Select(_ => router.ExpectMsg<WriteToShard>().Protocol).ToList();
        Assert.Equal(
            new[] { ProtocolType.Udp, ProtocolType.Tcp, ProtocolType.Arp }.OrderBy(p => p),
            routed.OrderBy(p => p));
    }

    //helpers
    private (IActorRef Handler, TestProbe Connection, TestProbe Auth, TestProbe Router) Handler()
    {
        var router = CreateTestProbe();
        var (handler, connection, auth, _) = Handler(router.Ref);
        return (handler, connection, auth, router);
    }

    private (IActorRef Handler, TestProbe Connection, TestProbe Auth, TestProbe Unused) Handler(IActorRef router)
    {
        var connection = CreateTestProbe();
        var auth = CreateTestProbe();
        var handler = Sys.ActorOf(ConnectionHandler.Props(connection.Ref, auth.Ref, router));
        return (handler, connection, auth, CreateTestProbe());
    }

    private static void Authenticate(IActorRef handler, TestProbe auth, TestProbe connection)
    {
        handler.Tell(Received(ApiKeyHasher.Hash("valid_api_key_1")));
        auth.ExpectMsg<AuthActor.Authenticate>();
        auth.Reply(true);
        connection.ExpectMsg<Tcp.Write>();   //"authenticated" greeting
    }

    private static Tcp.Received Received(string text) =>
        new(ByteString.FromBytes(Encoding.UTF8.GetBytes(text)));

    private static string ExpectWrittenText(TestProbe connection) =>
        Encoding.UTF8.GetString(connection.ExpectMsg<Tcp.Write>().Data.ToArray());

    private static string UdpPacket(string proto = "UDP", string sourceIp = "10.0.0.1") =>
        $$"""
        {"transaction_id":"tx-1","client_id":"c1","version":1,"source_port":51000,
         "dest_port":443,"source_ip":"{{sourceIp}}","dest_ip":"10.0.0.2",
         "source_mac":"aa:bb:cc:dd:ee:ff","dest_mac":"ff:ee:dd:cc:bb:aa","proto":"{{proto}}"}
        """.Replace("\n", "").Replace("\r", "");

    private Dictionary<ProtocolType, TestProbe> ProbePerProtocol() =>
        Enum.GetValues<ProtocolType>().ToDictionary(protocol => protocol, _ => CreateTestProbe());

    private static IReadOnlyDictionary<ProtocolType, Props> PropsFor(
        IReadOnlyDictionary<ProtocolType, TestProbe> shards) =>
        shards.ToDictionary(shard => shard.Key, shard => ForwardActor.Props(shard.Value.Ref));
}
