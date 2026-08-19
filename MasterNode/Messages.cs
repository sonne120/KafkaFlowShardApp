using PacketShard.Shared;

namespace PacketShard.Master;

public sealed record WriteToShard(ProtocolType Protocol, string Json);

public sealed record IncomingPacket(string Json);
