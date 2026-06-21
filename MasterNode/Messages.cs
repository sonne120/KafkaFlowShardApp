using KafkaFlowShardApp.Shared;

namespace KafkaFlowShardApp.Master;

public sealed record WriteToShard(ProtocolType Protocol, string Json);

public sealed record IncomingPacket(string Json);
