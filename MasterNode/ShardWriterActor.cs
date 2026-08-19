using Akka.Actor;
using PacketShard.Shared;
using MongoDB.Driver;
using Newtonsoft.Json;

namespace PacketShard.Master;

public sealed class ShardWriterActor : ReceiveActor
{
    private readonly IMongoCollection<PacketDocument> _collection;
    private readonly ProtocolType _protocol;

    public ShardWriterActor(ProtocolType protocol, string connectionString, string databaseName, string collectionName)
    {
        _protocol = protocol;
        var client = new MongoClient(connectionString);
        var database = client.GetDatabase(databaseName);
        _collection = database.GetCollection<PacketDocument>(collectionName);

        ReceiveAsync<WriteToShard>(async write =>
        {
            try
            {
                var snapshot = JsonConvert.DeserializeObject<SnapshotMessage>(write.Json)
                               ?? throw new InvalidOperationException("Payload deserialized to null.");

                var document = new PacketDocument
                {
                    transaction_id = string.IsNullOrWhiteSpace(snapshot.transaction_id)
                        ? Guid.NewGuid().ToString()
                        : snapshot.transaction_id,
                    client_id = string.IsNullOrWhiteSpace(snapshot.client_id)
                        ? snapshot.source_ip
                        : snapshot.client_id,
                    version = snapshot.version,
                    source_port = snapshot.source_port,
                    dest_port = snapshot.dest_port,
                    source_ip = snapshot.source_ip,
                    dest_ip = snapshot.dest_ip,
                    source_mac = snapshot.source_mac,
                    dest_mac = snapshot.dest_mac,
                    proto = snapshot.proto
                };

                await _collection.InsertOneAsync(document);
                Console.WriteLine($"[shard:{_protocol}] saved {document.proto} {document.source_ip} -> {document.dest_ip}");
                Sender.Tell("Ok");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[shard:{_protocol}] write failed: {ex.Message}");
                Sender.Tell($"Write failed: {ex.Message}");
            }
        });
    }

    public static Props Props(ProtocolType protocol, string connectionString, string databaseName, string collectionName) =>
        Akka.Actor.Props.Create(() => new ShardWriterActor(protocol, connectionString, databaseName, collectionName));
}
