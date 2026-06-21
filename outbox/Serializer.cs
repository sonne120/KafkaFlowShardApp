using Newtonsoft.Json;

namespace KafkaFlowShardApp.Outbox;

internal sealed class Serializer : ISerializer
{
    public string Serialize<T>(T data) where T : class
        => JsonConvert.SerializeObject(data);

    public T Deserialize<T>(string data) where T : class
        => JsonConvert.DeserializeObject<T>(data)!;
}
