using System.Security.Cryptography;
using System.Text;

namespace KafkaFlowShardApp.Shared;

public static class ApiKeyHasher
{
    public static string Hash(string apiKey)
    {
        var bytes = Encoding.UTF8.GetBytes(apiKey + "\n");
        var hash = SHA256.HashData(bytes);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
