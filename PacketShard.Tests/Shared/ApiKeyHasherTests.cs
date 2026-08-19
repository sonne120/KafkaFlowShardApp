using System.Security.Cryptography;
using System.Text;
using PacketShard.Shared;
using Xunit;

namespace PacketShard.Tests.Shared;

/// <summary>
/// The hash both ends of the TCP handshake agree on: srv_sub sends it, the MasterNode's AuthActor
/// compares it. Both sides call this one method, so its exact output is a wire contract — change
/// it and every deployed client fails authentication at once.
/// </summary>
[Trait("Category", "Unit")]
public sealed class ApiKeyHasherTests
{
    [Fact]
    public void Hash_is_deterministic()
    {
        Assert.Equal(ApiKeyHasher.Hash("valid_api_key_1"), ApiKeyHasher.Hash("valid_api_key_1"));
    }

    [Fact]
    public void Different_keys_hash_differently()
    {
        Assert.NotEqual(ApiKeyHasher.Hash("valid_api_key_1"), ApiKeyHasher.Hash("valid_api_key_2"));
    }

    [Fact]
    public void Hash_is_lowercase_hex_sha256()
    {
        var hash = ApiKeyHasher.Hash("valid_api_key_1");

        Assert.Equal(64, hash.Length);
        Assert.All(hash, c => Assert.True(char.IsAsciiDigit(c) || (c >= 'a' && c <= 'f'), $"'{c}' is not lowercase hex"));
    }

    [Fact]
    public void Hash_includes_the_trailing_newline_the_wire_protocol_expects()
    {
        // The implementation hashes key + "\n". That newline is part of the contract, not a
        // formatting accident — drop it and every deployed client's hash stops matching.
        const string key = "valid_api_key_1";

        Assert.Equal(Sha256Hex(key + "\n"), ApiKeyHasher.Hash(key));
        Assert.NotEqual(Sha256Hex(key), ApiKeyHasher.Hash(key));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("VALID_API_KEY_1")]
    public void Hash_is_sensitive_to_exact_input(string key)
    {
        Assert.NotEqual(ApiKeyHasher.Hash("valid_api_key_1"), ApiKeyHasher.Hash(key));
    }

    private static string Sha256Hex(string value) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value))).ToLowerInvariant();
}
