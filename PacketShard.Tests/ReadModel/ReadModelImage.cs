using System.Reflection;
using DotNet.Testcontainers.Builders;

namespace PacketShard.Tests.ReadModel;

internal static class ReadModelImage
{
    public const string Tag = "packetshard-postgres";

    private static readonly Lazy<Task<string>> Ready = new(BuildAsync);

    public static Task<string> EnsureAsync() => Ready.Value;

    private static async Task<string> BuildAsync()
    {
        var prebuilt = Environment.GetEnvironmentVariable("PACKETSHARD_TEST_PG_IMAGE");
        if (!string.IsNullOrWhiteSpace(prebuilt))
            return prebuilt;

        var image = new ImageFromDockerfileBuilder()
            .WithDockerfileDirectory(Path.Combine(RepositoryRoot, "postgres"))
            .WithDockerfile("Dockerfile")
            .WithName(Tag)
            .WithDeleteIfExists(false)
            .WithCleanUp(false)
            .Build();

        await image.CreateAsync();
        return Tag;
    }

    private static string RepositoryRoot { get; } =
        typeof(ReadModelImage).Assembly
            .GetCustomAttributes<AssemblyMetadataAttribute>()
            .SingleOrDefault(a => a.Key == "RepositoryRoot")?.Value
        ?? throw new InvalidOperationException(
            "RepositoryRoot assembly metadata is missing — check PacketShard.Tests.csproj.");
}
