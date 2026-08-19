using PacketShard.Outbox;
using Xunit;

namespace PacketShard.Tests.Outbox;

[Trait("Category", "Unit")]
public sealed class SqlQueriesTests
{
    private static readonly string[] RequiredQueries =
    {
        "OutboxTable.sql",
        "InsertInOutbox.sql",
        "ReserveForProcessing.sql",
        "MarkAsProcessed.sql",
        "DeleteProcessed.sql"
    };

    [Fact]
    public void Every_query_the_outbox_uses_is_embedded_exactly_once()
    {
        var resources = typeof(OutboxRecord).Assembly.GetManifestResourceNames();

        foreach (var query in RequiredQueries)
        {
            Assert.Single(resources, name => name.EndsWith(query, StringComparison.Ordinal));
        }
    }

    [Fact]
    public void Embedded_queries_are_not_empty()
    {
        var assembly = typeof(OutboxRecord).Assembly;

        foreach (var query in RequiredQueries)
        {
            var name = assembly.GetManifestResourceNames()
                .Single(n => n.EndsWith(query, StringComparison.Ordinal));

            using var stream = assembly.GetManifestResourceStream(name);
            Assert.NotNull(stream);
            using var reader = new StreamReader(stream!);
            Assert.False(string.IsNullOrWhiteSpace(reader.ReadToEnd()), $"{query} is empty");
        }
    }

    [Fact]
    public void The_reservation_query_still_uses_skip_locked()
    {
        var assembly = typeof(OutboxRecord).Assembly;
        var name = assembly.GetManifestResourceNames()
            .Single(n => n.EndsWith("ReserveForProcessing.sql", StringComparison.Ordinal));

        using var reader = new StreamReader(assembly.GetManifestResourceStream(name)!);
        var sql = reader.ReadToEnd();

        Assert.Contains("FOR UPDATE SKIP LOCKED", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("IsSequential = 0", sql, StringComparison.OrdinalIgnoreCase);
    }
}
