using System.Collections.Immutable;
using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MySqlConnector;
using PacketShard.Kafka;
using PacketShard.Outbox;
using PacketShard.Outbox.Persistence;
using Testcontainers.MySql;

namespace PacketShard.Tests.Outbox;

public sealed class MySqlOutboxFixture : Xunit.IAsyncLifetime
{
    private MySqlContainer _mySql = null!;
    private ServiceProvider _services = null!;

    public RecordingKafkaPub Kafka { get; } = new();

    public string ConnectionString => _mySql.GetConnectionString();

    public async Task InitializeAsync()
    {
        _mySql = new MySqlBuilder()
            .WithImage("mysql:8.0")
            .WithDatabase("outbox")
            .WithUsername("root")
            .WithPassword("root")
            .Build();

        await _mySql.StartAsync();

        var services = new ServiceCollection();
        services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Warning));
        services.AddSingleton<IKafkaMessagePub>(Kafka);
        services.AddOutbox(runRelayJobs: false);   // the relay is driven by hand
        services.AddPersistence<ApplicationDbContext>(ConnectionString);
        _services = services.BuildServiceProvider();

        await InitializeSchemaAsync();
    }

    public async Task DisposeAsync()
    {
        await _services.DisposeAsync();
        await _mySql.DisposeAsync();
    }

    public async Task InitializeSchemaAsync()
    {
        await using var scope = _services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IOutboxInitializer>()
            .InitializeAsync(CancellationToken.None);
    }

    public AsyncServiceScope CreateScope() => _services.CreateAsyncScope();

    public async Task ResetAsync()
    {
        Kafka.Reset();
        await using var conn = new MySqlConnection(ConnectionString);
        await conn.ExecuteAsync("DELETE FROM Outbox");
    }

    //raw reads
    public async Task<IReadOnlyList<OutboxRow>> RowsAsync()
    {
        await using var conn = new MySqlConnection(ConnectionString);
        var rows = await conn.QueryAsync<OutboxRow>(
            "SELECT Id, RawData, MessageType, Topic, PartitionBy, IsProcessed, IsSequential, " +
            "       Metadata, ReservedAt, ExpiredAt, IsProcessing, DateTimestamp " +
            "FROM Outbox ORDER BY DateTimestamp");
        return rows.ToList();
    }

    public async Task<T> ScalarAsync<T>(string sql)
    {
        await using var conn = new MySqlConnection(ConnectionString);
        return (await conn.ExecuteScalarAsync<T>(sql))!;
    }
}

public sealed class OutboxRow
{
    public Guid Id { get; init; }
    public string RawData { get; init; } = default!;
    public string MessageType { get; init; } = default!;
    public string Topic { get; init; } = default!;
    public string? PartitionBy { get; init; }
    public bool IsProcessed { get; init; }
    public bool IsSequential { get; init; }
    public string? Metadata { get; init; }
    public DateTime? ReservedAt { get; init; }
    public DateTime? ExpiredAt { get; init; }
    public bool IsProcessing { get; init; }
    public DateTime DateTimestamp { get; init; }
}

public sealed class RecordingKafkaPub : IKafkaMessagePub
{
    private readonly List<Message> _sent = new();

    public IReadOnlyList<Message> Sent
    {
        get { lock (_sent) return _sent.ToList(); }
    }

    public Exception? FailWith { get; set; }

    public Task SendAsync(ImmutableArray<Message> messages, CancellationToken cancellationToken)
    {
        if (FailWith is not null)
            return Task.FromException(FailWith);

        lock (_sent) _sent.AddRange(messages);
        return Task.CompletedTask;
    }

    public void Reset()
    {
        FailWith = null;
        lock (_sent) _sent.Clear();
    }
}
