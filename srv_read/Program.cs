using PacketShard.Read;
using Npgsql;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

var pgConnStr = builder.Configuration["Postgres__ConnStr"]
    ?? "Host=postgres;Port=5432;Database=readmodel;Username=postgres;Password=postgres";
var redisConnStr = builder.Configuration["Redis__ConnStr"] ?? "redis:6379";
var redisTtlDays = int.TryParse(builder.Configuration["Redis__TtlDays"], out var d) ? d : 7;

builder.Services.AddNpgsqlDataSource(pgConnStr);
builder.Services.AddSingleton<IConnectionMultiplexer>(
    _ => ConnectionMultiplexer.Connect(redisConnStr));
builder.Services.AddSingleton(sp =>
    new RedisFastPath(sp.GetRequiredService<IConnectionMultiplexer>(), TimeSpan.FromDays(redisTtlDays)));
builder.Services.AddSingleton<ReadModelStore>();
builder.Services.AddSingleton<ProjectionHandler>();
builder.Services.AddHostedService<CdcConsumer>();

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

app.MapGet("/stats/protocols", async (NpgsqlDataSource db, CancellationToken ct) =>
{
    const string sql = """
        SELECT proto, packet_count, first_seen, last_seen
        FROM packet_stats_by_proto
        ORDER BY packet_count DESC;
        """;

    var rows = new List<object>();
    await using var cmd = db.CreateCommand(sql);
    await using var reader = await cmd.ExecuteReaderAsync(ct);
    while (await reader.ReadAsync(ct))
    {
        rows.Add(new
        {
            proto = reader.GetString(0),
            packetCount = reader.GetInt64(1),
            firstSeen = reader.IsDBNull(2) ? (DateTime?)null : reader.GetDateTime(2),
            lastSeen = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3)
        });
    }
    return Results.Ok(rows);
});

// Last-value view: current state per client, kept correct by the version guard.
app.MapGet("/stats/clients", async (NpgsqlDataSource db, CancellationToken ct) =>
{
    const string sql = """
        SELECT client_id, version, last_proto, updated_at
        FROM client_state
        ORDER BY updated_at DESC
        LIMIT 100;
        """;

    var rows = new List<object>();
    await using var cmd = db.CreateCommand(sql);
    await using var reader = await cmd.ExecuteReaderAsync(ct);
    while (await reader.ReadAsync(ct))
    {
        rows.Add(new
        {
            clientId = reader.GetString(0),
            version = reader.GetInt64(1),
            lastProto = reader.GetString(2),
            updatedAt = reader.GetDateTime(3)
        });
    }
    return Results.Ok(rows);
});

app.MapGet("/stats/total", async (NpgsqlDataSource db, CancellationToken ct) =>
{
    await using var cmd = db.CreateCommand("SELECT count(*) FROM packet_ledger;");
    var total = (long)(await cmd.ExecuteScalarAsync(ct))!;
    return Results.Ok(new { totalPackets = total });
});

app.Run();
