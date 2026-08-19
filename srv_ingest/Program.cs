using PacketShard.Ingest.Services;
using PacketShard.Kafka;
using PacketShard.Outbox;
using PacketShard.Outbox.Persistence;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();
builder.Services.AddKafkaPublish("");

builder.Services.AddOutbox(runRelayJobs: false);

var outboxConnStr = builder.Configuration.GetConnectionString("Outbox")
                    ?? builder.Configuration["SqlConnStr"]
                    ?? throw new ArgumentException("Set ConnectionStrings:Outbox (or SqlConnStr)");

builder.Services.AddPersistence<ApplicationDbContext>(
    outboxConnStr,
    retryOnFailure: true,
    maxRetryCount: 5);


var grpcPort = int.TryParse(builder.Configuration["GrpcPort"], out var p) ? p : 8080;
builder.WebHost.ConfigureKestrel(options =>
    options.ListenAnyIP(grpcPort, listen => listen.Protocols = HttpProtocols.Http2));

var app = builder.Build();

await InitializeOutboxAsync(app);

app.MapGrpcService<PacketIngestService>();
app.MapGet("/", () => "PacketIngest gRPC endpoint. Use a gRPC client.");

app.Run();
return;

static async Task InitializeOutboxAsync(WebApplication app)
{
    var logger = app.Services.GetRequiredService<ILogger<Program>>();
    for (var attempt = 1; attempt <= 20; attempt++)
    {
        try
        {
            using var scope = app.Services.CreateScope();
            var initializer = scope.ServiceProvider.GetRequiredService<IOutboxInitializer>();
            await initializer.InitializeAsync(CancellationToken.None);
            logger.LogInformation("Outbox schema initialized");
            return;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Outbox init attempt {Attempt} failed; retrying in 3s", attempt);
            await Task.Delay(TimeSpan.FromSeconds(3));
        }
    }
}
