using KafkaFlowShardApp.Ingest.Services;
using KafkaFlowShardApp.Kafka;
using KafkaFlowShardApp.Outbox;
using KafkaFlowShardApp.Outbox.Persistence;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

// gRPC + the same durable outbox path srv_pub uses (write -> MySQL outbox -> relay -> Kafka).
builder.Services.AddGrpc();
builder.Services.AddKafkaPublish("");
// Write-only: ingest just stores packets; srv_pub owns the relay that drains the outbox to Kafka.
builder.Services.AddOutbox(runRelayJobs: false);
builder.Services.AddPersistence<ApplicationDbContext>(
    builder.Configuration["SqlConnStr"] ?? throw new ArgumentException("SqlConnStr is required"),
    retryOnFailure: true,
    maxRetryCount: 5);

// gRPC needs HTTP/2. We terminate plaintext h2c here (the load balancer talks h2c to us);
// TLS is the load balancer's job and is toggled there.
var grpcPort = int.TryParse(builder.Configuration["GrpcPort"], out var p) ? p : 8080;
builder.WebHost.ConfigureKestrel(options =>
    options.ListenAnyIP(grpcPort, listen => listen.Protocols = HttpProtocols.Http2));

var app = builder.Build();

await InitializeOutboxAsync(app);

app.MapGrpcService<PacketIngestService>();
app.MapGet("/", () => "PacketIngest gRPC endpoint. Use a gRPC client.");

app.Run();
return;

// Create the outbox table + stored procedure on startup, retrying while MySQL warms up.
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
