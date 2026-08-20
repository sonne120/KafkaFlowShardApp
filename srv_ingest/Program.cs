using PacketShard.Ingest.Services;
using PacketShard.Kafka;
using PacketShard.Outbox;
using PacketShard.Outbox.Persistence;
using PacketShard.ServiceDiscovery;
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


// Publishes this replica to Consul once its health check passes, so the gateway picks it up
// without anyone editing a destination list. A no-op while Consul:Enabled is false.
builder.Services.AddServiceRegistration(builder.Configuration);

var grpcPort = int.TryParse(builder.Configuration["GrpcPort"], out var p) ? p : 8080;

// A second listener purely for health probes. The gRPC port is HTTP/2-only — plaintext has no
// ALPN to negotiate with, so it cannot also carry HTTP/1.1 — and Consul's HTTP check speaks
// HTTP/1.1. Same endpoint, two ports: YARP probes it over h2c on the service port, Consul over
// HTTP/1.1 here.
var healthPort = int.TryParse(builder.Configuration["HealthPort"], out var hp) ? hp : 8081;

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(grpcPort, listen => listen.Protocols = HttpProtocols.Http2);
    options.ListenAnyIP(healthPort, listen => listen.Protocols = HttpProtocols.Http1);
});

var app = builder.Build();

await InitializeOutboxAsync(app);

app.MapGrpcService<PacketIngestService>();
app.MapGet("/", () => "PacketIngest gRPC endpoint. Use a gRPC client.");
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

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
