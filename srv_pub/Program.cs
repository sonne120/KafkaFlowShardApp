using KafkaFlowShardApp.Kafka;
using KafkaFlowShardApp.Outbox;
using KafkaFlowShardApp.Outbox.Persistence;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// srv_pub is the outbox RELAY: it owns the publish + cleanup jobs that drain the MySQL outbox
// to Kafka. Packets are no longer generated here — they arrive over gRPC (srv_ingest), which
// writes them to the outbox; this service publishes them.
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddKafkaPublish("");
builder.Services.AddOutbox(); // runRelayJobs: true -> PublishOutboxJob + CleanupOutboxJob
builder.Services.AddPersistence<ApplicationDbContext>(
    builder.Configuration["SqlConnStr"] ?? throw new ArgumentException("SqlConnStr is required"),
    retryOnFailure: true,
    maxRetryCount: 5);

var host = builder.Build();

await InitializeOutboxAsync(host);

host.Run();
return;

// Ensure the outbox table + stored procedure exist before the relay starts polling.
static async Task InitializeOutboxAsync(IHost host)
{
    var logger = host.Services.GetRequiredService<ILogger<Program>>();
    for (var attempt = 1; attempt <= 20; attempt++)
    {
        try
        {
            using var scope = host.Services.CreateScope();
            var initializer = scope.ServiceProvider.GetRequiredService<IOutboxInitializer>();
            await initializer.InitializeAsync(CancellationToken.None);
            logger.LogInformation("Outbox schema initialized; relay starting");
            return;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Outbox init attempt {Attempt} failed; retrying in 3s (waiting for MySQL)", attempt);
            await Task.Delay(TimeSpan.FromSeconds(3));
        }
    }
}
