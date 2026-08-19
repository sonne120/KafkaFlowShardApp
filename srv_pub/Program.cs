using PacketShard.Kafka;
using PacketShard.Outbox;
using PacketShard.Outbox.Persistence;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;


var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddKafkaPublish("");
builder.Services.AddOutbox();

var outboxConnStr = builder.Configuration.GetConnectionString("Outbox")
                    ?? builder.Configuration["SqlConnStr"]
                    ?? throw new ArgumentException("Set ConnectionStrings:Outbox (or SqlConnStr)");

builder.Services.AddPersistence<ApplicationDbContext>(
    outboxConnStr,
    retryOnFailure: true,
    maxRetryCount: 5);

var host = builder.Build();

await InitializeOutboxAsync(host);

host.Run();
return;

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
