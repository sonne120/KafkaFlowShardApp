using KafkaFlowShardApp.Kafka;
using KafkaFlowShardApp.Outbox;
using KafkaFlowShardApp.Outbox.Persistence;
using KafkaFlowShardApp.Pub;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<PacketGenerator>();

builder.Services.AddKafkaPublish("");
builder.Services.AddOutbox();
builder.Services.AddPersistence<ApplicationDbContext>(
    builder.Configuration["SqlConnStr"] ?? throw new ArgumentException("SqlConnStr is required"),
    retryOnFailure: true,
    maxRetryCount: 5);

builder.Services.AddHostedService<PublisherWorker>();

var host = builder.Build();
host.Run();
