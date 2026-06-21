using KafkaFlowShardApp.Shared;
using KafkaFlowShardApp.Sub;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<ISerializer, Serializer>();
builder.Services.AddSingleton<TcpForwarder>();
builder.Services.AddSingleton<DeadLetterProducer>();
builder.Services.AddHostedService<KafkaConsumerRx>();

var host = builder.Build();
host.Run();
