using PacketShard.ServiceDiscovery;
using PacketShard.Shared;
using PacketShard.Sub;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

// Lookups only — srv_sub is a consumer, it has no endpoint of its own to advertise.
builder.Services.AddServiceDiscovery(builder.Configuration);

builder.Services.AddSingleton<ISerializer, Serializer>();
builder.Services.AddSingleton<TcpForwarder>();
builder.Services.AddSingleton<DeadLetterProducer>();
builder.Services.AddHostedService<KafkaConsumerRx>();

var host = builder.Build();
host.Run();
