using KafkaFlowShardApp.Master;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<AkkaHostedService>();

var host = builder.Build();
host.Run();
