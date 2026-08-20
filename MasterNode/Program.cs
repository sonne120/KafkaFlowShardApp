using PacketShard.Master;
using PacketShard.ServiceDiscovery;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddHostedService<AkkaHostedService>();

// The MasterNode speaks a line protocol over raw TCP, not HTTP, so its Consul check opens a
// socket instead of issuing a request — enough to tell "accepting connections" from "gone".
builder.Services.AddServiceRegistration(builder.Configuration);

var host = builder.Build();
host.Run();
