using Consul;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace PacketShard.ServiceDiscovery;

public static class ServiceExtensions
{
    public static IServiceCollection AddServiceDiscovery(this IServiceCollection services, IConfiguration configuration)
    {
        var section = configuration.GetSection(DiscoveryOptions.SectionName);
        services.Configure<DiscoveryOptions>(section);

        var options = Read(configuration);

        switch (options.Provider)
        {
            case DiscoveryProvider.Consul:
                services.AddSingleton<IConsulClient>(_ => new ConsulClient(config =>
                {
                    config.Address = new Uri(options.Consul.Address);
                    if (!string.IsNullOrWhiteSpace(options.Consul.Token))
                        config.Token = options.Consul.Token;
                }));
                services.AddSingleton<IServiceDirectory, ConsulServiceDirectory>();
                break;

            case DiscoveryProvider.Dns:
                services.AddSingleton<IServiceDirectory, DnsServiceDirectory>();
                break;

            default:
                services.AddSingleton<IServiceDirectory, StaticServiceDirectory>();
                break;
        }

        return services;
    }

    public static IServiceCollection AddServiceRegistration(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddServiceDiscovery(configuration);

        var options = Read(configuration);
        if (options.Provider == DiscoveryProvider.Consul && options.Service.Enabled)
            services.AddHostedService<ConsulRegistrationService>();

        return services;
    }

    private static DiscoveryOptions Read(IConfiguration configuration)
        => configuration.GetSection(DiscoveryOptions.SectionName).Get<DiscoveryOptions>() ?? new DiscoveryOptions();
}
