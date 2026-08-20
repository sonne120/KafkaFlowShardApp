using Consul;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace PacketShard.ServiceDiscovery;

public static class ServiceExtensions
{
    public static IServiceCollection AddServiceDiscovery(this IServiceCollection services, IConfiguration configuration)
    {
        var section = configuration.GetSection(ConsulOptions.SectionName);
        services.Configure<ConsulOptions>(section);

        if (!Read(configuration).Enabled)
        {
            services.AddSingleton<IServiceDirectory, StaticServiceDirectory>();
            return services;
        }

        services.AddSingleton<IConsulClient>(_ =>
        {
            var options = Read(configuration);
            return new ConsulClient(config =>
            {
                config.Address = new Uri(options.Address);
                if (!string.IsNullOrWhiteSpace(options.Token))
                    config.Token = options.Token;
            });
        });

        services.AddSingleton<IServiceDirectory, ConsulServiceDirectory>();
        return services;
    }

    public static IServiceCollection AddServiceRegistration(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddServiceDiscovery(configuration);

        var options = Read(configuration);
        if (options.Enabled && options.Service.Enabled)
            services.AddHostedService<ConsulRegistrationService>();

        return services;
    }

    private static ConsulOptions Read(IConfiguration configuration)
        => configuration.GetSection(ConsulOptions.SectionName).Get<ConsulOptions>() ?? new ConsulOptions();
}
