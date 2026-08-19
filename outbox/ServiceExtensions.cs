using Microsoft.Extensions.DependencyInjection;

namespace PacketShard.Outbox;

public static class ServiceExtensions
{
    public static IServiceCollection AddOutbox(this IServiceCollection services, bool runRelayJobs = true)
    {
        services.AddSingleton<ISerializer, Serializer>();

        services.AddScoped<IOutbox, Outbox>();
        services.AddScoped<IRelay, Relay>();
        services.AddScoped<IUnitOfWork, UnitOfWork>();
        services.AddScoped<IOutboxInitializer, OutboxInitializer>();

        if (runRelayJobs)
        {
            services.AddHostedService<PublishOutboxJob>();
            services.AddHostedService<CleanupOutboxJob>();
        }

        return services;
    }
}
