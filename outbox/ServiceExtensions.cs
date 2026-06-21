using Microsoft.Extensions.DependencyInjection;

namespace KafkaFlowShardApp.Outbox;

public static class ServiceExtensions
{
    public static IServiceCollection AddOutbox(this IServiceCollection services)
    {
        services.AddSingleton<ISerializer, Serializer>();

        services.AddScoped<IOutbox, Outbox>();
        services.AddScoped<IRelay, Relay>();
        services.AddScoped<IUnitOfWork, UnitOfWork>();
        services.AddScoped<IOutboxInitializer, OutboxInitializer>();

        services.AddHostedService<PublishOutboxJob>();
        services.AddHostedService<CleanupOutboxJob>();

        return services;
    }
}
