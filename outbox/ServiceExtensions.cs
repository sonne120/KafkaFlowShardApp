using Microsoft.Extensions.DependencyInjection;

namespace PacketShard.Outbox;

public static class ServiceExtensions
{
    /// <param name="runRelayJobs">
    /// When true (default) this service also runs the publish + cleanup background jobs that
    /// drain the outbox to Kafka. Set false for write-only producers (e.g. srv_ingest) so the
    /// relay stays owned by a single service (srv_pub).
    /// </param>
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
