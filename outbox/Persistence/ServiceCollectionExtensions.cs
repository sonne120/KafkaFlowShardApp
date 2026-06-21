using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System.Reflection;

namespace KafkaFlowShardApp.Outbox.Persistence;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddPersistence<TContext>(this IServiceCollection services, string connectionString, bool retryOnFailure = false, int maxRetryCount = 5)
        where TContext : DbContext
    {
        services.AddApplicationDbContext<TContext>(connectionString, retryOnFailure, maxRetryCount);
        services.TryAddScoped<DbContext>(sp => sp.GetRequiredService<TContext>());
        return services;
    }

    private static void AddApplicationDbContext<TContext>(this IServiceCollection services, string connectionString, bool retryOnFailure, int maxRetryCount)
        where TContext : DbContext
    {
        services.AddDbContext<TContext>((sp, options) =>
        {
            var mySqlOptions = new MySqlServerVersion(new Version(8, 0, 0));

            var builder = options.UseMySql(
                connectionString,
                mySqlOptions,
                cfg =>
                {
                    cfg.MigrationsAssembly(Assembly.GetExecutingAssembly().FullName);

                    if (retryOnFailure)
                    {
                        cfg.EnableRetryOnFailure(
                            maxRetryCount: maxRetryCount,
                            maxRetryDelay: TimeSpan.FromSeconds(30),
                            errorNumbersToAdd: null);
                    }
                });

            builder.UseQueryTrackingBehavior(QueryTrackingBehavior.TrackAll);
        });
    }
}
