using Microsoft.EntityFrameworkCore;

namespace KafkaFlowShardApp.Outbox.Persistence;

public class ApplicationDbContext : DbContext
{
    public ApplicationDbContext(DbContextOptions options)
        : base(options)
    {
    }
}
