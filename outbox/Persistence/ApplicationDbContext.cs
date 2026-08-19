using Microsoft.EntityFrameworkCore;

namespace PacketShard.Outbox.Persistence;

public class ApplicationDbContext : DbContext
{
    public ApplicationDbContext(DbContextOptions options)
        : base(options)
    {
    }
}
