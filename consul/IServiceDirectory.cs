namespace PacketShard.ServiceDiscovery;

public interface IServiceDirectory
{
    ValueTask<ServiceLookup> ResolveAsync(string serviceName, CancellationToken cancellationToken);

    Task WaitForChangeAsync(string serviceName, ulong index, CancellationToken cancellationToken);
}
