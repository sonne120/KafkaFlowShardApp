using Npgsql;

namespace PacketShard.Tests.Infrastructure;

public sealed record LedgerRow(
    string TransactionId,
    string ClientId,
    long Version,
    string Proto,
    string? SourceIp,
    string? DestIp,
    int? SourcePort,
    int? DestPort,
    DateTimeOffset? StoredAt,
    string? Payload);

public sealed record ClientStateRow(
    string ClientId,
    long Version,
    string LastProto,
    DateTimeOffset UpdatedAt);

public sealed record ProtoStatRow(
    string Proto,
    long PacketCount,
    DateTimeOffset? FirstSeen,
    DateTimeOffset? LastSeen);

/// <summary>
/// Read-side helpers. These query the same objects the srv_read endpoints do, so a test asserts
/// against what the API would actually serve.
/// </summary>
public static class ReadModelQueries
{
    public static async Task<long> CountLedgerAsync(this NpgsqlDataSource db)
    {
        await using var cmd = db.CreateCommand("SELECT count(*) FROM packet_ledger;");
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    public static async Task<long> CountClientStateAsync(this NpgsqlDataSource db)
    {
        await using var cmd = db.CreateCommand("SELECT count(*) FROM client_state;");
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    public static async Task<LedgerRow?> GetLedgerRowAsync(this NpgsqlDataSource db, string transactionId)
    {
        await using var cmd = db.CreateCommand("""
            SELECT transaction_id, client_id, version, proto, source_ip, dest_ip,
                   source_port, dest_port, stored_at, payload::text
            FROM packet_ledger
            WHERE transaction_id = @tx;
            """);
        cmd.Parameters.AddWithValue("tx", transactionId);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return null;

        return new LedgerRow(
            reader.GetString(0),
            reader.GetString(1),
            reader.GetInt64(2),
            reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetInt32(6),
            reader.IsDBNull(7) ? null : reader.GetInt32(7),
            reader.IsDBNull(8) ? null : reader.GetFieldValue<DateTimeOffset>(8),
            reader.IsDBNull(9) ? null : reader.GetString(9));
    }

    public static async Task<ClientStateRow?> GetClientStateAsync(this NpgsqlDataSource db, string clientId)
    {
        await using var cmd = db.CreateCommand("""
            SELECT client_id, version, last_proto, updated_at
            FROM client_state
            WHERE client_id = @client;
            """);
        cmd.Parameters.AddWithValue("client", clientId);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return null;

        return new ClientStateRow(
            reader.GetString(0),
            reader.GetInt64(1),
            reader.GetString(2),
            reader.GetFieldValue<DateTimeOffset>(3));
    }

    /// <summary>Reads the pg_ivm IMMV as-is — no REFRESH, which is the whole point of it.</summary>
    public static async Task<IReadOnlyList<ProtoStatRow>> GetProtoStatsAsync(this NpgsqlDataSource db)
    {
        await using var cmd = db.CreateCommand("""
            SELECT proto, packet_count, first_seen, last_seen
            FROM packet_stats_by_proto
            ORDER BY proto;
            """);

        var rows = new List<ProtoStatRow>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new ProtoStatRow(
                reader.GetString(0),
                reader.GetInt64(1),
                reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTimeOffset>(2),
                reader.IsDBNull(3) ? null : reader.GetFieldValue<DateTimeOffset>(3)));
        }
        return rows;
    }

    /// <summary>Proves the payload landed as real jsonb rather than an opaque string.</summary>
    public static async Task<string?> GetPayloadFieldAsync(
        this NpgsqlDataSource db, string transactionId, string field)
    {
        await using var cmd = db.CreateCommand(
            "SELECT payload ->> @field FROM packet_ledger WHERE transaction_id = @tx;");
        cmd.Parameters.AddWithValue("field", field);
        cmd.Parameters.AddWithValue("tx", transactionId);
        return await cmd.ExecuteScalarAsync() as string;
    }
}
