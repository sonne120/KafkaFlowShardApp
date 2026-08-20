using Npgsql;
using NpgsqlTypes;

namespace PacketShard.Read;
public sealed class ReadModelStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly ILogger<ReadModelStore> _logger;

    public ReadModelStore(NpgsqlDataSource dataSource, ILogger<ReadModelStore> logger)
    {
        _dataSource = dataSource;
        _logger = logger;
    }
        public async Task<bool> ProjectAsync(PacketRecord record, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var tx = await conn.BeginTransactionAsync(ct);

        long inserted;
        await using (var cmd = new NpgsqlCommand(InsertLedgerSql, conn, tx))
        {
            cmd.Parameters.AddWithValue("transaction_id", record.TransactionId);
            cmd.Parameters.AddWithValue("client_id", record.ClientId);
            cmd.Parameters.AddWithValue("version", record.Version);
            cmd.Parameters.AddWithValue("proto", record.Proto);
            cmd.Parameters.AddWithValue("source_ip", record.SourceIp);
            cmd.Parameters.AddWithValue("dest_ip", record.DestIp);
            cmd.Parameters.AddWithValue("source_port", record.SourcePort);
            cmd.Parameters.AddWithValue("dest_port", record.DestPort);
            cmd.Parameters.AddWithValue("stored_at",
                NpgsqlDbType.TimestampTz,
                (object?)record.StoredAt ?? DBNull.Value);
            cmd.Parameters.AddWithValue("payload", NpgsqlDbType.Jsonb, record.Payload);

            // Rows affected is 1 on a real insert, 0 when ON CONFLICT swallows a duplicate.
            inserted = await cmd.ExecuteNonQueryAsync(ct);
        }


        await using (var cmd = new NpgsqlCommand(UpsertClientStateSql, conn, tx))
        {
            cmd.Parameters.AddWithValue("client_id", record.ClientId);
            cmd.Parameters.AddWithValue("version", record.Version);
            cmd.Parameters.AddWithValue("proto", record.Proto);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        await tx.CommitAsync(ct);
        return inserted > 0;
    }

    private const string InsertLedgerSql = """
        INSERT INTO packet_ledger
            (transaction_id, client_id, version, proto, source_ip, dest_ip,
             source_port, dest_port, stored_at, payload)
        VALUES
            (@transaction_id, @client_id, @version, @proto, @source_ip, @dest_ip,
             @source_port, @dest_port, @stored_at, @payload)
        ON CONFLICT (transaction_id) DO NOTHING;
        """;

    private const string UpsertClientStateSql = """
        INSERT INTO client_state (client_id, version, last_proto, updated_at)
        VALUES (@client_id, @version, @proto, now())
        ON CONFLICT (client_id) DO UPDATE
            SET version    = EXCLUDED.version,
                last_proto = EXCLUDED.last_proto,
                updated_at = now()
            WHERE EXCLUDED.version > client_state.version;
        """;
}
