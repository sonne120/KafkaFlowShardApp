using Dapper;
using Microsoft.EntityFrameworkCore.Storage;

namespace KafkaFlowShardApp.Outbox;

public sealed class OutboxInitializer(IUnitOfWork unitOfWork) : IOutboxInitializer
{
    private const string CreateOutboxTableQueryName = "OutboxTable.sql";
    private const string CreateReserveProcedureQueryName = "ReserveForProcessing.sql";
    private const string ReserveProcedureName = "GetDataFromTempTable";

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        var createTable = SqlQueriesReader.ReadWithCache(CreateOutboxTableQueryName);
        var createProcedure = SqlQueriesReader.ReadWithCache(CreateReserveProcedureQueryName);

        await using var transaction = await unitOfWork.BeginTransactionAsync(cancellationToken);

        var rawTransaction = transaction.GetDbTransaction();
        var rawConnection = rawTransaction.Connection;

        try
        {
            await rawConnection!.ExecuteAsync(createTable, transaction: rawTransaction);
            await rawConnection.ExecuteAsync($"DROP PROCEDURE IF EXISTS {ReserveProcedureName}", transaction: rawTransaction);
            await rawConnection.ExecuteAsync(createProcedure, transaction: rawTransaction);
            await transaction.CommitAsync(cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }
}
