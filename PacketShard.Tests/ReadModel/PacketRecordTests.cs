using PacketShard.Read;
using Xunit;

namespace PacketShard.Tests.ReadModel;


[Trait("Category", "Unit")]
public sealed class PacketRecordTests
{
    [Fact]
    public void Flattened_debezium_event_maps_every_indexed_field()
    {
        var record = PacketRecord.TryParse("""
            {
              "__op": "c",
              "transaction_id": "tx-1",
              "client_id": "c1",
              "version": 42,
              "proto": "HTTPS",
              "source_ip": "10.0.0.1",
              "dest_ip": "10.0.0.2",
              "source_port": 51000,
              "dest_port": 443,
              "storedAt": 1767268800000
            }
            """);

        Assert.NotNull(record);
        Assert.Equal("tx-1", record!.TransactionId);
        Assert.Equal("c1", record.ClientId);
        Assert.Equal(42, record.Version);
        Assert.Equal("HTTPS", record.Proto);
        Assert.Equal("10.0.0.1", record.SourceIp);
        Assert.Equal("10.0.0.2", record.DestIp);
        Assert.Equal(51000, record.SourcePort);
        Assert.Equal(443, record.DestPort);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1767268800000), record.StoredAt);
    }

    //nothing to project
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Tombstones_are_skipped(string? value)
    {
        Assert.Null(PacketRecord.TryParse(value));
    }

    [Theory]
    [InlineData("not json at all")]
    [InlineData("{ unterminated")]
    [InlineData("[1, 2, 3]")]       // valid JSON but not a document
    public void Malformed_values_are_skipped_rather_than_thrown(string value)
    {
        Assert.Null(PacketRecord.TryParse(value));
    }

    [Fact]
    public void Delete_events_are_skipped()
    {
        Assert.Null(PacketRecord.TryParse("""{"__op":"d","transaction_id":"tx-1"}"""));
    }

    [Theory]
    [InlineData("""{"__op":"c","client_id":"c1"}""")]                    // absent
    [InlineData("""{"__op":"c","transaction_id":""}""")]                 // empty
    [InlineData("""{"__op":"c","transaction_id":"   "}""")]              // blank
    [InlineData("""{"__op":"c","transaction_id":null}""")]               // explicit null
    public void Records_without_a_business_key_are_skipped(string value)
    {
        Assert.Null(PacketRecord.TryParse(value));
    }

    //defaults
    [Fact]
    public void Missing_optional_fields_fall_back_to_safe_defaults()
    {
        var record = PacketRecord.TryParse("""{"transaction_id":"tx-1"}""");

        Assert.NotNull(record);
        Assert.Equal(string.Empty, record!.ClientId);
        Assert.Equal(0, record.Version);
        Assert.Equal("OTHER", record.Proto);
        Assert.Equal(string.Empty, record.SourceIp);
        Assert.Equal(string.Empty, record.DestIp);
        Assert.Equal(0, record.SourcePort);
        Assert.Equal(0, record.DestPort);
        Assert.Null(record.StoredAt);
    }

    [Fact]
    public void An_event_with_no_op_field_is_still_projected()
    {
        // Only "d" is filtered. A snapshot read ("r") or an update ("u") is a row like any other.
        Assert.NotNull(PacketRecord.TryParse("""{"transaction_id":"tx-1"}"""));
        Assert.NotNull(PacketRecord.TryParse("""{"__op":"r","transaction_id":"tx-1"}"""));
        Assert.NotNull(PacketRecord.TryParse("""{"__op":"u","transaction_id":"tx-1"}"""));
    }

    //storedAt

    [Fact]
    public void StoredAt_accepts_epoch_millis()
    {
        var record = PacketRecord.TryParse("""{"transaction_id":"tx-1","storedAt":1767268800000}""");

        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1767268800000), record!.StoredAt);
    }

    [Fact]
    public void StoredAt_accepts_an_iso_string()
    {
        var record = PacketRecord.TryParse("""{"transaction_id":"tx-1","storedAt":"2026-01-01T12:00:00Z"}""");

        Assert.Equal(
            new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero),
            record!.StoredAt!.Value.ToUniversalTime());
    }

    [Theory]
    [InlineData("2026-01-01T12:00:00Z", "2026-01-01T12:00:00Z")]
    [InlineData("2026-01-01T12:00:00+05:00", "2026-01-01T07:00:00Z")]
    [InlineData("2026-01-01T12:00:00", "2026-01-01T12:00:00Z")]   // no offset: Debezium emits UTC
    public void StoredAt_is_independent_of_the_hosts_timezone(string storedAt, string expectedUtc)
    {
        // Newtonsoft hands this back as a DateTime, which is then converted to a DateTimeOffset by 
        // the record. The test runs in whatever timezone the host is in, so we assert against UTC.
        var record = PacketRecord.TryParse($$"""{"transaction_id":"tx-1","storedAt":"{{storedAt}}"}""");

        Assert.Equal(DateTimeOffset.Parse(expectedUtc), record!.StoredAt!.Value.ToUniversalTime());
    }

    [Theory]
    [InlineData("""{"transaction_id":"tx-1","storedAt":null}""")]
    [InlineData("""{"transaction_id":"tx-1","storedAt":"not a date"}""")]
    public void An_unusable_storedAt_becomes_null_rather_than_failing_the_record(string value)
    {
        var record = PacketRecord.TryParse(value);

        Assert.NotNull(record);
        Assert.Null(record!.StoredAt);
    }

    //payload

    [Fact]
    public void Payload_keeps_the_whole_document_including_fields_no_column_holds()
    {
        var record = PacketRecord.TryParse("""
            {"_id":"abc","transaction_id":"tx-1","custom_field":{"nested":true}}
            """);

        Assert.Contains("\"_id\":\"abc\"", record!.Payload);
        Assert.Contains("\"custom_field\"", record.Payload);
        Assert.Contains("\"nested\":true", record.Payload);
    }

    [Fact]
    public void Payload_is_compact_json_so_it_lands_as_jsonb_unchanged()
    {
        var record = PacketRecord.TryParse("""
            {
                "transaction_id" : "tx-1"
            }
            """);

        Assert.Equal("""{"transaction_id":"tx-1"}""", record!.Payload);
    }
}
