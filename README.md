# KafkaFlowShardApp

Microservice pipeline that generates test network packets and routes them by protocol
into 5 sharded MongoDB nodes: **MySQL outbox → Kafka → MasterNode → MongoDB shards**.

## Data flow

```
 srv_pub                       MySQL          relay          Kafka          srv_sub             MasterNode            5 MongoDB ShardNodes
┌─────────┐  tx insert   ┌────────────┐  poll ┌───────┐pub ┌────────┐cons ┌──────────┐  TCP  ┌─────────────┐ filter ┌──────────────────┐
│ generate │───────────▶ │ Outbox tbl │ ────▶ │ Relay │──▶ │Snapshot│───▶ │ ConsumerRx│─────▶│ auth + route │──────▶ │ shard by protocol │
│ packets  │  (atomic)   └────────────┘ mark  └───────┘    │ Topic  │     └──────────┘payload│ by protocol  │ insert │ HTTPS/TCP/UDP/    │
└─────────┘                processed                       └────────┘          ▲   │        └─────────────┘        │ ARP/OTHER         │
                                                                               │   │ "Ok"          │ "Ok" once saved└──────────────────┘
                                                                               └───┘ commit offset ◀┘
```

1. **srv_pub** generates randomized test packets and, instead of publishing directly,
   writes each into the MySQL **`Outbox`** table inside a DB transaction (`IOutbox.AddAsync`).
   The `PublishOutboxJob` relay polls the table (concurrency-safe `FOR UPDATE SKIP LOCKED`
   reservation), publishes reserved rows to the `SnapshotTopic` Kafka topic via
   `KafkaMessagePub`, and marks them processed; `CleanupOutboxJob` deletes processed rows.
   This removes the dual-write problem: nothing is lost if Kafka is down.
2. **srv_sub** consumes the topic and forwards each packet's payload over a **TCP**
   connection to the MasterNode. It commits the Kafka offset **only** when the
   MasterNode replies `"Ok"` — the `processed → _consumer.Commit()` pattern:
   ```csharp
   var processed = await _forwarder.SendAsync(envelope.Payload, stoppingToken);
   if (processed) _consumer.Commit(result);
   ```
3. **MasterNode** (Akka.NET TCP server) authenticates the API key, **filters each
   packet by its `proto`**, and routes it to one of **5 MongoDB ShardNodes**. The
   shard inserts the document and replies `"Ok"`, which flows back to srv_sub and
   triggers the commit.

## The 5 shards (one MongoDB instance per "main package" type)

| Shard | Protocol(s)        | Host port |
|-------|--------------------|-----------|
| 1     | HTTPS / TLS / SSL  | 27018     |
| 2     | TCP                | 27019     |
| 3     | UDP                | 27020     |
| 4     | ARP                | 27021     |
| 5     | OTHER (everything else, e.g. ICMP, DNS) | 27022 |

All shards store into database `pcap`, collection `packets`.

## Projects

| Project       | Type            | Role |
|---------------|-----------------|------|
| `Shared`      | class library   | `PacketMessage`, `SnapshotMessage`, `ProtocolType`, serializer, API-key hasher |
| `kafka`       | class library   | `KafkaMessagePub`, `TopicRepository`, `Message` (Kafka producer) |
| `outbox`      | class library   | Outbox table, `Outbox`/`Relay`, publish + cleanup jobs, MySQL persistence |
| `srv_pub`     | worker          | Generates test packets → writes to MySQL outbox; relay publishes to Kafka |
| `srv_sub`     | worker          | Consumes Kafka, forwards over TCP, commits on `"Ok"` |
| `MasterNode`  | console (Akka)  | TCP server: auth → filter → route to 5 shards → insert → reply |

### Outbox notes

- EF provider is **Pomelo MySQL**; the outbox transaction uses `RepeatableRead` isolation.
- Outbox `Id` is `CHAR(36)` (a `UUID()`).
- The reservation **stored procedure** `GetDataFromTempTable` is created on startup.
- `srv_pub` runs `IOutboxInitializer.InitializeAsync` on startup (with retry) to create
  the table + procedure.

## Run it

### Option A — everything in Docker (recommended)

```bash
cd KafkaFlowShardApp
docker compose up --build
```

This starts Zookeeper + Kafka, the 5 MongoDB shard nodes, then MasterNode, srv_sub
and srv_pub. Watch the logs: srv_pub publishes, srv_sub forwards, MasterNode prints
`[shard:Https] saved ...` etc.

Inspect what landed in a shard:

```bash
docker exec -it kafkaflowshard-mongo-https mongosh --eval 'db.getSiblingDB("pcap").packets.find().limit(5)'
docker exec -it kafkaflowshard-mongo-arp   mongosh --eval 'db.getSiblingDB("pcap").packets.countDocuments()'
```

### Option B — infra in Docker, apps on the host

```bash
cd KafkaFlowShardApp
# Start only Kafka + MySQL + the 5 Mongo shards
docker compose up -d zookeeper kafka mysql mongo-https mongo-tcp mongo-udp mongo-arp mongo-other

# In separate terminals (defaults already point at localhost):
dotnet run --project MasterNode
dotnet run --project srv_sub
dotnet run --project srv_pub
```

## Configuration

All settings are in each project's `appsettings.json` and overridable via environment
variables (double-underscore for nesting, e.g. `Shards__Https`).

| Setting | Used by | Default |
|---------|---------|---------|
| `KafkaServer` | srv_pub, srv_sub | `localhost:9092` |
| `SqlConnStr` | srv_pub (outbox) | `server=localhost;port=3306;database=outbox;user=root;password=root` |
| `RetryTopic` / `DeadletterTopic` | srv_pub (kafka) | `5sdelay` / `deadletter` |
| `Topic` | srv_pub, srv_sub | `SnapshotTopic` |
| `ConsumerGroup` | srv_sub | `ConsumerGroup` |
| `MasterNode__Host` / `MasterNode__Port` | srv_sub | `localhost` / `8000` |
| `Tcp__Port` | MasterNode | `8000` |
| `ApiKeys` | MasterNode | `valid_api_key_1`, `valid_api_key_2` |
| `Shards__{Https,Tcp,Udp,Arp,Other}` | MasterNode | `mongodb://localhost:2701{8..22}` |
| `Publisher__BatchSize` / `Publisher__IntervalMs` | srv_pub | `5` / `2000` |
