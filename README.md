# KafkaFlowShardApp

Microservice pipeline that generates test network packets and routes them by protocol
into 5 sharded MongoDB nodes: **MySQL outbox → Kafka → MasterNode → MongoDB shards**.

## Data flow

```
  ┌─────────────┐  tx insert  ┌─────────────┐   relay      ┌──────────────────┐
  │   srv_pub   │ ──────────▶ │ MySQL Outbox│  publish     │      Kafka       │
  │     × 3     │             │  (durable Q)│ ───────────▶ │  SnapshotTopic   │
  │  generate   │   poll  ◀── │   table     │              │  (5 partitions)  │
  └─────────────┘             └─────────────┘              └────────┬─────────┘
                                                                    │ consume
                                                                    ▼
                                                          ┌──────────────────┐
                  ┌─────────── "Ok" → commit offset ──────│    srv_sub × 5   │
                  │                                        │  (1 per partition)│
                  ▼                                        └────────┬─────────┘
        ┌───────────────────┐         forward payload (TCP)         │
        │     MasterNode    │ ◀────────────────────────────────────┘
        │  auth · filter    │
        │  proto · route    │──┐ rejected ✗ → retry ×3 → ┌────────────┐
        └─────────┬─────────┘  └────────────────────────▶│ deadletter │
                  │ insert (proto-routed)                 └────────────┘
                  ▼
  ┌──────────────────────────── 5 MongoDB shards ───────────────────────────┐
  │   HTTPS        TCP         UDP         ARP         OTHER (DNS / ICMP…)    │
  │  :27018      :27019      :27020      :27021       :27022                  │
  └─────────────────────────────────────────────────────────────────────────┘
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

## Scaling

`srv_pub` and `srv_sub` run as multiple replicas (set in `docker-compose.yml` via
`deploy.replicas`): **3× srv_pub** and **5× srv_sub** by default.

- **srv_pub ×3** — all producers write to the same MySQL outbox; the relay reserves rows
  with `FOR UPDATE SKIP LOCKED`, so the 3 instances never double-publish.
- **srv_sub ×5** — Kafka gives **one consumer per partition per group**, so the topic is
  created with **5 partitions** (`TopicPartitions`, set in `kafka/TopicRepository.cs`) and
  each of the 5 consumers gets its own partition.

```bash
docker compose up -d --build            # replicas come from deploy.replicas
docker compose ps                       # srv_pub-1..3, srv_sub-1..5
# proof all 5 consumers are active (5 partitions across 5 CONSUMER-IDs, lag ~0):
docker exec kafkaflowshard-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group ConsumerGroup
```

## Retries & dead-letter

`srv_sub` creates the `5sdelay` (retry) and `deadletter` topics **in code** at startup
(`DeadLetterProducer.EnsureTopicsAsync`, same as the main topic). Each consumed message
resolves to one of three outcomes:

| MasterNode result | Action |
|---|---|
| replies `Ok` | commit ✓ |
| replies but **rejects** (e.g. shard write failed, malformed payload) | count an attempt → re-queue to `SnapshotTopic` (attempt header `+1`), or `deadletter` once the limit is hit; then commit |
| **unreachable** (TCP can't connect) | rewind offset + wait 2s, retry — **not** counted as an attempt |

- Attempt count travels in a Kafka header (`attempts`); the dead-lettered copy also carries
  `x-failure-reason`.
- Limit is `MaxAttempts` (default **3**) — a poison message is tried 3× then dead-lettered.
- Transient outages don't burn attempts, so a MasterNode restart won't dump good packets.

```bash
docker exec kafkaflowshard-kafka kafka-topics --bootstrap-server localhost:9092 --list
# force rejections to see it fill: stop a shard so its writes fail
docker compose stop mongo-arp
docker exec -it kafkaflowshard-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic deadletter --from-beginning
```

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

docker compose logs -f srv_pub srv_sub masternode

# In separate terminals (defaults already point at localhost):
dotnet run --project MasterNode
dotnet run --project srv_sub
dotnet run --project srv_pub
```
