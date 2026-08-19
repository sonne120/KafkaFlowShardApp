# PacketShard

> **PacketShard — protocol-sharded packet pipeline: Kafka + outbox, Akka.NET routing, at-least-once delivery (offset commit after DB write), CQRS reads (Redis, Postgres + pg_ivm).**

Microservice pipeline: packets enter over **gRPC through a load balancer**, flow through a
**MySQL outbox → Kafka → Akka.NET MasterNode → 5 MongoDB shards** write path, and are projected by
**CDC (Debezium) into a Postgres read model (pg_ivm)** — a CQRS split with the protocol shards as the
write side and Postgres as the query side. End-to-end the pipeline guarantees **at-least-once
delivery**: a Kafka offset is committed **only after** the packet has been durably written to its
shard, and the read-side projection turns at-least-once input into an **exactly-once** result.

## Architecture

The diagram below illustrates the complete data flow, from packet ingress to the read model.
Two invariants hold at every hop:

- **Durability before acknowledgment** — a stage never confirms a packet until the next durable
  store (MySQL outbox, Kafka topic, MongoDB shard, Postgres ledger) has accepted it.
- **At-least-once delivery** — `srv_sub` commits its Kafka offset **only after** the MasterNode
  confirms the shard write with `"Ok"` (**offset commit after DB write**). A crash at any point
  causes redelivery, never loss.

Key stages:

- **gRPC ingress**: clients stream packets over gRPC (HTTP/2) to a YARP load balancer, which
  round-robins them across `srv_ingest` instances. Each instance writes the packet to the MySQL
  outbox inside a transaction — durable before the client call returns.
- **Outbox relay**: `srv_pub` polls the outbox with `FOR UPDATE SKIP LOCKED` and publishes to
  Kafka, eliminating the dual-write problem: nothing is lost if Kafka is down.
- **Akka.NET routing**: the MasterNode is an Akka.NET actor system that authenticates, filters by
  protocol, and routes each packet to one of 5 MongoDB shard nodes (see
  [Akka.NET routing](#akkanet-routing-inside-the-masternode)).
- **CQRS read side**: Debezium captures changes from the MongoDB shards and streams them via
  Kafka to `srv_read`, which projects the data into a Postgres read model (pg_ivm), with Redis as
  a fast-path duplicate filter.

```
┌────────────────────────┐
│ PacketGeneratorConsole │
│ PacketGeneratorClient  │
└────────────────────────┘
             │ gRPC (HTTP/2)
             ▼
┌────────────────────────┐
│ LoadBalancer (YARP)    │
│ :5001                  │
└────────────────────────┘
             │ round-robin
             ▼
┌────────────────────────┐
│ srv_ingest × 3         │
│ (gRPC, write)          │
└────────────────────────┘
             │ tx insert (durable before ack)
             ▼
┌────────────────────────┐   poll (SKIP LOCKED)   ┌───────────┐
│     MySQL Outbox       │ ◀───────────────────── │  srv_pub  │
│     (durable Q)        │                        │  (relay)  │
└────────────────────────┘                        └───────────┘
             │ publish (no dual-write)
             ▼
┌────────────────────────┐
│  Kafka SnapshotTopic   │
│     (5 partitions)     │
└────────────────────────┘
             │ consume
             ▼
┌────────────────────────┐
│ srv_sub × 5            │ ─── commit offset ◀── ONLY on "Ok"
│ (1 per partition)      │      (at-least-once: commit AFTER DB write)
└────────────────────────┘
             │ forward payload (TCP)        ▲
             ▼                              │ "Ok" after shard insert
┌────────────────────────┐                  │
│  MasterNode (Akka.NET) │ ─────────────────┘
│  auth · filter · route │──▶ rejected ✗ → retry ×3 → deadletter
└────────────────────────┘
             │ insert (proto-routed)
             ▼
┌────────────────────────────────────┐
│    5 MongoDB shards (write side)   │
│ HTTPS │ TCP │ UDP │ ARP │ OTHER    │
│ :27018 – :27022                    │
└────────────────────────────────────┘
             │ change streams (CDC)
             ▼
┌────────────────────────┐
│      Debezium × 5      │
│     (Kafka Connect)    │
└────────────────────────┘
             │ pcap.* topics
             ▼
┌────────────────────────┐
│        srv_read        │
│  (CDC consumer + API)  │
└────┬───────────┬───────┘
     │ check/mark│ GET /stats
     ▼           ▼
┌──────────┐ ┌───────────────────┐
│  Redis   │ │ Postgres + pg_ivm │
│(fast-path)│ │   (read model)    │
└──────────┘ └───────────────────┘
```

## Akka.NET routing (inside the MasterNode)

The MasterNode is not a monolithic handler — it is an **Akka.NET actor system** exposed as a TCP
server. Every inbound connection and every shard destination is an actor, which gives the routing
stage three properties for free:

- **Isolation** — a malformed packet or a failing shard crashes one actor, not the process; the
  supervision strategy restarts it while the rest of the pipeline keeps flowing.
- **Lock-free concurrency** — actors process one message at a time from their mailbox, so the
  auth → filter → route sequence needs no shared-state locking even with 5 `srv_sub` replicas
  pushing packets concurrently.
- **Explicit backpressure point** — the `"Ok"` reply is generated only after the shard actor’s
  insert succeeds, which is exactly the signal `srv_sub` waits for before committing its offset.

Message flow through the actor system:

```
        TCP (from srv_sub × 5)
             │
             ▼
┌──────────────────────────┐
│   Akka.IO TCP listener   │  accepts connections,
│      (server actor)      │  one handler per socket
└────────────┬─────────────┘
             │ Received(payload)
             ▼
┌──────────────────────────┐
│    Connection handler    │  deserialize PacketMessage,
│    (per-connection)      │  authenticate API key (hash)
└────────────┬─────────────┘
             │ authenticated ✓        ✗ auth fail → reject
             ▼
┌──────────────────────────┐
│      Protocol filter     │  inspect packet `proto`,
│         + router         │  pick target shard
└────────────┬─────────────┘
             │ route by protocol
   ┌─────┬───┴──┬──────┬───────┐
   ▼     ▼      ▼      ▼       ▼
┌─────┐┌─────┐┌─────┐┌─────┐┌───────┐
│HTTPS││ TCP ││ UDP ││ ARP ││ OTHER │   5 ShardNode actors —
│shard││shard││shard││shard││ shard │   each owns one MongoDB
└──┬──┘└──┬──┘└──┬──┘└──┬──┘└───┬───┘   connection, inserts doc
   └──────┴──────┴──────┴───────┘
             │ insert OK
             ▼
      reply "Ok" ──▶ back to srv_sub ──▶ _consumer.Commit(result)
```

The reply path is the heart of the delivery guarantee: **“Ok” flows backwards from the shard
insert to the Kafka commit**, so the offset moves only when the data is already on disk in Mongo.

## At-least-once delivery (offset commit after DB write)

Two mechanisms combine into an end-to-end at-least-once guarantee, one on each side of Kafka:

**Producer side — transactional outbox.** `srv_ingest` never talks to Kafka directly. It writes
each packet into the MySQL `Outbox` table **inside the same DB transaction** as its business write
(`IOutbox.AddAsync`). The `PublishOutboxJob` relay then polls the table (concurrency-safe
`FOR UPDATE SKIP LOCKED` reservation), publishes reserved rows to the `SnapshotTopic` Kafka topic
via `KafkaMessagePub`, and marks them processed; `CleanupOutboxJob` deletes processed rows. This
removes the dual-write problem: if Kafka is down, packets simply wait in MySQL.

**Consumer side — commit after DB write.** `srv_sub` consumes the topic and forwards each
packet’s payload over a **TCP** connection to the MasterNode. It commits the Kafka offset **only**
when the MasterNode replies `"Ok"` — i.e. only after the packet is durably inserted into its
MongoDB shard. This is the `processed → _consumer.Commit()` pattern:

```csharp
var processed = await _forwarder.SendAsync(envelope.Payload, stoppingToken);
if (processed) _consumer.Commit(result);
```

Failure analysis — what happens when a component dies mid-flight:

|Crash point                              |Outcome                                                             |
|-----------------------------------------|--------------------------------------------------------------------|
|after outbox insert, before Kafka publish|relay re-reserves the row on restart → published later, nothing lost|
|after Kafka publish, before offset commit|packet redelivered to `srv_sub` → forwarded again (at-least-once)   |
|after shard insert, before offset commit |packet redelivered → duplicate insert absorbed by read-side dedup   |

Duplicates are therefore possible by design — and that is exactly why the read side makes its
projection idempotent (see
[Correctness: at-least-once in, exactly-once projected](#correctness-at-least-once-in-exactly-once-projected)).

## Projects

- **`PacketGeneratorConsole` / `PacketGeneratorClient`**: gRPC clients that generate randomized
  test packets and stream them into the system.
- **`LoadBalancer`**: YARP-based reverse proxy that round-robins gRPC (HTTP/2) traffic across the
  ingest services.
- **`srv_ingest`**: gRPC service that receives packets and writes them to the MySQL outbox inside
  a DB transaction.
- **`srv_pub`**: worker that relays packets from the MySQL outbox to Kafka (`FOR UPDATE SKIP LOCKED` reservation, publish, mark processed, cleanup).
- **`srv_sub`**: worker that consumes Kafka, forwards packets over TCP to the MasterNode, and
  commits the offset only on `"Ok"` (at-least-once).
- **`MasterNode`**: Akka.NET TCP server — actor pipeline that authenticates the API key, filters
  each packet by `proto`, and routes it to the matching MongoDB shard.
- **`srv_read`**: CQRS read-side service. Consumes CDC events from Debezium/Kafka, projects them
  into a Postgres read model (pg_ivm), uses Redis as a fast-path duplicate filter, and exposes a
  read API (`GET /stats/*`).
- **Shared libraries**: `Shared` (`PacketMessage`, `SnapshotMessage`, `ProtocolType`, serializer,
  API-key hasher), `kafka` (`KafkaMessagePub`, `TopicRepository`, producer `Message`), and
  `outbox` (outbox table, `Outbox`/`Relay`, publish + cleanup jobs, MySQL persistence).

## The 5 shards (one MongoDB instance per “main package” type)

|Shard|Protocol(s)                            |Host port|
|-----|---------------------------------------|---------|
|1    |HTTPS / TLS / SSL                      |27018    |
|2    |TCP                                    |27019    |
|3    |UDP                                    |27020    |
|4    |ARP                                    |27021    |
|5    |OTHER (everything else, e.g. ICMP, DNS)|27022    |

All shards store into database `pcap`, collection `packets`.

### Outbox notes

- EF provider is **Pomelo MySQL**; the outbox transaction uses `RepeatableRead` isolation.
- Outbox `Id` is `CHAR(36)` (a `UUID()`).
- The reservation **stored procedure** `GetDataFromTempTable` is created on startup.
- `srv_ingest` runs `IOutboxInitializer.InitializeAsync` on startup (with retry) to create the
  table + procedure.

## CQRS read side — CDC → Postgres (pg_ivm) + Redis

The pipeline above is the write side. The read side adds an analytics/query model without touching
it: Debezium captures what actually landed in the Mongo shards (post-routing truth) and streams it
to a dedicated microservice that projects it into Postgres, where a pg_ivm incrementally
maintained view keeps per-protocol summaries live.

```
┌────────────────────────────┐
│   5 Mongo shards (rs0)     │
│   HTTPS|TCP|UDP|ARP|OTHER  │
└────────────────────────────┘
             │ change streams (CDC)
             ▼
┌────────────────────────────┐
│      Debezium × 5          │
│     (MongoDB connector)    │
└────────────────────────────┘
             │ pcap.<shard>.packets
             ▼
┌────────────────────────────┐
│         Kafka              │
│     (pcap.* topics)        │
└────────────────────────────┘
             │ consume (at-least-once in)
             ▼
┌────────────────────────────┐    INSERT ON CONFLICT ┌───────────────────┐
│        srv_read            │ ────────────────────▶ │ Postgres + pg_ivm │
│   (CDC consumer + API)     │  ② commit first       │   packet_ledger   │
└──────────┬─────────────────┘                       │  UNIQUE(tx_id)    │
           │                                         │  packet_stats_by_ │
           │ ① check   ③ mark (after commit)         │  proto (IMMV)     │
           ▼                                         └───────────────────┘
┌────────────────────────────┐                              ▲ SELECT
│       Redis                │                              │ (no agg)
│    (fast-path filter)      │ ◀────────────────────────────┘
└────────────────────────────┘           web client (GET /stats/*)
                                  ④ Kafka offset commit — always last
```

Why CDC and not just a new consumer group on SnapshotTopic? Because the shards hold the
post-routing truth — what survived auth → filter → routing. Rejected and dead-lettered packets
never reach the shards, so reading the shards (via CDC) summarizes what was actually stored, not
what was merely published.

### Correctness: at-least-once in, exactly-once projected

Kafka is at-least-once (by design — see
[At-least-once delivery](#at-least-once-delivery-offset-commit-after-db-write)), so the projection
is made idempotent. `srv_read` processes each event in a deliberate, crash-safe order
(`srv_read/CdcConsumer.cs`):

1. **Redis fast-path** — a read-only `EXISTS rm:tx:<id>` check skips known duplicates before they
   cost a Postgres round-trip. Redis is a filter, never the source of truth.
1. **Postgres commit** — one transaction applies both guards:
- dedup: `INSERT … ON CONFLICT (transaction_id) DO NOTHING` (permanent, not TTL-bound);
- ordering: `client_state` upsert with `WHERE EXCLUDED.version > client_state.version` (drops
  “hello from the past” for last-value views; inert for commutative counts).
1. **Redis mark** — `SET rm:tx:<id>` happens **only after** the commit.
1. **Kafka ack** — commit the offset last.

A crash between steps 2 and 3/4 is safe: on redelivery Redis still says “not seen”, the re-INSERT
hits `ON CONFLICT DO NOTHING`, and nothing is lost or doubled. This is the **Postgres-first**
order — the only crash-safe one. Note the symmetry with the write side: both `srv_sub` and
`srv_read` follow the same principle — **persist first, acknowledge (commit the offset) last**.

The per-protocol summary is a pg_ivm **IMMV** (`postgres/init.sql`): `count`/`min`/`max` are
commutative, so a trigger maintains them on every INSERT — no `REFRESH`, no query-time
aggregation.

## Scaling

Every stateless stage scales horizontally; coordination is delegated to the datastore or to Kafka
instead of app-level locks:

- **srv_ingest ×3 behind YARP** — ingest instances are stateless; the load balancer round-robins
  gRPC streams, and durability lives in the shared MySQL outbox.
- **srv_pub ×3** — all producers write to the same MySQL outbox; the relay reserves rows with
  `FOR UPDATE SKIP LOCKED`, so the 3 instances never double-publish.
- **srv_sub ×5** — Kafka gives **one consumer per partition per group**, so the topic is created
  with **5 partitions** (`TopicPartitions`, set in `kafka/TopicRepository.cs`) and each of the 5
  consumers gets its own partition — parallelism without rebalancing churn.
- **MasterNode (Akka.NET)** — concurrency inside a single process comes from the actor model: one
  handler actor per connection, one shard actor per MongoDB node, each with its own mailbox.

Replica counts are set in `docker-compose.yml` via `deploy.replicas`.

```
docker compose up -d --build            # replicas come from deploy.replicas
docker compose ps                       # srv_pub-1..3, srv_sub-1..5
# proof all 5 consumers are active (5 partitions across 5 CONSUMER-IDs, lag ~0):
docker exec kafkaflowshard-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group ConsumerGroup
```

## Retries & dead-letter

`srv_sub` creates the `5sdelay` (retry) and `deadletter` topics **in code** at startup
(`DeadLetterProducer.EnsureTopicsAsync`, same as the main topic). Each consumed message resolves
to one of three outcomes:

|MasterNode result                                                   |Action                                                                                                                  |
|--------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
|replies `Ok`                                                        |commit ✓                                                                                                                |
|replies but **rejects** (e.g. shard write failed, malformed payload)|count an attempt → re-queue to `SnapshotTopic` (attempt header `+1`), or `deadletter` once the limit is hit; then commit|
|**unreachable** (TCP can’t connect)                                 |rewind offset + wait 2s, retry — **not** counted as an attempt                                                          |

- Attempt count travels in a Kafka header (`attempts`); the dead-lettered copy also carries
  `x-failure-reason`.
- Limit is `MaxAttempts` (default **3**) — a poison message is tried 3× then dead-lettered.
- Transient outages don’t burn attempts, so a MasterNode restart won’t dump good packets. This
  distinction matters for the delivery guarantee: an unreachable MasterNode is a *transport*
  failure (rewind and wait — the packet is still good), while an explicit rejection is a
  *processing* failure (count it, and quarantine the packet after 3 strikes).

```
docker exec kafkaflowshard-kafka kafka-topics --bootstrap-server localhost:9092 --list
# force rejections to see it fill: stop a shard so its writes fail
docker compose stop mongo-arp
docker exec -it kafkaflowshard-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic deadletter --from-beginning
```

## Run it

### Option A — everything in Docker (recommended)

```
cd PacketShard
docker compose up --build
```

This starts Zookeeper + Kafka, MySQL, the 5 MongoDB shard nodes, then MasterNode, srv_sub,
srv_pub, the ingress tier (LoadBalancer + srv_ingest) and the read side (Debezium, Postgres,
Redis, srv_read). Watch the logs: srv_pub publishes, srv_sub forwards, MasterNode prints
`[shard:Https] saved ...` etc.

Inspect what landed in a shard:

```
docker exec -it kafkaflowshard-mongo-https mongosh --eval 'db.getSiblingDB("pcap").packets.find().limit(5)'
docker exec -it kafkaflowshard-mongo-arp   mongosh --eval 'db.getSiblingDB("pcap").packets.countDocuments()'
```

### Option B — infra in Docker, apps on the host

```
cd PacketShard
# Start only Kafka + MySQL + the 5 Mongo shards
docker compose up -d zookeeper kafka mysql mongo-https mongo-tcp mongo-udp mongo-arp mongo-other

docker compose logs -f srv_pub srv_sub masternode

# In separate terminals (defaults already point at localhost):
dotnet run --project MasterNode
dotnet run --project srv_sub
dotnet run --project srv_pub
```

## Live pipeline

[![PacketShard live logs](https://github.com/sonne120/PacketShard/raw/main/assets/terminal.png)](/sonne120/PacketShard/blob/main/assets/terminal.png)

Interleaved output of `docker compose logs -f srv_pub srv_sub masternode`: the five `srv_sub`
replicas (`srv_sub-1..5`) each forward packets and get `MasterNode response: Ok`, while
`masternode` routes them to the protocol shards — `[shard:Other] saved DNS …`,
`[shard:Arp] saved ARP …`, etc.
