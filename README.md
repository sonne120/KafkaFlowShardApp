# PacketShard

> **PacketShard — protocol-sharded packet pipeline: Kafka + outbox, Akka.NET routing, at-least-once delivery (offset commit after DB write).**

> **Read side: Debezium CDC streams shard writes back through Kafka into an idempotent Postgres projection (pg_ivm live stats), with Redis fast-path dedup — at-least-once in, exactly-once projected.**

> **High availability (optional): the MySQL outbox tier runs as a semi-synchronous primary/replica pair behind ProxySQL, so an acknowledged packet already exists on two nodes before the client call returns — switched on with one line in `.env`.**

> **Service discovery: services register themselves with Consul and the gateway resolves its backends from the catalog, so scaling a tier is a Compose change and a failed instance leaves the rotation on its own health check.**

Microservice pipeline: packets enter over **gRPC through a load balancer**, flow through a
**MySQL outbox → Kafka → Akka.NET MasterNode → 5 MongoDB shards** write path, and are projected by
**CDC (Debezium) into a Postgres read model (pg_ivm)** — a CQRS split with the protocol shards as the
write side and Postgres as the query side. End-to-end the pipeline guarantees **at-least-once
delivery**: a Kafka offset is committed **only after** the packet has been durably written to its
shard, and the read-side projection turns at-least-once input into an **exactly-once** result.

**Contents:**
[Architecture](#architecture) ·
[Akka.NET routing](#akkanet-routing-inside-the-masternode) ·
[At-least-once delivery](#at-least-once-delivery-offset-commit-after-db-write) ·
[Projects](#projects) ·
[CQRS read side](#cqrs-read-side--cdc--postgres-pg_ivm--redis) ·
[Scaling](#scaling) ·
[Service discovery](#service-discovery-consul) ·
[High availability](#high-availability-optional) ·
[Retries & dead-letter](#retries--dead-letter) ·
[Run it](#run-it) ·
[Tests](#tests)

## Architecture

The diagram below illustrates the complete data flow, from packet ingress to the read model.
Two invariants hold at every hop:

- **Durability before acknowledgment** — a stage never confirms a packet until the next durable
  store (MySQL outbox, Kafka topic, MongoDB shard, Postgres ledger) has accepted it.
- **At-least-once delivery** — `srv_sub` commits its Kafka offset **only after** the MasterNode
  confirms the shard write with `"Ok"` (**offset commit after DB write**). A crash at any point
  causes redelivery, never loss.

```
┌────────────────────────┐
│ PacketGeneratorConsole │
│ PacketGeneratorClient  │
└────────────────────────┘
             │ gRPC (HTTP/2)
             ▼
┌────────────────────────┐  "which are healthy?" ┌────────────────────────┐
│ LoadBalancer (YARP)    │ ────────────────────▶ │   Consul  :8500        │
│ :5001 gRPC · :5002 REST│ ◀──────────────────── │  catalog + health      │
└────────────────────────┘     instance list     └────────────────────────┘
             │ round-robin                                   ▲
             ▼                                               │ register
┌────────────────────────┐                                   │ + pass /health
│ srv_ingest × 3         │ ──────────────────────────────────┘
│ (gRPC, write)          │
└────────────────────────┘
             │ tx insert (durable before ack)
             ▼
┌────────────────────────┐   poll (SKIP LOCKED)   ┌───────────┐
│     MySQL Outbox       │ ◀───────────────────── │  srv_pub  │
│     (durable Q)        │                        │  (relay)  │
└────────────────────────┘                        └───────────┘
    ▲ one node by default; a primary/replica pair behind ProxySQL
      when the HA overlay is on — see "High availability" below
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
│  5 MongoDB shards (write side, rs0)│
│ HTTPS │ TCP │ UDP │ ARP │ OTHER    │
│ :27018 – :27022                    │
└────────────────────────────────────┘
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

Key stages:

- **gRPC ingress**: clients stream packets over gRPC (HTTP/2) to a YARP load balancer, which
  round-robins them across `srv_ingest` instances. Each instance writes the packet to the MySQL
  outbox inside a transaction — durable before the client call returns. The gateway learns which
  instances exist from Consul rather than from a list it has to be told about (see
  [Service discovery](#service-discovery-consul)).
- **Outbox relay**: `srv_pub` polls the outbox with `FOR UPDATE SKIP LOCKED` and publishes to
  Kafka, eliminating the dual-write problem: nothing is lost if Kafka is down.
- **Akka.NET routing**: the MasterNode is an Akka.NET actor system that authenticates, filters by
  protocol, and routes each packet to one of 5 MongoDB shard nodes (see
  [Akka.NET routing](#akkanet-routing-inside-the-masternode)).
- **CQRS read side**: Debezium captures changes from the MongoDB shards and streams them via
  Kafka to `srv_read`, which projects the data into a Postgres read model (pg_ivm), with Redis as
  a fast-path duplicate filter.

## Akka.NET routing (inside the MasterNode)

<details>
<summary><b>Why an actor system</b> — isolation, lock-free concurrency, and an explicit backpressure point</summary>

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

</details>

<details>
<summary><b>Message flow</b> — TCP listener → connection handler → filter → 5 shard actors, and the “Ok” back</summary>

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

</details>

<details>
<summary><b>When something fails</b> — which failures are reported, which are retried, and which burn an attempt</summary>

The routing stage distinguishes a packet that *cannot* be stored from infrastructure that is
*momentarily* unavailable. Confusing the two either quarantines good packets or retries a poison
one forever, so each failure has a defined answer:

|What fails                       |What the MasterNode does                                   |What `srv_sub` does                                          |
|---------------------------------|-----------------------------------------------------------|-------------------------------------------------------------|
|A shard's MongoDB node is down   |`ShardWriterActor` catches the write error and replies `Write failed: …` — the actor stays alive|treats it as a rejection: counts an attempt, re-queues, dead-letters after 3|
|Invalid API key                  |replies `Invalid API Key` and closes the connection        |the connection drops; nothing is routed                       |
|Malformed payload                |`proto` cannot be parsed, so it routes to the `OTHER` shard rather than dropping the packet|normal path — it is stored, not lost                          |
|A connection handler actor crashes|only that socket's actor dies; the listener and the five shard actors are untouched|reconnects on the next message                                |
|The whole MasterNode is down     |nothing to reply                                            |`SendAsync` returns `null` — rewind the offset, wait 2s, retry. **Not** counted as an attempt|

That last row is the important asymmetry. An unreachable MasterNode is a *transport* failure and
the packet is still good, so retrying forever is correct. An explicit rejection is a *processing*
failure, so it is counted and eventually quarantined. See
[Retries & dead-letter](#retries--dead-letter).

Two details worth stating precisely, because the actor model is often assumed to do more than it
does here:

- **The shard writers do not actually crash on a failed write.** `ShardWriterActor` catches the
  exception and answers with an error string, so no supervision restart occurs on the Mongo path —
  isolation shows up as *the other four shards keep working*, not as a restart. A shard that is
  down produces rejections, not an outage.
- **There is no custom `SupervisorStrategy`.** Anything that does throw gets Akka's default
  one-for-one restart, which is the right behaviour for a per-connection handler and is why one
  bad socket cannot take the listener with it.

The write path's own database tier has a separate failover story — see
[High availability](#high-availability-optional).

</details>

## At-least-once delivery (offset commit after DB write)

<details>
<summary><b>How the guarantee is built</b> — a transactional outbox on the producer side, commit-after-write on the consumer</summary>

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

</details>

<details>
<summary><b>Failure analysis</b> — what happens when a component dies mid-flight</summary>

|Crash point                              |Outcome                                                             |
|-----------------------------------------|--------------------------------------------------------------------|
|after outbox insert, before Kafka publish|relay re-reserves the row on restart → published later, nothing lost|
|after Kafka publish, before offset commit|packet redelivered to `srv_sub` → forwarded again (at-least-once)   |
|after shard insert, before offset commit |packet redelivered → duplicate insert absorbed by read-side dedup   |
|after Postgres commit, before Redis mark |redelivery re-INSERTs → `ON CONFLICT DO NOTHING` absorbs it, no double count|

Every row above is an executable test rather than a claim — see [Tests](#tests).

</details>

<details>
<summary><b>Why duplicates are expected</b> — and why that makes the read side idempotent</summary>

Duplicates are therefore possible by design — and that is exactly why the read side makes its
projection idempotent (see
[CQRS read side](#cqrs-read-side--cdc--postgres-pg_ivm--redis)).

</details>

## Projects

<details>
<summary><b>Project map</b> — what each service and library does</summary>

- **`PacketGeneratorConsole` / `PacketGeneratorClient`**: gRPC clients that generate randomized
  test packets and stream them into the system.
- **`LoadBalancer`**: YARP-based API gateway — round-robins gRPC (HTTP/2) traffic across the
  ingest services and REST traffic to `srv_read`, with JWT auth, per-caller rate limiting and
  optional TLS. Its destinations are written as `consul://<service>` and resolved from the
  catalog at runtime.
- **`srv_ingest`**: gRPC service that receives packets and writes them to the MySQL outbox inside
  a DB transaction.
- **`srv_pub`**: worker that relays packets from the MySQL outbox to Kafka (`FOR UPDATE SKIP LOCKED` reservation, publish, mark processed, cleanup).
- **`srv_sub`**: worker that consumes Kafka, forwards packets over TCP to the MasterNode, and
  commits the offset only on `"Ok"` (at-least-once).
- **`MasterNode`**: Akka.NET TCP server — actor pipeline that authenticates the API key, filters
  each packet by `proto`, and routes it to the matching MongoDB shard.
- **`srv_read`**: CQRS read-side service. `CdcConsumer` consumes CDC events from Debezium/Kafka
  and hands each to `ProjectionHandler`, which projects it into a Postgres read model (pg_ivm)
  using Redis as a fast-path duplicate filter. Exposes a read API (`GET /stats/*`).
- **Shared libraries**: `Shared` (`PacketMessage`, `SnapshotMessage`, `ProtocolType`, serializer,
  API-key hasher), `kafka` (`KafkaMessagePub`, `TopicRepository`, producer `Message`),
  `outbox` (outbox table, `Outbox`/`Relay`, publish + cleanup jobs, MySQL persistence), and
  `consul` (`IServiceDirectory` with a Consul-backed and a static implementation, plus the
  hosted service that registers a process and deregisters it on shutdown).
- **`PacketShard.Tests`**: one test project covering all of the above, split into a fast in-process
  lane and a Testcontainers-backed lane — see [Tests](#tests).

</details>

<details>
<summary><b>The 5 shards</b> — one MongoDB instance per “main package” type</summary>

|Shard|Protocol(s)                            |Host port|
|-----|---------------------------------------|---------|
|1    |HTTPS / TLS / SSL                      |27018    |
|2    |TCP                                    |27019    |
|3    |UDP                                    |27020    |
|4    |ARP                                    |27021    |
|5    |OTHER (everything else, e.g. ICMP, DNS)|27022    |

All shards store into database `pcap`, collection `packets`.
</details>

<details>
<summary><b>Outbox notes</b> — MySQL implementation details</summary>

- EF provider is **Pomelo MySQL**; the outbox transaction uses `RepeatableRead` isolation.
- Outbox `Id` is `CHAR(36)` (a `UUID()`).
- The reservation **stored procedure** `GetDataFromTempTable` is created on startup.
- Both `srv_ingest` and `srv_pub` run `IOutboxInitializer.InitializeAsync` on startup (with retry,
  since MySQL may still be warming up). It is idempotent, so whichever wins the race is fine.
- The connection string is read from `ConnectionStrings:Outbox`, falling back to `SqlConnStr`.
  That fallback is what lets the [HA overlay](#high-availability-optional) repoint the apps at
  ProxySQL without touching the base compose file or `appsettings.json`.

</details>

## CQRS read side — CDC → Postgres (pg_ivm) + Redis

<details>
<summary><b>Why CDC</b> — the shards hold the post-routing truth</summary>

The pipeline above is the write side. The read side adds an analytics/query model without touching
it: Debezium captures what actually landed in the Mongo shards (post-routing truth) and streams it
to a dedicated microservice that projects it into Postgres, where a pg_ivm incrementally
maintained view keeps per-protocol summaries live. The full read-side flow — shards → Debezium →
Kafka `pcap.*` topics → `srv_read` → Postgres/Redis, with the numbered ①–④ processing order — is
shown in the tail of the [main architecture diagram](#architecture) above.

Why CDC and not just a new consumer group on SnapshotTopic? Because the shards hold the
post-routing truth — what survived auth → filter → routing. Rejected and dead-lettered packets
never reach the shards, so reading the shards (via CDC) summarizes what was actually stored, not
what was merely published.

</details>

<details>
<summary><b>Correctness</b> — at-least-once in, exactly-once projected</summary>

Kafka is at-least-once (by design — see
[At-least-once delivery](#at-least-once-delivery-offset-commit-after-db-write)), so the projection
is made idempotent. `ProjectionHandler` (`srv_read/ProjectionHandler.cs`) applies a deliberate,
crash-safe order to every event; `CdcConsumer` around it is only Kafka plumbing — subscription,
the consume loop, and when to commit:

1. **Redis fast-path** — a read-only `EXISTS rm:tx:<id>` check skips known duplicates before they
   cost a Postgres round-trip. Redis is a filter, never the source of truth.
2. **Postgres commit** — one transaction applies both guards:
   - dedup: `INSERT … ON CONFLICT (transaction_id) DO NOTHING` (permanent, not TTL-bound);
   - ordering: `client_state` upsert with `WHERE EXCLUDED.version > client_state.version` (drops
     “hello from the past” for last-value views; inert for commutative counts).
3. **Redis mark** — `SET rm:tx:<id>` happens **only after** the commit.
4. **Kafka ack** — commit the offset last.

A crash between steps 2 and 3/4 is safe: on redelivery Redis still says “not seen”, the re-INSERT
hits `ON CONFLICT DO NOTHING`, and nothing is lost or doubled. This is the **Postgres-first**
order — the only crash-safe one. Reversing steps 2 and 3 would make Redis lie: the fast path would
skip a transaction Postgres never stored, and the packet would vanish without a trace. Note the
symmetry with the write side: both `srv_sub` and `srv_read` follow the same principle —
**persist first, acknowledge (commit the offset) last**.

Splitting the handler out of the consumer is what makes that order testable: it can be driven
against a real Postgres and a real Redis with no broker in the loop
(see [Tests](#tests)).

The per-protocol summary is a pg_ivm **IMMV** (`postgres/init.sql`): `count`/`min`/`max` are
commutative, so a trigger maintains them on every INSERT — no `REFRESH`, no query-time
aggregation.

</details>

## Scaling

<details>
<summary><b>How each stage scales</b> — coordination delegated to the datastore or to Kafka</summary>

Every stateless stage scales horizontally; coordination is delegated to the datastore or to Kafka
instead of app-level locks:

- **srv_ingest ×3 behind YARP** — ingest instances are stateless; the load balancer round-robins
  gRPC streams, and durability lives in the shared MySQL outbox. The replica count is not
  configured on the gateway: instances register themselves with Consul and the gateway resolves
  them, so a fourth replica joins the rotation on its own.
- **srv_pub ×3** — all producers write to the same MySQL outbox; the relay reserves rows with
  `FOR UPDATE SKIP LOCKED`, so the 3 instances never double-publish.
- **srv_sub ×5** — Kafka gives **one consumer per partition per group**, so the topic is created
  with **5 partitions** (`TopicPartitions`, set in `kafka/TopicRepository.cs`) and each of the 5
  consumers gets its own partition — parallelism without rebalancing churn.
- **MasterNode (Akka.NET)** — concurrency inside a single process comes from the actor model: one
  handler actor per connection, one shard actor per MongoDB node, each with its own mailbox.

Replica counts are set in `docker-compose.yml` via `deploy.replicas`.

</details>

<details>
<summary><b>Verify the scaling</b> — commands</summary>

```
docker compose up -d --build            # replicas come from deploy.replicas
docker compose ps                       # srv_pub-1..3, srv_sub-1..5
# proof all 5 consumers are active (5 partitions across 5 CONSUMER-IDs, lag ~0):
docker exec packetshard-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group ConsumerGroup
```

</details>

## Service discovery (Consul)

<details>
<summary><b>The problem</b> — a gateway that has to be told what exists</summary>

The gateway used to name its backends:

```json
"Destinations": {
  "ingest-1": { "Address": "http://srv_ingest-1:8080" },
  "ingest-2": { "Address": "http://srv_ingest-2:8080" },
  "ingest-3": { "Address": "http://srv_ingest-3:8080" }
}
```

That list is a second place the replica count lives, and it drifts. Scaling to four replicas means
editing gateway config and restarting it; an instance that dies keeps its slot in the rotation
until YARP's own probes notice; and an instance that is *up but not ready* — still replaying the
outbox schema, still connecting to MySQL — is indistinguishable from a healthy one.

Consul moves the list to where it is actually known: each instance publishes itself, and stops
being published when it stops passing its own health check.

</details>

<details>
<summary><b>Who registers, and how each one is checked</b></summary>

|Service       |Registers as |Port |Check                        |Why that check|
|--------------|-------------|-----|-----------------------------|--------------|
|`srv_ingest`×3|`srv-ingest` |8080 |HTTP `GET :8081/health`      |The service port is HTTP/2-only, so probes get their own HTTP/1.1 listener — see below|
|`srv_read`    |`srv-read`   |8080 |HTTP `GET :8080/health`      |Plain HTTP/1.1 service; one port is enough|
|`MasterNode`  |`masternode` |8000 |TCP connect                  |It speaks a line protocol over raw TCP, not HTTP|
|`LoadBalancer`|`gateway`    |5001 |HTTP `GET :5002/health`      |The gRPC port is HTTP/2-only; the REST port answers the probe|

Registration is deliberately **not** on the startup path. It runs in the background and retries,
the same reasoning as the outbox schema init: Consul may still be electing a leader when a service
boots, and being unregistered for a few seconds is a discovery gap, not a reason to hold up the
process. Each check starts in `critical` and only turns `passing` once it actually succeeds, so a
slow-booting instance never receives traffic it cannot serve. On a clean shutdown a service
deregisters itself; on a kill, `DeregisterCriticalServiceAfter` sweeps it up.

**Why `srv_ingest` listens twice.** Its service port carries gRPC, which is HTTP/2-only — on a
plaintext port there is no ALPN to negotiate with, so it cannot also serve HTTP/1.1. Consul's HTTP
check speaks HTTP/1.1. So the same `/health` endpoint is exposed on a second HTTP/1.1 listener
(`HealthPort`, 8081): YARP probes it over h2c on 8080, Consul over HTTP/1.1 on 8081.

</details>

<details>
<summary><b>How the gateway resolves</b> — <code>consul://</code> destinations</summary>

Only the *addresses* became dynamic. Routes, load-balancing policy, health-check policy and HTTP
version all stay in `appsettings.json`, where they are readable:

```json
"ingest-cluster": {
  "LoadBalancingPolicy": "RoundRobin",
  "HttpRequest": { "Version": "2", "VersionPolicy": "RequestVersionExact" },
  "Destinations": {
    "ingest": { "Address": "consul://srv-ingest" }
  }
}
```

`ConsulDestinationResolver` implements YARP's `IDestinationResolver`. It expands each
`consul://<service>` into one destination per healthy instance (`ingest[0]`, `ingest[1]`, …) and
returns a change token wired to a Consul **blocking query** — an idle watch is one parked
connection, not a poll loop. When the catalog moves past the index the snapshot was read at, the
token trips and YARP re-resolves. Addresses that are not `consul://` are passed through untouched,
so a cluster can mix discovered and pinned destinations.

Two details that matter in an outage:

- A lookup that **fails** returns the last known instance set rather than an empty one. An
  unreachable agent is not evidence that the backends went away, and reporting none would take the
  cluster down with the agent.
- A lookup that **succeeds with zero instances** is believed, and the service stays watched, so the
  cluster recovers by itself when instances come back.

</details>

<details>
<summary><b>The off switch</b> — the same config works with no agent</summary>

`Consul:Enabled` defaults to `false`, the same convention as the SSL and Auth toggles: the stack
still boots with no Consul in it. With discovery off, `consul://` destinations resolve from a fixed
map instead of an agent —

```json
"Consul": {
  "Enabled": "false",
  "Fallback": {
    "srv-ingest": "http://srv_ingest-1:8080,http://srv_ingest-2:8080,http://srv_ingest-3:8080",
    "srv-read": "http://srv_read:8080"
  }
}
```

— which is exactly the fixed replica list the gateway used before there was a catalog to ask.
Callers depend on `IServiceDirectory` and never learn which implementation answered.
`docker-compose.yml` sets `Consul__Enabled=true`, so the Docker stack runs with discovery on.

`srv_sub` uses the same directory to find the MasterNode: set `MasterNode__Service` and it resolves
by name, rotating to the next instance on every reconnect so the node that just dropped the
connection is not the first one tried again. Leave it unset and the configured `MasterNode__Host`
is dialled exactly as before.

</details>

<details>
<summary><b>Verify it</b> — commands</summary>

```bash
docker compose up -d --build
open http://localhost:8500                      # catalog UI

# what is registered, and what is actually passing
curl -s localhost:8500/v1/catalog/services | jq
curl -s 'localhost:8500/v1/health/service/srv-ingest?passing=true' \
  | jq -r '.[] | "\(.Service.ID)  \(.Service.Address):\(.Service.Port)"'

# the gateway logs what it resolved, and re-logs it whenever the set changes
docker compose logs -f loadbalancer | grep -E "instance\(s\) of|changed past index"
```

Add a replica and watch it land without touching gateway config or restarting anything — copy the
`srv_ingest-3` block in `docker-compose.yml`, give it its own `Consul__Service__Address` (that is
what makes it a separate catalog entry rather than an overwrite of an existing one), then:

```bash
docker compose up -d srv_ingest-4
docker compose logs loadbalancer | tail -3       # "ingest -> 4 instance(s) of srv-ingest: ..."
```

Kill an instance and the reverse happens — the check goes critical, Consul drops it from the
passing set, the blocking query returns, and the gateway re-resolves to the survivors.

</details>

<details>
<summary><b>Honest caveats</b></summary>

- **One agent, one server.** `-bootstrap-expect=1` is a dev-stack datacenter: it has no quorum, so
  the agent is a single point of failure for *discovery*. It is not on the data path — a request
  already in flight never touches Consul, and the last known instance set keeps serving traffic
  while the agent is down — but a service that starts during an outage cannot register. Production
  wants 3 or 5 servers.
- **No ACLs, no TLS, no gossip encryption.** The agent runs open inside the Compose network.
  `ConsulOptions.Token` exists for the ACL case, but nothing sets it here.
- **Consul is not a load balancer.** It answers "which instances are healthy"; YARP still decides
  where each request goes and keeps its own active/passive health checks. The two layers overlap
  on purpose — Consul catches a *dead process*, YARP catches a destination that is reachable but
  failing requests.
- **Registration is per-process, not sidecar-based.** Each service registers itself over the HTTP
  API. That keeps the Compose file simple and makes the check definition live next to the code it
  checks, at the cost of a Consul dependency inside the app.

</details>

## High availability (optional)

<details>
<summary><b>The overlay</b> — semi-sync MySQL + Orchestrator + ProxySQL, switched in one line of .env</summary>

The single `mysql` service is the write path's one hard dependency: if it is down, `srv_ingest`
cannot accept a packet at all, because the outbox insert *is* the durability guarantee. `ha/`
replaces it with a semi-synchronous primary + replica pair, [Orchestrator](https://github.com/openark/orchestrator)
for failover detection and promotion, and ProxySQL routing the apps to whichever node is currently
writable.

It ships as a compose **overlay**, so the main `docker-compose.yml` needs no edits. The mode is one
line in `.env`:

```
# HA mode: semi-sync MySQL + Orchestrator + ProxySQL
COMPOSE_FILE=docker-compose.yml:ha/docker-compose.ha.yml

# single-node mode: comment the line out
```

Everything after that is the usual `docker compose up -d --build`.

The overlay makes three couplings so the base file stays untouched:

- **The old `mysql` is parked**, not deleted — compose cannot remove a service during a merge, but
  `profiles: ["disabled"]` means nothing ever starts it. Parking alone is not enough, though:
  compose pulls a profiled service back in when an *active* service still `depends_on` it, so the
  app overrides drop that dependency with `depends_on: !override` (compose ≥ 2.24).
- **The apps wait for a writer.** `srv_ingest-1..3` and `srv_pub` gain
  `depends_on: ha-bootstrap (service_completed_successfully)`, so they never start against a node
  that is still `super_read_only`.
- **The connection string is repointed** via `ConnectionStrings__Outbox` → ProxySQL on `:6033`.
  The base file's `SqlConnStr` is still present in the merged environment and is simply outranked,
  which is what makes the toggle symmetric in both directions.

|Component     |Role                                                                 |Port|
|--------------|---------------------------------------------------------------------|----|
|`mysql-master`|semi-sync source; writable only by runtime appointment               |—   |
|`mysql-slave` |semi-sync replica, `super_read_only` until promoted                   |—   |
|`proxysql`    |routes the app to hostgroup 0 (the writer), follows `super_read_only`|6033 (app), 6032 (admin)|
|`orchestrator`|topology detection, promotion, re-parenting; web UI + API            |3000|
|`ha-bootstrap`|one-shot: appoints the initial writer, registers the topology         |—   |

</details>

<details>
<summary><b>Data flow through a failover</b> — what happens to in-flight packets when the primary dies</summary>

Steady state — every app connection goes to ProxySQL, which keeps exactly one node in the writer
hostgroup and decides which by polling `super_read_only`:

```
srv_ingest × 3 ─┐
                ├──▶ ProxySQL :6033 ──▶ hostgroup 0  ┌──────────────┐
srv_pub (relay)─┘      (writer only)    ═══════════▶ │ mysql-master │  read_only = OFF
                                                     └──────┬───────┘  (appointed)
                                        hostgroup 1         │ semi-sync AFTER_SYNC
                                        (parked, no traffic)│ commit waits for the
                                                     ┌──────▼───────┐ replica's ack
                                                     │ mysql-slave  │  super_read_only = ON
                                                     └──────────────┘
```

The source commits only after a replica has the binlog event, so an acknowledged outbox row exists
on two nodes before `srv_ingest` returns to the client — the same *durability before
acknowledgment* invariant the rest of the pipeline follows.

When the primary dies:

```
  ① mysql-master gone          ② promote                    ③ ProxySQL re-elects
┌──────────────┐            ┌──────────────┐              ┌──────────────┐
│ mysql-master │  ✗         │ mysql-slave  │              │ mysql-slave  │
│   (down)     │            │ super_ro=OFF │              │ hostgroup 0  │◀── writes resume
└──────────────┘            └──────────────┘              └──────────────┘
      │                            ▲                             ▲
      │ writes fail                │ SET GLOBAL (runtime only,   │ monitor sees
      ▼                            │ never persisted)            │ read_only flip
  srv_ingest returns an error ─────┴─────────────────────────────┘
  srv_pub's PublishAsync throws → transaction rolls back
```

Nothing in flight is lost, because every stage already assumes this can happen:

|In flight when the primary dies                |What happens                                                        |
|-----------------------------------------------|--------------------------------------------------------------------|
|gRPC call mid-`AddAsync`                       |the insert fails, the client gets an error and retries — the packet was never acknowledged|
|outbox rows reserved but not yet published      |`PublishAsync` throws, the transaction rolls back, `IsProcessing` clears; the next relay tick re-reserves them|
|rows published to Kafka but not marked processed|the same rows are reserved again after promotion and re-published — Kafka is at-least-once by design, and the read side's `ON CONFLICT DO NOTHING` absorbs the duplicate|
|rows already marked processed                   |committed on the old primary *and* acked by the replica before the commit returned, so they survive the promotion|

The failure mode this design refuses is a **second writer**. A crashed primary that restarts comes
back `super_read_only` (persisted in `mysqld-auto.cnf`), so it cannot accept writes on the way up;
ProxySQL leaves it in the reader hostgroup until something appoints it. Promotion is always
`SET GLOBAL`, which does not survive a restart — so the appointment has to be made deliberately,
every time.

> **Automatic promotion does not currently work.** Orchestrator issues `SHOW SLAVE STATUS`, which
> MySQL 8.4 removed in favour of `SHOW REPLICA STATUS`, so topology discovery fails with
> `Error 1064` and step ② never fires on its own. Replication, semi-sync and ProxySQL's re-election
> all work — a promotion done by hand propagates correctly:
>
> ```
> docker exec kafkaflowshard-mysql-slave mysql -uroot -proot \
>   -e "SET GLOBAL super_read_only = OFF; SET GLOBAL read_only = OFF;"
> ```
>
> Restoring automatic failover means pinning the HA nodes to `mysql:8.0`; openark/orchestrator has
> no 8.4-compatible release.

Writability is a **runtime** appointment (`SET GLOBAL`, never `SET PERSIST`): any node that
restarts comes back read-only, which is the split-brain failsafe. Details and failover drills are
in `ha/README-HA.md`.

</details>

## Retries & dead-letter

<details>
<summary><b>Three outcomes</b> — commit, retry, or quarantine</summary>

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

</details>

<details>
<summary><b>Watch the dead-letter topic fill</b> — commands</summary>

```
docker exec packetshard-kafka kafka-topics --bootstrap-server localhost:9092 --list
# force rejections to see it fill: stop a shard so its writes fail
docker compose stop mongo-arp
docker exec -it packetshard-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic deadletter --from-beginning
```

</details>

## Run it

<details>
<summary><b>Option A</b> — everything in Docker (recommended)</summary>

```
cd PacketShard
docker compose up --build
```

This starts Consul, Zookeeper + Kafka, MySQL, the 5 MongoDB shard nodes, then MasterNode, srv_sub,
srv_pub, the ingress tier (LoadBalancer + srv_ingest) and the read side (Debezium, Postgres,
Redis, srv_read). Watch the logs: srv_pub publishes, srv_sub forwards, MasterNode prints
`[shard:Https] saved ...` etc.

The catalog UI at <http://localhost:8500> shows every service that came up and whether its health
check is passing — a quick read on what is actually alive (see
[Service discovery](#service-discovery-consul)).

Which MySQL topology comes up depends on `.env` — a single `mysql` node by default, or the
primary/replica pair behind ProxySQL if the [HA overlay](#high-availability-optional) is enabled.
`docker compose ps` will tell you which you got.

Inspect what landed in a shard:

```
docker exec -it packetshard-mongo-https mongosh --eval 'db.getSiblingDB("pcap").packets.find().limit(5)'
docker exec -it packetshard-mongo-arp   mongosh --eval 'db.getSiblingDB("pcap").packets.countDocuments()'
```

</details>

<details>
<summary><b>Option B</b> — infra in Docker, apps on the host</summary>

```
cd PacketShard
# Start only Kafka + MySQL + the 5 Mongo shards.
# In HA mode the MySQL service is `mysql-master` (plus `mysql-slave proxysql orchestrator
# ha-bootstrap`) rather than `mysql` — `docker compose config --services` lists what your
# current .env resolves to.
docker compose up -d zookeeper kafka mysql mongo-https mongo-tcp mongo-udp mongo-arp mongo-other

docker compose logs -f srv_pub srv_sub masternode

# In separate terminals (defaults already point at localhost):
dotnet run --project MasterNode
dotnet run --project srv_sub
dotnet run --project srv_pub
```

</details>

## Tests

<details>
<summary><b>Two lanes</b> — 169 tests split by cost, not by layer</summary>

`PacketShard.Tests` covers the pipeline in two lanes. The split is by **cost**, not by layer — the
unit lane runs anywhere in a few seconds, the infrastructure lane starts real databases via
Testcontainers:

```
dotnet test                                       # 169 tests, ~33s
dotnet test --filter "Category=Unit"              # 128 tests,  ~5s, no Docker required
dotnet test --filter "Category=Infrastructure"    #  41 tests, ~33s, needs Docker
```

Those are wall-clock times, which is what you actually wait for. xunit's own `Duration:` line
reports only the time spent inside test bodies — it says `2 s` and `26 s` for the two lanes,
excluding build, test-host startup and, for the infrastructure lane, container startup. Most of
that lane's wall clock is databases booting rather than assertions running: it starts a fresh
Postgres (and, for the projection tests, a Redis) **per test** rather than sharing one. The MySQL
suite shares a container per class and empties the table between tests instead, because MySQL
boots an order of magnitude slower.

A full run costs about the same as the infrastructure lane alone — xunit runs test classes in
parallel, so the 128 in-process tests finish while the containers are still coming up.

Both lanes are tagged explicitly, so `Category=Unit` selects the fast one by name.
`--filter "Category!=Infrastructure"` picks the same 128 tests today, but it also sweeps up
anything added later without a trait — handy as a CI gate that fails loudly if a new container
test forgets its tag, and the wrong choice if you want only what is known to be in-process.

CI runs the two lanes as separate jobs (`.github/workflows/ci.yml`), so a broken branch is
reported by the fast one without waiting on Docker.

</details>

<details>
<summary><b>Why parts of it need real databases</b> — the guarantees live in the engine, not the C#</summary>

Most of the guarantees this README claims are enforced by the database, not by the C#:
`ON CONFLICT (transaction_id) DO NOTHING`, `WHERE EXCLUDED.version > client_state.version`,
`FOR UPDATE SKIP LOCKED`, and the pg_ivm trigger. Against a mock every one of those tests would
pass while the system was broken, so they run against the real engines:

- **Postgres** is built from `postgres/Dockerfile`, so the container carries pg_ivm and applies
  `postgres/init.sql` through the official entrypoint — the schema under test is the deployed one.
  Redelivering an event leaves one ledger row *and* an IMMV count of 1; eight concurrent
  projections of the same transaction insert exactly once.
- **MySQL** proves the reservation: 40 rows, 4 concurrent relay workers each asking for all 40,
  asserting the union is 40 distinct ids. That is `FOR UPDATE SKIP LOCKED` and nothing else.
- **Redis + Postgres together** execute the crash-point table above — the durable write commits,
  the Redis marker is deliberately never set, and the redelivery must be absorbed by Postgres
  dedup rather than double-counted.

</details>

<details>
<summary><b>The MasterNode needs no containers</b> — TestProbes in place of the five Mongo writers</summary>

The routing stage takes its shard `Props` as a constructor argument, so the five MongoDB writers
can be swapped for `TestProbe`s: a packet with `proto: "UDP"` is asserted to reach the UDP probe
**and no other**. The auth gate is tested the same way — an invalid API key must produce
`"Invalid API Key"`, close the connection, and route nothing.

</details>

<details>
<summary><b>Failure paths</b> — tested as first-class behaviour, not an afterthought</summary>

A test suite that only covers success would miss the point of a durable pipeline. The suite pins
the unhappy paths too: a failing Kafka publish rolls the outbox transaction back and leaves the
row pending for the next tick; a failing Postgres write leaves no Redis marker behind; an
abandoned reservation returns to the pool when it expires; a malformed CDC value is skipped rather
than crashing the consumer.

</details>

## Live pipeline

<details>
<summary><b>Live pipeline</b> — interleaved logs from srv_pub, srv_sub and masternode</summary>

[![PacketShard live logs](https://github.com/sonne120/PacketShard/raw/main/assets/terminal.png)](/sonne120/PacketShard/blob/main/assets/terminal.png)

Interleaved output of `docker compose logs -f srv_pub srv_sub masternode`: the five `srv_sub`
replicas (`srv_sub-1..5`) each forward packets and get `MasterNode response: Ok`, while
`masternode` routes them to the protocol shards — `[shard:Other] saved DNS …`,
`[shard:Arp] saved ARP …`, etc.

</details>
