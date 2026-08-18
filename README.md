# KafkaFlowShardApp

Microservice pipeline: packets enter over **gRPC through a load balancer**, flow through a
**MySQL outbox → Kafka → MasterNode → 5 MongoDB shards** write path, and are projected by
**CDC (Debezium) into a Postgres read model (pg_ivm)** — a CQRS split with the shards as the
write side and Postgres as the query side.

## Architecture

The diagram below illustrates the complete data flow, from packet ingress to the read model.

-   **gRPC Ingress**: Clients stream packets over gRPC to a load balancer, which distributes them to `srv_ingest` instances. These instances write the packets to a MySQL outbox for durability.
-   **CQRS Read Side**: Debezium captures changes from the MongoDB shards and streams them via Kafka to `srv_read`. This service projects the data into a Postgres read model, with Redis acting as a fast-path filter to prevent duplicate processing.

```
                                 gRPC Ingress
┌────────────────────────┐       (HTTP/2)         ┌──────────────┐      ┌────────────────┐      ┌──────────────────┐  publish  ┌───────────┐
│ PacketGeneratorConsole │ ──────────────┐        │ LoadBalancer │      │ srv_ingest × 3 │      │   MySQL Outbox   │ ◀──────── │  srv_pub  │
│ PacketGeneratorClient  │               ├───────▶│  YARP :5001  │─────▶│  (gRPC, write) │─────▶│   (durable Q)    │           │  (relay)  │
└────────────────────────┘               │        └──────────────┘      └────────────────┘      └───────┬──────────┘           └───────────┘
                                         │                                                              │ poll (SKIP LOCKED)
                                         │                                                              ▼
                                         │                                                        ┌───────────┐
                                         │                                                        │   Kafka   │
                                         │                                                        │  (5 part) │
                                         │                                                        └─────┬─────┘
                                         │                                                              │ consume
                                         │                                                              ▼
                                         │                                                        ┌──────────────────┐           ┌───────────────────┐
                                         │                                                        │    srv_sub × 5   │─── TCP ───▶│    MasterNode     │
                                         │                                                        │ (1 per part)     │           │ auth · route      │
                                         │                                                        └──────────────────┘           └────────┬──────────┘
                                         │                                                                                         │ insert
                                         │                                                                                         ▼
  ┌──────────────────────────────────── 5 MongoDB shards (rs0) ──────────────────────────────────┐   ┌──────────────┐          ┌────────────────────┐
  │   HTTPS | TCP | UDP | ARP | OTHER                                                            │◀──│ Debezium × 5 │◀──────────│      srv_read      │
  │  :27018-:27022                                                                                 │   │ Kafka Connect│          │ CDC consumer + API │
  └───────────────────────────────────┬────────────────────────────────────────────────────────────┘   └──────────────┘          └────┬──────────┬────┘
                                      │ change streams (CDC)                                                ┌──────────┐ check/mark    │          │ GET /stats
                                      ▼                                                                     │  Redis   │ ◀─────────────┘          ▼
                                  CQRS Read Side                                                              │ fast-path│            ┌──────────────────┐
                                                                                                              └──────────┘            │ Postgres + pg_ivm│
                                                                                                                                      │  (read model)    │
                                                                                                                                      └──────────────────┘
```

## Projects

*   **`PacketGeneratorConsole` / `PacketGeneratorClient`**: gRPC clients that generate and send packets into the system.
*   **`LoadBalancer`**: YARP-based reverse proxy that round-robins gRPC traffic to ingest services.
*   **`srv_ingest`**: gRPC service that receives packets and writes them to the MySQL outbox.
*   **`srv_pub`**: Worker that relays packets from the MySQL outbox to Kafka.
*   **`srv_sub`**: Worker that consumes packets from Kafka and forwards them to the `MasterNode`.
*   **`MasterNode`**: Akka.NET TCP server that authenticates, filters, and routes packets to the appropriate MongoDB shard.
*   **`srv_read`**: CQRS read-side service. It consumes CDC events from Debezium/Kafka, projects them into a Postgres read model, and exposes a read API for querying statistics.
*   **Shared Libraries**: `Shared`, `kafka`, and `outbox` provide common data models, Kafka producer logic, and outbox persistence logic, respectively.

## The 5 shards (one MongoDB instance per "main package" type)

| Shard | Protocol(s)        | Host port |
|-------|--------------------|-----------|
| 1     | HTTPS / TLS / SSL  | 27018     |
| 2     | TCP                | 27019     |
| 3     | UDP                | 27020     |
| 4     | ARP                | 27021     |
| 5     | OTHER (everything else, e.g. ICMP, DNS) | 27022 |

All shards store into database `pcap`, collection `packets`.

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

## Live pipeline

![KafkaFlowShardApp live logs](assets/terminal.png)

Interleaved output of `docker compose logs -f srv_pub srv_sub masternode`: the five
`srv_sub` replicas (`srv_sub-1..5`) each forward packets and get `MasterNode response: Ok`,
while `masternode` routes them to the protocol shards — `[shard:Other] saved DNS …`,
`[shard:Arp] saved ARP …`, etc.
