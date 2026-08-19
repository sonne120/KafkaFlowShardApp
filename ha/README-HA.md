# PacketShard — HA outbox layer (semi-sync MySQL + Orchestrator + ProxySQL)

Replaces the single `mysql` service with a highly-available pair, extending the
pipeline's core invariant — **durability before acknowledgment** — down into the
storage layer: an acknowledged outbox row exists on at least two nodes
*before* `srv_ingest` returns `Accepted` to the client.

```
srv_ingest / srv_pub
        │  Server=proxysql;Port=6033
        ▼
   ProxySQL :6033 ──── writer HG 0 ──▶ [ mysql-master ]  ── semi-sync ──▶ [ mysql-slave ]
        ▲              reader HG 1 ──▶ (read-only node parks here; unused)
        │  moves nodes between HGs by polling @@super_read_only
   Orchestrator :3000 ── co-detection, promotion, fencing hook
```

## Layers of defense (split-brain)

| Layer | Mechanism | Protects against |
|---|---|---|
| MySQL startup | `super-read-only=ON` in `replication.cnf`; writer is *appointed* at runtime, never persisted | a revived ex-master self-appointing as writer |
| ProxySQL | `check_type='super_read_only'` + `OFFLINE_HARD` fencing hook | any traffic (incl. pre-crash connections) reaching a non-legitimate node |
| Orchestrator | co-detection (replica must confirm the source is gone) + `RecoveryPeriodBlockSeconds` anti-flapping | false failover during network partition of the arbiter |
| Semi-sync | `AFTER_SYNC` (lossless); optional `timeout=∞` hard mode | committing on a node that has no confirming replica |

Honest caveat for interviews: with 2 data nodes split-brain cannot be *proven*
impossible (that requires a 3+ vote quorum, i.e. Group Replication). These four
layers make it practically impossible and — with the semi-sync hard mode —
make a divergent *write* impossible even if it happens.

## Run — one command for the whole stack

Create `.env` in the repo root (next to the main docker-compose.yml):

```
COMPOSE_FILE=docker-compose.yml:ha/docker-compose.ha.yml
```

Then the usual command starts everything — pipeline + HA layer merged:

```bash
docker compose up -d --build

docker logs -f kafkaflowshard-ha-bootstrap   # appoints writer, registers topology
open http://localhost:3000                   # orchestrator UI (topology graph)
```

The HA file overrides the main one on merge: the old single `mysql`
service is parked behind an unused profile (never starts), and
`srv_ingest`/`srv_pub` get `depends_on: ha-bootstrap (completed)` plus the
ProxySQL connection string via `ConnectionStrings__Outbox`:

```
Server=proxysql;Port=6033;User=app;Password=apppass;Database=outboxdb
```

If the connection string is hardcoded in appsettings.json rather than read
from configuration/env, move it to env (the double-underscore syntax maps
to `ConnectionStrings:Outbox` in .NET configuration).

To run without the HA layer, delete/comment the `.env` line — plain
`docker compose up` uses the original single-mysql topology again.

Both services always land on the writer hostgroup — deliberate, because
`FOR UPDATE SKIP LOCKED` and the outbox stored procedure must run on the
primary; locks do not exist on a replica.

## Verify semi-sync

```bash
docker exec kafkaflowshard-mysql-master mysql -uroot -proot --ssl-mode=DISABLED -e "
  SHOW STATUS LIKE 'Rpl_semi_sync_source_status';        -- ON
  SHOW STATUS LIKE 'Rpl_semi_sync_source_no_tx';         -- async-fallback counter (alert on growth)
  SHOW STATUS LIKE 'Rpl_semi_sync_source_avg_net_wait_time';"
```

## Chaos test 1 — replica dies (degradation, no failover)

```bash
docker compose stop mysql-slave
# writes continue after the 5s semi-sync timeout; no_tx starts growing:
docker exec kafkaflowshard-mysql-master mysql -uroot -proot --ssl-mode=DISABLED \
  -e "SHOW STATUS LIKE 'Rpl_semi_sync_source_no_tx';"
docker compose start mysql-slave     # GTID catches up, status back to ON
```

With the hard mode (`rpl_semi_sync_source_timeout = 4294967295` in
`replication.cnf`) the same test instead **halts ingest** — commits hang,
Polly cancels, clients get `Unavailable`. Durability strictly over availability.

## Chaos test 2 — master dies under load (automatic failover)

```bash
# keep the packet generator running, then:
docker compose stop mysql-master

# watch the recovery unfold:
docker exec kafkaflowshard-orchestrator cat /tmp/recovery.log
# t≈5s  DETECTED DeadMaster        (co-detection: slave confirms broken stream)
# t≈7s  PROMOTED mysql-slave       (super_read_only cleared on successor)
# t≈8s  [fence] hard-offlining mysql-master in ProxySQL

# proxysql moved the writer HG:
docker exec kafkaflowshard-proxysql mysql -h127.0.0.1 -P6032 -uadmin -padmin --ssl-mode=DISABLED \
  -e "SELECT hostgroup_id, hostname, status FROM runtime_mysql_servers;"
```

Expected pipeline behavior during the ~8–10s window:
- `srv_ingest`: Polly retries absorb part of the window; the rest surfaces as
  gRPC `Unavailable` (retryable) — never `InvalidArgument`.
- `srv_pub`: rows reserved via `SKIP LOCKED` at crash time lose their locks
  with the dead node → re-reserved after failover → possible duplicate
  publish → absorbed by the read-side idempotent projection. Guarantees compose.
- Nothing acked to a client is lost: `AFTER_SYNC` guarantees the promoted
  replica already had every acknowledged transaction.

## Chaos test 3 — the nasty one: arbiter partition

```bash
# isolate the master from orchestrator ONLY (proxysql still sees it):
docker network disconnect kafkaflowshard_default kafkaflowshard-orchestrator
sleep 30
docker network connect kafkaflowshard_default kafkaflowshard-orchestrator
```

Expected: **no failover**. Orchestrator alone losing sight of the master is
not `DeadMaster` — the replica still reports a healthy replication stream,
co-detection does not fire. This is the test that separates orchestrator
from a naive health-check-and-promote script.

## Re-admitting a recovered ex-master

The node boots read-only and is `OFFLINE_HARD` in ProxySQL. Orchestrator
re-parents it as a replica of the new source (visible in the UI). Once
`Replica_IO_Running: Yes`, re-admit it to the reader hostgroup:

```bash
docker exec kafkaflowshard-proxysql mysql -h127.0.0.1 -P6032 -uadmin -padmin --ssl-mode=DISABLED -e "
  UPDATE mysql_servers SET status='ONLINE' WHERE hostname='mysql-master';
  LOAD MYSQL SERVERS TO RUNTIME; SAVE MYSQL SERVERS TO DISK;"
```

It stays a read-only replica; promoting it back is a deliberate graceful
takeover via the orchestrator UI/API, never automatic.

## Files

```
ha/
├── docker-compose.ha.yml            # the whole HA layer
├── mysql/
│   ├── conf.d/replication.cnf       # shared: semi-sync AFTER_SYNC, GTID, boot read-only
│   ├── master-init/01-users.sql     # repl / orch / monitor / app users
│   └── slave-init/01-replica.sh     # GTID auto-position replication setup
├── orchestrator/
│   ├── Dockerfile                   # + mysql client for the fencing hook
│   ├── orchestrator.conf.json       # co-detection, recovery, hooks
│   └── hooks/fence-old-master.sh    # OFFLINE_HARD the failed node in ProxySQL
├── proxysql/proxysql.cnf            # HG 0/1, check_type=super_read_only
└── scripts/bootstrap.sh             # one-shot: appoint writer, register topology
```
