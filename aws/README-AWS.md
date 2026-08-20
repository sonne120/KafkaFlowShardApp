# PacketShard on AWS — full pipeline on ECS Fargate (hybrid-demo profile)

Terraform for the **entire** PacketShard pipeline on AWS:

```
Internet ──TCP:5001──▶ NLB (public, TCP passthrough)
                        │
                        ▼
              YARP gateway × 2 ──Cloud Map DNS──▶ srv_ingest × 3 (gRPC :8080)
                                                        │ tx insert
                                                        ▼
                                                  RDS MySQL (outbox)
                                                        │ poll SKIP LOCKED
                                                  srv_pub (relay)
                                                        │ publish
                                                  Kafka (KRaft, ECS+EFS)
                                                        │ 5 partitions
                                                  srv_sub × 5
                                                        │ TCP :8000 (Cloud Map)
                                                  MasterNode (Akka.NET)
                                                        │ route by proto
                                            5 × Mongo shards (rs0, ECS)
                                                        │ change streams
                                            Debezium Connect (ECS)
                                                        │ pcap.* topics
                                                  Kafka ──▶ srv_read ──▶ Postgres + pg_ivm (ECS+EFS)
                                                                │              ▲ SELECT
                                                              Redis            │
                                                                               │
Internet ──HTTPS──▶ API Gateway (HTTP API) ──VPC Link──▶ srv_read /stats/*  ───┘
```

**Hybrid-demo profile** (deliberate): the application services AND the stateful
tier run as ECS Fargate containers; only the outbox uses a managed database
(RDS MySQL). Zero application-code changes: Kafka stays plaintext inside the
VPC, Postgres keeps the project's own pg_ivm image, Mongo shards keep rs0.
Service discovery is uniform — one Cloud Map namespace (`packetshard.local`)
serves YARP → ingest, srv_sub → MasterNode, all Kafka clients, Debezium → Mongo,
and even API Gateway → srv_read (VPC Link integrates directly with Cloud Map).

**No application patch is needed.** The gateway's destinations are written as
`discover://srv-ingest:8080` and srv_sub names `MasterNode__Service`; which
registry answers is `Discovery__Provider`, set to `Dns` here and to `Consul` in
docker-compose. Same sentinel, same resolver, same `IServiceDirectory` — the
provider is the only thing that changes between a laptop and this VPC. Cloud Map
is the registry on AWS, so **no Consul agent is deployed** and nothing registers
from inside a task: ECS registers the task when it starts it.

Persistence: **Kafka and Postgres persist on EFS** (offsets/topics and the read
model survive restarts). **Mongo shards and Redis are task-ephemeral** — per the
project's own design Postgres is the source of truth and the write path
redelivers; a lost shard volume costs history, not the demonstrated invariants.
Swap in Atlas/EFS later if shard durability matters.

What is NOT deployed from the repo: `LoadBalancer` as a service (YARP still
runs, but the NLB replaces its public role), `mongo-init` / `connect-init`
one-shots (self-init + an ECS run-task replace them), zookeeper (KRaft), the
`consul` agent (Cloud Map is the registry here), and the whole `ha/` overlay
(RDS Multi-AZ is the managed equivalent — flip `multi_az = true`).

## Prerequisites

Terraform ≥ 1.6 (or OpenTofu), AWS CLI v2, Docker. Default region
`eu-central-1` (`-var region=...` to change).

## 1. Provision

```bash
cd terraform
terraform init
terraform apply     # ~10-15 min; RDS is the slow part
```

ECS services crash-loop until images are pushed — expected, next step.

## 2. Build and push the 7 images

```bash
REGION=eu-central-1
terraform output -json ecr_repos   # the 7 push targets

ACCOUNT_REGISTRY=$(terraform output -json ecr_repos | python3 -c "import sys,json; print(list(json.load(sys.stdin).values())[0].split('/')[0])")
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin "$ACCOUNT_REGISTRY"

# from the PacketShard repo root; add --platform linux/amd64 on Apple Silicon
declare -A BUILD=(
  [yarp]=LoadBalancer/Dockerfile
  [srv-ingest]=srv_ingest/Dockerfile
  [srv-pub]=srv_pub/Dockerfile
  [srv-sub]=srv_sub/Dockerfile
  [masternode]=MasterNode/Dockerfile
  [srv-read]=srv_read/Dockerfile
)
for name in "${!BUILD[@]}"; do
  repo=$(terraform output -json ecr_repos | python3 -c "import sys,json; print(json.load(sys.stdin)['$name'])")
  docker build -f "${BUILD[$name]}" -t "$repo:latest" .
  docker push "$repo:latest"
done

# postgres image builds from its own context:
repo=$(terraform output -json ecr_repos | python3 -c "import sys,json; print(json.load(sys.stdin)['postgres'])")
docker build -t "$repo:latest" postgres/
docker push "$repo:latest"

aws ecs update-service --cluster packetshard --service packetshard-yarp --force-new-deployment --region $REGION
# repeat for: srv-ingest, srv-pub, srv-sub, masternode, srv-read, postgres — or just wait for the retry loop
```

## 3. Register the Debezium connectors (once)

After all services are RUNNING (`aws ecs list-services --cluster packetshard`):

```bash
eval "$(terraform output -raw connect_init_command)"
```

This runs a one-shot Fargate task that waits for Connect and PUTs the five
MongoDB connectors (hosts = Cloud Map names, `replicaSet=rs0`). Idempotent —
safe to re-run.

## 4. Test end-to-end

```bash
NLB=$(terraform output -raw nlb_dns_name)
API=$(terraform output -raw read_api_endpoint)
```

- Point `PacketGeneratorConsole` / `PacketGeneratorClient` at `http://$NLB:5001`.
- Watch the flow: `aws logs tail /ecs/packetshard --follow --since 10m`
  (YARP resolves ingest IPs → ingest "Outbox schema initialized" → srv_pub
  publishes → srv_sub commits on "Ok" → Debezium streams → srv_read projects).
- Query the read model through API Gateway: `curl $API/stats/...` — same
  endpoints as `GET /stats/*` locally.

Startup order note: everything starts in parallel; the first ~2-3 minutes of
connection errors in logs are normal (the app's own retry loops are doing
their job) until Kafka, Mongo and Postgres settle.

## 5. Tear down

```bash
terraform destroy
```

Demo-profiled for clean destroy: no snapshots, zero-recovery secrets,
`force_delete` ECR, ephemeral shard storage.

## Continuous delivery

`.github/workflows/deploy-aws.yml` builds the seven images, pushes them to ECR and rolls the
services on every push to `main` that touches application code.

It deliberately **does not run Terraform**. Infrastructure stays a considered `terraform apply`,
because a pipeline that can silently replace an RDS instance is not one you want firing on a merge.
The workflow only moves code.

### Setup — one secret, no long-lived keys

```bash
cd terraform
terraform apply -var 'github_repo=owner/PacketShard'
terraform output -raw github_actions_role_arn
```

Put that ARN in the repo as the secret **`AWS_ROLE_ARN`**. GitHub then mints a short-lived token per
run and trades it for the role — there is no access key in the repo to leak or rotate. The trust
policy pins both the repo and the branch (`github_deploy_refs`, `refs/heads/main` by default), which
is the entire security boundary: without it any repo on GitHub could assume the role.

If the account already has a GitHub OIDC provider — you only get one — add
`-var 'create_github_oidc_provider=false'` and the role attaches to the existing one.

Two optional repository *variables* override the defaults: `AWS_REGION` (`eu-central-1`) and
`AWS_PROJECT` (`packetshard`, the Terraform `project` prefix).

Without `AWS_ROLE_ARN` the workflow skips cleanly rather than failing, so forks stay green.

### What a run does

Images are pushed twice — `:latest`, which is what the task definitions reference and therefore what
a rollout picks up, and `:<git-sha>`, which is how you tell later which commit is actually running.
Then each stateless service gets `update-service --force-new-deployment`, and the run waits on
`ecs wait services-stable`, so a crash-looping task fails the deploy instead of going green while
ECS quietly rolls back.

`postgres` is **not** rolled automatically. It is stateful, and restarting it drops every read-model
connection; its EFS data survives either way. Run the workflow manually with `redeploy_postgres`
checked when its image actually changed.

The role can push to ECR and restart services — nothing more. Rolling a service reuses its existing
task definition, so the role needs neither `RegisterTaskDefinition` nor `PassRole`: it cannot change
*what* a task runs as, only restart it.

### First deployment

CD needs the ECR repos to exist, so the very first push still goes through
[step 2](#2-build-and-push-the-7-images) by hand — or just `terraform apply` first and let the next
merge to `main` do the pushing.

## Cost (demo profile, eu-central-1, approximate)

~17 Fargate tasks ≈ $170–200/mo, NAT ≈ $37, NLB ≈ $20, RDS db.t4g.micro ≈ $15,
EFS/logs/Cloud Map/API GW — a few dollars at demo traffic.
**≈ $250–280/month ≈ $8–9/day.** Deploy → demo → destroy: a full demo day
costs about as much as a coffee.

## Design notes

- **NLB + YARP + Cloud Map** (instead of ALB): the project's own gateway stays
  the gRPC load balancer; `DnsServiceDirectory` expands the Cloud Map name into
  per-task destinations, re-resolving every 10s (records carry 5s TTL), with
  `EnableMultipleHttp2Connections` so HTTP/2 streams don't pin to one task.
  Expanding the record matters: left as a single name, SocketsHttpHandler picks
  one address and multiplexes every stream onto one task, so round-robin never
  sees the others. A record carries no port, which is why the sentinel spells it
  out — `discover://srv-ingest:8080`.
- **API Gateway → Cloud Map**: the VPC Link integrates with the `srv-read`
  discovery service directly — throttling and a public HTTPS edge with no
  extra load balancer.
- **MasterNode** stays a single task by design (stateful actor system). If it
  restarts, srv_sub stops getting "Ok" and stops committing — a pause, never
  a loss.
- **Kafka**: single KRaft broker (`apache/kafka:3.8.0`), fixed CLUSTER_ID so
  the EFS log dir survives restarts; `KAFKA_NUM_PARTITIONS=5` matches
  SnapshotTopic's partition count (the app also creates topics explicitly via
  TopicRepository).
- **Known limits**: single-AZ NAT; single broker; Mongo on ephemeral storage;
  plaintext inside the VPC. `terraform fmt`/`validate` now run in CI and pass;
  `terraform apply` has still never been run against a real account, so expect
  attribute-level fixes on the first one.

## Managed upgrade path (when you want the resume keywords)

RDS Multi-AZ (`multi_az=true`) → MSK + SASL/SCRAM (touches 4 Kafka client
classes) → MongoDB Atlas (`mongodbatlas` provider; Debezium conn strings
change to `mongodb+srv://`) → RDS Postgres + trigger-maintained stats table
instead of pg_ivm → ElastiCache.
