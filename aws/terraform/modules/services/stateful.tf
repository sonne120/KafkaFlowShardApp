# Stateful tier as ECS containers (hybrid-demo profile).
#
# Persistence policy (deliberate, demo-scoped):
#   - Kafka and Postgres persist on EFS — offsets/topics and the read model
#     survive task restarts (these are the durability anchors of the demo).
#   - Mongo shards and Redis use task-local ephemeral storage. Per the
#     project's own design Postgres is the source of truth on the read side,
#     and the write side redelivers (at-least-once) — so a lost shard volume
#     costs history, not correctness of the demonstrated invariants.
#     Swap to Atlas / EFS later if shard durability matters.

# ----------------------------------------------------------------- EFS -----

resource "aws_efs_file_system" "this" {
  creation_token = "${var.project}-data"
  encrypted      = true

  tags = { Name = "${var.project}-data" }
}

resource "aws_efs_mount_target" "this" {
  count           = 2
  file_system_id  = aws_efs_file_system.this.id
  subnet_id       = var.private_subnet_ids[count.index]
  security_groups = [var.efs_sg_id]
}

# apache/kafka runs as uid 1000 (appuser)
resource "aws_efs_access_point" "kafka" {
  file_system_id = aws_efs_file_system.this.id

  posix_user {
    uid = 1000
    gid = 1000
  }

  root_directory {
    path = "/kafka"
    creation_info {
      owner_uid   = 1000
      owner_gid   = 1000
      permissions = "0750"
    }
  }

  tags = { Name = "${var.project}-kafka" }
}

# postgres:16 runs as uid 999 (postgres)
resource "aws_efs_access_point" "postgres" {
  file_system_id = aws_efs_file_system.this.id

  posix_user {
    uid = 999
    gid = 999
  }

  root_directory {
    path = "/postgres"
    creation_info {
      owner_uid   = 999
      owner_gid   = 999
      permissions = "0700"
    }
  }

  tags = { Name = "${var.project}-postgres" }
}

# ------------------------------------------------- Kafka (KRaft, 1 node) ---

resource "aws_ecs_task_definition" "kafka" {
  family                   = "${var.project}-kafka"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  volume {
    name = "kafka-data"
    efs_volume_configuration {
      file_system_id     = aws_efs_file_system.this.id
      transit_encryption = "ENABLED"
      authorization_config {
        access_point_id = aws_efs_access_point.kafka.id
      }
    }
  }

  container_definitions = jsonencode([{
    name      = "kafka"
    image     = "apache/kafka:3.8.0"
    essential = true

    portMappings = [{ containerPort = 9092, protocol = "tcp" }]

    mountPoints = [{
      sourceVolume  = "kafka-data"
      containerPath = "/var/lib/kafka/data"
      readOnly      = false
    }]

    environment = [
      { name = "CLUSTER_ID", value = "4L6g3nShT-eMCtK--X86sw" }, # fixed so restarts reuse the formatted EFS log dir
      { name = "KAFKA_NODE_ID", value = "1" },
      { name = "KAFKA_PROCESS_ROLES", value = "broker,controller" },
      { name = "KAFKA_CONTROLLER_QUORUM_VOTERS", value = "1@localhost:9094" },
      { name = "KAFKA_LISTENERS", value = "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094" },
      { name = "KAFKA_ADVERTISED_LISTENERS", value = "PLAINTEXT://kafka.${local.ns}:9092" },
      { name = "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", value = "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT" },
      { name = "KAFKA_CONTROLLER_LISTENER_NAMES", value = "CONTROLLER" },
      { name = "KAFKA_INTER_BROKER_LISTENER_NAME", value = "PLAINTEXT" },
      { name = "KAFKA_LOG_DIRS", value = "/var/lib/kafka/data" },
      { name = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", value = "1" },
      { name = "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", value = "1" },
      { name = "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", value = "1" },
      { name = "KAFKA_AUTO_CREATE_TOPICS_ENABLE", value = "true" },
      { name = "KAFKA_NUM_PARTITIONS", value = "5" }, # SnapshotTopic partition count; harmless default for pcap.* topics
    ]

    logConfiguration = local.log_config
  }])
}

resource "aws_ecs_service" "kafka" {
  name            = "${var.project}-kafka"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.kafka.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.data_sg_id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.svc["kafka"].arn
  }
}

# ------------------------------------------------------ Mongo shards x5 ----
# Each shard is a single-node replica set (rs0) so Debezium can read its
# change stream — same as docker-compose. The container self-initiates the
# replica set with its Cloud Map name as the member host, replacing the
# compose one-shot `mongo-init`.

locals {
  mongo_shards = toset(["https", "tcp", "udp", "arp", "other"])
}

resource "aws_ecs_task_definition" "mongo" {
  for_each = local.mongo_shards

  family                   = "${var.project}-mongo-${each.key}"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 1024
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name      = "mongo"
    image     = "mongo:7"
    essential = true

    portMappings = [{ containerPort = 27017, protocol = "tcp" }]

    entryPoint = ["bash", "-c"]
    command = [<<-EOT
      mongod --replSet rs0 --bind_ip_all --dbpath /data/db &
      MONGOD_PID=$!
      sleep 10
      mongosh --quiet --eval '
        try { rs.status(); }
        catch (e) {
          rs.initiate({ _id: "rs0", members: [
            { _id: 0, host: "mongo-${each.key}.${local.ns}:27017" }
          ]});
        }' || true
      wait $MONGOD_PID
    EOT
    ]

    logConfiguration = local.log_config
  }])
}

resource "aws_ecs_service" "mongo" {
  for_each = local.mongo_shards

  name            = "${var.project}-mongo-${each.key}"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.mongo[each.key].arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.data_sg_id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.svc["mongo-${each.key}"].arn
  }
}

# --------------------------------------------- Postgres (pg_ivm, on EFS) ---

resource "aws_ecs_task_definition" "postgres" {
  family                   = "${var.project}-postgres"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 1024
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  volume {
    name = "pg-data"
    efs_volume_configuration {
      file_system_id     = aws_efs_file_system.this.id
      transit_encryption = "ENABLED"
      authorization_config {
        access_point_id = aws_efs_access_point.postgres.id
      }
    }
  }

  container_definitions = jsonencode([{
    name      = "postgres"
    image     = "${aws_ecr_repository.repo["postgres"].repository_url}:${var.image_tag}"
    essential = true
    # The EFS access point forces uid/gid 999; start as that user so the
    # entrypoint takes its non-root path (no chown over NFS).
    user = "999:999"

    portMappings = [{ containerPort = 5432, protocol = "tcp" }]

    mountPoints = [{
      sourceVolume  = "pg-data"
      containerPath = "/var/lib/postgresql/data"
      readOnly      = false
    }]

    environment = [
      { name = "POSTGRES_USER", value = "postgres" },
      { name = "POSTGRES_DB", value = "readmodel" },
    ]

    secrets = [
      { name = "POSTGRES_PASSWORD", valueFrom = var.pg_password_secret_arn },
    ]

    logConfiguration = local.log_config
  }])
}

resource "aws_ecs_service" "postgres" {
  name            = "${var.project}-postgres"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.postgres.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.data_sg_id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.svc["postgres"].arn
  }
}

# --------------------------------------------------------------- Redis -----

resource "aws_ecs_task_definition" "redis" {
  family                   = "${var.project}-redis"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name      = "redis"
    image     = "redis:7"
    essential = true

    portMappings = [{ containerPort = 6379, protocol = "tcp" }]

    command = ["redis-server", "--appendonly", "yes", "--appendfsync", "everysec"]

    logConfiguration = local.log_config
  }])
}

resource "aws_ecs_service" "redis" {
  name            = "${var.project}-redis"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.redis.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.data_sg_id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.svc["redis"].arn
  }
}

# ----------------------------------------- Kafka Connect (Debezium) --------

resource "aws_ecs_task_definition" "connect" {
  family                   = "${var.project}-connect"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 2048
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  container_definitions = jsonencode([{
    name      = "connect"
    image     = "debezium/connect:2.7.3.Final"
    essential = true

    portMappings = [{ containerPort = 8083, protocol = "tcp" }]

    environment = [
      { name = "BOOTSTRAP_SERVERS", value = "kafka.${local.ns}:9092" },
      { name = "GROUP_ID", value = "connect-cluster" },
      { name = "CONFIG_STORAGE_TOPIC", value = "connect_configs" },
      { name = "OFFSET_STORAGE_TOPIC", value = "connect_offsets" },
      { name = "STATUS_STORAGE_TOPIC", value = "connect_statuses" },
      { name = "CONFIG_STORAGE_REPLICATION_FACTOR", value = "1" },
      { name = "OFFSET_STORAGE_REPLICATION_FACTOR", value = "1" },
      { name = "STATUS_STORAGE_REPLICATION_FACTOR", value = "1" },
      { name = "KEY_CONVERTER", value = "org.apache.kafka.connect.json.JsonConverter" },
      { name = "VALUE_CONVERTER", value = "org.apache.kafka.connect.json.JsonConverter" },
    ]

    logConfiguration = local.log_config
  }])
}

resource "aws_ecs_service" "connect" {
  name            = "${var.project}-connect"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.connect.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.app_sg_id] # app tier: needs Kafka AND the Mongo shards
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.svc["connect"].arn
  }
}
