# Pipeline services: srv_pub (outbox relay), srv_sub x5 (partition consumers),
# MasterNode (Akka.NET router), srv_read (CDC consumer + read API),
# plus the one-shot Debezium connector registration task.

# ------------------------------------------------------------- srv_pub -----

resource "aws_ecs_task_definition" "pub" {
  family                   = "${var.project}-srv-pub"
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
    name      = "srv-pub"
    image     = "${aws_ecr_repository.repo["srv-pub"].repository_url}:${var.image_tag}"
    essential = true

    environment = [
      { name = "KafkaServer", value = "kafka.${local.ns}:9092" },
      { name = "Topic", value = "SnapshotTopic" },
      { name = "RetryTopic", value = "5sdelay" },
      { name = "DeadletterTopic", value = "deadletter" },
      { name = "TopicPartitions", value = "5" },
    ]

    secrets = [
      { name = "SqlConnStr", valueFrom = var.db_conn_secret_arn },
    ]

    logConfiguration = local.log_config
  }])
}

resource "aws_ecs_service" "pub" {
  name            = "${var.project}-srv-pub"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.pub.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.app_sg_id]
    assign_public_ip = false
  }
}

# ----------------------------------------------------------- MasterNode ----

resource "aws_ecs_task_definition" "masternode" {
  family                   = "${var.project}-masternode"
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
    name      = "masternode"
    image     = "${aws_ecr_repository.repo["masternode"].repository_url}:${var.image_tag}"
    essential = true

    portMappings = [{ containerPort = 8000, protocol = "tcp" }]

    environment = [
      { name = "Tcp__Port", value = "8000" },
      { name = "Shards__Https", value = "mongodb://mongo-https.${local.ns}:27017" },
      { name = "Shards__Tcp", value = "mongodb://mongo-tcp.${local.ns}:27017" },
      { name = "Shards__Udp", value = "mongodb://mongo-udp.${local.ns}:27017" },
      { name = "Shards__Arp", value = "mongodb://mongo-arp.${local.ns}:27017" },
      { name = "Shards__Other", value = "mongodb://mongo-other.${local.ns}:27017" },
    ]

    logConfiguration = local.log_config
  }])
}

# Single stateful actor system — desired_count stays 1. If the task is
# replaced, srv_sub stops receiving "Ok" and stops committing offsets: the
# pipeline pauses, nothing is lost (at-least-once by design).
resource "aws_ecs_service" "masternode" {
  name            = "${var.project}-masternode"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.masternode.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.app_sg_id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.svc["masternode"].arn
  }
}

# ------------------------------------------------------------- srv_sub -----

resource "aws_ecs_task_definition" "sub" {
  family                   = "${var.project}-srv-sub"
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
    name      = "srv-sub"
    image     = "${aws_ecr_repository.repo["srv-sub"].repository_url}:${var.image_tag}"
    essential = true

    environment = [
      { name = "KafkaServer", value = "kafka.${local.ns}:9092" },
      { name = "Topic", value = "SnapshotTopic" },
      { name = "RetryTopic", value = "5sdelay" },
      { name = "DeadletterTopic", value = "deadletter" },
      { name = "TopicPartitions", value = "5" },
      { name = "MaxAttempts", value = "3" },
      { name = "ConsumerGroup", value = "ConsumerGroup" },
      { name = "MasterNode__Host", value = "masternode.${local.ns}" },
      { name = "MasterNode__Port", value = "8000" },
      # Same seam as the gateway: srv_sub resolves the shard router through Cloud Map instead of
      # pinning one address. MasterNode__Host stays as the fallback, MasterNode__Port as the port
      # hint an A record cannot supply.
      { name = "MasterNode__Service", value = "masternode" },
      { name = "Discovery__Provider", value = "Dns" },
      { name = "Discovery__Dns__Suffix", value = local.ns },
      { name = "Discovery__Dns__Refresh", value = "00:00:10" },
    ]

    logConfiguration = local.log_config
  }])
}

# 5 identical consumers, one per SnapshotTopic partition (Kafka's consumer
# group assigns partitions — same as `deploy: replicas: 5` in compose).
resource "aws_ecs_service" "sub" {
  name            = "${var.project}-srv-sub"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.sub.arn
  desired_count   = var.sub_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.app_sg_id]
    assign_public_ip = false
  }
}

# ------------------------------------------------------------ srv_read -----

resource "aws_ecs_task_definition" "read" {
  family                   = "${var.project}-srv-read"
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
    name      = "srv-read"
    image     = "${aws_ecr_repository.repo["srv-read"].repository_url}:${var.image_tag}"
    essential = true

    portMappings = [{ containerPort = 8080, protocol = "tcp" }]

    environment = [
      { name = "ASPNETCORE_URLS", value = "http://+:8080" },
      { name = "KafkaServer", value = "kafka.${local.ns}:9092" },
      { name = "ConsumerGroup", value = "ReadModelGroup" },
      { name = "CdcTopicPattern", value = "^pcap\\..*packets$" },
      { name = "Redis__ConnStr", value = "redis.${local.ns}:6379" },
      { name = "Redis__TtlDays", value = "7" },
    ]

    secrets = [
      { name = "Postgres__ConnStr", valueFrom = var.pg_conn_secret_arn },
    ]

    logConfiguration = local.log_config
  }])
}

resource "aws_ecs_service" "read" {
  name            = "${var.project}-srv-read"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.read.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.app_sg_id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.svc["srv-read"].arn
  }
}

# ----------------------- Debezium connector registration (one-shot) --------
# Replaces compose's `connect-init`. Run after the stack is up:
#   aws ecs run-task --cluster packetshard \
#     --task-definition packetshard-connect-init \
#     --launch-type FARGATE \
#     --network-configuration 'awsvpcConfiguration={subnets=[<private>],securityGroups=[<app-sg>],assignPublicIp=DISABLED}'
# (the exact command is printed by `terraform output connect_init_command`)

locals {
  connector_config = {
    for shard in local.mongo_shards : shard => jsonencode({
      "connector.class"                = "io.debezium.connector.mongodb.MongoDbConnector"
      "tasks.max"                      = "1"
      "mongodb.connection.string"      = "mongodb://mongo-${shard}.${local.ns}:27017/?replicaSet=rs0"
      "topic.prefix"                   = "pcap.${shard}"
      "capture.mode"                   = "change_streams_update_full"
      "database.include.list"          = "pcap"
      "collection.include.list"        = "pcap.packets"
      "key.converter"                  = "org.apache.kafka.connect.json.JsonConverter"
      "key.converter.schemas.enable"   = "false"
      "value.converter"                = "org.apache.kafka.connect.json.JsonConverter"
      "value.converter.schemas.enable" = "false"
      "transforms"                     = "unwrap"
      "transforms.unwrap.type"         = "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState"
      "transforms.unwrap.add.fields"   = "op"
    })
  }

  connect_init_script = join("\n", concat(
    [
      "set -eu",
      "CONNECT_URL=http://connect.${local.ns}:8083",
      "echo \"Waiting for Kafka Connect at $CONNECT_URL ...\"",
      "until curl -fsS \"$CONNECT_URL/connectors\" >/dev/null 2>&1; do sleep 3; done",
      "echo 'Connect is up.'",
    ],
    [
      for shard in sort(tolist(local.mongo_shards)) :
      "echo 'Registering mongo-${shard} ...'; curl -fsS -X PUT -H 'Content-Type: application/json' \"$CONNECT_URL/connectors/mongo-${shard}/config\" -d '${local.connector_config[shard]}'; echo"
    ],
    ["echo 'All connectors registered.'"]
  ))
}

resource "aws_ecs_task_definition" "connect_init" {
  family                   = "${var.project}-connect-init"
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
    name       = "connect-init"
    image      = "curlimages/curl:8.10.1"
    essential  = true
    entryPoint = ["sh", "-c"]
    command    = [local.connect_init_script]

    logConfiguration = local.log_config
  }])
}
