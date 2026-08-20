# Services tier — core: ECS cluster, ECR, IAM, Cloud Map namespace,
# public NLB -> YARP gateway -> (Cloud Map DNS) -> srv_ingest.
#
# The rest of the pipeline lives in sibling files:
#   stateful.tf — EFS + Kafka (KRaft), 5x Mongo shards, Postgres (pg_ivm), Redis, Debezium Connect
#   pipeline.tf — srv_pub, srv_sub x5, MasterNode, srv_read, connector-init one-shot
#   apigw.tf    — API Gateway (HTTP API) + VPC Link -> srv_read via Cloud Map

data "aws_caller_identity" "current" {}

locals {
  ns = "${var.project}.local" # Cloud Map private DNS namespace

  # name -> {} ; images are pushed as <repo>:<image_tag>
  ecr_repos = toset([
    "yarp",       # LoadBalancer/Dockerfile (with the Cloud Map patch)
    "srv-ingest", # srv_ingest/Dockerfile
    "srv-pub",    # srv_pub/Dockerfile
    "srv-sub",    # srv_sub/Dockerfile
    "masternode", # MasterNode/Dockerfile
    "srv-read",   # srv_read/Dockerfile
    "postgres",   # postgres/Dockerfile (postgres:16 + pg_ivm + init.sql)
  ])

  log_config = {
    logDriver = "awslogs"
    options = {
      awslogs-group         = aws_cloudwatch_log_group.ecs.name
      awslogs-region        = var.region
      awslogs-stream-prefix = "ecs"
    }
  }
}

# ---------------------------------------------------------------- ECR ------

resource "aws_ecr_repository" "repo" {
  for_each     = local.ecr_repos
  name         = "${var.project}/${each.key}"
  force_delete = true # demo: allow destroy with images still inside
}

# ------------------------------------------------------------- cluster -----

resource "aws_ecs_cluster" "this" {
  name = var.project
}

resource "aws_cloudwatch_log_group" "ecs" {
  name              = "/ecs/${var.project}"
  retention_in_days = 7
}

# ----------------------------------------------------------------- IAM -----

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "task_execution" {
  name               = "${var.project}-task-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "task_execution" {
  role       = aws_iam_role.task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Allows the agent to inject secrets at container start.
resource "aws_iam_role_policy" "read_secrets" {
  name = "read-app-secrets"
  role = aws_iam_role.task_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["secretsmanager:GetSecretValue"]
      Resource = [
        var.db_conn_secret_arn,
        var.pg_password_secret_arn,
        var.pg_conn_secret_arn,
      ]
    }]
  })
}

resource "aws_iam_role" "task" {
  name               = "${var.project}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

# ----------------------------------------------------------- Cloud Map -----

resource "aws_service_discovery_private_dns_namespace" "this" {
  name = local.ns
  vpc  = var.vpc_id
}

# One discovery service per addressable component. All are multivalue A
# records; TTL 5s so clients track task replacement quickly.
locals {
  discovery_names = toset([
    "srv-ingest", # resolved by YARP
    "srv-read",   # resolved by the API Gateway VPC Link
    "masternode", # resolved by srv_sub
    "kafka",      # resolved by every Kafka client + Debezium
    "connect",    # resolved by the connector-init task
    "postgres",   # resolved by srv_read
    "redis",      # resolved by srv_read
    "mongo-https", "mongo-tcp", "mongo-udp", "mongo-arp", "mongo-other",
  ])
}

resource "aws_service_discovery_service" "svc" {
  for_each = local.discovery_names
  name     = each.key

  dns_config {
    namespace_id   = aws_service_discovery_private_dns_namespace.this.id
    routing_policy = "MULTIVALUE"

    dns_records {
      type = "A"
      ttl  = 5
    }
  }

  health_check_custom_config {
    failure_threshold = 1
  }
}

# ----------------------------------------------------------------- NLB -----

resource "aws_lb" "this" {
  name               = "${var.project}-nlb"
  load_balancer_type = "network"
  internal           = false
  subnets            = var.public_subnet_ids
  security_groups    = [var.nlb_sg_id]
}

resource "aws_lb_target_group" "yarp" {
  name        = "${var.project}-yarp"
  port        = 5001
  protocol    = "TCP"
  target_type = "ip"
  vpc_id      = var.vpc_id

  deregistration_delay = 30

  health_check {
    protocol = "TCP"
  }
}

resource "aws_lb_listener" "grpc" {
  load_balancer_arn = aws_lb.this.arn
  port              = 5001
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.yarp.arn
  }
}

# ----------------------------------------------- YARP gateway (ingress) ----

resource "aws_ecs_task_definition" "yarp" {
  family                   = "${var.project}-yarp"
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
    name      = "yarp"
    image     = "${aws_ecr_repository.repo["yarp"].repository_url}:${var.image_tag}"
    essential = true

    portMappings = [{ containerPort = 5001, protocol = "tcp" }]

    environment = [
      { name = "Listen__Port", value = "5001" },
      { name = "Ssl__Enabled", value = "false" }, # NLB passes TCP through; enable + mount a cert for end-to-end TLS
      # The gateway's destinations are written as discover://srv-ingest:8080 in appsettings.json.
      # Here that name is answered by Cloud Map rather than by a Consul agent: same sentinel, same
      # resolver, different provider. Nothing registers from inside the task — ECS registers it.
      { name = "Discovery__Provider", value = "Dns" },
      { name = "Discovery__Dns__Suffix", value = local.ns },
      # DNS cannot push, so this is a real poll interval. Cloud Map records carry a 5s TTL.
      { name = "Discovery__Dns__Refresh", value = "00:00:10" },
      # gRPC = long-lived HTTP/2; allow >1 connection per destination so load spreads.
      { name = "ReverseProxy__Clusters__ingest-cluster__HttpClient__EnableMultipleHttp2Connections", value = "true" },
    ]

    logConfiguration = local.log_config
  }])
}

resource "aws_ecs_service" "yarp" {
  name            = "${var.project}-yarp"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.yarp.arn
  desired_count   = var.yarp_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.yarp_sg_id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.yarp.arn
    container_name   = "yarp"
    container_port   = 5001
  }

  depends_on = [aws_lb_listener.grpc]
}

# ---------------------------------------------------------- srv_ingest -----

resource "aws_ecs_task_definition" "ingest" {
  family                   = "${var.project}-srv-ingest"
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
    name      = "srv-ingest"
    image     = "${aws_ecr_repository.repo["srv-ingest"].repository_url}:${var.image_tag}"
    essential = true

    portMappings = [{ containerPort = 8080, protocol = "tcp" }]

    environment = [
      { name = "GrpcPort", value = "8080" },
      { name = "KafkaServer", value = "kafka.${local.ns}:9092" },
      { name = "Topic", value = "SnapshotTopic" },
      { name = "TopicPartitions", value = "5" },
    ]

    secrets = [
      { name = "SqlConnStr", valueFrom = var.db_conn_secret_arn },
    ]

    logConfiguration = local.log_config
  }])
}

resource "aws_ecs_service" "ingest" {
  name            = "${var.project}-srv-ingest"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.ingest.arn
  desired_count   = var.ingest_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [var.app_sg_id]
    assign_public_ip = false
  }

  service_registries {
    registry_arn = aws_service_discovery_service.svc["srv-ingest"].arn
  }
}
