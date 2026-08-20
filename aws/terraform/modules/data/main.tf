# Data tier (vertical slice): RDS MySQL for the outbox.
# Replaces the compose `mysql` container AND the whole ha/ overlay
# (semi-sync replica + Orchestrator + ProxySQL) - Multi-AZ failover is a
# managed RDS feature; flip multi_az = true for the prod-like profile.
#
# srv_ingest runs `CREATE TABLE IF NOT EXISTS Outbox` itself on startup
# (OutboxInitializer), so the instance only needs the `outbox` database.

resource "random_password" "db" {
  length  = 24
  special = false
}

resource "aws_db_subnet_group" "this" {
  name       = "${var.project}-outbox"
  subnet_ids = var.private_subnet_ids
}

resource "aws_db_instance" "outbox" {
  identifier     = "${var.project}-outbox"
  engine         = "mysql"
  engine_version = "8.0"
  instance_class = "db.t4g.micro" # demo profile

  allocated_storage = 20
  storage_type      = "gp3"

  db_name  = "outbox"
  username = "outbox_admin"
  password = random_password.db.result

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [var.db_sg_id]

  multi_az            = false # demo profile; true = prod-like
  publicly_accessible = false

  # Demo lifecycle: clean destroy, no snapshots/backups.
  skip_final_snapshot     = true
  backup_retention_period = 0
  deletion_protection     = false
  apply_immediately       = true
}

# The full connection string goes to Secrets Manager and is injected into the
# task definition via `secrets` (never `environment`) - the app keeps reading
# the same SqlConnStr variable it reads under docker compose.
resource "aws_secretsmanager_secret" "conn_string" {
  name                    = "${var.project}/outbox-connstr"
  recovery_window_in_days = 0 # demo: allow immediate re-create after destroy
}

resource "aws_secretsmanager_secret_version" "conn_string" {
  secret_id = aws_secretsmanager_secret.conn_string.id
  secret_string = join(";", [
    "server=${aws_db_instance.outbox.address}",
    "port=3306",
    "database=outbox",
    "user=outbox_admin",
    "password=${random_password.db.result}",
  ])
}

# ---------------------------------------------------------------------------
# Postgres read model (runs as an ECS container with the project's pg_ivm
# image — see modules/services/stateful.tf). Only its credentials live here.
# The host name is its Cloud Map name, known statically.
# ---------------------------------------------------------------------------

resource "random_password" "postgres" {
  length  = 24
  special = false
}

# Injected as POSTGRES_PASSWORD into the postgres container.
resource "aws_secretsmanager_secret" "pg_password" {
  name                    = "${var.project}/postgres-password"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "pg_password" {
  secret_id     = aws_secretsmanager_secret.pg_password.id
  secret_string = random_password.postgres.result
}

# Injected as Postgres__ConnStr into srv_read.
resource "aws_secretsmanager_secret" "pg_conn_string" {
  name                    = "${var.project}/readmodel-connstr"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "pg_conn_string" {
  secret_id = aws_secretsmanager_secret.pg_conn_string.id
  secret_string = join(";", [
    "Host=postgres.${var.project}.local",
    "Port=5432",
    "Database=readmodel",
    "Username=postgres",
    "Password=${random_password.postgres.result}",
  ])
}
