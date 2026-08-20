output "rds_endpoint" {
  value = aws_db_instance.outbox.address
}

output "conn_string_secret_arn" {
  value = aws_secretsmanager_secret.conn_string.arn
}

output "pg_password_secret_arn" {
  value = aws_secretsmanager_secret.pg_password.arn
}

output "pg_conn_string_secret_arn" {
  value = aws_secretsmanager_secret.pg_conn_string.arn
}
