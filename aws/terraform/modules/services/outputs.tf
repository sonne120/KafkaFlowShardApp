output "nlb_dns_name" {
  value = aws_lb.this.dns_name
}

output "ecr_repos" {
  description = "map: logical name -> ECR repository URL"
  value       = { for name, repo in aws_ecr_repository.repo : name => repo.repository_url }
}

output "namespace_name" {
  value = aws_service_discovery_private_dns_namespace.this.name
}

output "read_api_endpoint" {
  description = "Public URL of the read API (API Gateway HTTP API)"
  value       = aws_apigatewayv2_stage.default.invoke_url
}

output "cluster_name" {
  value = aws_ecs_cluster.this.name
}

output "connect_init_command" {
  description = "Run once after the stack is green to register the 5 Debezium connectors"
  value = join(" ", [
    "aws ecs run-task",
    "--cluster ${aws_ecs_cluster.this.name}",
    "--task-definition ${aws_ecs_task_definition.connect_init.family}",
    "--launch-type FARGATE",
    "--region ${var.region}",
    "--network-configuration 'awsvpcConfiguration={subnets=[${join(",", var.private_subnet_ids)}],securityGroups=[${var.app_sg_id}],assignPublicIp=DISABLED}'",
  ])
}
