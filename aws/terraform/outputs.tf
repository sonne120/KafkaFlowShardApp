output "nlb_dns_name" {
  description = "Public gRPC entry point — point PacketGeneratorClient at http://<this>:5001"
  value       = module.services.nlb_dns_name
}

output "read_api_endpoint" {
  description = "Public read API (API Gateway) — GET <this>/stats/..."
  value       = module.services.read_api_endpoint
}

output "ecr_repos" {
  description = "docker push targets, keyed by logical name"
  value       = module.services.ecr_repos
}

output "rds_endpoint" {
  description = "MySQL outbox endpoint (private, reachable from the app SG only)"
  value       = module.data.rds_endpoint
}

output "cloud_map_namespace" {
  description = "Private DNS namespace for service discovery"
  value       = module.services.namespace_name
}

output "connect_init_command" {
  description = "Run once after the stack is green to register the 5 Debezium connectors"
  value       = module.services.connect_init_command
}

output "github_actions_role_arn" {
  description = "Set as the AWS_ROLE_ARN repo secret so .github/workflows/deploy-aws.yml can deploy. Empty unless github_repo is set."
  value       = try(aws_iam_role.github_actions[0].arn, "")
}
