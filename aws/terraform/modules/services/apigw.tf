# API Gateway (HTTP API) in front of the read side.
# Private integration goes through a VPC Link straight to the srv-read
# Cloud Map service — no extra load balancer needed: API Gateway resolves
# healthy task IPs from service discovery, the same mechanism YARP uses.

resource "aws_apigatewayv2_vpc_link" "this" {
  name               = "${var.project}-read"
  subnet_ids         = var.private_subnet_ids
  security_group_ids = [var.vpclink_sg_id]
}

resource "aws_apigatewayv2_api" "read" {
  name          = "${var.project}-read-api"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "read" {
  api_id             = aws_apigatewayv2_api.read.id
  integration_type   = "HTTP_PROXY"
  integration_method = "ANY"
  connection_type    = "VPC_LINK"
  connection_id      = aws_apigatewayv2_vpc_link.this.id
  integration_uri    = aws_service_discovery_service.svc["srv-read"].arn

  payload_format_version = "1.0"
}

resource "aws_apigatewayv2_route" "read" {
  api_id    = aws_apigatewayv2_api.read.id
  route_key = "ANY /{proxy+}"
  target    = "integrations/${aws_apigatewayv2_integration.read.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.read.id
  name        = "$default"
  auto_deploy = true

  default_route_settings {
    throttling_burst_limit = 100
    throttling_rate_limit  = 50
  }
}
