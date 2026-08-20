# Network: VPC with 2 AZs, public subnets (NLB, NAT) and private subnets (ECS tasks, RDS).
# Single NAT gateway — demo cost profile, not HA.

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 2)
}

resource "aws_vpc" "this" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = { Name = "${var.project}-vpc" }
}

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${var.project}-igw" }
}

resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.this.id
  cidr_block              = cidrsubnet(aws_vpc.this.cidr_block, 8, count.index)
  availability_zone       = local.azs[count.index]
  map_public_ip_on_launch = true

  tags = { Name = "${var.project}-public-${local.azs[count.index]}" }
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.this.id
  cidr_block        = cidrsubnet(aws_vpc.this.cidr_block, 8, count.index + 10)
  availability_zone = local.azs[count.index]

  tags = { Name = "${var.project}-private-${local.azs[count.index]}" }
}

resource "aws_eip" "nat" {
  domain = "vpc"
  tags   = { Name = "${var.project}-nat-eip" }
}

resource "aws_nat_gateway" "this" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id
  depends_on    = [aws_internet_gateway.this]

  tags = { Name = "${var.project}-nat" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this.id
  }

  tags = { Name = "${var.project}-public-rt" }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.this.id
  }

  tags = { Name = "${var.project}-private-rt" }
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# ---------------------------------------------------------------------------
# Security groups.
#
# Tiers:
#   nlb     — public gRPC entry point
#   yarp    — gateway tasks, accept only from NLB
#   app     — every .NET service + Kafka Connect; talk to each other freely
#   data    — stateful containers (Kafka, Mongo x5, Postgres, Redis);
#             accept only from app tier (+ each other)
#   db      — RDS MySQL outbox; accept only from app tier
#   efs     — EFS mount targets; NFS only from the data tier
#   vpclink — API Gateway VPC Link ENIs (egress-only; app accepts 8080 from it)
# ---------------------------------------------------------------------------

resource "aws_security_group" "nlb" {
  name        = "${var.project}-nlb"
  description = "Public gRPC entry point"
  vpc_id      = aws_vpc.this.id

  ingress {
    description = "gRPC (h2c) from anywhere - demo; restrict to your IP if desired"
    from_port   = 5001
    to_port     = 5001
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "yarp" {
  name        = "${var.project}-yarp"
  description = "YARP gateway tasks - accept only from the NLB"
  vpc_id      = aws_vpc.this.id

  ingress {
    description     = "gRPC from NLB"
    from_port       = 5001
    to_port         = 5001
    protocol        = "tcp"
    security_groups = [aws_security_group.nlb.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "vpclink" {
  name        = "${var.project}-vpclink"
  description = "API Gateway VPC Link ENIs"
  vpc_id      = aws_vpc.this.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "app" {
  name        = "${var.project}-app"
  description = "Application services (srv_*, MasterNode, Kafka Connect)"
  vpc_id      = aws_vpc.this.id

  ingress {
    description     = "gRPC ingest from YARP"
    from_port       = 8080
    to_port         = 8080
    protocol        = "tcp"
    security_groups = [aws_security_group.yarp.id]
  }

  ingress {
    description     = "read API from API Gateway VPC Link"
    from_port       = 8080
    to_port         = 8080
    protocol        = "tcp"
    security_groups = [aws_security_group.vpclink.id]
  }

  ingress {
    description = "app tier talks to itself (srv_sub -> MasterNode:8000, init -> Connect:8083)"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "data" {
  name        = "${var.project}-data"
  description = "Stateful containers (Kafka, Mongo shards, Postgres, Redis)"
  vpc_id      = aws_vpc.this.id

  ingress {
    description     = "from application tier"
    from_port       = 0
    to_port         = 65535
    protocol        = "tcp"
    security_groups = [aws_security_group.app.id]
  }

  ingress {
    description = "data tier internal (broker <-> broker, etc.)"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "efs" {
  name        = "${var.project}-efs"
  description = "EFS mount targets - NFS from the data tier"
  vpc_id      = aws_vpc.this.id

  ingress {
    description     = "NFS from stateful containers"
    from_port       = 2049
    to_port         = 2049
    protocol        = "tcp"
    security_groups = [aws_security_group.data.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "db" {
  name        = "${var.project}-db"
  description = "RDS MySQL outbox - accept only from app tasks"
  vpc_id      = aws_vpc.this.id

  ingress {
    description     = "MySQL from srv_ingest / srv_pub"
    from_port       = 3306
    to_port         = 3306
    protocol        = "tcp"
    security_groups = [aws_security_group.app.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
