# VPC basics for one environment's Databricks workspace (customer-managed VPC, ENV-01).
# Serverless compute (the default for jobs, pipelines and SQL) runs in Databricks' account and does not use this
# VPC. Classic clusters do, and need outbound internet: set enable_nat = true for them (a NAT gateway costs ~35 USD a
# month, so it is off by default). S3 traffic goes through a free gateway endpoint either way.

terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

variable "name_prefix" { type = string }
variable "env" { type = string }
variable "cidr" { type = string }

variable "azs" {
  description = "Two availability zones (Databricks needs subnets in two)."
  type        = list(string)
}

variable "enable_nat" {
  type    = bool
  default = false
}

locals {
  name = "${var.name_prefix}-${var.env}"
}

resource "aws_vpc" "this" {
  cidr_block           = var.cidr
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags                 = { Name = local.name }
}

resource "aws_subnet" "private" {
  count             = length(var.azs)
  vpc_id            = aws_vpc.this.id
  cidr_block        = cidrsubnet(var.cidr, 3, count.index)
  availability_zone = var.azs[count.index]
  tags              = { Name = "${local.name}-private-${count.index}" }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.this.id
  tags   = { Name = "${local.name}-private" }
}

resource "aws_route_table_association" "private" {
  count          = length(aws_subnet.private)
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${data.aws_region.current.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]
  tags              = { Name = "${local.name}-s3" }
}

data "aws_region" "current" {}

resource "aws_security_group" "workspace" {
  name        = "${local.name}-workspace"
  description = "Databricks classic compute: nodes talk to each other; outbound to the control plane."
  vpc_id      = aws_vpc.this.id
}

resource "aws_vpc_security_group_ingress_rule" "self" {
  security_group_id            = aws_security_group.workspace.id
  referenced_security_group_id = aws_security_group.workspace.id
  ip_protocol                  = "-1"
  description                  = "cluster nodes to each other"
}

resource "aws_vpc_security_group_egress_rule" "self" {
  security_group_id            = aws_security_group.workspace.id
  referenced_security_group_id = aws_security_group.workspace.id
  ip_protocol                  = "-1"
  description                  = "cluster nodes to each other"
}

resource "aws_vpc_security_group_egress_rule" "https" {
  security_group_id = aws_security_group.workspace.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  description       = "control plane, S3 and AWS APIs over TLS"
}

# --- optional internet egress for classic clusters ------------------------------------------------------------
resource "aws_internet_gateway" "this" {
  count  = var.enable_nat ? 1 : 0
  vpc_id = aws_vpc.this.id
}

resource "aws_subnet" "public" {
  count             = var.enable_nat ? 1 : 0
  vpc_id            = aws_vpc.this.id
  cidr_block        = cidrsubnet(var.cidr, 3, 7)
  availability_zone = var.azs[0]
  tags              = { Name = "${local.name}-public" }
}

resource "aws_route_table" "public" {
  count  = var.enable_nat ? 1 : 0
  vpc_id = aws_vpc.this.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this[0].id
  }
}

resource "aws_route_table_association" "public" {
  count          = var.enable_nat ? 1 : 0
  subnet_id      = aws_subnet.public[0].id
  route_table_id = aws_route_table.public[0].id
}

resource "aws_eip" "nat" {
  count  = var.enable_nat ? 1 : 0
  domain = "vpc"
}

resource "aws_nat_gateway" "this" {
  count         = var.enable_nat ? 1 : 0
  allocation_id = aws_eip.nat[0].id
  subnet_id     = aws_subnet.public[0].id
}

resource "aws_route" "private_nat" {
  count                  = var.enable_nat ? 1 : 0
  route_table_id         = aws_route_table.private.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.this[0].id
}

output "vpc_id" { value = aws_vpc.this.id }
output "subnet_ids" { value = aws_subnet.private[*].id }
output "security_group_id" { value = aws_security_group.workspace.id }
