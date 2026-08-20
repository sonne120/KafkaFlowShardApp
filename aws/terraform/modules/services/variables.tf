variable "project" {
  type = string
}

variable "region" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "public_subnet_ids" {
  type = list(string)
}

variable "private_subnet_ids" {
  type = list(string)
}

variable "nlb_sg_id" {
  type = string
}

variable "yarp_sg_id" {
  type = string
}

variable "app_sg_id" {
  type = string
}

variable "data_sg_id" {
  type = string
}

variable "efs_sg_id" {
  type = string
}

variable "vpclink_sg_id" {
  type = string
}

variable "db_conn_secret_arn" {
  type = string
}

variable "pg_password_secret_arn" {
  type = string
}

variable "pg_conn_secret_arn" {
  type = string
}

variable "image_tag" {
  type = string
}

variable "yarp_desired_count" {
  type = number
}

variable "ingest_desired_count" {
  type = number
}

variable "sub_desired_count" {
  type = number
}
