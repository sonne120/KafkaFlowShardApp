variable "project" {
  description = "Project name used as a prefix for all resources"
  type        = string
  default     = "packetshard"
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "eu-central-1"
}

variable "image_tag" {
  description = "Tag of the images pushed to ECR"
  type        = string
  default     = "latest"
}

variable "yarp_desired_count" {
  description = "Number of YARP gateway tasks behind the NLB"
  type        = number
  default     = 2
}

variable "ingest_desired_count" {
  description = "Number of srv_ingest tasks registered in Cloud Map"
  type        = number
  default     = 3
}

variable "sub_desired_count" {
  description = "Number of srv_sub consumers (one per SnapshotTopic partition)"
  type        = number
  default     = 5
}
