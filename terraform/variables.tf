variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "EC2 instance type for the single-node lift-and-shift host."
  type        = string
  default     = "t3.xlarge"
}

variable "allowed_admin_cidr" {
  description = "CIDR allowed to reach SSH (22), Spark master UI (8086), Grafana (3000) and Prometheus (9090). No default — must be set explicitly (e.g. \"1.2.3.4/32\")."
  type        = string
}

variable "repo_url" {
  description = "Git URL of this repo, cloned onto the instance by user_data. No default — must be set explicitly."
  type        = string
}
