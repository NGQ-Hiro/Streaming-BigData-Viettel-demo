output "instance_public_ip" {
  description = "Public IP of the EC2 instance running the docker-compose stack."
  value       = aws_instance.lakehouse.public_ip
}

output "s3_warehouse_bucket" {
  description = "Name of the S3 bucket used as the Iceberg warehouse."
  value       = aws_s3_bucket.warehouse.bucket
}

output "glue_database" {
  description = "Bootstrap Glue Data Catalog database name."
  value       = aws_glue_catalog_database.staging.name
}
