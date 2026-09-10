#!/bin/bash
set -euxo pipefail

# Docker + Compose plugin (Amazon Linux 2023)
dnf install -y docker git
systemctl enable --now docker
dnf install -y docker-compose-plugin || \
  ( dnf install -y dnf-plugins-core && dnf install -y docker-compose-plugin )

REPO_DIR=/opt/lakehouse-demo
if [ -d "$REPO_DIR/.git" ]; then
  git -C "$REPO_DIR" pull
else
  git clone ${repo_url} "$REPO_DIR"
fi

# Env vars for docker-compose.aws.yml (contract shared with the dbt/spark
# images: ICEBERG_CATALOG_TYPE=glue selects the Glue Spark-conf template).
cat > "$REPO_DIR/.env" <<EOF
AWS_REGION=${aws_region}
S3_WAREHOUSE_BUCKET=${s3_warehouse_bucket}
GLUE_DATABASE=${glue_database}
EOF

cd "$REPO_DIR"
docker compose -f docker-compose.yml -f docker-compose.aws.yml up -d
