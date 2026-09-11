#!/bin/sh
# Shared by spark_streaming/Dockerfile and dbt/Dockerfile. Renders
# $SPARK_CONF_DIR/spark-defaults.conf (Hudi + Hadoop S3A config) from the
# template matching $LAKE_ENV (rest=dev/MinIO, glue=aws) at container start,
# so one image works in both modes without a rebuild. Plain shell heredoc
# expansion instead of envsubst — nothing but $S3_WAREHOUSE_BUCKET needs
# substituting (glue template) and that package isn't already on either base
# image.
set -e

TEMPLATE="/opt/spark-conf-templates/${LAKE_ENV:-rest}.conf.template"
[ -f "$TEMPLATE" ] || { echo "no template for LAKE_ENV=$LAKE_ENV" >&2; exit 1; }
mkdir -p "$SPARK_CONF_DIR"
eval "cat <<CONFEOF
$(cat "$TEMPLATE")
CONFEOF" > "$SPARK_CONF_DIR/spark-defaults.conf"

exec "$@"
