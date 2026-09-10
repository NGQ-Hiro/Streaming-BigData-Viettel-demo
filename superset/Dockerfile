FROM apache/superset

# Hive/Spark Thrift driver stack. Stock Superset image has no Hive dialect
# baked in, and pyhive needs the sasl transport package too even though this
# cluster runs auth NONE (see README in this dir for the connection URI).
USER root
# pyhive[hive]'s `sasl` dep compiles a C extension against libsasl2 --
# needs a compiler + headers, neither in the base image. Superset itself
# runs out of the /app/.venv uv-managed venv, not the system python, so the
# driver has to land there (`uv pip install`) or Superset never sees it.
RUN apt-get update && apt-get install -y --no-install-recommends build-essential libsasl2-dev \
    && uv pip install --python /app/.venv/bin/python pyhive[hive] thrift thrift-sasl \
    && apt-get purge -y --auto-remove build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
COPY provision_dashboard.py /app/provision_dashboard.py
RUN chmod +x /app/docker-entrypoint.sh
USER superset

ENTRYPOINT ["/app/docker-entrypoint.sh"]
