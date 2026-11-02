FROM apache/airflow:3.0.3
# dbt lives in its own venv so its pins can't clash with Airflow's packages.
# dbt talks to spark-thrift (method: thrift), same as dbt/Dockerfile.
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-spark[PyHive]==1.11.0" thrift_sasl

# dbt packages (dbt_utils) are installed at build time, outside the ./dbt
# folder that compose mounts at runtime. The env var points dbt at them.
# `dbt_dir` is an extra build context set in docker-compose.yml.
ENV DBT_PACKAGES_INSTALL_PATH=/home/airflow/dbt_packages
COPY --from=dbt_dir --chown=airflow:root dbt_project.yml packages.yml package-lock.yml /tmp/dbt/
RUN cd /tmp/dbt && /home/airflow/dbt-venv/bin/dbt deps && rm -rf /tmp/dbt
