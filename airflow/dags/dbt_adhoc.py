from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# Manual-trigger dbt run, separate from the daily dbt() DAG.
# dbt-dbt:latest -- pre-built via `docker compose -f dbt/docker-compose.yml build`
@dag(
    schedule=None,
    start_date=None,
    catchup=False
)
def dbt_adhoc():
    run_dbt_adhoc = DockerOperator(
        task_id='dbt_adhoc',
        image='dbt-dbt:latest',
        command=['bash', '-c', 'dbt deps && dbt run --no-populate-cache'],
        working_dir='/usr/app',
        mounts=[
            Mount(source='/home/newuser/Project/streamify/dbt/my_project',
                  target='/usr/app', type='bind'),
            Mount(source='/home/newuser/Project/streamify/dbt',
                  target='/root/.dbt', type='bind'),
        ],
        network_mode='lakehouse-network',
        docker_url='unix://var/run/docker.sock',
        auto_remove='success'
    )
    run_dbt_adhoc
dbt_adhoc()
