from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# Full pipeline: dbt run, then refresh the Superset dataset so its columns
# pick up whatever the dbt run changed.
# dbt-dbt:latest -- pre-built via `docker compose -f dbt/docker-compose.yml build`
@dag(
    schedule=None,
    start_date=None,
    catchup=False
)
def full_pipeline():
    dbt_run = DockerOperator(
        task_id='dbt_run',
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

    # superset is a long-running service (docker-compose.yml), so this execs
    # into it rather than running a fresh container.
    refresh_superset = DockerOperator(
        task_id='refresh_superset',
        image='docker:cli',
        command=['exec', 'superset', 'python', '/app/refresh_dataset.py'],
        # docker:cli needs the socket mounted inside itself to run `docker
        # exec` against the superset container -- docker_url below is only
        # how Airflow launches this container, not what's inside it.
        mounts=[
            Mount(source='/var/run/docker.sock',
                  target='/var/run/docker.sock', type='bind'),
        ],
        docker_url='unix://var/run/docker.sock',
        auto_remove='success'
    )

    dbt_run >> refresh_superset
full_pipeline()
