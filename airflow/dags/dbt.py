from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
@dag(
    schedule="@daily",
    start_date=None,
    catchup=False
)
def dbt():
    run_dbt = DockerOperator(
        task_id='transform_data',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command='run',
        working_dir='/usr/app',
        mounts=[
            Mount(source='/home/newuser/Project/streamify/dbt/my_project',
                  target='/usr/app', type='bind'),
            Mount(source='/home/newuser/Project/streamify/dbt/profiles.yml',
                  target='/root/.dbt/profiles.yml', type='bind')
        ],
        network_mode='streamify_my-network',
        # must set authority for docker to run (sudo chmod 777 var/run/docker.sock)
        docker_url='unix://var/run/docker.sock',
        # remove container after running 
        auto_remove='success'
    )
    run_dbt
dbt()