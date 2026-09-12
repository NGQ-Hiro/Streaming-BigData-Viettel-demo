from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


# Manual-trigger Hudi compaction for the bronze (staging.<topic>) MOR
# ingestion tables -- merges accumulated delta logs into base files.
@dag(
    schedule=None,
    start_date=None,
    catchup=False
)
def spark_compact():
    run_spark_compact = DockerOperator(
        task_id='spark_compact',
        image='spark-lakehouse:latest',
        command=[
            'spark-submit',
            '--master', 'spark://spark-master:7077',
            '--conf', 'spark.cores.max=2',
            '/opt/spark/jobs/compact_bronze.py',
        ],
        mounts=[
            Mount(source='/home/newuser/Project/streamify/spark_streaming/jobs',
                  target='/opt/spark/jobs', type='bind'),
        ],
        network_mode='lakehouse-network',
        docker_url='unix://var/run/docker.sock',
        auto_remove='success'
    )
    run_spark_compact
spark_compact()
