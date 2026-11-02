from airflow.decorators import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param


# Manual-trigger dbt run, separate from full_pipeline. You fill the form when
# you press "Trigger": which dbt command, which models, full refresh or not.
# dbt runs inside the airflow container (own venv, see airflow/Dockerfile) and
# connects to spark-thrift; ./dbt is mounted at /opt/airflow/dbt.
@dag(
    schedule=None,
    start_date=None,
    catchup=False,
    params={
        'command': Param('run', enum=['run', 'build', 'seed', 'test', 'snapshot'],
                         description='dbt command'),
        'select': Param('', type='string',
                        description='--select value, e.g. dim_users+ or fact_streams wide_streams. Empty = all.'),
        'full_refresh': Param(False, type='boolean', description='Add --full-refresh'),
        'extra_args': Param('', type='string', description='Any other dbt flags, e.g. --vars "{a: 1}"'),
    },
)
def dbt_adhoc():
    # shortcut: params go into the shell unquoted (trusted internal users only), quote them if exposed more widely.
    BashOperator(
        task_id='dbt_adhoc',
        pool='thrift_pool',
        bash_command='cd /opt/airflow/dbt/my_project && '
                     '/home/airflow/dbt-venv/bin/dbt {{ params.command }} '
                     '{% if params.select %}--select {{ params.select }} {% endif %}'
                     '{% if params.full_refresh %}--full-refresh {% endif %}'
                     '{{ params.extra_args }} --no-populate-cache',
    )
dbt_adhoc()
