import pendulum
from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator


# Full pipeline: dbt run, then refresh the Superset dataset so its columns
# pick up whatever the dbt run changed.
@dag(
    schedule='*/30 * * * *',  # every 30 minutes (UTC)
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    max_active_runs=1,  # a slow run must not overlap the next one
)
def full_pipeline():
    # dbt runs inside the airflow container (own venv, see airflow/Dockerfile)
    # and connects to spark-thrift; ./dbt is mounted at /opt/airflow/dbt.
    dbt_run = BashOperator(
        task_id='dbt_run',
        pool='thrift_pool',
        bash_command='cd /opt/airflow/dbt/my_project && '
                     '/home/airflow/dbt-venv/bin/dbt seed --select state_codes --no-populate-cache && '
                     # all models are incremental or cheap: no full refresh (do it by hand after a schema change).
                     '/home/airflow/dbt-venv/bin/dbt run --no-populate-cache',
    )

    # Plain HTTP calls to the Superset API (script only needs `requests`);
    # ./superset is mounted at /opt/airflow/superset.
    refresh_superset = BashOperator(
        task_id='refresh_superset',
        pool='thrift_pool',
        env={'SUPERSET_URL': 'http://superset:8088'},
        append_env=True,
        # provision is idempotent: creates the dataset/dashboard on a fresh stack, no-op after.
        bash_command='cd /opt/airflow/superset && python provision_dashboard.py && python refresh_dataset.py',
    )

    dbt_run >> refresh_superset
full_pipeline()
