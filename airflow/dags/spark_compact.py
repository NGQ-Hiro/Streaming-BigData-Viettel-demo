import pendulum
from airflow.decorators import dag, task

# Same list/path as the old compact_bronze.py (bronze Hudi MOR tables).
TOPICS = ["listen_events", "page_view_events", "auth_events"]
WAREHOUSE = "s3a://lakehouse"
# Hudi's default lock (file-system based) cannot work on s3a ("fs can not support
# atomic creation"), so compaction failed. Use the in-process lock instead.
# shortcut: this lock only guards inside the thrift JVM, not against the streaming
# writer (another process). Fine for a demo; use a Hive-metastore/Zookeeper lock in prod.
LOCK_OPTS = (
    "hoodie.write.concurrency.mode=optimistic_concurrency_control,"
    "hoodie.write.lock.provider=org.apache.hudi.client.transaction.lock.InProcessLockProvider"
)


# Hudi compaction 4x a day, run as plain SQL through spark-thrift: it uses
# thrift's own cores, so no new Spark app and no free slots needed.
# pool thrift_pool (1 slot, created in docker-compose.yml) makes it run one
# at a time with dbt / Superset refresh, which use thrift too.
@dag(
    schedule='0 */6 * * *',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    max_active_runs=1,
)
def spark_compact():
    @task(pool='thrift_pool', retries=1)
    def compact():
        from pyhive import hive

        conn = hive.connect('spark-thrift', port=10000, username='airflow', auth='NONE')
        try:
            cur = conn.cursor()
            for topic in TOPICS:
                cur.execute(f"CALL run_compaction(op => 'run', path => '{WAREHOUSE}/staging/{topic}', options => '{LOCK_OPTS}')")
                # [] = nothing to compact; otherwise one row per compaction instant
                print(f"{topic}: {cur.fetchall()}")
        finally:
            conn.close()

    compact()
spark_compact()
