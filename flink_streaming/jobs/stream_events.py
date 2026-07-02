import os

from streaming_functions import (
    create_env,
    create_kafka_source,
    create_snowflake_sink,
    create_discard_sink,
    build_insert_sql,
)
from schema import SCHEMAS

KAFKA_BROKER = "broker-1:29092,broker-2:29092,broker-3:29092"

# TEMPORARY: transform but throw the results away (blackhole sink) instead of
# writing to Snowflake. Set back to False to restore the Snowflake sink.
DISCARD_OUTPUT = True

# (kafka_topic, snowflake_table, decode_latin1_strings)
TOPICS = [
    ("listen_events",    "LISTEN_EVENTS",    True),
    ("page_view_events", "PAGE_VIEW_EVENTS", True),
    ("auth_events",      "AUTH_EVENTS",      False),
]

env, t_env = create_env()

for topic, table_name, _ in TOPICS:
    create_kafka_source(t_env, topic, SCHEMAS[topic], KAFKA_BROKER)
    if DISCARD_OUTPUT:
        create_discard_sink(t_env, table_name, SCHEMAS[topic])
    else:
        create_snowflake_sink(t_env, table_name, SCHEMAS[topic])

stmt_set = t_env.create_statement_set()
for topic, table_name, decode_strings in TOPICS:
    stmt_set.add_insert_sql(build_insert_sql(topic, SCHEMAS[topic], table_name, decode_strings))

result = stmt_set.execute()

# When submitted detached (auto-submitter uses `flink run -d`), the job keeps
# running on the cluster after the client exits, and wait() is not valid.
# For a manual attached run, wait() blocks so the job stays live.
if os.environ.get("FLINK_DETACHED", "").lower() not in ("1", "true", "yes"):
    result.wait()

# Run manually in the flink-jobmanager container as:
# docker exec -it flink-jobmanager flink run -py /opt/flink/jobs/stream_events.py
# It is also submitted automatically at startup by the flink-job-submitter service.
