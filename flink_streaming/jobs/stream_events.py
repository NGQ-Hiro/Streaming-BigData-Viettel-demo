from streaming_functions import create_env, create_kafka_source, create_snowflake_sink, build_insert_sql
from schema import SCHEMAS

KAFKA_BROKER = "broker:29092"

# (kafka_topic, snowflake_table, decode_latin1_strings)
TOPICS = [
    ("listen_events",    "LISTEN_EVENTS",    True),
    ("page_view_events", "PAGE_VIEW_EVENTS", True),
    ("auth_events",      "AUTH_EVENTS",      False),
]

env, t_env = create_env()

for topic, table_name, _ in TOPICS:
    create_kafka_source(t_env, topic, SCHEMAS[topic], KAFKA_BROKER)
    create_snowflake_sink(t_env, table_name, SCHEMAS[topic])

stmt_set = t_env.create_statement_set()
for topic, table_name, decode_strings in TOPICS:
    stmt_set.add_insert_sql(build_insert_sql(topic, SCHEMAS[topic], table_name, decode_strings))

stmt_set.execute().wait()

# Run in flink-jobmanager container as:
# docker exec -it flink-jobmanager flink run -py /opt/flink/jobs/stream_events.py
