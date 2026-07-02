from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf


@udf(result_type=DataTypes.STRING())
def string_decode(s):
    if s:
        return (s.encode('latin1')
                  .decode('unicode-escape')
                  .encode('latin1')
                  .decode('utf-8'))
    return s


SNOWFLAKE = {
    "url":      "jdbc:snowflake://YSG-PI88233.snowflakecomputing.com/?db=STREAMIFY&schema=STAGING&warehouse=COMPUTE_WH",
    "username": "HI196732",
    "password": "8wDGncjn",
    "driver":   "net.snowflake.client.jdbc.SnowflakeDriver",
}


def create_env(parallelism=3, checkpoint_interval_ms=10000):
    """
    Creates a Flink StreamExecutionEnvironment and a StreamTableEnvironment.
    The string_decode UDF is registered for latin1 → UTF-8 conversion.

    parallelism matches the Kafka topic partition count (3) so each partition
    is read by its own subtask. Checkpointing is enabled so the Kafka source
    commits consumer-group offsets back to Kafka — that is what kafka-exporter
    and the Grafana "Kafka Lag" dashboard read.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)
    env.enable_checkpointing(checkpoint_interval_ms)
    t_env = StreamTableEnvironment.create(env)
    t_env.create_temporary_function("string_decode", string_decode)
    return env, t_env


def _col_defs(fields):
    return ",\n    ".join(f"`{name}` {ftype}" for name, ftype in fields)


def create_kafka_source(t_env, topic, fields, kafka_broker, group_id="flink-streamify"):
    """Registers a Kafka source table backed by the given topic."""
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE src_{topic} (
            {_col_defs(fields)}
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{topic}',
            'properties.bootstrap.servers' = '{kafka_broker}',
            'properties.group.id'          = '{group_id}',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json',
            'json.ignore-parse-errors'     = 'true'
        )
    """)


def _sink_fields(fields):
    """The transformed columns every sink receives (source cols + derived ts)."""
    return list(fields) + [
        ("ts_timestamp",   "TIMESTAMP(3)"),
        ("year_",          "INT"),
        ("month_",         "INT"),
        ("day_",           "INT"),
        ("hour_",          "INT"),
        ("processed_time", "TIMESTAMP(3)"),
    ]


def create_snowflake_sink(t_env, table_name, fields):
    """Registers a JDBC sink table targeting Snowflake."""
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE sink_{table_name} (
            {_col_defs(_sink_fields(fields))}
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = '{SNOWFLAKE["url"]}',
            'table-name' = '{table_name}',
            'username'   = '{SNOWFLAKE["username"]}',
            'password'   = '{SNOWFLAKE["password"]}',
            'driver'     = '{SNOWFLAKE["driver"]}'
        )
    """)


def create_discard_sink(t_env, table_name, fields):
    """
    Registers a blackhole sink: the full transform still runs, but every row is
    thrown away instead of being written anywhere. Handy for exercising the
    pipeline (and its metrics) without a downstream system.
    """
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE sink_{table_name} (
            {_col_defs(_sink_fields(fields))}
        ) WITH (
            'connector' = 'blackhole'
        )
    """)


def build_insert_sql(topic, fields, table_name, decode_strings=False):
    """Returns the INSERT INTO … SELECT … SQL for one topic → Snowflake table."""
    parts = []
    for name, _ in fields:
        if decode_strings and name in ("artist", "song"):
            parts.append(f"string_decode(`{name}`) AS `{name}`")
        else:
            parts.append(f"`{name}`")
    cols = ",\n    ".join(parts)
    return f"""
        INSERT INTO sink_{table_name}
        SELECT
            {cols},
            CAST(TO_TIMESTAMP_LTZ(`ts` / 1000, 3) AS TIMESTAMP(3))     AS ts_timestamp,
            CAST(YEAR(TO_TIMESTAMP_LTZ(`ts` / 1000, 3)) AS INT)       AS year_,
            CAST(MONTH(TO_TIMESTAMP_LTZ(`ts` / 1000, 3)) AS INT)      AS month_,
            CAST(DAYOFMONTH(TO_TIMESTAMP_LTZ(`ts` / 1000, 3)) AS INT) AS day_,
            CAST(HOUR(TO_TIMESTAMP_LTZ(`ts` / 1000, 3)) AS INT)       AS hour_,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))                    AS processed_time
        FROM src_{topic}
    """
