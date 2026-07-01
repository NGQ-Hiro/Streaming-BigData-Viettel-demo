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


def create_env(parallelism=1):
    """
    Creates a Flink StreamExecutionEnvironment and a StreamTableEnvironment.
    The string_decode UDF is registered for latin1 → UTF-8 conversion.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)
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


def create_snowflake_sink(t_env, table_name, fields):
    """Registers a JDBC sink table targeting Snowflake."""
    sink_fields = list(fields) + [
        ("ts_timestamp",   "TIMESTAMP(3)"),
        ("year_",          "INT"),
        ("month_",         "INT"),
        ("day_",           "INT"),
        ("hour_",          "INT"),
        ("processed_time", "TIMESTAMP(3)"),
    ]
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE sink_{table_name} (
            {_col_defs(sink_fields)}
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = '{SNOWFLAKE["url"]}',
            'table-name' = '{table_name}',
            'username'   = '{SNOWFLAKE["username"]}',
            'password'   = '{SNOWFLAKE["password"]}',
            'driver'     = '{SNOWFLAKE["driver"]}'
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
            TO_TIMESTAMP_LTZ(`ts` / 1000, 3)             AS ts_timestamp,
            YEAR(TO_TIMESTAMP_LTZ(`ts` / 1000, 3))       AS year_,
            MONTH(TO_TIMESTAMP_LTZ(`ts` / 1000, 3))      AS month_,
            DAYOFMONTH(TO_TIMESTAMP_LTZ(`ts` / 1000, 3)) AS day_,
            HOUR(TO_TIMESTAMP_LTZ(`ts` / 1000, 3))       AS hour_,
            CURRENT_TIMESTAMP                             AS processed_time
        FROM src_{topic}
    """
