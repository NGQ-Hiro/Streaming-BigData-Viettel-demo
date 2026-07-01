import snowflake.connector

# (Recommended) read secrets from env vars
ACCOUNT   = "....."        # e.g. "xy12345.ap-southeast-1"
USER      = "HI196732"
PASSWORD  = "....."
ROLE      = "ACCOUNTADMIN"     # needs create privileges
WAREHOUSE = "COMPUTE_WH"

DB_NAME     = "STREAMIFY"
SCHEMA_NAME = "STAGING"

DDL_TABLE_LISTEN_EVENT = f"""
CREATE TABLE IF NOT EXISTS {DB_NAME}.{SCHEMA_NAME}.LISTEN_EVENTS (
    artist       STRING,
    song         STRING,
    duration     DOUBLE,
    ts           TIMESTAMP,        -- use TIMESTAMP_NTZ if you prefer
    sessionid    INTEGER,
    auth         STRING,
    level        STRING,
    itemInSession INTEGER,
    city         STRING,
    zip          INTEGER,
    state        STRING,
    userAgent    STRING,
    lon          DOUBLE,
    lat          DOUBLE,
    userId       BIGINT,
    lastName     STRING,
    firstName    STRING,
    gender       STRING,
    registration BIGINT,
    year         INTEGER,
    month        INTEGER,
    day          INTEGER,
    hour         INTEGER,
    processed_time TIMESTAMP
);
"""
DDL_TABLE_PAGE_VIEW_EVENTS = f"""
CREATE TABLE IF NOT EXISTS {DB_NAME}.{SCHEMA_NAME}.PAGE_VIEW_EVENTS (
    ts TIMESTAMP,
    sessionId INTEGER,
    page STRING,
    auth STRING,
    method STRING,
    status INTEGER,
    level STRING,
    itemInSession INTEGER,
    city STRING,
    zip INTEGER,
    state STRING,
    userAgent STRING,
    lon DOUBLE,
    lat DOUBLE,
    userId INTEGER,
    lastName STRING,
    firstName STRING,
    gender STRING,
    registration BIGINT,
    artist STRING,
    song STRING,
    duration DOUBLE,
    year         INTEGER,
    month        INTEGER,
    day          INTEGER,
    hour         INTEGER,
    processed_time TIMESTAMP
);
"""
DDL_TABLE_AUTH_EVENTS = f"""
CREATE TABLE IF NOT EXISTS {DB_NAME}.{SCHEMA_NAME}.AUTH_EVENTS (
    ts TIMESTAMP,
    sessionId INTEGER,
    level STRING,
    itemInSession INTEGER,
    city STRING,
    zip INTEGER,
    state STRING,
    userAgent STRING,
    lon DOUBLE,
    lat DOUBLE,
    userId INTEGER,
    lastName STRING,
    firstName STRING,
    gender STRING,
    registration BIGINT,
    success BOOLEAN,
    year         INTEGER,
    month        INTEGER,
    day          INTEGER,
    hour         INTEGER,
    processed_time TIMESTAMP
);
"""
conn = snowflake.connector.connect(
    account   = ACCOUNT,
    user      = USER,
    password  = PASSWORD,
    role      = ROLE,
    warehouse = WAREHOUSE,
    autocommit=True,   # convenient for DDL
)

try:
    cur = conn.cursor()
    # Create database & schema (idempotent)
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cur.execute(f"USE DATABASE {DB_NAME}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS DIM")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_NAME}.{SCHEMA_NAME}")
    # Create table
    cur.execute(DDL_TABLE_LISTEN_EVENT)
    cur.execute(DDL_TABLE_PAGE_VIEW_EVENTS)
    cur.execute(DDL_TABLE_AUTH_EVENTS)
    print("✅ Database, schema, and table ensured.")
finally:
    cur.close()
    conn.close()
