# Superset

Connects to Spark via the Spark Thrift Server (HiveServer2-compatible), not a
new query engine. Register the database connection in the Superset UI
(admin/admin) with this SQLAlchemy URI:

```
hive://spark-thrift:10000/default
```

`spark-thrift` runs with `hive.server2.authentication` unset, i.e. NONE —
but HiveServer2's NONE mode still expects a SASL PLAIN handshake, not a raw
socket. pyhive's default auth mode does that; adding `?auth=NOSASL` skips
SASL entirely and the server just drops the connection
(`TTransportException: TSocket read 0 bytes`) — verified against the live
container, don't add it back.

Build dashboards against the `wide_streams` dbt model
(`dbt/my_project/models/wide_streams.sql`) — the pre-joined denormalized
table meant for BI tools. It carries both `state` (full name) and
`stateCode` (postal abbreviation) — use `stateCode` for map charts, since
Superset's US-states choropleth keys off postal codes.

## Provisioning the Streamify dashboard

`provision_dashboard.py` recreates the original Power BI dashboard (KPIs,
top songs/artists tables, gender split, user-activity-per-state map, and a
streams-over-time line chart) via the Superset REST API. Idempotent — safe
to re-run.

Run it once Superset and `spark-thrift` are up, and after a dbt run has
materialized `wide_streams`:

```
docker compose exec superset python /app/provision_dashboard.py
```

It prints the dashboard URL on success. Not scripted: the "State Filter"
and "Date Range" filter widgets from the original — add them via
**Edit Dashboard > Filters** in the UI (a couple of clicks) rather than a
hand-built `json_metadata` filter payload.
