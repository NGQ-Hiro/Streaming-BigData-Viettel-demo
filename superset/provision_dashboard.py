"""
Provisions the "Streamify" dashboard in Superset: the Spark database
connection, the wide_streams dataset, one chart per panel in the original
Power BI dashboard, and a dashboard that lays them out in a grid.

Idempotent: re-running finds existing objects by name and reuses their id
instead of creating duplicates.

Run once, after `docker compose up superset spark-thrift` (and after the
dbt run that materializes wide_streams):

    docker compose exec superset python /app/provision_dashboard.py

# ponytail: layout is a fixed 3-row grid built from ROW_SPECS below, not a
# drag-and-drop copy of the screenshot. Native filters (state dropdown, date
# range) aren't scripted -- add them via Edit Dashboard > Filters in the UI,
# a couple of clicks, versus a json_metadata payload that's easy to get
# subtly wrong and hard to debug. Upgrade path: script native filters too
# once the chart payload shapes above are confirmed against a live instance.
"""
import os
import sys

import requests

BASE_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
USERNAME = os.environ.get("SUPERSET_USERNAME", "admin")
PASSWORD = os.environ.get("SUPERSET_PASSWORD", "admin")

DATABASE_NAME = "Spark (spark-thrift)"
# `hive.server2.authentication` isn't set in compose, so HiveServer2 defaults
# to NONE -- but that still expects a SASL PLAIN handshake, not a raw socket.
# pyhive's default auth mode (omit `auth=`) does that; `auth=NOSASL` skips
# SASL entirely and the server just drops the connection (verified live).
SQLALCHEMY_URI = "hive://spark-thrift:10000/default"
TABLE_NAME = "wide_streams"
TABLE_SCHEMA = "analytics"
DASHBOARD_TITLE = "Streamify"


def login(session: requests.Session) -> None:
    resp = session.post(
        f"{BASE_URL}/api/v1/security/login",
        json={"username": USERNAME, "password": PASSWORD, "provider": "db", "refresh": True},
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    session.headers.update({"Authorization": f"Bearer {token}"})

    csrf = session.get(f"{BASE_URL}/api/v1/security/csrf_token/")
    csrf.raise_for_status()
    session.headers.update(
        {"X-CSRFToken": csrf.json()["result"], "Referer": BASE_URL, "Content-Type": "application/json"}
    )


def find_by_name(session: requests.Session, endpoint: str, name_field: str, name: str):
    resp = session.get(f"{BASE_URL}/api/v1/{endpoint}/", params={"page_size": 100})
    resp.raise_for_status()
    for row in resp.json().get("result", []):
        if row.get(name_field) == name:
            return row["id"]
    return None


def get_or_create_database(session: requests.Session) -> int:
    existing = find_by_name(session, "database", "database_name", DATABASE_NAME)
    if existing:
        return existing
    resp = session.post(
        f"{BASE_URL}/api/v1/database/",
        json={
            "database_name": DATABASE_NAME,
            "sqlalchemy_uri": SQLALCHEMY_URI,
            "expose_in_sqllab": True,
        },
    )
    resp.raise_for_status()
    return resp.json()["id"]


def get_or_create_dataset(session: requests.Session, database_id: int) -> int:
    existing = find_by_name(session, "dataset", "table_name", TABLE_NAME)
    if existing:
        return existing
    resp = session.post(
        f"{BASE_URL}/api/v1/dataset/",
        json={"database": database_id, "table_name": TABLE_NAME, "schema": TABLE_SCHEMA},
    )
    resp.raise_for_status()
    return resp.json()["id"]


def adhoc_metric(sql_expr: str, label: str) -> dict:
    return {
        "expressionType": "SQL",
        "sqlExpression": sql_expr,
        "label": label,
        "optionName": f"metric_{label.lower().replace(' ', '_')}",
    }


def get_or_create_chart(session: requests.Session, dataset_id: int, name: str, viz_type: str, params: dict) -> int:
    import json as _json

    full_params = _json.dumps({**params, "viz_type": viz_type, "datasource": f"{dataset_id}__table"})
    existing = find_by_name(session, "chart", "slice_name", name)
    if existing:
        # Sync params/viz_type on every run -- otherwise editing build_charts()
        # below and re-running silently does nothing to already-created charts.
        resp = session.put(
            f"{BASE_URL}/api/v1/chart/{existing}",
            json={"viz_type": viz_type, "params": full_params},
        )
        resp.raise_for_status()
        return existing

    payload = {
        "slice_name": name,
        "viz_type": viz_type,
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "params": full_params,
    }
    resp = session.post(f"{BASE_URL}/api/v1/chart/", json=payload)
    resp.raise_for_status()
    return resp.json()["id"]


def build_charts(session: requests.Session, dataset_id: int) -> dict:
    charts = {}

    charts["total_streams"] = get_or_create_chart(
        session, dataset_id, "Streams", "big_number_total",
        {"metric": adhoc_metric("COUNT(*)", "Streams"), "adhoc_filters": []},
    )
    charts["total_users"] = get_or_create_chart(
        session, dataset_id, "Total Users", "big_number_total",
        {"metric": adhoc_metric("COUNT(DISTINCT userId)", "Total Users"), "adhoc_filters": []},
    )
    charts["activity_per_state"] = get_or_create_chart(
        session, dataset_id, "Listen Events per State", "country_map",
        {
            # country_map's "entity" is matched against the USA shapefile's
            # ISO 3166-2 codes ("US-AZ", not bare "AZ") -- verified via the
            # bundled geojson properties. stateCode only has the bare postal
            # code, so build the ISO form with an adhoc SQL groupby column
            # rather than adding a column upstream in dbt for one chart.
            "entity": {
                "sqlExpression": "concat('US-', stateCode)",
                "label": "state_iso",
                "expressionType": "SQL",
            },
            # SUPERSET_DEFAULT is a categorical scheme, not a gradient -- for
            # a choropleth (color intensity = density) it needs one of the
            # actual sequential scheme ids (verified via the frontend bundle's
            # scheme registry, e.g. schemeBlues/schemeYlOrRd/blue_white_yellow).
            "linear_color_scheme": "schemeBlues",
            "metric": adhoc_metric("COUNT(*)", "Streams"),
            "select_country": "usa",
            "adhoc_filters": [],
        },
    )
    charts["chart_busters"] = get_or_create_chart(
        session, dataset_id, "Chart Busters", "table",
        {
            "groupby": ["songName"],
            "metrics": [adhoc_metric("COUNT(*)", "Streams")],
            "row_limit": 10,
            "order_desc": True,
            "adhoc_filters": [],
        },
    )
    charts["top_artists"] = get_or_create_chart(
        session, dataset_id, "Top Artists", "table",
        {
            "groupby": ["artistName"],
            "metrics": [adhoc_metric("COUNT(*)", "Streams")],
            "row_limit": 10,
            "order_desc": True,
            "adhoc_filters": [],
        },
    )
    charts["gender_distribution"] = get_or_create_chart(
        session, dataset_id, "Gender Distribution", "pie",
        {
            "groupby": ["gender"],
            "metric": adhoc_metric("COUNT(*)", "Streams"),
            "adhoc_filters": [],
        },
    )
    charts["user_level_by_gender"] = get_or_create_chart(
        # dist_bar is an NVD3 legacy viz_type, not registered in this
        # Superset version ("Item with key 'dist_bar' is not registered",
        # confirmed live) -- echarts_timeseries_bar is the modern equivalent
        # and works fine with a categorical (non-temporal) x_axis.
        session, dataset_id, "User Level By Gender", "echarts_timeseries_bar",
        {
            "x_axis": "gender",
            "x_axis_sort_asc": True,
            "groupby": ["level"],
            "metrics": [adhoc_metric("COUNT(DISTINCT userId)", "Users")],
            "row_limit": 100,
            "adhoc_filters": [],
        },
    )
    charts["user_activity"] = get_or_create_chart(
        session, dataset_id, "User Activity", "echarts_timeseries_line",
        {
            "x_axis": "dateHour",
            "time_grain_sqla": "PT1H",
            "metrics": [adhoc_metric("COUNT(*)", "Streams")],
            "adhoc_filters": [],
        },
    )
    return charts


# (row height, [(chart_key, width)]) -- widths sum to 12 per row (Superset's
# grid unit). KPI widths were 4+4, leaving 4 columns of dead white space
# (visible live) -- 6+6 fills the row. Heights are tuned per row instead of
# a flat 50 so the two-line KPI cards don't waste vertical space and the map
# gets more room to breathe.
ROW_SPECS = [
    (25, [("total_streams", 6), ("total_users", 6)]),
    (60, [("activity_per_state", 6), ("chart_busters", 3), ("top_artists", 3)]),
    (50, [("gender_distribution", 4), ("user_level_by_gender", 4), ("user_activity", 4)]),
]


def build_position_json(chart_ids: dict, chart_names: dict) -> dict:
    layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }
    for row_idx, (height, row) in enumerate(ROW_SPECS):
        row_id = f"ROW-{row_idx}"
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "parents": ["ROOT_ID", "GRID_ID"],
        }
        for chart_key, width in row:
            chart_id = chart_ids[chart_key]
            node_id = f"CHART-{chart_id}"
            layout[row_id]["children"].append(node_id)
            layout[node_id] = {
                "type": "CHART",
                "id": node_id,
                "children": [],
                "meta": {
                    "chartId": chart_id,
                    "width": width,
                    "height": height,
                    "sliceName": chart_names[chart_key],
                },
                "parents": ["ROOT_ID", "GRID_ID", row_id],
            }
    return layout


def get_or_create_dashboard(session: requests.Session, chart_ids: dict, chart_names: dict) -> int:
    import json as _json

    existing = find_by_name(session, "dashboard", "dashboard_title", DASHBOARD_TITLE)
    position_json = _json.dumps(build_position_json(chart_ids, chart_names))
    payload = {
        "dashboard_title": DASHBOARD_TITLE,
        "position_json": position_json,
        "published": True,
    }
    if existing:
        resp = session.put(f"{BASE_URL}/api/v1/dashboard/{existing}", json=payload)
        resp.raise_for_status()
        dashboard_id = existing
    else:
        resp = session.post(f"{BASE_URL}/api/v1/dashboard/", json=payload)
        resp.raise_for_status()
        dashboard_id = resp.json()["id"]

    # Attach every chart to the dashboard (separate from layout placement).
    # PUT, not POST -- POST /api/v1/chart/{id} isn't a valid route (405) and
    # silently leaves charts unassociated, which is what a bare `except` or
    # unchecked call here would have hidden (verified live).
    for chart_id in chart_ids.values():
        resp = session.put(f"{BASE_URL}/api/v1/chart/{chart_id}", json={"dashboards": [dashboard_id]})
        resp.raise_for_status()
    return dashboard_id


def main() -> None:
    session = requests.Session()
    login(session)

    database_id = get_or_create_database(session)
    dataset_id = get_or_create_dataset(session, database_id)
    chart_ids = build_charts(session, dataset_id)
    chart_names = {
        "total_streams": "Streams",
        "total_users": "Total Users",
        "activity_per_state": "Listen Events per State",
        "chart_busters": "Chart Busters",
        "top_artists": "Top Artists",
        "gender_distribution": "Gender Distribution",
        "user_level_by_gender": "User Level By Gender",
        "user_activity": "User Activity",
    }
    dashboard_id = get_or_create_dashboard(session, chart_ids, chart_names)

    print(f"Dashboard ready: {BASE_URL}/superset/dashboard/{dashboard_id}/")


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as exc:
        print(f"Superset API error: {exc.response.status_code} {exc.response.text}", file=sys.stderr)
        raise
