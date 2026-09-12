"""Refreshes the wide_streams dataset's column metadata in Superset after a
dbt run (picks up any schema change without re-provisioning charts/dashboard).

    docker compose exec superset python /app/refresh_dataset.py
"""
import sys

import requests

from provision_dashboard import BASE_URL, TABLE_NAME, find_by_name, login


def main() -> None:
    session = requests.Session()
    login(session)

    dataset_id = find_by_name(session, "dataset", "table_name", TABLE_NAME)
    if dataset_id is None:
        sys.exit(f"dataset {TABLE_NAME!r} not found -- run provision_dashboard.py first")

    resp = session.put(f"{BASE_URL}/api/v1/dataset/{dataset_id}/refresh")
    resp.raise_for_status()
    print(f"refreshed dataset {TABLE_NAME!r} (id={dataset_id})")


if __name__ == "__main__":
    main()
