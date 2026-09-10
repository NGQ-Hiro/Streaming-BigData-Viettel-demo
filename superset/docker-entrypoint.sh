#!/bin/bash
# One-shot init (metadata DB migration, dev admin user, default roles/examples-off
# init) then launch the webserver. Superset's stock image ships none of this
# wired up automatically, so it has to happen somewhere before first use.
set -e

superset db upgrade
superset fab create-admin \
  --username admin --password admin \
  --firstname Superset --lastname Admin --email admin@superset.local \
  || true
superset init

exec superset run -h 0.0.0.0 -p 8088
