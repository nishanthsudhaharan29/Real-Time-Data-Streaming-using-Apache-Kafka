#!/bin/bash
set -e

if [ -f "/requirements.txt" ]; then
  pip install --no-cache-dir -r /requirements.txt
fi

airflow db init

airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || true

exec "$@"
