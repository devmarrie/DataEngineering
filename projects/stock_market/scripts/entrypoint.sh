#!/bin/bash
export GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}
export AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT=${AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT}

set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install --user -r requirements.txt
fi



# Initialize database if needed
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init
fi

# Ensure PostgreSQL database is ready (optional but recommended)
# until psql -h postgres -U airflow -c '\q'; do
#   >&2 echo "PostgreSQL is unavailable - sleeping"
#   sleep 1
# done

# Create Airflow user (if not already existing)
if ! airflow users list | grep -q '^admin'; then
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

# Upgrade database schema
$(command -v airflow) db upgrade

exec airflow webserver