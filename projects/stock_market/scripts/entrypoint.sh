#!/bin/bash

set -e

# Activate the service account
gcloud auth activate-service-account --key-file="${GOOGLE_APPLICATION_CREDENTIALS}" --project="${PROJECT_ID}"

if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install --user -r requirements.txt
fi

# Print environment variables for debugging
echo "Environment Variables before setting JAVA_HOME:"
env

# Add these lines to set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Print environment variables after setting JAVA_HOME
echo "Environment Variables after setting JAVA_HOME:"
env


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

# Create GCP connection   <-- Add here
airflow connections add 'my_gcp_connection' \
  --conn-type 'google_cloud_platform' \
  --conn-extra '{"extra__google_cloud_platform__project": "tesla-stocks-410911"}'


# Upgrade database schema
$(command -v airflow) db upgrade

exec airflow webserver