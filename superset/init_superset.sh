#!/bin/bash
set -e

DB_HOST="postgres"
DB_PORT="5432"

echo "Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT}..."
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$POSTGRES_USER"; do
  sleep 5
done

echo "PostgreSQL is ready."

# Upgrade Superset metadata DB
superset db upgrade

# Create admin user (idempotent)
superset fab create-admin \
  --username "$SUPERSET_ADMIN" \
  --password "$SUPERSET_PASSWORD" \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  || true

# Initialize Superset (NO examples)
superset init

# Added DB with data
/app/.venv/bin/python /app/bootstrap/add_databases.py

# Start Superset
exec gunicorn \
  --bind "0.0.0.0:8088" \
  --access-logfile "-" \
  --error-logfile "-" \
  --workers 1 \
  --worker-class gthread \
  --threads 20 \
  --timeout 60 \
  --limit-request-line 0 \
  --limit-request-field_size 0 \
  "superset.app:create_app()"
