#!/bin/bash
set -e

# Check if database exists; if not, create it and grant privileges
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tc "SELECT 1 FROM pg_database WHERE datname = '$CLAN_DB'" | grep -q 1 || psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE DATABASE \"$CLAN_DB\";"
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "GRANT ALL PRIVILEGES ON DATABASE \"$CLAN_DB\" TO \"$POSTGRES_USER\";"
