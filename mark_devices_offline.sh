#!/usr/bin/env bash
set -euo pipefail

# DB connection (use same values as your app / .env)
PGHOST="${PGHOST:-172.31.17.177}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-dgx}"
PGUSER="${PGUSER:-app_user}"
PGPASSWORD="${PGPASSWORD:-strongpassword}"

export PGPASSWORD

psql "host=$PGHOST port=$PGPORT dbname=$PGDATABASE user=$PGUSER" -c "
  UPDATE public.device
  SET is_online = FALSE
  WHERE updated_at < NOW() - INTERVAL '1 minutes';
"

