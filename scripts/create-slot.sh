#!/usr/bin/env bash
set -euo pipefail

SLOT_NAME="${SLOT_NAME:-cdc_slot}"
PLUGIN="${PLUGIN:-pgoutput}"
DB_NAME="${DB_NAME:-cdc_demo}"
DB_USER="${DB_USER:-postgres}"
SERVICE="${SERVICE:-postgres}"

cd "$(dirname "$0")/.."

docker compose exec -T "$SERVICE" psql -U "$DB_USER" -d "$DB_NAME" <<SQL
DO \$\$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_publication
        WHERE pubname = 'cdc_publication'
    ) THEN
        CREATE PUBLICATION cdc_publication
        FOR TABLE public.clientes, public.enderecos;
    ELSIF NOT EXISTS (
        SELECT 1
        FROM pg_publication_tables
        WHERE pubname = 'cdc_publication'
          AND schemaname = 'public'
          AND tablename = 'enderecos'
    ) THEN
        ALTER PUBLICATION cdc_publication
        ADD TABLE public.enderecos;
    END IF;
END
\$\$;

SELECT
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM pg_replication_slots
            WHERE slot_name = '${SLOT_NAME}'
        )
        THEN 'slot already exists: ${SLOT_NAME}'
        ELSE (
            SELECT 'created slot: ' || slot_name || ' using plugin: ' || plugin
            FROM pg_create_logical_replication_slot('${SLOT_NAME}', '${PLUGIN}')
        )
    END AS result;
SQL
