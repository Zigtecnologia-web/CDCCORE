#!/usr/bin/env bash
set -euo pipefail

SLOT_NAME="${SLOT_NAME:-cdc_slot}"
PLUGIN="${PLUGIN:-pgoutput}"
DB_NAME="${DB_NAME:-cdc_demo}"
DB_USER="${DB_USER:-postgres}"
SERVICE="${SERVICE:-postgres}"

cd "$(dirname "$0")/.."

echo "Starting PostgreSQL..."
docker compose up -d

echo "Waiting for PostgreSQL to accept connections..."
until docker compose exec -T "$SERVICE" pg_isready -U "$DB_USER" -d postgres >/dev/null 2>&1; do
    sleep 1
done

echo
echo "1. PostgreSQL is running"
docker compose ps "$SERVICE"

echo
echo "2. Checking wal_level"
docker compose exec -T "$SERVICE" psql -U "$DB_USER" -d "$DB_NAME" -c "SHOW wal_level;"

echo
echo "Preparing deterministic test data"
docker compose exec -T "$SERVICE" psql -U "$DB_USER" -d "$DB_NAME" <<SQL
TRUNCATE TABLE enderecos, clientes RESTART IDENTITY;
ALTER TABLE clientes REPLICA IDENTITY FULL;
ALTER TABLE enderecos REPLICA IDENTITY FULL;
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
SELECT pg_drop_replication_slot('${SLOT_NAME}')
WHERE EXISTS (
    SELECT 1
    FROM pg_replication_slots
    WHERE slot_name = '${SLOT_NAME}'
);
SQL

echo
echo "3. Creating logical replication slot"
docker compose exec -T "$SERVICE" psql -U "$DB_USER" -d "$DB_NAME" <<SQL
SELECT *
FROM pg_create_logical_replication_slot('${SLOT_NAME}', '${PLUGIN}');
SQL

echo
echo "Current slot metadata"
docker compose exec -T "$SERVICE" psql -U "$DB_USER" -d "$DB_NAME" <<SQL
SELECT slot_name, plugin, slot_type, database, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name = '${SLOT_NAME}';
SQL

echo
echo "4. INSERT should generate a pgoutput event"
docker compose exec -T "$SERVICE" psql -U "$DB_USER" -d "$DB_NAME" <<SQL
INSERT INTO clientes (nome, email)
VALUES ('João', 'joao@email.com');

SELECT lsn, xid, data
FROM pg_logical_slot_get_binary_changes(
    '${SLOT_NAME}',
    NULL,
    NULL,
    'proto_version', '1',
    'publication_names', 'cdc_publication'
);
SQL

echo
echo "5. UPDATE should generate a pgoutput event"
docker compose exec -T "$SERVICE" psql -U "$DB_USER" -d "$DB_NAME" <<SQL
UPDATE clientes
SET email = 'joao.novo@email.com'
WHERE id = 1;

SELECT lsn, xid, data
FROM pg_logical_slot_get_binary_changes(
    '${SLOT_NAME}',
    NULL,
    NULL,
    'proto_version', '1',
    'publication_names', 'cdc_publication'
);
SQL

echo
echo "6. DELETE should generate a pgoutput event"
docker compose exec -T "$SERVICE" psql -U "$DB_USER" -d "$DB_NAME" <<SQL
DELETE FROM clientes
WHERE id = 1;

SELECT lsn, xid, data
FROM pg_logical_slot_get_binary_changes(
    '${SLOT_NAME}',
    NULL,
    NULL,
    'proto_version', '1',
    'publication_names', 'cdc_publication'
);
SQL

echo
echo "7. Done. The data column above contains binary pgoutput messages."
