#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="${SCHEMA_AUDIT_OUTPUT_DIR:-$ROOT_DIR/dist/schema-evolution-audit}"
CONSUMER_LOG="$OUTPUT_DIR/consumer.log"
SQL_LOG="$OUTPUT_DIR/sql.log"

mkdir -p "$OUTPUT_DIR"
: >"$CONSUMER_LOG"
: >"$SQL_LOG"

cd "$ROOT_DIR"

log_step() {
  printf '\n## %s\n' "$1" | tee -a "$SQL_LOG"
}

source_sql() {
  local sql="$1"
  printf '\n-- source\n%s\n' "$sql" >>"$SQL_LOG"
  docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d cdc_demo -c "$sql" | tee -a "$SQL_LOG"
}

source_sql_allow_error() {
  local sql="$1"
  printf '\n-- source allow-error\n%s\n' "$sql" >>"$SQL_LOG"
  set +e
  docker compose exec -T postgres psql -U postgres -d cdc_demo -c "$sql" 2>&1 | tee -a "$SQL_LOG"
  local status=${PIPESTATUS[0]}
  set -e
  printf 'exit_status=%d\n' "$status" | tee -a "$SQL_LOG"
}

target_sql() {
  local sql="$1"
  printf '\n-- target\n%s\n' "$sql" >>"$SQL_LOG"
  docker compose exec -T postgres-target psql -v ON_ERROR_STOP=1 -U postgres -d cdc_demo -c "$sql" | tee -a "$SQL_LOG"
}

catalog_snapshot() {
  local label="$1"
  log_step "CATALOG $label"
  source_sql "SELECT attnum, attname, format_type(atttypid, atttypmod) AS type, attnotnull, atthasdef FROM pg_attribute WHERE attrelid = 'public.clientes'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum;"
}

restart_consumer() {
  docker rm -f cdc-consumer-audit >/dev/null 2>&1 || true
  docker compose run --rm --no-deps --name cdc-consumer-audit \
    -e CDC_SCHEMA_AUDIT_LOG=true \
    -e CDC_SINK=file \
    -e CDC_OUTPUT_FILE=/tmp/schema-audit-events.jsonl \
    -e CDC_PUBLICATION_AUTOCONFIGURE=false \
    -e CDC_TABLE_INCLUDE=public.clientes \
    -e CDC_STATUS_INTERVAL=1s \
    consumer >>"$CONSUMER_LOG" 2>&1 &
  CONSUMER_PID=$!
  sleep 3
}

stop_consumer() {
  docker rm -f cdc-consumer-audit >/dev/null 2>&1 || true
  if [[ -n "${CONSUMER_PID:-}" ]]; then
    wait "$CONSUMER_PID" >/dev/null 2>&1 || true
  fi
}

reset_cliente_schema() {
  source_sql "DROP TABLE IF EXISTS public.enderecos; DROP TABLE IF EXISTS public.clientes; CREATE TABLE public.clientes (id BIGSERIAL PRIMARY KEY, nome TEXT NOT NULL, email TEXT); ALTER TABLE public.clientes REPLICA IDENTITY FULL; DROP PUBLICATION IF EXISTS cdc_publication; CREATE PUBLICATION cdc_publication FOR TABLE public.clientes;"
  target_sql "DROP TABLE IF EXISTS public.enderecos; DROP TABLE IF EXISTS public.clientes; CREATE TABLE public.clientes (id BIGINT PRIMARY KEY, nome TEXT NOT NULL, email TEXT); ALTER TABLE public.clientes REPLICA IDENTITY FULL;"
}

trap stop_consumer EXIT

log_step "RESET DOCKER ENVIRONMENT"
docker compose down -v --remove-orphans | tee -a "$SQL_LOG"
docker compose up -d --build postgres postgres-target create-slot | tee -a "$SQL_LOG"
docker compose build consumer | tee -a "$SQL_LOG"
docker compose wait create-slot | tee -a "$SQL_LOG" || true
reset_cliente_schema
restart_consumer

log_step "VERSIONS"
source_sql "SHOW server_version; SHOW wal_level;"
(cd consumer && go version && go list -m github.com/jackc/pgx/v5 github.com/jackc/pglogrepl) | tee -a "$SQL_LOG"

log_step "E1 BASELINE DML"
source_sql "INSERT INTO public.clientes (nome, email) VALUES ('E1 Insert', 'e1-insert@example.com');"
source_sql "UPDATE public.clientes SET nome = 'E1 Update' WHERE email = 'e1-insert@example.com';"
source_sql "DELETE FROM public.clientes WHERE email = 'e1-insert@example.com';"
sleep 2

log_step "E2 ADD COLUMN THEN DML"
catalog_snapshot "before add column"
source_sql "ALTER TABLE public.clientes ADD COLUMN telefone TEXT;"
catalog_snapshot "after add column before DML"
source_sql "INSERT INTO public.clientes (nome, email, telefone) VALUES ('E2 Add', 'e2@example.com', '999999999');"
sleep 2

log_step "E3 ADD COLUMN + DML SAME TRANSACTION"
reset_cliente_schema
source_sql "BEGIN; ALTER TABLE public.clientes ADD COLUMN telefone TEXT; INSERT INTO public.clientes (nome, email, telefone) VALUES ('E3 Same TX', 'e3@example.com', '999999999'); COMMIT;"
sleep 2

log_step "E4 DDL SEPARATE FROM DML"
reset_cliente_schema
source_sql "ALTER TABLE public.clientes ADD COLUMN telefone TEXT;"
sleep 1
source_sql "INSERT INTO public.clientes (nome, email, telefone) VALUES ('E4 Separate', 'e4@example.com', '999999999');"
sleep 2

log_step "E5 DROP COLUMN"
source_sql "ALTER TABLE public.clientes DROP COLUMN telefone;"
source_sql "INSERT INTO public.clientes (nome, email) VALUES ('E5 Drop', 'e5@example.com');"
sleep 2

log_step "E6 RENAME COLUMN"
reset_cliente_schema
source_sql "ALTER TABLE public.clientes RENAME COLUMN nome TO nome_completo;"
source_sql "INSERT INTO public.clientes (nome_completo, email) VALUES ('E6 Rename', 'e6@example.com');"
sleep 2

log_step "E7 ALTER TYPE"
reset_cliente_schema
source_sql "ALTER TABLE public.clientes ALTER COLUMN nome TYPE VARCHAR(200);"
source_sql "INSERT INTO public.clientes (nome, email) VALUES ('E7 Alter Type', 'e7@example.com');"
sleep 2

log_step "E8 SET NOT NULL"
reset_cliente_schema
source_sql "ALTER TABLE public.clientes ALTER COLUMN email SET NOT NULL;"
source_sql "INSERT INTO public.clientes (nome, email) VALUES ('E8 Not Null', 'e8@example.com');"
sleep 2

log_step "E9 DEFAULT"
reset_cliente_schema
source_sql "ALTER TABLE public.clientes ALTER COLUMN email SET DEFAULT '';"
source_sql "INSERT INTO public.clientes (nome) VALUES ('E9 Default');"
sleep 2

log_step "E10 CREATE TABLE"
source_sql "CREATE TABLE public.audit_enderecos (id BIGSERIAL PRIMARY KEY, cliente_id BIGINT, cidade TEXT); ALTER TABLE public.audit_enderecos REPLICA IDENTITY FULL; ALTER PUBLICATION cdc_publication ADD TABLE public.audit_enderecos;"
source_sql "INSERT INTO public.audit_enderecos (cliente_id, cidade) VALUES (1, 'Salvador');"
sleep 2

log_step "E11 DROP TABLE"
source_sql "DROP TABLE public.audit_enderecos;"
sleep 2

log_step "E13 TWO FAST CHANGES"
reset_cliente_schema
source_sql "ALTER TABLE public.clientes ADD COLUMN a TEXT; ALTER TABLE public.clientes ADD COLUMN b TEXT;"
source_sql "INSERT INTO public.clientes (nome, email, a, b) VALUES ('E13 Fast', 'e13@example.com', 'a', 'b');"
sleep 2

log_step "E14 DDL + DDL + DML SAME TRANSACTION"
reset_cliente_schema
source_sql "BEGIN; ALTER TABLE public.clientes ADD COLUMN a TEXT; ALTER TABLE public.clientes ADD COLUMN b TEXT; INSERT INTO public.clientes (nome, email, a, b) VALUES ('E14 Same TX', 'e14@example.com', 'a', 'b'); COMMIT;"
sleep 2

log_step "E15 ROLLBACK DDL"
reset_cliente_schema
source_sql_allow_error "BEGIN; ALTER TABLE public.clientes ADD COLUMN teste TEXT; ROLLBACK;"
catalog_snapshot "after rollback"
source_sql "INSERT INTO public.clientes (nome, email) VALUES ('E15 Rollback', 'e15@example.com');"
sleep 2

log_step "E16 FAILED DDL"
reset_cliente_schema
source_sql "INSERT INTO public.clientes (nome) VALUES ('E16 Null Email');"
source_sql_allow_error "ALTER TABLE public.clientes ALTER COLUMN email SET NOT NULL;"
source_sql "INSERT INTO public.clientes (nome, email) VALUES ('E16 Failed', 'e16@example.com');"
sleep 2

log_step "E17 REPEATED RELATION MESSAGE"
source_sql "INSERT INTO public.clientes (nome, email) VALUES ('E17 First', 'e17a@example.com');"
source_sql "INSERT INTO public.clientes (nome, email) VALUES ('E17 Second', 'e17b@example.com');"
sleep 2

log_step "E19 RELATION AND UPDATE DELETE AFTER CHANGE"
source_sql "ALTER TABLE public.clientes ADD COLUMN telefone TEXT;"
source_sql "INSERT INTO public.clientes (nome, email, telefone) VALUES ('E19 Insert', 'e19@example.com', '111');"
source_sql "UPDATE public.clientes SET telefone = '222' WHERE email = 'e19@example.com';"
source_sql "DELETE FROM public.clientes WHERE email = 'e19@example.com';"
sleep 2

log_step "E20 POSTGRESQLSINK INCOMPATIBILITY"
stop_consumer
reset_cliente_schema
source_sql "ALTER TABLE public.clientes ADD COLUMN telefone TEXT;"
docker compose run --rm --no-deps --name cdc-consumer-audit \
  -e CDC_SCHEMA_AUDIT_LOG=true \
  -e CDC_SINK=postgres \
  -e CDC_POSTGRES_APPLY_MODE=generic \
  -e CDC_SCHEMA_EVOLUTION=disabled \
  -e CDC_PUBLICATION_AUTOCONFIGURE=false \
  -e CDC_TABLE_INCLUDE=public.clientes \
  -e CDC_STATUS_INTERVAL=1s \
  consumer >>"$CONSUMER_LOG" 2>&1 &
CONSUMER_PID=$!
sleep 3
source_sql "INSERT INTO public.clientes (nome, email, telefone) VALUES ('E20 Sink Fail', 'e20@example.com', '333');"
sleep 5
stop_consumer

log_step "E21 MANUAL SCHEMA APPLY"
target_sql "ALTER TABLE public.clientes ADD COLUMN telefone TEXT;"
restart_consumer
source_sql "INSERT INTO public.clientes (nome, email, telefone) VALUES ('E21 Manual Apply', 'e21@example.com', '444');"
sleep 3

log_step "AUDIT COMPLETE"
printf 'consumer_log=%s\nsql_log=%s\n' "$CONSUMER_LOG" "$SQL_LOG" | tee -a "$SQL_LOG"
