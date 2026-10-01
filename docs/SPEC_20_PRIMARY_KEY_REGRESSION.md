# SPEC 20 - Correcao da Regressao de Primary Key na Schema Validation

## Causa raiz

`readTableSchemaTx` lia a tabela e suas colunas no PostgreSQL Destination, mas nao preenchia `tableSchema.PrimaryKey`.

No modo `CDC_POSTGRES_APPLY_MODE=generic`, `validateDestinationForEvents` usa esse campo para decidir se a tabela pode receber CDC generico. Como a PK vinha vazia, tabelas validas eram classificadas como:

```text
PRIMARY_KEY_MISMATCH
destination table has no primary key for generic CDC
```

mesmo quando o catalogo do PostgreSQL continha `PRIMARY KEY`.

## Arquivo/função corrigido

- `consumer/schema_apply.go`
- `readTableSchemaTx`

A funcao agora consulta `pg_catalog.pg_index` e `pg_catalog.pg_attribute` dentro da mesma transacao para preencher `PrimaryKey`, preservando a ordem definida pelo PostgreSQL.

## Comportamento anterior

Para uma tabela como:

```sql
CREATE TABLE clientes (
    id BIGSERIAL PRIMARY KEY,
    nome TEXT,
    email TEXT
);
```

o schema interno retornava:

```text
PrimaryKey = []
```

Isso provocava falso `PRIMARY_KEY_MISMATCH` no fluxo generico.

## Comportamento corrigido

Para a mesma tabela, o schema interno retorna:

```text
PrimaryKey = [id]
```

Para chave composta:

```sql
PRIMARY KEY (tenant_id, id)
```

o schema interno retorna:

```text
PrimaryKey = [tenant_id id]
```

Tabelas sem PK continuam retornando `PrimaryKey` vazio.

## Teste de PK simples

PASS

```text
TestReadTableSchemaTxReadsPrimaryKey
```

## Teste de PK composta

PASS

```text
TestReadTableSchemaTxReadsCompositePrimaryKeyInOrder
```

## Teste sem PK

PASS

```text
TestReadTableSchemaTxKeepsTableWithoutPrimaryKey
```

## Regressão SPEC 19

PASS

O fluxo completo voltou a processar eventos que antes falhavam com falso `PRIMARY_KEY_MISMATCH`.

Evidencia do log:

```text
event=schema_validation validation_result=COMPATIBLE reason=compatible
event=transaction_committed
```

## Schema Evolution

PASS

Foi adicionada uma coluna nullable nova no Source:

```text
spec20_score_1790824363 INTEGER
```

O Destination recebeu a coluna e o evento com valor `37` foi aplicado.

Metricas de schema apply:

```json
{
  "cdc_schema_apply_attempts_total": 2,
  "cdc_schema_apply_success_total": 2,
  "cdc_schema_apply_failure_total": 0
}
```

## Métricas antes

Antes do INSERT `SPEC-20`:

```json
{
  "events_received": 4,
  "events_applied": 4,
  "transactions_received": 4,
  "transactions_committed": 4,
  "transactions_failed": 0,
  "sink_errors": 0,
  "cdc_schema_validation_total": {
    "compatible": 4,
    "incompatible": 1,
    "unknown": 0
  },
  "last_processed_lsn": "0/1E03310",
  "last_confirmed_lsn": "0/1E03310"
}
```

## Métricas depois

Depois do INSERT `SPEC-20`:

```json
{
  "events_received": 5,
  "events_applied": 5,
  "transactions_received": 5,
  "transactions_committed": 5,
  "transactions_failed": 0,
  "sink_errors": 0,
  "cdc_schema_validation_total": {
    "compatible": 5,
    "incompatible": 1,
    "unknown": 0
  },
  "last_processed_lsn": "0/1E07210",
  "last_confirmed_lsn": "0/1E07210"
}
```

Depois de UPDATE e DELETE:

```json
{
  "events_received": 7,
  "events_applied": 7,
  "transactions_received": 7,
  "transactions_committed": 7,
  "transactions_failed": 0,
  "sink_errors": 0,
  "cdc_schema_validation_total": {
    "compatible": 7,
    "incompatible": 1,
    "unknown": 0
  },
  "last_confirmed_lsn": "0/1E073A8"
}
```

Depois de Schema Evolution:

```json
{
  "events_received": 8,
  "events_applied": 8,
  "transactions_received": 8,
  "transactions_committed": 8,
  "transactions_failed": 0,
  "sink_errors": 0,
  "cdc_schema_apply_attempts_total": 2,
  "cdc_schema_apply_success_total": 2,
  "cdc_schema_apply_failure_total": 0,
  "cdc_schema_validation_total": {
    "compatible": 8,
    "incompatible": 2,
    "unknown": 0
  },
  "last_confirmed_lsn": "0/1E08DE8"
}
```

O contador `incompatible` aumentou nos casos esperados de coluna ausente antes do schema apply; apos a aplicacao, a validacao final do DML foi `COMPATIBLE` e a transacao foi commitada.

## LSN antes

```text
last_processed_lsn = 0/1E03310
last_confirmed_lsn = 0/1E03310
```

## LSN depois

Depois do INSERT:

```text
last_processed_lsn = 0/1E07210
last_confirmed_lsn = 0/1E07210
```

Depois do Schema Evolution:

```text
last_confirmed_lsn = 0/1E08DE8
```

## Testes executados

```bash
go test -run 'TestReadTableSchemaTxReadsPrimaryKey|TestReadTableSchemaTxReadsCompositePrimaryKeyInOrder|TestReadTableSchemaTxKeepsTableWithoutPrimaryKey|TestPostgresSchemaApplyAddsNullableColumn|TestPostgresSchemaApplyRollsBackDDLWhenDMLFails|TestPostgresSchemaApplyBlocksNotNullColumn' -v
```

Resultado: PASS

```bash
go test ./...
```

Resultado: PASS

## Conclusão

A regressao foi corrigida de forma localizada em `readTableSchemaTx`.

Nenhuma regra de Schema Validation foi removida ou enfraquecida. O `PRIMARY_KEY_MISMATCH` continua valido para tabelas realmente sem chave primaria, mas deixou de ocorrer falsamente para tabelas que possuem PK no Destination.
