# SPEC 19 - Diagnostico de Regressao na Replicacao e Schema Validation

## Resultado

- [ ] Sem regressao
- [x] Regressao confirmada
- [ ] Falha causada por configuracao
- [ ] Falha causada por alteracao de schema
- [x] Falha causada pela Schema Validation
- [ ] Falha causada pelo Sink
- [ ] Falha causada pelo mecanismo de checkpoint/LSN
- [ ] Falha causada pelo reconnect
- [ ] Causa ainda nao determinada

## Contexto observado

Ambiente usado:

- `CDC_SINK=postgres`
- `CDC_POSTGRES_APPLY_MODE=generic`
- `CDC_SCHEMA_EVOLUTION=auto`
- `CDC_TABLE_INCLUDE=public.clientes,public.enderecos`

Metricas observadas apos iniciar o consumer:

```json
{
  "events_received": 6,
  "events_applied": 0,
  "transactions_received": 6,
  "transactions_committed": 0,
  "transactions_failed": 6,
  "sink_errors": 6,
  "reconnects": 5,
  "transactions_redelivered": 0,
  "cdc_schema_validation_total": {
    "compatible": 0,
    "incompatible": 12,
    "unknown": 0
  },
  "cdc_schema_apply_attempts_total": 6,
  "cdc_schema_apply_success_total": 0,
  "cdc_schema_apply_failure_total": 6,
  "last_processed_lsn": "0/1DEB838",
  "last_confirmed_lsn": "0/1DEB838"
}
```

## Evidencia do erro

O consumer rejeitou a transacao com:

```text
event=schema_validation
validation_result=INCOMPATIBLE
difference=PRIMARY_KEY_MISMATCH
reason=destination table has no primary key for generic CDC
```

O erro propagado para retry foi:

```text
schema validation rejected public.clientes: PRIMARY_KEY_MISMATCH column= reason=destination table has no primary key for generic CDC
```

Entretanto, o catalogo do PostgreSQL no destination confirma que a tabela possui primary key:

```text
destination | clientes_pkey | p | PRIMARY KEY (id)
```

Consulta direta em `pg_index` tambem confirma:

```text
indisprimary | attname | position
-------------+---------+---------
t            | id      | 1
```

## Causa raiz

A validacao final antes do DML chama:

```go
sink.validateDestinationForEvents(...)
```

Essa funcao usa:

```go
readTableSchemaTx(ctx, tx, table)
```

O problema esta em `readTableSchemaTx`, em `consumer/schema_apply.go`: a funcao le a existencia da tabela e suas colunas, mas nao consulta nem preenche `tableSchema.PrimaryKey`.

Com isso, mesmo quando o destination possui `PRIMARY KEY (id)`, o resultado em memoria chega com:

```text
DestinationSchema.PrimaryKey = []
```

Em seguida, no modo `generic`, `validateDestinationForEvents` executa:

```go
if sink.applyMode == "generic" && len(destination.PrimaryKey) == 0 {
    result.Status = compatibilityIncompatible
}
```

Esse caminho classifica indevidamente a transacao como `INCOMPATIBLE`.

## Comportamento anterior

Eventos `INSERT`, `UPDATE` e `DELETE` em uma tabela destination compativel eram aplicados quando havia chave primaria real no PostgreSQL.

## Comportamento atual

Em modo `generic`, a validacao transacional rejeita eventos para `public.clientes` com `PRIMARY_KEY_MISMATCH`, apesar de a chave primaria existir no banco.

Como a transacao falha:

- `events_received` aumenta;
- `events_applied` nao aumenta;
- `transactions_received` aumenta;
- `transactions_committed` nao aumenta;
- `transactions_failed` aumenta;
- `sink_errors` aumenta;
- `reconnects` aumenta por causa do loop de retry;
- `last_processed_lsn` e `last_confirmed_lsn` nao avancam alem do ultimo LSN confirmado antes da falha.

## Verificacao de LSN

O slot permaneceu em:

```text
confirmed_flush_lsn = 0/1DEB838
restart_lsn         = 0/1DEB838
```

As metricas tambem permaneceram em:

```text
last_processed_lsn = 0/1DEB838
last_confirmed_lsn = 0/1DEB838
```

Conclusao: a transacao rejeitada nao foi confirmada indevidamente.

## Redelivery e reconnect

O mesmo XID foi reprocessado apos reconnect:

```text
BEGIN xid=1295
...
event=retry attempt=4
...
BEGIN xid=1295
...
event=retry attempt=5
```

Isso confirma que o evento nao foi perdido. A metrica `transactions_redelivered` permaneceu `0`, indicando que o projeto ainda nao contabiliza essa redelivery nesse caminho de retry.

## Correção necessária

Atualizar `readTableSchemaTx` para carregar a chave primaria da tabela dentro da mesma transacao, com a mesma semantica ja usada por:

- `schemaCatalogReader.ReadTableSchema`
- `postgresSink.destinationMetadata`

Apos a correcao, a validacao deve enxergar `PrimaryKey=["id"]` para `public.clientes` e deixar de classificar uma tabela compativel como `PRIMARY_KEY_MISMATCH`.

## Escopo

Nenhuma alteracao de comportamento foi aplicada neste diagnostico. A correcao deve ser feita em uma etapa separada, com teste de regressao cobrindo `readTableSchemaTx` ou o fluxo `validateDestinationForEvents` em modo `generic`.
