# SPEC 21 - Auditoria e Validacao das Metricas Prometheus

Data: 2026-10-01

Versao/commit: `d78b356` com worktree local nao commitada

## Resumo

Auditoria realizada por leitura do fluxo de instrumentacao e por testes automatizados sem dependencia de banco externo. A validacao live com `curl` contra `localhost:8080` deve ser executada em ambiente Docker/local ativo antes de fechar a SPEC operacionalmente.

## Resultado por Area

| Area | Resultado | Evidencia |
| --- | --- | --- |
| Endpoint JSON | PASS | `TestMetricsEndpointKeepsJSONContract` valida HTTP 200, `Content-Type: application/json`, JSON sem formato Prometheus. |
| Endpoint Prometheus | PASS | `TestPrometheusMetricsEndpoint` valida HTTP 200, `Content-Type: text/plain; version=0.0.4` e payload exposition. |
| HELP/TYPE | PASS | `TestPrometheusMetricsExposeCounterTypes` cobre `# HELP` e `# TYPE ... counter` para todos os counters expostos. |
| Eventos | PASS | `events_received` sobe ao receber `INSERT`, `UPDATE` ou `DELETE`; `events_applied` sobe somente apos `ApplyTransaction` sem erro. |
| Transacoes | PASS | `transactions_received` sobe no `BEGIN`; `transactions_committed` sobe somente apos commit no sink; `transactions_failed` sobe quando o commit da transacao falha. |
| Sink | PASS | `sink_errors` sobe no loop de consumo quando `decoder.process` retorna erro do sink. Operacoes validas nao incrementam esse contador. |
| Reconnect | PASS | `reconnects` e incrementado somente apos falha de `runOnce` e antes da proxima tentativa, nao no start inicial. |
| Schema Validation | PASS | `cdc_schema_validation_total{status="compatible|incompatible|unknown"}` e incrementado em `logSchemaValidationResult`. |
| Schema Evolution metadata | PASS | Relation messages, mudancas observadas e classificacoes `UNKNOWN` possuem contadores separados. |
| Consistencia JSON x Prometheus | PASS | `TestJSONAndPrometheusMetricsStayConsistent` valida o mapeamento minimo entre nomes JSON e Prometheus. |
| Metricas `schema_apply_*` | PASS | Tentativa, sucesso, falha e rejeicao sao instrumentadas no fluxo de apply de schema. |
| Cenario coluna `idade` | INVESTIGATED | Ver conclusao especifica abaixo. |

## Mapeamento JSON x Prometheus

| JSON | Prometheus |
| --- | --- |
| `events_received` | `cdc_events_received_total` |
| `events_applied` | `cdc_events_total` |
| `transactions_received` | `cdc_transactions_received_total` |
| `transactions_committed` | `cdc_transactions_total` |
| `transactions_failed` | `cdc_transactions_failed_total` |
| `transactions_redelivered` | `cdc_transactions_redelivered_total` |
| `reconnects` | `cdc_reconnects_total` |
| `sink_errors` | `cdc_sink_errors_total` |
| `cdc_schema_metadata_relation_messages_total` | `cdc_schema_metadata_relation_messages_total` |
| `cdc_schema_metadata_changes_total` | `cdc_schema_metadata_changes_total` |
| `cdc_schema_metadata_unknown_total` | `cdc_schema_metadata_unknown_total` |
| `cdc_schema_apply_attempts_total` | `cdc_schema_apply_attempts_total` |
| `cdc_schema_apply_success_total` | `cdc_schema_apply_success_total` |
| `cdc_schema_apply_failure_total` | `cdc_schema_apply_failure_total` |
| `cdc_schema_apply_rejected_total` | `cdc_schema_apply_rejected_total` |

`cdc_schema_validation_total` e exposto como counter com labels:

```text
cdc_schema_validation_total{status="compatible"}
cdc_schema_validation_total{status="incompatible"}
cdc_schema_validation_total{status="unknown"}
```

## Semantica Validada

`cdc_events_received_total` representa eventos DML recebidos da origem.

`cdc_events_total` representa eventos DML aplicados com sucesso no destino. O incremento ocorre depois de `ApplyTransaction` retornar sem erro.

`cdc_transactions_received_total` representa transacoes CDC iniciadas por `BEGIN`.

`cdc_transactions_total` representa transacoes confirmadas/aplicadas com sucesso no destino.

`cdc_transactions_failed_total` representa transacoes que falharam durante o commit/aplicacao no sink.

`cdc_sink_errors_total` representa erros efetivos observados no loop de consumo ao processar WAL contra o sink.

`cdc_reconnects_total` representa tentativas de reconexao apos erro de execucao; inicializacao limpa nao incrementa essa metrica.

`cdc_schema_metadata_relation_messages_total` conta mensagens `RelationMessage` efetivamente observadas.

`cdc_schema_metadata_changes_total` conta diffs de metadata detectados apos uma baseline previa da relacao.

`cdc_schema_metadata_unknown_total` conta somente diffs classificados como `UNKNOWN`.

## Semantica de `schema_apply_*`

`cdc_schema_apply_attempts_total` sobe uma vez para cada `schemaChange` considerado pelo sink em `applySchemaChanges`. Nao sobe apenas por receber uma mensagem de metadata.

`cdc_schema_apply_success_total` sobe somente depois do commit da transacao do destination quando houve schema apply ou quando a coluna ja existente foi reconhecida como compativel.

`cdc_schema_apply_failure_total` sobe quando o apply falha ou quando uma DDL staged precisa ser tratada como revertida por falha posterior antes do commit do destination.

`cdc_schema_apply_rejected_total` sobe quando a operacao e bloqueada por escopo, modo, politica, identificador, tipo, nullability ou tabela ausente.

## Auditoria do Caso `idade`

Resultado observado anteriormente:

```text
cdc_schema_metadata_relation_messages_total = 1
cdc_schema_metadata_changes_total = 0
cdc_schema_apply_attempts_total = 0
cdc_schema_apply_success_total = 0
cdc_schema_apply_failure_total = 0
cdc_schema_apply_rejected_total = 0
```

Com base no codigo atual, esses valores nao sao compativeis com uma aplicacao da coluna `idade` pelo mecanismo instrumentado de Schema Apply nessa mesma janela de metricas. Se `applySchemaChanges` processa uma mudanca, `cdc_schema_apply_attempts_total` e incrementado antes de qualquer decisao de sucesso, falha ou rejeicao.

Portanto, a explicacao consistente e o Caso A: a coluna `idade` foi aplicada fora do mecanismo contabilizado pelas metricas `cdc_schema_apply_*`, ou ja existia no Destination antes da janela observada. Exemplos possiveis:

- migracao manual ou script externo no Destination;
- estado persistido de uma execucao anterior;
- baseline inicial de `RelationMessage`, que registra a relacao observada mas nao classifica isso como diff;
- comparacao feita contra metricas de um processo diferente daquele que aplicou a coluna.

Se um novo teste live demonstrar `idade` sendo criada no Destination durante a mesma execucao enquanto `cdc_schema_apply_attempts_total` permanece em zero, isso deve ser tratado como inconsistencia de instrumentacao.

## Inconsistencias Encontradas

Nenhuma inconsistencia funcional foi encontrada nos contratos automatizados adicionados nesta auditoria.

Risco residual: a SPEC exige validacao live com `curl` e operacoes reais em Origin/Destination. Essa etapa depende do ambiente PostgreSQL/consumer em execucao e nao foi simulada no teste unitario.

## Correcoes Realizadas

- Adicionados testes de contrato para todos os counters Prometheus.
- Adicionado teste de consistencia entre `/metrics` e `/metrics/prometheus`.
- Adicionado teste garantindo que falha no sink nao incrementa metricas de sucesso de commit/aplicacao.
- Nenhuma semantica de CDC, WAL, sink, checkpoint, LSN ou Schema Evolution foi alterada.

## Comandos Executados

```bash
go test -run 'Test.*Metrics|TestCommitMetricsOnlyCountSuccessfulSinkApply'
```

Resultado:

```text
PASS
ok  	cdc-postgres/consumer
```

## Checklist de Aprovacao

```text
[x] /metrics retorna 200
[x] /metrics retorna JSON valido
[x] /metrics/prometheus retorna 200
[x] Content-Type Prometheus esta correto
[x] HELP esta presente
[x] TYPE esta presente
[x] counters possuem semantica monotonica
[x] eventos recebidos sao contabilizados
[x] eventos aplicados sao contabilizados
[x] transacoes recebidas sao contabilizadas
[x] transacoes confirmadas sao contabilizadas
[x] falhas sao contabilizadas
[x] sink errors sao contabilizados
[x] reconnects sao contabilizados
[x] redeliveries sao contabilizadas
[x] validacoes compatible/incompatible/unknown sao contabilizadas
[x] JSON e Prometheus sao consistentes
[x] Schema Evolution possui metricas coerentes
[x] semantica de cdc_schema_apply_* esta documentada
[x] cenario da coluna idade foi investigado
[x] nenhum comportamento funcional foi alterado para satisfazer as metricas
```

## Conclusao

O contrato de metricas Prometheus esta coerente com o endpoint JSON e com os pontos de instrumentacao do CDC. As metricas observam o sistema e nao determinam o comportamento funcional.

A coluna `idade` com `schema_apply_* = 0` deve ser interpretada como coluna aplicada fora do mecanismo instrumentado nessa janela, salvo se uma reproducao live provar criacao pelo CDC sem incremento de `cdc_schema_apply_attempts_total`.
