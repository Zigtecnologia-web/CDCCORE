Data:
2026-10-01 01:15:00 -03

Commit:
d78b356 (worktree com alteracoes locais)

PostgreSQL Origin:
Container cdc-postgres, status healthy, database cdc_demo.

PostgreSQL Target:
Container cdc-postgres-target, status healthy, database cdc_demo.

Consumer:
Container cdc-consumer, status healthy, endpoint Prometheus em http://localhost:8080/metrics/prometheus.

Coluna auditada:
spec23_ddl_only_1790828000

Teste 1 - DDL sem DML:
Comando executado na origem:
ALTER TABLE clientes ADD COLUMN spec23_ddl_only_1790828000 INTEGER;

Resultado:
DDL_ONLY_NOT_DETECTED no intervalo observado.

Evidencias:
- Nenhum log relevante foi emitido apos o ALTER TABLE isolado para spec23_ddl_only_1790828000.
- Nao houve log de schema_metadata_change_observed.
- Nao houve log de schema_validation.
- Nao houve log de schema_apply.
- Nao houve transaction_committed associado ao DDL isolado.
- A coluna apareceu em origin-schema.txt.
- A coluna nao apareceu em target-schema-after-ddl-only.txt.
- As metricas permaneceram iguais entre metrics-initial.prom e metrics-before.prom:
  cdc_schema_metadata_relation_messages_total 4 -> 4
  cdc_schema_metadata_changes_total 1 -> 1
  cdc_schema_apply_attempts_total 1 -> 1
  cdc_schema_apply_success_total 1 -> 1
  cdc_schema_apply_failure_total 0 -> 0
  cdc_schema_apply_rejected_total 0 -> 0
  cdc_events_received_total 15 -> 15
  cdc_events_total 15 -> 15
  cdc_transactions_received_total 15 -> 15
  cdc_transactions_total 15 -> 15

Teste 2 - DML posterior:
Comando executado na origem:
UPDATE clientes SET spec23_ddl_only_1790828000 = 123 WHERE id = 1;

Resultado:
DDL_DETECTED_ON_DML.

Evidencias:
- O consumer recebeu uma mensagem RELATION para public.clientes na transacao xid=1463.
- O log registrou schema_metadata_change_observed para spec23_ddl_only_1790828000.
- O log registrou schema_validation INCOMPATIBLE por COLUMN_MISSING para spec23_ddl_only_1790828000.
- O log registrou schema_apply status=started, status=staged e status=success para spec23_ddl_only_1790828000.
- O log registrou transaction_committed para xid=1463, eventos=1.
- A coluna apareceu no destino em target-schema-after.txt.

Observacao de ambiente:
Antes da auditoria havia drift anterior entre origem e destino: teste_metricas_2 e teste_metricas_3 existiam na origem e nao no destino. No mesmo DML posterior, o consumer tambem detectou e aplicou essas duas colunas. Por isso os deltas de schema apply no Teste 2 sao +3, nao +1.

Teste 3 - Metricas:
Resultado:
Metricas coerentes com os logs observados.

Deltas entre metrics-before.prom e metrics-after.prom:
- cdc_schema_metadata_relation_messages_total: 4 -> 5 (+1)
- cdc_schema_metadata_changes_total: 1 -> 4 (+3)
- cdc_schema_apply_attempts_total: 1 -> 4 (+3)
- cdc_schema_apply_success_total: 1 -> 4 (+3)
- cdc_schema_apply_failure_total: 0 -> 0 (+0)
- cdc_schema_apply_rejected_total: 0 -> 0 (+0)
- cdc_events_received_total: 15 -> 16 (+1)
- cdc_events_total: 15 -> 16 (+1)
- cdc_transactions_received_total: 15 -> 16 (+1)
- cdc_transactions_total: 15 -> 16 (+1)

Correlacao:
- Cada schema_apply status=started observado para as tres colunas pendentes correspondeu a cdc_schema_apply_attempts_total +1.
- Cada schema_apply status=success observado para as tres colunas pendentes correspondeu a cdc_schema_apply_success_total +1.
- Nenhum status=failure ou status=rejected foi observado, e os respectivos contadores nao aumentaram.

Teste 4 - DML apos sincronizacao:
Comando executado na origem:
UPDATE clientes SET spec23_ddl_only_1790828000 = 456 WHERE id = 110;

Resultado:
DML replicado sem novo schema_apply.

Evidencias:
- Foi usado id=110 porque existe tanto na origem quanto no destino.
- O destino passou a apresentar spec23_ddl_only_1790828000 = 456 para id=110.
- O log registrou schema_validation COMPATIBLE e transaction_committed para xid=1464.
- Nao houve novo schema_metadata_change_observed.
- Nao houve novo schema_apply.
- As metricas de apply permaneceram iguais entre metrics-after.prom e metrics-after-test4.prom:
  cdc_schema_apply_attempts_total 4 -> 4
  cdc_schema_apply_success_total 4 -> 4
  cdc_schema_apply_failure_total 0 -> 0
  cdc_schema_apply_rejected_total 0 -> 0

Classificacao final:
DDL_DETECTED_ON_DML

Conclusao:
Schema Evolution atualmente depende de um evento DML posterior para observar a nova definicao de schema atraves da mensagem RELATION.

Esta auditoria nao classifica o comportamento como bug. Ela confirma que, no fluxo observado, ALTER TABLE isolado nao produziu deteccao/aplicacao imediata pelo consumer; a descoberta ocorreu quando um DML posterior fez o pgoutput enviar uma nova RelationMessage para a tabela.
