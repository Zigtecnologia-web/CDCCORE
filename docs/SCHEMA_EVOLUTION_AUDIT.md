# SPEC 18 - Auditoria Técnica de Schema Evolution

## Ambiente

Auditoria executada com a infraestrutura local do projeto via:

```sh
./scripts/schema-evolution-audit.sh
```

Artefatos gerados:

- `dist/schema-evolution-audit/sql.log`
- `dist/schema-evolution-audit/consumer.log`

Versões registradas em `sql.log`:

| Componente | Versão |
| --- | --- |
| PostgreSQL | 17.11 (Debian 17.11-1.pgdg13+2) |
| `wal_level` | `logical` |
| Go local | go1.27.1 darwin/arm64 |
| Imagem de build do consumer | golang:1.25-alpine |
| pgx | v5.11.0 |
| pglogrepl | v0.0.0-20260824121319-4ae5c490f7ce |
| Output plugin | `pgoutput` |
| Slot | `cdc_slot` |
| Publication | `cdc_publication` |

## Metodologia

Foi adicionada instrumentação temporária acionada por `CDC_SCHEMA_AUDIT_LOG=true`. Ela registra `event=schema_evolution_audit` para mensagens `BEGIN`, `RELATION`, `INSERT`, `UPDATE`, `DELETE` e `COMMIT`, incluindo `wal_start`, `server_wal_end`, `transaction_id`, `relation_id`, colunas de `RelationMessage` e um snapshot do catálogo atual quando a mensagem de relação é processada.

O script recria os volumes Docker, recompila o consumer, inicia um consumer com `FileSink` para observar o stream sem falhar por incompatibilidade de destino, executa os experimentos e depois testa o `PostgreSQLSink` genérico.

Observação importante: o script usa o mesmo slot durante toda a sequência e recria tabelas entre cenários. Portanto, alguns logs `schema_change_detected` refletem comparação entre cenários, não necessariamente um único DDL isolado. As conclusões abaixo priorizam os logs `schema_evolution_audit` e o SQL executado.

## Evidências Principais

Baseline DML:

- `INSERT`: `BEGIN -> RELATION -> INSERT -> COMMIT` nas linhas 6, 8, 10 e 11 do `consumer.log`.
- `UPDATE`: `BEGIN -> UPDATE -> COMMIT`; não houve novo `RELATION` porque a metadata já estava cacheada.
- `DELETE`: `BEGIN -> DELETE -> COMMIT`; o evento carregou a old tuple como `data`.

Schema changes observados:

| Cenário | Evidência |
| --- | --- |
| ADD COLUMN separado de DML | `ALTER TABLE` não gerou evento próprio; o próximo `INSERT` gerou `RELATION` com `telefone` nas linhas 29-35. |
| ADD COLUMN + DML na mesma transação | `BEGIN -> RELATION -> INSERT -> COMMIT`; não existe mensagem DDL separada, linhas 39-44. |
| DROP COLUMN | O próximo DML gerou `RELATION` sem `telefone`, linhas 57-63. |
| RENAME COLUMN | O stream mostrou `RELATION` com `nome_completo`; não indicou `RENAME`, linhas 67-74. |
| ALTER TYPE | `RELATION` expôs OID/typmod novos para `nome VARCHAR(200)`, linhas 77-83. |
| SET NOT NULL | `RelationMessage` não expôs nullability; só o snapshot do catálogo mostrou `email` sem `nullable`, linhas 87-102. |
| DEFAULT | `RelationMessage` não expôs default; só o catálogo mostrou `has_default`, linhas 97-102. |
| CREATE TABLE publicada | Primeiro DML na tabela nova gerou `RELATION` para `public.audit_enderecos`, linhas 106-112. |
| DROP TABLE | `DROP TABLE` não gerou mensagem útil no stream; não houve DML posterior possível para a tabela removida. |
| Duas alterações rápidas | Uma `RELATION` posterior mostrou estado final com `a` e `b`, linhas 117-123. |
| DDL + DDL + DML na mesma transação | Uma `RELATION` mostrou estado final com `a` e `b`, linhas 129-132. |
| Rollback DDL | Catálogo após rollback voltou a 3 colunas; o DML posterior gerou `RELATION` sem a coluna rollbackada, linhas 135-142. |
| DDL com falha | `ALTER COLUMN email SET NOT NULL` falhou com `contains null values`; DML posterior seguiu com schema anterior, linhas 146-158. |
| RelationMessage repetido | Inserts consecutivos sem schema change não geraram nova `RELATION`, linhas 162-172. |
| INSERT/UPDATE/DELETE após mudança | Após adicionar `telefone`, `INSERT`, `UPDATE` e `DELETE` usaram tuple de 4 colunas, linhas 176-196. |
| PostgreSQLSink incompatível | Validação marcou `COLUMN_MISSING telefone` e sink falhou com `coluna public.clientes.telefone nao existe no destination`, linhas 205-238. |
| Apply manual | Depois de adicionar `telefone` no destination, os eventos pendentes e novo DML foram aplicados e o ACK avançou, linhas 244-258. |

## Respostas Obrigatórias

**Q1. `pgoutput` transmite DDL diretamente?**
Não. Nenhum experimento mostrou mensagem DDL explícita.

**Q2. Existe mensagem específica para ADD/DROP/RENAME/ALTER/CREATE/DROP TABLE?**
Não. O que aparece é metadata de relação quando há DML publicado para aquela relação.

**Q3. `RelationMessage` representa mudança estrutural ou apenas metadata?**
Apenas metadata. Ela pode aparecer após mudança estrutural, na primeira necessidade de decodificar DML, mas também aparece no baseline e em replays.

**Q4. É possível determinar a operação DDL exata?**
Parcialmente, por comparação entre snapshots conhecidos no consumer. O protocolo não informa a operação. `RENAME` não é distinguível de drop+add sem heurística.

**Q5. É possível determinar o estado estrutural anterior?**
Parcialmente, apenas se o consumer já tinha snapshot anterior confiável para a mesma relação lógica.

**Q6. É possível determinar o estado estrutural posterior?**
Parcialmente. `RelationMessage` traz nomes, OIDs e typmod das colunas usadas na replicação. Nullability/default exigem catálogo atual.

**Q7. O catálogo pode reconstruir mudança histórica com segurança?**
Não como snapshot histórico. A consulta ao catálogo retorna o estado atual no momento da consulta. No teste ela coincidiu com a relação processada, mas duas mudanças rápidas já mostram que estados intermediários podem ser perdidos.

**Q8. Alterações consecutivas podem ser distinguidas?**
Parcialmente. Se houver DML entre elas e snapshots anteriores, sim. Duas alterações antes do próximo DML aparecem como um estado final.

**Q9. Alteração rollbackada pode ser diferenciada de commitada?**
Sim quanto ao efeito: rollback não apareceu como schema posterior no stream nem no catálogo final. Não há evento de rollback DDL a processar.

**Q10. DDL + DML na mesma transação preserva atomicidade?**
Parcialmente. O stream preserva `BEGIN -> RELATION -> DML -> COMMIT` e XID/commit LSN, mas não preserva o DDL como operação explícita.

**Q11. PostgreSQLSink pode aplicar mudança sem alterar invariantes?**
Para `ADD COLUMN` nullable, manualmente sim. Automaticamente só é seguro em escopo reduzido e depois de validação. Sem aplicar schema, o sink genérico falha antes do ACK.

**Q12. Qual parte das SPECs 15-17 permanece válida?**
Detect-only por comparação de metadata, validação Source x Destination no momento da relation, e apply manual permanecem válidos.

**Q13. Qual parte precisa ser modificada?**
Qualquer texto que trate `RelationMessage` como DDL, que prometa operação exata, ou que dependa de catálogo como snapshot histórico.

**Q14. Alguma capacidade deve sair do roadmap?**
Auto apply amplo deve sair por enquanto. Manter no máximo auto apply experimental para `ADD COLUMN` nullable, atrás de flag e validação.

## Matriz de Capacidades

| Capacidade | Resultado | Evidência |
| --- | --- | --- |
| Detect ADD COLUMN | PARTIALLY_SUPPORTED | Comparação de relation anterior/posterior detectou `telefone`; não há DDL explícito. |
| Detect DROP COLUMN | PARTIALLY_SUPPORTED | Relation posterior sem coluna permite comparar quando snapshot anterior existe. |
| Detect RENAME COLUMN | NOT_SUPPORTED | Observado como coluna removida + adicionada; operação exata ausente. |
| Detect ALTER TYPE | PARTIALLY_SUPPORTED | OID/typmod mudam em `RelationMessage`; operação exata não vem no protocolo. |
| Detect CREATE TABLE | PARTIALLY_SUPPORTED | Primeira DML em tabela publicada gera relation; CREATE isolado não. |
| Detect DROP TABLE | NOT_SUPPORTED | DROP isolado não gerou mensagem útil. |
| Detect nullability | PARTIALLY_SUPPORTED | Não vem no `RelationMessage`; só via catálogo atual. |
| Detect DEFAULT | PARTIALLY_SUPPORTED | Não vem no `RelationMessage`; só via catálogo atual. |
| Identificar transaction ID | SUPPORTED | `BEGIN` expõe XID; decoder associa relation/DML à transação ativa. |
| Identificar LSN | PARTIALLY_SUPPORTED | DML/commit têm LSN; `RelationMessage` pode chegar com WALStart `0/0`, usando contexto da transação. |
| Identificar operação exata | NOT_SUPPORTED | `pgoutput` não transmite DDL/operação. |
| Reconstruir schema anterior | PARTIALLY_SUPPORTED | Depende do cache interno anterior. |
| Reconstruir schema posterior | PARTIALLY_SUPPORTED | Relation traz nome/OID/typmod; catálogo complementa nullability/default de forma não histórica. |
| Preservar atomicidade DDL+DML | PARTIALLY_SUPPORTED | Fronteira transacional existe, mas DDL explícito não. |
| Aplicar ADD COLUMN | PARTIALLY_SUPPORTED | Manual funcionou; automático deve se limitar a coluna nullable e validação. |
| Recovery após DDL | SUPPORTED | Sink falhou sem ACK; replay reaplicou depois de ajuste manual e só então avançou ACK. |

## Revisão das SPECs

### SPEC 15 - Schema Change Detection

Manter `DETECT ONLY`, mas renomear conceitualmente para detecção de mudança de metadata observada. `RelationMessage` deve ser tratado como snapshot de metadata, não como evento DDL. Operações devem aceitar `UNKNOWN` como resultado normal.

### SPEC 16 - Schema Compatibility Validation

Validação Source x Destination é útil para bloquear aplicação incompatível. Porém o Source Schema lido do catálogo é estado atual, não snapshot histórico no LSN. A SPEC precisa explicitar essa limitação e evitar decisões que dependam de estados intermediários.

### SPEC 17 - Controlled Schema Apply

Auto apply geral não está suportado. A única candidata segura nesta fase é `ADD COLUMN` nullable, sem default obrigatório, validada antes e depois, com transação no destination e mantendo:

```text
Destination COMMIT
        -> processedLSN
        -> ACK Source
```

## Decisões Arquiteturais

1. `Schema Evolution = DETECT ONLY` continua sendo o default.
2. `CDC_SCHEMA_EVOLUTION=auto` não deve ser habilitado como capacidade geral.
3. `RelationMessage` não deve ser modelado como `SchemaChangeEvent` público.
4. O decoder pode manter snapshots por tabela para sinalizar mudanças observadas, sempre com `UNKNOWN` quando houver ambiguidade.
5. Catálogo PostgreSQL pode enriquecer validação, mas não substitui histórico de WAL.
6. `PostgreSQLSink` deve bloquear DML incompatível antes de ACK.
7. Apply automático, se mantido, deve começar apenas por `ADD COLUMN` nullable e precisa de testes de crash/replay específicos.

## Próximos Passos

1. Separar formalmente logs de auditoria (`CDC_SCHEMA_AUDIT_LOG`) do contrato de observabilidade.
2. Ajustar SPEC 15 para falar em "metadata change observed".
3. Ajustar SPEC 16 para declarar que catálogo é estado atual.
4. Reduzir SPEC 17 a manual apply e, opcionalmente, auto apply limitado para `ADD COLUMN` nullable.
5. Melhorar o executor para usar um slot/tabela isolados por cenário, evitando artefatos entre experimentos.
