# Estado Atual do CDCCore

Data do levantamento: 2026-09-27

## Resumo

O **CDCCore** e um consumidor e replicador de Change Data Capture. Ele usa `pgoutput`, preserva fronteiras transacionais, separa engine e sink, e confirma LSN ao PostgreSQL somente depois de sucesso no sink.

Sinks implementados:

- `FileSink`: destino padrao, JSON Lines em arquivo.
- `PostgreSQLSink`: aplica `INSERT`, `UPDATE` e `DELETE` usando uma conexao independente; oferece modo explicito e modo generico controlado por `CDC_TABLE_INCLUDE`.

## SPEC 03

Arquivos alterados:

- `consumer/main.go`
- `consumer/sink.go`
- `consumer/main_test.go`

Resultado:

- Criada abstracao minima de sink orientada a transacao.
- `PostgreSQLSink` aplica eventos em uma transacao do destino.
- Falhas durante aplicacao fazem rollback.

Limitacoes:

- O modo `explicit` suporta `public.clientes` e `public.enderecos`.
- O modo `generic` suporta tabelas com chave primaria presentes em `CDC_TABLE_INCLUDE`, desde que a estrutura ja exista no destination.
- Criacao e evolucao de schema no destination continuam sob responsabilidade de migrations.

## SPEC 04

Arquivos alterados:

- `consumer/sink.go`
- `consumer/main.go`
- `consumer/main_test.go`

Resultado:

- ACK do source continua condicionado ao sucesso do sink.
- `cdc_applied_transactions` e criado no destination.
- Idempotencia usa chave `(source_id, commit_lsn)` na mesma transacao dos dados de negocio.
- `CDC_TEST_PAUSE_AFTER_SINK_COMMIT=true` e aceito como alias do mecanismo deterministico de pausa antes do ACK.

## SPEC 05

Arquivos alterados:

- `consumer/config.go`
- `consumer/main.go`
- `consumer/observability.go`

Resultado:

- Retry com backoff progressivo configuravel.
- `CDC_RETRY_MAX_ATTEMPTS=0` representa retry infinito.
- Readiness fica indisponivel durante erro/retry.

Limitacoes:

- Ainda nao ha checkpoint paralelo; o restart continua baseado no `confirmed_flush_lsn`.

## SPEC 06

Arquivos alterados:

- `consumer/config.go`
- `consumer/.env.example`
- `README.md`

Resultado:

- Configuracao centralizada e validada.
- Source e destination possuem configuracoes separadas.
- TLS e configuravel separadamente por `CDC_SOURCE_PGSSLMODE` e `CDC_DEST_PGSSLMODE`.
- As variaveis genericas `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` e `PGSSLMODE` foram substituidas por `CDC_SOURCE_PG*`.
- Logs nao imprimem DSN completo nem senha.

## SPEC 07

Arquivos alterados:

- `consumer/observability.go`
- `consumer/main.go`

Resultado:

- Endpoint `/livez`.
- Endpoint `/readyz`.
- Endpoint `/metrics` em JSON com contadores internos e ultimos LSNs.

## SPEC 08

Arquivos alterados:

- `README.md`
- `consumer/main_test.go`

Resultado:

- Unicode segue coberto por teste automatizado.
- Tipos suportados documentados.
- Limites de carga e TOAST foram documentados como nao medidos nesta rodada.

## SPEC 09

Arquivos alterados:

- `consumer/Dockerfile`
- `consumer/.dockerignore`
- `docker-compose.yml`
- `README.md`

Resultado:

- Dockerfile multi-stage para o consumer.
- Compose local com PostgreSQL source, PostgreSQL destination, criacao do slot e consumer.
- Healthcheck do consumer usa `/readyz`.

## SPEC 10

Arquivos alterados:

- `consumer/main.go`
- `consumer/main_test.go`
- `README.md`
- `docs/STATUS.md`

Resultado:

- Documentadas separadamente as semanticas de `WALStart`, `ServerWALEnd`, `TransactionEndLSN`, `processedLSN` e `confirmedLSN`.
- Confirmado o framing do protocolo `pgoutput` v1: cada `OutputPluginWrite` e entregue em um `XLogData`, sem uma sequencia concatenada com comprimento interno para percorrer.
- O decoder preserva a transacao entre frames e rejeita evento, `COMMIT` ou novo `BEGIN` fora da ordem transacional valida.
- `processedLSN` so avanca depois de sucesso no sink e rejeita regressao.
- Adicionados testes deterministas para ordem de eventos, duas transacoes sequenciais, transacao atravessando varios frames, ausencia de ACK parcial e monotonicidade do LSN processado.

Limitacoes preservadas:

- O File Sink continua sem atomicidade e idempotencia.
- Transacoes grandes continuam acumuladas em memoria ate o `COMMIT`.
- Streaming de transacoes em andamento (`pgoutput` v2+) nao foi introduzido.

## SPEC 12

Arquivos alterados:

- `consumer/large_transaction_test.go`
- `README.md`
- `docs/STATUS.md`

Resultado:

- Criada medicao opt-in de memoria para transacoes pequenas, grandes e muito grandes usando `runtime.MemStats`.
- Confirmado que o caminho atual acumula toda a transacao em `pgoutputDecoder.tx.events []event` ate o `CommitMessage`.
- Confirmado que nao ha limite de eventos, bytes, memoria, timeout especifico de transacao grande ou backpressure interno.
- Investigado o suporte do PostgreSQL 17 e do `pglogrepl` para streaming de transacoes grandes.
- Streaming nao foi implementado nesta rodada.

Decisao final:

```text
ADIAR STREAMING
```

Status operacional de large transaction memory safety:

```text
SEM PROTECAO: sem limite de memoria/eventos/bytes e sem backpressure.
RISCO: crescimento ate OOM antes do COMMIT.
STREAMING: pgoutput v2 adiado.
```

O consumer nao rejeita, pausa, fragmenta nem descarrega para disco uma transacao ao atingir um limiar, pois nenhum limiar existe atualmente.

Justificativa:

- O ambiente atual usa PostgreSQL `17.11`, que suporta `pgoutput` `proto_version` 2 e streaming de grandes transacoes em andamento quando `streaming=on`.
- A versao instalada de `pglogrepl` ja possui `ParseV2`, `StreamStartMessageV2`, `StreamStopMessageV2`, `StreamCommitMessageV2`, `StreamAbortMessageV2` e mensagens DML v2 com `Xid`.
- O consumer atual inicia a replicacao com `proto_version '1'`, portanto nao recebe mensagens de streaming.
- A linha de base local mostrou crescimento linear, mas nao demonstrou necessidade imediata de alterar o contrato do sink.
- O maior risco de streaming nao e o parser: e preservar atomicidade source/destination sem confirmar fragmentos antes do commit source.

Medicao executada:

```bash
cd consumer
CDC_MEASURE_LARGE_TX=1 go test -run TestLargeTransactionMemoryBaseline -v
```

Resultado observado em 2026-09-27:

| Cenario | Eventos | Duracao | Heap antes do commit | Pico observado | Resultado |
| --- | ---: | ---: | ---: | ---: | --- |
| pequena | 1.000 | 2.374583ms | 1.49 MiB | 1.58 MiB | 1 transacao aplicada |
| grande | 25.000 | 28.800042ms | 16.20 MiB | 18.43 MiB | 1 transacao aplicada |
| muito grande | 100.000 | 131.511333ms | 63.83 MiB | 72.23 MiB | 1 transacao aplicada |

Modelo de memoria:

```text
pgoutputDecoder.tx.events
  -> []event
  -> event.Data / event.OldData
  -> map[string]any
  -> strings e valores decodificados
```

Observacoes:

- O payload sintetico usado na medicao e pequeno; colunas maiores e `OldData` aumentam o custo.
- No commit, o decoder copia o slice de eventos para `sourceTransaction`, elevando o pico.
- O custo real inclui overhead de slice, structs, mapas, interfaces e valores decodificados, nao apenas bytes recebidos do WAL.

Mensagens de streaming investigadas:

- `StreamStart`: inicia um segmento de stream de uma transacao e traz o `Xid`; `FirstSegment=1` indica primeiro segmento desse `Xid`.
- `StreamStop`: encerra o segmento atual; nao significa commit.
- `StreamCommit`: confirma a transacao em streaming e traz `Xid`, `CommitLSN` e `TransactionEndLSN`.
- `StreamAbort`: aborta a transacao ou subtransacao em streaming; no protocolo v2 traz `Xid` e `SubXid`.

Estrategias avaliadas:

- Buffer em disco: reduz memoria e permite aplicar no sink so depois de `StreamCommit`, preservando uma transacao curta no destino; exige armazenamento temporario, limpeza, validacao em recovery e replay serial.
- Transacao aberta no destino: reduz memoria sem replay intermediario, mas pode manter locks, WAL, conexao e vacuum pressionados durante toda a transacao source; falhas no meio dependem de rollback da conexao e reprocessamento desde o slot.

Semantica preservada:

```text
destination COMMIT
  -> processedLSN = TransactionEndLSN
  -> ACK via StandbyStatusUpdate
```

`processedLSN` nao deve avancar em `BEGIN`, DML, `StreamStart` ou `StreamStop`. Em streaming futuro, so poderia avancar depois de `StreamCommit` e commit confirmado no sink. O File Sink permanece sem atomicidade transacional e nao e referencia para validar streaming.

## XLogData / pgoutput / pglogrepl

- pglogrepl version: `v0.0.0-20260824121319-4ae5c490f7ce`.
- API utilizada: `pglogrepl.ParseXLogData` para retirar o cabecalho de replicacao e `pglogrepl.Parse` para decodificar `XLogData.WALData`.
- Entrada de `ParseXLogData`: payload do `CopyData` depois do byte identificador `w`.
- Saida de `ParseXLogData`: `WALStart`, `ServerWALEnd`, horario do servidor e `WALData`.
- Entrada de `Parse`: bytes de uma mensagem logica `pgoutput`, incluindo seu byte de tipo.
- Unidade retornada pelo parser: um valor que implementa `pglogrepl.Message`, como `BeginMessage`, `RelationMessage`, `InsertMessage`, `UpdateMessage`, `DeleteMessage` ou `CommitMessage`.
- Estado e bytes restantes: `Parse` nao mantem estado, nao retorna quantidade consumida e nao retorna bytes restantes.
- XLogData pode conter: no caminho atual `pgoutput` v1, uma escrita completa do output plugin, correspondente a uma mensagem logica. O walsender cria um `CopyData` para cada `OutputPluginWrite`.
- Multiplas mensagens: o consumer recebe mensagens sucessivas em `XLogData` sucessivos. Nao existe iteracao interna de mensagens concatenadas no contrato de `Parse`.
- Fragmentacao de mensagens: fragmentacao TCP e remontada por `pgconn` antes de `ReceiveMessage` retornar o `CopyData`. Uma escrita do output plugin nao e dividida pelo consumer entre dois `XLogData`; portanto nao ha mensagem `pgoutput` parcial a remontar nesta configuracao.
- Responsavel por reconstrucao: `pgconn` reconstitui a mensagem frontend/backend; `pgoutputDecoder.tx` mantem o estado da transacao logica entre as varias mensagens recebidas.
- Transacoes entre XLogData: `BEGIN`, metadados, alteracoes e `COMMIT` chegam em frames sucessivos. A transacao permanece em memoria ate o `COMMIT`.
- `WALStart`: posicao WAL associada ao frame recebido; nao e checkpoint de aplicacao.
- `ServerWALEnd`: fim atual do WAL conhecido pelo servidor; pode estar a frente do trabalho entregue e nao e confirmado automaticamente.
- `TransactionEndLSN`: posicao imediatamente posterior ao fim da transacao source, fornecida por `CommitMessage`.
- `processedLSN`: maior `TransactionEndLSN` cujo `ApplyTransaction` retornou sucesso; regressao e rejeitada.
- `confirmedLSN`: ultimo `processedLSN` enviado em `StandbyStatusUpdate`; como deriva de `processedLSN`, tambem e monotonicamente nao decrescente.
- ACK: `WALWritePosition`, `WALFlushPosition` e `WALApplyPosition` recebem o mesmo `processedLSN`, periodicamente, em keepalive com resposta solicitada ou no encerramento normal.
- Recovery: o reconnect consulta `confirmed_flush_lsn` e o fornece a `StartReplication`; trabalho aplicado no destino mas ainda nao confirmado pode ser reenviado e e protegido pela idempotencia do PostgreSQL Sink.
- Teste com PostgreSQL real: uma transacao `INSERT`, `UPDATE`, `DELETE` preservou ordem, XID e fronteira transacional; duas transacoes consecutivas permaneceram separadas e tiveram LSNs crescentes.
- FALHA ENCONTRADA: NAO. O processamento de uma mensagem por `XLogData` corresponde ao framing produzido pelo PostgreSQL e ao contrato da versao instalada de `pglogrepl`. Nao e necessario parser ou buffer adicional.

## Testes executados

```bash
cd consumer
go test ./...
```

Resultado:

```text
ok cdc-postgres/consumer
```

Testes ainda pendentes nesta rodada:

```bash
docker compose restart consumer
```

Validacoes executadas:

```bash
docker compose config
docker compose build consumer
go test -v ./...
go test -race ./...
go test -v -count=3 ./...
```

Resultados:

- `docker compose config`: OK.
- `docker compose build consumer`: OK.
- `go test -v ./...`: OK.
- `go test -race ./...`: OK quando executado isoladamente.
- `go test -v -count=3 ./...`: OK.

Observacao: os testes de integracao usam o replication slot real `cdc_slot`, portanto comandos de teste que executem em paralelo podem falhar por disputa legitima do slot.
