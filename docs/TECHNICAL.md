<p align="center">
  <img src="../assets/cdccore-logo-v2.png" alt="CDCCore" width="640">
</p>

<p align="center"><strong>Consumer and Replicator for Change Data Capture</strong></p>

**CDCCore** e um consumidor e replicador transacional de alteracoes do PostgreSQL. Uma alteracao entra no WAL, passa por logical replication com `pgoutput`, fica disponivel no replication slot `cdc_slot` e chega ao consumidor Go, que decodifica a mensagem, monta eventos estruturados e aplica a transacao em um sink.

Os sinks atuais sao `FileSink`, que adiciona eventos em JSON Lines ao arquivo `consumer/cdc.txt`, e `PostgreSQLSink`, que aplica `INSERT`, `UPDATE` e `DELETE` em um PostgreSQL de destino. O PostgreSQLSink possui um modo explicito para compatibilidade e um modo generico orientado por allowlist. Nao ha Kafka, Debezium, filas ou frameworks de CDC.

## Estrutura

```text
cdc-postgres/
├── assets/
│   ├── cdccore-logo.png
│   └── cdccore-logo-v2.png
├── consumer/
│   ├── go.mod
│   ├── go.sum
│   ├── .env.example
│   ├── main.go
│   └── cdc.txt
├── postgres/
│   └── init/
│       └── 001_create_database.sql
├── scripts/
│   ├── create-slot.sh
│   └── test-cdc.sh
├── docker-compose.yml
├── README.md
└── .gitignore
```

## Como os dados percorrem o projeto

1. Um `INSERT`, `UPDATE` ou `DELETE` altera uma linha no PostgreSQL.
2. Antes de a alteracao ser considerada persistida, o PostgreSQL a registra no WAL.
3. Como `wal_level=logical`, o WAL contem informacao suficiente para reconstruir mudancas logicas.
4. O plugin `pgoutput` emite mensagens estruturadas de replicacao logica.
5. O replication slot `cdc_slot` preserva a posicao do consumidor e impede que o WAL ainda necessario seja descartado.
6. O programa Go abre uma conexao pelo protocolo de replicacao e recebe mensagens `BEGIN`, `RELATION`, `INSERT`, `UPDATE`, `DELETE` e `COMMIT`.
7. O consumidor mantem metadados das relacoes, preserva o estado da transacao e chama o sink no `COMMIT`.
8. O consumidor so confirma o LSN ao PostgreSQL depois que o sink confirma a transacao.

Para um `INSERT`, o consumidor recebe a fronteira transacional e uma mensagem de linha. O evento so e persistido quando o `COMMIT` correspondente e reconhecido.

## Conceitos

### WAL

WAL significa Write-Ahead Log. E um registro sequencial das alteracoes do banco, escrito antes das paginas de dados correspondentes. Ele permite recuperacao depois de falhas e tambem alimenta os mecanismos de replicacao.

### `wal_level=logical`

`wal_level` define quanta informacao o PostgreSQL guarda no WAL. O valor `logical` inclui os dados necessarios para logical decoding. Esta e a unica configuracao alterada no `docker-compose.yml`; os valores padrao de slots e WAL senders sao suficientes para este experimento.

### Logical decoding

Logical decoding transforma os registros internos do WAL em alteracoes logicas, como insercao, atualizacao e remocao de linhas. O formato final e definido por um output plugin.

Este projeto usa `pgoutput`, o output plugin nativo usado pela replicacao logica do PostgreSQL. O consumidor nao depende de texto produzido por plugin: ele decodifica as mensagens do protocolo e cria um modelo interno de evento CDC.

### Replication slot

Um replication slot representa o progresso de um consumidor. O PostgreSQL mantem o WAL necessario enquanto o slot nao confirma o recebimento. Isso evita perda de eventos durante uma parada, mas um slot abandonado pode reter WAL e ocupar disco.

O slot deste projeto se chama `cdc_slot` e usa `pgoutput`. O programa Go exige que ele ja exista e nunca cria outro slot automaticamente.

### LSN

LSN significa Log Sequence Number. E a posicao de um registro dentro do WAL, semelhante a um endereco no fluxo de alteracoes. O PostgreSQL usa LSNs para recuperacao, replicacao e para saber ate onde cada slot consumiu.

O consumidor envia periodicamente seu progresso ao PostgreSQL. O checkpoint confirmado nunca avanca apenas porque um `XLogData` foi recebido: o programa separa a posicao recebida, a posicao processada e a posicao confirmada, e so envia ao PostgreSQL a posicao ja aceita pelo sink. Nao existe checkpoint proprio em arquivo ou banco nesta etapa; ao reiniciar, o programa consulta `confirmed_flush_lsn` do slot.

As posicoes usadas no fluxo possuem significados diferentes:

- `XLogData.WALStart` identifica a posicao WAL associada a mensagem `pgoutput` recebida. `ServerWALEnd` e apenas o fim atual do WAL informado pelo servidor e nao representa trabalho processado.
- `CommitMessage.TransactionEndLSN` identifica a posicao imediatamente posterior ao fim da transacao source. Essa e a posicao usada como identidade da transacao no sink e como candidata a progresso depois do commit do destino.
- `processedLSN` e o maior `TransactionEndLSN` cujo `ApplyTransaction` terminou com sucesso. Ele nunca avanca por `BEGIN`, `INSERT`, `UPDATE`, `DELETE` ou simples recebimento de WAL.
- `confirmedLSN` e o ultimo `processedLSN` enviado ao PostgreSQL por `StandbyStatusUpdate`; o slot reflete esse progresso em `confirmed_flush_lsn`.

No protocolo `pgoutput` v1 usado pelo projeto, cada chamada de escrita do output plugin e encapsulada pelo walsender em um `CopyData` do tipo `XLogData`. Assim, `WALData` contem uma mensagem logica completa e `pglogrepl.Parse` e chamado uma vez por frame. Uma transacao normalmente ocupa varios `XLogData` (`BEGIN`, metadados, alteracoes e `COMMIT`), e o estado da transacao permanece no decoder entre esses frames. O protocolo v1 nao define comprimento geral dentro de `WALData` para uma concatenacao arbitraria de mensagens; por isso o consumidor nao tenta subdividir bytes que pertencem a uma unica mensagem valida.

### Papel do programa Go

O programa nao consulta a tabela em polling. Ele se conecta ao PostgreSQL em modo de replicacao, inicia o stream do slot existente, recebe bytes `pgoutput`, decodifica mensagens de replicacao, monta eventos CDC e os grava em `cdc.txt`. `Ctrl+C` cancela a espera, fecha o arquivo e encerra a conexao.

### Persistencia em arquivo

Ao iniciar, o consumidor abre `consumer/cdc.txt` com as opcoes de criar o arquivo quando necessario e escrever sempre no final. O arquivo nao e carregado para memoria e nunca e truncado. Por isso, reiniciar o programa preserva os eventos anteriores e os novos eventos continuam o historico.

Cada evento de negocio ocupa uma linha JSON. O LSN processado so avanca depois que todos os eventos da transacao foram escritos sem erro. O arquivo nao substitui um checkpoint de LSN: nesta etapa, ele e apenas o destino persistente dos eventos estruturados.

O contrato exige `data` em `INSERT` e `DELETE`, e exige `old_data` e `data` em `UPDATE`. Para que o PostgreSQL forneca o estado anterior completo, as tabelas source devem usar `REPLICA IDENTITY FULL`; o consumer rejeita um `UPDATE` sem `old_data` ou um `DELETE` sem `data`. O `lsn` de cada evento preserva a posicao recebida com a alteracao, enquanto o checkpoint da transacao usa separadamente o `TransactionEndLSN` do `COMMIT`.

Exemplo:

```json
{"type":"INSERT","lsn":"0/19B5A20","transaction_id":1120,"schema":"public","table":"clientes","data":{"email":"maria@example.com","id":10,"nome":"Maria"}}
```

Quando o consumidor e encerrado, o replication slot mantem no PostgreSQL o progresso que ja foi confirmado. Alteracoes ainda nao consumidas permanecem disponiveis pelo slot para a proxima execucao. A semantica de entrega desta etapa e at-least-once: um evento pode reaparecer depois de falhas, mas uma posicao nao deve ser confirmada antes da persistencia correspondente no arquivo.

### PostgreSQLSink e idempotencia

Com `CDC_SINK=postgres`, o destino usa conexao independente da origem:

```dotenv
CDC_SOURCE_ID=cdc-demo
CDC_SINK=postgres
CDC_DEST_PGHOST=localhost
CDC_DEST_PGPORT=5433
CDC_DEST_PGDATABASE=cdc_demo
CDC_DEST_PGUSER=postgres
CDC_DEST_PGPASSWORD=postgres
CDC_DEST_PGSSLMODE=disable
```

O PostgreSQLSink possui dois modos de aplicacao:

- `explicit`: mantem os handlers dedicados de `public.clientes` e `public.enderecos`.
- `generic`: descobre colunas e chave primaria no destination e gera `INSERT`, `UPDATE` e `DELETE` parametrizados para qualquer tabela autorizada.

O modo generico exige uma allowlist e nao cria nem altera tabelas no destination:

```dotenv
CDC_POSTGRES_APPLY_MODE=generic
CDC_TABLE_INCLUDE=public.clientes,public.enderecos
CDC_PUBLICATION_AUTOCONFIGURE=false
```

Com `CDC_PUBLICATION_AUTOCONFIGURE=true`, o consumer adiciona a publication as tabelas da allowlist que ainda estiverem ausentes. Essa opcao exige que o usuario de source seja dono da publication ou tenha privilegios equivalentes. Ela somente adiciona tabelas; remover uma tabela da allowlist nao a remove automaticamente da publication.

Valores sao enviados por parametros PostgreSQL, e identificadores de schema, tabela e coluna sao escapados. Tabelas fora de `CDC_TABLE_INCLUDE`, tabelas sem chave primaria e colunas ausentes no destination interrompem a aplicacao sem confirmar o LSN.

Para idempotencia, o destino cria `cdc_applied_transactions` com chave primaria `(source_id, commit_lsn)`. Esse registro e gravado na mesma transacao que altera `public.clientes`. Se o processo morrer depois do commit no destino e antes do ACK ao source, a reentrega e detectada por essa chave e os dados de negocio nao sao reaplicados.

### Schema Evolution controlado

O decoder mantem snapshots locais das `RelationMessage` recebidas do `pgoutput`, indexados por `relation_id` e pela identidade `schema.table`. O modelo `RelationMetadata` guarda somente `RelationID`, schema, tabela e, por coluna, nome, OID de tipo e typmod. Nullability, default e DDL original nao sao atribuidos ao protocolo. O modelo interno de diferenca nao faz parte do contrato publico de Change Event.

As diferencas observaveis usam `COLUMN_ADDED`, `COLUMN_REMOVED`, `COLUMN_METADATA_CHANGED`, `RELATION_ID_CHANGED` e `RELATION_RENAMED_OR_REPLACED`. Quando a causa nao pode ser determinada com seguranca, o detector usa `UNKNOWN`. Por exemplo, uma coluna removida e outra adicionada simultaneamente pode representar rename ou operacoes distintas; o CDCCore nao converte isso em DDL presumido.

Essa fase nao cria eventos `SCHEMA_CHANGE` no contrato publico. As mudancas observadas usam o log `event=schema_metadata_change_observed`, associado ao relation ID, LSN e XID ativo quando disponivel. `/metrics` expoe os contadores `cdc_schema_metadata_relation_messages_total`, `cdc_schema_metadata_changes_total` e `cdc_schema_metadata_unknown_total`.

`CDC_SCHEMA_EVOLUTION` controla a etapa de aplicacao:

- `disabled`: padrao; nenhuma DDL e executada no destination.
- `manual`: registra bloqueio para autorizacao externa futura; nenhuma DDL e executada automaticamente.
- `auto`: avalia politica e aplica somente operacoes permitidas.

Na primeira politica automatica, apenas `ADD COLUMN` nullable pode ser aplicado e o modo `generic` e obrigatorio para que o DML inclua a nova coluna. O decoder enriquece a observacao com a validacao atual de source/destination; em recovery, uma coluna ausente validada no destination tambem gera um candidato interno sem alegar qual DDL ocorreu no source. O PostgreSQL Sink gera o DDL tipado usando escaping com `pgx.Identifier`. O CDCCore nunca executa SQL DDL recebido do source.

Tipos aceitos inicialmente sao uma allowlist pequena de tipos PostgreSQL escalares, incluindo `TEXT`, `INTEGER`, `BIGINT`, `BOOLEAN`, `JSONB`, `DATE`, `UUID`, `TIMESTAMP`, `TIME`, `NUMERIC` e `VARCHAR` com definicao vinda do catalogo. Tipos desconhecidos sao bloqueados.

O apply ocorre dentro da mesma transacao do PostgreSQL Sink, antes dos eventos DML da source transaction. Assim, `processedLSN` continua avancando somente depois que schema apply, DML e registro de idempotencia confirmarem juntos no destination. Se a coluna ja existir, o sink revalida tipo/nullability antes de considerar `ALREADY_APPLIED`; se existir de forma incompatível, a transacao falha e o ACK nao avanca.

`CREATE TABLE`, `DROP COLUMN`, `DROP TABLE`, `ALTER TYPE`, rename e alteracao de chave primaria continuam bloqueados. No modo `generic`, a mesma allowlist de `CDC_TABLE_INCLUDE` limita o schema apply. No modo `explicit`, o apply fica restrito as tabelas explicitamente suportadas. Os logs usam `event=schema_apply`; as metricas sao `cdc_schema_apply_attempts_total`, `cdc_schema_apply_success_total`, `cdc_schema_apply_failure_total` e `cdc_schema_apply_rejected_total`.

#### Investigacao do pgoutput

O protocolo usado continua sendo `pgoutput` v1. A `RelationMessage` descreve OID da relacao, namespace, nome, replica identity e, para cada coluna publicada, nome, flag de chave, OID do tipo e type modifier. Ela e metadata necessaria para interpretar DML e nao representa necessariamente um `ALTER TABLE`.

O PostgreSQL envia a descricao da relacao antes do primeiro DML relevante daquela relacao no stream. Reapresentar metadata identica nao gera deteccao. Quando a descricao muda, o detector compara os snapshots; o primeiro snapshot observado e apenas baseline. Como o protocolo v1 nao inclui XID dentro da propria `RelationMessage`, o `transaction_id` registrado e o `BEGIN` ativo no decoder. Ele identifica a transacao de stream que apresentou a metadata e o DML, mas nao prova qual transacao executou um DDL anterior. Para a posicao, o detector usa o `WALStart` da mensagem; quando o frame de metadata chega com `0/0`, usa o `FinalLSN` do `BEGIN` ativo como posicao transacional disponivel.

O `pgoutput` nao fornece o SQL DDL, default, nullability nem uma notificacao geral de `CREATE TABLE`/`DROP TABLE`. Detectar essas propriedades ou reconstruir a operacao exata exigira consultar `pg_catalog` ou adotar outra fonte de eventos em uma SPEC futura. Uma alteracao DDL sem DML publicado pode permanecer invisivel ate uma relacao voltar a ser descrita.

O snapshot e volatil e reconstruido a cada conexao. Depois de reconnect, a primeira descricao volta a ser baseline; se o intervalo reprocessado contiver snapshots anterior e posterior, a mudanca podera ser observada novamente. Nao existe historico persistente de Schema Changes nesta fase.

Referencias da investigacao: [formato das mensagens de replicacao logica](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html) e [comportamento de schema na replicacao logica](https://www.postgresql.org/docs/current/logical-replication-subscription.html).

## Large Transactions

### Current behavior

O consumidor continua usando `pgoutput` com `proto_version '1'`. Nesse modo, uma transacao source e mantida inteira em memoria no decoder ate o `COMMIT`: `pgoutputDecoder.tx.events` cresce como `[]event`, e cada `event` contem strings, mapas `Data`/`OldData`, valores decodificados e metadados de tabela derivados do registry de relacoes. No `CommitMessage`, o decoder monta um `sourceTransaction`, chama `ApplyTransaction` e so entao o `processedLSN` pode avancar para `TransactionEndLSN`.

Nao existe hoje limite de eventos por transacao, limite de bytes, limite de memoria, timeout especifico para uma transacao grande ou backpressure interno no decoder. Portanto:

```text
large transaction protection = inexistente
```

Status de seguranca de memoria: **nao protegido**. Uma transacao pode fazer o processo crescer ate a memoria disponivel e terminar por OOM. O consumer nao rejeita, pausa, fragmenta nem descarrega a transacao para disco ao atingir qualquer limiar, porque nao existem limites configurados de memoria, eventos ou bytes nem mecanismo de backpressure. O streaming de transacoes grandes via `pgoutput` v2 permanece **adiado**.

### Memory model

A memoria cresce principalmente em:

```text
pgoutputDecoder.tx.events
  -> []event
  -> event.Data / event.OldData
  -> map[string]any
  -> strings e valores decodificados
```

O custo de memoria nao e apenas o tamanho do payload recebido do PostgreSQL. Ha overhead de slice, structs, mapas, interfaces, strings, valores Go decodificados e, no momento do commit, uma copia do slice de eventos para `sourceTransaction`.

Foi adicionada uma medicao opt-in em `consumer/large_transaction_test.go`. Para executar:

```bash
cd consumer
CDC_MEASURE_LARGE_TX=1 go test -run TestLargeTransactionMemoryBaseline -v
```

O teste compara `heap before commit` e `peak observed heap` com a linha de base abaixo usando tolerancia padrao de `3.0x`. Para ajustar a tolerancia em uma maquina diferente:

```bash
CDC_MEASURE_LARGE_TX=1 CDC_LARGE_TX_MEMORY_TOLERANCE=4 go test -run TestLargeTransactionMemoryBaseline -v
```

Essa checagem e uma barreira contra regressao grosseira de memoria, nao uma garantia de limite operacional. Ela nao impede OOM em producao.

Linha de base observada em 2026-09-30 no ambiente local, com payload sintetico pequeno de `public.clientes`:

| Cenario | Eventos | Duracao | Heap antes do commit | Pico observado | Resultado |
| --- | ---: | ---: | ---: | ---: | --- |
| pequena | 1.000 | 2.901083ms | 1.72 MiB | 1.82 MiB | 1 transacao aplicada |
| grande | 25.000 | 63.408042ms | 18.84 MiB | 21.51 MiB | 1 transacao aplicada |
| muito grande | 100.000 | 229.230375ms | 74.35 MiB | 85.04 MiB | 1 transacao aplicada |

Esses valores nao sao requisito de performance: servem apenas como linha de base reproduzivel. Transacoes com colunas maiores, `OldData`, mais tabelas ou sinks mais caros podem consumir muito mais memoria.

### PostgreSQL streaming support

O Compose atual usa `postgres:17`; o ambiente medido reportou PostgreSQL `17.11` com `wal_level=logical`. A documentacao oficial do PostgreSQL 17 informa que o `pgoutput` aceita `proto_version` 1, 2, 3 e 4; a versao 2 existe em servidores 14+ e permite streaming de grandes transacoes em andamento quando a opcao `streaming` esta ligada. A mesma documentacao define que `streaming=off` e o padrao, `streaming=on` exige protocolo minimo 2, e `streaming=parallel` exige protocolo minimo 4.

No projeto atual, `StartReplication` usa:

```text
proto_version '1'
publication_names '<publication>'
```

Portanto o caminho atual nao recebe mensagens de streaming.

### pglogrepl support

A versao instalada e `github.com/jackc/pglogrepl v0.0.0-20260824121319-4ae5c490f7ce`. Ela ja fornece suporte para protocolo v2 em `messageV2.go`, incluindo:

- `ParseV2(data, inStream)`;
- `StreamStartMessageV2`;
- `StreamStopMessageV2`;
- `StreamCommitMessageV2`;
- `StreamAbortMessageV2`;
- mensagens DML v2 com `Xid` quando `inStream=true`.

Logo, se streaming for implementado no futuro, nao ha necessidade de criar structs proprias para essas mensagens. O consumer teria de manter o estado `inStream` entre `StreamStart` e `StreamStop`, chamar `ParseV2` e preservar o `Xid` da transacao em andamento.

### Decision

Decisao desta rodada: **ADIAR STREAMING**.

Justificativa:

- PostgreSQL 17 e `pglogrepl` suportam o protocolo necessario.
- A implementacao atual ainda preserva as garantias de ordem, at-least-once, idempotencia no PostgreSQL Sink e ACK somente apos sucesso no sink.
- A medicao local mostrou crescimento linear e reproduzivel, mas nao demonstrou necessidade imediata de trocar o modelo.
- Streaming reduziria memoria do decoder, mas introduz uma decisao de sink nao trivial: manter atomicidade source/destination com buffer em disco ou com transacao longa aberta no destino.
- Implementar streaming sem resolver essa atomicidade poderia quebrar a garantia mais importante do projeto.

### Future strategies

Duas estrategias ficam documentadas para uma SPEC futura:

```text
Source
  -> Stream segments
  -> Temporary storage
  -> StreamCommit
  -> Apply transaction
  -> Destination COMMIT
  -> ACK
```

Buffer em disco reduz pressao de heap e preserva aplicacao somente apos `StreamCommit`, mas exige armazenamento temporario, limpeza, recovery, replay e validacao de integridade.

```text
StreamStart
  -> Destination transaction
  -> DML
  -> StreamStop
  -> ...
  -> StreamCommit
  -> Destination COMMIT
  -> ACK
```

Transacao aberta no destination evita armazenar tudo em heap ou disco intermediario, mas pode manter locks, WAL, conexao e vacuum pressionados durante toda a transacao source e pode exigir rollback grande.

### Transaction semantics

No modelo atual:

```text
BEGIN source
  -> eventos em memoria
COMMIT source
  -> PostgreSQLSink BEGIN
  -> aplica todos os eventos
  -> registra cdc_applied_transactions(source_id, commit_lsn)
  -> PostgreSQLSink COMMIT
  -> processedLSN = TransactionEndLSN
  -> ACK posterior via StandbyStatusUpdate
```

No protocolo de streaming, o fluxo conceitual seria:

```text
StreamStart xid
  -> eventos parciais associados ao xid
StreamStop
...
StreamStart xid
  -> mais eventos
StreamStop
StreamCommit xid
```

`StreamStop` nao significa commit da transacao source. A confirmacao definitiva seria `StreamCommit`; `StreamAbort` descartaria o trabalho pendente. Antes de `StreamCommit`, `commit_lsn` ainda nao e identidade definitiva da transacao, entao qualquer estado temporario teria de ser identificado por `source_id + xid` ou mecanismo equivalente. O schema atual baseado em `(source_id, commit_lsn)` continua suficiente para o modelo nao streaming e para a idempotencia apos commit.

### Recovery semantics

O recovery atual continua baseado no replication slot:

```text
confirmed_flush_lsn
  -> StartReplication
  -> reentrega de trabalho nao confirmado
  -> PostgreSQLSink usa cdc_applied_transactions para idempotencia
```

Falhas antes do commit do destination nao avancam `processedLSN` e causam reprocessamento. Falhas depois do commit do destination e antes do ACK podem causar reentrega, mas o PostgreSQL Sink detecta `(source_id, commit_lsn)` ja aplicado na mesma transacao de destino e nao reaplica os dados.

Em streaming futuro, falhas no meio exigiriam um modelo explicito:

- antes de qualquer evento: nenhum estado duravel necessario;
- no meio com buffer em disco: limpar ou validar buffer temporario por `source_id + xid`;
- no meio com transacao aberta no destino: reconexao implica rollback do destino e reprocessamento desde o slot;
- durante `StreamCommit`: so avancar `processedLSN` depois do commit confirmado no destino;
- commit ambiguo no destino: preservar a protecao existente por `(source_id, commit_lsn)`.

### Known limitations

- Large transaction memory safety: sem limite de memoria, eventos ou bytes e sem backpressure; ha risco de OOM antes do `COMMIT`. Streaming via `pgoutput` v2 permanece adiado.
- O File Sink nao e transacional. Com transacoes grandes ou streaming futuro, ele pode produzir escrita parcial e duplicacao apos recovery; ele nao deve ser usado como referencia de atomicidade.
- O PostgreSQL Sink e a referencia de atomicidade, mas streaming futuro ainda exigiria escolher entre buffer em disco e transacao longa no destino.
- Nao ha protecao contra OOM para transacoes maiores que a memoria disponivel.
- Nao ha paralelismo nesta decisao; streaming, se vier, deve continuar serial ate nova SPEC.

## Preparacao

Requisitos:

- Docker com Docker Compose
- Go 1.25 ou mais recente

Na raiz do projeto, suba o PostgreSQL:

```bash
docker compose up -d
```

O banco `cdc_demo`, a tabela, a replica identity e a publication sao criados na primeira inicializacao do volume:

```sql
CREATE TABLE clientes (
    id BIGSERIAL PRIMARY KEY,
    nome TEXT NOT NULL,
    email TEXT
);

ALTER TABLE clientes REPLICA IDENTITY FULL;

CREATE PUBLICATION cdc_publication
FOR TABLE public.clientes;
```

Confirme a configuracao:

```bash
docker compose exec -T postgres psql -U postgres -d cdc_demo -c "SHOW wal_level;"
```

O resultado deve ser `logical`.

Crie o slot uma unica vez:

```bash
./scripts/create-slot.sh
```

Se o script informar que o slot ja existe, nenhuma alteracao e feita. Caso o consumidor seja executado sem o slot, ele termina com uma mensagem explicando como cria-lo.

## Configuracao da conexao

O consumidor carrega automaticamente `consumer/.env` quando o arquivo existe. Crie sua copia local a partir do exemplo:

```bash
cd consumer
cp .env.example .env
```

O arquivo contem as variaveis padrao do PostgreSQL:

```dotenv
# Origem
CDC_SOURCE_PGHOST=localhost
CDC_SOURCE_PGPORT=5432
CDC_SOURCE_PGDATABASE=cdc_demo
CDC_SOURCE_PGUSER=postgres
CDC_SOURCE_PGPASSWORD=postgres
CDC_SOURCE_PGSSLMODE=disable

# Destino
CDC_DEST_PGHOST=localhost
CDC_DEST_PGPORT=5433
CDC_DEST_PGDATABASE=cdc_demo
CDC_DEST_PGUSER=postgres
CDC_DEST_PGPASSWORD=postgres
CDC_DEST_PGSSLMODE=disable
```

O `.env` local e ignorado pelo Git; somente o `.env.example` e versionado. Variaveis ja exportadas no sistema tem prioridade sobre os valores do arquivo. A senha nao fica no codigo.

### Migracao das variaveis de conexao

As variaveis genericas de PostgreSQL usadas anteriormente foram substituidas por nomes especificos da origem. Essa separacao evita que as credenciais da origem sejam confundidas com as do destino:

| Variavel anterior | Variavel atual da origem |
| --- | --- |
| `PGHOST` | `CDC_SOURCE_PGHOST` |
| `PGPORT` | `CDC_SOURCE_PGPORT` |
| `PGDATABASE` | `CDC_SOURCE_PGDATABASE` |
| `PGUSER` | `CDC_SOURCE_PGUSER` |
| `PGPASSWORD` | `CDC_SOURCE_PGPASSWORD` |
| `PGSSLMODE` | `CDC_SOURCE_PGSSLMODE` |

As variaveis `PG*` antigas nao sao mais lidas pelo consumer. O destino continua usando seu proprio conjunto `CDC_DEST_PG*`.

Para executar o binario Go diretamente na maquina, use `localhost:5432` na origem e `localhost:5433` no destino. No Docker Compose, apenas os enderecos de rede sao sobrescritos para `postgres:5432` e `postgres-target:5432`; database, usuario, senha e SSL continuam vindo de `consumer/.env`.

O modo de aplicacao do PostgreSQLSink tambem e configurado no `.env`:

```dotenv
CDC_SINK=postgres
CDC_POSTGRES_APPLY_MODE=generic
CDC_TABLE_INCLUDE=public.clientes,public.enderecos
CDC_PUBLICATION_AUTOCONFIGURE=true
```

- `CDC_POSTGRES_APPLY_MODE=explicit` preserva os handlers dedicados existentes.
- `CDC_POSTGRES_APPLY_MODE=generic` aplica eventos usando os metadados das tabelas do destino.
- `CDC_TABLE_INCLUDE` e a allowlist no formato `schema.tabela`, separada por virgulas.
- `CDC_PUBLICATION_AUTOCONFIGURE=true` adiciona automaticamente a publication as tabelas ausentes da allowlist; a opcao nao remove tabelas.

Depois de alterar essas variaveis no ambiente Docker, recrie o consumer:

```bash
docker compose up -d --build consumer
```

O consumidor tambem aceita configuracoes opcionais:

```dotenv
CDC_SOURCE_ID=cdc-demo
CDC_SLOT=cdc_slot
CDC_PUBLICATION=cdc_publication
CDC_SINK=file
CDC_POSTGRES_APPLY_MODE=explicit
CDC_TABLE_INCLUDE=public.clientes,public.enderecos
CDC_PUBLICATION_AUTOCONFIGURE=false
CDC_OUTPUT_FILE=cdc.txt
CDC_STATUS_INTERVAL=10s
CDC_RETRY_INITIAL_DELAY=1s
CDC_RETRY_MAX_DELAY=30s
CDC_RETRY_MAX_ATTEMPTS=0
CDC_HEALTH_ENABLED=true
CDC_HEALTH_ADDR=:8080
```

Os valores acima sao os padroes. `CDC_PUBLICATION` informa a publication usada pelo `pgoutput`, `CDC_OUTPUT_FILE` permite que cada execucao use um arquivo isolado, e `CDC_STATUS_INTERVAL` controla a frequencia de confirmacao do progresso ao PostgreSQL. `CDC_RETRY_MAX_ATTEMPTS=0` significa retry infinito. O servidor de monitoramento pertence ao proprio binario: `CDC_HEALTH_ENABLED=false` o desabilita e `CDC_HEALTH_ADDR` escolhe o endereco e a porta de escuta. Use, por exemplo, `127.0.0.1:9090` para aceitar consultas somente no servidor pela porta `9090`, ou `:9090` para escutar em todas as interfaces. Os endpoints e as metricas estao descritos na secao Monitoramento do README.

O programa valida a configuracao na inicializacao e evita imprimir senhas ou DSNs completos em logs.

## Operacao local com source, destination e consumer

O Compose inclui PostgreSQL source, PostgreSQL destination, criacao do slot e consumer:

```bash
docker compose up --build
```

Depois disso:

- source: `localhost:5432`, banco `cdc_demo`;
- destination: `localhost:5433`, banco `cdc_demo`;
- health do consumer: `http://localhost:8080/livez` e `http://localhost:8080/readyz`;
- metricas internas simples: `http://localhost:8080/metrics`.

Para testar o fluxo:

```bash
docker compose exec -T postgres psql -U postgres -d cdc_demo -c \
"INSERT INTO clientes (nome, email) VALUES ('CDC Compose', 'compose@teste.com');"

docker compose exec -T postgres-target psql -U postgres -d cdc_demo -c \
"TABLE clientes;"
```

## Teste manual em dois terminais

Antes do teste, suba o PostgreSQL e garanta que o slot existe:

```bash
docker compose up -d
./scripts/create-slot.sh
```

### Terminal 1: consumidor

```bash
cd consumer
go run .
```

O programa permanece aguardando eventos. Um inicio normal mostra o banco, o slot, seu LSN confirmado e o arquivo de saida.

### Terminal 2: alteracoes SQL

```bash
docker exec -it cdc-postgres psql -U postgres -d cdc_demo
```

Execute:

```sql
INSERT INTO clientes (nome, email)
VALUES ('CDC TXT', 'cdc-txt@teste.com');

UPDATE clientes
SET nome = 'CDC TXT Atualizado'
WHERE email = 'cdc-txt@teste.com';

DELETE FROM clientes
WHERE email = 'cdc-txt@teste.com';
```

No Terminal 1 e em `consumer/cdc.txt`, a saida tera uma linha JSON por evento de negocio:

```json
{"type":"INSERT","lsn":"0/...","transaction_id":123,"schema":"public","table":"clientes","data":{"id":1,"nome":"CDC TXT","email":"cdc-txt@teste.com"}}
{"type":"UPDATE","lsn":"0/...","transaction_id":124,"schema":"public","table":"clientes","data":{"id":1,"nome":"CDC TXT Atualizado","email":"cdc-txt@teste.com"},"old_data":{"id":1,"nome":"CDC TXT","email":"cdc-txt@teste.com"}}
{"type":"DELETE","lsn":"0/...","transaction_id":125,"schema":"public","table":"clientes","data":{"id":1,"nome":"CDC TXT Atualizado","email":"cdc-txt@teste.com"}}
```

As operacoes seguintes aparecem como `UPDATE` e `DELETE`. Pressione `Ctrl+C` no Terminal 1 para fechar o arquivo e encerrar o consumidor. Execute novamente `go run .` e repita uma operacao para comprovar que os novos eventos sao adicionados ao final de `cdc.txt` sem apagar o conteudo existente.

## Testes automatizados de integracao

A suite em `consumer/main_test.go` usa o PostgreSQL, o logical decoding, o slot e o binario real do consumidor. Ela compila o consumidor uma unica vez, cria um arquivo temporario para cada teste e controla seus processos sem depender de outro terminal.

Antes de executar, o PostgreSQL deve estar ativo, o slot `cdc_slot` deve existir e nenhum consumidor manual pode estar usando esse slot:

```bash
docker compose up -d
./scripts/create-slot.sh

cd consumer
go test -v ./...
```

Os cenarios cobertos sao:

1. Recuperacao de evento gerado com o consumidor parado.
2. `SIGKILL` depois da persistencia e antes da confirmacao do LSN, seguido de reentrega.
3. Reinicializacao depois da confirmacao sem reentrega do evento confirmado.
4. Recuperacao de varios eventos na ordem de commit.
5. Falha de escrita sem avanco de `confirmed_flush_lsn`, seguida de recuperacao do evento no restart.
6. Preservacao de ordem e `transaction_id` em duas alteracoes na mesma transacao.
7. Eventos estruturados de `INSERT`, `UPDATE` e `DELETE`.
8. Rollback sem persistencia de evento de negocio.
9. Preservacao de Unicode em JSONL.
10. Registry de relacoes usado para resolver tabela e colunas.

Cada cenario usa identificadores unicos, timeout de 30 segundos, registros isolados e limpeza com `t.Cleanup()`. Os testes consultam `confirmed_flush_lsn` diretamente em `pg_replication_slots`.

Para tornar o crash deterministico, somente a suite define `CDC_TEST_PAUSE_BEFORE_ACK=true` e `CDC_TEST_PAUSE_MATCH=<identificador>`. Nesse modo, o consumidor persiste a transacao correspondente e pausa antes de enviar qualquer confirmacao de posicao ao PostgreSQL; o teste aguarda esse marcador e entao envia `SIGKILL`. A suite tambem pode definir `CDC_TEST_FAIL_WRITE_MATCH=<identificador>` para injetar falha de escrita em um evento especifico. Essas configuracoes ficam desligadas por padrao e nao alteram o uso normal.

## Comportamento em erros

- Slot inexistente: o consumidor encerra com a instrucao para executar `./scripts/create-slot.sh`.
- Slot ativo: apenas um consumidor pode usar o slot; o programa informa que outro consumidor deve ser encerrado.
- Plugin diferente: o programa informa que `pgoutput` era esperado.
- Conexao perdida: o consumidor marca readiness como indisponivel e tenta reconectar com backoff progressivo.
- Falha ao abrir ou criar `cdc.txt`: o consumidor informa o caminho e encerra com erro.
- Falha durante a escrita: o consumidor informa que o evento nao foi persistido e encerra sem avancar o LSN processado nem confirmar uma posicao posterior ao PostgreSQL.

## Administracao do slot

Consultar:

```bash
docker compose exec -T postgres psql -U postgres -d cdc_demo -c \
"SELECT slot_name, plugin, slot_type, database, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'cdc_slot';"
```

Remover, com o consumidor parado:

```bash
docker compose exec -T postgres psql -U postgres -d cdc_demo -c \
"SELECT pg_drop_replication_slot('cdc_slot');"
```

O script `./scripts/test-cdc.sh` continua disponivel para validar somente o PostgreSQL. Ele recria e consome o slot pela interface SQL, portanto nao deve ser executado enquanto o consumidor Go estiver usando o slot.

## Seguranca e permissoes

O ambiente local usa `postgres/postgres` e publica `0.0.0.0:5432` apenas por conveniencia de desenvolvimento. Em producao, use usuarios separados para source e destination.

O usuario de source precisa de permissao de replicacao logica e leitura dos metadados necessarios do slot/publication; ele nao deve ser superuser. Quando `CDC_PUBLICATION_AUTOCONFIGURE=true`, esse usuario tambem precisa poder alterar a publication. O usuario de destination deve ter permissao para consultar os metadados e executar `INSERT`, `UPDATE` e `DELETE` em todas as tabelas de `CDC_TABLE_INCLUDE`, alem de criar/alterar `cdc_applied_transactions` ou receber essa tabela ja provisionada por migracao. Com `CDC_SCHEMA_EVOLUTION=disabled` ou `manual`, Schema Evolution nao exige permissao DDL no destination. Com `CDC_SCHEMA_EVOLUTION=auto`, o usuario de destination precisa das permissoes minimas para as operacoes automaticas habilitadas, inicialmente `ALTER TABLE ... ADD COLUMN` nas tabelas permitidas; nao use superuser ou `GRANT ALL` como atalho operacional.

TLS pode ser configurado por `CDC_SOURCE_PGSSLMODE` no source e `CDC_DEST_PGSSLMODE` no destination. O modo local padrao e `disable`.

## Limites conhecidos

| Caracteristica | Resultado atual |
| --- | --- |
| 1k events | Nao medido nesta rodada |
| 10k events | Nao medido nesta rodada |
| 100k events | Nao medido nesta rodada |
| Transacao grande | Mantida em memoria ate o commit |
| Unicode | Coberto por teste automatizado |
| NULL | Decodificado como `nil`; `email` aceita NULL |
| Tipos | Cobertos na tabela atual: `BIGSERIAL`, `TEXT` |
| TOAST / TEXT grande | Ainda nao medido empiricamente |

## Comandos uteis

Compilar o consumidor:

```bash
cd consumer
go build .
```

Parar o PostgreSQL mantendo os dados:

```bash
docker compose down
```

Remover tambem o volume persistente e recriar o banco do zero:

```bash
docker compose down -v
```
