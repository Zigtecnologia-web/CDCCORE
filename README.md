<p align="center">
  <img src="assets/cdccore-logo-v2.png" alt="Logo do CDCCore" width="640">
</p>

<p align="center"><strong>Consumer and Replicator for Change Data Capture</strong></p>

O **CDCCore** captura alterações feitas em um PostgreSQL e as replica para um destino. Ele lê o WAL por replicação lógica, transforma cada `INSERT`, `UPDATE` e `DELETE` em um evento estruturado e processa a transação no destino antes de confirmar o progresso na origem.

O projeto foi escrito em Go e usa apenas recursos nativos do PostgreSQL. Não precisa de Kafka, Debezium ou outra fila.

## O que o projeto faz

```text
PostgreSQL de origem
        |
        v
  WAL + pgoutput
        |
        v
Replication slot
        |
        v
     CDCCore
        |
        +----> arquivo JSON Lines
        |
        +----> PostgreSQL de destino
```

Cada execução usa um único destino, escolhido por `CDC_SINK`:

- `postgres`: replica os dados para outro PostgreSQL;
- `file`: grava os eventos em um arquivo JSON Lines.

O CDCCore ainda não distribui o mesmo evento para vários destinos ao mesmo tempo.

## Início rápido

### Requisitos

- Docker com Docker Compose;
- portas `5432`, `5433` e `8080` livres.

### 1. Configure o ambiente

Na raiz do projeto, crie o arquivo local de configuração:

```bash
cp consumer/.env.example consumer/.env
```

O arquivo `consumer/.env` é ignorado pelo Git. Altere as senhas antes de usar o projeto fora de um ambiente local.

### 2. Inicie o projeto

```bash
docker compose up -d --build
```

Esse comando inicia:

- PostgreSQL de origem em `localhost:5432`;
- PostgreSQL de destino em `localhost:5433`;
- criação da publication e do replication slot;
- CDCCore como consumidor dos eventos.

Confira o estado dos servicos:

```bash
docker compose ps
```

### 3. Gere uma alteração

Insira um cliente no banco de origem:

```bash
docker compose exec -T postgres psql -U postgres -d cdc_demo -c \
"INSERT INTO clientes (nome, email) VALUES ('Maria', 'maria@exemplo.com');"
```

### 4. Confira a replicação

Consulte a mesma tabela no banco de destino:

```bash
docker compose exec -T postgres-target psql -U postgres -d cdc_demo -c \
"TABLE clientes;"
```

O registro de Maria deve aparecer no resultado. Isso confirma o caminho completo: origem, WAL, replication slot, CDCCore e destino.

## Configuração principal

As configurações ficam em `consumer/.env`.

### Conexão com a origem

```dotenv
CDC_SOURCE_PGHOST=localhost
CDC_SOURCE_PGPORT=5432
CDC_SOURCE_PGDATABASE=cdc_demo
CDC_SOURCE_PGUSER=postgres
CDC_SOURCE_PGPASSWORD=postgres
CDC_SOURCE_PGSSLMODE=disable
```

### Conexão com o destino

```dotenv
CDC_DEST_PGHOST=localhost
CDC_DEST_PGPORT=5433
CDC_DEST_PGDATABASE=cdc_demo
CDC_DEST_PGUSER=postgres
CDC_DEST_PGPASSWORD=postgres
CDC_DEST_PGSSLMODE=disable
```

### Captura e aplicação

```dotenv
CDC_SINK=postgres
CDC_POSTGRES_APPLY_MODE=generic
CDC_TABLE_INCLUDE=public.clientes,public.enderecos
CDC_PUBLICATION_AUTOCONFIGURE=true
```

| Variável | Para que serve |
| --- | --- |
| `CDC_SINK` | Escolhe o destino: `postgres` ou `file`. |
| `CDC_POSTGRES_APPLY_MODE` | Usa aplicação `explicit` ou `generic`. |
| `CDC_TABLE_INCLUDE` | Lista as tabelas permitidas no formato `schema.tabela`. |
| `CDC_PUBLICATION_AUTOCONFIGURE` | Adiciona automaticamente essas tabelas à publication quando vale `true`. |
| `CDC_SLOT` | Define o replication slot. O padrão é `cdc_slot`. |
| `CDC_PUBLICATION` | Define a publication. O padrão é `cdc_publication`. |
| `CDC_SOURCE_ID` | Identifica a origem para controle de idempotência. |

No modo `generic`, as tabelas precisam existir no destino e possuir chave primária. O CDCCore replica os dados, mas não cria nem atualiza o schema das tabelas.

Schema Evolution inicia em modo seguro:

```dotenv
CDC_SCHEMA_EVOLUTION=disabled
```

Quando o `pgoutput` enviar uma nova descricao de uma relacao ja conhecida e a metadata observada tiver mudado, o CDCCore registra `schema_metadata_change_observed`, sem alterar o contrato JSON de `INSERT`, `UPDATE` e `DELETE`. As classificacoes descrevem somente a diferenca entre snapshots: `COLUMN_ADDED`, `COLUMN_REMOVED`, `COLUMN_METADATA_CHANGED`, `RELATION_ID_CHANGED` e `RELATION_RENAMED_OR_REPLACED`. Evidencia insuficiente, como um possivel rename de coluna, resulta em `UNKNOWN`; o sistema nao afirma ter recebido o DDL original.

Valores suportados:

- `disabled`: detecta, valida quando houver PostgreSQL destination, mas nao aplica DDL;
- `manual`: reserva a mudanca para autorizacao externa futura e nao aplica DDL automaticamente;
- `auto`: aplica automaticamente apenas operacoes permitidas pela politica inicial.

Na politica inicial, `auto` exige `CDC_POSTGRES_APPLY_MODE=generic` e permite somente aplicar `ADD COLUMN` nullable em tabelas ja aceitas pelo PostgreSQL Sink. A acao no destination e derivada de metadata validada, usa uma allowlist por OID/typmod e identificadores escapados; SQL recebido da origem nunca e executado diretamente. `DROP COLUMN`, `DROP TABLE`, `ALTER TYPE`, rename, mudanca de chave primaria, tipos desconhecidos e colunas `NOT NULL` permanecem bloqueados. O apply, o DML e o registro de idempotencia participam da mesma transacao no destination.

Depois de alterar o `.env`, recrie o consumidor:

```bash
docker compose up -d --build consumer
```

## Modos de aplicação no PostgreSQL

### `explicit`

Usa código dedicado para as tabelas `public.clientes` e `public.enderecos`. Esse é o modo padrão e serve como exemplo de regras específicas por tabela.

### `generic`

Consulta as colunas e a chave primária no destino e monta os comandos SQL automaticamente. Somente as tabelas presentes em `CDC_TABLE_INCLUDE` são aceitas.

## Monitoramento

O servidor HTTP de monitoramento faz parte do próprio consumidor e funciona tanto no Docker quanto ao executar somente o binário. Ele é habilitado por padrão na porta `8080` e pode ser configurado no `.env`:

```dotenv
CDC_HEALTH_ENABLED=true
CDC_HEALTH_ADDR=:8080
```

Use `CDC_HEALTH_ENABLED=false` para desabilitá-lo. Para escolher outra porta, por exemplo `9090`, use `CDC_HEALTH_ADDR=:9090`.

O endereço também controla de onde o servidor aceita conexões:

- `:8080` ou `0.0.0.0:8080`: aceita conexões em todas as interfaces da máquina;
- `127.0.0.1:8080`: aceita apenas conexões originadas no próprio servidor.

Com a configuração padrão, os endpoints são:

- `GET http://localhost:8080/livez`: retorna HTTP `200` e `ok` enquanto o servidor de monitoramento estiver ativo. Não verifica as conexões com os bancos;
- `GET http://localhost:8080/readyz`: retorna HTTP `200` e `ready` quando o consumidor validou o slot, abriu o sink e iniciou a replicação. Durante uma falha ou reconexão, retorna HTTP `503` com a última mensagem de erro;
- `GET http://localhost:8080/metrics`: retorna em JSON os contadores internos e os últimos LSNs;
- `GET http://localhost:8080/metrics/prometheus`: retorna os contadores internos no formato de exposition do Prometheus.

Consulte os endpoints diretamente no servidor:

```bash
curl http://localhost:8080/livez
curl http://localhost:8080/readyz
curl http://localhost:8080/metrics
curl http://localhost:8080/metrics/prometheus
```

As métricas disponíveis são:

- `events_received`: alterações recebidas da origem;
- `events_applied`: alterações aplicadas com sucesso no destino;
- `transactions_received`: transações recebidas da origem;
- `transactions_committed`: transações confirmadas no destino;
- `transactions_failed`: transações que falharam;
- `transactions_redelivered`: contador reservado para reentregas; na implementação atual, ainda não é incrementado;
- `reconnects`: tentativas de reconexão realizadas;
- `sink_errors`: erros ao gravar no destino;
- `cdc_schema_metadata_relation_messages_total`: mensagens de relacao observadas;
- `cdc_schema_metadata_changes_total`: diferencas de metadata observadas;
- `cdc_schema_metadata_unknown_total`: diferencas classificadas como `UNKNOWN`;
- `cdc_schema_apply_attempts_total`: decisoes de apply avaliadas pelo PostgreSQL Sink;
- `cdc_schema_apply_success_total`: applies confirmados ou colunas compativeis reconhecidas apos commit;
- `cdc_schema_apply_failure_total`: falhas durante apply ou revalidacao;
- `cdc_schema_apply_rejected_total`: operacoes bloqueadas por modo ou politica;
- `cdc_schema_validation_total`: totais de validacao por status de compatibilidade;
- `last_processed_lsn`: última posição do WAL aplicada com sucesso no sink;
- `last_confirmed_lsn`: última posição aplicada que já foi confirmada ao PostgreSQL de origem.

O endpoint `/metrics/prometheus` expõe as métricas numéricas com prefixo `cdc_`, tipo `counter` e `Content-Type: text/plain; version=0.0.4`, incluindo `cdc_events_total`, `cdc_transactions_total`, `cdc_schema_metadata_relation_messages_total`, `cdc_schema_metadata_changes_total`, `cdc_schema_metadata_unknown_total`, `cdc_schema_apply_attempts_total`, `cdc_schema_apply_success_total`, `cdc_schema_apply_failure_total`, `cdc_schema_apply_rejected_total` e `cdc_schema_validation_total`.

O `last_processed_lsn` pode ficar brevemente à frente do `last_confirmed_lsn` até o envio do próximo ACK. Uma diferença persistente, especialmente acompanhada de `reconnects` ou `sink_errors`, indica que a confirmação de progresso deve ser investigada.

No Docker Compose, o `/readyz` é usado como healthcheck do container. Para consultar o estado e acompanhar os logs:

```bash
docker compose ps
docker compose logs -f consumer
```

Os endpoints não possuem autenticação. Em produção, restrinja o endereço a `127.0.0.1`, proteja a porta com firewall ou publique-a por meio de um proxy autenticado. Se a porta configurada já estiver em uso, o consumidor registra `health_server_failed`; a replicação pode continuar, mas os endpoints ficam indisponíveis.

## Garantias importantes

- As fronteiras das transações da origem são preservadas.
- O LSN só é confirmado depois que o sink aceita a transação.
- O PostgreSQL Sink aplica todos os eventos dentro de uma transação no destino.
- Transações já aplicadas são identificadas por `source_id` e `commit_lsn`, evitando reaplicação no PostgreSQL.
- Em caso de falha, o consumidor tenta se reconectar com espera progressiva.

No File Sink, a entrega é **at-least-once**: depois de determinadas falhas, uma linha pode aparecer novamente no arquivo.

## Executar sem o consumidor Docker

Pare o consumidor do Compose e deixe apenas os bancos e a preparação do slot no Docker:

```bash
docker compose stop consumer
docker compose up -d postgres postgres-target create-slot
```

Execute o consumidor diretamente com Go:

```bash
cd consumer
go run .
```

Use `Ctrl+C` para encerrar de forma segura.

## Build dos binários

Para gerar os executáveis de todas as plataformas suportadas, tenha a versão de Go definida em `consumer/go.mod` instalada e execute, na raiz do projeto:

```bash
./scripts/build.sh
```

O script recria `dist/` e gera os seguintes artefatos com `CGO_ENABLED=0`:

```text
dist/
|-- cdc-postgres-linux-amd64
|-- cdc-postgres-linux-arm64
|-- cdc-postgres-darwin-amd64
`-- cdc-postgres-darwin-arm64
```

Use o binário correspondente ao sistema operacional e à arquitetura da máquina. Por exemplo, em um Mac com Apple Silicon, o arquivo pode ser executado usando o `.env` existente em `consumer/`:

```bash
cd consumer
../dist/cdc-postgres-darwin-arm64
```

### Configuração do binário no servidor

Ao iniciar, o binário tenta carregar um arquivo chamado `.env` do diretório atual de execução. Esse diretório não precisa ser o mesmo em que o executável está armazenado, mas manter os dois juntos costuma ser a opção mais simples:

```text
/opt/cdc-postgres/
|-- cdc-postgres-linux-amd64
`-- .env
```

Nesse exemplo, execute:

```bash
cd /opt/cdc-postgres
./cdc-postgres-linux-amd64
```

Se o comando for iniciado em outro diretório, o programa procurará o `.env` nesse outro diretório. Por exemplo, executar `/opt/cdc-postgres/cdc-postgres-linux-amd64` a partir de `/tmp` fará o programa procurar `/tmp/.env`.

O `.env` é opcional. Todas as configurações também podem ser fornecidas diretamente como variáveis de ambiente:

```bash
CDC_SOURCE_PGHOST=db.exemplo.com \
CDC_SOURCE_PGUSER=cdc \
CDC_SOURCE_PGPASSWORD='senha' \
./cdc-postgres-linux-amd64
```

Variáveis já definidas no ambiente têm prioridade sobre valores presentes no `.env`. Em produção, elas podem ser fornecidas pelo mecanismo utilizado para iniciar o processo, como `EnvironmentFile` do `systemd`, secrets do Docker ou Secrets do Kubernetes.

O arquivo com credenciais deve pertencer ao usuário que executa o CDC e ter acesso restrito:

```bash
chmod 600 /opt/cdc-postgres/.env
```

O servidor de destino não precisa ter Go nem o código-fonte instalados. O `.env` não é incorporado ao executável, e os binários não incluem host, porta, usuário, senha, replication slot, publication ou banco de dados. O mesmo binário pode ser utilizado em desenvolvimento, homologação e produção; somente a configuração externa muda entre os ambientes.

Para conferir localmente o formato e a arquitetura dos artefatos:

```bash
file dist/cdc-postgres-*
```

O diretório `dist/` é ignorado pelo Git; os binários gerados não devem ser commitados no repositório.

## Testes

Os testes de integração usam o replication slot real. Como um slot só pode ter um consumidor ativo, pare o consumidor do Compose antes de executar a suíte:

```bash
docker compose stop consumer
cd consumer
go test -v ./...
```

Depois dos testes, volte para a raiz e inicie o consumidor novamente:

```bash
cd ..
docker compose up -d consumer
```

## Estrutura do projeto

```text
.
|-- assets/                 # Logo e recursos visuais
|-- consumer/               # Aplicação Go, sinks e testes
|-- dist/                   # Binários gerados localmente (ignorado pelo Git)
|-- docs/                   # Estado e documentação técnica
|-- postgres/init/          # Banco e tabelas do ambiente local
|-- scripts/                # Build, criação do slot e teste manual
|-- docker-compose.yml      # Ambiente local completo
`-- README.md
```

## Problemas comuns

### O slot já está ativo

Outro consumidor está usando `cdc_slot`. Pare o consumidor anterior antes de iniciar um novo:

```bash
docker compose stop consumer
```

### Uma tabela não foi replicada

Confira se ela:

- está em `CDC_TABLE_INCLUDE`;
- está na publication da origem;
- existe no destino;
- possui chave primária no modo `generic`.

### O consumidor não fica pronto

Consulte os logs e verifique as conexoes:

```bash
docker compose logs consumer
docker compose ps
```

## Limites atuais

- Uma transação grande permanece em memória até o `COMMIT`.
- Ainda não existem limites internos de memória, eventos ou bytes por transação.
- O streaming de transações grandes com `pgoutput` v2 ainda não foi implementado.
- O File Sink não oferece a mesma idempotência transacional do PostgreSQL Sink.

Benchmark opt-in de memória para 1.000, 25.000 e 100.000 eventos:

```bash
cd consumer
CDC_MEASURE_LARGE_TX=1 go test -run TestLargeTransactionMemoryBaseline -v
```

Para detalhes sobre protocolo, LSN, recuperação, segurança, transações grandes e decisões de implementação, consulte a [documentação técnica](docs/TECHNICAL.md) e o [estado do projeto](docs/STATUS.md).

## Licença

Consulte o arquivo [LICENSE](LICENSE).
