# SPEC 20 - Estrategia de Processamento de Large Transactions

## 1. Objetivo

Definir os requisitos arquiteturais para uma futura evolucao do CDCCore destinada ao processamento de transacoes grandes, reduzindo a dependencia de memoria RAM sem alterar as garantias de consistencia, ordenacao, checkpoint e ACK existentes.

Esta spec nao implementa uma estrategia especifica.

As duas estrategias inicialmente consideradas sao:

1. Buffer intermediario em disco.
2. Manutencao da transacao aberta no destination durante o processamento.

A decisao entre as estrategias devera ser tomada apos avaliacao tecnica especifica.

---

## 2. Contexto

A SPEC 19 estabeleceu uma fase de medicao e guardrails para large transactions.

Os benchmarks atuais registraram aproximadamente:

| Eventos | Pico observado |
| ------: | -------------: |
|   1.000 |       1,83 MiB |
|  25.000 |      21,52 MiB |
| 100.000 |      85,04 MiB |

Os resultados demonstram que o consumo de memoria cresce significativamente conforme o tamanho da transacao.

Atualmente, o CDCCore nao possui um mecanismo de streaming de eventos e pode manter uma quantidade significativa de dados da transacao em memoria.

O protocolo `pgoutput` permanece configurado com:

```text
proto_version '1'
```

Streaming de transacoes nao faz parte desta etapa.

---

## 3. Problema

Uma transacao PostgreSQL pode conter uma quantidade muito grande de eventos.

Exemplo:

```text
BEGIN
  INSERT
  INSERT
  INSERT
  ...
  INSERT
COMMIT
```

O CDCCore precisa preservar a semantica transacional:

```text
BEGIN
   v
eventos
   v
COMMIT
   v
ACK / processedLSN
```

Porem, manter todos os eventos necessarios em memoria pode aumentar progressivamente o heap.

Em situacoes extremas, isso pode resultar em:

```text
Large Transaction
       v
Memory Growth
       v
Heap Pressure
       v
GC Pressure
       v
OOM
       v
Process termination
```

A solucao futura devera reduzir esse risco sem introduzir inconsistencia.

---

## 4. Invariantes obrigatorios

Qualquer estrategia implementada a partir desta spec deve preservar os seguintes invariantes.

### 4.1 ACK

O CDCCore nao pode confirmar o processamento de uma transacao antes de sua aplicacao efetiva no destination.

A sequencia conceitual permanece:

```text
Transaction received
        v
Transaction applied
        v
Destination COMMIT
        v
processedLSN updated
        v
ACK
```

O ACK nao pode representar apenas:

```text
"eventos foram recebidos"
```

Ele deve representar:

```text
"eventos foram processados com sucesso"
```

---

### 4.2 Processed LSN

O `processedLSN` nao pode avancar antes do sucesso da aplicacao da transacao.

Em caso de falha:

```text
Destination failure
       v
processedLSN nao avanca
       v
ACK nao e confirmado
```

---

### 4.3 Ordenacao

Os eventos devem preservar a ordem definida pelo WAL.

A estrategia de reducao de memoria nao pode alterar:

```text
event A
event B
event C
```

para:

```text
event B
event A
event C
```

---

### 4.4 Atomicidade

Uma transacao de origem nao pode resultar em aplicacao parcialmente confirmada no destination.

Se a transacao possui:

```text
A
B
C
D
```

o comportamento esperado continua sendo:

```text
COMMIT
    v
A + B + C + D
```

ou:

```text
ROLLBACK
    v
nenhum dos eventos e confirmado
```

---

## 5. Estrategia A - Buffer em disco

Uma possivel implementacao consiste em retirar os eventos da memoria e armazena-los temporariamente em disco.

Fluxo conceitual:

```text
PostgreSQL
    |
    | WAL
    v
CDC Reader
    |
    v
Disk Buffer
    |
    | eventos
    v
Destination
    |
    v
COMMIT
    |
    v
processedLSN / ACK
```

O buffer pode armazenar temporariamente os eventos necessarios para reconstruir a transacao.

### 5.1 Requisitos

A implementacao devera definir:

- formato do buffer;
- localizacao;
- limite de utilizacao de disco;
- politica de limpeza;
- comportamento apos restart;
- identificacao da transacao;
- ordenacao dos eventos;
- integridade do conteudo;
- tratamento de corrupcao;
- politica de retencao.

---

### 5.2 Durabilidade

Se o buffer em disco for considerado parte da estrategia de recuperacao, a implementacao devera especificar claramente quando um evento e considerado persistido.

Nao deve existir uma situacao ambigua em que:

```text
evento saiu da memoria
```

mas:

```text
evento ainda nao esta seguramente armazenado
```

e simultaneamente:

```text
o processo pode perder o evento apos crash.
```

---

## 6. Estrategia B - Transaction Open no Destination

Outra possibilidade e manter a transacao do destination aberta enquanto os eventos da transacao de origem sao consumidos e aplicados.

Fluxo:

```text
Source Transaction
       |
       v
CDC Reader
       |
       v
Destination BEGIN
       |
       |-- event
       |-- event
       |-- event
       |-- ...
       |
       v
Destination COMMIT
       |
       v
processedLSN
       |
       v
ACK
```

Nesse modelo, o CDCCore nao precisa necessariamente manter todos os eventos da transacao em memoria.

Cada evento pode ser processado conforme e recebido.

---

## 7. Riscos da estrategia de transaction open

A implementacao devera avaliar explicitamente:

- duracao de transacoes no destination;
- locks;
- retencao de recursos;
- impacto sobre VACUUM;
- impacto sobre concorrencia;
- rollback de transacoes grandes;
- comportamento durante falhas;
- reconexao;
- timeout;
- disponibilidade do destination;
- transacoes que permanecem abertas por periodos muito longos.

Nao sera permitido assumir que manter uma transacao aberta e automaticamente equivalente a streaming seguro.

---

## 8. Selecao da estrategia

A decisao devera considerar pelo menos:

| Criterio               | Buffer em disco      | Transaction Open  |
| ---------------------- | -------------------- | ----------------- |
| Uso de RAM             | Avaliar              | Avaliar           |
| Uso de disco           | Avaliar              | Baixo             |
| Complexidade           | Avaliar              | Avaliar           |
| Recuperacao apos crash | Avaliar              | Avaliar           |
| Locks no destination   | Menor potencialmente | Deve ser avaliado |
| Transacoes longas      | Avaliar              | Risco relevante   |
| Throughput             | Medir                | Medir             |
| Latencia               | Medir                | Medir             |
| Operacao               | Avaliar              | Avaliar           |
| Consistencia           | Obrigatoria          | Obrigatoria       |

Nenhuma estrategia sera considerada escolhida apenas por analise teorica.

A decisao devera ser baseada em implementacao experimental e benchmark.

---

## 9. Limites de memoria

A futura implementacao devera possuir limites explicitos.

Exemplos:

```text
CDC_LARGE_TX_MEMORY_LIMIT
CDC_LARGE_TX_DISK_LIMIT
CDC_LARGE_TX_MAX_EVENTS
```

Os nomes definitivos ficam a criterio da implementacao.

O objetivo e impedir que uma unica transacao possa consumir memoria indefinidamente.

---

## 10. Comportamento ao atingir o limite

O comportamento ao atingir um limite devera ser deterministico.

Possibilidades incluem:

```text
memory threshold reached
        v
spill to disk
```

ou:

```text
memory threshold reached
        v
abort safely
        v
nao avancar processedLSN
        v
nao ACK
```

A implementacao nunca deve simplesmente descartar eventos para permanecer dentro do limite.

---

## 11. Crash Recovery

A solucao devera definir o comportamento nos seguintes cenarios:

### Crash antes do COMMIT

```text
BEGIN
events processed
process crash
```

Resultado esperado:

```text
processedLSN nao avanca
```

A transacao devera poder ser reprocessada com seguranca.

---

### Crash depois do COMMIT

```text
BEGIN
events
COMMIT
process crash
```

A implementacao devera definir como determinar que a transacao ja foi aplicada e evitar efeitos duplicados incompativeis com a semantica do destination.

---

### Falha durante o processamento

```text
BEGIN
A
B
C
destination failure
```

O sistema deve manter a garantia de que:

```text
processedLSN
```

nao avance incorretamente.

---

## 12. Idempotencia

A futura estrategia devera considerar explicitamente o comportamento de reprocessamento.

Apos um crash, o mesmo evento ou transacao podera ser recebido novamente.

A solucao nao deve depender da suposicao:

```text
"cada evento sera recebido exatamente uma vez"
```

sem uma garantia tecnica que sustente essa afirmacao.

---

## 13. Observabilidade

A estrategia devera produzir metricas suficientes para identificar o comportamento de large transactions.

Metricas possiveis:

```text
cdc_large_transactions_total
cdc_large_transaction_events_total
cdc_large_transaction_bytes_total
cdc_large_transaction_duration_seconds
cdc_large_transaction_memory_bytes
cdc_large_transaction_spills_total
cdc_large_transaction_failures_total
```

Caso seja utilizado buffer em disco:

```text
cdc_large_transaction_disk_bytes
cdc_large_transaction_disk_limit_bytes
cdc_large_transaction_spills_total
```

As metricas devem seguir a mesma arquitetura de observabilidade existente no CDCCore.

---

## 14. Compatibilidade com SPEC 19

A SPEC 19 permanece valida.

A futura implementacao devera continuar executando os benchmarks de large transactions.

Os resultados deverao permitir comparacao:

```text
SPEC 19
baseline atual
       |
       v
nova implementacao
       |
       v
comparacao
```

A evolucao nao devera ser considerada concluida somente porque o processo deixou de apresentar crescimento de heap.

Tambem deverao ser avaliados:

- duracao;
- throughput;
- CPU;
- memoria;
- disco, quando aplicavel;
- comportamento de restart;
- comportamento de falha;
- integridade dos dados.

---

## 15. Testes obrigatorios

A implementacao devera possuir testes para pelo menos:

### Tamanho

```text
1.000 eventos
25.000 eventos
100.000 eventos
```

E, quando suportado pelo ambiente:

```text
500.000 eventos
1.000.000 eventos
```

### Falhas

- falha antes do COMMIT;
- falha durante a aplicacao;
- falha depois do COMMIT;
- restart do consumer;
- reconnect;
- destination indisponivel.

### Integridade

- ordem dos eventos;
- quantidade de eventos;
- conteudo;
- atomicidade;
- processedLSN;
- ACK.

---

## 16. Criterios de aceite

A futura implementacao somente sera considerada concluida quando:

- [ ] large transactions nao dependerem exclusivamente de crescimento ilimitado de memoria;
- [ ] `processedLSN` continuar avancando somente apos processamento confirmado;
- [ ] ACK continuar representando processamento confirmado;
- [ ] ordem dos eventos for preservada;
- [ ] atomicidade for preservada;
- [ ] comportamento de crash estiver coberto por testes;
- [ ] reprocessamento estiver definido;
- [ ] limites operacionais estiverem documentados;
- [ ] metricas de large transactions estiverem disponiveis;
- [ ] benchmarks forem executados;
- [ ] resultados forem comparados com a baseline da SPEC 19;
- [ ] nao houver perda silenciosa de eventos;
- [ ] nao houver avanco incorreto de checkpoint;
- [ ] nao houver alteracao nao documentada da semantica CDC existente.

---

## 17. Fora do escopo

Esta spec nao implementa:

- streaming do `pgoutput`;
- alteracao do `proto_version`;
- Kafka;
- Debezium;
- mudanca do protocolo PostgreSQL;
- exactly-once;
- distribuicao do CDC;
- sharding;
- alteracao da semantica de ACK;
- alteracao do modelo de checkpoint.

A implementacao de streaming do protocolo PostgreSQL devera ser tratada em uma spec especifica caso seja necessaria futuramente.

---

## 18. Decisao arquitetural

A decisao entre:

```text
Buffer em disco
```

e:

```text
Transaction Open no Destination
```

devera permanecer **nao decidida nesta etapa**.

Primeiro deverao ser produzidos prototipos experimentais e benchmarks comparaveis.

A decisao final devera ser registrada em uma ADR ou SPEC posterior, contendo:

- estrategia escolhida;
- alternativas avaliadas;
- benchmarks;
- riscos;
- limitacoes;
- impacto operacional;
- impacto na consistencia;
- justificativa tecnica.

---

## 19. Estado

```text
STATUS: PROPOSED
```

A SPEC 19 permanece como implementacao vigente de:

```text
medicao
+
baseline
+
guardrails
```

A SPEC 20 define o caminho para uma futura solucao de processamento de large transactions sem antecipar a escolha da estrategia.
