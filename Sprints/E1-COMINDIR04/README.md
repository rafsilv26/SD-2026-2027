# E1-COMINDIR04 — heartbeat e envio em lote

A implementação está dividida em quatro projetos Java independentes, todos no
pacote `rep04`: `rep04-replica`, `rep04-lider`, `rep04-processador` e
`rep04-produtor`. Cada projeto contém as classes de que necessita e pode ser
compilado isoladamente com Java 11 ou superior.

## Compilar

Executar a partir da raiz do repositório:

```bash
cd Sprints/E1-COMINDIR04/rep04-replica
mkdir -p out
javac -d out src/rep04/*.java

cd ../rep04-lider
mkdir -p out
javac -d out src/rep04/*.java

cd ../rep04-processador
mkdir -p out
javac -d out src/rep04/*.java

cd ../rep04-produtor
mkdir -p out
javac -d out src/rep04/*.java
```

## Executar

Se necessário, inicializar o repositório Kubo uma única vez com `ipfs init`.
Ativar o PubSub e iniciar o daemon:

```bash
ipfs config --json Pubsub.Enabled true
ipfs daemon
```

Executar todos os processos na mesma máquina. Compilar cada projeto primeiro e
executar cada processo num terminal separado, respeitando a ordem abaixo:

```bash
# Terminais 1–3 — réplicas (arrancar primeiro)
cd Sprints/E1-COMINDIR04/rep04-replica
java -cp out rep04.PartitionReplica 1 0,1
```

```bash
# Terminal 2
cd Sprints/E1-COMINDIR04/rep04-replica
java -cp out rep04.PartitionReplica 2 1,2
```

```bash
# Terminal 3
cd Sprints/E1-COMINDIR04/rep04-replica
java -cp out rep04.PartitionReplica 3 2,0
```

```bash
# Terminais 4–6 — líderes (depois das réplicas)
cd Sprints/E1-COMINDIR04/rep04-lider
java -cp out rep04.PartitionLeader 0 3
```

```bash
# Terminal 5
cd Sprints/E1-COMINDIR04/rep04-lider
java -cp out rep04.PartitionLeader 1 3
```

```bash
# Terminal 6
cd Sprints/E1-COMINDIR04/rep04-lider
java -cp out rep04.PartitionLeader 2 3
```

```bash
# Terminal 7 — processador
cd Sprints/E1-COMINDIR04/rep04-processador
java -cp out rep04.AlarmProcessor 3
```

```bash
# Terminal 8 — produtor (por último)
cd Sprints/E1-COMINDIR04/rep04-produtor
java -cp out rep04.Producer 3
```

O produtor aceita `<sensor> <temperatura>`, `rajada <n> <intervalo_ms>`,
`rajada-quente <n> <intervalo_ms>` e `sair`. As rajadas usam sensores `S01` a
`S10`; a variante quente concentra 80% das leituras em `S01`. Os parâmetros
`-Drep04.periodo=<ms>` e `-Drep04.fator=<n>` configuram o período de heartbeat
e o fator do timeout; devem ser iguais em todos os processos. Por omissão,
Δ = 5000 ms, o timeout é 15000 ms e a verificação ocorre a cada 1000 ms.

Cada processo escreve os ficheiros de saída no seu diretório de trabalho.
Para analisar conjuntamente os dados, copiar `hb-p*.txt`, `lider-p*.txt`,
`monitor-*.txt` e os ficheiros das réplicas para uma pasta comum. Executar:

```bash
java -cp out rep04.HeartbeatCheck 3 <pasta> [falhas.txt]
java -cp out rep04.CausalCheck 3 <pasta>
```

Executar o teste local do formato de lote e do detetor a partir do projeto da
réplica:

```bash
cd Sprints/E1-COMINDIR04/rep04-replica
java -cp out rep04.BatchAndHeartbeatTest
```

As linhas de `falhas.txt` têm o formato `p<partição>;<instante_em_ms>`.
`HeartbeatCheck` calcula as métricas a partir dos registos recolhidos; as
medições dos ensaios dependem da execução real do Kubo, dos processos e da
máquina utilizada. A estimativa de bytes de `REG` usa `127.0.0.1` como endereço,
igual ao valor por omissão do líder.

## Comportamento

O líder persiste cada registo ao recebê-lo e acumula-o para o próximo `LOTE`.
Um `ScheduledExecutorService` publica um lote no arranque e depois a cada
período, mesmo vazio. `ultimoSeq` permite que a réplica peça por TCP registos
perdidos, inclusive quando o lote seguinte está vazio. Os heartbeats não
alteram `L` nem `V`.

Réplicas, processador e produtor mantêm um `HeartbeatMonitor` por partição. Uma
suspeita apenas sinaliza o silêncio: não elege outro líder nem garante a
entrega de leituras enviadas durante uma falha. A recuperação do último registo
publicado depende de um heartbeat posterior e da disponibilidade do líder por
TCP.
