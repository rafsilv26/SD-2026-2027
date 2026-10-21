# COMINDIR02 — implementação de referência

O código está dividido em três projetos independentes, compiláveis com Java 11
ou superior. Cada projeto inclui as cópias locais de `SensorRecord`,
`RecordFile`, `IpfsPubSub`, `Wire` e `Partitioner`.

## Compilar

Na raiz do repositório:

```bash
cd Sprints/E1-COMINDIR02/rep02-produtor
javac -d out src/rep02/*.java

cd ../rep02-lider
javac -d out src/rep02/*.java

cd ../rep02-replica
javac -d out src/rep02/*.java
```

## Executar

Ative o PubSub no Kubo (`ipfs config --json Pubsub.Enabled true`) e inicie o
daemon. Para a Parte A, use N = 1, a partição 0 e três réplicas, cada uma
num terminal separado:

```bash
# Terminal 1
cd Sprints/E1-COMINDIR02/rep02-replica
java -cp out rep02.PartitionReplica 1 0

# Terminal 2
cd Sprints/E1-COMINDIR02/rep02-replica
java -cp out rep02.PartitionReplica 2 0

# Terminal 3
cd Sprints/E1-COMINDIR02/rep02-replica
java -cp out rep02.PartitionReplica 3 0

# Terminal 4
cd Sprints/E1-COMINDIR02/rep02-lider
java -cp out rep02.PartitionLeader 0 1

# Terminal 5
cd Sprints/E1-COMINDIR02/rep02-produtor
java -cp out rep02.Producer 1
```

Na Parte B, use processos/terminais separados para as réplicas
`1 0,1`, `2 1,2` e `3 2,0`, os líderes `0 3`, `1 3` e `2 3`, e o produtor
`3`. Arranque sempre as réplicas, depois os líderes e por fim o produtor. O
produtor também aceita um endereço da API Kubo como argumento opcional; a
réplica aceita o porto da API como terceiro argumento.

Comandos do produtor: `<sensor> <temperatura>`, `rajada <n> <s>`,
`rajada-quente <n> <s>` e `sair`. As propriedades
`-Dperda=0.2` e `-Ddescartar=<seq>` permitem simular perdas exclusivamente nas
mensagens recebidas pelo PubSub.

Depois de copiar os ficheiros de líderes e réplicas para uma pasta comum,
verifique-os com `java -cp out rep02.PartitionCheck 3 <pasta>`. As medições
dos ensaios dependem do daemon, da rede e da máquina usados; devem ser
recolhidas durante a execução, não estimadas pelo código.
