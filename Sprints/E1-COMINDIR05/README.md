# E1-COMINDIR05 — Eleição do líder de partição

Implementação Java 11+ em três projetos independentes: `raft04-no`,
`raft04-produtor` e `raft04-processador`. Os ficheiros Java usam o pacote
`raft04`. A lógica central de eleição e persistência está comentada em
`RaftState`; a coordenação entre eleição, registos e lotes está comentada em
`PartitionNode`.

## Diagramas UML

Os ficheiros PlantUML em `uml/` cobrem a arquitetura e deployment, o modelo de
classes, a máquina de estados, as sequências de eleição/falha/recuperação e as
evidências usadas para verificar as métricas. Os diagramas de sequência agrupam
os participantes por zona e dividem o fluxo em fases. Para gerar SVG ou PNG com
PlantUML instalado:

```bash
plantuml -tsvg Sprints/E1-COMINDIR05/uml/*.puml
```

Os diagramas descrevem os mecanismos implementados e assinalam explicitamente
que não existe confirmação de escrita por maioria (*commit*).

## Compilar

Executar a partir da raiz do repositório:

```bash
cd Sprints/E1-COMINDIR05/raft04-no
mkdir -p out
javac -d out src/raft04/*.java

cd ../raft04-produtor
mkdir -p out
javac -d out src/raft04/*.java

cd ../raft04-processador
mkdir -p out
javac -d out src/raft04/*.java
```

## Executar

Ativar PubSub e iniciar o Kubo:

```bash
ipfs config --json Pubsub.Enabled true
ipfs daemon
```

Arrancar nove nós, três por partição, cada comando num terminal próprio. Todos
os nós devem usar o mesmo diretório de trabalho para que os marcadores de
isolamento sejam visíveis a cada processo. A ordem de arranque não é
significativa; cada grupo elege o próprio líder:

```bash
cd Sprints/E1-COMINDIR05/raft04-no
java -cp out raft04.PartitionNode 0 1 3
java -cp out raft04.PartitionNode 0 2 3
java -cp out raft04.PartitionNode 0 3 3
java -cp out raft04.PartitionNode 1 1 3
java -cp out raft04.PartitionNode 1 2 3
java -cp out raft04.PartitionNode 1 3 3
java -cp out raft04.PartitionNode 2 1 3
java -cp out raft04.PartitionNode 2 2 3
java -cp out raft04.PartitionNode 2 3 3
```

Depois, em terminais separados:

```bash
cd Sprints/E1-COMINDIR05/raft04-processador
java -cp out raft04.AlarmProcessor 3
```

```bash
cd Sprints/E1-COMINDIR05/raft04-produtor
java -cp out raft04.Producer 3
```

O produtor aceita `<sensor> <temperatura>`, `rajada <n> <intervalo_ms>`,
`rajada-quente <n> <intervalo_ms>` e `sair`. Para simular isolamento de um nó,
criar no diretório de trabalho partilhado `isolar-p<k>-n<i>` e apagá-lo para
reparar a rede. O canal de entrada do produtor continua ativo durante o
isolamento; tópicos de eleição/dados e recuperação TCP ficam bloqueados.

Todos os ficheiros de saída são escritos no diretório de trabalho dos processos.
Reunir `no-*`, `estado-*`, `hb-*`, `eventos-*`, `monitor-*` e `produtor.txt` numa
pasta para analisar:

```bash
cd Sprints/E1-COMINDIR05/raft04-no
java -cp out raft04.RaftCheck 3 <pasta> [falhas.txt]
java -cp out raft04.CausalCheck 3 <pasta>
java -cp out raft04.RaftStateTest
```

As propriedades de sistema `raft04.periodo`, `raft04.eleicaoMin`,
`raft04.eleicaoMax` e `raft04.fator` configuram os temporizadores. Os valores
por omissão são Δ = 2000 ms, prazo de eleição uniforme entre 6000 e 12000 ms e
verificação a cada 200 ms. Para nós em máquinas diferentes, definir
`-Draft04.host=<endereço-anunciável>` em cada processo e usar o mesmo endereço
no anúncio TCP do líder.
