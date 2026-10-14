# E1-COMINDIR01 — replicação de registos

O trabalho está separado em dois projetos Java independentes:

- `rep01-lider`: cria cada `SensorRecord`, acrescenta-o a `lider.txt` e propaga-o;
- `rep01-replica`: recebe registos, conserva apenas um prefixo contínuo da sequência e escreve em `replica-<id>.txt`.

As duas pastas têm cópias de `SensorRecord` e `RecordFile` porque cada projeto deve compilar e executar por si. A representação partilhada na rede e nos ficheiros é texto UTF-8 no formato `seq;sensor;temperatura;instante`; não é serialização nativa de objetos Java.

## Compilar

Executar a partir da raiz do repositório:

```bash
cd Sprints/E1-COMINDIR01/rep01-lider
javac -d out src/rep01/*.java

cd ../rep01-replica
javac -d out src/rep01/*.java
```

É necessário Java 11 ou superior.

## Parte A — multicast

Abrir três terminais no diretório `rep01-replica` e iniciar primeiro as réplicas:

```bash
java -cp out rep01.MulticastReplica 1
java -cp out rep01.MulticastReplica 2
java -cp out rep01.MulticastReplica 3
```

Depois, no diretório `rep01-lider`, iniciar:

```bash
java -cp out rep01.MulticastLeader
```

Introduzir registos no formato `<sensor> <temperatura>`, usar `rajada 1000` para uma rajada de teste e `sair` para terminar o líder. As réplicas devem arrancar antes do líder, pois o multicast não guarda mensagens para quem não estava à escuta.

O resumo de uma rajada na réplica usa um intervalo de silêncio de 1,5 segundos para separar execuções consecutivas. Como o protocolo não inclui um identificador ou marcador de fim de rajada, esse intervalo é apenas uma convenção de medição; não é uma garantia do multicast.

## Parte B — PubSub do IPFS

Instalar Kubo e, antes de arrancar o daemon, ativar o PubSub:

```bash
ipfs init
ipfs config --json Pubsub.Enabled true
ipfs daemon
```

Iniciar as réplicas antes do líder. Para um daemon local na porta padrão:

```bash
java -cp out rep01.PubSubReplica 1
java -cp out rep01.PubSubReplica 2
java -cp out rep01.PubSubReplica 3
```

No projeto do líder:

```bash
java -cp out rep01.PubSubLeader
```

O argumento opcional do líder é o endereço base da API Kubo, por omissão `http://127.0.0.1:5001/api/v0`. A réplica recebe o identificador e, opcionalmente, o porto da API:

```bash
java -cp out rep01.PubSubReplica 2 5002
```

Para testar vários nós, inicializar cada daemon com um `IPFS_PATH` e portos próprios, ligar os nós com `ipfs swarm connect` e confirmar o tópico com `ipfs pubsub peers rep01-temperaturas`. PubSub atravessa a rede de nós, mas também não guarda mensagens para subscritores offline.

## Recolha das métricas

Para cada mecanismo, executar `rajada 1000` três vezes com as réplicas já subscritas e registar as saídas do líder e de cada réplica. Os instantes são milissegundos desde a época Unix. Usar:

- duração de envio do líder = último envio − primeiro envio;
- débito do líder = 1000 ÷ duração em segundos;
- latência da rajada = última receção na réplica − primeiro envio no líder.

As contagens de recebidos, escritos, duplicados e retidos permitem distinguir perda de mensagens recebidas que ainda não podem ser aplicadas. Uma mensagem perdida definitivamente deixa a réplica à espera da sequência em falta; esta implementação não pede retransmissão nem recupera o estado.
