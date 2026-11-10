package raft04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Nó completo de uma partição: seguidor, candidato e líder.
 *
 * A ordem de locks é sempre monitor -> RaftState. RaftState nunca chama código
 * do nó; assim uma thread não consegue adquirir estes locks pela ordem inversa.
 */
public final class PartitionNode {
    private static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";

    private final int partition;
    private final int nodeId;
    private final int partitions;
    private final Partitioner partitioner;
    private final String address;
    private final IpfsPubSub pubSub;
    private final RecordFile file;
    private final Object monitor = new Object();
    private final Object logMonitor = new Object();
    private final List<SensorRecord> pending = new ArrayList<>();
    private final RaftState raft;
    private long lastSeq;
    private long lastTerm;
    private long lamport;
    private VectorClock vector;
    private long hb;
    private ScheduledExecutorService leaderSender;

    private PartitionNode(int partition, int nodeId, int partitions, String api)
            throws IOException {
        this.partition = partition;
        this.nodeId = nodeId;
        this.partitions = partitions;
        this.partitioner = new Partitioner(partitions);
        this.address = System.getProperty("raft04.host", "127.0.0.1");
        this.pubSub = new IpfsPubSub(api);
        this.file = new RecordFile("no-p" + partition + "-n" + nodeId + ".txt");

        // Ao reiniciar, a cópia local determina seq, termo e relógios causais.
        SensorRecord last = file.last();
        if (last == null) {
            lastSeq = 0;
            lastTerm = 0;
            lamport = 0;
            vector = new VectorClock(partitions);
        } else {
            lastSeq = last.getSeq();
            lastTerm = last.getTerm();
            lamport = last.getLamport();
            vector = last.getVector();
            if (vector.size() != partitions || vector.get(partition) != lastSeq) {
                throw new IOException("Último registo viola a dimensão do vetor ou V[k]=seq");
            }
        }

        // O callback de log só escreve eventos; não chama de volta o nó.
        this.raft = new RaftState(partition, nodeId, Wire.GRUPO,
                Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS,
                Path.of("estado-p" + partition + "-n" + nodeId + ".txt"),
                this::logEvent);
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 4) {
            System.err.println("Uso: PartitionNode <k> <i> [N] [api]");
            return;
        }
        try {
            int k = Integer.parseInt(args[0]);
            int i = Integer.parseInt(args[1]);
            int n = args.length > 2 ? Integer.parseInt(args[2]) : 3;
            String api = args.length > 3 ? args[3] : DEFAULT_API;
            if (n < 1 || k < 0 || k >= n || i < 1 || i > Wire.GRUPO) {
                throw new IllegalArgumentException("k, i ou N fora do intervalo");
            }
            int port = Wire.nodePort(k, i);
            if (port > 65535) throw new IllegalArgumentException("Porto TCP inválido");
            new PartitionNode(k, i, n, api).run(port);
        } catch (NumberFormatException e) {
            System.err.println("k, i e N têm de ser inteiros.");
        } catch (IllegalArgumentException | IOException e) {
            System.err.println("Não foi possível iniciar o nó: " + e.getMessage());
        }
    }

    private void run(int port) throws IOException {
        Thread server = new Thread(() -> runRecoveryServer(port), "raft04-tcp-p" + partition + "-n" + nodeId);
        server.setDaemon(true);
        server.start();

        // Cada subscrição tem a sua thread; a simulação de isolamento descarta
        // mensagens de consenso/dados sem afetar o canal raft04-in-p<k>.
        startSubscription(Wire.topicIn(partition), this::onInput, false);
        startSubscription(Wire.topicData(partition), this::onBatch, true);
        startSubscription(Wire.topicElection(partition), this::onElection, true);

        ScheduledExecutorService election = Executors.newSingleThreadScheduledExecutor();
        election.scheduleAtFixedRate(this::checkElection, Wire.VERIFICACAO_MS,
                Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
        logEvent("ARRANQUE;p" + partition + ";termo=" + raft.term() + ";seq=" + lastSeq);
        System.out.println("Nó p" + partition + "/n" + nodeId + " pronto; seq=" + lastSeq
                + "; termo=" + raft.term() + "; TCP=" + address + ":" + port);
        try {
            while (!Thread.currentThread().isInterrupted()) Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            election.shutdownNow();
            stopLeaderSender();
        }
    }

    private void startSubscription(String topic, java.util.function.Consumer<String> handler,
                                  boolean affectedByIsolation) {
        Thread thread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    pubSub.subscribe(topic, message -> {
                        if (affectedByIsolation && Wire.isolated(partition, nodeId)) return;
                        handler.accept(message);
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (IOException | RuntimeException e) {
                    logEvent("ERRO;p" + partition + ";subscricao=" + topic + ";"
                            + e.getMessage());
                    pauseBeforeRetry();
                }
            }
        }, "raft04-" + topic + "-n" + nodeId);
        thread.setDaemon(true);
        thread.start();
    }

    private void checkElection() {
        long electionTerm;
        synchronized (monitor) {
            if (Wire.isolated(partition, nodeId)) return;
            electionTerm = raft.tick();
        }
        if (electionTerm < 0) return;
        publishElection(new VoteRequest(electionTerm, nodeId, lastSeq, lastTerm).encode());
    }

    private void onElection(String message) {
        if (Wire.isolated(partition, nodeId)) return;
        try {
            String type = Wire.type(message);
            if ("PEDIDO_VOTO".equals(type)) {
                VoteRequest request = VoteRequest.decode(message);
                if (request.candidate() == nodeId) return;
                RaftState.Vote reply;
                synchronized (monitor) {
                    reply = raft.onVoteRequest(request.term(), request.candidate(),
                            request.lastSeq(), request.lastTerm(), lastSeq, lastTerm);
                }
                // Mesmo um voto recusado é respondido para o candidato conhecer
                // a decisão e, se necessário, o termo mais alto do eleitor.
                publishElection(new VoteReply(reply.term, nodeId,
                        request.candidate(), reply.granted).encode());
            } else if ("VOTO".equals(type)) {
                VoteReply reply = VoteReply.decode(message);
                if (reply.voter() == nodeId) return;
                synchronized (monitor) {
                    boolean becameLeader = raft.onVote(reply.term(), reply.voter(),
                            reply.candidate(), reply.granted());
                    if (becameLeader) startLeadershipLocked();
                }
            } else {
                throw new IllegalArgumentException("Tipo de eleição inesperado: " + type);
            }
        } catch (IllegalArgumentException | IOException e) {
            logEvent("ERRO;p" + partition + ";eleicao=" + e.getMessage());
        }
    }

    private void publishElection(String message) {
        if (Wire.isolated(partition, nodeId)) return;
        try {
            pubSub.publish(Wire.topicElection(partition), message);
        } catch (IOException e) {
            logEvent("ERRO;p" + partition + ";publicar_eleicao=" + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Called with monitor held so no IN can be assigned using a stale role/term. */
    private void startLeadershipLocked() throws IOException {
        SensorRecord last = file.last();
        lastSeq = last == null ? 0 : last.getSeq();
        lastTerm = last == null ? 0 : last.getTerm();
        lamport = last == null ? 0 : last.getLamport();
        vector = last == null ? new VectorClock(partitions) : last.getVector();
        pending.clear();
        hb = 0;
        stopLeaderSender();
        leaderSender = Executors.newSingleThreadScheduledExecutor();
        leaderSender.scheduleAtFixedRate(this::sendBatch, 0,
                Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
    }

    private void onInput(String message) {
        try {
            if (!"IN".equals(Wire.type(message))) throw new IllegalArgumentException("tipo inesperado");
            String[] f = Wire.body(message).split(";", -1);
            if (f.length != 4 || f[0].isBlank()) throw new IllegalArgumentException("IN mal formado");
            double temperature = Double.parseDouble(f[1]);
            long inputLamport = Long.parseLong(f[2]);
            VectorClock inputVector = VectorClock.parse(f[3], partitions);
            if (!Double.isFinite(temperature) || inputLamport < 0) {
                throw new IllegalArgumentException("IN contém valor inválido");
            }
            if (partitioner.of(f[0]) != partition) return;

            synchronized (monitor) {
                long currentTerm = raft.termIfLeader();
                if (currentTerm < 0) {
                    logEvent("IGNORADO;p" + partition + ";IN=" + f[0] + ";sem_lider");
                    return;
                }
                long nextSeq = lastSeq + 1;
                long nextLamport = Math.max(lamport, inputLamport) + 1;
                VectorClock nextVector = vector.copy();
                nextVector.merge(inputVector);
                nextVector.tick(partition);
                if (nextVector.get(partition) != nextSeq) {
                    throw new IllegalStateException("V[k] diferente do seq seguinte");
                }
                SensorRecord record = SensorRecord.now(nextSeq, currentTerm,
                        f[0], temperature, nextLamport, nextVector);
                file.append(record);
                lastSeq = nextSeq;
                lastTerm = currentTerm;
                lamport = nextLamport;
                vector = nextVector;
                pending.add(record);
                logEvent("RECEBIDO;p" + partition + ";seq=" + nextSeq + ";termo="
                        + currentTerm + ";sensor=" + f[0]);
            }
        } catch (IOException | IllegalArgumentException | IllegalStateException e) {
            logEvent("ERRO;p" + partition + ";IN=" + e.getMessage());
        }
    }

    private void sendBatch() {
        final Batch batch;
        final long batchHb;
        synchronized (monitor) {
            long currentTerm = raft.termIfLeader();
            if (currentTerm < 0) {
                stopLeaderSender();
                return;
            }
            // A fotografia e a troca da lista são atómicas relativamente a IN.
            List<SensorRecord> records = new ArrayList<>(pending);
            pending.clear();
            batchHb = ++hb;
            batch = new Batch(currentTerm, nodeId, address, Wire.nodePort(partition, nodeId),
                    batchHb, lastSeq, lastTerm, records);
        }
        if (Wire.isolated(partition, nodeId)) return;

        String encoded = batch.encode();
        int bytes = encoded.getBytes(StandardCharsets.UTF_8).length;
        try {
            pubSub.publish(Wire.topicData(partition), encoded);
            String line = batch.getTerm() + ";" + batchHb + ";" + System.currentTimeMillis()
                    + ";" + batch.getLastSeq() + ";" + batch.getLastTerm()
                    + ";" + batch.getRecords().size() + ";" + bytes
                    + System.lineSeparator();
            Files.writeString(Path.of("hb-p" + partition + "-n" + nodeId + ".txt"), line,
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            logEvent("ERRO;p" + partition + ";publicar_lote=" + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void onBatch(String message) {
        if (Wire.isolated(partition, nodeId)) return;
        try {
            Batch batch = Batch.decode(message, partitions);
            if (batch.getLeader() == nodeId) return;
            synchronized (monitor) {
                if (!raft.onBatch(batch.getTerm(), batch.getLeader())) {
                    logEvent("IGNORADO;p" + partition + ";LOTE_termo=" + batch.getTerm());
                    return;
                }
                logEvent("RECEBIDO;p" + partition + ";LOTE_termo=" + batch.getTerm()
                        + ";lider=n" + batch.getLeader() + ";hb=" + batch.getHb());
                if (!batch.getRecords().isEmpty()) {
                    applyBatchRecords(batch);
                }
                // O cabeçalho também deteta leituras não publicadas, lacunas e
                // caudas divergentes; TCP permite comparar com a cópia do líder.
                reconcile(batch.getAddress(), batch.getPort(), batch.getLastSeq(),
                        batch.getLastTerm());
            }
        } catch (IOException | IllegalArgumentException e) {
            logEvent("ERRO;p" + partition + ";LOTE=" + e.getMessage());
        }
    }

    /** Aplica o lote em ordem, recuperando lacunas e truncando conflitos por termo. */
    private void applyBatchRecords(Batch batch) throws IOException {
        for (SensorRecord incoming : batch.getRecords()) {
            if (incoming.getSeq() <= lastSeq) {
                SensorRecord local = file.range(incoming.getSeq(), incoming.getSeq()).stream()
                        .findFirst().orElse(null);
                if (local != null && local.getTerm() == incoming.getTerm()) continue;
                if (local != null) {
                    String discardedTerms = termsFrom(file.readAll(), incoming.getSeq());
                    List<SensorRecord> replacement = new ArrayList<>();
                    replacement.add(incoming);
                    int removed = file.truncateFrom(incoming.getSeq(), replacement);
                    refreshLocalState();
                    logEvent("TRUNCADO;p" + partition + ";de=" + incoming.getSeq()
                            + ";descartados=" + removed + ";termos=" + discardedTerms
                            + "->" + incoming.getTerm());
                }
                continue;
            }
            if (incoming.getSeq() > lastSeq + 1) {
                recoverRange(batch.getAddress(), batch.getPort(), lastSeq + 1,
                        incoming.getSeq() - 1);
            }
            if (incoming.getSeq() == lastSeq + 1) appendFollowerRecord(incoming);
        }
    }

    /**
     * Reconcilia a cauda: começa no último bloco local de termo e recua por
     * blocos até encontrar um registo comum; o primeiro par (seq,termo) diferente
     * define o ponto de truncagem e a substituição pela cópia do líder.
     */
    private void reconcile(String host, int port, long leaderLastSeq, long leaderLastTerm)
            throws IOException {
        if (lastSeq == leaderLastSeq && lastTerm == leaderLastTerm) return;
        List<SensorRecord> local = file.readAll();
        if (local.isEmpty()) {
            if (leaderLastSeq > 0) recoverRange(host, port, 1, leaderLastSeq);
            return;
        }

        int blockStartIndex = local.size() - 1;
        long tailTerm = local.get(blockStartIndex).getTerm();
        while (blockStartIndex > 0 && local.get(blockStartIndex - 1).getTerm() == tailTerm) {
            blockStartIndex--;
        }
        long from = local.get(blockStartIndex).getSeq();
        List<SensorRecord> remote = from > leaderLastSeq
                ? new ArrayList<>() : fetchRange(host, port, from, leaderLastSeq);

        // Se o primeiro registo do bloco divergir, recua um bloco de termo.
        while (!sameRecordAt(remote, 0, local.get(blockStartIndex))
                && blockStartIndex > 0) {
            long previousTerm = local.get(blockStartIndex - 1).getTerm();
            blockStartIndex--;
            while (blockStartIndex > 0
                    && local.get(blockStartIndex - 1).getTerm() == previousTerm) {
                blockStartIndex--;
            }
            from = local.get(blockStartIndex).getSeq();
            remote = from > leaderLastSeq
                    ? new ArrayList<>() : fetchRange(host, port, from, leaderLastSeq);
        }

        int common = 0;
        while (common < remote.size() && blockStartIndex + common < local.size()
                && sameRecordAt(remote, common, local.get(blockStartIndex + common))) {
            common++;
        }
        boolean localHasExtra = local.get(local.size() - 1).getSeq() > leaderLastSeq;
        boolean tailTermDiffers = local.get(local.size() - 1).getSeq() == leaderLastSeq
                && local.get(local.size() - 1).getTerm() != leaderLastTerm;
        if (lastSeq < leaderLastSeq && common == local.size() - blockStartIndex
                && !localHasExtra && !tailTermDiffers) {
            recoverRange(host, port, lastSeq + 1, leaderLastSeq);
            return;
        }
        long firstDivergent = common < remote.size()
                ? remote.get(common).getSeq()
                : (blockStartIndex + common < local.size()
                    ? local.get(blockStartIndex + common).getSeq() : Long.MAX_VALUE);
        if (firstDivergent != Long.MAX_VALUE || localHasExtra || tailTermDiffers) {
            if (firstDivergent == Long.MAX_VALUE) firstDivergent = leaderLastSeq + 1;
            List<SensorRecord> replacement = new ArrayList<>();
            for (SensorRecord record : remote) {
                if (record.getSeq() >= firstDivergent) replacement.add(record);
            }
            String discardedTerms = termsFrom(local, firstDivergent);
            int removed = file.truncateFrom(firstDivergent, replacement);
            refreshLocalState();
            logEvent("TRUNCADO;p" + partition + ";de=" + firstDivergent
                    + ";descartados=" + removed + ";termos=" + discardedTerms);
        } else if (lastSeq < leaderLastSeq) {
            recoverRange(host, port, lastSeq + 1, leaderLastSeq);
        }
    }

    private static boolean sameRecordAt(List<SensorRecord> records, int index,
                                        SensorRecord expected) {
        return index < records.size()
                && records.get(index).getSeq() == expected.getSeq()
                && records.get(index).getTerm() == expected.getTerm();
    }

    private static String termsFrom(List<SensorRecord> records, long fromSeq) {
        java.util.Set<Long> terms = new java.util.TreeSet<>();
        for (SensorRecord record : records) {
            if (record.getSeq() >= fromSeq) terms.add(record.getTerm());
        }
        return terms.isEmpty() ? "nenhum" : terms.toString();
    }

    private void recoverRange(String host, int port, long from, long to) throws IOException {
        if (from > to) return;
        List<SensorRecord> recovered = fetchRange(host, port, from, to);
        int count = 0;
        for (SensorRecord record : recovered) {
            if (record.getSeq() == lastSeq + 1) {
                appendFollowerRecord(record);
                count++;
            }
        }
        if (count > 0) {
            logEvent("RECUPERACAO;p" + partition + ";de=" + from
                    + ";ate=" + to + ";n=" + count);
        }
    }

    private List<SensorRecord> fetchRange(String host, int port, long from, long to)
            throws IOException {
        if (from > to) return new ArrayList<>();
        if (Wire.isolated(partition, nodeId)) throw new IOException("nó isolado");
        List<SensorRecord> result = new ArrayList<>();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), Wire.T_TCP_MS);
            socket.setSoTimeout(Wire.T_TCP_MS);
            try (BufferedReader in = new BufferedReader(new InputStreamReader(
                            socket.getInputStream(), StandardCharsets.UTF_8));
                 PrintWriter out = new PrintWriter(new OutputStreamWriter(
                         socket.getOutputStream(), StandardCharsets.UTF_8), true)) {
                out.println("PEDIDO " + from + " " + to);
                if (out.checkError()) throw new IOException("falha ao enviar PEDIDO");
                String line;
                boolean finished = false;
                while ((line = in.readLine()) != null) {
                    if ("FIM".equals(line)) {
                        finished = true;
                        break;
                    }
                    if (line.startsWith("ERRO ")) throw new IOException(line);
                    result.add(SensorRecord.fromLine(line));
                }
                if (!finished) throw new IOException("resposta TCP sem FIM");
            }
        }
        return result;
    }

    private void appendFollowerRecord(SensorRecord record) throws IOException {
        file.append(record);
        lastSeq = record.getSeq();
        lastTerm = record.getTerm();
        lamport = record.getLamport();
        vector = record.getVector();
    }

    private void refreshLocalState() throws IOException {
        SensorRecord last = file.last();
        lastSeq = last == null ? 0 : last.getSeq();
        lastTerm = last == null ? 0 : last.getTerm();
        lamport = last == null ? 0 : last.getLamport();
        vector = last == null ? new VectorClock(partitions) : last.getVector();
    }

    private void runRecoveryServer(int port) {
        try (ServerSocket server = new ServerSocket(port)) {
            while (!Thread.currentThread().isInterrupted()) {
                Socket socket = server.accept();
                Thread handler = new Thread(() -> {
                    try (socket) {
                        serveRecovery(socket);
                    } catch (IOException e) {
                        logEvent("ERRO;p" + partition + ";tcp=" + e.getMessage());
                    }
                }, "raft04-recovery-n" + nodeId);
                handler.setDaemon(true);
                handler.start();
            }
        } catch (IOException e) {
            logEvent("ERRO;p" + partition + ";servidor_tcp=" + e.getMessage());
        }
    }

    /** Servidor disponível em qualquer papel, mas fechado logicamente durante isolamento. */
    private void serveRecovery(Socket socket) throws IOException {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(
                        socket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(
                     socket.getOutputStream(), StandardCharsets.UTF_8), true)) {
            if (Wire.isolated(partition, nodeId)) {
                out.println("ERRO rede isolada");
                return;
            }
            String request = in.readLine();
            String[] f = request == null ? new String[0] : request.trim().split("\\s+");
            if (f.length != 3 || !"PEDIDO".equals(f[0])) {
                out.println("ERRO pedido mal formado");
                return;
            }
            long from;
            long to;
            try {
                from = Long.parseLong(f[1]);
                to = Long.parseLong(f[2]);
            } catch (NumberFormatException e) {
                out.println("ERRO intervalo inválido");
                return;
            }
            if (from < 1 || to < from) {
                out.println("ERRO intervalo inválido");
                return;
            }
            for (SensorRecord record : file.range(from, to)) {
                out.println(record.toLine());
            }
            out.println("FIM");
            if (out.checkError()) throw new IOException("falha ao responder PEDIDO");
        }
    }

    private void logEvent(String event) {
        String[] parts = event.split(";", 3);
        String kind = parts[0];
        String partitionField = parts.length > 1 && parts[1].startsWith("p")
                ? parts[1] : "p" + partition;
        String details = parts.length > 2 ? ";" + parts[2] : "";
        String line = System.currentTimeMillis() + ";" + kind + ";" + partitionField + details;
        synchronized (logMonitor) {
            try {
                Files.writeString(Path.of("eventos-p" + partition + "-n" + nodeId + ".txt"),
                        line + System.lineSeparator(), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.err.println("Não foi possível escrever evento: " + e.getMessage());
            }
        }
        System.out.println(line);
    }

    private void stopLeaderSender() {
        if (leaderSender != null) {
            leaderSender.shutdownNow();
            leaderSender = null;
        }
    }

    private static void pauseBeforeRetry() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
