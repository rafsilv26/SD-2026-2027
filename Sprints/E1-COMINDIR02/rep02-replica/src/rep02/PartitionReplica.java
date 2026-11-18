package rep02;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class PartitionReplica {
    private static final String DEFAULT_API_HOST = "127.0.0.1";

    private final String id;
    private final String api;
    private final List<PartitionState> states = new ArrayList<>();
    private final double lossProbability;
    private final long discardedSeq;
    private final AtomicBoolean discardedOnce = new AtomicBoolean();

    private PartitionReplica(String id, List<Integer> partitions, String api,
                             double lossProbability, long discardedSeq) throws IOException {
        this.id = id;
        this.api = api;
        this.lossProbability = lossProbability;
        this.discardedSeq = discardedSeq;
        for (int partition : partitions) {
            RecordFile file = new RecordFile("replica-" + id + "-p" + partition + ".txt");
            states.add(new PartitionState(partition, file, file.lastSeq()));
        }
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Uso: PartitionReplica <id> <k1,k2> [portoApi]");
            return;
        }
        try {
            String id = args[0];
            int replicaId = Integer.parseInt(id);
            if (replicaId < 1) {
                throw new IllegalArgumentException("id da réplica tem de ser positivo");
            }
            int apiPort = args.length > 2 ? Integer.parseInt(args[2]) : 5001;
            if (apiPort < 1 || apiPort > 65535) {
                throw new IllegalArgumentException("porto da API inválido");
            }
            List<Integer> partitions = parsePartitions(args[1]);
            double loss = Double.parseDouble(System.getProperty("perda", "0"));
            if (!Double.isFinite(loss) || loss < 0 || loss > 1) {
                throw new IllegalArgumentException("-Dperda tem de estar entre 0 e 1");
            }
            long discard = Long.parseLong(System.getProperty("descartar", "-1"));
            if (discard < -1 || discard == 0) {
                throw new IllegalArgumentException("-Ddescartar tem de ser >= 1");
            }
            String api = "http://" + DEFAULT_API_HOST + ":" + apiPort + "/api/v0";
            new PartitionReplica(id, partitions, api, loss, discard).run();
        } catch (NumberFormatException e) {
            System.err.println("Argumento numérico inválido: " + e.getMessage());
        } catch (IllegalArgumentException | IOException e) {
            System.err.println("Não foi possível iniciar a réplica: " + e.getMessage());
        }
    }

    private static List<Integer> parsePartitions(String value) {
        String[] fields = value.split(",", -1);
        Set<Integer> unique = new HashSet<>();
        for (String field : fields) {
            int partition = Integer.parseInt(field);
            if (partition < 0 || partition >= 3 || !unique.add(partition)) {
                throw new IllegalArgumentException("lista de partições inválida: " + value);
            }
        }
        if (unique.isEmpty()) {
            throw new IllegalArgumentException("indique pelo menos uma partição");
        }
        return new ArrayList<>(unique);
    }

    private void run() {
        IpfsPubSub pubSub = new IpfsPubSub(api);
        List<Thread> threads = new ArrayList<>();
        for (PartitionState state : states) {
            Thread thread = new Thread(() -> subscribe(pubSub, state),
                    "rep02-replica-" + id + "-p" + state.partition);
            threads.add(thread);
            thread.start();
            System.out.println("Réplica " + id + " a subscrever "
                    + Wire.topicData(state.partition) + "; último seq="
                    + state.lastSeq + "; API=" + api);
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Espera pelas subscrições interrompida.");
                return;
            }
        }
    }

    private void subscribe(IpfsPubSub pubSub, PartitionState state) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                pubSub.subscribe(Wire.topicData(state.partition),
                        message -> onMessage(state, message));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException | RuntimeException e) {
                System.err.println("Subscrição p" + state.partition + " falhou: "
                        + e.getMessage());
                pauseBeforeRetry();
            }
        }
    }

    private void onMessage(PartitionState state, String message) {
        final String[] parts;
        final SensorRecord record;
        try {
            parts = Wire.regParts(message);
            int port = Integer.parseInt(parts[1]);
            if (parts[0].isBlank() || port < 1 || port > 65535) {
                throw new IllegalArgumentException("endereço ou porto inválido");
            }
            record = SensorRecord.fromLine(parts[2]);
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + message);
            return;
        }

        state.leaderHost = parts[0];
        state.leaderPort = Integer.parseInt(parts[1]);
        if (shouldDiscard(record.getSeq())) {
            System.out.println("DESCARTADO (simulação): " + record.getSeq());
            return;
        }
        try {
            apply(state, record, true);
        } catch (IOException e) {
            System.err.println("Falha ao gravar p" + state.partition + ": " + e.getMessage());
        }
    }

    private boolean shouldDiscard(long seq) {
        if (discardedSeq == seq && discardedOnce.compareAndSet(false, true)) {
            return true;
        }
        return ThreadLocalRandom.current().nextDouble() < lossProbability;
    }

    private void apply(PartitionState state, SensorRecord record, boolean triggerRecovery)
            throws IOException {
        long seq = record.getSeq();
        if (seq <= state.lastSeq || state.retained.containsKey(seq)) {
            System.out.println("DUPLICADO: p" + state.partition + " seq=" + seq);
            return;
        }
        if (seq > state.lastSeq + 1) {
            state.retained.put(seq, record);
            System.out.println("EM ESPERA: p" + state.partition + " seq=" + seq
                    + " (falta " + (state.lastSeq + 1) + ")");
            if (triggerRecovery && state.retained.size() > Wire.LIMITE_RETIDOS) {
                recover(state);
            }
            return;
        }
        append(state, record);
        drainContiguous(state);
    }

    private void append(PartitionState state, SensorRecord record) throws IOException {
        state.file.append(record);
        state.lastSeq = record.getSeq();
        System.out.println("Aplicado p" + state.partition + ": " + record.toLine());
    }

    private void drainContiguous(PartitionState state) throws IOException {
        while (state.retained.containsKey(state.lastSeq + 1)) {
            SensorRecord next = state.retained.remove(state.lastSeq + 1);
            append(state, next);
        }
    }

    private void recover(PartitionState state) {
        long from = state.lastSeq + 1;
        long to = state.retained.firstKey() - 1;
        if (to < from || state.leaderHost == null || state.leaderPort < 1) {
            return;
        }
        System.out.println("RECUPERAÇÃO: " + from + ".." + to + " pedida a "
                + state.leaderHost + ":" + state.leaderPort);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(state.leaderHost, state.leaderPort),
                    Wire.T_TCP_MS);
            socket.setSoTimeout(Wire.T_TCP_MS);
            try (BufferedReader in = new BufferedReader(new InputStreamReader(
                            socket.getInputStream(), StandardCharsets.UTF_8));
                 PrintWriter out = new PrintWriter(new OutputStreamWriter(
                         socket.getOutputStream(), StandardCharsets.UTF_8), true)) {
                out.println("PEDIDO " + from + " " + to);
                if (out.checkError()) {
                    throw new IOException("falha ao enviar PEDIDO");
                }
                boolean finished = false;
                String line;
                while ((line = in.readLine()) != null) {
                    if ("FIM".equals(line)) {
                        finished = true;
                        break;
                    }
                    if (line.startsWith("ERRO ")) {
                        throw new IOException(line);
                    }
                    SensorRecord recovered;
                    try {
                        recovered = SensorRecord.fromLine(line);
                    } catch (IllegalArgumentException e) {
                        throw new IOException("registo inválido na resposta: " + line, e);
                    }
                    apply(state, recovered, false);
                    System.out.println("RECUPERADO: " + recovered.getSeq());
                }
                if (!finished) {
                    throw new IOException("resposta terminada antes de FIM");
                }
                drainContiguous(state);
            }
        } catch (IOException e) {
            System.out.println("RECUPERAÇÃO FALHOU: " + e.getMessage());
        }
    }

    private static void pauseBeforeRetry() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class PartitionState {
        private final int partition;
        private final RecordFile file;
        private final TreeMap<Long, SensorRecord> retained = new TreeMap<>();
        private long lastSeq;
        private String leaderHost;
        private int leaderPort;

        private PartitionState(int partition, RecordFile file, long lastSeq) {
            this.partition = partition;
            this.file = file;
            this.lastSeq = lastSeq;
        }
    }
}
