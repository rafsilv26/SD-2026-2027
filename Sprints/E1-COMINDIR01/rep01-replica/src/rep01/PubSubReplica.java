package rep01;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Variante da réplica que recebe registos por uma subscrição Kubo PubSub. */
public class PubSubReplica {
    static final String TOPIC = "rep01-temperaturas";
    private static final int BURST_IDLE_MILLIS = 1_500;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        int apiPort = 5001;
        if (args.length > 1) {
            try {
                apiPort = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("Porto da API inválido: " + args[1]);
                return;
            }
        }

        String api = "http://127.0.0.1:" + apiPort + "/api/v0";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        try {
            long lastApplied = file.lastSeq();
            System.out.println("Réplica " + id + " ligada ao tópico " + TOPIC
                    + " através de " + api + "; último seq aplicado=" + lastApplied);
            subscribe(api, file, lastApplied);
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }

    private static void subscribe(String api, RecordFile file, long initialLastApplied)
            throws IOException {
        IpfsPubSub pubSub = new IpfsPubSub(api);
        Map<Long, SensorRecord> pending = new TreeMap<>();
        AtomicInteger pendingCount = new AtomicInteger();
        BurstMetrics metrics = new BurstMetrics(pendingCount);

        // O fluxo PubSub não marca o fim da rajada; um período sem mensagens
        // é usado apenas para agrupar medições.
        Thread monitor = new Thread(metrics::monitor, "rep01-pubsub-metrics");
        monitor.setDaemon(true);
        monitor.start();

        long[] lastApplied = {initialLastApplied};
        try {
            pubSub.subscribe(TOPIC, message -> {
                long receivedAt = System.currentTimeMillis();
                SensorRecord record;
                try {
                    record = SensorRecord.fromLine(message);
                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + message);
                    return;
                }

                try {
                    metrics.recordReceived(receivedAt);
                    lastApplied[0] = apply(record, lastApplied[0], pending, file, metrics);
                    pendingCount.set(pending.size());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Subscrição interrompida.");
        } finally {
            metrics.printAndReset();
        }
    }

    /** Aplica a mesma regra de prefixo contínuo usada na réplica multicast. */
    private static long apply(SensorRecord record, long lastApplied,
                              Map<Long, SensorRecord> pending, RecordFile file,
                              BurstMetrics metrics) throws IOException {
        long seq = record.getSeq();
        if (seq <= lastApplied || pending.containsKey(seq)) {
            System.out.println("DUPLICADO: " + seq);
            metrics.recordDuplicate();
            return lastApplied;
        }

        if (seq > lastApplied + 1) {
            pending.put(seq, record);
            System.out.println("EM ESPERA: " + seq + " (falta o registo "
                    + (lastApplied + 1) + ")");
            return lastApplied;
        }

        lastApplied = append(record, file, metrics, lastApplied);
        while (pending.containsKey(lastApplied + 1)) {
            SensorRecord next = pending.remove(lastApplied + 1);
            lastApplied = append(next, file, metrics, lastApplied);
        }
        return lastApplied;
    }

    private static long append(SensorRecord record, RecordFile file,
                               BurstMetrics metrics, long previousSeq)
            throws IOException {
        file.append(record);
        metrics.recordWritten();
        System.out.println("Aplicado: " + record.toLine());
        return previousSeq + 1;
    }

    /**
     * PubSub não delimita rajadas. O intervalo de silêncio serve só para agrupar
     * medições e não prova que a rede entregou todos os registos.
     */
    private static final class BurstMetrics {
        private final AtomicInteger pendingCount;
        private long firstReceivedAt;
        private long lastReceivedAt;
        private long lastArrivalNanos;
        private int received;
        private int written;
        private int duplicates;

        BurstMetrics(AtomicInteger pendingCount) {
            this.pendingCount = pendingCount;
        }

        synchronized void recordReceived(long receivedAt) {
            if (received == 0) {
                firstReceivedAt = receivedAt;
            }
            lastReceivedAt = receivedAt;
            lastArrivalNanos = System.nanoTime();
            received++;
        }

        synchronized void recordWritten() {
            written++;
        }

        synchronized void recordDuplicate() {
            duplicates++;
        }

        synchronized void printAndReset() {
            if (received == 0) {
                return;
            }
            System.out.println("Rajada observada: recebidos=" + received
                    + "; escritos=" + written
                    + "; duplicados=" + duplicates
                    + "; retidos no fim=" + pendingCount.get()
                    + "; primeira receção(ms)=" + firstReceivedAt
                    + "; última receção(ms)=" + lastReceivedAt);
            firstReceivedAt = 0;
            lastReceivedAt = 0;
            lastArrivalNanos = 0;
            received = 0;
            written = 0;
            duplicates = 0;
        }

        void monitor() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }

                synchronized (this) {
                    long quietNanos = System.nanoTime() - lastArrivalNanos;
                    if (received > 0
                            && quietNanos >= BURST_IDLE_MILLIS * 1_000_000L) {
                        printAndReset();
                    }
                }
            }
        }
    }
}
