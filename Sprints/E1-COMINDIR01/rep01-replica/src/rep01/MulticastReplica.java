package rep01;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

/** Multicast replica that writes only a contiguous prefix of the leader's log. */
public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;
    private static final int BURST_IDLE_MILLIS = 1_500;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");

        try {
            long lastApplied = file.lastSeq();
            runReceiver(id, file, lastApplied);
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }

    private static void runReceiver(String id, RecordFile file, long lastApplied)
            throws IOException {
        // MulticastSocket enables address reuse for multicast receivers sharing a port.
        try (MulticastSocket socket = new MulticastSocket(PORT)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group);
            socket.setSoTimeout(BURST_IDLE_MILLIS);
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT
                    + "; último seq aplicado=" + lastApplied);

            Map<Long, SensorRecord> pending = new TreeMap<>();
            BurstMetrics metrics = new BurstMetrics();
            while (true) {
                byte[] buffer = new byte[1000];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                try {
                    socket.receive(packet); // Blocks for a datagram, or times out to close burst statistics.
                } catch (SocketTimeoutException e) {
                    metrics.printAndReset(pending.size());
                    continue;
                }

                long receivedAt = System.currentTimeMillis();
                String line = new String(packet.getData(), 0, packet.getLength(),
                        StandardCharsets.UTF_8);
                SensorRecord record;
                try {
                    // This is the trust boundary: received text is not assumed to be a valid record.
                    record = SensorRecord.fromLine(line);
                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + line);
                    continue;
                }

                metrics.recordReceived(receivedAt);
                lastApplied = apply(record, lastApplied, pending, file, metrics);
            }
        }
    }

    /**
     * Applies only the next expected sequence. Future records wait in memory
     * until every gap before them has arrived; this detects gaps but cannot
     * recover datagrams that were permanently lost.
     */
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
     * A burst has ended after 1.5 seconds without a datagram. Since messages
     * carry no burst identifier, this is an operational boundary for measuring,
     * not a property guaranteed by multicast.
     */
    private static final class BurstMetrics {
        private long firstReceivedAt;
        private long lastReceivedAt;
        private int received;
        private int written;
        private int duplicates;

        void recordReceived(long at) {
            if (received == 0) {
                firstReceivedAt = at;
            }
            lastReceivedAt = at;
            received++;
        }

        void recordWritten() {
            written++;
        }

        void recordDuplicate() {
            duplicates++;
        }

        void printAndReset(int pending) {
            if (received == 0) {
                return;
            }
            System.out.println("Rajada observada: recebidos=" + received
                    + "; escritos=" + written
                    + "; duplicados=" + duplicates
                    + "; retidos no fim=" + pending
                    + "; primeira receção(ms)=" + firstReceivedAt
                    + "; última receção(ms)=" + lastReceivedAt);
            firstReceivedAt = 0;
            lastReceivedAt = 0;
            received = 0;
            written = 0;
            duplicates = 0;
        }
    }
}
