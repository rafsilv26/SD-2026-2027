package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

public final class HeartbeatCheck {
    private static final class Heartbeat {
        final int partition;
        final long inc;
        final long hb;
        final long instant;
        final long lastSeq;
        final int records;
        final int bytes;

        Heartbeat(int partition, String[] fields) {
            this.partition = partition;
            this.inc = Long.parseLong(fields[0]);
            this.hb = Long.parseLong(fields[1]);
            this.instant = Long.parseLong(fields[2]);
            this.lastSeq = Long.parseLong(fields[3]);
            this.records = Integer.parseInt(fields[4]);
            this.bytes = Integer.parseInt(fields[5]);
        }
    }

    private static final class MonitorEvent {
        final String participant;
        final long instant;
        final String type;
        final int partition;
        final Map<String, String> details;

        MonitorEvent(String participant, String[] fields) {
            this.participant = participant;
            this.instant = Long.parseLong(fields[0]);
            this.type = fields[1];
            this.partition = Integer.parseInt(fields[2].substring(1));
            this.details = new HashMap<>();
            for (int i = 3; i < fields.length; i++) {
                int separator = fields[i].indexOf('=');
                if (separator > 0) {
                    details.put(fields[i].substring(0, separator),
                            fields[i].substring(separator + 1));
                }
            }
        }

        long detailLong(String name, long fallback) {
            String value = details.get(name);
            return value == null ? fallback : Long.parseLong(value);
        }
    }

    private static final class Failure {
        final int partition;
        final long instant;

        Failure(int partition, long instant) {
            this.partition = partition;
            this.instant = instant;
        }
    }

    private HeartbeatCheck() { }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Uso: HeartbeatCheck <N> <pasta> [falhas.txt]");
            System.exit(2);
        }
        try {
            int partitions = Integer.parseInt(args[0]);
            if (partitions < 1) {
                throw new IllegalArgumentException("N tem de ser positivo");
            }
            Path folder = Paths.get(args[1]);
            Path failures = args.length == 3 ? resolveFailures(folder, args[2]) : null;
            check(partitions, folder, failures);
        } catch (NumberFormatException e) {
            System.err.println("Argumento numérico inválido: " + e.getMessage());
            System.exit(2);
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("HeartbeatCheck falhou: " + e.getMessage());
            System.exit(2);
        }
    }

    private static Path resolveFailures(Path folder, String value) {
        Path path = Paths.get(value);
        if (!path.isAbsolute() && !Files.exists(path)) {
            path = folder.resolve(path);
        }
        return path;
    }

    private static void check(int partitions, Path folder, Path failures) throws IOException {
        if (!Files.isDirectory(folder)) {
            throw new IllegalArgumentException("pasta inexistente: " + folder);
        }

        Map<Integer, List<Heartbeat>> heartbeats = readHeartbeats(partitions, folder);
        Map<Integer, TreeMap<Long, SensorRecord>> records = readLeaderRecords(partitions, folder);
        List<MonitorEvent> events = readMonitorEvents(folder);
        List<Failure> knownFailures = failures == null
                ? new ArrayList<>() : readFailures(failures, partitions);

        printBatchMetrics(partitions, heartbeats);
        printSuspicionMetrics(heartbeats, events);
        printFailureDetection(partitions, events, knownFailures);
        printLatencyAndBytes(partitions, heartbeats, records, events);

        long lost = events.stream().filter(event -> "HB_PERDIDO".equals(event.type)).count();
        long recoveries = events.stream().filter(event -> "RECUPERACAO".equals(event.type)).count();
        System.out.println("EVENTOS: HB_PERDIDO=" + lost + "; RECUPERACAO=" + recoveries);
    }

    private static Map<Integer, List<Heartbeat>> readHeartbeats(int partitions, Path folder)
            throws IOException {
        Map<Integer, List<Heartbeat>> result = new HashMap<>();
        for (int k = 0; k < partitions; k++) {
            List<Heartbeat> entries = new ArrayList<>();
            Path path = folder.resolve("hb-p" + k + ".txt");
            if (Files.exists(path)) {
                int lineNumber = 0;
                for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                    lineNumber++;
                    if (line.isBlank()) {
                        continue;
                    }
                    String[] fields = line.trim().split(";", -1);
                    if (fields.length != 6) {
                        throw new IOException("linha inválida em " + path + ":" + lineNumber);
                    }
                    try {
                        Heartbeat heartbeat = new Heartbeat(k, fields);
                        if (heartbeat.inc < 0 || heartbeat.hb < 1 || heartbeat.lastSeq < 0
                                || heartbeat.records < 0 || heartbeat.bytes < 0) {
                            throw new IllegalArgumentException("valor negativo ou sequência inválida");
                        }
                        entries.add(heartbeat);
                    } catch (IllegalArgumentException e) {
                        throw new IOException("linha inválida em " + path + ":" + lineNumber, e);
                    }
                }
            }
            result.put(k, entries);
        }
        return result;
    }

    private static Map<Integer, TreeMap<Long, SensorRecord>> readLeaderRecords(
            int partitions, Path folder) throws IOException {
        Map<Integer, TreeMap<Long, SensorRecord>> result = new HashMap<>();
        for (int k = 0; k < partitions; k++) {
            TreeMap<Long, SensorRecord> partitionRecords = new TreeMap<>();
            Path path = folder.resolve("lider-p" + k + ".txt");
            if (Files.exists(path)) {
                int lineNumber = 0;
                for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                    lineNumber++;
                    if (line.isBlank()) {
                        continue;
                    }
                    try {
                        SensorRecord record = SensorRecord.fromLine(line);
                        partitionRecords.put(record.getSeq(), record);
                    } catch (IllegalArgumentException e) {
                        throw new IOException("linha inválida em " + path + ":" + lineNumber, e);
                    }
                }
            }
            result.put(k, partitionRecords);
        }
        return result;
    }

    private static List<MonitorEvent> readMonitorEvents(Path folder) throws IOException {
        List<MonitorEvent> result = new ArrayList<>();
        try (Stream<Path> paths = Files.list(folder)) {
            for (Path path : (Iterable<Path>) paths
                    .filter(file -> file.getFileName().toString().matches("monitor-.+\\.txt"))
                    .sorted()::iterator) {
                String name = path.getFileName().toString();
                String participant = name.substring("monitor-".length(),
                        name.length() - ".txt".length());
                int lineNumber = 0;
                for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                    lineNumber++;
                    if (line.isBlank()) {
                        continue;
                    }
                    String[] fields = line.trim().split(";", -1);
                    if (fields.length < 3 || !fields[2].matches("p[0-9]+")) {
                        throw new IOException("linha inválida em " + path + ":" + lineNumber);
                    }
                    try {
                        result.add(new MonitorEvent(participant, fields));
                    } catch (IllegalArgumentException e) {
                        throw new IOException("linha inválida em " + path + ":" + lineNumber, e);
                    }
                }
            }
        }
        result.sort(Comparator.comparingLong(event -> event.instant));
        return result;
    }

    private static List<Failure> readFailures(Path path, int partitions) throws IOException {
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("ficheiro de falhas inexistente: " + path);
        }
        List<Failure> result = new ArrayList<>();
        int lineNumber = 0;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            lineNumber++;
            if (line.isBlank()) {
                continue;
            }
            String[] fields = line.trim().split(";", -1);
            try {
                if (fields.length != 2 || !fields[0].matches("p[0-9]+")) {
                    throw new IllegalArgumentException("formato esperado p<k>;instante");
                }
                int partition = Integer.parseInt(fields[0].substring(1));
                long instant = Long.parseLong(fields[1]);
                if (partition < 0 || partition >= partitions) {
                    throw new IllegalArgumentException("partição fora do intervalo");
                }
                result.add(new Failure(partition, instant));
            } catch (IllegalArgumentException e) {
                throw new IOException("linha inválida em " + path + ":" + lineNumber, e);
            }
        }
        result.sort(Comparator.comparingLong(failure -> failure.instant));
        return result;
    }

    private static void printBatchMetrics(int partitions, Map<Integer, List<Heartbeat>> all) {
        System.out.println("LOTES por partição e incarnação:");
        for (int k = 0; k < partitions; k++) {
            Map<Long, List<Heartbeat>> byIncarnation = new TreeMap<>();
            for (Heartbeat heartbeat : all.get(k)) {
                byIncarnation.computeIfAbsent(heartbeat.inc, ignored -> new ArrayList<>())
                        .add(heartbeat);
            }
            if (byIncarnation.isEmpty()) {
                System.out.println("  p" + k + ": sem dados");
            }
            for (Map.Entry<Long, List<Heartbeat>> entry : byIncarnation.entrySet()) {
                List<Heartbeat> entries = entry.getValue();
                entries.sort(Comparator.comparingLong(heartbeat -> heartbeat.hb));
                long empty = 0;
                long records = 0;
                int maxRecords = 0;
                long intervalTotal = 0;
                long maxInterval = 0;
                int intervals = 0;
                for (int i = 0; i < entries.size(); i++) {
                    Heartbeat heartbeat = entries.get(i);
                    if (heartbeat.records == 0) {
                        empty++;
                    }
                    records += heartbeat.records;
                    maxRecords = Math.max(maxRecords, heartbeat.records);
                    if (i > 0) {
                        long interval = heartbeat.instant - entries.get(i - 1).instant;
                        intervalTotal += interval;
                        maxInterval = Math.max(maxInterval, interval);
                        intervals++;
                    }
                }
                double emptyRatio = (double) empty / entries.size();
                double avgRecords = (double) records / entries.size();
                double avgInterval = intervals == 0 ? 0 : (double) intervalTotal / intervals;
                System.out.printf("  p%d inc=%d: lotes=%d; vazios=%.3f; registos/lote=%.2f"
                                + "/%d (média/máx.); intervalo_ms=%.2f/%d (média/máx.)%n",
                        k, entry.getKey(), entries.size(), emptyRatio, avgRecords, maxRecords,
                        avgInterval, maxInterval);
            }
        }
    }

    private static void printSuspicionMetrics(Map<Integer, List<Heartbeat>> heartbeats,
                                              List<MonitorEvent> events) {
        Map<Integer, List<Heartbeat>> batchesByPartition = new HashMap<>();
        for (List<Heartbeat> partitionEntries : heartbeats.values()) {
            for (Heartbeat heartbeat : partitionEntries) {
                batchesByPartition.computeIfAbsent(heartbeat.partition,
                        ignored -> new ArrayList<>()).add(heartbeat);
            }
        }
        Map<String, List<MonitorEvent>> groups = new TreeMap<>();
        for (MonitorEvent event : events) {
            if ("SUSPEITA".equals(event.type)) {
                String key = event.participant + ";p" + event.partition;
                groups.computeIfAbsent(key, ignored -> new ArrayList<>()).add(event);
            }
        }
        System.out.println("SUSPEITAS por participante e partição:");
        if (groups.isEmpty()) {
            System.out.println("  sem suspeitas registadas");
        }
        for (Map.Entry<String, List<MonitorEvent>> entry : groups.entrySet()) {
            long real = 0;
            long falseSuspicions = 0;
            for (MonitorEvent suspicion : entry.getValue()) {
                long inc = suspicion.detailLong("inc", -1);
                MonitorEvent retoma = findSameIncarnationResume(suspicion, events, inc);
                if (retoma == null) {
                    real++;
                } else {
                    falseSuspicions++;
                }
                Long silence = silenceSincePublishedHeartbeat(suspicion,
                        batchesByPartition.get(suspicion.partition), inc);
                String classification = retoma == null ? "falha real" : "falsa suspeita";
                System.out.println("  " + entry.getKey() + " @" + suspicion.instant + ": "
                        + classification + "; inc=" + inc + "; silencio_desde_ultimo_hb_ms="
                        + (silence == null ? "indisponível" : silence));
            }
            System.out.println("  resumo " + entry.getKey() + ": reais=" + real
                    + "; falsas=" + falseSuspicions);
        }
    }

    private static MonitorEvent findSameIncarnationResume(
            MonitorEvent suspicion, List<MonitorEvent> events, long inc) {
        for (MonitorEvent event : events) {
            if (event.instant >= suspicion.instant && event.partition == suspicion.partition
                    && event.participant.equals(suspicion.participant)
                    && "RETOMA".equals(event.type)
                    && event.detailLong("inc", Long.MIN_VALUE) == inc) {
                return event;
            }
        }
        return null;
    }

    private static Long silenceSincePublishedHeartbeat(
            MonitorEvent suspicion, List<Heartbeat> heartbeats, long inc) {
        if (heartbeats == null) {
            return null;
        }
        Heartbeat latest = null;
        for (Heartbeat heartbeat : heartbeats) {
            if ((inc < 0 || heartbeat.inc == inc) && heartbeat.instant <= suspicion.instant
                    && (latest == null || heartbeat.instant > latest.instant)) {
                latest = heartbeat;
            }
        }
        return latest == null ? null : suspicion.instant - latest.instant;
    }

    private static void printFailureDetection(int partitions, List<MonitorEvent> events,
                                              List<Failure> failures) {
        if (failures.isEmpty()) {
            return;
        }
        System.out.println("TEMPO DE DETEÇÃO por falha e participante:");
        Set<MonitorEvent> used = new HashSet<>();
        for (Failure failure : failures) {
            Set<String> participants = expectedParticipants(partitions);
            for (String participant : participants) {
                if (!monitorsPartition(participant, failure.partition, partitions)) {
                    continue;
                }
                MonitorEvent detected = events.stream()
                        .filter(event -> !used.contains(event)
                                && event.partition == failure.partition
                                && event.participant.equals(participant)
                                && "SUSPEITA".equals(event.type)
                                && event.instant >= failure.instant
                                && findSameIncarnationResume(event, events,
                                event.detailLong("inc", -1)) == null)
                        .min(Comparator.comparingLong(event -> event.instant))
                        .orElse(null);
                if (detected == null) {
                    System.out.println("  p" + failure.partition + " "
                            + participant + ": sem deteção");
                } else {
                    used.add(detected);
                    long delay = detected.instant - failure.instant;
                    System.out.println("  p" + failure.partition + " "
                            + participant + ": " + delay + " ms");
                }
            }
        }
    }

    private static Set<String> expectedParticipants(int partitions) {
        Set<String> participants = new TreeSet<>();
        participants.add("processador");
        participants.add("produtor");
        for (int replica = 1; replica <= partitions; replica++) {
            participants.add("replica" + replica);
        }
        return participants;
    }

    private static boolean monitorsPartition(String participant, int partition, int partitions) {
        if ("processador".equals(participant) || "produtor".equals(participant)) {
            return true;
        }
        if (!participant.startsWith("replica")) {
            return false;
        }
        try {
            int replica = Integer.parseInt(participant.substring("replica".length()));
            return replica > 0 && (partition == (replica - 1) % partitions
                    || partition == replica % partitions);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static void printLatencyAndBytes(
            int partitions, Map<Integer, List<Heartbeat>> heartbeats,
            Map<Integer, TreeMap<Long, SensorRecord>> records, List<MonitorEvent> events) {
        System.out.println("LATÊNCIA dos registos em lotes:");
        long totalBatchBytes = 0;
        long totalBatchRecords = 0;
        long totalRegBytes = 0;
        long totalRegRecords = 0;
        for (int k = 0; k < partitions; k++) {
            long latencyTotal = 0;
            long latencyMax = Long.MIN_VALUE;
            long latencyCount = 0;
            TreeMap<Long, SensorRecord> partitionRecords = records.get(k);
            for (MonitorEvent event : events) {
                if (event.partition != k || !"RECEBIDO".equals(event.type)) {
                    continue;
                }
                String range = event.details.get("seq");
                if (range == null || "vazio".equals(range)) {
                    continue;
                }
                String[] bounds = range.split("\\.\\.", -1);
                if (bounds.length != 2) {
                    continue;
                }
                long from = Long.parseLong(bounds[0]);
                long to = Long.parseLong(bounds[1]);
                for (long seq = from; seq <= to; seq++) {
                    SensorRecord record = partitionRecords.get(seq);
                    if (record != null) {
                        long latency = event.instant - recordInstant(record);
                        latencyTotal += latency;
                        latencyMax = Math.max(latencyMax, latency);
                        latencyCount++;
                    }
                }
            }
            if (latencyCount == 0) {
                System.out.println("  p" + k + ": sem amostras");
            } else {
                System.out.printf("  p%d: média=%.2f ms; máximo=%d ms; amostras=%d%n",
                        k, (double) latencyTotal / latencyCount, latencyMax, latencyCount);
            }
            for (Heartbeat heartbeat : heartbeats.get(k)) {
                if (heartbeat.records == 0) {
                    continue;
                }
                totalBatchBytes += heartbeat.bytes;
                totalBatchRecords += heartbeat.records;
                long firstSeq = heartbeat.lastSeq - heartbeat.records + 1;
                for (long seq = firstSeq; seq <= heartbeat.lastSeq; seq++) {
                    SensorRecord record = partitionRecords.get(seq);
                    if (record != null) {
                        String reg = "REG;127.0.0.1;" + (Wire.PORTO_BASE + k) + ";"
                                + record.toLine();
                        totalRegBytes += reg.getBytes(StandardCharsets.UTF_8).length;
                        totalRegRecords++;
                    }
                }
            }
        }
        System.out.printf("BYTES por registo: LOTE=%.2f bytes; REG equivalente=%.2f bytes"
                        + " (host REG assumido 127.0.0.1)%n",
                totalBatchRecords == 0 ? 0.0 : (double) totalBatchBytes / totalBatchRecords,
                totalRegRecords == 0 ? 0.0 : (double) totalRegBytes / totalRegRecords);
    }

    private static long recordInstant(SensorRecord record) {
        try {
            return Long.parseLong(record.getTimestamp());
        } catch (NumberFormatException e) {
            return LocalDateTime.parse(record.getTimestamp()).atZone(ZoneId.systemDefault())
                    .toInstant().toEpochMilli();
        }
    }
}
