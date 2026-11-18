package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Analisador posterior à execução. Correlaciona eventos, heartbeats e ficheiros
 * dos três nós sem depender de relógios sincronizados durante a execução.
 */
public final class RaftCheck {
    private static final Pattern NODE_FILE = Pattern.compile("no-p(\\d+)-n(\\d+)\\.txt");
    private static final Pattern EVENT_FILE = Pattern.compile("eventos-p(\\d+)-n(\\d+)\\.txt");

    private static final class Event {
        final long time;
        final int partition;
        final int node;
        final String type;
        final String detail;

        Event(long time, int partition, int node, String type, String detail) {
            this.time = time;
            this.partition = partition;
            this.node = node;
            this.type = type;
            this.detail = detail;
        }

        long number(String name, long fallback) {
            Matcher m = Pattern.compile("(?:^|;)" + Pattern.quote(name) + "=([0-9]+)")
                    .matcher(detail);
            return m.find() ? Long.parseLong(m.group(1)) : fallback;
        }
    }

    private RaftCheck() { }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Uso: RaftCheck <N> <pasta> [falhas.txt]");
            System.exit(2);
        }
        try {
            int partitions = Integer.parseInt(args[0]);
            if (partitions < 1) throw new IllegalArgumentException("N tem de ser positivo");
            Path folder = Path.of(args[1]);
            Path failures = args.length == 3 ? Path.of(args[2]) : null;
            if (failures != null && !failures.isAbsolute() && !Files.exists(failures)
                    && Files.exists(folder.resolve(failures))) {
                failures = folder.resolve(failures);
            }
            check(partitions, folder, failures);
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("RaftCheck falhou: " + e.getMessage());
            System.exit(2);
        }
    }

    private static void check(int partitions, Path folder, Path failures) throws IOException {
        if (!Files.isDirectory(folder)) throw new IllegalArgumentException("Pasta inexistente: " + folder);
        List<Event> events = readEvents(folder);
        Map<Integer, List<SensorRecord>> recordsByNode = readNodeFiles(folder);
        Map<Integer, List<Event>> byPartition = new HashMap<>();
        for (Event event : events) {
            byPartition.computeIfAbsent(event.partition, ignored -> new ArrayList<>()).add(event);
        }

        int unsafeTerms = 0;
        int decreasingTerms = 0;
        for (int k = 0; k < partitions; k++) {
            List<Event> partEvents = byPartition.getOrDefault(k, List.of());
            partEvents.sort(Comparator.comparingLong(event -> event.time));
            Map<Long, List<Event>> terms = new TreeMap<>();
            long firstStart = Long.MAX_VALUE;
            long firstLeader = Long.MAX_VALUE;
            for (Event event : partEvents) {
                if (event.type.equals("CANDIDATO")) firstStart = Math.min(firstStart, event.time);
                if (event.type.equals("LIDER")) firstLeader = Math.min(firstLeader, event.time);
                long term = event.number("termo", -1);
                if (term >= 0) terms.computeIfAbsent(term, ignored -> new ArrayList<>()).add(event);
            }
            System.out.println("=== p" + k + " ===");
            for (Map.Entry<Long, List<Event>> item : terms.entrySet()) {
                long term = item.getKey();
                List<Event> termEvents = item.getValue();
                Set<Integer> leaders = new HashSet<>();
                Set<Integer> candidates = new HashSet<>();
                int votesReceived = 0;
                long started = Long.MAX_VALUE;
                long elected = Long.MAX_VALUE;
                for (Event event : termEvents) {
                    if (event.type.equals("CANDIDATO")) {
                        candidates.add(event.node);
                        started = Math.min(started, event.time);
                    } else if (event.type.equals("LIDER")) {
                        leaders.add(event.node);
                        elected = Math.min(elected, event.time);
                    } else if (event.type.equals("VOTO_RECEBIDO")) {
                        votesReceived++;
                    }
                }
                String outcome = leaders.isEmpty() ? "voto dividido/sem líder"
                        : "líderes=" + leaders;
                String duration = elected == Long.MAX_VALUE || started == Long.MAX_VALUE
                        ? "n/d" : (elected - started) + " ms";
                System.out.println("termo " + term + ": candidatos=" + candidates
                        + ", votos obtidos=" + (votesReceived + candidates.size())
                        + ", " + outcome
                        + ", duração eleição=" + duration);
                if (leaders.size() > 1) unsafeTerms++;
            }
            long previousByNode[] = new long[Wire.GRUPO + 1];
            for (Event event : partEvents) {
                long term = event.number("termo", -1);
                if (term > 0 && (event.type.equals("CANDIDATO") || event.type.equals("TERMO"))) {
                    if (term < previousByNode[event.node]) decreasingTerms++;
                    previousByNode[event.node] = Math.max(previousByNode[event.node], term);
                }
            }
            long start = partEvents.stream().filter(e -> e.type.equals("ARRANQUE"))
                    .mapToLong(e -> e.time).min().orElse(firstStart);
            System.out.println("tempo até primeiro líder: "
                    + (start == Long.MAX_VALUE || firstLeader == Long.MAX_VALUE
                        ? "n/d" : (firstLeader - start) + " ms"));

            List<SensorRecord> a = recordsByNode.getOrDefault(nodeKey(k, 1), List.of());
            List<SensorRecord> b = recordsByNode.getOrDefault(nodeKey(k, 2), List.of());
            List<SensorRecord> c = recordsByNode.getOrDefault(nodeKey(k, 3), List.of());
            printDivergence(k, a, b, c);
            for (Event event : partEvents) {
                if (event.type.equals("TRUNCADO")) {
                    System.out.println("TRUNCADO n" + event.node + ": " + event.detail);
                }
            }
        }
        System.out.println("segurança da eleição: termos com múltiplos líderes=" + unsafeTerms
                + "; diminuições de termo observadas=" + decreasingTerms);
        reportPersistentStates(folder);
        reportHeartbeats(folder, partitions);
        reportMonitors(folder);
        if (failures != null) reportFailures(folder, failures, events);
        reportLostReads(folder, recordsByNode, partitions);
    }

    private static List<Event> readEvents(Path folder) throws IOException {
        List<Event> result = new ArrayList<>();
        try (var files = Files.list(folder)) {
            for (Path path : (Iterable<Path>) files::iterator) {
                Matcher name = EVENT_FILE.matcher(path.getFileName().toString());
                if (!name.matches()) continue;
                int k = Integer.parseInt(name.group(1));
                int i = Integer.parseInt(name.group(2));
                for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                    String[] f = line.split(";", 4);
                    if (f.length >= 3) {
                        result.add(new Event(Long.parseLong(f[0]), k, i, f[1],
                                f.length == 4 ? f[3] : ""));
                    }
                }
            }
        }
        return result;
    }

    /** Chaves codificam p e i sem criar uma classe auxiliar para esta leitura. */
    private static Map<Integer, List<SensorRecord>> readNodeFiles(Path folder) throws IOException {
        Map<Integer, List<SensorRecord>> result = new HashMap<>();
        try (var files = Files.list(folder)) {
            for (Path path : (Iterable<Path>) files::iterator) {
                Matcher name = NODE_FILE.matcher(path.getFileName().toString());
                if (!name.matches()) continue;
                int k = Integer.parseInt(name.group(1));
                int i = Integer.parseInt(name.group(2));
                List<SensorRecord> records = new ArrayList<>();
                for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                    if (!line.isBlank()) records.add(SensorRecord.fromLine(line));
                }
                result.put(nodeKey(k, i), records);
            }
        }
        return result;
    }

    private static void printDivergence(int k, List<SensorRecord> a,
                                        List<SensorRecord> b, List<SensorRecord> c) {
        long min = Math.min(a.size(), Math.min(b.size(), c.size()));
        List<Long> different = new ArrayList<>();
        for (long seq = 1; seq <= min; seq++) {
            SensorRecord ar = a.get((int) seq - 1);
            SensorRecord br = b.get((int) seq - 1);
            SensorRecord cr = c.get((int) seq - 1);
            if (ar.getSeq() != seq || br.getSeq() != seq || cr.getSeq() != seq
                    || !ar.toLine().equals(br.toLine()) || !ar.toLine().equals(cr.toLine())) {
                different.add(seq);
            }
        }
        System.out.println("coincidência final até seq " + min + ": "
                + (different.isEmpty() ? "OK" : "seq divergentes=" + different));
    }

    private static void reportFailures(Path folder, Path failureFile, List<Event> events)
            throws IOException {
        if (!Files.exists(failureFile)) throw new IllegalArgumentException("Ficheiro de falhas inexistente");
        List<String> producer = Files.exists(folder.resolve("produtor.txt"))
                ? Files.readAllLines(folder.resolve("produtor.txt"), StandardCharsets.UTF_8)
                : List.of();
        for (String line : Files.readAllLines(failureFile, StandardCharsets.UTF_8)) {
            if (line.isBlank()) continue;
            String[] f = line.split(";");
            if (f.length != 3) continue;
            int k = Integer.parseInt(f[0].replace("p", ""));
            int failedNode = Integer.parseInt(f[1].replace("n", ""));
            long at = Long.parseLong(f[2]);
            long oldTerm = readHeartbeats(folder, k).stream()
                    .filter(h -> h.time <= at).mapToLong(h -> h.term).max().orElse(-1);
            Heartbeat recovery = readHeartbeats(folder, k).stream()
                    .filter(h -> h.time > at && h.term > oldTerm && h.node != failedNode)
                    .min(Comparator.comparingLong(h -> h.time)).orElse(null);
            if (recovery == null) {
                System.out.println("falha p" + k + "/n" + failedNode + ": sem novo LIDER observado");
                continue;
            }
            long sent = producer.stream().filter(row -> {
                String[] fields = row.split(";", 4);
                return fields.length >= 3 && Long.parseLong(fields[0]) >= at
                        && Long.parseLong(fields[0]) <= recovery.time
                        && fields[1].equals("p" + k);
            }).count();
            System.out.println("falha p" + k + "/n" + failedNode + ": indisponibilidade="
                    + (recovery.time - at) + " ms; LOTE de n" + recovery.node
                    + " termo=" + recovery.term + "; IN no intervalo=" + sent);
        }
    }

    private static void reportLostReads(Path folder,
                                        Map<Integer, List<SensorRecord>> recordsByNode,
                                        int partitions) throws IOException {
        Path producerFile = folder.resolve("produtor.txt");
        if (!Files.exists(producerFile)) return;
        for (int k = 0; k < partitions; k++) {
            Map<String, Integer> storedMaximum = new HashMap<>();
            for (int i = 1; i <= Wire.GRUPO; i++) {
                Map<String, Integer> nodeCounts = new HashMap<>();
                for (SensorRecord record : recordsByNode.getOrDefault(nodeKey(k, i), List.of())) {
                    nodeCounts.merge(record.getSensorId() + ";" + record.getTemperature(), 1,
                            Integer::sum);
                }
                nodeCounts.forEach((key, count) ->
                        storedMaximum.merge(key, count, Math::max));
            }
            Map<String, Integer> sentCounts = new HashMap<>();
            for (String line : Files.readAllLines(producerFile, StandardCharsets.UTF_8)) {
                String[] f = line.split(";", 4);
                if (f.length >= 4 && f[1].equals("p" + k)) {
                    sentCounts.merge(f[2] + ";" + f[3], 1, Integer::sum);
                }
            }
            long lost = sentCounts.entrySet().stream().mapToLong(entry ->
                    Math.max(0, entry.getValue()
                            - storedMaximum.getOrDefault(entry.getKey(), 0))).sum();
            System.out.println("leituras perdidas p" + k + ": " + lost);
        }
    }

    private static int nodeKey(int partition, int node) {
        return partition * 1000 + node;
    }

    private static final class Heartbeat {
        final int node;
        final long term;
        final long time;

        Heartbeat(int node, long term, long time) {
            this.node = node;
            this.term = term;
            this.time = time;
        }
    }

    private static List<Heartbeat> readHeartbeats(Path folder, int partition) throws IOException {
        List<Heartbeat> result = new ArrayList<>();
        Pattern pattern = Pattern.compile("hb-p" + partition + "-n(\\d+)\\.txt");
        try (var files = Files.list(folder)) {
            for (Path path : (Iterable<Path>) files::iterator) {
                Matcher name = pattern.matcher(path.getFileName().toString());
                if (!name.matches()) continue;
                int node = Integer.parseInt(name.group(1));
                for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                    String[] f = line.split(";", -1);
                    if (f.length >= 3) {
                        result.add(new Heartbeat(node, Long.parseLong(f[0]),
                                Long.parseLong(f[2])));
                    }
                }
            }
        }
        return result;
    }

    private static void reportPersistentStates(Path folder) throws IOException {
        try (var files = Files.list(folder)) {
            for (Path path : (Iterable<Path>) files::iterator) {
                if (!path.getFileName().toString().matches("estado-p\\d+-n\\d+\\.txt")) continue;
                String[] fields = Files.readString(path, StandardCharsets.UTF_8).trim().split(";");
                if (fields.length == 2) {
                    System.out.println(path.getFileName() + ": termo persistido=" + fields[0]
                            + "; votouEm=" + fields[1]);
                }
            }
        }
    }

    private static void reportHeartbeats(Path folder, int partitions) throws IOException {
        for (int k = 0; k < partitions; k++) {
            List<Heartbeat> heartbeats = readHeartbeats(folder, k);
            long first = heartbeats.stream().mapToLong(h -> h.time).min().orElse(-1);
            System.out.println("LOTEs observados p" + k + ": " + heartbeats.size()
                    + "; primeiro instante=" + (first < 0 ? "n/d" : first));
        }
    }

    private static void reportMonitors(Path folder) throws IOException {
        for (String name : List.of("monitor-processador.txt", "monitor-produtor.txt")) {
            Path path = folder.resolve(name);
            if (!Files.exists(path)) continue;
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            long warnings = lines.stream().filter(line -> line.contains(";AVISO;")).count();
            long suspicions = lines.stream()
                    .filter(line -> line.contains(";SUSPEITO;") || line.contains(";SUSPEITA;"))
                    .count();
            System.out.println(name + ": avisos=" + warnings + "; suspeitas=" + suspicions);
        }
    }
}
