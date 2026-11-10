package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/** Verifica V[k]=seq em todos os logs de nós e preserva a análise causal anterior. */
public final class CausalCheck {
    private static final class Event {
        final int partition;
        final SensorRecord record;
        final VectorClock vector;

        Event(int partition, SensorRecord record) {
            this.partition = partition;
            this.record = record;
            this.vector = record.getVector();
        }

        String id() { return "p" + partition + "#" + record.getSeq() + "@" + record.getTerm(); }
    }

    private CausalCheck() { }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Uso: CausalCheck <N> <pasta>");
            System.exit(2);
        }
        try {
            int n = Integer.parseInt(args[0]);
            if (n < 1) throw new IllegalArgumentException("N tem de ser positivo");
            int errors = check(n, Path.of(args[1]));
            if (errors > 0) System.exit(1);
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("CausalCheck falhou: " + e.getMessage());
            System.exit(2);
        }
    }

    private static int check(int partitions, Path folder) throws IOException {
        if (!Files.isDirectory(folder)) throw new IllegalArgumentException("Pasta inexistente");
        List<Event> events = new ArrayList<>();
        Set<String> unique = new HashSet<>();
        int errors = 0;
        try (Stream<Path> paths = Files.list(folder)) {
            for (Path path : (Iterable<Path>) paths::iterator) {
                String name = path.getFileName().toString();
                if (!name.matches("no-p\\d+-n\\d+\\.txt")) continue;
                int partition = Integer.parseInt(name.substring(4, name.indexOf("-n")));
                int bad = 0;
                for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) continue;
                    SensorRecord record = SensorRecord.fromLine(line);
                    VectorClock vector = record.getVector();
                    if (vector.size() != partitions || vector.get(partition) != record.getSeq()) {
                        System.out.println("ERRO " + name + " seq=" + record.getSeq()
                                + ": dimensão ou V[k]=seq inválida");
                        bad++;
                    }
                    // A mesma cópia pode existir em três nós; cada evento lógico
                    // é contado uma vez, mas versões de termos diferentes mantêm-se.
                    String identity = partition + ":" + record.getSeq() + ":" + record.getTerm();
                    if (unique.add(identity)) events.add(new Event(partition, record));
                }
                errors += bad;
                System.out.println(name + ": " + bad + " violação(ões) de V[k]=seq");
            }
        }

        long causal = 0;
        long concurrent = 0;
        long lamportErrors = 0;
        for (int i = 0; i < events.size(); i++) {
            Event a = events.get(i);
            for (int j = i + 1; j < events.size(); j++) {
                Event b = events.get(j);
                if (a.partition == b.partition) continue;
                switch (a.vector.compare(b.vector)) {
                    case BEFORE:
                        causal++;
                        if (a.record.getLamport() >= b.record.getLamport()) lamportErrors++;
                        break;
                    case AFTER:
                        causal++;
                        if (b.record.getLamport() >= a.record.getLamport()) lamportErrors++;
                        break;
                    case CONCURRENT:
                        concurrent++;
                        break;
                    case EQUAL:
                        System.out.println("ERRO: vetores iguais em eventos distintos "
                                + a.id() + " e " + b.id());
                        errors++;
                        break;
                    default:
                        throw new AssertionError();
                }
            }
        }
        errors += (int) lamportErrors;
        events.sort(Comparator.comparingLong(e -> e.record.getLamport()));
        List<String> order = new ArrayList<>();
        for (Event event : events) {
            order.add("(" + event.record.getLamport() + "," + event.partition + ") "
                    + event.id() + " V=(" + event.vector + ")");
        }
        Files.write(folder.resolve("vista-lamport.txt"), order, StandardCharsets.UTF_8);
        System.out.println("Eventos=" + events.size() + "; causais=" + causal
                + "; concorrentes=" + concurrent
                + "; violações Lamport=" + lamportErrors);
        return errors;
    }
}
