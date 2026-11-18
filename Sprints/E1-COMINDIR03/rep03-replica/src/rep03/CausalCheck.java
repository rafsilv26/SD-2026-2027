package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Verificação causal dos ficheiros dos líderes (lider-p*.txt):
 *  1. V[k] = seq em todos os registos da partição k;
 *  2. classifica cada par de eventos em causal (a -> b) ou concorrente (a || b);
 *  3. verifica a condição de Lamport: a -> b  implica  L(a) < L(b);
 *  4. conta os pares concorrentes com L distinto (a ordem de Lamport não é causal);
 *  5. escreve vista-lamport.txt com a ordem total por (L, k).
 */
public class CausalCheck {

    // um evento = um registo de um líder, com a partição onde ocorreu
    private static final class Event {
        final int partition;
        final SensorRecord record;
        final VectorClock vector;

        Event(int partition, SensorRecord record) {
            this.partition = partition;
            this.record = record;
            this.vector = record.getVector();
        }

        String id() { return "p" + partition + "#" + record.getSeq(); }
        long lamport() { return record.getLamport(); }
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Uso: CausalCheck <N> <pasta>");
            return;
        }
        try {
            int partitions = Integer.parseInt(args[0]);
            if (partitions < 1) {
                throw new IllegalArgumentException("N tem de ser positivo");
            }
            int errors = check(partitions, Paths.get(args[1]));
            if (errors > 0) {
                System.err.println("Verificação causal concluída com " + errors + " problema(s).");
                System.exit(1);
            }
        } catch (NumberFormatException e) {
            System.err.println("N tem de ser um inteiro.");
            System.exit(2);
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("Verificação causal falhou: " + e.getMessage());
            System.exit(2);
        }
    }

    private static int check(int partitions, Path folder) throws IOException {
        if (!Files.isDirectory(folder)) {
            throw new IllegalArgumentException("pasta inexistente: " + folder);
        }
        int errors = 0;
        List<Event> events = new ArrayList<>();

        // 1. ler os ficheiros e verificar V[k] = seq
        for (int k = 0; k < partitions; k++) {
            Path path = folder.resolve("lider-p" + k + ".txt");
            if (!Files.exists(path)) {
                System.out.println("ERRO p" + k + ": falta " + path.getFileName());
                errors++;
                continue;
            }
            int bad = 0;
            List<SensorRecord> records = read(path);
            for (SensorRecord record : records) {
                VectorClock v = record.getVector();
                if (v.size() != partitions) {
                    System.out.println("ERRO " + path.getFileName() + " seq=" + record.getSeq()
                            + ": vetor com " + v.size() + " posições, esperava " + partitions);
                    bad++;
                    continue;
                }
                if (v.get(k) != record.getSeq()) {
                    System.out.println("ERRO " + path.getFileName() + ": V[" + k + "]="
                            + v.get(k) + " diferente de seq=" + record.getSeq());
                    bad++;
                }
                events.add(new Event(k, record));
            }
            errors += bad;
            System.out.println("p" + k + ": " + records.size() + " eventos; V[" + k + "] = seq "
                    + (bad == 0 ? "OK" : "com " + bad + " erro(s)"));
        }

        // 2-4. comparar todos os pares
        long causal = 0;
        long concurrent = 0;
        long concurrentDistinctL = 0;
        long lamportViolations = 0;
        long equalVectors = 0;
        for (int i = 0; i < events.size(); i++) {
            Event a = events.get(i);
            for (int j = i + 1; j < events.size(); j++) {
                Event b = events.get(j);
                if (a.partition == b.partition) {
                    continue; // só interessam pares de partições diferentes
                }
                switch (a.vector.compare(b.vector)) {
                    case BEFORE:
                        causal++;
                        if (a.lamport() >= b.lamport()) {
                            lamportViolations++;
                            reportViolation(a, b);
                        }
                        break;
                    case AFTER:
                        causal++;
                        if (b.lamport() >= a.lamport()) {
                            lamportViolations++;
                            reportViolation(b, a);
                        }
                        break;
                    case CONCURRENT:
                        concurrent++;
                        if (a.lamport() != b.lamport()) {
                            concurrentDistinctL++;
                        }
                        break;
                    case EQUAL:
                        // dois eventos distintos nunca podem ter o mesmo vetor
                        equalVectors++;
                        System.out.println("ERRO: " + a.id() + " e " + b.id()
                                + " têm o mesmo vetor (" + a.vector + ")");
                        break;
                }
            }
        }
        errors += lamportViolations + equalVectors;

        long pairs = causal + concurrent + equalVectors;
        System.out.println("PARES: " + pairs + "; causais=" + causal
                + "; concorrentes=" + concurrent);
        System.out.println("CONDIÇÃO DE LAMPORT (a -> b => L(a) < L(b)): "
                + (lamportViolations == 0 ? "OK" : lamportViolations + " violação(ões)"));
        System.out.println("CONCORRENTES COM L DISTINTO: " + concurrentDistinctL
                + " (Lamport ordena-os, mas não há relação causal)");

        // 5. ordem total por (L, k)
        events.sort(Comparator.comparingLong(Event::lamport)
                .thenComparingInt(e -> e.partition)
                .thenComparingLong(e -> e.record.getSeq()));
        List<String> lines = new ArrayList<>();
        for (Event e : events) {
            lines.add("(" + e.lamport() + "," + e.partition + ") " + e.id() + " "
                    + e.record.getSensorId() + " " + e.record.getTemperature()
                    + " V=(" + e.vector + ")");
        }
        Path out = folder.resolve("vista-lamport.txt");
        Files.write(out, lines, StandardCharsets.UTF_8);
        System.out.println("Ordem total por (L, k) escrita em " + out + " (" + lines.size()
                + " eventos)");
        return errors;
    }

    private static void reportViolation(Event before, Event after) {
        System.out.println("ERRO LAMPORT: " + before.id() + " -> " + after.id()
                + " mas L=" + before.lamport() + " >= L=" + after.lamport());
    }

    private static List<SensorRecord> read(Path path) throws IOException {
        List<SensorRecord> records = new ArrayList<>();
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                try {
                    records.add(SensorRecord.fromLine(line));
                } catch (IllegalArgumentException e) {
                    throw new IOException("linha inválida em " + path + ": " + line, e);
                }
            }
        }
        return records;
    }
}
