package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class PartitionCheck {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Uso: PartitionCheck <N> <pasta>");
            return;
        }
        try {
            int partitions = Integer.parseInt(args[0]);
            if (partitions < 1) {
                throw new IllegalArgumentException("N tem de ser positivo");
            }
            int errors = check(partitions, Paths.get(args[1]));
            if (errors > 0) {
                System.err.println("Verificação concluída com " + errors + " problema(s).");
                System.exit(1);
            }
        } catch (NumberFormatException e) {
            System.err.println("N tem de ser um inteiro.");
            System.exit(2);
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("Verificação falhou: " + e.getMessage());
            System.exit(2);
        }
    }

    private static int check(int partitions, Path folder) throws IOException {
        if (!Files.isDirectory(folder)) {
            throw new IllegalArgumentException("pasta inexistente: " + folder);
        }
        Partitioner partitioner = new Partitioner(partitions);
        int errors = 0;
        long totalRecords = 0;
        for (int k = 0; k < partitions; k++) {
            Path leaderPath = folder.resolve("lider-p" + k + ".txt");
            if (!Files.exists(leaderPath)) {
                System.out.println("ERRO p" + k + ": falta " + leaderPath.getFileName());
                errors++;
                continue;
            }
            List<SensorRecord> leader = read(leaderPath);
            int leaderSequenceErrors = sequenceErrors(leader, leaderPath.getFileName().toString());
            errors += leaderSequenceErrors;
            Set<String> sensors = new HashSet<>();
            for (SensorRecord record : leader) {
                sensors.add(record.getSensorId());
                if (partitioner.of(record.getSensorId()) != k) {
                    System.out.println("ERRO p" + k + ": sensor "
                            + record.getSensorId() + " pertence a p"
                            + partitioner.of(record.getSensorId()));
                    errors++;
                }
            }
            totalRecords += leader.size();
            System.out.println("p" + k + ": líder=" + leader.size()
                    + " registos; sensores=" + sensors.size());

            List<Path> replicas = replicaFiles(folder, k);
            if (replicas.size() != 2) {
                System.out.println("ERRO p" + k + ": esperadas 2 réplicas, encontradas "
                        + replicas.size());
                errors++;
            }
            for (Path replicaPath : replicas) {
                List<SensorRecord> replica = read(replicaPath);
                errors += sequenceErrors(replica, replicaPath.getFileName().toString());
                boolean prefix = isPrefix(replica, leader);
                long missing = Math.max(0, leader.size() - replica.size());
                System.out.println("  " + replicaPath.getFileName() + ": "
                        + (prefix ? (replica.size() == leader.size() ? "IGUAL" : "PREFIXO")
                        : "DIVERGENTE")
                        + "; registos=" + replica.size() + "; em falta=" + missing);
                if (!prefix) {
                    errors++;
                }
            }
        }
        long sumByPartitions = 0;
        for (int k = 0; k < partitions; k++) {
            Path path = folder.resolve("lider-p" + k + ".txt");
            if (Files.exists(path)) {
                sumByPartitions += read(path).size();
            }
        }
        System.out.println("TOTAL: " + totalRecords + " registos; soma das partições="
                + sumByPartitions + (totalRecords == sumByPartitions ? " (OK)" : " (ERRO)"));
        if (totalRecords != sumByPartitions) {
            errors++;
        }
        return errors;
    }

    private static List<Path> replicaFiles(Path folder, int partition) throws IOException {
        List<Path> files = new ArrayList<>();
        try (Stream<Path> paths = Files.list(folder)) {
            paths.filter(path -> path.getFileName().toString()
                            .matches("replica-[0-9]+-p" + partition + "\\.txt"))
                    .sorted()
                    .forEach(files::add);
        }
        return files;
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

    private static int sequenceErrors(List<SensorRecord> records, String fileName) {
        int errors = 0;
        long expected = 1;
        for (SensorRecord record : records) {
            if (record.getSeq() != expected) {
                System.out.println("ERRO " + fileName + ": esperado seq="
                        + expected + ", encontrado=" + record.getSeq());
                errors++;
                expected = record.getSeq();
            }
            expected++;
        }
        return errors;
    }

    private static boolean isPrefix(List<SensorRecord> replica, List<SensorRecord> leader) {
        if (replica.size() > leader.size()) {
            return false;
        }
        for (int i = 0; i < replica.size(); i++) {
            if (!replica.get(i).toLine().equals(leader.get(i).toLine())) {
                return false;
            }
        }
        return true;
    }
}
