package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/** Acesso sincronizado ao ficheiro append-only de um nó. */
public final class RecordFile {
    private final Path path;

    public RecordFile(String fileName) {
        this.path = Path.of(fileName);
    }

    public synchronized void append(SensorRecord record) throws IOException {
        Files.writeString(path, record.toLine() + System.lineSeparator(),
                StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public synchronized List<SensorRecord> readAll() throws IOException {
        List<SensorRecord> result = new ArrayList<>();
        if (Files.exists(path)) {
            for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) result.add(SensorRecord.fromLine(line));
            }
        }
        return result;
    }

    public synchronized SensorRecord last() throws IOException {
        List<SensorRecord> records = readAll();
        return records.isEmpty() ? null : records.get(records.size() - 1);
    }

    public synchronized long lastSeq() throws IOException {
        SensorRecord last = last();
        return last == null ? 0 : last.getSeq();
    }

    public synchronized List<SensorRecord> range(long from, long to) throws IOException {
        List<SensorRecord> result = new ArrayList<>();
        for (SensorRecord record : readAll()) {
            if (record.getSeq() >= from && record.getSeq() <= to) result.add(record);
        }
        return result;
    }

    /** Substitui atomicamente todos os registos a partir de firstSeq. */
    public synchronized int truncateFrom(long firstSeq, List<SensorRecord> replacement)
            throws IOException {
        List<SensorRecord> old = readAll();
        List<String> lines = new ArrayList<>();
        int removed = 0;
        for (SensorRecord record : old) {
            if (record.getSeq() < firstSeq) lines.add(record.toLine());
            else removed++;
        }
        for (SensorRecord record : replacement) lines.add(record.toLine());
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        Files.write(tmp, lines, StandardCharsets.UTF_8);
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        return removed;
    }
}
