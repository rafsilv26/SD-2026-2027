package rep01;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/** Small persistence helper; each program writes only to its own local file. */
public class RecordFile {
    private final Path path;

    public RecordFile(String fileName) {
        this.path = Paths.get(fileName);
    }

    /** Appends one complete record line, creating the file on first use. */
    public synchronized void append(SensorRecord record) throws IOException {
        Files.writeString(path, record.toLine() + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    /**
     * Reads the last persisted sequence so a restarted process does not start
     * numbering again from zero. An empty or missing file has sequence zero.
     */
    public synchronized long lastSeq() throws IOException {
        if (!Files.exists(path)) {
            return 0;
        }

        long last = 0;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                last = SensorRecord.fromLine(line).getSeq();
            }
        }
        return last;
    }
}
