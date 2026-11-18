package rep01;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/** Auxiliar de persistência: cada programa escreve apenas no seu ficheiro local. */
public class RecordFile {
    private final Path path;

    public RecordFile(String fileName) {
        this.path = Paths.get(fileName);
    }

    /** Acrescenta uma linha completa e cria o ficheiro se ainda não existir. */
    public synchronized void append(SensorRecord record) throws IOException {
        Files.writeString(path, record.toLine() + System.lineSeparator(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
    }

    /**
     * Lê a última sequência persistida para o processo continuar a numeração
     * após reiniciar. Um ficheiro vazio ou inexistente corresponde à sequência zero.
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
