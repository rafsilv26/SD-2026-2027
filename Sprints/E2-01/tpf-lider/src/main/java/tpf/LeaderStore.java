package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import java.util.Set;

public final class LeaderStore implements AutoCloseable {
    private final Path registos;
    private final Path eventos;
    private final byte[] masterKey;
    private final Chunk chunk;
    private long ultimoLsn;

    public LeaderStore() throws Exception {
        this(Path.of("."));
    }

    public LeaderStore(Path base) throws Exception {
        Wire.validar();
        Files.createDirectories(base);
        Path selados = base.resolve("selados");
        Files.createDirectories(selados);
        registos = base.resolve("registos.log");
        eventos = base.resolve("eventos-lider.txt");
        masterKey = carregarOuCriarMasterKey(base.resolve("master.key"));
        lerUltimoLsn();

        chunk = new Chunk(masterKey, selados, this::appendEvent);
    }

    public synchronized long registar(String autor, String canal, String texto) throws Exception {
        long lsn = ultimoLsn + 1;
        long instante = System.currentTimeMillis();

        String linha = lsn + ";"
                + instante + ";"
                + Base64.getEncoder().encodeToString(textoBytes(autor)) + ";"
                + Base64.getEncoder().encodeToString(textoBytes(canal)) + ";"
                + Base64.getEncoder().encodeToString(textoBytes(texto));

        try (FileChannel channel = FileChannel.open(registos,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)) {
            ByteBuffer buffer = StandardCharsets.UTF_8.encode(linha + "\n");
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }

        ultimoLsn = lsn;
        appendEvent(instante + ";REGISTO;lsn=" + lsn);

        Registo registo = new Registo(lsn, instante, autor, canal, texto);
        chunk.add(registo);
        return lsn;
    }

    public synchronized void selarAgora() throws Exception {
        chunk.forceSeal();
    }

    public byte[] masterKey() {
        return masterKey.clone();
    }

    private void lerUltimoLsn() throws IOException {
        ultimoLsn = 0;
        if (!Files.exists(registos)) {
            return;
        }
        try (java.util.stream.Stream<String> linhas = Files.lines(registos,
                StandardCharsets.UTF_8)) {
            for (String linha : (Iterable<String>) linhas::iterator) {
                if (linha.isBlank()) continue;
                String[] partes = linha.split(";", -1);
                if (partes.length >= 1) {
                    try {
                        ultimoLsn = Long.parseLong(partes[0]);
                    } catch (NumberFormatException ignored) {
                        // Ignora linhas inválidas; a linha seguinte continua em ordem.
                    }
                }
            }
        }
    }

    private byte[] carregarOuCriarMasterKey(Path keyFile) throws IOException {
        if (!Files.exists(keyFile)) {
            byte[] key = new byte[32];
            new SecureRandom().nextBytes(key);
            Files.write(keyFile, key, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            try {
                Set<PosixFilePermission> ownerOnly =
                        PosixFilePermissions.fromString("rw-------");
                Files.setPosixFilePermissions(keyFile, ownerOnly);
            } catch (UnsupportedOperationException | SecurityException ignored) {
                keyFile.toFile().setReadable(true, true);
                keyFile.toFile().setWritable(true, true);
            }
            return key;
        }

        byte[] key = Files.readAllBytes(keyFile);
        if (key.length != 32) {
            throw new IOException("master.key deve ter exatamente 32 bytes");
        }
        return key;
    }

    private void appendEvent(String linha) {
        try (FileChannel channel = FileChannel.open(eventos,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)) {
            ByteBuffer buffer = StandardCharsets.UTF_8.encode(linha + "\n");
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        } catch (IOException e) {
            throw new RuntimeException("Erro ao escrever evento do líder", e);
        }
    }

    private static byte[] textoBytes(String value) {
        return value == null
                ? new byte[0]
                : value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws Exception {
        chunk.close();
        try {
            selarAgora();
        } catch (Exception ignored) {
            // O fecho não deve esconder o estado anterior do processo.
        }
    }
}
