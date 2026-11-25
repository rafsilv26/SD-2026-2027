package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class Chunk implements AutoCloseable {
    private final byte[] masterKey;
    private final Path selados;
    private final Consumer<String> eventLogger;
    private final ScheduledExecutorService timer;
    private final List<Registo> records = new ArrayList<>();
    private long firstInstantNanos;

    public Chunk(byte[] masterKey, Path selados, Consumer<String> eventLogger) {
        this.masterKey = masterKey;
        this.selados = selados;
        this.eventLogger = eventLogger;
        this.timer = Executors.newSingleThreadScheduledExecutor();
        this.timer.scheduleAtFixedRate(this::checkSeal, 100, 100, TimeUnit.MILLISECONDS);
    }

    public synchronized void add(Registo registo) throws Exception {
        if (records.isEmpty()) {
            firstInstantNanos = System.nanoTime();
        }
        records.add(registo);
        byte[] padded = serializar(records);
        if (padded.length >= Wire.CHUNK_MAX_BYTES) {
            seal();
        }
    }

    private void checkSeal() {
        try {
            synchronized (this) {
                if (!records.isEmpty()
                        && (System.nanoTime() - firstInstantNanos)
                        >= TimeUnit.MILLISECONDS.toNanos(Wire.SELAGEM_MS)) {
                    seal();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Falha ao selar chunk", e);
        }
    }

    private void seal() throws Exception {
        if (records.isEmpty()) {
            return;
        }

        byte[] plaintext = serializar(records);
        Sealed sealed = ChunkCrypto.seal(plaintext, masterKey);
        Files.write(selados.resolve(sealed.chunkId() + ".bin"),
                sealed.ciphertext(), StandardOpenOption.CREATE_NEW);

        String header = Wire.KEY_VERSION + ";"
                + Base64.getEncoder().encodeToString(sealed.salt()) + ";"
                + Base64.getEncoder().encodeToString(sealed.nonce());
        Files.writeString(selados.resolve(sealed.chunkId() + ".hdr"), header,
                StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);

        long primeiro = records.get(0).lsn();
        long ultimo = records.get(records.size() - 1).lsn();
        eventLogger.accept(System.currentTimeMillis() + ";SELADO;"
                + sealed.chunkId() + ";lsn=" + primeiro + "-" + ultimo
                + ";tamanho=" + plaintext.length);
        records.clear();
        firstInstantNanos = 0;
    }

    public synchronized void forceSeal() throws Exception {
        seal();
    }

    public static byte[] serializar(List<Registo> records) {
        int estimate = 4;
        for (Registo registo : records) {
            estimate += 16 + 12
                    + bytes(registo.autor()).length
                    + bytes(registo.canal()).length
                    + bytes(registo.texto()).length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(estimate + 4);
        buffer.putInt(records.size());
        for (Registo registo : records) {
            buffer.putLong(registo.lsn());
            buffer.putLong(registo.instante());
            putText(buffer, registo.autor());
            putText(buffer, registo.canal());
            putText(buffer, registo.texto());
        }

        int used = buffer.position();
        int paddedSize = ((used + Wire.PAD_BYTES - 1) / Wire.PAD_BYTES)
                * Wire.PAD_BYTES;
        byte[] padded = Arrays.copyOf(buffer.array(), paddedSize);
        return padded;
    }

    public static List<Registo> ler(byte[] padded) {
        ByteBuffer buffer = ByteBuffer.wrap(padded);
        int count = buffer.getInt();
        List<Registo> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long lsn = buffer.getLong();
            long instante = buffer.getLong();
            String autor = getText(buffer);
            String canal = getText(buffer);
            String texto = getText(buffer);
            result.add(new Registo(lsn, instante, autor, canal, texto));
        }
        return result;
    }

    public static byte[] abrirSelado(Path bin, Path hdr, byte[] masterKey)
            throws Exception {
        String id = bin.getFileName().toString().replace(".bin", "");
        String[] parts = Files.readString(hdr, StandardCharsets.UTF_8).split(";");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Cabeçalho do chunk inválido");
        }
        int keyVersion = Integer.parseInt(parts[0]);
        byte[] salt = Base64.getDecoder().decode(parts[1]);
        byte[] nonce = Base64.getDecoder().decode(parts[2]);
        Sealed sealed = new Sealed(Files.readAllBytes(bin),
                id, salt, nonce, keyVersion);
        return ChunkCrypto.open(sealed, masterKey);
    }

    private static void putText(ByteBuffer buffer, String text) {
        byte[] value = bytes(text);
        buffer.putInt(value.length);
        buffer.put(value);
    }

    private static String getText(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] value = new byte[length];
        buffer.get(value);
        return new String(value, StandardCharsets.UTF_8);
    }

    private static byte[] bytes(String value) {
        return value == null
                ? new byte[0]
                : value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }
}
