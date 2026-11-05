package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;

public class Producer {
    private static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) {
        int partitions = 1;
        String api = DEFAULT_API;
        try {
            if (args.length > 0) {
                if (args[0].startsWith("http://") || args[0].startsWith("https://")) {
                    api = args[0];
                } else {
                    partitions = Integer.parseInt(args[0]);
                }
            }
            if (args.length > 1) {
                api = args[1];
            }
            new Producer().run(partitions, api);
        } catch (IllegalArgumentException e) {
            System.err.println("Argumentos inválidos: " + e.getMessage());
            System.err.println("Uso: Producer <N> [api]");
        }
    }

    private void run(int partitions, String api) {
        Partitioner partitioner;
        try {
            partitioner = new Partitioner(partitions);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            return;
        }
        IpfsPubSub pubSub = new IpfsPubSub(api);
        HeartbeatMonitor[] monitors = new HeartbeatMonitor[partitions];
        for (int k = 0; k < partitions; k++) {
            final int partition = k;
            monitors[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT,
                    event -> logEvent(partition, event));
        }
        for (int k = 0; k < partitions; k++) {
            final int partition = k;
            Thread listener = new Thread(() -> subscribe(pubSub, partition, partitions, monitors),
                    "rep04-producer-p" + partition);
            listener.setDaemon(true);
            listener.start();
        }
        ScheduledExecutorService checker = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "rep04-producer-monitor");
            thread.setDaemon(true);
            return thread;
        });
        checker.scheduleAtFixedRate(
                () -> java.util.Arrays.stream(monitors).forEach(HeartbeatMonitor::check),
                Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
        System.out.println("Produtor pronto: N=" + partitions + "; API=" + api
                + ". Comandos: <sensor> <temperatura> | rajada <n> <intervalo_ms>"
                + " | rajada-quente <n> <intervalo_ms> | sair");
        try {
            try (Scanner scanner = new Scanner(System.in)) {
                while (true) {
                    System.out.print("> ");
                    if (!scanner.hasNextLine()) {
                        return;
                    }
                    String line = scanner.nextLine().trim();
                    if (line.equalsIgnoreCase("sair")) {
                        return;
                    }
                    try {
                        handle(line, pubSub, partitioner, monitors);
                    } catch (IOException e) {
                        System.err.println("Publicação falhou: " + e.getMessage());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        System.err.println("Publicação interrompida.");
                        return;
                    } catch (IllegalArgumentException e) {
                        System.err.println("Comando inválido: " + e.getMessage());
                    }
                }
            }
        } finally {
            checker.shutdownNow();
        }
    }

    private void handle(String line, IpfsPubSub pubSub, Partitioner partitioner,
                        HeartbeatMonitor[] monitors)
            throws IOException, InterruptedException {
        String[] fields = line.split("\\s+");
        if (fields.length == 3 && ("rajada".equalsIgnoreCase(fields[0])
                || "rajada-quente".equalsIgnoreCase(fields[0]))) {
            int count = positiveInt(fields[1], "n");
            int intervalMs = nonNegativeInt(fields[2], "intervalo_ms");
            boolean hot = "rajada-quente".equalsIgnoreCase(fields[0]);
            publishBurst(count, intervalMs, hot, pubSub, partitioner, monitors);
            return;
        }
        if (fields.length != 2) {
            throw new IllegalArgumentException(
                    "esperado <sensor> <temperatura> ou rajada <n> <intervalo_ms>");
        }
        double temperature = Double.parseDouble(fields[1]);
        if (!Double.isFinite(temperature)) {
            throw new IllegalArgumentException("temperatura não finita");
        }
        publishReading(fields[0], temperature, pubSub, partitioner, monitors);
    }

    private void publishBurst(int count, int intervalMs, boolean hot, IpfsPubSub pubSub,
                              Partitioner partitioner, HeartbeatMonitor[] monitors)
            throws IOException, InterruptedException {
        int[] perPartition = new int[partitioner.count()];
        long firstAt = 0;
        long lastAt = 0;
        long nextSendNanos = System.nanoTime();
        for (int i = 0; i < count; i++) {
            if (i > 0 && intervalMs > 0) {
                long remaining = nextSendNanos - System.nanoTime();
                if (remaining > 0) {
                    TimeUnit.NANOSECONDS.sleep(remaining);
                }
            }
            ThreadLocalRandom random = ThreadLocalRandom.current();
            String sensor;
            if (hot && random.nextInt(100) < 80) {
                sensor = "S01";
            } else {
                int first = hot ? 2 : 1;
                sensor = String.format("S%02d", random.nextInt(first, 11));
            }
            double temperature = 15.0 + random.nextDouble() * 15.0;
            long publishedAt = publishReading(sensor, temperature, pubSub, partitioner, monitors);
            if (i == 0) {
                firstAt = publishedAt;
            }
            lastAt = publishedAt;
            perPartition[partitioner.of(sensor)]++;
            nextSendNanos += intervalMs * 1_000_000L;
        }
        System.out.println("Rajada publicada: " + count
                + "; intervalo(ms)=" + intervalMs
                + "; primeira publicação(ms)=" + firstAt
                + "; última publicação(ms)=" + lastAt);
        for (int k = 0; k < perPartition.length; k++) {
            System.out.println("Leituras em p" + k + "=" + perPartition[k]);
        }
    }

    //Zona de escolha da partição
    private long publishReading(String sensor, double temperature, IpfsPubSub pubSub,
                                Partitioner partitioner, HeartbeatMonitor[] monitors)
            throws IOException, InterruptedException {
        int partition = partitioner.of(sensor);
        if (monitors[partition].state() == HeartbeatMonitor.State.SUSPEITO) {
            String warning = "AVISO: líder p" + partition + " suspeito; leitura "
                    + sensor + " pode perder-se";
            System.out.println(warning);
            logEvent(partition, "AVISO;p" + partition + ";leitura=" + sensor);
        }
        long publishedAt = System.currentTimeMillis();
        // o produtor não observa registos: envia sempre L = 0 e V = (0,...,0)
        pubSub.publish(Wire.topicIn(partition),
                Wire.in(sensor, temperature, 0, new VectorClock(partitioner.count())));
        System.out.println("Publicado IN em p" + partition + ": "
                + sensor + ";" + temperature);
        return publishedAt;
    }

    private void subscribe(IpfsPubSub pubSub, int partition, int partitions,
                           HeartbeatMonitor[] monitors) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                pubSub.subscribe(Wire.topicData(partition),
                        message -> onHeartbeat(partition, partitions, monitors, message));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException | RuntimeException e) {
                System.err.println("Subscrição p" + partition + " falhou: " + e.getMessage());
                pauseBeforeRetry();
            }
        }
    }

    private void onHeartbeat(int partition, int partitions, HeartbeatMonitor[] monitors,
                             String message) {
        try {
            String headerLine = message.split("\\R", 2)[0];
            String[] fields = headerLine.split(";", -1);
            if (fields.length != 7 || !"LOTE".equals(fields[0]) || fields[1].isBlank()) {
                throw new IllegalArgumentException("Cabeçalho LOTE mal formado");
            }
            int port = Integer.parseInt(fields[2]);
            long inc = Long.parseLong(fields[3]);
            long hb = Long.parseLong(fields[4]);
            long lastSeq = Long.parseLong(fields[5]);
            int count = Integer.parseInt(fields[6]);
            if (port < 1 || port > 65535 || inc < 0 || hb < 1 || lastSeq < 0 || count < 0) {
                throw new IllegalArgumentException("Cabeçalho LOTE inválido");
            }
            monitors[partition].onHeartbeat(inc, hb);
            logEvent(partition, "RECEBIDO;p" + partition + ";inc=" + inc + ";hb=" + hb
                    + ";ultimoSeq=" + lastSeq + ";n=" + count);
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + message + " (" + e.getMessage() + ")");
        }
    }

    private void logEvent(int partition, String event) {
        String[] parts = event.split(";", 3);
        String details = parts.length > 2 ? ";" + parts[2] : "";
        String line = System.currentTimeMillis() + ";" + parts[0] + ";p" + partition + details;
        synchronized (Producer.class) {
            try {
                Files.writeString(Path.of("monitor-produtor.txt"),
                        line + System.lineSeparator(), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.err.println("Não foi possível registar evento do detetor: "
                        + e.getMessage());
            }
        }
        System.out.println(line);
    }

    private static void pauseBeforeRetry() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static int positiveInt(String value, String name) {
        int parsed = Integer.parseInt(value);
        if (parsed <= 0) {
            throw new IllegalArgumentException(name + " tem de ser positivo");
        }
        return parsed;
    }

    private static int nonNegativeInt(String value, String name) {
        int parsed = Integer.parseInt(value);
        if (parsed < 0) {
            throw new IllegalArgumentException(name + " não pode ser negativo");
        }
        return parsed;
    }
}
