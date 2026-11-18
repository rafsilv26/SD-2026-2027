package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Processador de alarmes: subscreve os REG de todas as partições (uma thread por
 * partição), acumula o conhecimento causal em Lp/Vp e, quando uma leitura chega
 * ao limiar, publica uma leitura derivada ALARM-<sensor> com os seus carimbos.
 * Não gera eventos próprios: só transporta L e V de uma partição para outra.
 */
public class AlarmProcessor {
    private static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";

    private final int partitions;
    private final Partitioner partitioner;
    private final IpfsPubSub pubSub;
    private final List<HeartbeatMonitor> monitors = new ArrayList<>();
    private final Object monitorLogLock = new Object();

    // estado partilhado pelas threads, sempre acedido dentro de synchronized (lock)
    private final Object lock = new Object();
    private long lp;
    private final VectorClock vp;
    private final Set<String> seen = new HashSet<>();   // "k:seq" dos REG já processados

    private AlarmProcessor(int partitions, String api) {
        this.partitions = partitions;
        this.partitioner = new Partitioner(partitions);
        this.pubSub = new IpfsPubSub(api);
        this.lp = 0;
        this.vp = new VectorClock(partitions);
        for (int k = 0; k < partitions; k++) {
            final int partition = k;
            monitors.add(new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT,
                    event -> logEvent(partition, event)));
        }
    }

    public static void main(String[] args) {
        if (args.length < 1 || args.length > 2) {
            System.err.println("Uso: AlarmProcessor <N> [api]");
            return;
        }
        try {
            int partitions = Integer.parseInt(args[0]);
            String api = args.length > 1 ? args[1] : DEFAULT_API;
            new AlarmProcessor(partitions, api).run();
        } catch (NumberFormatException e) {
            System.err.println("N tem de ser um inteiro.");
        } catch (IllegalArgumentException e) {
            System.err.println("Não foi possível iniciar o processador: " + e.getMessage());
        }
    }

    private void run() {
        List<Thread> threads = new ArrayList<>();
        ScheduledExecutorService checker = Executors.newSingleThreadScheduledExecutor();
        checker.scheduleAtFixedRate(
                () -> monitors.forEach(HeartbeatMonitor::check),
                Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
        for (int k = 0; k < partitions; k++) {
            final int partition = k;
            Thread thread = new Thread(() -> subscribe(partition),
                    "rep04-processador-p" + partition);
            threads.add(thread);
            thread.start();
        }
        System.out.println("Processador pronto: N=" + partitions + "; limiar="
                + Wire.LIMIAR_ALARME + "; a subscrever rep04-p0..p" + (partitions - 1));
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                checker.shutdownNow();
                return;
            }
        }
        checker.shutdownNow();
    }

    private void subscribe(int partition) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                pubSub.subscribe(Wire.topicData(partition),
                        message -> onMessage(partition, message));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException | RuntimeException e) {
                System.err.println("Subscrição p" + partition + " falhou: " + e.getMessage());
                pauseBeforeRetry();
            }
        }
    }

    private void onMessage(int partition, String message) {
        Batch batch;
        try {
            batch = Batch.decode(message, partitions);
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + message);
            return;
        }
        monitors.get(partition).onHeartbeat(batch.getInc(), batch.getHb());
        List<SensorRecord> records = batch.getRecords();
        String range = records.isEmpty() ? "vazio"
                : records.get(0).getSeq() + ".." + records.get(records.size() - 1).getSeq();
        logEvent(partition, "RECEBIDO;p" + partition + ";inc=" + batch.getInc()
                + ";hb=" + batch.getHb() + ";ultimoSeq=" + batch.getLastSeq()
                + ";n=" + records.size() + ";seq=" + range);
        for (SensorRecord record : records) {
            processRecord(partition, record);
        }
    }

    private void processRecord(int partition, SensorRecord record) {

        String sensor = record.getSensorId();
        boolean isAlarm = sensor.startsWith(Wire.PREFIXO_ALARME);
        boolean raise = !isAlarm && record.getTemperature() >= Wire.LIMIAR_ALARME;

        long alarmLamport;
        VectorClock alarmVector;
        synchronized (lock) {
            // o mesmo REG pode chegar duas vezes: só conta uma
            if (!seen.add(partition + ":" + record.getSeq())) {
                System.out.println("DUPLICADO: p" + partition + " seq=" + record.getSeq());
                return;
            }
            // acumula o conhecimento causal de todos os REG, incluindo os ALARM-*
            try {
                vp.merge(record.getVector());
            } catch (IllegalArgumentException e) {
                seen.remove(partition + ":" + record.getSeq());
                System.out.println("REJEITADO: " + record.toLine() + " (" + e.getMessage() + ")");
                return;
            }
            lp = Math.max(lp, record.getLamport());
            System.out.println("OBSERVADO: p" + partition + " seq=" + record.getSeq()
                    + " " + sensor + " " + record.getTemperature()
                    + " -> Lp=" + lp + " Vp=(" + vp + ")");
            if (!raise) {
                return;
            }
            // fotografia dos carimbos neste instante: a publicação não partilha o Vp
            alarmLamport = lp;
            alarmVector = vp.copy();
        }

        // publicar fora do cadeado: a rede pode demorar e não deve bloquear as outras threads
        String alarmSensor = Wire.PREFIXO_ALARME + sensor;
        int target = partitioner.of(alarmSensor);
        if (monitors.get(target).state() == HeartbeatMonitor.State.SUSPEITO) {
            String warning = "AVISO: líder p" + target + " suspeito; alarme "
                    + alarmSensor + " pode perder-se";
            System.out.println(warning);
            logEvent(target, "AVISO;p" + target + ";alarme=" + alarmSensor);
        }
        try {
            pubSub.publish(Wire.topicIn(target),
                    Wire.in(alarmSensor, record.getTemperature(), alarmLamport, alarmVector));
            System.out.println("ALARME: " + alarmSensor + " " + record.getTemperature()
                    + " -> p" + target + " com L=" + alarmLamport + " V=(" + alarmVector + ")");
        } catch (IOException e) {
            System.err.println("Publicação do alarme falhou: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void logEvent(int partition, String event) {
        String[] parts = event.split(";", 3);
        String details = parts.length > 2 ? ";" + parts[2] : "";
        String line = System.currentTimeMillis() + ";" + parts[0] + ";p" + partition + details;
        synchronized (monitorLogLock) {
            try {
                Files.writeString(Path.of("monitor-processador.txt"),
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
}
