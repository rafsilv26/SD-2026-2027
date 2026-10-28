package rep03;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        for (int k = 0; k < partitions; k++) {
            final int partition = k;
            Thread thread = new Thread(() -> subscribe(partition),
                    "rep03-processador-p" + partition);
            threads.add(thread);
            thread.start();
        }
        System.out.println("Processador pronto: N=" + partitions + "; limiar="
                + Wire.LIMIAR_ALARME + "; a subscrever rep03-p0..p" + (partitions - 1));
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
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
        SensorRecord record;
        try {
            record = SensorRecord.fromLine(Wire.regParts(message)[2]);
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + message);
            return;
        }

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
                System.out.println("REJEITADO: " + message + " (" + e.getMessage() + ")");
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

    private static void pauseBeforeRetry() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
