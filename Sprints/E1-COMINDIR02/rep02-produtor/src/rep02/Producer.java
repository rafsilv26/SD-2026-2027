package rep02;

import java.io.IOException;
import java.util.Scanner;
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
        System.out.println("Produtor pronto: N=" + partitions + "; API=" + api
                + ". Comandos: <sensor> <temperatura> | rajada <n> <s>"
                + " | rajada-quente <n> <s> | sair");
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
                    handle(line, pubSub, partitioner);
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
    }

    private void handle(String line, IpfsPubSub pubSub, Partitioner partitioner)
            throws IOException, InterruptedException {
        String[] fields = line.split("\\s+");
        if (fields.length == 3 && ("rajada".equalsIgnoreCase(fields[0])
                || "rajada-quente".equalsIgnoreCase(fields[0]))) {
            int count = positiveInt(fields[1], "n");
            int sensors = positiveInt(fields[2], "s");
            boolean hot = "rajada-quente".equalsIgnoreCase(fields[0]);
            if (hot && sensors < 2) {
                throw new IllegalArgumentException("rajada-quente exige s >= 2");
            }
            publishBurst(count, sensors, hot, pubSub, partitioner);
            return;
        }
        if (fields.length != 2) {
            throw new IllegalArgumentException(
                    "esperado <sensor> <temperatura> ou rajada <n> <s>");
        }
        double temperature = Double.parseDouble(fields[1]);
        if (!Double.isFinite(temperature)) {
            throw new IllegalArgumentException("temperatura não finita");
        }
        publishReading(fields[0], temperature, pubSub, partitioner);
    }

    private void publishBurst(int count, int sensorCount, boolean hot, IpfsPubSub pubSub,
                              Partitioner partitioner)
            throws IOException, InterruptedException {
        int[] perPartition = new int[partitioner.count()];
        long firstAt = 0;
        long lastAt = 0;
        for (int i = 0; i < count; i++) {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            String sensor;
            if (hot && random.nextInt(100) < 80) {
                sensor = "S01";
            } else {
                int first = hot ? 2 : 1;
                sensor = String.format("S%02d", random.nextInt(first, sensorCount + 1));
            }
            double temperature = 15.0 + random.nextDouble() * 15.0;
            long publishedAt = publishReading(sensor, temperature, pubSub, partitioner);
            if (i == 0) {
                firstAt = publishedAt;
            }
            lastAt = publishedAt;
            perPartition[partitioner.of(sensor)]++;
        }
        System.out.println("Rajada publicada: " + count
                + "; primeira publicação(ms)=" + firstAt
                + "; última publicação(ms)=" + lastAt);
        for (int k = 0; k < perPartition.length; k++) {
            System.out.println("Leituras em p" + k + "=" + perPartition[k]);
        }
    }

    //Zona de escolha da partição
    private long publishReading(String sensor, double temperature, IpfsPubSub pubSub,
                                Partitioner partitioner)
            throws IOException, InterruptedException {
        int partition = partitioner.of(sensor);
        long publishedAt = System.currentTimeMillis();
        pubSub.publish(Wire.topicIn(partition), Wire.in(sensor, temperature));
        System.out.println("Publicado IN em p" + partition + ": "
                + sensor + ";" + temperature);
        return publishedAt;
    }

    private static int positiveInt(String value, String name) {
        int parsed = Integer.parseInt(value);
        if (parsed <= 0) {
            throw new IllegalArgumentException(name + " tem de ser positivo");
        }
        return parsed;
    }
}
