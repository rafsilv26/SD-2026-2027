package rep01;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

/** Variante do líder que publica registos num tópico PubSub do Kubo. */
public class PubSubLeader {
    static final String TOPIC = "rep01-temperaturas";
    static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : DEFAULT_API;

        try (Scanner scanner = new Scanner(System.in)) {
            IpfsPubSub pubSub = new IpfsPubSub(api);
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();
            System.out.println("Líder PubSub pronto em " + api
                    + ". Formato: <sensor> <temperatura> | rajada <n> | sair");

            while (true) {
                System.out.print("> ");
                if (!scanner.hasNextLine()) { // Bloqueia à espera de um comando da CLI.
                    break;
                }

                String line = scanner.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) {
                    break;
                }

                String[] parts = line.split("\\s+");
                if (parts.length == 2 && parts[0].equalsIgnoreCase("rajada")) {
                    int count;
                    try {
                        count = Integer.parseInt(parts[1]);
                    } catch (NumberFormatException e) {
                        System.out.println("Número de registos inválido: " + parts[1]);
                        continue;
                    }
                    if (count <= 0) {
                        System.out.println("O tamanho da rajada tem de ser positivo.");
                        continue;
                    }

                    long firstPublishAt = 0;
                    long lastPublishAt = 0;
                    for (int i = 0; i < count; i++) {
                        double temperature = 15.0
                                + ThreadLocalRandom.current().nextDouble() * 15.0;
                        SensorRecord record = SensorRecord.now(++seq, "SIM", temperature);
                        file.append(record); // Persiste localmente antes de propagar.
                        long publishAt = publish(pubSub, record);
                        if (i == 0) {
                            firstPublishAt = publishAt;
                        }
                        lastPublishAt = publishAt;
                    }

                    System.out.println("Rajada publicada: " + count
                            + " registos; primeira publicação(ms)=" + firstPublishAt
                            + "; última publicação(ms)=" + lastPublishAt);
                    continue;
                }

                if (parts.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }

                double temperature;
                try {
                    temperature = Double.parseDouble(parts[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + parts[1]);
                    continue;
                }

                SensorRecord record = SensorRecord.now(++seq, parts[0], temperature);
                file.append(record);
                publish(pubSub, record);
                System.out.println("Registado e publicado: " + record.toLine());
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Publicação interrompida.");
        }
    }

    /**
     * Publica através da API HTTP local do Kubo. Uma resposta HTTP de sucesso
     * confirma apenas a aceitação pelo daemon, não a persistência nas réplicas.
     */
    private static long publish(IpfsPubSub pubSub, SensorRecord record)
            throws IOException, InterruptedException {
        long publishAt = System.currentTimeMillis();
        pubSub.publish(TOPIC, record.toLine());
        return publishAt;
    }
}
