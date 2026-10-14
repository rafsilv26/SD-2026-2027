package rep01;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

/** Leader that stores each reading locally and sends one UDP multicast datagram. */
public class MulticastLeader {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        try (MulticastSocket socket = new MulticastSocket();
             Scanner scanner = new Scanner(System.in)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.setTimeToLive(1); // Limit multicast forwarding to the local network.

            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq(); // Persisted sequence prevents reuse after restart.
            System.out.println("Líder pronto. Formato: <sensor> <temperatura> | "
                    + "rajada <n> | sair");

            while (true) {
                System.out.print("> ");
                if (!scanner.hasNextLine()) { // Blocks until the user enters a command.
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

                    long firstSendAt = 0;
                    long lastSendAt = 0;
                    for (int i = 0; i < count; i++) {
                        double temperature = 15.0
                                + ThreadLocalRandom.current().nextDouble() * 15.0;
                        SensorRecord record = SensorRecord.now(++seq, "SIM", temperature);
                        file.append(record); // The leader persists before propagating.
                        long sendAt = send(socket, group, record);
                        if (i == 0) {
                            firstSendAt = sendAt;
                        }
                        lastSendAt = sendAt;
                    }

                    System.out.println("Rajada enviada: " + count
                            + " registos; primeiro envio(ms)=" + firstSendAt
                            + "; último envio(ms)=" + lastSendAt);
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
                file.append(record); // Persist first; UDP send has no delivery acknowledgement.
                send(socket, group, record);
                System.out.println("Registado e enviado: " + record.toLine());
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }

    /**
     * Converts the record to UTF-8 text and sends a single datagram to the
     * group. The timestamp is taken immediately before send; this does not
     * mean any replica has received or persisted the datagram.
     */
    private static long send(MulticastSocket socket, InetAddress group,
                             SensorRecord record) throws IOException {
        byte[] bytes = record.toLine().getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length, group, PORT);
        long sendAt = System.currentTimeMillis();
        socket.send(packet);
        return sendAt;
    }
}
