import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;

public class UDPServer {
    private static final int SERVER_PORT = 6789;

    public static void main(String[] args) {
        int lastAcceptedSequence = 0;

        try (DatagramSocket socket = new DatagramSocket(SERVER_PORT)) {
            System.out.println("Servidor UDP a escutar no porto " + SERVER_PORT);

            while (true) {
                byte[] buffer = new byte[1024];
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);

                socket.receive(request);

                String receivedMessage = new String(
                        request.getData(),
                        request.getOffset(),
                        request.getLength(),
                        StandardCharsets.UTF_8
                );

                System.out.println(
                        "Recebido de " + request.getAddress().getHostAddress()
                                + ":" + request.getPort()
                                + " -> " + receivedMessage
                );

                int commaIndex = receivedMessage.indexOf(',');

                if (commaIndex <= 0) {
                    System.out.println("Mensagem mal formada: falta número ou vírgula.");
                    continue;
                }

                int sequenceNumber;

                try {
                    sequenceNumber = Integer.parseInt(
                            receivedMessage.substring(0, commaIndex).trim()
                    );
                } catch (NumberFormatException exception) {
                    System.out.println("Mensagem mal formada: número de sequência inválido.");
                    continue;
                }

                byte[] responseBytes;

                if (sequenceNumber == lastAcceptedSequence + 1) {
                    lastAcceptedSequence = sequenceNumber;
                    responseBytes = receivedMessage.getBytes(StandardCharsets.UTF_8);

                    System.out.println("Aceite. L = " + lastAcceptedSequence);
                } else {
                    String response = "waitingfor," + (lastAcceptedSequence + 1);
                    responseBytes = response.getBytes(StandardCharsets.UTF_8);

                    System.out.println(
                            "Fora de ordem. Esperava "
                                    + (lastAcceptedSequence + 1)
                                    + ". L mantém-se em " + lastAcceptedSequence
                    );
                }

                DatagramPacket reply = new DatagramPacket(
                        responseBytes,
                        responseBytes.length,
                        request.getAddress(),
                        request.getPort()
                );

                socket.send(reply);
            }
        } catch (SocketException exception) {
            System.out.println("Socket: " + exception.getMessage());
        } catch (IOException exception) {
            System.out.println("IO: " + exception.getMessage());
        }
    }
}