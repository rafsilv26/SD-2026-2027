import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UDPServer {
    private static final int SERVER_PORT = 6789;

    private static final List<String> deliveredMessages = new ArrayList<>();
    private static final Map<Integer, String> pendingMessages = new HashMap<>();

    public static int processDeliveredMessages(
            int nLastMessageInOrder,
            int nCurrentMessage,
            String currentMessage
    ) {
        if (nCurrentMessage == nLastMessageInOrder + 1) {
            deliveredMessages.add(currentMessage);
            nLastMessageInOrder = nCurrentMessage;

            while (pendingMessages.containsKey(nLastMessageInOrder + 1)) {
                int nextSequence = nLastMessageInOrder + 1;
                String nextMessage = pendingMessages.remove(nextSequence);

                deliveredMessages.add(nextMessage);
                nLastMessageInOrder = nextSequence;
            }
        } else {
            pendingMessages.put(nCurrentMessage, currentMessage);
        }

        return nLastMessageInOrder;
    }

    public static void main(String[] args) {
        int lastMessageInOrder = 0;

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

                int commaIndex = receivedMessage.indexOf(',');

                if (commaIndex <= 0) {
                    System.out.println("Mensagem mal formada: " + receivedMessage);
                    printState(lastMessageInOrder, Collections.emptyList());
                    continue;
                }

                int sequenceNumber;

                try {
                    sequenceNumber = Integer.parseInt(
                            receivedMessage.substring(0, commaIndex).trim()
                    );
                } catch (NumberFormatException exception) {
                    System.out.println("Número de sequência inválido: " + receivedMessage);
                    printState(lastMessageInOrder, Collections.emptyList());
                    continue;
                }

                String messageText = receivedMessage.substring(commaIndex + 1);

                int previousLastMessageInOrder = lastMessageInOrder;
                int deliveredCountBefore = deliveredMessages.size();

                lastMessageInOrder = processDeliveredMessages(
                        lastMessageInOrder,
                        sequenceNumber,
                        messageText
                );

                List<String> deliveredThisStep = new ArrayList<>(
                        deliveredMessages.subList(
                                deliveredCountBefore,
                                deliveredMessages.size()
                        )
                );

                boolean currentMessageWasDelivered =
                        lastMessageInOrder > previousLastMessageInOrder;

                byte[] responseBytes;

                if (currentMessageWasDelivered) {
                    responseBytes = receivedMessage.getBytes(StandardCharsets.UTF_8);
                } else {
                    String response = "waitingfor," + (lastMessageInOrder + 1);
                    responseBytes = response.getBytes(StandardCharsets.UTF_8);
                }

                DatagramPacket reply = new DatagramPacket(
                        responseBytes,
                        responseBytes.length,
                        request.getAddress(),
                        request.getPort()
                );

                socket.send(reply);

                printState(lastMessageInOrder, deliveredThisStep);
            }
        } catch (SocketException exception) {
            System.out.println("Socket: " + exception.getMessage());
        } catch (IOException exception) {
            System.out.println("IO: " + exception.getMessage());
        }
    }

    private static void printState(int lastMessageInOrder, List<String> deliveredThisStep) {
        System.out.println("L = " + lastMessageInOrder);
        System.out.println("Temporárias: " + pendingMessages);
        System.out.println("Entregues neste passo: " + deliveredThisStep);
        System.out.println("Lista de receção: " + deliveredMessages);
    }
}