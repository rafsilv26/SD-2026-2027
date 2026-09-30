import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class UDPServer {
    private static final int SERVER_PORT = 6789;
    private static final int MAX_DATAGRAM_BYTES = 1000;
    private static final int MAX_SEQUENCE_DIGITS = 10;
    private static final int MAX_SEQUENCE_AHEAD = 10000;

    private static final List<String> deliveredMessages = new ArrayList<>();
    private static final Map<Integer, String> pendingMessages = new HashMap<>();

    /**
     * Processes the current message and any consecutive messages already pending.
     *
     * @return the number of the last message delivered in order
     */
    public static int processDeliveredMessages(
            int nLastMessageInOrder,
            int nCurrentMessage,
            String currentMessage
    ) {
        if (nCurrentMessage <= 0 || currentMessage == null
                || nCurrentMessage <= nLastMessageInOrder) {
            return nLastMessageInOrder;
        }
        if (currentMessage.getBytes(StandardCharsets.UTF_8).length > MAX_DATAGRAM_BYTES
                || containsControlCharacters(currentMessage)) {
            return nLastMessageInOrder;
        }
        if ((long) nCurrentMessage - nLastMessageInOrder > MAX_SEQUENCE_AHEAD) {
            return nLastMessageInOrder;
        }

        if (nLastMessageInOrder < Integer.MAX_VALUE
                && nCurrentMessage == nLastMessageInOrder + 1) {
            deliveredMessages.add(currentMessage);
            nLastMessageInOrder = nCurrentMessage;

            while (nLastMessageInOrder < Integer.MAX_VALUE) {
                int nextSequence = nLastMessageInOrder + 1;
                String nextMessage = pendingMessages.remove(nextSequence);
                if (nextMessage == null) {
                    break;
                }

                deliveredMessages.add(nextMessage);
                nLastMessageInOrder = nextSequence;
            }
        } else {
            pendingMessages.putIfAbsent(nCurrentMessage, currentMessage);
        }

        return nLastMessageInOrder;
    }

    public static void main(String[] args) {
        int lastMessageInOrder = 0;

        try (DatagramSocket socket = new DatagramSocket(SERVER_PORT)) {
            System.out.println("Servidor UDP a escutar no porto " + SERVER_PORT);

            while (true) {
                byte[] buffer = new byte[MAX_DATAGRAM_BYTES + 1];
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                socket.receive(request);

                String receivedMessage = null;
                int previousLastMessageInOrder = lastMessageInOrder;
                int deliveredCountBefore = deliveredMessages.size();
                String response;
                String invalidReason = null;

                if (request.getLength() > MAX_DATAGRAM_BYTES) {
                    invalidReason = "datagrama demasiado grande";
                } else {
                    try {
                        receivedMessage = StandardCharsets.UTF_8.newDecoder()
                                .onMalformedInput(CodingErrorAction.REPORT)
                                .onUnmappableCharacter(CodingErrorAction.REPORT)
                                .decode(ByteBuffer.wrap(
                                        request.getData(),
                                        request.getOffset(),
                                        request.getLength()
                                ))
                                .toString();

                        int commaIndex = receivedMessage.indexOf(',');
                        if (commaIndex <= 0) {
                            invalidReason = "formato esperado: <N>,<mensagem>";
                        } else {
                            String sequenceText = receivedMessage.substring(0, commaIndex);
                            if (sequenceText.length() > MAX_SEQUENCE_DIGITS
                                    || !containsOnlyAsciiDigits(sequenceText)) {
                                invalidReason = "número de sequência inválido";
                            } else {
                                int sequenceNumber;
                                try {
                                    sequenceNumber = Integer.parseInt(sequenceText);
                                } catch (NumberFormatException exception) {
                                    sequenceNumber = -1;
                                }

                                if (sequenceNumber <= 0) {
                                    invalidReason = "número de sequência fora do limite";
                                } else {
                                    String messageText = receivedMessage.substring(commaIndex + 1);
                                    if (containsControlCharacters(messageText)) {
                                        invalidReason = "a mensagem contém caracteres de controlo";
                                    } else if ((long) sequenceNumber - lastMessageInOrder
                                            > MAX_SEQUENCE_AHEAD) {
                                        invalidReason = "número demasiado distante da sequência atual";
                                    } else {
                                        lastMessageInOrder = processDeliveredMessages(
                                                lastMessageInOrder,
                                                sequenceNumber,
                                                messageText
                                        );
                                    }
                                }
                            }
                        }
                    } catch (CharacterCodingException exception) {
                        invalidReason = "datagrama não contém texto UTF-8 válido";
                    }
                }

                if (invalidReason != null) {
                    response = "error,invalid_datagram";
                } else if (lastMessageInOrder > previousLastMessageInOrder) {
                    response = receivedMessage;
                } else {
                    response = "waitingfor," + ((long) lastMessageInOrder + 1);
                }

                List<String> deliveredThisStep = new ArrayList<>(
                        deliveredMessages.subList(
                                deliveredCountBefore,
                                deliveredMessages.size()
                        )
                );

                if (invalidReason != null) {
                    System.out.println("Datagrama inválido (" + invalidReason + "): "
                            + (receivedMessage == null ? "<não descodificável>" : receivedMessage));
                } else {
                    System.out.println("Recebido: " + receivedMessage);
                }
                System.out.println("Resposta: " + response);
                printState(lastMessageInOrder, deliveredThisStep);

                byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
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

    private static boolean containsOnlyAsciiDigits(String value) {
        if (value.isEmpty()) {
            return false;
        }
        for (int index = 0; index < value.length(); index++) {
            if (value.charAt(index) < '0' || value.charAt(index) > '9') {
                return false;
            }
        }
        return true;
    }

    private static boolean containsControlCharacters(String value) {
        for (int index = 0; index < value.length();) {
            int codePoint = value.codePointAt(index);
            if (Character.isISOControl(codePoint)) {
                return true;
            }
            index += Character.charCount(codePoint);
        }
        return false;
    }

    private static void printState(int lastMessageInOrder, List<String> deliveredThisStep) {
        System.out.println("L = " + lastMessageInOrder);
        System.out.println("Temporárias: " + new TreeMap<>(pendingMessages));
        System.out.println("Entregues neste passo: " + deliveredThisStep);
        System.out.println("Lista de receção: " + deliveredMessages);
        System.out.println();
    }
}
