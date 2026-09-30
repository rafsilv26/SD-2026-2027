import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

public class UDPClient {
    private static final int SERVER_PORT = 6789;
    private static final int MAX_DATAGRAM_BYTES = 1000;
    private static final int MAX_SEQUENCE_DIGITS = 10;
    private static final String EXIT_COMMAND = "sair";

    public static void main(String[] args) {
        String hostName = args.length > 0 ? args[0] : "localhost";
        int serverPort = SERVER_PORT;
        if (args.length > 1) {
            try {
                serverPort = Integer.parseInt(args[1]);
                if (serverPort < 1 || serverPort > 65535) {
                    System.out.println("O porto tem de estar entre 1 e 65535.");
                    return;
                }
            } catch (NumberFormatException exception) {
                System.out.println("Porto inválido: " + args[1]);
                return;
            }
        }

        try (BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
             DatagramSocket socket = new DatagramSocket()) {
            InetAddress serverAddress = InetAddress.getByName(hostName);
            boolean manualNumbering = chooseNumberingMode(console);
            int nextSequence = 1;

            while (true) {
                System.out.print("Mensagem (\"" + EXIT_COMMAND + "\" para terminar): ");
                ConsoleLine input = readBoundedLine(console, MAX_DATAGRAM_BYTES);
                if (input == null) {
                    break;
                }
                if (input.tooLong) {
                    System.out.println("Mensagem demasiado longa; limite: "
                            + MAX_DATAGRAM_BYTES + " caracteres.");
                    continue;
                }

                String messageText = input.value;
                if (messageText == null || EXIT_COMMAND.equalsIgnoreCase(messageText.trim())) {
                    break;
                }
                if (containsControlCharacters(messageText)) {
                    System.out.println("A mensagem não pode conter caracteres de controlo.");
                    continue;
                }

                int sequenceNumber;
                if (manualNumbering) {
                    sequenceNumber = readSequenceNumber(console);
                    if (sequenceNumber < 1) {
                        continue;
                    }
                } else {
                    sequenceNumber = nextSequence;
                }

                String message = sequenceNumber + "," + messageText;
                byte[] requestBytes = message.getBytes(StandardCharsets.UTF_8);
                if (requestBytes.length > MAX_DATAGRAM_BYTES) {
                    System.out.println("Datagrama demasiado grande em UTF-8; limite: "
                            + MAX_DATAGRAM_BYTES + " bytes.");
                    continue;
                }
                DatagramPacket request = new DatagramPacket(
                        requestBytes,
                        requestBytes.length,
                        serverAddress,
                        serverPort
                );
                socket.send(request);

                byte[] responseBuffer = new byte[MAX_DATAGRAM_BYTES];
                DatagramPacket reply = new DatagramPacket(responseBuffer, responseBuffer.length);
                socket.receive(reply);

                String response = new String(
                        reply.getData(),
                        reply.getOffset(),
                        reply.getLength(),
                        StandardCharsets.UTF_8
                );
                if (response.startsWith("waitingfor,")) {
                    System.out.println("Servidor pede a mensagem em falta: " + response);
                } else if (response.startsWith("error,")) {
                    System.out.println("Servidor rejeitou o datagrama: " + response);
                } else {
                    System.out.println("Echo: " + response);
                }

                if (!manualNumbering) {
                    if (nextSequence == Integer.MAX_VALUE) {
                        System.out.println("Limite da numeração automática atingido.");
                        break;
                    }
                    nextSequence++;
                }
            }
        } catch (IOException exception) {
            System.out.println("IO: " + exception.getMessage());
        }
    }

    private static boolean chooseNumberingMode(BufferedReader console) throws IOException {
        while (true) {
            System.out.print("Modo de numeração: automático (a) ou manual (m)? ");
            ConsoleLine input = readBoundedLine(console, 16);
            if (input == null) {
                return false;
            }
            if (input.tooLong) {
                System.out.println("Opção demasiado longa.");
                continue;
            }
            String mode = input.value;
            if ("a".equalsIgnoreCase(mode.trim())) {
                return false;
            }
            if ("m".equalsIgnoreCase(mode.trim())) {
                return true;
            }
            System.out.println("Escolhe 'a' ou 'm'.");
        }
    }

    private static int readSequenceNumber(BufferedReader console) throws IOException {
        while (true) {
            System.out.print("Número de sequência: ");
            ConsoleLine input = readBoundedLine(console, MAX_SEQUENCE_DIGITS);
            if (input == null) {
                return -1;
            }
            if (input.tooLong) {
                System.out.println("Número demasiado longo.");
                continue;
            }
            String value = input.value;
            if (!containsOnlyAsciiDigits(value)) {
                System.out.println("Indica um número inteiro positivo.");
                continue;
            }
            try {
                int sequenceNumber = Integer.parseInt(value);
                if (sequenceNumber > 0) {
                    return sequenceNumber;
                }
            } catch (NumberFormatException ignored) {
                // Prompt again for malformed console input.
            }
            System.out.println("Indica um número inteiro positivo.");
        }
    }

    private static ConsoleLine readBoundedLine(BufferedReader console, int maxCharacters)
            throws IOException {
        StringBuilder value = new StringBuilder(Math.min(maxCharacters, 128));
        boolean tooLong = false;
        boolean readAnyCharacter = false;

        while (true) {
            int character = console.read();
            if (character == -1 || character == '\n') {
                if (!readAnyCharacter && character == -1) {
                    return null;
                }
                return new ConsoleLine(value.toString(), tooLong);
            }
            if (character == '\r') {
                continue;
            }

            readAnyCharacter = true;
            if (value.length() < maxCharacters) {
                value.append((char) character);
            } else {
                tooLong = true;
            }
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
            char current = value.charAt(index);
            if (Character.isHighSurrogate(current)) {
                if (index + 1 >= value.length()
                        || !Character.isLowSurrogate(value.charAt(index + 1))) {
                    return true;
                }
            } else if (Character.isLowSurrogate(current)) {
                return true;
            }

            int codePoint = value.codePointAt(index);
            if (Character.isISOControl(codePoint)) {
                return true;
            }
            index += Character.charCount(codePoint);
        }
        return false;
    }

    private static final class ConsoleLine {
        private final String value;
        private final boolean tooLong;

        private ConsoleLine(String value, boolean tooLong) {
            this.value = value;
            this.tooLong = tooLong;
        }
    }
}
