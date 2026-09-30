import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class UDPClient {
    private static final String SERVER_HOST = "localhost";
    private static final int SERVER_PORT = 6789;

    public static void main(String[] args) {
        int contador = 0;

        try (
                DatagramSocket socket = new DatagramSocket();
                Scanner scanner = new Scanner(System.in)
        ) {
            InetAddress serverAddress = InetAddress.getByName(SERVER_HOST);
            socket.setSoTimeout(5000);

            System.out.println("Escreve uma mensagem ou 'sair' para terminar.");
            System.out.println("Para forçar um número: N,mensagem");

            while (true) {
                System.out.print("> ");
                String message = scanner.nextLine();

                if (message.equalsIgnoreCase("sair")) {
                    break;
                }

                String aEnviar;
                int numeroEnviado;

                if (message.matches("\\d+,.*")) {
                    aEnviar = message;
                    int commaIndex = message.indexOf(',');
                    numeroEnviado = Integer.parseInt(message.substring(0, commaIndex));
                } else {
                    contador++;
                    numeroEnviado = contador;
                    aEnviar = numeroEnviado + "," + message;
                }

                byte[] messageBytes = aEnviar.getBytes(StandardCharsets.UTF_8);

                DatagramPacket request = new DatagramPacket(
                        messageBytes,
                        messageBytes.length,
                        serverAddress,
                        SERVER_PORT
                );

                socket.send(request);

                try {
                    byte[] buffer = new byte[1024];
                    DatagramPacket reply = new DatagramPacket(buffer, buffer.length);

                    socket.receive(reply);

                    String response = new String(
                            reply.getData(),
                            reply.getOffset(),
                            reply.getLength(),
                            StandardCharsets.UTF_8
                    );

                    if (response.startsWith("waitingfor,")) {
                        String expectedText = response.substring("waitingfor,".length());

                        try {
                            int expected = Integer.parseInt(expectedText);
                            contador = expected - 1;
                            System.out.println(
                                    "O servidor espera a mensagem "
                                            + expected
                                            + ". O contador foi ajustado."
                            );
                        } catch (NumberFormatException exception) {
                            System.out.println("Resposta waitingfor inválida: " + response);
                        }
                    } else {
                        contador = numeroEnviado;
                        System.out.println("Echo: " + response);
                    }
                } catch (SocketTimeoutException exception) {
                    System.out.println("Sem resposta do servidor após 5 segundos.");
                }
            }
        } catch (SocketException exception) {
            System.out.println("Socket: " + exception.getMessage());
        } catch (IOException exception) {
            System.out.println("IO: " + exception.getMessage());
        }
    }
}