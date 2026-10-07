package tcp01;

import java.io.*;
import java.net.*;

public class TCPServer {
    public static void main(String[] args) {
        try {
            int serverPort = 7896;
            ServerSocket listenSocket = new ServerSocket(serverPort);
            while (true) {
                Socket clientSocket = listenSocket.accept();    // BLOQUEIA: espera que um cliente se ligue
                Connection c = new Connection(clientSocket);    // processa o pedido noutra thread
            }
        } catch (IOException e) {
            System.out.println("Listen: " + e.getMessage());
        }
    }
}
