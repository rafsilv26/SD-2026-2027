package tcp01;

import java.io.*;
import java.net.*;

public class TCPClient {
    public static void main(String[] args) {
        Socket s = null;
        try {
            int serverPort = 7896;                              // porto do servidor
            s = new Socket("localhost", serverPort);            // BLOQUEIA até a ligação ser aceite (ou falha)
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream()); // escreve o cabeçalho
            DataInputStream in = new DataInputStream(s.getInputStream());

            Place place = new Place("3500-001", "Viseu");
            Person person = new Person("Maria Silva", place, 2001);
            out.writeObject(person);                            // envia a Person (e o Place com ela)
            out.flush();

            String data = in.readUTF();                         // BLOQUEIA à espera da resposta
            System.out.println("Received: " + data);
        } catch (UnknownHostException e) {
            System.out.println("Sock: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e);
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    System.out.println("close: " + e.getMessage());
                }
            }
        }
    }
}
