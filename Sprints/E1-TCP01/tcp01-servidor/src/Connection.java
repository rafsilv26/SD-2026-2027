package tcp01;

import java.io.*;
import java.net.*;

public class Connection extends Thread {
    ObjectInputStream in;      // entrada: objetos (Person)
    DataOutputStream out;      // saída: texto (localidade)
    Socket clientSocket;

    public Connection(Socket aClientSocket) {
        try {
            clientSocket = aClientSocket;
            // Só o sentido cliente->servidor transporta objetos, por isso não há
            // risco de bloqueio mútuo ao criar o ObjectInputStream.
            // BLOQUEIA: espera pelo cabeçalho que o ObjectOutputStream do cliente escreve.
            in = new ObjectInputStream(clientSocket.getInputStream());
            out = new DataOutputStream(clientSocket.getOutputStream());
            this.start();                                       // executa run() numa thread separada
        } catch (IOException e) {
            System.out.println("Connection: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            Object obj = in.readObject();                       // BLOQUEIA: espera um objeto completo
            if (obj instanceof Person) {
                Person p = (Person) obj;                        // cast para a classe esperada
                String locality = (p.getPlace() != null) ? p.getPlace().getLocality() : "(sem local)";
                System.out.println("Recebido: " + p);
                out.writeUTF(locality);                         // devolve a localidade como texto
            } else {
                out.writeUTF("Erro: objeto recebido não é uma Person ("
                        + (obj == null ? "null" : obj.getClass().getName()) + ")");
            }
        } catch (ClassNotFoundException e) {
            System.out.println("ClassNotFound: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e);
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                /* falha ao fechar */
            }
        }
    }
}
