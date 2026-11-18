package rep03;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PartitionLeader {
    private static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";

    private final int partition;
    private final int partitions;
    private final Partitioner partitioner;
    private final String advertisedHost;
    private final IpfsPubSub pubSub;
    private final RecordFile file;
    private final Object stateLock = new Object();
    // estado protegido por stateLock: seq, L e V mudam sempre juntos
    private long lastSeq;
    private long lamport;
    private VectorClock vector;

    private PartitionLeader(int partition, int partitions, String advertisedHost, String api)
            throws IOException {
        this.partition = partition;
        this.partitions = partitions;
        this.partitioner = new Partitioner(partitions);
        this.advertisedHost = advertisedHost;
        this.pubSub = new IpfsPubSub(api);
        this.file = new RecordFile("lider-p" + partition + ".txt");
        // arrancar com seq, L e V do último registo gravado (ou a zeros se não houver)
        SensorRecord last = file.last();
        if (last == null) {
            this.lastSeq = 0;
            this.lamport = 0;
            this.vector = new VectorClock(partitions);
        } else {
            this.lastSeq = last.getSeq();
            this.lamport = last.getLamport();
            this.vector = last.getVector();
            if (vector.size() != partitions) {
                throw new IllegalArgumentException("o ficheiro " + "lider-p" + partition
                        + ".txt tem vetores com " + vector.size() + " posições, mas N="
                        + partitions);
            }
            if (vector.get(partition) != lastSeq) {
                throw new IllegalArgumentException("o último registo do ficheiro viola V["
                        + partition + "] = seq");
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 4) {
            System.err.println("Uso: PartitionLeader <k> <N> [endereço] [api]");
            return;
        }
        try {
            int partition = Integer.parseInt(args[0]);
            int partitions = Integer.parseInt(args[1]);
            String host = args.length > 2 ? args[2] : "127.0.0.1";
            String api = args.length > 3 ? args[3] : DEFAULT_API;
            if (partitions < 1 || partition < 0 || partition >= partitions) {
                throw new IllegalArgumentException("partição fora do intervalo [0, N)");
            }
            int port = Wire.PORTO_BASE + partition;
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("porto de recuperação inválido: " + port);
            }
            new PartitionLeader(partition, partitions, host, api).run(port);
        } catch (NumberFormatException e) {
            System.err.println("k e N têm de ser inteiros.");
        } catch (IllegalArgumentException | IOException e) {
            System.err.println("Não foi possível iniciar o líder: " + e.getMessage());
        }
    }

    //Cria as duas threads, de entrada(subscrição) + principal(pedido TCP)
    private void run(int port) throws IOException {
        try (ServerSocket server = new ServerSocket(port)) {
            Thread input = new Thread(this::subscribe, "rep03-leader-input-p" + partition);
            input.start();
            System.out.println("Líder p" + partition + " pronto; tópico="
                    + Wire.topicIn(partition) + "; recuperação="
                    + advertisedHost + ":" + port
                    + "; seq=" + lastSeq + "; L=" + lamport + "; V=" + vector);
            while (!Thread.currentThread().isInterrupted()) {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(Wire.T_TCP_MS);
                    serve(socket);
                } catch (IOException e) {
                    System.err.println("Recuperação interrompida: " + e.getMessage());
                }
            }
        }
    }

    private void subscribe() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                pubSub.subscribe(Wire.topicIn(partition), this::onInput);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                System.err.println("Subscrição p" + partition + " falhou: " + e.getMessage());
                pauseBeforeRetry();
            } catch (RuntimeException e) {
                System.err.println("Erro na subscrição p" + partition + ": " + e.getMessage());
                pauseBeforeRetry();
            }
        }
    }

    private void onInput(String message) {
        SensorRecord record;
        try {
            if (!"IN".equals(Wire.type(message))) {
                throw new IllegalArgumentException("tipo inesperado");
            }
            // IN;sensor;temp;L;V
            String[] fields = Wire.body(message).split(";", -1);
            if (fields.length != 4 || fields[0].isBlank()) {
                throw new IllegalArgumentException("IN mal formado");
            }
            double temperature = Double.parseDouble(fields[1]);
            if (!Double.isFinite(temperature)) {
                throw new IllegalArgumentException("temperatura não finita");
            }
            long inLamport = Long.parseLong(fields[2]);
            if (inLamport < 0) {
                throw new IllegalArgumentException("L negativo");
            }
            // recusa vetores com dimensão diferente de N (ex.: produtor com N = 4)
            VectorClock inVector = VectorClock.parse(fields[3], partitions);
            int actualPartition = partitioner.of(fields[0]);
            if (actualPartition != partition) {
                System.out.println("PARTIÇÃO ERRADA: " + fields[0]
                        + " pertence a p" + actualPartition);
                return;
            }
            synchronized (stateLock) {
                // calcula o estado novo em variáveis locais; só passa a valer
                // depois do append, para um erro de escrita não deixar L/V avançados
                long newLamport = Math.max(lamport, inLamport) + 1;   // regra de Lamport
                VectorClock newVector = vector.copy();
                newVector.merge(inVector);                            // máximo posição a posição
                newVector.tick(partition);                            // evento deste líder
                long seq = lastSeq + 1;
                if (newVector.get(partition) != seq) {
                    throw new IllegalStateException("V[" + partition + "]="
                            + newVector.get(partition) + " diferente de seq=" + seq);
                }
                record = SensorRecord.now(seq, fields[0], temperature, newLamport, newVector);
                file.append(record);
                lastSeq = seq;
                lamport = newLamport;
                vector = newVector;
            }
            System.out.println("EVENTO: p" + partition + " seq=" + record.getSeq()
                    + " L=" + record.getLamport() + " V=(" + record.getVector() + ") "
                    + record.getSensorId() + " " + record.getTemperature());
        } catch (IllegalStateException e) {
            System.err.println("INVARIANTE VIOLADO em p" + partition + ": " + e.getMessage());
            return;
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + message + " (" + e.getMessage() + ")");
            return;
        } catch (IOException e) {
            System.err.println("Não foi possível persistir IN em p" + partition + ": "
                    + e.getMessage());
            return;
        }

        try {
            pubSub.publish(Wire.topicData(partition),
                    Wire.reg(advertisedHost, Wire.PORTO_BASE + partition, record));
            System.out.println("Publicado REG p" + partition + ": " + record.toLine());
        } catch (IOException e) {
            System.err.println("Publicação p" + partition + " falhou: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Publicação p" + partition + " interrompida.");
        }
    }

    private void serve(Socket socket) throws IOException {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(
                        socket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(
                     socket.getOutputStream(), StandardCharsets.UTF_8), true)) {
            String request = in.readLine();
            String[] fields = request == null ? new String[0] : request.trim().split("\\s+");
            if (fields.length != 3 || !"PEDIDO".equals(fields[0])) {
                sendError(out, "pedido mal formado");
                return;
            }
            long from;
            long requestedTo;
            try {
                from = Long.parseLong(fields[1]);
                requestedTo = Long.parseLong(fields[2]);
            } catch (NumberFormatException e) {
                sendError(out, "sequência inválida");
                return;
            }
            if (from < 1 || requestedTo < 1 || from > requestedTo) {
                sendError(out, "intervalo inválido");
                return;
            }

            List<SensorRecord> records;
            long to;
            synchronized (stateLock) {
                to = Math.min(requestedTo, lastSeq);
                records = file.range(from, to);
            }
            for (SensorRecord record : records) {
                out.println(record.toLine());
            }
            out.println("FIM");
            if (out.checkError()) {
                throw new IOException("falha ao enviar resposta TCP");
            }
            System.out.println("RECUPERAÇÃO: " + from + ".." + to
                    + " para " + socket.getRemoteSocketAddress()
                    + " (" + records.size() + " registos)");
        }
    }

    private static void sendError(PrintWriter out, String reason) throws IOException {
        out.println("ERRO " + reason);
        if (out.checkError()) {
            throw new IOException("falha ao enviar erro TCP");
        }
    }

    private static void pauseBeforeRetry() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
