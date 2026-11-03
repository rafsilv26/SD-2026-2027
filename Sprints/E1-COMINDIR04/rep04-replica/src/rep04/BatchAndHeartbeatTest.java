package rep04;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class BatchAndHeartbeatTest {
    private BatchAndHeartbeatTest() { }

    public static void main(String[] args) throws InterruptedException {
        SensorRecord record = new SensorRecord(1, "S01", 21.0, "1700000000000",
                1, VectorClock.parse("1,0,0", 3));
        Batch batch = new Batch("127.0.0.1", 7000, 1700000000000L, 1, 1,
                Arrays.asList(record));
        Batch decoded = Batch.decode(batch.encode(), 3);
        require(decoded.getRecords().size() == 1, "o registo do lote foi descodificado");
        require(decoded.getRecords().get(0).toLine().equals(record.toLine()),
                "o conteúdo do registo foi preservado");
        require(decoded.getLastSeq() == 1 && decoded.getHb() == 1,
                "o cabeçalho do lote foi preservado");
        rejects(() -> Batch.decode(batch.encode().replace(";1\n", ";2\n"), 3),
                "contagem de registos inconsistente");

        List<String> events = new ArrayList<>();
        HeartbeatMonitor monitor = new HeartbeatMonitor(0, 1, 1, events::add);
        monitor.onHeartbeat(10, 1);
        monitor.onHeartbeat(10, 3);
        require(events.stream().anyMatch(event -> event.startsWith("HB_PERDIDO;")),
                "o salto no número de heartbeat foi detetado");
        Thread.sleep(5);
        monitor.check();
        require(monitor.state() == HeartbeatMonitor.State.SUSPEITO,
                "o timeout assinalou suspeita");
        monitor.onHeartbeat(10, 4);
        require(monitor.state() == HeartbeatMonitor.State.ATIVO,
                "um heartbeat válido repôs o estado ativo");
        require(events.stream().anyMatch(event -> event.startsWith("RETOMA;")),
                "a retoma foi registada");
        int eventCount = events.size();
        monitor.onHeartbeat(9, 100);
        require(events.size() == eventCount, "incarnação antiga ignorada");

        System.out.println("OK: Batch e HeartbeatMonitor");
    }

    private static void rejects(Runnable action, String description) {
        try {
            action.run();
            throw new AssertionError("FALHOU: " + description);
        } catch (IllegalArgumentException expected) {
            System.out.println("OK: " + description);
        }
    }

    private static void require(boolean condition, String description) {
        if (!condition) {
            throw new AssertionError("FALHOU: " + description);
        }
    }
}
