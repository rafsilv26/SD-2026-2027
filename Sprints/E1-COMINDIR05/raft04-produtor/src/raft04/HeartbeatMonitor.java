package raft04;

import java.util.function.Consumer;

/** Deteção de silêncio e mudanças de líder a partir dos heartbeats LOTE. */
public final class HeartbeatMonitor {
    public enum State { DESCONHECIDO, ATIVO, SUSPEITO }

    private final int partition;
    private final long timeoutNanos;
    private final Consumer<String> log;
    private State state = State.DESCONHECIDO;
    private long lastNanos = System.nanoTime();
    private long term = -1;
    private int leader;
    private long hb;

    public HeartbeatMonitor(int partition, long periodMs, int factor, Consumer<String> log) {
        if (partition < 0 || periodMs <= 0 || factor < 1 || log == null) {
            throw new IllegalArgumentException("Parâmetros do monitor inválidos");
        }
        this.partition = partition;
        this.timeoutNanos = Math.multiplyExact(Math.multiplyExact(periodMs, factor), 1_000_000L);
        this.log = log;
    }

    public synchronized void onHeartbeat(long newTerm, int newLeader, long newHb) {
        if (newTerm < 0 || newLeader < 1 || newHb < 1) {
            throw new IllegalArgumentException("Heartbeat inválido");
        }
        if (newTerm < term || (newTerm == term && newHb <= hb)) return;
        if (newTerm > term) {
            log.accept("NOVO_LIDER;p" + partition + ";termo=" + newTerm
                    + ";lider=n" + newLeader);
        } else if (term >= 0 && newHb > hb + 1) {
            log.accept("HB_PERDIDO;p" + partition + ";esperado=" + (hb + 1)
                    + ";recebido=" + newHb);
        }
        if (state == State.SUSPEITO) {
            log.accept("RETOMA;p" + partition + ";termo=" + newTerm + ";hb=" + newHb);
        }
        term = newTerm;
        leader = newLeader;
        hb = newHb;
        lastNanos = System.nanoTime();
        state = State.ATIVO;
    }

    public synchronized void check() {
        long elapsed = System.nanoTime() - lastNanos;
        if (state != State.SUSPEITO && elapsed > timeoutNanos) {
            state = State.SUSPEITO;
            log.accept("SUSPEITO;p" + partition + ";termo=" + term + ";hb=" + hb
                    + ";silencio_ms=" + elapsed / 1_000_000L);
        }
    }

    public synchronized State state() { return state; }
    public synchronized long term() { return term; }
    public synchronized int leader() { return leader; }
}
