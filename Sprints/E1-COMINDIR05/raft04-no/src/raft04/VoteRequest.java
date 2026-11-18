package raft04;

/** Pedido de voto com a posição do último registo do candidato. */
public final class VoteRequest {
    private final long term;
    private final int candidate;
    private final long lastSeq;
    private final long lastTerm;

    public VoteRequest(long term, int candidate, long lastSeq, long lastTerm) {
        if (term < 0 || candidate < 1 || lastSeq < 0 || lastTerm < 0) {
            throw new IllegalArgumentException("PEDIDO_VOTO inválido");
        }
        this.term = term;
        this.candidate = candidate;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
    }

    public String encode() {
        return "PEDIDO_VOTO;" + term + ";" + candidate + ";" + lastSeq + ";" + lastTerm;
    }

    public static VoteRequest decode(String message) {
        String[] f = message.trim().split(";", -1);
        if (f.length != 5 || !"PEDIDO_VOTO".equals(f[0])) {
            throw new IllegalArgumentException("PEDIDO_VOTO mal formado");
        }
        return new VoteRequest(Long.parseLong(f[1]), Integer.parseInt(f[2]),
                Long.parseLong(f[3]), Long.parseLong(f[4]));
    }

    public long term() { return term; }
    public int candidate() { return candidate; }
    public long lastSeq() { return lastSeq; }
    public long lastTerm() { return lastTerm; }
}
