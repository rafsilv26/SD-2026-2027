package raft04;

/** Resposta explícita, positiva ou negativa, a um pedido de voto. */
public final class VoteReply {
    private final long term;
    private final int voter;
    private final int candidate;
    private final boolean granted;

    public VoteReply(long term, int voter, int candidate, boolean granted) {
        if (term < 0 || voter < 1 || candidate < 1) {
            throw new IllegalArgumentException("VOTO inválido");
        }
        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }

    public String encode() {
        return "VOTO;" + term + ";" + voter + ";" + candidate + ";" + (granted ? "S" : "N");
    }

    public static VoteReply decode(String message) {
        String[] f = message.trim().split(";", -1);
        if (f.length != 5 || !"VOTO".equals(f[0])
                || (!"S".equals(f[4]) && !"N".equals(f[4]))) {
            throw new IllegalArgumentException("VOTO mal formado");
        }
        return new VoteReply(Long.parseLong(f[1]), Integer.parseInt(f[2]),
                Integer.parseInt(f[3]), "S".equals(f[4]));
    }

    public long term() { return term; }
    public int voter() { return voter; }
    public int candidate() { return candidate; }
    public boolean granted() { return granted; }
}
