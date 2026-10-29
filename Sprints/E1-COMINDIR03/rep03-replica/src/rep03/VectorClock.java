package rep03;

public class VectorClock {

    public enum Order { BEFORE, AFTER, EQUAL, CONCURRENT }

    // um contador por processo: v[i] = quantos eventos do processo i este vetor conhece
    private final long[] v;

    // cria um vetor com n posições, todas a 0
    public VectorClock(int n) {
        v = new long[n];
    }

    // construtor privado: usado pelo copy() e pelo parse()
    private VectorClock(long[] valores) {
        v = valores;
    }

    // soma 1 à posição k (um evento novo do processo k)
    public void tick(int k) {
        v[k]++;
    }

    // devolve o valor da posição k
    public long get(int k) {
        return v[k];
    }

    // número de posições (N)
    public int size() {
        return v.length;
    }

    // fica com o maior de cada posição; recusa vetores de tamanho diferente
    public void merge(VectorClock outro) {
        verificaTamanho(outro);
        for (int i = 0; i < v.length; i++) {
            v[i] = Math.max(v[i], outro.v[i]);
        }
    }

    // devolve um VectorClock novo, com um array novo, com os mesmos valores
    public VectorClock copy() {
        return new VectorClock(v.clone());
    }

    // compara este vetor com outro: BEFORE, AFTER, EQUAL ou CONCURRENT
    public Order compare(VectorClock outro) {
        verificaTamanho(outro);

        boolean algumaMenor = false;
        boolean algumaMaior = false;

        for (int i = 0; i < v.length; i++) {
            if (v[i] < outro.v[i]) algumaMenor = true;
            else if (v[i] > outro.v[i]) algumaMaior = true;
        }

        if (algumaMenor && algumaMaior) return Order.CONCURRENT;
        if (algumaMenor) return Order.BEFORE;
        if (algumaMaior) return Order.AFTER;
        return Order.EQUAL;
    }

    // transforma "1,2,3" num VectorClock; recusa se não tiver exatamente n posições
    public static VectorClock parse(String texto, int n) {
        String[] partes = texto.trim().split(",");
        if (partes.length != n) {
            throw new IllegalArgumentException(
                "VectorClock com " + partes.length + " posições, esperava " + n + ": \"" + texto + "\"");
        }
        long[] valores = new long[n];
        for (int i = 0; i < n; i++) {
            valores[i] = Long.parseLong(partes[i].trim());
        }
        return new VectorClock(valores);
    }

    // formato de texto: "0,0,0" (o mesmo que o parse lê)
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < v.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(v[i]);
        }
        return sb.toString();
    }

    private void verificaTamanho(VectorClock outro) {
        if (outro.v.length != v.length) {
            throw new IllegalArgumentException(
                "VectorClock com tamanhos diferentes: " + v.length + " vs " + outro.v.length);
        }
    }
}