package rep02;

public final class Partitioner {
    private final int n;

    public Partitioner(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("Número de partições inválido: " + n);
        }
        this.n = n;
    }

    public int of(String sensorId) {
        if (sensorId == null || sensorId.isBlank()) {
            throw new IllegalArgumentException("Identificador de sensor inválido");
        }
        return Math.floorMod(sensorId.hashCode(), n);
    }

    public int count() { return n; }
}
