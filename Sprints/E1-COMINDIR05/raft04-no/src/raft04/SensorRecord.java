package raft04;

/** Registo imutável; o termo identifica a época em que o líder lhe atribuiu seq. */
public final class SensorRecord {
    private final long seq;
    private final long term;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;

    public SensorRecord(long seq, long term, String sensorId, double temperature,
                        String timestamp, long lamport, VectorClock vector) {
        if (seq < 1 || term < 0 || sensorId == null || sensorId.isBlank()
                || !Double.isFinite(temperature) || timestamp == null
                || lamport < 0 || vector == null) {
            throw new IllegalArgumentException("Registo inválido");
        }
        this.seq = seq;
        this.term = term;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector.copy();
    }

    /** Mantém a API usada pelo produtor e pelo processador, que não criam registos persistidos. */
    public SensorRecord(long seq, String sensorId, double temperature, String timestamp,
                        long lamport, VectorClock vector) {
        this(seq, 0, sensorId, temperature, timestamp, lamport, vector);
    }

    public static SensorRecord now(long seq, long term, String sensorId, double temperature,
                                   long lamport, VectorClock vector) {
        return new SensorRecord(seq, term, sensorId, temperature,
                Long.toString(System.currentTimeMillis()), lamport, vector);
    }

    public static SensorRecord now(long seq, String sensorId, double temperature,
                                   long lamport, VectorClock vector) {
        return now(seq, 0, sensorId, temperature, lamport, vector);
    }

    /** Formato persistido: seq;termo;sensor;temperatura;instante;L;V. */
    public String toLine() {
        return seq + ";" + term + ";" + sensorId + ";" + temperature + ";"
                + timestamp + ";" + lamport + ";" + vector;
    }

    public static SensorRecord fromLine(String line) {
        String[] fields = line.trim().split(";", -1);
        if (fields.length != 7) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        int vectorSize = fields[6].split(",", -1).length;
        return new SensorRecord(Long.parseLong(fields[0]), Long.parseLong(fields[1]),
                fields[2], Double.parseDouble(fields[3]), fields[4],
                Long.parseLong(fields[5]), VectorClock.parse(fields[6], vectorSize));
    }

    public long getSeq() { return seq; }
    public long getTerm() { return term; }
    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public String getTimestamp() { return timestamp; }
    public long getLamport() { return lamport; }
    public VectorClock getVector() { return vector.copy(); }
}
