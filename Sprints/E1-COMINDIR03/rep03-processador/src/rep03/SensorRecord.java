package rep03;

import java.time.LocalDateTime;

public class SensorRecord {
    private final long seq;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;          // L: relógio de Lamport do líder no evento
    private final VectorClock vector;    // V: relógio vetorial do líder no evento

    public SensorRecord(long seq, String sensorId, double temperature, String timestamp,
                        long lamport, VectorClock vector) {
        this.seq = seq;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector.copy();     // o registo guarda a sua própria fotografia
    }

    public static SensorRecord now(long seq, String sensorId, double temperature,
                                   long lamport, VectorClock vector) {
        return new SensorRecord(seq, sensorId, temperature,
                LocalDateTime.now().withNano(0).toString(), lamport, vector);
    }

    // seq;sensor;temp;instante;L;V  (é aqui que os carimbos são serializados)
    public String toLine() {
        return seq + ";" + sensorId + ";" + temperature + ";" + timestamp
                + ";" + lamport + ";" + vector;
    }

    public static SensorRecord fromLine(String line) {
        String[] fields = line.trim().split(";", -1);
        if (fields.length != 6) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        long lamport = Long.parseLong(fields[4]);
        if (lamport < 0) {
            throw new IllegalArgumentException("L negativo: " + line);
        }
        int n = fields[5].split(",", -1).length;
        VectorClock vector = VectorClock.parse(fields[5], n);
        return new SensorRecord(Long.parseLong(fields[0]), fields[1],
                Double.parseDouble(fields[2]), fields[3], lamport, vector);
    }

    public long getSeq() { return seq; }
    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public String getTimestamp() { return timestamp; }
    public long getLamport() { return lamport; }
    public VectorClock getVector() { return vector.copy(); }
}
