package rep01;

import java.time.LocalDateTime;

/**
 * One sensor reading shared by the leader and replica projects.
 *
 * <p>The network representation is deliberately plain text rather than Java
 * object serialization, so any implementation that understands this format
 * can decode a record.</p>
 */
public class SensorRecord {
    private final long seq;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;

    public SensorRecord(long seq, String sensorId, double temperature, String timestamp) {
        this.seq = seq;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
    }

    /** Creates a record timestamped at whole-second precision for the leader. */
    public static SensorRecord now(long seq, String sensorId, double temperature) {
        return new SensorRecord(seq, sensorId, temperature,
                LocalDateTime.now().withNano(0).toString());
    }

    /** Uses the shared wire/file format: seq;sensor;temperature;timestamp. */
    public String toLine() {
        return seq + ";" + sensorId + ";" + temperature + ";" + timestamp;
    }

    /**
     * Reconstructs a record from the shared text format.
     * Invalid field counts or numeric values are rejected rather than silently
     * producing a partially interpreted record.
     */
    public static SensorRecord fromLine(String line) {
        String[] fields = line.trim().split(";", -1);
        if (fields.length != 4) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        return new SensorRecord(
                Long.parseLong(fields[0]),
                fields[1],
                Double.parseDouble(fields[2]),
                fields[3]);
    }

    public long getSeq() {
        return seq;
    }

    public String getSensorId() {
        return sensorId;
    }

    public double getTemperature() {
        return temperature;
    }

    public String getTimestamp() {
        return timestamp;
    }
}
