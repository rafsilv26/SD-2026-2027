package rep01;

import java.time.LocalDateTime;

/**
 * Registo de leitura partilhado pelos projetos do líder e das réplicas.
 *
 * <p>A rede transporta texto simples, não objetos Java serializados. Qualquer
 * programa que conheça este formato consegue interpretar o registo.</p>
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

    /** Cria um registo com o instante atual, arredondado ao segundo. */
    public static SensorRecord now(long seq, String sensorId, double temperature) {
        return new SensorRecord(seq, sensorId, temperature,
                LocalDateTime.now().withNano(0).toString());
    }

    /** Produz o formato comum usado na rede e nos ficheiros. */
    public String toLine() {
        return seq + ";" + sensorId + ";" + temperature + ";" + timestamp;
    }

    /**
     * Reconstrói um registo a partir do formato textual comum.
     * Rejeita campos em falta ou valores numéricos inválidos, em vez de criar
     * silenciosamente um registo interpretado apenas em parte.
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
