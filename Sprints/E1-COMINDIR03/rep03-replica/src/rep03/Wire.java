package rep03;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;
    public static final double LIMIAR_ALARME = 28.0;
    public static final String PREFIXO_ALARME = "ALARM-";

    private Wire() { }

    public static String topicIn(int k) { return "rep03-in-p" + k; }
    public static String topicData(int k) { return "rep03-p" + k; }

    // IN;sensor;temp;L;V
    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector;
    }

    // REG;host;porto;seq;sensor;temp;instante;L;V  (L e V vêm no toLine do registo)
    public static String reg(String host, int port, SensorRecord record) {
        return "REG;" + host + ";" + port + ";" + record.toLine();
    }

    public static String type(String message) {
        String trimmed = message.trim();
        int separator = trimmed.indexOf(';');
        if (separator <= 0) {
            throw new IllegalArgumentException("Mensagem sem tipo: " + message);
        }
        return trimmed.substring(0, separator);
    }

    public static String body(String message) {
        String trimmed = message.trim();
        int separator = trimmed.indexOf(';');
        if (separator < 0) {
            throw new IllegalArgumentException("Mensagem sem conteúdo: " + message);
        }
        return trimmed.substring(separator + 1);
    }

    public static String[] regParts(String message) {
        String[] fields = message.trim().split(";", 4);
        if (fields.length != 4 || !"REG".equals(fields[0])) {
            throw new IllegalArgumentException("REG mal formado: " + message);
        }
        return new String[] {fields[1], fields[2], fields[3]};
    }
}
