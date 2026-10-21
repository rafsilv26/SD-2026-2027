package rep02;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;

    private Wire() { }

    public static String topicIn(int k) { return "rep02-in-p" + k; }
    public static String topicData(int k) { return "rep02-p" + k; }
    public static String in(String sensor, double temp) { return "IN;" + sensor + ";" + temp; }
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
