package rep04;

public final class Wire {
    public static final long PERIODO_HB_MS =
            Long.parseLong(System.getProperty("rep04.periodo", "5000"));
    public static final int FATOR_TIMEOUT =
            Integer.parseInt(System.getProperty("rep04.fator", "3"));
    public static final long VERIFICACAO_MS = 1000L;
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;
    public static final double LIMIAR_ALARME = 28.0;
    public static final String PREFIXO_ALARME = "ALARM-";

    static {
        if (PERIODO_HB_MS <= 0 || FATOR_TIMEOUT < 1) {
            throw new IllegalArgumentException("rep04.periodo e rep04.fator têm de ser positivos");
        }
    }

    private Wire() { }

    public static String topicIn(int k) { return "rep04-in-p" + k; }
    public static String topicData(int k) { return "rep04-p" + k; }

    // IN;sensor;temp;L;V
    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector;
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

}
