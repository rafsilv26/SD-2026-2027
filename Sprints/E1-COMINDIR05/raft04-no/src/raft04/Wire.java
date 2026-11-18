package raft04;

import java.nio.file.Files;
import java.nio.file.Path;

/** Configuração e nomes dos canais de comunicação da tarefa. */
public final class Wire {
    public static final long PERIODO_HB_MS = Long.parseLong(
            System.getProperty("raft04.periodo", "2000"));
    public static final int FATOR_TIMEOUT = Integer.parseInt(
            System.getProperty("raft04.fator", "3"));
    public static final long VERIFICACAO_MS = 200L;
    public static final long ELEICAO_MIN_MS = Long.parseLong(
            System.getProperty("raft04.eleicaoMin", Long.toString(3 * PERIODO_HB_MS)));
    public static final long ELEICAO_MAX_MS = Long.parseLong(
            System.getProperty("raft04.eleicaoMax", Long.toString(6 * PERIODO_HB_MS)));
    public static final int GRUPO = 3;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7200;
    public static final double LIMIAR_ALARME = 28.0;
    public static final String PREFIXO_ALARME = "ALARM-";

    static {
        if (PERIODO_HB_MS <= 0 || FATOR_TIMEOUT < 1
                || ELEICAO_MIN_MS <= 0 || ELEICAO_MAX_MS < ELEICAO_MIN_MS) {
            throw new IllegalArgumentException("Configuração de temporização raft04 inválida");
        }
    }

    private Wire() { }

    public static String topicIn(int k) { return "raft04-in-p" + k; }
    public static String topicData(int k) { return "raft04-p" + k; }
    public static String topicElection(int k) { return "raft04-eleicao-p" + k; }
    public static int nodePort(int k, int i) {
        if (k < 0 || i < 1 || i > GRUPO) {
            throw new IllegalArgumentException("Identificador de nó inválido");
        }
        return PORTO_BASE + k * GRUPO + i - 1;
    }
    public static boolean isolated(int k, int i) {
        return Files.exists(Path.of("isolar-p" + k + "-n" + i));
    }

    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector;
    }

    public static String type(String message) {
        int separator = message.trim().indexOf(';');
        if (separator <= 0) throw new IllegalArgumentException("Mensagem sem tipo");
        return message.trim().substring(0, separator);
    }

    public static String body(String message) {
        int separator = message.trim().indexOf(';');
        if (separator < 0) throw new IllegalArgumentException("Mensagem sem conteúdo");
        return message.trim().substring(separator + 1);
    }
}
