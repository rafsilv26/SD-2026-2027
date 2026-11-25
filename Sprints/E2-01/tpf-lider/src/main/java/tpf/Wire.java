package tpf;

public final class Wire {
    public static final int CHUNK_MAX_BYTES =
            Integer.getInteger("tpf.chunkMaxBytes", 65536);
    public static final int PAD_BYTES =
            Integer.getInteger("tpf.padBytes", 4096);
    public static final long SELAGEM_MS =
            Long.getLong("tpf.selagemMs", 5000L);
    public static final int KEY_VERSION = 1;

    private Wire() {
    }

    public static void validar() {
        if (CHUNK_MAX_BYTES <= 0 || PAD_BYTES <= 0 || SELAGEM_MS < 0) {
            throw new IllegalArgumentException("Parâmetros tpf.* inválidos");
        }
    }
}
