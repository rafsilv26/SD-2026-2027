package tpf;

public record Sealed(byte[] ciphertext, String chunkId,
                     byte[] salt, byte[] nonce, int keyVersion) {
}
