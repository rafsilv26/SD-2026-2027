package tpf;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;

public final class ChunkCrypto {
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final byte[] INFO = "tpf-chunk-v1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] AAD_PREFIX = "tpf-v1".getBytes(StandardCharsets.UTF_8);

    private ChunkCrypto() {
    }

    public static byte[] randomBytes(int length) {
        byte[] value = new byte[length];
        RANDOM.nextBytes(value);
        return value;
    }

    public static byte[] hkdf(byte[] masterKey, byte[] salt) throws Exception {
        Mac extract = Mac.getInstance("HmacSHA256");
        extract.init(new SecretKeySpec(salt, "HmacSHA256"));
        byte[] prk = extract.doFinal(masterKey);

        Mac expand = Mac.getInstance("HmacSHA256");
        expand.init(new SecretKeySpec(prk, "HmacSHA256"));
        expand.update(INFO);
        expand.update((byte) 0x01);
        return Arrays.copyOf(expand.doFinal(), 32);
    }

    public static Sealed seal(byte[] plaintext, byte[] masterKey) throws Exception {
        byte[] salt = randomBytes(32);
        byte[] nonce = randomBytes(12);
        return seal(plaintext, masterKey, salt, nonce, Wire.KEY_VERSION);
    }

    public static Sealed seal(byte[] plaintext, byte[] masterKey, byte[] salt,
                              byte[] nonce, int keyVersion) throws Exception {
        if (salt.length != 32 || nonce.length != 12) {
            throw new IllegalArgumentException("salt ou nonce com tamanho inválido");
        }
        if (keyVersion != Wire.KEY_VERSION) {
            throw new IllegalArgumentException("Versão da chave inválida");
        }

        byte[] dek = hkdf(masterKey, salt);
        byte[] aad = aad(keyVersion, salt, nonce);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(dek, "AES"),
                new GCMParameterSpec(128, nonce));
        cipher.updateAAD(aad);
        byte[] ciphertext = cipher.doFinal(plaintext);
        Arrays.fill(dek, (byte) 0);

        return new Sealed(ciphertext,
                sha256Hex(ciphertext), salt, nonce, keyVersion);
    }

    public static byte[] open(Sealed sealed, byte[] masterKey) throws Exception {
        String actual = sha256Hex(sealed.ciphertext());
        if (!actual.equals(sealed.chunkId())) {
            throw new IllegalArgumentException("Hash do chunk não coincide com o chunkId");
        }
        if (sealed.keyVersion() != Wire.KEY_VERSION) {
            throw new IllegalArgumentException("Versão da chave não suportada");
        }
        if (sealed.salt().length != 32 || sealed.nonce().length != 12) {
            throw new IllegalArgumentException("salt ou nonce com tamanho inválido");
        }

        byte[] dek = hkdf(masterKey, sealed.salt());
        byte[] aad = aad(sealed.keyVersion(), sealed.salt(), sealed.nonce());
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dek, "AES"),
                new GCMParameterSpec(128, sealed.nonce()));
        cipher.updateAAD(aad);
        try {
            return cipher.doFinal(sealed.ciphertext());
        } finally {
            Arrays.fill(dek, (byte) 0);
        }
    }

    public static byte[] aad(int keyVersion, byte[] salt, byte[] nonce) {
        ByteBuffer buffer = ByteBuffer.allocate(AAD_PREFIX.length + 4
                + salt.length + nonce.length);
        buffer.put(AAD_PREFIX);
        buffer.putInt(keyVersion);
        buffer.put(salt);
        buffer.put(nonce);
        return buffer.array();
    }

    public static byte[] sha256(byte[] data) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(data);
    }

    public static String sha256Hex(byte[] data) throws Exception {
        byte[] hash = sha256(data);
        StringBuilder result = new StringBuilder();
        for (byte value : hash) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }
}
