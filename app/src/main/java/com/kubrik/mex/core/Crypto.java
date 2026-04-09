package com.kubrik.mex.core;

import com.kubrik.mex.store.AppPaths;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Base64;

/** AES-GCM encryption with a per-install key file. */
public final class Crypto {

    private static final String ALG = "AES/GCM/NoPadding";
    private static final int IV_LEN = 12;
    private static final int TAG_BITS = 128;

    private final SecretKeySpec key;

    public Crypto() {
        try {
            Path kf = AppPaths.keyFile();
            byte[] keyBytes;
            if (Files.exists(kf)) {
                keyBytes = Files.readAllBytes(kf);
            } else {
                Files.createDirectories(kf.getParent());
                keyBytes = new byte[32];
                new SecureRandom().nextBytes(keyBytes);
                Files.write(kf, keyBytes);
                try { kf.toFile().setReadable(false, false); kf.toFile().setReadable(true, true); } catch (Exception ignored) {}
            }
            this.key = new SecretKeySpec(keyBytes, "AES");
        } catch (Exception e) {
            throw new RuntimeException("crypto init failed", e);
        }
    }

    public String encrypt(String plain) {
        if (plain == null || plain.isEmpty()) return null;
        try {
            byte[] iv = new byte[IV_LEN];
            new SecureRandom().nextBytes(iv);
            Cipher c = Cipher.getInstance(ALG);
            c.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            byte[] ct = c.doFinal(plain.getBytes());
            byte[] out = new byte[iv.length + ct.length];
            System.arraycopy(iv, 0, out, 0, iv.length);
            System.arraycopy(ct, 0, out, iv.length, ct.length);
            return Base64.getEncoder().encodeToString(out);
        } catch (Exception e) {
            throw new RuntimeException("encrypt failed", e);
        }
    }

    public String decrypt(String b64) {
        if (b64 == null || b64.isEmpty()) return null;
        try {
            byte[] all = Base64.getDecoder().decode(b64);
            byte[] iv = new byte[IV_LEN];
            System.arraycopy(all, 0, iv, 0, IV_LEN);
            byte[] ct = new byte[all.length - IV_LEN];
            System.arraycopy(all, IV_LEN, ct, 0, ct.length);
            Cipher c = Cipher.getInstance(ALG);
            c.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            return new String(c.doFinal(ct));
        } catch (Exception e) {
            throw new RuntimeException("decrypt failed", e);
        }
    }
}
