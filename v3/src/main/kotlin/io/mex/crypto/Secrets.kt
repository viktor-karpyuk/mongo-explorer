package io.mex.crypto

import java.nio.file.*
import java.nio.file.attribute.PosixFilePermissions
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec
import kotlin.io.path.*

private const val KEY_BYTES = 32
private const val IV_BYTES = 12
private const val GCM_TAG_BITS = 128

object Secrets {
    private val keyPath: Path =
        Paths.get(System.getProperty("user.home"), ".mex-v3", "master.key")

    private val secretKey: SecretKey by lazy { loadOrCreateKey() }

    private fun loadOrCreateKey(): SecretKey {
        if (keyPath.exists()) {
            val raw = Files.readAllBytes(keyPath)
            require(raw.size == KEY_BYTES) { "Corrupted master key at $keyPath" }
            return SecretKeySpec(raw, "AES")
        }
        Files.createDirectories(keyPath.parent)
        val key = KeyGenerator.getInstance("AES").apply { init(256) }.generateKey()
        val raw = key.encoded
        Files.write(keyPath, raw, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
        runCatching {
            Files.setPosixFilePermissions(keyPath, PosixFilePermissions.fromString("rw-------"))
        }
        return SecretKeySpec(raw, "AES")
    }

    fun encrypt(plain: String): ByteArray {
        val iv = ByteArray(IV_BYTES).also { java.security.SecureRandom().nextBytes(it) }
        val cipher = Cipher.getInstance("AES/GCM/NoPadding").apply {
            init(Cipher.ENCRYPT_MODE, secretKey, GCMParameterSpec(GCM_TAG_BITS, iv))
        }
        val cipherText = cipher.doFinal(plain.toByteArray(Charsets.UTF_8))
        // Prepend IV so decryption is a single blob.
        return iv + cipherText
    }

    fun decrypt(blob: ByteArray): String {
        require(blob.size > IV_BYTES) { "Cipher blob too short" }
        val iv = blob.copyOfRange(0, IV_BYTES)
        val cipherText = blob.copyOfRange(IV_BYTES, blob.size)
        val cipher = Cipher.getInstance("AES/GCM/NoPadding").apply {
            init(Cipher.DECRYPT_MODE, secretKey, GCMParameterSpec(GCM_TAG_BITS, iv))
        }
        return cipher.doFinal(cipherText).toString(Charsets.UTF_8)
    }
}
