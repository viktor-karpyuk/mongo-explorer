package io.mex.provision.k8s

import java.security.MessageDigest

/**
 * The preview-hash invariant (K8P-CR-2, ← PROV-15a — "load-bearing across the series").
 *
 * Apply is authorized against the hash of the exact bytes the user read. Any edit
 * re-renders to a different hash and invalidates the confirmation; apply re-renders at
 * execution time and refuses when the hash moved. Fails closed: nothing is applied.
 */

/** Documents joined the same way they are fed to `kubectl apply -f -`. */
fun bundleText(docs: List<YamlDoc>): String = docs.joinToString("\n---\n") { it.yaml.trimEnd() } + "\n"

fun bundleHash(docs: List<YamlDoc>): String {
    val digest = MessageDigest.getInstance("SHA-256").digest(bundleText(docs).toByteArray())
    return "sha256:" + digest.joinToString("") { "%02x".format(it) }
}

/** Short form for the UI (`sha256:3f9c1a72…e441`). */
fun shortHash(hash: String): String {
    val body = hash.removePrefix("sha256:")
    return if (body.length <= 16) hash else "sha256:${body.take(8)}…${body.takeLast(4)}"
}

class StalePreviewException(val confirmed: String, val actual: String) :
    RuntimeException("stale preview — the configuration changed after it was reviewed; nothing was applied")

/** Throws unless the freshly rendered bundle matches what the user confirmed. */
fun requireFreshPreview(confirmedHash: String, freshDocs: List<YamlDoc>) {
    val actual = bundleHash(freshDocs)
    if (actual != confirmedHash) throw StalePreviewException(confirmedHash, actual)
}
