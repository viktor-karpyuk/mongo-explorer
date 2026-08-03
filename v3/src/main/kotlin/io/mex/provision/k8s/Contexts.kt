package io.mex.provision.k8s

import io.mex.backup.ToolInfo
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * Cluster access probes (K8P-CTX-3..6): contexts, server version vs the blessed
 * matrix, namespaces, and the RBAC verb sweep. Pure decision logic is separated
 * from execution for testability.
 */

/** Blessed Kubernetes minors (K8P-CTX-4); outside = non-blocking WARN. */
val BLESSED_K8S = setOf("1.32", "1.33", "1.34")

sealed interface VersionVerdict {
    data class Ok(val version: String) : VersionVerdict
    data class Warn(val version: String, val message: String) : VersionVerdict
    data class Unknown(val message: String) : VersionVerdict
}

/** Pure: server major.minor → verdict against the blessed matrix. */
fun versionVerdict(major: String?, minor: String?): VersionVerdict {
    if (major == null || minor == null) return VersionVerdict.Unknown("server version not reported")
    // EKS reports minors like "30+"; strip the suffix before matching.
    val cleanMinor = minor.trimEnd('+')
    val v = "$major.$cleanMinor"
    return if (v in BLESSED_K8S) VersionVerdict.Ok(v)
    else VersionVerdict.Warn(v, "outside blessed matrix ${BLESSED_K8S.sorted().joinToString("–")} — proceed with care")
}

fun parseServerVersion(versionJson: String): Pair<String?, String?> = runCatching {
    val o = Json.parseToJsonElement(versionJson).jsonObject["serverVersion"]?.jsonObject ?: return null to null
    o["major"]?.jsonPrimitive?.content to o["minor"]?.jsonPrimitive?.content
}.getOrDefault(null to null)

/**
 * Display name for a kubeconfig context. EKS contexts are full ARNs
 * (`arn:aws:eks:sa-east-1:532465846520:cluster/kubrik-k8s`) and GKE contexts are
 * underscore-packed (`gke_project_zone_name`); showing those verbatim in cards and
 * dialogs buries the one part a human reads. The full value stays available wherever
 * precision matters.
 */
fun shortContext(context: String): String = when {
    context.startsWith("arn:") && "/" in context -> context.substringAfterLast('/')
    context.startsWith("gke_") -> context.substringAfterLast('_')
    else -> context
}

fun listContexts(tool: ToolInfo, kubeconfig: String? = null): List<String> =
    kubectlRead(tool, contextsArgs(kubeconfig))
        ?.takeIf { it.first == 0 }
        ?.second?.filter { it.isNotBlank() }
        .orEmpty()

fun serverVersion(tool: ToolInfo, target: KubeTarget): VersionVerdict {
    val (code, lines) = kubectlRead(tool, versionArgs(target), timeoutSec = 15)
        ?: return VersionVerdict.Unknown("kubectl version timed out — context unreachable?")
    // `kubectl version` exits non-zero when the server is unreachable but still prints client JSON.
    val (major, minor) = parseServerVersion(lines.joinToString("\n"))
    if (code != 0 && major == null) {
        return VersionVerdict.Unknown(lines.lastOrNull()?.take(200) ?: "context unreachable")
    }
    return versionVerdict(major, minor)
}

/** `kubectl get namespaces -o name` → bare names. */
fun listNamespaces(tool: ToolInfo, target: KubeTarget): List<String> =
    kubectlRead(tool, namespacesArgs(target))
        ?.takeIf { it.first == 0 }
        ?.second?.mapNotNull { it.removePrefix("namespace/").ifBlank { null } }
        .orEmpty()

/* ===================== RBAC probe (K8P-CTX-5) ===================== */

data class RbacRequirement(val verb: String, val resource: String)

/** Every verb the provision→teardown flow needs; any "no" blocks preflight. */
val REQUIRED_RBAC: List<RbacRequirement> = listOf(
    RbacRequirement("create", "secrets"),
    RbacRequirement("delete", "secrets"),
    RbacRequirement("get", "pods"),
    RbacRequirement("list", "pods"),
    RbacRequirement("create", "pods/portforward"),
    RbacRequirement("delete", "persistentvolumeclaims"),
    RbacRequirement("create", "poddisruptionbudgets"),
    RbacRequirement("get", "events"),
)

/** Operator-specific CR permissions, added to [REQUIRED_RBAC] per chosen operator. */
fun operatorRbac(crPlural: String): List<RbacRequirement> = listOf(
    RbacRequirement("create", crPlural),
    RbacRequirement("get", crPlural),
    RbacRequirement("delete", crPlural),
)

/** Runs `auth can-i` per requirement; returns the requirements that came back "no". */
fun missingRbac(
    tool: ToolInfo,
    target: KubeTarget,
    namespace: String,
    requirements: List<RbacRequirement>,
): List<RbacRequirement> = requirements.filter { req ->
    val out = kubectlRead(tool, canIArgs(target, req.verb, req.resource, namespace), timeoutSec = 10)
    // Timeout or error counts as missing — preflight must fail closed, not open.
    out == null || out.second.firstOrNull()?.trim() != "yes"
}
