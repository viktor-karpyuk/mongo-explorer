package io.mex.provision.k8s

import io.mex.backup.ToolInfo
import io.mex.data.K8sOperator
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * Operator detection is CRD-based (K8P-OP-1); capabilities gate the wizard
 * (K8P-OP-2 — MCO cannot express sharding). Detection never installs (K8P-OP-3).
 */

const val MCO_CRD = "mongodbcommunity.mongodbcommunity.mongodb.com"
const val PSMDB_CRD = "perconaservermongodbs.psmdb.percona.com"
const val CERT_MANAGER_CRD = "certificates.cert-manager.io"

/** The CR the app renders and manages per operator. */
fun crKind(op: K8sOperator): String = when (op) {
    K8sOperator.mco -> "MongoDBCommunity"
    K8sOperator.psmdb -> "PerconaServerMongoDB"
}

fun crPlural(op: K8sOperator): String = when (op) {
    K8sOperator.mco -> "mongodbcommunity"
    K8sOperator.psmdb -> "perconaservermongodbs"
}

fun crApiVersion(op: K8sOperator): String = when (op) {
    K8sOperator.mco -> "mongodbcommunity.mongodb.com/v1"
    K8sOperator.psmdb -> "psmdb.percona.com/v1"
}

enum class OperatorCapability { REPLICA_SET, SHARDED, PBM_BACKUP, CERT_MANAGER_TLS, SELF_SIGNED_TLS }

/** Decision 8 / OP-MCO-3 ported: MCO = RS only, no native backup. */
fun capabilities(op: K8sOperator): Set<OperatorCapability> = when (op) {
    K8sOperator.mco -> setOf(
        OperatorCapability.REPLICA_SET,
        OperatorCapability.CERT_MANAGER_TLS,
    )
    K8sOperator.psmdb -> setOf(
        OperatorCapability.REPLICA_SET,
        OperatorCapability.SHARDED,
        OperatorCapability.PBM_BACKUP,
        OperatorCapability.CERT_MANAGER_TLS,
        OperatorCapability.SELF_SIGNED_TLS,
    )
}

data class DetectedOperator(val operator: K8sOperator, val version: String?)

private fun crdPresent(tool: ToolInfo, target: KubeTarget, crd: String): Boolean =
    kubectlRead(tool, getJsonArgs(target, "crd", crd))
        ?.let { (code, lines) -> code == 0 && lines.any { it.isNotBlank() } }
        ?: false

fun certManagerPresent(tool: ToolInfo, target: KubeTarget): Boolean =
    crdPresent(tool, target, CERT_MANAGER_CRD)

/**
 * CRD presence per operator, plus a best-effort version read from the operator
 * deployment's image tag — unknown versions are tolerated with a WARN (K8P-OP-4).
 */
fun detectOperators(tool: ToolInfo, target: KubeTarget): List<DetectedOperator> = buildList {
    if (crdPresent(tool, target, MCO_CRD)) {
        add(DetectedOperator(K8sOperator.mco, operatorVersion(tool, target, "mongodb-kubernetes-operator")))
    }
    if (crdPresent(tool, target, PSMDB_CRD)) {
        add(DetectedOperator(K8sOperator.psmdb, operatorVersion(tool, target, "percona-server-mongodb-operator")))
    }
}

/** Server-side label selector keeps this cheap on clusters with many deployments (K8P-NFR-4). */
private fun operatorVersion(tool: ToolInfo, target: KubeTarget, appName: String): String? {
    val args = listOf(
        "get", "deployments", "-A",
        "-l", "app.kubernetes.io/name=$appName",
        "-o", "json",
    ) + ctxArgs(target)
    val (code, lines) = kubectlRead(tool, args) ?: return null
    if (code != 0) return null
    return parseFirstImageTag(lines.joinToString("\n"))
}

fun parseFirstImageTag(deploymentsJson: String): String? = runCatching {
    val items = Json.parseToJsonElement(deploymentsJson).jsonObject["items"]?.jsonArray ?: return null
    val image = items.firstOrNull()?.jsonObject
        ?.get("spec")?.jsonObject
        ?.get("template")?.jsonObject
        ?.get("spec")?.jsonObject
        ?.get("containers")?.jsonArray
        ?.firstOrNull()?.jsonObject
        ?.get("image")?.jsonPrimitive?.content ?: return null
    image.substringAfterLast(":", missingDelimiterValue = "").ifBlank { null }
}.getOrNull()
