package io.mex.provision.k8s

import io.mex.data.K8sDeployStatus
import io.mex.data.K8sOperator
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * CR status → app status (K8P-CR-4). Honest by construction (K8P-NFR-3): anything the
 * operator reports that we don't recognise maps to `pending` with the raw string kept,
 * never to a confident "ready".
 */

data class CrStatus(val status: K8sDeployStatus, val detail: String)

/** Absent CR (deleted out-of-band) → missing (K8P-TEAR-5). */
fun mapCrStatus(operator: K8sOperator, json: String?): CrStatus {
    if (json.isNullOrBlank()) return CrStatus(K8sDeployStatus.missing, "resources no longer exist")
    val obj = runCatching { Json.parseToJsonElement(json).jsonObject }.getOrNull()
        ?: return CrStatus(K8sDeployStatus.pending, "status unreadable")
    val status = obj["status"]?.jsonObject ?: return CrStatus(K8sDeployStatus.pending, "no status yet")
    return when (operator) {
        K8sOperator.psmdb -> mapPsmdb(status)
        K8sOperator.mco -> mapMco(status)
    }
}

private fun mapPsmdb(status: kotlinx.serialization.json.JsonObject): CrStatus {
    val state = status["state"]?.jsonPrimitive?.content ?: return CrStatus(K8sDeployStatus.pending, "no state yet")
    val message = status["message"]?.jsonPrimitive?.content
    val detail = message?.takeIf { it.isNotBlank() } ?: state
    return when (state) {
        "ready" -> CrStatus(K8sDeployStatus.ready, detail)
        "initializing" -> CrStatus(K8sDeployStatus.pending, detail)
        "stopping", "paused" -> CrStatus(K8sDeployStatus.degraded, detail)
        "error" -> CrStatus(K8sDeployStatus.degraded, detail)
        else -> CrStatus(K8sDeployStatus.pending, detail)
    }
}

private fun mapMco(status: kotlinx.serialization.json.JsonObject): CrStatus {
    val phase = status["phase"]?.jsonPrimitive?.content ?: return CrStatus(K8sDeployStatus.pending, "no phase yet")
    val current = status["currentStatefulSetReplicas"]?.jsonPrimitive?.content?.toIntOrNull()
    val desired = status["mongodsPerShardCount"]?.jsonPrimitive?.content?.toIntOrNull()
        ?: status["members"]?.jsonPrimitive?.content?.toIntOrNull()
    val counts = if (current != null && desired != null) " ($current/$desired members ready)" else ""
    val message = status["message"]?.jsonPrimitive?.content.orEmpty()
    return when (phase) {
        "Running" -> CrStatus(K8sDeployStatus.ready, "Running$counts")
        "Pending" -> CrStatus(K8sDeployStatus.pending, "Pending$counts $message".trim())
        "Failed" -> CrStatus(K8sDeployStatus.degraded, "Failed $message".trim())
        else -> CrStatus(K8sDeployStatus.pending, "$phase$counts")
    }
}

/** Per-component readiness lines for the status sheet (K8P-UI-4). */
data class ComponentStatus(val name: String, val ready: String, val detail: String = "")

fun psmdbComponents(json: String?): List<ComponentStatus> {
    if (json.isNullOrBlank()) return emptyList()
    val status = runCatching { Json.parseToJsonElement(json).jsonObject["status"]?.jsonObject }.getOrNull()
        ?: return emptyList()
    return buildList {
        status["replsets"]?.jsonObject?.forEach { (name, v) ->
            val o = v.jsonObject
            val ready = o["ready"]?.jsonPrimitive?.content ?: "?"
            val size = o["size"]?.jsonPrimitive?.content ?: "?"
            add(ComponentStatus("rs $name", "$ready/$size", o["status"]?.jsonPrimitive?.content.orEmpty()))
        }
        status["mongos"]?.jsonObject?.let { o ->
            val ready = o["ready"]?.jsonPrimitive?.content ?: "?"
            val size = o["size"]?.jsonPrimitive?.content ?: "?"
            add(ComponentStatus("mongos", "$ready/$size", o["status"]?.jsonPrimitive?.content.orEmpty()))
        }
        status["backup"]?.jsonObject?.let { o ->
            add(ComponentStatus("backup", o["status"]?.jsonPrimitive?.content ?: "?"))
        }
    }
}

/** Recent events, newest last as kubectl sorts them (K8P-UI-4). */
fun parseEventLines(lines: List<String>): List<String> =
    lines.filter { it.isNotBlank() && !it.startsWith("LAST SEEN") }.takeLast(20)

/** Service/pod the port-forward should target (K8P-CONN-1). */
fun forwardTarget(operator: K8sOperator, name: String, topology: io.mex.data.K8sTopology): String =
    when (topology) {
        is io.mex.data.K8sTopology.Sharded -> "svc/$name-mongos"
        is io.mex.data.K8sTopology.ReplicaSet -> when (operator) {
            K8sOperator.psmdb -> "svc/$name-rs0"
            K8sOperator.mco -> "pod/$name-0"
        }
    }

/** Extracts the operator-managed CA/cert secret name for the client TLS bundle. */
fun tlsSecretName(operator: K8sOperator, spec: io.mex.data.K8sDeploySpec): String? = when (val t = spec.tls) {
    is io.mex.data.TlsChoice.ByoCa -> t.secretName
    is io.mex.data.TlsChoice.CertManager -> "${spec.name}-tls"
    io.mex.data.TlsChoice.OperatorSelfSigned -> if (operator == K8sOperator.psmdb) "${spec.name}-ssl" else null
    io.mex.data.TlsChoice.Off -> null
}
