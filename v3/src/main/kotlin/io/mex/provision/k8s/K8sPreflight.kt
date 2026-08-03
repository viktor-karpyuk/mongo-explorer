package io.mex.provision.k8s

import io.mex.backup.ToolInfo
import io.mex.data.K8sDeploySpec
import io.mex.data.K8sOperator
import io.mex.data.K8sProfile
import io.mex.data.PreflightCheck
import io.mex.data.PreflightResult
import io.mex.data.TlsChoice

/**
 * Preflight (K8P-PRE): blocking checks gate Preview/Apply, warnings never do.
 * Probe results are gathered by [runK8sPreflight]; the verdict logic is pure so the
 * decision table is testable without a cluster.
 */

data class ProbeFacts(
    val reachable: Boolean,
    val versionVerdict: VersionVerdict,
    val operators: List<DetectedOperator>,
    val missingRbac: List<RbacRequirement>,
    val storageClasses: List<String>,
    val certManager: Boolean,
    val schedulableNodes: Int,
    val zones: Int,
    val unreachableDetail: String? = null,
)

/** How many pods the topology will try to place — the node-inventory check (← PRE-5.6). */
fun requiredNodes(spec: K8sDeploySpec): Int = when (val t = spec.topology) {
    is io.mex.data.K8sTopology.ReplicaSet -> t.members
    is io.mex.data.K8sTopology.Sharded -> maxOf(t.membersPerShard, t.configServers, t.mongos)
}

fun preflightVerdict(spec: K8sDeploySpec, facts: ProbeFacts): PreflightResult {
    val checks = mutableListOf<PreflightCheck>()

    checks += if (facts.reachable) {
        val v = facts.versionVerdict
        val detail = when (v) {
            is VersionVerdict.Ok -> "server ${v.version}"
            is VersionVerdict.Warn -> "server ${v.version}"
            is VersionVerdict.Unknown -> v.message
        }
        PreflightCheck("Context ${spec.context} reachable", true, detail)
    } else {
        // Surface kubectl's own message — "unreachable" alone tells nobody whether it is
        // DNS, an expired token, or a typo in the context name.
        val detail = facts.unreachableDetail
            ?: (facts.versionVerdict as? VersionVerdict.Unknown)?.message
            ?: "unreachable"
        PreflightCheck("Context ${spec.context} reachable", false, detail)
    }
    // Version is a WARN, never a block (K8P-CTX-4/K8P-PRE-2).
    (facts.versionVerdict as? VersionVerdict.Warn)?.let {
        checks += PreflightCheck("Kubernetes ${it.version}", true, it.message, warn = true)
    }

    checks += if (facts.missingRbac.isEmpty()) {
        PreflightCheck("RBAC", true, "all required verbs allowed")
    } else {
        PreflightCheck(
            "RBAC",
            false,
            "missing: " + facts.missingRbac.joinToString(", ") { "${it.verb} ${it.resource}" },
        )
    }

    val detected = facts.operators.firstOrNull { it.operator == spec.operator }
    checks += if (detected != null) {
        PreflightCheck(
            "${spec.operator.name.uppercase()} operator",
            true,
            detected.version?.let { "version $it" } ?: "version unknown",
        )
    } else {
        PreflightCheck(
            "${spec.operator.name.uppercase()} operator",
            false,
            "CRDs not found — install the operator, then re-run preflight",
        )
    }
    if (detected != null && detected.version == null) {
        checks += PreflightCheck("Operator version", true, "not detectable from the deployment image", warn = true)
    }

    // Sharding is capability-gated, restated here so preflight is self-explanatory.
    if (spec.topology is io.mex.data.K8sTopology.Sharded && spec.operator == K8sOperator.mco) {
        checks += PreflightCheck("Sharded topology", false, "MCO cannot express sharded clusters")
    }

    spec.storageClass?.let { sc ->
        checks += if (facts.storageClasses.isEmpty() || sc in facts.storageClasses) {
            PreflightCheck("StorageClass $sc", true)
        } else {
            PreflightCheck("StorageClass $sc", false, "not found — available: ${facts.storageClasses.joinToString(", ")}")
        }
    }

    if (spec.tls is TlsChoice.CertManager) {
        checks += if (facts.certManager) {
            PreflightCheck("cert-manager", true)
        } else {
            PreflightCheck("cert-manager", false, "CRDs not found but the chosen TLS path needs them")
        }
    }

    val need = requiredNodes(spec)
    checks += if (facts.schedulableNodes >= need) {
        PreflightCheck("Node inventory", true, "${facts.schedulableNodes} schedulable ≥ $need needed")
    } else {
        PreflightCheck("Node inventory", false, "${facts.schedulableNodes} schedulable < $need needed for this topology")
    }

    if (spec.profile == K8sProfile.prod && facts.zones < 3) {
        checks += PreflightCheck(
            "Failure domains",
            true,
            "${facts.zones} zone(s) detected — 3 recommended for DoNotSchedule spread",
            warn = true,
        )
    }

    return PreflightResult(checks.all { it.ok }, checks)
}

/** Gathers facts from the cluster, then delegates to [preflightVerdict]. */
fun runK8sPreflight(tool: ToolInfo, spec: K8sDeploySpec): PreflightResult {
    val target = KubeTarget(spec.context, spec.kubeconfigPath)
    val version = serverVersion(tool, target)
    val reachable = version !is VersionVerdict.Unknown
    if (!reachable) {
        return preflightVerdict(
            spec,
            ProbeFacts(
                reachable = false,
                versionVerdict = version,
                operators = emptyList(),
                missingRbac = emptyList(),
                storageClasses = emptyList(),
                certManager = false,
                schedulableNodes = 0,
                zones = 0,
                unreachableDetail = (version as VersionVerdict.Unknown).message,
            ),
        )
    }
    val operators = detectOperators(tool, target)
    val rbac = missingRbac(
        tool, target, spec.namespace,
        REQUIRED_RBAC + operatorRbac(crPlural(spec.operator)),
    )
    val nodes = nodeFacts(tool, target)
    return preflightVerdict(
        spec,
        ProbeFacts(
            reachable = true,
            versionVerdict = version,
            operators = operators,
            missingRbac = rbac,
            storageClasses = listStorageClasses(tool, target),
            certManager = certManagerPresent(tool, target),
            schedulableNodes = nodes.first,
            zones = nodes.second,
        ),
    )
}

fun listStorageClasses(tool: ToolInfo, target: KubeTarget): List<String> =
    kubectlRead(tool, listOf("get", "storageclass", "-o", "name") + ctxArgs(target))
        ?.takeIf { it.first == 0 }
        ?.second?.mapNotNull { it.substringAfter("/").ifBlank { null } }
        .orEmpty()

/** (schedulable node count, distinct zone count) — unschedulable/tainted nodes excluded. */
private fun nodeFacts(tool: ToolInfo, target: KubeTarget): Pair<Int, Int> {
    val args = listOf(
        "get", "nodes",
        "-o", "jsonpath={range .items[*]}{.spec.unschedulable}{\" \"}{.metadata.labels.topology\\.kubernetes\\.io/zone}{\"\\n\"}{end}",
    ) + ctxArgs(target)
    val (code, lines) = kubectlRead(tool, args) ?: return 0 to 0
    if (code != 0) return 0 to 0
    val rows = lines.filter { it.isNotBlank() }
    val schedulable = rows.filterNot { it.trim().startsWith("true") }
    val zones = schedulable.mapNotNull { it.trim().substringAfter(" ", "").ifBlank { null } }.toSet()
    return schedulable.size to zones.size
}
