package io.mex.provision.k8s

import io.mex.data.BackupChoice
import io.mex.data.K8sDeploySpec
import io.mex.data.K8sProfile
import io.mex.data.TlsChoice

/**
 * The teardown cascade (K8P-TEAR-1, ← TEAR-9). The order is fixed and every owned
 * resource declares its position — "any release that introduces a new owned resource
 * must name its position in this order. No silent re-ordering."
 *
 * Pure: [teardownSteps] produces the plan; the runner executes it and aborts the
 * remainder on the first failure (K8P-TEAR-4).
 */

data class TeardownStep(
    val label: String,
    val kind: String,
    val name: String,
    /** Waits for pod drain; null means fire-and-forget. */
    val waitSeconds: Long? = null,
)

fun teardownSteps(spec: K8sDeploySpec, releasePvcs: Boolean): List<TeardownStep> = buildList {
    // 1. the CR itself — the operator drains pods in order.
    add(TeardownStep("Delete ${crKind(spec.operator)}", crKind(spec.operator).lowercase(), spec.name, waitSeconds = 300))
    // 2. PDB / monitoring hooks (only rendered for prod).
    if (spec.profile == K8sProfile.prod) {
        add(TeardownStep("Delete PodDisruptionBudget", "poddisruptionbudget", spec.name))
    }
    // 3. TLS objects we own (a BYO-CA secret belongs to the user — never deleted).
    if (spec.tls is TlsChoice.CertManager) {
        add(TeardownStep("Delete Certificate", "certificate", "${spec.name}-tls"))
        add(TeardownStep("Delete TLS Secret", "secret", "${spec.name}-tls"))
    }
    // 4. secrets we created (PBM credentials are user-supplied and stay).
    add(TeardownStep("Delete users Secret", "secret", usersSecretName(spec)))
    // 5. PVCs — only on explicit release; prod defaults to keeping data (K8P-TEAR-2).
    if (releasePvcs) {
        add(TeardownStep("Delete data volumes", "pvc", "-l app.kubernetes.io/instance=${spec.name}", waitSeconds = 120))
    }
    // 6. namespace, only when this app created it and nothing else lives there.
    if (spec.namespaceCreated) {
        add(TeardownStep("Delete namespace", "namespace", spec.namespace, waitSeconds = 120))
    }
}

/** Human summary for the typed-confirm dialog (K8P-TEAR-3). */
fun teardownConsequence(spec: K8sDeploySpec, releasePvcs: Boolean, connectionName: String?): String = buildString {
    append("This deletes the ${crKind(spec.operator)} \"${spec.name}\"")
    append(", its ")
    val owned = buildList {
        if (spec.profile == K8sProfile.prod) add("PDB")
        if (spec.tls is TlsChoice.CertManager) add("TLS certificate")
        add("credentials Secret")
    }
    append(owned.joinToString(", "))
    append(" in context \"${spec.context}\", namespace \"${spec.namespace}\", and stops the port-forward. ")
    if (releasePvcs) {
        append("Data volumes WILL be deleted — this cannot be undone. ")
    } else {
        append("Data volumes are kept (they keep costing storage). ")
    }
    if (spec.backup is BackupChoice.Pbm) append("PBM credentials and existing backups are left untouched. ")
    if (connectionName != null) append("The connection \"$connectionName\" is removed if you leave the box ticked.")
}
