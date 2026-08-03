package io.mex.provision.k8s

import io.mex.backup.ToolInfo
import io.mex.backup.findTool
import io.mex.backup.runBounded

/**
 * kubectl integration (K8P-CTX-1): CLI shell-out only, every invocation bounded.
 * Builders are pure and tested; execution goes through [kubectl] / ToolProcess.
 */

@Volatile
private var cachedKubectl: ToolInfo? = null

/** Rancher Desktop uses `~/.rd/bin`; Docker Desktop `~/.docker/bin`. Cached like docker. */
fun findKubectl(refresh: Boolean = false): ToolInfo? {
    if (!refresh) cachedKubectl?.let { return it }
    val home = System.getProperty("user.home")
    return findTool("kubectl", extraDirs = listOf("$home/.docker/bin", "$home/.rd/bin"))
        .also { cachedKubectl = it }
}

/** A kubeconfig context selection; the app never mutates kubeconfig (K8P-CTX-2). */
data class KubeTarget(val context: String, val kubeconfig: String? = null)

/** Context/kubeconfig flags appended to every command so the user's current-context is never relied on. */
fun ctxArgs(target: KubeTarget): List<String> = buildList {
    add("--context")
    add(target.context)
    target.kubeconfig?.let {
        add("--kubeconfig")
        add(it)
    }
}

fun contextsArgs(kubeconfig: String? = null): List<String> = buildList {
    add("config")
    add("get-contexts")
    add("-o")
    add("name")
    kubeconfig?.let {
        add("--kubeconfig")
        add(it)
    }
}

fun versionArgs(target: KubeTarget): List<String> =
    listOf("version", "-o", "json") + ctxArgs(target)

fun namespacesArgs(target: KubeTarget): List<String> =
    listOf("get", "namespaces", "-o", "name") + ctxArgs(target)

fun getJsonArgs(target: KubeTarget, kind: String, name: String? = null, namespace: String? = null): List<String> =
    buildList {
        add("get")
        add(kind)
        name?.let { add(it) }
        namespace?.let { add("-n"); add(it) }
        add("-o")
        add("json")
        add("--ignore-not-found")
    } + ctxArgs(target)

/** The bundle is applied from stdin — manifests with secret values never touch argv (K8P-SEC-1). */
fun applyStdinArgs(target: KubeTarget, namespace: String? = null): List<String> =
    buildList {
        add("apply")
        add("-f")
        add("-")
        namespace?.let { add("-n"); add(it) }
    } + ctxArgs(target)

fun deleteArgs(
    target: KubeTarget,
    kind: String,
    name: String,
    namespace: String? = null,
    waitSeconds: Long? = null,
): List<String> = buildList {
    add("delete")
    add(kind)
    add(name)
    namespace?.let { add("-n"); add(it) }
    add("--ignore-not-found")
    if (waitSeconds != null) {
        add("--wait=true")
        add("--timeout=${waitSeconds}s")
    } else {
        add("--wait=false")
    }
} + ctxArgs(target)

fun canIArgs(target: KubeTarget, verb: String, resource: String, namespace: String? = null): List<String> =
    buildList {
        add("auth")
        add("can-i")
        add(verb)
        add(resource)
        namespace?.let { add("-n"); add(it) }
    } + ctxArgs(target)

fun portForwardArgs(
    target: KubeTarget,
    resource: String, // "svc/name" or "pod/name"
    localPort: Int,
    remotePort: Int,
    namespace: String,
): List<String> = listOf(
    "port-forward", resource, "$localPort:$remotePort",
    "-n", namespace,
    // Loopback-only, same doctrine as lab ports (PRV-SEC-4).
    "--address", "127.0.0.1",
) + ctxArgs(target)

fun eventsArgs(target: KubeTarget, namespace: String): List<String> =
    listOf("get", "events", "-n", namespace, "--sort-by=.lastTimestamp") + ctxArgs(target)

/* ===================== bounded execution ===================== */

/** Read-path invocation with a hard deadline (K8P-NFR-1). */
fun kubectlRead(tool: ToolInfo, args: List<String>, timeoutSec: Long = 15): Pair<Int, List<String>>? =
    runBounded(listOf(tool.path) + args, timeoutSec)
