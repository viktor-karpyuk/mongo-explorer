package io.mex.provision

import io.mex.backup.ToolInfo
import io.mex.backup.findTool
import io.mex.data.LabStatus
import io.mex.data.PreflightCheck
import io.mex.data.PreflightResult
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/** Label stamped on every container/volume/network a lab owns (PRV-DOCKER-5). */
const val LAB_LABEL = "mex.lab.id"

@Volatile
private var cachedDocker: ToolInfo? = null

/**
 * Docker Desktop installs its CLI under `~/.docker/bin` on macOS (PRV-DOCKER-1).
 * Discovery spawns up to 6 `--version` probes, so a successful result is cached for the
 * app's lifetime; [refresh] (the guidance panel's Retry) re-probes after an install.
 */
fun findDocker(refresh: Boolean = false): ToolInfo? {
    if (!refresh) cachedDocker?.let { return it }
    return findTool("docker", extraDirs = listOf("${System.getProperty("user.home")}/.docker/bin"))
        .also { cachedDocker = it }
}

/** PRV-DOCKER-2 — `Docker version 28.1.1, build …` → true when ≥ 20.10. */
fun dockerVersionOk(versionLine: String): Boolean {
    val m = Regex("(\\d+)\\.(\\d+)").find(versionLine) ?: return false
    val (major, minor) = m.destructured
    return major.toInt() * 100 + minor.toInt() >= 20 * 100 + 10
}

/* ===================== argument builders (pure, tested) ===================== */

fun composeArgs(dir: String, vararg cmd: String): List<String> =
    listOf("compose", "--project-directory", dir) + cmd

/** `docker ps` filtered to lab-owned containers only (PRV-NFR-5). */
fun psArgs(labId: String? = null): List<String> = listOf(
    "ps", "-a",
    "--filter", if (labId == null) "label=$LAB_LABEL" else "label=$LAB_LABEL=$labId",
    "--format", "{{json .}}",
)

fun inspectHealthArgs(container: String): List<String> =
    listOf("inspect", "--format", "{{.State.Health.Status}}", container)

/**
 * One-spawn health snapshot of a whole lab: `Status` renders as e.g.
 * `Up 13 seconds (healthy)`. Polling per-service `inspect` would spawn N processes
 * per tick (24 for the largest sharded shape).
 */
fun psStatusArgs(labId: String): List<String> = listOf(
    "ps", "-a", "--filter", "label=$LAB_LABEL=$labId", "--format", "{{.Names}} {{.Status}}",
)

/** The script is a single argv element — no shell is involved, so no quoting hazards. */
fun execMongoshArgs(container: String, script: String): List<String> =
    listOf("exec", container, "mongosh", "--quiet", "--eval", script)

/**
 * mongosh reading its script from stdin — for scripts carrying secrets (PRV-SEC-3):
 * argv is world-readable via `ps`/procfs for the lifetime of the exec.
 */
fun execMongoshStdinArgs(container: String): List<String> =
    listOf("exec", "-i", container, "mongosh", "--quiet")

/** Compose v2 default container name for a service (no `container_name` is rendered). */
fun containerName(project: String, service: String): String = "$project-$service-1"

/* ===================== docker ps parsing & reconciliation ===================== */

data class PsRow(val name: String, val state: String, val labId: String?)

fun parsePsLine(line: String): PsRow? = runCatching {
    val o = Json.parseToJsonElement(line).jsonObject
    val labels = o["Labels"]?.jsonPrimitive?.content.orEmpty()
    val labId = labels.split(",").firstOrNull { it.startsWith("$LAB_LABEL=") }?.substringAfter("=")
    PsRow(
        name = o["Names"]!!.jsonPrimitive.content,
        state = o["State"]!!.jsonPrimitive.content,
        labId = labId,
    )
}.getOrNull()

/**
 * PRV-LIFE-1/4 — Docker is the source of truth; the row is the record. Returns the
 * corrected status, or null when the row already tells the truth. `provisioning` is
 * only reconciled at boot (the caller excludes labs with a live operation).
 */
fun reconcileStatus(current: LabStatus, containerStates: List<String>): LabStatus? {
    // "restarting" is a live container in a crash loop — treating it as stopped would
    // let a Start be issued against containers Docker is still supervising.
    val anyRunning = containerStates.any { it == "running" || it == "restarting" }
    return when {
        current == LabStatus.provisioning -> LabStatus.failed
        current == LabStatus.running && containerStates.isEmpty() -> LabStatus.missing
        current == LabStatus.stopped && containerStates.isEmpty() -> LabStatus.missing
        current == LabStatus.running && !anyRunning -> LabStatus.stopped
        current == LabStatus.stopped && anyRunning -> LabStatus.running
        else -> null
    }
}

/* ===================== preflight ===================== */

/** PRV-UI-3 — same check-row shape as migration preflight; all failures block Create. */
fun dockerPreflight(portCount: Int): PreflightResult {
    val checks = mutableListOf<PreflightCheck>()
    val docker = findDocker()
    if (docker == null) {
        checks += PreflightCheck("Docker CLI", false, "docker not found on PATH or common locations")
        return PreflightResult(false, checks)
    }
    checks += if (dockerVersionOk(docker.version)) {
        PreflightCheck("Docker CLI", true, "${docker.version.substringBefore(",")} (${docker.path})")
    } else {
        PreflightCheck("Docker CLI", false, "${docker.version} — 20.10 or newer required")
    }

    val compose = runQuick(docker.path, listOf("compose", "version"))
    checks += if (compose?.first == 0) {
        PreflightCheck("Compose v2", true)
    } else {
        PreflightCheck("Compose v2", false, "`docker compose` unavailable")
    }

    val daemon = runQuick(docker.path, listOf("info", "--format", "{{json .ServerVersion}}"))
    checks += if (daemon?.first == 0) {
        PreflightCheck("Daemon reachable", true, "engine ${daemon.second.trim().trim('"')}")
    } else {
        PreflightCheck("Daemon reachable", false, "is Docker running?")
    }

    checks += runCatching {
        allocatePorts((1..portCount).map { "probe-$it" })
        PreflightCheck("$portCount local port${if (portCount == 1) "" else "s"} reservable", true)
    }.getOrElse { PreflightCheck("Local ports", false, it.message) }

    return PreflightResult(checks.all { it.ok }, checks)
}

/** Runs a short-lived command; (exitCode, firstOutputLine), or null on spawn failure/timeout. */
private fun runQuick(binary: String, args: List<String>, timeoutSec: Long = 5): Pair<Int, String>? =
    io.mex.backup.runBounded(listOf(binary) + args, timeoutSec)?.let { (code, lines) ->
        code to lines.firstOrNull().orEmpty()
    }
