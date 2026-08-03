package io.mex.backup

import io.mex.data.BackupScope
import java.util.concurrent.TimeUnit

/* ===================== tool discovery ===================== */

data class ToolInfo(val path: String, val version: String)

/**
 * Runs a short-lived command with a hard deadline: (exitCode, output lines), or null on
 * spawn failure or timeout. Output is drained on a daemon thread so the deadline holds
 * even when the pipe never produces a line — a read-before-waitFor would block forever
 * on a wedged daemon and defeat every caller's timeout.
 */
fun runBounded(cmd: List<String>, timeoutSec: Long): Pair<Int, List<String>>? = runCatching {
    val p = ProcessBuilder(cmd).redirectErrorStream(true).start()
    val lines = mutableListOf<String>()
    val reader = Thread {
        runCatching {
            p.inputStream.bufferedReader().forEachLine { synchronized(lines) { lines.add(it) } }
        }
    }.apply { isDaemon = true; start() }
    if (!p.waitFor(timeoutSec, TimeUnit.SECONDS)) {
        p.destroyForcibly()
        return@runCatching null
    }
    reader.join(2_000)
    p.exitValue() to synchronized(lines) { lines.toList() }
}.getOrNull()

/**
 * Locates an external binary (mongodump/mongorestore, docker, …). PATH first, then the
 * usual install prefixes; callers with tool-specific locations pass them via [extraDirs].
 */
fun findTool(name: String, extraDirs: List<String> = emptyList()): ToolInfo? {
    val home = System.getProperty("user.home")
    val dirs = listOf("/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "$home/bin") + extraDirs
    val candidates = listOf(name) + dirs.map { "$it/$name" }
    for (candidate in candidates) {
        val result = runBounded(listOf(candidate, "--version"), timeoutSec = 5)
        if (result != null && result.first == 0) {
            result.second.firstOrNull()?.takeIf { it.isNotBlank() }?.let { return ToolInfo(candidate, it) }
        }
    }
    return null
}

/* ===================== argument builders (pure, tested) ===================== */

/**
 * Writes the connection URI into an owner-only YAML config consumed via `--config` —
 * a URI in argv exposes credentials to every local process via `ps` for the whole
 * dump/restore. Callers delete the file when the tool exits.
 */
fun writeToolConfig(uri: String, dir: java.nio.file.Path? = null): java.nio.file.Path {
    val f = if (dir != null) {
        java.nio.file.Files.createTempFile(dir, "tool-", ".yaml")
    } else {
        java.nio.file.Files.createTempFile("mex-tool-", ".yaml")
    }
    runCatching {
        java.nio.file.Files.setPosixFilePermissions(
            f,
            java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"),
        )
    }
    val escaped = uri.replace("\\", "\\\\").replace("\"", "\\\"")
    java.nio.file.Files.writeString(f, "uri: \"$escaped\"\n")
    return f
}

/** `mongodump` invocation for a scope. Layout under [outDir] is one directory per database. */
fun dumpArgs(configPath: String, scope: BackupScope, gzip: Boolean, outDir: String): List<String> = buildList {
    add("--config=$configPath")
    scope.db?.let { add("--db=$it") }
    scope.coll?.let { add("--collection=$it") }
    if (gzip) add("--gzip")
    add("--out=$outDir")
}

/** The namespace filter a backup's scope maps to on restore; null restores everything. */
fun nsInclude(scope: BackupScope): String? = when {
    scope.db == null -> null
    scope.coll == null -> "${scope.db}.*"
    else -> "${scope.db}.${scope.coll}"
}

/**
 * `mongorestore` invocation. [dryRun] maps to the tool's own `--dryRun` — the doctrine's
 * preview step, which reads the archive and reports what would be restored without
 * writing. [renameDb] remaps the backup's database onto another name via nsFrom/nsTo.
 */
fun restoreArgs(
    configPath: String,
    scope: BackupScope,
    dir: String,
    gzip: Boolean,
    drop: Boolean,
    dryRun: Boolean,
    renameDb: String? = null,
): List<String> = buildList {
    add("--config=$configPath")
    nsInclude(scope)?.let { add("--nsInclude=$it") }
    if (renameDb != null && scope.db != null) {
        if (scope.coll == null) {
            add("--nsFrom=${scope.db}.*")
            add("--nsTo=$renameDb.*")
        } else {
            add("--nsFrom=${scope.db}.${scope.coll}")
            add("--nsTo=$renameDb.${scope.coll}")
        }
    }
    if (gzip) add("--gzip")
    if (drop) add("--drop")
    if (dryRun) {
        add("--dryRun")
        // --dryRun only reports at verbose level; without this the preview looks empty.
        add("--verbose")
    }
    add("--dir=$dir")
}
