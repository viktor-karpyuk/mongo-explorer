package io.mex.backup

import io.mex.data.BackupScope
import java.util.concurrent.TimeUnit

/* ===================== tool discovery ===================== */

data class ToolInfo(val path: String, val version: String)

/**
 * Locates a MongoDB Database Tools binary (mongodump/mongorestore ship separately from
 * mongosh). PATH first, then the usual install prefixes.
 */
fun findTool(name: String): ToolInfo? {
    val home = System.getProperty("user.home")
    val candidates = listOf(
        name,
        "/opt/homebrew/bin/$name",
        "/usr/local/bin/$name",
        "/usr/bin/$name",
        "$home/bin/$name",
    )
    for (candidate in candidates) {
        val version = runCatching {
            val p = ProcessBuilder(candidate, "--version").redirectErrorStream(true).start()
            val out = p.inputStream.bufferedReader().readLine().orEmpty()
            if (!p.waitFor(5, TimeUnit.SECONDS)) { p.destroy(); return@runCatching null }
            if (p.exitValue() == 0) out else null
        }.getOrNull()
        if (version != null) return ToolInfo(candidate, version)
    }
    return null
}

/* ===================== argument builders (pure, tested) ===================== */

/** `mongodump` invocation for a scope. Layout under [outDir] is one directory per database. */
fun dumpArgs(uri: String, scope: BackupScope, gzip: Boolean, outDir: String): List<String> = buildList {
    add("--uri=$uri")
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
    uri: String,
    scope: BackupScope,
    dir: String,
    gzip: Boolean,
    drop: Boolean,
    dryRun: Boolean,
    renameDb: String? = null,
): List<String> = buildList {
    add("--uri=$uri")
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
