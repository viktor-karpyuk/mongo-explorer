package io.mex.backup

import io.mex.AppContext
import io.mex.data.BackupEntry
import io.mex.data.BackupScope
import io.mex.data.BackupStatus
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.launch
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

sealed class BackupEvent {
    abstract val backupId: String

    data class Log(override val backupId: String, val line: String) : BackupEvent()
    data class Done(override val backupId: String, val status: BackupStatus) : BackupEvent()
}

/** One spawned tool process whose output streams line-by-line to [onLine]. */
class ToolProcess(
    private val binary: String,
    private val args: List<String>,
    private val scope: CoroutineScope,
    private val onLine: (String) -> Unit,
    private val onExit: (Int) -> Unit,
) {
    @Volatile
    private var process: Process? = null

    @Volatile
    var cancelled = false
        private set

    fun start(): Boolean = runCatching {
        val p = ProcessBuilder(listOf(binary) + args).redirectErrorStream(true).start()
        process = p
        scope.launch {
            p.inputStream.bufferedReader().useLines { lines ->
                lines.forEach(onLine)
            }
        }
        scope.launch { onExit(p.waitFor()) }
        true
    }.getOrElse {
        onLine("failed to spawn $binary: ${it.message}")
        onExit(127)
        false
    }

    fun cancel() {
        cancelled = true
        process?.destroy()
    }
}

/**
 * Orchestrates `mongodump` runs and keeps the catalog honest (DBA-BKP-1/2).
 * Restores run through [ToolProcess] directly from the restore dialog — they stream to a
 * dialog-scoped log and don't create catalog rows.
 */
class BackupRunner(private val ctx: AppContext, private val backupsDir: Path) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val running = ConcurrentHashMap<String, ToolProcess>()

    private val _events = MutableSharedFlow<BackupEvent>(extraBufferCapacity = 1024)
    val events: SharedFlow<BackupEvent> = _events

    fun isRunning(backupId: String): Boolean = running.containsKey(backupId)

    /** Starts a dump; returns the catalog id, or null when spawning failed outright. */
    fun startDump(connectionId: String, scope: BackupScope, gzip: Boolean): String? {
        val record = ctx.connections.get(connectionId) ?: return null
        val tool = findTool("mongodump") ?: return null

        val entry = ctx.backups.create(
            connectionId = connectionId,
            connectionName = record.name,
            scope = scope,
            gzip = gzip,
            path = backupsDir.resolve("pending").toString(),
        )
        val dir = backupsDir.resolve(entry.id)
        Files.createDirectories(dir)
        // The row is created before the path is known (the path embeds the row's id).
        ctx.store.conn.prepareStatement("UPDATE backups SET path = ? WHERE id = ?").use { ps ->
            ps.setString(1, dir.toString())
            ps.setString(2, entry.id)
            ps.executeUpdate()
        }

        val proc = ToolProcess(
            binary = tool.path,
            args = dumpArgs(record.uri, scope, gzip, dir.toString()),
            scope = this.scope,
            onLine = { _events.tryEmit(BackupEvent.Log(entry.id, it)) },
            onExit = { code ->
                val cancelled = running.remove(entry.id)?.cancelled == true
                val status = when {
                    cancelled -> BackupStatus.cancelled
                    code == 0 -> BackupStatus.completed
                    else -> BackupStatus.failed
                }
                ctx.backups.finish(
                    id = entry.id,
                    status = status,
                    error = if (status == BackupStatus.failed) "mongodump exited with code $code" else null,
                    sizeBytes = directorySize(dir.toFile()),
                )
                _events.tryEmit(BackupEvent.Done(entry.id, status))
            },
        )
        running[entry.id] = proc
        if (!proc.start()) {
            running.remove(entry.id)
            ctx.backups.finish(entry.id, BackupStatus.failed, "could not spawn mongodump")
            return null
        }
        return entry.id
    }

    fun cancel(backupId: String) {
        running[backupId]?.cancel()
    }

    /** Removes the catalog row and the dump files it points at. */
    fun delete(entry: BackupEntry) {
        running[entry.id]?.cancel()
        ctx.backups.delete(entry.id)
        val dir = File(entry.path)
        // Only ever delete inside the managed backups directory — never a user path.
        if (dir.toPath().startsWith(backupsDir)) dir.deleteRecursively()
    }

    private fun directorySize(dir: File): Long =
        dir.walkBottomUp().filter { it.isFile }.sumOf { it.length() }
}
