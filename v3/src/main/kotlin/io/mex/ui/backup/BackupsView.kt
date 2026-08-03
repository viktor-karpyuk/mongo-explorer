package io.mex.ui.backup

import androidx.compose.foundation.VerticalScrollbar
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.rememberScrollbarAdapter
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.selection.SelectionContainer
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.backup.BackupEvent
import io.mex.backup.BackupRunner
import io.mex.backup.ToolProcess
import io.mex.backup.findTool
import io.mex.backup.restoreArgs
import io.mex.data.BackupEntry
import io.mex.data.BackupScope
import io.mex.data.BackupStatus
import io.mex.mongo.ConnectionState
import io.mex.mongo.MongoRegistry
import io.mex.mongo.listCollections
import io.mex.mongo.listDatabases
import io.mex.ui.components.ConfirmDangerDialog
import io.mex.util.formatAgo
import io.mex.util.formatBytes
import io.mex.util.formatDuration
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

private const val LOG_TAIL = 300

/** Backup catalog + mongodump/mongorestore orchestration (DBA-BKP-1..3). */
@Composable
fun BackupsView(ctx: AppContext, registry: MongoRegistry, runner: BackupRunner) {
    var entries by remember { mutableStateOf<List<BackupEntry>>(emptyList()) }
    val logs = remember { mutableStateMapOf<String, List<String>>() }
    var showNew by remember { mutableStateOf(false) }
    var restoring by remember { mutableStateOf<BackupEntry?>(null) }
    var deleting by remember { mutableStateOf<BackupEntry?>(null) }
    var expandedLog by remember { mutableStateOf<String?>(null) }

    fun reload() { entries = ctx.backups.list() }
    LaunchedEffect(Unit) { reload() }
    LaunchedEffect(Unit) {
        runner.events.collect { ev ->
            when (ev) {
                is BackupEvent.Log ->
                    logs[ev.backupId] = (logs[ev.backupId].orEmpty() + ev.line).takeLast(LOG_TAIL)
                is BackupEvent.Done -> reload()
            }
        }
    }

    // Tool discovery spawns `--version` subprocesses — off the UI thread (it blocked the
    // first composition of this view for hundreds of ms). Pair tracks probe completion so
    // the "not found" guidance doesn't flash while the probe is still running.
    val probe by produceState<Pair<Boolean, io.mex.backup.ToolInfo?>>(initialValue = false to null) {
        value = true to withContext(Dispatchers.IO) { findTool("mongodump") }
    }
    val (probed, tool) = probe

    Column(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Text("Backups", style = MaterialTheme.typography.headlineSmall, modifier = Modifier.weight(1f))
            Button(onClick = { showNew = true }, enabled = tool != null) { Text("+ New backup") }
        }
        when {
            !probed -> Text(
                "Locating mongodump…",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            tool == null -> Text(
                "mongodump not found — install the MongoDB Database Tools " +
                    "(brew install mongodb-database-tools) and reopen this view.",
                color = MaterialTheme.colorScheme.error,
                style = MaterialTheme.typography.bodySmall,
            )
            else -> Text(
                tool.version,
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
        Spacer(modifier = Modifier.height(12.dp))
        if (entries.isEmpty()) {
            Text(
                "No backups yet. Dumps are stored under ${ctx.backupsDir} and tracked here.",
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                style = MaterialTheme.typography.bodySmall,
            )
        }
        val listState = rememberLazyListState()
        Box(modifier = Modifier.weight(1f)) {
            LazyColumn(state = listState, verticalArrangement = Arrangement.spacedBy(8.dp)) {
                items(entries, key = { it.id }) { entry ->
                    BackupRow(
                        entry = entry,
                        running = runner.isRunning(entry.id),
                        log = logs[entry.id].orEmpty(),
                        logExpanded = expandedLog == entry.id,
                        onToggleLog = { expandedLog = if (expandedLog == entry.id) null else entry.id },
                        onCancel = { runner.cancel(entry.id) },
                        onRestore = { restoring = entry },
                        onDelete = { deleting = entry },
                    )
                }
            }
            VerticalScrollbar(
                adapter = rememberScrollbarAdapter(listState),
                modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
            )
        }
    }

    if (showNew) {
        NewBackupDialog(
            ctx = ctx,
            registry = registry,
            onClose = { showNew = false },
            onStart = { connectionId, scope, gzip ->
                showNew = false
                runner.startDump(connectionId, scope, gzip)
                reload()
            },
        )
    }
    restoring?.let { entry ->
        RestoreDialog(ctx, registry, entry, onClose = { restoring = null })
    }
    deleting?.let { entry ->
        ConfirmDangerDialog(
            title = "Delete backup?",
            text = "This removes the catalog entry AND deletes the dump files at\n${entry.path}\nThis cannot be undone.",
            confirmLabel = "Delete backup",
            onConfirm = {
                runner.delete(entry)
                deleting = null
                reload()
            },
            onCancel = { deleting = null },
        )
    }
}

@Composable
private fun BackupRow(
    entry: BackupEntry,
    running: Boolean,
    log: List<String>,
    logExpanded: Boolean,
    onToggleLog: () -> Unit,
    onCancel: () -> Unit,
    onRestore: () -> Unit,
    onDelete: () -> Unit,
) {
    val clipboard = LocalClipboardManager.current
    val statusColor = when (entry.status) {
        BackupStatus.completed -> Color(0xFF4ADE80)
        BackupStatus.running -> Color(0xFFFBBF24)
        BackupStatus.failed -> Color(0xFFF87171)
        BackupStatus.cancelled -> Color(0xFFA78BFA)
    }
    Card(border = CardDefaults.outlinedCardBorder()) {
        Column(modifier = Modifier.padding(12.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(entry.status.name, color = statusColor, style = MaterialTheme.typography.labelSmall, modifier = Modifier.width(90.dp))
                Column(modifier = Modifier.weight(1f)) {
                    Text(
                        "${entry.connectionName} · ${entry.scope.label}",
                        style = MaterialTheme.typography.bodyMedium,
                    )
                    Text(
                        buildString {
                            append(formatAgo(entry.startedAt))
                            entry.finishedAt?.let { f ->
                                if (f > entry.startedAt) append(" · took ${formatDuration((f - entry.startedAt) / 1000)}")
                            }
                            entry.sizeBytes?.let { append(" · ${formatBytes(it)}") }
                            if (entry.gzip) append(" · gzip")
                        },
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        fontFamily = FontFamily.Monospace,
                    )
                    entry.error?.let {
                        Text("Error: $it", color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.labelSmall)
                    }
                }
                if (running) {
                    TextButton(onClick = onCancel) { Text("Cancel") }
                } else {
                    if (entry.status == BackupStatus.completed) {
                        Button(onClick = onRestore) { Text("Restore…") }
                    }
                    TextButton(onClick = { clipboard.setText(AnnotatedString(entry.path)) }) { Text("Copy path") }
                    TextButton(onClick = onDelete) { Text("Delete") }
                }
                if (log.isNotEmpty()) {
                    TextButton(onClick = onToggleLog) { Text(if (logExpanded) "Hide log" else "Log") }
                }
            }
            if (running || logExpanded) {
                LogPane(log, maxHeight = 160.dp)
            }
        }
    }
}

@Composable
private fun LogPane(lines: List<String>, maxHeight: androidx.compose.ui.unit.Dp) {
    val scroll = rememberScrollState()
    LaunchedEffect(lines.size) { scroll.scrollTo(scroll.maxValue) }
    Surface(
        color = Color(0xFF0A0C10),
        shape = RoundedCornerShape(4.dp),
        modifier = Modifier.fillMaxWidth().heightIn(max = maxHeight).padding(top = 6.dp),
    ) {
        SelectionContainer {
            Column(modifier = Modifier.padding(8.dp).verticalScroll(scroll)) {
                Text(
                    lines.joinToString("\n"),
                    color = Color(0xFFD4D7DF),
                    fontFamily = FontFamily.Monospace,
                    style = MaterialTheme.typography.labelSmall,
                )
            }
        }
    }
}

/* ============================ new backup ============================ */

@Composable
private fun NewBackupDialog(
    ctx: AppContext,
    registry: MongoRegistry,
    onClose: () -> Unit,
    onStart: (connectionId: String, scope: BackupScope, gzip: Boolean) -> Unit,
) {
    val states by registry.states.collectAsState()
    // A dump only reads, so read-only connections are legitimate sources.
    val open = remember(states) { ctx.connections.list().filter { states[it.id] is ConnectionState.Connected } }
    var connId by remember { mutableStateOf(open.firstOrNull()?.id ?: "") }
    var connMenu by remember { mutableStateOf(false) }
    var mode by remember { mutableStateOf("database") } // full | database | collection
    var dbs by remember { mutableStateOf<List<String>>(emptyList()) }
    var colls by remember { mutableStateOf<List<String>>(emptyList()) }
    var db by remember { mutableStateOf("") }
    var coll by remember { mutableStateOf("") }
    var dbMenu by remember { mutableStateOf(false) }
    var collMenu by remember { mutableStateOf(false) }
    var gzip by remember { mutableStateOf(true) }

    LaunchedEffect(connId) {
        val client = registry.client(connId) ?: return@LaunchedEffect
        dbs = withContext(Dispatchers.IO) { listDatabases(client).map { it.name } }
        db = dbs.firstOrNull().orEmpty()
    }
    LaunchedEffect(connId, db) {
        if (db.isBlank()) return@LaunchedEffect
        val client = registry.client(connId) ?: return@LaunchedEffect
        colls = withContext(Dispatchers.IO) { listCollections(client, db).map { it.name } }
        coll = colls.firstOrNull().orEmpty()
    }

    val scopeReady = when (mode) {
        "full" -> true
        "database" -> db.isNotBlank()
        else -> db.isNotBlank() && coll.isNotBlank()
    }

    AlertDialog(
        onDismissRequest = onClose,
        title = { Text("New backup", style = MaterialTheme.typography.titleMedium) },
        confirmButton = {
            Button(
                enabled = connId.isNotBlank() && scopeReady,
                onClick = {
                    val scope = when (mode) {
                        "full" -> BackupScope()
                        "database" -> BackupScope(db)
                        else -> BackupScope(db, coll)
                    }
                    onStart(connId, scope, gzip)
                },
            ) { Text("Start dump") }
        },
        dismissButton = { TextButton(onClick = onClose) { Text("Cancel") } },
        text = {
            Column(modifier = Modifier.width(460.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
                Box {
                    OutlinedButton(onClick = { connMenu = true }) {
                        Text("Source: ${open.firstOrNull { it.id == connId }?.name ?: "(none)"}")
                    }
                    DropdownMenu(expanded = connMenu, onDismissRequest = { connMenu = false }) {
                        open.forEach { c ->
                            DropdownMenuItem(text = { Text(c.name) }, onClick = { connMenu = false; connId = c.id })
                        }
                    }
                }
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    listOf("full" to "Full deployment", "database" to "Database", "collection" to "Collection").forEach { (m, label) ->
                        FilterChip(selected = mode == m, onClick = { mode = m }, label = { Text(label, style = MaterialTheme.typography.labelMedium) })
                    }
                }
                if (mode != "full") {
                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        Box {
                            OutlinedButton(onClick = { dbMenu = true }) { Text(db.ifBlank { "database ⌄" }) }
                            DropdownMenu(expanded = dbMenu, onDismissRequest = { dbMenu = false }) {
                                dbs.forEach { d -> DropdownMenuItem(text = { Text(d) }, onClick = { dbMenu = false; db = d }) }
                            }
                        }
                        if (mode == "collection") {
                            Box {
                                OutlinedButton(onClick = { collMenu = true }) { Text(coll.ifBlank { "collection ⌄" }) }
                                DropdownMenu(expanded = collMenu, onDismissRequest = { collMenu = false }) {
                                    colls.forEach { c -> DropdownMenuItem(text = { Text(c) }, onClick = { collMenu = false; coll = c }) }
                                }
                            }
                        }
                    }
                }
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Checkbox(checked = gzip, onCheckedChange = { gzip = it })
                    Text("Compress (gzip)", style = MaterialTheme.typography.bodySmall)
                }
            }
        },
    )
}

/* ============================ restore ============================ */

@Composable
private fun RestoreDialog(
    ctx: AppContext,
    registry: MongoRegistry,
    entry: BackupEntry,
    onClose: () -> Unit,
) {
    val states by registry.states.collectAsState()
    // Restores write, so read-only connections can never be targets (DBA-RO-1).
    val targets = remember(states) {
        ctx.connections.list().filter { states[it.id] is ConnectionState.Connected && !it.readOnly }
    }
    var targetId by remember { mutableStateOf(targets.firstOrNull()?.id ?: "") }
    var targetMenu by remember { mutableStateOf(false) }
    var drop by remember { mutableStateOf(false) }
    var renameDb by remember { mutableStateOf("") }
    var log by remember { mutableStateOf<List<String>>(emptyList()) }
    var phase by remember { mutableStateOf("idle") } // idle | dry | dryOk | confirm | restoring | done | failed
    val procScope = remember { CoroutineScope(SupervisorJob() + Dispatchers.IO) }
    var proc by remember { mutableStateOf<ToolProcess?>(null) }
    // Cancel the scope too — each opened dialog leaked its SupervisorJob otherwise.
    DisposableEffect(Unit) {
        onDispose {
            proc?.cancel()
            procScope.coroutineContext[kotlinx.coroutines.Job]?.cancel()
        }
    }

    fun run(dryRun: Boolean) = procScope.launch {
        // findTool spawns --version probes; keep the click handler off the UI thread.
        val record = ctx.connections.get(targetId) ?: return@launch
        val tool = findTool("mongorestore") ?: run {
            log = log + "mongorestore not found — install the MongoDB Database Tools."
            phase = "failed"
            return@launch
        }
        log = emptyList()
        phase = if (dryRun) "dry" else "restoring"
        // Credentials via --config, never argv (PRV-SEC doctrine, applied to restores too).
        val config = io.mex.backup.writeToolConfig(record.uri)
        val args = restoreArgs(
            configPath = config.toString(),
            scope = entry.scope,
            dir = entry.path,
            gzip = entry.gzip,
            drop = drop,
            dryRun = dryRun,
            renameDb = renameDb.trim().ifBlank { null },
        )
        val p = ToolProcess(
            binary = tool.path,
            args = args,
            scope = procScope,
            onLine = { line -> log = (log + line).takeLast(LOG_TAIL) },
            onExit = { code ->
                runCatching { java.nio.file.Files.deleteIfExists(config) }
                phase = when {
                    dryRun && code == 0 -> "dryOk"
                    dryRun -> "failed"
                    code == 0 -> "done"
                    else -> "failed"
                }
                if (code != 0) log = log + "exited with code $code"
            },
        )
        proc = p
        p.start()
    }

    AlertDialog(
        onDismissRequest = { if (phase != "restoring") onClose() },
        title = { Text("Restore — ${entry.connectionName} · ${entry.scope.label}", style = MaterialTheme.typography.titleMedium) },
        confirmButton = {
            when (phase) {
                "dry", "restoring" -> TextButton(onClick = { proc?.cancel() }) { Text("Cancel run") }
                "dryOk" -> Button(onClick = { phase = "confirm" }) { Text("Restore") }
                "done" -> Button(onClick = onClose) { Text("Close") }
                else -> OutlinedButton(
                    enabled = targetId.isNotBlank(),
                    onClick = { run(dryRun = true) },
                ) { Text("Dry-run") }
            }
        },
        dismissButton = {
            if (phase != "restoring") TextButton(onClick = onClose) { Text("Close") }
        },
        text = {
            Column(modifier = Modifier.width(560.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Box {
                    OutlinedButton(onClick = { targetMenu = true }, enabled = phase == "idle" || phase == "dryOk" || phase == "failed") {
                        Text("Target: ${targets.firstOrNull { it.id == targetId }?.name ?: "(none)"}")
                    }
                    DropdownMenu(expanded = targetMenu, onDismissRequest = { targetMenu = false }) {
                        targets.forEach { c ->
                            DropdownMenuItem(text = { Text(c.name) }, onClick = { targetMenu = false; targetId = c.id; phase = "idle" })
                        }
                    }
                }
                if (targets.isEmpty()) {
                    Text(
                        "No writable target — open a connection that is not read-only.",
                        color = MaterialTheme.colorScheme.error,
                        style = MaterialTheme.typography.labelSmall,
                    )
                }
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Checkbox(checked = drop, onCheckedChange = { drop = it; phase = "idle" })
                    Text(
                        "Drop each collection before restoring it (destructive on the target)",
                        style = MaterialTheme.typography.bodySmall,
                        color = if (drop) MaterialTheme.colorScheme.error else MaterialTheme.colorScheme.onSurface,
                    )
                }
                if (entry.scope.db != null) {
                    OutlinedTextField(
                        value = renameDb,
                        onValueChange = { renameDb = it; phase = "idle" },
                        label = { Text("Restore into a different database (optional)") },
                        placeholder = { Text(entry.scope.db) },
                        singleLine = true,
                        modifier = Modifier.fillMaxWidth(),
                        textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
                    )
                }
                Text(
                    when (phase) {
                        "idle" -> "Run the dry-run first — it reads the dump and reports what would be restored without writing."
                        "dry" -> "Dry-run in progress…"
                        "dryOk" -> "Dry-run passed. Restore is now enabled."
                        "restoring" -> "Restoring…"
                        "done" -> "Restore completed."
                        "failed" -> "Run failed — see the log."
                        else -> ""
                    },
                    style = MaterialTheme.typography.labelSmall,
                    color = when (phase) {
                        "dryOk", "done" -> Color(0xFF4ADE80)
                        "failed" -> MaterialTheme.colorScheme.error
                        else -> MaterialTheme.colorScheme.onSurfaceVariant
                    },
                )
                if (log.isNotEmpty()) LogPane(log, maxHeight = 240.dp)
            }
        },
    )

    if (phase == "confirm") {
        val targetName = targets.firstOrNull { it.id == targetId }?.name ?: "?"
        ConfirmDangerDialog(
            title = "Restore into $targetName?",
            text = buildString {
                append("This writes ${entry.scope.label} from the backup into \"$targetName\"")
                renameDb.trim().ifBlank { null }?.let { append(" as database \"$it\"") }
                append(". ")
                if (drop) append("Each restored collection is DROPPED on the target first. ")
                append("This cannot be undone.")
            },
            confirmLabel = "Restore",
            onConfirm = { phase = "restoring"; run(dryRun = false) },
            onCancel = { phase = "dryOk" },
        )
    }
}
