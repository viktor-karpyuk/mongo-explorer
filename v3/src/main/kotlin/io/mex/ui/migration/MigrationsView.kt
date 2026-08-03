package io.mex.ui.migration

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
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
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.data.ConflictPolicy
import io.mex.data.MigrationJob
import io.mex.data.MigrationNs
import io.mex.data.MigrationPhase
import io.mex.data.MigrationProgress
import io.mex.data.MigrationSpec
import io.mex.data.MigrationStatus
import io.mex.data.NsReport
import io.mex.migration.MigrationRunner
import io.mex.migration.preflight
import io.mex.mongo.ConnectionState
import io.mex.mongo.MongoRegistry
import io.mex.ui.components.ConfirmDangerDialog
import io.mex.mongo.listCollections
import io.mex.mongo.listDatabases
import io.mex.util.formatAgo
import io.mex.util.formatCount
import io.mex.util.formatDuration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

@Composable
fun MigrationsView(ctx: AppContext, registry: MongoRegistry, runner: MigrationRunner) {
    var jobs by remember { mutableStateOf<List<MigrationJob>>(emptyList()) }
    val progress = remember { mutableStateMapOf<String, MigrationProgress>() }
    var showNew by remember { mutableStateOf(false) }
    var confirmingDelete by remember { mutableStateOf<MigrationJob?>(null) }
    var confirmingRestart by remember { mutableStateOf<MigrationJob?>(null) }
    var reportFor by remember { mutableStateOf<MigrationJob?>(null) }

    fun reload() { jobs = ctx.migrations.list() }
    LaunchedEffect(Unit) { reload() }
    LaunchedEffect(Unit) {
        runner.progress.collect { ev ->
            val wasTerminal = progress[ev.jobId]?.status != ev.status
            progress[ev.jobId] = ev
            // The jobs table only changes on status transitions — reloading per batch was
            // one SQLite round-trip per 1k documents copied.
            if (wasTerminal) reload()
        }
    }

    Column(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Text("Migrations", style = MaterialTheme.typography.headlineSmall, modifier = Modifier.weight(1f))
            Button(onClick = { showNew = true }) { Text("+ New migration") }
        }
        Spacer(modifier = Modifier.height(12.dp))
        // One id→name query per reload — JobRow used to run two full SELECTs per row per
        // progress event (tens of UI-thread JDBC queries/second during a fast copy).
        val connNames = remember(jobs) { ctx.connections.list().associate { it.id to it.name } }
        if (jobs.isEmpty()) {
            Text("No migrations yet. Migrate data between two open connections, collection-by-collection.", color = MaterialTheme.colorScheme.onSurfaceVariant)
        } else {
            LazyColumn(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                items(jobs, key = { it.id }) { job ->
                    JobRow(
                        job = job,
                        progress = progress[job.id],
                        names = connNames,
                        onStart = { runner.start(job.id); reload() },
                        onPause = { runner.pause(job.id) },
                        onCancel = { runner.cancel(job.id) },
                        onRestart = { confirmingRestart = job },
                        onDelete = { confirmingDelete = job },
                        onReport = { reportFor = job },
                    )
                }
            }
        }
    }

    if (showNew) {
        NewMigrationDialog(
            ctx = ctx,
            registry = registry,
            onClose = { showNew = false },
            onCreate = { spec ->
                ctx.migrations.create(spec)
                showNew = false
                reload()
            },
        )
    }

    reportFor?.let { job -> ReportDialog(job, onClose = { reportFor = null }) }

    confirmingRestart?.let { job ->
        ConfirmDangerDialog(
            title = "Restart from the beginning?",
            text = "This discards the saved checkpoint and copies every selected namespace again " +
                "from its first document, under policy \"${policyLabel(job.spec.conflictPolicy)}\". " +
                "Use Resume instead to continue where the job stopped.",
            confirmLabel = "Restart",
            onConfirm = {
                runner.restart(job.id)
                confirmingRestart = null
                reload()
            },
            onCancel = { confirmingRestart = null },
        )
    }

    confirmingDelete?.let { job ->
        ConfirmDangerDialog(
            title = "Delete migration job?",
            text = "This removes the migration history entry from the local store. " +
                "Any data already copied to the target remains. This cannot be undone.",
            confirmLabel = "Delete",
            onConfirm = {
                ctx.migrations.delete(job.id)
                confirmingDelete = null
                reload()
            },
            onCancel = { confirmingDelete = null },
        )
    }
}

@Composable
private fun JobRow(
    job: MigrationJob,
    progress: MigrationProgress?,
    names: Map<String, String>,
    onStart: () -> Unit,
    onPause: () -> Unit,
    onCancel: () -> Unit,
    onRestart: () -> Unit,
    onDelete: () -> Unit,
    onReport: () -> Unit,
) {
    val sourceName = names[job.spec.sourceId] ?: "?"
    val targetName = names[job.spec.targetId] ?: "?"
    val statusColor = when (job.status) {
        MigrationStatus.completed -> Color(0xFF4ADE80)
        MigrationStatus.running -> Color(0xFFFBBF24)
        MigrationStatus.failed -> Color(0xFFF87171)
        MigrationStatus.cancelled -> Color(0xFFA78BFA)
        else -> MaterialTheme.colorScheme.onSurfaceVariant
    }
    val live = progress?.takeIf { job.status == MigrationStatus.running || job.status == MigrationStatus.paused }

    Card(border = CardDefaults.outlinedCardBorder()) {
        Column(modifier = Modifier.padding(12.dp).fillMaxWidth()) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(job.status.name, color = statusColor, style = MaterialTheme.typography.labelSmall, modifier = Modifier.width(90.dp))
                Column(modifier = Modifier.weight(1f)) {
                    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        Text("$sourceName → $targetName", style = MaterialTheme.typography.bodyMedium)
                        // Verdict chip once a verified job has its report (MIG-UI-3/5).
                        if (job.status == MigrationStatus.completed && job.spec.verify) {
                            job.report?.let { r ->
                                val (label, tint) = if (r.ok) "verified ✓" to Color(0xFF4ADE80) else "verify failed" to Color(0xFFFBBF24)
                                Text(
                                    label,
                                    style = MaterialTheme.typography.labelSmall,
                                    color = tint,
                                    modifier = Modifier
                                        .background(tint.copy(alpha = 0.12f), MaterialTheme.shapes.small)
                                        .padding(horizontal = 6.dp, vertical = 1.dp),
                                )
                            }
                        }
                        if (job.resumable) {
                            Text(
                                "resumable",
                                style = MaterialTheme.typography.labelSmall,
                                color = MaterialTheme.colorScheme.primary,
                                modifier = Modifier
                                    .background(MaterialTheme.colorScheme.primary.copy(alpha = 0.12f), MaterialTheme.shapes.small)
                                    .padding(horizontal = 6.dp, vertical = 1.dp),
                            )
                        }
                    }
                    val phaseLabel = when (progress?.phase) {
                        MigrationPhase.copy -> "copying"
                        MigrationPhase.indexes -> "indexes"
                        MigrationPhase.verify -> "verifying"
                        null -> null
                    }
                    Text(
                        buildString {
                            append("${job.spec.namespaces.size} namespace(s) · ${job.spec.conflictPolicy.name}")
                            live?.currentNs?.let { ns -> append(" · ${ns.db}.${ns.coll}") }
                            if (live != null && phaseLabel != null) append(" · $phaseLabel")
                            live?.let { append(" · ${formatCount(it.copied)}") }
                            live?.takeIf { it.skipped > 0 }?.let { append(" · skipped ${formatCount(it.skipped)}") }
                            live?.takeIf { it.docErrors > 0 }?.let { append(" · errors ${formatCount(it.docErrors)}") }
                        },
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        fontFamily = FontFamily.Monospace,
                    )
                    JobTimes(job)
                    job.error?.let { Text("Error: $it", color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.labelSmall) }
                }
                when (job.status) {
                    MigrationStatus.pending -> Button(onClick = onStart) { Text("Start") }
                    MigrationStatus.paused -> Button(onClick = onStart) { Text("Resume") }
                    MigrationStatus.running -> {
                        TextButton(onClick = onPause) { Text("Pause") }
                        TextButton(onClick = onCancel) { Text("Cancel") }
                    }
                    else -> {
                        // A failed/cancelled job keeps its checkpoint, so it can pick up
                        // where it stopped rather than recopying everything.
                        if (job.resumable) {
                            Button(onClick = onStart) { Text("Resume") }
                            TextButton(onClick = onRestart) { Text("Restart") }
                        } else if (job.status != MigrationStatus.completed) {
                            Button(onClick = onStart) { Text("Retry") }
                        }
                        if (job.report != null) TextButton(onClick = onReport) { Text("Report") }
                        TextButton(onClick = onDelete) { Text("Delete") }
                    }
                }
            }
            if (live != null && live.phase == MigrationPhase.copy) {
                Spacer(modifier = Modifier.height(8.dp))
                CopyProgress(live)
            }
        }
    }
}

/** Progress bar + rate + ETA for the copy phase. */
@Composable
private fun CopyProgress(p: MigrationProgress) {
    val estimated = p.estimated ?: 0L
    val fraction = if (estimated > 0) (p.copied.toDouble() / estimated).coerceIn(0.0, 1.0).toFloat() else null
    Column(verticalArrangement = Arrangement.spacedBy(3.dp)) {
        if (fraction != null) {
            LinearProgressIndicator(progress = { fraction }, modifier = Modifier.fillMaxWidth())
        } else {
            LinearProgressIndicator(modifier = Modifier.fillMaxWidth())
        }
        val remaining = if (estimated > p.copied) estimated - p.copied else 0L
        val eta = p.docsPerSecond?.takeIf { it > 0 && remaining > 0 }?.let { formatDuration((remaining / it).toLong()) }
        Text(
            buildString {
                if (p.nsTotal > 0) append("namespace ${p.nsIndex + 1} of ${p.nsTotal} · ")
                append("${formatCount(p.copied)}")
                if (estimated > 0) append(" of ~${formatCount(estimated)}")
                if (fraction != null) append(" (${(fraction * 100).toInt()}%)")
                p.docsPerSecond?.let { append(" · ${formatCount(it.toLong())} docs/s") }
                eta?.let { append(" · ~$it left") }
            },
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            fontFamily = FontFamily.Monospace,
        )
    }
}

@Composable
private fun JobTimes(job: MigrationJob) {
    val parts = buildList {
        job.startedAt?.let { add("started ${formatAgo(it)}") }
        job.finishedAt?.let { f ->
            add("finished ${formatAgo(f)}")
            job.startedAt?.let { s -> if (f > s) add("took ${formatDuration((f - s) / 1000)}") }
        }
    }
    if (parts.isEmpty()) return
    Text(
        parts.joinToString(" · "),
        style = MaterialTheme.typography.labelSmall,
        color = MaterialTheme.colorScheme.onSurfaceVariant,
    )
}

/** Per-namespace verification report (MIG-UI-4). Read-only. */
@Composable
private fun ReportDialog(job: MigrationJob, onClose: () -> Unit) {
    val report = job.report ?: return
    val clipboard = LocalClipboardManager.current
    AlertDialog(
        onDismissRequest = onClose,
        confirmButton = { TextButton(onClick = onClose) { Text("Close") } },
        dismissButton = {
            TextButton(onClick = { clipboard.setText(AnnotatedString(reportAsText(job))) }) { Text("Copy report") }
        },
        title = {
            Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Text("Migration report", style = MaterialTheme.typography.titleMedium)
                val (label, tint) = if (report.ok) "PASSED" to Color(0xFF4ADE80) else "FAILED" to Color(0xFFF87171)
                Text(label, style = MaterialTheme.typography.labelSmall, color = tint)
                Spacer(modifier = Modifier.weight(1f))
                Text(
                    "policy ${job.spec.conflictPolicy.name}",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
        },
        text = {
            SelectionContainer {
                Column(modifier = Modifier.width(760.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
                    Row {
                        ReportHeaderCell("NAMESPACE", Modifier.weight(1f))
                        ReportHeaderCell("SOURCE", Modifier.width(90.dp))
                        ReportHeaderCell("TARGET", Modifier.width(90.dp))
                        ReportHeaderCell("SKIP", Modifier.width(60.dp))
                        ReportHeaderCell("ERR", Modifier.width(50.dp))
                        ReportHeaderCell("INDEXES", Modifier.width(70.dp))
                        ReportHeaderCell("RESULT", Modifier.width(56.dp))
                    }
                    HorizontalDivider()
                    Column(modifier = Modifier.heightIn(max = 420.dp).verticalScroll(rememberScrollState())) {
                        report.rows.forEach { row -> ReportRow(row) }
                    }
                }
            }
        },
    )
}

private fun reportAsText(job: MigrationJob): String {
    val r = job.report ?: return ""
    return buildString {
        appendLine("Migration report — ${if (r.ok) "PASSED" else "FAILED"} (policy ${job.spec.conflictPolicy.name})")
        r.rows.forEach { row ->
            appendLine(
                "${row.db}.${row.coll}\tsource=${row.sourceCount}\ttarget=${row.targetCount}" +
                    "\tskipped=${row.skipped}\terrors=${row.errors}\tindexes=${row.indexesCopied}" +
                    "\t${if (row.ok) "OK" else "FAIL"}",
            )
            row.missingIndexes.forEach { appendLine("  missing index: $it") }
            row.errorSamples.forEach { appendLine("  $it") }
        }
    }
}

@Composable
private fun ReportHeaderCell(text: String, modifier: Modifier) {
    Text(text, style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant, modifier = modifier)
}

@Composable
private fun ReportRow(row: NsReport) {
    fun count(v: Long) = if (v < 0) "—" else formatCount(v)
    Column(modifier = Modifier.padding(vertical = 4.dp)) {
        Row {
            Text("${row.db}.${row.coll}", style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace, modifier = Modifier.weight(1f))
            Text(count(row.sourceCount), style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace, modifier = Modifier.width(90.dp))
            Text(count(row.targetCount), style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace, modifier = Modifier.width(90.dp))
            Text(count(row.skipped), style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace, modifier = Modifier.width(60.dp))
            Text(count(row.errors), style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace, modifier = Modifier.width(50.dp))
            Text(
                if (row.missingIndexes.isEmpty()) "${row.indexesCopied} ✓" else "${row.indexesCopied} of ${row.indexesCopied + row.missingIndexes.size}",
                style = MaterialTheme.typography.bodySmall,
                fontFamily = FontFamily.Monospace,
                modifier = Modifier.width(70.dp),
            )
            Text(
                if (row.ok) "✓" else "✗",
                style = MaterialTheme.typography.bodySmall,
                color = if (row.ok) Color(0xFF4ADE80) else Color(0xFFF87171),
                modifier = Modifier.width(56.dp),
            )
        }
        row.missingIndexes.forEach {
            Text("  missing index: $it", style = MaterialTheme.typography.labelSmall, color = Color(0xFFFBBF24))
        }
        row.errorSamples.forEach {
            Text("  $it", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.error)
        }
    }
}

@Composable
private fun NewMigrationDialog(
    ctx: AppContext,
    registry: MongoRegistry,
    onClose: () -> Unit,
    onCreate: (MigrationSpec) -> Unit,
) {
    val states by registry.states.collectAsState()
    val list = remember(states) {
        ctx.connections.list().filter { states[it.id] is ConnectionState.Connected }
    }
    // Reading from a read-only connection is fine; writing into one is exactly what the
    // flag exists to prevent, so those never appear as targets (DBA-RO-1).
    val targetList = remember(list) { list.filter { !it.readOnly } }
    var sourceId by remember { mutableStateOf(list.firstOrNull()?.id ?: "") }
    var targetId by remember {
        mutableStateOf(
            targetList.firstOrNull { it.id != sourceId }?.id ?: targetList.firstOrNull()?.id ?: "",
        )
    }
    var sourceMenu by remember { mutableStateOf(false) }
    var targetMenu by remember { mutableStateOf(false) }
    var dbs by remember { mutableStateOf<List<String>>(emptyList()) }
    val colls = remember { mutableStateMapOf<String, List<String>>() }
    val selected = remember { mutableStateMapOf<String, Boolean>() } // "db.coll" -> true
    var preflightResult by remember { mutableStateOf<io.mex.data.PreflightResult?>(null) }
    var busy by remember { mutableStateOf(false) }
    var policy by remember { mutableStateOf(ConflictPolicy.abort) }
    var policyMenu by remember { mutableStateOf(false) }
    var copyIndexes by remember { mutableStateOf(true) }
    var copyOptions by remember { mutableStateOf(true) }
    var readFromSecondary by remember { mutableStateOf(false) }
    var verify by remember { mutableStateOf(true) }
    var dropConfirmed by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    val namespaces = selected.entries.filter { it.value }.map {
        val parts = it.key.split(".", limit = 2)
        MigrationNs(parts[0], parts[1])
    }
    // Any change to what would be checked invalidates a previous pass (MIG-UI-6).
    val preflightKey = "$sourceId|$targetId|$policy|" + namespaces.joinToString { "${it.db}.${it.coll}" }
    var preflightFor by remember { mutableStateOf("") }
    val preflightStale = preflightResult != null && preflightFor != preflightKey

    LaunchedEffect(sourceId) {
        // A different source invalidates everything loaded/ticked for the old one —
        // otherwise the tree shows source A's collections under source B and the spec
        // can name namespaces that don't exist there.
        colls.clear()
        selected.clear()
        dbs = emptyList()
        val client = registry.client(sourceId) ?: return@LaunchedEffect
        dbs = withContext(Dispatchers.IO) { listDatabases(client).map { it.name } }
    }

    AlertDialog(
        onDismissRequest = onClose,
        title = { Text("New migration", style = MaterialTheme.typography.titleMedium) },
        confirmButton = {
            Button(
                onClick = {
                    onCreate(
                        MigrationSpec(
                            sourceId, targetId, namespaces,
                            conflictPolicy = policy,
                            copyIndexes = copyIndexes,
                            verify = verify,
                            copyCollectionOptions = copyOptions,
                            readFromSecondary = readFromSecondary,
                        ),
                    )
                },
                enabled = preflightResult?.ok == true && !preflightStale && !busy &&
                    (policy != ConflictPolicy.drop || dropConfirmed),
            ) { Text("Create migration") }
        },
        dismissButton = { TextButton(onClick = onClose) { Text("Cancel") } },
        text = {
            Column(
                modifier = Modifier.width(720.dp).heightIn(max = 560.dp).verticalScroll(rememberScrollState()),
                verticalArrangement = Arrangement.spacedBy(10.dp),
            ) {
                Row(horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                    Box(modifier = Modifier.weight(1f)) {
                        PickConnection("Source", sourceId, sourceMenu, { sourceMenu = it }, list) { sourceId = it }
                    }
                    Box(modifier = Modifier.weight(1f)) {
                        PickConnection("Target", targetId, targetMenu, { targetMenu = it }, targetList) { targetId = it }
                    }
                }
                if (targetList.isEmpty()) {
                    Text(
                        "No writable target — every open connection is read-only.",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.error,
                    )
                }
                if (sourceId == targetId && sourceId.isNotEmpty()) {
                    Text(
                        "Source and target are the same connection — documents will be copied onto themselves.",
                        style = MaterialTheme.typography.labelSmall,
                        color = Color(0xFFB45309),
                    )
                }

                Text("Select namespaces", style = MaterialTheme.typography.titleSmall)
                Column(modifier = Modifier.heightIn(max = 240.dp).verticalScroll(rememberScrollState())) {
                    dbs.forEach { db ->
                        var open by remember(db) { mutableStateOf(false) }
                        suspend fun loadColls() {
                            val client = registry.client(sourceId) ?: return
                            if (colls[db] == null) {
                                colls[db] = withContext(Dispatchers.IO) { listCollections(client, db).map { it.name } }
                            }
                        }
                        Row(verticalAlignment = Alignment.CenterVertically) {
                            TextButton(onClick = {
                                open = !open
                                if (open) scope.launch { loadColls() }
                            }) { Text(if (open) "▾ $db" else "▸ $db", fontFamily = FontFamily.Monospace) }
                            if (open) {
                                val all = colls[db].orEmpty()
                                val allOn = all.isNotEmpty() && all.all { selected["$db.$it"] == true }
                                TextButton(onClick = {
                                    all.forEach { selected["$db.$it"] = !allOn }
                                }) {
                                    Text(
                                        if (allOn) "clear all" else "select all",
                                        style = MaterialTheme.typography.labelSmall,
                                    )
                                }
                            }
                        }
                        if (open) {
                            colls[db]?.forEach { col ->
                                val key = "$db.$col"
                                Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(start = 16.dp)) {
                                    Checkbox(checked = selected[key] == true, onCheckedChange = { selected[key] = it })
                                    Text(col, style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace)
                                }
                            }
                        }
                    }
                }
                Text(
                    "${namespaces.size} namespace(s) selected",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )

                // Options — MIG-UI-1.
                Text("Options", style = MaterialTheme.typography.titleSmall)
                Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(10.dp)) {
                    Text("ON CONFLICT", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                    Box {
                        OutlinedButton(onClick = { policyMenu = true }) { Text(policyLabel(policy)) }
                        DropdownMenu(expanded = policyMenu, onDismissRequest = { policyMenu = false }) {
                            ConflictPolicy.entries.forEach { p ->
                                DropdownMenuItem(
                                    text = { Column { Text(policyLabel(p)); Text(policyHint(p), style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant) } },
                                    onClick = { policyMenu = false; policy = p; if (p != ConflictPolicy.drop) dropConfirmed = false },
                                )
                            }
                        }
                    }
                    Text(policyHint(policy), style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                }
                OptionCheck(copyIndexes, { copyIndexes = it }, "Copy indexes — recreate secondary indexes on the target after documents.")
                OptionCheck(copyOptions, { copyOptions = it }, "Copy collection options — replay validators, collation, capped and time-series settings.")
                OptionCheck(verify, { verify = it }, "Verify after copy — compare document counts and index definitions when the copy finishes.")
                OptionCheck(readFromSecondary, { readFromSecondary = it }, "Read from secondary — use secondaryPreferred so the copy doesn't load a live primary.")
                if (policy == ConflictPolicy.drop) {
                    // MIG-CONFLICT-5 — Create stays disabled until the consequence is acknowledged.
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Checkbox(checked = dropConfirmed, onCheckedChange = { dropConfirmed = it })
                        Text(
                            "I understand each selected target collection will be dropped before copying.",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.error,
                        )
                    }
                }

                if (preflightStale) {
                    Text(
                        "Selection changed — run preflight again.",
                        style = MaterialTheme.typography.labelSmall,
                        color = Color(0xFFB45309),
                    )
                }
                preflightResult?.takeIf { !preflightStale }?.let { r ->
                    Surface(color = if (r.ok) MaterialTheme.colorScheme.primaryContainer else MaterialTheme.colorScheme.errorContainer) {
                        Column(modifier = Modifier.padding(8.dp)) {
                            Text(if (r.ok) "Preflight passed." else "Preflight has failures.", style = MaterialTheme.typography.labelMedium)
                            r.checks.forEach {
                                val mark = when { it.warn -> "⚠"; it.ok -> "✓"; else -> "✗" }
                                Text(
                                    "$mark ${it.name}${it.detail?.let { d -> " — $d" }.orEmpty()}",
                                    style = MaterialTheme.typography.labelSmall,
                                    fontFamily = FontFamily.Monospace,
                                    color = when {
                                        it.warn -> Color(0xFFB45309)
                                        it.ok -> Color.Unspecified
                                        else -> MaterialTheme.colorScheme.error
                                    },
                                )
                            }
                        }
                    }
                }

                OutlinedButton(
                    onClick = {
                        scope.launch {
                            busy = true
                            try {
                                val s = registry.client(sourceId) ?: return@launch
                                val t = registry.client(targetId) ?: return@launch
                                preflightResult = withContext(Dispatchers.IO) { preflight(s, t, namespaces, policy) }
                                preflightFor = preflightKey
                            } finally { busy = false }
                        }
                    },
                    enabled = namespaces.isNotEmpty() && !busy,
                ) { Text(if (busy) "Checking…" else "Preflight") }
            }
        },
    )
}

@Composable
private fun OptionCheck(checked: Boolean, onChange: (Boolean) -> Unit, label: String) {
    Row(verticalAlignment = Alignment.CenterVertically) {
        Checkbox(checked = checked, onCheckedChange = onChange)
        Text(label, style = MaterialTheme.typography.bodySmall)
    }
}

private fun policyLabel(p: ConflictPolicy): String = when (p) {
    ConflictPolicy.abort -> "Abort on conflict"
    ConflictPolicy.append -> "Append (target wins)"
    ConflictPolicy.upsert -> "Upsert (source wins)"
    ConflictPolicy.drop -> "Drop target first"
}

private fun policyHint(p: ConflictPolicy): String = when (p) {
    ConflictPolicy.abort -> "Stop the job on the first duplicate _id."
    ConflictPolicy.append -> "Keep existing target documents; skip duplicates."
    ConflictPolicy.upsert -> "Overwrite target documents with the same _id."
    ConflictPolicy.drop -> "Drop each target collection before copying."
}

@Composable
private fun PickConnection(
    label: String,
    selectedId: String,
    open: Boolean,
    onOpen: (Boolean) -> Unit,
    options: List<io.mex.data.ConnectionSummary>,
    onSelect: (String) -> Unit,
) {
    Column {
        Text(label.uppercase(), style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
        Box {
            OutlinedButton(onClick = { onOpen(true) }) {
                Text(options.firstOrNull { it.id == selectedId }?.name ?: "(none)")
            }
            DropdownMenu(expanded = open, onDismissRequest = { onOpen(false) }) {
                options.forEach { c ->
                    DropdownMenuItem(text = { Text(c.name) }, onClick = { onOpen(false); onSelect(c.id) })
                }
            }
        }
    }
}
