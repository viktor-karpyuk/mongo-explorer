package io.mex.ui.migration

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Dialog
import io.mex.AppContext
import io.mex.data.MigrationJob
import io.mex.data.MigrationNs
import io.mex.data.MigrationProgress
import io.mex.data.MigrationSpec
import io.mex.data.MigrationStatus
import io.mex.migration.MigrationRunner
import io.mex.migration.preflight
import io.mex.mongo.ConnectionState
import io.mex.mongo.MongoRegistry
import io.mex.ui.components.ConfirmDangerDialog
import io.mex.mongo.listCollections
import io.mex.mongo.listDatabases
import io.mex.util.formatCount
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

@Composable
fun MigrationsView(ctx: AppContext, registry: MongoRegistry, runner: MigrationRunner) {
    var jobs by remember { mutableStateOf<List<MigrationJob>>(emptyList()) }
    val progress = remember { mutableStateMapOf<String, MigrationProgress>() }
    var showNew by remember { mutableStateOf(false) }
    var confirmingDelete by remember { mutableStateOf<MigrationJob?>(null) }
    val scope = rememberCoroutineScope()

    fun reload() { jobs = ctx.migrations.list() }
    LaunchedEffect(Unit) { reload() }
    LaunchedEffect(Unit) {
        runner.progress.collect { ev ->
            progress[ev.jobId] = ev
            reload()
        }
    }

    Column(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Text("Migrations", style = MaterialTheme.typography.headlineSmall, modifier = Modifier.weight(1f))
            Button(onClick = { showNew = true }) { Text("+ New migration") }
        }
        Spacer(modifier = Modifier.height(12.dp))
        if (jobs.isEmpty()) {
            Text("No migrations yet. Migrate data between two open connections, collection-by-collection.", color = MaterialTheme.colorScheme.onSurfaceVariant)
        } else {
            LazyColumn(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                items(jobs) { job ->
                    JobRow(
                        job = job,
                        progress = progress[job.id],
                        connections = ctx,
                        onStart = { runner.start(job.id) },
                        onPause = { runner.pause(job.id) },
                        onCancel = { runner.cancel(job.id) },
                        onDelete = { confirmingDelete = job },
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
    connections: AppContext,
    onStart: () -> Unit,
    onPause: () -> Unit,
    onCancel: () -> Unit,
    onDelete: () -> Unit,
) {
    val sourceName = connections.connections.list().find { it.id == job.spec.sourceId }?.name ?: "?"
    val targetName = connections.connections.list().find { it.id == job.spec.targetId }?.name ?: "?"
    val statusColor = when (job.status) {
        MigrationStatus.completed -> Color(0xFF4ADE80)
        MigrationStatus.running -> Color(0xFFFBBF24)
        MigrationStatus.failed -> Color(0xFFF87171)
        else -> MaterialTheme.colorScheme.onSurfaceVariant
    }

    Card(border = CardDefaults.outlinedCardBorder()) {
        Row(modifier = Modifier.padding(12.dp).fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) {
            Text(job.status.name, color = statusColor, style = MaterialTheme.typography.labelSmall, modifier = Modifier.width(90.dp))
            Column(modifier = Modifier.weight(1f)) {
                Text("$sourceName → $targetName", style = MaterialTheme.typography.bodyMedium)
                Text(
                    "${job.spec.namespaces.size} namespace(s)" +
                        progress?.let { " · ${it.currentNs?.let { ns -> "${ns.db}.${ns.coll}" } ?: ""} · ${formatCount(it.copied)}" }.orEmpty(),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    fontFamily = FontFamily.Monospace,
                )
                job.error?.let { Text("Error: $it", color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.labelSmall) }
            }
            when (job.status) {
                MigrationStatus.pending, MigrationStatus.paused -> Button(onClick = onStart) { Text("Start") }
                MigrationStatus.running -> {
                    TextButton(onClick = onPause) { Text("Pause") }
                    TextButton(onClick = onCancel) { Text("Cancel") }
                }
                else -> TextButton(onClick = onDelete) { Text("Delete") }
            }
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
    var sourceId by remember { mutableStateOf(list.firstOrNull()?.id ?: "") }
    var targetId by remember { mutableStateOf(list.getOrNull(1)?.id ?: list.firstOrNull()?.id ?: "") }
    var sourceMenu by remember { mutableStateOf(false) }
    var targetMenu by remember { mutableStateOf(false) }
    var dbs by remember { mutableStateOf<List<String>>(emptyList()) }
    val colls = remember { mutableStateMapOf<String, List<String>>() }
    val selected = remember { mutableStateMapOf<String, Boolean>() } // "db.coll" -> true
    var preflightResult by remember { mutableStateOf<io.mex.data.PreflightResult?>(null) }
    var busy by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    LaunchedEffect(sourceId) {
        val client = registry.client(sourceId) ?: return@LaunchedEffect
        dbs = withContext(Dispatchers.IO) { listDatabases(client).map { it.name } }
    }

    Dialog(onDismissRequest = onClose) {
        Surface(modifier = Modifier.width(720.dp).heightIn(max = 720.dp), shape = MaterialTheme.shapes.medium, tonalElevation = 6.dp) {
            Column(modifier = Modifier.padding(18.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
                Text("New migration", style = MaterialTheme.typography.titleMedium)

                Row(horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                    Box(modifier = Modifier.weight(1f)) {
                        PickConnection("Source", sourceId, sourceMenu, { sourceMenu = it }, list) { sourceId = it }
                    }
                    Box(modifier = Modifier.weight(1f)) {
                        PickConnection("Target", targetId, targetMenu, { targetMenu = it }, list) { targetId = it }
                    }
                }

                Text("Select namespaces", style = MaterialTheme.typography.titleSmall)
                Column(modifier = Modifier.heightIn(max = 280.dp).verticalScroll(rememberScrollState())) {
                    dbs.forEach { db ->
                        var open by remember(db) { mutableStateOf(false) }
                        Row(verticalAlignment = Alignment.CenterVertically) {
                            TextButton(onClick = {
                                open = !open
                                if (open && colls[db] == null) {
                                    scope.launch {
                                        val client = registry.client(sourceId) ?: return@launch
                                        colls[db] = withContext(Dispatchers.IO) { listCollections(client, db).map { it.name } }
                                    }
                                }
                            }) { Text(if (open) "▾ $db" else "▸ $db", fontFamily = FontFamily.Monospace) }
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

                preflightResult?.let { r ->
                    Surface(color = if (r.ok) MaterialTheme.colorScheme.primaryContainer else MaterialTheme.colorScheme.errorContainer) {
                        Column(modifier = Modifier.padding(8.dp)) {
                            Text(if (r.ok) "Preflight passed." else "Preflight has failures.", style = MaterialTheme.typography.labelMedium)
                            r.checks.forEach {
                                Text("${if (it.ok) "✓" else "✗"} ${it.name}${it.detail?.let { d -> " — $d" }.orEmpty()}", style = MaterialTheme.typography.labelSmall)
                            }
                        }
                    }
                }

                Row {
                    val namespaces = selected.entries.filter { it.value }.map {
                        val parts = it.key.split(".", limit = 2)
                        MigrationNs(parts[0], parts[1])
                    }
                    OutlinedButton(
                        onClick = {
                            scope.launch {
                                busy = true
                                try {
                                    val s = registry.client(sourceId) ?: return@launch
                                    val t = registry.client(targetId) ?: return@launch
                                    preflightResult = withContext(Dispatchers.IO) { preflight(s, t, namespaces) }
                                } finally { busy = false }
                            }
                        },
                        enabled = namespaces.isNotEmpty() && !busy,
                    ) { Text("Preflight") }
                    Spacer(modifier = Modifier.weight(1f))
                    TextButton(onClick = onClose) { Text("Cancel") }
                    Button(
                        onClick = { onCreate(MigrationSpec(sourceId, targetId, namespaces)) },
                        enabled = preflightResult?.ok == true && !busy,
                    ) { Text("Create migration") }
                }
            }
        }
    }
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
