package io.mex.ui.provision

import androidx.compose.foundation.VerticalScrollbar
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.rememberScrollbarAdapter
import androidx.compose.foundation.rememberScrollState
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
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.data.Lab
import io.mex.data.LabStatus
import io.mex.data.ProvisionPhase
import io.mex.provision.findDocker
import io.mex.provision.plan
import io.mex.provision.summary
import io.mex.ui.components.TypedConfirmDialog
import io.mex.ui.connections.ConnectionsViewModel
import io.mex.ui.state.Selection
import io.mex.ui.state.SelectionStore
import io.mex.util.redactCredentials
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/** Local cluster provisioning — lab list, builder wizard, live pipeline log (PRV-UI-1..8). */
@Composable
fun ProvisionView(
    ctx: AppContext,
    ui: ProvisionUiState,
    connectionsVm: ConnectionsViewModel,
    selection: SelectionStore,
) {
    val runner = ui.runner
    var labs by remember { mutableStateOf<List<Lab>>(emptyList()) }
    var showWizard by remember { mutableStateOf(false) }
    var expandedLog by remember { mutableStateOf<String?>(null) }
    var details by remember { mutableStateOf<Lab?>(null) }
    var destroying by remember { mutableStateOf<Lab?>(null) }
    var dockerProbe by remember { mutableStateOf(0) }
    var dockerProbed by remember { mutableStateOf(false) }
    var docker by remember { mutableStateOf<io.mex.backup.ToolInfo?>(null) }
    val scope = rememberCoroutineScope()

    fun reload() { labs = ctx.labs.list() }
    // Reload on open and on every terminal runner event, wherever it happened (PRV-LIFE-1).
    LaunchedEffect(ui.epoch) {
        reload()
        runner.reconcile()
    }
    // Probing spawns `docker --version` processes — never on the UI thread (PRV-NFR-3).
    LaunchedEffect(dockerProbe) {
        dockerProbed = false
        docker = withContext(Dispatchers.IO) { findDocker() }
        dockerProbed = true
    }

    if (!dockerProbed) {
        Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
            CircularProgressIndicator(modifier = Modifier.size(24.dp), strokeWidth = 2.dp)
        }
        return
    }
    val dockerInfo = docker
    if (dockerInfo == null) {
        NoDockerPanel(onRetry = { dockerProbe++ })
        return
    }

    Column(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Text("Provision", style = MaterialTheme.typography.headlineSmall, modifier = Modifier.weight(1f))
            Button(onClick = { showWizard = true }) { Text("+ New lab") }
        }
        Text(
            dockerInfo.version.substringBefore(","),
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Spacer(modifier = Modifier.height(12.dp))
        if (labs.isEmpty()) {
            Text(
                "No labs yet. Build a standalone, replica set or sharded cluster on your own Docker " +
                    "— it lands in the sidebar as a ready-to-use connection.",
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                style = MaterialTheme.typography.bodySmall,
            )
        }
        val listState = rememberLazyListState()
        Box(modifier = Modifier.weight(1f)) {
            LazyColumn(state = listState, verticalArrangement = Arrangement.spacedBy(8.dp)) {
                items(labs, key = { it.id }) { lab ->
                    LabRow(
                        ctx = ctx,
                        lab = lab,
                        busy = runner.isBusy(lab.id),
                        phase = ui.phases[lab.id],
                        log = ui.logs[lab.id].orEmpty(),
                        logExpanded = expandedLog == lab.id,
                        onToggleLog = { expandedLog = if (expandedLog == lab.id) null else lab.id },
                        onOpen = {
                            lab.connectionId?.let { id ->
                                scope.launch {
                                    connectionsVm.open(id)
                                    selection.select(Selection.ConnectionView(id))
                                }
                            }
                        },
                        onStart = { runner.start(lab.id) },
                        onStop = { runner.stop(lab.id) },
                        onCancel = { runner.cancel(lab.id) },
                        onDestroy = { destroying = lab },
                        onDetails = { details = lab },
                    )
                }
            }
            VerticalScrollbar(
                adapter = rememberScrollbarAdapter(listState),
                modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
            )
        }
        Text(
            "Labs run on your machine for development and testing. One host is not high availability.",
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }

    if (showWizard) {
        LabWizard(
            ctx = ctx,
            onClose = { showWizard = false },
            onCreate = { name, topology, tag, auth ->
                showWizard = false
                runner.provision(name, topology, tag, auth)
                reload()
            },
        )
    }
    details?.let { lab -> LabDetailsDialog(ctx, lab, onClose = { details = null }) }
    ui.pendingResult?.let { done -> ProvisionResultBanner(done, labs, onOpen = { connId ->
        ui.consumeResult()
        scope.launch {
            connectionsVm.open(connId)
            selection.select(Selection.ConnectionView(connId))
        }
    }, onClose = { ui.consumeResult() }) }
    destroying?.let { lab ->
        val connName = lab.connectionId?.let { ctx.connections.get(it)?.name }
        val f = io.mex.provision.footprint(lab.topology)
        TypedConfirmDialog(
            title = "Destroy lab?",
            consequence = "This permanently deletes ${f.containers} container${if (f.containers == 1) "" else "s"}, " +
                "${f.volumes} volume${if (f.volumes == 1) "" else "s"} and the lab directory for \"${lab.name}\". " +
                "Data cannot be recovered.",
            requiredText = lab.name,
            confirmLabel = "Destroy",
            checkboxLabel = connName?.let { "Also remove connection \"$it\"" },
            onConfirm = { alsoConn ->
                destroying = null
                runner.destroy(lab.id, alsoConnection = alsoConn && connName != null)
            },
            onCancel = { destroying = null },
        )
    }
}

/* ============================ rows ============================ */

@Composable
private fun LabRow(
    ctx: AppContext,
    lab: Lab,
    busy: Boolean,
    phase: ProvisionPhase?,
    log: List<String>,
    logExpanded: Boolean,
    onToggleLog: () -> Unit,
    onOpen: () -> Unit,
    onStart: () -> Unit,
    onStop: () -> Unit,
    onCancel: () -> Unit,
    onDestroy: () -> Unit,
    onDetails: () -> Unit,
) {
    val clipboard = LocalClipboardManager.current
    var menu by remember { mutableStateOf(false) }
    val statusColor = when (lab.status) {
        LabStatus.running -> Color(0xFF4ADE80)
        LabStatus.provisioning -> Color(0xFFFBBF24)
        LabStatus.stopped -> MaterialTheme.colorScheme.onSurfaceVariant
        LabStatus.failed -> Color(0xFFF87171)
        LabStatus.missing -> Color(0xFFB45309)
    }
    val statusLabel = when (lab.status) {
        LabStatus.stopped -> "STOPPED · data kept"
        LabStatus.missing -> "MISSING"
        else -> lab.status.name.uppercase()
    }
    Card(border = CardDefaults.outlinedCardBorder()) {
        Column(modifier = Modifier.padding(12.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(statusLabel, color = statusColor, style = MaterialTheme.typography.labelSmall, modifier = Modifier.width(130.dp))
                Column(modifier = Modifier.weight(1f)) {
                    Text(lab.name, style = MaterialTheme.typography.bodyMedium, fontWeight = FontWeight.SemiBold)
                    Text(
                        buildString {
                            append(lab.mongoTag)
                            append(" · ${summary(lab.topology)}")
                            append(if (lab.auth) " · auth" else " · no auth")
                        },
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        fontFamily = FontFamily.Monospace,
                    )
                    if (lab.portMap.isNotEmpty()) {
                        val ports = plan(lab).clientServices.mapNotNull { lab.portMap[it] }
                        Text(
                            "127.0.0.1 → ${ports.joinToString(", ")}",
                            style = MaterialTheme.typography.labelSmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            fontFamily = FontFamily.Monospace,
                        )
                    }
                    lab.error?.let {
                        Text("Error: $it", color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.labelSmall)
                    }
                    if (lab.status == LabStatus.failed && !busy) {
                        Text(
                            "Containers are kept for inspection — Destroy cleans everything up.",
                            style = MaterialTheme.typography.labelSmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                    }
                }
                when {
                    busy && lab.status == LabStatus.provisioning -> TextButton(onClick = onCancel) { Text("Cancel") }
                    busy -> CircularProgressIndicator(modifier = Modifier.size(18.dp), strokeWidth = 2.dp)
                    else -> {
                        when (lab.status) {
                            LabStatus.running -> {
                                if (lab.connectionId != null) Button(onClick = onOpen) { Text("Open") }
                                TextButton(onClick = onStop) { Text("Stop") }
                            }
                            LabStatus.stopped -> TextButton(onClick = onStart, enabled = lab.appMajor <= io.mex.data.LAB_APP_MAJOR) { Text("Start") }
                            else -> Unit
                        }
                        TextButton(onClick = onDestroy) { Text("Destroy") }
                        Box {
                            TextButton(onClick = { menu = true }, contentPadding = PaddingValues(0.dp), modifier = Modifier.size(24.dp)) { Text("⋯") }
                            DropdownMenu(expanded = menu, onDismissRequest = { menu = false }) {
                                DropdownMenuItem(text = { Text("Show details") }, onClick = { menu = false; onDetails() })
                                lab.connectionId?.let { connId ->
                                    DropdownMenuItem(
                                        text = { Text("Copy URI (redacted)") },
                                        onClick = {
                                            menu = false
                                            ctx.connections.get(connId)?.let {
                                                clipboard.setText(AnnotatedString(redactCredentials(it.uri)))
                                            }
                                        },
                                    )
                                }
                                if (log.isNotEmpty()) {
                                    DropdownMenuItem(text = { Text(if (logExpanded) "Hide log" else "View log") }, onClick = { menu = false; onToggleLog() })
                                }
                            }
                        }
                    }
                }
            }
            if (busy && lab.status == LabStatus.provisioning) {
                PhaseStrip(lab, phase)
            }
            if (busy || logExpanded) {
                if (log.isNotEmpty()) LogPane(log)
            }
        }
    }
}

/** `render → up → wait → …` with the active phase highlighted (PRV-UI-4). */
@Composable
private fun PhaseStrip(lab: Lab, current: ProvisionPhase?) {
    val relevant = ProvisionPhase.entries.filter { p ->
        when (p) {
            ProvisionPhase.shards -> lab.topology is io.mex.data.LabTopology.Sharded
            ProvisionPhase.auth -> lab.auth
            ProvisionPhase.initiate -> lab.topology !is io.mex.data.LabTopology.Standalone
            else -> true
        }
    }
    Row(modifier = Modifier.padding(top = 6.dp), verticalAlignment = Alignment.CenterVertically) {
        relevant.forEachIndexed { i, p ->
            val active = p == current
            val done = current != null && relevant.indexOf(current) > i
            Text(
                p.name,
                style = MaterialTheme.typography.labelSmall,
                fontWeight = if (active) FontWeight.Bold else FontWeight.Normal,
                color = when {
                    active -> Color(0xFFFBBF24)
                    done -> Color(0xFF4ADE80)
                    else -> MaterialTheme.colorScheme.onSurfaceVariant
                },
            )
            if (i < relevant.lastIndex) {
                Text(" → ", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
        }
    }
}

@Composable
internal fun LogPane(lines: List<String>) {
    val scroll = rememberScrollState()
    // Keyed on the list itself: once the ring buffer is full, size stays constant while
    // content keeps changing — a size key would freeze autoscroll mid-provision.
    LaunchedEffect(lines) { scroll.scrollTo(scroll.maxValue) }
    Surface(
        color = Color(0xFF0A0C10),
        shape = RoundedCornerShape(4.dp),
        modifier = Modifier.fillMaxWidth().heightIn(max = 180.dp).padding(top = 6.dp),
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

/* ============================ no docker ============================ */

@Composable
private fun NoDockerPanel(onRetry: () -> Unit) {
    Column(
        modifier = Modifier.fillMaxSize().padding(24.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.Center,
    ) {
        Text("Docker is required to provision local clusters", style = MaterialTheme.typography.titleMedium)
        Spacer(modifier = Modifier.height(8.dp))
        Text(
            "docker not found on PATH or common locations",
            color = MaterialTheme.colorScheme.error,
            style = MaterialTheme.typography.bodySmall,
        )
        Spacer(modifier = Modifier.height(16.dp))
        Text(
            "Install Docker Desktop:  brew install --cask docker\nor see docs.docker.com/get-docker",
            style = MaterialTheme.typography.bodySmall,
            fontFamily = FontFamily.Monospace,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Spacer(modifier = Modifier.height(16.dp))
        OutlinedButton(onClick = onRetry) { Text("Retry") }
        Spacer(modifier = Modifier.height(24.dp))
        Text(
            "Provisioning to Kubernetes (production profiles) arrives in a later release.",
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }
}
