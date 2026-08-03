package io.mex.ui.provision

import androidx.compose.foundation.VerticalScrollbar
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.rememberScrollbarAdapter
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.selection.SelectionContainer
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
import io.mex.data.LabTopology
import io.mex.data.ProvisionPhase
import io.mex.provision.findDocker
import io.mex.provision.plan
import io.mex.provision.summary
import io.mex.ui.components.CopyChip
import io.mex.ui.components.StatusPill
import io.mex.ui.components.TypedConfirmDialog
import io.mex.ui.connections.ConnectionsViewModel
import io.mex.ui.state.Selection
import io.mex.ui.state.SelectionStore
import io.mex.util.redactCredentials
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/**
 * Provisioning host: local Docker labs (v3.4) and Kubernetes deployments (v3.5).
 * Two substrates, one honest split — single-host labs are never labelled production.
 */
@Composable
fun ProvisionHost(
    ctx: AppContext,
    ui: ProvisionUiState,
    k8sUi: io.mex.ui.provision.k8s.K8sUiState,
    connectionsVm: ConnectionsViewModel,
    selection: SelectionStore,
) {
    var tab by remember { mutableStateOf(0) }
    Column(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Text("Provision", style = MaterialTheme.typography.headlineSmall, modifier = Modifier.weight(1f))
        }
        Spacer(modifier = Modifier.height(8.dp))
        TabRow(selectedTabIndex = tab, modifier = Modifier.fillMaxWidth()) {
            Tab(selected = tab == 0, onClick = { tab = 0 }, text = { Text("Local (Docker)") })
            Tab(selected = tab == 1, onClick = { tab = 1 }, text = { Text("Kubernetes") })
        }
        Spacer(modifier = Modifier.height(14.dp))
        Box(modifier = Modifier.weight(1f)) {
            when (tab) {
                0 -> ProvisionView(ctx, ui, connectionsVm, selection)
                else -> io.mex.ui.provision.k8s.K8sTab(ctx, k8sUi, connectionsVm, selection)
            }
        }
    }
}

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
    var wizardPreset by remember { mutableStateOf<WizardPreset?>(null) }
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
    // Cached after the first success; Retry forces a re-probe.
    LaunchedEffect(dockerProbe) {
        dockerProbed = false
        docker = withContext(Dispatchers.IO) { findDocker(refresh = dockerProbe > 0) }
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

    fun openWizard(preset: WizardPreset?) {
        wizardPreset = preset
        showWizard = true
    }

    Column(modifier = Modifier.fillMaxSize()) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Text(
                dockerInfo.version.substringBefore(","),
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.weight(1f),
            )
            Button(onClick = { openWizard(null) }) { Text("+ New lab") }
        }
        Spacer(modifier = Modifier.height(12.dp))
        if (labs.isEmpty()) {
            EmptyState(onPreset = ::openWizard)
        }
        val listState = rememberLazyListState()
        Box(modifier = Modifier.weight(1f)) {
            LazyColumn(state = listState, verticalArrangement = Arrangement.spacedBy(10.dp)) {
                items(labs, key = { it.id }) { lab ->
                    LabRow(
                        ctx = ctx,
                        lab = lab,
                        busy = ui.busy.containsKey(lab.id) || runner.isBusy(lab.id),
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
            initialPreset = wizardPreset,
            onClose = { showWizard = false },
            onCreate = { name, topology, tag, auth ->
                showWizard = false
                runner.provision(name, topology, tag, auth)
                reload()
            },
        )
    }
    details?.let { lab -> LabDetailsDialog(ctx, lab, onClose = { details = null }) }
    ui.pendingResult?.let { done ->
        ProvisionResultBanner(done, onOpen = { connId ->
            ui.consumeResult()
            scope.launch {
                connectionsVm.open(connId)
                selection.select(Selection.ConnectionView(connId))
            }
        }, onClose = { ui.consumeResult() })
    }
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

/* ============================ empty state ============================ */

enum class WizardPreset { Standalone, Rs3, Sharded }

@Composable
private fun EmptyState(onPreset: (WizardPreset?) -> Unit) {
    Column(
        modifier = Modifier.fillMaxWidth().padding(vertical = 40.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        Text("Spin up a MongoDB cluster in one click", style = MaterialTheme.typography.titleMedium)
        Text(
            "Standalone, replica sets or a full sharded cluster — built on your own Docker,\n" +
                "auth enabled, registered in the sidebar and ready to break.",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Spacer(modifier = Modifier.height(8.dp))
        Row(horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            OutlinedButton(onClick = { onPreset(WizardPreset.Standalone) }) { Text("Standalone") }
            OutlinedButton(onClick = { onPreset(WizardPreset.Rs3) }) { Text("Replica set ×3") }
            Button(onClick = { onPreset(WizardPreset.Sharded) }) { Text("Sharded cluster") }
        }
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
        LabStatus.stopped -> "stopped · data kept"
        else -> lab.status.name
    }
    // plan() + port lookup only when the lab identity or ports change, not per log flush.
    val ports = remember(lab.id, lab.portMap) {
        plan(lab).clientServices.mapNotNull { svc -> lab.portMap[svc]?.let { svc to it } }
    }

    Card(border = CardDefaults.outlinedCardBorder()) {
        Column(modifier = Modifier.padding(14.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Column(modifier = Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        Text(lab.name, style = MaterialTheme.typography.bodyMedium, fontWeight = FontWeight.SemiBold)
                        StatusPill(statusLabel, statusColor, pulsing = busy)
                    }
                    Text(
                        "${lab.mongoTag} · ${summary(lab.topology)} · ${if (lab.auth) "auth" else "no auth"}",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        fontFamily = FontFamily.Monospace,
                    )
                    if (ports.isNotEmpty() && lab.status != LabStatus.missing) {
                        Row(horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                            for ((svc, port) in ports.take(7)) {
                                CopyChip(label = "$svc :$port", copyText = "127.0.0.1:$port")
                            }
                            if (ports.size > 7) {
                                Text(
                                    "+${ports.size - 7}",
                                    style = MaterialTheme.typography.labelSmall,
                                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                                )
                            }
                        }
                    }
                }
                when {
                    busy && lab.status == LabStatus.provisioning -> TextButton(onClick = onCancel) { Text("Cancel") }
                    busy -> CircularProgressIndicator(modifier = Modifier.size(18.dp), strokeWidth = 2.dp)
                    else -> Row(verticalAlignment = Alignment.CenterVertically) {
                        when (lab.status) {
                            LabStatus.running -> {
                                if (lab.connectionId != null) Button(onClick = onOpen) { Text("Open") }
                                TextButton(onClick = onStop) { Text("Stop") }
                            }
                            LabStatus.stopped -> Button(
                                onClick = onStart,
                                enabled = lab.appMajor <= io.mex.data.LAB_APP_MAJOR,
                            ) { Text("Start") }
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
            lab.error?.let { err ->
                Surface(
                    color = MaterialTheme.colorScheme.errorContainer.copy(alpha = 0.45f),
                    shape = RoundedCornerShape(6.dp),
                    modifier = Modifier.fillMaxWidth().padding(top = 8.dp),
                ) {
                    Column(modifier = Modifier.padding(horizontal = 10.dp, vertical = 6.dp)) {
                        Text(err, color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.labelSmall)
                        if (lab.status == LabStatus.failed && !busy) {
                            Text(
                                "Containers are kept for inspection — Destroy cleans everything up.",
                                style = MaterialTheme.typography.labelSmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                            )
                        }
                    }
                }
            }
            if (busy && lab.status == LabStatus.provisioning) {
                PhaseStepper(lab, phase)
            }
            if (busy || logExpanded) {
                if (log.isNotEmpty()) LogPane(log)
            }
        }
    }
}

/** Stepper chips: done ✓ green, active pulsing amber, upcoming muted (PRV-UI-4). */
@Composable
private fun PhaseStepper(lab: Lab, current: ProvisionPhase?) {
    val relevant = remember(lab.id) {
        ProvisionPhase.entries.filter { p ->
            when (p) {
                ProvisionPhase.shards -> lab.topology is LabTopology.Sharded
                ProvisionPhase.auth -> lab.auth
                ProvisionPhase.initiate -> lab.topology !is LabTopology.Standalone
                else -> true
            }
        }
    }
    val activeIdx = current?.let { relevant.indexOf(it) } ?: -1
    Row(
        modifier = Modifier.padding(top = 10.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(4.dp),
    ) {
        relevant.forEachIndexed { i, p ->
            val (bg, fg, label) = when {
                i < activeIdx -> Triple(Color(0xFF4ADE80).copy(alpha = 0.12f), Color(0xFF4ADE80), "✓ ${p.name}")
                i == activeIdx -> Triple(Color(0xFFFBBF24).copy(alpha = 0.16f), Color(0xFFFBBF24), p.name)
                else -> Triple(
                    MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.4f),
                    MaterialTheme.colorScheme.onSurfaceVariant,
                    p.name,
                )
            }
            Text(
                label,
                style = MaterialTheme.typography.labelSmall,
                fontWeight = if (i == activeIdx) FontWeight.SemiBold else FontWeight.Normal,
                color = fg,
                modifier = Modifier
                    .background(bg, RoundedCornerShape(999.dp))
                    .padding(horizontal = 8.dp, vertical = 2.dp),
            )
        }
    }
}

/**
 * Virtualized log: per-line items instead of one giant re-measured Text — pull-progress
 * bursts would otherwise rebuild a 40 KB string and re-layout it per line batch.
 */
@Composable
internal fun LogPane(lines: List<String>) {
    val clipboard = LocalClipboardManager.current
    val listState = rememberLazyListState()
    LaunchedEffect(lines) {
        if (lines.isNotEmpty()) listState.scrollToItem(lines.lastIndex)
    }
    Surface(
        color = Color(0xFF0A0C10),
        shape = RoundedCornerShape(6.dp),
        modifier = Modifier.fillMaxWidth().heightIn(max = 190.dp).padding(top = 8.dp),
    ) {
        Box {
            SelectionContainer {
                LazyColumn(state = listState, modifier = Modifier.padding(horizontal = 8.dp, vertical = 6.dp)) {
                    items(lines.size) { i ->
                        Text(
                            lines[i],
                            color = Color(0xFFD4D7DF),
                            fontFamily = FontFamily.Monospace,
                            style = MaterialTheme.typography.labelSmall,
                        )
                    }
                }
            }
            TextButton(
                onClick = { clipboard.setText(AnnotatedString(lines.joinToString("\n"))) },
                modifier = Modifier.align(Alignment.TopEnd),
                contentPadding = PaddingValues(horizontal = 8.dp, vertical = 0.dp),
            ) { Text("copy", style = MaterialTheme.typography.labelSmall, color = Color(0xFF8B93A7)) }
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
