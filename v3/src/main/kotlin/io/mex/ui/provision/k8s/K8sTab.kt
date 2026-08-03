package io.mex.ui.provision.k8s

import androidx.compose.foundation.VerticalScrollbar
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.rememberScrollbarAdapter
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.data.K8sDeployStatus
import io.mex.data.K8sDeployment
import io.mex.data.K8sProfile
import io.mex.provision.k8s.ForwardState
import io.mex.provision.k8s.findKubectl
import io.mex.provision.k8s.k8sSummary
import io.mex.provision.k8s.shortContext
import io.mex.provision.k8s.shortHash
import io.mex.provision.k8s.teardownConsequence
import io.mex.ui.components.CopyChip
import io.mex.ui.components.StatusPill
import io.mex.ui.components.TypedConfirmDialog
import io.mex.ui.connections.ConnectionsViewModel
import io.mex.ui.provision.LogPane
import io.mex.ui.state.Selection
import io.mex.ui.state.SelectionStore
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/** Kubernetes deployments: cards, wizard entry, status detail, teardown (K8P-UI-1/3). */
@Composable
fun K8sTab(
    ctx: AppContext,
    ui: K8sUiState,
    connectionsVm: ConnectionsViewModel,
    selection: SelectionStore,
) {
    val runner = ui.runner
    var deployments by remember { mutableStateOf<List<K8sDeployment>>(emptyList()) }
    var showWizard by remember { mutableStateOf(false) }
    var expandedLog by remember { mutableStateOf<String?>(null) }
    var statusFor by remember { mutableStateOf<K8sDeployment?>(null) }
    var previewFor by remember { mutableStateOf<K8sDeployment?>(null) }
    var tearingDown by remember { mutableStateOf<K8sDeployment?>(null) }
    var probe by remember { mutableStateOf(0) }
    var probed by remember { mutableStateOf(false) }
    var kubectl by remember { mutableStateOf<io.mex.backup.ToolInfo?>(null) }
    val scope = rememberCoroutineScope()

    LaunchedEffect(ui.epoch) { deployments = ctx.k8sDeployments.list() }
    LaunchedEffect(probe) {
        probed = false
        kubectl = withContext(Dispatchers.IO) { findKubectl(refresh = probe > 0) }
        probed = true
    }

    if (!probed) {
        Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
            CircularProgressIndicator(modifier = Modifier.size(24.dp), strokeWidth = 2.dp)
        }
        return
    }
    val tool = kubectl
    if (tool == null) {
        NoKubectlPanel(onRetry = { probe++ })
        return
    }

    Column(modifier = Modifier.fillMaxSize()) {
        Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(bottom = 12.dp)) {
            Column(modifier = Modifier.weight(1f)) {
                Text(
                    tool.version.substringBefore(","),
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
            Button(onClick = { showWizard = true }) { Text("+ New deployment") }
        }
        if (deployments.isEmpty()) {
            K8sEmptyState()
        }
        val listState = rememberLazyListState()
        Box(modifier = Modifier.weight(1f)) {
            LazyColumn(state = listState, verticalArrangement = Arrangement.spacedBy(10.dp)) {
                items(deployments, key = { it.id }) { d ->
                    DeploymentCard(
                        ctx = ctx,
                        d = d,
                        busy = ui.busy.containsKey(d.id) || runner.isBusy(d.id),
                        forward = ui.forwards[d.id],
                        log = ui.logs[d.id].orEmpty(),
                        logExpanded = expandedLog == d.id,
                        onToggleLog = { expandedLog = if (expandedLog == d.id) null else d.id },
                        onConnect = { runner.connect(d.id) },
                        onDisconnect = { runner.disconnect(d.id) },
                        onOpen = {
                            d.connectionId?.let { id ->
                                scope.launch {
                                    connectionsVm.open(id)
                                    selection.select(Selection.ConnectionView(id))
                                }
                            }
                        },
                        onRefresh = { runner.refresh(d.id) },
                        onStatus = { statusFor = d },
                        onPreview = { previewFor = d },
                        onTeardown = { tearingDown = d },
                    )
                }
            }
            VerticalScrollbar(
                adapter = rememberScrollbarAdapter(listState),
                modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
            )
        }
        Text(
            "Deployments are operator-managed. This app creates and deletes; day-2 operations " +
                "run through the connection's DBA tabs.",
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }

    if (showWizard) {
        DeployWizard(
            ctx = ctx,
            tool = tool,
            onClose = { showWizard = false },
            onApply = { spec, hash ->
                showWizard = false
                runner.apply(spec, hash)
            },
        )
    }
    statusFor?.let { d -> StatusDetailDialog(tool, d, onClose = { statusFor = null }) }
    previewFor?.let { d -> RenderedBundleDialog(d, onClose = { previewFor = null }) }
    ui.pendingResult?.let { done ->
        io.mex.ui.provision.ProvisionResultBanner(
            done = io.mex.provision.LabEvent.Done(
                labId = done.id,
                status = io.mex.data.LabStatus.running,
                password = done.password,
                uri = done.uri,
                labName = done.name,
                connectionId = done.connectionId,
            ),
            onOpen = { connId ->
                ui.consumeResult()
                scope.launch {
                    connectionsVm.open(connId)
                    selection.select(Selection.ConnectionView(connId))
                }
            },
            onClose = { ui.consumeResult() },
        )
    }
    tearingDown?.let { d ->
        val connName = d.connectionId?.let { ctx.connections.get(it)?.name }
        var releasePvcs by remember(d.id) { mutableStateOf(d.spec.profile != K8sProfile.prod) }
        TypedConfirmDialog(
            title = "Tear down deployment?",
            consequence = teardownConsequence(d.spec, releasePvcs, connName),
            requiredText = d.spec.name,
            confirmLabel = "Tear down",
            checkboxLabel = connName?.let { "Also remove connection \"$it\"" },
            onConfirm = { alsoConn ->
                tearingDown = null
                runner.teardown(d.id, releasePvcs = releasePvcs, alsoConnection = alsoConn && connName != null)
            },
            onCancel = { tearingDown = null },
            extraContent = {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Checkbox(checked = releasePvcs, onCheckedChange = { releasePvcs = it })
                    Text(
                        if (d.spec.profile == K8sProfile.prod) {
                            "Also release the data volumes (Prod keeps them by default)"
                        } else {
                            "Also release the data volumes"
                        },
                        style = MaterialTheme.typography.bodySmall,
                        color = if (releasePvcs) MaterialTheme.colorScheme.error else MaterialTheme.colorScheme.onSurface,
                    )
                }
            },
        )
    }
}

@Composable
private fun K8sEmptyState() {
    Column(
        modifier = Modifier.fillMaxWidth().padding(vertical = 32.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(6.dp),
    ) {
        Text("Provision a production cluster", style = MaterialTheme.typography.titleMedium)
        Text(
            "Render operator CRs for MongoDB Community or Percona, review the exact YAML,\n" +
                "and apply it behind a typed confirmation. Prod locks TLS, auth, spread and backups on.",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }
}

@Composable
private fun DeploymentCard(
    ctx: AppContext,
    d: K8sDeployment,
    busy: Boolean,
    forward: ForwardState?,
    log: List<String>,
    logExpanded: Boolean,
    onToggleLog: () -> Unit,
    onConnect: () -> Unit,
    onDisconnect: () -> Unit,
    onOpen: () -> Unit,
    onRefresh: () -> Unit,
    onStatus: () -> Unit,
    onPreview: () -> Unit,
    onTeardown: () -> Unit,
) {
    var menu by remember { mutableStateOf(false) }
    val color = when (d.status) {
        K8sDeployStatus.ready -> Color(0xFF4ADE80)
        K8sDeployStatus.applying, K8sDeployStatus.pending -> Color(0xFFFBBF24)
        K8sDeployStatus.degraded -> Color(0xFFB45309)
        K8sDeployStatus.failed -> Color(0xFFF87171)
        K8sDeployStatus.deleting -> Color(0xFFA78BFA)
        K8sDeployStatus.missing -> MaterialTheme.colorScheme.onSurfaceVariant
    }
    Card(border = CardDefaults.outlinedCardBorder()) {
        Column(modifier = Modifier.padding(14.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Column(modifier = Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        Text(d.spec.name, style = MaterialTheme.typography.bodyMedium, fontWeight = FontWeight.SemiBold)
                        StatusPill(d.status.name, color, pulsing = busy)
                        ProfileChip(d.spec.profile)
                    }
                    Text(
                        "${shortContext(d.spec.context)} · ${d.spec.namespace}",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        fontFamily = FontFamily.Monospace,
                    )
                    Text(
                        "${d.spec.operator.name.uppercase()} · ${k8sSummary(d.spec.topology)} · mongo ${d.spec.mongoVersion}",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        fontFamily = FontFamily.Monospace,
                    )
                    Text(
                        backupLine(d),
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                    d.statusDetail?.takeIf { it.isNotBlank() }?.let {
                        Text("state: $it", style = MaterialTheme.typography.labelSmall, color = color)
                    }
                    when (val f = forward) {
                        is ForwardState.Up -> Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                            Text("forward", style = MaterialTheme.typography.labelSmall, color = Color(0xFF4ADE80))
                            CopyChip(label = "127.0.0.1:${f.localPort}")
                        }
                        is ForwardState.Starting -> Text("forward starting…", style = MaterialTheme.typography.labelSmall, color = Color(0xFFFBBF24))
                        is ForwardState.Down -> Text("forward down — ${f.reason}", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.error)
                        null -> Unit
                    }
                }
                if (busy) {
                    CircularProgressIndicator(modifier = Modifier.size(18.dp), strokeWidth = 2.dp)
                } else {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        if (d.status == K8sDeployStatus.ready) {
                            if (forward is ForwardState.Up && d.connectionId != null) {
                                Button(onClick = onOpen) { Text("Open") }
                                TextButton(onClick = onDisconnect) { Text("Disconnect") }
                            } else {
                                Button(onClick = onConnect) { Text("Connect") }
                            }
                        }
                        TextButton(onClick = onTeardown) { Text("Tear down") }
                        Box {
                            TextButton(onClick = { menu = true }, contentPadding = PaddingValues(0.dp), modifier = Modifier.size(24.dp)) { Text("⋯") }
                            DropdownMenu(expanded = menu, onDismissRequest = { menu = false }) {
                                DropdownMenuItem(text = { Text("Status detail") }, onClick = { menu = false; onStatus() })
                                DropdownMenuItem(text = { Text("Rendered bundle") }, onClick = { menu = false; onPreview() })
                                DropdownMenuItem(text = { Text("Refresh status") }, onClick = { menu = false; onRefresh() })
                                if (log.isNotEmpty()) {
                                    DropdownMenuItem(
                                        text = { Text(if (logExpanded) "Hide log" else "View log") },
                                        onClick = { menu = false; onToggleLog() },
                                    )
                                }
                            }
                        }
                    }
                }
            }
            d.error?.let { err ->
                Surface(
                    color = MaterialTheme.colorScheme.errorContainer.copy(alpha = 0.45f),
                    shape = RoundedCornerShape(6.dp),
                    modifier = Modifier.fillMaxWidth().padding(top = 8.dp),
                ) {
                    Text(
                        err,
                        color = MaterialTheme.colorScheme.error,
                        style = MaterialTheme.typography.labelSmall,
                        modifier = Modifier.padding(horizontal = 10.dp, vertical = 6.dp),
                    )
                }
            }
            d.bundleHash?.let {
                Text(
                    "applied ${shortHash(it)}",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    fontFamily = FontFamily.Monospace,
                    modifier = Modifier.padding(top = 6.dp),
                )
            }
            if (busy || logExpanded) {
                if (log.isNotEmpty()) LogPane(log)
            }
        }
    }
}

private fun backupLine(d: K8sDeployment): String = when (val b = d.spec.backup) {
    is io.mex.data.BackupChoice.Pbm -> "backups: PBM → ${b.endpoint.substringAfter("://")}/${b.bucket}"
    io.mex.data.BackupChoice.ByoDeclared -> "backups: BYO (declared)"
    io.mex.data.BackupChoice.None -> "backups: none (dev)"
}

@Composable
private fun ProfileChip(profile: K8sProfile) {
    val tint = if (profile == K8sProfile.prod) Color(0xFF4ADE80) else MaterialTheme.colorScheme.onSurfaceVariant
    Text(
        profile.name.uppercase(),
        style = MaterialTheme.typography.labelSmall,
        color = tint,
        fontWeight = FontWeight.SemiBold,
        modifier = Modifier
            .background(tint.copy(alpha = 0.12f), RoundedCornerShape(4.dp))
            .padding(horizontal = 5.dp, vertical = 1.dp),
    )
}

@Composable
private fun NoKubectlPanel(onRetry: () -> Unit) {
    Column(
        modifier = Modifier.fillMaxSize().padding(24.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.Center,
    ) {
        Text("kubectl is required to provision to Kubernetes", style = MaterialTheme.typography.titleMedium)
        Spacer(modifier = Modifier.height(8.dp))
        Text(
            "kubectl not found on PATH or common locations",
            color = MaterialTheme.colorScheme.error,
            style = MaterialTheme.typography.bodySmall,
        )
        Spacer(modifier = Modifier.height(16.dp))
        Text(
            "Install:  brew install kubectl\ndocs: kubernetes.io/docs/tasks/tools",
            style = MaterialTheme.typography.bodySmall,
            fontFamily = FontFamily.Monospace,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Spacer(modifier = Modifier.height(16.dp))
        OutlinedButton(onClick = onRetry) { Text("Retry") }
        Spacer(modifier = Modifier.height(20.dp))
        Text(
            "Local Docker labs are unaffected — see the Local tab.",
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }
}
