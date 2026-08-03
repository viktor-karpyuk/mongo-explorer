package io.mex.ui.cluster

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.background
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.grid.GridCells
import androidx.compose.foundation.lazy.grid.LazyVerticalGrid
import androidx.compose.foundation.lazy.grid.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
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
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.data.ConnectionInput
import io.mex.mongo.ClusterSnapshot
import io.mex.mongo.MemberInfo
import io.mex.mongo.MongoRegistry
import io.mex.mongo.RsConfig
import io.mex.mongo.RsMemberConfig
import io.mex.mongo.RsSetting
import io.mex.mongo.ShardInfo
import io.mex.mongo.clusterSnapshot
import io.mex.mongo.directNodeUri
import io.mex.mongo.freezeMember
import io.mex.mongo.setBalancer
import io.mex.ui.connections.ConnectionsViewModel
import io.mex.ui.state.Selection
import io.mex.ui.state.SelectionStore
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

@Composable
fun ClusterPanel(
    ctx: AppContext,
    connectionId: String,
    registry: MongoRegistry,
    readOnly: Boolean = false,
    connectionsVm: ConnectionsViewModel,
    selection: SelectionStore,
) {
    var snap by remember(connectionId) { mutableStateOf<ClusterSnapshot?>(null) }
    var error by remember(connectionId) { mutableStateOf<String?>(null) }
    // Kept apart from [error]: the poll clears that one on every success, which wiped
    // action failures (freeze, balancer, direct connect) off the screen within 5 s.
    var actionError by remember(connectionId) { mutableStateOf<String?>(null) }
    var auto by remember(connectionId) { mutableStateOf(true) }
    var steppingDown by remember(connectionId) { mutableStateOf(false) }
    var editingMember by remember(connectionId) { mutableStateOf<RsMemberConfig?>(null) }
    var freezingHost by remember(connectionId) { mutableStateOf<String?>(null) }
    var balancerTarget by remember(connectionId) { mutableStateOf<Boolean?>(null) }
    var removingMember by remember(connectionId) { mutableStateOf<RsMemberConfig?>(null) }
    var removingShard by remember(connectionId) { mutableStateOf<ShardInfo?>(null) }
    val scope = rememberCoroutineScope()

    suspend fun fetch() {
        val client = registry.client(connectionId) ?: return
        try {
            snap = withContext(Dispatchers.IO) { clusterSnapshot(client) }
            // Clearing on success matters — a single transient failure used to leave a
            // permanent error banner sitting above perfectly healthy data.
            error = null
        } catch (e: Exception) {
            error = e.message
        }
    }
    fun reload() { scope.launch { fetch() } }

    /**
     * Registers (or reuses, by name) a directConnection=true twin of this connection
     * aimed at [host], connects it and navigates there — the whole app scoped to one
     * node. Wrong-credential nodes surface as a normal error pill on the new record.
     */
    fun directConnect(host: String) {
        scope.launch {
            runCatching {
                val record = withContext(Dispatchers.IO) { ctx.connections.get(connectionId) }
                    ?: return@launch
                val name = "${record.name} → $host"
                val existingId = connectionsVm.list.firstOrNull { it.name == name }?.id
                val id = existingId ?: withContext(Dispatchers.IO) {
                    ctx.connections.create(
                        ConnectionInput(
                            name = name,
                            uri = directNodeUri(record.uri, host),
                            notes = "Direct connection to $host (created from the topology diagram)",
                            readOnly = record.readOnly,
                        ),
                    ).id
                }.also { connectionsVm.reload() }
                connectionsVm.open(id)
                selection.select(Selection.ConnectionView(id))
            }.onFailure { actionError = "Direct connect failed: ${it.message}" }
        }
    }
    // Elections, lag and drains move on their own; poll while auto is on so the
    // diagram tells the truth without the user hammering Refresh.
    LaunchedEffect(connectionId, auto) {
        fetch()
        while (auto) {
            delay(5_000)
            fetch()
        }
    }

    Column(
        modifier = Modifier.fillMaxSize().padding(24.dp).verticalScroll(rememberScrollState()),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            Text("Cluster", style = MaterialTheme.typography.headlineSmall, modifier = Modifier.weight(1f))
            // Cluster-level admin action — replica sets only, never on read-only connections.
            if (!readOnly && snap?.topology?.type == "replicaset" && snap?.topology?.primary != null) {
                OutlinedButton(onClick = { steppingDown = true }) { Text("Step down primary…") }
            }
            FilterChip(
                selected = auto,
                onClick = { auto = !auto },
                label = { Text("Auto 5s", style = MaterialTheme.typography.labelSmall) },
            )
            OutlinedButton(onClick = { reload() }) { Text("Refresh") }
        }
        error?.let { Text(it, color = MaterialTheme.colorScheme.error) }
        actionError?.let { msg ->
            Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Text(msg, color = MaterialTheme.colorScheme.error, modifier = Modifier.weight(1f))
                TextButton(onClick = { actionError = null }) { Text("Dismiss") }
            }
        }

        snap?.let { s ->
            Card(border = CardDefaults.outlinedCardBorder()) {
                Row(
                    modifier = Modifier.padding(16.dp),
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(24.dp),
                ) {
                    HealthCircle(s.health.score)
                    Column(verticalArrangement = Arrangement.spacedBy(4.dp)) {
                        InfoRow("Type", s.topology.type)
                        s.topology.setName?.let { InfoRow("Replica set", it) }
                        s.topology.primary?.let { InfoRow("Primary", it) }
                    }
                }
                if (s.health.reasons.isNotEmpty()) {
                    Column(modifier = Modifier.padding(start = 16.dp, end = 16.dp, bottom = 12.dp)) {
                        s.health.reasons.forEach {
                            Text("• $it", style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                        }
                    }
                }
            }

            Text("Topology", style = MaterialTheme.typography.titleMedium)
            Card(border = CardDefaults.outlinedCardBorder()) {
                val actions = TopologyActions(
                    onStepDown = if (!readOnly && s.topology.primary != null) {
                        { steppingDown = true }
                    } else null,
                    onEditMember = if (!readOnly) {
                        { m -> editingMember = m }
                    } else null,
                    // Navigation, not mutation — available on read-only connections too;
                    // the twin record inherits the read-only flag.
                    onDirectConnect = { host -> directConnect(host) },
                    onFreeze = if (!readOnly) {
                        { host -> freezingHost = host }
                    } else null,
                    onRemoveMember = if (!readOnly) {
                        { m -> removingMember = m }
                    } else null,
                    onRemoveShard = if (!readOnly) {
                        { sh -> removingShard = sh }
                    } else null,
                )
                ClusterTopologyDiagram(s, actions)
                s.sharded?.balancerEnabled?.let { on ->
                    Row(
                        verticalAlignment = Alignment.CenterVertically,
                        horizontalArrangement = Arrangement.spacedBy(8.dp),
                        modifier = Modifier.padding(start = 20.dp, bottom = 8.dp),
                    ) {
                        Text(
                            if (on) "balancer enabled" else "balancer disabled",
                            style = MaterialTheme.typography.labelSmall,
                            color = if (on) MaterialTheme.colorScheme.onSurfaceVariant else Color(0xFFFACC15),
                        )
                        if (!readOnly) {
                            TextButton(onClick = { balancerTarget = !on }) {
                                Text(if (on) "Pause…" else "Resume…", style = MaterialTheme.typography.labelSmall)
                            }
                        }
                    }
                }
            }

            if (s.topology.type == "replicaset") {
                ReplicationSection(connectionId, registry)
            }

            if (s.topology.members.isNotEmpty()) {
                Text("Members", style = MaterialTheme.typography.titleMedium)
                LazyVerticalGrid(
                    columns = GridCells.Adaptive(minSize = 220.dp),
                    horizontalArrangement = Arrangement.spacedBy(10.dp),
                    verticalArrangement = Arrangement.spacedBy(10.dp),
                    modifier = Modifier.heightIn(min = 100.dp, max = 600.dp),
                ) {
                    items(s.topology.members) { MemberCard(it) }
                }
            }

            s.rsConfig?.let { RsConfigSection(it, onEditMember = if (readOnly) null else { m -> editingMember = m }) }
        }
    }

    if (steppingDown) {
        StepDownDialog(
            connectionId = connectionId,
            registry = registry,
            primary = snap?.topology?.primary,
            onClose = { steppingDown = false },
            onDone = { reload() },
        )
    }
    editingMember?.let { m ->
        EditMemberDialog(
            connectionId = connectionId,
            registry = registry,
            member = m,
            allMembers = snap?.rsConfig?.members.orEmpty(),
            onClose = { editingMember = null },
            onDone = { reload() },
        )
    }
    freezingHost?.let { host ->
        FreezeDialog(
            host = host,
            onRun = { secs ->
                freezingHost = null
                scope.launch {
                    runCatching {
                        val record = withContext(Dispatchers.IO) { ctx.connections.get(connectionId) }
                            ?: error("connection record missing")
                        withContext(Dispatchers.IO) { freezeMember(record.uri, host, secs) }
                    }.onFailure { actionError = "Freeze failed: ${it.message}" }
                    fetch()
                }
            },
            onClose = { freezingHost = null },
        )
    }
    removingMember?.let { m ->
        RemoveMemberDialog(
            connectionId = connectionId,
            registry = registry,
            member = m,
            allMembers = snap?.rsConfig?.members.orEmpty(),
            onClose = { removingMember = null },
            onDone = { reload() },
        )
    }
    removingShard?.let { sh ->
        RemoveShardDialog(
            connectionId = connectionId,
            registry = registry,
            shard = sh,
            shardCount = snap?.sharded?.shards?.size ?: 0,
            balancerPaused = snap?.sharded?.balancerEnabled == false,
            onClose = { removingShard = null },
            onDone = { reload() },
        )
    }
    balancerTarget?.let { enable ->
        BalancerDialog(
            enable = enable,
            onConfirm = {
                balancerTarget = null
                scope.launch {
                    runCatching {
                        val client = registry.client(connectionId) ?: error("not connected")
                        withContext(Dispatchers.IO) { setBalancer(client, enable) }
                    }.onFailure { actionError = "Balancer change failed: ${it.message}" }
                    fetch()
                }
            },
            onClose = { balancerTarget = null },
        )
    }
}

@Composable
private fun HealthCircle(score: Int) {
    val color = when {
        score >= 80 -> Color(0xFF4ADE80)
        score >= 50 -> Color(0xFFFACC15)
        else -> Color(0xFFF87171)
    }
    Box(
        modifier = Modifier.size(96.dp).background(MaterialTheme.colorScheme.surface, CircleShape),
        contentAlignment = Alignment.Center,
    ) {
        Box(modifier = Modifier.size(96.dp).background(color.copy(alpha = 0.15f), CircleShape))
        Column(horizontalAlignment = Alignment.CenterHorizontally) {
            Text("$score", style = MaterialTheme.typography.headlineMedium, color = color)
            Text("health", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
        }
    }
}

@Composable
private fun InfoRow(label: String, value: String) {
    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        Text(label.uppercase(), style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
        Text(value, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodyMedium)
    }
}

@Composable
private fun MemberCard(m: MemberInfo) {
    val accent = when (m.state) {
        "PRIMARY" -> Color(0xFF4ADE80)
        "SECONDARY" -> MaterialTheme.colorScheme.primary
        "ARBITER" -> Color(0xFFA78BFA)
        "DOWN", "ROLLBACK", "UNKNOWN" -> Color(0xFFF87171)
        else -> MaterialTheme.colorScheme.onSurfaceVariant
    }
    Card(border = CardDefaults.outlinedCardBorder()) {
        Column(modifier = Modifier.padding(12.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(m.state, style = MaterialTheme.typography.labelSmall, color = accent, modifier = Modifier.weight(1f))
                Box(modifier = Modifier.size(8.dp).background(if (m.health == 1) Color(0xFF4ADE80) else Color(0xFFF87171), CircleShape))
            }
            Text(m.name, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodyMedium)
            Spacer(modifier = Modifier.height(4.dp))
            InfoRow("Ping", m.pingMs?.let { "$it ms" } ?: "—")
            m.lagSeconds?.let { InfoRow("Lag", "${it}s") }
            InfoRow("Uptime", formatUptime(m.uptime))
            InfoRow("Priority", formatPriority(m.priority))
            InfoRow("Votes", m.votes.toString())
        }
    }
}

/* ===================== rs.conf() — structured view ====================== */

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun RsConfigSection(cfg: RsConfig, onEditMember: ((RsMemberConfig) -> Unit)? = null) {
    var showJson by remember { mutableStateOf(false) }
    val clipboard = LocalClipboardManager.current

    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        Text("Configuration", style = MaterialTheme.typography.titleMedium)
        Text(
            "rs.conf()",
            fontFamily = FontFamily.Monospace,
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Spacer(modifier = Modifier.weight(1f))
        TextButton(onClick = { clipboard.setText(AnnotatedString(cfg.json)) }) { Text("Copy JSON") }
        TextButton(onClick = { showJson = !showJson }) { Text(if (showJson) "Hide JSON" else "View JSON") }
    }

    Card(border = CardDefaults.outlinedCardBorder()) {
        Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(14.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Text(cfg.setName, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.titleSmall)
                Chip("v${cfg.version}")
                cfg.term?.let { Chip("term $it") }
                cfg.protocolVersion?.let { Chip("pv$it") }
                if (cfg.configServer) Chip("config server", MaterialTheme.colorScheme.primary)
            }

            Row(horizontalArrangement = Arrangement.spacedBy(24.dp)) {
                Stat("${cfg.members.size}", "members")
                Stat("${cfg.votingMembers}", "voting")
                Stat("${cfg.majority}", "majority")
                if (cfg.arbiters > 0) Stat("${cfg.arbiters}", "arbiters")
                if (cfg.hiddenMembers > 0) Stat("${cfg.hiddenMembers}", "hidden")
                if (cfg.delayedMembers > 0) Stat("${cfg.delayedMembers}", "delayed")
            }

            HorizontalDivider()

            Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Row {
                    Text(
                        "MEMBER",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.weight(1f),
                    )
                    Text(
                        "PRIORITY",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.width(72.dp),
                    )
                    Text(
                        "VOTES",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.width(48.dp),
                    )
                }
                cfg.members.sortedWith(compareByDescending<RsMemberConfig> { it.priority }.thenBy { it.host })
                    .forEach { MemberConfigRow(it, onEdit = onEditMember?.let { cb -> { cb(it) } }) }
            }

            if (cfg.settings.isNotEmpty()) {
                HorizontalDivider()
                FlowRow(
                    horizontalArrangement = Arrangement.spacedBy(28.dp),
                    verticalArrangement = Arrangement.spacedBy(12.dp),
                ) {
                    cfg.settings.forEach { SettingBlock(it) }
                }
            }

            cfg.replicaSetId?.let {
                Text(
                    "replicaSetId $it",
                    fontFamily = FontFamily.Monospace,
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
        }
    }

    AnimatedVisibility(showJson) {
        Card(border = CardDefaults.outlinedCardBorder()) {
            SelectionContainer {
                Text(
                    cfg.json,
                    modifier = Modifier.padding(12.dp).horizontalScroll(rememberScrollState()),
                    fontFamily = FontFamily.Monospace,
                    style = MaterialTheme.typography.bodySmall,
                )
            }
        }
    }
}

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun MemberConfigRow(m: RsMemberConfig, onEdit: (() -> Unit)? = null) {
    Row(verticalAlignment = Alignment.Top) {
        Column(modifier = Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(4.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Text(
                    "#${m.id}",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.width(28.dp),
                )
                Text(m.host, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodyMedium)
            }
            val badges = buildList {
                if (m.arbiterOnly) add("arbiter" to Color(0xFFA78BFA))
                if (m.hidden) add("hidden" to Color(0xFFFACC15))
                if (m.secondaryDelaySecs > 0) add("delayed ${formatUptime(m.secondaryDelaySecs)}" to Color(0xFF60A5FA))
                if (m.votes == 0 && !m.arbiterOnly) add("non-voting" to Color(0xFFF87171))
                if (!m.buildIndexes) add("no indexes" to Color(0xFFF87171))
            }
            if (badges.isNotEmpty() || m.tags.isNotEmpty()) {
                FlowRow(
                    horizontalArrangement = Arrangement.spacedBy(6.dp),
                    verticalArrangement = Arrangement.spacedBy(4.dp),
                    modifier = Modifier.padding(start = 36.dp),
                ) {
                    badges.forEach { (text, color) -> Badge(text, color) }
                    m.tags.forEach { (k, v) -> Badge("$k=$v", MaterialTheme.colorScheme.primary) }
                }
            }
        }
        Text(
            formatPriority(m.priority),
            fontFamily = FontFamily.Monospace,
            style = MaterialTheme.typography.bodyMedium,
            color = if (m.priority != 1.0) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurface,
            modifier = Modifier.width(72.dp),
        )
        Text(
            "${m.votes}",
            fontFamily = FontFamily.Monospace,
            style = MaterialTheme.typography.bodyMedium,
            color = if (m.votes != 1) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurface,
            modifier = Modifier.width(48.dp),
        )
        if (onEdit != null && !m.arbiterOnly) {
            TextButton(onClick = onEdit, contentPadding = PaddingValues(horizontal = 6.dp)) {
                Text("Edit…", style = MaterialTheme.typography.labelSmall)
            }
        }
    }
}

@Composable
private fun SettingBlock(s: RsSetting) {
    Column(verticalArrangement = Arrangement.spacedBy(2.dp)) {
        Text(
            s.label.uppercase(),
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)) {
            Text(
                s.value,
                fontFamily = FontFamily.Monospace,
                style = MaterialTheme.typography.bodyMedium,
                color = if (s.isDefault) MaterialTheme.colorScheme.onSurface else MaterialTheme.colorScheme.primary,
            )
            if (!s.isDefault) Badge("tuned", MaterialTheme.colorScheme.primary)
        }
    }
}

@Composable
private fun Chip(text: String, color: Color = MaterialTheme.colorScheme.onSurfaceVariant) {
    Text(
        text,
        style = MaterialTheme.typography.labelSmall,
        color = color,
        modifier = Modifier
            .background(color.copy(alpha = 0.12f), RoundedCornerShape(999.dp))
            .padding(horizontal = 8.dp, vertical = 2.dp),
    )
}

@Composable
private fun Badge(text: String, color: Color) {
    Text(
        text,
        style = MaterialTheme.typography.labelSmall,
        color = color,
        modifier = Modifier
            .background(color.copy(alpha = 0.12f), RoundedCornerShape(4.dp))
            .padding(horizontal = 6.dp, vertical = 1.dp),
    )
}

@Composable
private fun Stat(value: String, label: String) {
    Column {
        Text(value, style = MaterialTheme.typography.titleMedium)
        Text(label, style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
    }
}

private fun formatPriority(p: Double): String =
    if (p == p.toLong().toDouble()) "${p.toLong()}" else "$p"

private fun formatUptime(seconds: Long): String = when {
    seconds < 60 -> "${seconds}s"
    seconds < 3600 -> "${seconds / 60}m"
    seconds < 86400 -> "${seconds / 3600}h"
    else -> "${seconds / 86400}d"
}
