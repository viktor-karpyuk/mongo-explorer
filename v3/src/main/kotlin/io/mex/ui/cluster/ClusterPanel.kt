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
import io.mex.mongo.ClusterSnapshot
import io.mex.mongo.MemberInfo
import io.mex.mongo.MongoRegistry
import io.mex.mongo.RsConfig
import io.mex.mongo.RsMemberConfig
import io.mex.mongo.RsSetting
import io.mex.mongo.clusterSnapshot
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

@Composable
fun ClusterPanel(connectionId: String, registry: MongoRegistry) {
    var snap by remember(connectionId) { mutableStateOf<ClusterSnapshot?>(null) }
    var error by remember(connectionId) { mutableStateOf<String?>(null) }
    val scope = rememberCoroutineScope()

    fun reload() {
        scope.launch {
            val client = registry.client(connectionId) ?: return@launch
            try {
                snap = withContext(Dispatchers.IO) { clusterSnapshot(client) }
                // Clearing on success matters — a single transient failure used to leave a
                // permanent error banner sitting above perfectly healthy data.
                error = null
            } catch (e: Exception) {
                error = e.message
            }
        }
    }
    LaunchedEffect(connectionId) { reload() }

    Column(
        modifier = Modifier.fillMaxSize().padding(24.dp).verticalScroll(rememberScrollState()),
        verticalArrangement = Arrangement.spacedBy(12.dp),
    ) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Text("Cluster", style = MaterialTheme.typography.headlineSmall, modifier = Modifier.weight(1f))
            OutlinedButton(onClick = { reload() }) { Text("Refresh") }
        }
        error?.let { Text(it, color = MaterialTheme.colorScheme.error) }

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

            s.rsConfig?.let { RsConfigSection(it) }
        }
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
private fun RsConfigSection(cfg: RsConfig) {
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
                    .forEach { MemberConfigRow(it) }
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
private fun MemberConfigRow(m: RsMemberConfig) {
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
