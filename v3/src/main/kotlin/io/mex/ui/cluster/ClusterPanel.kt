package io.mex.ui.cluster

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.grid.GridCells
import androidx.compose.foundation.lazy.grid.LazyVerticalGrid
import androidx.compose.foundation.lazy.grid.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import io.mex.mongo.ClusterSnapshot
import io.mex.mongo.MemberInfo
import io.mex.mongo.MongoRegistry
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

            s.rsConfigJson?.let { json ->
                Text("rs.conf()", style = MaterialTheme.typography.titleMedium)
                Card(border = CardDefaults.outlinedCardBorder()) {
                    Text(
                        json,
                        modifier = Modifier.padding(12.dp),
                        fontFamily = FontFamily.Monospace,
                        style = MaterialTheme.typography.bodySmall,
                    )
                }
            }
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
            InfoRow("Priority", m.priority.toString())
            InfoRow("Votes", m.votes.toString())
        }
    }
}

private fun formatUptime(seconds: Long): String = when {
    seconds < 60 -> "${seconds}s"
    seconds < 3600 -> "${seconds / 60}m"
    seconds < 86400 -> "${seconds / 3600}h"
    else -> "${seconds / 86400}d"
}
