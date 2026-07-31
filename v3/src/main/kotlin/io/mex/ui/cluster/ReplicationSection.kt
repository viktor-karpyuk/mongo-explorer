package io.mex.ui.cluster

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import io.mex.mongo.MongoRegistry
import io.mex.mongo.OplogInfo
import io.mex.mongo.ReplSeverity
import io.mex.mongo.fetchMemberOptimes
import io.mex.mongo.fetchOplogInfo
import io.mex.mongo.lagAgainstPrimary
import io.mex.mongo.lagSeverity
import io.mex.mongo.oplogWindowSeverity
import io.mex.ui.components.Sparkline
import io.mex.util.formatBytes
import io.mex.util.formatDuration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext

private const val POLL_MS = 5_000L
private const val HISTORY = 60 // 60 samples · 5 s = 5 minutes of lag history

private fun severityColor(s: ReplSeverity): Color = when (s) {
    ReplSeverity.ok -> Color(0xFF4ADE80)
    ReplSeverity.warn -> Color(0xFFFBBF24)
    ReplSeverity.critical -> Color(0xFFF87171)
}

/**
 * Oplog window + per-secondary lag history (DBA-OPLOG-1/2). Polls every 5 s while the
 * Cluster tab is open — replication problems develop in minutes, not on manual refresh.
 */
@Composable
fun ReplicationSection(connectionId: String, registry: MongoRegistry) {
    var oplog by remember(connectionId) { mutableStateOf<OplogInfo?>(null) }
    var oplogError by remember(connectionId) { mutableStateOf<String?>(null) }
    var lag by remember(connectionId) { mutableStateOf<Map<String, Long>>(emptyMap()) }
    val lagHistory = remember(connectionId) { mutableStateMapOf<String, List<Double>>() }

    LaunchedEffect(connectionId) {
        while (isActive) {
            val client = registry.client(connectionId)
            if (client != null) {
                // The oplog needs read on `local`, which restricted users may lack —
                // keep the lag half alive even when that half fails.
                try {
                    oplog = withContext(Dispatchers.IO) { fetchOplogInfo(client) }
                    oplogError = null
                } catch (e: Exception) {
                    oplogError = e.message
                }
                runCatching {
                    val current = withContext(Dispatchers.IO) { lagAgainstPrimary(fetchMemberOptimes(client)) }
                    lag = current
                    for ((member, seconds) in current) {
                        val prev = lagHistory[member].orEmpty()
                        lagHistory[member] = (prev + seconds.toDouble()).takeLast(HISTORY)
                    }
                    lagHistory.keys.toList().forEach { if (it !in current) lagHistory.remove(it) }
                }
            }
            delay(POLL_MS)
        }
    }

    Text("Replication", style = MaterialTheme.typography.titleMedium)
    Row(horizontalArrangement = Arrangement.spacedBy(10.dp)) {
        OplogCard(oplog, oplogError, modifier = Modifier.weight(1f))
        lag.entries.sortedBy { it.key }.forEach { (member, seconds) ->
            LagCard(member, seconds, lagHistory[member].orEmpty(), modifier = Modifier.weight(1f))
        }
        if (lag.isEmpty() && oplog != null) {
            Card(border = CardDefaults.outlinedCardBorder(), modifier = Modifier.weight(1f)) {
                Column(modifier = Modifier.padding(12.dp)) {
                    Text(
                        "REPLICATION LAG",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                    Spacer(modifier = Modifier.height(4.dp))
                    Text(
                        "No secondaries reporting (single-node set or no primary).",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
            }
        }
    }
}

@Composable
private fun OplogCard(oplog: OplogInfo?, error: String?, modifier: Modifier = Modifier) {
    Card(border = CardDefaults.outlinedCardBorder(), modifier = modifier) {
        Column(modifier = Modifier.padding(12.dp)) {
            Text(
                "OPLOG WINDOW",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            Spacer(modifier = Modifier.height(4.dp))
            when {
                error != null -> Text(
                    "unavailable — $error (reading local.oplog.rs needs extra privileges)",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
                oplog == null -> Text("…", style = MaterialTheme.typography.titleLarge)
                else -> {
                    val severity = oplogWindowSeverity(oplog.windowSeconds)
                    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        Text(
                            formatDuration(oplog.windowSeconds),
                            style = MaterialTheme.typography.titleLarge,
                            fontFamily = FontFamily.Monospace,
                            color = severityColor(severity),
                        )
                        if (severity != ReplSeverity.ok) {
                            Text(
                                if (severity == ReplSeverity.critical)
                                    "a secondary down longer than this needs a full resync"
                                else
                                    "under 24 h of margin",
                                style = MaterialTheme.typography.labelSmall,
                                color = severityColor(severity),
                            )
                        }
                    }
                    Spacer(modifier = Modifier.height(4.dp))
                    Text(
                        "${formatBytes(oplog.usedBytes)} of ${formatBytes(oplog.maxBytes)} " +
                            "(%.0f%%)".format(oplog.usedPercent),
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        fontFamily = FontFamily.Monospace,
                    )
                }
            }
        }
    }
}

@Composable
private fun LagCard(member: String, seconds: Long, history: List<Double>, modifier: Modifier = Modifier) {
    val severity = lagSeverity(seconds)
    Card(border = CardDefaults.outlinedCardBorder(), modifier = modifier) {
        Column(modifier = Modifier.padding(12.dp)) {
            Text(
                "LAG · $member",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                maxLines = 1,
            )
            Spacer(modifier = Modifier.height(4.dp))
            Text(
                if (seconds == 0L) "in sync" else formatDuration(seconds),
                style = MaterialTheme.typography.titleLarge,
                fontFamily = FontFamily.Monospace,
                color = severityColor(severity),
            )
            Spacer(modifier = Modifier.height(4.dp))
            Sparkline(history, severityColor(severity))
        }
    }
}
