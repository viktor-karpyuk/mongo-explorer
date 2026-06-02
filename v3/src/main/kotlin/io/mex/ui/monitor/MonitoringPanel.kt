package io.mex.ui.monitor

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.grid.GridCells
import androidx.compose.foundation.lazy.grid.LazyVerticalGrid
import androidx.compose.foundation.lazy.grid.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import io.mex.mongo.MongoRegistry
import io.mex.mongo.MonitorTick
import io.mex.mongo.ProfilerLevel
import io.mex.mongo.SlowOp
import io.mex.mongo.getProfilerLevel
import io.mex.mongo.listSlowOps
import io.mex.mongo.setProfilerLevel
import io.mex.mongo.tick
import io.mex.ui.components.Sparkline
import io.mex.util.formatBytes
import io.mex.util.formatCount
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

private const val HISTORY = 60

@Composable
fun MonitoringPanel(connectionId: String, registry: MongoRegistry) {
    val opsRate = remember { mutableStateListOf<Double>() }
    val netIn = remember { mutableStateListOf<Double>() }
    val netOut = remember { mutableStateListOf<Double>() }
    val conns = remember { mutableStateListOf<Double>() }
    val cachePct = remember { mutableStateListOf<Double>() }
    val latReads = remember { mutableStateListOf<Double>() }
    val latWrites = remember { mutableStateListOf<Double>() }

    var latest by remember { mutableStateOf<MonitorTick?>(null) }
    var prev by remember { mutableStateOf<MonitorTick?>(null) }
    var error by remember { mutableStateOf<String?>(null) }

    LaunchedEffect(connectionId) {
        opsRate.clear(); netIn.clear(); netOut.clear()
        conns.clear(); cachePct.clear(); latReads.clear(); latWrites.clear()
        latest = null; prev = null

        while (isActive) {
            val client = registry.client(connectionId)
            if (client != null) {
                try {
                    val t = withContext(Dispatchers.IO) { tick(client) }
                    val p = prev
                    prev = t
                    latest = t
                    if (p != null) {
                        val dt = ((t.ts - p.ts) / 1000.0).coerceAtLeast(1.0)
                        push(opsRate, (t.opsTotal - p.opsTotal) / dt)
                        push(netIn, (t.networkInBytes - p.networkInBytes) / dt)
                        push(netOut, (t.networkOutBytes - p.networkOutBytes) / dt)
                        push(conns, t.currentConnections.toDouble())
                        push(cachePct, t.wtCachePercent)
                        push(latReads, t.latencyReadsAvgUs ?: 0.0)
                        push(latWrites, t.latencyWritesAvgUs ?: 0.0)
                    }
                    error = null
                } catch (e: Exception) {
                    error = e.message
                }
            }
            delay(2000)
        }
    }

    Column(modifier = Modifier.fillMaxSize().padding(24.dp).verticalScroll(rememberScrollState())) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Text("Monitoring", style = MaterialTheme.typography.headlineSmall, modifier = Modifier.weight(1f))
            Text("2 s refresh · last 2 min", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
        }
        error?.let { Text(it, color = MaterialTheme.colorScheme.error) }
        Spacer(modifier = Modifier.height(12.dp))

        val metrics = listOf(
            Metric("Operations / sec", formatCount((opsRate.lastOrNull() ?: 0.0).toLong()), opsRate.toList(), Color(0xFF4EA1FF)),
            Metric("Network in / sec", formatBytes((netIn.lastOrNull() ?: 0.0).toLong()), netIn.toList(), Color(0xFF34D399)),
            Metric("Network out / sec", formatBytes((netOut.lastOrNull() ?: 0.0).toLong()), netOut.toList(), Color(0xFFA78BFA)),
            Metric(
                "Active connections",
                "${latest?.currentConnections ?: 0} / ${(latest?.currentConnections ?: 0L) + (latest?.availableConnections ?: 0L)}",
                conns.toList(),
                Color(0xFFFBBF24),
            ),
            Metric("WT cache used", "%.1f%%".format(latest?.wtCachePercent ?: 0.0), cachePct.toList(), Color(0xFFF472B6)),
            Metric(
                "Avg read latency",
                latest?.latencyReadsAvgUs?.let { "%.2f ms".format(it / 1000.0) } ?: "—",
                latReads.toList(),
                Color(0xFF60A5FA),
            ),
            Metric(
                "Avg write latency",
                latest?.latencyWritesAvgUs?.let { "%.2f ms".format(it / 1000.0) } ?: "—",
                latWrites.toList(),
                Color(0xFFF87171),
            ),
            Metric("Resident memory", "${latest?.residentMb ?: 0} MB", emptyList(), Color(0xFF94A3B8)),
        )

        LazyVerticalGrid(
            columns = GridCells.Adaptive(minSize = 220.dp),
            horizontalArrangement = Arrangement.spacedBy(10.dp),
            verticalArrangement = Arrangement.spacedBy(10.dp),
            modifier = Modifier.heightIn(min = 100.dp, max = 800.dp),
        ) {
            items(metrics) { MetricCard(it) }
        }
        Spacer(modifier = Modifier.height(20.dp))
        ProfilerSection(connectionId, registry)
    }
}

private data class Metric(val label: String, val value: String, val spark: List<Double>, val color: Color)

@Composable
private fun MetricCard(m: Metric) {
    Card(border = CardDefaults.outlinedCardBorder()) {
        Column(modifier = Modifier.padding(12.dp)) {
            Text(
                m.label.uppercase(),
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            Spacer(modifier = Modifier.height(4.dp))
            Text(m.value, style = MaterialTheme.typography.titleLarge, fontFamily = FontFamily.Monospace)
            Spacer(modifier = Modifier.height(4.dp))
            Sparkline(m.spark, m.color)
        }
    }
}

@Composable
private fun ProfilerSection(connectionId: String, registry: MongoRegistry) {
    var dbName by remember { mutableStateOf("admin") }
    var profile by remember { mutableStateOf(ProfilerLevel(0, 100)) }
    var slow by remember { mutableStateOf<List<SlowOp>>(emptyList()) }
    var error by remember { mutableStateOf<String?>(null) }
    val scope = rememberCoroutineScope()

    fun load() {
        scope.launch {
            val client = registry.client(connectionId) ?: return@launch
            try {
                profile = withContext(Dispatchers.IO) { getProfilerLevel(client, dbName) }
                slow = withContext(Dispatchers.IO) { listSlowOps(client, dbName) }
                error = null
            } catch (e: Exception) {
                error = e.message
            }
        }
    }

    Card(border = CardDefaults.outlinedCardBorder()) {
        Column(modifier = Modifier.padding(12.dp)) {
            Text("Profiler / slow ops", style = MaterialTheme.typography.titleMedium)
            Spacer(modifier = Modifier.height(8.dp))
            Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                OutlinedTextField(dbName, { dbName = it }, label = { Text("Database") }, singleLine = true, modifier = Modifier.width(180.dp))
                OutlinedTextField(
                    profile.level.toString(),
                    { profile = profile.copy(level = it.toIntOrNull() ?: profile.level) },
                    label = { Text("Level 0/1/2") },
                    singleLine = true,
                    modifier = Modifier.width(120.dp),
                )
                OutlinedTextField(
                    profile.slowMs.toString(),
                    { profile = profile.copy(slowMs = it.toIntOrNull() ?: profile.slowMs) },
                    label = { Text("slowMs") },
                    singleLine = true,
                    modifier = Modifier.width(120.dp),
                )
                OutlinedButton(onClick = { load() }) { Text("Load") }
                Button(onClick = {
                    scope.launch {
                        val client = registry.client(connectionId) ?: return@launch
                        try {
                            withContext(Dispatchers.IO) { setProfilerLevel(client, dbName, profile) }
                            load()
                        } catch (e: Exception) { error = e.message }
                    }
                }) { Text("Apply") }
            }
            error?.let { Spacer(modifier = Modifier.height(4.dp)); Text(it, color = MaterialTheme.colorScheme.error) }
            Spacer(modifier = Modifier.height(8.dp))
            slow.forEach { op ->
                Row(
                    modifier = Modifier.fillMaxWidth().padding(vertical = 2.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    Text(op.op, modifier = Modifier.width(80.dp), style = MaterialTheme.typography.bodySmall)
                    Text(op.ns, fontFamily = FontFamily.Monospace, color = MaterialTheme.colorScheme.primary, modifier = Modifier.width(220.dp), style = MaterialTheme.typography.bodySmall)
                    Text("${op.millis} ms", color = Color(0xFFFBBF24), modifier = Modifier.width(80.dp), style = MaterialTheme.typography.bodySmall)
                    Text(
                        op.command?.take(120) ?: "",
                        fontFamily = FontFamily.Monospace,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
            }
        }
    }
}

private fun push(list: MutableList<Double>, v: Double) {
    if (list.size >= HISTORY) list.removeAt(0)
    list.add(v)
}
