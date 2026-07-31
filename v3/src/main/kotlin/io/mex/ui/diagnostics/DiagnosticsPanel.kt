package io.mex.ui.diagnostics

import androidx.compose.foundation.VerticalScrollbar
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
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
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import io.mex.mongo.LogLine
import io.mex.mongo.MongoRegistry
import io.mex.mongo.ParamRow
import io.mex.mongo.fetchServerLog
import io.mex.mongo.fetchServerParameters
import io.mex.mongo.fetchStartupWarnings
import io.mex.mongo.severityRank
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

private enum class DiagTab { Log, Parameters }

/**
 * Server diagnostics (DBA-DIAG-1..3): startup warnings surfaced first, the in-memory log
 * tail with severity/search filtering, and every server parameter with curated tunables
 * flagged when they differ from their default. Read-only by nature — no gating needed.
 */
@Composable
fun DiagnosticsPanel(connectionId: String, registry: MongoRegistry) {
    var tab by remember(connectionId) { mutableStateOf(DiagTab.Log) }
    var warnings by remember(connectionId) { mutableStateOf<List<LogLine>>(emptyList()) }
    var log by remember(connectionId) { mutableStateOf<List<LogLine>>(emptyList()) }
    var params by remember(connectionId) { mutableStateOf<List<ParamRow>>(emptyList()) }
    var error by remember(connectionId) { mutableStateOf<String?>(null) }
    var loading by remember(connectionId) { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    fun load() {
        scope.launch {
            val client = registry.client(connectionId) ?: return@launch
            loading = true
            try {
                withContext(Dispatchers.IO) {
                    warnings = runCatching { fetchStartupWarnings(client) }.getOrDefault(emptyList())
                    log = fetchServerLog(client)
                    params = fetchServerParameters(client)
                }
                error = null
            } catch (e: Exception) {
                error = e.message
            } finally {
                loading = false
            }
        }
    }
    LaunchedEffect(connectionId) { load() }

    Column(modifier = Modifier.fillMaxSize().padding(horizontal = 24.dp, vertical = 16.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            Text("Diagnostics", style = MaterialTheme.typography.headlineSmall)
            Spacer(modifier = Modifier.weight(1f))
            Text(
                "${log.size} log line(s) · ${params.size} parameter(s) · ${params.count { it.tuned }} tuned",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            OutlinedButton(onClick = { load() }, enabled = !loading) {
                Text(if (loading) "Loading…" else "Refresh")
            }
        }
        error?.let { Text(it, color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.bodySmall) }

        StartupWarnings(warnings)

        Row {
            DiagTabBtn("Server log", tab == DiagTab.Log) { tab = DiagTab.Log }
            DiagTabBtn(
                "Parameters${params.count { it.tuned }.takeIf { it > 0 }?.let { " ($it tuned)" } ?: ""}",
                tab == DiagTab.Parameters,
            ) { tab = DiagTab.Parameters }
        }
        HorizontalDivider()
        Box(modifier = Modifier.weight(1f)) {
            when (tab) {
                DiagTab.Log -> LogTab(log)
                DiagTab.Parameters -> ParametersTab(params)
            }
        }
    }
}

@Composable
private fun DiagTabBtn(label: String, active: Boolean, onClick: () -> Unit) {
    val color = if (active) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant
    TextButton(onClick = onClick) { Text(label, color = color, style = MaterialTheme.typography.labelMedium) }
}

/* ============================ startup warnings ============================ */

@Composable
private fun StartupWarnings(warnings: List<LogLine>) {
    // getLog delivers braces and blank filler lines around the real warnings.
    val real = warnings.filter { it.message.isNotBlank() && it.message != "{" && it.message != "}" }
    if (real.isEmpty()) {
        Text(
            "No startup warnings.",
            style = MaterialTheme.typography.labelSmall,
            color = Color(0xFF4ADE80),
            modifier = Modifier.padding(vertical = 6.dp),
        )
        return
    }
    Surface(
        color = Color(0xFFFBBF24).copy(alpha = 0.10f),
        shape = RoundedCornerShape(6.dp),
        modifier = Modifier.fillMaxWidth().padding(vertical = 6.dp),
    ) {
        SelectionContainer {
            Column(modifier = Modifier.padding(10.dp), verticalArrangement = Arrangement.spacedBy(2.dp)) {
                Text(
                    "STARTUP WARNINGS (${real.size})",
                    style = MaterialTheme.typography.labelSmall,
                    color = Color(0xFFB45309),
                )
                real.take(12).forEach {
                    Text(
                        "⚠ ${it.message}",
                        style = MaterialTheme.typography.bodySmall,
                        color = Color(0xFFB45309),
                        fontFamily = FontFamily.Monospace,
                    )
                }
                if (real.size > 12) {
                    Text("… ${real.size - 12} more", style = MaterialTheme.typography.labelSmall, color = Color(0xFFB45309))
                }
            }
        }
    }
}

/* ============================ server log ============================ */

@Composable
private fun LogTab(log: List<LogLine>) {
    var minSeverity by remember { mutableStateOf("I") }
    var sevMenu by remember { mutableStateOf(false) }
    var search by remember { mutableStateOf("") }

    val visible = remember(log, minSeverity, search) {
        log.asReversed() // newest first
            .filter { severityRank(it.severity) >= severityRank(minSeverity) }
            .filter {
                search.isBlank() ||
                    it.message.contains(search, true) ||
                    it.component.contains(search, true) ||
                    it.context.contains(search, true) ||
                    (it.attrJson?.contains(search, true) ?: false)
            }
    }

    Column(modifier = Modifier.fillMaxSize()) {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            modifier = Modifier.padding(vertical = 6.dp),
        ) {
            OutlinedTextField(
                value = search,
                onValueChange = { search = it },
                placeholder = { Text("Search message, component, attributes…", style = MaterialTheme.typography.bodySmall) },
                singleLine = true,
                modifier = Modifier.weight(1f),
                textStyle = MaterialTheme.typography.bodySmall,
            )
            Box {
                OutlinedButton(onClick = { sevMenu = true }) {
                    Text("≥ ${severityLabel(minSeverity)} ⌄", style = MaterialTheme.typography.labelSmall)
                }
                DropdownMenu(expanded = sevMenu, onDismissRequest = { sevMenu = false }) {
                    listOf("D", "I", "W", "E").forEach { s ->
                        DropdownMenuItem(
                            text = { Text(severityLabel(s)) },
                            onClick = { sevMenu = false; minSeverity = s },
                        )
                    }
                }
            }
        }
        if (visible.isEmpty()) {
            Text(
                "No log lines match.",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(vertical = 12.dp),
            )
        }
        val listState = rememberLazyListState()
        Box(modifier = Modifier.weight(1f)) {
            LazyColumn(state = listState) {
                items(visible.size) { i -> LogRow(visible[i]) }
            }
            VerticalScrollbar(
                adapter = rememberScrollbarAdapter(listState),
                modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
            )
        }
    }
}

private fun severityLabel(s: String) = when (s) {
    "F" -> "fatal"
    "E" -> "error"
    "W" -> "warning"
    "I" -> "info"
    else -> "debug"
}

@Composable
private fun LogRow(line: LogLine) {
    var expanded by remember(line.raw) { mutableStateOf(false) }
    val sevColor = when (line.severity.firstOrNull()) {
        'F', 'E' -> Color(0xFFF87171)
        'W' -> Color(0xFFFBBF24)
        'I' -> MaterialTheme.colorScheme.onSurfaceVariant
        else -> Color.Gray
    }
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .clickable { expanded = !expanded }
            .padding(vertical = 1.dp),
    ) {
        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            Text(
                line.ts.take(19).replace("T", " "),
                style = MaterialTheme.typography.labelSmall,
                fontFamily = FontFamily.Monospace,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.width(140.dp),
            )
            Text(
                line.severity,
                style = MaterialTheme.typography.labelSmall,
                fontFamily = FontFamily.Monospace,
                color = sevColor,
                modifier = Modifier.width(18.dp),
            )
            Text(
                line.component,
                style = MaterialTheme.typography.labelSmall,
                fontFamily = FontFamily.Monospace,
                color = MaterialTheme.colorScheme.primary,
                modifier = Modifier.width(84.dp),
                maxLines = 1,
                overflow = TextOverflow.Ellipsis,
            )
            Text(
                line.message,
                style = MaterialTheme.typography.bodySmall,
                fontFamily = FontFamily.Monospace,
                maxLines = if (expanded) 6 else 1,
                overflow = TextOverflow.Ellipsis,
                modifier = Modifier.weight(1f),
            )
        }
        if (expanded && line.attrJson != null) {
            SelectionContainer {
                Text(
                    line.attrJson,
                    style = MaterialTheme.typography.labelSmall,
                    fontFamily = FontFamily.Monospace,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.padding(start = 166.dp, bottom = 3.dp),
                    maxLines = 14,
                    overflow = TextOverflow.Ellipsis,
                )
            }
        }
    }
}

/* ============================ parameters ============================ */

@Composable
private fun ParametersTab(params: List<ParamRow>) {
    var search by remember { mutableStateOf("") }
    var onlyTuned by remember { mutableStateOf(false) }

    val visible = remember(params, search, onlyTuned) {
        params
            .filter { !onlyTuned || it.tuned }
            .filter { search.isBlank() || it.name.contains(search, true) || it.value.contains(search, true) }
    }

    Column(modifier = Modifier.fillMaxSize()) {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            modifier = Modifier.padding(vertical = 6.dp),
        ) {
            OutlinedTextField(
                value = search,
                onValueChange = { search = it },
                placeholder = { Text("Search parameters…", style = MaterialTheme.typography.bodySmall) },
                singleLine = true,
                modifier = Modifier.weight(1f),
                textStyle = MaterialTheme.typography.bodySmall,
            )
            Row(verticalAlignment = Alignment.CenterVertically) {
                Checkbox(checked = onlyTuned, onCheckedChange = { onlyTuned = it })
                Text("only tuned", style = MaterialTheme.typography.bodySmall)
            }
        }
        if (visible.isEmpty()) {
            Text(
                if (onlyTuned) "No curated tunable differs from its default." else "No parameters match.",
                style = MaterialTheme.typography.bodySmall,
                color = if (onlyTuned) Color(0xFF4ADE80) else MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(vertical = 12.dp),
            )
        }
        val listState = rememberLazyListState()
        Box(modifier = Modifier.weight(1f)) {
            LazyColumn(state = listState) {
                items(visible, key = { it.name }) { p ->
                    Row(
                        verticalAlignment = Alignment.CenterVertically,
                        horizontalArrangement = Arrangement.spacedBy(8.dp),
                        modifier = Modifier.fillMaxWidth().padding(vertical = 2.dp),
                    ) {
                        Text(
                            p.name,
                            style = MaterialTheme.typography.bodySmall,
                            fontFamily = FontFamily.Monospace,
                            color = if (p.tuned) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurface,
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis,
                            modifier = Modifier.weight(0.5f),
                        )
                        SelectionContainer(modifier = Modifier.weight(0.5f)) {
                            Text(
                                p.value,
                                style = MaterialTheme.typography.bodySmall,
                                fontFamily = FontFamily.Monospace,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                                maxLines = 1,
                                overflow = TextOverflow.Ellipsis,
                            )
                        }
                        if (p.tuned) {
                            Text(
                                "tuned · default ${p.default}",
                                style = MaterialTheme.typography.labelSmall,
                                color = Color(0xFFFBBF24),
                                modifier = Modifier
                                    .background(Color(0xFFFBBF24).copy(alpha = 0.12f), RoundedCornerShape(4.dp))
                                    .padding(horizontal = 5.dp, vertical = 1.dp),
                            )
                        }
                    }
                    HorizontalDivider(thickness = 0.5.dp)
                }
            }
            VerticalScrollbar(
                adapter = rememberScrollbarAdapter(listState),
                modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
            )
        }
    }
}
