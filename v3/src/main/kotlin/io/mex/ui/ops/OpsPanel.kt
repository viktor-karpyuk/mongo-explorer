package io.mex.ui.ops

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
import io.mex.mongo.CurrentOp
import io.mex.mongo.MongoRegistry
import io.mex.mongo.killOp
import io.mex.mongo.listCurrentOps
import io.mex.ui.components.ConfirmDangerDialog
import io.mex.ui.components.ReadOnlyBadge
import io.mex.util.formatDuration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/**
 * Live `$currentOp` view with kill (DBA-OP-1/2) — the first thing a DBA opens when
 * "the database is slow". Long-runners float to the top, collection scans and ops
 * waiting on locks are flagged, and any op can be killed behind a typed confirm.
 */
@Composable
fun OpsPanel(connectionId: String, registry: MongoRegistry, readOnly: Boolean) {
    var ops by remember(connectionId) { mutableStateOf<List<CurrentOp>>(emptyList()) }
    var error by remember(connectionId) { mutableStateOf<String?>(null) }
    var paused by remember(connectionId) { mutableStateOf(false) }
    var includeIdle by remember(connectionId) { mutableStateOf(false) }
    var includeSystem by remember(connectionId) { mutableStateOf(false) }
    var filter by remember(connectionId) { mutableStateOf("") }
    var killing by remember(connectionId) { mutableStateOf<CurrentOp?>(null) }
    var killError by remember(connectionId) { mutableStateOf<String?>(null) }
    val scope = rememberCoroutineScope()

    suspend fun refresh() {
        val client = registry.client(connectionId) ?: return
        try {
            ops = withContext(Dispatchers.IO) { listCurrentOps(client, includeIdle) }
            error = null
        } catch (e: Exception) {
            error = e.message
        }
    }

    LaunchedEffect(connectionId, paused, includeIdle) {
        while (isActive) {
            if (!paused) refresh()
            delay(2_000)
        }
    }

    val visible = ops
        .asSequence()
        .filter { includeSystem || !it.system }
        .filter {
            filter.isBlank() ||
                it.ns.contains(filter, true) ||
                it.op.contains(filter, true) ||
                (it.appName?.contains(filter, true) ?: false) ||
                (it.client?.contains(filter, true) ?: false) ||
                (it.effectiveUser?.contains(filter, true) ?: false) ||
                it.opid.contains(filter, true)
        }
        .sortedWith(compareByDescending<CurrentOp> { it.active }.thenByDescending { it.secsRunning ?: -1 })
        .toList()

    Column(modifier = Modifier.fillMaxSize().padding(horizontal = 24.dp, vertical = 16.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            Text("Operations", style = MaterialTheme.typography.headlineSmall)
            if (readOnly) ReadOnlyBadge()
            Spacer(modifier = Modifier.weight(1f))
            val collscans = visible.count { it.collscan }
            if (collscans > 0) {
                Text(
                    "$collscans COLLSCAN",
                    style = MaterialTheme.typography.labelSmall,
                    color = Color(0xFFF87171),
                )
            }
            Text(
                "${visible.size} op(s) · ${if (paused) "paused" else "2 s refresh"}",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            TextButton(onClick = { paused = !paused }) { Text(if (paused) "Resume" else "Pause") }
        }

        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(12.dp)) {
            OutlinedTextField(
                value = filter,
                onValueChange = { filter = it },
                placeholder = { Text("Filter by ns, op, app, client, user, opid…", style = MaterialTheme.typography.bodySmall) },
                singleLine = true,
                modifier = Modifier.weight(1f),
                textStyle = MaterialTheme.typography.bodySmall,
            )
            Row(verticalAlignment = Alignment.CenterVertically) {
                Checkbox(checked = includeIdle, onCheckedChange = { includeIdle = it })
                Text("idle connections", style = MaterialTheme.typography.bodySmall)
            }
            Row(verticalAlignment = Alignment.CenterVertically) {
                Checkbox(checked = includeSystem, onCheckedChange = { includeSystem = it })
                Text("system ops", style = MaterialTheme.typography.bodySmall)
            }
        }
        Spacer(modifier = Modifier.height(8.dp))
        error?.let {
            Text(it, color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.bodySmall)
            Spacer(modifier = Modifier.height(4.dp))
        }
        killError?.let {
            Text("Kill failed: $it", color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.bodySmall)
            Spacer(modifier = Modifier.height(4.dp))
        }

        HeaderRow()
        HorizontalDivider()
        if (visible.isEmpty() && error == null) {
            Text(
                if (ops.isEmpty()) "No in-flight operations right now."
                else "Nothing matches the current filter.",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(vertical = 16.dp),
            )
        }
        val listState = rememberLazyListState()
        Box(modifier = Modifier.weight(1f)) {
            LazyColumn(state = listState, modifier = Modifier.fillMaxSize()) {
                items(visible, key = { it.opid + it.desc.orEmpty() }) { op ->
                    OpRow(op, readOnly, onKill = { killing = op; killError = null })
                    HorizontalDivider(thickness = 0.5.dp)
                }
            }
            VerticalScrollbar(
                adapter = rememberScrollbarAdapter(listState),
                modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
            )
        }
    }

    killing?.let { op ->
        ConfirmDangerDialog(
            title = "Kill operation ${op.opid}?",
            text = buildString {
                append("This sends { killOp: 1, op: ${op.opid} } to the server. ")
                append("The operation is interrupted at its next yield point; the client sees an error. ")
                append("\n\n${op.op} on ${op.ns.ifBlank { "(no namespace)" }}")
                op.secsRunning?.let { append(" · running ${formatDuration(it)}") }
                op.appName?.let { append(" · $it") }
            },
            confirmLabel = "Kill op",
            onConfirm = {
                val captured = op
                killing = null
                scope.launch {
                    val client = registry.client(connectionId) ?: return@launch
                    try {
                        withContext(Dispatchers.IO) { killOp(client, captured.opidRaw) }
                        refresh()
                    } catch (e: Exception) {
                        killError = e.message
                    }
                }
            },
            onCancel = { killing = null },
        )
    }
}

@Composable
private fun HeaderRow() {
    Row(modifier = Modifier.padding(vertical = 4.dp)) {
        HeaderCell("OPID", 110.dp)
        HeaderCell("OP", 70.dp)
        Text(
            "NAMESPACE",
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.weight(1f),
        )
        HeaderCell("TIME", 70.dp)
        HeaderCell("PLAN", 90.dp)
        Text(
            "CLIENT / APP",
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.width(200.dp),
        )
        Spacer(modifier = Modifier.width(60.dp))
    }
}

@Composable
private fun HeaderCell(text: String, width: androidx.compose.ui.unit.Dp) {
    Text(
        text,
        style = MaterialTheme.typography.labelSmall,
        color = MaterialTheme.colorScheme.onSurfaceVariant,
        modifier = Modifier.width(width),
    )
}

@Composable
private fun OpRow(op: CurrentOp, readOnly: Boolean, onKill: () -> Unit) {
    var expanded by remember(op.opid) { mutableStateOf(false) }
    // Escalating time colour: white under 10 s, amber under a minute, red beyond.
    val timeColor = when {
        (op.secsRunning ?: 0) >= 60 -> Color(0xFFF87171)
        (op.secsRunning ?: 0) >= 10 -> Color(0xFFFBBF24)
        else -> MaterialTheme.colorScheme.onSurface
    }
    Column(modifier = Modifier.clickable { expanded = !expanded }.padding(vertical = 3.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Text(
                op.opid,
                style = MaterialTheme.typography.bodySmall,
                fontFamily = FontFamily.Monospace,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis,
                modifier = Modifier.width(110.dp),
            )
            Text(
                op.op,
                style = MaterialTheme.typography.bodySmall,
                fontFamily = FontFamily.Monospace,
                color = if (op.active) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.width(70.dp),
            )
            Row(modifier = Modifier.weight(1f), verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                Text(
                    op.ns.ifBlank { op.desc ?: "—" },
                    style = MaterialTheme.typography.bodySmall,
                    fontFamily = FontFamily.Monospace,
                    maxLines = 1,
                    overflow = TextOverflow.Ellipsis,
                )
                if (op.waitingForLock) Badge("lock wait", Color(0xFFFBBF24))
            }
            Text(
                op.secsRunning?.let { formatDuration(it) } ?: "—",
                style = MaterialTheme.typography.bodySmall,
                fontFamily = FontFamily.Monospace,
                color = timeColor,
                modifier = Modifier.width(70.dp),
            )
            Box(modifier = Modifier.width(90.dp)) {
                when {
                    op.collscan -> Badge("COLLSCAN", Color(0xFFF87171))
                    op.planSummary != null -> Text(
                        op.planSummary,
                        style = MaterialTheme.typography.labelSmall,
                        fontFamily = FontFamily.Monospace,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
            }
            Text(
                listOfNotNull(op.appName, op.effectiveUser, op.client).joinToString(" · ").ifBlank { "—" },
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis,
                modifier = Modifier.width(200.dp),
            )
            Box(modifier = Modifier.width(60.dp), contentAlignment = Alignment.CenterEnd) {
                if (!readOnly && op.opidRaw != null && op.active) {
                    TextButton(onClick = onKill, contentPadding = PaddingValues(horizontal = 6.dp)) {
                        Text("Kill", color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.labelSmall)
                    }
                }
            }
        }
        if (expanded && op.command != null) {
            SelectionContainer {
                Text(
                    op.command,
                    style = MaterialTheme.typography.labelSmall,
                    fontFamily = FontFamily.Monospace,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(start = 110.dp, top = 2.dp, bottom = 4.dp, end = 8.dp),
                    maxLines = 12,
                    overflow = TextOverflow.Ellipsis,
                )
            }
        }
    }
}

@Composable
private fun Badge(text: String, color: Color) {
    Text(
        text,
        style = MaterialTheme.typography.labelSmall,
        color = color,
        modifier = Modifier
            .background(color.copy(alpha = 0.14f), RoundedCornerShape(4.dp))
            .padding(horizontal = 5.dp, vertical = 1.dp),
    )
}
