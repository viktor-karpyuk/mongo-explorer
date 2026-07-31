package io.mex.ui.cluster

import androidx.compose.foundation.background
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.selection.SelectionContainer
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import io.mex.mongo.MemberPatch
import io.mex.mongo.MongoRegistry
import io.mex.mongo.RsMemberConfig
import io.mex.mongo.applyReconfig
import io.mex.mongo.buildReconfig
import io.mex.mongo.fetchRawRsConfig
import io.mex.mongo.majorityShift
import io.mex.mongo.reconfigDiff
import io.mex.mongo.stepDownCommand
import io.mex.mongo.stepDownPrimary
import io.mex.mongo.validateMemberPatch
import io.mex.ui.components.ConfirmDangerDialog
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings

private val PRETTY = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).indent(true).build()

/* ============================ step down ============================ */

/**
 * Guided `rs.stepDown()` (DBA-RCFG-1): the exact command is previewed, the consequence
 * is spelled out, and execution sits behind a typed confirm.
 */
@Composable
fun StepDownDialog(
    connectionId: String,
    registry: MongoRegistry,
    primary: String?,
    onClose: () -> Unit,
    onDone: () -> Unit,
) {
    var stepDownSecs by remember { mutableStateOf("60") }
    var catchUpSecs by remember { mutableStateOf("10") }
    var confirming by remember { mutableStateOf(false) }
    var running by remember { mutableStateOf(false) }
    var result by remember { mutableStateOf<String?>(null) }
    var failed by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    val secs = stepDownSecs.toIntOrNull()
    val catchUp = catchUpSecs.toIntOrNull()
    val valid = secs != null && secs > 0 && catchUp != null && catchUp >= 0
    val command = if (valid) stepDownCommand(secs!!, catchUp!!).toJson() else ""

    AlertDialog(
        onDismissRequest = { if (!running) onClose() },
        title = { Text("Step down primary", style = MaterialTheme.typography.titleMedium) },
        confirmButton = {
            if (result == null) {
                Button(enabled = valid && !running, onClick = { confirming = true }) {
                    Text(if (running) "Stepping down…" else "Step down…")
                }
            } else {
                Button(onClick = { onDone(); onClose() }) { Text("Close") }
            }
        },
        dismissButton = { if (result == null && !running) TextButton(onClick = onClose) { Text("Cancel") } },
        text = {
            Column(modifier = Modifier.width(480.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Text(
                    "Asks ${primary ?: "the primary"} to yield. It becomes ineligible for " +
                        "re-election for the step-down window while the set elects a new primary; " +
                        "writes fail during the election (normally a few seconds).",
                    style = MaterialTheme.typography.bodySmall,
                )
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    OutlinedTextField(
                        value = stepDownSecs,
                        onValueChange = { stepDownSecs = it.filter { c -> c.isDigit() } },
                        label = { Text("stepDownSecs") },
                        singleLine = true,
                        modifier = Modifier.weight(1f),
                    )
                    OutlinedTextField(
                        value = catchUpSecs,
                        onValueChange = { catchUpSecs = it.filter { c -> c.isDigit() } },
                        label = { Text("catch-up period (s)") },
                        singleLine = true,
                        modifier = Modifier.weight(1f),
                    )
                }
                if (valid) CommandPreview(command)
                result?.let {
                    Text(
                        it,
                        style = MaterialTheme.typography.bodySmall,
                        color = if (failed) MaterialTheme.colorScheme.error else Color(0xFF4ADE80),
                    )
                }
            }
        },
    )

    if (confirming) {
        ConfirmDangerDialog(
            title = "Step down ${primary ?: "the primary"}?",
            text = "This sends $command to the primary. An election follows; writes are " +
                "briefly unavailable. If no secondary catches up within ${catchUp}s the " +
                "command fails and the primary stays.",
            confirmLabel = "Step down",
            onConfirm = {
                confirming = false
                running = true
                scope.launch {
                    try {
                        val client = registry.client(connectionId) ?: error("Not connected")
                        withContext(Dispatchers.IO) { stepDownPrimary(client, secs!!, catchUp!!) }
                        result = "Step-down accepted — a new election is under way."
                        failed = false
                    } catch (e: Exception) {
                        // Old servers close every connection on step-down, so a network
                        // error here is ambiguous; say so instead of guessing.
                        result = "${e.message} — note: some server versions drop connections " +
                            "on step-down, so this can still have succeeded. Check the Cluster tab."
                        failed = true
                    } finally {
                        running = false
                    }
                }
            },
            onCancel = { confirming = false },
        )
    }
}

/* ============================ member reconfig ============================ */

/**
 * Guided per-member `rs.reconfig` (DBA-RCFG-2): edit priority/votes/hidden/delay with
 * client-side validation, preview the diff + full new config fetched fresh from the
 * server, then apply behind a typed confirm.
 */
@Composable
fun EditMemberDialog(
    connectionId: String,
    registry: MongoRegistry,
    member: RsMemberConfig,
    allMembers: List<RsMemberConfig>,
    onClose: () -> Unit,
    onDone: () -> Unit,
) {
    var priority by remember { mutableStateOf(member.priority.toString()) }
    var votes by remember { mutableStateOf(member.votes) }
    var hidden by remember { mutableStateOf(member.hidden) }
    var delay by remember { mutableStateOf(member.secondaryDelaySecs.toString()) }
    var previewJson by remember { mutableStateOf<String?>(null) }
    var previewConfig by remember { mutableStateOf<org.bson.Document?>(null) }
    var confirming by remember { mutableStateOf(false) }
    var running by remember { mutableStateOf(false) }
    var result by remember { mutableStateOf<String?>(null) }
    var failed by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    val patch = MemberPatch(
        id = member.id,
        host = member.host,
        priority = priority.toDoubleOrNull() ?: -1.0,
        votes = votes,
        hidden = hidden,
        secondaryDelaySecs = delay.toLongOrNull() ?: -1L,
    )
    val problems = validateMemberPatch(patch)
    val diff = reconfigDiff(member, patch)
    val (majBefore, majAfter) = majorityShift(allMembers, patch)

    AlertDialog(
        onDismissRequest = { if (!running) onClose() },
        title = { Text("Edit member — ${member.host}", style = MaterialTheme.typography.titleMedium) },
        confirmButton = {
            when {
                result != null -> Button(onClick = { onDone(); onClose() }) { Text("Close") }
                previewJson == null -> OutlinedButton(
                    enabled = problems.isEmpty() && diff.isNotEmpty() && !running,
                    onClick = {
                        scope.launch {
                            try {
                                val client = registry.client(connectionId) ?: error("Not connected")
                                // Fetched fresh so the version bump applies to the config
                                // as it is now, not as it was when the dialog opened.
                                val raw = withContext(Dispatchers.IO) { fetchRawRsConfig(client) }
                                val next = buildReconfig(raw, patch)
                                previewConfig = next
                                previewJson = next.toJson(PRETTY)
                                failed = false
                            } catch (e: Exception) {
                                result = e.message
                                failed = true
                            }
                        }
                    },
                ) { Text("Preview reconfig") }
                else -> Button(enabled = !running, onClick = { confirming = true }) {
                    Text(if (running) "Applying…" else "Apply…")
                }
            }
        },
        dismissButton = { if (result == null && !running) TextButton(onClick = onClose) { Text("Cancel") } },
        text = {
            Column(
                modifier = Modifier.width(560.dp).heightIn(max = 560.dp).verticalScroll(rememberScrollState()),
                verticalArrangement = Arrangement.spacedBy(8.dp),
            ) {
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalAlignment = Alignment.CenterVertically) {
                    OutlinedTextField(
                        value = priority,
                        onValueChange = { priority = it; previewJson = null },
                        label = { Text("Priority (0–1000)") },
                        singleLine = true,
                        modifier = Modifier.weight(1f),
                    )
                    Box {
                        var votesMenu by remember { mutableStateOf(false) }
                        OutlinedButton(onClick = { votesMenu = true }) { Text("votes: $votes ⌄") }
                        DropdownMenu(expanded = votesMenu, onDismissRequest = { votesMenu = false }) {
                            listOf(1, 0).forEach { v ->
                                DropdownMenuItem(
                                    text = { Text("$v") },
                                    onClick = { votesMenu = false; votes = v; previewJson = null },
                                )
                            }
                        }
                    }
                    OutlinedTextField(
                        value = delay,
                        onValueChange = { delay = it.filter { c -> c.isDigit() }; previewJson = null },
                        label = { Text("Delay (s)") },
                        singleLine = true,
                        modifier = Modifier.width(110.dp),
                    )
                }
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Checkbox(checked = hidden, onCheckedChange = { hidden = it; previewJson = null })
                    Text("Hidden — invisible to clients, never primary", style = MaterialTheme.typography.bodySmall)
                }
                problems.forEach {
                    Text("✗ $it", color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.labelSmall)
                }
                if (problems.isEmpty() && diff.isEmpty()) {
                    Text(
                        "No changes yet.",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
                if (diff.isNotEmpty()) {
                    Column {
                        Text("Changes", style = MaterialTheme.typography.titleSmall)
                        diff.forEach { Text("• $it", style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace) }
                        if (majBefore != majAfter) {
                            Text(
                                "⚠ voting majority moves from $majBefore to $majAfter",
                                style = MaterialTheme.typography.bodySmall,
                                color = Color(0xFFFBBF24),
                            )
                        }
                    }
                }
                previewJson?.let { json ->
                    Text("New configuration (version bumped)", style = MaterialTheme.typography.titleSmall)
                    Surface(color = Color(0xFF0A0C10), shape = RoundedCornerShape(4.dp)) {
                        SelectionContainer {
                            Text(
                                json,
                                color = Color(0xFFD4D7DF),
                                fontFamily = FontFamily.Monospace,
                                style = MaterialTheme.typography.labelSmall,
                                modifier = Modifier
                                    .padding(8.dp)
                                    .heightIn(max = 220.dp)
                                    .verticalScroll(rememberScrollState())
                                    .horizontalScroll(rememberScrollState()),
                            )
                        }
                    }
                }
                result?.let {
                    Text(
                        it,
                        style = MaterialTheme.typography.bodySmall,
                        color = if (failed) MaterialTheme.colorScheme.error else Color(0xFF4ADE80),
                    )
                }
            }
        },
    )

    if (confirming) {
        ConfirmDangerDialog(
            title = "Reconfigure ${member.host}?",
            text = buildString {
                append("This sends { replSetReconfig: … } with: ${diff.joinToString("; ")}. ")
                if (majBefore != majAfter) append("The voting majority moves from $majBefore to $majAfter. ")
                append("A reconfig briefly interrupts replication heartbeats.")
            },
            confirmLabel = "Apply reconfig",
            onConfirm = {
                confirming = false
                running = true
                scope.launch {
                    try {
                        val client = registry.client(connectionId) ?: error("Not connected")
                        withContext(Dispatchers.IO) { applyReconfig(client, previewConfig!!) }
                        result = "Reconfig applied."
                        failed = false
                    } catch (e: Exception) {
                        result = e.message
                        failed = true
                    } finally {
                        running = false
                    }
                }
            },
            onCancel = { confirming = false },
        )
    }
}

@Composable
private fun CommandPreview(command: String) {
    Surface(color = Color(0xFF0A0C10), shape = RoundedCornerShape(4.dp)) {
        SelectionContainer {
            Text(
                command,
                color = Color(0xFFD4D7DF),
                fontFamily = FontFamily.Monospace,
                style = MaterialTheme.typography.labelSmall,
                modifier = Modifier.padding(8.dp),
            )
        }
    }
}
