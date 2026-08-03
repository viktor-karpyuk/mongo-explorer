package io.mex.ui.cluster

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.width
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.material3.ButtonDefaults
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import io.mex.mongo.MongoRegistry
import io.mex.mongo.RemoveShardStatus
import io.mex.mongo.RsMemberConfig
import io.mex.mongo.ShardInfo
import io.mex.mongo.applyReconfig
import io.mex.mongo.buildRemoveMember
import io.mex.mongo.fetchRawRsConfig
import io.mex.mongo.removalMajorityShift
import io.mex.mongo.removeShard
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

/**
 * `rs.freeze()` — Tier-1 (reversible) so a plain confirm with the seconds spelled
 * out is enough friction. 0 seconds lifts an existing freeze.
 */
@Composable
fun FreezeDialog(host: String, onRun: (Int) -> Unit, onClose: () -> Unit) {
    var secsText by remember { mutableStateOf("120") }
    val secs = secsText.toIntOrNull()

    AlertDialog(
        onDismissRequest = onClose,
        title = { Text("Freeze elections") },
        text = {
            Column(modifier = Modifier.width(420.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
                Text(
                    "Prevents this member from seeking election for the given number of " +
                        "seconds — useful while doing maintenance on the rest of the set. " +
                        "Enter 0 to lift an existing freeze.",
                    style = MaterialTheme.typography.bodySmall,
                )
                Text(host, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall)
                OutlinedTextField(
                    value = secsText,
                    onValueChange = { secsText = it },
                    label = { Text("Seconds") },
                    singleLine = true,
                    isError = secs == null || secs < 0,
                    modifier = Modifier.fillMaxWidth(),
                )
            }
        },
        confirmButton = {
            Button(
                enabled = secs != null && secs >= 0,
                onClick = { onRun(secs!!) },
            ) { Text(if (secs == 0) "Unfreeze" else "Freeze") }
        },
        dismissButton = { TextButton(onClick = onClose) { Text("Cancel") } },
    )
}

/**
 * Removes a member from rs.conf via reconfig — Tier-2, so the host must be typed
 * verbatim and the majority shift is previewed before the button arms (TOPO-5).
 */
@Composable
fun RemoveMemberDialog(
    connectionId: String,
    registry: MongoRegistry,
    member: RsMemberConfig,
    allMembers: List<RsMemberConfig>,
    onClose: () -> Unit,
    onDone: () -> Unit,
) {
    var typed by remember { mutableStateOf("") }
    var running by remember { mutableStateOf(false) }
    var result by remember { mutableStateOf<String?>(null) }
    var failed by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()
    val (majBefore, majAfter) = removalMajorityShift(allMembers, member.id)
    val votersAfter = allMembers.count { it.id != member.id && it.votes > 0 }
    val lastMember = allMembers.size <= 1

    AlertDialog(
        onDismissRequest = onClose,
        title = { Text("Remove member from set") },
        text = {
            Column(modifier = Modifier.width(440.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
                Text(member.host, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall)
                Text(
                    "The member is removed from the replica set configuration. Its data files " +
                        "stay on disk but stop replicating, and clients stop routing to it.",
                    style = MaterialTheme.typography.bodySmall,
                )
                if (lastMember) {
                    Text(
                        "This is the only member — the set cannot be emptied.",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.error,
                    )
                }
                if (majBefore != majAfter) {
                    Text(
                        "Voting majority changes: $majBefore → $majAfter.",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.error,
                    )
                }
                if (!lastMember && votersAfter > 0 && votersAfter % 2 == 0) {
                    Text(
                        "The set is left with $votersAfter voting members — an even number " +
                            "cannot break election ties on its own.",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.error,
                    )
                }
                if (result == null) {
                    OutlinedTextField(
                        value = typed,
                        onValueChange = { typed = it },
                        label = { Text("Type \"${member.host}\" to confirm") },
                        singleLine = true,
                        isError = typed.isNotEmpty() && typed != member.host,
                        modifier = Modifier.fillMaxWidth(),
                        textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
                    )
                }
                result?.let {
                    Text(
                        it,
                        style = MaterialTheme.typography.bodySmall,
                        color = if (failed) MaterialTheme.colorScheme.error else MaterialTheme.colorScheme.primary,
                    )
                }
            }
        },
        confirmButton = {
            if (result == null || failed) {
                Button(
                    enabled = typed == member.host && !running && !lastMember,
                    onClick = {
                        running = true
                        scope.launch {
                            runCatching {
                                val client = registry.client(connectionId) ?: error("not connected")
                                withContext(Dispatchers.IO) {
                                    applyReconfig(client, buildRemoveMember(fetchRawRsConfig(client), member.id))
                                }
                            }.onSuccess {
                                result = "Member removed from the set."
                                failed = false
                                onDone()
                            }.onFailure {
                                result = it.message ?: "Reconfig failed"
                                failed = true
                            }
                            running = false
                        }
                    },
                    colors = ButtonDefaults.buttonColors(
                        containerColor = MaterialTheme.colorScheme.error,
                        contentColor = MaterialTheme.colorScheme.onError,
                    ),
                ) { Text(if (running) "Removing…" else "Remove member") }
            } else {
                Button(onClick = onClose) { Text("Close") }
            }
        },
        dismissButton = { TextButton(onClick = onClose) { Text("Cancel") } },
    )
}

/**
 * Drain & remove a shard, or show drain progress — the same server command does both,
 * so the typed confirm only guards the initial (state-changing) invocation (TOPO-5).
 */
@Composable
fun RemoveShardDialog(
    connectionId: String,
    registry: MongoRegistry,
    shard: ShardInfo,
    shardCount: Int,
    balancerPaused: Boolean,
    onClose: () -> Unit,
    onDone: () -> Unit,
) {
    var typed by remember { mutableStateOf("") }
    var running by remember { mutableStateOf(false) }
    var status by remember { mutableStateOf<RemoveShardStatus?>(null) }
    var errText by remember { mutableStateOf<String?>(null) }
    val scope = rememberCoroutineScope()
    val lastShard = shardCount <= 1

    fun runCommand() {
        running = true
        scope.launch {
            runCatching {
                val client = registry.client(connectionId) ?: error("not connected")
                withContext(Dispatchers.IO) { removeShard(client, shard.name) }
            }.onSuccess {
                status = it
                errText = null
                onDone()
            }.onFailure { e ->
                val msg = e.message.orEmpty()
                // A drain that finished between polls removes the shard, and the status
                // probe then errors — that outcome is a success, not a failure.
                errText = if (msg.contains("ShardNotFound", ignoreCase = true) ||
                    msg.contains("does not exist", ignoreCase = true)
                ) {
                    "Shard is no longer part of the cluster — the drain completed and it was removed."
                } else {
                    msg.ifEmpty { "removeShard failed" }
                }
            }
            running = false
        }
    }

    // Already draining → the command is a pure progress probe; run it on open.
    LaunchedEffect(Unit) { if (shard.draining) runCommand() }

    AlertDialog(
        onDismissRequest = onClose,
        title = { Text(if (shard.draining || status != null) "Shard drain status" else "Drain & remove shard") },
        text = {
            Column(modifier = Modifier.width(460.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
                Text(shard.name, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall)
                if (balancerPaused) {
                    Text(
                        "The balancer is paused — chunks only migrate while it runs, so the " +
                            "drain will not make progress until the balancer is resumed.",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.error,
                    )
                }
                if (status == null && !shard.draining) {
                    Text(
                        "Every chunk on this shard migrates to the remaining shards, then the " +
                            "shard is removed from the cluster. Draining can take hours on large " +
                            "shards and keeps running server-side; re-adding later is a fresh " +
                            "addShard with empty history.",
                        style = MaterialTheme.typography.bodySmall,
                    )
                    if (lastShard) {
                        Text(
                            "This is the only shard — it cannot be removed.",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.error,
                        )
                    } else {
                        OutlinedTextField(
                            value = typed,
                            onValueChange = { typed = it },
                            label = { Text("Type \"${shard.name}\" to confirm") },
                            singleLine = true,
                            isError = typed.isNotEmpty() && typed != shard.name,
                            modifier = Modifier.fillMaxWidth(),
                            textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
                        )
                    }
                }
                status?.let { s ->
                    Text("State: ${s.state}", style = MaterialTheme.typography.bodySmall)
                    s.remainingChunks?.let {
                        Text("Remaining chunks: $it", style = MaterialTheme.typography.bodySmall)
                    }
                    if (s.dbsToMove.isNotEmpty()) {
                        Text(
                            "Databases with their primary on this shard — move them with " +
                                "movePrimary before the drain can finish: ${s.dbsToMove.joinToString(", ")}",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.error,
                        )
                    }
                    if (s.state == "completed") {
                        Text(
                            "Drain complete — the shard has been removed.",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.primary,
                        )
                    }
                }
                errText?.let {
                    Text(it, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.error)
                }
            }
        },
        confirmButton = {
            when {
                status == null && !shard.draining -> Button(
                    enabled = typed == shard.name && !running && !lastShard,
                    onClick = { runCommand() },
                    colors = ButtonDefaults.buttonColors(
                        containerColor = MaterialTheme.colorScheme.error,
                        contentColor = MaterialTheme.colorScheme.onError,
                    ),
                ) { Text(if (running) "Starting…" else "Start drain") }
                status?.state == "completed" -> Button(onClick = onClose) { Text("Close") }
                else -> Button(enabled = !running, onClick = { runCommand() }) {
                    Text(if (running) "Checking…" else "Refresh status")
                }
            }
        },
        dismissButton = { TextButton(onClick = onClose) { Text("Close") } },
    )
}

/** Balancer pause/resume — reversible, so a plain confirm that names the consequence. */
@Composable
fun BalancerDialog(enable: Boolean, onConfirm: () -> Unit, onClose: () -> Unit) {
    AlertDialog(
        onDismissRequest = onClose,
        title = { Text(if (enable) "Resume balancer" else "Pause balancer") },
        text = {
            Text(
                if (enable) {
                    "Chunk migrations resume: the balancer will move chunks between shards " +
                        "whenever the distribution is uneven."
                } else {
                    "Chunk migrations stop until the balancer is resumed. Shards keep serving " +
                        "reads and writes, but an uneven data distribution will not self-correct."
                },
                style = MaterialTheme.typography.bodySmall,
            )
        },
        confirmButton = {
            Button(onClick = onConfirm) { Text(if (enable) "Resume" else "Pause") }
        },
        dismissButton = { TextButton(onClick = onClose) { Text("Cancel") } },
    )
}
