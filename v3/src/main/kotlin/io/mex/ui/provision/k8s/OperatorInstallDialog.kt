package io.mex.ui.provision.k8s

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import io.mex.backup.ToolInfo
import io.mex.backup.ToolProcess
import io.mex.provision.k8s.InstallPlan
import io.mex.provision.k8s.KubeTarget
import io.mex.provision.k8s.previewCommands
import io.mex.ui.components.TypedConfirmDialog
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlin.coroutines.resume

/**
 * Guided operator install: what will run, then a typed confirm naming the cluster, then
 * the real output streamed. Installing CRDs is cluster-wide and irreversible-ish, so it
 * gets the same ceremony as any other destructive-adjacent action.
 */
@Composable
fun OperatorInstallDialog(
    tool: ToolInfo,
    target: KubeTarget,
    plan: InstallPlan,
    onClose: () -> Unit,
    onInstalled: () -> Unit,
) {
    var phase by remember { mutableStateOf("idle") } // idle | confirm | running | done | failed
    var log by remember { mutableStateOf<List<String>>(emptyList()) }
    var stepIndex by remember { mutableStateOf(0) }
    val scope = remember { CoroutineScope(SupervisorJob() + Dispatchers.IO) }
    var current by remember { mutableStateOf<ToolProcess?>(null) }
    DisposableEffect(Unit) {
        onDispose {
            current?.cancel()
            scope.coroutineContext[Job]?.cancel()
        }
    }

    fun run() {
        phase = "running"
        log = emptyList()
        scope.launch {
            for ((i, step) in plan.steps.withIndex()) {
                stepIndex = i
                log = log + "▶ ${step.label}"
                val code = suspendCancellableCoroutine { cont ->
                    val p = ToolProcess(
                        binary = tool.path,
                        args = step.args,
                        scope = scope,
                        onLine = { line -> log = (log + line).takeLast(400) },
                        onExit = { c -> if (cont.isActive) cont.resume(c) },
                        stdin = step.stdin,
                    )
                    current = p
                    cont.invokeOnCancellation { p.cancel() }
                    p.start()
                }
                if (code != 0) {
                    log = log + "✗ step failed with exit code $code — nothing further was attempted"
                    phase = "failed"
                    return@launch
                }
            }
            log = log + "✓ ${plan.title} ${plan.version} installed"
            phase = "done"
        }
    }

    AlertDialog(
        onDismissRequest = { if (phase != "running") onClose() },
        title = { Text("Install ${plan.title}", style = MaterialTheme.typography.titleMedium) },
        confirmButton = {
            when (phase) {
                "idle", "failed" -> Button(onClick = { phase = "confirm" }) {
                    Text(if (phase == "failed") "Retry" else "Install…")
                }
                "running" -> TextButton(onClick = { current?.cancel() }) { Text("Cancel") }
                else -> Button(onClick = { onInstalled(); onClose() }) { Text("Done — re-detect") }
            }
        },
        dismissButton = {
            if (phase != "running") TextButton(onClick = onClose) { Text("Close") }
        },
        text = {
            Column(modifier = Modifier.width(660.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Text("${plan.summary} (version ${plan.version})", style = MaterialTheme.typography.bodySmall)
                plan.notes.forEach {
                    Text("• $it", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                }
                Text(
                    "Target: ${target.context}",
                    style = MaterialTheme.typography.labelSmall,
                    fontFamily = FontFamily.Monospace,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
                Text(
                    if (phase == "running") "Running step ${stepIndex + 1} of ${plan.steps.size}…"
                    else "These ${plan.steps.size} commands will run, in order:",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
                MonoScrollPane(previewCommands(plan, tool.path), maxHeight = 150)
                if (phase == "running") {
                    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        CircularProgressIndicator(modifier = Modifier.size(14.dp), strokeWidth = 2.dp)
                        Text("installing…", style = MaterialTheme.typography.labelSmall)
                    }
                }
                if (log.isNotEmpty()) {
                    Spacer(modifier = Modifier.height(2.dp))
                    MonoScrollPane(log, maxHeight = 220)
                }
                when (phase) {
                    "done" -> Text(
                        "Installed. Re-detect to continue the wizard.",
                        style = MaterialTheme.typography.labelSmall,
                        color = Color(0xFF4ADE80),
                    )
                    "failed" -> Text(
                        "Install failed — read the log above. Common causes: no permission to create " +
                            "cluster-wide CRDs, or the cluster cannot reach GitHub.",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.error,
                    )
                    else -> Unit
                }
                Text(
                    "Docs: ${plan.docsUrl}",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
        },
    )

    if (phase == "confirm") {
        TypedConfirmDialog(
            title = "Install into ${target.context}?",
            consequence = "This applies ${plan.steps.size} manifests to the cluster \"${target.context}\", " +
                "including cluster-wide CustomResourceDefinitions. It affects every namespace, " +
                "not just the one you are deploying into.",
            requiredText = target.context,
            confirmLabel = "Install",
            onConfirm = { phase = "idle"; run() },
            onCancel = { phase = "idle" },
        )
    }
}
