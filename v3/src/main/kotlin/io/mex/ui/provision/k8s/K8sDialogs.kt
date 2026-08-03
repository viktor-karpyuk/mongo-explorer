package io.mex.ui.provision.k8s

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.selection.SelectionContainer
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import io.mex.backup.ToolInfo
import io.mex.data.K8sDeployment
import io.mex.provision.k8s.ComponentStatus
import io.mex.provision.k8s.KubeTarget
import io.mex.provision.k8s.bundleText
import io.mex.provision.k8s.crPlural
import io.mex.provision.k8s.ctxArgs
import io.mex.provision.k8s.eventsArgs
import io.mex.provision.k8s.getJsonArgs
import io.mex.provision.k8s.kubectlRead
import io.mex.provision.k8s.parseEventLines
import io.mex.provision.k8s.psmdbComponents
import io.mex.provision.k8s.render
import io.mex.provision.k8s.shortHash
import io.mex.util.formatAgo
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/** Per-component readiness + recent events, read on demand (K8P-UI-4). */
@Composable
fun StatusDetailDialog(tool: ToolInfo, d: K8sDeployment, onClose: () -> Unit) {
    var components by remember { mutableStateOf<List<ComponentStatus>>(emptyList()) }
    var events by remember { mutableStateOf<List<String>>(emptyList()) }
    var loading by remember { mutableStateOf(true) }

    LaunchedEffect(d.id) {
        loading = true
        val target = KubeTarget(d.spec.context, d.spec.kubeconfigPath)
        val cr = withContext(Dispatchers.IO) {
            kubectlRead(tool, getJsonArgs(target, crPlural(d.spec.operator), d.spec.name, d.spec.namespace))
        }
        components = psmdbComponents(cr?.second?.joinToString("\n"))
        val ev = withContext(Dispatchers.IO) {
            kubectlRead(tool, eventsArgs(target, d.spec.namespace), timeoutSec = 15)
        }
        events = parseEventLines(ev?.second.orEmpty())
        loading = false
    }

    AlertDialog(
        onDismissRequest = onClose,
        title = { Text("${d.spec.name} — status", style = MaterialTheme.typography.titleMedium) },
        confirmButton = { TextButton(onClick = onClose) { Text("Close") } },
        text = {
            Column(modifier = Modifier.width(620.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
                Text(
                    buildString {
                        append(d.status.name)
                        d.appliedAt?.let { append(" · applied ${formatAgo(it)}") }
                        d.bundleHash?.let { append(" · ${shortHash(it)}") }
                    },
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    fontFamily = FontFamily.Monospace,
                )
                if (loading) {
                    CircularProgressIndicator(modifier = Modifier.width(20.dp), strokeWidth = 2.dp)
                }
                if (components.isNotEmpty()) {
                    Text("COMPONENTS", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                    components.forEach { c ->
                        Row {
                            Text(c.name, style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace, modifier = Modifier.width(150.dp))
                            Text(c.ready, style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace, modifier = Modifier.width(80.dp))
                            Text(c.detail, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                        }
                    }
                }
                if (events.isNotEmpty()) {
                    Text(
                        "RECENT EVENTS (${d.spec.namespace})",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.padding(top = 6.dp),
                    )
                    MonoScrollPane(events, maxHeight = 200)
                }
            }
        },
    )
}

/** Read-only view of the bundle as rendered from the stored spec (K8P-UI-3). */
@Composable
fun RenderedBundleDialog(d: K8sDeployment, onClose: () -> Unit) {
    val clipboard = LocalClipboardManager.current
    val docs = remember(d.id) { runCatching { render(d.spec) }.getOrNull().orEmpty() }
    val lines = remember(docs) { bundleText(docs).lines() }
    AlertDialog(
        onDismissRequest = onClose,
        title = { Text("${d.spec.name} — rendered bundle", style = MaterialTheme.typography.titleMedium) },
        confirmButton = { TextButton(onClick = onClose) { Text("Close") } },
        dismissButton = {
            TextButton(onClick = { clipboard.setText(AnnotatedString(bundleText(docs))) }) { Text("Copy YAML") }
        },
        text = {
            Column(modifier = Modifier.width(680.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
                Text(
                    docs.joinToString(" · ") { "${it.kind} ${it.name}" },
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
                MonoScrollPane(lines, maxHeight = 420)
            }
        },
    )
}

/** Virtualized monospace pane — bundles and event lists can be long. */
@Composable
internal fun MonoScrollPane(lines: List<String>, maxHeight: Int) {
    val listState = rememberLazyListState()
    androidx.compose.material3.Surface(
        color = Color(0xFF0A0C10),
        shape = RoundedCornerShape(6.dp),
        modifier = Modifier.fillMaxWidth().heightIn(max = maxHeight.dp),
    ) {
        Box {
            SelectionContainer {
                LazyColumn(state = listState, modifier = Modifier.padding(8.dp)) {
                    items(lines.size) { i ->
                        Text(
                            lines[i].ifBlank { " " },
                            color = Color(0xFFD4D7DF),
                            fontFamily = FontFamily.Monospace,
                            style = MaterialTheme.typography.labelSmall,
                        )
                    }
                }
            }
            androidx.compose.foundation.VerticalScrollbar(
                adapter = androidx.compose.foundation.rememberScrollbarAdapter(listState),
                modifier = Modifier.align(Alignment.CenterEnd).background(Color.Transparent),
            )
        }
    }
}
