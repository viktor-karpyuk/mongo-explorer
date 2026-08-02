package io.mex.ui.provision

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.width
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.data.Lab
import io.mex.data.LabTopology
import io.mex.provision.LabEvent
import io.mex.provision.needsKeyfile
import io.mex.provision.plan
import io.mex.provision.summary
import io.mex.util.redactCredentials

/** Read-only details sheet: per-member ports, artifact paths, connection link (PRV-UI-5). */
@Composable
fun LabDetailsDialog(ctx: AppContext, lab: Lab, onClose: () -> Unit) {
    val p = plan(lab)
    val connection = lab.connectionId?.let { ctx.connections.get(it) }
    AlertDialog(
        onDismissRequest = onClose,
        title = { Text("${lab.name} — details", style = MaterialTheme.typography.titleMedium) },
        confirmButton = { TextButton(onClick = onClose) { Text("Close") } },
        text = {
            Column(modifier = Modifier.width(520.dp), verticalArrangement = Arrangement.spacedBy(4.dp)) {
                Text(
                    "${summary(lab.topology)} · mongo ${lab.mongoTag} · ${if (lab.auth) "auth on" else "no auth"}",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    fontFamily = FontFamily.Monospace,
                )
                Spacer(modifier = Modifier.height(6.dp))
                if (lab.portMap.isNotEmpty()) {
                    Row {
                        Text("SERVICE", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant, modifier = Modifier.width(140.dp))
                        Text("HOST PORT", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                    }
                    p.clientServices.forEach { svc ->
                        lab.portMap[svc]?.let { port ->
                            Row {
                                Text(svc, style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace, modifier = Modifier.width(140.dp))
                                Text(
                                    "127.0.0.1:$port" + if (svc == p.clientServices.first() && lab.topology is LabTopology.ReplicaSet) "   ← registered connection" else "",
                                    style = MaterialTheme.typography.bodySmall,
                                    fontFamily = FontFamily.Monospace,
                                )
                            }
                        }
                    }
                }
                if (lab.topology is LabTopology.ReplicaSet) {
                    Spacer(modifier = Modifier.height(4.dp))
                    Text(
                        "ℹ The connection uses directConnection to the first member: members advertise " +
                            "internal names your machine can't resolve. After a failover, open another " +
                            "member's port from this list.",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
                Spacer(modifier = Modifier.height(6.dp))
                KeyValue("compose", "${lab.dir}/compose.yaml")
                if (needsKeyfile(lab)) KeyValue("keyfile", "${lab.dir}/keyfile")
                KeyValue("connection", connection?.name ?: if (lab.connectionId != null) "connection removed" else "none")
                connection?.let { KeyValue("uri", redactCredentials(it.uri)) }
            }
        },
    )
}

@Composable
private fun KeyValue(key: String, value: String) {
    Row {
        Text(key, style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant, modifier = Modifier.width(80.dp))
        Text(value, style = MaterialTheme.typography.labelSmall, fontFamily = FontFamily.Monospace)
    }
}

/** The show-once result banner with the generated root password (PRV-SEC-3). */
@Composable
fun ProvisionResultBanner(
    done: LabEvent.Done,
    labs: List<Lab>,
    onOpen: (connectionId: String) -> Unit,
    onClose: () -> Unit,
) {
    val clipboard = LocalClipboardManager.current
    val lab = labs.firstOrNull { it.id == done.labId }
    AlertDialog(
        onDismissRequest = onClose,
        title = { Text("✓ ${lab?.name ?: "lab"} is running", style = MaterialTheme.typography.titleMedium) },
        confirmButton = {
            lab?.connectionId?.let { connId ->
                Button(onClick = { onOpen(connId) }) { Text("Open connection") }
            } ?: TextButton(onClick = onClose) { Text("Close") }
        },
        dismissButton = { TextButton(onClick = onClose) { Text("Close") } },
        text = {
            Column(modifier = Modifier.width(500.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Text(
                    "Connection \"${lab?.name} (lab)\" was added to the sidebar.",
                    style = MaterialTheme.typography.bodySmall,
                )
                done.password?.let { pw ->
                    Row(verticalAlignment = androidx.compose.ui.Alignment.CenterVertically) {
                        Text("Root password (shown once):  ", style = MaterialTheme.typography.bodySmall)
                        Text(pw, style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace, color = Color(0xFF4ADE80))
                        TextButton(onClick = { clipboard.setText(AnnotatedString(pw)) }) { Text("copy") }
                    }
                    Text(
                        "It is stored inside the encrypted connection — this dialog is the only place it is displayed.",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
                done.uri?.let {
                    Text(redactCredentials(it), style = MaterialTheme.typography.labelSmall, fontFamily = FontFamily.Monospace, color = MaterialTheme.colorScheme.onSurfaceVariant)
                }
            }
        },
    )
}
