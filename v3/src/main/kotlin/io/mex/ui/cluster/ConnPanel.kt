package io.mex.ui.cluster

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.mongo.ConnectionState
import io.mex.mongo.MongoRegistry
import io.mex.ui.components.ReadOnlyBadge
import io.mex.ui.components.StatePill
import io.mex.ui.monitor.MonitoringPanel
import io.mex.ui.ops.OpsPanel
import io.mex.ui.security.SecurityPanel
import io.mex.ui.shell.ShellPanel
import io.mex.ui.storage.StoragePanel
import kotlinx.coroutines.launch

enum class ConnTab { Cluster, Operations, Monitoring, Security, Storage, Shell }

@Composable
fun ConnectionPanel(ctx: AppContext, connectionId: String, registry: MongoRegistry) {
    var tab by remember(connectionId) { mutableStateOf(ConnTab.Cluster) }
    val states by registry.states.collectAsState()
    val state = states[connectionId] ?: ConnectionState.Disconnected
    val name = remember(connectionId) { ctx.connections.list().find { it.id == connectionId }?.name ?: "Connection" }
    val readOnly = remember(connectionId) { ctx.connections.isReadOnly(connectionId) }
    val scope = rememberCoroutineScope()

    Column(modifier = Modifier.fillMaxSize()) {
        // Cluster-level identity and lifecycle live here; without them the only way to
        // close a connection was to leave for the Connections screen.
        Row(
            modifier = Modifier.fillMaxWidth().padding(horizontal = 12.dp, vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(10.dp),
        ) {
            Text(name, style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
            StatePill(state)
            if (readOnly) ReadOnlyBadge()
            Spacer(modifier = Modifier.weight(1f))
            if (state is ConnectionState.Connected) {
                Text(
                    "${state.topology} · v${state.serverVersion}",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
                OutlinedButton(onClick = { scope.launch { registry.disconnect(connectionId) } }) {
                    Text("Disconnect")
                }
            }
        }
        HorizontalDivider()
        Row(modifier = Modifier.fillMaxWidth().padding(horizontal = 12.dp, vertical = 4.dp)) {
            TabBtn("Cluster", tab == ConnTab.Cluster) { tab = ConnTab.Cluster }
            TabBtn("Operations", tab == ConnTab.Operations) { tab = ConnTab.Operations }
            TabBtn("Monitoring", tab == ConnTab.Monitoring) { tab = ConnTab.Monitoring }
            TabBtn("Security", tab == ConnTab.Security) { tab = ConnTab.Security }
            TabBtn("Storage", tab == ConnTab.Storage) { tab = ConnTab.Storage }
            TabBtn("Shell", tab == ConnTab.Shell) { tab = ConnTab.Shell }
        }
        HorizontalDivider()
        Box(modifier = Modifier.weight(1f)) {
            when (tab) {
                ConnTab.Cluster -> ClusterPanel(connectionId, registry)
                ConnTab.Operations -> OpsPanel(connectionId, registry, readOnly)
                ConnTab.Monitoring -> MonitoringPanel(connectionId, registry, readOnly)
                ConnTab.Security -> SecurityPanel(connectionId, registry, readOnly)
                ConnTab.Storage -> StoragePanel(connectionId, registry, readOnly)
                ConnTab.Shell -> ShellPanel(ctx, connectionId, readOnly)
            }
        }
    }
}

@Composable
private fun TabBtn(label: String, active: Boolean, onClick: () -> Unit) {
    val color = if (active) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant
    TextButton(onClick = onClick) {
        Text(label, color = color, style = MaterialTheme.typography.labelMedium)
    }
}
