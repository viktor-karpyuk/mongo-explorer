package io.mex.ui.cluster

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import io.mex.mongo.MongoRegistry
import io.mex.ui.monitor.MonitoringPanel

enum class ConnTab { Cluster, Monitoring }

@Composable
fun ConnectionPanel(connectionId: String, registry: MongoRegistry) {
    var tab by remember(connectionId) { mutableStateOf(ConnTab.Cluster) }

    Column(modifier = Modifier.fillMaxSize()) {
        Row(modifier = Modifier.fillMaxWidth().padding(horizontal = 12.dp, vertical = 4.dp)) {
            TabBtn("Cluster", tab == ConnTab.Cluster) { tab = ConnTab.Cluster }
            TabBtn("Monitoring", tab == ConnTab.Monitoring) { tab = ConnTab.Monitoring }
        }
        HorizontalDivider()
        Box(modifier = Modifier.weight(1f)) {
            when (tab) {
                ConnTab.Cluster -> ClusterPanel(connectionId, registry)
                ConnTab.Monitoring -> MonitoringPanel(connectionId, registry)
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
