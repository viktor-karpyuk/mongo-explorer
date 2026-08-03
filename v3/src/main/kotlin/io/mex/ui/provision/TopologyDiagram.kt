package io.mex.ui.provision

import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import io.mex.data.LabTopology

/**
 * Live picture of the cluster being built (PRV-UI-2 polish): the wizard's steppers are
 * numbers, this is the shape. Node boxes are cheap composables — even 6×3+3+3 sharded
 * is ~24 boxes, nothing for the layout engine.
 */
@OptIn(ExperimentalLayoutApi::class)
@Composable
fun TopologyDiagram(topology: LabTopology, modifier: Modifier = Modifier) {
    Column(
        modifier = modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.25f), RoundedCornerShape(8.dp))
            .padding(12.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        when (topology) {
            is LabTopology.Standalone -> NodeGroup("mongod", 1, primary = false)
            is LabTopology.ReplicaSet -> NodeGroup("rs0", topology.members, primary = true)
            is LabTopology.Sharded -> {
                Row(
                    horizontalArrangement = Arrangement.spacedBy(6.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    repeat(topology.mongos) { NodeBox("mongos", router = true) }
                    Text(
                        "→ clients connect here",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
                ConnectorLine()
                NodeGroup("config servers", topology.configServers, primary = true, compact = true)
                FlowRow(
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                    verticalArrangement = Arrangement.spacedBy(8.dp),
                ) {
                    repeat(topology.shards) { i ->
                        NodeGroup("shard ${i + 1}", topology.membersPerShard, primary = true, compact = true)
                    }
                }
            }
        }
    }
}

@Composable
private fun NodeGroup(label: String, members: Int, primary: Boolean, compact: Boolean = false) {
    Column(
        modifier = Modifier
            .border(1.dp, MaterialTheme.colorScheme.outlineVariant, RoundedCornerShape(6.dp))
            .padding(horizontal = 8.dp, vertical = 6.dp),
        verticalArrangement = Arrangement.spacedBy(4.dp),
    ) {
        Text(
            label,
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            fontFamily = FontFamily.Monospace,
        )
        Row(horizontalArrangement = Arrangement.spacedBy(4.dp)) {
            repeat(members) { i ->
                NodeBox(
                    label = if (primary && i == 0 && members > 1) "P" else if (members > 1) "S" else "1",
                    accent = primary && i == 0 && members > 1,
                    small = compact,
                )
            }
        }
    }
}

@Composable
private fun NodeBox(label: String, router: Boolean = false, accent: Boolean = false, small: Boolean = false) {
    val bg = when {
        router -> Color(0xFF60A5FA).copy(alpha = 0.18f)
        accent -> Color(0xFF4ADE80).copy(alpha = 0.18f)
        else -> MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.7f)
    }
    val fg = when {
        router -> Color(0xFF60A5FA)
        accent -> Color(0xFF4ADE80)
        else -> MaterialTheme.colorScheme.onSurfaceVariant
    }
    Box(
        modifier = Modifier
            .height(if (small) 20.dp else 24.dp)
            .background(bg, RoundedCornerShape(4.dp))
            .padding(horizontal = if (router) 8.dp else 6.dp),
        contentAlignment = Alignment.Center,
    ) {
        Text(
            label,
            style = MaterialTheme.typography.labelSmall,
            fontFamily = FontFamily.Monospace,
            color = fg,
        )
    }
}

@Composable
private fun ConnectorLine() {
    Box(
        modifier = Modifier
            .padding(start = 12.dp)
            .width(1.dp)
            .height(8.dp)
            .background(MaterialTheme.colorScheme.outlineVariant),
    )
}
