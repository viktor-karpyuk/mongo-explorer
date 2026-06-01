package io.mex.ui.components

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.unit.dp
import io.mex.mongo.ConnectionState

@Composable
fun StatePill(state: ConnectionState) {
    val (label, color) = when (state) {
        is ConnectionState.Disconnected -> "disconnected" to MaterialTheme.colorScheme.outline
        is ConnectionState.Connecting -> "connecting…" to Color(0xFFFACC15)
        is ConnectionState.Connected -> "connected · ${state.pingMs} ms" to Color(0xFF4ADE80)
        is ConnectionState.Error -> "error" to MaterialTheme.colorScheme.error
    }
    Row(
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(6.dp),
        modifier = Modifier
            .background(color.copy(alpha = 0.10f), shape = RoundedCornerShape(999.dp))
            .padding(horizontal = 8.dp, vertical = 3.dp),
    ) {
        Box(
            modifier = Modifier
                .size(6.dp)
                .background(color, CircleShape),
        )
        Text(
            text = label,
            color = color,
            style = MaterialTheme.typography.labelSmall,
        )
    }
}
