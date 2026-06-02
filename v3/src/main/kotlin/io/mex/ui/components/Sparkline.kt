package io.mex.ui.components

import androidx.compose.foundation.Canvas
import androidx.compose.foundation.layout.size
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.drawscope.Stroke
import androidx.compose.ui.unit.dp

@Composable
fun Sparkline(values: List<Double>, color: Color, widthDp: Int = 160, heightDp: Int = 32) {
    Canvas(modifier = Modifier.size(widthDp.dp, heightDp.dp)) {
        if (values.size < 2) return@Canvas
        val w = size.width
        val h = size.height
        val max = values.maxOrNull() ?: 1.0
        val min = values.minOrNull() ?: 0.0
        val range = (max - min).coerceAtLeast(1e-9)
        val path = Path()
        values.forEachIndexed { i, v ->
            val x = (i.toFloat() / (values.size - 1)) * w
            val y = h - ((v - min) / range).toFloat() * h
            if (i == 0) path.moveTo(x, y) else path.lineTo(x, y)
        }
        drawPath(path, color = color, style = Stroke(width = 1.5f))
    }
}
