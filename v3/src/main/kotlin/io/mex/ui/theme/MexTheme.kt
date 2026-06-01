package io.mex.ui.theme

import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.graphics.Color

private val DarkColors = darkColorScheme(
    primary = Color(0xFF4EA1FF),
    onPrimary = Color(0xFF0B1018),
    background = Color(0xFF0F1115),
    onBackground = Color(0xFFE6E8EC),
    surface = Color(0xFF161A21),
    onSurface = Color(0xFFE6E8EC),
    surfaceVariant = Color(0xFF1D222C),
    onSurfaceVariant = Color(0xFF8A93A3),
    outline = Color(0xFF2F3645),
    error = Color(0xFFF87171),
)

private val LightColors = lightColorScheme(
    primary = Color(0xFF1E6EE0),
    background = Color(0xFFF7F8FA),
    surface = Color(0xFFFFFFFF),
    surfaceVariant = Color(0xFFEEF1F5),
    onSurfaceVariant = Color(0xFF5A6573),
    outline = Color(0xFFC7CDD9),
)

@Composable
fun MexTheme(
    darkTheme: Boolean = isSystemInDarkTheme(),
    content: @Composable () -> Unit,
) {
    MaterialTheme(
        colorScheme = if (darkTheme) DarkColors else LightColors,
        content = content,
    )
}
