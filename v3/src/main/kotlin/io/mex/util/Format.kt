package io.mex.util

fun formatBytes(bytes: Long): String {
    if (bytes < 0) return "—"
    if (bytes < 1024) return "$bytes B"
    val units = listOf("KB", "MB", "GB", "TB", "PB")
    var value = bytes.toDouble() / 1024.0
    var i = 0
    while (value >= 1024.0 && i < units.size - 1) {
        value /= 1024.0
        i++
    }
    val formatted = if (value >= 10.0) "%.0f".format(value) else "%.1f".format(value)
    return "$formatted ${units[i]}"
}

fun formatCount(n: Long): String = "%,d".format(n)

fun formatAgo(epochMs: Long, now: Long = System.currentTimeMillis()): String {
    val s = ((now - epochMs) / 1000).coerceAtLeast(0)
    return when {
        s < 60 -> "just now"
        s < 3_600 -> "${s / 60}m ago"
        s < 86_400 -> "${s / 3_600}h ago"
        else -> "${s / 86_400}d ago"
    }
}
