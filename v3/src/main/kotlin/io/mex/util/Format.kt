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
