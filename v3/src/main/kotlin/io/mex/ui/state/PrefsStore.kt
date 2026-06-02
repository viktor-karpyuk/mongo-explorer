package io.mex.ui.state

import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import io.mex.data.PrefsRepo

enum class ThemePref { System, Dark, Light }

class PrefsStore(private val repo: PrefsRepo) {
    var theme by mutableStateOf(
        runCatching { ThemePref.valueOf(repo.get("theme") ?: "System") }.getOrDefault(ThemePref.System),
    )
        private set

    var editorFontSize by mutableStateOf((repo.get("editor.fontSize")?.toDoubleOrNull() ?: 12.5).toFloat())
        private set

    var tabWidth by mutableStateOf(repo.get("editor.tabWidth")?.toIntOrNull() ?: 2)
        private set

    fun applyTheme(t: ThemePref) {
        theme = t
        repo.set("theme", t.name)
    }

    fun applyEditorFontSize(v: Float) {
        editorFontSize = v.coerceIn(10f, 20f)
        repo.set("editor.fontSize", editorFontSize.toString())
    }

    fun applyTabWidth(n: Int) {
        tabWidth = n.coerceIn(2, 8)
        repo.set("editor.tabWidth", tabWidth.toString())
    }
}
