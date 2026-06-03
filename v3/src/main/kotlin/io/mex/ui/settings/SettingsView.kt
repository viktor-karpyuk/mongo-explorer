package io.mex.ui.settings

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.data.UriHistoryEntry
import io.mex.ui.components.ConfirmDangerDialog
import io.mex.ui.state.PrefsStore
import io.mex.ui.state.ThemePref

@Composable
fun SettingsView(ctx: AppContext, prefs: PrefsStore) {
    var history by remember { mutableStateOf<List<UriHistoryEntry>>(emptyList()) }
    var confirmingDelete by remember { mutableStateOf<UriHistoryEntry?>(null) }
    LaunchedEffect(Unit) { history = ctx.uriHistory.list() }

    Column(modifier = Modifier.fillMaxSize().padding(24.dp).verticalScroll(rememberScrollState()), verticalArrangement = Arrangement.spacedBy(12.dp)) {
        Text("Settings", style = MaterialTheme.typography.headlineSmall)

        Card(border = CardDefaults.outlinedCardBorder()) {
            Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Text("Appearance", style = MaterialTheme.typography.titleMedium)
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Text("Theme", modifier = Modifier.weight(1f))
                    ThemePref.entries.forEach { t ->
                        FilterChip(selected = prefs.theme == t, onClick = { prefs.applyTheme(t) }, label = { Text(t.name) })
                        Spacer(modifier = Modifier.width(4.dp))
                    }
                }
            }
        }

        Card(border = CardDefaults.outlinedCardBorder()) {
            Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Text("Editor", style = MaterialTheme.typography.titleMedium)
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Text("Font size", modifier = Modifier.weight(1f))
                    TextButton(onClick = { prefs.applyEditorFontSize(prefs.editorFontSize - 0.5f) }) { Text("A−") }
                    Text("%.1f px".format(prefs.editorFontSize), fontFamily = FontFamily.Monospace)
                    TextButton(onClick = { prefs.applyEditorFontSize(prefs.editorFontSize + 0.5f) }) { Text("A+") }
                }
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Text("Tab width", modifier = Modifier.weight(1f))
                    listOf(2, 4, 8).forEach { n ->
                        FilterChip(selected = prefs.tabWidth == n, onClick = { prefs.applyTabWidth(n) }, label = { Text("$n") })
                        Spacer(modifier = Modifier.width(4.dp))
                    }
                }
            }
        }

        Card(border = CardDefaults.outlinedCardBorder()) {
            Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
                Text("URI history", style = MaterialTheme.typography.titleMedium)
                if (history.isEmpty()) {
                    Text("No saved URIs yet.", style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                }
                history.forEach { h ->
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Text(h.preview, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall, modifier = Modifier.weight(1f))
                        TextButton(onClick = { confirmingDelete = h }) {
                            Text("✕", color = MaterialTheme.colorScheme.error)
                        }
                    }
                }
            }
        }

        Card(border = CardDefaults.outlinedCardBorder()) {
            Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(6.dp)) {
                Text("Keyboard shortcuts", style = MaterialTheme.typography.titleMedium)
                listOf(
                    "Run query" to "⌘/Ctrl + ↵",
                    "Shell: send" to "↵",
                    "Shell: newline" to "Shift + ↵",
                    "Shell: history" to "⌘/Ctrl + ↑/↓",
                ).forEach { (k, v) ->
                    Row {
                        Text(k, modifier = Modifier.weight(1f), style = MaterialTheme.typography.bodySmall)
                        Text(v, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.labelSmall)
                    }
                }
            }
        }
    }

    confirmingDelete?.let { entry ->
        ConfirmDangerDialog(
            title = "Forget this URI?",
            text = "Remove \"${entry.preview}\" from the URI history. " +
                "Saved connections that use this URI are unaffected.",
            confirmLabel = "Forget",
            onConfirm = {
                ctx.uriHistory.delete(entry.id)
                history = ctx.uriHistory.list()
                confirmingDelete = null
            },
            onCancel = { confirmingDelete = null },
        )
    }
}
