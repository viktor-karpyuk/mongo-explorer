package io.mex.ui.shell

import androidx.compose.foundation.VerticalScrollbar
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.rememberScrollbarAdapter
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.selection.SelectionContainer
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.input.key.*
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.shell.MongoShellSession
import io.mex.shell.ShellEvent

@Composable
fun ShellPanel(ctx: AppContext, connectionId: String, readOnly: Boolean = false) {
    var session by remember(connectionId) { mutableStateOf<MongoShellSession?>(null) }
    var output by remember(connectionId) { mutableStateOf("") }
    var input by remember(connectionId) { mutableStateOf("") }
    val history = remember(connectionId) { mutableStateListOf<String>() }
    var historyCursor by remember(connectionId) { mutableStateOf(-1) }
    val scrollState = rememberScrollState()
    val clipboard = LocalClipboardManager.current

    DisposableEffect(connectionId) { onDispose { session?.close() } }

    LaunchedEffect(session) {
        val s = session ?: return@LaunchedEffect
        s.events.collect { ev ->
            when (ev) {
                is ShellEvent.Output -> output += ev.text
                is ShellEvent.Exit -> {
                    output += "\n[mongosh exited with code ${ev.code}]\n"
                    session = null
                }
            }
        }
    }
    LaunchedEffect(output) { scrollState.scrollTo(scrollState.maxValue) }

    Column(modifier = Modifier.fillMaxSize().padding(16.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Text("Shell", style = MaterialTheme.typography.titleMedium, modifier = Modifier.weight(1f))
            if (output.isNotEmpty()) {
                TextButton(onClick = { clipboard.setText(AnnotatedString(output)) }) { Text("Copy output") }
                TextButton(onClick = { output = "" }) { Text("Clear") }
            }
            if (session == null) {
                Button(onClick = {
                    val record = ctx.connections.get(connectionId) ?: return@Button
                    output = ""
                    val s = MongoShellSession(record.uri)
                    if (s.start()) session = s
                }) { Text("Start mongosh") }
            } else {
                OutlinedButton(onClick = { session?.close(); session = null }) { Text("Stop session") }
            }
        }
        Spacer(modifier = Modifier.height(6.dp))
        if (readOnly) {
            // mongosh runs with whatever privileges the URI grants — the app cannot restrict
            // it, so the honest thing is a visible warning rather than a false sense of safety.
            Text(
                "⚠ This connection is marked read-only, but mongosh is a raw shell — commands you type " +
                    "here are NOT restricted by the app.",
                style = MaterialTheme.typography.labelSmall,
                color = Color(0xFFFBBF24),
            )
            Spacer(modifier = Modifier.height(4.dp))
        }
        Text(
            "Press Enter to send · Shift+Enter for newline · ↑/↓ for history",
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Spacer(modifier = Modifier.height(8.dp))
        Surface(
            color = Color(0xFF0A0C10),
            shape = RoundedCornerShape(4.dp),
            modifier = Modifier.fillMaxWidth().weight(1f),
        ) {
            Box {
                // Selectable so mongosh output can actually be copied out of the pane.
                SelectionContainer {
                    Column(modifier = Modifier.fillMaxSize().padding(10.dp).verticalScroll(scrollState)) {
                        Text(
                            output.ifEmpty { "(no output yet — start a session)" },
                            color = Color(0xFFD4D7DF),
                            fontFamily = FontFamily.Monospace,
                            style = MaterialTheme.typography.bodySmall,
                        )
                    }
                }
                VerticalScrollbar(
                    adapter = rememberScrollbarAdapter(scrollState),
                    modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
                )
            }
        }
        Spacer(modifier = Modifier.height(8.dp))
        OutlinedTextField(
            value = input,
            onValueChange = { input = it },
            placeholder = { Text("db.users.find({}).limit(5)", fontFamily = FontFamily.Monospace) },
            modifier = Modifier.fillMaxWidth().onPreviewKeyEvent { e ->
                if (e.type != KeyEventType.KeyDown) return@onPreviewKeyEvent false
                when {
                    e.key == Key.Enter && !e.isShiftPressed -> {
                        if (input.isNotBlank() && session != null) {
                            output += "> $input\n"
                            history.add(0, input); if (history.size > 100) history.removeAt(history.size - 1)
                            historyCursor = -1
                            session!!.send(input)
                            input = ""
                        }
                        true
                    }
                    // Plain ↑/↓ is what a terminal user reaches for; the modifier variant
                    // stays available for anyone already used to it.
                    e.key == Key.DirectionUp -> {
                        if (history.isNotEmpty()) {
                            historyCursor = minOf(historyCursor + 1, history.size - 1)
                            input = history.getOrNull(historyCursor) ?: ""
                        }
                        true
                    }
                    e.key == Key.DirectionDown -> {
                        if (historyCursor > 0) {
                            historyCursor--
                            input = history.getOrNull(historyCursor) ?: ""
                        } else { historyCursor = -1; input = "" }
                        true
                    }
                    else -> false
                }
            },
            textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
            enabled = session != null,
            minLines = 2,
            maxLines = 6,
        )
    }
}
