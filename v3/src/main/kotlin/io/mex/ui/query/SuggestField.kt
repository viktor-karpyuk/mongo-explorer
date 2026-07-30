package io.mex.ui.query

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.focus.onFocusChanged
import androidx.compose.ui.input.key.*
import androidx.compose.ui.layout.onSizeChanged
import androidx.compose.ui.text.TextRange
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.input.TextFieldValue
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.IntOffset
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Popup
import io.mex.mongo.SchemaField

/**
 * A query input that completes field paths from the collection's sampled schema.
 *
 * The popup is deliberately non-focusable so the text field keeps the caret and every
 * key (↑/↓/Tab/Enter/Esc) is handled here — a DropdownMenu would steal focus and break
 * typing mid-completion.
 */
@Composable
fun SuggestingQueryField(
    label: String,
    value: String,
    hint: String,
    fields: List<SchemaField>,
    context: FieldContext,
    onChange: (String) -> Unit,
    onSubmit: (() -> Unit)? = null,
) {
    var tfv by remember { mutableStateOf(TextFieldValue(value)) }
    // Keep in step when the value is replaced from outside (history pick, reset).
    LaunchedEffect(value) {
        if (value != tfv.text) tfv = TextFieldValue(value, TextRange(value.length))
    }

    var focused by remember { mutableStateOf(false) }
    var dismissed by remember { mutableStateOf(false) }
    var highlighted by remember { mutableStateOf(0) }
    var fieldWidth by remember { mutableStateOf(0) }
    var fieldHeight by remember { mutableStateOf(0) }

    val suggestions = remember(tfv.text, tfv.selection.start, fields, context) {
        if (fields.isEmpty()) emptyList()
        else suggestionsFor(tfv.text, tfv.selection.start, fields, context)
    }
    val open = focused && !dismissed && suggestions.isNotEmpty()
    LaunchedEffect(suggestions) { highlighted = 0 }

    fun accept(index: Int) {
        val s = suggestions.getOrNull(index) ?: return
        val (text, caret) = applySuggestion(tfv.text, tfv.selection.start, s)
        tfv = TextFieldValue(text, TextRange(caret))
        onChange(text)
        dismissed = true
    }

    Box {
        OutlinedTextField(
            value = tfv,
            onValueChange = {
                val textChanged = it.text != tfv.text
                tfv = it
                if (textChanged) {
                    dismissed = false
                    onChange(it.text)
                }
            },
            label = { Text(label, style = MaterialTheme.typography.labelSmall) },
            placeholder = { Text(hint, style = MaterialTheme.typography.bodySmall) },
            singleLine = true,
            modifier = Modifier
                .fillMaxWidth()
                .onSizeChanged { fieldWidth = it.width; fieldHeight = it.height }
                .onFocusChanged { focused = it.isFocused; if (!it.isFocused) dismissed = false }
                .onPreviewKeyEvent { e ->
                    if (e.type != KeyEventType.KeyDown) return@onPreviewKeyEvent false
                    when {
                        !open && e.key == Key.Enter && onSubmit != null -> { onSubmit(); true }
                        !open -> false
                        e.key == Key.DirectionDown -> {
                            highlighted = (highlighted + 1) % suggestions.size; true
                        }
                        e.key == Key.DirectionUp -> {
                            highlighted = (highlighted - 1 + suggestions.size) % suggestions.size; true
                        }
                        e.key == Key.Enter || e.key == Key.Tab -> { accept(highlighted); true }
                        e.key == Key.Escape -> { dismissed = true; true }
                        else -> false
                    }
                },
            textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
        )

        if (open) {
            Popup(
                alignment = Alignment.TopStart,
                offset = IntOffset(0, fieldHeight),
                onDismissRequest = { dismissed = true },
            ) {
                Surface(
                    shape = RoundedCornerShape(6.dp),
                    tonalElevation = 8.dp,
                    shadowElevation = 8.dp,
                    modifier = Modifier
                        .width(with(androidx.compose.ui.platform.LocalDensity.current) { fieldWidth.toDp() })
                        .heightIn(max = 240.dp),
                ) {
                    Column(modifier = Modifier.verticalScroll(rememberScrollState())) {
                        suggestions.forEachIndexed { i, s ->
                            SuggestionRow(s, i == highlighted) { accept(i) }
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun SuggestionRow(s: Suggestion, highlighted: Boolean, onClick: () -> Unit) {
    val bg =
        if (highlighted) MaterialTheme.colorScheme.primary.copy(alpha = 0.14f)
        else MaterialTheme.colorScheme.surface
    val accent = when (s.kind) {
        SuggestKind.field -> MaterialTheme.colorScheme.primary
        SuggestKind.operator -> MaterialTheme.colorScheme.tertiary
        SuggestKind.value -> MaterialTheme.colorScheme.secondary
    }
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(bg)
            .clickable(onClick = onClick)
            .padding(horizontal = 10.dp, vertical = 5.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Text(
            s.label,
            style = MaterialTheme.typography.bodySmall,
            fontFamily = FontFamily.Monospace,
            color = accent,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis,
            modifier = Modifier.weight(1f),
        )
        Spacer(modifier = Modifier.width(8.dp))
        Text(
            s.detail,
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            maxLines = 1,
        )
    }
}
