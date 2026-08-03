package io.mex.ui.results

import androidx.compose.foundation.ScrollState
import androidx.compose.foundation.VerticalScrollbar
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.focusable
import androidx.compose.foundation.gestures.detectDragGestures
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.rememberScrollbarAdapter
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.selection.SelectionContainer
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.focus.FocusRequester
import androidx.compose.ui.focus.focusRequester
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.input.key.*
import androidx.compose.ui.input.pointer.PointerIcon
import androidx.compose.ui.input.pointer.pointerHoverIcon
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.layout.onSizeChanged
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import io.mex.mongo.FindResult
import io.mex.util.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import java.awt.Cursor

enum class ResultTab { Table, Tree, Json, Error }

@Composable
fun ResultsPane(
    result: FindResult?,
    running: Boolean,
    skip: Int,
    limit: Int,
    total: Long? = null,
    onPrev: () -> Unit,
    onNext: () -> Unit,
    onFirst: (() -> Unit)? = null,
    onLimitChange: ((Int) -> Unit)? = null,
    onEditRow: ((doc: String) -> Unit)? = null,
    onDeleteRow: ((idEjson: String) -> Unit)? = null,
) {
    var tab by remember { mutableStateOf(ResultTab.Table) }
    var selectedIndex by remember(result) { mutableStateOf(-1) }
    var splitFraction by remember { mutableStateOf(0.62f) }
    var splitContainerHeightPx by remember { mutableStateOf(0) }
    val isError = result is FindResult.Failed
    // Switch back off the Error tab when a rerun succeeds — otherwise the pane sticks on
    // "(no error)" and the user has to find the Table tab by hand.
    LaunchedEffect(isError) {
        if (isError) tab = ResultTab.Error else if (tab == ResultTab.Error) tab = ResultTab.Table
    }

    val rows = (result as? FindResult.Ok)?.rows
    val listState = rememberLazyListState()
    val focusRequester = remember { FocusRequester() }
    val scope = rememberCoroutineScope()

    // ↑/↓ step the selection through rows and keep it scrolled into view; without this
    // the only way to walk documents was clicking each row.
    fun move(delta: Int) {
        val count = rows?.size ?: 0
        if (count == 0) return
        val next = if (selectedIndex < 0) 0 else (selectedIndex + delta).coerceIn(0, count - 1)
        selectedIndex = next
        if (tab == ResultTab.Table) scope.launch { listState.animateScrollToItem(next) }
    }

    Column(modifier = Modifier.fillMaxSize()) {
        TabRow(tab = tab, hasError = isError, onSelect = { tab = it })
        // Split container — measure its height so the drag delta maps to a fraction.
        Column(
            modifier = Modifier.weight(1f).fillMaxWidth().onSizeChanged {
                splitContainerHeightPx = it.height
            },
        ) {
            // Result body
            Box(
                modifier = Modifier
                    .weight(splitFraction)
                    .fillMaxWidth()
                    .focusRequester(focusRequester)
                    .focusable()
                    .onPreviewKeyEvent { e ->
                        if (e.type != KeyEventType.KeyDown) return@onPreviewKeyEvent false
                        when (e.key) {
                            Key.DirectionDown -> { move(1); true }
                            Key.DirectionUp -> { move(-1); true }
                            Key.Home -> { move(-Int.MAX_VALUE / 2); true }
                            Key.MoveEnd -> { move(Int.MAX_VALUE / 2); true }
                            else -> false
                        }
                    },
            ) {
                val select: (Int) -> Unit = { i ->
                    selectedIndex = i
                    focusRequester.requestFocus()
                }
                when {
                    running && result == null -> CenterText("Running…")
                    result == null -> CenterText("Run a query to see results.")
                    result is FindResult.Failed && tab == ResultTab.Error -> ErrorBody(result.error)
                    result is FindResult.Ok -> when (tab) {
                        ResultTab.Table -> TableBody(result.rows, selectedIndex, listState, select)
                        ResultTab.Tree -> TreeBody(result.rows, selectedIndex, select)
                        ResultTab.Json -> JsonBody(result.rows)
                        ResultTab.Error -> ErrorBody("(no error)")
                    }
                    else -> {}
                }
            }
            // Draggable divider
            SplitDivider(
                onDragPx = { deltaY ->
                    if (splitContainerHeightPx > 0) {
                        splitFraction = (splitFraction + deltaY / splitContainerHeightPx)
                            .coerceIn(0.15f, 0.85f)
                    }
                },
            )
            // Preview / edit panel
            PreviewPanel(
                modifier = Modifier.weight(1f - splitFraction).fillMaxWidth(),
                rows = rows,
                selectedIndex = selectedIndex,
                onEditRow = onEditRow,
                onDeleteRow = onDeleteRow,
            )
        }
        Pager(
            result = result,
            running = running,
            skip = skip,
            limit = limit,
            total = total,
            onPrev = onPrev,
            onNext = onNext,
            onFirst = onFirst,
            onLimitChange = onLimitChange,
        )
    }
}

@Composable
private fun SplitDivider(onDragPx: (Float) -> Unit) {
    val resizeCursor = remember { PointerIcon(Cursor.getPredefinedCursor(Cursor.N_RESIZE_CURSOR)) }
    Box(
        modifier = Modifier
            .fillMaxWidth()
            .height(6.dp)
            .background(MaterialTheme.colorScheme.surfaceVariant)
            .pointerHoverIcon(resizeCursor)
            .pointerInput(Unit) {
                detectDragGestures { change, drag ->
                    change.consume()
                    onDragPx(drag.y)
                }
            },
    ) {
        // Subtle grip in the middle of the divider.
        Box(
            modifier = Modifier
                .align(Alignment.Center)
                .width(36.dp)
                .height(2.dp)
                .background(MaterialTheme.colorScheme.outline),
        )
    }
}

@Composable
private fun PreviewPanel(
    modifier: Modifier,
    rows: List<String>?,
    selectedIndex: Int,
    onEditRow: ((String) -> Unit)?,
    onDeleteRow: ((String) -> Unit)?,
) {
    val raw = rows?.getOrNull(selectedIndex)
    val parsed = remember(raw) { raw?.let { parseRow(it) } }
    val idEjson = remember(parsed) { parsed?.get("_id")?.let { idElementToEjson(it) } }
    val canMutate = onEditRow != null && onDeleteRow != null && idEjson != null

    Column(modifier = modifier.background(MaterialTheme.colorScheme.surface)) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .background(MaterialTheme.colorScheme.surfaceVariant)
                .padding(horizontal = 12.dp, vertical = 6.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Text(
                if (selectedIndex < 0) "Preview" else "Preview · row ${selectedIndex + 1}",
                style = MaterialTheme.typography.labelMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.weight(1f),
            )
            if (raw != null) {
                CopyButton({ prettyPrint(raw) })
            }
            if (onEditRow != null) {
                TextButton(onClick = { raw?.let(onEditRow) }, enabled = canMutate) {
                    Text("Edit", style = MaterialTheme.typography.labelMedium)
                }
            }
            if (onDeleteRow != null) {
                TextButton(onClick = { idEjson?.let(onDeleteRow) }, enabled = canMutate) {
                    Text("Delete", style = MaterialTheme.typography.labelMedium, color = MaterialTheme.colorScheme.error)
                }
            }
        }
        Box(modifier = Modifier.fillMaxSize()) {
            when {
                raw == null -> CenterText(
                    if (rows.isNullOrEmpty()) "Run a query to see rows."
                    else "Click a row to preview.",
                )
                else -> {
                    val scroll = rememberScrollState()
                    // SelectionContainer so any fragment of the document — a value, an
                    // _id, one nested field — can be swept and copied, not just the whole
                    // thing via the Copy button.
                    SelectionContainer {
                        Column(
                            modifier = Modifier
                                .fillMaxSize()
                                .verticalScroll(scroll)
                                .padding(12.dp),
                        ) {
                            HighlightedJson(raw)
                        }
                    }
                    VerticalScrollbar(
                        adapter = rememberScrollbarAdapter(scroll),
                        modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
                    )
                }
            }
        }
    }
}

private fun idElementToEjson(id: JsonElement): String {
    // Build the EJSON snippet that goes inside a filter, e.g. { _id: <here> }
    return when (id) {
        is JsonObject -> id.toString()
        is JsonPrimitive -> id.toString()
        else -> id.toString()
    }
}

@Composable
private fun TabRow(tab: ResultTab, hasError: Boolean, onSelect: (ResultTab) -> Unit) {
    Row(
        modifier = Modifier.fillMaxWidth().padding(horizontal = 12.dp).padding(top = 4.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        TabBtn("Table", tab == ResultTab.Table) { onSelect(ResultTab.Table) }
        TabBtn("Tree", tab == ResultTab.Tree) { onSelect(ResultTab.Tree) }
        TabBtn("JSON", tab == ResultTab.Json) { onSelect(ResultTab.Json) }
        if (hasError) TabBtn("Error", tab == ResultTab.Error, error = true) { onSelect(ResultTab.Error) }
    }
    HorizontalDivider()
}

@Composable
private fun TabBtn(label: String, active: Boolean, error: Boolean = false, onClick: () -> Unit) {
    val color = when {
        error -> MaterialTheme.colorScheme.error
        active -> MaterialTheme.colorScheme.primary
        else -> MaterialTheme.colorScheme.onSurfaceVariant
    }
    TextButton(onClick = onClick) {
        Text(label, color = color, style = MaterialTheme.typography.labelMedium)
    }
}

@Composable
private fun CenterText(text: String) {
    Box(modifier = Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
        Text(text, color = MaterialTheme.colorScheme.onSurfaceVariant)
    }
}

@Composable
private fun TableBody(
    rows: List<String>,
    selectedIndex: Int,
    listState: androidx.compose.foundation.lazy.LazyListState,
    onSelect: (Int) -> Unit,
) {
    val parsed = remember(rows) { rows.map { parseRow(it) } }
    val columns = remember(rows) { collectColumns(rows) }
    if (columns.isEmpty()) {
        CenterText("No documents to render.")
        return
    }
    Column(modifier = Modifier.fillMaxSize()) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .background(MaterialTheme.colorScheme.surfaceVariant)
                .padding(vertical = 4.dp),
        ) {
            IndexCell("#", header = true)
            columns.forEach { c -> HeaderCell(c) }
        }
        HorizontalDivider()
        Box(modifier = Modifier.weight(1f).fillMaxWidth()) {
            LazyColumn(state = listState, modifier = Modifier.fillMaxSize()) {
                items(parsed.size) { i ->
                    val doc = parsed[i]
                    val isSelected = i == selectedIndex
                    Row(
                        modifier = Modifier
                            .fillMaxWidth()
                            .background(
                                if (isSelected)
                                    MaterialTheme.colorScheme.primary.copy(alpha = 0.16f)
                                else
                                    Color.Transparent,
                            )
                            .clickable { onSelect(i) }
                            .padding(vertical = 2.dp),
                    ) {
                        IndexCell((i + 1).toString(), selected = isSelected)
                        columns.forEach { col ->
                            val v = doc?.get(col)
                            CellView(v)
                        }
                    }
                    HorizontalDivider(thickness = 0.5.dp)
                }
            }
            VerticalScrollbar(
                adapter = rememberScrollbarAdapter(listState),
                modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
            )
        }
    }
}

@Composable
private fun RowScope.IndexCell(text: String, header: Boolean = false, selected: Boolean = false) {
    Box(modifier = Modifier.width(60.dp).padding(horizontal = 8.dp)) {
        Text(
            text,
            style = MaterialTheme.typography.bodySmall,
            color = when {
                header -> MaterialTheme.colorScheme.onSurfaceVariant
                selected -> MaterialTheme.colorScheme.primary
                else -> Color.Gray
            },
            fontFamily = FontFamily.Monospace,
        )
    }
}

@Composable
private fun RowScope.HeaderCell(text: String) {
    Box(modifier = Modifier.weight(1f).widthIn(min = 120.dp).padding(horizontal = 8.dp)) {
        Text(
            text,
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            fontFamily = FontFamily.Monospace,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis,
        )
    }
}

@Composable
private fun RowScope.CellView(v: JsonElement?) {
    Box(modifier = Modifier.weight(1f).widthIn(min = 120.dp).padding(horizontal = 8.dp)) {
        if (v == null) {
            Text("—", color = Color.DarkGray, style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace)
        } else {
            val f = formatValue(v)
            Text(
                f.label,
                style = MaterialTheme.typography.bodySmall,
                fontFamily = FontFamily.Monospace,
                color = colorFor(f.type),
                maxLines = 1,
                overflow = TextOverflow.Ellipsis,
            )
        }
    }
}

/**
 * Virtualized: a plain Column composed all documents at once — a 500-row page of multi-KB
 * documents froze for seconds on tab switch and re-paid the cost on every new result.
 */
@Composable
private fun TreeBody(
    rows: List<String>,
    selectedIndex: Int,
    onSelect: (Int) -> Unit,
) {
    val listState = rememberLazyListState()
    Box(modifier = Modifier.fillMaxSize()) {
        LazyColumn(
            state = listState,
            modifier = Modifier.fillMaxSize().padding(12.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            items(rows.size, key = { it }) { i ->
                val row = rows[i]
                val parsed = remember(row) { parseRow(row) }
                val isSelected = i == selectedIndex
                Card(
                    border = CardDefaults.outlinedCardBorder(),
                    colors = CardDefaults.cardColors(
                        containerColor = if (isSelected)
                            MaterialTheme.colorScheme.primary.copy(alpha = 0.10f)
                        else
                            MaterialTheme.colorScheme.surface,
                    ),
                    modifier = Modifier.fillMaxWidth().clickable { onSelect(i) },
                ) {
                    Column(modifier = Modifier.padding(10.dp)) {
                        Text(
                            "Document ${i + 1}",
                            style = MaterialTheme.typography.labelSmall,
                            color = if (isSelected) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                        Spacer(modifier = Modifier.height(4.dp))
                        SelectionContainer {
                            Column {
                                if (parsed != null) {
                                    NodeView("", parsed, 0, root = true)
                                } else {
                                    Text(row, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall)
                                }
                            }
                        }
                    }
                }
            }
        }
        VerticalScrollbar(
            adapter = rememberScrollbarAdapter(listState),
            modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
        )
    }
}

@Composable
private fun NodeView(label: String, value: JsonElement, depth: Int, root: Boolean = false) {
    val indent = (depth * 16).dp
    val isContainer = value is JsonObject || value is JsonArray
    val isWrapped = value is JsonObject && isWrapper(value)

    var open by remember { mutableStateOf(root || depth < 1) }

    if (!isContainer || isWrapped) {
        val f = formatValue(value)
        Row(modifier = Modifier.padding(start = indent, top = 1.dp, bottom = 1.dp)) {
            if (label.isNotEmpty()) {
                Text(
                    "$label: ",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    fontFamily = FontFamily.Monospace,
                )
            }
            Text(
                f.label,
                style = MaterialTheme.typography.bodySmall,
                color = colorFor(f.type),
                fontFamily = FontFamily.Monospace,
            )
        }
        return
    }

    val (entries, prefix) = when (value) {
        is JsonObject -> value.entries.map { it.key to it.value } to "{"
        is JsonArray -> value.mapIndexed { i, v -> i.toString() to v } to "["
        else -> emptyList<Pair<String, JsonElement>>() to ""
    }

    Row(modifier = Modifier.padding(start = indent, top = 1.dp, bottom = 1.dp)) {
        TextButton(
            onClick = { open = !open },
            contentPadding = PaddingValues(0.dp),
            modifier = Modifier.size(20.dp),
        ) { Text(if (open) "▾" else "▸", style = MaterialTheme.typography.labelSmall) }
        if (label.isNotEmpty()) {
            Text(
                "$label: ",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                fontFamily = FontFamily.Monospace,
            )
        }
        Text(
            "$prefix${entries.size}",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            fontFamily = FontFamily.Monospace,
        )
    }
    if (open) {
        entries.forEach { (k, v) -> NodeView(k, v, depth + 1) }
    }
}

/**
 * Virtualized, and the copy-all string is built only when the button is clicked — the
 * eager pretty-print + join of a whole page was a multi-second freeze on large results.
 */
@Composable
private fun JsonBody(rows: List<String>) {
    Surface(color = MaterialTheme.colorScheme.surface) {
        Column(modifier = Modifier.fillMaxSize()) {
            Row(
                modifier = Modifier.fillMaxWidth().padding(horizontal = 12.dp),
                verticalAlignment = Alignment.CenterVertically,
            ) {
                Text(
                    "${rows.size} document${if (rows.size == 1) "" else "s"}",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.weight(1f),
                )
                CopyButton({ rows.joinToString("\n\n") { prettyPrint(it) } }, label = "Copy all")
            }
            val listState = rememberLazyListState()
            Box(modifier = Modifier.weight(1f).fillMaxWidth()) {
                SelectionContainer {
                    LazyColumn(
                        state = listState,
                        modifier = Modifier
                            .fillMaxSize()
                            .padding(horizontal = 12.dp),
                        verticalArrangement = Arrangement.spacedBy(12.dp),
                    ) {
                        items(rows.size, key = { it }) { i -> HighlightedJson(rows[i]) }
                    }
                }
                VerticalScrollbar(
                    adapter = rememberScrollbarAdapter(listState),
                    modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
                )
            }
        }
    }
}

/** Copies the provided text and flashes "Copied ✓" for a moment. */
@Composable
private fun CopyButton(textProvider: () -> String?, label: String = "Copy") {
    val clipboard = LocalClipboardManager.current
    var copied by remember { mutableStateOf(false) }
    LaunchedEffect(copied) {
        if (copied) {
            delay(1500)
            copied = false
        }
    }
    TextButton(
        onClick = {
            textProvider()?.let {
                clipboard.setText(AnnotatedString(it))
                copied = true
            }
        },
    ) {
        Text(
            if (copied) "Copied ✓" else label,
            style = MaterialTheme.typography.labelMedium,
            color = if (copied) MaterialTheme.colorScheme.primary else Color.Unspecified,
        )
    }
}

@Composable
private fun ErrorBody(message: String) {
    Column(modifier = Modifier.fillMaxSize().padding(12.dp)) {
        Text(
            "Query failed",
            color = MaterialTheme.colorScheme.error,
            style = MaterialTheme.typography.titleSmall,
        )
        Spacer(modifier = Modifier.height(6.dp))
        Surface(
            color = MaterialTheme.colorScheme.errorContainer,
            shape = RoundedCornerShape(4.dp),
        ) {
            Text(
                message,
                modifier = Modifier.padding(10.dp),
                fontFamily = FontFamily.Monospace,
                style = MaterialTheme.typography.bodySmall,
            )
        }
    }
}

private val PAGE_SIZES = listOf(25, 50, 100, 250, 500)

@Composable
private fun Pager(
    result: FindResult?,
    running: Boolean,
    skip: Int,
    limit: Int,
    total: Long?,
    onPrev: () -> Unit,
    onNext: () -> Unit,
    onFirst: (() -> Unit)?,
    onLimitChange: ((Int) -> Unit)?,
) {
    val rowsCount = (result as? FindResult.Ok)?.rows?.size ?: 0
    val page = if (limit > 0) skip / limit + 1 else 1
    val pageCount = if (total != null && limit > 0) ((total + limit - 1) / limit).coerceAtLeast(1) else null
    var sizeMenu by remember { mutableStateOf(false) }

    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surfaceVariant)
            .padding(horizontal = 12.dp, vertical = 6.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        if (onFirst != null) {
            TextButton(onClick = onFirst, enabled = skip > 0 && !running) { Text("« First") }
        }
        TextButton(onClick = onPrev, enabled = skip > 0 && !running) { Text("← Prev") }
        Text(
            buildString {
                append("page $page")
                pageCount?.let { append(" of $it") }
            },
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurface,
        )
        TextButton(
            onClick = onNext,
            enabled = result is FindResult.Ok && result.hasMore && !running,
        ) { Text("Next →") }

        Spacer(modifier = Modifier.width(12.dp))
        Text(
            "rows ${if (rowsCount == 0) 0 else skip + 1}–${skip + rowsCount}" +
                (total?.let { " of ${formatCount(it)}" } ?: ""),
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )

        Spacer(modifier = Modifier.weight(1f))
        if (result is FindResult.Ok) {
            Text(
                "${result.durationMs} ms",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            Spacer(modifier = Modifier.width(8.dp))
        }
        // Page size belongs next to the paging controls, not only in the query form.
        if (onLimitChange != null) {
            Box {
                TextButton(onClick = { sizeMenu = true }) {
                    Text("$limit / page ⌄", style = MaterialTheme.typography.labelSmall)
                }
                DropdownMenu(expanded = sizeMenu, onDismissRequest = { sizeMenu = false }) {
                    PAGE_SIZES.forEach { n ->
                        DropdownMenuItem(
                            text = { Text("$n per page") },
                            onClick = { sizeMenu = false; onLimitChange(n) },
                        )
                    }
                }
            }
        } else {
            Text(
                "limit $limit",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
    }
}
