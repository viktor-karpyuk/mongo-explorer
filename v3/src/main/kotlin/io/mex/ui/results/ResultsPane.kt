package io.mex.ui.results

import androidx.compose.foundation.background
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import io.mex.mongo.FindResult
import io.mex.util.*
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonElement

enum class ResultTab { Table, Tree, Json, Error }

@Composable
fun ResultsPane(
    result: FindResult?,
    running: Boolean,
    skip: Int,
    limit: Int,
    onPrev: () -> Unit,
    onNext: () -> Unit,
) {
    var tab by remember { mutableStateOf(ResultTab.Table) }
    val isError = result is FindResult.Failed
    LaunchedEffect(isError) { if (isError) tab = ResultTab.Error }

    Column(modifier = Modifier.fillMaxSize()) {
        TabRow(tab = tab, hasError = isError, onSelect = { tab = it })
        Box(modifier = Modifier.weight(1f).fillMaxWidth()) {
            when {
                running && result == null -> CenterText("Running…")
                result == null -> CenterText("Run a query to see results.")
                result is FindResult.Failed && tab == ResultTab.Error -> ErrorBody(result.error)
                result is FindResult.Ok -> when (tab) {
                    ResultTab.Table -> TableBody(result.rows)
                    ResultTab.Tree -> TreeBody(result.rows)
                    ResultTab.Json -> JsonBody(result.rows)
                    ResultTab.Error -> ErrorBody("(no error)")
                }
                else -> {}
            }
        }
        Pager(
            result = result,
            running = running,
            skip = skip,
            limit = limit,
            onPrev = onPrev,
            onNext = onNext,
        )
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
private fun TableBody(rows: List<String>) {
    val parsed = remember(rows) { rows.map { parseRow(it) } }
    val columns = remember(rows) { collectColumns(rows) }
    if (columns.isEmpty()) {
        CenterText("No documents to render.")
        return
    }
    val hscroll = rememberScrollState()
    Column(modifier = Modifier.fillMaxSize().horizontalScroll(hscroll)) {
        Row(
            modifier = Modifier
                .background(MaterialTheme.colorScheme.surfaceVariant)
                .padding(vertical = 4.dp),
        ) {
            IndexCell("#", header = true)
            columns.forEach { c -> HeaderCell(c) }
        }
        HorizontalDivider()
        LazyColumn(modifier = Modifier.weight(1f)) {
            items(parsed.size) { i ->
                val doc = parsed[i]
                Row(modifier = Modifier.fillMaxWidth().padding(vertical = 2.dp)) {
                    IndexCell((i + 1).toString())
                    columns.forEach { col ->
                        val v = doc?.get(col)
                        CellView(v)
                    }
                }
                HorizontalDivider(thickness = 0.5.dp)
            }
        }
    }
}

@Composable
private fun IndexCell(text: String, header: Boolean = false) {
    Box(modifier = Modifier.width(60.dp).padding(horizontal = 8.dp)) {
        Text(
            text,
            style = MaterialTheme.typography.bodySmall,
            color = if (header) MaterialTheme.colorScheme.onSurfaceVariant else Color.Gray,
            fontFamily = FontFamily.Monospace,
        )
    }
}

@Composable
private fun HeaderCell(text: String) {
    Box(modifier = Modifier.width(180.dp).padding(horizontal = 8.dp)) {
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
private fun CellView(v: JsonElement?) {
    Box(modifier = Modifier.width(180.dp).padding(horizontal = 8.dp)) {
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

private fun colorFor(t: EjsonType): Color = when (t) {
    EjsonType.Number, EjsonType.Long, EjsonType.Decimal -> Color(0xFF60A5FA)
    EjsonType.Boolean -> Color(0xFFFBBF24)
    EjsonType.Null -> Color.Gray
    EjsonType.ObjectId -> Color(0xFF34D399)
    EjsonType.Date -> Color(0xFFA78BFA)
    EjsonType.Binary, EjsonType.Regex -> Color(0xFFF472B6)
    EjsonType.MinKey, EjsonType.MaxKey -> Color.DarkGray
    EjsonType.ArrayT, EjsonType.ObjectT -> Color.Gray
    else -> Color.Unspecified
}

@Composable
private fun TreeBody(rows: List<String>) {
    Column(
        modifier = Modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(12.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        rows.forEachIndexed { i, row ->
            val parsed = remember(row) { parseRow(row) }
            Card(border = CardDefaults.outlinedCardBorder()) {
                Column(modifier = Modifier.padding(10.dp)) {
                    Text(
                        "Document ${i + 1}",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                    Spacer(modifier = Modifier.height(4.dp))
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

    Row(
        modifier = Modifier
            .background(if (root) Color.Unspecified else Color.Transparent)
            .padding(start = indent, top = 1.dp, bottom = 1.dp),
    ) {
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

@Composable
private fun JsonBody(rows: List<String>) {
    val text = remember(rows) { rows.joinToString("\n\n") { prettyPrint(it) } }
    Surface(color = MaterialTheme.colorScheme.surface) {
        Column(modifier = Modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(12.dp)) {
            Text(
                text,
                fontFamily = FontFamily.Monospace,
                style = MaterialTheme.typography.bodySmall,
            )
        }
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

@Composable
private fun Pager(
    result: FindResult?,
    running: Boolean,
    skip: Int,
    limit: Int,
    onPrev: () -> Unit,
    onNext: () -> Unit,
) {
    val rowsCount = (result as? FindResult.Ok)?.rows?.size ?: 0
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surfaceVariant)
            .padding(horizontal = 12.dp, vertical = 6.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        TextButton(onClick = onPrev, enabled = skip > 0 && !running) { Text("← Prev") }
        Spacer(modifier = Modifier.width(8.dp))
        Text(
            "rows ${if (rowsCount == 0) 0 else skip + 1}–${skip + rowsCount}",
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Spacer(modifier = Modifier.weight(1f))
        if (result is FindResult.Ok) {
            Text(
                "${result.rows.size} rows · ${result.durationMs} ms${if (result.hasMore) " · more available" else ""}",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            Spacer(modifier = Modifier.width(8.dp))
        }
        TextButton(
            onClick = onNext,
            enabled = result is FindResult.Ok && result.hasMore && !running,
        ) { Text("Next →") }
        Spacer(modifier = Modifier.width(8.dp))
        Text(
            "page $limit",
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }
}
