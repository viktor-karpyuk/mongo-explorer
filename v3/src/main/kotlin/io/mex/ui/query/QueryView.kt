package io.mex.ui.query

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.key.*
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.data.QueryHistoryRow
import io.mex.data.QueryKind
import io.mex.util.formatAgo
import io.mex.mongo.MongoRegistry
import io.mex.ui.io.ExportDialog
import io.mex.ui.io.ImportDialog
import io.mex.ui.mutate.DeleteDialog
import io.mex.ui.mutate.InsertDialog
import io.mex.ui.mutate.ReplaceDialog
import io.mex.ui.mutate.RowEditDialog
import io.mex.ui.mutate.UpdateDialog
import io.mex.ui.results.ResultsPane
import kotlinx.coroutines.launch

@Composable
fun QueryView(
    ctx: AppContext,
    registry: MongoRegistry,
    connectionId: String,
    db: String,
    collection: String,
    store: QueryStore,
) {
    val key = nsKey(connectionId, db, collection)
    val draft = store.draft(key)
    val result = store.result(key)
    val running = store.running(key)
    val scope = rememberCoroutineScope()

    fun run() = scope.launch {
        store.run(key, connectionId, db, collection)
        store.refreshCount(key, connectionId, db, collection)
    }
    fun next() = scope.launch { store.nextPage(key, connectionId, db, collection) }
    fun prev() = scope.launch { store.prevPage(key, connectionId, db, collection) }
    fun first() = scope.launch { store.firstPage(key, connectionId, db, collection) }
    fun pageSize(n: Int) = scope.launch { store.setPageSize(key, n, connectionId, db, collection) }

    // Sampled once per namespace; drives field-path autocomplete in the query inputs.
    LaunchedEffect(key) { store.loadSchema(key, connectionId, db, collection) }
    val schemaFields = store.schema(key)

    // DBA-RO-1 — a read-only connection exposes no mutating affordances at all.
    val readOnly = remember(connectionId) { ctx.connections.isReadOnly(connectionId) }

    var dialog by remember(connectionId, db, collection) { mutableStateOf<String?>(null) }
    // Holds the pre-filled state when Edit/Delete is invoked from a result row.
    var rowEdit by remember(connectionId, db, collection) { mutableStateOf<Pair<String, String>?>(null) }
    var rowDeleteFilter by remember(connectionId, db, collection) { mutableStateOf<String?>(null) }
    val onDone = { run(); Unit }

    Column(
        modifier = Modifier
            .fillMaxSize()
            .onPreviewKeyEvent { e ->
                if (e.type == KeyEventType.KeyDown && e.key == Key.Enter && (e.isMetaPressed || e.isCtrlPressed)) {
                    run(); true
                } else false
            },
    ) {
        // Editor area
        Column(
            modifier = Modifier.fillMaxWidth().padding(12.dp),
            verticalArrangement = Arrangement.spacedBy(6.dp),
        ) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(
                    "$db.$collection",
                    style = MaterialTheme.typography.titleMedium,
                )
                if (readOnly) {
                    Spacer(modifier = Modifier.width(8.dp))
                    io.mex.ui.components.ReadOnlyBadge()
                }
                Spacer(modifier = Modifier.weight(1f))
                if (!readOnly) {
                    TextButton(onClick = { dialog = "insert" }) { Text("Insert") }
                    TextButton(onClick = { dialog = "update" }) { Text("Update") }
                    TextButton(onClick = { dialog = "replace" }) { Text("Replace") }
                    TextButton(onClick = { dialog = "delete" }) { Text("Delete") }
                }
                TextButton(onClick = { dialog = "export" }) { Text("Export") }
                if (!readOnly) {
                    TextButton(onClick = { dialog = "import" }) { Text("Import") }
                }
                HistoryButton(ctx, connectionId, db, collection) { body ->
                    draft.filter = body.filter
                    draft.projection = body.projection
                    draft.sort = body.sort
                    body.skip?.let { draft.skip = it }
                    body.limit?.let { draft.limit = it.coerceAtLeast(1) }
                    run()
                }
                Button(onClick = { run() }, enabled = !running) {
                    Text(if (running) "Running…" else "Run ⌘↵")
                }
            }
            SuggestingQueryField(
                label = "Filter",
                value = draft.filter,
                hint = "{ status: \"active\" }",
                fields = schemaFields,
                context = FieldContext.filter,
                onChange = { draft.filter = it },
                onSubmit = { run() },
            )
            SuggestingQueryField(
                label = "Projection",
                value = draft.projection,
                hint = "{ name: 1, _id: 0 }",
                fields = schemaFields,
                context = FieldContext.projection,
                onChange = { draft.projection = it },
                onSubmit = { run() },
            )
            SuggestingQueryField(
                label = "Sort",
                value = draft.sort,
                hint = "{ createdAt: -1 }",
                fields = schemaFields,
                context = FieldContext.sort,
                onChange = { draft.sort = it },
                onSubmit = { run() },
            )
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                NumberField("Skip", draft.skip) { draft.skip = it }
                NumberField("Limit", draft.limit) { draft.limit = it.coerceAtLeast(1) }
                NumberField("maxTimeMs", draft.maxTimeMs.toInt()) { draft.maxTimeMs = it.toLong() }
            }
        }
        HorizontalDivider()
        ResultsPane(
            result = result,
            running = running,
            skip = draft.skip,
            limit = draft.limit,
            total = store.total(key),
            onPrev = { prev() },
            onNext = { next() },
            onFirst = { first() },
            onLimitChange = { pageSize(it) },
            // Null callbacks make the preview's Edit/Delete disappear entirely.
            onEditRow = if (readOnly) null else fun(doc: String) {
                val id = extractIdEjson(doc) ?: return
                rowEdit = """{ "_id": $id }""" to prettyDoc(doc)
            },
            onDeleteRow = if (readOnly) null else fun(idEjson: String) {
                rowDeleteFilter = """{ "_id": $idEjson }"""
            },
        )
    }

    when (dialog) {
        "insert" -> InsertDialog(registry, connectionId, db, collection, { dialog = null }) { onDone() }
        "update" -> UpdateDialog(registry, connectionId, db, collection, draft.filter, { dialog = null }) { onDone() }
        "replace" -> ReplaceDialog(registry, connectionId, db, collection, draft.filter, "{}", { dialog = null }) { onDone() }
        "delete" -> DeleteDialog(registry, connectionId, db, collection, draft.filter, { dialog = null }) { onDone() }
        "export" -> ExportDialog(registry, connectionId, db, collection, draft.filter) { dialog = null }
        "import" -> ImportDialog(registry, connectionId, db, collection, { dialog = null }) { onDone() }
    }

    rowEdit?.let { (filter, doc) ->
        RowEditDialog(
            registry = registry,
            connectionId = connectionId,
            db = db,
            collection = collection,
            initialFilter = filter,
            initialDoc = doc,
            onClose = { rowEdit = null },
            onDone = { onDone() },
        )
    }
    rowDeleteFilter?.let { filter ->
        DeleteDialog(
            registry = registry,
            connectionId = connectionId,
            db = db,
            coll = collection,
            initialFilter = filter,
            onClose = { rowDeleteFilter = null },
        ) { onDone() }
    }
}

/** Returns the canonical EJSON for the `_id` field, suitable for embedding in a filter. */
private fun extractIdEjson(rowEjson: String): String? = runCatching {
    val obj = kotlinx.serialization.json.Json.parseToJsonElement(rowEjson).let {
        it as? kotlinx.serialization.json.JsonObject
    } ?: return@runCatching null
    obj["_id"]?.toString()
}.getOrNull()

private fun prettyDoc(rowEjson: String): String = runCatching {
    val element = kotlinx.serialization.json.Json.parseToJsonElement(rowEjson)
    kotlinx.serialization.json.Json {
        prettyPrint = true
        prettyPrintIndent = "  "
    }.encodeToString(kotlinx.serialization.json.JsonElement.serializer(), element)
}.getOrDefault(rowEjson)

@Composable
private fun HistoryButton(
    ctx: AppContext,
    connectionId: String,
    db: String,
    collection: String,
    onPick: (FindBody) -> Unit,
) {
    var open by remember(connectionId, db, collection) { mutableStateOf(false) }
    var entries by remember { mutableStateOf<List<Pair<QueryHistoryRow, FindBody>>>(emptyList()) }
    LaunchedEffect(open) {
        if (open) {
            entries = kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.IO) {
                ctx.queryHistory.list(connectionId)
                    .filter { it.database == db && it.collection == collection && it.kind == QueryKind.find }
                    .mapNotNull { row -> parseFindBody(row.body)?.let { row to it } }
                    .take(20)
            }
        }
    }
    Box {
        TextButton(onClick = { open = true }) { Text("History") }
        DropdownMenu(
            expanded = open,
            onDismissRequest = { open = false },
            modifier = Modifier.widthIn(min = 320.dp, max = 520.dp),
        ) {
            if (entries.isEmpty()) {
                DropdownMenuItem(
                    text = { Text("No queries run here yet.", style = MaterialTheme.typography.bodySmall) },
                    onClick = { open = false },
                    enabled = false,
                )
            }
            entries.forEach { (row, body) ->
                DropdownMenuItem(
                    text = { HistoryEntry(row, body) },
                    onClick = {
                        open = false
                        onPick(body)
                    },
                )
            }
        }
    }
}

@Composable
private fun HistoryEntry(row: QueryHistoryRow, body: FindBody) {
    Column {
        Text(
            body.filter.ifBlank { "{}" },
            style = MaterialTheme.typography.bodySmall,
            fontFamily = FontFamily.Monospace,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis,
        )
        val outcome = when {
            row.error != null -> "failed"
            row.rowCount != null -> "${row.rowCount} rows"
            else -> ""
        }
        val meta = buildList {
            add(formatAgo(row.ranAt))
            row.durationMs?.let { add("$it ms") }
            if (outcome.isNotEmpty()) add(outcome)
        }.joinToString(" · ")
        Text(
            meta,
            style = MaterialTheme.typography.labelSmall,
            color = if (row.error != null) MaterialTheme.colorScheme.error
            else MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }
}

/**
 * Numeric input that keeps the raw string while editing.
 *
 * Parsing on every keystroke and falling back to the previous value made the field
 * impossible to clear — backspacing to empty instantly restored the old number.
 */
@Composable
fun NumberField(label: String, value: Int, modifier: Modifier = Modifier, onChange: (Int) -> Unit) {
    var raw by remember(value) { mutableStateOf(value.toString()) }
    val parsed = raw.toIntOrNull()
    val invalid = raw.isNotEmpty() && parsed == null
    OutlinedTextField(
        value = raw,
        onValueChange = {
            raw = it.filter { c -> c.isDigit() }
            raw.toIntOrNull()?.let(onChange)
        },
        label = { Text(label, style = MaterialTheme.typography.labelSmall) },
        singleLine = true,
        isError = invalid,
        modifier = modifier.width(140.dp),
        textStyle = MaterialTheme.typography.bodySmall.copy(
            fontFamily = androidx.compose.ui.text.font.FontFamily.Monospace,
        ),
    )
}
