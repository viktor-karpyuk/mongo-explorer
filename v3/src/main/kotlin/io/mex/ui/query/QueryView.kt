package io.mex.ui.query

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.key.*
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.mongo.MongoRegistry
import io.mex.ui.mutate.DeleteDialog
import io.mex.ui.mutate.InsertDialog
import io.mex.ui.mutate.ReplaceDialog
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

    fun run() = scope.launch { store.run(key, connectionId, db, collection) }
    fun next() = scope.launch { store.nextPage(key, connectionId, db, collection) }
    fun prev() = scope.launch { store.prevPage(key, connectionId, db, collection) }

    var dialog by remember(connectionId, db, collection) { mutableStateOf<String?>(null) }
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
                    modifier = Modifier.weight(1f),
                )
                TextButton(onClick = { dialog = "insert" }) { Text("Insert") }
                TextButton(onClick = { dialog = "update" }) { Text("Update") }
                TextButton(onClick = { dialog = "replace" }) { Text("Replace") }
                TextButton(onClick = { dialog = "delete" }) { Text("Delete") }
                Button(onClick = { run() }, enabled = !running) {
                    Text(if (running) "Running…" else "Run ⌘↵")
                }
            }
            QueryField("Filter", draft.filter, "{ status: \"active\" }") { draft.filter = it }
            QueryField("Projection", draft.projection, "{ name: 1, _id: 0 }") { draft.projection = it }
            QueryField("Sort", draft.sort, "{ createdAt: -1 }") { draft.sort = it }
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
            onPrev = { prev() },
            onNext = { next() },
        )
    }

    when (dialog) {
        "insert" -> InsertDialog(registry, connectionId, db, collection, { dialog = null }) { onDone() }
        "update" -> UpdateDialog(registry, connectionId, db, collection, draft.filter, { dialog = null }) { onDone() }
        "replace" -> ReplaceDialog(registry, connectionId, db, collection, draft.filter, "{}", { dialog = null }) { onDone() }
        "delete" -> DeleteDialog(registry, connectionId, db, collection, draft.filter, { dialog = null }) { onDone() }
    }
}

@Composable
private fun QueryField(label: String, value: String, hint: String, onChange: (String) -> Unit) {
    OutlinedTextField(
        value = value,
        onValueChange = onChange,
        label = { Text(label, style = MaterialTheme.typography.labelSmall) },
        placeholder = { Text(hint, style = MaterialTheme.typography.bodySmall) },
        singleLine = true,
        modifier = Modifier.fillMaxWidth(),
        textStyle = MaterialTheme.typography.bodySmall.copy(
            fontFamily = androidx.compose.ui.text.font.FontFamily.Monospace,
        ),
    )
}

@Composable
private fun NumberField(label: String, value: Int, onChange: (Int) -> Unit) {
    OutlinedTextField(
        value = value.toString(),
        onValueChange = { onChange(it.toIntOrNull() ?: value) },
        label = { Text(label, style = MaterialTheme.typography.labelSmall) },
        singleLine = true,
        modifier = Modifier.width(140.dp),
        textStyle = MaterialTheme.typography.bodySmall.copy(
            fontFamily = androidx.compose.ui.text.font.FontFamily.Monospace,
        ),
    )
}
