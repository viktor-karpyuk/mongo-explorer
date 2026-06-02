package io.mex.ui.mutate

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Dialog
import io.mex.mongo.MongoRegistry
import io.mex.mongo.MutateResult
import io.mex.mongo.deleteDocs
import io.mex.mongo.insertMany
import io.mex.mongo.insertOne
import io.mex.mongo.replaceOne
import io.mex.mongo.updateDocs
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

@Composable
fun MutateShell(
    title: String,
    primaryLabel: String,
    canSubmit: Boolean,
    onCancel: () -> Unit,
    onSubmit: suspend () -> MutateResult,
    body: @Composable () -> Unit,
) {
    var result by remember { mutableStateOf<MutateResult?>(null) }
    var running by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    Dialog(onDismissRequest = onCancel) {
        Surface(modifier = Modifier.width(560.dp), shape = MaterialTheme.shapes.medium, tonalElevation = 6.dp) {
            Column {
                Row(modifier = Modifier.fillMaxWidth().padding(horizontal = 18.dp, vertical = 12.dp)) {
                    Text(title, style = MaterialTheme.typography.titleMedium, modifier = Modifier.weight(1f))
                    TextButton(onClick = onCancel) { Text("✕") }
                }
                HorizontalDivider()
                Column(modifier = Modifier.padding(18.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
                    body()
                    result?.let { r ->
                        Surface(
                            color = if (r.ok) MaterialTheme.colorScheme.primaryContainer else MaterialTheme.colorScheme.errorContainer,
                            modifier = Modifier.fillMaxWidth(),
                        ) {
                            Text(
                                modifier = Modifier.padding(10.dp),
                                text = if (r.ok)
                                    "Done · ${r.durationMs} ms" +
                                        (r.matched?.let { " · matched $it" } ?: "") +
                                        (r.modified?.let { " · modified $it" } ?: "") +
                                        (r.inserted?.let { " · inserted $it" } ?: "") +
                                        (r.deleted?.let { " · deleted $it" } ?: "")
                                else "Failed: ${r.error}",
                                style = MaterialTheme.typography.bodySmall,
                            )
                        }
                    }
                }
                HorizontalDivider()
                Row(
                    modifier = Modifier.fillMaxWidth().padding(horizontal = 18.dp, vertical = 12.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    Spacer(modifier = Modifier.weight(1f))
                    TextButton(onClick = onCancel) { Text(if (result?.ok == true) "Close" else "Cancel") }
                    if (result?.ok != true) {
                        Button(
                            onClick = {
                                scope.launch {
                                    running = true
                                    try { result = onSubmit() } finally { running = false }
                                }
                            },
                            enabled = canSubmit && !running,
                        ) { Text(if (running) "Running…" else primaryLabel) }
                    }
                }
            }
        }
    }
}

@Composable
fun InsertDialog(registry: MongoRegistry, connectionId: String, db: String, coll: String, onClose: () -> Unit, onDone: () -> Unit) {
    var many by remember { mutableStateOf(false) }
    var body by remember { mutableStateOf("{\n  \"name\": \"\"\n}") }
    var ordered by remember { mutableStateOf(true) }

    MutateShell(
        title = "Insert into $db.$coll",
        primaryLabel = if (many) "Insert many" else "Insert one",
        canSubmit = body.isNotBlank(),
        onCancel = onClose,
        onSubmit = {
            val client = registry.client(connectionId) ?: return@MutateShell MutateResult(false, error = "Connection closed", durationMs = 0)
            val r = withContext(Dispatchers.IO) {
                if (many) insertMany(client, db, coll, body, ordered)
                else insertOne(client, db, coll, body)
            }
            if (r.ok) onDone()
            r
        },
    ) {
        Row {
            FilterChip(selected = !many, onClick = { many = false }, label = { Text("Insert one") })
            Spacer(modifier = Modifier.width(6.dp))
            FilterChip(selected = many, onClick = { many = true }, label = { Text("Insert many (array or NDJSON)") })
        }
        OutlinedTextField(
            value = body,
            onValueChange = { body = it },
            label = { Text("Document${if (many) "s" else ""}") },
            modifier = Modifier.fillMaxWidth().height(220.dp),
            textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
        )
        if (many) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Checkbox(ordered, { ordered = it })
                Text("Ordered (stop at first error)")
            }
        }
    }
}

@Composable
fun UpdateDialog(
    registry: MongoRegistry, connectionId: String, db: String, coll: String,
    initialFilter: String, onClose: () -> Unit, onDone: () -> Unit,
) {
    var filter by remember { mutableStateOf(initialFilter) }
    var update by remember { mutableStateOf("{ \$set: {  } }") }
    var many by remember { mutableStateOf(false) }
    var upsert by remember { mutableStateOf(false) }

    MutateShell(
        title = "Update in $db.$coll",
        primaryLabel = if (many) "Update many" else "Update one",
        canSubmit = filter.isNotBlank() && update.isNotBlank(),
        onCancel = onClose,
        onSubmit = {
            val client = registry.client(connectionId) ?: return@MutateShell MutateResult(false, error = "Connection closed", durationMs = 0)
            val r = withContext(Dispatchers.IO) { updateDocs(client, db, coll, filter, update, many, upsert) }
            if (r.ok) onDone()
            r
        },
    ) {
        OutlinedTextField(filter, { filter = it }, label = { Text("Filter") }, modifier = Modifier.fillMaxWidth().height(80.dp), textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace))
        OutlinedTextField(update, { update = it }, label = { Text("Update expression") }, modifier = Modifier.fillMaxWidth().height(120.dp), textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace))
        Row { Checkbox(many, { many = it }); Text("Update many") }
        Row { Checkbox(upsert, { upsert = it }); Text("Upsert") }
    }
}

@Composable
fun ReplaceDialog(
    registry: MongoRegistry, connectionId: String, db: String, coll: String,
    initialFilter: String, initialDoc: String = "{}", onClose: () -> Unit, onDone: () -> Unit,
) {
    var filter by remember { mutableStateOf(initialFilter) }
    var replacement by remember { mutableStateOf(initialDoc) }
    var upsert by remember { mutableStateOf(false) }

    MutateShell(
        title = "Replace in $db.$coll",
        primaryLabel = "Replace one",
        canSubmit = filter.isNotBlank() && replacement.isNotBlank(),
        onCancel = onClose,
        onSubmit = {
            val client = registry.client(connectionId) ?: return@MutateShell MutateResult(false, error = "Connection closed", durationMs = 0)
            val r = withContext(Dispatchers.IO) { replaceOne(client, db, coll, filter, replacement, upsert) }
            if (r.ok) onDone()
            r
        },
    ) {
        OutlinedTextField(filter, { filter = it }, label = { Text("Filter") }, modifier = Modifier.fillMaxWidth().height(80.dp), textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace))
        OutlinedTextField(replacement, { replacement = it }, label = { Text("Replacement document") }, modifier = Modifier.fillMaxWidth().height(200.dp), textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace))
        Row { Checkbox(upsert, { upsert = it }); Text("Upsert") }
    }
}

@Composable
fun DeleteDialog(
    registry: MongoRegistry, connectionId: String, db: String, coll: String,
    initialFilter: String, onClose: () -> Unit, onDone: () -> Unit,
) {
    var filter by remember { mutableStateOf(initialFilter) }
    var many by remember { mutableStateOf(false) }

    MutateShell(
        title = "Delete from $db.$coll",
        primaryLabel = if (many) "Delete many" else "Delete one",
        canSubmit = filter.isNotBlank(),
        onCancel = onClose,
        onSubmit = {
            val client = registry.client(connectionId) ?: return@MutateShell MutateResult(false, error = "Connection closed", durationMs = 0)
            val r = withContext(Dispatchers.IO) { deleteDocs(client, db, coll, filter, many) }
            if (r.ok) onDone()
            r
        },
    ) {
        OutlinedTextField(filter, { filter = it }, label = { Text("Filter") }, modifier = Modifier.fillMaxWidth().height(120.dp), textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace))
        Row { Checkbox(many, { many = it }); Text("Delete many") }
    }
}
