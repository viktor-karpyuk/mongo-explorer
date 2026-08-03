package io.mex.ui.io

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Dialog
import io.mex.io.ExportFormat
import io.mex.io.ExportRequest
import io.mex.io.ExportResult
import io.mex.io.ImportFormat
import io.mex.io.ImportRequest
import io.mex.io.ImportResult
import io.mex.io.exportFind
import io.mex.io.runImport
import io.mex.mongo.MongoRegistry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.nio.file.Path

@Composable
fun ExportDialog(
    registry: MongoRegistry,
    connectionId: String,
    db: String,
    collection: String,
    initialFilter: String,
    onClose: () -> Unit,
) {
    var format by remember { mutableStateOf(ExportFormat.ndjson) }
    var filter by remember { mutableStateOf(initialFilter) }
    var limit by remember { mutableStateOf(0) }
    var path by remember { mutableStateOf("") }
    var result by remember { mutableStateOf<ExportResult?>(null) }
    var busy by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    Dialog(onDismissRequest = { if (!busy) onClose() }) {
        Surface(modifier = Modifier.width(520.dp), shape = MaterialTheme.shapes.medium, tonalElevation = 6.dp) {
            Column(modifier = Modifier.padding(18.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
                Text("Export · $db.$collection", style = MaterialTheme.typography.titleMedium)
                Row(horizontalArrangement = Arrangement.spacedBy(4.dp)) {
                    ExportFormat.entries.forEach { f ->
                        FilterChip(selected = format == f, onClick = { format = f }, label = { Text(f.name.uppercase()) })
                    }
                }
                OutlinedTextField(filter, { filter = it }, label = { Text("Filter") }, modifier = Modifier.fillMaxWidth().height(80.dp), textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace))
                OutlinedTextField(limit.toString(), { limit = it.toIntOrNull() ?: 0 }, label = { Text("Limit (0 = all)") }, singleLine = true, modifier = Modifier.fillMaxWidth())
                Row {
                    OutlinedTextField(path, { path = it }, label = { Text("Target file") }, modifier = Modifier.weight(1f), singleLine = true)
                    Spacer(modifier = Modifier.width(4.dp))
                    OutlinedButton(onClick = {
                        val chosen = javax.swing.JFileChooser().let { fc ->
                            fc.selectedFile = java.io.File("$collection.${if (format == ExportFormat.csv) "csv" else if (format == ExportFormat.json) "json" else "jsonl"}")
                            if (fc.showSaveDialog(null) == javax.swing.JFileChooser.APPROVE_OPTION) fc.selectedFile?.absolutePath else null
                        }
                        chosen?.let { path = it }
                    }) { Text("Choose…") }
                }
                result?.let { r ->
                    Surface(color = if (r.ok) MaterialTheme.colorScheme.primaryContainer else MaterialTheme.colorScheme.errorContainer) {
                        Text(
                            if (r.ok) "Wrote ${r.written} documents in ${r.durationMs} ms." else "Failed: ${r.error}",
                            modifier = Modifier.padding(10.dp), style = MaterialTheme.typography.bodySmall,
                        )
                    }
                }
                Row {
                    Spacer(modifier = Modifier.weight(1f))
                    TextButton(onClick = onClose) { Text("Close") }
                    Button(
                        onClick = {
                            scope.launch {
                                busy = true
                                try {
                                    val client = registry.client(connectionId) ?: return@launch
                                    result = withContext(Dispatchers.IO) {
                                        exportFind(client, ExportRequest(connectionId, db, collection, format, filter, "", "", limit, Path.of(path)))
                                    }
                                } finally { busy = false }
                            }
                        },
                        enabled = path.isNotBlank() && !busy,
                    ) { Text(if (busy) "Exporting…" else "Export") }
                }
            }
        }
    }
}

@Composable
fun ImportDialog(
    registry: MongoRegistry,
    connectionId: String,
    db: String,
    collection: String,
    onClose: () -> Unit,
    onDone: () -> Unit,
) {
    var format by remember { mutableStateOf(ImportFormat.ndjson) }
    var path by remember { mutableStateOf("") }
    var ordered by remember { mutableStateOf(true) }
    var dryRunResult by remember { mutableStateOf<ImportResult?>(null) }
    var commitResult by remember { mutableStateOf<ImportResult?>(null) }
    var busy by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    fun run(dryRun: Boolean) {
        scope.launch {
            busy = true
            try {
                val client = registry.client(connectionId) ?: return@launch
                val r = withContext(Dispatchers.IO) {
                    runImport(client, ImportRequest(db, collection, format, Path.of(path), dryRun, ordered))
                }
                if (dryRun) dryRunResult = r else { commitResult = r; if (r.ok) onDone() }
            } finally { busy = false }
        }
    }

    Dialog(onDismissRequest = { if (!busy) onClose() }) {
        Surface(modifier = Modifier.width(520.dp), shape = MaterialTheme.shapes.medium, tonalElevation = 6.dp) {
            Column(modifier = Modifier.padding(18.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
                Text("Import into $db.$collection", style = MaterialTheme.typography.titleMedium)
                Row(horizontalArrangement = Arrangement.spacedBy(4.dp)) {
                    ImportFormat.entries.forEach { f ->
                        FilterChip(selected = format == f, onClick = { format = f }, label = { Text(f.name.uppercase()) })
                    }
                }
                Row {
                    OutlinedTextField(path, { path = it }, label = { Text("Source file") }, modifier = Modifier.weight(1f), singleLine = true)
                    Spacer(modifier = Modifier.width(4.dp))
                    OutlinedButton(onClick = {
                        val chosen = javax.swing.JFileChooser().let { fc ->
                            if (fc.showOpenDialog(null) == javax.swing.JFileChooser.APPROVE_OPTION) fc.selectedFile?.absolutePath else null
                        }
                        chosen?.let { path = it }
                    }) { Text("Choose…") }
                }
                Row { Checkbox(ordered, { ordered = it }); Text("Ordered") }
                dryRunResult?.let { r ->
                    Surface(color = if (r.ok) MaterialTheme.colorScheme.primaryContainer else MaterialTheme.colorScheme.errorContainer) {
                        Text(
                            if (r.ok) "Parsed ${r.read} docs in ${r.durationMs} ms (nothing inserted)." else "Dry-run failed: ${r.error}",
                            modifier = Modifier.padding(10.dp), style = MaterialTheme.typography.bodySmall,
                        )
                    }
                }
                commitResult?.let { r ->
                    Surface(color = if (r.ok) MaterialTheme.colorScheme.primaryContainer else MaterialTheme.colorScheme.errorContainer) {
                        Text(
                            if (r.ok) "Inserted ${r.inserted} / ${r.read} in ${r.durationMs} ms." else "Failed: ${r.error}",
                            modifier = Modifier.padding(10.dp), style = MaterialTheme.typography.bodySmall,
                        )
                    }
                }
                Row {
                    OutlinedButton(onClick = { run(true) }, enabled = path.isNotBlank() && !busy) { Text("Dry run") }
                    Spacer(modifier = Modifier.weight(1f))
                    TextButton(onClick = onClose) { Text("Close") }
                    Button(onClick = { run(false) }, enabled = path.isNotBlank() && !busy) {
                        Text(if (busy) "Importing…" else "Import")
                    }
                }
            }
        }
    }
}
