package io.mex.ui.indexes

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Dialog
import io.mex.mongo.CreateIndexInput
import io.mex.mongo.IndexInfo
import io.mex.mongo.IndexKey
import io.mex.mongo.IndexStat
import io.mex.mongo.MongoRegistry
import io.mex.mongo.createIndex
import io.mex.mongo.dropIndex
import io.mex.mongo.indexStats
import io.mex.mongo.listIndexes
import io.mex.util.formatCount
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

@Composable
fun IndexesPanel(connectionId: String, db: String, collection: String, registry: MongoRegistry) {
    var indexes by remember(connectionId, db, collection) { mutableStateOf<List<IndexInfo>>(emptyList()) }
    var stats by remember(connectionId, db, collection) { mutableStateOf<Map<String, IndexStat>>(emptyMap()) }
    var error by remember(connectionId, db, collection) { mutableStateOf<String?>(null) }
    var creating by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    fun load() {
        scope.launch {
            val client = registry.client(connectionId) ?: return@launch
            try {
                indexes = withContext(Dispatchers.IO) { listIndexes(client, db, collection) }
                stats = withContext(Dispatchers.IO) { indexStats(client, db, collection) }.associateBy { it.name }
            } catch (e: Exception) {
                error = e.message
            }
        }
    }

    LaunchedEffect(connectionId, db, collection) { load() }

    Column(modifier = Modifier.fillMaxSize().padding(24.dp).verticalScroll(rememberScrollState())) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Text("Indexes · $db.$collection", style = MaterialTheme.typography.titleMedium, modifier = Modifier.weight(1f))
            Button(onClick = { creating = true }) { Text("+ Create index") }
        }
        Spacer(modifier = Modifier.height(12.dp))
        error?.let { Text(it, color = MaterialTheme.colorScheme.error) }

        indexes.forEach { idx ->
            Card(border = CardDefaults.outlinedCardBorder(), modifier = Modifier.fillMaxWidth().padding(vertical = 2.dp)) {
                Row(
                    modifier = Modifier.fillMaxWidth().padding(8.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    Column(modifier = Modifier.weight(1f)) {
                        Text(idx.name, fontFamily = FontFamily.Monospace, color = MaterialTheme.colorScheme.primary)
                        Text(
                            idx.keys.joinToString(", ") { "${it.field}: ${it.direction}" },
                            style = MaterialTheme.typography.bodySmall,
                            fontFamily = FontFamily.Monospace,
                        )
                        Text(
                            listOfNotNull(
                                if (idx.unique) "unique" else null,
                                if (idx.sparse) "sparse" else null,
                                if (idx.background) "background" else null,
                                idx.ttlSeconds?.let { "ttl ${it}s" },
                                if (idx.partialFilter != null) "partial" else null,
                            ).joinToString(" · ").ifEmpty { "—" },
                            style = MaterialTheme.typography.labelSmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                    }
                    Text(
                        stats[idx.name]?.let { formatCount(it.ops) + " ops" } ?: "—",
                        style = MaterialTheme.typography.labelSmall,
                        modifier = Modifier.padding(end = 8.dp),
                    )
                    TextButton(
                        onClick = {
                            if (idx.name == "_id_") return@TextButton
                            val confirm = javax.swing.JOptionPane.showInputDialog(null, "Type the index name to drop:", idx.name)
                            if (confirm == idx.name) {
                                scope.launch {
                                    val client = registry.client(connectionId) ?: return@launch
                                    withContext(Dispatchers.IO) { dropIndex(client, db, collection, idx.name) }
                                    load()
                                }
                            }
                        },
                        enabled = idx.name != "_id_",
                    ) { Text("Drop") }
                }
            }
        }
    }

    if (creating) {
        CreateIndexDialog(
            onCancel = { creating = false },
            onCreate = { input ->
                scope.launch {
                    val client = registry.client(connectionId) ?: return@launch
                    try {
                        withContext(Dispatchers.IO) { createIndex(client, db, collection, input) }
                        creating = false
                        load()
                    } catch (e: Exception) {
                        error = e.message
                    }
                }
            },
        )
    }
}

@Composable
private fun CreateIndexDialog(onCancel: () -> Unit, onCreate: (CreateIndexInput) -> Unit) {
    var field by remember { mutableStateOf("") }
    var direction by remember { mutableStateOf("1") }
    var unique by remember { mutableStateOf(false) }
    var sparse by remember { mutableStateOf(false) }
    var background by remember { mutableStateOf(true) }
    var ttl by remember { mutableStateOf("") }
    var name by remember { mutableStateOf("") }

    Dialog(onDismissRequest = onCancel) {
        Surface(modifier = Modifier.width(420.dp), shape = MaterialTheme.shapes.medium, tonalElevation = 6.dp) {
            Column(modifier = Modifier.padding(18.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Text("Create index", style = MaterialTheme.typography.titleMedium)
                OutlinedTextField(field, { field = it }, label = { Text("Field") }, singleLine = true, modifier = Modifier.fillMaxWidth())
                OutlinedTextField(direction, { direction = it }, label = { Text("Direction (1 / -1 / 2dsphere / text / hashed)") }, singleLine = true, modifier = Modifier.fillMaxWidth())
                OutlinedTextField(name, { name = it }, label = { Text("Name (optional)") }, singleLine = true, modifier = Modifier.fillMaxWidth())
                Row { Checkbox(unique, { unique = it }); Text("Unique") }
                Row { Checkbox(sparse, { sparse = it }); Text("Sparse") }
                Row { Checkbox(background, { background = it }); Text("Background") }
                OutlinedTextField(ttl, { ttl = it }, label = { Text("TTL seconds (optional)") }, singleLine = true, modifier = Modifier.fillMaxWidth())
                Row(modifier = Modifier.fillMaxWidth()) {
                    Spacer(modifier = Modifier.weight(1f))
                    TextButton(onClick = onCancel) { Text("Cancel") }
                    Button(
                        onClick = {
                            onCreate(
                                CreateIndexInput(
                                    keys = listOf(IndexKey(field, direction)),
                                    name = name.takeIf { it.isNotBlank() },
                                    unique = unique,
                                    sparse = sparse,
                                    background = background,
                                    ttlSeconds = ttl.toLongOrNull(),
                                ),
                            )
                        },
                        enabled = field.isNotBlank(),
                    ) { Text("Create") }
                }
            }
        }
    }
}
