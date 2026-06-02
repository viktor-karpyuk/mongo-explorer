package io.mex.ui.schema

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import io.mex.mongo.MongoRegistry
import io.mex.mongo.SchemaReport
import io.mex.mongo.analyzeSchema
import io.mex.util.formatCount
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

private val TYPE_COLORS = mapOf(
    "string" to Color(0xFF60A5FA),
    "number" to Color(0xFFFBBF24),
    "boolean" to Color(0xFFF472B6),
    "null" to Color(0xFF6B7280),
    "array" to Color(0xFFA78BFA),
    "object" to Color(0xFF94A3B8),
    "objectid" to Color(0xFF34D399),
    "date" to Color(0xFFA78BFA),
)

@Composable
fun SchemaPanel(connectionId: String, db: String, collection: String, registry: MongoRegistry) {
    var report by remember(connectionId, db, collection) { mutableStateOf<SchemaReport?>(null) }
    var error by remember(connectionId, db, collection) { mutableStateOf<String?>(null) }
    var sampleSize by remember { mutableStateOf(1000) }
    var loading by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    fun load() {
        scope.launch {
            loading = true
            error = null
            val client = registry.client(connectionId) ?: run { loading = false; return@launch }
            try {
                report = withContext(Dispatchers.IO) { analyzeSchema(client, db, collection, sampleSize) }
            } catch (e: Exception) {
                error = e.message
            } finally {
                loading = false
            }
        }
    }

    LaunchedEffect(connectionId, db, collection) { load() }

    Column(modifier = Modifier.fillMaxSize().padding(24.dp).verticalScroll(rememberScrollState())) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Text("Schema · $db.$collection", style = MaterialTheme.typography.titleMedium, modifier = Modifier.weight(1f))
            OutlinedTextField(
                value = sampleSize.toString(),
                onValueChange = { sampleSize = it.toIntOrNull() ?: sampleSize },
                label = { Text("Sample", style = MaterialTheme.typography.labelSmall) },
                singleLine = true,
                modifier = Modifier.width(120.dp),
            )
            Spacer(modifier = Modifier.width(8.dp))
            Button(onClick = { load() }, enabled = !loading) {
                Text(if (loading) "Sampling…" else "Re-sample")
            }
        }
        Spacer(modifier = Modifier.height(12.dp))
        error?.let { Text(it, color = MaterialTheme.colorScheme.error) }
        report?.let { r ->
            Text(
                "Analyzed ${formatCount(r.sampleSize.toLong())} of ~${formatCount(r.totalDocs)}",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            Spacer(modifier = Modifier.height(8.dp))
            r.fields.forEach { field ->
                Card(border = CardDefaults.outlinedCardBorder(), modifier = Modifier.fillMaxWidth().padding(vertical = 2.dp)) {
                    Column(modifier = Modifier.padding(8.dp)) {
                        Row(verticalAlignment = Alignment.CenterVertically) {
                            Text(field.path, fontFamily = FontFamily.Monospace, color = MaterialTheme.colorScheme.primary, modifier = Modifier.weight(1f))
                            Text("${(field.presence * 100).toInt()}%", style = MaterialTheme.typography.labelSmall)
                        }
                        Spacer(modifier = Modifier.height(4.dp))
                        // type bar
                        Row(modifier = Modifier.fillMaxWidth().height(8.dp)) {
                            field.types.entries.sortedByDescending { it.value }.forEach { (type, frac) ->
                                Box(
                                    modifier = Modifier
                                        .weight(frac.toFloat())
                                        .fillMaxHeight()
                                        .background(TYPE_COLORS[type] ?: Color.Gray),
                                )
                            }
                        }
                        Text(
                            field.types.keys.joinToString(" · "),
                            style = MaterialTheme.typography.labelSmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                    }
                }
            }
        }
    }
}
