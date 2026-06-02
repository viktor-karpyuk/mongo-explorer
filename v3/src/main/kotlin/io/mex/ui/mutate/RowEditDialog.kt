package io.mex.ui.mutate

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.TextUnit
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.compose.ui.window.Dialog
import androidx.compose.ui.window.DialogProperties
import io.mex.mongo.MongoRegistry
import io.mex.mongo.MutateResult
import io.mex.mongo.replaceOne
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*

private enum class EditorMode { Structured, Json }

private enum class FieldType(val label: String, val color: Color) {
    String_("String", Color(0xFF16A34A)),
    Int32("Int32", Color(0xFFD97706)),
    Int64("Int64", Color(0xFFD97706)),
    Double("Double", Color(0xFFD97706)),
    Boolean_("Boolean", Color(0xFF9333EA)),
    ObjectId("ObjectId", Color(0xFF0891B2)),
    Date("Date", Color(0xFFBE185D)),
    Null("Null", Color(0xFF6B7280)),
    Object("Object", Color(0xFF2563EB)),
    Array("Array", Color(0xFF2563EB)),
    Binary("Binary", Color(0xFF6B7280)),
    Regex("Regex", Color(0xFFDC2626)),
}

private data class FieldRow(
    val id: Long,
    var key: String,
    var type: FieldType,
    var value: String, // String form; for Object/Array it's the JSON representation
)

@Composable
fun RowEditDialog(
    registry: MongoRegistry,
    connectionId: String,
    db: String,
    collection: String,
    initialFilter: String,
    initialDoc: String,
    onClose: () -> Unit,
    onDone: () -> Unit,
) {
    var mode by remember { mutableStateOf(EditorMode.Structured) }
    var jsonText by remember { mutableStateOf(prettyJson(initialDoc)) }
    var fontSize by remember { mutableStateOf(13.sp) }
    val structuredRows = remember { mutableStateListOf<FieldRow>() }
    var nextId by remember { mutableStateOf(0L) }

    // Initial load → structured
    LaunchedEffect(Unit) {
        loadIntoStructured(jsonText, structuredRows) { nextId++ }
    }

    var jsonError by remember { mutableStateOf<String?>(null) }
    LaunchedEffect(jsonText) {
        jsonError = try {
            Json.parseToJsonElement(jsonText); null
        } catch (e: Exception) {
            e.message
        }
    }

    var saveResult by remember { mutableStateOf<MutateResult?>(null) }
    var saving by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    fun switchTo(target: EditorMode) {
        if (target == mode) return
        if (target == EditorMode.Json) {
            // structured → json
            jsonText = prettyJson(structuredToJsonString(structuredRows))
        } else {
            // json → structured
            loadIntoStructured(jsonText, structuredRows) { nextId++ }
        }
        mode = target
    }

    fun currentDocJson(): String =
        if (mode == EditorMode.Json) jsonText else structuredToJsonString(structuredRows)

    Dialog(
        onDismissRequest = onClose,
        properties = DialogProperties(usePlatformDefaultWidth = false),
    ) {
        Surface(
            modifier = Modifier.width(860.dp).heightIn(min = 520.dp, max = 720.dp),
            shape = MaterialTheme.shapes.medium,
            tonalElevation = 6.dp,
        ) {
            Column {
                // Header
                Row(
                    modifier = Modifier.fillMaxWidth().padding(start = 20.dp, end = 12.dp, top = 14.dp, bottom = 8.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    Column(modifier = Modifier.weight(1f)) {
                        Text("Edit document", style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.SemiBold)
                        Text(
                            "$db.$collection · filter: $initialFilter",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                        )
                    }
                    // View toggle
                    Row {
                        ToggleBtn("Structured", mode == EditorMode.Structured) { switchTo(EditorMode.Structured) }
                        ToggleBtn("JSON", mode == EditorMode.Json) { switchTo(EditorMode.Json) }
                    }
                }
                HorizontalDivider()

                // Editor area
                Box(modifier = Modifier.weight(1f).fillMaxWidth()) {
                    when (mode) {
                        EditorMode.Structured -> StructuredEditor(
                            rows = structuredRows,
                            onAdd = {
                                structuredRows.add(
                                    FieldRow(id = nextId++, key = "", type = FieldType.String_, value = ""),
                                )
                            },
                            onRemove = { target -> structuredRows.removeIf { it.id == target.id } },
                        )
                        EditorMode.Json -> JsonEditor(
                            value = jsonText,
                            onChange = { jsonText = it },
                            fontSize = fontSize,
                        )
                    }
                }
                HorizontalDivider()

                // Toolbar
                Row(
                    modifier = Modifier.fillMaxWidth().padding(horizontal = 18.dp, vertical = 8.dp),
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                ) {
                    if (mode == EditorMode.Structured) {
                        OutlinedButton(onClick = {
                            structuredRows.add(
                                FieldRow(id = nextId++, key = "", type = FieldType.String_, value = ""),
                            )
                        }) { Text("+ Add Field") }
                    } else {
                        OutlinedButton(onClick = {
                            runCatching {
                                jsonText = prettyJson(jsonText)
                            }
                        }) { Text("Format") }
                        TextButton(onClick = { fontSize = (fontSize.value - 1f).coerceAtLeast(9f).sp }) { Text("A−") }
                        TextButton(onClick = { fontSize = (fontSize.value + 1f).coerceAtMost(24f).sp }) { Text("A+") }
                    }
                    Spacer(modifier = Modifier.weight(1f))
                    if (mode == EditorMode.Json) {
                        ValidationStatus(jsonError)
                    }
                }

                saveResult?.let { r ->
                    Surface(
                        color = if (r.ok) MaterialTheme.colorScheme.primaryContainer else MaterialTheme.colorScheme.errorContainer,
                        modifier = Modifier.fillMaxWidth(),
                    ) {
                        Text(
                            modifier = Modifier.padding(horizontal = 18.dp, vertical = 8.dp),
                            text = if (r.ok)
                                "Saved · matched ${r.matched ?: 0} · modified ${r.modified ?: 0} · ${r.durationMs} ms"
                            else "Save failed: ${r.error}",
                            style = MaterialTheme.typography.bodySmall,
                        )
                    }
                }

                HorizontalDivider()
                Row(
                    modifier = Modifier.fillMaxWidth().padding(horizontal = 18.dp, vertical = 12.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    Spacer(modifier = Modifier.weight(1f))
                    TextButton(onClick = onClose) { Text(if (saveResult?.ok == true) "Close" else "Cancel") }
                    Spacer(modifier = Modifier.width(8.dp))
                    val canSave = saveResult?.ok != true && !saving && (mode == EditorMode.Structured || jsonError == null)
                    Button(
                        onClick = {
                            scope.launch {
                                saving = true
                                try {
                                    val client = registry.client(connectionId)
                                    if (client == null) {
                                        saveResult = MutateResult(false, error = "Connection closed", durationMs = 0)
                                        return@launch
                                    }
                                    val replacement = currentDocJson()
                                    val r = withContext(Dispatchers.IO) {
                                        replaceOne(client, db, collection, initialFilter, replacement, upsert = false)
                                    }
                                    saveResult = r
                                    if (r.ok) onDone()
                                } finally { saving = false }
                            }
                        },
                        enabled = canSave,
                        colors = ButtonDefaults.buttonColors(
                            containerColor = Color(0xFF16A34A),
                            contentColor = Color.White,
                        ),
                    ) { Text(if (saving) "Saving…" else "Save") }
                }
            }
        }
    }
}

@Composable
private fun ToggleBtn(label: String, selected: Boolean, onClick: () -> Unit) {
    val bg = if (selected) MaterialTheme.colorScheme.primary.copy(alpha = 0.12f) else Color.Transparent
    val color = if (selected) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant
    Box(
        modifier = Modifier
            .background(bg, RoundedCornerShape(6.dp))
            .padding(horizontal = 2.dp),
    ) {
        TextButton(onClick = onClick) {
            Text(label, color = color, style = MaterialTheme.typography.labelMedium)
        }
    }
}

@Composable
private fun ValidationStatus(error: String?) {
    val ok = error == null
    Text(
        text = if (ok) "✓ valid JSON" else "✗ ${error?.take(60)}",
        color = if (ok) Color(0xFF16A34A) else MaterialTheme.colorScheme.error,
        style = MaterialTheme.typography.labelSmall,
        fontFamily = FontFamily.Monospace,
    )
}

@Composable
private fun JsonEditor(value: String, onChange: (String) -> Unit, fontSize: TextUnit) {
    OutlinedTextField(
        value = value,
        onValueChange = onChange,
        modifier = Modifier.fillMaxSize().padding(16.dp),
        textStyle = androidx.compose.ui.text.TextStyle(
            fontFamily = FontFamily.Monospace,
            fontSize = fontSize,
        ),
    )
}

@Composable
private fun StructuredEditor(
    rows: List<FieldRow>,
    onAdd: () -> Unit,
    onRemove: (FieldRow) -> Unit,
) {
    Column(
        modifier = Modifier.fillMaxSize().verticalScroll(rememberScrollState()).padding(12.dp),
        verticalArrangement = Arrangement.spacedBy(4.dp),
    ) {
        // Header
        Row(
            modifier = Modifier.fillMaxWidth().padding(start = 8.dp, end = 8.dp, bottom = 4.dp),
        ) {
            Text("KEY", modifier = Modifier.weight(0.30f), style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
            Text("TYPE", modifier = Modifier.width(120.dp), style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
            Text("VALUE", modifier = Modifier.weight(0.60f), style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
            Spacer(modifier = Modifier.width(40.dp))
        }
        if (rows.isEmpty()) {
            Text(
                "(empty document)",
                modifier = Modifier.padding(start = 8.dp),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
        for (row in rows) {
            key(row.id) { FieldRowView(row = row, onRemove = { onRemove(row) }) }
        }
        Spacer(modifier = Modifier.height(8.dp))
        TextButton(onClick = onAdd) { Text("+ Add Field") }
    }
}

@Composable
private fun FieldRowView(row: FieldRow, onRemove: () -> Unit) {
    var key by remember(row.id) { mutableStateOf(row.key) }
    var type by remember(row.id) { mutableStateOf(row.type) }
    var value by remember(row.id) { mutableStateOf(row.value) }
    var typeMenu by remember { mutableStateOf(false) }

    // Keep model in sync.
    LaunchedEffect(key, type, value) {
        row.key = key
        row.type = type
        row.value = value
    }

    Row(
        modifier = Modifier.fillMaxWidth(),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        // Key
        OutlinedTextField(
            value = key,
            onValueChange = { key = it },
            modifier = Modifier.weight(0.30f),
            singleLine = true,
            textStyle = androidx.compose.ui.text.TextStyle(fontFamily = FontFamily.Monospace, fontSize = 13.sp),
        )
        // Type
        Box(modifier = Modifier.width(120.dp)) {
            OutlinedButton(
                onClick = { typeMenu = true },
                contentPadding = PaddingValues(horizontal = 10.dp, vertical = 6.dp),
                modifier = Modifier.fillMaxWidth(),
            ) {
                Text(type.label, color = type.color, style = MaterialTheme.typography.labelSmall, fontFamily = FontFamily.Monospace)
            }
            DropdownMenu(expanded = typeMenu, onDismissRequest = { typeMenu = false }) {
                FieldType.entries.forEach { t ->
                    DropdownMenuItem(
                        text = { Text(t.label, color = t.color, fontFamily = FontFamily.Monospace) },
                        onClick = {
                            typeMenu = false
                            type = t
                            // Reset value when changing type to incompatible defaults.
                            value = defaultValueFor(t)
                        },
                    )
                }
            }
        }
        // Value
        Box(modifier = Modifier.weight(0.60f)) {
            when (type) {
                FieldType.Boolean_ -> {
                    val checked = value.equals("true", ignoreCase = true)
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Switch(checked = checked, onCheckedChange = { value = it.toString() })
                        Spacer(modifier = Modifier.width(8.dp))
                        Text(if (checked) "true" else "false", fontFamily = FontFamily.Monospace)
                    }
                }
                FieldType.Null -> {
                    Text("null", color = MaterialTheme.colorScheme.onSurfaceVariant, fontFamily = FontFamily.Monospace)
                }
                else -> {
                    OutlinedTextField(
                        value = value,
                        onValueChange = { value = it },
                        modifier = Modifier.fillMaxWidth(),
                        singleLine = type !in listOf(FieldType.Object, FieldType.Array),
                        textStyle = androidx.compose.ui.text.TextStyle(fontFamily = FontFamily.Monospace, fontSize = 13.sp),
                        placeholder = { Text(hintFor(type), style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant) },
                    )
                }
            }
        }
        // Remove
        TextButton(onClick = onRemove, modifier = Modifier.width(40.dp)) {
            Text("✕", color = MaterialTheme.colorScheme.error)
        }
    }
}

// ---------- JSON <-> Structured round-trip ----------

private fun loadIntoStructured(
    jsonText: String,
    rows: MutableList<FieldRow>,
    nextId: () -> Long,
) {
    rows.clear()
    val parsed = runCatching { Json.parseToJsonElement(jsonText) as? JsonObject }.getOrNull() ?: return
    for ((k, v) in parsed) {
        val (type, value) = elementToTyped(v)
        rows.add(FieldRow(id = nextId(), key = k, type = type, value = value))
    }
}

private fun structuredToJsonString(rows: List<FieldRow>): String {
    val obj = buildJsonObject {
        for (row in rows) {
            if (row.key.isBlank()) continue
            put(row.key, typedToElement(row.type, row.value))
        }
    }
    return Json { prettyPrint = true; prettyPrintIndent = "  " }
        .encodeToString(JsonElement.serializer(), obj)
}

private fun elementToTyped(v: JsonElement): Pair<FieldType, String> = when (v) {
    is JsonNull -> FieldType.Null to ""
    is JsonPrimitive -> when {
        v.isString -> FieldType.String_ to v.content
        v.booleanOrNull != null -> FieldType.Boolean_ to v.boolean.toString()
        else -> {
            val s = v.content
            if (s.contains('.') || s.contains('e') || s.contains('E')) FieldType.Double to s
            else if (s.toIntOrNull() != null) FieldType.Int32 to s
            else FieldType.Int64 to s
        }
    }
    is JsonArray -> FieldType.Array to v.toString()
    is JsonObject -> when {
        "\$oid" in v -> FieldType.ObjectId to (v["\$oid"] as JsonPrimitive).content
        "\$date" in v -> {
            val raw = v["\$date"]!!
            val text = if (raw is JsonPrimitive) raw.content else raw.toString()
            FieldType.Date to text
        }
        "\$numberLong" in v -> FieldType.Int64 to (v["\$numberLong"] as JsonPrimitive).content
        "\$numberDecimal" in v -> FieldType.Double to (v["\$numberDecimal"] as JsonPrimitive).content
        "\$binary" in v -> FieldType.Binary to v.toString()
        "\$regularExpression" in v -> FieldType.Regex to v.toString()
        else -> FieldType.Object to v.toString()
    }
}

private fun typedToElement(type: FieldType, value: String): JsonElement = when (type) {
    FieldType.String_ -> JsonPrimitive(value)
    FieldType.Int32 -> JsonPrimitive(value.toIntOrNull() ?: 0)
    FieldType.Int64 -> buildJsonObject { put("\$numberLong", JsonPrimitive(value.ifBlank { "0" })) }
    FieldType.Double -> JsonPrimitive(value.toDoubleOrNull() ?: 0.0)
    FieldType.Boolean_ -> JsonPrimitive(value.equals("true", ignoreCase = true))
    FieldType.ObjectId -> buildJsonObject { put("\$oid", JsonPrimitive(value)) }
    FieldType.Date -> buildJsonObject { put("\$date", JsonPrimitive(value)) }
    FieldType.Null -> JsonNull
    FieldType.Object, FieldType.Array, FieldType.Binary, FieldType.Regex -> {
        runCatching { Json.parseToJsonElement(value) }.getOrElse { JsonNull }
    }
}

private fun defaultValueFor(type: FieldType): String = when (type) {
    FieldType.String_ -> ""
    FieldType.Int32, FieldType.Int64 -> "0"
    FieldType.Double -> "0.0"
    FieldType.Boolean_ -> "false"
    FieldType.ObjectId -> ""
    FieldType.Date -> ""
    FieldType.Null -> ""
    FieldType.Object -> "{}"
    FieldType.Array -> "[]"
    FieldType.Binary -> """{"${'$'}binary": {"base64": "", "subType": "0"}}"""
    FieldType.Regex -> """{"${'$'}regularExpression": {"pattern": "", "options": ""}}"""
}

private fun hintFor(type: FieldType): String = when (type) {
    FieldType.ObjectId -> "24-hex string"
    FieldType.Date -> "ISO 8601 e.g. 2024-01-01T00:00:00Z"
    FieldType.Object -> "{ \"key\": ... }"
    FieldType.Array -> "[ ... ]"
    else -> ""
}

private fun prettyJson(s: String): String = runCatching {
    val element = Json.parseToJsonElement(s)
    Json { prettyPrint = true; prettyPrintIndent = "  " }.encodeToString(JsonElement.serializer(), element)
}.getOrDefault(s)
