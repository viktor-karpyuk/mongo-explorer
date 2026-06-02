package io.mex.ui.validator

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import io.mex.mongo.MongoRegistry
import io.mex.mongo.ValidatorPayload
import io.mex.mongo.getValidator
import io.mex.mongo.setValidator
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

@Composable
fun ValidatorPanel(connectionId: String, db: String, collection: String, registry: MongoRegistry) {
    var body by remember(connectionId, db, collection) { mutableStateOf("") }
    var level by remember(connectionId, db, collection) { mutableStateOf("moderate") }
    var action by remember(connectionId, db, collection) { mutableStateOf("error") }
    var feedback by remember(connectionId, db, collection) { mutableStateOf<String?>(null) }
    val scope = rememberCoroutineScope()

    fun load() {
        scope.launch {
            val client = registry.client(connectionId) ?: return@launch
            try {
                val v = withContext(Dispatchers.IO) { getValidator(client, db, collection) }
                body = v.validator ?: ""
                v.validationLevel?.let { level = it }
                v.validationAction?.let { action = it }
            } catch (e: Exception) {
                feedback = "Load failed: ${e.message}"
            }
        }
    }

    LaunchedEffect(connectionId, db, collection) { load() }

    Column(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Text("Validator · $db.$collection", style = MaterialTheme.typography.titleMedium)
        Spacer(modifier = Modifier.height(8.dp))
        feedback?.let {
            Surface(color = MaterialTheme.colorScheme.surfaceVariant, modifier = Modifier.fillMaxWidth()) {
                Text(it, modifier = Modifier.padding(8.dp), style = MaterialTheme.typography.bodySmall)
            }
            Spacer(modifier = Modifier.height(8.dp))
        }
        OutlinedTextField(
            value = body,
            onValueChange = { body = it },
            label = { Text("Validator JSON (\$jsonSchema or expression)") },
            modifier = Modifier.fillMaxWidth().weight(1f),
            textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
        )
        Spacer(modifier = Modifier.height(8.dp))
        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            LevelSelector("Level", listOf("off", "moderate", "strict"), level) { level = it }
            LevelSelector("Action", listOf("warn", "error"), action) { action = it }
            Spacer(modifier = Modifier.weight(1f))
            OutlinedButton(onClick = { load() }) { Text("Revert") }
            Button(onClick = {
                scope.launch {
                    val client = registry.client(connectionId) ?: return@launch
                    try {
                        withContext(Dispatchers.IO) {
                            setValidator(client, db, collection, ValidatorPayload(body.takeIf { it.isNotBlank() }, level, action))
                        }
                        feedback = "Validator updated."
                    } catch (e: Exception) {
                        feedback = "Update failed: ${e.message}"
                    }
                }
            }) { Text("Apply") }
        }
    }
}

@Composable
private fun LevelSelector(label: String, options: List<String>, value: String, onChange: (String) -> Unit) {
    var expanded by remember { mutableStateOf(false) }
    Box {
        OutlinedButton(onClick = { expanded = true }) {
            Text("$label: $value", style = MaterialTheme.typography.labelSmall)
        }
        DropdownMenu(expanded = expanded, onDismissRequest = { expanded = false }) {
            options.forEach { opt ->
                DropdownMenuItem(text = { Text(opt) }, onClick = { expanded = false; onChange(opt) })
            }
        }
    }
}
