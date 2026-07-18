package io.mex.ui.tree

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.key.Key
import androidx.compose.ui.input.key.KeyEventType
import androidx.compose.ui.input.key.key
import androidx.compose.ui.input.key.onPreviewKeyEvent
import androidx.compose.ui.input.key.type
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.launch

sealed class CreateTarget {
    abstract val connectionId: String

    /** Create a database (with a required initial collection) on a connection. */
    data class Database(override val connectionId: String, val connectionName: String) : CreateTarget()

    /** Create a collection inside an existing database. */
    data class Collection(override val connectionId: String, val db: String) : CreateTarget()
}

/**
 * Material dialog for both create flows. [onCreate] runs the server call and
 * returns null on success or a message to surface inline on failure.
 */
@Composable
fun CreateNamespaceDialog(
    target: CreateTarget,
    onClose: () -> Unit,
    onCreate: suspend (db: String, collection: String) -> String?,
) {
    var dbName by remember { mutableStateOf(if (target is CreateTarget.Collection) target.db else "") }
    var collName by remember { mutableStateOf("") }
    var busy by remember { mutableStateOf(false) }
    var serverError by remember { mutableStateOf<String?>(null) }
    val scope = rememberCoroutineScope()

    val isDbMode = target is CreateTarget.Database
    val dbError = if (isDbMode) validateDatabaseName(dbName) else null
    val collError = validateCollectionName(collName)
    val canCreate = !busy &&
        dbName.isNotBlank() && collName.isNotBlank() &&
        dbError == null && collError == null

    fun submit() {
        if (!canCreate) return
        busy = true
        serverError = null
        scope.launch {
            val err = onCreate(dbName.trim(), collName.trim())
            if (err == null) {
                onClose()
            } else {
                serverError = err
                busy = false
            }
        }
    }

    AlertDialog(
        onDismissRequest = { if (!busy) onClose() },
        title = {
            Text(if (isDbMode) "Create database" else "Create collection")
        },
        text = {
            Column(
                verticalArrangement = Arrangement.spacedBy(10.dp),
                modifier = Modifier.onPreviewKeyEvent { e ->
                    if (e.type == KeyEventType.KeyDown && e.key == Key.Enter && canCreate) {
                        submit(); true
                    } else false
                },
            ) {
                Text(
                    when (target) {
                        is CreateTarget.Database -> "on ${target.connectionName}"
                        is CreateTarget.Collection -> "in ${target.db}"
                    },
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
                if (isDbMode) {
                    OutlinedTextField(
                        value = dbName,
                        onValueChange = { dbName = it; serverError = null },
                        label = { Text("Database name") },
                        singleLine = true,
                        isError = dbError != null,
                        supportingText = { dbError?.let { Text(it) } },
                        enabled = !busy,
                        modifier = Modifier.fillMaxWidth(),
                    )
                }
                OutlinedTextField(
                    value = collName,
                    onValueChange = { collName = it; serverError = null },
                    label = { Text(if (isDbMode) "Initial collection" else "Collection name") },
                    singleLine = true,
                    isError = collError != null,
                    supportingText = {
                        when {
                            collError != null -> Text(collError)
                            isDbMode -> Text("MongoDB only lists a database once it contains a collection.")
                        }
                    },
                    enabled = !busy,
                    modifier = Modifier.fillMaxWidth(),
                )
                serverError?.let {
                    Text(
                        it,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.error,
                    )
                }
            }
        },
        confirmButton = {
            Button(onClick = { submit() }, enabled = canCreate) {
                Text(if (busy) "Creating…" else "Create")
            }
        },
        dismissButton = {
            TextButton(onClick = onClose, enabled = !busy) { Text("Cancel") }
        },
    )
}

private val DB_FORBIDDEN = "/\\. \"$*<>:|?".toCharArray()

internal fun validateDatabaseName(name: String): String? = when {
    name.isEmpty() -> null // blank just disables Create; no error shown while typing
    name.length > 63 -> "63 characters maximum"
    name.any { it in DB_FORBIDDEN } -> "Cannot contain / \\ . \" \$ * < > : | ? or spaces"
    else -> null
}

internal fun validateCollectionName(name: String): String? = when {
    name.isEmpty() -> null
    name.startsWith("system.") -> "The \"system.\" prefix is reserved"
    name.contains('$') -> "Cannot contain \$"
    name.length > 200 -> "Name is too long"
    else -> null
}
