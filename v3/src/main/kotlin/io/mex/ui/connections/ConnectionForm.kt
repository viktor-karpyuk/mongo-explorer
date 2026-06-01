package io.mex.ui.connections

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Dialog
import androidx.compose.ui.window.DialogProperties
import io.mex.data.ConnectionInput
import io.mex.data.ConnectionRecord
import io.mex.data.UriHistoryEntry
import io.mex.mongo.TestResult
import kotlinx.coroutines.launch

@Composable
fun ConnectionForm(
    initial: ConnectionRecord?,
    history: List<UriHistoryEntry>,
    onCancel: () -> Unit,
    onSave: (ConnectionInput) -> Unit,
    onTest: suspend (String) -> TestResult,
) {
    var name by remember { mutableStateOf(initial?.name ?: "") }
    var uri by remember { mutableStateOf(initial?.uri ?: "mongodb://localhost:27017") }
    var notes by remember { mutableStateOf(initial?.notes ?: "") }
    var testing by remember { mutableStateOf(false) }
    var testResult by remember { mutableStateOf<TestResult?>(null) }
    var showHistory by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    Dialog(
        onDismissRequest = onCancel,
        properties = DialogProperties(usePlatformDefaultWidth = false),
    ) {
        Surface(
            modifier = Modifier
                .width(560.dp)
                .heightIn(max = 720.dp),
            shape = MaterialTheme.shapes.medium,
            tonalElevation = 6.dp,
            border = BorderStrokeMin,
        ) {
            Column {
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(horizontal = 18.dp, vertical = 14.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    Text(
                        if (initial != null) "Edit connection" else "New connection",
                        style = MaterialTheme.typography.titleMedium,
                        modifier = Modifier.weight(1f),
                    )
                    TextButton(onClick = onCancel) { Text("✕") }
                }
                HorizontalDivider()

                Column(
                    modifier = Modifier
                        .padding(18.dp)
                        .verticalScroll(rememberScrollState()),
                    verticalArrangement = Arrangement.spacedBy(12.dp),
                ) {
                    OutlinedTextField(
                        value = name,
                        onValueChange = { name = it },
                        label = { Text("Name") },
                        singleLine = true,
                        modifier = Modifier.fillMaxWidth(),
                    )

                    Box {
                        OutlinedTextField(
                            value = uri,
                            onValueChange = { uri = it },
                            label = { Text("Connection URI") },
                            singleLine = true,
                            modifier = Modifier.fillMaxWidth(),
                            trailingIcon = {
                                if (history.isNotEmpty()) {
                                    TextButton(onClick = { showHistory = true }) { Text("⌄") }
                                }
                            },
                        )
                        DropdownMenu(
                            expanded = showHistory,
                            onDismissRequest = { showHistory = false },
                        ) {
                            history.forEach { h ->
                                DropdownMenuItem(
                                    text = { Text(h.preview) },
                                    onClick = {
                                        uri = h.uri
                                        showHistory = false
                                    },
                                )
                            }
                        }
                    }

                    OutlinedTextField(
                        value = notes,
                        onValueChange = { notes = it },
                        label = { Text("Notes (optional)") },
                        modifier = Modifier.fillMaxWidth(),
                        minLines = 2,
                        maxLines = 4,
                    )

                    testResult?.let { r ->
                        Surface(
                            tonalElevation = 2.dp,
                            color = if (r.ok)
                                MaterialTheme.colorScheme.primaryContainer
                            else
                                MaterialTheme.colorScheme.errorContainer,
                            modifier = Modifier.fillMaxWidth(),
                        ) {
                            Text(
                                text = if (r.ok)
                                    "Reachable · MongoDB ${r.serverVersion} · ${r.topology} · ${r.pingMs} ms"
                                else
                                    "Failed: ${r.error}",
                                modifier = Modifier.padding(10.dp),
                                style = MaterialTheme.typography.bodySmall,
                            )
                        }
                    }
                }

                HorizontalDivider()
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(horizontal = 18.dp, vertical = 12.dp),
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    OutlinedButton(
                        onClick = {
                            testing = true
                            testResult = null
                            scope.launch {
                                testResult = onTest(uri.trim())
                                testing = false
                            }
                        },
                        enabled = !testing && uri.isNotBlank(),
                    ) { Text(if (testing) "Testing…" else "Test connection") }
                    Spacer(modifier = Modifier.weight(1f))
                    TextButton(onClick = onCancel) { Text("Cancel") }
                    Button(
                        onClick = {
                            onSave(ConnectionInput(name.trim(), uri.trim(), notes.takeIf { it.isNotBlank() }))
                        },
                        enabled = name.isNotBlank() && uri.isNotBlank(),
                    ) { Text("Save") }
                }
            }
        }
    }
}

private val BorderStrokeMin = androidx.compose.foundation.BorderStroke(
    1.dp,
    androidx.compose.ui.graphics.Color.Unspecified,
)
