package io.mex.ui.connections

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.text.input.VisualTransformation
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

    // Manual (host & credentials) mode — an alternative way to author the same URI.
    val prefill = remember { initial?.uri?.let { parseManualUri(it) } }
    var manualMode by remember { mutableStateOf(prefill?.user?.isNotBlank() == true) }
    var srv by remember { mutableStateOf(prefill?.srv ?: false) }
    var host by remember { mutableStateOf(prefill?.host ?: "localhost") }
    var port by remember { mutableStateOf(prefill?.port ?: "27017") }
    var username by remember { mutableStateOf(prefill?.user ?: "") }
    var password by remember { mutableStateOf(prefill?.pass ?: "") }
    var authDb by remember { mutableStateOf(prefill?.authDb ?: "") }
    var showPassword by remember { mutableStateOf(false) }

    fun effectiveUri(): String =
        if (manualMode) buildManualUri(srv, host, port, username, password, authDb)
        else uri.trim()
    val canSubmit = if (manualMode) host.isNotBlank() else uri.isNotBlank()

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
                        .weight(1f, fill = false)
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

                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        ModeChip("Connection string", !manualMode) { manualMode = false }
                        ModeChip("Host & credentials", manualMode) { manualMode = true }
                    }

                    if (!manualMode) Box {
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
                    } else {
                        Row(
                            horizontalArrangement = Arrangement.spacedBy(8.dp),
                            verticalAlignment = Alignment.CenterVertically,
                        ) {
                            OutlinedTextField(
                                value = host,
                                onValueChange = { host = it },
                                label = { Text("Host") },
                                singleLine = true,
                                modifier = Modifier.weight(1f),
                            )
                            OutlinedTextField(
                                value = port,
                                onValueChange = { s -> port = s.filter { it.isDigit() }.take(5) },
                                label = { Text("Port") },
                                singleLine = true,
                                enabled = !srv,
                                modifier = Modifier.width(110.dp),
                            )
                        }
                        Row(verticalAlignment = Alignment.CenterVertically) {
                            Checkbox(checked = srv, onCheckedChange = { srv = it })
                            Text("DNS seed list (mongodb+srv)", style = MaterialTheme.typography.bodySmall)
                        }
                        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                            OutlinedTextField(
                                value = username,
                                onValueChange = { username = it },
                                label = { Text("Username") },
                                singleLine = true,
                                modifier = Modifier.weight(1f),
                            )
                            OutlinedTextField(
                                value = password,
                                onValueChange = { password = it },
                                label = { Text("Password") },
                                singleLine = true,
                                visualTransformation = if (showPassword) VisualTransformation.None
                                else PasswordVisualTransformation(),
                                trailingIcon = {
                                    TextButton(onClick = { showPassword = !showPassword }) {
                                        Text(if (showPassword) "Hide" else "Show", style = MaterialTheme.typography.labelSmall)
                                    }
                                },
                                modifier = Modifier.weight(1f),
                            )
                        }
                        OutlinedTextField(
                            value = authDb,
                            onValueChange = { authDb = it },
                            label = { Text("Auth database (optional, e.g. admin)") },
                            singleLine = true,
                            modifier = Modifier.fillMaxWidth(),
                        )
                        Text(
                            buildManualUri(srv, host, port, username, if (password.isBlank()) "" else "•••••", authDb),
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            fontFamily = androidx.compose.ui.text.font.FontFamily.Monospace,
                        )
                    }

                    OutlinedTextField(
                        value = notes,
                        onValueChange = { notes = it },
                        label = { Text("Notes (optional)") },
                        modifier = Modifier.fillMaxWidth(),
                        minLines = 2,
                        maxLines = 4,
                    )

                }

                HorizontalDivider()
                // Kept outside the scroll area so the outcome of Test connection is always visible.
                testResult?.let { r ->
                    Surface(
                        tonalElevation = 2.dp,
                        color = if (r.ok)
                            MaterialTheme.colorScheme.primaryContainer
                        else
                            MaterialTheme.colorScheme.errorContainer,
                        modifier = Modifier
                            .fillMaxWidth()
                            .padding(horizontal = 18.dp)
                            .padding(top = 10.dp),
                        shape = MaterialTheme.shapes.small,
                    ) {
                        Text(
                            text = if (r.ok)
                                "Reachable · MongoDB ${r.serverVersion} · ${r.topology} · ${r.pingMs} ms"
                            else
                                "Failed: ${r.error}",
                            modifier = Modifier.padding(10.dp),
                            style = MaterialTheme.typography.bodySmall,
                            maxLines = 3,
                        )
                    }
                }
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
                                testResult = onTest(effectiveUri())
                                testing = false
                            }
                        },
                        enabled = !testing && canSubmit,
                    ) { Text(if (testing) "Testing…" else "Test connection") }
                    Spacer(modifier = Modifier.weight(1f))
                    TextButton(onClick = onCancel) { Text("Cancel") }
                    Button(
                        onClick = {
                            onSave(ConnectionInput(name.trim(), effectiveUri(), notes.takeIf { it.isNotBlank() }))
                        },
                        enabled = name.isNotBlank() && canSubmit,
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

@Composable
private fun ModeChip(label: String, selected: Boolean, onClick: () -> Unit) {
    FilterChip(
        selected = selected,
        onClick = onClick,
        label = { Text(label, style = MaterialTheme.typography.labelMedium) },
    )
}

private fun encodeUserInfo(s: String): String =
    java.net.URLEncoder.encode(s, Charsets.UTF_8).replace("+", "%20")

/** Composes a mongodb:// URI from the Host & credentials fields. */
internal fun buildManualUri(
    srv: Boolean,
    host: String,
    port: String,
    username: String,
    password: String,
    authDb: String,
): String {
    val scheme = if (srv) "mongodb+srv" else "mongodb"
    val cred = when {
        username.isBlank() -> ""
        password.isBlank() -> "${encodeUserInfo(username.trim())}@"
        else -> "${encodeUserInfo(username.trim())}:${encodeUserInfo(password)}@"
    }
    val portPart = if (!srv && port.isNotBlank()) ":${port.trim()}" else ""
    val query = if (authDb.isNotBlank()) "/?authSource=${encodeUserInfo(authDb.trim())}" else ""
    return "$scheme://$cred${host.trim()}$portPart$query"
}

internal data class ManualFields(
    val srv: Boolean,
    val host: String,
    val port: String,
    val user: String,
    val pass: String,
    val authDb: String,
)

/** Best-effort decomposition of a single-host URI into form fields; null for shapes the form can't express. */
internal fun parseManualUri(uri: String): ManualFields? = runCatching {
    val u = java.net.URI(uri.trim())
    if (u.scheme != "mongodb" && u.scheme != "mongodb+srv") return null
    val userInfo = u.userInfo?.split(":", limit = 2)
    val authSource = (u.query ?: "").split("&")
        .firstOrNull { it.startsWith("authSource=") }
        ?.substringAfter("=")
    ManualFields(
        srv = u.scheme == "mongodb+srv",
        host = u.host ?: return null,
        port = u.port.takeIf { it >= 0 }?.toString() ?: "27017",
        user = userInfo?.getOrNull(0)?.let { java.net.URLDecoder.decode(it, Charsets.UTF_8) } ?: "",
        pass = userInfo?.getOrNull(1)?.let { java.net.URLDecoder.decode(it, Charsets.UTF_8) } ?: "",
        authDb = authSource?.let { java.net.URLDecoder.decode(it, Charsets.UTF_8) } ?: "",
    )
}.getOrNull()
