package io.mex.ui.connections

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.grid.GridCells
import androidx.compose.foundation.lazy.grid.LazyVerticalGrid
import androidx.compose.foundation.lazy.grid.items
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Delete
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.data.ConnectionInput
import io.mex.data.ConnectionRecord
import io.mex.data.ConnectionSummary
import io.mex.mongo.ConnectionState
import io.mex.mongo.MongoRegistry
import io.mex.ui.components.StatePill
import io.mex.ui.state.Selection
import io.mex.ui.state.SelectionStore
import io.mex.util.formatUriPreview
import kotlinx.coroutines.launch

@Composable
fun ConnectionsView(
    ctx: AppContext,
    registry: MongoRegistry,
    vm: ConnectionsViewModel,
    selection: SelectionStore? = null,
) {
    val states by vm.states.collectAsState()
    val scope = rememberCoroutineScope()

    var editing by remember { mutableStateOf<Pair<Boolean, ConnectionRecord?>>(false to null) }
    var confirmingDelete by remember { mutableStateOf<ConnectionSummary?>(null) }

    Column(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Row(
            verticalAlignment = Alignment.CenterVertically,
            modifier = Modifier.fillMaxWidth().padding(bottom = 16.dp),
        ) {
            Text("Connections", style = MaterialTheme.typography.headlineSmall, modifier = Modifier.weight(1f))
            Button(onClick = { editing = true to null }) { Text("+ New connection") }
        }

        if (vm.list.isEmpty()) {
            EmptyState(onNew = { editing = true to null })
        } else {
            LazyVerticalGrid(
                columns = GridCells.Adaptive(minSize = 320.dp),
                horizontalArrangement = Arrangement.spacedBy(12.dp),
                verticalArrangement = Arrangement.spacedBy(12.dp),
                modifier = Modifier.fillMaxSize(),
            ) {
                items(items = vm.list, key = { c -> c.id }) { conn: ConnectionSummary ->
                    ConnectionCard(
                        conn = conn,
                        state = states[conn.id] ?: ConnectionState.Disconnected,
                        onOpen = {
                            scope.launch {
                                val s = vm.open(conn.id)
                                if (s is ConnectionState.Connected) {
                                    selection?.select(Selection.ConnectionView(conn.id))
                                }
                            }
                        },
                        onClose = { scope.launch { vm.close(conn.id) } },
                        onEdit = {
                            scope.launch {
                                val record = ctx.connections.get(conn.id)
                                editing = true to record
                            }
                        },
                        onDuplicate = { vm.duplicate(conn.id) },
                        onDelete = { confirmingDelete = conn },
                        previewProvider = {
                            val record = ctx.connections.get(conn.id) ?: return@ConnectionCard "…"
                            formatUriPreview(record.uri)
                        },
                    )
                }
            }
        }
    }

    if (editing.first) {
        ConnectionForm(
            initial = editing.second,
            history = vm.history,
            onCancel = { editing = false to null },
            onSave = { input: ConnectionInput ->
                val target = editing.second
                if (target != null) vm.update(target.id, input) else vm.create(input)
                editing = false to null
            },
            onTest = vm::test,
        )
    }

    confirmingDelete?.let { target ->
        io.mex.ui.components.ConfirmDangerDialog(
            title = "Delete connection?",
            text = "This will permanently remove \"${target.name}\" from this app. " +
                "The MongoDB cluster itself is untouched. This cannot be undone.",
            confirmLabel = "Delete",
            onConfirm = {
                vm.delete(target.id)
                confirmingDelete = null
            },
            onCancel = { confirmingDelete = null },
        )
    }
}

@OptIn(androidx.compose.foundation.layout.ExperimentalLayoutApi::class)
@Composable
private fun ConnectionCard(
    conn: ConnectionSummary,
    state: ConnectionState,
    onOpen: () -> Unit,
    onClose: () -> Unit,
    onEdit: () -> Unit,
    onDuplicate: () -> Unit,
    onDelete: () -> Unit,
    previewProvider: () -> String,
) {
    val preview = remember(conn.updatedAt) { previewProvider() }

    Card(
        modifier = Modifier.fillMaxWidth(),
        elevation = CardDefaults.cardElevation(defaultElevation = 0.dp),
        border = CardDefaults.outlinedCardBorder(),
    ) {
        Column(modifier = Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Row(verticalAlignment = Alignment.Top) {
                Column(modifier = Modifier.weight(1f)) {
                    Text(conn.name, style = MaterialTheme.typography.titleMedium)
                    Text(
                        preview,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
                StatePill(state)
                Spacer(modifier = Modifier.width(6.dp))
                IconButton(
                    onClick = onDelete,
                    modifier = Modifier.size(28.dp),
                ) {
                    Icon(
                        imageVector = androidx.compose.material.icons.Icons.Outlined.Delete,
                        contentDescription = "Delete",
                        tint = MaterialTheme.colorScheme.error,
                        modifier = Modifier.size(18.dp),
                    )
                }
            }

            if (state is ConnectionState.Error) {
                Surface(
                    color = MaterialTheme.colorScheme.errorContainer,
                    modifier = Modifier.fillMaxWidth(),
                ) {
                    Text(
                        state.message,
                        modifier = Modifier.padding(8.dp),
                        style = MaterialTheme.typography.bodySmall,
                    )
                }
            }

            FlowRow(
                horizontalArrangement = Arrangement.spacedBy(4.dp),
                verticalArrangement = Arrangement.spacedBy(4.dp),
                modifier = Modifier.fillMaxWidth(),
            ) {
                if (state is ConnectionState.Connected) {
                    TextButton(onClick = onClose) { Text("Disconnect") }
                } else {
                    Button(
                        onClick = onOpen,
                        enabled = state !is ConnectionState.Connecting,
                    ) {
                        Text(if (state is ConnectionState.Connecting) "Connecting…" else "Connect")
                    }
                }
                TextButton(onClick = onEdit) { Text("Edit") }
                TextButton(onClick = onDuplicate) { Text("Duplicate") }
            }
        }
    }
}

@Composable
private fun EmptyState(onNew: () -> Unit) {
    Column(
        modifier = Modifier.fillMaxSize(),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.Center,
    ) {
        Text("No connections yet", style = MaterialTheme.typography.titleMedium)
        Spacer(modifier = Modifier.height(6.dp))
        Text(
            "Add your first MongoDB connection. URIs and credentials encrypt locally with AES-256-GCM.",
            style = MaterialTheme.typography.bodyMedium,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Spacer(modifier = Modifier.height(14.dp))
        Button(onClick = onNew) { Text("+ Add connection") }
    }
}
