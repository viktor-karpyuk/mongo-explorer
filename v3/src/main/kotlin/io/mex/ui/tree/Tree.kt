package io.mex.ui.tree

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Delete
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.pointer.PointerEventType
import androidx.compose.ui.input.pointer.PointerIcon
import androidx.compose.ui.input.pointer.pointerHoverIcon
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.mongo.CollectionInfo
import io.mex.mongo.ConnectionState
import io.mex.mongo.DatabaseInfo
import io.mex.mongo.MongoRegistry
import io.mex.mongo.createCollection
import io.mex.mongo.createDatabase
import io.mex.mongo.dropCollection
import io.mex.mongo.dropDatabase
import io.mex.ui.connections.ConnectionsViewModel
import io.mex.ui.state.NamespacesStore
import io.mex.ui.state.Selection
import io.mex.ui.state.SelectionStore
import io.mex.mongo.CreateCollectionInput
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

@Composable
fun Tree(
    ctx: AppContext,
    registry: MongoRegistry,
    selection: SelectionStore,
    namespaces: NamespacesStore,
    vm: ConnectionsViewModel,
) {
    val states by registry.states.collectAsState()
    val connected = vm.list.filter { states[it.id] is ConnectionState.Connected }

    var filter by remember { mutableStateOf("") }
    val current = selection.current
    val scope = rememberCoroutineScope()

    // Load databases when a connection becomes connected.
    LaunchedEffect(connected.map { it.id }) {
        for (c in connected) {
            if (namespaces.cache(c.id).databases.isEmpty()) {
                namespaces.loadDatabases(c.id)
            }
        }
    }

    var sortAsc by remember { mutableStateOf(true) }
    var pendingDrop by remember { mutableStateOf<PendingDrop?>(null) }

    Column(
        modifier = Modifier.fillMaxSize().background(MaterialTheme.colorScheme.surface),
    ) {
        Row(
            modifier = Modifier.fillMaxWidth().padding(8.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            OutlinedTextField(
                value = filter,
                onValueChange = { filter = it },
                placeholder = { Text("Filter namespaces…", style = MaterialTheme.typography.bodySmall) },
                singleLine = true,
                modifier = Modifier.weight(1f),
                textStyle = MaterialTheme.typography.bodySmall,
            )
            Spacer(modifier = Modifier.width(4.dp))
            OutlinedButton(
                onClick = { sortAsc = !sortAsc },
                contentPadding = PaddingValues(horizontal = 10.dp, vertical = 6.dp),
            ) {
                Text(
                    if (sortAsc) "A↓" else "Z↑",
                    style = MaterialTheme.typography.labelSmall,
                )
            }
        }

        Column(modifier = Modifier.fillMaxSize().verticalScroll(rememberScrollState())) {
            if (connected.isEmpty()) {
                Text(
                    "Open a connection to browse.",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.padding(12.dp),
                )
            }

            for (conn in connected) {
                val cache = namespaces.cache(conn.id)
                val isConnSel = current is Selection.ConnectionView && current.connectionId == conn.id
                ConnRow(
                    name = conn.name,
                    selected = isConnSel,
                    onClick = { selection.select(Selection.ConnectionView(conn.id)) },
                )

                val dbs = cache.databases
                    .filter { filter.isEmpty() || it.name.contains(filter, ignoreCase = true) }
                    .sortedBy { it.name.lowercase() }
                    .let { if (sortAsc) it else it.asReversed() }
                for (db in dbs) {
                    val expanded = cache.expanded.value.contains(db.name)
                    val isDbSel = current is Selection.Database &&
                        current.connectionId == conn.id && current.db == db.name
                    DbRow(
                        db = db,
                        expanded = expanded,
                        selected = isDbSel,
                        onToggle = { scope.launch { namespaces.toggleExpanded(conn.id, db.name) } },
                        onSelect = { selection.select(Selection.Database(conn.id, db.name)) },
                        onCreateColl = {
                            val name = singlePrompt("New collection name", "") ?: return@DbRow
                            scope.launch {
                                val client = registry.client(conn.id) ?: return@launch
                                withContext(Dispatchers.IO) {
                                    createCollection(client, db.name, CreateCollectionInput(name))
                                }
                                namespaces.loadCollections(conn.id, db.name)
                            }
                        },
                        onDrop = { pendingDrop = PendingDrop.Db(conn.id, db.name) },
                    )

                    if (expanded) {
                        val colls = cache.collections[db.name].orEmpty()
                        if (cache.collectionsLoading[db.name] == true) {
                            Text(
                                "loading…",
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                                modifier = Modifier.padding(start = 38.dp, top = 2.dp, bottom = 2.dp),
                            )
                        }
                        val visibleColls = colls
                            .filter { filter.isEmpty() || it.name.contains(filter, ignoreCase = true) }
                            .sortedBy { it.name.lowercase() }
                            .let { if (sortAsc) it else it.asReversed() }
                        for (c in visibleColls) {
                            val isCollSel = current is Selection.Collection &&
                                current.connectionId == conn.id &&
                                current.db == db.name &&
                                current.collection == c.name
                            CollRow(
                                c = c,
                                selected = isCollSel,
                                onSelect = {
                                    selection.select(Selection.Collection(conn.id, db.name, c.name))
                                },
                                onDrop = { pendingDrop = PendingDrop.Coll(conn.id, db.name, c.name) },
                            )
                        }
                    }
                }

                // Connection-level "Create database" trigger
                TextButton(
                    onClick = {
                        val name = singlePrompt("New database name", "") ?: return@TextButton
                        scope.launch {
                            val client = registry.client(conn.id) ?: return@launch
                            withContext(Dispatchers.IO) { createDatabase(client, name) }
                            namespaces.loadDatabases(conn.id)
                        }
                    },
                    modifier = Modifier.padding(start = 14.dp, top = 2.dp, bottom = 6.dp),
                ) {
                    Text("+ Database", style = MaterialTheme.typography.labelSmall)
                }
            }
        }
    }

    pendingDrop?.let { target ->
        val (label, ns) = when (target) {
            is PendingDrop.Db -> "database" to target.db
            is PendingDrop.Coll -> "collection" to "${target.db}.${target.coll}"
        }
        io.mex.ui.components.ConfirmDangerDialog(
            title = "Drop $label?",
            text = "This will permanently drop \"$ns\" on the server. " +
                "All documents and indexes inside are deleted. This cannot be undone.",
            confirmLabel = "Drop $label",
            onConfirm = {
                val captured = target
                pendingDrop = null
                scope.launch {
                    val client = registry.client(captured.connectionId) ?: return@launch
                    when (captured) {
                        is PendingDrop.Db -> {
                            withContext(Dispatchers.IO) { dropDatabase(client, captured.db) }
                            namespaces.loadDatabases(captured.connectionId)
                        }
                        is PendingDrop.Coll -> {
                            withContext(Dispatchers.IO) {
                                dropCollection(client, captured.db, captured.coll)
                            }
                            namespaces.loadCollections(captured.connectionId, captured.db)
                        }
                    }
                }
            },
            onCancel = { pendingDrop = null },
        )
    }
}

private sealed class PendingDrop {
    abstract val connectionId: String
    data class Db(override val connectionId: String, val db: String) : PendingDrop()
    data class Coll(override val connectionId: String, val db: String, val coll: String) : PendingDrop()
}

@Composable
private fun ConnRow(name: String, selected: Boolean, onClick: () -> Unit) {
    val bg = if (selected) MaterialTheme.colorScheme.primary.copy(alpha = 0.12f) else MaterialTheme.colorScheme.surface
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(bg)
            .clickable(onClick = onClick)
            .padding(horizontal = 12.dp, vertical = 6.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Text(name, style = MaterialTheme.typography.bodyMedium, fontWeight = FontWeight.SemiBold)
    }
}

@Composable
private fun DbRow(
    db: DatabaseInfo,
    expanded: Boolean,
    selected: Boolean,
    onToggle: () -> Unit,
    onSelect: () -> Unit,
    onCreateColl: () -> Unit,
    onDrop: () -> Unit,
) {
    var menu by remember { mutableStateOf(false) }
    val bg = if (selected) MaterialTheme.colorScheme.primary.copy(alpha = 0.12f) else MaterialTheme.colorScheme.surface
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(bg)
            .pointerHoverIcon(PointerIcon.Default)
            .clickable(onClick = onSelect)
            .padding(start = 14.dp, end = 8.dp, top = 3.dp, bottom = 3.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        TextButton(
            onClick = onToggle,
            contentPadding = PaddingValues(0.dp),
            modifier = Modifier.size(20.dp),
        ) { Text(if (expanded) "▾" else "▸", style = MaterialTheme.typography.labelSmall) }
        Spacer(modifier = Modifier.width(4.dp))
        Text(db.name, style = MaterialTheme.typography.bodyMedium, modifier = Modifier.weight(1f))
        TextButton(onClick = { menu = true }, contentPadding = PaddingValues(0.dp), modifier = Modifier.size(24.dp)) {
            Text("⋯")
        }
        DropdownMenu(expanded = menu, onDismissRequest = { menu = false }) {
            DropdownMenuItem(text = { Text("Create collection…") }, onClick = { menu = false; onCreateColl() })
            DropdownMenuItem(text = { Text("Drop database") }, onClick = { menu = false; onDrop() })
        }
    }
}

@Composable
private fun CollRow(
    c: CollectionInfo,
    selected: Boolean,
    onSelect: () -> Unit,
    onDrop: () -> Unit,
) {
    var menu by remember { mutableStateOf(false) }
    val bg = if (selected) MaterialTheme.colorScheme.primary.copy(alpha = 0.12f) else MaterialTheme.colorScheme.surface
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(bg)
            .clickable(onClick = onSelect)
            .padding(start = 38.dp, end = 8.dp, top = 2.dp, bottom = 2.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Text(c.name, style = MaterialTheme.typography.bodySmall, modifier = Modifier.weight(1f))
        if (c.type != io.mex.mongo.CollectionType.collection) {
            Text(
                c.type.name,
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.tertiary,
            )
        }
        TextButton(onClick = { menu = true }, contentPadding = PaddingValues(0.dp), modifier = Modifier.size(24.dp)) {
            Text("⋯")
        }
        DropdownMenu(expanded = menu, onDismissRequest = { menu = false }) {
            DropdownMenuItem(text = { Text("Drop collection") }, onClick = { menu = false; onDrop() })
        }
    }
}

private fun singlePrompt(title: String, initial: String): String? {
    // Compose Desktop has no native prompt — fall back to javax.swing for now.
    return javax.swing.JOptionPane.showInputDialog(null, title, initial)
        ?.takeIf { it.isNotBlank() }
}
