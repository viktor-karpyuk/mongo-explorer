package io.mex.ui.tree

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.data.ConnectionSummary
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

/**
 * One rendered line of the sidebar. The tree is flattened into these so it can live in a
 * LazyColumn — the previous nested `Column` composed every collection of every database
 * of every cluster on each state change.
 */
private sealed class TreeRow {
    abstract val key: String

    data class Conn(
        val conn: ConnectionSummary,
        val state: ConnectionState,
        val expanded: Boolean,
    ) : TreeRow() { override val key = "c:${conn.id}" }

    data class Db(
        val connId: String,
        val db: DatabaseInfo,
        val expanded: Boolean,
        val selected: Boolean,
    ) : TreeRow() { override val key = "d:$connId:${db.name}" }

    data class Coll(
        val connId: String,
        val db: String,
        val info: CollectionInfo,
        val selected: Boolean,
    ) : TreeRow() { override val key = "n:$connId:$db:${info.name}" }

    data class Note(val text: String, val indent: Int, override val key: String) : TreeRow()
}

@Composable
fun Tree(
    ctx: AppContext,
    registry: MongoRegistry,
    selection: SelectionStore,
    namespaces: NamespacesStore,
    vm: ConnectionsViewModel,
) {
    val states by registry.states.collectAsState()
    // Connections stay visible while connecting or after an error — hiding them made a
    // dropped cluster vanish from the sidebar with no explanation.
    val visible = vm.list.filter { states[it.id] !is ConnectionState.Disconnected && states[it.id] != null }
    val connectedIds = visible.filter { states[it.id] is ConnectionState.Connected }.map { it.id }

    var filter by remember { mutableStateOf("") }
    val current = selection.current
    val scope = rememberCoroutineScope()

    LaunchedEffect(connectedIds) {
        for (id in connectedIds) {
            if (namespaces.cache(id).databases.isEmpty()) namespaces.loadDatabases(id)
        }
    }
    // Filtering has to search collections the user never expanded, so pull them in once
    // a filter is actually typed rather than eagerly on connect.
    LaunchedEffect(filter, connectedIds) {
        if (filter.isNotBlank()) {
            for (id in connectedIds) namespaces.loadAllCollections(id)
        }
    }

    var sortAsc by remember { mutableStateOf(true) }
    var pendingDrop by remember { mutableStateOf<PendingDrop?>(null) }
    var creating by remember { mutableStateOf<CreateTarget?>(null) }

    val rows = buildRows(visible, states, namespaces, current, filter, sortAsc)

    Column(modifier = Modifier.fillMaxSize().background(MaterialTheme.colorScheme.surface)) {
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
                trailingIcon = {
                    if (filter.isNotEmpty()) {
                        TextButton(
                            onClick = { filter = "" },
                            contentPadding = PaddingValues(0.dp),
                            modifier = Modifier.size(24.dp),
                        ) { Text("✕", style = MaterialTheme.typography.labelSmall) }
                    }
                },
            )
            Spacer(modifier = Modifier.width(4.dp))
            OutlinedButton(
                onClick = { sortAsc = !sortAsc },
                contentPadding = PaddingValues(horizontal = 10.dp, vertical = 6.dp),
            ) {
                Text(if (sortAsc) "A↓" else "Z↑", style = MaterialTheme.typography.labelSmall)
            }
        }

        LazyColumn(state = rememberLazyListState(), modifier = Modifier.fillMaxSize()) {
            if (rows.isEmpty()) {
                item {
                    Text(
                        if (visible.isEmpty()) "Open a connection to browse."
                        else "Nothing matches \"$filter\".",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.padding(12.dp),
                    )
                }
            }
            items(rows.size, key = { rows[it].key }) { i ->
                when (val row = rows[i]) {
                    is TreeRow.Conn -> ConnRow(
                        name = row.conn.name,
                        state = row.state,
                        expanded = row.expanded,
                        selected = current is Selection.ConnectionView && current.connectionId == row.conn.id,
                        readOnly = row.conn.readOnly,
                        onToggle = { namespaces.toggleConnExpanded(row.conn.id) },
                        onSelect = { selection.select(Selection.ConnectionView(row.conn.id)) },
                        onDisconnect = { scope.launch { vm.close(row.conn.id) } },
                        onReconnect = { scope.launch { vm.open(row.conn.id) } },
                        onRefresh = { scope.launch { namespaces.refresh(row.conn.id) } },
                        onCreateDb = { creating = CreateTarget.Database(row.conn.id, row.conn.name) },
                    )
                    is TreeRow.Db -> DbRow(
                        db = row.db,
                        expanded = row.expanded,
                        selected = row.selected,
                        readOnly = vm.isReadOnly(row.connId),
                        onToggle = { scope.launch { namespaces.toggleExpanded(row.connId, row.db.name) } },
                        onSelect = { selection.select(Selection.Database(row.connId, row.db.name)) },
                        onCreateColl = { creating = CreateTarget.Collection(row.connId, row.db.name) },
                        onRefresh = { scope.launch { namespaces.loadCollections(row.connId, row.db.name) } },
                        onDrop = { pendingDrop = PendingDrop.Db(row.connId, row.db.name) },
                    )
                    is TreeRow.Coll -> CollRow(
                        c = row.info,
                        selected = row.selected,
                        readOnly = vm.isReadOnly(row.connId),
                        onSelect = { selection.select(Selection.Collection(row.connId, row.db, row.info.name)) },
                        onDrop = { pendingDrop = PendingDrop.Coll(row.connId, row.db, row.info.name) },
                    )
                    is TreeRow.Note -> Text(
                        row.text,
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.padding(start = row.indent.dp, top = 2.dp, bottom = 2.dp),
                    )
                }
            }
        }
    }

    creating?.let { target ->
        CreateNamespaceDialog(
            target = target,
            onClose = { creating = null },
            onCreate = { db, coll ->
                val client = registry.client(target.connectionId)
                if (client == null) {
                    "Not connected."
                } else {
                    val result = withContext(Dispatchers.IO) {
                        runCatching {
                            when (target) {
                                is CreateTarget.Database -> createDatabase(client, db, coll)
                                is CreateTarget.Collection ->
                                    createCollection(client, db, CreateCollectionInput(coll))
                            }
                        }
                    }
                    result.fold(
                        onSuccess = {
                            namespaces.loadDatabases(target.connectionId)
                            namespaces.expand(target.connectionId, db)
                            selection.select(Selection.Collection(target.connectionId, db, coll))
                            null
                        },
                        onFailure = { it.message ?: it::class.java.simpleName },
                    )
                }
            },
        )
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

@Composable
private fun buildRows(
    connections: List<ConnectionSummary>,
    states: Map<String, ConnectionState>,
    namespaces: NamespacesStore,
    current: Selection,
    filter: String,
    sortAsc: Boolean,
): List<TreeRow> {
    val rows = mutableListOf<TreeRow>()
    for (conn in connections) {
        val state = states[conn.id] ?: ConnectionState.Disconnected
        val cache = namespaces.cache(conn.id)
        rows += TreeRow.Conn(conn, state, cache.connExpanded)
        if (!cache.connExpanded) continue

        if (state is ConnectionState.Error) {
            rows += TreeRow.Note(state.message, 24, "e:${conn.id}")
            continue
        }
        if (state is ConnectionState.Connecting) {
            rows += TreeRow.Note("connecting…", 24, "g:${conn.id}")
            continue
        }
        if (cache.loading && cache.databases.isEmpty()) {
            rows += TreeRow.Note("loading databases…", 24, "l:${conn.id}")
            continue
        }

        val expandedDbs = cache.expanded.value
        val dbs = cache.databases
            .sortedBy { it.name.lowercase() }
            .let { if (sortAsc) it else it.asReversed() }

        for (db in dbs) {
            val colls = cache.collections[db.name].orEmpty()
            val matchingColls = colls
                .filter { filter.isBlank() || it.name.contains(filter, ignoreCase = true) }
                .sortedBy { it.name.lowercase() }
                .let { if (sortAsc) it else it.asReversed() }
            val dbMatches = filter.isBlank() || db.name.contains(filter, ignoreCase = true)
            // A database whose name doesn't match stays visible when one of its
            // collections does, otherwise the match would be unreachable.
            if (!dbMatches && matchingColls.isEmpty()) continue

            // An active filter force-opens matching databases so hits are visible.
            val expanded = expandedDbs.contains(db.name) || (filter.isNotBlank() && matchingColls.isNotEmpty())
            rows += TreeRow.Db(
                connId = conn.id,
                db = db,
                expanded = expanded,
                selected = current is Selection.Database &&
                    current.connectionId == conn.id && current.db == db.name,
            )
            if (!expanded) continue
            if (cache.collectionsLoading[db.name] == true) {
                rows += TreeRow.Note("loading…", 38, "cl:${conn.id}:${db.name}")
            }
            val shown = if (dbMatches && filter.isNotBlank() && matchingColls.isEmpty()) {
                colls.sortedBy { it.name.lowercase() }.let { if (sortAsc) it else it.asReversed() }
            } else {
                matchingColls
            }
            for (c in shown) {
                rows += TreeRow.Coll(
                    connId = conn.id,
                    db = db.name,
                    info = c,
                    selected = current is Selection.Collection &&
                        current.connectionId == conn.id &&
                        current.db == db.name &&
                        current.collection == c.name,
                )
            }
        }
    }
    return rows
}

private sealed class PendingDrop {
    abstract val connectionId: String
    data class Db(override val connectionId: String, val db: String) : PendingDrop()
    data class Coll(override val connectionId: String, val db: String, val coll: String) : PendingDrop()
}

@Composable
private fun ConnRow(
    name: String,
    state: ConnectionState,
    expanded: Boolean,
    selected: Boolean,
    readOnly: Boolean,
    onToggle: () -> Unit,
    onSelect: () -> Unit,
    onDisconnect: () -> Unit,
    onReconnect: () -> Unit,
    onRefresh: () -> Unit,
    onCreateDb: () -> Unit,
) {
    var menu by remember { mutableStateOf(false) }
    val connected = state is ConnectionState.Connected
    val dot = when (state) {
        is ConnectionState.Connected -> Color(0xFF4ADE80)
        is ConnectionState.Connecting -> Color(0xFFFACC15)
        is ConnectionState.Error -> MaterialTheme.colorScheme.error
        is ConnectionState.Disconnected -> MaterialTheme.colorScheme.outline
    }
    val bg = if (selected) MaterialTheme.colorScheme.primary.copy(alpha = 0.12f) else MaterialTheme.colorScheme.surface
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(bg)
            .clickable(onClick = onSelect)
            .padding(start = 4.dp, end = 8.dp, top = 6.dp, bottom = 6.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        TextButton(
            onClick = onToggle,
            contentPadding = PaddingValues(0.dp),
            modifier = Modifier.size(22.dp),
        ) { Text(if (expanded) "▾" else "▸", style = MaterialTheme.typography.labelSmall) }
        Box(modifier = Modifier.size(6.dp).background(dot, CircleShape))
        Spacer(modifier = Modifier.width(6.dp))
        Text(
            name,
            style = MaterialTheme.typography.bodyMedium,
            fontWeight = FontWeight.SemiBold,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis,
            modifier = Modifier.weight(1f),
        )
        if (readOnly) {
            io.mex.ui.components.ReadOnlyBadge(compact = true)
            Spacer(modifier = Modifier.width(4.dp))
        }
        TextButton(onClick = { menu = true }, contentPadding = PaddingValues(0.dp), modifier = Modifier.size(24.dp)) {
            Text("⋯")
        }
        DropdownMenu(expanded = menu, onDismissRequest = { menu = false }) {
            if (connected) {
                DropdownMenuItem(
                    text = { Text("Refresh") },
                    onClick = { menu = false; onRefresh() },
                )
                if (!readOnly) {
                    DropdownMenuItem(
                        text = { Text("Create database…") },
                        onClick = { menu = false; onCreateDb() },
                    )
                }
                HorizontalDivider()
                DropdownMenuItem(
                    text = { Text("Disconnect", color = MaterialTheme.colorScheme.error) },
                    onClick = { menu = false; onDisconnect() },
                )
            } else {
                DropdownMenuItem(
                    text = { Text("Reconnect") },
                    onClick = { menu = false; onReconnect() },
                )
                DropdownMenuItem(
                    text = { Text("Close") },
                    onClick = { menu = false; onDisconnect() },
                )
            }
        }
    }
}

@Composable
private fun DbRow(
    db: DatabaseInfo,
    expanded: Boolean,
    selected: Boolean,
    readOnly: Boolean,
    onToggle: () -> Unit,
    onSelect: () -> Unit,
    onCreateColl: () -> Unit,
    onRefresh: () -> Unit,
    onDrop: () -> Unit,
) {
    var menu by remember { mutableStateOf(false) }
    val bg = if (selected) MaterialTheme.colorScheme.primary.copy(alpha = 0.12f) else MaterialTheme.colorScheme.surface
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(bg)
            .clickable(onClick = onSelect)
            .padding(start = 14.dp, end = 8.dp, top = 3.dp, bottom = 3.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        TextButton(
            onClick = onToggle,
            contentPadding = PaddingValues(0.dp),
            modifier = Modifier.size(22.dp),
        ) { Text(if (expanded) "▾" else "▸", style = MaterialTheme.typography.labelSmall) }
        Spacer(modifier = Modifier.width(4.dp))
        Text(
            db.name,
            style = MaterialTheme.typography.bodyMedium,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis,
            modifier = Modifier.weight(1f),
        )
        TextButton(onClick = { menu = true }, contentPadding = PaddingValues(0.dp), modifier = Modifier.size(24.dp)) {
            Text("⋯")
        }
        DropdownMenu(expanded = menu, onDismissRequest = { menu = false }) {
            if (!readOnly) {
                DropdownMenuItem(text = { Text("Create collection…") }, onClick = { menu = false; onCreateColl() })
            }
            DropdownMenuItem(text = { Text("Refresh collections") }, onClick = { menu = false; onRefresh() })
            if (!readOnly) {
                HorizontalDivider()
                DropdownMenuItem(
                    text = { Text("Drop database", color = MaterialTheme.colorScheme.error) },
                    onClick = { menu = false; onDrop() },
                )
            }
        }
    }
}

@Composable
private fun CollRow(
    c: CollectionInfo,
    selected: Boolean,
    readOnly: Boolean,
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
        Text(
            c.name,
            style = MaterialTheme.typography.bodySmall,
            fontFamily = FontFamily.Monospace,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis,
            modifier = Modifier.weight(1f),
        )
        if (c.type != io.mex.mongo.CollectionType.collection) {
            Text(
                c.type.name,
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.tertiary,
            )
        }
        // Drop is the only action here, so a read-only connection gets no menu at all.
        if (!readOnly) {
            TextButton(onClick = { menu = true }, contentPadding = PaddingValues(0.dp), modifier = Modifier.size(24.dp)) {
                Text("⋯")
            }
            DropdownMenu(expanded = menu, onDismissRequest = { menu = false }) {
                DropdownMenuItem(
                    text = { Text("Drop collection", color = MaterialTheme.colorScheme.error) },
                    onClick = { menu = false; onDrop() },
                )
            }
        }
    }
}
