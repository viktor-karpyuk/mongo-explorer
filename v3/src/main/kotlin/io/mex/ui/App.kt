package io.mex.ui

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.mongo.ConnectionState
import io.mex.mongo.MongoRegistry
import io.mex.ui.cluster.ConnectionPanel
import io.mex.ui.coll.CollPanel
import io.mex.ui.connections.ConnectionsView
import io.mex.ui.db.DbStatsPanel
import io.mex.ui.query.QueryStore
import io.mex.ui.state.NamespacesStore
import io.mex.ui.state.Selection
import io.mex.ui.state.SelectionStore
import io.mex.ui.theme.MexTheme
import io.mex.ui.tree.Tree

@Composable
fun App(ctx: AppContext) {
    val registry = remember { MongoRegistry() }
    val selection = remember { SelectionStore() }
    val namespaces = remember { NamespacesStore(registry) }
    val queries = remember { QueryStore(ctx, registry) }

    // Auto-reset cached namespaces when a connection drops.
    val states by registry.states.collectAsState()
    LaunchedEffect(states) {
        for ((id, state) in states) {
            if (state !is ConnectionState.Connected) namespaces.reset(id)
        }
    }

    MexTheme {
        Surface(
            modifier = Modifier.fillMaxSize(),
            color = MaterialTheme.colorScheme.background,
        ) {
            Row(modifier = Modifier.fillMaxSize()) {
                Surface(
                    modifier = Modifier.width(280.dp).fillMaxHeight(),
                    color = MaterialTheme.colorScheme.surface,
                ) {
                    Tree(ctx, registry, selection, namespaces)
                }
                VerticalDivider()
                Box(modifier = Modifier.weight(1f).fillMaxHeight()) {
                    when (val s = selection.current) {
                        Selection.Welcome -> ConnectionsView(ctx, registry)
                        Selection.Migrations -> PlaceholderPanel("Migrations")
                        Selection.Settings -> PlaceholderPanel("Settings")
                        is Selection.ConnectionView -> ConnectionPanel(ctx, s.connectionId, registry)
                        is Selection.Database -> DbStatsPanel(s.connectionId, s.db, registry)
                        is Selection.Collection -> CollPanel(
                            ctx = ctx,
                            registry = registry,
                            queries = queries,
                            connectionId = s.connectionId,
                            db = s.db,
                            collection = s.collection,
                        )
                    }
                }
            }
        }
    }
}

@Composable
private fun PlaceholderPanel(label: String) {
    Column(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Text(label, style = MaterialTheme.typography.headlineSmall)
        Spacer(modifier = Modifier.height(8.dp))
        Text("Phase pending.", color = MaterialTheme.colorScheme.onSurfaceVariant)
    }
}
