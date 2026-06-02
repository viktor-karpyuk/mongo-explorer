package io.mex.ui

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.mongo.MongoRegistry
import io.mex.mongo.ConnectionState
import io.mex.ui.coll.CollStatsPanel
import io.mex.ui.connections.ConnectionsView
import io.mex.ui.db.DbStatsPanel
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
                    border = MaterialTheme.shapes.small.let { null },
                ) {
                    Tree(ctx, registry, selection, namespaces)
                }
                VerticalDivider()
                Box(modifier = Modifier.weight(1f).fillMaxHeight()) {
                    when (val s = selection.current) {
                        Selection.Welcome -> ConnectionsView(ctx, registry)
                        Selection.Migrations -> PlaceholderPanel("Migrations")
                        Selection.Settings -> PlaceholderPanel("Settings")
                        is Selection.ConnectionView -> PlaceholderPanel("Cluster (Phase I)")
                        is Selection.Database -> DbStatsPanel(s.connectionId, s.db, registry)
                        is Selection.Collection -> CollStatsPanel(s.connectionId, s.db, s.collection, registry)
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
