package io.mex.ui

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.migration.MigrationRunner
import io.mex.mongo.ConnectionState
import io.mex.mongo.MongoRegistry
import io.mex.ui.cluster.ConnectionPanel
import io.mex.ui.coll.CollPanel
import io.mex.ui.connections.ConnectionsView
import io.mex.ui.connections.ConnectionsViewModel
import io.mex.ui.db.DbStatsPanel
import io.mex.ui.migration.MigrationsView
import io.mex.ui.query.QueryStore
import io.mex.ui.settings.SettingsView
import io.mex.ui.state.NamespacesStore
import io.mex.ui.state.PrefsStore
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
    val migrations = remember { MigrationRunner(ctx, registry) }
    val prefs = remember { PrefsStore(ctx.prefs) }
    val connectionsVm = remember { ConnectionsViewModel(ctx, registry) }

    val states by registry.states.collectAsState()
    LaunchedEffect(states) {
        for ((id, state) in states) {
            if (state !is ConnectionState.Connected) namespaces.reset(id)
        }
    }

    MexTheme(theme = prefs.theme) {
        Surface(
            modifier = Modifier.fillMaxSize(),
            color = MaterialTheme.colorScheme.background,
        ) {
            Row(modifier = Modifier.fillMaxSize()) {
                Surface(
                    modifier = Modifier.width(280.dp).fillMaxHeight(),
                    color = MaterialTheme.colorScheme.surface,
                ) {
                    Tree(ctx, registry, selection, namespaces, connectionsVm)
                }
                VerticalDivider()
                Box(modifier = Modifier.weight(1f).fillMaxHeight()) {
                    when (val s = selection.current) {
                        Selection.Welcome -> ConnectionsView(ctx, registry, connectionsVm, selection)
                        Selection.Migrations -> MigrationsView(ctx, registry, migrations)
                        Selection.Settings -> SettingsView(ctx, prefs)
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
