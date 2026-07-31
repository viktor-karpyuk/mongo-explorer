package io.mex.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.backup.BackupRunner
import io.mex.migration.MigrationRunner
import io.mex.mongo.ConnectionState
import io.mex.mongo.MongoRegistry
import io.mex.ui.backup.BackupsView
import io.mex.ui.cluster.ConnectionPanel
import io.mex.ui.compare.CompareView
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
    val backups = remember { BackupRunner(ctx, ctx.backupsDir) }
    val prefs = remember { PrefsStore(ctx.prefs) }
    val connectionsVm = remember { ConnectionsViewModel(ctx, registry) }

    val states by registry.states.collectAsState()
    LaunchedEffect(states) {
        for ((id, state) in states) {
            if (state !is ConnectionState.Connected) namespaces.reset(id)
        }
        // A panel bound to a closed connection can never load anything — it would sit on
        // "Loading…" forever — so send the user somewhere that still works.
        val bound = when (val s = selection.current) {
            is Selection.ConnectionView -> s.connectionId
            is Selection.Database -> s.connectionId
            is Selection.Collection -> s.connectionId
            else -> null
        }
        if (bound != null && states[bound] !is ConnectionState.Connected) {
            selection.select(Selection.Welcome)
        }
    }

    // Keeps the latency shown in the state pill honest rather than frozen at connect time.
    LaunchedEffect(Unit) {
        while (true) {
            kotlinx.coroutines.delay(30_000)
            for ((id, state) in registry.states.value) {
                if (state is ConnectionState.Connected) registry.refreshPing(id)
            }
        }
    }

    MexTheme(theme = prefs.theme) {
        Surface(
            modifier = Modifier.fillMaxSize(),
            color = MaterialTheme.colorScheme.background,
        ) {
            Column(modifier = Modifier.fillMaxSize()) {
                TopBar(
                    selection = selection,
                    connectedCount = states.values.count { it is ConnectionState.Connected },
                )
                HorizontalDivider()
                Row(modifier = Modifier.fillMaxWidth().weight(1f)) {
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
                            Selection.Backups -> BackupsView(ctx, registry, backups)
                            Selection.Compare -> CompareView(ctx, registry)
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
}

@Composable
private fun TopBar(selection: SelectionStore, connectedCount: Int) {
    val current = selection.current
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surface)
            .padding(horizontal = 16.dp, vertical = 8.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Text(
            "Mongo Explorer",
            style = MaterialTheme.typography.titleSmall,
            fontWeight = FontWeight.SemiBold,
        )
        Spacer(modifier = Modifier.width(24.dp))
        TopBarItem("Connections", current is Selection.Welcome) {
            selection.select(Selection.Welcome)
        }
        TopBarItem("Migrations", current is Selection.Migrations) {
            selection.select(Selection.Migrations)
        }
        TopBarItem("Backups", current is Selection.Backups) {
            selection.select(Selection.Backups)
        }
        TopBarItem("Compare", current is Selection.Compare) {
            selection.select(Selection.Compare)
        }
        TopBarItem("Settings", current is Selection.Settings) {
            selection.select(Selection.Settings)
        }
        Spacer(modifier = Modifier.weight(1f))
        if (connectedCount > 0) {
            Text(
                "$connectedCount cluster${if (connectedCount == 1) "" else "s"} connected",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
    }
}

@Composable
private fun TopBarItem(label: String, selected: Boolean, onClick: () -> Unit) {
    val bg = if (selected)
        MaterialTheme.colorScheme.primary.copy(alpha = 0.12f)
    else
        MaterialTheme.colorScheme.surface
    val color = if (selected)
        MaterialTheme.colorScheme.primary
    else
        MaterialTheme.colorScheme.onSurfaceVariant
    Box(
        modifier = Modifier
            .padding(horizontal = 4.dp)
            .background(bg, RoundedCornerShape(6.dp))
            .clickable(onClick = onClick)
            .padding(horizontal = 12.dp, vertical = 6.dp),
    ) {
        Text(
            label,
            style = MaterialTheme.typography.labelMedium,
            fontWeight = FontWeight.Medium,
            color = color,
        )
    }
}
