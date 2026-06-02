package io.mex.ui.coll

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.mongo.MongoRegistry
import io.mex.ui.query.QueryStore
import io.mex.ui.query.QueryView

enum class CollTab { Query, Stats }

@Composable
fun CollPanel(
    ctx: AppContext,
    registry: MongoRegistry,
    queries: QueryStore,
    connectionId: String,
    db: String,
    collection: String,
) {
    var tab by remember(connectionId, db, collection) { mutableStateOf(CollTab.Query) }

    Column(modifier = Modifier.fillMaxSize()) {
        Row(
            modifier = Modifier.fillMaxWidth().padding(horizontal = 12.dp, vertical = 4.dp),
        ) {
            TabBtn("Find", tab == CollTab.Query) { tab = CollTab.Query }
            TabBtn("Stats", tab == CollTab.Stats) { tab = CollTab.Stats }
        }
        HorizontalDivider()
        Box(modifier = Modifier.weight(1f)) {
            when (tab) {
                CollTab.Query -> QueryView(ctx, registry, connectionId, db, collection, queries)
                CollTab.Stats -> CollStatsPanel(connectionId, db, collection, registry)
            }
        }
    }
}

@Composable
private fun TabBtn(label: String, active: Boolean, onClick: () -> Unit) {
    val color = if (active) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant
    TextButton(onClick = onClick) {
        Text(label, color = color, style = MaterialTheme.typography.labelMedium)
    }
}
