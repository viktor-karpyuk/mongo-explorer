package io.mex.ui.coll

import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.mongo.MongoRegistry
import io.mex.ui.aggregate.AggregationView
import io.mex.ui.indexes.IndexesPanel
import io.mex.ui.query.QueryStore
import io.mex.ui.query.QueryView
import io.mex.ui.schema.SchemaPanel
import io.mex.ui.validator.ValidatorPanel

enum class CollTab { Query, Aggregate, Schema, Indexes, Validator, Stats }

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
            TabBtn("Aggregate", tab == CollTab.Aggregate) { tab = CollTab.Aggregate }
            TabBtn("Schema", tab == CollTab.Schema) { tab = CollTab.Schema }
            TabBtn("Indexes", tab == CollTab.Indexes) { tab = CollTab.Indexes }
            TabBtn("Validator", tab == CollTab.Validator) { tab = CollTab.Validator }
            TabBtn("Stats", tab == CollTab.Stats) { tab = CollTab.Stats }
        }
        HorizontalDivider()
        Box(modifier = Modifier.weight(1f)) {
            when (tab) {
                CollTab.Query -> QueryView(ctx, registry, connectionId, db, collection, queries)
                CollTab.Aggregate -> AggregationView(ctx, registry, connectionId, db, collection)
                CollTab.Schema -> SchemaPanel(connectionId, db, collection, registry)
                CollTab.Indexes -> IndexesPanel(connectionId, db, collection, registry)
                CollTab.Validator -> ValidatorPanel(connectionId, db, collection, registry)
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
