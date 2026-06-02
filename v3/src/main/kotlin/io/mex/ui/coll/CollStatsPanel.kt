package io.mex.ui.coll

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.grid.GridCells
import androidx.compose.foundation.lazy.grid.LazyVerticalGrid
import androidx.compose.foundation.lazy.grid.items
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import io.mex.mongo.CollStats
import io.mex.mongo.MongoRegistry
import io.mex.mongo.collStats
import io.mex.util.formatBytes
import io.mex.util.formatCount
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

@Composable
fun CollStatsPanel(connectionId: String, db: String, collection: String, registry: MongoRegistry) {
    var stats by remember(connectionId, db, collection) { mutableStateOf<CollStats?>(null) }
    var error by remember(connectionId, db, collection) { mutableStateOf<String?>(null) }

    LaunchedEffect(connectionId, db, collection) {
        val client = registry.client(connectionId) ?: return@LaunchedEffect
        try {
            stats = withContext(Dispatchers.IO) { collStats(client, db, collection) }
        } catch (e: Exception) {
            error = e.message
        }
    }

    Column(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Text("$db › $collection", style = MaterialTheme.typography.headlineSmall)
        Spacer(modifier = Modifier.height(16.dp))
        error?.let { Text(it, color = MaterialTheme.colorScheme.error) }
        if (stats == null && error == null) Text("Loading…")
        stats?.let { s ->
            val items = listOf(
                "Documents" to formatCount(s.count),
                "Avg doc size" to formatBytes(s.avgObjSize.toLong()),
                "Data size" to formatBytes(s.size),
                "Storage size" to formatBytes(s.storageSize),
                "Indexes" to formatCount(s.nindexes),
                "Total index size" to formatBytes(s.totalIndexSize),
                "Capped" to if (s.capped) "yes" else "no",
                "Namespace" to s.ns,
            )
            LazyVerticalGrid(
                columns = GridCells.Adaptive(minSize = 200.dp),
                horizontalArrangement = Arrangement.spacedBy(10.dp),
                verticalArrangement = Arrangement.spacedBy(10.dp),
            ) {
                items(items) { (label, value) ->
                    Card(border = CardDefaults.outlinedCardBorder()) {
                        Column(modifier = Modifier.padding(12.dp)) {
                            Text(
                                label.uppercase(),
                                style = MaterialTheme.typography.labelSmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                            )
                            Spacer(modifier = Modifier.height(4.dp))
                            Text(value, style = MaterialTheme.typography.titleMedium)
                        }
                    }
                }
            }
        }
    }
}
