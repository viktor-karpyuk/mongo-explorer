package io.mex.ui.db

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.grid.GridCells
import androidx.compose.foundation.lazy.grid.LazyVerticalGrid
import androidx.compose.foundation.lazy.grid.items
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import io.mex.mongo.DbStats
import io.mex.mongo.MongoRegistry
import io.mex.mongo.dbStats
import io.mex.util.formatBytes
import io.mex.util.formatCount
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

@Composable
fun DbStatsPanel(connectionId: String, db: String, registry: MongoRegistry) {
    var stats by remember(connectionId, db) { mutableStateOf<DbStats?>(null) }
    var error by remember(connectionId, db) { mutableStateOf<String?>(null) }

    LaunchedEffect(connectionId, db) {
        val client = registry.client(connectionId) ?: return@LaunchedEffect
        try {
            stats = withContext(Dispatchers.IO) { dbStats(client, db) }
        } catch (e: Exception) {
            error = e.message
        }
    }

    Column(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Text(db, style = MaterialTheme.typography.headlineSmall)
        Spacer(modifier = Modifier.height(16.dp))

        error?.let { Text(it, color = MaterialTheme.colorScheme.error) }
        if (stats == null && error == null) Text("Loading…")

        stats?.let { s ->
            val items = listOf(
                "Collections" to formatCount(s.collections),
                "Views" to formatCount(s.views),
                "Documents" to formatCount(s.objects),
                "Avg doc size" to formatBytes(s.avgObjSize.toLong()),
                "Data size" to formatBytes(s.dataSize),
                "Storage size" to formatBytes(s.storageSize),
                "Indexes" to formatCount(s.indexes),
                "Index size" to formatBytes(s.indexSize),
            )
            LazyVerticalGrid(
                columns = GridCells.Adaptive(minSize = 200.dp),
                horizontalArrangement = Arrangement.spacedBy(10.dp),
                verticalArrangement = Arrangement.spacedBy(10.dp),
            ) {
                items(items) { (label, value) -> StatCard(label, value) }
            }
        }
    }
}

@Composable
private fun StatCard(label: String, value: String) {
    Card(border = CardDefaults.outlinedCardBorder()) {
        Column(modifier = Modifier.padding(12.dp)) {
            Text(
                label.uppercase(),
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            Spacer(modifier = Modifier.height(4.dp))
            Text(value, style = MaterialTheme.typography.headlineSmall)
        }
    }
}
