package io.mex.ui.state

import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateMapOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import io.mex.mongo.CollectionInfo
import io.mex.mongo.DatabaseInfo
import io.mex.mongo.MongoRegistry
import io.mex.mongo.listCollections
import io.mex.mongo.listDatabases
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

class NsCache {
    var loading by mutableStateOf(false)
    var databases by mutableStateOf<List<DatabaseInfo>>(emptyList())
    val collections = mutableStateMapOf<String, List<CollectionInfo>>()
    val collectionsLoading = mutableStateMapOf<String, Boolean>()
    val expanded = mutableStateOf<Set<String>>(emptySet())

    /** Whether the whole cluster subtree is shown. Collapsing folds every database away. */
    var connExpanded by mutableStateOf(true)
}

class NamespacesStore(private val registry: MongoRegistry) {
    private val _byConnection = mutableStateMapOf<String, NsCache>()
    val byConnection: Map<String, NsCache> get() = _byConnection

    fun cache(connectionId: String): NsCache =
        _byConnection.getOrPut(connectionId) { NsCache() }

    fun toggleConnExpanded(connectionId: String) {
        val c = cache(connectionId)
        c.connExpanded = !c.connExpanded
    }

    suspend fun loadDatabases(connectionId: String) {
        val client = registry.client(connectionId) ?: return
        val cache = cache(connectionId)
        cache.loading = true
        try {
            cache.databases = withContext(Dispatchers.IO) { listDatabases(client) }
        } finally {
            cache.loading = false
        }
    }

    suspend fun loadCollections(connectionId: String, db: String) {
        val client = registry.client(connectionId) ?: return
        val cache = cache(connectionId)
        cache.collectionsLoading[db] = true
        try {
            cache.collections[db] = withContext(Dispatchers.IO) { listCollections(client, db) }
        } finally {
            cache.collectionsLoading[db] = false
        }
    }

    /**
     * Loads collections for every database that hasn't been listed yet.
     *
     * The sidebar filter matches against loaded rows only, so without this a search for a
     * collection name silently misses every database the user never expanded.
     */
    suspend fun loadAllCollections(connectionId: String) {
        val cache = cache(connectionId)
        for (db in cache.databases.map { it.name }) {
            if (cache.collections[db] == null && cache.collectionsLoading[db] != true) {
                loadCollections(connectionId, db)
            }
        }
    }

    /** Re-reads databases and every already-loaded collection list from the server. */
    suspend fun refresh(connectionId: String) {
        val cache = cache(connectionId)
        val known = cache.collections.keys.toList()
        loadDatabases(connectionId)
        val live = cache.databases.map { it.name }.toSet()
        known.filter { it !in live }.forEach {
            cache.collections.remove(it)
            cache.expanded.value = cache.expanded.value - it
        }
        for (db in known.filter { it in live }) loadCollections(connectionId, db)
    }

    /** Idempotent expand — used after creating a namespace so the new row is visible. */
    suspend fun expand(connectionId: String, db: String) {
        val cache = cache(connectionId)
        cache.expanded.value = cache.expanded.value + db
        loadCollections(connectionId, db)
    }

    suspend fun toggleExpanded(connectionId: String, db: String) {
        val cache = cache(connectionId)
        val current = cache.expanded.value
        cache.expanded.value = if (current.contains(db)) current - db else current + db
        if (db !in current && cache.collections[db] == null) loadCollections(connectionId, db)
    }

    fun reset(connectionId: String) {
        _byConnection.remove(connectionId)
    }
}
