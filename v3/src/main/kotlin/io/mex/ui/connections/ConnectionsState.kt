package io.mex.ui.connections

import androidx.compose.runtime.*
import io.mex.AppContext
import io.mex.data.ConnectionInput
import io.mex.data.ConnectionSummary
import io.mex.data.UriHistoryEntry
import io.mex.mongo.ConnectionState
import io.mex.mongo.MongoRegistry
import io.mex.mongo.TestResult
import kotlinx.coroutines.flow.StateFlow

class ConnectionsViewModel(
    private val ctx: AppContext,
    val registry: MongoRegistry,
) {
    private var _list by mutableStateOf<List<ConnectionSummary>>(emptyList())
    val list: List<ConnectionSummary> get() = _list

    val states: StateFlow<Map<String, ConnectionState>> get() = registry.states

    private var _history by mutableStateOf<List<UriHistoryEntry>>(emptyList())
    val history: List<UriHistoryEntry> get() = _history

    init { reload() }

    fun reload() {
        _list = ctx.connections.list()
        _history = ctx.uriHistory.list()
    }

    fun create(input: ConnectionInput) {
        ctx.connections.create(input)
        ctx.uriHistory.add(input.uri)
        reload()
    }

    fun update(id: String, input: ConnectionInput) {
        ctx.connections.update(id, input)
        ctx.uriHistory.add(input.uri)
        reload()
    }

    /** Closes the live client first — deleting the record alone leaked the MongoClient. */
    suspend fun delete(id: String) {
        registry.disconnect(id)
        ctx.connections.delete(id)
        reload()
    }

    fun duplicate(id: String) {
        val record = ctx.connections.get(id) ?: return
        ctx.connections.create(ConnectionInput("${record.name} (copy)", record.uri, record.notes))
        reload()
    }

    suspend fun open(id: String): ConnectionState {
        val record = ctx.connections.get(id) ?: return ConnectionState.Error("Connection not found")
        val state = registry.connect(record)
        if (state is ConnectionState.Connected) {
            ctx.connections.touchLastUsed(id)
            reload()
        }
        return state
    }

    suspend fun close(id: String) = registry.disconnect(id)

    suspend fun test(uri: String): TestResult = registry.test(uri)
}

@Composable
fun rememberConnectionsViewModel(ctx: AppContext, registry: MongoRegistry): ConnectionsViewModel =
    remember(ctx, registry) { ConnectionsViewModel(ctx, registry) }
