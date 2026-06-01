package io.mex.mongo

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import io.mex.data.ConnectionRecord
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.withContext
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class MongoRegistry {
    private val clients = ConcurrentHashMap<String, MongoClient>()
    private val _states = MutableStateFlow<Map<String, ConnectionState>>(emptyMap())
    val states: StateFlow<Map<String, ConnectionState>> = _states.asStateFlow()

    fun state(id: String): ConnectionState = _states.value[id] ?: ConnectionState.Disconnected

    fun client(id: String): MongoClient? = clients[id]

    suspend fun connect(record: ConnectionRecord): ConnectionState =
        withContext(Dispatchers.IO) {
            clients[record.id]?.let { return@withContext state(record.id) }
            setState(record.id, ConnectionState.Connecting)
            try {
                val client = buildClient(record.uri)
                val info = probeServer(client)
                clients[record.id] = client
                val state = ConnectionState.Connected(
                    pingMs = info.pingMs,
                    serverVersion = info.version,
                    topology = info.topology,
                    connectedAt = System.currentTimeMillis(),
                )
                setState(record.id, state)
                state
            } catch (e: Exception) {
                ConnectionState.Error(e.message ?: e::class.java.simpleName).also {
                    setState(record.id, it)
                }
            }
        }

    suspend fun disconnect(id: String): Unit = withContext(Dispatchers.IO) {
        clients.remove(id)?.runCatching { close() }
        setState(id, ConnectionState.Disconnected)
    }

    suspend fun disconnectAll(): Unit = withContext(Dispatchers.IO) {
        clients.keys.toList().forEach { disconnect(it) }
    }

    suspend fun test(uri: String): TestResult = withContext(Dispatchers.IO) {
        var client: MongoClient? = null
        try {
            client = buildClient(uri)
            val info = probeServer(client)
            TestResult(
                ok = true,
                pingMs = info.pingMs,
                serverVersion = info.version,
                topology = info.topology,
            )
        } catch (e: Exception) {
            TestResult(ok = false, error = e.message ?: e::class.java.simpleName)
        } finally {
            client?.runCatching { close() }
        }
    }

    private fun buildClient(uri: String): MongoClient {
        val settings = MongoClientSettings.builder()
            .applicationName("mongo-explorer-v3")
            .applyConnectionString(ConnectionString(uri))
            .applyToClusterSettings { it.serverSelectionTimeout(8, TimeUnit.SECONDS) }
            .applyToSocketSettings { it.connectTimeout(8, TimeUnit.SECONDS) }
            .build()
        return MongoClients.create(settings)
    }

    private fun setState(id: String, state: ConnectionState) {
        _states.update { it + (id to state) }
    }
}
