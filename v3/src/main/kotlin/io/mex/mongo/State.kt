package io.mex.mongo

sealed class ConnectionState {
    object Disconnected : ConnectionState()
    object Connecting : ConnectionState()
    data class Connected(
        val pingMs: Int,
        val serverVersion: String,
        val topology: String,
        val connectedAt: Long,
    ) : ConnectionState()
    data class Error(val message: String) : ConnectionState()
}

data class TestResult(
    val ok: Boolean,
    val pingMs: Int? = null,
    val serverVersion: String? = null,
    val topology: String? = null,
    val error: String? = null,
)
