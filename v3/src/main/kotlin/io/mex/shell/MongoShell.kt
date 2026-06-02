package io.mex.shell

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow
import java.io.BufferedReader
import java.io.InputStreamReader

class MongoShellSession(private val uri: String) : AutoCloseable {
    private var process: Process? = null
    private var writer: java.io.BufferedWriter? = null
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    private val _events = MutableSharedFlow<ShellEvent>(extraBufferCapacity = 256)
    val events: SharedFlow<ShellEvent> = _events

    fun start(): Boolean {
        return runCatching {
            val pb = ProcessBuilder("mongosh", "--quiet", "--norc", uri)
                .redirectErrorStream(false)
            val p = pb.start()
            process = p
            writer = p.outputWriter()
            scope.launch { pump(p.inputStream.bufferedReader(), false) }
            scope.launch { pump(p.errorStream.bufferedReader(), true) }
            scope.launch {
                val code = p.waitFor()
                _events.tryEmit(ShellEvent.Exit(code))
            }
            true
        }.getOrElse {
            _events.tryEmit(ShellEvent.Output("Failed to spawn mongosh: ${it.message}\n", true))
            _events.tryEmit(ShellEvent.Exit(127))
            false
        }
    }

    fun send(line: String) {
        val w = writer ?: return
        w.write(if (line.endsWith("\n")) line else "$line\n")
        w.flush()
    }

    override fun close() {
        process?.destroy()
        scope.cancel()
    }

    private suspend fun pump(reader: BufferedReader, isErr: Boolean) = withContext(Dispatchers.IO) {
        try {
            val buf = CharArray(2048)
            while (true) {
                val n = reader.read(buf)
                if (n <= 0) break
                _events.emit(ShellEvent.Output(String(buf, 0, n), isErr))
            }
        } catch (_: Exception) {
            /* stream closed */
        }
    }
}

sealed class ShellEvent {
    data class Output(val text: String, val isErr: Boolean) : ShellEvent()
    data class Exit(val code: Int) : ShellEvent()
}
