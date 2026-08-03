package io.mex.ui.provision

import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateMapOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import io.mex.data.LabStatus
import io.mex.data.ProvisionPhase
import io.mex.provision.LabEvent
import io.mex.provision.ProvisionRunner
import io.mex.ui.connections.ConnectionsViewModel

const val LOG_TAIL = 400

/**
 * App-lifetime holder for provisioning events. Provisions run for minutes; if the only
 * collector lived inside ProvisionView, navigating to another tab would drop the Done
 * event — losing the show-once root password and leaving the sidebar without the new
 * connection until an unrelated reload. Bound once from App, it outlives view switches.
 */
class ProvisionUiState(val runner: ProvisionRunner) {
    val logs = mutableStateMapOf<String, List<String>>()
    val phases = mutableStateMapOf<String, ProvisionPhase>()

    /** Held until the result banner is actually displayed and dismissed (PRV-SEC-3). */
    var pendingResult by mutableStateOf<LabEvent.Done?>(null)
        private set

    /** Bumped on every terminal event so views reload their lab list. */
    var epoch by mutableStateOf(0)
        private set

    fun consumeResult() {
        pendingResult = null
    }

    /** Collect on the composition's main dispatcher — state maps are not thread-safe. */
    suspend fun bind(connectionsVm: ConnectionsViewModel) {
        runner.events.collect { ev ->
            when (ev) {
                is LabEvent.Phase -> phases[ev.labId] = ev.phase
                is LabEvent.Log ->
                    logs[ev.labId] = (logs[ev.labId].orEmpty() + ev.line).takeLast(LOG_TAIL)
                is LabEvent.Done -> {
                    phases.remove(ev.labId)
                    epoch++
                    connectionsVm.reload()
                    if (ev.status == LabStatus.running && ev.uri != null) pendingResult = ev
                }
            }
        }
    }
}
