package io.mex.ui.provision

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.Checkbox
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.FilterChip
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.data.LabTopology
import io.mex.data.PreflightResult
import io.mex.provision.FOOTPRINT_WARN_MIB
import io.mex.provision.MONGO_TAGS
import io.mex.provision.dockerPreflight
import io.mex.provision.footprint
import io.mex.provision.plan
import io.mex.provision.validate
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * The New-lab wizard: presets are prefilled builder states, never a separate path
 * (PRV-TOPO-2); Create requires a valid topology, a unique name, and green preflight
 * (PRV-UI-2/3).
 */
@Composable
fun LabWizard(
    ctx: AppContext,
    onClose: () -> Unit,
    onCreate: (name: String, topology: LabTopology, mongoTag: String, auth: Boolean) -> Unit,
) {
    var name by remember { mutableStateOf("") }
    var tag by remember { mutableStateOf(MONGO_TAGS.first()) }
    var tagMenu by remember { mutableStateOf(false) }
    var shape by remember { mutableStateOf("replicaSet") } // standalone | replicaSet | sharded
    var rsMembers by remember { mutableStateOf(3) }
    var shards by remember { mutableStateOf(2) }
    var perShard by remember { mutableStateOf(3) }
    var mongos by remember { mutableStateOf(2) }
    var csrs by remember { mutableStateOf(3) }
    var auth by remember { mutableStateOf(true) }
    var preflight by remember { mutableStateOf<PreflightResult?>(null) }

    val topology = when (shape) {
        "standalone" -> LabTopology.Standalone
        "replicaSet" -> LabTopology.ReplicaSet(rsMembers)
        else -> LabTopology.Sharded(shards, perShard, mongos, csrs)
    }
    val violations = validate(topology)
    val f = footprint(topology)
    val nameTrim = name.trim()
    val nameTaken = nameTrim.isNotEmpty() && ctx.labs.nameExists(nameTrim)
    val portCount = remember(topology) {
        plan(labStub(topology, tag, auth)).clientServices.size
    }

    LaunchedEffect(portCount) {
        preflight = withContext(Dispatchers.IO) { dockerPreflight(portCount) }
    }

    val ready = violations.isEmpty() && nameTrim.isNotEmpty() && !nameTaken && preflight?.ok == true

    AlertDialog(
        onDismissRequest = onClose,
        title = { Text("New lab", style = MaterialTheme.typography.titleMedium) },
        confirmButton = {
            Button(enabled = ready, onClick = { onCreate(nameTrim, topology, tag, auth) }) { Text("Create") }
        },
        dismissButton = { TextButton(onClick = onClose) { Text("Cancel") } },
        text = {
            Column(modifier = Modifier.width(520.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalAlignment = Alignment.CenterVertically) {
                    OutlinedTextField(
                        value = name,
                        onValueChange = { name = it },
                        label = { Text("Name") },
                        singleLine = true,
                        isError = nameTaken,
                        modifier = Modifier.weight(1f),
                        textStyle = MaterialTheme.typography.bodySmall,
                    )
                    androidx.compose.foundation.layout.Box {
                        OutlinedButton(onClick = { tagMenu = true }) { Text("mongo $tag ⌄") }
                        DropdownMenu(expanded = tagMenu, onDismissRequest = { tagMenu = false }) {
                            MONGO_TAGS.forEach { t ->
                                DropdownMenuItem(text = { Text(t) }, onClick = { tagMenu = false; tag = t })
                            }
                        }
                    }
                }
                if (nameTaken) {
                    Text("A lab with this name already exists.", color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.labelSmall)
                }

                // Presets — just prefilled builder states (PRV-TOPO-2).
                Row(horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                    PresetChip("Standalone") { shape = "standalone" }
                    PresetChip("Replica set ×3") { shape = "replicaSet"; rsMembers = 3 }
                    PresetChip("Replica set ×5") { shape = "replicaSet"; rsMembers = 5 }
                    PresetChip("Sharded starter") {
                        shape = "sharded"; shards = 2; perShard = 3; mongos = 2; csrs = 3
                    }
                }

                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    listOf(
                        "standalone" to "Standalone",
                        "replicaSet" to "Replica set",
                        "sharded" to "Sharded",
                    ).forEach { (s, label) ->
                        FilterChip(selected = shape == s, onClick = { shape = s }, label = { Text(label, style = MaterialTheme.typography.labelMedium) })
                    }
                }

                when (shape) {
                    "standalone" -> Caption("One mongod, no replication.")
                    "replicaSet" -> {
                        ChipPicker("Members", listOf(1, 3, 5, 7), rsMembers) { rsMembers = it }
                        Caption("Production-like elections need 3+. Single-member still gives you an oplog.")
                    }
                    else -> {
                        Stepper("Shards", shards, 1, 6) { shards = it }
                        ChipPicker("Members per shard", listOf(1, 3), perShard) { perShard = it }
                        Stepper("Mongos routers", mongos, 1, 3) { mongos = it }
                        Caption("Two routers is the professional minimum — clients should never depend on one.")
                        ChipPicker("Config servers", listOf(1, 3), csrs) { csrs = it }
                        Caption("The CSRS holds cluster metadata; 3 is the production shape.")
                    }
                }
                violations.forEach {
                    Text(it, color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.labelSmall)
                }

                Row(verticalAlignment = Alignment.CenterVertically) {
                    Checkbox(checked = auth, onCheckedChange = { auth = it })
                    Text(
                        if (auth) "Authentication — keyfile between members, SCRAM for clients; a root user is created for you."
                        else "No auth — teaching only.",
                        style = MaterialTheme.typography.bodySmall,
                    )
                }

                Text(
                    buildString {
                        append("Footprint: ${f.containers} container${if (f.containers == 1) "" else "s"}")
                        append(" · ~%.1f GiB".format(f.estMemMiB / 1024.0))
                        append(" · ${f.volumes} volume${if (f.volumes == 1) "" else "s"}")
                        if (f.estMemMiB > FOOTPRINT_WARN_MIB) append("  ⚠ this is a lot for one machine")
                    },
                    style = MaterialTheme.typography.labelSmall,
                    color = if (f.estMemMiB > FOOTPRINT_WARN_MIB) Color(0xFFB45309) else MaterialTheme.colorScheme.onSurfaceVariant,
                    fontFamily = FontFamily.Monospace,
                )

                preflight?.let { r ->
                    Column(verticalArrangement = Arrangement.spacedBy(2.dp)) {
                        r.checks.forEach { c ->
                            val mark = if (c.ok) "✓" else "✗"
                            Text(
                                "$mark ${c.name}${c.detail?.let { " — $it" } ?: ""}",
                                style = MaterialTheme.typography.labelSmall,
                                fontFamily = FontFamily.Monospace,
                                color = if (c.ok) Color(0xFF4ADE80) else MaterialTheme.colorScheme.error,
                            )
                        }
                    }
                } ?: Text("Preflight…", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)

                Text(
                    "Labs run on your machine for development and testing.",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
        },
    )
}

/** plan() only reads topology/auth/id — a stub is enough for the port count. */
private fun labStub(topology: LabTopology, tag: String, auth: Boolean) = io.mex.data.Lab(
    id = "wizardpreview", name = "", topology = topology, status = io.mex.data.LabStatus.provisioning,
    mongoTag = tag, auth = auth, portMap = emptyMap(), connectionId = null, dir = "",
    appMajor = io.mex.data.LAB_APP_MAJOR, error = null, createdAt = 0L,
)

@Composable
private fun PresetChip(label: String, onClick: () -> Unit) {
    OutlinedButton(onClick = onClick, contentPadding = PaddingValues(horizontal = 10.dp, vertical = 4.dp)) {
        Text(label, style = MaterialTheme.typography.labelSmall)
    }
}

@Composable
private fun Caption(text: String) {
    Text(text, style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
}

@Composable
private fun Stepper(label: String, value: Int, min: Int, max: Int, onChange: (Int) -> Unit) {
    Row(verticalAlignment = Alignment.CenterVertically) {
        Text(label, style = MaterialTheme.typography.bodySmall, modifier = Modifier.width(150.dp))
        OutlinedButton(
            onClick = { onChange((value - 1).coerceAtLeast(min)) },
            enabled = value > min,
            contentPadding = PaddingValues(0.dp),
            modifier = Modifier.size(28.dp),
        ) { Text("−") }
        Text(
            "$value",
            style = MaterialTheme.typography.bodyMedium,
            fontFamily = FontFamily.Monospace,
            modifier = Modifier.width(36.dp),
            textAlign = androidx.compose.ui.text.style.TextAlign.Center,
        )
        OutlinedButton(
            onClick = { onChange((value + 1).coerceAtMost(max)) },
            enabled = value < max,
            contentPadding = PaddingValues(0.dp),
            modifier = Modifier.size(28.dp),
        ) { Text("+") }
    }
}

@Composable
private fun ChipPicker(label: String, options: List<Int>, selected: Int, onSelect: (Int) -> Unit) {
    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)) {
        Text(label, style = MaterialTheme.typography.bodySmall, modifier = Modifier.width(150.dp))
        options.forEach { o ->
            FilterChip(selected = selected == o, onClick = { onSelect(o) }, label = { Text("$o", style = MaterialTheme.typography.labelMedium) })
        }
        Spacer(modifier = Modifier.width(2.dp))
    }
}
