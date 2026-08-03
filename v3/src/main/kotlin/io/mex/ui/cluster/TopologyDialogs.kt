package io.mex.ui.cluster

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.width
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp

/**
 * `rs.freeze()` — Tier-1 (reversible) so a plain confirm with the seconds spelled
 * out is enough friction. 0 seconds lifts an existing freeze.
 */
@Composable
fun FreezeDialog(host: String, onRun: (Int) -> Unit, onClose: () -> Unit) {
    var secsText by remember { mutableStateOf("120") }
    val secs = secsText.toIntOrNull()

    AlertDialog(
        onDismissRequest = onClose,
        title = { Text("Freeze elections") },
        text = {
            Column(modifier = Modifier.width(420.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
                Text(
                    "Prevents this member from seeking election for the given number of " +
                        "seconds — useful while doing maintenance on the rest of the set. " +
                        "Enter 0 to lift an existing freeze.",
                    style = MaterialTheme.typography.bodySmall,
                )
                Text(host, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall)
                OutlinedTextField(
                    value = secsText,
                    onValueChange = { secsText = it },
                    label = { Text("Seconds") },
                    singleLine = true,
                    isError = secs == null || secs < 0,
                    modifier = Modifier.fillMaxWidth(),
                )
            }
        },
        confirmButton = {
            Button(
                enabled = secs != null && secs >= 0,
                onClick = { onRun(secs!!) },
            ) { Text(if (secs == 0) "Unfreeze" else "Freeze") }
        },
        dismissButton = { TextButton(onClick = onClose) { Text("Cancel") } },
    )
}

/** Balancer pause/resume — reversible, so a plain confirm that names the consequence. */
@Composable
fun BalancerDialog(enable: Boolean, onConfirm: () -> Unit, onClose: () -> Unit) {
    AlertDialog(
        onDismissRequest = onClose,
        title = { Text(if (enable) "Resume balancer" else "Pause balancer") },
        text = {
            Text(
                if (enable) {
                    "Chunk migrations resume: the balancer will move chunks between shards " +
                        "whenever the distribution is uneven."
                } else {
                    "Chunk migrations stop until the balancer is resumed. Shards keep serving " +
                        "reads and writes, but an uneven data distribution will not self-correct."
                },
                style = MaterialTheme.typography.bodySmall,
            )
        },
        confirmButton = {
            Button(onClick = onConfirm) { Text(if (enable) "Resume" else "Pause") }
        },
        dismissButton = { TextButton(onClick = onClose) { Text("Cancel") } },
    )
}
