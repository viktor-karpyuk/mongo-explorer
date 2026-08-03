package io.mex.ui.components

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.width
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Warning
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Checkbox
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp

/**
 * Type-the-name-to-confirm dialog for actions whose consequence deserves more friction
 * than a click (PRV-UI-6). The destructive button stays disabled until [requiredText]
 * is typed verbatim. [checkboxLabel] adds an optional secondary decision whose state is
 * passed to [onConfirm].
 */
@Composable
fun TypedConfirmDialog(
    title: String,
    consequence: String,
    requiredText: String,
    confirmLabel: String,
    onConfirm: (checkboxChecked: Boolean) -> Unit,
    onCancel: () -> Unit,
    checkboxLabel: String? = null,
    checkboxDefault: Boolean = true,
    /** Extra decisions that belong inside the confirmation itself (e.g. PVC release). */
    extraContent: (@Composable () -> Unit)? = null,
) {
    var typed by remember { mutableStateOf("") }
    var checked by remember { mutableStateOf(checkboxDefault) }
    val match = typed == requiredText

    AlertDialog(
        onDismissRequest = onCancel,
        icon = {
            Icon(
                imageVector = Icons.Outlined.Warning,
                contentDescription = null,
                tint = MaterialTheme.colorScheme.error,
            )
        },
        title = { Text(title) },
        text = {
            Column(modifier = Modifier.width(440.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
                Text(consequence, style = MaterialTheme.typography.bodySmall)
                extraContent?.invoke()
                if (checkboxLabel != null) {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Checkbox(checked = checked, onCheckedChange = { checked = it })
                        Text(checkboxLabel, style = MaterialTheme.typography.bodySmall)
                    }
                }
                OutlinedTextField(
                    value = typed,
                    onValueChange = { typed = it },
                    label = { Text("Type \"$requiredText\" to confirm") },
                    singleLine = true,
                    isError = typed.isNotEmpty() && !match,
                    modifier = Modifier.fillMaxWidth(),
                    textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
                )
            }
        },
        confirmButton = {
            Button(
                enabled = match,
                onClick = { onConfirm(checked) },
                colors = ButtonDefaults.buttonColors(
                    containerColor = MaterialTheme.colorScheme.error,
                    contentColor = MaterialTheme.colorScheme.onError,
                ),
            ) { Text(confirmLabel) }
        },
        dismissButton = {
            TextButton(onClick = onCancel) { Text("Cancel") }
        },
    )
}
