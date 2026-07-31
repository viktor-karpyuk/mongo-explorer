package io.mex.ui.security

import androidx.compose.foundation.VerticalScrollbar
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.rememberScrollbarAdapter
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.unit.dp
import io.mex.mongo.CheckLevel
import io.mex.mongo.MongoRegistry
import io.mex.mongo.SecurityCheck
import io.mex.mongo.evaluateSecurity
import io.mex.mongo.gatherSecurityFacts
import io.mex.mongo.securityScore
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

private fun levelColor(level: CheckLevel): Color = when (level) {
    CheckLevel.pass -> Color(0xFF4ADE80)
    CheckLevel.warn -> Color(0xFFFBBF24)
    CheckLevel.fail -> Color(0xFFF87171)
    CheckLevel.info -> Color(0xFF94A3B8)
}

/**
 * CIS-style read-only security posture check (DBA-CIS-1). Not a full benchmark — covers the
 * high-impact settings observable without host access: auth, TLS, network binding, version
 * currency, superuser sprawl, server-side JS, cluster auth, audit.
 */
@Composable
fun AuditTab(connectionId: String, registry: MongoRegistry) {
    var checks by remember(connectionId) { mutableStateOf<List<SecurityCheck>>(emptyList()) }
    var score by remember(connectionId) { mutableStateOf(100) }
    var loading by remember(connectionId) { mutableStateOf(false) }
    var error by remember(connectionId) { mutableStateOf<String?>(null) }
    val scope = rememberCoroutineScope()

    fun run() {
        scope.launch {
            val client = registry.client(connectionId) ?: return@launch
            loading = true
            try {
                val result = withContext(Dispatchers.IO) {
                    val facts = gatherSecurityFacts(client)
                    evaluateSecurity(facts)
                }
                checks = result
                score = securityScore(result)
                error = null
            } catch (e: Exception) {
                error = e.message
            } finally {
                loading = false
            }
        }
    }
    LaunchedEffect(connectionId) { run() }

    Column(modifier = Modifier.fillMaxSize()) {
        Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(vertical = 8.dp), horizontalArrangement = Arrangement.spacedBy(12.dp)) {
            if (checks.isNotEmpty()) ScoreBadge(score)
            Column(modifier = Modifier.weight(1f)) {
                Text(
                    "Read-only posture check — a pragmatic subset of the CIS benchmark, " +
                        "not host-level hardening.",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
                if (checks.isNotEmpty()) {
                    val fails = checks.count { it.level == CheckLevel.fail }
                    val warns = checks.count { it.level == CheckLevel.warn }
                    Text(
                        "${checks.count { it.level == CheckLevel.pass }} passed · $warns warning(s) · $fails failure(s)",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
            }
            OutlinedButton(onClick = { run() }, enabled = !loading) { Text(if (loading) "Checking…" else "Re-run") }
        }
        error?.let {
            Text(
                "$it — some checks read getCmdLineOpts, which needs cluster-level privileges.",
                color = MaterialTheme.colorScheme.error,
                style = MaterialTheme.typography.bodySmall,
            )
        }
        HorizontalDivider()
        val listState = rememberLazyListState()
        Box(modifier = Modifier.weight(1f)) {
            LazyColumn(state = listState, verticalArrangement = Arrangement.spacedBy(6.dp), modifier = Modifier.padding(top = 8.dp)) {
                items(checks, key = { it.id }) { CheckRow(it) }
            }
            VerticalScrollbar(
                adapter = rememberScrollbarAdapter(listState),
                modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
            )
        }
    }
}

@Composable
private fun ScoreBadge(score: Int) {
    val color = when {
        score >= 80 -> Color(0xFF4ADE80)
        score >= 50 -> Color(0xFFFBBF24)
        else -> Color(0xFFF87171)
    }
    Box(
        modifier = Modifier.size(64.dp).background(color.copy(alpha = 0.14f), CircleShape),
        contentAlignment = Alignment.Center,
    ) {
        Column(horizontalAlignment = Alignment.CenterHorizontally) {
            Text("$score", style = MaterialTheme.typography.titleLarge, color = color)
            Text("score", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
        }
    }
}

@Composable
private fun CheckRow(check: SecurityCheck) {
    val color = levelColor(check.level)
    val mark = when (check.level) {
        CheckLevel.pass -> "✓"
        CheckLevel.warn -> "⚠"
        CheckLevel.fail -> "✗"
        CheckLevel.info -> "•"
    }
    Card(border = CardDefaults.outlinedCardBorder()) {
        Row(modifier = Modifier.fillMaxWidth().padding(12.dp), horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            Text(mark, color = color, style = MaterialTheme.typography.titleMedium)
            Column(modifier = Modifier.weight(1f)) {
                Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    Text(check.title, style = MaterialTheme.typography.bodyMedium)
                    Text(
                        check.level.name.uppercase(),
                        style = MaterialTheme.typography.labelSmall,
                        color = color,
                        modifier = Modifier
                            .background(color.copy(alpha = 0.14f), RoundedCornerShape(3.dp))
                            .padding(horizontal = 5.dp, vertical = 1.dp),
                    )
                }
                Text(check.detail, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                if (check.remediation.isNotBlank()) {
                    Text(
                        "→ ${check.remediation}",
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.primary,
                    )
                }
            }
        }
    }
}
