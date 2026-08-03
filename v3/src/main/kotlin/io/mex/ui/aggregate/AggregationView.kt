package io.mex.ui.aggregate

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.data.QueryHistoryInput
import io.mex.data.QueryKind
import io.mex.mongo.AggregateRequest
import io.mex.mongo.FindResult
import io.mex.mongo.MongoRegistry
import io.mex.mongo.STAGE_OPERATORS
import io.mex.mongo.StageDraft
import io.mex.mongo.TEMPLATES
import io.mex.mongo.executeAggregate
import io.mex.ui.query.NumberField
import io.mex.ui.query.QueryStore
import io.mex.ui.results.ResultsPane
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

@Composable
fun AggregationView(
    ctx: AppContext,
    registry: MongoRegistry,
    connectionId: String,
    db: String,
    collection: String,
    queries: QueryStore,
) {
    // Hoisted per-namespace: local remembers lost the whole pipeline when the user
    // peeked at Schema/Indexes and came back.
    val d = queries.aggregation("$connectionId/$db/$collection")
    val stages = d.stages
    var limit by d::limit
    var maxTimeMs by d::maxTimeMs
    var result by d::result
    var running by d::running
    var templateMenu by remember { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    // The pipeline the current result came from, so paging re-runs the same stages
    // even if the user has since edited the editor.
    var skip by d::skip
    var lastUpTo by d::lastUpTo

    fun runUpTo(upTo: Int = stages.size - 1, atSkip: Int = 0) {
        scope.launch {
            val client = registry.client(connectionId) ?: return@launch
            running = true
            skip = atSkip
            lastUpTo = upTo
            try {
                val enabled = stages.take(upTo + 1).filter { it.enabled }
                val req = AggregateRequest(
                    connectionId = connectionId,
                    db = db,
                    collection = collection,
                    pipeline = enabled.map { it.operator to it.body },
                    limit = limit,
                    maxTimeMs = maxTimeMs,
                    skip = atSkip,
                )
                val res = withContext(Dispatchers.IO) { executeAggregate(client, req) }
                result = res
                withContext(Dispatchers.IO) {
                    ctx.queryHistory.record(
                        QueryHistoryInput(
                            connectionId = connectionId,
                            database = db,
                            collection = collection,
                            kind = QueryKind.aggregate,
                            body = enabled.joinToString { "${it.operator} -> ${it.body}" },
                            durationMs = if (res is FindResult.Ok) res.durationMs else (res as FindResult.Failed).durationMs,
                            rowCount = if (res is FindResult.Ok) res.rows.size else null,
                            error = if (res is FindResult.Failed) res.error else null,
                        ),
                    )
                }
            } finally {
                running = false
            }
        }
    }

    Column(modifier = Modifier.fillMaxSize()) {
        Row(
            modifier = Modifier.fillMaxWidth().padding(12.dp),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            Text("Pipeline · $db.$collection", style = MaterialTheme.typography.titleMedium, modifier = Modifier.weight(1f))
            Box {
                OutlinedButton(onClick = { templateMenu = true }) { Text("Load template ⌄") }
                DropdownMenu(expanded = templateMenu, onDismissRequest = { templateMenu = false }) {
                    TEMPLATES.forEach { t ->
                        DropdownMenuItem(
                            text = { Text("${t.name} — ${t.description}") },
                            onClick = {
                                templateMenu = false
                                stages.clear()
                                stages.addAll(t.stages.map { StageDraft(operator = it.first, body = it.second) })
                            },
                        )
                    }
                }
            }
            NumberField("Limit", limit, Modifier.width(100.dp)) { limit = it.coerceAtLeast(1) }
            NumberField("maxTimeMs", maxTimeMs.toInt(), Modifier.width(120.dp)) { maxTimeMs = it.toLong() }
            Button(
                onClick = { runUpTo() },
                enabled = !running && stages.isNotEmpty(),
            ) { Text(if (running) "Running…" else "Run pipeline") }
        }
        HorizontalDivider()

        // Stages
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .heightIn(max = 320.dp)
                .verticalScroll(rememberScrollState())
                .padding(12.dp),
            verticalArrangement = Arrangement.spacedBy(6.dp),
        ) {
            stages.forEachIndexed { i, stage ->
                StageRow(
                    index = i,
                    stage = stage,
                    count = stages.size,
                    onMove = { dir ->
                        val j = i + dir
                        if (j in stages.indices) {
                            val tmp = stages[i]; stages[i] = stages[j]; stages[j] = tmp
                        }
                    },
                    onRemove = { stages.removeAt(i) },
                    onToggle = { stage.enabled = !stage.enabled; stages[i] = stage.copy() },
                    onOperator = { stage.operator = it; stages[i] = stage.copy() },
                    onBody = { stage.body = it; stages[i] = stage.copy() },
                    onRunToHere = { runUpTo(i) },
                )
            }
            OutlinedButton(onClick = { stages.add(StageDraft()) }) { Text("+ Add stage") }
        }
        HorizontalDivider()

        ResultsPane(
            result = result,
            running = running,
            skip = skip,
            limit = limit,
            // Paging appends $skip/$limit to the pipeline and re-runs it — the buttons
            // used to be wired to no-ops while still rendering as enabled.
            onPrev = { runUpTo(lastUpTo, (skip - limit).coerceAtLeast(0)) },
            onNext = { runUpTo(lastUpTo, skip + limit) },
            onFirst = { runUpTo(lastUpTo, 0) },
            onLimitChange = { limit = it; runUpTo(lastUpTo, 0) },
        )
    }
}

@Composable
private fun StageRow(
    index: Int,
    stage: StageDraft,
    count: Int,
    onMove: (Int) -> Unit,
    onRemove: () -> Unit,
    onToggle: () -> Unit,
    onOperator: (String) -> Unit,
    onBody: (String) -> Unit,
    onRunToHere: () -> Unit,
) {
    var opMenu by remember { mutableStateOf(false) }

    Card(border = CardDefaults.outlinedCardBorder()) {
        Column {
            Row(
                modifier = Modifier.fillMaxWidth().padding(horizontal = 8.dp, vertical = 4.dp),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(6.dp),
            ) {
                Text("${index + 1}", style = MaterialTheme.typography.labelSmall)
                Box {
                    OutlinedButton(
                        onClick = { opMenu = true },
                        contentPadding = PaddingValues(horizontal = 8.dp, vertical = 4.dp),
                    ) {
                        Text(stage.operator, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.labelSmall)
                    }
                    DropdownMenu(expanded = opMenu, onDismissRequest = { opMenu = false }) {
                        STAGE_OPERATORS.forEach { op ->
                            DropdownMenuItem(
                                text = { Text(op, fontFamily = FontFamily.Monospace) },
                                onClick = { opMenu = false; onOperator(op) },
                            )
                        }
                    }
                }
                TextButton(onClick = onToggle) {
                    Text(if (stage.enabled) "Disable" else "Enable", style = MaterialTheme.typography.labelSmall)
                }
                TextButton(onClick = { onMove(-1) }, enabled = index > 0) { Text("↑") }
                TextButton(onClick = { onMove(1) }, enabled = index < count - 1) { Text("↓") }
                TextButton(onClick = onRunToHere) { Text("Run to here", style = MaterialTheme.typography.labelSmall) }
                Spacer(modifier = Modifier.weight(1f))
                TextButton(onClick = onRemove) { Text("✕") }
            }
            OutlinedTextField(
                value = stage.body,
                onValueChange = onBody,
                modifier = Modifier.fillMaxWidth().padding(8.dp),
                textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
                minLines = 2,
            )
        }
    }
}
