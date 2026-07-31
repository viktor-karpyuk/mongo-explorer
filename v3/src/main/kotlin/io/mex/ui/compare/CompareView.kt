package io.mex.ui.compare

import androidx.compose.foundation.VerticalScrollbar
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.rememberScrollbarAdapter
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.data.ConnectionSummary
import io.mex.mongo.*
import io.mex.util.formatCount
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

private fun sideColor(side: DiffSide): Color = when (side) {
    DiffSide.onlyLeft -> Color(0xFF60A5FA)
    DiffSide.onlyRight -> Color(0xFFF472B6)
    DiffSide.both -> Color(0xFF4ADE80)
}

/** Data & schema comparison between two open connections (DBA-DIFF-1..3). */
@Composable
fun CompareView(ctx: AppContext, registry: MongoRegistry) {
    val states by registry.states.collectAsState()
    val open = remember(states) { ctx.connections.list().filter { states[it.id] is ConnectionState.Connected } }
    var leftId by remember { mutableStateOf("") }
    var rightId by remember { mutableStateOf("") }

    LaunchedEffect(open) {
        if (leftId.isBlank()) leftId = open.firstOrNull()?.id.orEmpty()
        if (rightId.isBlank()) rightId = open.getOrNull(1)?.id ?: open.firstOrNull()?.id.orEmpty()
    }

    var collections by remember { mutableStateOf<List<CollectionDiff>>(emptyList()) }
    var comparing by remember { mutableStateOf(false) }
    var error by remember { mutableStateOf<String?>(null) }
    var selectedNs by remember { mutableStateOf<CollectionDiff?>(null) }
    val scope = rememberCoroutineScope()

    fun compareCollections() {
        scope.launch {
            val l = registry.client(leftId) ?: return@launch
            val r = registry.client(rightId) ?: return@launch
            comparing = true
            selectedNs = null
            try {
                collections = withContext(Dispatchers.IO) {
                    diffCollections(collectCollectionCounts(l), collectCollectionCounts(r))
                }
                error = null
            } catch (e: Exception) {
                error = e.message
            } finally {
                comparing = false
            }
        }
    }

    Column(modifier = Modifier.fillMaxSize().padding(24.dp)) {
        Text("Compare connections", style = MaterialTheme.typography.headlineSmall)
        Spacer(modifier = Modifier.height(4.dp))
        Text(
            "Diff collections, sampled schema and indexes between two open connections — " +
                "verify a migration, or spot drift between environments.",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Spacer(modifier = Modifier.height(12.dp))

        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            ConnPicker("LEFT", leftId, open, sideColor(DiffSide.onlyLeft)) { leftId = it }
            Text("↔", style = MaterialTheme.typography.titleLarge, color = MaterialTheme.colorScheme.onSurfaceVariant)
            ConnPicker("RIGHT", rightId, open, sideColor(DiffSide.onlyRight)) { rightId = it }
            Spacer(modifier = Modifier.weight(1f))
            Button(
                onClick = { compareCollections() },
                enabled = leftId.isNotBlank() && rightId.isNotBlank() && leftId != rightId && !comparing,
            ) { Text(if (comparing) "Comparing…" else "Compare") }
        }
        if (leftId == rightId && leftId.isNotBlank()) {
            Text("Pick two different connections.", color = Color(0xFFB45309), style = MaterialTheme.typography.labelSmall)
        }
        error?.let { Text(it, color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.bodySmall) }
        Spacer(modifier = Modifier.height(12.dp))

        if (collections.isNotEmpty()) {
            val onlyL = collections.count { it.side == DiffSide.onlyLeft }
            val onlyR = collections.count { it.side == DiffSide.onlyRight }
            val mismatched = collections.count { it.side == DiffSide.both && !it.countMatches }
            Text(
                "${collections.size} namespace(s) · $onlyL only left · $onlyR only right · $mismatched count mismatch",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                fontFamily = FontFamily.Monospace,
            )
        }

        Row(modifier = Modifier.weight(1f)) {
            Box(modifier = Modifier.weight(1f)) {
                CollectionDiffList(collections, selectedNs) { selectedNs = it }
            }
            selectedNs?.takeIf { it.side == DiffSide.both }?.let { ns ->
                VerticalDivider()
                Box(modifier = Modifier.weight(1f)) {
                    NamespaceDetail(registry, leftId, rightId, ns)
                }
            }
        }
    }
}

@Composable
private fun ConnPicker(label: String, selectedId: String, options: List<ConnectionSummary>, accent: Color, onSelect: (String) -> Unit) {
    var open by remember { mutableStateOf(false) }
    Column {
        Text(label, style = MaterialTheme.typography.labelSmall, color = accent)
        Box {
            OutlinedButton(onClick = { open = true }) {
                Text(options.firstOrNull { it.id == selectedId }?.name ?: "(none)")
            }
            DropdownMenu(expanded = open, onDismissRequest = { open = false }) {
                options.forEach { c ->
                    DropdownMenuItem(text = { Text(c.name) }, onClick = { open = false; onSelect(c.id) })
                }
            }
        }
    }
}

@Composable
private fun CollectionDiffList(diffs: List<CollectionDiff>, selected: CollectionDiff?, onSelect: (CollectionDiff) -> Unit) {
    if (diffs.isEmpty()) {
        Text(
            "Run a comparison to see collection differences.",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        return
    }
    val listState = rememberLazyListState()
    Box(modifier = Modifier.fillMaxSize()) {
        LazyColumn(state = listState, verticalArrangement = Arrangement.spacedBy(2.dp)) {
            items(diffs, key = { "${it.db}.${it.coll}" }) { d ->
                val isSel = selected?.db == d.db && selected.coll == d.coll
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .background(if (isSel) MaterialTheme.colorScheme.primary.copy(alpha = 0.12f) else Color.Transparent)
                        .clickable { onSelect(d) }
                        .padding(horizontal = 6.dp, vertical = 4.dp),
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                ) {
                    SideMark(d.side)
                    Text(
                        "${d.db}.${d.coll}",
                        style = MaterialTheme.typography.bodySmall,
                        fontFamily = FontFamily.Monospace,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis,
                        modifier = Modifier.weight(1f),
                    )
                    when (d.side) {
                        DiffSide.both -> {
                            val match = d.countMatches
                            Text(
                                "${formatCount(d.leftCount ?: 0)} ↔ ${formatCount(d.rightCount ?: 0)}",
                                style = MaterialTheme.typography.labelSmall,
                                fontFamily = FontFamily.Monospace,
                                color = if (match) MaterialTheme.colorScheme.onSurfaceVariant else Color(0xFFF87171),
                            )
                            if (d.side == DiffSide.both) {
                                Text(if (match) "→" else "≠", color = if (match) sideColor(DiffSide.both) else Color(0xFFF87171))
                            }
                        }
                        DiffSide.onlyLeft -> Text("only left · ${formatCount(d.leftCount ?: 0)}", style = MaterialTheme.typography.labelSmall, color = sideColor(DiffSide.onlyLeft))
                        DiffSide.onlyRight -> Text("only right · ${formatCount(d.rightCount ?: 0)}", style = MaterialTheme.typography.labelSmall, color = sideColor(DiffSide.onlyRight))
                    }
                }
            }
        }
        VerticalScrollbar(
            adapter = rememberScrollbarAdapter(listState),
            modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
        )
    }
}

@Composable
private fun SideMark(side: DiffSide) {
    val (text, color) = when (side) {
        DiffSide.both -> "=" to sideColor(DiffSide.both)
        DiffSide.onlyLeft -> "L" to sideColor(DiffSide.onlyLeft)
        DiffSide.onlyRight -> "R" to sideColor(DiffSide.onlyRight)
    }
    Text(
        text,
        style = MaterialTheme.typography.labelSmall,
        color = color,
        fontFamily = FontFamily.Monospace,
        modifier = Modifier
            .background(color.copy(alpha = 0.14f), RoundedCornerShape(3.dp))
            .padding(horizontal = 4.dp),
    )
}

@Composable
private fun NamespaceDetail(registry: MongoRegistry, leftId: String, rightId: String, ns: CollectionDiff) {
    var comparison by remember(ns) { mutableStateOf<NamespaceComparison?>(null) }
    var loading by remember(ns) { mutableStateOf(true) }
    var error by remember(ns) { mutableStateOf<String?>(null) }

    LaunchedEffect(ns) {
        val l = registry.client(leftId) ?: return@LaunchedEffect
        val r = registry.client(rightId) ?: return@LaunchedEffect
        loading = true
        try {
            comparison = withContext(Dispatchers.IO) { compareNamespace(l, r, ns.db, ns.coll) }
            error = null
        } catch (e: Exception) {
            error = e.message
        } finally {
            loading = false
        }
    }

    Column(modifier = Modifier.fillMaxSize().padding(start = 12.dp)) {
        Text("${ns.db}.${ns.coll}", style = MaterialTheme.typography.titleMedium, fontFamily = FontFamily.Monospace)
        if (loading) { Text("Sampling both sides…", style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant); return }
        error?.let { Text(it, color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.bodySmall); return }
        val c = comparison ?: return

        Text(
            buildString {
                append("counts ${formatCount(c.leftCount)} ↔ ${formatCount(c.rightCount)} ")
                append(if (c.countsMatch) "✓" else "≠")
                append(" · fields ${if (c.fieldsInSync) "in sync ✓" else "differ"}")
                append(" · indexes ${if (c.indexesInSync) "in sync ✓" else "differ"}")
            },
            style = MaterialTheme.typography.labelSmall,
            color = if (c.countsMatch && c.fieldsInSync && c.indexesInSync) Color(0xFF4ADE80) else Color(0xFFFBBF24),
            fontFamily = FontFamily.Monospace,
        )
        Spacer(modifier = Modifier.height(8.dp))

        val listState = rememberLazyListState()
        Box(modifier = Modifier.weight(1f)) {
            LazyColumn(state = listState, verticalArrangement = Arrangement.spacedBy(2.dp)) {
                item { SectionLabel("FIELDS (sampled)") }
                val fields = c.fields
                if (fields.all { it.side == DiffSide.both && !it.typeMismatch }) {
                    item { Text("  all sampled fields match", style = MaterialTheme.typography.labelSmall, color = Color(0xFF4ADE80)) }
                }
                items(fields, key = { "f:${it.path}" }) { f -> FieldRow(f) }

                item { Spacer(modifier = Modifier.height(8.dp)); SectionLabel("INDEXES") }
                items(c.indexes, key = { "i:${it.name}" }) { i -> IndexRow(i) }
            }
            VerticalScrollbar(
                adapter = rememberScrollbarAdapter(listState),
                modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
            )
        }
    }
}

@Composable
private fun SectionLabel(text: String) {
    Text(text, style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
    HorizontalDivider()
}

@Composable
private fun FieldRow(f: FieldDiff) {
    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp), modifier = Modifier.fillMaxWidth()) {
        SideMark(f.side)
        Text(f.path, style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace, modifier = Modifier.weight(1f), maxLines = 1, overflow = TextOverflow.Ellipsis)
        when {
            f.typeMismatch -> Text(
                "${f.leftType} ≠ ${f.rightType}",
                style = MaterialTheme.typography.labelSmall,
                color = Color(0xFFF87171),
                fontFamily = FontFamily.Monospace,
            )
            f.side == DiffSide.both -> Text(f.leftType ?: "", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant, fontFamily = FontFamily.Monospace)
            f.side == DiffSide.onlyLeft -> Text(f.leftType ?: "", style = MaterialTheme.typography.labelSmall, color = sideColor(DiffSide.onlyLeft), fontFamily = FontFamily.Monospace)
            else -> Text(f.rightType ?: "", style = MaterialTheme.typography.labelSmall, color = sideColor(DiffSide.onlyRight), fontFamily = FontFamily.Monospace)
        }
    }
}

@Composable
private fun IndexRow(i: IndexDiff) {
    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp), modifier = Modifier.fillMaxWidth()) {
        SideMark(i.side)
        Text(i.name, style = MaterialTheme.typography.bodySmall, fontFamily = FontFamily.Monospace, modifier = Modifier.weight(1f), maxLines = 1, overflow = TextOverflow.Ellipsis)
        Text(
            (i.leftKeys ?: i.rightKeys).orEmpty(),
            style = MaterialTheme.typography.labelSmall,
            color = when (i.side) {
                DiffSide.both -> MaterialTheme.colorScheme.onSurfaceVariant
                DiffSide.onlyLeft -> sideColor(DiffSide.onlyLeft)
                DiffSide.onlyRight -> sideColor(DiffSide.onlyRight)
            },
            fontFamily = FontFamily.Monospace,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis,
        )
    }
}
