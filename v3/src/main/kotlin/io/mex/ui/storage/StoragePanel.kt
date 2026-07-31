package io.mex.ui.storage

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
import io.mex.mongo.CollWeight
import io.mex.mongo.IndexUsage
import io.mex.mongo.MongoRegistry
import io.mex.mongo.StorageReport
import io.mex.mongo.dropIndex
import io.mex.mongo.observationDays
import io.mex.mongo.scanStorage
import io.mex.mongo.unusedCandidates
import io.mex.ui.components.ConfirmDangerDialog
import io.mex.ui.components.ReadOnlyBadge
import io.mex.util.formatBytes
import io.mex.util.formatCount
import io.mex.util.formatDuration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

private enum class StTab { Collections, Unused, Ttl }

private enum class SortKey { total, dataSize, storageSize, reclaim, indexSize, docs }

/**
 * Estate-wide storage analysis (DBA-STG-1/2): per-collection weight with reclaimable
 * space, unused-index drop candidates ranked by wasted bytes, and the TTL overview.
 */
@Composable
fun StoragePanel(connectionId: String, registry: MongoRegistry, readOnly: Boolean) {
    var tab by remember(connectionId) { mutableStateOf(StTab.Collections) }
    var report by remember(connectionId) { mutableStateOf<StorageReport?>(null) }
    var scanning by remember(connectionId) { mutableStateOf(false) }
    var progress by remember(connectionId) { mutableStateOf("") }
    var error by remember(connectionId) { mutableStateOf<String?>(null) }
    val scope = rememberCoroutineScope()

    fun scan() {
        scope.launch {
            val client = registry.client(connectionId) ?: return@launch
            scanning = true
            try {
                report = withContext(Dispatchers.IO) {
                    scanStorage(client) { done, total, ns ->
                        progress = if (ns.isBlank()) "" else "$done / $total · $ns"
                    }
                }
                error = null
            } catch (e: Exception) {
                error = e.message
            } finally {
                scanning = false
                progress = ""
            }
        }
    }
    LaunchedEffect(connectionId) { scan() }

    Column(modifier = Modifier.fillMaxSize().padding(horizontal = 24.dp, vertical = 16.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            Text("Storage", style = MaterialTheme.typography.headlineSmall)
            if (readOnly) ReadOnlyBadge()
            Spacer(modifier = Modifier.weight(1f))
            report?.let { r ->
                Text(
                    "${r.collections.size} collection(s) · data ${formatBytes(r.totalData)} · " +
                        "storage ${formatBytes(r.totalStorage)} · indexes ${formatBytes(r.totalIndex)}",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    fontFamily = FontFamily.Monospace,
                )
            }
            OutlinedButton(onClick = { scan() }, enabled = !scanning) {
                Text(if (scanning) "Scanning…" else "Rescan")
            }
        }
        if (scanning && progress.isNotBlank()) {
            Text(progress, style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
        }
        error?.let { Text(it, color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.bodySmall) }
        report?.errors?.takeIf { it.isNotEmpty() }?.let {
            Text(
                "${it.size} namespace(s) skipped: ${it.first()}${if (it.size > 1) " …" else ""}",
                style = MaterialTheme.typography.labelSmall,
                color = Color(0xFFFBBF24),
            )
        }

        val unused = remember(report) { report?.let { unusedCandidates(it.indexes) }.orEmpty() }
        val ttl = remember(report) { report?.indexes.orEmpty().filter { it.ttlSeconds != null } }

        Row {
            StTabBtn("Collections", tab == StTab.Collections) { tab = StTab.Collections }
            StTabBtn("Unused indexes${if (unused.isNotEmpty()) " (${unused.size})" else ""}", tab == StTab.Unused) { tab = StTab.Unused }
            StTabBtn("TTL${if (ttl.isNotEmpty()) " (${ttl.size})" else ""}", tab == StTab.Ttl) { tab = StTab.Ttl }
        }
        HorizontalDivider()
        Box(modifier = Modifier.weight(1f)) {
            when (tab) {
                StTab.Collections -> CollectionsTab(report?.collections.orEmpty())
                StTab.Unused -> UnusedTab(connectionId, registry, unused, readOnly, onDropped = { scan() })
                StTab.Ttl -> TtlTab(ttl)
            }
        }
    }
}

@Composable
private fun StTabBtn(label: String, active: Boolean, onClick: () -> Unit) {
    val color = if (active) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant
    TextButton(onClick = onClick) { Text(label, color = color, style = MaterialTheme.typography.labelMedium) }
}

/* ============================ Collections ============================ */

@Composable
private fun CollectionsTab(collections: List<CollWeight>) {
    var sortKey by remember { mutableStateOf(SortKey.total) }
    var desc by remember { mutableStateOf(true) }

    val sorted = remember(collections, sortKey, desc) {
        val base = when (sortKey) {
            SortKey.total -> collections.sortedBy { it.totalSize }
            SortKey.dataSize -> collections.sortedBy { it.dataSize }
            SortKey.storageSize -> collections.sortedBy { it.storageSize }
            SortKey.reclaim -> collections.sortedBy { it.reclaimablePct }
            SortKey.indexSize -> collections.sortedBy { it.indexSize }
            SortKey.docs -> collections.sortedBy { it.count }
        }
        if (desc) base.asReversed() else base
    }

    Column(modifier = Modifier.fillMaxSize()) {
        Row(modifier = Modifier.padding(vertical = 4.dp), verticalAlignment = Alignment.CenterVertically) {
            Text(
                "NAMESPACE",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.weight(1f),
            )
            SortHeader("DOCS", SortKey.docs, sortKey, desc, 90.dp) { sortKey = it.first; desc = it.second }
            SortHeader("DATA", SortKey.dataSize, sortKey, desc, 90.dp) { sortKey = it.first; desc = it.second }
            SortHeader("STORAGE", SortKey.storageSize, sortKey, desc, 90.dp) { sortKey = it.first; desc = it.second }
            SortHeader("RECLAIM", SortKey.reclaim, sortKey, desc, 90.dp) { sortKey = it.first; desc = it.second }
            SortHeader("INDEXES", SortKey.indexSize, sortKey, desc, 110.dp) { sortKey = it.first; desc = it.second }
            SortHeader("TOTAL", SortKey.total, sortKey, desc, 90.dp) { sortKey = it.first; desc = it.second }
        }
        HorizontalDivider()
        if (sorted.isEmpty()) {
            Text(
                "No user collections found.",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(vertical = 12.dp),
            )
        }
        val listState = rememberLazyListState()
        Box(modifier = Modifier.weight(1f)) {
            LazyColumn(state = listState) {
                items(sorted, key = { it.ns }) { c ->
                    Row(
                        modifier = Modifier.fillMaxWidth().padding(vertical = 3.dp),
                        verticalAlignment = Alignment.CenterVertically,
                    ) {
                        Text(
                            c.ns,
                            style = MaterialTheme.typography.bodySmall,
                            fontFamily = FontFamily.Monospace,
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis,
                            modifier = Modifier.weight(1f),
                        )
                        Cell(formatCount(c.count), 90.dp)
                        Cell(formatBytes(c.dataSize), 90.dp)
                        Cell(formatBytes(c.storageSize), 90.dp)
                        ReclaimCell(c, 90.dp)
                        Cell("${c.indexCount} · ${formatBytes(c.indexSize)}", 110.dp)
                        Cell(formatBytes(c.totalSize), 90.dp)
                    }
                    HorizontalDivider(thickness = 0.5.dp)
                }
            }
            VerticalScrollbar(
                adapter = rememberScrollbarAdapter(listState),
                modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
            )
        }
    }
}

@Composable
private fun SortHeader(
    label: String,
    key: SortKey,
    current: SortKey,
    desc: Boolean,
    width: androidx.compose.ui.unit.Dp,
    onSort: (Pair<SortKey, Boolean>) -> Unit,
) {
    val active = key == current
    Text(
        label + if (active) (if (desc) " ↓" else " ↑") else "",
        style = MaterialTheme.typography.labelSmall,
        color = if (active) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant,
        modifier = Modifier
            .width(width)
            .clickable { onSort(key to if (active) !desc else true) },
    )
}

@Composable
private fun Cell(text: String, width: androidx.compose.ui.unit.Dp) {
    Text(
        text,
        style = MaterialTheme.typography.bodySmall,
        fontFamily = FontFamily.Monospace,
        maxLines = 1,
        modifier = Modifier.width(width),
    )
}

@Composable
private fun ReclaimCell(c: CollWeight, width: androidx.compose.ui.unit.Dp) {
    val pct = c.reclaimablePct
    val color = when {
        pct >= 50 -> Color(0xFFF87171)
        pct >= 30 -> Color(0xFFFBBF24)
        else -> MaterialTheme.colorScheme.onSurface
    }
    Text(
        "%.0f%%".format(pct) + if (c.freeStorageSize == null) " ~" else "",
        style = MaterialTheme.typography.bodySmall,
        fontFamily = FontFamily.Monospace,
        color = color,
        maxLines = 1,
        modifier = Modifier.width(width),
    )
}

/* ============================ Unused indexes ============================ */

@Composable
private fun UnusedTab(
    connectionId: String,
    registry: MongoRegistry,
    unused: List<IndexUsage>,
    readOnly: Boolean,
    onDropped: () -> Unit,
) {
    var dropping by remember { mutableStateOf<IndexUsage?>(null) }
    var opError by remember { mutableStateOf<String?>(null) }
    val scope = rememberCoroutineScope()

    Column(modifier = Modifier.fillMaxSize()) {
        Text(
            "Indexes with zero recorded reads since the counters started. " +
                "\$indexStats resets on server restart — trust a short observation window less. " +
                "TTL indexes are excluded (zero reads is their normal state).",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(vertical = 6.dp),
        )
        opError?.let { Text(it, color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.bodySmall) }
        if (unused.isEmpty()) {
            Text(
                "No unused indexes — every secondary index has recorded reads.",
                style = MaterialTheme.typography.bodySmall,
                color = Color(0xFF4ADE80),
                modifier = Modifier.padding(vertical = 12.dp),
            )
        }
        val listState = rememberLazyListState()
        Box(modifier = Modifier.weight(1f)) {
            LazyColumn(state = listState, verticalArrangement = Arrangement.spacedBy(4.dp)) {
                items(unused, key = { it.id }) { idx ->
                    Card(border = CardDefaults.outlinedCardBorder()) {
                        Row(
                            modifier = Modifier.fillMaxWidth().padding(horizontal = 12.dp, vertical = 6.dp),
                            verticalAlignment = Alignment.CenterVertically,
                            horizontalArrangement = Arrangement.spacedBy(8.dp),
                        ) {
                            Column(modifier = Modifier.weight(1f)) {
                                Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                                    Text(idx.name, style = MaterialTheme.typography.bodyMedium, fontFamily = FontFamily.Monospace)
                                    if (idx.unique) Chip("unique — enforces a constraint even unread", Color(0xFFFBBF24))
                                }
                                Text(
                                    idx.ns,
                                    style = MaterialTheme.typography.labelSmall,
                                    fontFamily = FontFamily.Monospace,
                                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                                )
                            }
                            observationDays(idx.sinceMillis)?.let { days ->
                                Text(
                                    "observed ${days}d",
                                    style = MaterialTheme.typography.labelSmall,
                                    color = if (days < 7) Color(0xFFFBBF24) else MaterialTheme.colorScheme.onSurfaceVariant,
                                )
                            }
                            Text(
                                formatBytes(idx.sizeBytes),
                                style = MaterialTheme.typography.bodySmall,
                                fontFamily = FontFamily.Monospace,
                            )
                            if (!readOnly) {
                                TextButton(onClick = { dropping = idx }) {
                                    Text("Drop", color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.labelSmall)
                                }
                            }
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

    dropping?.let { idx ->
        ConfirmDangerDialog(
            title = "Drop index ${idx.name}?",
            text = buildString {
                append("This drops \"${idx.name}\" on ${idx.ns} (${formatBytes(idx.sizeBytes)}). ")
                if (idx.unique) append("It is UNIQUE — dropping it stops enforcing that constraint. ")
                append("Queries that relied on it fall back to a collection scan. This cannot be undone.")
            },
            confirmLabel = "Drop index",
            onConfirm = {
                val captured = idx
                dropping = null
                scope.launch {
                    try {
                        val client = registry.client(connectionId) ?: error("Not connected")
                        withContext(Dispatchers.IO) { dropIndex(client, captured.db, captured.coll, captured.name) }
                        opError = null
                        onDropped()
                    } catch (e: Exception) {
                        opError = e.message
                    }
                }
            },
            onCancel = { dropping = null },
        )
    }
}

/* ============================ TTL ============================ */

@Composable
private fun TtlTab(ttl: List<IndexUsage>) {
    Column(modifier = Modifier.fillMaxSize()) {
        Text(
            "Indexes that expire documents automatically.",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
            modifier = Modifier.padding(vertical = 6.dp),
        )
        if (ttl.isEmpty()) {
            Text(
                "No TTL indexes in the scanned namespaces.",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(vertical = 12.dp),
            )
        }
        val listState = rememberLazyListState()
        Box(modifier = Modifier.weight(1f)) {
            LazyColumn(state = listState, verticalArrangement = Arrangement.spacedBy(4.dp)) {
                items(ttl, key = { it.id }) { idx ->
                    Card(border = CardDefaults.outlinedCardBorder()) {
                        Row(
                            modifier = Modifier.fillMaxWidth().padding(horizontal = 12.dp, vertical = 6.dp),
                            verticalAlignment = Alignment.CenterVertically,
                            horizontalArrangement = Arrangement.spacedBy(8.dp),
                        ) {
                            Column(modifier = Modifier.weight(1f)) {
                                Text(idx.name, style = MaterialTheme.typography.bodyMedium, fontFamily = FontFamily.Monospace)
                                Text(
                                    idx.ns,
                                    style = MaterialTheme.typography.labelSmall,
                                    fontFamily = FontFamily.Monospace,
                                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                                )
                            }
                            Chip("expires after ${formatDuration(idx.ttlSeconds ?: 0)}", MaterialTheme.colorScheme.primary)
                            Text(
                                formatBytes(idx.sizeBytes),
                                style = MaterialTheme.typography.bodySmall,
                                fontFamily = FontFamily.Monospace,
                            )
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
}

@Composable
private fun Chip(text: String, color: Color) {
    Text(
        text,
        style = MaterialTheme.typography.labelSmall,
        color = color,
        modifier = Modifier
            .background(color.copy(alpha = 0.12f), RoundedCornerShape(4.dp))
            .padding(horizontal = 5.dp, vertical = 1.dp),
    )
}
