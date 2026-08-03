package io.mex.ui.cluster

import androidx.compose.foundation.Canvas
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.MoreVert
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Rect
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.Path
import androidx.compose.ui.graphics.PathEffect
import androidx.compose.ui.graphics.drawscope.DrawScope
import androidx.compose.ui.layout.LayoutCoordinates
import androidx.compose.ui.layout.onGloballyPositioned
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.toSize
import io.mex.mongo.ClusterSnapshot
import io.mex.mongo.MemberInfo
import io.mex.mongo.RouterInfo
import io.mex.mongo.RsMemberConfig
import io.mex.mongo.ShardInfo

/**
 * Mutating / navigating actions the diagram can offer on its nodes. A null callback
 * means the action is unavailable (read-only connection, or the capability isn't
 * wired) and the menu entry is simply absent.
 */
class TopologyActions(
    val onStepDown: (() -> Unit)? = null,
    val onEditMember: ((RsMemberConfig) -> Unit)? = null,
    val onDirectConnect: ((String) -> Unit)? = null,
    val onFreeze: ((String) -> Unit)? = null,
    val onRemoveMember: ((RsMemberConfig) -> Unit)? = null,
    val onRemoveShard: ((ShardInfo) -> Unit)? = null,
)

private data class MenuEntry(val label: String, val danger: Boolean = false, val onClick: () -> Unit)

/** What the detail strip shows for the currently selected node. */
private sealed class Payload {
    class Member(val m: MemberInfo, val cfg: RsMemberConfig?) : Payload()
    class Router(val r: RouterInfo) : Payload()
    class Shard(val s: ShardInfo, val total: Long?) : Payload()
    class ConfigSvr(val rsName: String?, val hosts: List<String>) : Payload()
    class Single(val endpoint: String?) : Payload()
}

/**
 * Node-and-wire picture of the cluster: routers over config/shards for sharded
 * clusters, primary over the other members for replica sets, a single node for
 * standalones. Wires are drawn on a canvas behind the nodes from their measured
 * positions. Clicking a node selects it (detail strip below); the ⋮ menu holds
 * its actions.
 */
@Composable
fun ClusterTopologyDiagram(snap: ClusterSnapshot, actions: TopologyActions = TopologyActions()) {
    // Keyed on cluster identity: selection survives the 5 s polls (same cluster) but
    // resets when the panel is re-pointed at a different connection.
    var selected by remember(snap.topology.setName, snap.endpoint) { mutableStateOf<String?>(null) }
    val payloads = payloadsFor(snap)

    Column(modifier = Modifier.fillMaxWidth()) {
        Box(
            modifier = Modifier.fillMaxWidth().padding(vertical = 20.dp),
            contentAlignment = Alignment.Center,
        ) {
            Box(modifier = Modifier.horizontalScroll(rememberScrollState()).padding(horizontal = 20.dp)) {
                val select: (String) -> Unit = { key -> selected = if (selected == key) null else key }
                when (snap.topology.type) {
                    "sharded" -> ShardedDiagram(snap, actions, selected, select)
                    "replicaset" -> ReplicaSetDiagram(snap, actions, selected, select)
                    else -> StandaloneDiagram(snap.endpoint, actions, selected, select)
                }
            }
        }
        payloads[selected]?.let { p ->
            HorizontalDivider()
            DetailStrip(p)
        }
    }
}

/* ===================== node inventories (shared with strip) ===================== */

private fun routersFor(snap: ClusterSnapshot): List<RouterInfo> =
    snap.sharded?.routers?.takeIf { it.isNotEmpty() }
        ?: listOfNotNull(snap.endpoint?.let { RouterInfo(it, 0, null) })

/*
 * Node keys are identity-based ("m:<host>", "s:<name>", "r:<host>") — positional keys
 * made the selection and wire colours silently jump to a different node whenever an
 * election or a shard removal reordered the underlying lists between polls.
 */
private fun payloadsFor(snap: ClusterSnapshot): Map<String, Payload> = buildMap {
    when (snap.topology.type) {
        "sharded" -> {
            routersFor(snap).forEach { r -> put("r:${r.host}", Payload.Router(r)) }
            val sh = snap.sharded
            if (!sh?.configHosts.isNullOrEmpty()) {
                put("cfg", Payload.ConfigSvr(sh?.configRsName, sh?.configHosts.orEmpty()))
            }
            val shards = sh?.shards.orEmpty()
            val total = shards.mapNotNull { it.chunks }.sum().takeIf { it > 0 }
            shards.forEach { s -> put("s:${s.name}", Payload.Shard(s, total)) }
        }
        "replicaset" -> {
            val cfgByHost = snap.rsConfig?.members?.associateBy { it.host }.orEmpty()
            snap.topology.members.forEach { m ->
                put("m:${m.name}", Payload.Member(m, cfgByHost[m.name]))
            }
        }
        else -> put("solo", Payload.Single(snap.endpoint))
    }
}

/* ============================ layouts per type ============================ */

@Composable
private fun ShardedDiagram(
    snap: ClusterSnapshot,
    actions: TopologyActions,
    selected: String?,
    onSelect: (String) -> Unit,
) {
    val clipboard = LocalClipboardManager.current
    val copy: (String) -> Unit = { clipboard.setText(AnnotatedString(it)) }
    val sh = snap.sharded
    val routers = routersFor(snap)
    val hasConfig = !sh?.configHosts.isNullOrEmpty()
    val shards = sh?.shards.orEmpty()

    DiagramSurface(
        sources = routers.map { "r:${it.host}" },
        targets = buildList {
            if (hasConfig) add("cfg" to LinkStyle.Dashed)
            shards.forEach { add("s:${it.name}" to LinkStyle.Solid) }
        },
    ) { anchor ->
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(44.dp),
        ) {
            NodeRow {
                routers.forEach { r ->
                    val key = "r:${r.host}"
                    Node(
                        anchor = anchor(key),
                        selected = selected == key,
                        onSelect = { onSelect(key) },
                        menu = buildList {
                            add(MenuEntry("Copy host") { copy(r.host) })
                            actions.onDirectConnect?.let { dc ->
                                add(MenuEntry("Connect directly") { dc(r.host) })
                            }
                        },
                    ) {
                        NodeTitle("mongos", if (r.active) StatusTone.Good else StatusTone.Bad)
                        HostLine(r.host)
                        r.version?.let { Small(it) }
                        if (!r.active) Small("last ping ${r.lastPingAgeSecs?.let { formatAge(it) } ?: "unknown"}")
                    }
                }
            }
            NodeRow {
                if (hasConfig) {
                    Node(
                        anchor = anchor("cfg"),
                        selected = selected == "cfg",
                        onSelect = { onSelect("cfg") },
                        menu = listOf(
                            MenuEntry("Copy hosts") { copy(sh?.configHosts.orEmpty().joinToString(",")) },
                        ),
                        tinted = true,
                    ) {
                        NodeTitle("config servers", StatusTone.Neutral, sh?.configRsName)
                        sh?.configHosts.orEmpty().forEach { HostLine(it) }
                    }
                }
                val totalChunks = shards.mapNotNull { it.chunks }.sum()
                shards.forEach { s ->
                    val key = "s:${s.name}"
                    Node(
                        anchor = anchor(key),
                        selected = selected == key,
                        onSelect = { onSelect(key) },
                        menu = buildList {
                            add(MenuEntry("Copy hosts") { copy(s.hosts.joinToString(",")) })
                            actions.onRemoveShard?.let { rm ->
                                if (s.draining) add(MenuEntry("Drain status…") { rm(s) })
                                else add(MenuEntry("Drain & remove…", danger = true) { rm(s) })
                            }
                        },
                    ) {
                        NodeTitle(
                            s.name,
                            if (s.draining) StatusTone.Warn else StatusTone.Good,
                            s.rsName.takeIf { it != s.name },
                        )
                        if (s.draining) Small("draining")
                        s.hosts.forEach { HostLine(it) }
                        s.chunks?.let { n ->
                            Small("$n chunk${if (n == 1L) "" else "s"}")
                            if (totalChunks > 0) ChunkBar(n.toFloat() / totalChunks)
                        }
                    }
                }
                if (shards.isEmpty() && !hasConfig) {
                    Node({}) { Small("config database not readable with this user") }
                }
            }
        }
    }
}

@Composable
private fun ReplicaSetDiagram(
    snap: ClusterSnapshot,
    actions: TopologyActions,
    selected: String?,
    onSelect: (String) -> Unit,
) {
    val clipboard = LocalClipboardManager.current
    val copy: (String) -> Unit = { clipboard.setText(AnnotatedString(it)) }
    val members = snap.topology.members
    if (members.isEmpty()) {
        Small("replSetGetStatus not readable with this user")
        return
    }
    val cfgByHost = snap.rsConfig?.members?.associateBy { it.host }.orEmpty()
    val primaryMember = members.firstOrNull { it.state == "PRIMARY" }
    val rest = members.filter { it !== primaryMember }

    fun menuFor(m: MemberInfo): List<MenuEntry> = buildList {
        val cfg = cfgByHost[m.name]
        add(MenuEntry("Copy host") { copy(m.name) })
        actions.onDirectConnect?.let { dc -> add(MenuEntry("Connect directly") { dc(m.name) }) }
        if (m.state == "PRIMARY") {
            actions.onStepDown?.let { add(MenuEntry("Step down…", danger = true) { it() }) }
        }
        if (m.state == "SECONDARY") {
            actions.onFreeze?.let { fr -> add(MenuEntry("Freeze elections…") { fr(m.name) }) }
        }
        if (cfg != null && !cfg.arbiterOnly) {
            actions.onEditMember?.let { ed -> add(MenuEntry("Edit member…") { ed(cfg) }) }
        }
        if (cfg != null && m.state != "PRIMARY") {
            actions.onRemoveMember?.let { rm -> add(MenuEntry("Remove from set…", danger = true) { rm(cfg) }) }
        }
    }

    DiagramSurface(
        sources = listOfNotNull(primaryMember?.let { "m:${it.name}" }),
        targets = rest.map { m ->
            "m:${m.name}" to if (m.state == "ARBITER") LinkStyle.Dashed else LinkStyle.Solid
        },
        linkTone = { key ->
            val m = members.firstOrNull { "m:${it.name}" == key }
            when {
                m == null -> StatusTone.Neutral
                m.health != 1 || m.state == "DOWN" -> StatusTone.Bad
                (m.lagSeconds ?: 0) > 10 -> StatusTone.Warn
                else -> StatusTone.Neutral
            }
        },
        arrowsToTargets = true,
    ) { anchor ->
        Column(
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(44.dp),
        ) {
            primaryMember?.let { p ->
                val key = "m:${p.name}"
                NodeRow {
                    MemberNode(
                        p, cfgByHost[p.name],
                        anchor(key), selected == key,
                        { onSelect(key) }, menuFor(p),
                    )
                }
            }
            NodeRow {
                rest.forEach { m ->
                    val key = "m:${m.name}"
                    MemberNode(
                        m, cfgByHost[m.name],
                        anchor(key), selected == key,
                        { onSelect(key) }, menuFor(m),
                    )
                }
                if (primaryMember == null) {
                    Node({}, tinted = true) {
                        NodeTitle("no primary", StatusTone.Bad)
                        snap.topology.primary?.let { Small("last known: $it") }
                    }
                }
            }
        }
    }
}

@Composable
private fun StandaloneDiagram(
    endpoint: String?,
    actions: TopologyActions,
    selected: String?,
    onSelect: (String) -> Unit,
) {
    val clipboard = LocalClipboardManager.current
    Node(
        anchor = {},
        selected = selected == "solo",
        onSelect = { onSelect("solo") },
        menu = buildList {
            endpoint?.let { e -> add(MenuEntry("Copy host") { clipboard.setText(AnnotatedString(e)) }) }
        },
    ) {
        NodeTitle("mongod", StatusTone.Good)
        HostLine(endpoint ?: "standalone")
        Small("standalone — no replication")
    }
}

@Composable
private fun MemberNode(
    m: MemberInfo,
    cfg: RsMemberConfig?,
    anchor: (LayoutCoordinates) -> Unit,
    selected: Boolean,
    onSelect: () -> Unit,
    menu: List<MenuEntry>,
) {
    val tone = when (m.state) {
        "PRIMARY" -> StatusTone.Good
        "SECONDARY" -> if ((m.lagSeconds ?: 0) > 10) StatusTone.Warn else StatusTone.Neutral
        "ARBITER" -> StatusTone.Neutral
        else -> StatusTone.Bad
    }
    Node(anchor, selected = selected, onSelect = onSelect, menu = menu) {
        NodeTitle(m.state.lowercase(), tone)
        HostLine(m.name)
        val detail = buildList {
            m.lagSeconds?.takeIf { m.state == "SECONDARY" }?.let { add("lag ${it}s") }
            m.pingMs?.let { add("$it ms") }
        }
        if (detail.isNotEmpty()) Small(detail.joinToString(" · "))
        val flags = buildList {
            if (cfg?.hidden == true) add("hidden" to Color(0xFFFACC15))
            if ((cfg?.secondaryDelaySecs ?: 0) > 0) add("delayed" to Color(0xFF60A5FA))
            if (cfg != null && cfg.votes == 0 && !cfg.arbiterOnly) add("non-voting" to Color(0xFFF87171))
        }
        if (flags.isNotEmpty()) {
            Row(horizontalArrangement = Arrangement.spacedBy(4.dp)) {
                flags.forEach { (t, c) -> FlagBadge(t, c) }
            }
        }
    }
}

/* ============================== detail strip ============================== */

@Composable
private fun DetailStrip(p: Payload) {
    Row(
        modifier = Modifier.fillMaxWidth().padding(horizontal = 20.dp, vertical = 12.dp),
        horizontalArrangement = Arrangement.spacedBy(24.dp),
        verticalAlignment = Alignment.Top,
    ) {
        when (p) {
            is Payload.Member -> {
                KV("state", p.m.state)
                KV("host", p.m.name)
                KV("uptime", formatUptime(p.m.uptime))
                p.m.pingMs?.let { KV("ping", "$it ms") }
                p.m.lagSeconds?.let { KV("lag", "${it}s") }
                KV("priority", formatPriority(p.cfg?.priority ?: p.m.priority))
                KV("votes", "${p.cfg?.votes ?: p.m.votes}")
                p.cfg?.let { c ->
                    if (c.hidden) KV("hidden", "yes")
                    if (c.secondaryDelaySecs > 0) KV("delay", "${c.secondaryDelaySecs}s")
                    if (c.tags.isNotEmpty()) KV("tags", c.tags.entries.joinToString { "${it.key}=${it.value}" })
                }
            }
            is Payload.Router -> {
                KV("role", "mongos router")
                KV("host", p.r.host)
                p.r.version?.let { KV("version", it) }
                KV("last ping", p.r.lastPingAgeSecs?.let { formatAge(it) } ?: "unknown")
            }
            is Payload.Shard -> {
                KV("shard", p.s.name)
                p.s.rsName?.let { KV("replica set", it) }
                KV("members", "${p.s.hosts.size}")
                KV("hosts", p.s.hosts.joinToString(", "))
                p.s.chunks?.let { n ->
                    KV("chunks", "$n")
                    p.total?.takeIf { it > 0 }?.let { t -> KV("share", "${(n * 100 / t)}%") }
                }
                if (p.s.draining) KV("status", "draining")
            }
            is Payload.ConfigSvr -> {
                KV("role", "config server replica set (CSRS)")
                p.rsName?.let { KV("replica set", it) }
                KV("hosts", p.hosts.joinToString(", "))
            }
            is Payload.Single -> {
                KV("role", "standalone mongod")
                p.endpoint?.let { KV("host", it) }
            }
        }
    }
}

@Composable
private fun KV(label: String, value: String) {
    Column(verticalArrangement = Arrangement.spacedBy(2.dp)) {
        Text(
            label.uppercase(),
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Text(value, fontFamily = FontFamily.Monospace, style = MaterialTheme.typography.bodySmall)
    }
}

/* ====================== wiring surface + connectors ====================== */

private enum class LinkStyle { Solid, Dashed }
private enum class StatusTone { Good, Warn, Bad, Neutral }

@Composable
private fun DiagramSurface(
    sources: List<String>,
    targets: List<Pair<String, LinkStyle>>,
    linkTone: (String) -> StatusTone = { StatusTone.Neutral },
    arrowsToTargets: Boolean = false,
    content: @Composable ((String) -> (LayoutCoordinates) -> Unit) -> Unit,
) {
    val bounds = remember { mutableStateMapOf<String, Rect>() }
    var root by remember { mutableStateOf<LayoutCoordinates?>(null) }

    val neutral = MaterialTheme.colorScheme.onSurfaceVariant.copy(alpha = 0.45f)
    val toneColors = mapOf(
        StatusTone.Good to Color(0xFF4ADE80),
        StatusTone.Warn to Color(0xFFFACC15),
        StatusTone.Bad to Color(0xFFF87171),
        StatusTone.Neutral to neutral,
    )

    Box(modifier = Modifier.onGloballyPositioned { root = it }) {
        Canvas(modifier = Modifier.matchParentSize()) {
            drawWires(bounds, sources, targets, linkTone, toneColors, neutral, arrowsToTargets)
        }
        content { key ->
            { coords ->
                root?.let { r ->
                    bounds[key] = Rect(r.localPositionOf(coords, Offset.Zero), coords.size.toSize())
                }
            }
        }
    }
}

private fun DrawScope.drawWires(
    bounds: Map<String, Rect>,
    sources: List<String>,
    targets: List<Pair<String, LinkStyle>>,
    linkTone: (String) -> StatusTone,
    toneColors: Map<StatusTone, Color>,
    busColor: Color,
    arrowsToTargets: Boolean,
) {
    val srcRects = sources.mapNotNull { bounds[it] }
    val tgtRects = targets.mapNotNull { (k, _) -> bounds[k] }
    if (tgtRects.isEmpty()) return

    val stroke = 1.5.dp.toPx()
    val dash = PathEffect.dashPathEffect(floatArrayOf(6.dp.toPx(), 4.dp.toPx()))

    val srcBottom = srcRects.maxOfOrNull { it.bottom }
    val tgtTop = tgtRects.minOf { it.top }
    val busY = if (srcBottom != null) (srcBottom + tgtTop) / 2f else tgtTop - 16.dp.toPx()

    val xs = buildList {
        srcRects.forEach { add(it.center.x) }
        targets.forEach { (k, _) -> bounds[k]?.let { add(it.center.x) } }
    }
    if (xs.size > 1) {
        drawLine(busColor, Offset(xs.min(), busY), Offset(xs.max(), busY), stroke)
    }
    srcRects.forEach {
        drawLine(busColor, Offset(it.center.x, it.bottom), Offset(it.center.x, busY), stroke)
    }
    targets.forEach { (key, style) ->
        val r = bounds[key] ?: return@forEach
        val color = toneColors[linkTone(key)] ?: busColor
        drawLine(
            color = color,
            start = Offset(r.center.x, busY),
            end = Offset(r.center.x, r.top),
            strokeWidth = stroke,
            pathEffect = if (style == LinkStyle.Dashed) dash else null,
        )
        if (arrowsToTargets) {
            val w = 4.dp.toPx()
            val h = 6.dp.toPx()
            drawPath(
                Path().apply {
                    moveTo(r.center.x, r.top)
                    lineTo(r.center.x - w, r.top - h)
                    lineTo(r.center.x + w, r.top - h)
                    close()
                },
                color,
            )
        }
    }
}

/* ============================== node pieces ============================== */

@Composable
private fun NodeRow(content: @Composable RowScope.() -> Unit) {
    Row(
        horizontalArrangement = Arrangement.spacedBy(20.dp),
        verticalAlignment = Alignment.Top,
        content = content,
    )
}

@Composable
private fun Node(
    anchor: (LayoutCoordinates) -> Unit,
    selected: Boolean = false,
    onSelect: (() -> Unit)? = null,
    menu: List<MenuEntry> = emptyList(),
    tinted: Boolean = false,
    content: @Composable ColumnScope.() -> Unit,
) {
    var menuOpen by remember { mutableStateOf(false) }
    val borderColor =
        if (selected) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.outlineVariant
    Box {
        Column(
            modifier = Modifier
                .onGloballyPositioned(anchor)
                .background(
                    if (tinted) MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.5f)
                    else MaterialTheme.colorScheme.surface,
                    RoundedCornerShape(10.dp),
                )
                .border(if (selected) 1.5.dp else 1.dp, borderColor, RoundedCornerShape(10.dp))
                .then(if (onSelect != null) Modifier.clickable(onClick = onSelect) else Modifier)
                .padding(start = 14.dp, end = if (menu.isEmpty()) 14.dp else 6.dp, top = 10.dp, bottom = 10.dp)
                .widthIn(min = 130.dp, max = 250.dp),
            verticalArrangement = Arrangement.spacedBy(3.dp),
        ) {
            if (menu.isEmpty()) {
                content()
            } else {
                Row(verticalAlignment = Alignment.Top) {
                    Column(
                        modifier = Modifier.weight(1f, fill = false),
                        verticalArrangement = Arrangement.spacedBy(3.dp),
                    ) { content() }
                    Icon(
                        Icons.Default.MoreVert,
                        contentDescription = "Node actions",
                        tint = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier
                            .size(18.dp)
                            .clickable { menuOpen = true },
                    )
                }
            }
        }
        DropdownMenu(expanded = menuOpen, onDismissRequest = { menuOpen = false }) {
            menu.forEach { entry ->
                DropdownMenuItem(
                    text = {
                        Text(
                            entry.label,
                            style = MaterialTheme.typography.bodySmall,
                            color = if (entry.danger) MaterialTheme.colorScheme.error
                            else MaterialTheme.colorScheme.onSurface,
                        )
                    },
                    onClick = { menuOpen = false; entry.onClick() },
                )
            }
        }
    }
}

@Composable
private fun NodeTitle(text: String, tone: StatusTone, sub: String? = null) {
    val dot = when (tone) {
        StatusTone.Good -> Color(0xFF4ADE80)
        StatusTone.Warn -> Color(0xFFFACC15)
        StatusTone.Bad -> Color(0xFFF87171)
        StatusTone.Neutral -> MaterialTheme.colorScheme.onSurfaceVariant
    }
    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)) {
        Box(modifier = Modifier.size(7.dp).background(dot, CircleShape))
        Text(
            text.uppercase(),
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        sub?.let {
            Text(
                it,
                style = MaterialTheme.typography.labelSmall,
                fontFamily = FontFamily.Monospace,
                color = MaterialTheme.colorScheme.primary,
            )
        }
    }
}

@Composable
private fun HostLine(host: String) {
    Text(
        host,
        fontFamily = FontFamily.Monospace,
        style = MaterialTheme.typography.bodySmall,
        maxLines = 1,
        overflow = TextOverflow.Ellipsis,
    )
}

@Composable
private fun Small(text: String) {
    Text(
        text,
        style = MaterialTheme.typography.labelSmall,
        color = MaterialTheme.colorScheme.onSurfaceVariant,
    )
}

/**
 * Thin proportional bar: this shard's slice of all chunks. Fixed width — fillMaxWidth
 * inside the node's widthIn(max) column inflated every shard node to maximum width.
 */
@Composable
private fun ChunkBar(fraction: Float) {
    Box(
        modifier = Modifier
            .width(120.dp)
            .height(4.dp)
            .background(MaterialTheme.colorScheme.outlineVariant.copy(alpha = 0.4f), RoundedCornerShape(2.dp)),
    ) {
        Box(
            modifier = Modifier
                .fillMaxWidth(fraction.coerceIn(0f, 1f))
                .height(4.dp)
                .background(MaterialTheme.colorScheme.primary, RoundedCornerShape(2.dp)),
        )
    }
}

@Composable
private fun FlagBadge(text: String, color: Color) {
    Text(
        text,
        style = MaterialTheme.typography.labelSmall,
        color = color,
        modifier = Modifier
            .background(color.copy(alpha = 0.12f), RoundedCornerShape(4.dp))
            .padding(horizontal = 5.dp, vertical = 1.dp),
    )
}

private fun formatAge(seconds: Long): String = when {
    seconds < 60 -> "${seconds}s ago"
    seconds < 3600 -> "${seconds / 60}m ago"
    seconds < 86400 -> "${seconds / 3600}h ago"
    else -> "${seconds / 86400}d ago"
}

private fun formatPriority(p: Double): String =
    if (p == p.toLong().toDouble()) "${p.toLong()}" else "$p"

private fun formatUptime(seconds: Long): String = when {
    seconds < 60 -> "${seconds}s"
    seconds < 3600 -> "${seconds / 60}m"
    seconds < 86400 -> "${seconds / 3600}h"
    else -> "${seconds / 86400}d"
}
