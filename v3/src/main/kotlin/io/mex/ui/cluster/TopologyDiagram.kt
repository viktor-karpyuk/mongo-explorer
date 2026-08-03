package io.mex.ui.cluster

import androidx.compose.foundation.Canvas
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.geometry.Offset
import androidx.compose.ui.geometry.Rect
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.PathEffect
import androidx.compose.ui.graphics.drawscope.DrawScope
import androidx.compose.ui.layout.LayoutCoordinates
import androidx.compose.ui.layout.onGloballyPositioned
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.toSize
import io.mex.mongo.ClusterSnapshot
import io.mex.mongo.MemberInfo
import io.mex.mongo.RouterInfo

/**
 * Node-and-wire picture of the cluster: routers over config/shards for sharded
 * clusters, primary over the other members for replica sets, a single node for
 * standalones. Wires are drawn on a canvas behind the nodes from their measured
 * positions, so row wrapping and resize keep the picture consistent.
 */
@Composable
fun ClusterTopologyDiagram(snap: ClusterSnapshot) {
    when (snap.topology.type) {
        "sharded" -> ShardedDiagram(snap)
        "replicaset" -> ReplicaSetDiagram(snap.topology.members, snap.topology.primary)
        else -> StandaloneDiagram(snap.endpoint)
    }
}

/* ============================ layouts per type ============================ */

@Composable
private fun ShardedDiagram(snap: ClusterSnapshot) {
    val sh = snap.sharded
    // config.mongos can be unreadable (auth) — fall back to the endpoint we're on.
    val routers = sh?.routers?.takeIf { it.isNotEmpty() }
        ?: listOfNotNull(snap.endpoint?.let { RouterInfo(it, 0, null) })
    val hasConfig = !sh?.configHosts.isNullOrEmpty()
    val shards = sh?.shards.orEmpty()

    DiagramSurface(
        sources = routers.indices.map { "r$it" },
        targets = buildList {
            if (hasConfig) add("cfg" to LinkStyle.Dashed)
            shards.indices.forEach { add("s$it" to LinkStyle.Solid) }
        },
    ) { anchor ->
        Column(
            modifier = Modifier.fillMaxWidth(),
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(44.dp),
        ) {
            NodeRow {
                routers.forEachIndexed { i, r ->
                    Node(anchor("r$i")) {
                        NodeTitle("mongos", if (r.active) StatusTone.Good else StatusTone.Bad)
                        HostLine(r.host)
                        r.version?.let { Small(it) }
                        if (!r.active) Small("last ping ${r.lastPingAgeSecs?.let { formatAge(it) } ?: "unknown"}")
                    }
                }
            }
            NodeRow {
                if (hasConfig) {
                    Node(anchor("cfg"), tinted = true) {
                        NodeTitle("config servers", StatusTone.Neutral, sh?.configRsName)
                        sh?.configHosts.orEmpty().forEach { HostLine(it) }
                    }
                }
                shards.forEachIndexed { i, s ->
                    Node(anchor("s$i")) {
                        NodeTitle(
                            s.name,
                            if (s.draining) StatusTone.Warn else StatusTone.Good,
                            s.rsName.takeIf { it != s.name },
                        )
                        if (s.draining) Small("draining")
                        s.hosts.forEach { HostLine(it) }
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
private fun ReplicaSetDiagram(members: List<MemberInfo>, primary: String?) {
    val primaryMember = members.firstOrNull { it.state == "PRIMARY" }
    val rest = members.filter { it !== primaryMember }
    if (members.isEmpty()) {
        Small("replSetGetStatus not readable with this user")
        return
    }

    DiagramSurface(
        sources = listOfNotNull(primaryMember?.let { "p" }),
        targets = rest.indices.map { i ->
            "m$i" to if (rest[i].state == "ARBITER") LinkStyle.Dashed else LinkStyle.Solid
        },
        linkTone = { key ->
            val m = rest[key.removePrefix("m").toInt()]
            when {
                m.health != 1 || m.state == "DOWN" -> StatusTone.Bad
                (m.lagSeconds ?: 0) > 10 -> StatusTone.Warn
                else -> StatusTone.Neutral
            }
        },
    ) { anchor ->
        Column(
            modifier = Modifier.fillMaxWidth(),
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(44.dp),
        ) {
            primaryMember?.let { p ->
                NodeRow { MemberNode(p, anchor("p")) }
            }
            NodeRow {
                rest.forEachIndexed { i, m -> MemberNode(m, anchor("m$i")) }
                // Primary listed but unreachable: primaryMember is null yet the set knows one.
                if (primaryMember == null && primary != null) {
                    Node({}, tinted = true) {
                        NodeTitle("no primary", StatusTone.Bad)
                        Small("last known: $primary")
                    }
                }
            }
        }
    }
}

@Composable
private fun StandaloneDiagram(endpoint: String?) {
    Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.Center) {
        Node({}) {
            NodeTitle("mongod", StatusTone.Good)
            HostLine(endpoint ?: "standalone")
            Small("standalone — no replication")
        }
    }
}

@Composable
private fun MemberNode(m: MemberInfo, anchor: (LayoutCoordinates) -> Unit) {
    val tone = when (m.state) {
        "PRIMARY" -> StatusTone.Good
        "SECONDARY" -> if ((m.lagSeconds ?: 0) > 10) StatusTone.Warn else StatusTone.Neutral
        "ARBITER" -> StatusTone.Neutral
        else -> StatusTone.Bad
    }
    Node(anchor) {
        NodeTitle(m.state.lowercase(), tone)
        HostLine(m.name)
        val detail = buildList {
            m.lagSeconds?.takeIf { m.state == "SECONDARY" }?.let { add("lag ${it}s") }
            m.pingMs?.let { add("$it ms") }
            if (m.votes == 0 && m.state != "ARBITER") add("non-voting")
        }
        if (detail.isNotEmpty()) Small(detail.joinToString(" · "))
    }
}

/* ====================== wiring surface + connectors ====================== */

private enum class LinkStyle { Solid, Dashed }
private enum class StatusTone { Good, Warn, Bad, Neutral }

/**
 * Hosts the node rows and draws source→bus→target wires behind them. [sources]
 * hang from the top row, [targets] from the rows below; the bus sits in the gap
 * between the lowest source and the highest target.
 */
@Composable
private fun DiagramSurface(
    sources: List<String>,
    targets: List<Pair<String, LinkStyle>>,
    linkTone: (String) -> StatusTone = { StatusTone.Neutral },
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

    Box(modifier = Modifier.fillMaxWidth().onGloballyPositioned { root = it }) {
        Canvas(modifier = Modifier.matchParentSize()) {
            drawWires(bounds, sources, targets, linkTone, toneColors, neutral)
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
) {
    val srcRects = sources.mapNotNull { bounds[it] }
    val tgtRects = targets.mapNotNull { (k, _) -> bounds[k] }
    if (tgtRects.isEmpty()) return

    val stroke = 1.5.dp.toPx()
    val dash = PathEffect.dashPathEffect(floatArrayOf(6.dp.toPx(), 4.dp.toPx()))

    // The bus lives in the whitespace between the source block and target block;
    // with no sources it hugs the targets so stubs stay short.
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
        drawLine(
            color = toneColors[linkTone(key)] ?: busColor,
            start = Offset(r.center.x, busY),
            end = Offset(r.center.x, r.top),
            strokeWidth = stroke,
            pathEffect = if (style == LinkStyle.Dashed) dash else null,
        )
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
    tinted: Boolean = false,
    content: @Composable ColumnScope.() -> Unit,
) {
    Column(
        modifier = Modifier
            .onGloballyPositioned(anchor)
            .background(
                if (tinted) MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.5f)
                else MaterialTheme.colorScheme.surface,
                RoundedCornerShape(10.dp),
            )
            .border(1.dp, MaterialTheme.colorScheme.outlineVariant, RoundedCornerShape(10.dp))
            .padding(horizontal = 14.dp, vertical = 10.dp)
            .widthIn(min = 130.dp, max = 240.dp),
        verticalArrangement = Arrangement.spacedBy(3.dp),
        content = content,
    )
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

private fun formatAge(seconds: Long): String = when {
    seconds < 60 -> "${seconds}s ago"
    seconds < 3600 -> "${seconds / 60}m ago"
    seconds < 86400 -> "${seconds / 3600}h ago"
    else -> "${seconds / 86400}d ago"
}
