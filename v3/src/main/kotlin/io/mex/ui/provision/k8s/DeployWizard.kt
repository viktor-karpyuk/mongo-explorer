package io.mex.ui.provision.k8s

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import io.mex.AppContext
import io.mex.backup.ToolInfo
import io.mex.data.BackupChoice
import io.mex.data.K8sDeploySpec
import io.mex.data.K8sOperator
import io.mex.data.K8sProfile
import io.mex.data.K8sTopology
import io.mex.data.PreflightResult
import io.mex.data.TlsChoice
import io.mex.provision.k8s.DetectedOperator
import io.mex.provision.k8s.K8S_MONGO_VERSIONS
import io.mex.provision.k8s.KubeTarget
import io.mex.provision.k8s.OperatorCapability
import io.mex.provision.k8s.bundleHash
import io.mex.provision.k8s.bundleText
import io.mex.provision.k8s.capabilities
import io.mex.provision.k8s.detectOperators
import io.mex.provision.k8s.listContexts
import io.mex.provision.k8s.listNamespaces
import io.mex.provision.k8s.listStorageClasses
import io.mex.provision.k8s.render
import io.mex.provision.k8s.runK8sPreflight
import io.mex.provision.k8s.shortHash
import io.mex.provision.k8s.validate
import io.mex.ui.components.TypedConfirmDialog
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * The provisioning wizard (K8P-UI-2). Everything funnels into one [K8sDeploySpec];
 * the preview renders it, hashes it, and the typed confirm authorizes *that* hash —
 * any later edit invalidates it (K8P-CR-2).
 */
@Composable
fun DeployWizard(
    ctx: AppContext,
    tool: ToolInfo,
    onClose: () -> Unit,
    onApply: (K8sDeploySpec, String) -> Unit,
) {
    var step by remember { mutableStateOf(0) }
    var name by remember { mutableStateOf("") }
    var context by remember { mutableStateOf("") }
    var namespace by remember { mutableStateOf("") }
    var namespaceCreated by remember { mutableStateOf(false) }
    var operator by remember { mutableStateOf<K8sOperator?>(null) }
    var profile by remember { mutableStateOf(K8sProfile.dev) }
    var shape by remember { mutableStateOf("rs") }
    var members by remember { mutableStateOf(3) }
    var shards by remember { mutableStateOf(2) }
    var perShard by remember { mutableStateOf(3) }
    var mongos by remember { mutableStateOf(2) }
    var version by remember { mutableStateOf(K8S_MONGO_VERSIONS.first()) }
    var storageClass by remember { mutableStateOf("") }
    var storageGi by remember { mutableStateOf(20) }
    var cpu by remember { mutableStateOf("1") }
    var memory by remember { mutableStateOf("2Gi") }
    var tlsMode by remember { mutableStateOf("certManager") }
    var issuer by remember { mutableStateOf("") }
    var caSecret by remember { mutableStateOf("") }
    var pbmEndpoint by remember { mutableStateOf("") }
    var pbmBucket by remember { mutableStateOf("") }
    var pbmSecret by remember { mutableStateOf("") }
    var byoDeclared by remember { mutableStateOf(false) }

    var contexts by remember { mutableStateOf<List<String>>(emptyList()) }
    var namespaces by remember { mutableStateOf<List<String>>(emptyList()) }
    var storageClasses by remember { mutableStateOf<List<String>>(emptyList()) }
    var detected by remember { mutableStateOf<List<DetectedOperator>>(emptyList()) }
    var probing by remember { mutableStateOf(false) }
    var preflight by remember { mutableStateOf<PreflightResult?>(null) }
    var preflighting by remember { mutableStateOf(false) }
    var confirming by remember { mutableStateOf(false) }

    LaunchedEffect(Unit) {
        contexts = withContext(Dispatchers.IO) { listContexts(tool) }
        context = contexts.firstOrNull().orEmpty()
    }
    LaunchedEffect(context) {
        if (context.isBlank()) return@LaunchedEffect
        probing = true
        val target = KubeTarget(context)
        namespaces = withContext(Dispatchers.IO) { listNamespaces(tool, target) }
        detected = withContext(Dispatchers.IO) { detectOperators(tool, target) }
        storageClasses = withContext(Dispatchers.IO) { listStorageClasses(tool, target) }
        operator = detected.firstOrNull()?.operator
        probing = false
    }

    val topology: K8sTopology = if (shape == "rs") {
        K8sTopology.ReplicaSet(members)
    } else {
        K8sTopology.Sharded(shards, perShard, mongos, 3)
    }
    val tls: TlsChoice = when (tlsMode) {
        "certManager" -> TlsChoice.CertManager(issuer.trim())
        "byoCa" -> TlsChoice.ByoCa(caSecret.trim())
        "selfSigned" -> TlsChoice.OperatorSelfSigned
        else -> TlsChoice.Off
    }
    val backup: BackupChoice = when {
        operator == K8sOperator.psmdb && pbmEndpoint.isNotBlank() ->
            BackupChoice.Pbm(pbmEndpoint.trim(), pbmBucket.trim(), pbmSecret.trim())
        operator == K8sOperator.mco && byoDeclared -> BackupChoice.ByoDeclared
        profile == K8sProfile.dev -> BackupChoice.None
        else -> BackupChoice.None
    }

    val spec = K8sDeploySpec(
        name = name.trim(), context = context, namespace = namespace.trim(),
        namespaceCreated = namespaceCreated,
        operator = operator ?: K8sOperator.psmdb, profile = profile, topology = topology,
        mongoVersion = version,
        storageClass = storageClass.ifBlank { null }, storageGi = storageGi,
        cpu = cpu, memory = memory, tls = tls, backup = backup,
    )
    val violations = validate(spec)
    val nameTaken = spec.name.isNotBlank() &&
        ctx.k8sDeployments.nameExists(spec.context, spec.namespace, spec.name)
    val docs = remember(spec) { if (violations.isEmpty()) runCatching { render(spec) }.getOrNull() else null }
    val hash = remember(docs) { docs?.let { bundleHash(it) } }
    // Any edit re-renders and re-hashes; the previous confirmation dies with it.
    LaunchedEffect(hash) { confirming = false }

    val canPreview = violations.isEmpty() && !nameTaken && preflight?.ok == true

    AlertDialog(
        onDismissRequest = onClose,
        title = {
            Text(
                "New deployment — ${STEP_TITLES[step]} (${step + 1}/${STEP_TITLES.size})",
                style = MaterialTheme.typography.titleMedium,
            )
        },
        confirmButton = {
            if (step < STEP_TITLES.lastIndex) {
                Button(onClick = { step++ }, enabled = stepReady(step, context, namespace, name, operator, violations)) {
                    Text("Continue")
                }
            } else {
                Button(onClick = { confirming = true }, enabled = canPreview && hash != null) { Text("Apply…") }
            }
        },
        dismissButton = {
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                TextButton(onClick = onClose) { Text("Cancel") }
                if (step > 0) OutlinedButton(onClick = { step-- }) { Text("Back") }
            }
        },
        text = {
            Column(
                modifier = Modifier.width(660.dp).heightIn(max = 560.dp).verticalScroll(rememberScrollState()),
                verticalArrangement = Arrangement.spacedBy(10.dp),
            ) {
                if (probing) LinearProgressIndicator(modifier = Modifier.fillMaxWidth())
                when (step) {
                    0 -> ContextStep(
                        contexts, context, { context = it },
                        namespaces, namespace, { namespace = it; namespaceCreated = it !in namespaces },
                        namespaceCreated,
                    )
                    1 -> OperatorStep(detected, operator) { operator = it }
                    2 -> ProfileStep(profile) { profile = it }
                    3 -> TopologyStep(
                        operator, profile, shape, { shape = it },
                        members, { members = it }, shards, { shards = it },
                        perShard, { perShard = it }, mongos, { mongos = it },
                        version, { version = it },
                    )
                    4 -> StorageStep(
                        storageClasses, storageClass, { storageClass = it },
                        storageGi, { storageGi = it }, cpu, { cpu = it }, memory, { memory = it }, profile,
                    )
                    5 -> SecurityStep(
                        operator, profile, tlsMode, { tlsMode = it },
                        issuer, { issuer = it }, caSecret, { caSecret = it }, name, { name = it },
                        nameTaken,
                    )
                    6 -> BackupStep(
                        operator, profile,
                        pbmEndpoint, { pbmEndpoint = it }, pbmBucket, { pbmBucket = it },
                        pbmSecret, { pbmSecret = it }, byoDeclared, { byoDeclared = it },
                    )
                    7 -> PreflightStep(
                        preflight, preflighting,
                        onRun = {
                            preflighting = true
                            preflight = withContext(Dispatchers.IO) { runK8sPreflight(tool, spec) }
                            preflighting = false
                        },
                    )
                    8 -> PreviewStep(docs, hash, violations, nameTaken)
                }
                if (violations.isNotEmpty() && step >= 3) {
                    violations.forEach {
                        Text(it, color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.labelSmall)
                    }
                }
            }
        },
    )

    if (confirming && hash != null) {
        TypedConfirmDialog(
            title = "Apply to ${spec.context}?",
            consequence = "This applies ${docs?.size ?: 0} documents to context \"${spec.context}\", " +
                "namespace \"${spec.namespace}\"" +
                (if (spec.profile == K8sProfile.prod) ". Deletion protection will be ON." else "."),
            requiredText = spec.name,
            confirmLabel = "Apply",
            onConfirm = {
                confirming = false
                onApply(spec, hash)
            },
            onCancel = { confirming = false },
        )
    }
}

private val STEP_TITLES = listOf(
    "Context & namespace", "Operator", "Profile", "Topology",
    "Storage & resources", "Security", "Backups", "Preflight", "Preview",
)

private fun stepReady(
    step: Int,
    context: String,
    namespace: String,
    name: String,
    operator: K8sOperator?,
    violations: List<String>,
): Boolean = when (step) {
    0 -> context.isNotBlank() && namespace.isNotBlank()
    1 -> operator != null
    5 -> name.isNotBlank() && violations.none { it.startsWith("Name") }
    else -> true
}

/* ===================== steps ===================== */

@Composable
private fun ContextStep(
    contexts: List<String>,
    context: String,
    onContext: (String) -> Unit,
    namespaces: List<String>,
    namespace: String,
    onNamespace: (String) -> Unit,
    willCreate: Boolean,
) {
    Caption("The app never changes your current-context or edits kubeconfig.")
    Picker("Context", contexts, context, onContext)
    OutlinedTextField(
        value = namespace,
        onValueChange = onNamespace,
        label = { Text("Namespace") },
        singleLine = true,
        modifier = Modifier.fillMaxWidth(),
        textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
    )
    if (namespaces.isNotEmpty()) {
        Row(horizontalArrangement = Arrangement.spacedBy(6.dp), modifier = Modifier.fillMaxWidth()) {
            namespaces.take(6).forEach { ns ->
                OutlinedButton(
                    onClick = { onNamespace(ns) },
                    contentPadding = PaddingValues(horizontal = 8.dp, vertical = 2.dp),
                ) { Text(ns, style = MaterialTheme.typography.labelSmall) }
            }
        }
    }
    if (willCreate && namespace.isNotBlank()) {
        Caption("Namespace \"$namespace\" will be created and removed at teardown if empty.")
    }
}

@Composable
private fun OperatorStep(detected: List<DetectedOperator>, selected: K8sOperator?, onSelect: (K8sOperator) -> Unit) {
    if (detected.isEmpty()) {
        Text(
            "No supported operator detected in this cluster.",
            color = MaterialTheme.colorScheme.error,
            style = MaterialTheme.typography.bodySmall,
        )
        Caption("Install the MongoDB Community Operator or the Percona Server for MongoDB Operator, then re-open this wizard. This app detects operators; it never installs them.")
        return
    }
    detected.forEach { d ->
        val caps = capabilities(d.operator)
        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            RadioButton(selected = selected == d.operator, onClick = { onSelect(d.operator) })
            Column {
                Text(
                    "${d.operator.name.uppercase()} ${d.version ?: "(version unknown)"}",
                    style = MaterialTheme.typography.bodyMedium,
                )
                Text(
                    caps.joinToString(" · ") { it.name.lowercase().replace('_', ' ') },
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
        }
    }
    if (detected.none { it.operator == K8sOperator.psmdb }) {
        Caption("Sharded clusters need the Percona operator — MCO cannot express sharding.")
    }
}

@Composable
private fun ProfileStep(profile: K8sProfile, onSelect: (K8sProfile) -> Unit) {
    Row(verticalAlignment = Alignment.CenterVertically) {
        RadioButton(selected = profile == K8sProfile.dev, onClick = { onSelect(K8sProfile.dev) })
        Column {
            Text("DEV", style = MaterialTheme.typography.bodyMedium, fontWeight = FontWeight.SemiBold)
            Caption("Flexible preset. Self-signed TLS allowed, resources optional, volumes deleted on teardown.")
        }
    }
    Row(verticalAlignment = Alignment.Top) {
        RadioButton(selected = profile == K8sProfile.prod, onClick = { onSelect(K8sProfile.prod) })
        Column {
            Text("PROD", style = MaterialTheme.typography.bodyMedium, fontWeight = FontWeight.SemiBold)
            Caption("Strict mode. These locks cannot be turned off:")
            Column(
                modifier = Modifier
                    .padding(top = 4.dp)
                    .background(MaterialTheme.colorScheme.surfaceVariant.copy(alpha = 0.3f), RoundedCornerShape(6.dp))
                    .padding(8.dp),
            ) {
                listOf(
                    "TLS (cert-manager or your own CA)" to "auth with a 24-char root password",
                    "members ≥ 3" to "explicit StorageClass",
                    "resource requests = limits" to "PodDisruptionBudget maxUnavailable 1",
                    "zone + host anti-affinity" to "backups wired",
                    "deletion protection" to "data volumes kept on teardown",
                ).forEach { (l, r) ->
                    Row {
                        LockItem(l, Modifier.weight(1f))
                        LockItem(r, Modifier.weight(1f))
                    }
                }
            }
            Caption("\"Prod locks what makes it Prod. Every lock is a guarantee your future self collects on.\"")
        }
    }
}

@Composable
private fun LockItem(text: String, modifier: Modifier = Modifier) {
    Text(
        "✓ $text",
        style = MaterialTheme.typography.labelSmall,
        color = Color(0xFF4ADE80),
        modifier = modifier.padding(end = 8.dp, bottom = 2.dp),
    )
}

@Composable
private fun TopologyStep(
    operator: K8sOperator?,
    profile: K8sProfile,
    shape: String,
    onShape: (String) -> Unit,
    members: Int,
    onMembers: (Int) -> Unit,
    shards: Int,
    onShards: (Int) -> Unit,
    perShard: Int,
    onPerShard: (Int) -> Unit,
    mongos: Int,
    onMongos: (Int) -> Unit,
    version: String,
    onVersion: (String) -> Unit,
) {
    val canShard = operator?.let { OperatorCapability.SHARDED in capabilities(it) } == true
    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        FilterChip(selected = shape == "rs", onClick = { onShape("rs") }, label = { Text("Replica set") })
        FilterChip(
            selected = shape == "sharded",
            onClick = { if (canShard) onShape("sharded") },
            enabled = canShard,
            label = { Text("Sharded") },
        )
    }
    if (!canShard) Caption("ⓘ Sharded is disabled — MCO cannot express sharded clusters.")
    val memberOptions = if (profile == K8sProfile.prod) listOf(3, 5) else listOf(1, 3)
    if (shape == "rs") {
        ChipRow("Members", memberOptions, members, onMembers)
    } else {
        Stepper("Shards", shards, 1, 8, onShards)
        ChipRow("Members per shard", memberOptions, perShard, onPerShard)
        Stepper("Mongos routers", mongos, if (profile == K8sProfile.prod) 2 else 1, 4, onMongos)
        Caption("Config servers are fixed at 3 — the production shape.")
    }
    Picker("MongoDB version", K8S_MONGO_VERSIONS, version, onVersion)
}

@Composable
private fun StorageStep(
    classes: List<String>,
    storageClass: String,
    onClass: (String) -> Unit,
    gi: Int,
    onGi: (Int) -> Unit,
    cpu: String,
    onCpu: (String) -> Unit,
    memory: String,
    onMemory: (String) -> Unit,
    profile: K8sProfile,
) {
    if (classes.isNotEmpty()) {
        Picker("StorageClass", classes, storageClass.ifBlank { classes.first() }, onClass)
    } else {
        OutlinedTextField(
            value = storageClass, onValueChange = onClass,
            label = { Text("StorageClass") }, singleLine = true, modifier = Modifier.fillMaxWidth(),
        )
    }
    if (profile == K8sProfile.prod) Caption("Prod requires an explicit StorageClass — no silent cluster default.")
    Stepper("Storage per member (Gi)", gi, 1, 2000, onGi, step = 10)
    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        OutlinedTextField(
            value = cpu, onValueChange = onCpu, label = { Text("CPU") },
            singleLine = true, modifier = Modifier.weight(1f),
        )
        OutlinedTextField(
            value = memory, onValueChange = onMemory, label = { Text("Memory") },
            singleLine = true, modifier = Modifier.weight(1f),
        )
    }
    Caption("Requests and limits are pinned to the same values — a throttled database fails at the worst moment.")
}

@Composable
private fun SecurityStep(
    operator: K8sOperator?,
    profile: K8sProfile,
    mode: String,
    onMode: (String) -> Unit,
    issuer: String,
    onIssuer: (String) -> Unit,
    caSecret: String,
    onCaSecret: (String) -> Unit,
    name: String,
    onName: (String) -> Unit,
    nameTaken: Boolean,
) {
    OutlinedTextField(
        value = name, onValueChange = onName,
        label = { Text("Deployment name") },
        singleLine = true, isError = nameTaken,
        modifier = Modifier.fillMaxWidth(),
        textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
    )
    if (nameTaken) {
        Text("A deployment with this name already exists here.", color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.labelSmall)
    }
    Text("TLS", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
    Row(horizontalArrangement = Arrangement.spacedBy(6.dp)) {
        FilterChip(selected = mode == "certManager", onClick = { onMode("certManager") }, label = { Text("cert-manager") })
        FilterChip(selected = mode == "byoCa", onClick = { onMode("byoCa") }, label = { Text("Your CA") })
        if (profile == K8sProfile.dev) {
            if (operator == K8sOperator.psmdb) {
                FilterChip(selected = mode == "selfSigned", onClick = { onMode("selfSigned") }, label = { Text("Self-signed") })
            }
            FilterChip(selected = mode == "off", onClick = { onMode("off") }, label = { Text("Off") })
        }
    }
    when (mode) {
        "certManager" -> OutlinedTextField(
            value = issuer, onValueChange = onIssuer,
            label = { Text("Issuer name (in this namespace)") },
            singleLine = true, modifier = Modifier.fillMaxWidth(),
        )
        "byoCa" -> OutlinedTextField(
            value = caSecret, onValueChange = onCaSecret,
            label = { Text("CA Secret name") },
            singleLine = true, modifier = Modifier.fillMaxWidth(),
        )
    }
    Caption("A 24-character root password is generated and stored in a Kubernetes Secret. It is shown once, then only inside the encrypted connection.")
}

@Composable
private fun BackupStep(
    operator: K8sOperator?,
    profile: K8sProfile,
    endpoint: String,
    onEndpoint: (String) -> Unit,
    bucket: String,
    onBucket: (String) -> Unit,
    secret: String,
    onSecret: (String) -> Unit,
    byoDeclared: Boolean,
    onByoDeclared: (Boolean) -> Unit,
) {
    if (operator == K8sOperator.psmdb) {
        Caption("PBM writes to S3-compatible storage. Prod requires it; dev may leave it empty.")
        OutlinedTextField(value = endpoint, onValueChange = onEndpoint, label = { Text("S3 endpoint") }, singleLine = true, modifier = Modifier.fillMaxWidth())
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            OutlinedTextField(value = bucket, onValueChange = onBucket, label = { Text("Bucket") }, singleLine = true, modifier = Modifier.weight(1f))
            OutlinedTextField(value = secret, onValueChange = onSecret, label = { Text("Credentials Secret") }, singleLine = true, modifier = Modifier.weight(1f))
        }
        Caption("Schedule: nightly full at 01:00 with continuous PITR.")
    } else {
        Caption("MCO has no native backup integration.")
        Row(verticalAlignment = Alignment.CenterVertically) {
            Checkbox(checked = byoDeclared, onCheckedChange = onByoDeclared)
            Text(
                "Backups for this deployment are handled outside this app.",
                style = MaterialTheme.typography.bodySmall,
            )
        }
        if (profile == K8sProfile.prod) {
            Caption("Prod requires this acknowledgement — it is recorded and shown on the deployment card.")
        }
    }
}

@Composable
private fun PreflightStep(result: PreflightResult?, running: Boolean, onRun: suspend () -> Unit) {
    var trigger by remember { mutableStateOf(0) }
    LaunchedEffect(trigger) { if (trigger > 0) onRun() }
    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        Button(onClick = { trigger++ }, enabled = !running) { Text(if (result == null) "Run preflight" else "Re-run") }
        if (running) CircularProgressIndicator(modifier = Modifier.size(16.dp), strokeWidth = 2.dp)
    }
    result?.checks?.forEach { c ->
        val mark = when { c.warn -> "⚠"; c.ok -> "✓"; else -> "✗" }
        Text(
            "$mark ${c.name}${c.detail?.let { " — $it" } ?: ""}",
            style = MaterialTheme.typography.labelSmall,
            fontFamily = FontFamily.Monospace,
            color = when {
                c.warn -> Color(0xFFB45309)
                c.ok -> Color(0xFF4ADE80)
                else -> MaterialTheme.colorScheme.error
            },
        )
    }
    if (result != null && !result.ok) {
        Caption("Blocking checks must pass before the bundle can be applied.")
    }
}

@Composable
private fun PreviewStep(
    docs: List<io.mex.provision.k8s.YamlDoc>?,
    hash: String?,
    violations: List<String>,
    nameTaken: Boolean,
) {
    if (docs == null || hash == null) {
        Text(
            if (nameTaken) "Choose a unique name to preview." else "Resolve the issues above to preview.",
            color = MaterialTheme.colorScheme.error,
            style = MaterialTheme.typography.bodySmall,
        )
        return
    }
    Text(
        "${docs.size} documents · ${shortHash(hash)}",
        style = MaterialTheme.typography.labelSmall,
        fontFamily = FontFamily.Monospace,
        color = MaterialTheme.colorScheme.onSurfaceVariant,
    )
    Caption("This is exactly what will be applied. Editing any field re-renders and invalidates the confirmation.")
    MonoScrollPane(bundleText(docs).lines(), maxHeight = 340)
}

/* ===================== small controls ===================== */

@Composable
private fun Caption(text: String) {
    Text(text, style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
}

@Composable
private fun Picker(label: String, options: List<String>, selected: String, onSelect: (String) -> Unit) {
    var open by remember { mutableStateOf(false) }
    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        Text(label, style = MaterialTheme.typography.bodySmall, modifier = Modifier.width(140.dp))
        Box {
            OutlinedButton(onClick = { open = true }) {
                Text(selected.ifBlank { "(none)" }, style = MaterialTheme.typography.labelMedium)
            }
            DropdownMenu(expanded = open, onDismissRequest = { open = false }) {
                options.forEach { o ->
                    DropdownMenuItem(text = { Text(o) }, onClick = { open = false; onSelect(o) })
                }
            }
        }
    }
}

@Composable
private fun ChipRow(label: String, options: List<Int>, selected: Int, onSelect: (Int) -> Unit) {
    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)) {
        Text(label, style = MaterialTheme.typography.bodySmall, modifier = Modifier.width(140.dp))
        options.forEach { o ->
            FilterChip(selected = selected == o, onClick = { onSelect(o) }, label = { Text("$o") })
        }
    }
}

@Composable
private fun Stepper(label: String, value: Int, min: Int, max: Int, onChange: (Int) -> Unit, step: Int = 1) {
    Row(verticalAlignment = Alignment.CenterVertically) {
        Text(label, style = MaterialTheme.typography.bodySmall, modifier = Modifier.width(180.dp))
        OutlinedButton(
            onClick = { onChange((value - step).coerceAtLeast(min)) },
            enabled = value > min,
            contentPadding = PaddingValues(0.dp),
            modifier = Modifier.size(28.dp),
        ) { Text("−") }
        Text(
            "$value",
            style = MaterialTheme.typography.bodyMedium,
            fontFamily = FontFamily.Monospace,
            modifier = Modifier.width(48.dp),
            textAlign = androidx.compose.ui.text.style.TextAlign.Center,
        )
        OutlinedButton(
            onClick = { onChange((value + step).coerceAtMost(max)) },
            enabled = value < max,
            contentPadding = PaddingValues(0.dp),
            modifier = Modifier.size(28.dp),
        ) { Text("+") }
    }
}
