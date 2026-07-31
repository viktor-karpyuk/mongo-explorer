package io.mex.ui.security

import androidx.compose.foundation.VerticalScrollbar
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.rememberScrollbarAdapter
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.selection.SelectionContainer
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import io.mex.mongo.*
import io.mex.ui.components.ConfirmDangerDialog
import io.mex.ui.components.ReadOnlyBadge
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext

private enum class SecTab { Users, Roles, Access, Audit }

/**
 * Users & roles administration (DBA-RBAC-1..4): estate-wide user list with dangerous-account
 * insights, role browser with expanded privileges, and the inverse question — "who can do
 * X on this namespace" — answered from server-resolved privilege sets.
 */
@Composable
fun SecurityPanel(connectionId: String, registry: MongoRegistry, readOnly: Boolean) {
    var tab by remember(connectionId) { mutableStateOf(SecTab.Users) }
    var users by remember(connectionId) { mutableStateOf<List<MongoUser>>(emptyList()) }
    var roles by remember(connectionId) { mutableStateOf<List<MongoRole>>(emptyList()) }
    var dbs by remember(connectionId) { mutableStateOf<List<String>>(emptyList()) }
    var error by remember(connectionId) { mutableStateOf<String?>(null) }
    var loading by remember(connectionId) { mutableStateOf(false) }
    val scope = rememberCoroutineScope()

    fun load() {
        scope.launch {
            val client = registry.client(connectionId) ?: return@launch
            loading = true
            try {
                withContext(Dispatchers.IO) {
                    users = listAllUsers(client)
                    roles = listCustomRoles(client)
                    dbs = (listDatabases(client).map { it.name } + "admin").distinct().sorted()
                }
                error = null
            } catch (e: Exception) {
                error = e.message
            } finally {
                loading = false
            }
        }
    }
    LaunchedEffect(connectionId) { load() }

    val insights = remember(users, roles) { computeInsights(users, roles) }

    Column(modifier = Modifier.fillMaxSize().padding(horizontal = 24.dp, vertical = 16.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(10.dp)) {
            Text("Security", style = MaterialTheme.typography.headlineSmall)
            if (readOnly) ReadOnlyBadge()
            Spacer(modifier = Modifier.weight(1f))
            Text(
                "${users.size} user(s) · ${roles.count { !it.isBuiltin }} custom role(s)",
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
            OutlinedButton(onClick = { load() }, enabled = !loading) {
                Text(if (loading) "Loading…" else "Refresh")
            }
        }
        error?.let {
            Text(
                "$it — listing users needs the viewUser privilege (or an auth-less deployment).",
                color = MaterialTheme.colorScheme.error,
                style = MaterialTheme.typography.bodySmall,
            )
        }

        // Dangerous-account insights, always visible above the tabs.
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp), modifier = Modifier.padding(vertical = 6.dp)) {
            InsightChip(
                count = insights.superusers.size,
                label = "superuser account(s)",
                tint = Color(0xFFF87171),
                detail = insights.superusers.joinToString { it.id },
            )
            InsightChip(
                count = insights.usersWithoutRoles.size,
                label = "user(s) without roles",
                tint = Color(0xFFFBBF24),
                detail = insights.usersWithoutRoles.joinToString { it.id },
            )
            InsightChip(
                count = insights.unusedCustomRoles.size,
                label = "unused custom role(s)",
                tint = Color(0xFFFBBF24),
                detail = insights.unusedCustomRoles.joinToString { it.id },
            )
        }

        Row {
            SecTabBtn("Users", tab == SecTab.Users) { tab = SecTab.Users }
            SecTabBtn("Roles", tab == SecTab.Roles) { tab = SecTab.Roles }
            SecTabBtn("Access check", tab == SecTab.Access) { tab = SecTab.Access }
            SecTabBtn("Audit", tab == SecTab.Audit) { tab = SecTab.Audit }
        }
        HorizontalDivider()
        Box(modifier = Modifier.weight(1f)) {
            when (tab) {
                SecTab.Users -> UsersTab(connectionId, registry, users, roles, dbs, readOnly, onChanged = { load() })
                SecTab.Roles -> RolesTab(connectionId, registry, roles, dbs, readOnly, onChanged = { load() })
                SecTab.Access -> AccessTab(connectionId, registry, users, dbs)
                SecTab.Audit -> AuditTab(connectionId, registry)
            }
        }
    }
}

@Composable
private fun InsightChip(count: Int, label: String, tint: Color, detail: String) {
    if (count == 0) return
    val text = "$count $label"
    Text(
        if (detail.isBlank() || detail.length > 60) text else "$text — $detail",
        style = MaterialTheme.typography.labelSmall,
        color = tint,
        maxLines = 1,
        overflow = TextOverflow.Ellipsis,
        modifier = Modifier
            .background(tint.copy(alpha = 0.12f), RoundedCornerShape(4.dp))
            .padding(horizontal = 6.dp, vertical = 2.dp),
    )
}

@Composable
private fun SecTabBtn(label: String, active: Boolean, onClick: () -> Unit) {
    val color = if (active) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant
    TextButton(onClick = onClick) { Text(label, color = color, style = MaterialTheme.typography.labelMedium) }
}

/* ============================ Users ============================ */

@Composable
private fun UsersTab(
    connectionId: String,
    registry: MongoRegistry,
    users: List<MongoUser>,
    roles: List<MongoRole>,
    dbs: List<String>,
    readOnly: Boolean,
    onChanged: () -> Unit,
) {
    var creating by remember { mutableStateOf(false) }
    var editingRoles by remember { mutableStateOf<MongoUser?>(null) }
    var changingPwd by remember { mutableStateOf<MongoUser?>(null) }
    var dropping by remember { mutableStateOf<MongoUser?>(null) }
    var opError by remember { mutableStateOf<String?>(null) }
    val scope = rememberCoroutineScope()

    fun run(block: suspend () -> Unit) {
        scope.launch {
            try {
                withContext(Dispatchers.IO) { block() }
                opError = null
                onChanged()
            } catch (e: Exception) {
                opError = e.message
            }
        }
    }

    Column(modifier = Modifier.fillMaxSize()) {
        Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(vertical = 6.dp)) {
            opError?.let {
                Text(it, color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.bodySmall, modifier = Modifier.weight(1f))
            } ?: Spacer(modifier = Modifier.weight(1f))
            if (!readOnly) {
                Button(onClick = { creating = true }) { Text("+ Create user") }
            }
        }
        if (users.isEmpty()) {
            Text(
                "No users found — the deployment may run without authentication.",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(vertical = 12.dp),
            )
        }
        val listState = rememberLazyListState()
        Box(modifier = Modifier.weight(1f)) {
            LazyColumn(state = listState, verticalArrangement = Arrangement.spacedBy(6.dp)) {
                items(users, key = { it.id }) { u ->
                    UserRow(
                        u = u,
                        readOnly = readOnly,
                        onEditRoles = { editingRoles = u },
                        onChangePwd = { changingPwd = u },
                        onDrop = { dropping = u },
                    )
                }
            }
            VerticalScrollbar(
                adapter = rememberScrollbarAdapter(listState),
                modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
            )
        }
    }

    if (creating) {
        UserDialog(
            title = "Create user",
            dbs = dbs,
            customRoles = roles.filter { !it.isBuiltin },
            initialRoles = emptyList(),
            askPassword = true,
            onCancel = { creating = false },
            onSubmit = { db, name, pwd, picked ->
                creating = false
                run {
                    val client = registry.client(connectionId) ?: error("Not connected")
                    createUser(client, db, name, pwd, picked)
                }
            },
        )
    }
    editingRoles?.let { u ->
        UserDialog(
            title = "Roles of ${u.id}",
            dbs = dbs,
            customRoles = roles.filter { !it.isBuiltin },
            initialRoles = u.roles,
            askPassword = false,
            fixedIdentity = u,
            onCancel = { editingRoles = null },
            onSubmit = { _, _, _, picked ->
                editingRoles = null
                run {
                    val client = registry.client(connectionId) ?: error("Not connected")
                    updateUserRoles(client, u, picked)
                }
            },
        )
    }
    changingPwd?.let { u ->
        PasswordDialog(
            user = u,
            onCancel = { changingPwd = null },
            onSubmit = { pwd ->
                changingPwd = null
                run {
                    val client = registry.client(connectionId) ?: error("Not connected")
                    changeUserPassword(client, u, pwd)
                }
            },
        )
    }
    dropping?.let { u ->
        ConfirmDangerDialog(
            title = "Drop user ${u.id}?",
            text = "This sends { dropUser: \"${u.user}\" } to database \"${u.db}\". " +
                "Applications authenticating as this user stop working immediately. This cannot be undone.",
            confirmLabel = "Drop user",
            onConfirm = {
                val captured = u
                dropping = null
                run {
                    val client = registry.client(connectionId) ?: error("Not connected")
                    dropUser(client, captured)
                }
            },
            onCancel = { dropping = null },
        )
    }
}

@Composable
private fun UserRow(
    u: MongoUser,
    readOnly: Boolean,
    onEditRoles: () -> Unit,
    onChangePwd: () -> Unit,
    onDrop: () -> Unit,
) {
    var menu by remember { mutableStateOf(false) }
    Card(border = CardDefaults.outlinedCardBorder()) {
        Row(
            modifier = Modifier.fillMaxWidth().padding(horizontal = 12.dp, vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically,
        ) {
            Column(modifier = Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(3.dp)) {
                Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    Text(u.user, style = MaterialTheme.typography.bodyMedium, fontFamily = FontFamily.Monospace)
                    Text("@${u.db}", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                    if (u.superuser) Chip("superuser", Color(0xFFF87171))
                    if (u.roles.isEmpty()) Chip("no roles", Color(0xFFFBBF24))
                }
                Row(horizontalArrangement = Arrangement.spacedBy(4.dp)) {
                    u.roles.take(8).forEach { Chip(it.toString(), MaterialTheme.colorScheme.primary) }
                    if (u.roles.size > 8) {
                        Text("+${u.roles.size - 8} more", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                    }
                }
            }
            Text(
                u.mechanisms.joinToString(","),
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(end = 8.dp),
            )
            if (!readOnly) {
                TextButton(onClick = { menu = true }, contentPadding = PaddingValues(0.dp), modifier = Modifier.size(24.dp)) {
                    Text("⋯")
                }
                DropdownMenu(expanded = menu, onDismissRequest = { menu = false }) {
                    DropdownMenuItem(text = { Text("Edit roles…") }, onClick = { menu = false; onEditRoles() })
                    DropdownMenuItem(text = { Text("Change password…") }, onClick = { menu = false; onChangePwd() })
                    HorizontalDivider()
                    DropdownMenuItem(
                        text = { Text("Drop user", color = MaterialTheme.colorScheme.error) },
                        onClick = { menu = false; onDrop() },
                    )
                }
            }
        }
    }
}

/* ============================ Roles ============================ */

@Composable
private fun RolesTab(
    connectionId: String,
    registry: MongoRegistry,
    roles: List<MongoRole>,
    dbs: List<String>,
    readOnly: Boolean,
    onChanged: () -> Unit,
) {
    var creating by remember { mutableStateOf(false) }
    var dropping by remember { mutableStateOf<MongoRole?>(null) }
    var opError by remember { mutableStateOf<String?>(null) }
    val scope = rememberCoroutineScope()

    Column(modifier = Modifier.fillMaxSize()) {
        Row(verticalAlignment = Alignment.CenterVertically, modifier = Modifier.padding(vertical = 6.dp)) {
            opError?.let {
                Text(it, color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.bodySmall, modifier = Modifier.weight(1f))
            } ?: Spacer(modifier = Modifier.weight(1f))
            if (!readOnly) {
                Button(onClick = { creating = true }) { Text("+ Create role") }
            }
        }
        if (roles.isEmpty()) {
            Text(
                "No custom roles defined.",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(vertical = 12.dp),
            )
        }
        val listState = rememberLazyListState()
        Box(modifier = Modifier.weight(1f)) {
            LazyColumn(state = listState, verticalArrangement = Arrangement.spacedBy(6.dp)) {
                items(roles, key = { it.id }) { r ->
                    RoleRow(r, readOnly, onDrop = { dropping = r })
                }
            }
            VerticalScrollbar(
                adapter = rememberScrollbarAdapter(listState),
                modifier = Modifier.align(Alignment.CenterEnd).fillMaxHeight(),
            )
        }
    }

    if (creating) {
        CreateRoleDialog(
            dbs = dbs,
            onCancel = { creating = false },
            onSubmit = { db, name, inherited, privJson ->
                creating = false
                scope.launch {
                    try {
                        val client = registry.client(connectionId) ?: error("Not connected")
                        withContext(Dispatchers.IO) { createRole(client, db, name, inherited, privJson) }
                        opError = null
                        onChanged()
                    } catch (e: Exception) {
                        opError = e.message
                    }
                }
            },
        )
    }
    dropping?.let { r ->
        ConfirmDangerDialog(
            title = "Drop role ${r.id}?",
            text = "This sends { dropRole: \"${r.role}\" } to database \"${r.db}\". " +
                "Users granted this role lose its privileges immediately. This cannot be undone.",
            confirmLabel = "Drop role",
            onConfirm = {
                val captured = r
                dropping = null
                scope.launch {
                    try {
                        val client = registry.client(connectionId) ?: error("Not connected")
                        withContext(Dispatchers.IO) { dropRole(client, captured) }
                        opError = null
                        onChanged()
                    } catch (e: Exception) {
                        opError = e.message
                    }
                }
            },
            onCancel = { dropping = null },
        )
    }
}

@Composable
private fun RoleRow(r: MongoRole, readOnly: Boolean, onDrop: () -> Unit) {
    var expanded by remember(r.id) { mutableStateOf(false) }
    Card(border = CardDefaults.outlinedCardBorder()) {
        Column(modifier = Modifier.fillMaxWidth().clickable { expanded = !expanded }.padding(horizontal = 12.dp, vertical = 8.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Text(if (expanded) "▾" else "▸", style = MaterialTheme.typography.labelSmall)
                Text(r.role, style = MaterialTheme.typography.bodyMedium, fontFamily = FontFamily.Monospace)
                Text("@${r.db}", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                Chip(if (r.isBuiltin) "builtin" else "custom", if (r.isBuiltin) MaterialTheme.colorScheme.onSurfaceVariant else MaterialTheme.colorScheme.tertiary)
                Spacer(modifier = Modifier.weight(1f))
                Text(
                    "${r.privileges.size} privilege(s) · ${r.inherited.size} inherited",
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
                if (!readOnly && !r.isBuiltin) {
                    TextButton(onClick = onDrop, contentPadding = PaddingValues(horizontal = 6.dp)) {
                        Text("Drop", color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.labelSmall)
                    }
                }
            }
            if (expanded) {
                SelectionContainer {
                    Column(modifier = Modifier.padding(start = 20.dp, top = 4.dp), verticalArrangement = Arrangement.spacedBy(1.dp)) {
                        if (r.inherited.isNotEmpty()) {
                            Text(
                                "inherits: ${r.inherited.joinToString()}",
                                style = MaterialTheme.typography.labelSmall,
                                fontFamily = FontFamily.Monospace,
                                color = MaterialTheme.colorScheme.primary,
                            )
                        }
                        r.privileges.take(20).forEach {
                            Text(
                                it.describe(),
                                style = MaterialTheme.typography.labelSmall,
                                fontFamily = FontFamily.Monospace,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                            )
                        }
                        if (r.privileges.size > 20) {
                            Text("… ${r.privileges.size - 20} more", style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
                        }
                    }
                }
            }
        }
    }
}

/* ============================ Access check ============================ */

private data class AccessHit(val user: MongoUser, val via: List<Privilege>)

@Composable
private fun AccessTab(
    connectionId: String,
    registry: MongoRegistry,
    users: List<MongoUser>,
    dbs: List<String>,
) {
    var db by remember { mutableStateOf("") }
    var coll by remember { mutableStateOf("") }
    var action by remember { mutableStateOf("write") }
    var actionMenu by remember { mutableStateOf(false) }
    var dbMenu by remember { mutableStateOf(false) }
    var checking by remember { mutableStateOf(false) }
    var hits by remember { mutableStateOf<List<AccessHit>?>(null) }
    var error by remember { mutableStateOf<String?>(null) }
    val scope = rememberCoroutineScope()

    val actions = mapOf(
        "read" to setOf("find"),
        "write" to WRITE_ACTIONS,
        "drop collection" to setOf("dropCollection"),
        "create index" to setOf("createIndex"),
    )

    Column(modifier = Modifier.fillMaxSize().padding(top = 8.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
        Text(
            "Who can perform an action on a namespace? Resolved from each user's server-expanded privilege set.",
            style = MaterialTheme.typography.bodySmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant,
        )
        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            Box {
                OutlinedButton(onClick = { dbMenu = true }) { Text(db.ifBlank { "database ⌄" }) }
                DropdownMenu(expanded = dbMenu, onDismissRequest = { dbMenu = false }) {
                    dbs.forEach { d ->
                        DropdownMenuItem(text = { Text(d) }, onClick = { dbMenu = false; db = d })
                    }
                }
            }
            OutlinedTextField(
                value = coll,
                onValueChange = { coll = it },
                label = { Text("Collection", style = MaterialTheme.typography.labelSmall) },
                singleLine = true,
                modifier = Modifier.width(220.dp),
                textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
            )
            Box {
                OutlinedButton(onClick = { actionMenu = true }) { Text("can $action ⌄") }
                DropdownMenu(expanded = actionMenu, onDismissRequest = { actionMenu = false }) {
                    actions.keys.forEach { a ->
                        DropdownMenuItem(text = { Text(a) }, onClick = { actionMenu = false; action = a })
                    }
                }
            }
            Button(
                enabled = db.isNotBlank() && coll.isNotBlank() && !checking && users.isNotEmpty(),
                onClick = {
                    scope.launch {
                        checking = true
                        try {
                            val client = registry.client(connectionId) ?: error("Not connected")
                            val wanted = actions[action] ?: emptySet()
                            hits = withContext(Dispatchers.IO) {
                                users.mapNotNull { u ->
                                    val privs = fetchUserPrivileges(client, u)
                                    val via = grantsOn(privs, db, coll, wanted)
                                    if (via.isEmpty()) null else AccessHit(u, via)
                                }
                            }
                            error = null
                        } catch (e: Exception) {
                            error = e.message
                        } finally {
                            checking = false
                        }
                    }
                },
            ) { Text(if (checking) "Checking…" else "Check") }
        }
        error?.let { Text(it, color = MaterialTheme.colorScheme.error, style = MaterialTheme.typography.bodySmall) }

        hits?.let { list ->
            if (list.isEmpty()) {
                Text(
                    "No user can $action on $db.$coll.",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            } else {
                Text(
                    "${list.size} user(s) can $action on $db.$coll:",
                    style = MaterialTheme.typography.titleSmall,
                )
                val listState = rememberLazyListState()
                Box(modifier = Modifier.weight(1f)) {
                    LazyColumn(state = listState, verticalArrangement = Arrangement.spacedBy(4.dp)) {
                        items(list, key = { it.user.id }) { hit ->
                            Card(border = CardDefaults.outlinedCardBorder()) {
                                Column(modifier = Modifier.fillMaxWidth().padding(horizontal = 12.dp, vertical = 6.dp)) {
                                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp), verticalAlignment = Alignment.CenterVertically) {
                                        Text(hit.user.id, style = MaterialTheme.typography.bodyMedium, fontFamily = FontFamily.Monospace)
                                        if (hit.user.superuser) Chip("superuser", Color(0xFFF87171))
                                        Spacer(modifier = Modifier.weight(1f))
                                        Text(
                                            hit.user.roles.joinToString(),
                                            style = MaterialTheme.typography.labelSmall,
                                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                                            maxLines = 1,
                                            overflow = TextOverflow.Ellipsis,
                                        )
                                    }
                                    hit.via.take(3).forEach {
                                        Text(
                                            "via ${it.describe()}",
                                            style = MaterialTheme.typography.labelSmall,
                                            fontFamily = FontFamily.Monospace,
                                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                                        )
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
        }
    }
}

/* ============================ dialogs ============================ */

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

/** Create-user and edit-roles share this dialog; [fixedIdentity] locks name+db when editing. */
@Composable
private fun UserDialog(
    title: String,
    dbs: List<String>,
    customRoles: List<MongoRole>,
    initialRoles: List<RoleRef>,
    askPassword: Boolean,
    fixedIdentity: MongoUser? = null,
    onCancel: () -> Unit,
    onSubmit: (db: String, name: String, password: String, roles: List<RoleRef>) -> Unit,
) {
    var name by remember { mutableStateOf(fixedIdentity?.user ?: "") }
    var db by remember { mutableStateOf(fixedIdentity?.db ?: "admin") }
    var pwd by remember { mutableStateOf("") }
    var pwd2 by remember { mutableStateOf("") }
    val picked = remember { mutableStateListOf<RoleRef>().apply { addAll(initialRoles) } }

    val pwdOk = !askPassword || (pwd.isNotEmpty() && pwd == pwd2)

    AlertDialog(
        onDismissRequest = onCancel,
        title = { Text(title, style = MaterialTheme.typography.titleMedium) },
        confirmButton = {
            Button(
                enabled = name.isNotBlank() && db.isNotBlank() && pwdOk,
                onClick = { onSubmit(db, name.trim(), pwd, picked.toList()) },
            ) { Text(if (fixedIdentity != null) "Save roles" else "Create") }
        },
        dismissButton = { TextButton(onClick = onCancel) { Text("Cancel") } },
        text = {
            Column(modifier = Modifier.width(480.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                if (fixedIdentity == null) {
                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        OutlinedTextField(
                            value = name,
                            onValueChange = { name = it },
                            label = { Text("Username") },
                            singleLine = true,
                            modifier = Modifier.weight(1f),
                        )
                        DbPicker("Auth DB", db, dbs) { db = it }
                    }
                }
                if (askPassword) {
                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        OutlinedTextField(
                            value = pwd,
                            onValueChange = { pwd = it },
                            label = { Text("Password") },
                            singleLine = true,
                            visualTransformation = PasswordVisualTransformation(),
                            modifier = Modifier.weight(1f),
                        )
                        OutlinedTextField(
                            value = pwd2,
                            onValueChange = { pwd2 = it },
                            label = { Text("Confirm") },
                            singleLine = true,
                            visualTransformation = PasswordVisualTransformation(),
                            isError = pwd2.isNotEmpty() && pwd != pwd2,
                            modifier = Modifier.weight(1f),
                        )
                    }
                }
                Text("Roles", style = MaterialTheme.typography.titleSmall)
                RolePicker(dbs = dbs, customRoles = customRoles, onAdd = { if (it !in picked) picked.add(it) })
                if (picked.isEmpty()) {
                    Text(
                        "No roles — the account will authenticate but be able to do nothing.",
                        style = MaterialTheme.typography.labelSmall,
                        color = Color(0xFFFBBF24),
                    )
                }
                Column(verticalArrangement = Arrangement.spacedBy(3.dp)) {
                    picked.forEach { r ->
                        Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                            Chip(r.toString(), MaterialTheme.colorScheme.primary)
                            if (r.role == "root") Chip("superuser", Color(0xFFF87171))
                            TextButton(
                                onClick = { picked.remove(r) },
                                contentPadding = PaddingValues(0.dp),
                                modifier = Modifier.size(20.dp),
                            ) { Text("✕", style = MaterialTheme.typography.labelSmall) }
                        }
                    }
                }
            }
        },
    )
}

@Composable
private fun RolePicker(dbs: List<String>, customRoles: List<MongoRole>, onAdd: (RoleRef) -> Unit) {
    var role by remember { mutableStateOf("readWrite") }
    var db by remember { mutableStateOf("admin") }
    var roleMenu by remember { mutableStateOf(false) }
    Row(verticalAlignment = Alignment.CenterVertically, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        Box {
            OutlinedButton(onClick = { roleMenu = true }) { Text(role) }
            DropdownMenu(expanded = roleMenu, onDismissRequest = { roleMenu = false }) {
                BUILTIN_ROLES.forEach { r ->
                    DropdownMenuItem(text = { Text(r) }, onClick = { roleMenu = false; role = r })
                }
                if (customRoles.isNotEmpty()) {
                    HorizontalDivider()
                    customRoles.forEach { r ->
                        DropdownMenuItem(
                            text = { Text("${r.role} (custom @${r.db})") },
                            onClick = { roleMenu = false; role = r.role; db = r.db },
                        )
                    }
                }
            }
        }
        DbPicker("on DB", db, dbs) { db = it }
        OutlinedButton(onClick = { onAdd(RoleRef(role, db)) }) { Text("Add") }
    }
}

@Composable
private fun DbPicker(label: String, value: String, dbs: List<String>, onPick: (String) -> Unit) {
    var open by remember { mutableStateOf(false) }
    Box {
        OutlinedButton(onClick = { open = true }) { Text("$label: $value") }
        DropdownMenu(expanded = open, onDismissRequest = { open = false }) {
            dbs.forEach { d ->
                DropdownMenuItem(text = { Text(d) }, onClick = { open = false; onPick(d) })
            }
        }
    }
}

@Composable
private fun PasswordDialog(user: MongoUser, onCancel: () -> Unit, onSubmit: (String) -> Unit) {
    var pwd by remember { mutableStateOf("") }
    var pwd2 by remember { mutableStateOf("") }
    AlertDialog(
        onDismissRequest = onCancel,
        title = { Text("Change password — ${user.id}", style = MaterialTheme.typography.titleMedium) },
        confirmButton = {
            Button(
                enabled = pwd.isNotEmpty() && pwd == pwd2,
                onClick = { onSubmit(pwd) },
            ) { Text("Change password") }
        },
        dismissButton = { TextButton(onClick = onCancel) { Text("Cancel") } },
        text = {
            Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                OutlinedTextField(
                    value = pwd,
                    onValueChange = { pwd = it },
                    label = { Text("New password") },
                    singleLine = true,
                    visualTransformation = PasswordVisualTransformation(),
                    modifier = Modifier.fillMaxWidth(),
                )
                OutlinedTextField(
                    value = pwd2,
                    onValueChange = { pwd2 = it },
                    label = { Text("Confirm") },
                    singleLine = true,
                    visualTransformation = PasswordVisualTransformation(),
                    isError = pwd2.isNotEmpty() && pwd != pwd2,
                    modifier = Modifier.fillMaxWidth(),
                )
            }
        },
    )
}

@Composable
private fun CreateRoleDialog(
    dbs: List<String>,
    onCancel: () -> Unit,
    onSubmit: (db: String, name: String, inherited: List<RoleRef>, privilegesJson: String) -> Unit,
) {
    var name by remember { mutableStateOf("") }
    var db by remember { mutableStateOf("admin") }
    var privJson by remember { mutableStateOf("") }
    val inherited = remember { mutableStateListOf<RoleRef>() }

    AlertDialog(
        onDismissRequest = onCancel,
        title = { Text("Create custom role", style = MaterialTheme.typography.titleMedium) },
        confirmButton = {
            Button(
                enabled = name.isNotBlank() && (inherited.isNotEmpty() || privJson.isNotBlank()),
                onClick = { onSubmit(db, name.trim(), inherited.toList(), privJson) },
            ) { Text("Create role") }
        },
        dismissButton = { TextButton(onClick = onCancel) { Text("Cancel") } },
        text = {
            Column(modifier = Modifier.width(520.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    OutlinedTextField(
                        value = name,
                        onValueChange = { name = it },
                        label = { Text("Role name") },
                        singleLine = true,
                        modifier = Modifier.weight(1f),
                    )
                    DbPicker("Defined on", db, dbs) { db = it }
                }
                Text("Inherited roles", style = MaterialTheme.typography.titleSmall)
                RolePicker(dbs = dbs, customRoles = emptyList(), onAdd = { if (it !in inherited) inherited.add(it) })
                Row(horizontalArrangement = Arrangement.spacedBy(4.dp)) {
                    inherited.forEach { r ->
                        Row(verticalAlignment = Alignment.CenterVertically) {
                            Chip(r.toString(), MaterialTheme.colorScheme.primary)
                            TextButton(
                                onClick = { inherited.remove(r) },
                                contentPadding = PaddingValues(0.dp),
                                modifier = Modifier.size(20.dp),
                            ) { Text("✕", style = MaterialTheme.typography.labelSmall) }
                        }
                    }
                }
                OutlinedTextField(
                    value = privJson,
                    onValueChange = { privJson = it },
                    label = { Text("Privileges JSON (optional array)") },
                    placeholder = {
                        Text(
                            """[{ "resource": { "db": "app", "collection": "" }, "actions": ["find"] }]""",
                            style = MaterialTheme.typography.bodySmall,
                        )
                    },
                    minLines = 3,
                    maxLines = 6,
                    modifier = Modifier.fillMaxWidth(),
                    textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
                )
            }
        },
    )
}
