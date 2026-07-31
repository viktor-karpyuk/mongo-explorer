package io.mex.mongo

import com.mongodb.client.MongoClient
import org.bson.Document

/* ============================ models ============================ */

data class RoleRef(val role: String, val db: String) {
    override fun toString(): String = "$role@$db"
}

data class MongoUser(
    val user: String,
    val db: String,
    val roles: List<RoleRef>,
    val mechanisms: List<String>,
) {
    val id: String get() = "$user@$db"

    /** Roles that make this account effectively an estate-wide superuser. */
    val superuser: Boolean
        get() = roles.any {
            it.role == "root" ||
                (it.db == "admin" && it.role in setOf("userAdminAnyDatabase", "dbOwner", "userAdmin"))
        }
}

data class PrivResource(
    val db: String? = null,
    val collection: String? = null,
    val cluster: Boolean = false,
    val anyResource: Boolean = false,
)

data class Privilege(val resource: PrivResource, val actions: List<String>) {
    fun describe(): String {
        val res = when {
            resource.anyResource -> "anyResource"
            resource.cluster -> "cluster"
            resource.db.isNullOrEmpty() && resource.collection.isNullOrEmpty() -> "all databases"
            resource.collection.isNullOrEmpty() -> resource.db ?: "?"
            resource.db.isNullOrEmpty() -> "*.${resource.collection}"
            else -> "${resource.db}.${resource.collection}"
        }
        return "$res → ${actions.joinToString(",")}"
    }
}

data class MongoRole(
    val role: String,
    val db: String,
    val isBuiltin: Boolean,
    val inherited: List<RoleRef>,
    val privileges: List<Privilege>,
) {
    val id: String get() = "$role@$db"
}

/** Common built-in roles offered by the role picker. */
val BUILTIN_ROLES = listOf(
    "read", "readWrite", "dbAdmin", "dbOwner", "userAdmin",
    "clusterAdmin", "clusterManager", "clusterMonitor", "hostManager",
    "backup", "restore",
    "readAnyDatabase", "readWriteAnyDatabase", "userAdminAnyDatabase", "dbAdminAnyDatabase",
    "root",
)

val WRITE_ACTIONS = setOf("insert", "update", "remove")

/* ============================ queries ============================ */

/** Every user in the deployment (`usersInfo.forAllDBs`) — needs `viewUser` on all DBs. */
fun listAllUsers(client: MongoClient): List<MongoUser> {
    val res = client.getDatabase("admin")
        .runCommand(Document("usersInfo", Document("forAllDBs", true)))
    return (res["users"] as? List<*>).orEmpty().filterIsInstance<Document>().map { it.toUser() }
}

private fun Document.toUser(): MongoUser = MongoUser(
    user = getString("user") ?: "?",
    db = getString("db") ?: "?",
    roles = (this["roles"] as? List<*>).orEmpty().filterIsInstance<Document>().map {
        RoleRef(it.getString("role") ?: "?", it.getString("db") ?: "?")
    },
    mechanisms = (this["mechanisms"] as? List<*>).orEmpty().map { it.toString() },
)

/**
 * The user's fully-resolved privilege set (`inheritedPrivileges`) — the server expands
 * builtin and nested roles for us, which keeps the access check honest.
 */
fun fetchUserPrivileges(client: MongoClient, user: MongoUser): List<Privilege> {
    val res = client.getDatabase("admin").runCommand(
        Document("usersInfo", Document("user", user.user).append("db", user.db))
            .append("showPrivileges", true),
    )
    val doc = (res["users"] as? List<*>).orEmpty().filterIsInstance<Document>().firstOrNull()
        ?: return emptyList()
    return (doc["inheritedPrivileges"] as? List<*>).orEmpty()
        .filterIsInstance<Document>()
        .map { it.toPrivilege() }
}

private fun Document.toPrivilege(): Privilege {
    val r = this["resource"] as? Document ?: Document()
    return Privilege(
        resource = PrivResource(
            db = r.getString("db"),
            collection = r.getString("collection"),
            cluster = r["cluster"] == true,
            anyResource = r["anyResource"] == true,
        ),
        actions = (this["actions"] as? List<*>).orEmpty().map { it.toString() },
    )
}

/** Custom (user-defined) roles of every database, privileges expanded. */
fun listCustomRoles(client: MongoClient): List<MongoRole> {
    val out = mutableListOf<MongoRole>()
    for (db in listDatabases(client).map { it.name } + "admin") {
        val res = runCatching {
            client.getDatabase(db).runCommand(
                Document("rolesInfo", 1).append("showPrivileges", true),
            )
        }.getOrNull() ?: continue
        (res["roles"] as? List<*>).orEmpty().filterIsInstance<Document>().forEach { doc ->
            val role = MongoRole(
                role = doc.getString("role") ?: return@forEach,
                db = doc.getString("db") ?: db,
                isBuiltin = doc["isBuiltin"] == true,
                inherited = (doc["roles"] as? List<*>).orEmpty().filterIsInstance<Document>().map {
                    RoleRef(it.getString("role") ?: "?", it.getString("db") ?: "?")
                },
                privileges = (doc["privileges"] as? List<*>).orEmpty()
                    .filterIsInstance<Document>()
                    .map { it.toPrivilege() },
            )
            if (out.none { it.id == role.id }) out += role
        }
    }
    return out.sortedWith(compareBy({ it.db }, { it.role }))
}

/* ============================ mutations ============================ */

private fun rolesArg(roles: List<RoleRef>): List<Document> =
    roles.map { Document("role", it.role).append("db", it.db) }

fun createUser(client: MongoClient, db: String, user: String, password: String, roles: List<RoleRef>) {
    client.getDatabase(db).runCommand(
        Document("createUser", user).append("pwd", password).append("roles", rolesArg(roles)),
    )
}

fun dropUser(client: MongoClient, user: MongoUser) {
    client.getDatabase(user.db).runCommand(Document("dropUser", user.user))
}

/** Replaces the user's role set wholesale — simpler and more predictable than grant/revoke diffs. */
fun updateUserRoles(client: MongoClient, user: MongoUser, roles: List<RoleRef>) {
    client.getDatabase(user.db).runCommand(
        Document("updateUser", user.user).append("roles", rolesArg(roles)),
    )
}

fun changeUserPassword(client: MongoClient, user: MongoUser, password: String) {
    client.getDatabase(user.db).runCommand(
        Document("updateUser", user.user).append("pwd", password),
    )
}

fun createRole(
    client: MongoClient,
    db: String,
    role: String,
    inherited: List<RoleRef>,
    privilegesJson: String,
) {
    val privileges = privilegesJson.trim().ifBlank { "[]" }.let {
        Document.parse("""{"p": $it}""")["p"] as? List<*> ?: emptyList<Any>()
    }
    client.getDatabase(db).runCommand(
        Document("createRole", role)
            .append("privileges", privileges)
            .append("roles", rolesArg(inherited)),
    )
}

fun dropRole(client: MongoClient, role: MongoRole) {
    client.getDatabase(role.db).runCommand(Document("dropRole", role.role))
}

/* ===================== pure logic (tested) ===================== */

/**
 * Whether a privilege resource covers `db.coll` under MongoDB's resource-document rules:
 * `{anyResource}` covers everything, `{cluster}` covers no namespace, `{db:"",collection:""}`
 * covers every non-system namespace, an empty db means "this collection in every database"
 * and an empty collection means "every collection of this database".
 */
fun resourceMatches(res: PrivResource, db: String, coll: String): Boolean = when {
    res.anyResource -> true
    res.cluster -> false
    res.db.isNullOrEmpty() && res.collection.isNullOrEmpty() -> true
    res.db.isNullOrEmpty() -> res.collection == coll
    res.collection.isNullOrEmpty() -> res.db == db
    else -> res.db == db && res.collection == coll
}

/** The privileges out of [privileges] that grant at least one of [actions] on `db.coll`. */
fun grantsOn(privileges: List<Privilege>, db: String, coll: String, actions: Set<String>): List<Privilege> =
    privileges.filter { p ->
        resourceMatches(p.resource, db, coll) && p.actions.any { it in actions }
    }

data class SecurityInsights(
    val superusers: List<MongoUser>,
    val usersWithoutRoles: List<MongoUser>,
    val unusedCustomRoles: List<MongoRole>,
)

/** The "dangerous accounts" summary shown above the user list (DBA-RBAC-4). */
fun computeInsights(users: List<MongoUser>, roles: List<MongoRole>): SecurityInsights {
    val referenced = users.flatMap { it.roles }.map { "${it.role}@${it.db}" }.toSet() +
        roles.flatMap { r -> r.inherited.map { "${it.role}@${it.db}" } }
    return SecurityInsights(
        superusers = users.filter { it.superuser },
        usersWithoutRoles = users.filter { it.roles.isEmpty() },
        unusedCustomRoles = roles.filter { !it.isBuiltin && it.id !in referenced },
    )
}
