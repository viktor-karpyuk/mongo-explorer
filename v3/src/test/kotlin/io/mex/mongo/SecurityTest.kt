package io.mex.mongo

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private fun user(name: String, db: String = "admin", vararg roles: RoleRef) =
    MongoUser(name, db, roles.toList(), listOf("SCRAM-SHA-256"))

private fun role(name: String, db: String = "admin", builtin: Boolean = false, inherited: List<RoleRef> = emptyList()) =
    MongoRole(name, db, builtin, inherited, emptyList())

class ResourceMatchesTest {
    @Test
    fun `anyResource covers every namespace`() {
        assertTrue(resourceMatches(PrivResource(anyResource = true), "app", "users"))
    }

    @Test
    fun `cluster resource covers no namespace`() {
        assertFalse(resourceMatches(PrivResource(cluster = true), "app", "users"))
    }

    @Test
    fun `empty db and collection means all normal namespaces`() {
        assertTrue(resourceMatches(PrivResource(db = "", collection = ""), "app", "users"))
    }

    @Test
    fun `db-wide resource covers every collection of that db only`() {
        val res = PrivResource(db = "app", collection = "")
        assertTrue(resourceMatches(res, "app", "users"))
        assertTrue(resourceMatches(res, "app", "orders"))
        assertFalse(resourceMatches(res, "other", "users"))
    }

    @Test
    fun `collection-across-dbs resource matches by collection name`() {
        val res = PrivResource(db = "", collection = "users")
        assertTrue(resourceMatches(res, "app", "users"))
        assertTrue(resourceMatches(res, "other", "users"))
        assertFalse(resourceMatches(res, "app", "orders"))
    }

    @Test
    fun `exact resource matches exactly`() {
        val res = PrivResource(db = "app", collection = "users")
        assertTrue(resourceMatches(res, "app", "users"))
        assertFalse(resourceMatches(res, "app", "orders"))
        assertFalse(resourceMatches(res, "other", "users"))
    }
}

class GrantsOnTest {
    private val readAll = Privilege(PrivResource(db = "app", collection = ""), listOf("find"))
    private val writeUsers = Privilege(PrivResource(db = "app", collection = "users"), listOf("insert", "update", "remove"))
    private val clusterPriv = Privilege(PrivResource(cluster = true), listOf("shutdown"))

    @Test
    fun `finds the privileges granting a write on the namespace`() {
        val via = grantsOn(listOf(readAll, writeUsers, clusterPriv), "app", "users", WRITE_ACTIONS)
        assertEquals(listOf(writeUsers), via)
    }

    @Test
    fun `read-only privileges do not grant writes`() {
        assertTrue(grantsOn(listOf(readAll), "app", "users", WRITE_ACTIONS).isEmpty())
    }

    @Test
    fun `matching resource with wrong action does not count`() {
        assertTrue(grantsOn(listOf(writeUsers), "app", "users", setOf("dropCollection")).isEmpty())
    }
}

class SuperuserTest {
    @Test
    fun `root anywhere is a superuser`() {
        assertTrue(user("svc", "app", RoleRef("root", "admin")).superuser)
    }

    @Test
    fun `userAdminAnyDatabase on admin is a superuser`() {
        assertTrue(user("ops", "admin", RoleRef("userAdminAnyDatabase", "admin")).superuser)
    }

    @Test
    fun `dbOwner on a normal db is not a superuser`() {
        assertFalse(user("app", "app", RoleRef("dbOwner", "app")).superuser)
    }

    @Test
    fun `readWrite is not a superuser`() {
        assertFalse(user("app", "app", RoleRef("readWrite", "app")).superuser)
    }
}

class InsightsTest {
    @Test
    fun `flags superusers and roleless users`() {
        val users = listOf(
            user("root", "admin", RoleRef("root", "admin")),
            user("app", "app", RoleRef("readWrite", "app")),
            user("orphan", "app"),
        )
        val i = computeInsights(users, emptyList())
        assertEquals(listOf("root@admin"), i.superusers.map { it.id })
        assertEquals(listOf("orphan@app"), i.usersWithoutRoles.map { it.id })
    }

    @Test
    fun `a custom role nobody references is unused`() {
        val roles = listOf(role("reporting", "app"), role("granted", "app"))
        val users = listOf(user("analyst", "app", RoleRef("granted", "app")))
        val i = computeInsights(users, roles)
        assertEquals(listOf("reporting@app"), i.unusedCustomRoles.map { it.id })
    }

    @Test
    fun `a custom role inherited by another role counts as used`() {
        val base = role("base", "app")
        val wrapper = role("wrapper", "app", inherited = listOf(RoleRef("base", "app")))
        val users = listOf(user("u", "app", RoleRef("wrapper", "app")))
        val i = computeInsights(users, listOf(base, wrapper))
        assertTrue(i.unusedCustomRoles.isEmpty())
    }

    @Test
    fun `builtin roles are never reported unused`() {
        val i = computeInsights(emptyList(), listOf(role("read", builtin = true)))
        assertTrue(i.unusedCustomRoles.isEmpty())
    }
}
