package io.mex.backup

import io.mex.data.BackupScope
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

private const val URI = "mongodb://localhost:27017"

class DumpArgsTest {
    @Test
    fun `full deployment dumps everything`() {
        val args = dumpArgs(URI, BackupScope(), gzip = true, outDir = "/b/x")
        assertEquals(listOf("--uri=$URI", "--gzip", "--out=/b/x"), args)
    }

    @Test
    fun `database scope adds --db`() {
        val args = dumpArgs(URI, BackupScope("app"), gzip = false, outDir = "/b/x")
        assertEquals(listOf("--uri=$URI", "--db=app", "--out=/b/x"), args)
    }

    @Test
    fun `collection scope adds --db and --collection`() {
        val args = dumpArgs(URI, BackupScope("app", "users"), gzip = true, outDir = "/b/x")
        assertTrue("--db=app" in args)
        assertTrue("--collection=users" in args)
    }
}

class NsIncludeTest {
    @Test
    fun `scope maps to the restore namespace filter`() {
        assertNull(nsInclude(BackupScope()))
        assertEquals("app.*", nsInclude(BackupScope("app")))
        assertEquals("app.users", nsInclude(BackupScope("app", "users")))
    }
}

class RestoreArgsTest {
    @Test
    fun `plain restore filters by the backup scope`() {
        val args = restoreArgs(URI, BackupScope("app"), "/b/x", gzip = true, drop = false, dryRun = false)
        assertEquals(listOf("--uri=$URI", "--nsInclude=app.*", "--gzip", "--dir=/b/x"), args)
    }

    @Test
    fun `dry-run adds --dryRun and --verbose so the preview is not silent`() {
        val args = restoreArgs(URI, BackupScope("app"), "/b/x", gzip = false, drop = false, dryRun = true)
        assertTrue("--dryRun" in args)
        assertTrue("--verbose" in args)
    }

    @Test
    fun `drop is only present when explicitly requested`() {
        val without = restoreArgs(URI, BackupScope("app"), "/b/x", gzip = false, drop = false, dryRun = false)
        val with = restoreArgs(URI, BackupScope("app"), "/b/x", gzip = false, drop = true, dryRun = false)
        assertFalse("--drop" in without)
        assertTrue("--drop" in with)
    }

    @Test
    fun `renaming a database maps every collection via nsFrom-nsTo`() {
        val args = restoreArgs(URI, BackupScope("app"), "/b/x", gzip = false, drop = false, dryRun = false, renameDb = "staging")
        assertTrue("--nsFrom=app.*" in args)
        assertTrue("--nsTo=staging.*" in args)
    }

    @Test
    fun `renaming a single-collection backup keeps the collection name`() {
        val args = restoreArgs(URI, BackupScope("app", "users"), "/b/x", gzip = false, drop = false, dryRun = false, renameDb = "staging")
        assertTrue("--nsFrom=app.users" in args)
        assertTrue("--nsTo=staging.users" in args)
    }

    @Test
    fun `a full-deployment backup ignores the rename`() {
        val args = restoreArgs(URI, BackupScope(), "/b/x", gzip = false, drop = false, dryRun = false, renameDb = "staging")
        assertFalse(args.any { it.startsWith("--nsFrom") })
        assertFalse(args.any { it.startsWith("--nsTo") })
    }
}
