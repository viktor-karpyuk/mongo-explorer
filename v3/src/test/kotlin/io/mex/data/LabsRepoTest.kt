package io.mex.data

import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class LabsRepoTest {
    private val dir = Files.createTempDirectory("mex-labs-test")
    private val store = Store.open(dir.resolve("test.db"))
    private val repo = LabsRepo(store)

    @AfterTest
    fun tearDown() {
        store.close()
        dir.toFile().deleteRecursively()
    }

    @Test
    fun `fresh store reaches schema version 6`() {
        store.conn.createStatement().use { st ->
            st.executeQuery("SELECT MAX(version) FROM schema_version").use { rs ->
                rs.next()
                assertEquals(6, rs.getInt(1))
            }
        }
    }

    @Test
    fun `lab round-trips including sealed topology json`() {
        val topology = LabTopology.Sharded(shards = 3, membersPerShard = 3, mongos = 2, configServers = 3)
        val created = repo.create("dev-shard", topology, "8.0", auth = true, labsDir = "/tmp/labs")

        val loaded = repo.get(created.id)!!
        assertEquals("dev-shard", loaded.name)
        assertEquals(topology, loaded.topology)
        assertEquals(LabStatus.provisioning, loaded.status)
        assertEquals("8.0", loaded.mongoTag)
        assertTrue(loaded.auth)
        assertEquals("/tmp/labs/${created.id}", loaded.dir)
        assertEquals(LAB_APP_MAJOR, loaded.appMajor)
        assertEquals(emptyMap(), loaded.portMap)
        assertNull(loaded.connectionId)
    }

    @Test
    fun `all three topology shapes survive persistence`() {
        val a = repo.create("s", LabTopology.Standalone, "8.0", true, "/l")
        val b = repo.create("r", LabTopology.ReplicaSet(5), "7.0", false, "/l")
        assertEquals(LabTopology.Standalone, repo.get(a.id)!!.topology)
        assertEquals(LabTopology.ReplicaSet(5), repo.get(b.id)!!.topology)
    }

    @Test
    fun `ports and connection update independently`() {
        val lab = repo.create("x", LabTopology.ReplicaSet(3), "8.0", true, "/l")
        repo.setPorts(lab.id, mapOf("rs-n1" to 61101, "rs-n2" to 61102))
        repo.setConnection(lab.id, "conn-1")
        val loaded = repo.get(lab.id)!!
        assertEquals(61101, loaded.portMap["rs-n1"])
        assertEquals("conn-1", loaded.connectionId)
        assertEquals(setOf("conn-1"), repo.connectionIds())

        repo.setConnection(lab.id, null)
        assertNull(repo.get(lab.id)!!.connectionId)
        assertEquals(emptySet(), repo.connectionIds())
    }

    @Test
    fun `status transitions persist with and without error`() {
        val lab = repo.create("x", LabTopology.Standalone, "8.0", true, "/l")
        repo.setStatus(lab.id, LabStatus.failed, "wait: rs-n1 unhealthy")
        assertEquals("wait: rs-n1 unhealthy", repo.get(lab.id)!!.error)
        repo.setStatus(lab.id, LabStatus.running)
        assertNull(repo.get(lab.id)!!.error)
    }

    @Test
    fun `reconcileOrphans fails interrupted provisions only`() {
        val stuck = repo.create("stuck", LabTopology.Standalone, "8.0", true, "/l")
        val fine = repo.create("fine", LabTopology.Standalone, "8.0", true, "/l")
        repo.setStatus(fine.id, LabStatus.stopped)

        repo.reconcileOrphans()

        assertEquals(LabStatus.failed, repo.get(stuck.id)!!.status)
        assertEquals(LabStatus.stopped, repo.get(fine.id)!!.status)
    }

    @Test
    fun `name uniqueness is queryable and delete removes the row`() {
        val lab = repo.create("dup", LabTopology.Standalone, "8.0", true, "/l")
        assertTrue(repo.nameExists("dup"))
        repo.delete(lab.id)
        assertTrue(!repo.nameExists("dup"))
        assertNull(repo.get(lab.id))
    }
}
