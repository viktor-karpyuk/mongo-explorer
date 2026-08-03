package io.mex.provision

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClients
import io.mex.AppContext
import io.mex.data.BackupsRepo
import io.mex.data.ConnectionsRepo
import io.mex.data.Lab
import io.mex.data.LabStatus
import io.mex.data.LabTopology
import io.mex.data.LabsRepo
import io.mex.data.MigrationJobsRepo
import io.mex.data.PrefsRepo
import io.mex.data.QueryHistoryRepo
import io.mex.data.Store
import io.mex.data.UriHistoryRepo
import io.mex.mongo.MongoRegistry
import org.bson.Document
import java.io.File
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * E2E against the real local Docker daemon (spec §9 E1–E4), driving the actual
 * [ProvisionRunner] end to end: render → up → wait → initiate → shards → auth →
 * verify → register, then stop/start and destroy.
 *
 * Skipped unless MEX_E2E=1 — it needs Docker and takes minutes:
 *   MEX_E2E=1 ./gradlew :v3:test --tests 'io.mex.provision.ProvisionE2ETest'
 */
class ProvisionE2ETest {
    private val enabled = System.getenv("MEX_E2E") == "1"
    private val tag = System.getenv("MEX_E2E_TAG") ?: "6.0" // cached locally; 8.0 works but pulls

    private val dir = Files.createTempDirectory("mex-e2e")
    private val store by lazy { Store.open(dir.resolve("e2e.db")) }
    private val ctx by lazy {
        AppContext(
            store = store,
            connections = ConnectionsRepo(store),
            uriHistory = UriHistoryRepo(store),
            prefs = PrefsRepo(store),
            queryHistory = QueryHistoryRepo(store),
            migrations = MigrationJobsRepo(store),
            backups = BackupsRepo(store),
            labs = LabsRepo(store),
            dataDir = dir,
        )
    }
    private val registry by lazy { MongoRegistry() }
    private val runner by lazy { ProvisionRunner(ctx, registry) }

    @AfterTest
    fun tearDown() {
        if (!enabled) return
        // Belt and braces: destroy anything the test left behind, then wipe the temp dir.
        for (lab in ctx.labs.list()) {
            runCatching {
                exec(listOf("docker") + composeArgs(lab.dir, "down", "-v"), 120)
            }
        }
        store.close()
        dir.toFile().deleteRecursively()
    }

    @Test
    fun `E1 replica set x3 with auth provisions, verifies and registers`() {
        if (!enabled) return
        val labId = runner.provision("e2e-rs", LabTopology.ReplicaSet(3), tag, auth = true)
        val lab = awaitStatus(labId, LabStatus.running, timeoutSec = 360)

        // Registered connection exists and reaches the cluster as root.
        val conn = ctx.connections.get(lab.connectionId!!)!!
        assertTrue("directConnection=true" in conn.uri)
        client(conn.uri).use { c ->
            val status = c.getDatabase("admin").runCommand(Document("replSetGetStatus", 1))
            assertEquals(3, (status["members"] as List<*>).size)
        }

        // Loopback-only publishing (PRV-SEC-4): docker port must never say 0.0.0.0.
        val p = plan(lab)
        for (svc in p.clientServices) {
            val out = exec(listOf("docker", "port", containerName(p.project, svc)), 10)
            assertTrue(out.lines().filter { it.isNotBlank() }.all { "127.0.0.1" in it }, "docker port $svc → $out")
        }

        destroyAndAssertGone(lab)
    }

    @Test
    fun `E2 sharded 2x1 with two mongos provisions and registers both routers`() {
        if (!enabled) return
        val topology = LabTopology.Sharded(shards = 2, membersPerShard = 1, mongos = 2, configServers = 1)
        val labId = runner.provision("e2e-shard", topology, tag, auth = true)
        val lab = awaitStatus(labId, LabStatus.running, timeoutSec = 420)

        val conn = ctx.connections.get(lab.connectionId!!)!!
        assertEquals(2, Regex("127\\.0\\.0\\.1:\\d+").findAll(conn.uri).count(), conn.uri)
        client(conn.uri).use { c ->
            assertEquals(2L, c.getDatabase("config").getCollection("shards").countDocuments())
        }

        destroyAndAssertGone(lab)
    }

    @Test
    fun `E3 stop then start keeps data and ports`() {
        if (!enabled) return
        val labId = runner.provision("e2e-cycle", LabTopology.ReplicaSet(1), tag, auth = true)
        var lab = awaitStatus(labId, LabStatus.running, timeoutSec = 240)
        val conn = ctx.connections.get(lab.connectionId!!)!!
        val portsBefore = lab.portMap

        client(conn.uri).use { c ->
            c.getDatabase("e2e").getCollection("t").insertOne(Document("k", "survives"))
        }

        runner.stop(labId)
        lab = awaitStatus(labId, LabStatus.stopped, timeoutSec = 60)

        runner.start(labId)
        lab = awaitStatus(labId, LabStatus.running, timeoutSec = 240)
        assertEquals(portsBefore, lab.portMap)
        client(conn.uri).use { c ->
            assertEquals(1L, c.getDatabase("e2e").getCollection("t").countDocuments(Document("k", "survives")))
        }

        destroyAndAssertGone(lab)
    }

    /* ===================== helpers ===================== */

    private fun destroyAndAssertGone(lab: Lab) {
        val connId = lab.connectionId
        runner.destroy(lab.id, alsoConnection = true)
        awaitGone(lab.id, timeoutSec = 120)

        assertNull(ctx.labs.get(lab.id))
        connId?.let { assertNull(ctx.connections.get(it)) }
        assertTrue(!File(lab.dir).exists(), "lab dir should be deleted")
        val ps = exec(listOf("docker") + psArgs(lab.id), 15)
        assertTrue(ps.lines().none { it.isNotBlank() }, "no containers should remain: $ps")
        val vols = exec(listOf("docker", "volume", "ls", "--filter", "label=$LAB_LABEL=${lab.id}", "-q"), 15)
        assertTrue(vols.isBlank(), "no volumes should remain: $vols")
    }

    private fun awaitStatus(labId: String, expected: LabStatus, timeoutSec: Long): Lab {
        val deadline = System.currentTimeMillis() + timeoutSec * 1000
        while (System.currentTimeMillis() < deadline) {
            val lab = ctx.labs.get(labId) ?: throw AssertionError("lab row vanished")
            if (lab.status == expected) return lab
            if (lab.status == LabStatus.failed) throw AssertionError("lab failed: ${lab.error}")
            Thread.sleep(2000)
        }
        throw AssertionError("timed out waiting for $expected — last: ${ctx.labs.get(labId)?.status}/${ctx.labs.get(labId)?.error}")
    }

    private fun awaitGone(labId: String, timeoutSec: Long) {
        val deadline = System.currentTimeMillis() + timeoutSec * 1000
        while (System.currentTimeMillis() < deadline) {
            if (ctx.labs.get(labId) == null) return
            Thread.sleep(1000)
        }
        throw AssertionError("timed out waiting for destroy")
    }

    private fun client(uri: String) = MongoClients.create(
        MongoClientSettings.builder()
            .applyConnectionString(ConnectionString(uri))
            .applyToClusterSettings { it.serverSelectionTimeout(10, TimeUnit.SECONDS) }
            .build(),
    )

    private fun exec(cmd: List<String>, timeoutSec: Long): String {
        val p = ProcessBuilder(cmd).redirectErrorStream(true).start()
        val out = p.inputStream.bufferedReader().readText()
        p.waitFor(timeoutSec, TimeUnit.SECONDS)
        return out
    }
}
