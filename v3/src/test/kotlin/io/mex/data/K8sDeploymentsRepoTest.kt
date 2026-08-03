package io.mex.data

import java.nio.file.Files
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class K8sDeploymentsRepoTest {
    private val dir = Files.createTempDirectory("mex-k8s-test")
    private val store = Store.open(dir.resolve("test.db"))
    private val repo = K8sDeploymentsRepo(store)

    private val spec = K8sDeploySpec(
        name = "pay-db", context = "prod-eu", namespace = "payments",
        operator = K8sOperator.psmdb, profile = K8sProfile.prod,
        topology = K8sTopology.Sharded(2, 3, mongos = 2, configServers = 3),
        mongoVersion = "8.0", storageClass = "gp3",
        tls = TlsChoice.CertManager("letsencrypt"),
        backup = BackupChoice.Pbm("https://minio:9000", "bkt", "creds"),
        secretFingerprints = mapOf("pay-db-users" to "abc123"),
    )

    @AfterTest
    fun tearDown() {
        store.close()
        dir.toFile().deleteRecursively()
    }

    @Test
    fun `fresh store reaches schema version 7`() {
        store.conn.createStatement().use { st ->
            st.executeQuery("SELECT MAX(version) FROM schema_version").use { rs ->
                rs.next()
                assertEquals(7, rs.getInt(1))
            }
        }
    }

    @Test
    fun `spec round-trips through json including sealed choices`() {
        val d = repo.create(spec)
        val loaded = repo.get(d.id)!!
        assertEquals(spec, loaded.spec)
        assertEquals(K8sDeployStatus.applying, loaded.status)
        assertNull(loaded.bundleHash)
    }

    @Test
    fun `status detail and applied stamp persist`() {
        val d = repo.create(spec)
        repo.setStatus(d.id, K8sDeployStatus.pending, detail = "initializing")
        assertEquals("initializing", repo.get(d.id)!!.statusDetail)
        repo.setApplied(d.id, "sha256:abc", spec.copy(namespaceCreated = true))
        val loaded = repo.get(d.id)!!
        assertEquals("sha256:abc", loaded.bundleHash)
        assertTrue(loaded.spec.namespaceCreated)
        assertTrue(loaded.appliedAt != null)
    }

    @Test
    fun `connection ids feed the badge and tolerate deletion`() {
        val d = repo.create(spec)
        repo.setConnection(d.id, "conn-1")
        assertEquals(setOf("conn-1"), repo.connectionIds())
        repo.setConnection(d.id, null)
        assertEquals(emptySet(), repo.connectionIds())
    }

    @Test
    fun `reconcile fails interrupted applying and deleting rows only`() {
        val a = repo.create(spec)
        val b = repo.create(spec.copy(name = "other"))
        repo.setStatus(b.id, K8sDeployStatus.ready)
        repo.reconcileOrphans()
        assertEquals(K8sDeployStatus.failed, repo.get(a.id)!!.status)
        assertEquals(K8sDeployStatus.ready, repo.get(b.id)!!.status)
    }

    @Test
    fun `uniqueness is per context-namespace-name`() {
        repo.create(spec)
        assertTrue(repo.nameExists("prod-eu", "payments", "pay-db"))
        assertTrue(!repo.nameExists("staging", "payments", "pay-db"))
    }
}
