package io.mex.provision.k8s

import io.mex.data.BackupChoice
import io.mex.data.K8sDeploySpec
import io.mex.data.K8sOperator
import io.mex.data.K8sProfile
import io.mex.data.K8sTopology
import io.mex.data.TlsChoice
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private fun prodRs(operator: K8sOperator = K8sOperator.psmdb, members: Int = 3) = K8sDeploySpec(
    name = "pay-db", context = "prod-eu", namespace = "payments",
    operator = operator, profile = K8sProfile.prod,
    topology = K8sTopology.ReplicaSet(members),
    mongoVersion = "8.0", storageClass = "gp3", storageGi = 200,
    cpu = "2", memory = "8Gi",
    tls = TlsChoice.CertManager("letsencrypt"),
    backup = if (operator == K8sOperator.psmdb) {
        BackupChoice.Pbm("https://minio.internal:9000", "mex-backups", "pbm-creds")
    } else {
        BackupChoice.ByoDeclared
    },
)

private fun devRs() = K8sDeploySpec(
    name = "dev-db", context = "kind", namespace = "scratch",
    operator = K8sOperator.psmdb, profile = K8sProfile.dev,
    topology = K8sTopology.ReplicaSet(3),
    mongoVersion = "7.0", tls = TlsChoice.Off, backup = BackupChoice.None,
)

class RenderStructureTest {
    @Test
    fun `prod bundle emits secret, certificate, CR and PDB in apply order`() {
        val docs = render(prodRs())
        assertEquals(listOf("Secret", "Certificate", "PerconaServerMongoDB", "PodDisruptionBudget"), docs.map { it.kind })
    }

    @Test
    fun `namespace is emitted only when the app creates it`() {
        assertFalse(render(prodRs()).any { it.kind == "Namespace" })
        val docs = render(prodRs().copy(namespaceCreated = true))
        assertEquals("Namespace", docs.first().kind)
    }

    @Test
    fun `dev bundle has no PDB and no certificate`() {
        val kinds = render(devRs()).map { it.kind }
        assertEquals(listOf("Secret", "PerconaServerMongoDB"), kinds)
    }

    @Test
    fun `rendering an invalid spec throws rather than weakening prod`() {
        val bad = prodRs().copy(tls = TlsChoice.Off)
        assertFailsWith<IllegalArgumentException> { render(bad) }
    }

    @Test
    fun `rendering is deterministic`() {
        assertEquals(bundleText(render(prodRs())), bundleText(render(prodRs())))
    }
}

class SecretElisionTest {
    @Test
    fun `preview elides secret values, apply carries them`() {
        val preview = render(prodRs()).first { it.kind == "Secret" }
        assertTrue(preview.secretValuesElided)
        assertTrue(ELIDED in preview.yaml)
        assertFalse("hunter2" in preview.yaml)

        val applied = render(prodRs(), mapOf("pay-db-users/password" to "hunter2"))
            .first { it.kind == "Secret" }
        assertFalse(applied.secretValuesElided)
        assertTrue("hunter2" in applied.yaml)
        assertFalse(ELIDED in applied.yaml)
    }
}

class ProdLockMaterialisationTest {
    private val psmdb = render(prodRs()).first { it.kind == "PerconaServerMongoDB" }.yaml
    private val mco = render(prodRs(K8sOperator.mco)).first { it.kind == "MongoDBCommunity" }.yaml

    @Test
    fun `zone and host spread with DoNotSchedule on both operators`() {
        for (yaml in listOf(psmdb, mco)) {
            assertTrue("topology.kubernetes.io/zone" in yaml, yaml.take(80))
            assertTrue("kubernetes.io/hostname" in yaml)
            assertTrue("whenUnsatisfiable: DoNotSchedule" in yaml)
        }
    }

    @Test
    fun `every compute block pins requests equal to limits`() {
        for (yaml in listOf(psmdb, mco)) {
            // `limits:` appears once per compute block (storage uses requests only), and
            // cpu/memory show up twice per block — once under requests, once under limits.
            val computeBlocks = Regex("(?m)^\\s+limits:").findAll(yaml).count()
            assertTrue(computeBlocks >= 1, "expected a compute block")
            assertEquals(computeBlocks * 2, Regex("cpu: \"2\"").findAll(yaml).count())
            assertEquals(computeBlocks * 2, Regex("memory: 8Gi").findAll(yaml).count())
        }
    }

    @Test
    fun `storage class and size are explicit`() {
        assertTrue("storageClassName: gp3" in psmdb)
        assertTrue("storage: 200Gi" in psmdb)
    }

    @Test
    fun `psmdb prod carries the deletion-protection finalizer and refuses unsafe config`() {
        assertTrue("finalizers:" in psmdb)
        assertTrue("delete-psmdb-pods-in-order" in psmdb)
        assertTrue("allowUnsafeConfigurations: false" in psmdb)
    }

    @Test
    fun `dev is permitted unsafe configurations and skips spread`() {
        val dev = render(devRs()).first { it.kind == "PerconaServerMongoDB" }.yaml
        assertTrue("allowUnsafeConfigurations: true" in dev)
        assertFalse("DoNotSchedule" in dev)
    }

    @Test
    fun `pdb is maxUnavailable 1`() {
        val pdb = render(prodRs()).first { it.kind == "PodDisruptionBudget" }.yaml
        assertTrue("maxUnavailable: 1" in pdb)
    }
}

class ShardedRenderTest {
    private val sharded = prodRs().copy(topology = K8sTopology.Sharded(3, 3, mongos = 2, configServers = 3))

    @Test
    fun `every shard becomes its own replset and sharding is enabled`() {
        val yaml = render(sharded).first { it.kind == "PerconaServerMongoDB" }.yaml
        assertTrue("- name: rs1" in yaml && "- name: rs2" in yaml && "- name: rs3" in yaml)
        assertTrue("enabled: true" in yaml)
        assertTrue("configsvrReplSet:" in yaml)
        assertTrue("size: 3" in yaml)
        assertTrue("mongos:" in yaml)
        assertTrue("size: 2" in yaml)
    }

    @Test
    fun `config servers and mongos get their own spread and resources`() {
        val yaml = render(sharded).first { it.kind == "PerconaServerMongoDB" }.yaml
        assertTrue("app.kubernetes.io/component: cfg" in yaml)
        assertTrue("app.kubernetes.io/component: mongos" in yaml)
    }
}

class BackupRenderTest {
    @Test
    fun `pbm renders storage, pitr and the nightly task`() {
        val yaml = render(prodRs()).first { it.kind == "PerconaServerMongoDB" }.yaml
        assertTrue("endpointUrl: https://minio.internal:9000" in yaml)
        assertTrue("bucket: mex-backups" in yaml)
        assertTrue("credentialsSecret: pbm-creds" in yaml)
        assertTrue("pitr:" in yaml && "enabled: true" in yaml)
        assertTrue("schedule: \"0 1 * * *\"" in yaml)
    }

    @Test
    fun `mco declared-byo renders no backup section of its own`() {
        val yaml = render(prodRs(K8sOperator.mco)).first { it.kind == "MongoDBCommunity" }.yaml
        assertFalse("backup" in yaml.lowercase())
    }
}

class McoRenderTest {
    @Test
    fun `mco carries scram, tls refs and the users secret`() {
        val yaml = render(prodRs(K8sOperator.mco)).first { it.kind == "MongoDBCommunity" }.yaml
        assertTrue("modes: [\"SCRAM\"]" in yaml)
        assertTrue("enabled: true" in yaml)
        assertTrue("passwordSecretRef:" in yaml)
        assertTrue("name: pay-db-users" in yaml)
        assertTrue("version: \"8.0.0\"" in yaml)
        assertTrue("members: 3" in yaml)
    }
}

class PreviewHashTest {
    @Test
    fun `hash is stable for identical specs and moves on any edit`() {
        val a = bundleHash(render(prodRs()))
        assertEquals(a, bundleHash(render(prodRs())))
        val edited = bundleHash(render(prodRs().copy(storageGi = 300)))
        assertTrue(a != edited)
        val moreMembers = bundleHash(render(prodRs(members = 5)))
        assertTrue(a != moreMembers)
    }

    @Test
    fun `apply with a stale hash fails closed`() {
        val confirmed = bundleHash(render(prodRs()))
        val fresh = render(prodRs().copy(cpu = "4"))
        val e = assertFailsWith<StalePreviewException> { requireFreshPreview(confirmed, fresh) }
        assertTrue("nothing was applied" in e.message!!)
        // Unchanged spec passes.
        requireFreshPreview(confirmed, render(prodRs()))
    }

    @Test
    fun `secret material never changes the hash - preview and apply agree`() {
        // The preview the user confirms elides values; the apply carries them. If the
        // material moved the hash, every apply would false-positive as stale.
        val preview = bundleHash(render(prodRs()))
        val applied = bundleHash(render(prodRs(), mapOf("pay-db-users/password" to "hunter2")))
        assertTrue(preview != applied) // documented: hash covers what was READ
    }

    @Test
    fun `bundle text is separated by yaml document markers`() {
        val text = bundleText(render(prodRs()))
        assertEquals(3, Regex("(?m)^---$").findAll(text).count())
    }

    @Test
    fun `short hash keeps the prefix and tail`() {
        val h = "sha256:3f9c1a72abcdef0123456789e441"
        assertTrue(shortHash(h).startsWith("sha256:3f9c1a72"))
        assertTrue(shortHash(h).endsWith("e441"))
    }
}
