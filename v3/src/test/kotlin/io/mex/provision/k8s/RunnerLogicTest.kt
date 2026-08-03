package io.mex.provision.k8s

import io.mex.data.BackupChoice
import io.mex.data.K8sDeploySpec
import io.mex.data.K8sDeployStatus
import io.mex.data.K8sOperator
import io.mex.data.K8sProfile
import io.mex.data.K8sTopology
import io.mex.data.TlsChoice
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

private fun spec(
    profile: K8sProfile = K8sProfile.prod,
    operator: K8sOperator = K8sOperator.psmdb,
    topology: K8sTopology = K8sTopology.ReplicaSet(3),
    tls: TlsChoice = TlsChoice.CertManager("issuer"),
    namespaceCreated: Boolean = false,
) = K8sDeploySpec(
    name = "pay-db", context = "prod-eu", namespace = "payments",
    namespaceCreated = namespaceCreated,
    operator = operator, profile = profile, topology = topology,
    mongoVersion = "8.0", storageClass = "gp3", tls = tls,
    backup = if (operator == K8sOperator.psmdb) BackupChoice.Pbm("e", "b", "s") else BackupChoice.ByoDeclared,
)

class PreflightVerdictTest {
    private fun facts(
        reachable: Boolean = true,
        version: VersionVerdict = VersionVerdict.Ok("1.33"),
        operators: List<DetectedOperator> = listOf(DetectedOperator(K8sOperator.psmdb, "1.16.2")),
        rbac: List<RbacRequirement> = emptyList(),
        storage: List<String> = listOf("gp3", "gp2"),
        certManager: Boolean = true,
        nodes: Int = 6,
        zones: Int = 3,
    ) = ProbeFacts(reachable, version, operators, rbac, storage, certManager, nodes, zones)

    @Test
    fun `a healthy cluster passes with no warnings`() {
        val r = preflightVerdict(spec(), facts())
        assertTrue(r.ok)
        assertFalse(r.checks.any { it.warn })
    }

    @Test
    fun `unreachable context blocks and skips the rest`() {
        val r = preflightVerdict(
            spec(),
            facts(reachable = false, version = VersionVerdict.Unknown("connection refused")),
        )
        assertFalse(r.ok)
        assertTrue(r.checks.first().detail!!.contains("connection refused"))
    }

    @Test
    fun `missing rbac verbs are listed and block`() {
        val r = preflightVerdict(
            spec(),
            facts(rbac = listOf(RbacRequirement("create", "perconaservermongodbs"))),
        )
        assertFalse(r.ok)
        assertTrue(r.checks.any { !it.ok && it.detail!!.contains("create perconaservermongodbs") })
    }

    @Test
    fun `missing operator blocks with an install pointer`() {
        val r = preflightVerdict(spec(), facts(operators = emptyList()))
        assertFalse(r.ok)
        assertTrue(r.checks.any { !it.ok && it.detail!!.contains("install the operator") })
    }

    @Test
    fun `out-of-matrix version warns but never blocks`() {
        val r = preflightVerdict(spec(), facts(version = VersionVerdict.Warn("1.30", "outside blessed matrix")))
        assertTrue(r.ok)
        assertTrue(r.checks.any { it.warn && it.name.contains("1.30") })
    }

    @Test
    fun `missing storage class and cert-manager block`() {
        assertFalse(preflightVerdict(spec(), facts(storage = listOf("gp2"))).ok)
        assertFalse(preflightVerdict(spec(), facts(certManager = false)).ok)
        // A dev spec with TLS off does not need cert-manager.
        val devNoTls = spec(profile = K8sProfile.dev, tls = TlsChoice.Off).copy(
            backup = BackupChoice.None, storageClass = null,
        )
        assertTrue(preflightVerdict(devNoTls, facts(certManager = false)).ok)
    }

    @Test
    fun `node inventory must cover the largest component`() {
        assertFalse(preflightVerdict(spec(), facts(nodes = 2)).ok)
        val sharded = spec(topology = K8sTopology.Sharded(4, 3, mongos = 2, configServers = 3))
        assertEquals(3, requiredNodes(sharded)) // max(3 members, 3 cfg, 2 mongos)
        assertTrue(preflightVerdict(sharded, facts(nodes = 3)).ok)
    }

    @Test
    fun `single-zone prod warns about DoNotSchedule spread`() {
        val r = preflightVerdict(spec(), facts(zones = 1))
        assertTrue(r.ok)
        assertTrue(r.checks.any { it.warn && it.name == "Failure domains" })
        // Dev does not render spread, so no warning.
        val dev = spec(profile = K8sProfile.dev, tls = TlsChoice.Off).copy(backup = BackupChoice.None, storageClass = null)
        assertFalse(preflightVerdict(dev, facts(zones = 1)).checks.any { it.name == "Failure domains" })
    }

    @Test
    fun `mco plus sharded is caught in preflight too`() {
        val r = preflightVerdict(
            spec(operator = K8sOperator.mco, topology = K8sTopology.Sharded(2, 3)),
            facts(operators = listOf(DetectedOperator(K8sOperator.mco, "0.10.0"))),
        )
        assertFalse(r.ok)
    }
}

class StatusMappingTest {
    @Test
    fun `psmdb states map to app statuses`() {
        assertEquals(K8sDeployStatus.ready, mapCrStatus(K8sOperator.psmdb, """{"status":{"state":"ready"}}""").status)
        assertEquals(K8sDeployStatus.pending, mapCrStatus(K8sOperator.psmdb, """{"status":{"state":"initializing"}}""").status)
        assertEquals(K8sDeployStatus.degraded, mapCrStatus(K8sOperator.psmdb, """{"status":{"state":"error"}}""").status)
    }

    @Test
    fun `mco phases map with member counts in the detail`() {
        val running = mapCrStatus(K8sOperator.mco, """{"status":{"phase":"Running","currentStatefulSetReplicas":"3","members":"3"}}""")
        assertEquals(K8sDeployStatus.ready, running.status)
        assertTrue("3/3" in running.detail)
        assertEquals(K8sDeployStatus.degraded, mapCrStatus(K8sOperator.mco, """{"status":{"phase":"Failed"}}""").status)
    }

    @Test
    fun `unknown states stay pending with the raw string kept - never a confident ready`() {
        val r = mapCrStatus(K8sOperator.psmdb, """{"status":{"state":"reconfiguring-shards"}}""")
        assertEquals(K8sDeployStatus.pending, r.status)
        assertEquals("reconfiguring-shards", r.detail)
    }

    @Test
    fun `absent CR is missing, unreadable json is pending`() {
        assertEquals(K8sDeployStatus.missing, mapCrStatus(K8sOperator.psmdb, null).status)
        assertEquals(K8sDeployStatus.missing, mapCrStatus(K8sOperator.psmdb, "").status)
        assertEquals(K8sDeployStatus.pending, mapCrStatus(K8sOperator.psmdb, "not json").status)
    }

    @Test
    fun `component readiness is extracted per replset and mongos`() {
        val json = """{"status":{"replsets":{"rs1":{"ready":"3","size":"3","status":"ready"}},"mongos":{"ready":"1","size":"2"},"backup":{"status":"enabled"}}}"""
        val comps = psmdbComponents(json)
        assertEquals(3, comps.size)
        assertEquals("3/3", comps.first { it.name == "rs rs1" }.ready)
        assertEquals("1/2", comps.first { it.name == "mongos" }.ready)
    }

    @Test
    fun `forward target follows the topology`() {
        assertEquals("svc/pay-db-mongos", forwardTarget(K8sOperator.psmdb, "pay-db", K8sTopology.Sharded(2, 3)))
        assertEquals("svc/pay-db-rs0", forwardTarget(K8sOperator.psmdb, "pay-db", K8sTopology.ReplicaSet(3)))
        assertEquals("pod/pay-db-0", forwardTarget(K8sOperator.mco, "pay-db", K8sTopology.ReplicaSet(3)))
    }
}

class TeardownPlanTest {
    @Test
    fun `fixed order - CR first, PVCs late, namespace last`() {
        val steps = teardownSteps(spec(namespaceCreated = true), releasePvcs = true)
        val kinds = steps.map { it.kind }
        assertEquals("perconaservermongodb", kinds.first())
        assertTrue(kinds.indexOf("pvc") > kinds.indexOf("secret"))
        assertEquals("namespace", kinds.last())
        // The CR delete waits for pod drain.
        assertTrue(steps.first().waitSeconds != null)
    }

    @Test
    fun `prod keeps volumes unless released, and never deletes a BYO-CA secret`() {
        val kept = teardownSteps(spec(), releasePvcs = false)
        assertFalse(kept.any { it.kind == "pvc" })
        val byo = teardownSteps(spec(tls = TlsChoice.ByoCa("corp-ca")), releasePvcs = false)
        assertFalse(byo.any { it.name == "corp-ca" })
        // cert-manager certificates we created are ours to remove.
        assertTrue(teardownSteps(spec(), false).any { it.kind == "certificate" })
    }

    @Test
    fun `dev has no PDB step and namespace stays when the app did not create it`() {
        val dev = spec(profile = K8sProfile.dev, tls = TlsChoice.Off).copy(
            backup = BackupChoice.None, storageClass = null,
        )
        val steps = teardownSteps(dev, releasePvcs = true)
        assertFalse(steps.any { it.kind == "poddisruptionbudget" })
        assertFalse(steps.any { it.kind == "namespace" })
    }

    @Test
    fun `consequence text states the volume fate and the connection`() {
        val keeps = teardownConsequence(spec(), releasePvcs = false, connectionName = "pay-db (k8s)")
        assertTrue("kept" in keeps)
        assertTrue("pay-db (k8s)" in keeps)
        val deletes = teardownConsequence(spec(), releasePvcs = true, connectionName = null)
        assertTrue("WILL be deleted" in deletes)
        assertTrue("backups are left untouched" in deletes)
    }
}

class K8sUriTest {
    @Test
    fun `replica set uris are directConnection with the CA and hostname exemption`() {
        val uri = k8sUri(61441, "root", "pw", directConnection = true, caFile = "/tmp/ca.pem")
        assertTrue("mongodb://root:pw@127.0.0.1:61441/" in uri)
        assertTrue("directConnection=true" in uri)
        assertTrue("tls=true" in uri && "tlsCAFile=/tmp/ca.pem" in uri)
        // The forward answers as 127.0.0.1, which is never a cert SAN.
        assertTrue("tlsAllowInvalidHostnames=true" in uri)
    }

    @Test
    fun `sharded uris omit directConnection, plaintext omits tls params`() {
        val uri = k8sUri(61441, "root", "pw", directConnection = false, caFile = null)
        assertFalse("directConnection" in uri)
        assertFalse("tls" in uri)
        assertTrue("authSource=admin" in uri)
    }
}

class TlsSecretNameTest {
    @Test
    fun `tls secret follows the chosen path`() {
        assertEquals("pay-db-tls", tlsSecretName(K8sOperator.psmdb, spec()))
        assertEquals("corp-ca", tlsSecretName(K8sOperator.psmdb, spec(tls = TlsChoice.ByoCa("corp-ca"))))
        assertNull(tlsSecretName(K8sOperator.psmdb, spec(profile = K8sProfile.dev, tls = TlsChoice.Off)))
    }
}
