package io.mex.provision.k8s

import io.mex.data.K8sOperator
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

private val T = KubeTarget("prod-eu")
private val TK = KubeTarget("prod-eu", "/tmp/kc")

class ArgBuilderTest {
    @Test
    fun `context flags thread through every command, kubeconfig only when set`() {
        assertEquals(listOf("--context", "prod-eu"), ctxArgs(T))
        assertEquals(listOf("--context", "prod-eu", "--kubeconfig", "/tmp/kc"), ctxArgs(TK))
        assertTrue(versionArgs(T).containsAll(listOf("version", "-o", "json", "--context", "prod-eu")))
    }

    @Test
    fun `get json scopes name and namespace and tolerates absence`() {
        val args = getJsonArgs(T, "crd", PSMDB_CRD)
        assertTrue("crd" in args && PSMDB_CRD in args && "--ignore-not-found" in args)
        val scoped = getJsonArgs(T, "perconaservermongodb", "pay-db", "payments")
        assertTrue(scoped.containsAll(listOf("-n", "payments", "pay-db")))
    }

    @Test
    fun `apply reads the bundle from stdin`() {
        assertEquals(
            listOf("apply", "-f", "-", "-n", "payments", "--context", "prod-eu"),
            applyStdinArgs(T, "payments"),
        )
    }

    @Test
    fun `delete waits only when asked and always tolerates absence`() {
        val nowait = deleteArgs(T, "secret", "s1", "payments")
        assertTrue("--ignore-not-found" in nowait && "--wait=false" in nowait)
        val waited = deleteArgs(T, "perconaservermongodb", "pay-db", "payments", waitSeconds = 120)
        assertTrue("--wait=true" in waited && "--timeout=120s" in waited)
    }

    @Test
    fun `port-forward binds loopback only`() {
        val args = portForwardArgs(T, "svc/pay-db-mongos", 61441, 27017, "payments")
        assertTrue("--address" in args && "127.0.0.1" in args)
        assertTrue("61441:27017" in args)
    }

    @Test
    fun `can-i is verb resource namespace`() {
        assertEquals(
            listOf("auth", "can-i", "create", "pods/portforward", "-n", "payments", "--context", "prod-eu"),
            canIArgs(T, "create", "pods/portforward", "payments"),
        )
    }
}

class VersionVerdictTest {
    @Test
    fun `blessed minors pass, others warn, missing is unknown`() {
        assertTrue(versionVerdict("1", "33") is VersionVerdict.Ok)
        assertTrue(versionVerdict("1", "32") is VersionVerdict.Ok)
        assertTrue(versionVerdict("1", "30") is VersionVerdict.Warn)
        assertTrue(versionVerdict("1", "35") is VersionVerdict.Warn)
        assertTrue(versionVerdict(null, null) is VersionVerdict.Unknown)
    }

    @Test
    fun `eks plus-suffixed minors are matched`() {
        assertTrue(versionVerdict("1", "33+") is VersionVerdict.Ok)
    }

    @Test
    fun `server version parses from kubectl version json`() {
        val json = """{"clientVersion":{"major":"1","minor":"30"},"serverVersion":{"major":"1","minor":"33","gitVersion":"v1.33.2"}}"""
        assertEquals("1" to "33", parseServerVersion(json))
        assertEquals(null to null, parseServerVersion("not json"))
    }
}

class OperatorMetaTest {
    @Test
    fun `cr identifiers per operator`() {
        assertEquals("MongoDBCommunity", crKind(K8sOperator.mco))
        assertEquals("PerconaServerMongoDB", crKind(K8sOperator.psmdb))
        assertEquals("psmdb.percona.com/v1", crApiVersion(K8sOperator.psmdb))
    }

    @Test
    fun `capability gates - mco has no sharding or pbm`() {
        val mco = capabilities(K8sOperator.mco)
        assertTrue(OperatorCapability.SHARDED !in mco)
        assertTrue(OperatorCapability.PBM_BACKUP !in mco)
        val psmdb = capabilities(K8sOperator.psmdb)
        assertTrue(OperatorCapability.SHARDED in psmdb && OperatorCapability.PBM_BACKUP in psmdb)
    }

    @Test
    fun `operator version comes from the deployment image tag`() {
        val json = """{"items":[{"spec":{"template":{"spec":{"containers":[{"image":"percona/percona-server-mongodb-operator:1.16.2"}]}}}}]}"""
        assertEquals("1.16.2", parseFirstImageTag(json))
        assertNull(parseFirstImageTag("""{"items":[]}"""))
        assertNull(parseFirstImageTag("garbage"))
    }
}

class RbacTest {
    @Test
    fun `port-forward and pvc deletion are part of the sweep`() {
        assertTrue(RbacRequirement("create", "pods/portforward") in REQUIRED_RBAC)
        assertTrue(RbacRequirement("delete", "persistentvolumeclaims") in REQUIRED_RBAC)
    }

    @Test
    fun `operator rbac adds cr verbs`() {
        val reqs = operatorRbac(crPlural(K8sOperator.psmdb))
        assertTrue(reqs.any { it.verb == "create" && it.resource == "perconaservermongodbs" })
        assertEquals(3, reqs.size)
    }
}
