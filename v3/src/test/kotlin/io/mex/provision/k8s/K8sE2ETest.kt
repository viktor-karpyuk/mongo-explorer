package io.mex.provision.k8s

import io.mex.AppContext
import io.mex.backup.BackupsRepo
import io.mex.data.BackupChoice
import io.mex.data.ConnectionsRepo
import io.mex.data.K8sDeploySpec
import io.mex.data.K8sDeployStatus
import io.mex.data.K8sDeploymentsRepo
import io.mex.data.K8sOperator
import io.mex.data.K8sProfile
import io.mex.data.K8sTopology
import io.mex.data.LabsRepo
import io.mex.data.MigrationJobsRepo
import io.mex.data.PrefsRepo
import io.mex.data.QueryHistoryRepo
import io.mex.data.Store
import io.mex.data.TlsChoice
import io.mex.data.UriHistoryRepo
import io.mex.mongo.MongoRegistry
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * E2E against a real Kubernetes cluster with an operator installed (spec §9).
 * Gated on MEX_K8S_E2E=1 — it applies real resources and takes minutes:
 *
 *   kind create cluster --name mex-k8s-it   # or k3d / minikube
 *   ./testing/kind/install-operators.sh
 *   MEX_K8S_E2E=1 MEX_K8S_CTX=kind-mex-k8s-it \
 *     ./gradlew :v3:test --tests 'io.mex.provision.k8s.K8sE2ETest'
 *
 * A single-node cluster cannot satisfy the Prod zone spread, so the prod case asserts
 * the *rendered* locks and the preflight warning rather than reaching ready.
 */
class K8sE2ETest {
    private val enabled = System.getenv("MEX_K8S_E2E") == "1"
    private val ctxName = System.getenv("MEX_K8S_CTX") ?: "kind-mex-k8s-it"
    private val namespace = System.getenv("MEX_K8S_NS") ?: "mex-e2e"

    private val dir = Files.createTempDirectory("mex-k8s-e2e")
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
            k8sDeployments = K8sDeploymentsRepo(store),
            dataDir = dir,
        )
    }
    private val runner by lazy { DeployRunner(ctx, MongoRegistry()) }
    private val tool by lazy { findKubectl() }

    private fun devSpec(name: String) = K8sDeploySpec(
        name = name, context = ctxName, namespace = namespace, namespaceCreated = true,
        operator = K8sOperator.psmdb, profile = K8sProfile.dev,
        topology = K8sTopology.ReplicaSet(1),
        mongoVersion = "7.0", storageGi = 1, cpu = "300m", memory = "512Mi",
        tls = TlsChoice.OperatorSelfSigned, backup = BackupChoice.None,
    )

    @AfterTest
    fun tearDown() {
        if (!enabled) return
        for (d in ctx.k8sDeployments.list()) {
            runCatching {
                exec(listOf("kubectl", "delete", "ns", d.spec.namespace, "--ignore-not-found", "--context", ctxName), 180)
            }
        }
        store.close()
        dir.toFile().deleteRecursively()
    }

    @Test
    fun `E0 cluster is reachable and an operator is installed`() {
        if (!enabled) return
        val t = tool ?: throw AssertionError("kubectl not found")
        val target = KubeTarget(ctxName)
        val version = serverVersion(t, target)
        assertTrue(version !is VersionVerdict.Unknown, "context $ctxName unreachable: $version")
        val ops = detectOperators(t, target)
        assertTrue(ops.isNotEmpty(), "no MongoDB operator detected — run testing/kind/install-operators.sh")
    }

    @Test
    fun `E1 dev replica set applies, reaches ready, registers a connection, then tears down`() {
        if (!enabled) return
        val spec = devSpec("e2e-rs")
        val hash = bundleHash(render(spec))
        val id = runner.apply(spec, hash)

        val d = awaitStatus(id, K8sDeployStatus.ready, timeoutSec = 900)
        assertTrue(d.bundleHash == hash)
        assertTrue(d.connectionId != null, "a ready deployment must register a connection")
        val conn = ctx.connections.get(d.connectionId!!)!!
        assertTrue("127.0.0.1:" in conn.uri && "directConnection=true" in conn.uri)

        runner.teardown(id, releasePvcs = true, alsoConnection = true)
        awaitGone(id, timeoutSec = 600)
        assertNull(ctx.k8sDeployments.get(id))
        assertNull(ctx.connections.get(conn.id))
    }

    @Test
    fun `E3 apply with a stale hash is refused and nothing is created`() {
        if (!enabled) return
        val spec = devSpec("e2e-stale")
        val staleHash = bundleHash(render(spec.copy(storageGi = 99)))
        val id = runner.apply(spec, staleHash)

        val d = awaitStatus(id, K8sDeployStatus.failed, timeoutSec = 120)
        assertTrue(d.error!!.contains("stale preview"), d.error)
        // Nothing reached the cluster.
        val out = exec(
            listOf("kubectl", "get", "perconaservermongodbs", "-n", namespace, "-o", "name", "--context", ctxName),
            30,
        )
        assertTrue("e2e-stale" !in out, out)
        ctx.k8sDeployments.delete(id)
    }

    @Test
    fun `E5 out-of-band CR deletion reconciles the row to missing`() {
        if (!enabled) return
        val spec = devSpec("e2e-oob")
        val id = runner.apply(spec, bundleHash(render(spec)))
        awaitStatus(id, K8sDeployStatus.ready, timeoutSec = 900)

        exec(
            listOf("kubectl", "delete", "perconaservermongodb", "e2e-oob", "-n", namespace, "--context", ctxName, "--wait=false"),
            120,
        )
        runner.refresh(id)
        val d = awaitStatus(id, K8sDeployStatus.missing, timeoutSec = 180)
        assertEquals(K8sDeployStatus.missing, d.status)
        runner.teardown(id, releasePvcs = true, alsoConnection = true)
        awaitGone(id, timeoutSec = 300)
    }

    /* ===================== helpers ===================== */

    private fun awaitStatus(id: String, expected: K8sDeployStatus, timeoutSec: Long): io.mex.data.K8sDeployment {
        val deadline = System.currentTimeMillis() + timeoutSec * 1000
        while (System.currentTimeMillis() < deadline) {
            val d = ctx.k8sDeployments.get(id) ?: throw AssertionError("row vanished")
            if (d.status == expected) return d
            if (expected != K8sDeployStatus.failed && d.status == K8sDeployStatus.failed) {
                throw AssertionError("deployment failed: ${d.error}")
            }
            Thread.sleep(5000)
        }
        val last = ctx.k8sDeployments.get(id)
        throw AssertionError("timed out waiting for $expected — last ${last?.status}/${last?.statusDetail}/${last?.error}")
    }

    private fun awaitGone(id: String, timeoutSec: Long) {
        val deadline = System.currentTimeMillis() + timeoutSec * 1000
        while (System.currentTimeMillis() < deadline) {
            if (ctx.k8sDeployments.get(id) == null) return
            Thread.sleep(3000)
        }
        throw AssertionError("timed out waiting for teardown")
    }

    private fun exec(cmd: List<String>, timeoutSec: Long): String {
        val p = ProcessBuilder(cmd).redirectErrorStream(true).start()
        val out = p.inputStream.bufferedReader().readText()
        p.waitFor(timeoutSec, TimeUnit.SECONDS)
        return out
    }
}
