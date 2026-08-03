package io.mex.provision.k8s

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private val T = KubeTarget("prod-eu")

class InstallPlanTest {
    @Test
    fun `every step targets the chosen context explicitly`() {
        for (c in InstallComponent.entries) {
            val plan = installPlan(c, T)
            assertTrue(plan.steps.isNotEmpty(), "${c.name} has no steps")
            plan.steps.forEach { step ->
                assertTrue("--context" in step.args, "${c.name}/${step.label} missing --context")
                assertTrue("prod-eu" in step.args)
            }
        }
    }

    @Test
    fun `kubeconfig flag threads through when set`() {
        val plan = installPlan(InstallComponent.psmdb, KubeTarget("c", "/tmp/kc"))
        assertTrue(plan.steps.all { "--kubeconfig" in it.args && "/tmp/kc" in it.args })
    }

    @Test
    fun `psmdb installs the bundle into its own namespace and waits for the operator`() {
        val plan = installPlan(InstallComponent.psmdb, T)
        val ns = plan.steps.first()
        assertTrue(ns.stdin!!.contains("kind: Namespace") && ns.stdin!!.contains(PSMDB_NAMESPACE))
        assertTrue(plan.steps.any { it.args.any { a -> a.endsWith("/deploy/bundle.yaml") } })
        val wait = plan.steps.last()
        assertTrue("rollout" in wait.args && "deploy/percona-server-mongodb-operator" in wait.args)
        assertTrue(plan.version == PSMDB_VERSION)
    }

    @Test
    fun `mco applies crd, rbac trio, manager, then waits`() {
        val plan = installPlan(InstallComponent.mco, T)
        val urls = plan.steps.flatMap { it.args }.filter { it.startsWith("https://") }
        assertTrue(urls.any { it.contains("mongodbcommunity.mongodb.com_mongodbcommunity.yaml") })
        assertTrue(urls.any { it.endsWith("config/rbac/role.yaml") })
        assertTrue(urls.any { it.endsWith("config/rbac/role_binding.yaml") })
        assertTrue(urls.any { it.endsWith("config/rbac/service_account.yaml") })
        assertTrue(urls.any { it.endsWith("config/manager/manager.yaml") })
        assertTrue(urls.all { "v$MCO_VERSION" in it }, "all URLs must be version-pinned")
        assertTrue(plan.steps.last().args.contains("deploy/mongodb-kubernetes-operator"))
    }

    @Test
    fun `cert-manager waits for all three deployments`() {
        val plan = installPlan(InstallComponent.certManager, T)
        val waits = plan.steps.filter { "rollout" in it.args }
        assertEquals(3, waits.size)
        assertTrue(waits.any { "deploy/cert-manager-webhook" in it.args })
        assertTrue(waits.any { "deploy/cert-manager-cainjector" in it.args })
    }

    @Test
    fun `manifest applies are server-side`() {
        // Client-side apply blows the 256KB last-applied annotation limit on the operator
        // CRDs — verified against a real cluster, not theoretical.
        for (c in InstallComponent.entries) {
            installPlan(c, T).steps
                .filter { it.args.contains("apply") && it.args.none { a -> a == "-" } }
                .forEach { step ->
                    assertTrue("--server-side" in step.args, "${c.name}/${step.label} must be server-side")
                    assertTrue("--force-conflicts" in step.args, "${c.name}/${step.label} must tolerate re-runs")
                }
        }
    }

    @Test
    fun `namespace creation is idempotent via apply, never create`() {
        for (c in listOf(InstallComponent.mco, InstallComponent.psmdb)) {
            val first = installPlan(c, T).steps.first()
            assertTrue("apply" in first.args)
            assertFalse("create" in first.args)
            assertTrue(first.stdin != null)
        }
    }

    @Test
    fun `the CR version the renderer emits matches the operator the installer deploys`() {
        // A crVersion ahead of the running operator is rejected outright.
        assertEquals(PSMDB_VERSION, io.mex.provision.k8s.PSMDB_VERSION)
        val spec = io.mex.data.K8sDeploySpec(
            name = "x", context = "c", namespace = "n",
            operator = io.mex.data.K8sOperator.psmdb, profile = io.mex.data.K8sProfile.dev,
            topology = io.mex.data.K8sTopology.ReplicaSet(1), mongoVersion = "7.0",
            tls = io.mex.data.TlsChoice.Off, backup = io.mex.data.BackupChoice.None,
        )
        val yaml = render(spec).first { it.kind == "PerconaServerMongoDB" }.yaml
        assertTrue("crVersion: $PSMDB_VERSION" in yaml, yaml.lines().first { "crVersion" in it })
    }
}

class PreviewCommandsTest {
    @Test
    fun `preview shows the binary name and flags a stdin manifest`() {
        val cmds = previewCommands(installPlan(InstallComponent.psmdb, T), "/usr/local/bin/kubectl")
        assertTrue(cmds.first().startsWith("kubectl apply -f -"))
        assertTrue(cmds.first().contains("namespace manifest"))
        assertTrue(cmds.any { it.contains("bundle.yaml") })
        assertEquals(installPlan(InstallComponent.psmdb, T).steps.size, cmds.size)
    }
}
