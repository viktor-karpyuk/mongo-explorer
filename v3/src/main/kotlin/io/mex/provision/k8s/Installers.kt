package io.mex.provision.k8s

/**
 * Guided operator installation.
 *
 * v3.5 originally shipped detect-and-link-only (K8P-OP-3). In practice that hands the
 * user a dead end: "install the operator" is a five-command dance against version-pinned
 * URLs, and getting it wrong is how you end up with a half-installed CRD set. The app
 * now offers to run it — but never silently: every plan renders the exact commands, they
 * go behind a typed confirm naming the cluster, and the output is streamed verbatim.
 *
 * Steps mirror `testing/kind/install-operators.sh`, the versions this project has
 * actually smoke-tested against.
 */

const val CERT_MANAGER_VERSION = "v1.15.1"
const val MCO_VERSION = "0.10.0"
const val PSMDB_VERSION = "1.17.0"

/** Namespaces the operators are installed into (their own, not the database namespace). */
const val MCO_NAMESPACE = "mongodb"
const val PSMDB_NAMESPACE = "psmdb"

enum class InstallComponent { certManager, mco, psmdb }

/**
 * One command in a plan. [stdin] carries a manifest for `apply -f -` so namespace
 * creation is idempotent without shell pipes.
 */
data class InstallStep(val label: String, val args: List<String>, val stdin: String? = null)

data class InstallPlan(
    val component: InstallComponent,
    val title: String,
    val version: String,
    val summary: String,
    val notes: List<String>,
    val docsUrl: String,
    val steps: List<InstallStep>,
)

private fun namespaceManifest(name: String) = """
    apiVersion: v1
    kind: Namespace
    metadata:
      name: $name
""".trimIndent()

/**
 * Operator CRDs are enormous (the PSMDB one is ~1 MB). Client-side apply stores the whole
 * manifest in the `last-applied-configuration` annotation and the API server rejects it:
 * "metadata.annotations: Too long: may not be more than 262144 bytes". Server-side apply
 * has no such annotation; `--force-conflicts` keeps re-runs idempotent when another field
 * manager (e.g. a previous helm install) owns some fields.
 */
private val SSA = listOf("--server-side", "--force-conflicts")

fun installPlan(component: InstallComponent, target: KubeTarget): InstallPlan {
    val ctx = ctxArgs(target)
    return when (component) {
        InstallComponent.certManager -> InstallPlan(
            component = component,
            title = "cert-manager",
            version = CERT_MANAGER_VERSION,
            summary = "Issues and renews the TLS certificates the Prod profile requires.",
            notes = listOf(
                "Installs cluster-wide CRDs and a controller in the cert-manager namespace.",
                "Safe to run on a cluster that already has cert-manager — apply is idempotent.",
            ),
            docsUrl = "https://cert-manager.io/docs/installation/",
            steps = listOf(
                InstallStep(
                    "Apply cert-manager $CERT_MANAGER_VERSION",
                    listOf(
                        "apply", "-f",
                        "https://github.com/cert-manager/cert-manager/releases/download/$CERT_MANAGER_VERSION/cert-manager.yaml",
                    ) + SSA + ctx,
                ),
                InstallStep(
                    "Wait for the controller",
                    listOf("-n", "cert-manager", "rollout", "status", "deploy/cert-manager", "--timeout=180s") + ctx,
                ),
                InstallStep(
                    "Wait for the webhook",
                    listOf("-n", "cert-manager", "rollout", "status", "deploy/cert-manager-webhook", "--timeout=180s") + ctx,
                ),
                InstallStep(
                    "Wait for the CA injector",
                    listOf("-n", "cert-manager", "rollout", "status", "deploy/cert-manager-cainjector", "--timeout=180s") + ctx,
                ),
            ),
        )

        InstallComponent.mco -> InstallPlan(
            component = component,
            title = "MongoDB Community Operator",
            version = MCO_VERSION,
            summary = "Replica sets with SCRAM and TLS. Cannot express sharded clusters.",
            notes = listOf(
                "Installs a cluster-wide CRD plus a namespaced operator in \"$MCO_NAMESPACE\".",
                "RBAC is namespace-scoped: the operator watches \"$MCO_NAMESPACE\" by default.",
            ),
            docsUrl = "https://github.com/mongodb/mongodb-kubernetes-operator",
            steps = buildList {
                add(
                    InstallStep(
                        "Create namespace $MCO_NAMESPACE",
                        listOf("apply", "-f", "-") + ctx,
                        stdin = namespaceManifest(MCO_NAMESPACE),
                    ),
                )
                add(
                    InstallStep(
                        "Apply the MongoDBCommunity CRD",
                        listOf(
                            "apply", "-f",
                            "$MCO_RAW/v$MCO_VERSION/config/crd/bases/mongodbcommunity.mongodb.com_mongodbcommunity.yaml",
                        ) + SSA + ctx,
                    ),
                )
                for ((label, file) in listOf(
                    "role" to "config/rbac/role.yaml",
                    "role binding" to "config/rbac/role_binding.yaml",
                    "service account" to "config/rbac/service_account.yaml",
                )) {
                    add(
                        InstallStep(
                            "Apply operator $label",
                            listOf("-n", MCO_NAMESPACE, "apply", "-f", "$MCO_RAW/v$MCO_VERSION/$file") + SSA + ctx,
                        ),
                    )
                }
                add(
                    InstallStep(
                        "Apply the operator deployment",
                        listOf(
                            "-n", MCO_NAMESPACE, "apply", "-f",
                            "$MCO_RAW/v$MCO_VERSION/config/manager/manager.yaml",
                        ) + SSA + ctx,
                    ),
                )
                add(
                    InstallStep(
                        "Wait for the operator",
                        listOf(
                            "-n", MCO_NAMESPACE, "rollout", "status",
                            "deploy/mongodb-kubernetes-operator", "--timeout=180s",
                        ) + ctx,
                    ),
                )
            },
        )

        InstallComponent.psmdb -> InstallPlan(
            component = component,
            title = "Percona Server for MongoDB Operator",
            version = PSMDB_VERSION,
            summary = "Replica sets AND sharded clusters, with PBM backups to S3.",
            notes = listOf(
                "Installs cluster-wide CRDs plus a namespaced operator in \"$PSMDB_NAMESPACE\".",
                "Required for sharded topologies — MCO cannot express them.",
            ),
            docsUrl = "https://docs.percona.com/percona-operator-for-mongodb/",
            steps = listOf(
                InstallStep(
                    "Create namespace $PSMDB_NAMESPACE",
                    listOf("apply", "-f", "-") + ctx,
                    stdin = namespaceManifest(PSMDB_NAMESPACE),
                ),
                InstallStep(
                    "Apply the operator bundle $PSMDB_VERSION",
                    listOf(
                        "apply", "-f",
                        "$PSMDB_RAW/v$PSMDB_VERSION/deploy/bundle.yaml",
                        "-n", PSMDB_NAMESPACE,
                    ) + SSA + ctx,
                ),
                InstallStep(
                    "Wait for the operator",
                    listOf(
                        "-n", PSMDB_NAMESPACE, "rollout", "status",
                        "deploy/percona-server-mongodb-operator", "--timeout=180s",
                    ) + ctx,
                ),
            ),
        )
    }
}

private const val MCO_RAW = "https://raw.githubusercontent.com/mongodb/mongodb-kubernetes-operator"
private const val PSMDB_RAW = "https://raw.githubusercontent.com/percona/percona-server-mongodb-operator"

/** Renders the plan the way it will actually be executed, for the preview pane. */
fun previewCommands(plan: InstallPlan, kubectlPath: String = "kubectl"): List<String> =
    plan.steps.map { step ->
        val cmd = (listOf(kubectlPath.substringAfterLast('/')) + step.args).joinToString(" ")
        if (step.stdin != null) "$cmd  <<< (namespace manifest)" else cmd
    }
