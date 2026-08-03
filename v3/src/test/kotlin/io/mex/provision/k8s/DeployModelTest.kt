package io.mex.provision.k8s

import io.mex.data.BackupChoice
import io.mex.data.K8sDeploySpec
import io.mex.data.K8sOperator
import io.mex.data.K8sProfile
import io.mex.data.K8sTopology
import io.mex.data.TlsChoice
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private fun spec(
    profile: K8sProfile = K8sProfile.dev,
    operator: K8sOperator = K8sOperator.psmdb,
    topology: K8sTopology = K8sTopology.ReplicaSet(3),
    tls: TlsChoice = if (profile == K8sProfile.prod) TlsChoice.CertManager("issuer") else TlsChoice.Off,
    backup: BackupChoice = when {
        profile == K8sProfile.dev -> BackupChoice.None
        operator == K8sOperator.psmdb -> BackupChoice.Pbm("https://minio:9000", "bkt", "creds")
        else -> BackupChoice.ByoDeclared
    },
    storageClass: String? = if (profile == K8sProfile.prod) "gp3" else null,
    name: String = "pay-db",
) = K8sDeploySpec(
    name = name, context = "prod-eu", namespace = "payments",
    operator = operator, profile = profile, topology = topology,
    mongoVersion = "8.0", storageClass = storageClass,
    tls = tls, backup = backup,
)

class ValidateDevTest {
    @Test
    fun `dev preset is permissive`() {
        assertEquals(emptyList(), validate(spec()))
        assertEquals(emptyList(), validate(spec(topology = K8sTopology.ReplicaSet(1))))
        assertEquals(
            emptyList(),
            validate(spec(topology = K8sTopology.Sharded(2, 1, mongos = 1), tls = TlsChoice.OperatorSelfSigned)),
        )
    }

    @Test
    fun `names must be dns-1123`() {
        assertTrue(validate(spec(name = "Pay_DB")).isNotEmpty())
        assertTrue(validate(spec(name = "-bad")).isNotEmpty())
        assertEquals(emptyList(), validate(spec(name = "pay-db-2")))
    }

    @Test
    fun `mco cannot shard and has no self-signed mode`() {
        val sharded = validate(spec(operator = K8sOperator.mco, topology = K8sTopology.Sharded(2, 3)))
        assertTrue(sharded.any { "MCO cannot express sharded" in it })
        assertTrue(
            validate(spec(operator = K8sOperator.mco, tls = TlsChoice.OperatorSelfSigned))
                .any { "self-signed" in it },
        )
    }
}

class ProdLockTest {
    @Test
    fun `a fully locked prod spec is valid`() {
        assertEquals(emptyList(), validate(spec(profile = K8sProfile.prod)))
        assertEquals(
            emptyList(),
            validate(spec(profile = K8sProfile.prod, topology = K8sTopology.Sharded(3, 3, mongos = 2))),
        )
    }

    @Test
    fun `every lock produces its own message`() {
        assertTrue(validate(spec(profile = K8sProfile.prod, tls = TlsChoice.Off)).any { "locks TLS on" in it })
        assertTrue(
            validate(spec(profile = K8sProfile.prod, tls = TlsChoice.OperatorSelfSigned))
                .any { "self-signed" in it },
        )
        assertTrue(
            validate(spec(profile = K8sProfile.prod, storageClass = null)).any { "StorageClass" in it },
        )
        assertTrue(
            validate(spec(profile = K8sProfile.prod, backup = BackupChoice.None)).any { "backups" in it.lowercase() },
        )
    }

    @Test
    fun `prod member minimums - no 1-member anything`() {
        assertTrue(validate(spec(profile = K8sProfile.prod, topology = K8sTopology.ReplicaSet(1))).isNotEmpty())
        assertTrue(
            validate(spec(profile = K8sProfile.prod, topology = K8sTopology.Sharded(2, 1))).isNotEmpty(),
        )
        assertTrue(
            validate(spec(profile = K8sProfile.prod, topology = K8sTopology.Sharded(2, 3, mongos = 1)))
                .any { "Mongos" in it },
        )
    }

    @Test
    fun `backup policy per operator - psmdb pbm, mco declared byo`() {
        assertTrue(
            validate(spec(profile = K8sProfile.prod, backup = BackupChoice.ByoDeclared))
                .any { "PBM" in it }, // psmdb must use pbm
        )
        assertEquals(
            emptyList(),
            validate(spec(profile = K8sProfile.prod, operator = K8sOperator.mco, backup = BackupChoice.ByoDeclared)),
        )
        assertTrue(
            validate(spec(operator = K8sOperator.mco, backup = BackupChoice.Pbm("e", "b", "s")))
                .any { "PSMDB-only" in it },
        )
    }

    @Test
    fun `pbm needs endpoint bucket and secret`() {
        val v = validate(spec(profile = K8sProfile.prod, backup = BackupChoice.Pbm("", "", "")))
        assertEquals(3, v.size)
    }
}

class SummaryTest {
    @Test
    fun `summaries are compact`() {
        assertEquals("replica set ×5", k8sSummary(K8sTopology.ReplicaSet(5)))
        assertEquals(
            "sharded 2×3 · csrs 3 · mongos 2",
            k8sSummary(K8sTopology.Sharded(2, 3, mongos = 2, configServers = 3)),
        )
    }
}
