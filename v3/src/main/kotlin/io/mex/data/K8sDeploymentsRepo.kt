package io.mex.data

import com.github.f4b6a3.ulid.UlidCreator
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

private val json = Json { ignoreUnknownKeys = true }

class K8sDeploymentsRepo(private val store: Store) {
    private val conn get() = store.conn

    /** Rows mid-apply/mid-teardown when the app died can never finish (PRV-LIFE doctrine). */
    fun reconcileOrphans() {
        conn.prepareStatement(
            "UPDATE k8s_deployments SET status = 'failed', error = 'interrupted — the app closed during the operation' WHERE status IN ('applying','deleting')",
        ).use { it.executeUpdate() }
    }

    fun list(): List<K8sDeployment> {
        val out = mutableListOf<K8sDeployment>()
        conn.prepareStatement("SELECT * FROM k8s_deployments ORDER BY created_at DESC").use { ps ->
            ps.executeQuery().use { rs ->
                while (rs.next()) parse(rs)?.let { out += it }
            }
        }
        return out
    }

    fun get(id: String): K8sDeployment? {
        conn.prepareStatement("SELECT * FROM k8s_deployments WHERE id = ?").use { ps ->
            ps.setString(1, id)
            ps.executeQuery().use { rs ->
                return if (rs.next()) parse(rs) else null
            }
        }
    }

    fun nameExists(context: String, namespace: String, name: String): Boolean {
        conn.prepareStatement(
            "SELECT 1 FROM k8s_deployments WHERE context = ? AND namespace = ? AND name = ?",
        ).use { ps ->
            ps.setString(1, context)
            ps.setString(2, namespace)
            ps.setString(3, name)
            ps.executeQuery().use { rs -> return rs.next() }
        }
    }

    fun create(spec: K8sDeploySpec): K8sDeployment {
        val id = UlidCreator.getUlid().toString()
        conn.prepareStatement(
            """
            INSERT INTO k8s_deployments (id, name, context, namespace, spec, status, created_at)
            VALUES (?, ?, ?, ?, ?, 'applying', ?)
            """.trimIndent(),
        ).use { ps ->
            ps.setString(1, id)
            ps.setString(2, spec.name)
            ps.setString(3, spec.context)
            ps.setString(4, spec.namespace)
            ps.setString(5, json.encodeToString(spec))
            ps.setLong(6, System.currentTimeMillis())
            ps.executeUpdate()
        }
        return get(id)!!
    }

    fun setStatus(id: String, status: K8sDeployStatus, detail: String? = null, error: String? = null) {
        conn.prepareStatement(
            "UPDATE k8s_deployments SET status = ?, status_detail = ?, error = ? WHERE id = ?",
        ).use { ps ->
            ps.setString(1, status.name)
            ps.setString(2, detail)
            ps.setString(3, error)
            ps.setString(4, id)
            ps.executeUpdate()
        }
    }

    fun setApplied(id: String, bundleHash: String, spec: K8sDeploySpec) {
        conn.prepareStatement(
            "UPDATE k8s_deployments SET bundle_hash = ?, applied_at = ?, spec = ? WHERE id = ?",
        ).use { ps ->
            ps.setString(1, bundleHash)
            ps.setLong(2, System.currentTimeMillis())
            ps.setString(3, json.encodeToString(spec))
            ps.setString(4, id)
            ps.executeUpdate()
        }
    }

    fun setConnection(id: String, connectionId: String?) {
        conn.prepareStatement("UPDATE k8s_deployments SET connection_id = ? WHERE id = ?").use { ps ->
            ps.setString(1, connectionId)
            ps.setString(2, id)
            ps.executeUpdate()
        }
    }

    fun delete(id: String) {
        conn.prepareStatement("DELETE FROM k8s_deployments WHERE id = ?").use { ps ->
            ps.setString(1, id)
            ps.executeUpdate()
        }
    }

    /** Connection ids owned by k8s deployments — feeds the K8S badge (K8P-CONN-3). */
    fun connectionIds(): Set<String> {
        val out = mutableSetOf<String>()
        conn.prepareStatement("SELECT connection_id FROM k8s_deployments WHERE connection_id IS NOT NULL").use { ps ->
            ps.executeQuery().use { rs ->
                while (rs.next()) out += rs.getString(1)
            }
        }
        return out
    }

    /** Null on unreadable spec JSON (forward-compat) — one row must not sink the view. */
    private fun parse(rs: java.sql.ResultSet): K8sDeployment? {
        val spec = rs.getString("spec").let {
            runCatching { json.decodeFromString<K8sDeploySpec>(it) }.getOrNull()
        } ?: return null
        return K8sDeployment(
            id = rs.getString("id"),
            spec = spec,
            status = K8sDeployStatus.valueOf(rs.getString("status")),
            statusDetail = rs.getString("status_detail"),
            bundleHash = rs.getString("bundle_hash"),
            connectionId = rs.getString("connection_id"),
            error = rs.getString("error"),
            createdAt = rs.getLong("created_at"),
            appliedAt = rs.getLong("applied_at").takeIf { !rs.wasNull() },
        )
    }
}
