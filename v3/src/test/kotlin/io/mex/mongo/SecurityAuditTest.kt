package io.mex.mongo

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private fun facts(
    auth: Boolean = true,
    tls: String? = "requireTLS",
    bindAll: Boolean = false,
    version: String = "8.0.0",
    superusers: Int = 1,
    js: Boolean? = false,
    clusterAuth: String? = null,
    audit: Boolean? = null,
) = SecurityFacts(
    authEnabled = auth,
    tlsMode = tls,
    bindIp = if (bindAll) "0.0.0.0" else "127.0.0.1",
    bindIpAll = bindAll,
    serverVersion = version,
    userCount = 3,
    superuserCount = superusers,
    usersWithoutTls = false,
    javascriptEnabled = js,
    auditLogEnabled = audit,
    clusterAuthMode = clusterAuth,
)

private fun List<SecurityCheck>.byId(id: String) = single { it.id == id }

class ParseMajorTest {
    @Test
    fun `reads the major version`() {
        assertEquals(8, parseMajor("8.0.4"))
        assertEquals(4, parseMajor("4.4.29"))
        assertEquals(null, parseMajor("unknown"))
    }
}

class EvaluateSecurityTest {
    @Test
    fun `no auth is a failure`() {
        assertEquals(CheckLevel.fail, evaluateSecurity(facts(auth = false)).byId("AUTH").level)
    }

    @Test
    fun `auth enabled passes`() {
        assertEquals(CheckLevel.pass, evaluateSecurity(facts(auth = true)).byId("AUTH").level)
    }

    @Test
    fun `tls modes map to the right severity`() {
        assertEquals(CheckLevel.pass, evaluateSecurity(facts(tls = "requireTLS")).byId("TLS").level)
        assertEquals(CheckLevel.warn, evaluateSecurity(facts(tls = "preferTLS")).byId("TLS").level)
        assertEquals(CheckLevel.fail, evaluateSecurity(facts(tls = "disabled")).byId("TLS").level)
        assertEquals(CheckLevel.fail, evaluateSecurity(facts(tls = null)).byId("TLS").level)
    }

    @Test
    fun `bind to all interfaces warns`() {
        assertEquals(CheckLevel.warn, evaluateSecurity(facts(bindAll = true)).byId("BIND").level)
        assertEquals(CheckLevel.pass, evaluateSecurity(facts(bindAll = false)).byId("BIND").level)
    }

    @Test
    fun `an ancient version fails, a trailing one warns, current passes`() {
        assertEquals(CheckLevel.fail, evaluateSecurity(facts(version = "4.4.0")).byId("VER").level)
        assertEquals(CheckLevel.warn, evaluateSecurity(facts(version = "7.0.0")).byId("VER").level)
        assertEquals(CheckLevel.pass, evaluateSecurity(facts(version = "8.0.0")).byId("VER").level)
    }

    @Test
    fun `superuser sprawl warns`() {
        assertEquals(CheckLevel.pass, evaluateSecurity(facts(superusers = 2)).byId("SUPER").level)
        assertEquals(CheckLevel.warn, evaluateSecurity(facts(superusers = 5)).byId("SUPER").level)
    }

    @Test
    fun `server-side javascript on warns, off passes`() {
        assertEquals(CheckLevel.warn, evaluateSecurity(facts(js = true)).byId("JS").level)
        assertEquals(CheckLevel.pass, evaluateSecurity(facts(js = false)).byId("JS").level)
    }

    @Test
    fun `optional checks are omitted when the fact is null`() {
        val checks = evaluateSecurity(facts(js = null, clusterAuth = null, audit = null))
        assertTrue(checks.none { it.id == "JS" })
        assertTrue(checks.none { it.id == "CLUSTERAUTH" })
        assertTrue(checks.none { it.id == "AUDIT" })
    }

    @Test
    fun `keyfile cluster auth warns, x509 passes`() {
        assertEquals(CheckLevel.warn, evaluateSecurity(facts(clusterAuth = "keyFile")).byId("CLUSTERAUTH").level)
        assertEquals(CheckLevel.pass, evaluateSecurity(facts(clusterAuth = "x509")).byId("CLUSTERAUTH").level)
    }

    @Test
    fun `failing checks carry remediation, passing ones do not`() {
        val checks = evaluateSecurity(facts(auth = false))
        assertTrue(checks.byId("AUTH").remediation.isNotBlank())
        assertTrue(checks.byId("BIND").remediation.isBlank()) // passing
    }
}

class SecurityScoreTest {
    @Test
    fun `a clean posture scores 100`() {
        assertEquals(100, securityScore(evaluateSecurity(facts())))
    }

    @Test
    fun `failures drag the score down hard`() {
        val insecure = securityScore(evaluateSecurity(facts(auth = false, tls = "disabled", bindAll = true, version = "4.2.0")))
        assertTrue(insecure < 50, "expected a low score, got $insecure")
    }

    @Test
    fun `info-only checks do not affect the score`() {
        // clusterAuth "unknown" is info; it must not change a perfect score.
        assertEquals(100, securityScore(evaluateSecurity(facts(clusterAuth = "something-odd"))))
    }
}
