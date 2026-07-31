package io.mex.mongo

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ParseLogLineTest {
    @Test
    fun `parses a structured 4_4+ line`() {
        val raw = """{"t":{"${'$'}date":"2026-07-31T10:00:00.123+00:00"},"s":"W","c":"NETWORK","id":22943,"ctx":"listener","msg":"Connection refused","attr":{"remote":"1.2.3.4:5"}}"""
        val line = parseLogLine(raw)
        assertEquals("2026-07-31T10:00:00.123Z", line.ts)
        assertEquals("W", line.severity)
        assertEquals("NETWORK", line.component)
        assertEquals("listener", line.context)
        assertEquals("Connection refused", line.message)
        assertTrue(line.attrJson!!.contains("remote"))
    }

    @Test
    fun `an unparseable line survives verbatim instead of being dropped`() {
        val line = parseLogLine("plain old 4.2-style text line")
        assertEquals("plain old 4.2-style text line", line.message)
        assertEquals("I", line.severity)
        assertNull(line.attrJson)
    }

    @Test
    fun `missing fields default sanely`() {
        val line = parseLogLine("""{"msg":"hello"}""")
        assertEquals("hello", line.message)
        assertEquals("I", line.severity)
        assertEquals("", line.component)
    }
}

class SeverityRankTest {
    @Test
    fun `orders F over E over W over I over debug`() {
        assertTrue(severityRank("F") > severityRank("E"))
        assertTrue(severityRank("E") > severityRank("W"))
        assertTrue(severityRank("W") > severityRank("I"))
        assertTrue(severityRank("I") > severityRank("D1"))
        assertEquals(severityRank("D1"), severityRank("D5"))
    }
}

class ClassifyParametersTest {
    @Test
    fun `a curated tunable differing from default is flagged`() {
        val rows = classifyParameters(mapOf("logLevel" to 2), mapOf("logLevel" to 0))
        assertEquals(1, rows.size)
        assertTrue(rows[0].tuned)
        assertEquals("0", rows[0].default)
    }

    @Test
    fun `numeric type differences do not create false tuned flags`() {
        // BSON returns Int/Long/Double interchangeably.
        val rows = classifyParameters(
            mapOf("journalCommitInterval" to 100L, "syncdelay" to 60),
            mapOf("journalCommitInterval" to 100, "syncdelay" to 60.0),
        )
        assertTrue(rows.none { it.tuned })
    }

    @Test
    fun `parameters without a known default get no verdict`() {
        val rows = classifyParameters(mapOf("someObscureFlag" to true), emptyMap())
        assertFalse(rows[0].tuned)
        assertNull(rows[0].default)
    }

    @Test
    fun `command envelope keys are stripped and tuned rows sort first`() {
        val rows = classifyParameters(
            mapOf("ok" to 1.0, "\$clusterTime" to "x", "operationTime" to "y", "aaa" to 1, "logLevel" to 2),
            mapOf("logLevel" to 0),
        )
        assertEquals(listOf("logLevel", "aaa"), rows.map { it.name })
    }
}
