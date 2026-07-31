package io.mex.mongo

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OplogWindowTest {
    @Test
    fun `window and used percent derive from the boundaries`() {
        val info = OplogInfo(firstTsSec = 1_000, lastTsSec = 87_400, usedBytes = 512, maxBytes = 1_024)
        assertEquals(86_400, info.windowSeconds)
        assertEquals(50.0, info.usedPercent)
    }

    @Test
    fun `a window never goes negative`() {
        assertEquals(0, OplogInfo(firstTsSec = 100, lastTsSec = 50, usedBytes = 0, maxBytes = 0).windowSeconds)
    }

    @Test
    fun `severity thresholds match the DBA rules of thumb`() {
        assertEquals(ReplSeverity.ok, oplogWindowSeverity(86_400))       // ≥ 24 h
        assertEquals(ReplSeverity.warn, oplogWindowSeverity(86_399))     // < 24 h
        assertEquals(ReplSeverity.warn, oplogWindowSeverity(3_600))      // ≥ 1 h
        assertEquals(ReplSeverity.critical, oplogWindowSeverity(3_599))  // < 1 h
    }
}

class LagSeverityTest {
    @Test
    fun `lag thresholds`() {
        assertEquals(ReplSeverity.ok, lagSeverity(0))
        assertEquals(ReplSeverity.ok, lagSeverity(9))
        assertEquals(ReplSeverity.warn, lagSeverity(10))
        assertEquals(ReplSeverity.warn, lagSeverity(59))
        assertEquals(ReplSeverity.critical, lagSeverity(60))
    }
}

class LagAgainstPrimaryTest {
    @Test
    fun `lag is primary optime minus member optime`() {
        val members = listOf(
            MemberOptime("p:27017", "PRIMARY", 100_000),
            MemberOptime("s1:27017", "SECONDARY", 95_000),
            MemberOptime("s2:27017", "SECONDARY", 100_000),
        )
        val lag = lagAgainstPrimary(members)
        assertEquals(5L, lag["s1:27017"])
        assertEquals(0L, lag["s2:27017"])
    }

    @Test
    fun `a member ahead of the primary clamps to zero rather than going negative`() {
        val members = listOf(
            MemberOptime("p:27017", "PRIMARY", 100_000),
            MemberOptime("s1:27017", "SECONDARY", 101_000),
        )
        assertEquals(0L, lagAgainstPrimary(members)["s1:27017"])
    }

    @Test
    fun `no primary means no lag can be computed`() {
        val members = listOf(
            MemberOptime("s1:27017", "SECONDARY", 95_000),
            MemberOptime("s2:27017", "SECONDARY", 90_000),
        )
        assertTrue(lagAgainstPrimary(members).isEmpty())
    }

    @Test
    fun `arbiters and unknown states are excluded`() {
        val members = listOf(
            MemberOptime("p:27017", "PRIMARY", 100_000),
            MemberOptime("a:27017", "ARBITER", null),
            MemberOptime("x:27017", "OTHER", 90_000),
        )
        assertTrue(lagAgainstPrimary(members).isEmpty())
    }
}
