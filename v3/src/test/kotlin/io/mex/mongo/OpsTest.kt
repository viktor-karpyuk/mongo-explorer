package io.mex.mongo

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private fun op(
    planSummary: String? = null,
    desc: String? = null,
) = CurrentOp(
    opidRaw = 42,
    opid = "42",
    active = true,
    op = "query",
    ns = "app.users",
    secsRunning = 5,
    planSummary = planSummary,
    appName = null,
    client = null,
    effectiveUser = null,
    waitingForLock = false,
    command = null,
    desc = desc,
)

class CurrentOpTest {
    @Test
    fun `a collection scan is flagged`() {
        assertTrue(op(planSummary = "COLLSCAN").collscan)
        assertFalse(op(planSummary = "IXSCAN { _id: 1 }").collscan)
        assertFalse(op(planSummary = null).collscan)
    }

    @Test
    fun `server-internal threads are classified as system ops`() {
        assertTrue(op(desc = "Checkpointer").system)
        assertTrue(op(desc = "TTLMonitor").system)
        assertTrue(op(desc = "WTJournalFlusher").system)
        assertTrue(op(desc = "ReplBatcher").system)
    }

    @Test
    fun `client operations are not system ops`() {
        assertFalse(op(desc = "conn123").system)
        assertFalse(op(desc = null).system)
    }
}
