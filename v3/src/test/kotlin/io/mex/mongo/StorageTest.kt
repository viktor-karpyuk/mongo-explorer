package io.mex.mongo

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private fun idx(
    name: String,
    ops: Long,
    size: Long = 1_000,
    unique: Boolean = false,
    ttl: Long? = null,
    since: Long? = null,
) = IndexUsage("app", "users", name, ops, since, size, unique, ttl)

class FragmentationTest {
    @Test
    fun `prefers WiredTiger's exact free bytes`() {
        assertEquals(25.0, fragmentationPct(dataSize = 10, storageSize = 1_000, freeStorageSize = 250))
    }

    @Test
    fun `falls back to the data-storage ratio`() {
        assertEquals(40.0, fragmentationPct(dataSize = 600, storageSize = 1_000, freeStorageSize = null))
    }

    @Test
    fun `empty storage is zero, not division by zero`() {
        assertEquals(0.0, fragmentationPct(dataSize = 0, storageSize = 0, freeStorageSize = null))
    }

    @Test
    fun `compressed collections clamp at zero rather than going negative`() {
        // data > storage happens with compression; the estimate must not go below 0.
        assertEquals(0.0, fragmentationPct(dataSize = 2_000, storageSize = 1_000, freeStorageSize = null))
    }
}

class UnusedCandidatesTest {
    @Test
    fun `zero-ops secondary indexes rank by wasted bytes`() {
        val out = unusedCandidates(
            listOf(
                idx("small_idx", ops = 0, size = 10),
                idx("big_idx", ops = 0, size = 9_999),
                idx("hot_idx", ops = 42),
            ),
        )
        assertEquals(listOf("big_idx", "small_idx"), out.map { it.name })
    }

    @Test
    fun `the _id index is never a candidate`() {
        assertTrue(unusedCandidates(listOf(idx("_id_", ops = 0))).isEmpty())
    }

    @Test
    fun `TTL indexes are excluded — zero reads is their normal state`() {
        assertTrue(unusedCandidates(listOf(idx("expiry_idx", ops = 0, ttl = 3_600))).isEmpty())
    }

    @Test
    fun `unique indexes stay listed — the badge warns, the list does not hide`() {
        val out = unusedCandidates(listOf(idx("uq_email", ops = 0, unique = true)))
        assertEquals(listOf("uq_email"), out.map { it.name })
    }
}

class ObservationDaysTest {
    @Test
    fun `days since the counters started`() {
        val now = 10L * 86_400_000
        assertEquals(3L, observationDays(7L * 86_400_000, now))
    }

    @Test
    fun `unknown start is unknown, future clamps to zero`() {
        assertEquals(null, observationDays(null))
        assertEquals(0L, observationDays(2L * 86_400_000, 86_400_000))
    }
}

class CollWeightTest {
    @Test
    fun `total is storage plus indexes`() {
        val c = CollWeight("app", "users", 10, 1.0, 500, 1_000, null, 2, 300, emptyMap())
        assertEquals(1_300, c.totalSize)
        assertEquals("app.users", c.ns)
    }
}
