package io.mex.migration

import io.mex.data.ConflictPolicy
import io.mex.data.MigrationCheckpoint
import io.mex.data.MigrationJob
import io.mex.data.MigrationNs
import io.mex.data.MigrationSpec
import io.mex.data.MigrationStatus
import org.bson.Document
import org.bson.types.Decimal128
import org.bson.types.ObjectId
import java.math.BigDecimal
import java.util.Date
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class EncodeLastIdTest {
    private fun roundTrip(id: Any?): Any? = Document.parse(encodeLastId(id))["_id"]

    @Test
    fun `round-trips an ObjectId`() {
        val id = ObjectId()
        assertEquals(id, roundTrip(id))
    }

    @Test
    fun `round-trips a string id`() {
        assertEquals("user-42", roundTrip("user-42"))
    }

    @Test
    fun `round-trips numeric ids without collapsing the type`() {
        assertEquals(7L, roundTrip(7L))
        assertEquals(7, roundTrip(7))
    }

    @Test
    fun `round-trips a Decimal128`() {
        val id = Decimal128(BigDecimal("1.5000"))
        assertEquals(id, roundTrip(id))
    }

    @Test
    fun `round-trips a date`() {
        val id = Date(1_700_000_000_000)
        assertEquals(id, roundTrip(id))
    }

    @Test
    fun `round-trips a compound id`() {
        val id = Document("tenant", "acme").append("seq", 3)
        assertEquals(id, roundTrip(id))
    }
}

class ResumeFilterTest {
    @Test
    fun `no checkpoint copies from the start`() {
        assertEquals(Document(), resumeFilter(null))
    }

    @Test
    fun `unparseable legacy checkpoint restarts the namespace rather than throwing`() {
        assertEquals(Document(), resumeFilter("ObjectId(\"abc\""))
    }

    @Test
    fun `resumes with a type-agnostic comparison`() {
        val id = ObjectId()
        val filter = resumeFilter(encodeLastId(id))
        // A plain {_id: {$gt: …}} is type-bracketed and would skip every document whose
        // _id type sorts before the checkpoint's, so the filter must use $expr.
        val expr = filter["\$expr"] as? Document
        assertNotNull(expr, "expected an \$expr filter, got $filter")
        val args = expr["\$gt"] as List<*>
        assertEquals("\$_id", args[0])
        assertEquals(id, args[1])
    }

    @Test
    fun `preserves the id type across the checkpoint`() {
        val filter = resumeFilter(encodeLastId("abc"))
        val args = (filter["\$expr"] as Document)["\$gt"] as List<*>
        assertEquals("abc", args[1])
    }
}

class ReplayPolicyTest {
    @Test
    fun `abort and drop replay idempotently`() {
        // Replaying the pending batch under abort would hit a duplicate _id and fail the
        // job permanently, which is exactly the crash-resume bug this guards.
        assertEquals(ConflictPolicy.upsert, replayPolicy(ConflictPolicy.abort))
        assertEquals(ConflictPolicy.upsert, replayPolicy(ConflictPolicy.drop))
    }

    @Test
    fun `already-idempotent policies keep their semantics`() {
        assertEquals(ConflictPolicy.append, replayPolicy(ConflictPolicy.append))
        assertEquals(ConflictPolicy.upsert, replayPolicy(ConflictPolicy.upsert))
    }
}

class ResumableTest {
    private fun job(status: MigrationStatus, cp: MigrationCheckpoint?) = MigrationJob(
        id = "j",
        spec = MigrationSpec("s", "t", listOf(MigrationNs("db", "c"))),
        status = status,
        startedAt = null,
        finishedAt = null,
        error = null,
        checkpoint = cp,
    )

    @Test
    fun `a failed job with progress can resume`() {
        assertTrue(job(MigrationStatus.failed, MigrationCheckpoint(copiedInNs = 10)).resumable)
    }

    @Test
    fun `a cancelled job with progress can resume`() {
        assertTrue(job(MigrationStatus.cancelled, MigrationCheckpoint(nsIndex = 1)).resumable)
    }

    @Test
    fun `a failed job that never started has nothing to resume`() {
        assertFalse(job(MigrationStatus.failed, MigrationCheckpoint()).resumable)
        assertFalse(job(MigrationStatus.failed, null).resumable)
    }

    @Test
    fun `a completed job is not resumable`() {
        assertFalse(job(MigrationStatus.completed, MigrationCheckpoint(copiedInNs = 10)).resumable)
    }
}
