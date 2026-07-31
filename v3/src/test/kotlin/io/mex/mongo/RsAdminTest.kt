package io.mex.mongo

import org.bson.Document
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private fun member(
    id: Int,
    host: String = "m$id:27017",
    priority: Double = 1.0,
    votes: Int = 1,
    hidden: Boolean = false,
    delay: Long = 0,
) = RsMemberConfig(id, host, priority, votes, false, hidden, true, delay, emptyMap())

private fun patch(
    id: Int = 1,
    priority: Double = 1.0,
    votes: Int = 1,
    hidden: Boolean = false,
    delay: Long = 0,
) = MemberPatch(id, "m$id:27017", priority, votes, hidden, delay)

class StepDownCommandTest {
    @Test
    fun `builds the exact command the preview shows`() {
        val cmd = stepDownCommand(60, 10)
        assertEquals(60, cmd["replSetStepDown"])
        assertEquals(10, cmd["secondaryCatchUpPeriodSecs"])
    }
}

class ValidateMemberPatchTest {
    @Test
    fun `a plain voting member is valid`() {
        assertTrue(validateMemberPatch(patch()).isEmpty())
    }

    @Test
    fun `hidden requires priority zero`() {
        assertTrue(validateMemberPatch(patch(priority = 1.0, hidden = true)).isNotEmpty())
        assertTrue(validateMemberPatch(patch(priority = 0.0, hidden = true, votes = 1)).isEmpty())
    }

    @Test
    fun `delayed requires priority zero`() {
        assertTrue(validateMemberPatch(patch(priority = 1.0, delay = 3600)).isNotEmpty())
        assertTrue(validateMemberPatch(patch(priority = 0.0, delay = 3600, votes = 1)).isEmpty())
    }

    @Test
    fun `an electable member must vote`() {
        assertTrue(validateMemberPatch(patch(priority = 5.0, votes = 0)).isNotEmpty())
    }

    @Test
    fun `bounds are enforced`() {
        assertTrue(validateMemberPatch(patch(priority = 1001.0)).isNotEmpty())
        assertTrue(validateMemberPatch(patch(priority = -1.0)).isNotEmpty())
        assertTrue(validateMemberPatch(patch(votes = 2)).isNotEmpty())
        assertTrue(validateMemberPatch(patch(delay = -5)).isNotEmpty())
    }
}

class BuildReconfigTest {
    private val raw = Document("_id", "rs0")
        .append("version", 7)
        .append(
            "members",
            listOf(
                Document("_id", 0).append("host", "m0:27017").append("priority", 1.0)
                    .append("votes", 1).append("hidden", false),
                Document("_id", 1).append("host", "m1:27017").append("priority", 1.0)
                    .append("votes", 1).append("hidden", false).append("slaveDelay", 0L),
            ),
        )

    @Test
    fun `bumps the version and patches only the target member`() {
        val next = buildReconfig(raw, patch(id = 1, priority = 0.0, votes = 0, hidden = true, delay = 3600))
        assertEquals(8, next["version"])
        @Suppress("UNCHECKED_CAST")
        val members = next["members"] as List<Document>
        assertEquals(1.0, members[0]["priority"]) // untouched
        assertEquals(0.0, members[1]["priority"])
        assertEquals(0, members[1]["votes"])
        assertEquals(true, members[1]["hidden"])
        assertEquals(3600L, members[1]["secondaryDelaySecs"])
    }

    @Test
    fun `the legacy slaveDelay field is replaced by secondaryDelaySecs`() {
        val next = buildReconfig(raw, patch(id = 1, delay = 60, priority = 0.0, votes = 1))
        @Suppress("UNCHECKED_CAST")
        val m1 = (next["members"] as List<Document>)[1]
        assertTrue("slaveDelay" !in m1.keys)
        assertEquals(60L, m1["secondaryDelaySecs"])
    }

    @Test
    fun `the set name round-trips untouched`() {
        val next = buildReconfig(raw, patch(id = 0))
        assertEquals("rs0", next["_id"])
    }
}

class ReconfigDiffTest {
    @Test
    fun `lists only what changed`() {
        val diff = reconfigDiff(member(1, priority = 1.0, votes = 1), patch(id = 1, priority = 0.0, votes = 0, hidden = true))
        assertEquals(listOf("priority 1 → 0", "votes 1 → 0", "becomes hidden"), diff)
    }

    @Test
    fun `no changes means an empty diff`() {
        assertTrue(reconfigDiff(member(1), patch(id = 1)).isEmpty())
    }
}

class MajorityShiftTest {
    @Test
    fun `removing a vote from a 3-member set keeps majority at 2`() {
        val members = listOf(member(0), member(1), member(2))
        val (before, after) = majorityShift(members, patch(id = 2, votes = 0, priority = 0.0))
        assertEquals(2, before)
        assertEquals(2, after)
    }

    @Test
    fun `removing a vote from a 5-member set drops majority to 3`() {
        val members = (0..4).map { member(it) }
        val (before, after) = majorityShift(members, patch(id = 4, votes = 0, priority = 0.0))
        assertEquals(3, before)
        assertEquals(3, after) // 4 voters → majority 3
    }

    @Test
    fun `granting a vote can raise the majority`() {
        val members = listOf(member(0), member(1), member(2, votes = 0, priority = 0.0))
        val (before, after) = majorityShift(members, patch(id = 2, votes = 1, priority = 0.0))
        assertEquals(2, before)
        assertEquals(2, after)
    }
}
