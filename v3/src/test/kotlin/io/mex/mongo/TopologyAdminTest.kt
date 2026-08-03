package io.mex.mongo

import org.bson.Document
import kotlin.test.Test
import kotlin.test.assertEquals

class TopologyAdminTest {

    private fun member(id: Int, host: String, votes: Int = 1, priority: Double = 1.0) =
        RsMemberConfig(
            id = id, host = host, priority = priority, votes = votes,
            arbiterOnly = false, hidden = false, buildIndexes = true,
            secondaryDelaySecs = 0, tags = emptyMap(),
        )

    @Test
    fun `buildRemoveMember drops the member and bumps version, others untouched`() {
        val raw = Document("_id", "rs0").append("version", 7).append(
            "members",
            listOf(
                Document("_id", 0).append("host", "a:1").append("priority", 2.0),
                Document("_id", 1).append("host", "b:1"),
                Document("_id", 2).append("host", "c:1"),
            ),
        )
        val next = buildRemoveMember(raw, 1)
        assertEquals(8, next["version"])
        val hosts = (next["members"] as List<*>).filterIsInstance<Document>().map { it.getString("host") }
        assertEquals(listOf("a:1", "c:1"), hosts)
        // Untouched member keeps its extra fields.
        assertEquals(2.0, (next["members"] as List<*>).filterIsInstance<Document>().first()["priority"])
        // The input document is not mutated.
        assertEquals(3, (raw["members"] as List<*>).size)
        assertEquals(7, raw["version"])
    }

    @Test
    fun `removalMajorityShift reports before and after majorities`() {
        val members = listOf(member(0, "a:1"), member(1, "b:1"), member(2, "c:1"))
        assertEquals(2 to 2, removalMajorityShift(members, 2))
        val five = members + listOf(member(3, "d:1"), member(4, "e:1"))
        assertEquals(3 to 3, removalMajorityShift(five, 4))
        // Removing a non-voter never moves the majority.
        val withNonVoter = members + member(3, "d:1", votes = 0)
        assertEquals(2 to 2, removalMajorityShift(withNonVoter, 3))
    }

    @Test
    fun `removeShard result parses started, ongoing and completed shapes`() {
        val started = parseRemoveShardResult(
            Document("state", "started").append("shard", "shard1"),
        )
        assertEquals("started", started.state)
        assertEquals(null, started.remainingChunks)

        val ongoing = parseRemoveShardResult(
            Document("state", "ongoing")
                .append("remaining", Document("chunks", 42L).append("dbs", 1))
                .append("dbsToMove", listOf("orders", "users")),
        )
        assertEquals("ongoing", ongoing.state)
        assertEquals(42L, ongoing.remainingChunks)
        assertEquals(1L, ongoing.remainingDbs)
        assertEquals(listOf("orders", "users"), ongoing.dbsToMove)

        val completed = parseRemoveShardResult(Document("state", "completed"))
        assertEquals("completed", completed.state)
        assertEquals(emptyList(), completed.dbsToMove)
    }
}
