package io.mex.provision

import io.mex.data.LabTopology
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ValidateTest {
    @Test
    fun `standalone is always valid`() {
        assertEquals(emptyList(), validate(LabTopology.Standalone))
    }

    @Test
    fun `replica set allows odd member counts up to 7`() {
        for (m in listOf(1, 3, 5, 7)) {
            assertEquals(emptyList(), validate(LabTopology.ReplicaSet(m)), "members=$m")
        }
    }

    @Test
    fun `replica set rejects even and out-of-range member counts`() {
        for (m in listOf(0, 2, 4, 6, 9, -1)) {
            assertTrue(validate(LabTopology.ReplicaSet(m)).isNotEmpty(), "members=$m")
        }
    }

    @Test
    fun `sharded default preset is valid`() {
        assertEquals(emptyList(), validate(LabTopology.Sharded(shards = 2, membersPerShard = 3)))
    }

    @Test
    fun `sharded bounds are enforced per field`() {
        assertTrue(validate(LabTopology.Sharded(0, 3)).isNotEmpty())
        assertTrue(validate(LabTopology.Sharded(7, 3)).isNotEmpty())
        assertTrue(validate(LabTopology.Sharded(2, 2)).isNotEmpty())
        assertTrue(validate(LabTopology.Sharded(2, 3, mongos = 0)).isNotEmpty())
        assertTrue(validate(LabTopology.Sharded(2, 3, mongos = 4)).isNotEmpty())
        assertTrue(validate(LabTopology.Sharded(2, 3, configServers = 2)).isNotEmpty())
    }

    @Test
    fun `each violation gets its own message`() {
        val violations = validate(LabTopology.Sharded(0, 2, mongos = 9, configServers = 2))
        assertEquals(4, violations.size)
    }
}

class FootprintTest {
    @Test
    fun `standalone is one container one volume`() {
        assertEquals(Footprint(1, 350, 1), footprint(LabTopology.Standalone))
    }

    @Test
    fun `five node replica set`() {
        assertEquals(Footprint(5, 5 * 350, 5), footprint(LabTopology.ReplicaSet(5)))
    }

    @Test
    fun `full sharded starter is 14 containers and mongos gets no volume`() {
        // 3 shards x 3 members + 3 config servers = 12 mongods, + 2 mongos.
        val f = footprint(LabTopology.Sharded(shards = 3, membersPerShard = 3, mongos = 2, configServers = 3))
        assertEquals(14, f.containers)
        assertEquals(12, f.volumes)
        assertEquals(12 * 350 + 2 * 150, f.estMemMiB)
    }
}

class SummaryTest {
    @Test
    fun `summaries are compact one-liners`() {
        assertEquals("standalone", summary(LabTopology.Standalone))
        assertEquals("replica set ×3", summary(LabTopology.ReplicaSet(3)))
        assertEquals(
            "sharded 3×3 · csrs 3 · mongos 2",
            summary(LabTopology.Sharded(3, 3, mongos = 2, configServers = 3)),
        )
    }
}
