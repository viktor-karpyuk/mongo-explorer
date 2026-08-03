package io.mex.mongo

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ClusterParseTest {

    @Test
    fun `replica set host string splits into set name and hosts`() {
        val (rs, hosts) = parseHostString("shard0/mongo-a:27018,mongo-b:27018,mongo-c:27018")
        assertEquals("shard0", rs)
        assertEquals(listOf("mongo-a:27018", "mongo-b:27018", "mongo-c:27018"), hosts)
    }

    @Test
    fun `host string without set name keeps hosts and null name`() {
        val (rs, hosts) = parseHostString("localhost:27017")
        assertNull(rs)
        assertEquals(listOf("localhost:27017"), hosts)
    }

    @Test
    fun `blank and whitespace-polluted strings are handled`() {
        assertEquals(null to emptyList(), parseHostString(""))
        val (rs, hosts) = parseHostString("cfg/ a:1 , b:2 ,")
        assertEquals("cfg", rs)
        assertEquals(listOf("a:1", "b:2"), hosts)
    }

    @Test
    fun `chunk group rows map to counts and skip malformed rows`() {
        val rows = listOf(
            org.bson.Document("_id", "shard0").append("n", 120),
            org.bson.Document("_id", "shard1").append("n", 80L),
            org.bson.Document("_id", null).append("n", 5),
            org.bson.Document("_id", "shard2"),
        )
        assertEquals(mapOf("shard0" to 120L, "shard1" to 80L), chunkCountsByShard(rows))
    }

    @Test
    fun `router is active only with a recent ping`() {
        assertTrue(RouterInfo("r1:27017", 4, "8.0.4").active)
        assertFalse(RouterInfo("r1:27017", 301, null).active)
        assertFalse(RouterInfo("r1:27017", null, null).active)
    }
}
