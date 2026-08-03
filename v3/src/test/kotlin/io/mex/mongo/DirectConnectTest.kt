package io.mex.mongo

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DirectConnectTest {

    @Test
    fun `replica set uri targets one host and drops replicaSet`() {
        assertEquals(
            "mongodb://u:p@h2:27017/?directConnection=true",
            directNodeUri("mongodb://u:p@h1:27017,h2:27017/?replicaSet=rs0", "h2:27017"),
        )
    }

    @Test
    fun `path auth db and other options round-trip`() {
        assertEquals(
            "mongodb://u:p@node:27018/admin?tls=true&directConnection=true",
            directNodeUri("mongodb://u:p@a:1,b:2/admin?replicaSet=rs&tls=true", "node:27018"),
        )
    }

    @Test
    fun `srv uri gains tls and authSource defaults`() {
        assertEquals(
            "mongodb://u:p@shard-00-01.x.mongodb.net:27017/?w=majority&tls=true&authSource=admin&directConnection=true",
            directNodeUri("mongodb+srv://u:p@cluster0.x.mongodb.net/?w=majority", "shard-00-01.x.mongodb.net:27017"),
        )
    }

    @Test
    fun `srv defaults are not forced when already pinned`() {
        assertEquals(
            "mongodb://u:p@n:27017/?tls=false&authSource=test&directConnection=true",
            directNodeUri("mongodb+srv://u:p@c.x.net/?tls=false&authSource=test", "n:27017"),
        )
    }

    @Test
    fun `credential-free uri gets no authSource and keeps empty path`() {
        assertEquals(
            "mongodb://localhost:27018/?directConnection=true",
            directNodeUri("mongodb://localhost:27017", "localhost:27018"),
        )
    }

    @Test
    fun `existing directConnection and srv-only options are not duplicated`() {
        assertEquals(
            "mongodb://n:1/?directConnection=true",
            directNodeUri("mongodb://a:1/?directConnection=true&srvMaxHosts=2", "n:1"),
        )
    }

    @Test
    fun `password with @ inside userinfo survives`() {
        assertEquals(
            "mongodb://u:p%40ss@n:1/?directConnection=true",
            directNodeUri("mongodb://u:p%40ss@a:1,b:2/?replicaSet=r", "n:1"),
        )
    }

    @Test
    fun `loadBalanced is dropped — it conflicts with directConnection`() {
        assertEquals(
            "mongodb://u:p@n:1/?tls=true&authSource=admin&directConnection=true",
            directNodeUri("mongodb+srv://u:p@c.x.net/?loadBalanced=true", "n:1"),
        )
    }

    @Test
    fun `garbage is rejected`() {
        assertFailsWith<IllegalArgumentException> { directNodeUri("postgres://x", "h:1") }
    }
}
