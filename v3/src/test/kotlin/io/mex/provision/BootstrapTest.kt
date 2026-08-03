package io.mex.provision

import io.mex.data.Lab
import io.mex.data.LabStatus
import io.mex.data.LabTopology
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

private fun lab(topology: LabTopology, auth: Boolean = true) = Lab(
    id = "01JABCDEF012345678ABCDEFGH",
    name = "test-lab",
    topology = topology,
    status = LabStatus.provisioning,
    mongoTag = "8.0",
    auth = auth,
    portMap = emptyMap(),
    connectionId = null,
    dir = "/tmp/labs/x",
    appMajor = 3,
    error = null,
    createdAt = 0L,
)

class InitiateScriptTest {
    @Test
    fun `members are listed explicitly with service-name hosts`() {
        val script = initiateScript("rs0", listOf("rs-n1", "rs-n2", "rs-n3"), configSvr = false)
        assertTrue("{_id: 0, host: \"rs-n1:27017\"}" in script)
        assertTrue("{_id: 2, host: \"rs-n3:27017\"}" in script)
        assertTrue("_id: 'rs0'" in script)
        assertFalse("configsvr" in script)
    }

    @Test
    fun `config server replica set carries the configsvr flag`() {
        val script = initiateScript("cfg", listOf("cfg-n1"), configSvr = true)
        assertTrue("configsvr: true" in script)
    }

    @Test
    fun `initiate is guarded and waits for a primary sentinel`() {
        val script = initiateScript("rs0", listOf("rs-n1"), configSvr = false)
        assertTrue("NotYetInitialized" in script)
        assertTrue("print('PRIMARY')" in script)
        assertTrue("print('NO_PRIMARY'); quit(1)" in script)
    }
}

class AddShardScriptTest {
    @Test
    fun `shard seed lists the replica set and every member`() {
        val script = addShardScript("sh2", listOf("sh2-n1", "sh2-n2", "sh2-n3"))
        assertTrue("sh.addShard('sh2/sh2-n1:27017,sh2-n2:27017,sh2-n3:27017')" in script)
        assertTrue("print('ADDED')" in script)
    }

}

class CreateRootScriptTest {
    @Test
    fun `refuses secondaries so the runner can walk members`() {
        val script = createRootScript("s3cret")
        assertTrue("NOT_PRIMARY" in script)
        assertTrue("createUser" in script)
        assertTrue("'root'" in script)
        assertTrue("s3cret" in script)
        assertTrue("print('CREATED')" in script)
    }
}

class LabUriTest {
    private val pw = "Pw123"

    @Test
    fun `standalone uri is single host with authSource`() {
        val uri = labUri(lab(LabTopology.Standalone), mapOf("rs-n1" to 61000), pw)
        assertEquals("mongodb://root:Pw123@127.0.0.1:61000/?authSource=admin", uri)
    }

    @Test
    fun `replica set uri is directConnection to the first member`() {
        val ports = mapOf("rs-n1" to 61101, "rs-n2" to 61102, "rs-n3" to 61103)
        val uri = labUri(lab(LabTopology.ReplicaSet(3)), ports, pw)
        assertEquals("mongodb://root:Pw123@127.0.0.1:61101/?directConnection=true&authSource=admin", uri)
    }

    @Test
    fun `sharded uri lists every mongos and no directConnection`() {
        val l = lab(LabTopology.Sharded(shards = 2, membersPerShard = 3, mongos = 2, configServers = 3))
        val uri = labUri(l, mapOf("mongos-1" to 61201, "mongos-2" to 61202), pw)
        assertEquals("mongodb://root:Pw123@127.0.0.1:61201,127.0.0.1:61202/?authSource=admin", uri)
    }

    @Test
    fun `auth-off uris carry no credentials or authSource`() {
        val uri = labUri(lab(LabTopology.ReplicaSet(3), auth = false), mapOf("rs-n1" to 61101), null)
        assertEquals("mongodb://127.0.0.1:61101/?directConnection=true", uri)
    }
}

class GeneratePasswordTest {
    @Test
    fun `password is 24 alphanumeric chars and unique`() {
        val a = generatePassword()
        val b = generatePassword()
        assertEquals(24, a.length)
        assertTrue(a.matches(Regex("[A-Za-z0-9]+")))
        assertTrue(a != b)
    }
}

class RedactTest {
    @Test
    fun `secrets never survive into log lines`() {
        val pw = "Xk39fQw7"
        val line = "createUser({user: 'root', pwd: '$pw'}) → mongodb://root:$pw@127.0.0.1:1"
        val out = redact(line, listOf(pw))
        assertFalse(pw in out)
        assertEquals(2, Regex("•••").findAll(out).count())
    }

    @Test
    fun `empty secret list is a no-op`() {
        assertEquals("hello", redact("hello", emptyList()))
        assertEquals("hello", redact("hello", listOf("")))
    }
}
