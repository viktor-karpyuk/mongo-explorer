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

class PlanTest {
    @Test
    fun `standalone plan is one mongod facing the host`() {
        val p = plan(lab(LabTopology.Standalone))
        assertEquals(listOf("rs-n1"), p.mongods.map { it.name })
        assertEquals(listOf("rs-n1"), p.clientServices)
        assertTrue(p.replicaSets.isEmpty())
        assertEquals("mex-lab-abcdefgh", p.project)
    }

    @Test
    fun `replica set plan exposes every member to the host`() {
        val p = plan(lab(LabTopology.ReplicaSet(5)))
        assertEquals(5, p.mongods.size)
        assertEquals(p.mongods.map { it.name }, p.clientServices)
        assertEquals(listOf(RsSpec("rs0", (1..5).map { "rs-n$it" }, configSvr = false)), p.replicaSets)
    }

    @Test
    fun `sharded plan exposes only mongos to the host`() {
        val p = plan(lab(LabTopology.Sharded(shards = 2, membersPerShard = 3, mongos = 2, configServers = 3)))
        assertEquals(listOf("mongos-1", "mongos-2"), p.clientServices)
        assertEquals(9, p.mongods.size) // 3 cfg + 2x3 shard members
        assertEquals(3, p.replicaSets.size) // cfg + sh1 + sh2
        assertTrue(p.replicaSets.first { it.name == "cfg" }.configSvr)
        assertEquals(listOf("sh2-n1", "sh2-n2", "sh2-n3"), p.replicaSets.first { it.name == "sh2" }.members)
    }
}

class RenderComposeTest {
    @Test
    fun `replica set with auth stages the keyfile and binds loopback only`() {
        val l = lab(LabTopology.ReplicaSet(3))
        val ports = mapOf("rs-n1" to 61101, "rs-n2" to 61102, "rs-n3" to 61103)
        val yaml = renderCompose(l, ports)

        assertTrue("name: mex-lab-abcdefgh" in yaml)
        for ((svc, port) in ports) {
            assertTrue("  $svc:" in yaml, svc)
            assertTrue("ports: [\"127.0.0.1:$port:27017\"]" in yaml, svc)
        }
        assertFalse("0.0.0.0" in yaml)
        // Keyfile: staged via entrypoint wrapper, mounted read-only, flag present.
        assertTrue("chmod 400 /run/mex/keyfile" in yaml)
        assertTrue("chown mongodb:mongodb" in yaml)
        assertTrue("exec docker-entrypoint.sh mongod" in yaml)
        assertTrue("./keyfile:/keyfile-src/keyfile:ro" in yaml)
        assertTrue("--keyFile /run/mex/keyfile" in yaml)
        assertTrue("--replSet rs0" in yaml)
        assertTrue("--wiredTigerCacheSizeGB 0.25" in yaml)
        // Every service labelled, every mongod has a volume + healthcheck.
        assertEquals(3, Regex("mex\\.lab\\.id=01JABCDEF012345678ABCDEFGH").findAll(yaml).count())
        assertEquals(3, Regex("-data:/data/db").findAll(yaml).count())
        assertEquals(3, Regex("healthcheck:").findAll(yaml).count())
    }

    @Test
    fun `auth-off replica set has no keyfile and no auth flag`() {
        val yaml = renderCompose(lab(LabTopology.ReplicaSet(3), auth = false), mapOf("rs-n1" to 1, "rs-n2" to 2, "rs-n3" to 3))
        assertFalse("keyfile" in yaml.lowercase())
        assertFalse("--auth" in yaml)
        assertTrue("command: mongod --replSet rs0" in yaml)
    }

    @Test
    fun `standalone with auth uses --auth without a keyfile`() {
        val yaml = renderCompose(lab(LabTopology.Standalone), mapOf("rs-n1" to 61000))
        assertTrue("--auth" in yaml)
        assertFalse("keyFile" in yaml)
        assertTrue("command: mongod --port 27017" in yaml)
    }

    @Test
    fun `sharded lab exposes only mongos ports and wires configdb`() {
        val l = lab(LabTopology.Sharded(shards = 2, membersPerShard = 3, mongos = 2, configServers = 3))
        val ports = mapOf("mongos-1" to 61201, "mongos-2" to 61202)
        val yaml = renderCompose(l, ports)

        assertEquals(2, Regex("ports: \\[").findAll(yaml).count())
        assertTrue("--configdb cfg/cfg-n1:27017,cfg-n2:27017,cfg-n3:27017" in yaml)
        assertTrue("--configsvr" in yaml)
        assertTrue("--shardsvr" in yaml)
        assertTrue("--replSet sh2" in yaml)
        // mongos is stateless: no data volume; keyfile is its only mount.
        val mongosBlock = yaml.substringAfter("  mongos-1:").substringBefore("  mongos-2:")
        assertFalse("-data:/data/db" in mongosBlock)
        assertTrue("./keyfile:/keyfile-src/keyfile:ro" in mongosBlock)
        // 9 mongod volumes declared with labels.
        assertEquals(9, Regex("\\n  [a-z0-9-]+-data:").findAll(yaml).count())
    }

    @Test
    fun `rendering is deterministic`() {
        val l = lab(LabTopology.Sharded(shards = 3, membersPerShard = 3, mongos = 2, configServers = 3))
        val ports = mapOf("mongos-1" to 61201, "mongos-2" to 61202)
        assertEquals(renderCompose(l, ports), renderCompose(l, ports))
    }
}

class KeyfileTest {
    @Test
    fun `keyfile is base64 lines within mongod limits`() {
        val k = generateKeyfile()
        assertTrue(k.endsWith("\n"))
        val lines = k.trim().lines()
        assertTrue(lines.all { it.matches(Regex("[A-Za-z0-9+/=]+")) })
        val chars = lines.sumOf { it.length }
        assertTrue(chars in 6..1024, "content chars $chars")
    }

    @Test
    fun `keyfiles are unique per generation`() {
        assertTrue(generateKeyfile() != generateKeyfile())
    }
}

class AllocatePortsTest {
    @Test
    fun `allocates a distinct free port per service`() {
        val services = (1..7).map { "svc-$it" }
        val map = allocatePorts(services)
        assertEquals(services.toSet(), map.keys)
        assertEquals(7, map.values.toSet().size)
        assertTrue(map.values.all { it in 1024..65535 })
    }
}
