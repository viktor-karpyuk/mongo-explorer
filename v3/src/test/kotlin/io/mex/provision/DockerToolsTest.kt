package io.mex.provision

import io.mex.data.LabStatus
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ArgBuildersTest {
    @Test
    fun `compose args carry the project directory`() {
        assertEquals(
            listOf("compose", "--project-directory", "/labs/x", "up", "-d"),
            composeArgs("/labs/x", "up", "-d"),
        )
        assertEquals(
            listOf("compose", "--project-directory", "/labs/x", "down", "-v"),
            composeArgs("/labs/x", "down", "-v"),
        )
    }

    @Test
    fun `ps filters on the lab label, optionally scoped to one lab`() {
        assertTrue("label=mex.lab.id" in psArgs())
        assertTrue("label=mex.lab.id=01J" in psArgs("01J"))
        assertTrue("{{json .}}" in psArgs())
    }

    @Test
    fun `exec mongosh passes the script as one argv element`() {
        val script = "print('hi \"there\"')"
        val args = execMongoshArgs("mex-lab-ab-rs-n1-1", script)
        assertEquals(listOf("exec", "mex-lab-ab-rs-n1-1", "mongosh", "--quiet", "--eval", script), args)
    }

    @Test
    fun `compose v2 container naming`() {
        assertEquals("mex-lab-abcdefgh-rs-n1-1", containerName("mex-lab-abcdefgh", "rs-n1"))
    }
}

class DockerVersionTest {
    @Test
    fun `modern and minimum versions pass, older fail`() {
        assertTrue(dockerVersionOk("Docker version 28.1.1, build 4eba377"))
        assertTrue(dockerVersionOk("Docker version 20.10.0"))
        assertFalse(dockerVersionOk("Docker version 19.03.13"))
        assertFalse(dockerVersionOk("garbage"))
    }
}

class ParsePsLineTest {
    @Test
    fun `extracts name, state and lab id from docker ps json`() {
        val line = """{"Names":"mex-lab-ab-rs-n1-1","State":"running","Labels":"a=b,mex.lab.id=01JX,c=d"}"""
        val row = parsePsLine(line)!!
        assertEquals("mex-lab-ab-rs-n1-1", row.name)
        assertEquals("running", row.state)
        assertEquals("01JX", row.labId)
    }

    @Test
    fun `unlabelled containers and garbage are tolerated`() {
        assertNull(parsePsLine("not json"))
        val row = parsePsLine("""{"Names":"other","State":"exited","Labels":""}""")!!
        assertNull(row.labId)
    }
}

class ReconcileStatusTest {
    @Test
    fun `interrupted provisioning becomes failed`() {
        assertEquals(LabStatus.failed, reconcileStatus(LabStatus.provisioning, listOf("running")))
        assertEquals(LabStatus.failed, reconcileStatus(LabStatus.provisioning, emptyList()))
    }

    @Test
    fun `running with no containers is missing`() {
        assertEquals(LabStatus.missing, reconcileStatus(LabStatus.running, emptyList()))
        assertEquals(LabStatus.missing, reconcileStatus(LabStatus.stopped, emptyList()))
    }

    @Test
    fun `running with all containers exited is stopped, and vice versa`() {
        assertEquals(LabStatus.stopped, reconcileStatus(LabStatus.running, listOf("exited", "exited")))
        assertEquals(LabStatus.running, reconcileStatus(LabStatus.stopped, listOf("running", "running")))
    }

    @Test
    fun `truthful rows are untouched`() {
        assertNull(reconcileStatus(LabStatus.running, listOf("running", "running")))
        assertNull(reconcileStatus(LabStatus.stopped, listOf("exited")))
        assertNull(reconcileStatus(LabStatus.failed, emptyList()))
        assertNull(reconcileStatus(LabStatus.missing, emptyList()))
    }

    @Test
    fun `partially running lab counts as running truth`() {
        // One member crashed: still "running" from the row's perspective — not stopped.
        assertNull(reconcileStatus(LabStatus.running, listOf("running", "exited")))
    }

    @Test
    fun `restarting containers are live, not stopped`() {
        assertNull(reconcileStatus(LabStatus.running, listOf("restarting", "restarting")))
        assertEquals(LabStatus.running, reconcileStatus(LabStatus.stopped, listOf("restarting")))
    }
}
