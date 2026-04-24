package com.kubrik.mex.labs.k8s.distro;

import com.kubrik.mex.labs.k8s.model.LabK8sDistro;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class K3dAdapterTest {

    private final K3dAdapter adapter = new K3dAdapter();

    @Test
    void distro_identity() {
        assertEquals(LabK8sDistro.K3D, adapter.distro());
    }

    @Test
    void context_name_prefixes_k3d() {
        assertEquals("k3d-my-lab", adapter.contextFor("my-lab"));
    }

    @Test
    void is_running_on_server_running_count() {
        String up = "[{\"name\":\"my-lab\",\"serversRunning\": 1,\"nodesRunning\": 2}]";
        String upCompact = "[{\"serversRunning\":1}]";
        String down = "[{\"name\":\"my-lab\",\"serversRunning\": 0,\"nodesRunning\": 0}]";
        assertTrue(adapter.isRunning(new CliRunner.CliResult(0, up, "")));
        assertTrue(adapter.isRunning(new CliRunner.CliResult(0, upCompact, "")));
        assertFalse(adapter.isRunning(new CliRunner.CliResult(0, down, "")));
    }

    @Test
    void is_running_false_on_error_exit() {
        assertFalse(adapter.isRunning(new CliRunner.CliResult(1, "", "docker daemon not found")));
    }
}
