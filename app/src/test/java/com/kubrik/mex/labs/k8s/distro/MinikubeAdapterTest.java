package com.kubrik.mex.labs.k8s.distro;

import com.kubrik.mex.labs.k8s.model.LabK8sDistro;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MinikubeAdapterTest {

    private final MinikubeAdapter adapter = new MinikubeAdapter();

    @Test
    void distro_identity() {
        assertEquals(LabK8sDistro.MINIKUBE, adapter.distro());
    }

    @Test
    void context_name_is_profile_name_verbatim() {
        assertEquals("my-lab", adapter.contextFor("my-lab"));
    }

    @Test
    void is_running_parses_spaced_and_unspaced_json() {
        String pretty = "{\n  \"APIServer\": \"Running\"\n}";
        String compact = "{\"APIServer\":\"Running\"}";
        String down = "{\"APIServer\":\"Stopped\"}";
        assertTrue(adapter.isRunning(new CliRunner.CliResult(0, pretty, "")));
        assertTrue(adapter.isRunning(new CliRunner.CliResult(0, compact, "")));
        assertFalse(adapter.isRunning(new CliRunner.CliResult(0, down, "")));
    }

    @Test
    void is_running_false_when_exit_non_zero() {
        assertFalse(adapter.isRunning(new CliRunner.CliResult(
                1, "{\"APIServer\":\"Running\"}", "profile not found")));
    }

    @Test
    void kubeconfig_default_points_at_home() {
        assertTrue(adapter.kubeconfigPath().toString().endsWith(".kube/config"));
    }
}
