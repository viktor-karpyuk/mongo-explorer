package com.kubrik.mex.labs.k8s.distro;

import com.kubrik.mex.labs.k8s.model.LabK8sDistro;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class DistroDetectorTest {

    @Test
    void caches_first_detect_result() {
        StubAdapter minikube = new StubAdapter(LabK8sDistro.MINIKUBE, Optional.of("v1.34.0"));
        StubAdapter k3d = new StubAdapter(LabK8sDistro.K3D, Optional.empty());
        DistroDetector d = new DistroDetector(Map.of(
                LabK8sDistro.MINIKUBE, minikube,
                LabK8sDistro.K3D, k3d));

        d.detect(LabK8sDistro.MINIKUBE);
        d.detect(LabK8sDistro.MINIKUBE);
        assertEquals(1, minikube.detectCalls, "second detect must hit the cache");
    }

    @Test
    void is_available_true_when_version_present() {
        DistroDetector d = new DistroDetector(Map.of(
                LabK8sDistro.MINIKUBE, new StubAdapter(LabK8sDistro.MINIKUBE, Optional.of("v1.34.0")),
                LabK8sDistro.K3D, new StubAdapter(LabK8sDistro.K3D, Optional.empty())));
        assertTrue(d.isAvailable(LabK8sDistro.MINIKUBE));
        assertFalse(d.isAvailable(LabK8sDistro.K3D));
    }

    @Test
    void refresh_clears_cache() {
        StubAdapter minikube = new StubAdapter(LabK8sDistro.MINIKUBE, Optional.of("v1.34.0"));
        DistroDetector d = new DistroDetector(Map.of(
                LabK8sDistro.MINIKUBE, minikube,
                LabK8sDistro.K3D, new StubAdapter(LabK8sDistro.K3D, Optional.empty())));
        d.detect(LabK8sDistro.MINIKUBE);
        d.refresh();
        d.detect(LabK8sDistro.MINIKUBE);
        assertEquals(2, minikube.detectCalls);
    }

    @Test
    void adapter_throws_on_unknown_distro_entry() {
        DistroDetector d = new DistroDetector(new EnumMap<>(LabK8sDistro.class));
        assertThrows(IllegalArgumentException.class,
                () -> d.adapter(LabK8sDistro.MINIKUBE));
    }

    /* ============================ fixture ============================ */

    static final class StubAdapter implements DistroAdapter {
        private final LabK8sDistro distro;
        private final Optional<String> version;
        int detectCalls = 0;

        StubAdapter(LabK8sDistro distro, Optional<String> version) {
            this.distro = distro;
            this.version = version;
        }

        @Override public LabK8sDistro distro() { return distro; }
        @Override public Optional<String> detectVersion() {
            detectCalls++;
            return version;
        }
        @Override public CliRunner.CliResult create(String id, int w) throws IOException { throw new IOException("stub"); }
        @Override public CliRunner.CliResult start(String id, int w) throws IOException { throw new IOException("stub"); }
        @Override public CliRunner.CliResult stop(String id, int w) throws IOException { throw new IOException("stub"); }
        @Override public CliRunner.CliResult delete(String id, int w) throws IOException { throw new IOException("stub"); }
        @Override public CliRunner.CliResult status(String id, int w) throws IOException { throw new IOException("stub"); }
        @Override public boolean isRunning(CliRunner.CliResult r) { return false; }
    }
}
