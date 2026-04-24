package com.kubrik.mex.labs.k8s.distro;

import com.kubrik.mex.labs.k8s.model.LabK8sDistro;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

/**
 * v2.8.1 Q2.8-N2 — Detects which supported distros are installed.
 *
 * <p>Runs each adapter's version check and caches the result. The UI's
 * "Add Lab K8s cluster" flow greys out distros whose CLI isn't on
 * {@code PATH} so users can't try to create a k3d cluster on a box
 * that only has minikube installed.</p>
 */
public final class DistroDetector {

    private final Map<LabK8sDistro, DistroAdapter> adapters;
    private final Map<LabK8sDistro, Optional<String>> cache =
            new EnumMap<>(LabK8sDistro.class);

    public DistroDetector() {
        Map<LabK8sDistro, DistroAdapter> m = new EnumMap<>(LabK8sDistro.class);
        m.put(LabK8sDistro.MINIKUBE, new MinikubeAdapter());
        m.put(LabK8sDistro.K3D, new K3dAdapter());
        this.adapters = Map.copyOf(m);
    }

    /** Test seam — inject custom adapters. */
    DistroDetector(Map<LabK8sDistro, DistroAdapter> adapters) {
        this.adapters = Map.copyOf(adapters);
    }

    public DistroAdapter adapter(LabK8sDistro distro) {
        DistroAdapter a = adapters.get(distro);
        if (a == null) throw new IllegalArgumentException("no adapter for " + distro);
        return a;
    }

    /**
     * Detect every distro; returns the version string per distro or
     * {@link Optional#empty} if the CLI isn't on PATH. Results are
     * cached — call {@link #refresh} to re-probe.
     */
    public synchronized Map<LabK8sDistro, Optional<String>> detectAll() {
        for (LabK8sDistro d : LabK8sDistro.values()) {
            cache.computeIfAbsent(d, k -> adapters.get(k).detectVersion());
        }
        return Map.copyOf(cache);
    }

    public synchronized Optional<String> detect(LabK8sDistro distro) {
        return cache.computeIfAbsent(distro,
                d -> adapters.get(d).detectVersion());
    }

    public synchronized boolean isAvailable(LabK8sDistro distro) {
        return detect(distro).isPresent();
    }

    public synchronized void refresh() {
        cache.clear();
    }
}
