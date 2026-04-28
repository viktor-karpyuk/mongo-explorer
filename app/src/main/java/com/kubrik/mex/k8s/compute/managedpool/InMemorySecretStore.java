package com.kubrik.mex.k8s.compute.managedpool;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * v2.8.4 Q2.8.4-A — Test + headless-runtime fallback {@link SecretStore}.
 *
 * <p>Used by unit tests + as the production fallback when the OS
 * keychain CLI isn't available (CI containers, locked-down VMs).
 * The fallback emits a single warning at construction time so users
 * know secrets aren't crossing the JVM boundary.</p>
 */
public final class InMemorySecretStore implements SecretStore {

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(InMemorySecretStore.class);
    private final ConcurrentMap<String, String> map = new ConcurrentHashMap<>();

    public InMemorySecretStore() {
        log.warn("InMemorySecretStore active — cloud credentials live only "
                + "for the lifetime of this JVM. Install the OS keychain "
                + "tooling for production use.");
    }

    @Override public void store(String ref, String secret) { map.put(ref, secret); }
    @Override public Optional<String> read(String ref) {
        return Optional.ofNullable(map.get(ref));
    }
    @Override public void delete(String ref) { map.remove(ref); }
}
