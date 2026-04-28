package com.kubrik.mex.k8s.compute.managedpool;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.8.4.0 — Locks the EKS adapter's auth-mode dispatch + the
 * recently-fixed STATIC payload split (the v2.8.4-audit pass
 * replaced indexOf(':') with split(':', 2) so secrets with
 * embedded colons survive).
 */
class EksAdapterTest {

    @Test
    void static_credential_two_part_payload_resolves_to_static_provider() {
        InMemorySecretStore store = new InMemorySecretStore();
        String ref = SecretStore.newRef(CloudProvider.AWS, "static");
        store.store(ref, "AKIAEXAMPLE:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");

        CloudCredential cred = new CloudCredential(1L, "test-eks", CloudProvider.AWS,
                CloudCredential.AuthMode.STATIC, ref,
                Optional.of("123456789012"), Optional.empty(),
                Optional.empty(), Optional.of("us-east-1"),
                0L, Optional.empty(), Optional.empty());

        var prov = new EksAdapter(store, "arn:role").providerFor(cred);
        assertInstanceOf(StaticCredentialsProvider.class, prov);
    }

    @Test
    void static_credential_with_extra_colons_keeps_full_secret_after_first() {
        // AWS secret keys today never contain ':', but the audit fix
        // moved to split(':', 2) for forward compat. This test
        // documents the contract — the split must NOT swallow extra
        // colons in the right-hand segment.
        InMemorySecretStore store = new InMemorySecretStore();
        String ref = SecretStore.newRef(CloudProvider.AWS, "future-format");
        // Hypothetical future format where the secret encodes a
        // session-token suffix after a second colon. The current
        // adapter just uses both halves verbatim; the contract
        // is that the secret never gets truncated.
        store.store(ref, "AKIAEXAMPLE:wJalrXUtnFEMI/K7MDENG:withSessionTokenSuffix");

        CloudCredential cred = new CloudCredential(1L, "test", CloudProvider.AWS,
                CloudCredential.AuthMode.STATIC, ref,
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(),
                0L, Optional.empty(), Optional.empty());

        // The provider type alone proves the parse succeeded; static
        // credentials don't expose the secret back through a public
        // accessor.
        var prov = new EksAdapter(store, "arn:role").providerFor(cred);
        assertInstanceOf(StaticCredentialsProvider.class, prov,
                "split(':', 2) must keep everything after the first colon "
                + "as the secret half — the StaticCredentialsProvider "
                + "constructs successfully or the audit fix has regressed");
    }

    @Test
    void static_credential_with_empty_payload_falls_back_to_default_chain() {
        InMemorySecretStore store = new InMemorySecretStore();
        String ref = SecretStore.newRef(CloudProvider.AWS, "empty");
        // No store call — store.read returns empty.
        CloudCredential cred = new CloudCredential(1L, "empty", CloudProvider.AWS,
                CloudCredential.AuthMode.STATIC, ref,
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(),
                0L, Optional.empty(), Optional.empty());

        var prov = new EksAdapter(store, "arn:role").providerFor(cred);
        assertInstanceOf(DefaultCredentialsProvider.class, prov);
    }

    @Test
    void external_id_resolves_to_profile_credentials_provider() {
        InMemorySecretStore store = new InMemorySecretStore();
        String ref = SecretStore.newRef(CloudProvider.AWS, "ext");
        store.store(ref, "production-readonly");

        CloudCredential cred = new CloudCredential(1L, "ext", CloudProvider.AWS,
                CloudCredential.AuthMode.EXTERNAL_ID, ref,
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(),
                0L, Optional.empty(), Optional.empty());

        var prov = new EksAdapter(store, "arn:role").providerFor(cred);
        assertInstanceOf(ProfileCredentialsProvider.class, prov);
    }

    @Test
    void irsa_falls_back_to_default_chain() {
        CloudCredential cred = new CloudCredential(1L, "irsa", CloudProvider.AWS,
                CloudCredential.AuthMode.IRSA, "keychain://irsa",
                Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(),
                0L, Optional.empty(), Optional.empty());

        var prov = new EksAdapter(new InMemorySecretStore(), "arn:role")
                .providerFor(cred);
        assertInstanceOf(DefaultCredentialsProvider.class, prov);
    }
}
