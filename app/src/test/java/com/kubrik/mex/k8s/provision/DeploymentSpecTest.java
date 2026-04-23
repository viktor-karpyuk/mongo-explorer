package com.kubrik.mex.k8s.provision;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class DeploymentSpecTest {

    @Test
    void round_trip_preserves_every_model_field() throws IOException {
        ProvisionModel m = new ProfileEnforcer().switchProfile(
                ProvisionModel.defaults(42L, "mongo", "prod-rs")
                        .withOperator(OperatorId.PSMDB)
                        .withTopology(Topology.RS5),
                Profile.PROD).model()
                .withTls(TlsSpec.certManager("mongo-issuer"))
                .withStorage(new StorageSpec(Optional.of("gp3"), 250, 25));

        DeploymentSpec exported = DeploymentSpecExporter.build(m, "source/prod-rs");
        byte[] json = DeploymentSpecExporter.toJson(exported, DeploymentSpecExporter.NULL_SIGNER);

        DeploymentSpec imported = DeploymentSpecImporter.parse(json);
        ProvisionModel back = DeploymentSpecImporter.toModel(imported);

        assertEquals(m.profile(), back.profile());
        assertEquals(m.operator(), back.operator());
        assertEquals(m.topology(), back.topology());
        assertEquals(m.tls().mode(), back.tls().mode());
        assertEquals(m.tls().certManagerIssuer(), back.tls().certManagerIssuer());
        assertEquals(m.storage().dataSizeGib(), back.storage().dataSizeGib());
        assertEquals(m.storage().storageClass(), back.storage().storageClass());
        assertEquals(m.backup().mode(), back.backup().mode());
    }

    @Test
    void export_strips_provided_password() throws IOException {
        ProvisionModel m = ProvisionModel.defaults(1L, "ns", "d")
                .withAuth(new AuthSpec("root",
                        AuthSpec.PasswordMode.PROVIDE,
                        Optional.of("leak-me-not")));
        byte[] json = DeploymentSpecExporter.toJson(
                DeploymentSpecExporter.build(m, null),
                DeploymentSpecExporter.NULL_SIGNER);
        String text = new String(json, StandardCharsets.UTF_8);
        assertFalse(text.contains("leak-me-not"),
                "export must never write provided passwords to the spec file");
    }

    @Test
    void import_of_wrong_schema_version_throws() {
        String bad = "{\"schemaVersion\":\"mex.k8s/v3.0\",\"exportedAt\":\"now\","
                + "\"sourceDeployment\":null,\"evidenceSig\":null,\"model\":{}}";
        assertThrows(IOException.class, () ->
                DeploymentSpecImporter.parse(bad.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void verify_fails_when_signature_missing() throws IOException {
        byte[] json = DeploymentSpecExporter.toJson(
                DeploymentSpecExporter.build(
                        ProvisionModel.defaults(1L, "ns", "d"), null),
                null);
        assertFalse(DeploymentSpecImporter.verify(json, (b, s) -> true));
    }

    @Test
    void verify_passes_when_signer_returns_stable_signature() throws IOException {
        DeploymentSpecExporter.EvidenceSigner signer = body -> "fixed-signature";
        DeploymentSpecImporter.Verifier verifier =
                (body, sig) -> "fixed-signature".equals(sig);
        byte[] json = DeploymentSpecExporter.toJson(
                DeploymentSpecExporter.build(
                        ProvisionModel.defaults(1L, "ns", "d"), null),
                signer);
        assertTrue(DeploymentSpecImporter.verify(json, verifier));
    }

    @Test
    void unsigned_bytes_are_independent_of_signature_field() {
        DeploymentSpec a = DeploymentSpecExporter.build(
                ProvisionModel.defaults(1L, "ns", "d"), null);
        DeploymentSpec b = new DeploymentSpec(a.schemaVersion(), a.exportedAt(),
                a.sourceDeployment(), Optional.of("sig-a"), a.model());
        DeploymentSpec c = new DeploymentSpec(a.schemaVersion(), a.exportedAt(),
                a.sourceDeployment(), Optional.of("sig-b"), a.model());
        assertArrayEquals(DeploymentSpecExporter.unsignedBytes(b),
                DeploymentSpecExporter.unsignedBytes(c),
                "unsigned bytes must be stable across different signature values "
                + "so verifiers can recompute + compare");
    }
}
