package com.kubrik.mex.k8s.provision;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import java.io.IOException;

/**
 * v2.8.1 Q2.8.1-J — Read a {@link DeploymentSpec} JSON file and
 * hand back the {@link ProvisionModel} to pre-fill the wizard.
 *
 * <p>Schema-version check is strict: a future reader refuses to
 * interpret an unknown schema string rather than silently dropping
 * fields. Users update Mongo Explorer; they don't hand-edit a spec
 * file to cheat the reader.</p>
 *
 * <p>Evidence signature verification is delegated — {@link
 * #verify(byte[], Verifier)} returns true/false; the wizard UI
 * surfaces the result as a badge on the import row.</p>
 */
public final class DeploymentSpecImporter {

    private static final ObjectMapper JSON = new ObjectMapper()
            .registerModule(new Jdk8Module())
            // The records expose derived accessors (isProdAcceptable, coordinates,
            // replicasPerReplset, …) that Jackson picks up as properties on the
            // writer side. We don't want them to fail the reader when a spec
            // written by a newer mex install carries additional derived fields.
            .configure(com.fasterxml.jackson.databind.DeserializationFeature
                    .FAIL_ON_UNKNOWN_PROPERTIES, false);

    private DeploymentSpecImporter() {}

    public static DeploymentSpec parse(byte[] bytes) throws IOException {
        DeploymentSpec spec = JSON.readValue(bytes, DeploymentSpec.class);
        if (!DeploymentSpec.CURRENT_SCHEMA.equals(spec.schemaVersion())) {
            throw new IOException("unsupported schema " + spec.schemaVersion()
                    + "; this install reads " + DeploymentSpec.CURRENT_SCHEMA);
        }
        return spec;
    }

    public static ProvisionModel toModel(DeploymentSpec spec) {
        // The exporter strips the provided password so PROVIDE mode
        // needs user input at re-apply. Everything else carries
        // through verbatim.
        return spec.model();
    }

    /** Recompute unsigned bytes + compare signature. */
    public static boolean verify(byte[] specJson, Verifier verifier) {
        try {
            DeploymentSpec parsed = JSON.readValue(specJson, DeploymentSpec.class);
            if (parsed.evidenceSig().isEmpty()) return false;
            byte[] unsigned = DeploymentSpecExporter.unsignedBytes(parsed);
            return verifier.verify(unsigned, parsed.evidenceSig().get());
        } catch (IOException ioe) {
            return false;
        }
    }

    /** Symmetrical to {@code DeploymentSpecExporter.EvidenceSigner}. */
    public interface Verifier {
        boolean verify(byte[] body, String signature);
    }
}
