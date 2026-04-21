package com.kubrik.mex.backup.manifest;

import com.kubrik.mex.backup.spec.ArchiveSpec;
import com.kubrik.mex.backup.spec.Scope;
import com.kubrik.mex.cluster.util.CanonicalJson;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

/**
 * v2.5 BKP-RUN-5 — canonical manifest emitted at the end of every backup run.
 *
 * <p>{@link #toCanonicalJson()} is byte-stable (sorted keys, deterministic
 * ordering), so the SHA-256 of the serialised output ({@link #footerSha256})
 * is the authoritative identity of a run's tree. The verifier (Q2.5-D)
 * recomputes the footer hash against the on-disk bytes and flags any
 * mismatch.</p>
 *
 * <p>Reuses v2.4's {@link CanonicalJson} writer so backup + topology share
 * a single JSON canonicaliser.</p>
 */
public record BackupManifest(
        String mexVersion,
        int manifestVersion,
        Instant createdAt,
        long policyId,
        String connectionId,
        Scope scope,
        ArchiveSpec archive,
        List<FileRecord> files,
        OplogSlice oplog
) {
    public static final int MANIFEST_VERSION = 1;

    public BackupManifest {
        if (mexVersion == null || mexVersion.isBlank())
            throw new IllegalArgumentException("mexVersion");
        if (createdAt == null) throw new IllegalArgumentException("createdAt");
        if (connectionId == null || connectionId.isBlank())
            throw new IllegalArgumentException("connectionId");
        if (scope == null) throw new IllegalArgumentException("scope");
        if (archive == null) throw new IllegalArgumentException("archive");
        files = files == null ? List.of() : List.copyOf(files);
    }

    public long totalBytes() {
        return files.stream().mapToLong(FileRecord::bytes).sum();
    }

    /**
     * Canonical JSON over every field except {@code footerSha256} (that hash
     * is computed <em>of</em> this output). Keys are sorted and the
     * {@code files} array is stable-sorted by path.
     */
    public String toCanonicalJson() {
        CanonicalJson.ObjectWriter obj = CanonicalJson.object();
        obj.putString("mexVersion", mexVersion);
        obj.putLong("manifestVersion", manifestVersion);
        obj.putString("createdAt", createdAt.toString());
        obj.putLong("policyId", policyId);
        obj.putString("connectionId", connectionId);

        CanonicalJson.ObjectWriter scopeObj = obj.putObject("scope");
        writeScope(scopeObj, scope);
        scopeObj.close();

        CanonicalJson.ObjectWriter archObj = obj.putObject("archive");
        archObj.putBool("gzip", archive.gzip());
        archObj.putLong("level", archive.level());
        archObj.putString("outputDirTemplate", archive.outputDirTemplate());
        archObj.close();

        CanonicalJson.ArrayWriter arr = obj.putArray("files");
        List<FileRecord> sorted = new java.util.ArrayList<>(files);
        sorted.sort(Comparator.comparing(FileRecord::path));
        for (FileRecord f : sorted) {
            CanonicalJson.ObjectWriter fo = arr.addObject();
            fo.putString("path", f.path());
            fo.putLong("bytes", f.bytes());
            fo.putString("sha256", f.sha256());
            fo.close();
        }
        arr.close();

        if (oplog != null) {
            CanonicalJson.ObjectWriter oo = obj.putObject("oplog");
            oo.putLong("firstTs", oplog.firstTs());
            oo.putLong("lastTs",  oplog.lastTs());
            oo.putString("path",  oplog.path());
            oo.putString("sha256", oplog.sha256());
            oo.close();
        }

        return obj.toJson();
    }

    /** SHA-256 over {@link #toCanonicalJson()}; the "footer hash" recorded
     *  into {@code backup_catalog.manifest_sha256}. */
    public String footerSha256() {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(toCanonicalJson().getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static void writeScope(CanonicalJson.ObjectWriter obj, Scope scope) {
        switch (scope) {
            case Scope.WholeCluster w -> obj.putString("kind", "whole_cluster");
            case Scope.Databases d -> {
                obj.putString("kind", "databases");
                CanonicalJson.ArrayWriter a = obj.putArray("names");
                for (String n : d.names()) a.addString(n);
                a.close();
            }
            case Scope.Namespaces ns -> {
                obj.putString("kind", "namespaces");
                CanonicalJson.ArrayWriter a = obj.putArray("namespaces");
                for (String n : ns.namespaces()) a.addString(n);
                a.close();
            }
        }
    }
}
