package com.kubrik.mex.backup.sink;

/**
 * v2.5 Q2.5-H — thrown by cloud {@link StorageTarget} stubs in v2.5.0.
 *
 * <p>Cloud impls (S3 / GCS / Azure / SFTP) need heavyweight SDK dependencies
 * (AWS SDK, google-cloud-storage, azure-storage-blob, JSch) that would
 * inflate the app bundle by tens of MB each. v2.5.0 ships only the
 * {@link LocalFsTarget}; cloud targets land with v2.5.1 once the SDK
 * selection + credential UI land.</p>
 */
public final class CloudSinkUnavailableException extends UnsupportedOperationException {
    public CloudSinkUnavailableException(String kind) {
        super(kind + " sink is scaffolded but unimplemented in v2.5.0 — "
                + "lands with v2.5.1 (AWS / GCS / Azure / SFTP SDK integration).");
    }
}
