package com.kubrik.mex.monitoring.recording;

import com.kubrik.mex.monitoring.recording.store.RecordingSampleDao;

import java.sql.SQLException;
import java.util.function.LongSupplier;

/**
 * Enforces the global recording storage cap (REC-STORE-10) at recording-start time.
 *
 * <p>Accounting is a cheap approximation — see technical-spec §6. Estimated bytes =
 * {@code SUM(LENGTH(labels_json)) + 28 * COUNT(*)} across {@code recording_samples};
 * the approximation is good to ± 20 % which is fine because the cap is a *soft*
 * block (refuses new starts, never auto-deletes). Callers surface the
 * {@link RecordingException.StorageCapExceeded} as UI message {@code ME-23-03}.
 */
public final class RecordingStorageCap {

    private final RecordingSampleDao sampleDao;
    private final LongSupplier capBytes;

    public RecordingStorageCap(RecordingSampleDao sampleDao, LongSupplier capBytes) {
        this.sampleDao = sampleDao;
        this.capBytes = capBytes;
    }

    public long capBytes() { return capBytes.getAsLong(); }

    public long usedBytes() throws SQLException { return sampleDao.estimateBytes(); }

    /**
     * Guard before recording-start — throws when the existing recording-sample
     * footprint already meets or exceeds the configured cap.
     */
    public void checkCanStart() throws SQLException {
        long cap = capBytes();
        long used = usedBytes();
        if (used >= cap) {
            throw new RecordingException.StorageCapExceeded(used, cap).asRuntime();
        }
    }
}
