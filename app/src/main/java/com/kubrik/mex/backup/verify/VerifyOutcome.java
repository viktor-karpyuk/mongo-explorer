package com.kubrik.mex.backup.verify;

/**
 * v2.5 Q2.5-D — outcome of one {@code CatalogVerifier.verify} invocation.
 * {@code VERIFIED} means every file round-tripped byte-equal + the manifest
 * footer hash matched. Anything else is a failure mode the history pane
 * renders as amber.
 */
public enum VerifyOutcome {
    VERIFIED,
    MANIFEST_MISSING,
    MANIFEST_TAMPERED,
    FILE_MISSING,
    FILE_MISMATCH,
    IO_ERROR
}
