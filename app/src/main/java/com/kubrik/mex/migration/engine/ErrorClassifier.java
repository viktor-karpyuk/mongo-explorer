package com.kubrik.mex.migration.engine;

import com.kubrik.mex.migration.spec.ConflictMode;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;

/** Classifies driver-raised throwables so the retry policy and error counter can react
 *  correctly. See docs/mvp-technical-spec.md §11.1. */
public final class ErrorClassifier {

    private ErrorClassifier() {}

    public enum ErrorKind { FATAL, TRANSIENT, PER_DOCUMENT }

    public static ErrorKind classify(Throwable t, ConflictMode mode) {
        if (t instanceof MongoSecurityException) return ErrorKind.FATAL;
        if (t instanceof MongoCommandException cmd && cmd.getErrorCode() == 13) return ErrorKind.FATAL; // unauthorized
        if (t instanceof MongoNotPrimaryException) return ErrorKind.TRANSIENT;
        if (t instanceof MongoSocketException) return ErrorKind.TRANSIENT;
        if (t instanceof MongoTimeoutException) return ErrorKind.TRANSIENT;  // incl. server-selection timeouts
        if (t instanceof MongoBulkWriteException bw) {
            if (bw.getWriteConcernError() != null) return ErrorKind.TRANSIENT;   // T-30
            if (mode == ConflictMode.ABORT) return ErrorKind.FATAL;              // T-5: target was supposed to be empty
            return ErrorKind.PER_DOCUMENT;
        }
        if (t instanceof PerDocumentException) return ErrorKind.PER_DOCUMENT;
        return ErrorKind.FATAL;
    }
}
