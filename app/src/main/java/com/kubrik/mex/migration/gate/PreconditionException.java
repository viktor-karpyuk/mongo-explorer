package com.kubrik.mex.migration.gate;

/** Thrown by {@link PreconditionGate#check} when a job must not start. */
public final class PreconditionException extends RuntimeException {
    public PreconditionException(String message) { super(message); }
}
