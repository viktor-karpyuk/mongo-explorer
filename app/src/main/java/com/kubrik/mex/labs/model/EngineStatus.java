package com.kubrik.mex.labs.model;

/**
 * v2.8.4 LAB-DOCKER-3 — Docker engine availability signal for the
 * Labs tab empty state. Each value maps to a distinct UI surface:
 * install link, start-Docker hint, upgrade hint, or proceed.
 */
public enum EngineStatus {
    /** `docker` on PATH + daemon responding + version >= minimum. */
    READY,
    /** `docker` binary not found on PATH. */
    CLI_MISSING,
    /** Binary present but `docker info` fails — daemon not running. */
    DAEMON_DOWN,
    /** CLI version below the minimum supported (LAB-DOCKER-2). */
    VERSION_LOW,
    /** CLI present but we couldn't classify the failure. */
    UNKNOWN
}
