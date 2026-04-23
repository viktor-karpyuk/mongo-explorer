package com.kubrik.mex.k8s.provision;

/**
 * v2.8.1 Q2.8.1-D1 — Which Kubernetes operator renders the CR.
 *
 * <p>Enterprise MongoDB Kubernetes Operator is deliberately absent —
 * the commercial Ops Manager / Cloud Manager auth story is a
 * separate design (milestone NG-2.8-3, deferred past v2.8.0 GA).</p>
 */
public enum OperatorId {
    /** MongoDB Community Operator. */
    MCO,
    /** Percona Server for MongoDB Operator. */
    PSMDB;
}
