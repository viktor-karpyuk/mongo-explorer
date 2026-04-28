package com.kubrik.mex.migration.spec;

/** One field-level cast in a transform (XFORM-1). Target types per §10.4 of the functional spec. */
public record CastOp(String field, String from, String to) {}
