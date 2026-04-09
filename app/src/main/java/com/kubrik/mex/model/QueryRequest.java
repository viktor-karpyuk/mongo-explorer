package com.kubrik.mex.model;

public record QueryRequest(
        String dbName,
        String collName,
        String filterJson,
        String projectionJson,
        String sortJson,
        int skip,
        int limit,
        long maxTimeMs
) {}
