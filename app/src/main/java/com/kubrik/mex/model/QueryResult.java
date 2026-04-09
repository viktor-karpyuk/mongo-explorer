package com.kubrik.mex.model;

import org.bson.Document;

import java.util.List;

public record QueryResult(
        List<Document> documents,
        long durationMs,
        boolean hasMore,
        String error
) {}
