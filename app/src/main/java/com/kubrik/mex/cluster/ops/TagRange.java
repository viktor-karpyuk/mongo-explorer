package com.kubrik.mex.cluster.ops;

import org.bson.Document;

/**
 * v2.4 SHARD-14..16 — one entry in {@code config.tags}. The UI renders the
 * namespace, min / max bounds, and target zone as a row; {@code minJson} /
 * {@code maxJson} preserve the BSON payload as relaxed JSON so users can
 * copy / paste them into a removal dialog without re-typing.
 */
public record TagRange(String ns, String tag, String minJson, String maxJson,
                       Document minBson, Document maxBson) {}
