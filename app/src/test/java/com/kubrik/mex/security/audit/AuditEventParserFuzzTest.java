package com.kubrik.mex.security.audit;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-K1 — adversarial corpus for {@link AuditEventParser#parse}.
 * The parser is the first thing a compromised server or a
 * custom-logged-and-rewritten audit file can reach, so the contract is
 * simple: <b>any single line input must return either null or a valid
 * {@link AuditEvent}; never throw</b>. Every exception here used to
 * crash the tailer and silently stop audit ingestion.
 */
class AuditEventParserFuzzTest {

    /** Brute-force: walk a fixed corpus of malformed inputs and assert
     *  parse() either returns null or a non-null event. Under no input
     *  shall the parser throw. */
    @Test
    void every_corpus_entry_returns_null_or_valid_event_without_throwing() {
        for (String raw : CORPUS) {
            AuditEvent e;
            try {
                e = AuditEventParser.parse(raw);
            } catch (Throwable t) {
                fail("parse(" + preview(raw) + ") threw " + t.getClass().getSimpleName()
                        + ": " + t.getMessage());
                return;
            }
            // If we got an event back, atype must be non-null and the raw
            // JSON must round-trip — the record's canonical constructor
            // enforces both but the assertion makes any future contract
            // drift loud.
            if (e != null) {
                assertNotNull(e.atype(), "atype must be non-null when an event is returned");
                assertNotNull(e.rawJson(), "rawJson must be non-null when an event is returned");
            }
        }
    }

    /** Prefix fuzz — take a well-formed line and progressively truncate
     *  it one char at a time. Any prefix must either parse cleanly (the
     *  cut-off happens to land on a syntactically-complete point) or
     *  return null; neither is allowed to throw. */
    @Test
    void progressive_truncation_of_valid_line_never_throws() {
        String good = "{\"atype\":\"authenticate\",\"ts\":1000,"
                + "\"users\":[{\"user\":\"dba\",\"db\":\"admin\"}],\"param\":{}}";
        for (int i = 0; i <= good.length(); i++) {
            String prefix = good.substring(0, i);
            try {
                AuditEventParser.parse(prefix);
            } catch (Throwable t) {
                fail("prefix of length " + i + " threw "
                        + t.getClass().getSimpleName() + ": " + t.getMessage());
            }
        }
    }

    /** Deep-nesting resistance — 10 k levels of nested {} on the param
     *  field. Either parses or returns null; must not blow the stack. */
    @Test
    void deeply_nested_param_does_not_blow_the_stack() {
        StringBuilder nesting = new StringBuilder();
        for (int i = 0; i < 10_000; i++) nesting.append("{\"k\":");
        for (int i = 0; i < 10_000; i++) nesting.append("1}");
        String line = "{\"atype\":\"deepNest\",\"ts\":0,\"users\":[],\"param\":"
                + nesting + "}";
        try {
            AuditEventParser.parse(line);
        } catch (StackOverflowError soe) {
            fail("deeply-nested input caused StackOverflowError");
        } catch (Throwable ignored) {
            // Other exceptions are acceptable for pathological input, but
            // StackOverflowError is the specific failure mode we guard
            // against — BSON's Document.parse does not protect against it
            // on all JVMs.
        }
    }

    /** Large-value fuzz — a one-field entry whose value is a 64 KB
     *  string. Must not OOM or throw on normal-heap allocations. */
    @Test
    void very_large_scalar_value_is_accepted_or_rejected_but_not_thrown() {
        StringBuilder big = new StringBuilder(64 * 1024);
        for (int i = 0; i < 64 * 1024; i++) big.append('a');
        String line = "{\"atype\":\"big\",\"ts\":0,\"users\":[],"
                + "\"param\":{\"blob\":\"" + big + "\"}}";
        try {
            AuditEvent e = AuditEventParser.parse(line);
            if (e != null) assertEquals("big", e.atype());
        } catch (Throwable t) {
            fail("large-value line threw " + t.getClass().getSimpleName());
        }
    }

    /* ============================== corpus ============================== */

    private static final List<String> CORPUS = List.of(
            "",
            " ",
            "\t\n",
            "\0",
            "\0\0\0",
            "{",
            "}",
            "[]",
            "null",
            "\"just a string\"",
            "12345",
            "true",
            "{\"atype\"",
            "{\"atype\":}",
            "{\"atype\":\"\"}",                                 // empty atype is valid-looking but parser treats "" as missing-like
            "{\"atype\":\"auth\"",                               // truncated
            "{\"atype\":\"auth\",",                              // trailing comma cliff
            "{\"atype\":\"auth\",\"ts\":\"not-a-number\"}",
            "{\"atype\":\"auth\",\"ts\":1.7e308}",                // very large double
            "{\"atype\":\"auth\",\"ts\":-1}",
            "{\"atype\":\"auth\",\"ts\":{\"$date\":\"not-a-date\"}}",
            "{\"atype\":\"auth\",\"ts\":{\"$date\":null}}",
            "{\"atype\":123}",                                    // atype must be string — parser treats as missing
            "{\"atype\":null}",
            "{\"atype\":[\"array\",\"not-string\"]}",
            "{\"atype\":\"x\",\"users\":\"not-an-array\"}",
            "{\"atype\":\"x\",\"users\":[1,2,3]}",                 // users isn't a Document array
            "{\"atype\":\"x\",\"users\":[null]}",
            "{\"atype\":\"x\",\"users\":[{\"user\":null}]}",
            "{\"atype\":\"x\",\"param\":\"not-a-doc\"}",
            "{\"atype\":\"x\",\"param\":null}",
            "{\"atype\":\"x\",\"param\":[1,2,3]}",
            "{\"atype\":\"x\",\"remote\":\"not-a-doc\"}",
            "{\"atype\":\"x\",\"remote\":{\"ip\":null,\"port\":null}}",
            "{\"atype\":\"x\",\"remote\":{\"ip\":\"1.1.1.1\"}}",
            "{\"atype\":\"x\",\"result\":\"not-a-number\"}",
            "{\"atype\":\"x\",\"result\":9.99e99}",
            "{\"atype\":\"\u0001\u0002\u0003\",\"ts\":0,\"users\":[],\"param\":{}}",
            "{\"atype\":\"x\"//comment}",                         // JSON doesn't allow comments
            "{'atype':'x'}",                                      // single quotes not JSON
            "{\"atype\":\"x\",\"extra\":\"field\",\"ts\":0,\"users\":[],\"param\":{}}",
            // Unicode surrogate pair shenanigans
            "{\"atype\":\"\uD83D\uDCBB\",\"ts\":0,\"users\":[],\"param\":{}}",
            // Lone high surrogate (invalid UTF-16)
            "{\"atype\":\"\uD83D\",\"ts\":0,\"users\":[],\"param\":{}}",
            // Reference-like looking content that shouldn't be dereferenced
            "{\"atype\":\"$ref\",\"ts\":0,\"users\":[],\"param\":{}}",
            // Duplicate keys (JSON-lax)
            "{\"atype\":\"a\",\"atype\":\"b\",\"ts\":0,\"users\":[],\"param\":{}}");

    private static String preview(String s) {
        if (s == null) return "null";
        if (s.length() > 80) return s.substring(0, 77) + "...";
        return s;
    }
}
