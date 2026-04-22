package com.kubrik.mex.maint.approval;

import com.kubrik.mex.security.EvidenceSigner;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * v2.7 Q2.7-A — Minimal HS256 JWS producer + verifier used for
 * approval tokens. Not a general-purpose JWS library; the footprint
 * is small because this is the only caller and we avoid dragging in
 * jose4j just for a fixed shape.
 *
 * <p>Token shape (RFC 7515, compact serialization):</p>
 * <pre>base64url(headerJson).base64url(payloadJson).base64url(HMAC-SHA-256(signingInput))</pre>
 *
 * <p>Signing material is the v2.6 evidence key (HMAC-SHA-256), shared
 * across every maintenance signature surface so auditors only reason
 * about one key per install. Reviewer and executor running the same
 * install verifies trivially; cross-install verification requires prior
 * out-of-band key import (out of scope per NG-2.7-4).</p>
 */
public final class JwsSigner {

    private final EvidenceSigner evidenceSigner;

    /** Fixed header; {@code kid} is omitted because v2.7 does not
     *  federate keys. When cross-install support lands, add a
     *  per-install key id lookup here. */
    private static final String HEADER_JSON = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";

    public JwsSigner(EvidenceSigner evidenceSigner) {
        this.evidenceSigner = evidenceSigner;
    }

    /** Produce a compact-serialized JWS with the supplied payload
     *  claims. The claims map preserves iteration order so the
     *  serialized JSON is deterministic — a tamper detection property
     *  the verifier relies on. */
    public String sign(Map<String, Object> claims) {
        String headerB64 = b64url(HEADER_JSON.getBytes(StandardCharsets.UTF_8));
        String payloadJson = serialize(claims);
        String payloadB64 = b64url(payloadJson.getBytes(StandardCharsets.UTF_8));
        String signingInput = headerB64 + "." + payloadB64;
        // evidenceSigner.sign returns hex; JWS expects raw-bytes-b64url
        // so we compute the HMAC bytes directly here via the same key.
        byte[] sig = hmacBytes(signingInput.getBytes(StandardCharsets.UTF_8));
        return signingInput + "." + b64url(sig);
    }

    /** Verify a token and return its payload claims if the signature
     *  checks out; {@link java.util.Optional#empty()} for any failure
     *  (malformed, wrong alg, bad signature). Does NOT verify
     *  expiration — the {@code ApprovalService} owns that policy so
     *  replay windows can be reasoned about in one place. */
    public java.util.Optional<Map<String, Object>> verify(String jws) {
        if (jws == null) return java.util.Optional.empty();
        String[] parts = jws.split("\\.");
        if (parts.length != 3) return java.util.Optional.empty();
        String headerB64 = parts[0];
        String payloadB64 = parts[1];
        String sigB64 = parts[2];
        try {
            String header = new String(b64urlDecode(headerB64), StandardCharsets.UTF_8);
            // Constant-time enough — header is short and comes from us.
            if (!HEADER_JSON.equals(header)) return java.util.Optional.empty();
            String signingInput = headerB64 + "." + payloadB64;
            byte[] expected = hmacBytes(signingInput.getBytes(StandardCharsets.UTF_8));
            byte[] got = b64urlDecode(sigB64);
            if (!java.security.MessageDigest.isEqual(expected, got)) {
                return java.util.Optional.empty();
            }
            String payloadJson = new String(b64urlDecode(payloadB64),
                    StandardCharsets.UTF_8);
            return java.util.Optional.of(parseFlatJsonObject(payloadJson));
        } catch (Exception e) {
            return java.util.Optional.empty();
        }
    }

    /* ============================ helpers ============================ */

    /** Compute HMAC-SHA-256 bytes by piggy-backing on EvidenceSigner's
     *  hex output. Decoding hex → bytes is cheaper than plumbing a
     *  second Mac instance through. */
    private byte[] hmacBytes(byte[] payload) {
        String hex = evidenceSigner.sign(payload);
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    /** RFC 4648 §5 — JWS uses unpadded base64url. */
    private static String b64url(byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static byte[] b64urlDecode(String s) {
        return Base64.getUrlDecoder().decode(s);
    }

    /** Deterministic JSON serializer for a flat string/number/boolean
     *  claim map. Intentionally narrow: JWS payloads here only carry
     *  primitives, and avoiding Jackson keeps the signing path
     *  dependency-free on a hot code path. */
    private static String serialize(Map<String, Object> claims) {
        StringBuilder sb = new StringBuilder(128);
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> e : claims.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(escape(e.getKey())).append("\":");
            Object v = e.getValue();
            if (v == null) sb.append("null");
            else if (v instanceof Number || v instanceof Boolean) sb.append(v);
            else sb.append('"').append(escape(v.toString())).append('"');
        }
        sb.append('}');
        return sb.toString();
    }

    /** Mini JSON parser — only understands the flat shape
     *  {@link #serialize} emits. Richer JSON would need Jackson, but
     *  the JWS payload is entirely our own output so the parser
     *  matches the writer one-to-one. */
    private static Map<String, Object> parseFlatJsonObject(String json) {
        Map<String, Object> out = new LinkedHashMap<>();
        int i = 0, n = json.length();
        if (n < 2 || json.charAt(0) != '{' || json.charAt(n - 1) != '}')
            throw new IllegalArgumentException("not a json object");
        i = 1;
        while (i < n - 1) {
            while (i < n - 1 && (json.charAt(i) == ' ' || json.charAt(i) == ',')) i++;
            if (i >= n - 1) break;
            if (json.charAt(i) != '"') throw new IllegalArgumentException(
                    "expected key string at " + i);
            int keyEnd = findStringEnd(json, i + 1);
            String key = unescape(json.substring(i + 1, keyEnd));
            i = keyEnd + 1;
            while (i < n - 1 && (json.charAt(i) == ' ' || json.charAt(i) == ':')) i++;
            Object value;
            if (json.charAt(i) == '"') {
                int vEnd = findStringEnd(json, i + 1);
                value = unescape(json.substring(i + 1, vEnd));
                i = vEnd + 1;
            } else {
                int vStart = i;
                while (i < n - 1 && json.charAt(i) != ',' && json.charAt(i) != '}') i++;
                String token = json.substring(vStart, i).trim();
                if ("null".equals(token)) value = null;
                else if ("true".equals(token)) value = Boolean.TRUE;
                else if ("false".equals(token)) value = Boolean.FALSE;
                else if (token.contains(".")) value = Double.parseDouble(token);
                else value = Long.parseLong(token);
            }
            out.put(key, value);
        }
        return out;
    }

    private static int findStringEnd(String json, int from) {
        for (int i = from; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '\\') { i++; continue; }
            if (c == '"') return i;
        }
        throw new IllegalArgumentException("unterminated string");
    }

    private static String escape(String s) {
        StringBuilder out = new StringBuilder(s.length() + 4);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\' -> out.append("\\\\");
                case '"'  -> out.append("\\\"");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
                    else out.append(c);
                }
            }
        }
        return out.toString();
    }

    private static String unescape(String s) {
        StringBuilder out = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(++i);
                switch (n) {
                    case '\\' -> out.append('\\');
                    case '"'  -> out.append('"');
                    case 'n'  -> out.append('\n');
                    case 'r'  -> out.append('\r');
                    case 't'  -> out.append('\t');
                    case 'u' -> {
                        if (i + 4 >= s.length())
                            throw new IllegalArgumentException("bad \\u escape");
                        out.append((char) Integer.parseInt(s.substring(i + 1, i + 5), 16));
                        i += 4;
                    }
                    default -> out.append(n);
                }
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }
}
