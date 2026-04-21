package com.kubrik.mex.security.cert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;

/**
 * v2.6 Q2.6-E1 — reads the server-presented certificate chain for a
 * {@code host:port} via a TLS handshake and parses each certificate into
 * a {@link CertRecord}.
 *
 * <p>The fetcher uses a <b>trust-all</b> trust manager deliberately: we're
 * inspecting certificates the server chose to present, not using them to
 * establish authenticated traffic. Using the JVM default trust store
 * would reject self-signed or expired certs — exactly the ones the DBA
 * needs to see. The fetcher never returns a {@link javax.net.ssl.SSLSocket}
 * to the caller, and never transmits application data on the handshake.</p>
 *
 * <p>Parsing is extracted to a package-private static {@link #toRecord}
 * so tests feed an inline self-signed {@link X509Certificate} fixture
 * and exercise every record field.</p>
 */
public final class CertFetcher {

    private static final Logger log = LoggerFactory.getLogger(CertFetcher.class);
    private static final int HANDSHAKE_TIMEOUT_MS = 3_000;

    /** Opens an SSL handshake to {@code host:port}, captures the chain,
     *  and returns one {@link CertRecord} per cert in the order the server
     *  presented them (leaf first). Returns an empty list on any network
     *  or TLS error — the caller decides whether absence means "no TLS" or
     *  "unreachable". */
    public List<CertRecord> fetch(String host, int port) {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, new TrustManager[]{ALLOW_ALL}, new java.security.SecureRandom());
            SSLSocketFactory factory = ctx.getSocketFactory();
            try (SSLSocket socket = (SSLSocket) factory.createSocket()) {
                socket.setSoTimeout(HANDSHAKE_TIMEOUT_MS);
                socket.connect(new java.net.InetSocketAddress(host, port), HANDSHAKE_TIMEOUT_MS);
                socket.startHandshake();
                Certificate[] chain = socket.getSession().getPeerCertificates();
                String label = host + ":" + port;
                List<CertRecord> out = new ArrayList<>(chain.length);
                for (Certificate c : chain) {
                    if (c instanceof X509Certificate x) out.add(toRecord(label, x));
                }
                return out;
            }
        } catch (Exception e) {
            log.debug("cert fetch {}:{} failed: {}", host, port, e.getMessage());
            return List.of();
        }
    }

    /* =========================== parser ============================ */

    static CertRecord toRecord(String host, X509Certificate x) {
        String subjectCn = extractCn(x.getSubjectX500Principal().getName());
        String issuerCn  = extractCn(x.getIssuerX500Principal().getName());
        List<String> sans = extractSans(x);
        long notBefore = x.getNotBefore().getTime();
        long notAfter  = x.getNotAfter().getTime();
        String serial  = x.getSerialNumber().toString(16);
        String fingerprint = sha256Hex(encodedDer(x));
        return new CertRecord(host, subjectCn, issuerCn, sans,
                notBefore, notAfter, serial, fingerprint);
    }

    private static String extractCn(String distinguishedName) {
        if (distinguishedName == null) return "";
        // RFC 2253 DN: CN=foo,OU=ops,O=acme — comma-separated RDNs.
        for (String rdn : distinguishedName.split(",")) {
            String r = rdn.trim();
            if (r.regionMatches(true, 0, "CN=", 0, 3)) return r.substring(3);
        }
        return "";
    }

    @SuppressWarnings("unchecked")
    static List<String> extractSans(X509Certificate x) {
        try {
            var raw = x.getSubjectAlternativeNames();
            if (raw == null) return List.of();
            List<String> out = new ArrayList<>();
            for (var entry : raw) {
                // entry = [type, value]; type 2 = DNS, type 7 = IP.
                Object type = entry.get(0);
                Object value = entry.get(1);
                if (value == null) continue;
                int t = ((Number) type).intValue();
                String prefix = switch (t) {
                    case 2 -> "DNS:";
                    case 7 -> "IP:";
                    case 1 -> "email:";
                    case 6 -> "URI:";
                    default -> "type" + t + ":";
                };
                out.add(prefix + value);
            }
            return List.copyOf(out);
        } catch (Exception e) {
            return List.of();
        }
    }

    private static byte[] encodedDer(X509Certificate x) {
        try { return x.getEncoded(); }
        catch (Exception e) { return new byte[0]; }
    }

    /** Lower-case hex SHA-256. 64 chars, matching the sec_cert_cache
     *  fingerprint column contract. */
    static String sha256Hex(byte[] payload) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] d = md.digest(payload == null ? new byte[0] : payload);
            StringBuilder sb = new StringBuilder(d.length * 2);
            for (byte b : d) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            // SHA-256 is guaranteed on any JRE; this is here only to
            // satisfy the compiler + keep the method signature clean.
            return "".repeat(64).replace("", "0").substring(0, 64);
        }
    }

    /* =========================== trust manager =========================== */

    /** Accepts every server cert — the fetcher's job is to read the cert
     *  the server presents, not to authenticate traffic against it. The
     *  certs themselves are the evidence; we never reuse the socket. */
    private static final X509TrustManager ALLOW_ALL = new X509TrustManager() {
        public void checkClientTrusted(X509Certificate[] c, String t) {}
        public void checkServerTrusted(X509Certificate[] c, String t) {}
        public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
    };

    /** Helper exposed so callers that want to hash an arbitrary payload
     *  (e.g., the scope string of a test fixture) can match the cache's
     *  fingerprint format. */
    public static String hashUtf8(String payload) {
        return sha256Hex(payload == null ? new byte[0] : payload.getBytes(StandardCharsets.UTF_8));
    }
}
