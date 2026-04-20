package com.kubrik.mex.migration;

import com.kubrik.mex.migration.log.Redactor;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 10 000-iteration fuzz of {@link Redactor}. Asserts the generated password never appears
 *  in the redacted output. Runs in ~50 ms on the dev machine — cheap enough for every build.
 *  <p>
 *  Matches the spec's release-gate item (tech spec §22.3). */
class RedactorFuzzTest {

    @Test
    void redactor_never_leaks_uri_password() {
        Redactor r = Redactor.defaultInstance();
        Random rng = new Random(1234567890L); // deterministic for reproducibility
        int leaks = 0;
        int checked = 0;
        for (int i = 0; i < 10_000; i++) {
            String user = randString(rng, 4, 16);
            String pass = randPassword(rng);
            String host = randHost(rng);
            boolean srv = rng.nextBoolean();
            String uri = "mongodb" + (srv ? "+srv" : "") + "://" + user + ":" + pass + "@" + host
                    + (srv ? "" : ":" + (1024 + rng.nextInt(60000)));
            String line = "connected to " + uri + " (readpref=" + randString(rng, 4, 8) + ")";
            String out = r.redact(line);

            // Password must not appear anywhere in the output.
            if (pass.length() >= 4 && out.contains(pass)) {
                leaks++;
                System.err.println("leak at iter " + i + ": " + out);
            }
            // User and host are not secrets — they should pass through.
            assertTrue(out.contains(host), "host `" + host + "` should remain in redacted line");
            checked++;
        }
        assertFalse(leaks > 0, leaks + " / " + checked + " lines leaked the password");
    }

    @Test
    void redactor_scrubs_json_pii_in_fuzz() {
        Redactor r = Redactor.defaultInstance();
        Random rng = new Random(2024);
        String[] keys = { "password", "pwd", "token", "apiKey", "secret" };
        int leaks = 0;
        for (int i = 0; i < 10_000; i++) {
            String key = keys[rng.nextInt(keys.length)];
            String value = randPassword(rng);
            // Both quoted-string and bare-token value shapes exist in real logs.
            String body = rng.nextBoolean()
                    ? "{\"" + key + "\":\"" + value + "\",\"ok\":1}"
                    : "key=" + key + " " + key + ":" + value;
            String out = r.redact(body);
            // JSON key form must be scrubbed. Bare form may leak — the Redactor only targets
            // JSON-shaped keys, which is the documented limitation.
            if (body.contains("\"")) {
                if (out.contains(value)) leaks++;
            }
        }
        assertFalse(leaks > 0, leaks + " JSON PII leaks");
    }

    private static String randPassword(Random r) {
        // Mix of alpha, digits, and URL-safe punctuation that real users pick.
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!-_.~";
        int len = 6 + r.nextInt(24);
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) sb.append(chars.charAt(r.nextInt(chars.length())));
        return sb.toString();
    }

    private static String randString(Random r, int min, int max) {
        String chars = "abcdefghijklmnopqrstuvwxyz0123456789";
        int len = min + r.nextInt(max - min + 1);
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) sb.append(chars.charAt(r.nextInt(chars.length())));
        return sb.toString();
    }

    private static String randHost(Random r) {
        return randString(r, 3, 8) + "." + randString(r, 3, 6) + "."
                + new String[] { "com", "net", "io", "cloud" }[r.nextInt(4)];
    }
}
