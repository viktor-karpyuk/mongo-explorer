package com.kubrik.mex.security.cert;

import com.kubrik.mex.model.MongoConnection;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * v2.6 Q2.6-E3 — seed-member parsing rules for
 * {@link CertExpiryScheduler#parseMembers}. Full sweep behaviour is
 * integration-tested; the classifier logic (FORM vs. URI, empty,
 * comma-split, whitespace) is covered here.
 */
class CertExpiryParseTest {

    @Test
    void parses_single_host_in_FORM_mode() {
        MongoConnection c = connection("FORM", "localhost:27017");
        assertEquals(List.of("localhost:27017"), CertExpiryScheduler.parseMembers(c));
    }

    @Test
    void parses_comma_separated_host_list_with_spaces() {
        MongoConnection c = connection("FORM", "h1:27017, h2:27017 , h3:27018");
        assertEquals(List.of("h1:27017", "h2:27017", "h3:27018"),
                CertExpiryScheduler.parseMembers(c));
    }

    @Test
    void skips_URI_mode_connections() {
        MongoConnection c = connection("URI", "ignored");
        assertTrue(CertExpiryScheduler.parseMembers(c).isEmpty());
    }

    @Test
    void skips_DNS_SRV_connections() {
        // DNS_SRV connections use the 'srvHost' field, not hosts; the
        // sweep can't probe without resolving SRV records which is a
        // Q2.6-K topic.
        MongoConnection c = new MongoConnection(
                "id", "n", "FORM", "",
                "DNS_SRV", "", "cluster.example.com",
                "NONE", "", null, "admin", "", "",
                false, "", "", null, false, false,
                false, "", 22, "", "PASSWORD", null, "", null,
                "NONE", "", 1080, "", null,
                "", "primary", "", "MongoExplorer", "",
                0L, 0L);
        // hosts is empty so parseMembers returns empty.
        assertTrue(CertExpiryScheduler.parseMembers(c).isEmpty());
    }

    @Test
    void blank_hosts_returns_empty() {
        assertTrue(CertExpiryScheduler.parseMembers(connection("FORM", "")).isEmpty());
        assertTrue(CertExpiryScheduler.parseMembers(connection("FORM", "   ")).isEmpty());
    }

    @Test
    void null_connection_returns_empty() {
        assertTrue(CertExpiryScheduler.parseMembers(null).isEmpty());
    }

    @Test
    void strips_rsName_prefix_so_host_port_downstream_split_stays_clean() {
        // MongoDB seed strings sometimes carry the replset name ahead
        // of the host list: "myRs/h1:27017,h2:27017". Without the
        // strip, the first entry stayed "myRs/h1:27017" and the
        // downstream host:port splitter would mis-parse.
        MongoConnection c = connection("FORM", "myRs/h1:27017, h2:27017");
        assertEquals(List.of("h1:27017", "h2:27017"),
                CertExpiryScheduler.parseMembers(c));
    }

    @Test
    void bracketed_IPv6_host_survives_because_it_has_no_slash() {
        // Edge: '[::1]:27017' contains no '/', so the strip is a no-op.
        MongoConnection c = connection("FORM", "[::1]:27017");
        assertEquals(List.of("[::1]:27017"),
                CertExpiryScheduler.parseMembers(c));
    }

    private static MongoConnection connection(String mode, String hosts) {
        return new MongoConnection(
                "id", "n", mode, "",
                "STANDALONE", hosts, "",
                "NONE", "", null, "admin", "", "",
                false, "", "", null, false, false,
                false, "", 22, "", "PASSWORD", null, "", null,
                "NONE", "", 1080, "", null,
                "", "primary", "", "MongoExplorer", "",
                0L, 0L);
    }
}
