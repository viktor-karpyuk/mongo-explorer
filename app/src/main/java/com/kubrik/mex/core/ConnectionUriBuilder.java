package com.kubrik.mex.core;

import com.kubrik.mex.model.MongoConnection;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Builds a MongoDB connection URI from a {@link MongoConnection} form definition. */
public final class ConnectionUriBuilder {

    private ConnectionUriBuilder() {}

    public static String build(MongoConnection c, Crypto crypto) {
        if ("URI".equals(c.mode())) {
            return c.uri();
        }
        boolean srv = "DNS_SRV".equals(c.connectionType());
        StringBuilder sb = new StringBuilder(srv ? "mongodb+srv://" : "mongodb://");

        boolean hasUser = nb(c.username());
        if (hasUser) {
            sb.append(enc(c.username()));
            String pwd = crypto.decrypt(c.encPassword());
            if (nb(pwd)) sb.append(":").append(enc(pwd));
            sb.append("@");
        }

        if (srv) {
            sb.append(c.srvHost() != null ? c.srvHost() : "");
        } else {
            sb.append(c.hosts() != null && !c.hosts().isBlank() ? c.hosts() : "localhost:27017");
        }
        sb.append("/");
        if (nb(c.defaultDb())) sb.append(enc(c.defaultDb()));

        List<String> opts = new ArrayList<>();
        String authMode = c.authMode();
        if (authMode != null && !authMode.equals("NONE") && !authMode.equals("DEFAULT")) {
            opts.add("authMechanism=" + authMode);
        }
        if (hasUser && nb(c.authDb())) opts.add("authSource=" + enc(c.authDb()));
        if (nb(c.gssapiServiceName()) && "GSSAPI".equals(authMode)) {
            opts.add("authMechanismProperties=SERVICE_NAME:" + enc(c.gssapiServiceName()));
        }
        if (nb(c.replicaSetName())) opts.add("replicaSet=" + enc(c.replicaSetName()));
        if (c.tlsEnabled() || srv) {
            if (c.tlsEnabled()) opts.add("tls=true");
            if (c.tlsAllowInvalidCertificates()) opts.add("tlsAllowInvalidCertificates=true");
            if (c.tlsAllowInvalidHostnames()) opts.add("tlsAllowInvalidHostnames=true");
            if (nb(c.tlsCaFile())) opts.add("tlsCAFile=" + enc(c.tlsCaFile()));
            if (nb(c.tlsClientCertFile())) opts.add("tlsCertificateKeyFile=" + enc(c.tlsClientCertFile()));
            String certPwd = crypto.decrypt(c.encTlsClientCertPassword());
            if (nb(certPwd)) opts.add("tlsCertificateKeyFilePassword=" + enc(certPwd));
        }
        if (nb(c.readPreference()) && !"primary".equalsIgnoreCase(c.readPreference())) {
            opts.add("readPreference=" + c.readPreference());
        }
        if (nb(c.appName())) opts.add("appName=" + enc(c.appName()));
        if ("SOCKS5".equals(c.proxyType()) && nb(c.proxyHost())) {
            opts.add("proxyHost=" + enc(c.proxyHost()));
            opts.add("proxyPort=" + c.proxyPort());
            if (nb(c.proxyUser())) opts.add("proxyUsername=" + enc(c.proxyUser()));
            String pp = crypto.decrypt(c.encProxyPassword());
            if (nb(pp)) opts.add("proxyPassword=" + enc(pp));
        }
        if (nb(c.manualUriOptions())) {
            // Strip leading "?" or "&" if user pasted them.
            String m = c.manualUriOptions().trim();
            while (m.startsWith("?") || m.startsWith("&")) m = m.substring(1);
            if (!m.isEmpty()) opts.add(m);
        }
        if (!opts.isEmpty()) sb.append("?").append(String.join("&", opts));
        return sb.toString();
    }

    private static boolean nb(String s) { return s != null && !s.isBlank(); }
    private static String enc(String s) { return URLEncoder.encode(s, StandardCharsets.UTF_8); }
}
