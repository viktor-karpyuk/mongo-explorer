package com.kubrik.mex.model;

/**
 * Connection definition. Mirrors the field set of Studio 3T's connection editor:
 * Server / Authentication / SSL TLS / SSH Tunnel / Proxy / Advanced / URI.
 *
 * Stored secrets (passwords, key passphrases) are AES-GCM encrypted via Crypto.
 */
public record MongoConnection(
        String id,
        String name,
        String mode,                 // "FORM" or "URI"
        String uri,                  // raw URI (used when mode = URI)

        // ---- Server ----
        String connectionType,       // STANDALONE | REPLICA_SET | SHARDED | DNS_SRV
        String hosts,                // comma-separated host:port list
        String srvHost,              // hostname for mongodb+srv://

        // ---- Authentication ----
        String authMode,             // NONE | DEFAULT | SCRAM-SHA-1 | SCRAM-SHA-256 |
                                     // MONGODB-X509 | GSSAPI | PLAIN | MONGODB-AWS
        String username,
        String encPassword,
        String authDb,
        String gssapiServiceName,
        String awsSessionToken,

        // ---- SSL / TLS ----
        boolean tlsEnabled,
        String tlsCaFile,
        String tlsClientCertFile,
        String encTlsClientCertPassword,
        boolean tlsAllowInvalidHostnames,
        boolean tlsAllowInvalidCertificates,

        // ---- SSH Tunnel (UI + storage only; not yet wired into the driver) ----
        boolean sshEnabled,
        String sshHost,
        int sshPort,
        String sshUser,
        String sshAuthMode,          // PASSWORD | KEY
        String encSshPassword,
        String sshKeyFile,
        String encSshKeyPassphrase,

        // ---- Proxy ----
        String proxyType,            // NONE | SOCKS5 | HTTP
        String proxyHost,
        int proxyPort,
        String proxyUser,
        String encProxyPassword,

        // ---- Advanced ----
        String replicaSetName,
        String readPreference,       // primary | primaryPreferred | secondary | secondaryPreferred | nearest
        String defaultDb,
        String appName,
        String manualUriOptions,     // "key=value&key2=value2"

        long createdAt,
        long updatedAt
) {
    public static MongoConnection blank() {
        return new MongoConnection(
                null, "", "FORM", "",
                "STANDALONE", "localhost:27017", "",
                "NONE", "", null, "admin", "", "",
                false, "", "", null, false, false,
                false, "", 22, "", "PASSWORD", null, "", null,
                "NONE", "", 1080, "", null,
                "", "primary", "", "MongoExplorer", "",
                0L, 0L);
    }
}
