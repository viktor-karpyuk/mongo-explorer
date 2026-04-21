package com.kubrik.mex.ui.security;

import com.kubrik.mex.core.MongoService;
import com.kubrik.mex.security.EvidenceSigner;
import com.kubrik.mex.security.access.UsersRolesFetcher;
import com.kubrik.mex.security.audit.AuditIndex;
import com.kubrik.mex.security.authn.AuthBackendProbe;
import com.kubrik.mex.security.baseline.SecurityBaselineCaptureService;
import com.kubrik.mex.security.baseline.SecurityBaselineDao;
import com.kubrik.mex.security.cert.CertFetcher;
import com.kubrik.mex.security.cert.CertRecord;
import com.kubrik.mex.security.cis.CisRunner;
import com.kubrik.mex.security.cis.CisSuppressionsDao;
import com.kubrik.mex.security.cis.ComplianceContext;
import com.kubrik.mex.security.cis.rules.CisRulePack;
import com.kubrik.mex.security.drift.DriftAckDao;
import com.kubrik.mex.security.encryption.EncryptionProbe;
import com.kubrik.mex.security.encryption.EncryptionStatus;
import javafx.scene.control.Tab;
import javafx.scene.control.TabPane;
import javafx.scene.layout.BorderPane;

import java.util.List;
import java.util.function.Supplier;

/**
 * v2.6 top-level Security tab. Hosts every sub-pane produced by the
 * workstreams and owns the wire-up that closes probe / DAO / service
 * suppliers over the connection's {@link MongoService}.
 *
 * <p>Construction is per-connection: the tab owns its probes and panes,
 * and tears them down when the connection closes. Heavy work (probes,
 * scans) runs on virtual threads inside each pane, so the tab itself
 * stays lightweight.</p>
 */
public final class SecurityTab extends BorderPane {

    private final RoleMatrixPane rolesPane = new RoleMatrixPane();
    private final AuditPane auditPane = new AuditPane();
    private final DriftPane driftPane = new DriftPane();
    private final CertificatesPane certsPane = new CertificatesPane();
    private final AuthBackendPane authPane = new AuthBackendPane();
    private final EncryptionPane encryptionPane = new EncryptionPane();
    private final CisPane cisPane = new CisPane();

    public SecurityTab(String connectionId,
                        String capturedBy,
                        MongoService svc,
                        List<String> clusterMembers,
                        SecurityBaselineDao baselineDao,
                        DriftAckDao ackDao,
                        CisSuppressionsDao suppressionsDao,
                        AuditIndex auditIndex,
                        EvidenceSigner signer) {
        setStyle("-fx-background-color: white;");

        UsersRolesFetcher fetcher = new UsersRolesFetcher();
        AuthBackendProbe authProbe = new AuthBackendProbe();
        EncryptionProbe encryptionProbe = new EncryptionProbe();
        CertFetcher certFetcher = new CertFetcher();
        SecurityBaselineCaptureService captureService =
                new SecurityBaselineCaptureService(fetcher, baselineDao, java.time.Clock.systemUTC());

        /* ---------- shared suppliers that probes close over ---------- */
        Supplier<UsersRolesFetcher.Snapshot> matrixSnapshot = () ->
                fetcher.fetch(svc, UsersRolesFetcher.FetchOptions.forMatrix());
        Supplier<UsersRolesFetcher.Snapshot> baselineSnapshot = () ->
                fetcher.fetch(svc, UsersRolesFetcher.FetchOptions.forBaseline());
        Supplier<AuthBackendProbe.Snapshot> authSnapshot = () -> authProbe.probe(svc);
        Supplier<List<EncryptionStatus>> encryptionSnapshot = () ->
                clusterMembers.isEmpty()
                        ? List.of(encryptionProbe.probe(svc, "<cluster>"))
                        : clusterMembers.stream()
                                .map(h -> encryptionProbe.probe(svc, h))
                                .toList();
        Supplier<List<CertRecord>> certSnapshot = () -> clusterMembers.stream()
                .flatMap(m -> {
                    int colon = m.indexOf(':');
                    String host = colon < 0 ? m : m.substring(0, colon);
                    int port = colon < 0 ? 27017 : Integer.parseInt(m.substring(colon + 1));
                    return certFetcher.fetch(host, port).stream();
                }).toList();
        Supplier<com.kubrik.mex.security.cis.CisReport> scanner = () -> {
            ComplianceContext ctx = new ComplianceContext(connectionId,
                    matrixSnapshot.get(), authSnapshot.get(),
                    encryptionSnapshot.get(), certSnapshot.get());
            return new CisRunner(CisRulePack.all()).run(ctx,
                    suppressionsDao.listActive(connectionId,
                            System.currentTimeMillis()),
                    System.currentTimeMillis());
        };

        /* ---------- wire each pane ---------- */
        rolesPane.setLoader(matrixSnapshot);
        rolesPane.setOnCaptureBaseline(() -> {
            UsersRolesFetcher.Snapshot snap = baselineSnapshot.get();
            captureService.persist(connectionId, capturedBy,
                    "captured from role matrix", snap);
        });
        auditPane.configure(connectionId, auditIndex);
        driftPane.configure(connectionId, capturedBy,
                baselineDao, ackDao, captureService, baselineSnapshot);
        certsPane.setLoader(certSnapshot);
        authPane.setLoader(authSnapshot);
        encryptionPane.setLoader(encryptionSnapshot);
        cisPane.configure(connectionId, capturedBy, scanner,
                suppressionsDao, signer);

        TabPane tabs = new TabPane(
                closeable("Roles", rolesPane),
                closeable("Audit", auditPane),
                closeable("Drift", driftPane),
                closeable("Certificates", certsPane),
                closeable("Auth", authPane),
                closeable("Encryption", encryptionPane),
                closeable("CIS", cisPane));
        tabs.setTabClosingPolicy(TabPane.TabClosingPolicy.UNAVAILABLE);
        setCenter(tabs);
    }

    private static Tab closeable(String title, javafx.scene.Node content) {
        Tab t = new Tab(title, content);
        t.setClosable(false);
        return t;
    }

    /* ------------ accessors used by unit / smoke tests ------------ */

    public RoleMatrixPane rolesPane()         { return rolesPane; }
    public AuditPane auditPane()              { return auditPane; }
    public DriftPane driftPane()              { return driftPane; }
    public CertificatesPane certsPane()       { return certsPane; }
    public AuthBackendPane authPane()         { return authPane; }
    public EncryptionPane encryptionPane()    { return encryptionPane; }
    public CisPane cisPane()                  { return cisPane; }
}
