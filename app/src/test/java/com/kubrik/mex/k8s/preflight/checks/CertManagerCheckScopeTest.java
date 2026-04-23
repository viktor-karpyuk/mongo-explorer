package com.kubrik.mex.k8s.preflight.checks;

import com.kubrik.mex.k8s.preflight.PreflightCheck;
import com.kubrik.mex.k8s.provision.ProvisionModel;
import com.kubrik.mex.k8s.provision.TlsSpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CertManagerCheckScopeTest {

    private final CertManagerCheck check = new CertManagerCheck();

    @Test
    void skips_when_tls_is_off() {
        ProvisionModel m = ProvisionModel.defaults(1L, "ns", "d").withTls(TlsSpec.off());
        assertEquals(PreflightCheck.PreflightScope.SKIP, check.scope(m));
    }

    @Test
    void runs_conditional_when_cert_manager_mode() {
        ProvisionModel m = ProvisionModel.defaults(1L, "ns", "d")
                .withTls(TlsSpec.certManager("my-issuer"));
        assertEquals(PreflightCheck.PreflightScope.CONDITIONAL, check.scope(m));
    }

    @Test
    void skips_for_byo_secret() {
        ProvisionModel m = ProvisionModel.defaults(1L, "ns", "d")
                .withTls(TlsSpec.byoSecret("my-tls"));
        assertEquals(PreflightCheck.PreflightScope.SKIP, check.scope(m));
    }
}
