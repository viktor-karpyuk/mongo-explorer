package com.kubrik.mex.labs.lifecycle;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LabAppExitHookTest {

    @Test
    void parsePolicy_maps_setting_strings() {
        assertEquals(LabAppExitHook.Policy.STOP,
                LabAppExitHook.parsePolicy(null));
        assertEquals(LabAppExitHook.Policy.STOP,
                LabAppExitHook.parsePolicy("stop"));
        assertEquals(LabAppExitHook.Policy.STOP,
                LabAppExitHook.parsePolicy("STOP"));
        assertEquals(LabAppExitHook.Policy.LEAVE_RUNNING,
                LabAppExitHook.parsePolicy("leave_running"));
        assertEquals(LabAppExitHook.Policy.DESTROY,
                LabAppExitHook.parsePolicy("destroy"));
        // Unknown values fall through to STOP (safest default).
        assertEquals(LabAppExitHook.Policy.STOP,
                LabAppExitHook.parsePolicy("yolo"));
    }
}
