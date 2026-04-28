package com.kubrik.mex.cluster.model;

/**
 * v2.4 TOPO-11 — canonical member state enum. The numeric codes match the
 * {@code replSetGetStatus.members[i].state} wire codes so round-tripping is
 * lossless.
 */
public enum MemberState {
    STARTUP(0),
    PRIMARY(1),
    SECONDARY(2),
    RECOVERING(3),
    STARTUP2(5),
    UNKNOWN(6),
    ARBITER(7),
    DOWN(8),
    ROLLBACK(9),
    REMOVED(10);

    private final int code;

    MemberState(int code) { this.code = code; }

    public int code() { return code; }

    /** Map a mongod-reported state code or {@code stateStr} to the enum. */
    public static MemberState from(Integer code, String stateStr) {
        if (code != null) {
            for (MemberState s : values()) if (s.code == code) return s;
        }
        if (stateStr != null) {
            try { return MemberState.valueOf(stateStr); } catch (IllegalArgumentException ignored) { }
        }
        return UNKNOWN;
    }
}
