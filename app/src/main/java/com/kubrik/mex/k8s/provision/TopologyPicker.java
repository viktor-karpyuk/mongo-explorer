package com.kubrik.mex.k8s.provision;

import java.util.EnumSet;
import java.util.Set;

/**
 * v2.8.1 Q2.8.1-D3 — Profile × operator topology availability matrix.
 *
 * <p>Milestone §7.8 blessed set for v2.8.1 Alpha:</p>
 * <table>
 *   <tr><th></th><th>Dev/Test</th><th>Prod</th></tr>
 *   <tr><td>MCO</td>  <td>STANDALONE, RS3</td>      <td>RS3, RS5</td></tr>
 *   <tr><td>PSMDB</td><td>STANDALONE, RS3</td>      <td>RS3, RS5, SHARDED</td></tr>
 * </table>
 *
 * <p>Pure function — the wizard's Topology step calls this to
 * populate / restrict the radio group whenever profile or operator
 * change.</p>
 */
public final class TopologyPicker {

    private TopologyPicker() {}

    public static Set<Topology> availableTopologies(Profile profile, OperatorId operator) {
        return switch (profile) {
            case DEV_TEST -> EnumSet.of(Topology.STANDALONE, Topology.RS3);
            case PROD -> switch (operator) {
                case MCO -> EnumSet.of(Topology.RS3, Topology.RS5);
                case PSMDB -> EnumSet.of(Topology.RS3, Topology.RS5, Topology.SHARDED);
            };
        };
    }

    /**
     * Pick a sensible default when the user switches operator or
     * profile. Prefers the current choice when it's still valid,
     * otherwise falls back to RS3 (the lowest-friction available
     * option across every profile / operator pair).
     */
    public static Topology defaultFor(Profile profile, OperatorId operator, Topology current) {
        Set<Topology> avail = availableTopologies(profile, operator);
        if (avail.contains(current)) return current;
        return Topology.RS3;
    }

    /** {@code true} when the user's SHARDED pick is legal on the current profile + operator. */
    public static boolean allowsSharded(Profile profile, OperatorId operator) {
        return profile == Profile.PROD && operator == OperatorId.PSMDB;
    }
}
