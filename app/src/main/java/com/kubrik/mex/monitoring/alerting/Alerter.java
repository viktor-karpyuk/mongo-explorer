package com.kubrik.mex.monitoring.alerting;

import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import com.kubrik.mex.monitoring.model.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Applies every {@link AlertRule} against incoming {@link MetricSample}s and
 * emits {@link AlertEvent}s on fired / cleared transitions. Rules are matched by
 * metric ID and label-subset; per-connection rules override globals.
 */
public final class Alerter {

    private static final Logger log = LoggerFactory.getLogger(Alerter.class);

    private final CopyOnWriteArrayList<AlertRule> rules = new CopyOnWriteArrayList<>();
    private final SustainTracker tracker = new SustainTracker();
    private final Consumer<AlertEvent> firedSink;
    private final Consumer<AlertEvent> clearedSink;

    // Active fires keyed by (connectionId, ruleId, labelsJson). Maintained alongside
    // the SustainTracker — every firedSink dispatch adds, every clearedSink removes.
    // Used by INSTANCE-CARD-5 to render the worst-severity dot on the connection card.
    private final java.util.concurrent.ConcurrentMap<String, Severity> active =
            new java.util.concurrent.ConcurrentHashMap<>();

    public Alerter(Consumer<AlertEvent> firedSink, Consumer<AlertEvent> clearedSink) {
        this.firedSink = firedSink;
        this.clearedSink = clearedSink;
    }

    public void installRules(List<AlertRule> newRules) {
        rules.clear();
        rules.addAll(newRules);
    }

    public void addRule(AlertRule r) { rules.add(r); }
    public void removeRule(String id) { rules.removeIf(r -> r.id().equals(id)); }
    public List<AlertRule> currentRules() { return List.copyOf(rules); }

    /**
     * Process one batch of samples — called from the metrics event-bus subscriber.
     * The {@link SustainTracker} is documented as single-threaded (technical-spec §11);
     * {@link com.kubrik.mex.events.EventBus} publishes synchronously but multiple
     * sampler virtual-threads can concurrently invoke {@code publishMetrics}, so we
     * serialise here.
     */
    public synchronized void onSamples(List<MetricSample> batch) {
        for (MetricSample s : batch) {
            evaluate(s);
        }
    }

    private void evaluate(MetricSample s) {
        List<AlertRule> matched = matchingRules(s);
        if (matched.isEmpty()) return;
        // Compute labels.toJson() once for the whole sample — it's a
        // StringBuilder build over every label, and the previous
        // shape called it twice per matching rule (once on fire, once
        // on clear) AND once more inside activeKey. With ~2 matching
        // rules per sample, that's 6 builds per sample.
        String labelsJson = s.labels().toJson();
        for (AlertRule rule : matched) {
            Severity observed = classify(rule, s.value());
            SustainTracker.Transition tr = tracker.observe(rule, s.labels().labels(), observed, s.tsMs());
            if (tr == null) continue;
            if (tr.fired()) {
                Severity sev = (tr.to() == SustainTracker.State.CRIT) ? Severity.CRIT : Severity.WARN;
                active.put(activeKey(rule, s, labelsJson), sev);
                firedSink.accept(toEvent(rule, s, sev));
            } else if (tr.cleared()) {
                active.remove(activeKey(rule, s, labelsJson));
                clearedSink.accept(toEvent(rule, s, Severity.OK));
            }
        }
    }

    private static String activeKey(AlertRule rule, MetricSample s, String labelsJson) {
        return s.connectionId() + "\u0000" + rule.id() + "\u0000" + labelsJson;
    }

    /** Worst currently-active severity for a connection — {@link Severity#OK} when none. */
    public Severity severityFor(String connectionId) {
        Severity worst = Severity.OK;
        String prefix = connectionId + "\u0000";
        for (var e : active.entrySet()) {
            if (!e.getKey().startsWith(prefix)) continue;
            if (e.getValue() == Severity.CRIT) return Severity.CRIT;
            if (e.getValue() == Severity.WARN) worst = Severity.WARN;
        }
        return worst;
    }

    /** Rule ids currently firing for a connection. Stable iteration order for tooltips. */
    public java.util.List<String> firingRulesFor(String connectionId) {
        java.util.List<String> out = new java.util.ArrayList<>();
        String prefix = connectionId + "\u0000";
        for (String k : active.keySet()) {
            if (!k.startsWith(prefix)) continue;
            int first = k.indexOf('\u0000');
            int second = k.indexOf('\u0000', first + 1);
            if (first >= 0 && second > first) out.add(k.substring(first + 1, second));
        }
        java.util.Collections.sort(out);
        return out;
    }

    private List<AlertRule> matchingRules(MetricSample s) {
        // Hot-path early-return: with no rules installed (the common
        // case for users who haven't opened the alerts pane), skip
        // the per-sample ArrayList allocation entirely. Saves
        // ~50 bytes × samples-per-second × connections.
        if (rules.isEmpty()) return List.of();

        List<AlertRule> per = null;
        List<AlertRule> global = null;
        for (AlertRule r : rules) {
            if (!r.enabled()) continue;
            if (r.metric() != s.metric()) continue;
            if (!r.matches(s.labels())) continue;
            if (s.connectionId().equals(r.connectionId())) {
                if (per == null) per = new ArrayList<>(2);
                per.add(r);
            } else if (r.connectionId() == null) {
                if (global == null) global = new ArrayList<>(2);
                global.add(r);
            }
        }
        // Per-connection rules shadow same-metric globals per ALERT-RULE-7.
        if (per != null) return per;
        return global != null ? global : List.of();
    }

    private Severity classify(AlertRule rule, double v) {
        Double warn = rule.warnThreshold();
        Double crit = rule.critThreshold();
        return switch (rule.comparator()) {
            case GT -> {
                if (crit != null && v > crit) yield Severity.CRIT;
                if (warn != null && v > warn) yield Severity.WARN;
                yield Severity.OK;
            }
            case LT -> {
                if (crit != null && v < crit) yield Severity.CRIT;
                if (warn != null && v < warn) yield Severity.WARN;
                yield Severity.OK;
            }
            case EQ -> {
                if (crit != null && v == crit) yield Severity.CRIT;
                if (warn != null && v == warn) yield Severity.WARN;
                yield Severity.OK;
            }
            case CHANGED -> Severity.OK; // CHANGED is stateful; evaluator is a no-op here
        };
    }

    private AlertEvent toEvent(AlertRule rule, MetricSample s, Severity sev) {
        return new AlertEvent(
                UUID.randomUUID().toString(),
                rule.id(),
                s.connectionId(),
                sev,
                s.tsMs(),
                sev == Severity.OK ? s.tsMs() : null,
                s.value(),
                s.labels().labels(),
                renderMessage(rule, s, sev));
    }

    private static String renderMessage(AlertRule r, MetricSample s, Severity sev) {
        return "%s %s %s %s (value=%s)".formatted(
                sev.name(), r.metric().metricName(), r.comparator().name(),
                s.labels().toJson(), s.value());
    }

    /** Exposed for tests. */
    public SustainTracker tracker() { return tracker; }
}
