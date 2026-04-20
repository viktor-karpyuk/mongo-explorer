package com.kubrik.mex.monitoring;

import com.kubrik.mex.monitoring.exporter.MetricRegistry;
import com.kubrik.mex.monitoring.exporter.PrometheusFormat;
import com.kubrik.mex.monitoring.model.LabelSet;
import com.kubrik.mex.monitoring.model.MetricId;
import com.kubrik.mex.monitoring.model.MetricSample;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PrometheusFormatTest {

    @Test
    void usesMexMongoPrefixAndConvertsTimeUnits() {
        MetricRegistry r = new MetricRegistry();
        r.onSamples(List.of(
                new MetricSample("c1", MetricId.LAT_5, LabelSet.of("host", "h0"), 1_000_000, 250_000),
                new MetricSample("c1", MetricId.WT_3,  LabelSet.of("host", "h0"), 1_000_000, 0.42)
        ));
        String out = PrometheusFormat.render(r.snapshot());
        assertTrue(out.contains("mex_mongo_latency_reads_p99_us"), out);
        // Time unit conversion: 250_000 µs → 0.25 s
        assertTrue(out.contains("0.25"), "expected µs→s conversion, got " + out);
        assertTrue(out.contains("connection=\"c1\""));
        assertTrue(out.contains("host=\"h0\""));
    }

    @Test
    void emitsHelpAndTypeHeadersPerMetric() {
        MetricRegistry r = new MetricRegistry();
        r.onSamples(List.of(
                new MetricSample("c1", MetricId.WT_3, LabelSet.EMPTY, 1_000, 0.5),
                new MetricSample("c1", MetricId.WT_3, LabelSet.of("host", "h1"), 1_000, 0.7)
        ));
        String out = PrometheusFormat.render(r.snapshot());
        // one HELP, one TYPE for a metric regardless of label count
        long helpCount = out.lines().filter(l -> l.startsWith("# HELP mex_mongo_wt_cache_fill_ratio")).count();
        long typeCount = out.lines().filter(l -> l.startsWith("# TYPE mex_mongo_wt_cache_fill_ratio")).count();
        assertEquals(1, helpCount);
        assertEquals(1, typeCount);
    }

    @Test
    void escapesQuotesAndBackslashInLabelValues() {
        MetricRegistry r = new MetricRegistry();
        r.onSamples(List.of(new MetricSample(
                "c1", MetricId.WT_3, LabelSet.of("coll", "a\"b\\c"), 1_000, 0.5)));
        String out = PrometheusFormat.render(r.snapshot());
        assertTrue(out.contains("coll=\"a\\\"b\\\\c\""), "label escaping wrong: " + out);
    }
}
