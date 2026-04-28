package com.kubrik.mex.monitoring.store;

import com.kubrik.mex.monitoring.model.LabelSet;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Minimal parser for the compact JSON form produced by {@link LabelSet#toJson()}.
 * Accepts only the flat {@code {"k":"v",...}} shape with double-quoted string values
 * and the limited escapes we emit ({@code \"} and {@code \\}). Anything else is a
 * parse error — tolerated inputs are the ones we create ourselves.
 */
final class LabelSetJson {

    private LabelSetJson() {}

    static LabelSet parse(String json) {
        if (json == null || json.isEmpty() || json.equals("{}")) return LabelSet.EMPTY;
        if (json.charAt(0) != '{' || json.charAt(json.length() - 1) != '}') {
            throw new IllegalArgumentException("not a JSON object: " + json);
        }
        Map<String, String> out = new LinkedHashMap<>();
        int i = 1;
        int end = json.length() - 1;
        while (i < end) {
            if (json.charAt(i) == ',') { i++; continue; }
            if (json.charAt(i) != '"') throw new IllegalArgumentException("expected key quote at " + i);
            int keyStart = ++i;
            StringBuilder k = new StringBuilder();
            while (i < end && json.charAt(i) != '"') {
                char c = json.charAt(i);
                if (c == '\\' && i + 1 < end) { k.append(json.charAt(i + 1)); i += 2; }
                else { k.append(c); i++; }
            }
            if (i >= end) throw new IllegalArgumentException("unterminated key");
            i++; // consume closing "
            if (i >= end || json.charAt(i) != ':') throw new IllegalArgumentException("expected ':' at " + i);
            i++;
            if (i >= end || json.charAt(i) != '"') throw new IllegalArgumentException("expected value quote at " + i);
            i++;
            StringBuilder v = new StringBuilder();
            while (i < end && json.charAt(i) != '"') {
                char c = json.charAt(i);
                if (c == '\\' && i + 1 < end) { v.append(json.charAt(i + 1)); i += 2; }
                else { v.append(c); i++; }
            }
            if (i >= end) throw new IllegalArgumentException("unterminated value");
            i++; // consume closing "
            out.put(k.toString(), v.toString());
        }
        return new LabelSet(out);
    }
}
