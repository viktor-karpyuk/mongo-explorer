package com.kubrik.mex.labs.model;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PortMapTest {

    @Test
    void empty_has_no_ports() {
        PortMap pm = PortMap.empty();
        assertFalse(pm.has("mongos"));
        assertEquals("{}", pm.toJson());
    }

    @Test
    void toJson_preserves_insertion_order() {
        Map<String, Integer> m = new LinkedHashMap<>();
        m.put("b", 2);
        m.put("a", 1);
        m.put("c", 3);
        assertEquals("{\"b\":2,\"a\":1,\"c\":3}", new PortMap(m).toJson());
    }

    @Test
    void fromJson_round_trips() {
        Map<String, Integer> m = new LinkedHashMap<>();
        m.put("mongos", 27100);
        m.put("cfg1", 27101);
        String json = new PortMap(m).toJson();
        PortMap parsed = PortMap.fromJson(json);
        assertEquals(27100, parsed.portFor("mongos"));
        assertEquals(27101, parsed.portFor("cfg1"));
    }

    @Test
    void portFor_unknown_throws_with_known_set_listed() {
        PortMap pm = new PortMap(Map.of("mongos", 27100));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> pm.portFor("cfg1"));
        assertTrue(e.getMessage().contains("mongos"));
    }

    @Test
    void ports_map_is_immutable() {
        Map<String, Integer> m = new LinkedHashMap<>();
        m.put("mongos", 27100);
        PortMap pm = new PortMap(m);
        // Caller can't mutate the backing store.
        assertThrows(UnsupportedOperationException.class,
                () -> pm.ports().put("cfg1", 27101));
    }

    @Test
    void fromJson_null_or_blank_returns_empty() {
        assertEquals(PortMap.empty(), PortMap.fromJson(null));
        assertEquals(PortMap.empty(), PortMap.fromJson(""));
    }
}
