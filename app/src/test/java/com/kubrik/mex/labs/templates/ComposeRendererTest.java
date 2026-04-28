package com.kubrik.mex.labs.templates;

import com.kubrik.mex.labs.model.LabTemplate;
import com.kubrik.mex.labs.model.PortMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ComposeRendererTest {

    private final ComposeRenderer renderer = new ComposeRenderer();
    private final LabTemplateRegistry registry = new LabTemplateRegistry();

    @Test
    void standalone_renders_without_unreplaced_placeholders(@TempDir Path tmp)
            throws Exception {
        registry.loadBuiltins();
        LabTemplate t = registry.byId("standalone").orElseThrow();
        Map<String, Integer> ports = new LinkedHashMap<>();
        ports.put("mongo", 27100);

        Path out = renderer.render(t, new PortMap(ports), "mongo:latest",
                /*auth=*/false, "mex-lab-standalone-abcdef12", tmp);

        String body = Files.readString(out);
        assertTrue(body.contains("127.0.0.1:27100:27017"));
        assertTrue(body.contains("name: mex-lab-standalone-abcdef12"));
        assertTrue(body.contains("image: mongo:latest"));
        assertFalse(body.contains("{{"));
        assertFalse(body.contains("}}"));
    }

    @Test
    void is_deterministic_same_inputs_same_bytes(@TempDir Path tmp) throws Exception {
        registry.loadBuiltins();
        LabTemplate t = registry.byId("rs-3").orElseThrow();
        Map<String, Integer> ports = new LinkedHashMap<>();
        ports.put("rs1a", 27100); ports.put("rs1b", 27101); ports.put("rs1c", 27102);
        ports.put("init", 27103);
        PortMap pm = new PortMap(ports);

        String a = renderer.renderString(t, pm, "mongo:8", false, "mex-lab-rs-3-11111111");
        String b = renderer.renderString(t, pm, "mongo:8", false, "mex-lab-rs-3-11111111");
        assertEquals(a, b);
    }

    @Test
    void unrendered_placeholder_throws() {
        LabTemplate t = new LabTemplate("broken", "Broken", "",
                100, 10, "mongo:latest",
                java.util.List.of("x"),
                "name: {{projectName}}\nservices:\n  x:\n    image: {{mongoTag}}\n    ports: [\"{{ports.nope}}\"]\n",
                java.util.Optional.empty(), 1);
        assertThrows(IllegalStateException.class,
                () -> renderer.renderString(t, PortMap.empty(),
                        "mongo:latest", false, "proj"));
    }

    @Test
    void template_without_services_header_rejected() {
        LabTemplate t = new LabTemplate("broken", "", "",
                100, 10, "mongo:latest",
                java.util.List.of("x"),
                "name: {{projectName}}\nfoo:\n  bar: 1\n",
                java.util.Optional.empty(), 1);
        assertThrows(IllegalStateException.class,
                () -> renderer.render(t, PortMap.empty(),
                        "mongo:latest", false, "proj",
                        Files.createTempDirectory("mex-render-test-")));
    }
}
