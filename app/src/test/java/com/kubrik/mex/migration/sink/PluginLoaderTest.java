package com.kubrik.mex.migration.sink;

import com.kubrik.mex.migration.engine.Batch;
import com.kubrik.mex.migration.engine.Namespaces;
import com.kubrik.mex.migration.spec.SinkSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.*;

/** Round-trips a ServiceLoader-style plugin JAR through {@link PluginLoader} and
 *  {@link PluginSinkRegistry}. Uses a test-only factory class ({@code SamplePluginFactory})
 *  that already sits on the test classpath; we pack its classfile into a fresh JAR with a
 *  matching {@code META-INF/services} descriptor and point the loader at it. */
class PluginLoaderTest {

    @TempDir Path pluginsDir;
    private URLClassLoader pluginClassLoader;

    @BeforeEach
    void setUp() { PluginSinkRegistry.clearForTesting(); }

    @AfterEach
    void tearDown() throws Exception {
        PluginSinkRegistry.clearForTesting();
        if (pluginClassLoader != null) pluginClassLoader.close();
    }

    @Test
    void missing_plugins_dir_is_noop() throws Exception {
        Path nonexistent = pluginsDir.resolve("no-such-dir");
        pluginClassLoader = PluginLoader.loadFrom(nonexistent);
        assertNull(pluginClassLoader, "loadFrom returns null when the dir doesn't exist");
        assertTrue(PluginSinkRegistry.registered().isEmpty());
    }

    @Test
    void empty_plugins_dir_registers_nothing() throws Exception {
        pluginClassLoader = PluginLoader.loadFrom(pluginsDir);
        assertNull(pluginClassLoader, "no JARs → null classloader");
        assertTrue(PluginSinkRegistry.registered().isEmpty());
    }

    @Test
    void service_loader_registers_factory_from_jar() throws Exception {
        Path jarFile = pluginsDir.resolve("sample-plugin.jar");
        buildPluginJar(jarFile, SamplePluginFactory.class);

        pluginClassLoader = PluginLoader.loadFrom(pluginsDir);
        assertNotNull(pluginClassLoader, "classloader should be built when JARs are present");

        assertTrue(PluginSinkRegistry.registered().contains("sample-plugin"),
                "registered names should include `sample-plugin`, got: " + PluginSinkRegistry.registered());
        MigrationSinkFactory f = PluginSinkRegistry.resolve("sample-plugin");
        assertNotNull(f);
        assertEquals(".sample", f.extension());

        MigrationSink sink = f.create(new SinkSpec(SinkSpec.SinkKind.PLUGIN, "/tmp", "sample-plugin"));
        assertNotNull(sink);
        assertTrue(sink instanceof SamplePluginSink);
    }

    /** Package the given class file + a {@code META-INF/services} descriptor naming it as
     *  a {@link MigrationSinkFactory} into a freshly-written JAR at {@code target}. */
    private static void buildPluginJar(Path target, Class<? extends MigrationSinkFactory> factory) throws IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(target), manifest)) {
            // META-INF/services descriptor
            jar.putNextEntry(new JarEntry("META-INF/services/" + MigrationSinkFactory.class.getName()));
            jar.write(factory.getName().getBytes(StandardCharsets.UTF_8));
            jar.write('\n');
            jar.closeEntry();

            // Class files — emit the factory class and every inner/associated class we need.
            writeClass(jar, factory);
            writeClass(jar, SamplePluginSink.class);
        }
    }

    private static void writeClass(JarOutputStream jar, Class<?> cls) throws IOException {
        String internal = cls.getName().replace('.', '/') + ".class";
        jar.putNextEntry(new JarEntry(internal));
        try (InputStream in = cls.getClassLoader().getResourceAsStream(internal)) {
            assertNotNull(in, "classfile not on test classpath: " + internal);
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            in.transferTo(buf);
            jar.write(buf.toByteArray());
        }
        jar.closeEntry();
    }

    /** Test-only factory — declared public + top-level so ServiceLoader can instantiate it. */
    public static final class SamplePluginFactory implements MigrationSinkFactory {
        public SamplePluginFactory() {}   // ServiceLoader contract
        @Override public String name()      { return "sample-plugin"; }
        @Override public String extension() { return ".sample"; }
        @Override public MigrationSink create(SinkSpec spec) { return new SamplePluginSink(); }
    }

    /** Minimal sink impl — never opened or written to in this test; existence is enough. */
    public static final class SamplePluginSink implements MigrationSink {
        @Override public void open(Namespaces.Ns target) {}
        @Override public void writeBatch(Batch batch) {}
        @Override public void close() {}
    }
}
