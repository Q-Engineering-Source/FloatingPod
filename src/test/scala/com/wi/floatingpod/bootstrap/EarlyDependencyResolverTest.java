package com.wi.floatingpod.bootstrap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import static org.junit.jupiter.api.Assertions.*;

class EarlyDependencyResolverTest {
    @TempDir Path directory;
    private Path jar(String name, Map<String, byte[]> entries) throws IOException {
        Path path = directory.resolve(name);
        try (var out = new JarOutputStream(Files.newOutputStream(path))) {
            for (var e : entries.entrySet()) {
                out.putNextEntry(new JarEntry(e.getKey())); out.write(e.getValue()); out.closeEntry();
            }
        }
        return path;
    }
    private Path nestedBundle(String name, Map<String, byte[]> payloads, String declaredPayloads) throws IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("NonModDeps", "true");
        manifest.getMainAttributes().putValue("ContainedDeps", declaredPayloads);
        Path path = directory.resolve(name);
        try (var out = new JarOutputStream(Files.newOutputStream(path), manifest)) {
            for (var payload : payloads.entrySet()) {
                out.putNextEntry(new JarEntry(payload.getKey())); out.write(payload.getValue()); out.closeEntry();
            }
        }
        return path;
    }
    private EarlyDependencyResolver.ScalarPins scalarPins(Path outer, String version,
            byte[] library, byte[] parser) throws IOException {
        return new EarlyDependencyResolver.ScalarPins(EarlyDependencyResolver.hashFile(outer), version,
            EarlyDependencyResolver.sha256(new ByteArrayInputStream(library)),
            EarlyDependencyResolver.sha256(new ByteArrayInputStream(parser)));
    }
    private EarlyDependencyResolver.Spec spec(Path path, String... resources) throws IOException {
        return new EarlyDependencyResolver.Spec(path.getFileName().toString(), EarlyDependencyResolver.hashFile(path), List.of(resources));
    }
    private EarlyDependencyResolver.Snapshot snapshot(List<Path> paths, List<Path> loaded,
            List<EarlyDependencyResolver.ResourceOrigin> resources) {
        return new EarlyDependencyResolver.Snapshot(List.of(), List.of(), paths, loaded, resources);
    }

    @Test void realArchivePlanVerifiesAllAnchorsBeforeReturningUniqueUrl() throws Exception {
        Path p = jar("runtime.jar", Map.of("scala/Predef.class", new byte[]{1}, "scala/deriving/Mirror.class", new byte[]{2}));
        var spec = spec(p, "scala/Predef.class", "scala/deriving/Mirror.class");
        var plan = EarlyDependencyResolver.verifySnapshot(snapshot(List.of(p, p), List.of(), List.of()), List.of(spec));
        assertEquals(List.of(p.toRealPath().toUri().toURL()), plan.additions());
        assertEquals(List.of("scala/Predef.class", "scala/deriving/Mirror.class"), plan.archives().getFirst().resources());
        assertThrows(UnsupportedOperationException.class, () -> plan.archives().clear());
    }
    @Test void discoveredOuterSpellingSurvivesLaunchUrlWhileIdentityStaysCanonical() throws Exception {
        Path archive = jar("sjdl.jar", Map.of("provider/Distribution.class", new byte[]{1}));
        Path discovered = directory.resolve(".").resolve(archive.getFileName());
        var snapshot = new EarlyDependencyResolver.Snapshot(List.of(discovered), List.of(), List.of(),
                List.of(archive), List.of(), List.of());
        var plan = EarlyDependencyResolver.verifySnapshot(snapshot, List.of(spec(archive, "provider/Distribution.class")));
        assertEquals(archive.toRealPath(), plan.archives().getFirst().path());
        var launchFile = new java.io.File(plan.additions().getFirst().toURI());
        assertEquals(discovered.toFile().getAbsoluteFile(), launchFile,
                "Cleanroom File.equals candidate deduplication must see the original discovered outer spelling");
        var candidates = new java.util.HashSet<java.io.File>();
        candidates.add(discovered.toFile().getAbsoluteFile());
        assertFalse(candidates.add(launchFile), "early classpath and later mod scan must be one candidate");
        assertEquals(1, candidates.size());
        var alreadyLoaded = new EarlyDependencyResolver.Snapshot(List.of(discovered), List.of(), List.of(),
                List.of(archive), List.of(archive.toRealPath()), List.of());
        assertTrue(EarlyDependencyResolver.verifySnapshot(alreadyLoaded,
                List.of(spec(archive, "provider/Distribution.class"))).additions().isEmpty());
    }
    @Test void wrongHashAndPartialScalaProviderExplicitlyFail() throws Exception {
        Path p = jar("partial.jar", Map.of("scala/Predef.class", new byte[]{1}));
        var mismatch = new EarlyDependencyResolver.Spec("partial", "0".repeat(64), List.of("scala/Predef.class"));
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class,
            () -> EarlyDependencyResolver.verifySnapshot(snapshot(List.of(p), List.of(), List.of()), List.of(mismatch)));
        var incomplete = spec(p, "scala/Predef.class", "scala/deriving/Mirror.class");
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class,
            () -> EarlyDependencyResolver.verifySnapshot(snapshot(List.of(p), List.of(), List.of()), List.of(incomplete)));
    }
    @Test void duplicateCandidateProviderEvenWithSameHashFails() throws Exception {
        Path p = jar("first.jar", Map.of("provider/Driver.class", new byte[]{1}));
        Path second = directory.resolve("second.jar"); Files.copy(p, second);
        var spec = spec(p, "provider/Driver.class");
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class,
            () -> EarlyDependencyResolver.verifySnapshot(snapshot(List.of(p, second), List.of(), List.of()), List.of(spec)));
    }
    @Test void actualUrlLoaderResourcesAreInspectedAndExistingSourceIsNotReadded() throws Exception {
        Path p = jar("existing.jar", Map.of("provider/Driver.class", new byte[]{3}));
        try (var loader = new URLClassLoader(new URL[]{p.toUri().toURL()}, null)) {
            var resources = EarlyDependencyResolver.inspectResources(loader, List.of("provider/Driver.class"));
            assertEquals(p.toRealPath(), resources.getFirst().archive());
            assertEquals(EarlyDependencyResolver.hashFile(p), resources.getFirst().archiveSha256());
            var plan = EarlyDependencyResolver.verifySnapshot(snapshot(List.of(p), List.of(p), resources),
                List.of(spec(p, "provider/Driver.class")));
            assertTrue(plan.additions().isEmpty());
            EarlyDependencyResolver.validateVisible(resources, plan.archives(), true);
        }
    }
    @Test void parentResourceConflictIsRetainedAndRejected() throws Exception {
        Path parentJar = jar("parent.jar", Map.of("provider/Driver.class", new byte[]{1}));
        Path childJar = jar("child.jar", Map.of("provider/Driver.class", new byte[]{2}));
        try (var parent = new URLClassLoader(new URL[]{parentJar.toUri().toURL()}, null);
             var child = new URLClassLoader(new URL[]{childJar.toUri().toURL()}, parent)) {
            var resources = EarlyDependencyResolver.inspectResources(child, List.of("provider/Driver.class"));
            assertEquals(2, resources.size());
            assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () ->
                EarlyDependencyResolver.verifySnapshot(snapshot(List.of(childJar), List.of(childJar), resources),
                    List.of(spec(childJar, "provider/Driver.class"))));
        }
    }
    @Test void directoryResourceOwnerAndMissingProviderExplicitlyFail() throws Exception {
        Path dir = Files.createDirectory(directory.resolve("classes"));
        Path cls = dir.resolve("provider/Driver.class"); Files.createDirectories(cls.getParent()); Files.write(cls, new byte[]{1});
        var spec = new EarlyDependencyResolver.Spec("provider", "0".repeat(64), List.of("provider/Driver.class"));
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () ->
            EarlyDependencyResolver.verifySnapshot(snapshot(List.of(dir), List.of(), List.of()), List.of(spec)));
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () ->
            EarlyDependencyResolver.verifySnapshot(snapshot(List.of(), List.of(), List.of()), List.of(spec)));
    }
    @Test void scalarRuntimeMetadataUsesExactModIdAndVersionAndPreservesParseCause() throws Exception {
        Path valid = jar("scalar.jar", Map.of("mcmod.info", "[{\"modid\":\"scalar\",\"version\":\"3.8.4\"}]".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        EarlyDependencyResolver.verifyMetadata(valid, "scalar", "3.8.4");
        Path current = jar("scalar-3.9.jar", Map.of("mcmod.info", "[{\"modid\":\"scalar\",\"version\":\"3.9.0\"}]".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        EarlyDependencyResolver.verifyMetadata(current, "scalar", "3.9.0");
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () ->
            EarlyDependencyResolver.verifyMetadata(valid, "scalar", "3.8.4-1"));
        Path malformed = jar("bad.jar", Map.of("mcmod.info", "{".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        var cause = assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () ->
            EarlyDependencyResolver.verifyMetadata(malformed, "scalar", "3.8.4"));
        assertNotNull(cause.getCause());
        assertTrue(cause.getCause().getStackTrace().length > 0);
    }
    @Test void realNestedPayloadManifestAndBytesMustBothMatch() throws Exception {
        byte[] payload = new byte[]{4, 5, 6};
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("NonModDeps", "true");
        manifest.getMainAttributes().putValue("ContainedDeps", "payload.jar");
        Path outer = directory.resolve("outer.jar");
        try (var out = new JarOutputStream(Files.newOutputStream(outer), manifest)) {
            out.putNextEntry(new JarEntry("payload.jar")); out.write(payload); out.closeEntry();
        }
        String hash = EarlyDependencyResolver.sha256(new ByteArrayInputStream(payload));
        EarlyDependencyResolver.verifyContained(outer, EarlyDependencyResolver.hashFile(outer),
            List.of(new EarlyDependencyResolver.Spec("payload.jar", hash, List.of())), Set.of());
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () ->
            EarlyDependencyResolver.verifyContained(outer, EarlyDependencyResolver.hashFile(outer),
                List.of(new EarlyDependencyResolver.Spec("payload.jar", "0".repeat(64), List.of())), Set.of()));
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () ->
            EarlyDependencyResolver.verifyContained(outer, EarlyDependencyResolver.hashFile(outer),
                List.of(new EarlyDependencyResolver.Spec("other.jar", hash, List.of())), Set.of()));
    }

    @Test void scalarRuntimeVersionMeetsMinimumAndNamesItsIndependentlyPinnedLibrary() throws Exception {
        for (String accepted : List.of("3.8.4", "3.9.0", "3.10.0", "4.0.0")) {
            byte[] library = new byte[]{31}; byte[] parser = new byte[]{32};
            Path outer = nestedBundle("scalar-" + accepted + ".jar",
                Map.of("scala-library-" + accepted + ".jar", library,
                    "scala-parser-combinators_3-2.4.0.jar", parser),
                "scala-library-" + accepted + ".jar scala-parser-combinators_3-2.4.0.jar");
            EarlyDependencyResolver.verifyScalarContained(outer, scalarPins(outer, accepted, library, parser));
        }
        for (String rejected : List.of("3.8.3", "03.9.0", "3.9", "3.9.0-1", "3.9.0+build", ""))
            assertThrows(IllegalArgumentException.class, () -> new EarlyDependencyResolver.ScalarPins(
                "1".repeat(64), rejected, "2".repeat(64), "3".repeat(64)));
    }

    @Test void scalarVerifierAcceptsLegacyPairAndPinnedVersionMatchedOptionalTastyInspector() throws Exception {
        byte[] library = new byte[]{11, 12};
        byte[] parser = new byte[]{13, 14};
        byte[] tasty = new byte[]{15, 16};
        for (boolean includeTasty : List.of(false, true)) {
            Map<String, byte[]> nested = new java.util.LinkedHashMap<>();
            nested.put("scala-library-3.9.0.jar", library);
            nested.put("scala-parser-combinators_3-2.4.0.jar", parser);
            if (includeTasty) nested.put("scala3-tasty-inspector_3-3.9.0.jar", tasty);
            String declared = String.join(" ", nested.keySet());
            Path outer = nestedBundle("scalar-bundle-" + includeTasty + ".jar", nested, declared);
            var pins = new EarlyDependencyResolver.ScalarPins(EarlyDependencyResolver.hashFile(outer), "3.9.0",
                EarlyDependencyResolver.sha256(new ByteArrayInputStream(library)),
                EarlyDependencyResolver.sha256(new ByteArrayInputStream(parser)));
            EarlyDependencyResolver.verifyScalarContained(outer, pins);

            var wrongLibrary = new EarlyDependencyResolver.ScalarPins(pins.outerSha256(), "3.9.0",
                "0".repeat(64), pins.parserSha256());
            assertThrows(EarlyDependencyResolver.DependencyUnavailable.class,
                () -> EarlyDependencyResolver.verifyScalarContained(outer, wrongLibrary));
            var wrongOuter = new EarlyDependencyResolver.ScalarPins("0".repeat(64), "3.9.0",
                pins.librarySha256(), pins.parserSha256());
            assertThrows(EarlyDependencyResolver.DependencyUnavailable.class,
                () -> EarlyDependencyResolver.verifyScalarContained(outer, wrongOuter));
        }
    }

    @Test void scalarVerifierRejectsWrongOrUnmanifestedOptionalPayloadAndMissingPinnedPayload() throws Exception {
        byte[] library = new byte[]{21};
        byte[] parser = new byte[]{22};
        Path wrongTasty = nestedBundle("wrong-tasty.jar", Map.of("scala-library-3.9.0.jar", library,
            "scala-parser-combinators_3-2.4.0.jar", parser, "scala3-tasty-inspector_3-3.8.4.jar", new byte[]{23}),
            "scala-library-3.9.0.jar scala-parser-combinators_3-2.4.0.jar scala3-tasty-inspector_3-3.8.4.jar");
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class,
            () -> EarlyDependencyResolver.verifyScalarContained(wrongTasty, scalarPins(wrongTasty,"3.9.0",library,parser)));

        Path unknown = nestedBundle("unknown-payload.jar", Map.of("scala-library-3.9.0.jar", library,
            "scala-parser-combinators_3-2.4.0.jar", parser, "other.jar", new byte[]{24}),
            "scala-library-3.9.0.jar scala-parser-combinators_3-2.4.0.jar other.jar");
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class,
            () -> EarlyDependencyResolver.verifyScalarContained(unknown, scalarPins(unknown,"3.9.0",library,parser)));

        Path unmanifested = nestedBundle("unmanifested-payload.jar", Map.of("scala-library-3.9.0.jar", library,
            "scala-parser-combinators_3-2.4.0.jar", parser, "scala3-tasty-inspector_3-3.9.0.jar", new byte[]{25}),
            "scala-library-3.9.0.jar scala-parser-combinators_3-2.4.0.jar");
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class,
            () -> EarlyDependencyResolver.verifyScalarContained(unmanifested, scalarPins(unmanifested,"3.9.0",library,parser)));

        Path missingParser = nestedBundle("missing-parser.jar", Map.of("scala-library-3.9.0.jar", library),
            "scala-library-3.9.0.jar");
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class,
            () -> EarlyDependencyResolver.verifyScalarContained(missingParser, scalarPins(missingParser,"3.9.0",library,parser)));

        Path changedAfterPin = nestedBundle("changed-after-pin.jar", Map.of("scala-library-3.9.0.jar", library,
            "scala-parser-combinators_3-2.4.0.jar", parser),
            "scala-library-3.9.0.jar scala-parser-combinators_3-2.4.0.jar");
        var originalPin = scalarPins(changedAfterPin,"3.9.0",library,parser);
        Files.write(changedAfterPin, new byte[]{0}, java.nio.file.StandardOpenOption.APPEND);
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class,
            () -> EarlyDependencyResolver.verifyScalarContained(changedAfterPin, originalPin));
    }
    @Test void missingExplicitScalarPinsIsStickyUnavailableWithoutDiscoveryOrAssembly() throws Exception {
        var resolver = new EarlyDependencyResolver(Thread.currentThread(),
            new EarlyDependencyResolver.ExpectedPins(EarlyDependencyResolver.SQL_OUTER, Optional.empty()));
        var unavailable = assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () -> resolver.resolve(noDiscovery()));
        assertEquals("explicit Scalar artifact pins unavailable", unavailable.getMessage());
        assertEquals("dependency assembly already attempted",
            assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () -> resolver.resolve(noDiscovery())).getMessage());
    }
    @Test void wrongBootThreadDoesNotConsumeValidOwnerAttempt() throws Exception {
        var resolver = new EarlyDependencyResolver(Thread.currentThread(),
            new EarlyDependencyResolver.ExpectedPins(EarlyDependencyResolver.SQL_OUTER, Optional.empty()));
        var failure = new AtomicReference<Throwable>();
        Thread wrong = new Thread(() -> {
            try {
                assertEquals("requires exact boot owner",
                    assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () -> resolver.resolve(noDiscovery())).getMessage());
            } catch (Throwable cause) { failure.set(cause); }
        }, "wrong-early-dependency-owner");
        try { wrong.start(); wrong.join(5000); assertFalse(wrong.isAlive()); }
        finally { if (wrong.isAlive()) { wrong.interrupt(); wrong.join(5000); assertFalse(wrong.isAlive()); } }
        if (failure.get() != null) throw new AssertionError("wrong owner test failed", failure.get());
        assertEquals("explicit Scalar artifact pins unavailable",
            assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () -> resolver.resolve(noDiscovery())).getMessage());
    }
    private static final String FUGUE = "com/cleanroommc/fugue/common/Fugue.class";
    private static final String FUGUE_PLUGIN = "com/cleanroommc/fugue/common/FugueLoadingPlugin.class";
    private EarlyDependencyResolver.ScalarPins scalarPins() {
        return new EarlyDependencyResolver.ScalarPins("1".repeat(64), "3.8.4", "2".repeat(64), "3".repeat(64));
    }
    private Path fugueJar(String name, String metadata) throws IOException {
        return jar(name, Map.of("mcmod.info", metadata.getBytes(java.nio.charset.StandardCharsets.UTF_8),
            FUGUE, new byte[]{11}, FUGUE_PLUGIN, new byte[]{12}));
    }
    @Test void missingFuguePinsCompatConstructorFailsBeforeDiscoveryAndStaysAttempted() throws Exception {
        var resolver = new EarlyDependencyResolver(Thread.currentThread(),
            new EarlyDependencyResolver.ExpectedPins(EarlyDependencyResolver.SQL_OUTER, Optional.of(scalarPins())));
        assertEquals("explicit Fugue artifact pins unavailable",
            assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () -> resolver.resolve(noDiscovery())).getMessage());
        assertEquals("dependency assembly already attempted",
            assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () -> resolver.resolve(noDiscovery())).getMessage());
    }
    @Test void wrongOwnerCannotConsumeMissingFugueAttempt() throws Exception {
        var resolver = new EarlyDependencyResolver(Thread.currentThread(),
            new EarlyDependencyResolver.ExpectedPins(EarlyDependencyResolver.SQL_OUTER, Optional.of(scalarPins()), Optional.empty()));
        var failure = new AtomicReference<Throwable>();
        Thread wrong = new Thread(() -> {
            try {
                assertEquals("requires exact boot owner",
                    assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () -> resolver.resolve(noDiscovery())).getMessage());
            } catch (Throwable cause) { failure.set(cause); }
        }, "wrong-fugue-gate-owner");
        try { wrong.start(); wrong.join(5000); assertFalse(wrong.isAlive()); }
        finally { if (wrong.isAlive()) { wrong.interrupt(); wrong.join(5000); assertFalse(wrong.isAlive()); } }
        if (failure.get() != null) throw new AssertionError("wrong Fugue owner test failed", failure.get());
        assertEquals("explicit Fugue artifact pins unavailable",
            assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () -> resolver.resolve(noDiscovery())).getMessage());
    }
    @Test void fugueCallerVersionIsExactUpgradeableAndMetadataIsUniqueString() throws Exception {
        Path p = fugueJar("future-fugue.jar", "[{\"modid\":\"fugue\",\"version\":\"0.99.1\"}]");
        var pins = new EarlyDependencyResolver.FuguePins(EarlyDependencyResolver.hashFile(p), "0.99.1");
        EarlyDependencyResolver.verifyMetadata(p, "fugue", pins.runtimeVersion());
        var plan = EarlyDependencyResolver.verifySnapshot(snapshot(List.of(p), List.of(), List.of()),
            List.of(spec(p, FUGUE, FUGUE_PLUGIN)));
        assertEquals(p.toRealPath(), plan.archives().getFirst().path());
        assertEquals(" 0.99.1 ", new EarlyDependencyResolver.FuguePins(pins.outerSha256(), " 0.99.1 ").runtimeVersion());
        assertThrows(IllegalArgumentException.class, () -> new EarlyDependencyResolver.FuguePins(pins.outerSha256(), " "));
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () -> EarlyDependencyResolver.verifyMetadata(p, "fugue", " 0.99.1 "));
        for (String text : List.of("[{\"modid\":\"other\",\"version\":\"0.99.1\"}]",
                "[{\"modid\":\"fugue\",\"version\":99}]",
                "[{\"modid\":99,\"version\":\"0.99.1\"}]",
                "[{\"modid\":\"fugue\",\"version\":\"0.99.1\"},{\"modid\":\"fugue\",\"version\":\"0.99.1\"}]")) {
            Path wrong = fugueJar("metadata-" + Math.abs(text.hashCode()) + ".jar", text);
            assertThrows(EarlyDependencyResolver.DependencyUnavailable.class,
                () -> EarlyDependencyResolver.verifyMetadata(wrong, "fugue", "0.99.1"));
        }
        Path malformed = fugueJar("malformed-fugue.jar", "{");
        var error = assertThrows(EarlyDependencyResolver.DependencyUnavailable.class,
            () -> EarlyDependencyResolver.verifyMetadata(malformed, "fugue", "0.99.1"));
        assertNotNull(error.getCause());
        assertTrue(error.getCause().getStackTrace().length > 0);
    }
    @Test void actualResolveRequiresUniqueDiscoveredFugueBeforeAnyUrlAssembly() throws Exception {
        Path dummy = jar("discovered-other.jar", Map.of("other", new byte[]{1}));
        Path p = fugueJar("discovered-fugue.jar", "[{\"modid\":\"fugue\",\"version\":\"0.99.1\"}]");
        Path second = directory.resolve("second-discovered-fugue.jar"); Files.copy(p, second);
        for (List<Path> sources : List.of(List.<Path>of(), List.of(p, second))) {
            var resolver = new EarlyDependencyResolver(Thread.currentThread(),
                new EarlyDependencyResolver.ExpectedPins(EarlyDependencyResolver.SQL_OUTER, Optional.of(scalarPins()),
                    Optional.of(new EarlyDependencyResolver.FuguePins(EarlyDependencyResolver.hashFile(p), "0.99.1"))));
            var discovery = new EarlyDependencyResolver.Discovery() {
                @Override public EarlyDependencyResolver.Snapshot snapshot(List<String> anchors) {
                    assertTrue(anchors.contains(FUGUE)); assertTrue(anchors.contains(FUGUE_PLUGIN));
                    return new EarlyDependencyResolver.Snapshot(List.of(dummy), List.of(dummy), sources,
                        List.of(dummy, p, second), List.of(), List.of());
                }
                @Override public void addURL(URL url) { fail("all sources must verify before first addURL"); }
                @Override public List<EarlyDependencyResolver.ResourceOrigin> resources(List<String> names) { throw new AssertionError("must not reach post assembly"); }
            };
            assertEquals("requires unique discovered Fugue outer",
                assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () -> resolver.resolve(discovery)).getMessage());
            assertEquals("dependency assembly already attempted",
                assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () -> resolver.resolve(discovery)).getMessage());
        }
    }
    @Test void partialOrWrongHashFuguePreventsEntireArchivePlan() throws Exception {
        Path first = jar("first-approved.jar", Map.of("other/Anchor.class", new byte[]{1}));
        Path partial = jar("partial-fugue.jar", Map.of(FUGUE, new byte[]{11}));
        Path whole = fugueJar("whole-fugue.jar", "[{\"modid\":\"fugue\",\"version\":\"0.99.1\"}]");
        var partialSpec = spec(partial, FUGUE, FUGUE_PLUGIN);
        var wrongHash = new EarlyDependencyResolver.Spec("Fugue outer", "0".repeat(64), List.of(FUGUE, FUGUE_PLUGIN));
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () ->
            EarlyDependencyResolver.verifySnapshot(snapshot(List.of(first, partial), List.of(), List.of()),
                List.of(spec(first, "other/Anchor.class"), partialSpec)));
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () ->
            EarlyDependencyResolver.verifySnapshot(snapshot(List.of(first, whole), List.of(), List.of()),
                List.of(spec(first, "other/Anchor.class"), wrongHash)));
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () ->
            EarlyDependencyResolver.verifySnapshot(snapshot(List.of(first), List.of(), List.of()),
                List.of(spec(first, "other/Anchor.class"), spec(whole, FUGUE, FUGUE_PLUGIN))));
    }
    @Test void actualFugueResourcesKeepParentDuplicatesAndWrongVisibleOrigin() throws Exception {
        Path p = fugueJar("visible-fugue.jar", "[{\"modid\":\"fugue\",\"version\":\"0.99.1\"}]");
        Path other = fugueJar("parent-fugue.jar", "[{\"modid\":\"fugue\",\"version\":\"0.99.2\"}]");
        try (var parent = new URLClassLoader(new URL[]{other.toUri().toURL()}, null);
             var child = new URLClassLoader(new URL[]{p.toUri().toURL()}, parent)) {
            var resources = EarlyDependencyResolver.inspectResources(child, List.of(FUGUE, FUGUE_PLUGIN));
            assertEquals(4, resources.size());
            assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () ->
                EarlyDependencyResolver.verifySnapshot(snapshot(List.of(p), List.of(p), resources),
                    List.of(spec(p, FUGUE, FUGUE_PLUGIN))));
        }
        try (var loader = new URLClassLoader(new URL[]{other.toUri().toURL()}, null)) {
            var resources = EarlyDependencyResolver.inspectResources(loader, List.of(FUGUE, FUGUE_PLUGIN));
            assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () ->
                EarlyDependencyResolver.verifySnapshot(snapshot(List.of(p), List.of(p), resources),
                    List.of(spec(p, FUGUE, FUGUE_PLUGIN))));
        }
        try (var loader = new URLClassLoader(new URL[]{p.toUri().toURL()}, null)) {
            var resources = EarlyDependencyResolver.inspectResources(loader, List.of(FUGUE, FUGUE_PLUGIN));
            var plan = EarlyDependencyResolver.verifySnapshot(snapshot(List.of(p), List.of(p), resources),
                List.of(spec(p, FUGUE, FUGUE_PLUGIN)));
            assertTrue(plan.additions().isEmpty());
            EarlyDependencyResolver.validateVisible(resources, plan.archives(), true);
        }
    }
    private EarlyDependencyResolver.Discovery noDiscovery() {
        return new EarlyDependencyResolver.Discovery() {
            @Override public EarlyDependencyResolver.Snapshot snapshot(List<String> resources) { throw new AssertionError("must fail before discovery"); }
            @Override public void addURL(URL url) { throw new AssertionError("must not assemble"); }
            @Override public List<EarlyDependencyResolver.ResourceOrigin> resources(List<String> resources) { throw new AssertionError("must fail before resources"); }
        };
    }
    private URL syntheticLaunchSource(String text) throws IOException {
        return new URL(null, text, new java.net.URLStreamHandler() {
            @Override protected java.net.URLConnection openConnection(URL url) {
                throw new AssertionError("inventory conversion must not open a source");
            }
        });
    }
    @Test void forgeAsmgenRootDoesNotBecomeAnArchiveOrRejectTheLaunchInventory() throws Exception {
        URL file = directory.resolve("dependency.jar").toUri().toURL();
        // Exact root shape constructed by the installed Cleanroom ASMTransformerWrapper.
        URL generated = syntheticLaunchSource("asmgen:/");
        assertEquals(List.of(Path.of(file.toURI())),
            EarlyDependencyResolver.localLaunchSources(List.of(file, generated)));
    }
    @Test void nonlocalSourcesAndNonRootAsmgenFormsRemainRejectedWithoutOpeningThem() throws Exception {
        for (String text : List.of("https://invalid.example/dependency.jar?secret=HIDDEN",
                "asmgen:/dependency.class", "asmgen://host/", "asmgen:/?query", "asmgen:/#fragment")) {
            var failure = assertThrows(EarlyDependencyResolver.DependencyUnavailable.class,
                () -> EarlyDependencyResolver.localLaunchSources(List.of(syntheticLaunchSource(text))));
            assertFalse(failure.getMessage().contains("HIDDEN"));
        }
    }
    @Test void generatedDependencyResourceStillFailsTheOriginalResourceProvenanceCheck() throws Exception {
        URL generated = new URL(null, "asmgen:/scala/Predef.class", new java.net.URLStreamHandler() {
            @Override protected java.net.URLConnection openConnection(URL url) {
                return new java.net.URLConnection(url) {
                    @Override public void connect() { throw new AssertionError("must not connect"); }
                };
            }
        });
        ClassLoader loader = new ClassLoader(null) {
            @Override public java.util.Enumeration<URL> getResources(String name) {
                return java.util.Collections.enumeration(List.of(generated));
            }
        };
        assertThrows(EarlyDependencyResolver.DependencyUnavailable.class,
            () -> EarlyDependencyResolver.inspectResources(loader, List.of("scala/Predef.class")));
    }
    @Test void localJarRootAndResourceViewsResolveToTheirActualArchive() throws Exception {
        Path archive = jar("local source # % space.jar", Map.of("sample/Anchor.class", new byte[]{1}));
        URL root = new URL("jar:" + archive.toUri().toASCIIString() + "!/");
        try (var loader = new URLClassLoader(new URL[]{archive.toUri().toURL()}, null)) {
            URL resource = loader.getResource("sample/Anchor.class");
            assertNotNull(resource);
            assertEquals(List.of(archive, archive), EarlyDependencyResolver.localLaunchSources(List.of(root, resource)));
            var actual = EarlyDependencyResolver.inspectResources(loader, List.of("sample/Anchor.class"));
            var plan = EarlyDependencyResolver.verifySnapshot(
                snapshot(List.of(archive), EarlyDependencyResolver.localLaunchSources(List.of(root)), actual),
                List.of(spec(archive, "sample/Anchor.class")));
            assertTrue(plan.additions().isEmpty());
            EarlyDependencyResolver.validateVisible(actual, plan.archives(), true);
            var wrong = new EarlyDependencyResolver.Spec("wrong archive pin", "0".repeat(64), List.of("sample/Anchor.class"));
            assertThrows(EarlyDependencyResolver.DependencyUnavailable.class, () ->
                EarlyDependencyResolver.verifySnapshot(snapshot(List.of(),
                    EarlyDependencyResolver.localLaunchSources(List.of(root)), actual), List.of(wrong)));
        }
    }
    @Test void jarSourcesNeverOpenRemoteNestedOrAmbiguousArchiveLocations() throws Exception {
        for (String text : List.of("jar:https://invalid.example/private.jar?secret=HIDDEN!/",
                "jar:jar:file:/outer.jar!/inner.jar!/", "jar:file:/outer.jar!/inner.jar!/",
                "jar:file://remote/outer.jar!/", "jar:file:/outer.jar?secret=HIDDEN!/",
                "jar:file:/outer.jar!/?secret=HIDDEN", "jar:file:/outer.jar!/#fragment",
                "jar:file:/outer.jar")) {
            var failure = assertThrows(EarlyDependencyResolver.DependencyUnavailable.class,
                () -> EarlyDependencyResolver.localLaunchSources(List.of(syntheticLaunchSource(text))));
            assertFalse(failure.getMessage().contains("HIDDEN"));
        }
    }
}

