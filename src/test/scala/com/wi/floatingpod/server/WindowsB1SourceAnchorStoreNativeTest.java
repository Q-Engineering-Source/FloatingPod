package com.wi.floatingpod.server;

import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

/** 未执行 native 候选；root 必须提供冻结 child Java25/classpath 才能运行。 */
@Tag("b1-anchor-native")
class WindowsB1SourceAnchorStoreNativeTest {
    @TempDir Path temporary;
    private Path fixture(String name) throws Exception {
        Path root = Files.createDirectory(temporary.resolve(name)); B1SourceAnchorFixture.writeMetadata(root); return root;
    }
    private static String child(String mode, Path root) throws Exception {
        String java = System.getProperty("fp.b1.anchor.child.java");
        String classpath = System.getProperty("fp.b1.anchor.child.classpath");
        if (java == null || classpath == null || !Path.of(java).isAbsolute() || !Files.isRegularFile(Path.of(java)) || classpath.isBlank())
            throw new IOException("Root must supply actual Java25 executable and reviewed frozen child classpath");
        if (mode.equals("resume-first-refusal")) {
            try (var fixture = RetainedNativeFixtureSupervisor.start(java, classpath, Child.class, root)) {
                return fixture.finish(mode);
            }
        }
        Process process = new ProcessBuilder(java, "--enable-native-access=ALL-UNNAMED", "-cp", classpath,
            Child.class.getName(), mode, root.toAbsolutePath().toString()).redirectErrorStream(true).start();
        try {
            process.getOutputStream().close();
            if (!process.waitFor(20, TimeUnit.SECONDS)) throw new IOException("Anchor native child exceeded bounded execution window");
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(0, process.exitValue(), "Native child failed: " + output); return output;
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                if (!process.waitFor(5, TimeUnit.SECONDS)) throw new IOException("Anchor native child did not exit after bounded cleanup");
            }
        }
    }
    @Test void actualFreshPublicationAndExactRebarrierPreserveCanonicalIdentity() throws Exception {
        Path root = fixture("fresh"); assertTrue(child("fresh-replay", root).contains("NATIVE_ANCHOR_REPLAY=true"));
        assertTrue(Files.isRegularFile(root.resolve("config/runtime.properties.b1-source-v4")));
        assertFalse(Files.exists(root.resolve("config/runtime.properties.b1-source-v4.tmp")));
    }
    @Test void actualConflictingCanonicalIsPreservedWithoutAck() throws Exception {
        assertTrue(child("conflict", fixture("conflict")).contains("REFUSED_CANONICAL_CONFLICT=true"));
    }
    @Test void actualUnknownTemporaryIsRetainedAndCannotPublish() throws Exception {
        Path root = fixture("temporary"); assertTrue(child("temporary", root).contains("REFUSED_TEMPORARY=true"));
        assertArrayEquals(new byte[]{1,2,3}, Files.readAllBytes(root.resolve("config/runtime.properties.b1-source-v4.tmp")));
        assertFalse(Files.exists(root.resolve("config/runtime.properties.b1-source-v4")));
    }
    @Test void actualMetadataChangeAfterCaptureFailsWithoutAnchor() throws Exception {
        Path root = fixture("metadata"); assertTrue(child("metadata-change", root).contains("REFUSED_METADATA_CHANGE=true"));
        assertFalse(Files.exists(root.resolve("config/runtime.properties.b1-source-v4")));
    }
    @Test void actualConfigWithinSourceWorldIsRejected() throws Exception {
        assertTrue(child("inside-world", fixture("inside")).contains("REFUSED_CONFIG_INSIDE_WORLD=true"));
    }
    @Test void actualWrongOwnedThreadFailsBeforeNativeWork() throws Exception {
        assertTrue(child("wrong-thread", fixture("wrong-thread")).contains("REFUSED_WRONG_THREAD=true"));
    }
    @Test @Tag("b1-anchor-restart") void sourceAnchorAbsentAfterActualWorldCreationIsProvisioningIncomplete() throws Exception {
        Path root = fixture("incomplete"); assertTrue(child("partial-preanchor", root).contains("CREATED_WITHOUT_ANCHOR=true"));
        assertTrue(child("read-incomplete", root).contains("PROVISIONING_INCOMPLETE=true"));
    }
    @Test @Tag("b1-anchor-restart") void successorReadsOriginalExpectedPinsAndRebarriersAnchorWithoutLearningFromWorld() throws Exception {
        Path root = fixture("restart"); assertTrue(child("publish-exit", root).contains("PUBLISHED_BEFORE_EXIT=true"));
        assertTrue(child("read-restart", root).contains("RESTART_EXPECTED_SOURCE=true"));
    }
    @Test @Tag("b1-anchor-restart") void actualResumeResultCannotIssueFirstCreationAnchor() throws Exception {
        Path root = fixture("resume-first");
        assertTrue(child("resume-first-refusal", root).contains("REFUSED_RESUME_FIRST_ISSUER=true"));
        assertFalse(Files.exists(root.resolve("config/runtime.properties.b1-source-v4")));
    }
    @Test @Tag("b1-anchor-restart") void changedWorldReceiptCannotReplaceAnchorExpectedSourcePins() throws Exception {
        Path root = fixture("world-tamper"); child("publish-exit", root);
        byte[] receipt = Files.readAllBytes(root.resolve("world/" + WindowsWorldProvisioner.RECEIPT_NAME)); receipt[40] ^= 1;
        Files.write(root.resolve("world/" + WindowsWorldProvisioner.RECEIPT_NAME), receipt);
        assertTrue(child("read-world-conflict", root).contains("REFUSED_WORLD_SOURCE_CHANGE=true"));
        assertArrayEquals(receipt, Files.readAllBytes(root.resolve("world/" + WindowsWorldProvisioner.RECEIPT_NAME)));
    }
    @Test void actualParentReplacementAndMetadataReplacementAreRejected() throws Exception {
        assertTrue(child("parent-replace", fixture("parent-replace")).contains("REFUSED_PARENT_REPLACEMENT=true"));
        assertTrue(child("metadata-replace", fixture("metadata-replace")).contains("REFUSED_METADATA_REPLACEMENT=true"));
    }
    @Test void actualFormat3PublishedAndReadExpectedInputReturnsCanonicalBarrierNamespace() throws Exception {
        Path root = fixture("format3-namespace");
        Path metadata = root.resolve("config/runtime.properties");
        String properties = Files.readString(metadata, StandardCharsets.UTF_8)
            .replace("approvedSchemaFormat=2", "approvedSchemaFormat=3")
            .replace(Format3MetadataAnchorTest.C2, Format3MetadataAnchorTest.C3)
            + "format3RuntimeContractPath=" + root.resolve("format3-contract.json").toAbsolutePath().toString().replace('\\', '/') + "\n"
            + "format3RuntimeContractSha256=" + "d".repeat(64) + "\n";
        Files.writeString(metadata, properties, StandardCharsets.UTF_8);
        assertTrue(child("format3-namespace", root).contains("NATIVE_FORMAT3_ANCHOR_NAMESPACE=true"));
    }
    /** 隔离测试进程正常返回会终止其进程生命周期；不声称 IOExit proof 或断电证据。 */
    public static final class Child {
        private static void retainedResumeFirst(Path root, String java, String cp) throws Exception {
            WindowsMarkerIo io = new WindowsMarkerIo(); var store = new WindowsB1SourceAnchorStore(io);
            Path metadata = root.resolve("config/runtime.properties");
            var location = store.captureLocation(metadata, WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(MySqlRuntimeMetadata.load(metadata)), UUID.randomUUID());
            var provisioner = new WindowsWorldProvisioner(io);
            try (var creator = RetainedNativeFixtureSupervisor.exitedCreator(io, java, cp, Child.class, "partial-hold", root)) {
                RetainedNativeFixtureSupervisor.signal(root, ".retained-supervisor-ready");
                String mode = RetainedNativeFixtureSupervisor.nextMode();
                if (mode == null) return;
                if (!mode.equals("resume-first-refusal")) throw new IllegalArgumentException("Exact anchor retained mode required");
                byte[] original = Files.readAllBytes(root.resolve("world/" + WindowsWorldProvisioner.RECEIPT_NAME));
                var resumed = creator.resume(provisioner, root.resolve("world"), B1SourceAnchorFixture.plan(),
                    WindowsWorldProvisioner.sha256(original), new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(), 2));
                assertFalse(resumed.createdThisBoot());
                assertThrows(IOException.class, () -> store.publishCreatedSource(location, resumed));
                System.out.println("REFUSED_RESUME_FIRST_ISSUER=true");
            }
        }
        public static void main(String[] args) throws Exception {
            if (args.length == 4 && args[0].equals("retained-supervisor")) {
                retainedResumeFirst(Path.of(args[1]), args[2], args[3]); return;
            }
            if (args.length != 2) throw new IllegalArgumentException("Exact mode and fixture directory required");
            String mode = args[0]; Path root = Path.of(args[1]); Path metadataPath = root.resolve("config/runtime.properties");
            WindowsMarkerIo io = new WindowsMarkerIo(); var store = new WindowsB1SourceAnchorStore(io);
            var config = WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(MySqlRuntimeMetadata.load(metadataPath));
            var location = store.captureLocation(metadataPath, config, UUID.randomUUID());
            if (mode.equals("read-incomplete")) {
                assertThrows(WindowsB1SourceAnchorStore.ProvisioningIncomplete.class, () -> store.readExpectedSource(location));
                io.close(); System.out.println("PROVISIONING_INCOMPLETE=true"); return;
            }
            if (mode.equals("read-restart")) {
                var expected = store.readExpectedSource(location);
                assertEquals(B1SourceAnchorFixture.plan(), expected.expectedPlan());
                byte[] original = Files.readAllBytes(root.resolve("world/" + WindowsWorldProvisioner.RECEIPT_NAME));
                assertArrayEquals(original, expected.originalReceiptCanonicalBytes());
                assertEquals(WindowsWorldProvisioner.sha256(original), expected.expectedReceiptSha256());
                var ack = store.rebarrierExisting(location, expected);
                assertEquals(expected.anchorSha256(), ack.anchorSha256());
                // This reads world solely as test observation after expected pins have been loaded from anchor.
                io.close(); System.out.println("RESTART_EXPECTED_SOURCE=true"); return;
            }
            if (mode.equals("read-world-conflict")) {
                var expected = store.readExpectedSource(location);
                byte[] changed = Files.readAllBytes(root.resolve("world/" + WindowsWorldProvisioner.RECEIPT_NAME));
                assertNotEquals(WindowsWorldProvisioner.sha256(changed), expected.expectedReceiptSha256());
                byte[] guardBefore = Files.readAllBytes(root.resolve("world/" + WindowsWorldProvisioner.GUARD_NAME));
                assertThrows(IOException.class, () -> new WindowsWorldProvisioner(io).resumeExisting(root.resolve("world"),
                    expected.expectedPlan(), expected.expectedReceiptSha256(), new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(), 2)));
                assertArrayEquals(guardBefore, Files.readAllBytes(root.resolve("world/" + WindowsWorldProvisioner.GUARD_NAME)));
                io.close(); System.out.println("REFUSED_WORLD_SOURCE_CHANGE=true"); return;
            }
            if (mode.equals("parent-replace")) {
                Files.move(metadataPath.getParent(), root.resolve("original-config"));
                Files.createDirectory(metadataPath.getParent());
                Files.copy(root.resolve("original-config/runtime.properties"), metadataPath);
                assertThrows(IOException.class, () -> store.readExpectedSource(location));
                io.close(); System.out.println("REFUSED_PARENT_REPLACEMENT=true"); return;
            }
            if (mode.equals("metadata-replace")) {
                Files.move(metadataPath, metadataPath.resolveSibling("original.properties"));
                Files.copy(metadataPath.resolveSibling("original.properties"), metadataPath);
                assertThrows(IOException.class, () -> store.readExpectedSource(location));
                io.close(); System.out.println("REFUSED_METADATA_REPLACEMENT=true"); return;
            }
            if (mode.equals("wrong-thread")) {
                var failure = new AtomicReference<Throwable>();
                Thread wrong = new Thread(() -> { try { store.readExpectedSource(location); } catch (Throwable actual) { failure.set(actual); } });
                wrong.start(); wrong.join(5000); assertFalse(wrong.isAlive()); assertInstanceOf(IllegalStateException.class, failure.get());
                io.close(); System.out.println("REFUSED_WRONG_THREAD=true"); return;
            }
            if (!java.util.Set.of("fresh-replay", "conflict", "temporary", "metadata-change", "inside-world", "partial-preanchor", "partial-hold", "publish-exit", "format3-namespace").contains(mode))
                throw new IllegalArgumentException("Unknown anchor native fixture mode");
            var provisioner = new WindowsWorldProvisioner(io);
            var actualPlan = B1SourceAnchorFixture.plan();
            if (mode.equals("format3-namespace")) {
                var p = actualPlan;
                actualPlan = new WindowsWorldProvisioner.Plan(p.creationOperationUuid(), p.bootstrapOperationUuid(),
                    p.prepareWriteUuid(), p.certifyWriteUuid(), p.bindingUuid(), p.physicalFormat(),
                    p.mysqlServerUuid(), p.schema(), 3, p.ddlSha256(), p.provisioningUuid());
            }
            var created = provisioner.createFresh(root.resolve("world"), actualPlan,
                new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(), 1));
            if (mode.equals("partial-hold")) { RetainedNativeFixtureSupervisor.holdCreated(root, created); return; }
            if (mode.equals("partial-preanchor")) { System.out.println("CREATED_WITHOUT_ANCHOR=true"); return; }
            if (mode.equals("inside-world")) {
                Path inside = B1SourceAnchorFixture.writeMetadata(root.resolve("world"));
                var insideConfig = WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(MySqlRuntimeMetadata.load(inside));
                var insideLocation = store.captureLocation(inside, insideConfig, UUID.randomUUID());
                assertThrows(IOException.class, () -> store.publishCreatedSource(insideLocation, created));
                System.out.println("REFUSED_CONFIG_INSIDE_WORLD=true"); return;
            }
            if (mode.equals("temporary")) {
                Files.write(location.temporaryPath(), new byte[]{1,2,3});
                assertThrows(IOException.class, () -> store.publishCreatedSource(location, created));
                System.out.println("REFUSED_TEMPORARY=true"); return;
            }
            if (mode.equals("metadata-change")) {
                Files.writeString(metadataPath, B1SourceAnchorFixture.properties(metadataPath.getParent())
                    .replace("database.example.invalid", "other.example.invalid"), StandardCharsets.UTF_8);
                assertThrows(IOException.class, () -> store.publishCreatedSource(location, created));
                System.out.println("REFUSED_METADATA_CHANGE=true"); return;
            }
            var ack = store.publishCreatedSource(location, created);
            if (mode.equals("format3-namespace")) {
                var disk = store.readExpectedSource(location);
                String expected = B1SourceAnchorCodec.namespace(MySqlRuntimeMetadata.load(metadataPath));
                assertEquals(expected, ack.verifiedInput().barrierNamespace());
                assertEquals(expected, disk.barrierNamespace());
                assertEquals(actualPlan, disk.expectedPlan());
                System.out.println("NATIVE_FORMAT3_ANCHOR_NAMESPACE=true");
                // 原创建 custody 保留到当前 child 的实际进程生命周期结束。
                return;
            }
            if (mode.equals("publish-exit")) { System.out.println("PUBLISHED_BEFORE_EXIT=true"); return; }
            if (mode.equals("conflict")) {
                byte[] conflicting = Files.readAllBytes(location.canonicalPath()); conflicting[40] ^= 1;
                Files.write(location.canonicalPath(), conflicting);
                assertThrows(IOException.class, () -> store.publishCreatedSource(location, created));
                assertArrayEquals(conflicting, Files.readAllBytes(location.canonicalPath()));
                System.out.println("REFUSED_CANONICAL_CONFLICT=true"); return;
            }
            var again = store.publishCreatedSource(location, created);
            assertEquals(ack.fileIdentity(), again.fileIdentity()); assertEquals(ack.anchorSha256(), again.anchorSha256());
            var disk = store.readExpectedSource(location); var rebarrier = store.rebarrierExisting(location, disk);
            assertEquals(ack.fileIdentity(), rebarrier.fileIdentity()); assertEquals(ack.anchorSha256(), rebarrier.anchorSha256());
            assertEquals(created.freshProvisioningProof().creationProcessFileTime(),
                WindowsWorldProvisioner.decodeReceipt(disk.originalReceiptCanonicalBytes()).creatorCreationTime());
            System.out.println("NATIVE_ANCHOR_REPLAY=true");
            // Original custody remains owned for this child process lifetime; no early handle release.
        }
    }
}
