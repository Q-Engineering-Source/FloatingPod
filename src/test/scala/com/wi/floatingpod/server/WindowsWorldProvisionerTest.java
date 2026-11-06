package com.wi.floatingpod.server;

import java.io.IOException;
import com.wi.floatingpod.config.MySqlRuntimeMetadata;
import static com.wi.floatingpod.server.B1ResumeRecord.*;
import java.lang.foreign.MemoryLayout;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** 子进程使用 Root 审核的 Java 25 与完整 classpath，并通过参数文件启动。 */
class WindowsWorldProvisionerTest {
    @TempDir Path temporaryRoot;
    static final long TEST_FILETIME = 133800000000000123L;
    private static final int READINESS_DIAGNOSTIC_LIMIT_BYTES = 64 * 1024;
    static WindowsWorldProvisioner.Plan plan() {
        return new WindowsWorldProvisioner.Plan(UUID.fromString("0fc62f9f-7e40-4b7c-8c11-6c4446b4d80a"),
            UUID.fromString("5c7867c6-cb41-497b-b5fb-5a3ca8900924"), UUID.fromString("1be6d597-6a89-4e86-8881-247c17ad603d"),
            UUID.fromString("3189ce56-515d-4d01-a5e8-cac6492510ec"), UUID.fromString("3e97c5c5-140e-4663-9e8d-fef3c94082b0"),
            2, UUID.fromString("ed281a8f-ef7b-4b38-bb15-487971aa6210"), "fp_b1_native_fixture", 2, "AB".repeat(32),
            UUID.fromString("d79fdd2d-4a7d-49ee-ab00-d459b6982e9e"));
    }
    private static String utf16LeSha256(String value) throws Exception {
        return HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("SHA-256")
            .digest(value.getBytes(StandardCharsets.UTF_16LE)));
    }
    private static String expectedNativePathArgument(String absoluteNormalizedPath) {
        if (absoluteNormalizedPath.startsWith("\\\\?\\") || absoluteNormalizedPath.length() + 1 <= 260)
            return absoluteNormalizedPath;
        if (absoluteNormalizedPath.startsWith("\\\\")) return "\\\\?\\UNC\\" + absoluteNormalizedPath.substring(2);
        if (absoluteNormalizedPath.length() >= 3 && Character.isLetter(absoluteNormalizedPath.charAt(0))
            && absoluteNormalizedPath.charAt(1) == ':' && absoluteNormalizedPath.charAt(2) == '\\')
            return "\\\\?\\" + absoluteNormalizedPath;
        throw new IllegalArgumentException("Expected absolute Windows drive, UNC, or extended path");
    }
    private static WindowsWorldProvisioner.ReceiptData rawData() {
        var directory = new WindowsMarkerIo.FileIdentity(0x1234, new byte[16]); byte[] guardId = new byte[16]; guardId[15] = 1;
        return new WindowsWorldProvisioner.ReceiptData(plan(), directory.versionedIdentity(),
            new WindowsMarkerIo.FileIdentity(0x1234, guardId).versionedIdentity(), 1234, TEST_FILETIME,
            new WindowsWorldProvisioner.BootIdentity(UUID.fromString("74b00f82-f777-4b09-8b21-7fbc11c68317"), 1));
    }
    @Test void completeCanonicalReceiptRoundTripPreservesEveryPlanAndFullIdentityField() throws Exception {
        var raw = rawData(); byte[] encoded = WindowsWorldProvisioner.encodeReceipt(raw);
        assertEquals(raw, WindowsWorldProvisioner.decodeReceipt(encoded));
        assertArrayEquals(encoded, WindowsWorldProvisioner.encodeReceipt(WindowsWorldProvisioner.decodeReceipt(encoded)));
        assertTrue(encoded.length > 24); assertEquals(64, WindowsWorldProvisioner.sha256(encoded).length());
        assertEquals("FP_WIN32_FILE_ID_V1;volume=0000000000001234;id=00000000000000000000000000000000", raw.sourceIdentity());
        assertNotEquals(raw.sourceIdentity(), raw.guardIdentity());
    }
    private static byte[] withDigest(byte[] body) {
        byte[] encoded = Arrays.copyOf(body, body.length + 32);
        System.arraycopy(WindowsWorldProvisioner.digest(body), 0, encoded, body.length, 32); return encoded;
    }
    @Test void completeReceiptRejectsTamperingUnknownVersionTrailingFieldsAndMalformedUtf8() throws Exception {
        byte[] good = WindowsWorldProvisioner.encodeReceipt(rawData()), tampered = good.clone(); tampered[40] ^= 1;
        assertThrows(IOException.class, () -> WindowsWorldProvisioner.decodeReceipt(tampered));
        byte[] body = Arrays.copyOf(good, good.length - 32), version = body.clone(); version[11] = 2;
        assertThrows(IOException.class, () -> WindowsWorldProvisioner.decodeReceipt(withDigest(version)));
        assertThrows(IOException.class, () -> WindowsWorldProvisioner.decodeReceipt(withDigest(Arrays.copyOf(body, body.length + 1))));
        byte[] invalidText = body.clone(); invalidText[116] = (byte) 0xc0;
        var failure = assertThrows(IOException.class, () -> WindowsWorldProvisioner.decodeReceipt(withDigest(invalidText)));
        assertInstanceOf(CharacterCodingException.class, failure.getCause());
    }
    @Test void planRejectsSameWriteUuidUnsupportedFormatsAndCallerSuppliedWrongBinding() {
        var p = plan();
        assertThrows(IllegalArgumentException.class, () -> new WindowsWorldProvisioner.Plan(p.creationOperationUuid(), p.bootstrapOperationUuid(),
            p.prepareWriteUuid(), p.prepareWriteUuid(), p.bindingUuid(), 2, p.mysqlServerUuid(), p.schema(), 2, p.ddlSha256(), p.provisioningUuid()));
        assertThrows(IllegalArgumentException.class, () -> new WindowsWorldProvisioner.Plan(p.creationOperationUuid(), p.bootstrapOperationUuid(),
            p.prepareWriteUuid(), p.certifyWriteUuid(), p.bindingUuid(), 1, p.mysqlServerUuid(), p.schema(), 2, p.ddlSha256(), p.provisioningUuid()));
        assertThrows(IllegalArgumentException.class, () -> new WindowsWorldProvisioner.Plan(p.creationOperationUuid(), p.bootstrapOperationUuid(),
            p.prepareWriteUuid(), p.certifyWriteUuid(), new UUID(0, 0), 2, p.mysqlServerUuid(), p.schema(), 2, p.ddlSha256(), p.provisioningUuid()));
        assertThrows(IllegalArgumentException.class, () -> WindowsWorldProvisioner.hashBytes("00"));
        assertThrows(IOException.class, () -> WindowsWorldProvisioner.validateIdentityText("folder-name-or-path-hash"));
    }
    @Test void exactFileTimeConversionPreservesNativeHundredNanosecondsAndBounds() {
        Instant instant = WindowsMarkerIo.fileTimeInstant(TEST_FILETIME);
        assertEquals(12300, instant.getNano());
        assertEquals(TEST_FILETIME, (instant.getEpochSecond() + 11_644_473_600L) * 10_000_000L + instant.getNano() / 100);
        assertThrows(IllegalArgumentException.class, () -> WindowsMarkerIo.validateProcessIdentity(0, TEST_FILETIME));
        assertThrows(IllegalArgumentException.class, () -> WindowsMarkerIo.validateProcessIdentity(0x100000000L, TEST_FILETIME));
        assertThrows(IllegalArgumentException.class, () -> WindowsMarkerIo.payloadBound(65537));
    }
    @Test void javaProcessEntry32WLayoutMatchesExpectedX64Constants() {
        var layout = WindowsMarkerIo.PROCESSENTRY32W;
        assertEquals(568, layout.byteSize());
        assertEquals(0, layout.byteOffset(MemoryLayout.PathElement.groupElement("dwSize")));
        assertEquals(4, layout.byteOffset(MemoryLayout.PathElement.groupElement("cntUsage")));
        assertEquals(8, layout.byteOffset(MemoryLayout.PathElement.groupElement("th32ProcessID")));
        assertEquals(16, layout.byteOffset(MemoryLayout.PathElement.groupElement("th32DefaultHeapID")));
        assertEquals(24, layout.byteOffset(MemoryLayout.PathElement.groupElement("th32ModuleID")));
        assertEquals(28, layout.byteOffset(MemoryLayout.PathElement.groupElement("cntThreads")));
        assertEquals(32, layout.byteOffset(MemoryLayout.PathElement.groupElement("th32ParentProcessID")));
        assertEquals(36, layout.byteOffset(MemoryLayout.PathElement.groupElement("pcPriClassBase")));
        assertEquals(40, layout.byteOffset(MemoryLayout.PathElement.groupElement("dwFlags")));
        assertEquals(44, layout.byteOffset(MemoryLayout.PathElement.groupElement("szExeFile")));
        assertEquals(520, layout.select(MemoryLayout.PathElement.groupElement("szExeFile")).byteSize());
    }
    @Test void actualOpenProcessLiveOwnerNeverProducesPriorLifetimeEvidence() throws Exception {
        UUID operation = UUID.randomUUID();
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            WindowsMarkerIo.ProcessIdentity current = io.captureCurrentProcessIdentity(operation);
            IOException failure = assertThrows(IOException.class, () -> io.observePreviousProcessLifetime(
                current.pid(), current.creationFileTime(), operation));
            assertEquals("Previous owner process is still live; no restart evidence", failure.getMessage());
        }
    }
    @Test void actualCreationTimeMismatchIsTypedSeparatelyAndCannotBeClaimedAsTheObservedIdentity() throws Exception {
        UUID operation = UUID.randomUUID();
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            WindowsMarkerIo.ProcessIdentity current = io.captureCurrentProcessIdentity(operation);
            long deliberatelyUnpairedCreation = current.creationFileTime() - 10_000_000L;
            WindowsMarkerIo.PreviousProcessLifetimeObservation observation = io.observePreviousProcessLifetime(
                current.pid(), deliberatelyUnpairedCreation, operation);
            WindowsMarkerIo.ProcessIdReused reused = assertInstanceOf(WindowsMarkerIo.ProcessIdReused.class, observation);
            assertEquals(WindowsMarkerIo.PreviousProcessEvidenceKind.PROCESS_ID_REUSED, reused.kind());
            assertEquals(current.creationFileTime(), reused.observedCreationFileTime());
            assertEquals(258, reused.waitResult());
            assertNotEquals(current.creationFileTime(), reused.previousCreationFileTime());
            assertThrows(IOException.class, () -> io.claimPreviousProcessLifetimeObservation(
                reused, current.pid(), current.creationFileTime(), operation));
            // 此处的旧 FILETIME 仅用来验证 native 类型分流；没有 durable world owner，不能签发恢复权限。
        }
    }
    @Test void actualCurrentHandleRejectsFutureDurableCreationTimeWithoutLifetimeEvidence() throws Exception {
        UUID operation = UUID.randomUUID();
        try (WindowsMarkerIo io = new WindowsMarkerIo()) {
            WindowsMarkerIo.ProcessIdentity current = io.captureCurrentProcessIdentity(operation);
            long futureDurableCreation = Math.addExact(current.creationFileTime(), 10_000_000L);
            assertTrue(futureDurableCreation > current.creationFileTime());
            IOException failure = assertThrows(IOException.class, () -> io.observePreviousProcessLifetime(
                current.pid(), futureDurableCreation, operation));
            assertEquals("Observed process creation FILETIME predates durable previous owner; no lifetime evidence",
                failure.getMessage());
        }
    }
    @Test void actualToolhelpFallbackRequiresACompleteSnapshotAndReturnsOnlyTypedOsObservation() throws Exception {
        UUID operation = UUID.randomUUID();
        long absentPid = 0xFFFF_FFFEL;
        try (WindowsMarkerIo io = new WindowsMarkerIo(); WindowsMarkerIo foreignIo = new WindowsMarkerIo()) {
            WindowsMarkerIo.PreviousProcessLifetimeObservation observation = io.observePreviousProcessLifetime(
                absentPid, TEST_FILETIME, operation);
            WindowsMarkerIo.OldLifetimeEndedByCompleteSnapshot absent = assertInstanceOf(
                WindowsMarkerIo.OldLifetimeEndedByCompleteSnapshot.class, observation);
            assertEquals(WindowsMarkerIo.PreviousProcessEvidenceKind.OLD_LIFETIME_ABSENT_FROM_COMPLETE_SNAPSHOT,
                absent.kind());
            assertEquals(18, absent.terminalError());
            assertTrue(absent.enumeratedProcessCount() > 0);
            assertThrows(IOException.class, () -> foreignIo.claimPreviousProcessLifetimeObservation(
                observation, absentPid, TEST_FILETIME, operation));
            assertThrows(IOException.class, () -> io.claimPreviousProcessLifetimeObservation(
                observation, absentPid, TEST_FILETIME, UUID.randomUUID()));
            io.claimPreviousProcessLifetimeObservation(observation, absentPid, TEST_FILETIME, operation);
            assertThrows(IOException.class, () -> io.claimPreviousProcessLifetimeObservation(
                observation, absentPid, TEST_FILETIME, operation));
            io.requireClaimedPreviousProcessLifetimeObservation(observation, absentPid, TEST_FILETIME, operation);
            assertThrows(IOException.class, () -> io.requireClaimedPreviousProcessLifetimeObservation(
                observation, absentPid, TEST_FILETIME, UUID.randomUUID()));
            // 此低层 observation 本身不携带 world、guard、receipt 或 custody 权限。
        }
    }
    private static final class ChildProcess implements AutoCloseable {
        final JavaArgumentFileChildLauncher.StartedChild launched;
        final Process process;
        ChildProcess(JavaArgumentFileChildLauncher.StartedChild launched) {
            this.launched = launched;
            this.process = launched.process();
        }
        private void deleteArgumentFile(Throwable primary) throws IOException {
            try {
                launched.deleteArgumentFileAfterExit();
            } catch (IOException | RuntimeException | Error cleanup) {
                if (primary != null) primary.addSuppressed(cleanup);
                else throw cleanup;
            }
        }
        String finish() throws Exception {
            Throwable primary = null;
            try {
                process.getOutputStream().close();
                if (!process.waitFor(20, TimeUnit.SECONDS)) throw new IOException("Native child exceeded 20-second bound");
                String text = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                assertEquals(0, process.exitValue(), "Child failed: " + text); return text;
            } catch (Exception | Error failure) {
                primary = failure;
                throw failure;
            } finally {
                deleteArgumentFile(primary);
            }
        }
        void attachReadinessDiagnosticsAfterOwnedWait(Throwable primary) {
            try { close(); }
            catch (Throwable cleanup) { primary.addSuppressed(cleanup); }
            boolean alive;
            try { alive = process.isAlive(); }
            catch (RuntimeException | Error inspection) {
                primary.addSuppressed(inspection);
                primary.addSuppressed(new IOException("Owned native child exit state is unavailable after bounded close/wait"));
                return;
            }
            if (alive) {
                primary.addSuppressed(new IOException("Owned native child remains alive after bounded close/wait; exit/output unavailable"));
                return;
            }
            String exitCode = "UNKNOWN";
            try { exitCode = Integer.toString(process.exitValue()); }
            catch (RuntimeException | Error failure) { primary.addSuppressed(failure); }
            String output = "<unavailable>";
            try {
                byte[] captured = process.getInputStream().readNBytes(READINESS_DIAGNOSTIC_LIMIT_BYTES + 1);
                boolean truncated = captured.length > READINESS_DIAGNOSTIC_LIMIT_BYTES;
                if (truncated) captured = Arrays.copyOf(captured, READINESS_DIAGNOSTIC_LIMIT_BYTES);
                output = new String(captured, StandardCharsets.UTF_8)
                    + (truncated ? "<merged child output truncated after 65536 bytes>" : "");
            } catch (IOException | RuntimeException | Error failure) {
                primary.addSuppressed(failure);
            }
            primary.addSuppressed(new IOException("Owned native child diagnostics after bounded wait: exitCode="
                + exitCode + "; mergedStdoutStderr=" + output));
        }
        @Override public void close() throws Exception {
            Throwable primary = null;
            try {
                process.getOutputStream().close();
            } catch (Exception | Error failure) {
                primary = failure;
            }
            try {
                if (!process.waitFor(5, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    if (!process.waitFor(5, TimeUnit.SECONDS)) throw new IOException("Native child did not exit after bounded cleanup");
                }
            } catch (Exception | Error cleanup) {
                if (primary == null) primary = cleanup;
                else primary.addSuppressed(cleanup);
            }
            try {
                launched.deleteArgumentFileAfterExit();
            } catch (IOException | RuntimeException | Error cleanup) {
                if (primary == null) primary = cleanup;
                else primary.addSuppressed(cleanup);
            }
            if (primary instanceof Error error) throw error;
            if (primary instanceof Exception exception) throw exception;
        }
    }
    private static ChildProcess child(String mode, Path directory, String expectedSha) throws IOException {
        String java = System.getProperty("fp.b1.native.child.java"), classpath = System.getProperty("fp.b1.native.child.classpath");
        if (java == null || classpath == null || !Path.of(java).isAbsolute() || !Files.isRegularFile(Path.of(java)) || classpath.isBlank())
            throw new IOException("Root must supply reviewed actual Java25 executable and frozen child classpath");
        return child(mode, directory, expectedSha, java, classpath);
    }
    private static ChildProcess child(String mode, Path directory, String expectedSha, String javaExecutable, String classpath,
                                      String... extraArguments) throws IOException {
        if (javaExecutable == null || classpath == null || !Path.of(javaExecutable).isAbsolute() || !Files.isRegularFile(Path.of(javaExecutable)) || classpath.isBlank())
            throw new IOException("Root must supply reviewed actual Java25 executable and frozen child classpath");
        var programArguments = new ArrayList<String>();
        programArguments.add(mode);
        programArguments.add(directory.toAbsolutePath().toString());
        programArguments.add(expectedSha);
        if (mode.startsWith("supervisor-")) {
            if (extraArguments.length != 0) throw new IllegalArgumentException("Supervisor launcher owns its reviewed Java/classpath arguments");
            programArguments.add(javaExecutable);
            programArguments.add(classpath);
        }
        programArguments.addAll(List.of(extraArguments));
        var launched = JavaArgumentFileChildLauncher.start(Path.of(javaExecutable), classpath, Child.class.getName(),
            programArguments, Path.of(System.getProperty("java.io.tmpdir")).toAbsolutePath());
        return new ChildProcess(launched);
    }
    private String createAndExit(Path world) throws Exception {
        try (var process = child("fresh", world, "NONE")) {
            String text = process.finish(); assertTrue(text.contains("CREATED=true"));
            String receipt = Files.readString(world.resolve("test-result.sha"), StandardCharsets.US_ASCII);
            assertTrue(receipt.matches("[0-9A-F]{64}"));
            byte[] bytes = Files.readAllBytes(world.resolve(WindowsWorldProvisioner.RECEIPT_NAME));
            assertEquals(receipt, WindowsWorldProvisioner.sha256(bytes)); assertEquals(plan(), WindowsWorldProvisioner.decodeReceipt(bytes).plan());
            return receipt;
        }
    }
    @Test void actualNewDirectoryReceiptAndCrossProcessRestartRetainCompleteImmutableSource() throws Exception {
        Path world = temporaryRoot.resolve("new-world");
        String java = System.getProperty("fp.b1.native.child.java"), classpath = System.getProperty("fp.b1.native.child.classpath");
        try (var supervisor = child("supervisor-retained-resume", world, "NONE", java, classpath)) {
            assertTrue(supervisor.finish().contains("RETAINED_RESUME=true"));
        }
        String sha = Files.readString(world.resolve("test-result.sha"), StandardCharsets.US_ASCII);
        byte[] original = Files.readAllBytes(world.resolve("test-before-receipt.bin"));
        byte[] priorOwner = Files.readAllBytes(world.resolve("test-before-owner.bin"));
        assertArrayEquals(original, Files.readAllBytes(world.resolve(WindowsWorldProvisioner.RECEIPT_NAME)));
        assertFalse(Arrays.equals(priorOwner, Files.readAllBytes(world.resolve(WindowsWorldProvisioner.GUARD_NAME))));
        assertEquals(WindowsWorldProvisioner.OWNER_SIZE, Files.size(world.resolve(WindowsWorldProvisioner.GUARD_NAME)));
    }
    @Test void actualOrdinaryResumeInspectionUsesBoundNativePriorOwnerEvidenceWithoutMutation() throws Exception {
        Path world = temporaryRoot.resolve("ordinary-inspection");
        String sha = createAndExit(world);
        byte[] receipt = Files.readAllBytes(world.resolve(WindowsWorldProvisioner.RECEIPT_NAME));
        byte[] owner = Files.readAllBytes(world.resolve(WindowsWorldProvisioner.GUARD_NAME));
        try (var process = child("ordinary-inspection", world, sha)) {
            String output = process.finish();
            assertTrue(output.contains("ORDINARY_INSPECTION="));
            assertTrue(output.contains("EXACT_OPEN_PROCESS_HANDLE_EXIT")
                || output.contains("PID_REUSED_NOT_PROCESS_EXIT_ACKNOWLEDGEMENT")
                || output.contains("ABSENT_FROM_COMPLETE_PROCESS_SNAPSHOT_NOT_PROCESS_EXIT_ACKNOWLEDGEMENT"));
        }
        assertArrayEquals(receipt, Files.readAllBytes(world.resolve(WindowsWorldProvisioner.RECEIPT_NAME)));
        assertArrayEquals(owner, Files.readAllBytes(world.resolve(WindowsWorldProvisioner.GUARD_NAME)));
    }
    @Test void actualOrdinaryFourArgumentResumeChangesOnlyOwnerAfterPriorCreatorExit() throws Exception {
        Path world = temporaryRoot.resolve("ordinary-resume");
        String sha = createAndExit(world);
        byte[] receipt = Files.readAllBytes(world.resolve(WindowsWorldProvisioner.RECEIPT_NAME));
        byte[] priorOwner = Files.readAllBytes(world.resolve(WindowsWorldProvisioner.GUARD_NAME));
        try (var process = child("ordinary-resume", world, sha)) {
            assertTrue(process.finish().contains("ORDINARY_RESUME=true;generation=2"));
        }
        assertArrayEquals(receipt, Files.readAllBytes(world.resolve(WindowsWorldProvisioner.RECEIPT_NAME)));
        byte[] resumedOwner = Files.readAllBytes(world.resolve(WindowsWorldProvisioner.GUARD_NAME));
        assertEquals(WindowsWorldProvisioner.OWNER_SIZE, resumedOwner.length);
        assertFalse(Arrays.equals(priorOwner, resumedOwner));
    }
    @Test void actualExistingDirectoryCannotReceiveFreshReceipt() throws Exception {
        Path world = Files.createDirectory(temporaryRoot.resolve("existing"));
        try (var refused = child("existing-refusal", world, "NONE")) { assertTrue(refused.finish().contains("REFUSED_EXISTING=183")); }
        assertFalse(Files.exists(world.resolve(WindowsWorldProvisioner.RECEIPT_NAME)));
        assertFalse(Files.exists(world.resolve(WindowsWorldProvisioner.GUARD_NAME)));
    }
    @Test void actualShareZeroGuardAndLivePreviousProcessRejectSecondCustody() throws Exception {
        Path world = temporaryRoot.resolve("held-world");
        try (var held = child("hold", world, "NONE")) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            Path shaPath = world.resolve("test-result.sha");
            while (!Files.exists(shaPath) && held.process.isAlive() && System.nanoTime() < deadline) Thread.sleep(20);
            assertTrue(Files.exists(shaPath)); String sha = Files.readString(shaPath, StandardCharsets.US_ASCII);
            try (var refused = child("guard-refusal", world, sha)) { assertTrue(refused.finish().contains("REFUSED_GUARD=32")); }
            assertTrue(held.process.isAlive()); assertTrue(held.finish().contains("CREATED=true"));
        }
    }
    @Test void actualSameProcessRequiresRealIoExitAndCannotReacquire() throws Exception {
        try (var refused = child("same-process", temporaryRoot.resolve("same-process"), "NONE")) {
            assertTrue(refused.finish().contains("REFUSED_SAME_PROCESS=true"));
        }
    }
    @Test void actualSameNameCopiedReceiptAndGuardCannotAuthenticateNewDirectoryIdentity() throws Exception {
        Path firstParent = Files.createDirectory(temporaryRoot.resolve("a")), secondParent = Files.createDirectory(temporaryRoot.resolve("b"));
        Path first = firstParent.resolve("world"); String sha = createAndExit(first);
        Path second = Files.createDirectory(secondParent.resolve("world"));
        Files.copy(first.resolve(WindowsWorldProvisioner.RECEIPT_NAME), second.resolve(WindowsWorldProvisioner.RECEIPT_NAME));
        Files.copy(first.resolve(WindowsWorldProvisioner.GUARD_NAME), second.resolve(WindowsWorldProvisioner.GUARD_NAME));
        try (var refused = child("source-refusal", second, sha)) { assertTrue(refused.finish().contains("REFUSED_SOURCE=true")); }
        assertArrayEquals(Files.readAllBytes(first.resolve(WindowsWorldProvisioner.RECEIPT_NAME)), Files.readAllBytes(second.resolve(WindowsWorldProvisioner.RECEIPT_NAME)));
    }
    @Test void actualPartialGuardDoesNotFallBackToCreationPidOrOverwriteUnknownOwner() throws Exception {
        Path world = temporaryRoot.resolve("partial"); String sha = createAndExit(world);
        byte[] partial = new byte[]{1, 2, 3}; Files.write(world.resolve(WindowsWorldProvisioner.GUARD_NAME), partial);
        try (var refused = child("partial-refusal", world, sha)) { assertTrue(refused.finish().contains("REFUSED_PARTIAL=true")); }
        assertArrayEquals(partial, Files.readAllBytes(world.resolve(WindowsWorldProvisioner.GUARD_NAME)));
    }
    @Test void actualZeroLengthInitialOwnerGapRequiresExactCreationSourceAndExitedCreator() throws Exception {
        Path world = temporaryRoot.resolve("initial-owner-gap");
        String java = System.getProperty("fp.b1.native.child.java"), classpath = System.getProperty("fp.b1.native.child.classpath");
        try (var supervisor = child("supervisor-zero-owner-gap", world, "NONE", java, classpath)) {
            assertTrue(supervisor.finish().contains("ZERO_OWNER_GAP_RESUME=true"));
        }
        byte[] receipt = Files.readAllBytes(world.resolve("test-before-receipt.bin"));
        assertArrayEquals(receipt, Files.readAllBytes(world.resolve(WindowsWorldProvisioner.RECEIPT_NAME)));
        assertEquals(WindowsWorldProvisioner.OWNER_SIZE, Files.size(world.resolve(WindowsWorldProvisioner.GUARD_NAME)));
    }

    @Test void actualRetainedObservationRejectsWrongScopeAndThenResumesItsExactWorld() throws Exception {
        Path root = temporaryRoot.resolve("wrong-scope-supervisor");
        String java = System.getProperty("fp.b1.native.child.java"), classpath = System.getProperty("fp.b1.native.child.classpath");
        try (var supervisor = child("supervisor-wrong-scope", root, "NONE", java, classpath)) {
            assertTrue(supervisor.finish().contains("WRONG_SCOPE_REJECTED=true"));
        }
        Path worldB = root.resolve("world-b");
        assertArrayEquals(Files.readAllBytes(root.resolve("world-b-before-owner.bin")),
            Files.readAllBytes(worldB.resolve(WindowsWorldProvisioner.GUARD_NAME)));
        assertEquals(WindowsWorldProvisioner.OWNER_SIZE,
            Files.size(root.resolve("world-a").resolve(WindowsWorldProvisioner.GUARD_NAME)));
    }

    @Test void actualRetainedObservationEnforcesIssuerOperationThreadTimeoutAndOneTimeUse() throws Exception {
        Path world = temporaryRoot.resolve("retained-observation-contract");
        String java = System.getProperty("fp.b1.native.child.java"), classpath = System.getProperty("fp.b1.native.child.classpath");
        try (var supervisor = child("supervisor-observation-contract", world, "NONE", java, classpath)) {
            assertTrue(supervisor.finish().contains("OBSERVATION_CONTRACT=true"));
        }
        assertEquals(WindowsWorldProvisioner.OWNER_SIZE, Files.size(world.resolve(WindowsWorldProvisioner.GUARD_NAME)));
    }

    @Test void actualRetainedInspectionRevalidatesDurableWorldWithoutReopeningConsumedProcessHandle() throws Exception {
        Path world = temporaryRoot.resolve("retained-inspection");
        String java = System.getProperty("fp.b1.native.child.java"), classpath = System.getProperty("fp.b1.native.child.classpath");
        try (var supervisor = child("supervisor-inspection", world, "NONE", java, classpath)) {
            assertTrue(supervisor.finish().contains("RETAINED_INSPECTION=true"));
        }
        byte[] originalOwner = Files.readAllBytes(world.resolve("test-before-owner.bin"));
        assertArrayEquals(originalOwner, Files.readAllBytes(world.resolve(WindowsWorldProvisioner.GUARD_NAME)));
    }
    @Test void actualExistingUnboundDirectoryCannotFabricateCreationReceiptOrCustodyGuard() throws Exception {
        Path world = Files.createDirectory(temporaryRoot.resolve("unbound"));
        Path expectedNativeTarget = world.toAbsolutePath().normalize().resolve(WindowsWorldProvisioner.GUARD_NAME);
        String actualNativeTarget = expectedNativePathArgument(expectedNativeTarget.toString());
        try (var refused = child("missing-source-refusal", world, "00".repeat(32))) {
            String output = refused.finish();
            String safeFingerprint = "REFUSED_MISSING_SOURCE=2;nativeTargetUtf16CodeUnits="
                + actualNativeTarget.length() + ";nativeTargetSha256Utf16Le="
                + utf16LeSha256(actualNativeTarget);
            assertTrue(output.contains(safeFingerprint));
            assertFalse(output.contains(expectedNativeTarget.toString()));
        }
        try (var listing = Files.list(world)) { assertEquals(0, listing.count()); }
    }
    @Test void actualWrongControlledReceiptShaFailsBeforeCustodyOwnerMutation() throws Exception {
        Path world = temporaryRoot.resolve("wrong-sha"); createAndExit(world);
        byte[] prior = Files.readAllBytes(world.resolve(WindowsWorldProvisioner.GUARD_NAME));
        try (var refused = child("sha-refusal", world, "00".repeat(32))) { assertTrue(refused.finish().contains("REFUSED_SHA=true")); }
        assertArrayEquals(prior, Files.readAllBytes(world.resolve(WindowsWorldProvisioner.GUARD_NAME)));
    }
    @Test void actualFreshFixedDataParentThenNativeMarkerAckUsesActualDataIdentity() throws Exception {
        Path world = temporaryRoot.resolve("fixed-marker");
        try (var process = child("marker-location", world, "NONE")) {
            assertTrue(process.finish().contains("MARKER_LOCATION_ACK=true"));
        }
        assertTrue(Files.isDirectory(world.resolve("data")));
        assertArrayEquals(WindowsMarkerIo.markerBytes(plan().bindingUuid(), 2), Files.readAllBytes(world.resolve("data/floatingpod-world-binding.bin")));
    }
    @Test void actualLongPathUsesExtendedWin32NamespaceForCreateFreshAndMarkerRebarrier() throws Exception {
        Path worldRoot = temporaryRoot.resolve("long-native-world");
        try (var process = child("long-path-marker", worldRoot, "NONE")) {
            String output = process.finish();
            assertTrue(output.contains("LONG_PATH_MARKER_REPLAY=true"));
            assertTrue(output.contains("nativeTargetUtf16CodeUnits="));
        }
    }
    @Test void actualExistingCorrectDataAndMarkerReplaysWithoutIdentityChange() throws Exception {
        Path world = temporaryRoot.resolve("fixed-replay");
        try (var process = child("marker-location-replay", world, "NONE")) {
            assertTrue(process.finish().contains("MARKER_LOCATION_REPLAY=true"));
        }
        assertArrayEquals(WindowsMarkerIo.markerBytes(plan().bindingUuid(), 2), Files.readAllBytes(world.resolve("data/floatingpod-world-binding.bin")));
    }
    @Test void actualForeignDataReparsePointIsRejectedWithoutPublishingMarker() throws Exception {
        Path world = temporaryRoot.resolve("foreign-data");
        try (var process = child("marker-location-reparse", world, "NONE")) {
            assertTrue(process.finish().contains("REFUSED_MARKER_REPARSE=true"));
        }
        assertTrue(Files.isSymbolicLink(world.resolve("data")));
        assertFalse(Files.exists(temporaryRoot.resolve("foreign-data-target/floatingpod-world-binding.bin")));
    }
    @Test void actualOpaqueCustodyFromDifferentFactoryIsRejectedBeforeDataCreation() throws Exception {
        Path world = temporaryRoot.resolve("wrong-custody");
        try (var process = child("marker-location-wrong-custody", world, "NONE")) {
            assertTrue(process.finish().contains("REFUSED_MARKER_CUSTODY=true"));
        }
        assertFalse(Files.exists(world.resolve("data")));
    }
    /** 只供未来root审阅的隔离子进程执行，不是runtime业务入口。 */
    public static final class Child {
        public static void main(String[] args) throws Exception {
            if (args.length < 3) throw new IllegalArgumentException("Exact test child mode/directory/expectedSHA required");
            String mode = args[0]; Path directory = Path.of(args[1]); String expectedSha = args[2];
            if (mode.startsWith("supervisor-")) {
                if (args.length != 5) throw new IllegalArgumentException("Supervisor requires reviewed Java25 executable and frozen classpath");
                runRetainedSupervisor(mode, directory, args[3], args[4]);
                return;
            }
            WindowsMarkerIo io = new WindowsMarkerIo(); var provisioner = new WindowsWorldProvisioner(io);
            var boot = new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(), 1);
            if (mode.equals("ordinary-inspection")) {
                var inspection = provisioner.beginResumeInspection(directory, plan(), expectedSha,
                    new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(), 2));
                inspection.validateInspection();
                var evidence = inspection.previousOwnerEvidenceKind();
                assertNotEquals(WindowsWorldProvisioner.PreviousOwnerEvidenceKind.RETAINED_PROCESS_EXIT_ACKNOWLEDGEMENT,
                    evidence);
                assertFalse(inspection.ownerPublicationAttempted());
                System.out.println("ORDINARY_INSPECTION=" + evidence);
                return;
            }
            if (mode.equals("ordinary-resume")) {
                var resumed = provisioner.resumeExisting(directory, plan(), expectedSha,
                    new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(), 2));
                assertFalse(resumed.createdThisBoot());
                resumed.bootWorldCustodyProof().validateLive();
                assertEquals(2, resumed.bootWorldCustodyProof().generation());
                System.out.println("ORDINARY_RESUME=true;generation=" + resumed.bootWorldCustodyProof().generation());
                return;
            }
            if (mode.equals("readiness-failure")) {
                Throwable primary;
                try {
                    var failure = assertThrows(WindowsMarkerIo.NativeFailure.class,
                        () -> provisioner.createFresh(directory, plan(), boot));
                    assertEquals("CreateDirectoryW", failure.api()); assertEquals(183, failure.win32Error());
                    primary = failure;
                } catch (Throwable failure) {
                    primary = failure;
                }
                try { io.close(); }
                catch (Throwable cleanup) { primary.addSuppressed(cleanup); }
                if (primary instanceof Exception exception) throw exception;
                if (primary instanceof Error error) throw error;
                throw new AssertionError(primary);
            }
            if (mode.equals("existing-refusal")) {
                var failure = assertThrows(WindowsMarkerIo.NativeFailure.class, () -> provisioner.createFresh(directory, plan(), boot));
                assertEquals("CreateDirectoryW", failure.api()); assertEquals(183, failure.win32Error()); io.close();
                System.out.println("REFUSED_EXISTING=183"); return;
            }
            if (mode.equals("missing-source-refusal")) {
                var failure = assertThrows(WindowsMarkerIo.NativeFailure.class, () -> provisioner.resumeExisting(directory, plan(), expectedSha, boot));
                assertEquals("CreateFileW", failure.api()); assertEquals(2, failure.win32Error());
                Path target = directory.toAbsolutePath().normalize().resolve(WindowsWorldProvisioner.GUARD_NAME);
                String actualTarget = expectedNativePathArgument(target.toString());
                var diagnostic = failure.nativePathDiagnostic().orElseThrow();
                assertEquals(actualTarget.length(), diagnostic.utf16CodeUnitsExcludingTerminator());
                assertEquals(utf16LeSha256(actualTarget), diagnostic.sha256Utf16LeExcludingTerminator());
                assertFalse(failure.getMessage().contains(target.toString()));
                io.close();
                System.out.println("REFUSED_MISSING_SOURCE=2;nativeTargetUtf16CodeUnits="
                    + diagnostic.utf16CodeUnitsExcludingTerminator() + ";nativeTargetSha256Utf16Le="
                    + diagnostic.sha256Utf16LeExcludingTerminator()); return;
            }
            if (mode.equals("guard-refusal")) {
                var failure = assertThrows(WindowsMarkerIo.NativeFailure.class, () -> provisioner.resumeExisting(directory, plan(), expectedSha, boot));
                assertEquals("CreateFileW", failure.api()); assertEquals(32, failure.win32Error()); io.close();
                System.out.println("REFUSED_GUARD=32"); return;
            }
            if (mode.equals("source-refusal") || mode.equals("partial-refusal") || mode.equals("sha-refusal")) {
                var failure = assertThrows(IOException.class, () -> provisioner.resumeExisting(directory, plan(), expectedSha, boot));
                String message = switch (mode) {
                    case "source-refusal" -> "Actual directory/guard FileId differs from durable creation source";
                    case "partial-refusal" -> "Partial custody owner record; reconciliation required";
                    default -> "Controlled full receipt SHA mismatch; no custody";
                };
                assertEquals(message, failure.getMessage()); io.close();
                System.out.println(mode.equals("source-refusal") ? "REFUSED_SOURCE=true" : mode.equals("partial-refusal") ? "REFUSED_PARTIAL=true" : "REFUSED_SHA=true"); return;
            }
            if (mode.equals("long-path-marker")) {
                Path actualParent = directory;
                assertTrue(actualParent.toString().length() + 1 <= 248, "test root setup must stay inside the legacy CreateDirectoryW limit");
                io.createDirectoryNew(actualParent, plan().creationOperationUuid());
                String mediumPrefix = "medium-";
                int mediumComponentLength = 252 - actualParent.toString().length() - 1;
                assertTrue(mediumComponentLength >= mediumPrefix.length() && mediumComponentLength <= 255,
                    "fixture must fit one legal directory component at the CreateDirectoryW boundary");
                Path mediumDirectory = actualParent.resolve(mediumPrefix + "m".repeat(mediumComponentLength - mediumPrefix.length()));
                assertEquals(252, mediumDirectory.toString().length());
                io.createDirectoryNew(mediumDirectory, plan().creationOperationUuid());
                actualParent = mediumDirectory;
                String publishedLeaf = "published-world-" + "y".repeat(36);
                final Path publishedWorld = actualParent.resolve(publishedLeaf);
                assertTrue(actualParent.toString().length() + 1 <= 260, "parent setup must stay inside legacy MAX_PATH");
                assertTrue(publishedWorld.toString().length() + 1 > 260, "CREATE_NEW target must cross legacy MAX_PATH");
                String guardArgument = expectedNativePathArgument(
                    publishedWorld.resolve(WindowsWorldProvisioner.GUARD_NAME).toString());

                var created = provisioner.createFresh(publishedWorld, plan(), boot);
                created.bootWorldCustodyProof().validateLive();
                var location = provisioner.ensureCanonicalMarkerLocation(created.bootWorldCustodyProof());
                var barrier = new WindowsCanonicalMarkerPublicationBarrier(io);
                var firstAck = barrier.publish(location.barrierRequest());
                assertFalse(firstAck.retainedExisting());
                assertEquals(location.dataDirectoryIdentity(), firstAck.parentIdentity());
                location.validateLive();

                var replayLocation = provisioner.ensureCanonicalMarkerLocation(created.bootWorldCustodyProof());
                var replayAck = barrier.rebarrierExisting(replayLocation.barrierRequest(), firstAck);
                assertTrue(replayAck.retainedExisting());
                assertEquals(firstAck.fileIdentity(), replayAck.fileIdentity());
                assertEquals(firstAck.markerSha256(), replayAck.markerSha256());
                replayLocation.validateLive();

                var refused = assertThrows(WindowsMarkerIo.NativeFailure.class, () -> provisioner.resumeExisting(
                    publishedWorld, plan(), created.freshProvisioningProof().receiptSha256(),
                    new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(), 2)));
                assertEquals("CreateFileW", refused.api()); assertEquals(32, refused.win32Error());
                var expectedDiagnostic = new WindowsMarkerIo.NativePathDiagnostic(
                    guardArgument.length(), utf16LeSha256(guardArgument));
                assertEquals(expectedDiagnostic, refused.nativePathDiagnostic().orElseThrow());
                assertFalse(refused.getMessage().contains(publishedWorld.toString()));
                created.bootWorldCustodyProof().validateLive();
                System.out.println("LONG_PATH_MARKER_REPLAY=true;directoryBoundaryUtf16CodeUnits="
                    + mediumDirectory.toString().length() + ";nativeTargetUtf16CodeUnits="
                    + expectedDiagnostic.utf16CodeUnitsExcludingTerminator()
                    + ";nativeTargetSha256Utf16Le=" + expectedDiagnostic.sha256Utf16LeExcludingTerminator()
                    + ";retainedGuardRefusal=32");
                return;
            }
            if (!(mode.equals("fresh") || mode.equals("hold") || mode.equals("hold-formal-handoff") || mode.equals("same-process")
                || mode.equals("marker-location") || mode.equals("marker-location-replay") || mode.equals("marker-location-reparse")
                || mode.equals("marker-location-wrong-custody")))
                throw new IllegalArgumentException("Unknown native test child mode");
            var result = provisioner.createFresh(directory, plan(), boot);
            result.bootWorldCustodyProof().validateLive();
            if (mode.equals("marker-location") || mode.equals("marker-location-replay")) {
                var proof = result.bootWorldCustodyProof();
                byte[] receiptBefore = Files.readAllBytes(directory.resolve(WindowsWorldProvisioner.RECEIPT_NAME));
                var location = provisioner.ensureCanonicalMarkerLocation(proof);
                assertEquals(directory.toAbsolutePath().normalize().resolve("data/floatingpod-world-binding.bin"), location.canonicalPath());
                assertEquals(proof.bootId(), location.bootId()); assertEquals(proof.generation(), location.generation());
                assertEquals(proof.sourceWorldIdentity(), location.sourceWorldIdentity()); assertEquals(plan(), location.plan());
                var first = new WindowsCanonicalMarkerPublicationBarrier(io).publish(location.barrierRequest());
                assertEquals(location.dataDirectoryIdentity(), first.parentIdentity());
                assertEquals(plan().bootstrapOperationUuid(), first.operationUuid());
                assertFalse(first.retainedExisting()); location.validateLive();
                if (mode.equals("marker-location-replay")) {
                    var secondLocation = provisioner.ensureCanonicalMarkerLocation(proof);
                    assertEquals(location.dataDirectoryIdentity(), secondLocation.dataDirectoryIdentity());
                    assertEquals(location.canonicalPath(), secondLocation.canonicalPath());
                    var second = new WindowsCanonicalMarkerPublicationBarrier(io).publish(secondLocation.barrierRequest());
                    assertTrue(second.retainedExisting()); assertEquals(first.fileIdentity(), second.fileIdentity());
                    secondLocation.validateLive();
                }
                assertArrayEquals(receiptBefore, Files.readAllBytes(directory.resolve(WindowsWorldProvisioner.RECEIPT_NAME)));
                System.out.println(mode.equals("marker-location") ? "MARKER_LOCATION_ACK=true" : "MARKER_LOCATION_REPLAY=true"); return;
            }
            if (mode.equals("marker-location-reparse")) {
                Path foreign = Files.createDirectory(directory.resolveSibling("foreign-data-target"));
                // 真实reparse fixture；权限不足失败，不Assumption/skip、不改成普通目录假通过。
                Files.createSymbolicLink(directory.resolve("data"), foreign);
                assertTrue(Files.isSymbolicLink(directory.resolve("data")));
                var failure = assertThrows(IOException.class, () -> provisioner.ensureCanonicalMarkerLocation(result.bootWorldCustodyProof()));
                assertNotNull(failure.getCause());
                assertTrue(failure.getCause().getMessage().startsWith("Marker handle has wrong directory/reparse type"));
                assertFalse(Files.exists(foreign.resolve("floatingpod-world-binding.bin")));
                result.bootWorldCustodyProof().validateLive();
                System.out.println("REFUSED_MARKER_REPARSE=true"); return;
            }
            if (mode.equals("marker-location-wrong-custody")) {
                var foreignFactory = new WindowsWorldProvisioner(io);
                var failure = assertThrows(IOException.class, () -> foreignFactory.ensureCanonicalMarkerLocation(result.bootWorldCustodyProof()));
                assertEquals("Canonical marker location requires same-factory opaque custody", failure.getMessage());
                assertFalse(Files.exists(directory.resolve("data"))); result.bootWorldCustodyProof().validateLive();
                System.out.println("REFUSED_MARKER_CUSTODY=true"); return;
            }
            if (mode.equals("same-process")) {
                var current = io.captureCurrentProcessIdentity(UUID.randomUUID());
                var observationFailure = assertThrows(IOException.class, () -> io.observeLiveProcessCandidate(
                    current.pid(), plan().bootstrapOperationUuid()));
                assertTrue(observationFailure.getMessage().contains("Same-process previous physical writer"));
                var failure = assertThrows(IOException.class, () -> io.requirePreviousProcessExited(current.pid(), current.creationFileTime(), UUID.randomUUID()));
                assertTrue(failure.getMessage().contains("Retained live process observation required"));
                assertThrows(IOException.class, () -> provisioner.resumeExisting(directory, plan(), result.freshProvisioningProof().receiptSha256(),
                    new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(), 2)));
                System.out.println("REFUSED_SAME_PROCESS=true"); return;
            }
            if(mode.equals("hold-formal-handoff")){
                // 正式 normal fixture 在 creator 仍 live 时真实发布原 source anchor 与完整两槽。
                Path control=Files.createDirectory(directory.resolveSibling(directory.getFileName()+"-formal-control"));
                Path metadata=B1SourceAnchorFixture.writeMetadata(control);
                var anchors=new WindowsB1SourceAnchorStore(io);
                var location=anchors.captureLocation(metadata,
                    WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(MySqlRuntimeMetadata.load(metadata)),UUID.randomUUID());
                var anchor=anchors.publishCreatedSource(location,result);
                var slots=new WindowsB1TwoSlotStore(io);
                var scope=slots.bindSource(anchors,location,anchor,result.bootWorldCustodyProof());
                var process=io.captureCurrentProcessIdentity(plan().creationOperationUuid());
                var assertions=B1TwoSlotFixture.genesis(new NativeOwnerAssertions(
                    process.pid(),process.creationFileTime(),boot.bootId(),boot.generation()));
                var genesis=slots.publishGenesis(scope,assertions);
                slots.publishNext(scope,genesis,Kind.PREPARED_CONFIRMED,B1TwoSlotFixture.prepared(assertions));
            }
            Files.writeString(directory.resolve("test-result.sha"), result.freshProvisioningProof().receiptSha256(), StandardCharsets.US_ASCII);
            System.out.println("CREATED=" + result.createdThisBoot()); System.out.flush();
            if (mode.equals("hold") || mode.equals("hold-formal-handoff")) { System.in.read(); result.bootWorldCustodyProof().validateLive(); }
            // 无提前释放API；结束子进程真实结束其所有thread/ownedIO并由OS释放custody handles。
        }

        private static void runRetainedSupervisor(String mode, Path directory, String java, String classpath) throws Exception {
            Path world = mode.equals("supervisor-wrong-scope") ? directory.resolve("world-a") : directory;
            Files.createDirectories(world.getParent());
            WindowsMarkerIo io = new WindowsMarkerIo();
            WindowsWorldProvisioner provisioner = new WindowsWorldProvisioner(io);
            WindowsWorldProvisioner.Plan plan = WindowsWorldProvisionerTest.plan();
            WindowsWorldProvisioner.BootIdentity nextBoot = new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(), 2);

            if (mode.equals("supervisor-wrong-scope")) {
                runWrongScopeScenario(directory, java, classpath, io, provisioner, plan, nextBoot);
                return;
            }
            if (mode.equals("supervisor-observation-contract"))
                verifyReadinessFailureKeepsOwnedChildDiagnostics(directory, java, classpath);

            try (ChildProcess held = child(mode.equals("supervisor-observation-contract") ? "hold-formal-handoff" : "hold", world, "NONE", java, classpath)) {
                String sha = awaitReadyReceipt(world, held);
                WindowsMarkerIo.RetainedProcessObservation observation = io.observeLiveProcessCandidate(
                    held.process.pid(), plan.bootstrapOperationUuid());
                assertFalse(observation.isSignaled());

                if (mode.equals("supervisor-observation-contract")) {
                    exerciseWrongThread(observation);
                    WindowsMarkerIo.RetainedProcessObservation wrongOperation = io.observeLiveProcessCandidate(
                        held.process.pid(), UUID.randomUUID());
                    IOException wrongOp = assertThrows(IOException.class, () -> provisioner.beginResumeInspection(
                        world, plan, sha, nextBoot, wrongOperation));
                    assertEquals("Retained process observation operation mismatch", wrongOp.getMessage());
                    wrongOperation.close();
                    WindowsMarkerIo foreignIo = new WindowsMarkerIo();
                    try {
                        IOException foreign = assertThrows(IOException.class, () -> new WindowsWorldProvisioner(foreignIo)
                            .beginResumeInspection(world, plan, sha, nextBoot, observation));
                        assertEquals("Exact retained process observation from this WindowsMarkerIo required", foreign.getMessage());
                    } finally { foreignIo.close(); }
                }

                WindowsWorldProvisioner.RetainedResumeAttempt pending = provisioner.resumeExisting(
                    world, plan, sha, nextBoot, observation);
                assertInstanceOf(WindowsWorldProvisioner.ResumePending.class, pending);
                assertSame(observation, ((WindowsWorldProvisioner.ResumePending) pending).observation());
                assertTrue(held.process.isAlive());
                IOException heldClose = assertThrows(IOException.class, io::close);
                assertTrue(heldClose.getMessage().contains("Unconfirmed native handle obligations remain: file=1 find=0"));
                assertFalse(observation.isSignaled());

                byte[] oldOwner;
                if (mode.equals("supervisor-zero-owner-gap")) {
                    held.finish();
                    oldOwner = Files.readAllBytes(world.resolve(WindowsWorldProvisioner.GUARD_NAME));
                    Files.write(world.resolve(WindowsWorldProvisioner.GUARD_NAME), new byte[0]);
                } else {
                    held.finish();
                    oldOwner = Files.readAllBytes(world.resolve(WindowsWorldProvisioner.GUARD_NAME));
                }
                Files.write(directory.resolve("test-before-receipt.bin"),
                    Files.readAllBytes(world.resolve(WindowsWorldProvisioner.RECEIPT_NAME)));
                Files.write(directory.resolve("test-before-owner.bin"), oldOwner);
                assertTrue(observation.isSignaled());

                if (mode.equals("supervisor-inspection")) {
                    WindowsWorldProvisioner.RetainedResumeAttempt inspectionResult = provisioner.beginResumeInspection(
                        world, plan, sha, nextBoot, observation);
                    assertInstanceOf(WindowsWorldProvisioner.ResumeInspectionReady.class, inspectionResult);
                    WindowsWorldProvisioner.ResumeInspection inspection =
                        ((WindowsWorldProvisioner.ResumeInspectionReady) inspectionResult).inspection();
                    inspection.validateInspection();
                    assertFalse(inspection.ownerPublicationAttempted());
                    // validateInspection 已通过保留的 share-zero handle 核验当前 owner；不再以第二个文件句柄重开 guard。
                    assertArrayEquals(oldOwner, inspection.originalOwnerCanonicalBytes());
                    System.out.println("RETAINED_INSPECTION=true");
                    return;
                }

                if (mode.equals("supervisor-observation-contract")) {
                    // 四参机械 API 有自己的 typed lifetime 路径；不在此 world 调用它改 owner。
                    // 此正式分支只消费原 retained observation，经 inspection/真实 ACK 安装。
                    assertArrayEquals(oldOwner, Files.readAllBytes(world.resolve(WindowsWorldProvisioner.GUARD_NAME)));
                    assertTrue(observation.isSignaled());
                    String wrongSha = (sha.charAt(0) == '0' ? "1" : "0") + sha.substring(1);
                    IOException badSha = assertThrows(IOException.class, () -> provisioner.resumeExisting(
                        world, plan, wrongSha, nextBoot, observation));
                    assertEquals("Controlled full receipt SHA mismatch; no custody", badSha.getMessage());
                    assertArrayEquals(oldOwner, Files.readAllBytes(world.resolve(WindowsWorldProvisioner.GUARD_NAME)));
                    assertTrue(observation.isSignaled());
                    // 错误 receipt 未消耗观察；从此仅真实 formal path，source/guard 原槽不能提前改。
                    var ready=provisioner.beginResumeInspection(world,plan,sha,nextBoot,observation);
                    assertInstanceOf(WindowsWorldProvisioner.ResumeInspectionReady.class,ready);
                    var inspection=((WindowsWorldProvisioner.ResumeInspectionReady)ready).inspection();
                    inspection.validateInspection();
                    assertEquals(WindowsWorldProvisioner.PreviousOwnerEvidenceKind.RETAINED_PROCESS_EXIT_ACKNOWLEDGEMENT,
                        inspection.previousOwnerEvidenceKind());
                    assertArrayEquals(oldOwner,inspection.originalOwnerCanonicalBytes());
                    assertFalse(inspection.ownerPublicationAttempted());
                    WindowsB1TwoSlotStore.DurableHandoffNativeAck noAck=null;
                    assertThrows(NullPointerException.class,()->provisioner.installResumeOwner(inspection,noAck));
                    inspection.validateInspection();assertFalse(inspection.ownerPublicationAttempted());
                    assertArrayEquals(oldOwner,inspection.originalOwnerCanonicalBytes());
                    var anchors=new WindowsB1SourceAnchorStore(io);
                    Path metadata=world.resolveSibling(world.getFileName()+"-formal-control").resolve("config/runtime.properties");
                    var location=anchors.captureLocation(metadata,
                        WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(MySqlRuntimeMetadata.load(metadata)),UUID.randomUUID());
                    var expected=anchors.readExpectedSource(location);var anchor=anchors.rebarrierExisting(location,expected);
                    assertEquals(plan,expected.expectedPlan());assertEquals(sha,expected.expectedReceiptSha256());
                    assertArrayEquals(Files.readAllBytes(directory.resolve("test-before-receipt.bin")),expected.originalReceiptCanonicalBytes());
                    var slots=new WindowsB1TwoSlotStore(io);var scope=slots.bindResumeSource(anchors,location,anchor,inspection);
                    var records=slots.readCurrent(scope);var current=slots.rebarrierCurrent(scope,records.current());
                    var retired=slots.retireInactiveSlot(scope,current,records.inactive());
                    var old=retired.retainedRecord().facts().record().assertions();
                    var target=inspection.targetOwner();var original=old.originalPrepareWitness();UUID operation=UUID.randomUUID();
                    var context=new OperationAssertions(target.boot(),old.currentWriter().epoch(),old.currentWriter().binding(),
                        operation,original.context().effects(),original.context().originalOperationFingerprint());
                    var handoff=new AttemptAssertions(UUID.randomUUID(),Phase.HANDOFF,operation,target.generation(),UUID.randomUUID(),
                        123,expected.barrierNamespace(),context,old.currentWriter(),old.targetWriter(),original.beforeGraph(),original.targetGraph());
                    var assertions=new FullStateAssertions(target,inspection.previousOwner(),old.retirement(),original,
                        old.currentWriter(),old.targetWriter(),handoff,old.latestSettlement(),old.marker(),old.acknowledgedCertifyWitness());
                    // SQL 字段仅测试 DATA；native ACK 必须来自真正完整发布/flush/readback/checked close。
                    var ack=slots.publishHandoffPrepared(scope,retired.retainedRecord(),assertions);
                    inspection.validateInspection();assertFalse(inspection.ownerPublicationAttempted());
                    assertArrayEquals(oldOwner,inspection.originalOwnerCanonicalBytes());
                    assertEquals(Kind.HANDOFF_PREPARED,ack.record().kind());
                    assertEquals(target,ack.record().assertions().currentOwner());
                    assertEquals(inspection.previousOwner(),ack.record().assertions().previousOwner());
                    var actual=provisioner.installResumeOwner(inspection,ack);
                    assertFalse(actual.createdThisBoot());actual.bootWorldCustodyProof().validateLive();
                    assertEquals(expected.originalSourceIdentity(),actual.freshProvisioningProof().sourceWorldIdentity());
                    assertArrayEquals(expected.originalReceiptCanonicalBytes(),actual.freshProvisioningProof().receiptCanonicalBytes());
                    assertEquals(nextBoot.bootId(),actual.bootWorldCustodyProof().bootId());
                    assertEquals(nextBoot.generation(),actual.bootWorldCustodyProof().generation());
                    assertThrows(IOException.class,inspection::validateInspection);
                    assertThrows(IOException.class,()->provisioner.installResumeOwner(inspection,ack));
                    var repeated=assertThrows(IOException.class,()->provisioner.beginResumeInspection(
                        world,plan,sha,new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(),3),observation));
                    assertEquals("Retained process observation is not live and unconsumed",repeated.getMessage());
                    System.out.println("OBSERVATION_CONTRACT=true");return;
                }

                WindowsWorldProvisioner.RetainedResumeAttempt resumed = provisioner.resumeExisting(
                    world, plan, sha, nextBoot, observation);
                assertInstanceOf(WindowsWorldProvisioner.ResumeCustodyReady.class, resumed);
                WindowsWorldProvisioner.ProvisionedWorld actual =
                    ((WindowsWorldProvisioner.ResumeCustodyReady) resumed).world();
                assertFalse(actual.createdThisBoot()); actual.bootWorldCustodyProof().validateLive();

                IOException repeated = assertThrows(IOException.class, () -> provisioner.resumeExisting(
                    world, plan, sha, new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(), 3), observation));
                assertEquals("Retained process observation is not live and unconsumed", repeated.getMessage());
                System.out.println(mode.equals("supervisor-zero-owner-gap")
                    ? "ZERO_OWNER_GAP_RESUME=true" : mode.equals("supervisor-observation-contract")
                    ? "OBSERVATION_CONTRACT=true" : "RETAINED_RESUME=true");
            }
        }

        private static void verifyReadinessFailureKeepsOwnedChildDiagnostics(Path supervisorDirectory, String java, String classpath) throws Exception {
            Path diagnosticWorld = supervisorDirectory.resolveSibling(supervisorDirectory.getFileName() + "-ready-failure");
            Files.createDirectory(diagnosticWorld);
            try (ChildProcess failing = child("readiness-failure", diagnosticWorld, "NONE", java, classpath)) {
                IOException readiness = assertThrows(IOException.class, () -> awaitReadyReceipt(diagnosticWorld, failing));
                assertEquals("Live helper did not publish its test coordination receipt", readiness.getMessage());
                assertEquals("awaitReadyReceipt", readiness.getStackTrace()[0].getMethodName());
                IOException childDiagnostics = Arrays.stream(readiness.getSuppressed())
                    .filter(IOException.class::isInstance).map(IOException.class::cast)
                    .filter(problem -> problem.getMessage() != null
                        && problem.getMessage().startsWith("Owned native child diagnostics after bounded wait:"))
                    .findFirst().orElseThrow();
                assertTrue(childDiagnostics.getMessage().contains("exitCode=1"));
                assertTrue(childDiagnostics.getMessage().contains("NativeFailure: CreateDirectoryW"));
                assertTrue(childDiagnostics.getMessage().length() <= READINESS_DIAGNOSTIC_LIMIT_BYTES + 512);
            }
        }

        private static String awaitReadyReceipt(Path world, ChildProcess child) throws Exception {
            Process process = child.process;
            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                Path ready = world.resolve("test-result.sha");
                while (!Files.exists(ready) && process.isAlive() && System.nanoTime() < deadline) Thread.sleep(10);
                if (!Files.exists(ready)) throw new IOException("Live helper did not publish its test coordination receipt");
                String sha = Files.readString(ready, StandardCharsets.US_ASCII);
                if (!sha.matches("[0-9A-F]{64}")) throw new IOException("Child published invalid test receipt SHA");
                return sha;
            } catch (Throwable primary) {
                child.attachReadinessDiagnosticsAfterOwnedWait(primary);
                if (primary instanceof Exception exception) throw exception;
                if (primary instanceof Error error) throw error;
                throw new AssertionError(primary);
            }
        }

        private static void exerciseWrongThread(WindowsMarkerIo.RetainedProcessObservation observation) throws Exception {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread wrong = new Thread(() -> {
                try { observation.isSignaled(); } catch (Throwable problem) { failure.set(problem); }
            }, "wrong-native-observation-owner");
            wrong.start(); wrong.join(5000);
            assertFalse(wrong.isAlive()); assertInstanceOf(IllegalStateException.class, failure.get());
            assertTrue(failure.get().getMessage().contains("owner thread mismatch"));
        }

        private static void runWrongScopeScenario(Path root, String java, String classpath, WindowsMarkerIo io,
                WindowsWorldProvisioner provisioner, WindowsWorldProvisioner.Plan plan,
            WindowsWorldProvisioner.BootIdentity nextBoot) throws Exception {
            Path worldA = root.resolve("world-a"), worldB = root.resolve("world-b");
            Files.createDirectories(root);
            try (ChildProcess heldA = child("hold", worldA, "NONE", java, classpath)) {
                String shaA = awaitReadyReceipt(worldA, heldA);
                WindowsMarkerIo.RetainedProcessObservation observation = io.observeLiveProcessCandidate(
                    heldA.process.pid(), plan.bootstrapOperationUuid());
                String shaB;
                try (ChildProcess createdB = child("fresh", worldB, "NONE", java, classpath)) {
                    assertTrue(createdB.finish().contains("CREATED=true"));
                    shaB = Files.readString(worldB.resolve("test-result.sha"), StandardCharsets.US_ASCII);
                }
                heldA.finish();
                byte[] ownerB = Files.readAllBytes(worldB.resolve(WindowsWorldProvisioner.GUARD_NAME));
                IOException mismatch = assertThrows(IOException.class, () -> provisioner.resumeExisting(
                    worldB, plan, shaB, nextBoot, observation));
                assertEquals("Durable previous process identity does not match retained native HANDLE", mismatch.getMessage());
                assertTrue(observation.isSignaled());
                Files.write(root.resolve("world-b-before-owner.bin"), ownerB);
                WindowsWorldProvisioner.RetainedResumeAttempt ready = provisioner.resumeExisting(
                    worldA, plan, shaA, nextBoot, observation);
                assertInstanceOf(WindowsWorldProvisioner.ResumeCustodyReady.class, ready);
                assertArrayEquals(ownerB, Files.readAllBytes(worldB.resolve(WindowsWorldProvisioner.GUARD_NAME)));
                System.out.println("WRONG_SCOPE_REJECTED=true");
            }
        }
    }

    @Test void pureLegacyNativePlanAndReceiptAdaptersRemainStrict2And3Only()throws Exception {
        var p=plan();
        for(int format:new int[]{1,4,5,6})assertThrows(IllegalArgumentException.class,()->new WindowsWorldProvisioner.Plan(
            p.creationOperationUuid(),p.bootstrapOperationUuid(),p.prepareWriteUuid(),p.certifyWriteUuid(),p.bindingUuid(),2,p.mysqlServerUuid(),p.schema(),format,p.ddlSha256(),p.provisioningUuid()));
        for(int format:new int[]{2,3}) {
            byte[] bytes=WindowsWorldProvisionReceiptDataCodec.encode(WindowsWorldProvisionReceiptDataCodecTest.receipt(format));
            var decoded=WindowsWorldProvisioner.decodeReceipt(bytes);
            assertEquals(format,decoded.plan().schemaFormat());assertArrayEquals(bytes,WindowsWorldProvisioner.encodeReceipt(decoded));
        }
        byte[] schema5=WindowsWorldProvisionReceiptDataCodec.encode(WindowsWorldProvisionReceiptDataCodecTest.receipt(5));
        assertThrows(IOException.class,()->WindowsWorldProvisioner.decodeReceipt(schema5));
    }
}
