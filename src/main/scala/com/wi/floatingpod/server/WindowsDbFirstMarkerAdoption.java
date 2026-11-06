package com.wi.floatingpod.server;

import com.wi.floatingpod.server.source.SourceForestAllocationBudget;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * FirstBinding6 的旧 marker 保留与新 marker 发布步骤。SQL PREPARED、实际来源回执和 Ready
 * 仍由上层 coordinator 负责；本类只返回带真实 native publication ACK 的 DATA。
 */
public final class WindowsDbFirstMarkerAdoption {
    public static final long NATIVE_WORKSPACE_BYTES = 393_216L;
    public static final String CANONICAL_MARKER_BASENAME = "floatingpod-world-binding.bin";
    private static final String WORKSPACE_CONTEXT = "FirstBinding6 marker adoption native/path workspace";
    private static final String PUBLICATION_PROTOCOL = "WIN32_WRITE_FLUSH_MOVE_WRITE_THROUGH_CANONICAL_FLUSH_READ_CLOSE_V1";

    private final WindowsMarkerIo io;
    private final SourceForestAllocationBudget allocation;
    private final WindowsCanonicalMarkerPublicationBarrier publicationBarrier;

    /** IO 和预算均由上层原有 owner 持有；adopter 不关闭 IO，也不创建新的预算根。 */
    public WindowsDbFirstMarkerAdoption(WindowsMarkerIo callerRetainedIo,
            SourceForestAllocationBudget callerAllocation) {
        io = Objects.requireNonNull(callerRetainedIo, "caller-retained WindowsMarkerIo");
        allocation = Objects.requireNonNull(callerAllocation, "caller-owned allocation account");
        publicationBarrier = new WindowsCanonicalMarkerPublicationBarrier(io);
    }

    /**
     * 先在调用方预算内保留 393216 字节工作区，再触碰路径、native HANDLE 或 scratch。
     * 预算释放失败会使本次调用失败；已发生的 marker 变化由下次按固定 sibling 状态恢复。
     */
    public AdoptionData adopt(Request request) throws IOException {
        Objects.requireNonNull(request, "FirstBinding6 request");
        allocation.reserveTemporary(NATIVE_WORKSPACE_BYTES, WORKSPACE_CONTEXT);
        Throwable primary = null;
        try {
            return adoptReserved(request);
        } catch (Throwable failure) {
            primary = failure;
            rethrow(failure);
            throw new AssertionError("unreachable");
        } finally {
            try {
                allocation.releaseTemporary(NATIVE_WORKSPACE_BYTES, WORKSPACE_CONTEXT);
            } catch (Throwable cleanup) {
                if (primary != null) {
                    if (cleanup != primary) primary.addSuppressed(cleanup);
                } else {
                    rethrow(cleanup);
                }
            }
        }
    }

    private AdoptionData adoptReserved(Request request) throws IOException {
        WindowsMarkerIo.Handle parent = null;
        WindowsMarkerIo.Handle canonical = null;
        WindowsMarkerIo.Handle backup = null;
        WindowsMarkerIo.Handle preserved = null;
        try {
            UUID operation = request.operationUuid();
            Path canonicalPath = request.canonicalPath();
            Path parentPath = canonicalPath.getParent();
            String preservedBasename = preservedOldBasename(operation);
            Path backupPath = canonicalPath.resolveSibling(preservedBasename);

            parent = io.directory(parentPath, operation);
            io.metadata(parent, true);
            requireIdentity(request.expectedParentIdentity(), io.identity(parent), "captured native parent changed");
            String nativeParentPath = io.finalPath(parent);
            String expectedCanonicalNativePath = exactChild(nativeParentPath, CANONICAL_MARKER_BASENAME);
            String expectedBackupNativePath = exactChild(nativeParentPath, preservedBasename);

            canonical = openOptional(canonicalPath, operation);
            backup = openOptional(backupPath, operation);
            ExpectedOldMarker old = request.oldMarkerExpectation() instanceof ExpectedOldMarker actual ? actual : null;
            byte[] newMarker = WindowsMarkerIo.markerBytes(request.newBindingUuid(), 2);
            Outcome outcome;

            if (canonical != null) {
                byte[] canonicalBytes = observeMarker(canonical, expectedCanonicalNativePath,
                    request.expectedParentIdentity());
                if (Arrays.equals(canonicalBytes, newMarker)) {
                    flushAndReverifyMarker(canonical, expectedCanonicalNativePath,
                        request.expectedParentIdentity(), io.identity(canonical), newMarker);
                    if (old == null) {
                        if (backup != null)
                            throw new IOException("FirstBinding6 found an unexpected preserved-old sibling");
                    } else {
                        if (backup == null)
                            throw new IOException("FirstBinding6 new marker lacks the expected preserved-old sibling");
                        verifyOldMarker(backup, expectedBackupNativePath, request.expectedParentIdentity(), old);
                        preserved = backup;
                        backup = null;
                    }
                    outcome = Outcome.CANONICAL_ALREADY_NEW;
                    io.closeChecked(canonical);
                    canonical = null;
                } else if (old != null) {
                    verifyOldMarker(canonical, expectedCanonicalNativePath, request.expectedParentIdentity(), old);
                    if (backup != null)
                        throw new IOException("FirstBinding6 preserved-old sibling collision; evidence was left unchanged");

                    // 先用 READ/WRITE 权限检查常见状态；确认确为旧对象后才取得 DELETE HANDLE，并重核来源身份。
                    // helper 只改名这个仍独占持有的 HANDLE，不使用 close-then-path-move。
                    io.closeChecked(canonical);
                    canonical = null;
                    canonical = io.open(canonicalPath,
                        WindowsMarkerIo.READ | WindowsMarkerIo.WRITE | WindowsMarkerIo.DELETE, 0,
                        WindowsMarkerIo.OPEN_EXISTING, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, operation);
                    verifyOldMarker(canonical, expectedCanonicalNativePath, request.expectedParentIdentity(), old);
                    io.renameHeldMarkerNoReplace(canonical, parent, preservedBasename,
                        old.fileIdentity(), request.expectedParentIdentity());
                    verifyOldMarker(canonical, expectedBackupNativePath, request.expectedParentIdentity(), old);
                    preserved = canonical;
                    canonical = null;
                    outcome = Outcome.OLD_MARKER_PRESERVED_NOW;
                } else {
                    throw new IOException("FirstBinding6 canonical marker differs from the requested new binding");
                }
            } else if (backup != null) {
                if (old == null)
                    throw new IOException("FirstBinding6 found a preserved-old sibling without an old-marker expectation");
                verifyOldMarker(backup, expectedBackupNativePath, request.expectedParentIdentity(), old);
                preserved = backup;
                backup = null;
                outcome = Outcome.OLD_MARKER_ALREADY_PRESERVED;
            } else if (old != null) {
                throw new IOException("FirstBinding6 old marker and its preserved sibling are both missing");
            } else {
                outcome = Outcome.OLD_MARKER_ABSENT;
            }

            if (backup != null)
                throw new IOException("FirstBinding6 found an unclassified preserved-old sibling");
            if (canonical != null) {
                io.closeChecked(canonical);
                canonical = null;
            }

            WindowsCanonicalMarkerPublicationBarrier.Request publicationRequest =
                new WindowsCanonicalMarkerPublicationBarrier.Request(canonicalPath, request.newBindingUuid(),
                    2, operation, request.expectedParentIdentity());
            WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck ack = publicationBarrier.publish(publicationRequest);
            verifyPublicationAck(request, ack);

            if (preserved != null) {
                verifyOldMarker(preserved, expectedBackupNativePath, request.expectedParentIdentity(), old);
                io.closeChecked(preserved);
                preserved = null;
            }
            requireStableParent(parent, nativeParentPath, request.expectedParentIdentity());
            io.closeChecked(parent);
            parent = null;
            return new AdoptionData(request, outcome, ack);
        } catch (Throwable failure) {
            WindowsMarkerIo.closeSuppressed(canonical, failure);
            WindowsMarkerIo.closeSuppressed(backup, failure);
            WindowsMarkerIo.closeSuppressed(preserved, failure);
            WindowsMarkerIo.closeSuppressed(parent, failure);
            rethrow(failure);
            throw new AssertionError("unreachable");
        }
    }

    private WindowsMarkerIo.Handle openOptional(Path path, UUID operation) throws IOException {
        try {
            return io.open(path, WindowsMarkerIo.READ | WindowsMarkerIo.WRITE, 0,
                WindowsMarkerIo.OPEN_EXISTING, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, operation);
        } catch (WindowsMarkerIo.NativeFailure failure) {
            if (failure.win32Error() == 2) return null;
            throw failure;
        }
    }

    private byte[] observeMarker(WindowsMarkerIo.Handle handle, String expectedNativePath,
            WindowsMarkerIo.FileIdentity expectedParent) throws IOException {
        io.metadata(handle, false);
        WindowsMarkerIo.FileIdentity identity = io.identity(handle);
        if (identity.volumeSerialNumber() != expectedParent.volumeSerialNumber())
            throw new IOException("FirstBinding6 marker is outside the captured parent volume");
        if (!expectedNativePath.equals(io.finalPath(handle)))
            throw new IOException("FirstBinding6 marker HANDLE is not the exact native child");
        byte[] bytes = io.readBackExact(handle);
        io.metadata(handle, false);
        if (!identity.equals(io.identity(handle)) || !expectedNativePath.equals(io.finalPath(handle)))
            throw new IOException("FirstBinding6 marker identity or path changed during verification");
        return bytes;
    }

    private void flushAndReverifyMarker(WindowsMarkerIo.Handle handle, String expectedNativePath,
            WindowsMarkerIo.FileIdentity expectedParent, WindowsMarkerIo.FileIdentity expectedIdentity,
            byte[] expectedBytes) throws IOException {
        io.metadata(handle, false);
        requireIdentity(expectedIdentity, io.identity(handle), "marker FileId changed before flush");
        if (expectedIdentity.volumeSerialNumber() != expectedParent.volumeSerialNumber()
            || !expectedNativePath.equals(io.finalPath(handle)))
            throw new IOException("FirstBinding6 marker HANDLE changed native parent before flush");
        io.flushChecked(handle);
        io.metadata(handle, false);
        if (!expectedIdentity.equals(io.identity(handle)) || !expectedNativePath.equals(io.finalPath(handle)))
            throw new IOException("FirstBinding6 marker identity or path changed during verification");
        WindowsCanonicalMarkerPublicationBarrier.requireExact(expectedBytes, io.readBackExact(handle));
    }

    private void verifyOldMarker(WindowsMarkerIo.Handle handle, String expectedNativePath,
            WindowsMarkerIo.FileIdentity expectedParent, ExpectedOldMarker expected) throws IOException {
        requireIdentity(expected.fileIdentity(), io.identity(handle), "old marker FileId differs from the source receipt");
        byte[] expectedBytes = expected.bytes();
        WindowsCanonicalMarkerPublicationBarrier.requireExact(expectedBytes,
            observeMarker(handle, expectedNativePath, expectedParent));
        flushAndReverifyMarker(handle, expectedNativePath, expectedParent, expected.fileIdentity(), expectedBytes);
    }

    private void requireStableParent(WindowsMarkerIo.Handle parent, String expectedNativePath,
            WindowsMarkerIo.FileIdentity expectedIdentity) throws IOException {
        io.metadata(parent, true);
        requireIdentity(expectedIdentity, io.identity(parent), "native parent FileId changed");
        if (!expectedNativePath.equals(io.finalPath(parent)))
            throw new IOException("FirstBinding6 native parent path changed while pinned");
    }

    private static void verifyPublicationAck(Request request,
            WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck ack) throws IOException {
        if (!request.canonicalPath().equals(ack.canonicalPath())
            || !request.operationUuid().equals(ack.operationUuid())
            || !request.newBindingUuid().equals(ack.bindingUuid())
            || ack.physicalFormat() != 2
            || !request.expectedParentIdentity().equals(ack.parentIdentity())
            || !PUBLICATION_PROTOCOL.equals(ack.durabilityProtocol()))
            throw new IOException("FirstBinding6 publication barrier returned an ACK for a different marker request");
    }

    private static void requireIdentity(WindowsMarkerIo.FileIdentity expected,
            WindowsMarkerIo.FileIdentity actual, String reason) throws IOException {
        if (!expected.equals(actual)) throw new IOException("FirstBinding6 " + reason);
    }

    private static String exactChild(String parentPath, String basename) {
        return (parentPath.endsWith("\\") ? parentPath : parentPath + "\\") + basename;
    }

    /** 与 native helper 固定的 operation-derived sibling 名称保持一致。 */
    public static String preservedOldBasename(UUID operation) {
        return ".floatingpod-adoption6-" + Objects.requireNonNull(operation) + ".old-binding.bin";
    }

    private static void rethrow(Throwable failure) throws IOException {
        if (failure instanceof IOException ioFailure) throw ioFailure;
        if (failure instanceof RuntimeException runtimeFailure) throw runtimeFailure;
        if (failure instanceof Error error) throw error;
        throw new IOException("FirstBinding6 marker adoption failed", failure);
    }

    /** 调用方必须先取得真实来源回执和 SQL PREPARED 身份，再构造此请求。 */
    public record Request(Path canonicalPath, UUID newBindingUuid, UUID operationUuid,
                          WindowsMarkerIo.FileIdentity expectedParentIdentity,
                          OldMarkerExpectation oldMarkerExpectation) {
        public Request {
            Objects.requireNonNull(canonicalPath, "canonical marker path");
            Objects.requireNonNull(newBindingUuid, "new binding UUID");
            Objects.requireNonNull(operationUuid, "operation UUID");
            Objects.requireNonNull(expectedParentIdentity, "captured parent identity");
            Objects.requireNonNull(oldMarkerExpectation, "explicit old-marker expectation");
            canonicalPath = canonicalPath.toAbsolutePath().normalize();
            if (canonicalPath.getParent() == null || canonicalPath.getFileName() == null
                || !CANONICAL_MARKER_BASENAME.equals(canonicalPath.getFileName().toString()))
                throw new IllegalArgumentException("FirstBinding6 requires the exact canonical world marker path");
            if (newBindingUuid.version() != 4)
                throw new IllegalArgumentException("FirstBinding6 new marker requires a random UUID v4 binding");
            if (isUnobservable(expectedParentIdentity))
                throw new IllegalArgumentException("FirstBinding6 requires an observable parent FileId");
            if (oldMarkerExpectation instanceof ExpectedOldMarker old
                && (old.fileIdentity().volumeSerialNumber() != expectedParentIdentity.volumeSerialNumber()
                    || isUnobservable(old.fileIdentity())))
                throw new IllegalArgumentException("FirstBinding6 old marker requires an observable FileId on the captured parent volume");
        }
    }

    public sealed interface OldMarkerExpectation permits ExpectedAbsence, ExpectedOldMarker { }

    public enum ExpectedAbsence implements OldMarkerExpectation {
        INSTANCE
    }

    private static boolean isUnobservable(WindowsMarkerIo.FileIdentity identity) {
        return Arrays.equals(identity.fileId128(), new byte[16]);
    }

    /** 旧 marker 的来源身份及 24 字节内容必须同时精确匹配。 */
    public static final class ExpectedOldMarker implements OldMarkerExpectation {
        private final WindowsMarkerIo.FileIdentity fileIdentity;
        private final byte[] bytes;

        public ExpectedOldMarker(WindowsMarkerIo.FileIdentity fileIdentity, byte[] bytes) {
            this.fileIdentity = Objects.requireNonNull(fileIdentity, "old marker FileId");
            if (bytes == null || bytes.length != 24)
                throw new IllegalArgumentException("Old marker expectation must contain exactly 24 bytes");
            this.bytes = bytes.clone();
        }

        public WindowsMarkerIo.FileIdentity fileIdentity() { return fileIdentity; }
        public byte[] bytes() { return bytes.clone(); }

        @Override public boolean equals(Object other) {
            return other instanceof ExpectedOldMarker that
                && fileIdentity.equals(that.fileIdentity) && Arrays.equals(bytes, that.bytes);
        }

        @Override public int hashCode() { return 31 * fileIdentity.hashCode() + Arrays.hashCode(bytes); }
        @Override public String toString() { return "ExpectedOldMarker[fileIdentity=" + fileIdentity.versionedIdentity() + ", bytes=24]"; }
    }

    public enum Outcome {
        OLD_MARKER_PRESERVED_NOW,
        OLD_MARKER_ALREADY_PRESERVED,
        OLD_MARKER_ABSENT,
        CANONICAL_ALREADY_NEW
    }

    /** 非 Ready DATA；其中 ACK 仅证明新 canonical marker 的 native publication 屏障完成。 */
    public record AdoptionData(Request request, Outcome outcome,
                               WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck markerPublicationAck) {
        public AdoptionData {
            Objects.requireNonNull(request);
            Objects.requireNonNull(outcome);
            Objects.requireNonNull(markerPublicationAck);
        }
    }
}
