package com.wi.floatingpod.server;

import java.io.IOException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import java.util.UUID;

/**
 * B1 的 Windows native publication 屏障。调用端负责 SQL 阶段、Fresh receipt、
 * 当前 boot custody 和 owned fence；此类不创建世界目录，也不授予 Ready。
 * 旧 Scala publish(binding,path) 缺 operation/source 身份，故这里不制造兼容 ACK。
 */
public final class WindowsCanonicalMarkerPublicationBarrier {
    private final WindowsMarkerIo io;
    public WindowsCanonicalMarkerPublicationBarrier(WindowsMarkerIo io) { this.io = Objects.requireNonNull(io); }
    public record Request(Path canonicalPath, UUID bindingUuid, int physicalFormat, UUID operationUuid,
                          WindowsMarkerIo.FileIdentity expectedParentIdentity) {
        public Request {
            Objects.requireNonNull(canonicalPath); Objects.requireNonNull(bindingUuid);
            Objects.requireNonNull(operationUuid); Objects.requireNonNull(expectedParentIdentity);
            canonicalPath = canonicalPath.toAbsolutePath().normalize();
            if (canonicalPath.getParent() == null || canonicalPath.getFileName() == null)
                throw new IllegalArgumentException("Canonical marker requires a parent and filename");
            WindowsMarkerIo.markerBytes(bindingUuid, physicalFormat);
            if (physicalFormat == 2 && bindingUuid.version() != 4)
                throw new IllegalArgumentException("Physical format 2 requires the supplied random UUID v4 token");
        }
    }
    /** 只能由全部 native 检查和 CloseHandle 成功后的 publication 流程构造。 */
    public static final class MarkerPublicationAck {
        private final Request request;
        private final WindowsMarkerIo.FileIdentity parent, file;
        private final String bytesSha256;
        private final boolean retainedExisting;
        private MarkerPublicationAck(Request request, WindowsMarkerIo.FileIdentity parent,
                                     WindowsMarkerIo.FileIdentity file, byte[] bytes, boolean retainedExisting) {
            this.request = request; this.parent = parent; this.file = file;
            this.bytesSha256 = sha256(bytes); this.retainedExisting = retainedExisting;
        }
        public UUID operationUuid() { return request.operationUuid(); }
        public UUID bindingUuid() { return request.bindingUuid(); }
        public int physicalFormat() { return request.physicalFormat(); }
        public Path canonicalPath() { return request.canonicalPath(); }
        public WindowsMarkerIo.FileIdentity parentIdentity() { return parent; }
        public WindowsMarkerIo.FileIdentity fileIdentity() { return file; }
        public String markerSha256() { return bytesSha256; }
        public boolean retainedExisting() { return retainedExisting; }
        public String durabilityProtocol() { return "WIN32_WRITE_FLUSH_MOVE_WRITE_THROUGH_CANONICAL_FLUSH_READ_CLOSE_V1"; }
    }
    private static String sha256(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (NoSuchAlgorithmException failure) { throw new IllegalStateException("Required SHA-256 unavailable", failure); }
    }
    static void requireExact(byte[] expected, byte[] actual) throws IOException {
        if (!Arrays.equals(expected, actual)) throw new IOException("Canonical marker token/format/bytes conflict");
    }
    private static void requireIdentity(WindowsMarkerIo.FileIdentity expected, WindowsMarkerIo.FileIdentity actual) throws IOException {
        if (!expected.equals(actual)) throw new IOException("Native marker file identity conflict");
    }
    static void requireSameNativeParent(String parent, String child) throws IOException {
        String prefix = parent.endsWith("\\") ? parent : parent + "\\";
        if (!child.startsWith(prefix) || child.substring(prefix.length()).indexOf('\\') >= 0
            || child.length() == prefix.length()) throw new IOException("Native marker resolved outside captured immediate parent");
    }
    private WindowsMarkerIo.Handle openCanonical(Request request) throws IOException {
        try {
            return io.open(request.canonicalPath(), WindowsMarkerIo.READ | WindowsMarkerIo.WRITE, 0,
                WindowsMarkerIo.OPEN_EXISTING, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, request.operationUuid());
        } catch (WindowsMarkerIo.NativeFailure failure) {
            if (failure.win32Error() == 2) return null; // 只有明确 FILE_NOT_FOUND 可以走 CREATE_NEW。
            throw failure;
        }
    }
    private WindowsMarkerIo.FileIdentity validateCanonical(WindowsMarkerIo.Handle handle, byte[] bytes,
                                                          String parentPath, WindowsMarkerIo.FileIdentity createdIdentity) throws IOException {
        io.metadata(handle, false);
        WindowsMarkerIo.FileIdentity identity = io.identity(handle);
        if (createdIdentity != null) requireIdentity(createdIdentity, identity);
        requireSameNativeParent(parentPath, io.finalPath(handle));
        // 异 token 不产生新的 durable ACK；相同 token 也必须重新执行真实 writable flush。
        requireExact(bytes, io.readBackExact(handle));
        io.flushChecked(handle);
        requireExact(bytes, io.readBackExact(handle));
        io.metadata(handle, false);
        requireIdentity(identity, io.identity(handle));
        return identity;
    }
    /** 只核已有 canonical，缺失或原ACK身份冲突明确失败；绝不重建、写入或移动文件。 */
    public MarkerPublicationAck rebarrierExisting(Request request, MarkerPublicationAck expected) throws IOException {
        Objects.requireNonNull(request); Objects.requireNonNull(expected);
        byte[] bytes = WindowsMarkerIo.markerBytes(request.bindingUuid(), request.physicalFormat());
        if (!request.equals(expected.request) || !sha256(bytes).equals(expected.markerSha256()))
            throw new IOException("Existing marker request differs from original actual ACK");
        WindowsMarkerIo.Handle parent = null, canonical = null;
        try {
            parent = io.directory(request.canonicalPath().getParent(), request.operationUuid());
            var parentIdentity = io.identity(parent);
            requireIdentity(request.expectedParentIdentity(), parentIdentity);
            requireIdentity(expected.parentIdentity(), parentIdentity);
            String parentPath = io.finalPath(parent);
            canonical = io.open(request.canonicalPath(), WindowsMarkerIo.READ | WindowsMarkerIo.WRITE, 0,
                WindowsMarkerIo.OPEN_EXISTING, WindowsMarkerIo.NORMAL | WindowsMarkerIo.OPEN_REPARSE, request.operationUuid());
            var file = validateCanonical(canonical, bytes, parentPath, expected.fileIdentity());
            requireIdentity(parentIdentity, io.identity(parent));
            if (!parentPath.equals(io.finalPath(parent))) throw new IOException("Existing marker parent moved; no ACK");
            io.closeChecked(canonical); canonical = null;
            io.closeChecked(parent); parent = null;
            return new MarkerPublicationAck(request, parentIdentity, file, bytes, true);
        } catch (Throwable primary) {
            WindowsMarkerIo.closeSuppressed(canonical, primary);
            WindowsMarkerIo.closeSuppressed(parent, primary);
            if (primary instanceof IOException failure) throw failure;
            if (primary instanceof RuntimeException failure) throw failure;
            if (primary instanceof Error failure) throw failure;
            throw WindowsMarkerIo.ioFailure("Existing marker native rebarrier", primary);
        }
    }
    public MarkerPublicationAck publish(Request request) throws IOException {
        Objects.requireNonNull(request);
        byte[] bytes = WindowsMarkerIo.markerBytes(request.bindingUuid(), request.physicalFormat());
        WindowsMarkerIo.Handle parent = null, temporary = null, canonical = null;
        boolean safeToDeleteTemporary = false;
        try {
            parent = io.directory(request.canonicalPath().getParent(), request.operationUuid());
            WindowsMarkerIo.FileIdentity parentIdentity = io.identity(parent);
            requireIdentity(request.expectedParentIdentity(), parentIdentity);
            String parentPath = io.finalPath(parent);
            WindowsMarkerIo.FileIdentity markerIdentity;
            boolean existing;
            canonical = openCanonical(request);
            if (canonical != null) {
                existing = true;
                markerIdentity = validateCanonical(canonical, bytes, parentPath, null);
            } else {
                if (request.physicalFormat() != 2) throw new IOException("Missing restored format 1 canonical marker; no new publication");
                Path temporaryPath = null;
                IOException lastCollision = null;
                for (int attempt = 0; attempt < 16 && temporary == null; attempt++) {
                    Path candidate = request.canonicalPath().resolveSibling(".fpwb-" + request.operationUuid() + "-" + UUID.randomUUID() + ".tmp");
                    try {
                        temporary = io.createMarkerHandle(candidate, request.operationUuid());
                        temporaryPath = candidate;
                    } catch (WindowsMarkerIo.NativeFailure collision) {
                        if (collision.win32Error() != 80 && collision.win32Error() != 183) throw collision;
                        lastCollision = collision;
                    }
                }
                if (temporary == null) throw new IOException("CREATE_NEW temporary collision bound exhausted", lastCollision);
                safeToDeleteTemporary = true;
                requireSameNativeParent(parentPath, io.finalPath(temporary));
                io.writeAll(temporary, bytes);
                io.metadata(temporary, false);
                WindowsMarkerIo.FileIdentity temporaryIdentity = io.identity(temporary);
                io.flushChecked(temporary);
                // 非 collision 的 Move 失败可能有不确定外部结果；不能按原路径盲删已发布对象。
                safeToDeleteTemporary = false;
                boolean collision = false;
                try { io.moveWriteThroughNoReplace(temporaryPath, request.canonicalPath(), request.operationUuid()); }
                catch (WindowsMarkerIo.NativeFailure moveFailure) {
                    if (moveFailure.win32Error() != 80 && moveFailure.win32Error() != 183) throw moveFailure;
                    collision = true;
                    safeToDeleteTemporary = true;
                }
                if (collision) {
                    io.deleteOwnedTemporary(temporary);
                    safeToDeleteTemporary = false;
                    io.closeChecked(temporary); temporary = null;
                    canonical = openCanonical(request);
                    if (canonical == null) throw new IOException("Colliding canonical marker disappeared; no ACK");
                    existing = true;
                    markerIdentity = validateCanonical(canonical, bytes, parentPath, null);
                } else {
                    // temp 的分享策略不允许 RW reopen；必须先检查其 CloseHandle。
                    io.closeChecked(temporary); temporary = null;
                    canonical = openCanonical(request);
                    if (canonical == null) throw new IOException("Published canonical marker missing; no ACK");
                    existing = false;
                    markerIdentity = validateCanonical(canonical, bytes, parentPath, temporaryIdentity);
                }
            }
            requireIdentity(parentIdentity, io.identity(parent));
            if (!parentPath.equals(io.finalPath(parent))) throw new IOException("Captured parent moved; no ACK");
            io.closeChecked(canonical); canonical = null;
            io.closeChecked(parent); parent = null;
            return new MarkerPublicationAck(request, parentIdentity, markerIdentity, bytes, existing);
        } catch (Throwable primary) {
            if (temporary != null && safeToDeleteTemporary && !temporary.closeAttempted) {
                try { io.deleteOwnedTemporary(temporary); } catch (Throwable cleanup) { primary.addSuppressed(cleanup); }
            }
            WindowsMarkerIo.closeSuppressed(canonical, primary);
            WindowsMarkerIo.closeSuppressed(temporary, primary);
            WindowsMarkerIo.closeSuppressed(parent, primary);
            if (primary instanceof IOException failure) throw failure;
            if (primary instanceof RuntimeException failure) throw failure;
            if (primary instanceof Error failure) throw failure;
            throw WindowsMarkerIo.ioFailure("publish operation=" + request.operationUuid(), primary);
        }
    }
}
