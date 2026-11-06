package com.wi.floatingpod.server;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

/** 单个 owner 线程持有的一次 region 读取责任；失败资源仍保留在唯一槽位。 */
public final class CatalogReadResponsibility {
    private final Thread ownerThread;
    private final WindowsMarkerIo.RegionInitFault initFault;
    private final WindowsMarkerIo.RegionArenaCloseAttempt arenaCloseAttempt;
    private final WindowsMarkerIo.RegionHandleCloseAttempt handleCloseAttempt;
    private ReadOnlyRegionCatalog reader;
    private RegionReadSession session;

    public CatalogReadResponsibility(Thread ownerThread) {
        this(ownerThread, () -> { }, Arena::close, (io, handle) -> { });
    }

    CatalogReadResponsibility(
        Thread ownerThread,
        WindowsMarkerIo.RegionInitFault initFault,
        WindowsMarkerIo.RegionArenaCloseAttempt arenaCloseAttempt,
        WindowsMarkerIo.RegionHandleCloseAttempt handleCloseAttempt
    ) {
        this.ownerThread = Objects.requireNonNull(ownerThread, "ownerThread");
        this.initFault = Objects.requireNonNull(initFault, "initFault");
        this.arenaCloseAttempt = Objects.requireNonNull(arenaCloseAttempt, "arenaCloseAttempt");
        this.handleCloseAttempt = Objects.requireNonNull(handleCloseAttempt, "handleCloseAttempt");
    }

    public Thread ownerThread() {
        return ownerThread;
    }

    /** 返回仍登记在本实例中的同一失败 session；读取不会释放或转移其资源责任。 */
    public RegionReadSession retainedFailure(ReadOnlyRegionCatalog exactReader, UUID exactAttempt) {
        requireOwnerThread();
        Objects.requireNonNull(exactReader, "reader");
        Objects.requireNonNull(exactAttempt, "attempt");
        if (reader != exactReader) {
            throw new IllegalStateException("Retained session lookup requires the exact attached reader");
        }
        RegionReadSession current = session;
        if (current == null) return null;
        if (current.reader() != exactReader || !current.attempt().equals(exactAttempt)
            || current.ownerThread() != ownerThread) {
            throw new IllegalStateException("Retained session identity does not match the exact reader, attempt, and owner");
        }
        if (current.state() != RegionReadSession.State.CLOSE_FAILED_RETAINED) return null;
        return current;
    }

    /** true 表示当前单槽仍有任何 session，包括正在读或尚未收口的失败 session。 */
    public boolean hasUnresolvedSession() {
        requireOwnerThread();
        return session != null;
    }

    /** 读取当前单槽的关联 attempt token；不暴露或转移 session 资源。 */
    public UUID registeredAttempt(ReadOnlyRegionCatalog exactReader) {
        requireOwnerThread();
        Objects.requireNonNull(exactReader, "reader");
        if (reader != exactReader) {
            throw new IllegalStateException("Attempt lookup requires the exact attached reader");
        }
        return session == null ? null : session.attempt();
    }

    void attachReader(ReadOnlyRegionCatalog exactReader) {
        requireOwnerThread();
        Objects.requireNonNull(exactReader, "reader");
        if (reader != null || session != null) {
            throw new IllegalStateException("A catalog reader may be attached exactly once before registration");
        }
        reader = exactReader;
    }

    void requireAttachedReader(ReadOnlyRegionCatalog exactReader, Thread scanOwner) {
        requireOwnerThread();
        if (reader != exactReader || scanOwner != ownerThread) {
            throw new IllegalStateException("The exact catalog reader and owner thread must be registered before native access");
        }
    }

    RegionReadSession register(ReadOnlyRegionCatalog exactReader, Thread scanOwner, UUID attempt, Path regionPath) {
        requireAttachedReader(exactReader, scanOwner);
        if (session != null) {
            throw new IllegalStateException("The prior region read session remains owned and unresolved");
        }
        RegionReadSession registered = new RegionReadSession(
            this, exactReader, ownerThread, Objects.requireNonNull(attempt), Objects.requireNonNull(regionPath));
        session = registered;
        return registered;
    }

    void releaseClosedSession(RegionReadSession exactSession) {
        requireOwnerThread();
        if (session != exactSession || exactSession.state() != RegionReadSession.State.CLOSED) {
            throw new IllegalStateException("Only the exact fully closed region session may release its slot");
        }
        session = null;
    }

    RegionReadSession registeredSession() {
        requireOwnerThread();
        return session;
    }

    WindowsMarkerIo.RegionInitFault initFault() {
        return initFault;
    }

    WindowsMarkerIo.RegionArenaCloseAttempt arenaCloseAttempt() {
        return arenaCloseAttempt;
    }

    WindowsMarkerIo.RegionHandleCloseAttempt handleCloseAttempt() {
        return handleCloseAttempt;
    }

    private void requireOwnerThread() {
        if (Thread.currentThread() != ownerThread) {
            throw new IllegalStateException("Catalog read responsibility owner thread mismatch");
        }
    }
}
