package com.wi.floatingpod.server;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;

/** 一次 region 读取的有限资源责任；失败 HANDLE/Arena 仍由注册槽持有。 */
public final class RegionReadSession {
    public enum State { REGISTERED, INITIALIZING, INITIALIZED, READING, CLEANING, CLOSED, CLOSE_FAILED_RETAINED }

    private final CatalogReadResponsibility responsibility;
    private final ReadOnlyRegionCatalog reader;
    private final Thread ownerThread;
    private final UUID attempt;
    private final Path regionPath;
    private State state = State.REGISTERED;
    private boolean initAttempted;
    private boolean ioConstructionSucceeded;
    private Arena exactLibraryArena;
    private WindowsMarkerIo initializedIo;
    private WindowsMarkerIo.Handle h1;
    private WindowsMarkerIo.Handle h2;
    private Throwable originalPrimary;
    private Throwable h1CloseFailure;
    private Throwable h2CloseFailure;
    private Throwable arenaCloseFailure;
    private boolean h1CloseAttempted;
    private boolean h1Closed;
    private boolean h2CloseAttempted;
    private boolean h2Closed;
    private boolean arenaCloseAttempted;
    private boolean arenaClosed;
    private boolean cleanupStarted;
    private WindowsMarkerIo.RegionObservation h1InitialObservation;
    private WindowsMarkerIo.RegionObservation h1FinalObservation;
    private WindowsMarkerIo.RegionObservation h2InitialObservation;
    private WindowsMarkerIo.RegionObservation h2FinalObservation;
    private byte[] h1InitialHeader;
    private byte[] h1RecheckedHeader;
    private byte[] h2Header;

    RegionReadSession(
        CatalogReadResponsibility responsibility,
        ReadOnlyRegionCatalog reader,
        Thread ownerThread,
        UUID attempt,
        Path regionPath
    ) {
        this.responsibility = responsibility;
        this.reader = reader;
        this.ownerThread = ownerThread;
        this.attempt = attempt;
        this.regionPath = regionPath;
    }

    void beginWindowsIoInitialization() {
        requireOwner();
        if (responsibility.registeredSession() != this || state != State.REGISTERED || initAttempted
            || exactLibraryArena != null || initializedIo != null || h1 != null || h2 != null) {
            throw new IllegalStateException("Only a pristine registered region session may initialize native IO");
        }
        initAttempted = true;
        state = State.INITIALIZING;
    }

    void registerLibraryArena(Arena exactArena) {
        requireOwner();
        if (state != State.INITIALIZING || !initAttempted || exactLibraryArena != null || initializedIo != null) {
            throw new IllegalStateException("The original region library Arena may be registered exactly once");
        }
        exactLibraryArena = exactArena;
    }

    void initializeWindowsIo() throws IOException {
        requireOwner();
        try {
            WindowsMarkerIo exactIo = new WindowsMarkerIo(this);
            initializedIo = exactIo;
            ioConstructionSucceeded = true;
            state = State.INITIALIZED;
        } catch (IOException | RuntimeException | Error failure) {
            recordInitializationFailure(failure);
            throw failure;
        }
    }

    void recordInitializationFailure(Throwable failure) {
        requireOwner();
        recordPrimary(failure);
    }

    void beginRead() {
        requireOwner();
        if (state != State.INITIALIZED || !ioConstructionSucceeded || initializedIo == null || exactLibraryArena == null) {
            throw new IllegalStateException("Region reads require the fully initialized registered Windows IO");
        }
        state = State.READING;
    }

    void registerRegionHandle(WindowsMarkerIo.Handle handle, boolean secondPathHandle) {
        requireOwner();
        if (!ioConstructionSucceeded || initializedIo == null || state != State.READING) {
            throw new IllegalStateException("A region HANDLE may only be registered during its initialized read");
        }
        if (secondPathHandle) {
            if (h1 == null || h2 != null) throw new IllegalStateException("The H2 path HANDLE must follow exactly one H1 HANDLE");
            h2 = handle;
        } else {
            if (h1 != null || h2 != null) throw new IllegalStateException("The H1 file HANDLE may be registered exactly once");
            h1 = handle;
        }
    }

    void requireRegionHandle(WindowsMarkerIo.Handle exactHandle) {
        requireOwner();
        if (exactHandle != h1 && exactHandle != h2) {
            throw new IllegalStateException("Native region operation used a HANDLE outside this exact registered session");
        }
    }

    void recordH1Initial(WindowsMarkerIo.RegionObservation observation, byte[] header) {
        requireOwner();
        if (h1InitialObservation != null || h1InitialHeader != null) throw new IllegalStateException("H1 initial observation already recorded");
        h1InitialObservation = observation;
        h1InitialHeader = header.clone();
    }

    void recordH1Final(WindowsMarkerIo.RegionObservation observation, byte[] header) {
        requireOwner();
        if (h1FinalObservation != null || h1RecheckedHeader != null) throw new IllegalStateException("H1 final observation already recorded");
        h1FinalObservation = observation;
        h1RecheckedHeader = header.clone();
    }

    void recordH2Initial(WindowsMarkerIo.RegionObservation observation, byte[] header) {
        requireOwner();
        if (h2InitialObservation != null || h2Header != null) throw new IllegalStateException("H2 observation already recorded");
        h2InitialObservation = observation;
        h2Header = header.clone();
    }

    void recordH2Final(WindowsMarkerIo.RegionObservation observation) {
        requireOwner();
        if (h2FinalObservation != null) throw new IllegalStateException("H2 final observation already recorded");
        h2FinalObservation = observation;
    }

    void handleCloseAttempted(WindowsMarkerIo.Handle exactHandle) {
        requireRegionHandle(exactHandle);
        if (exactHandle == h2) {
            if (h2CloseAttempted) throw new IllegalStateException("H2 CloseHandle attempt already recorded");
            h2CloseAttempted = true;
        } else {
            if (h1CloseAttempted) throw new IllegalStateException("H1 CloseHandle attempt already recorded");
            h1CloseAttempted = true;
        }
    }

    void handleCloseSucceeded(WindowsMarkerIo.Handle exactHandle) {
        requireRegionHandle(exactHandle);
        if (exactHandle == h2) {
            if (!h2CloseAttempted || h2Closed) throw new IllegalStateException("H2 close result has no unique prior attempt");
            h2Closed = true;
        } else {
            if (!h1CloseAttempted || h1Closed) throw new IllegalStateException("H1 close result has no unique prior attempt");
            h1Closed = true;
        }
    }

    void handleCloseFailed(WindowsMarkerIo.Handle exactHandle, Throwable failure) {
        requireRegionHandle(exactHandle);
        if (exactHandle == h2) {
            if (!h2CloseAttempted || h2Closed || h2CloseFailure != null) throw new IllegalStateException("H2 close failure is inconsistent");
            h2CloseFailure = failure;
        } else {
            if (!h1CloseAttempted || h1Closed || h1CloseFailure != null) throw new IllegalStateException("H1 close failure is inconsistent");
            h1CloseFailure = failure;
        }
    }

    void closeInitializedArena(
        WindowsMarkerIo exactIo,
        Arena exactArena,
        WindowsMarkerIo.RegionArenaCloseAttempt closeAttempt
    ) throws IOException {
        requireOwner();
        if (!ioConstructionSucceeded || initializedIo != exactIo || exactLibraryArena != exactArena
            || arenaCloseAttempted || !h1Closed && h1 != null || !h2Closed && h2 != null) {
            throw new IllegalStateException("Only the exact initialized session may make its single Arena close attempt after HANDLE closure");
        }
        arenaCloseAttempted = true;
        try {
            closeAttempt.close(exactArena);
            arenaClosed = true;
        } catch (IOException | RuntimeException | Error failure) {
            arenaCloseFailure = failure;
            throw failure;
        }
    }

    private void closePartialInitializationArena() throws IOException {
        requireOwner();
        if (ioConstructionSucceeded || initializedIo != null || exactLibraryArena == null || arenaCloseAttempted) {
            throw new IllegalStateException("Partial initialization cleanup does not own this Arena close attempt");
        }
        arenaCloseAttempted = true;
        try {
            responsibility.arenaCloseAttempt().close(exactLibraryArena);
            arenaClosed = true;
        } catch (IOException | RuntimeException | Error failure) {
            arenaCloseFailure = failure;
            throw failure;
        }
    }

    Throwable cleanupAfter(Throwable primary) {
        requireOwner();
        if (cleanupStarted) throw new IllegalStateException("Region session cleanup is single-entry and cannot retry native closes");
        cleanupStarted = true;
        recordPrimary(primary);
        state = State.CLEANING;
        if (h2 != null && !h2.closeAttempted) {
            try { h2.close(); }
            catch (IOException | RuntimeException | Error failure) { recordPrimary(failure); }
        }
        if (h1 != null && !h1.closeAttempted) {
            try { h1.close(); }
            catch (IOException | RuntimeException | Error failure) { recordPrimary(failure); }
        }
        boolean handlesClosed = (h1 == null || h1Closed) && (h2 == null || h2Closed);
        if (handlesClosed && initializedIo != null && !arenaCloseAttempted) {
            try { initializedIo.close(); }
            catch (IOException | RuntimeException | Error failure) { recordPrimary(failure); }
        } else if (handlesClosed && initializedIo == null && exactLibraryArena != null && !arenaCloseAttempted) {
            try { closePartialInitializationArena(); }
            catch (IOException | RuntimeException | Error failure) { recordPrimary(failure); }
        }
        boolean unresolvedHandle = (h1 != null && !h1Closed) || (h2 != null && !h2Closed);
        boolean unresolvedArena = exactLibraryArena != null && !arenaClosed;
        if (unresolvedHandle || unresolvedArena) {
            state = State.CLOSE_FAILED_RETAINED;
        } else {
            state = State.CLOSED;
            responsibility.releaseClosedSession(this);
        }
        return originalPrimary;
    }

    private void recordPrimary(Throwable failure) {
        if (failure == null) return;
        if (originalPrimary == null) originalPrimary = failure;
        else if (originalPrimary != failure && Arrays.stream(originalPrimary.getSuppressed()).noneMatch(item -> item == failure))
            originalPrimary.addSuppressed(failure);
    }

    private void requireOwner() {
        if (Thread.currentThread() != ownerThread || responsibility.ownerThread() != ownerThread
            || responsibility.registeredSession() != this) {
            throw new IllegalStateException("Exact region session owner and responsibility slot are required");
        }
    }

    WindowsMarkerIo.RegionInitFault initFault() { return responsibility.initFault(); }
    WindowsMarkerIo.RegionArenaCloseAttempt arenaCloseAttempt() { return responsibility.arenaCloseAttempt(); }
    WindowsMarkerIo.RegionHandleCloseAttempt handleCloseAttempt() { return responsibility.handleCloseAttempt(); }
    public State state() { return state; }
    public UUID attempt() { return attempt; }
    Path regionPath() { return regionPath; }
    public Thread ownerThread() { return ownerThread; }
    ReadOnlyRegionCatalog reader() { return reader; }
    Arena exactLibraryArena() { return exactLibraryArena; }
    WindowsMarkerIo initializedIo() { return initializedIo; }
    WindowsMarkerIo.Handle h1() { return h1; }
    WindowsMarkerIo.Handle h2() { return h2; }
    public Throwable primary() { return originalPrimary; }
    Throwable originalPrimary() { return originalPrimary; }
    Throwable h1CloseFailure() { return h1CloseFailure; }
    Throwable h2CloseFailure() { return h2CloseFailure; }
    Throwable arenaCloseFailure() { return arenaCloseFailure; }
    boolean ioConstructionSucceeded() { return ioConstructionSucceeded; }
    boolean h1CloseAttempted() { return h1CloseAttempted; }
    boolean h1Closed() { return h1Closed; }
    boolean h2CloseAttempted() { return h2CloseAttempted; }
    boolean h2Closed() { return h2Closed; }
    boolean arenaCloseAttempted() { return arenaCloseAttempted; }
    boolean arenaClosed() { return arenaClosed; }
    WindowsMarkerIo.RegionObservation h1InitialObservation() { return h1InitialObservation; }
    WindowsMarkerIo.RegionObservation h1FinalObservation() { return h1FinalObservation; }
    WindowsMarkerIo.RegionObservation h2InitialObservation() { return h2InitialObservation; }
    WindowsMarkerIo.RegionObservation h2FinalObservation() { return h2FinalObservation; }
    byte[] h1InitialHeader() { return h1InitialHeader == null ? null : h1InitialHeader.clone(); }
    byte[] h1RecheckedHeader() { return h1RecheckedHeader == null ? null : h1RecheckedHeader.clone(); }
    byte[] h2Header() { return h2Header == null ? null : h2Header.clone(); }
}
