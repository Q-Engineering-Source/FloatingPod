package com.wi.floatingpod.server;

import com.wi.floatingpod.storage.mysql.MySqlFormat3ReadBudget;
import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.source.SourceForestAllocationBudget;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Windows x64 同步文件 API；句柄和 confined arena 只属于创建它的线程。 */
public final class WindowsMarkerIo implements AutoCloseable {
    static final int READ = 0x80000000, WRITE = 0x40000000, DELETE = 0x10000;
    static final int SHARE_READ = 1, SHARE_WRITE = 2, SHARE_DELETE = 4;
    static final int CREATE_NEW = 1, OPEN_EXISTING = 3;
    static final int NORMAL = 0x80, BACKUP = 0x02000000, OPEN_REPARSE = 0x00200000;
    static final int WRITE_THROUGH = 8;
    private static final int SOURCE_ARTIFACT_TRANSFER_BYTES = 65_536;
    private static final long SOURCE_ARTIFACT_SCRATCH_CHARGE = SOURCE_ARTIFACT_TRANSFER_BYTES;
    private static final long SOURCE_ARTIFACT_TRANSFER_COUNT_CHARGE = Integer.BYTES;
    private static final long SOURCE_ARTIFACT_OBSERVATION_CHARGE = 1_024L;
    private static final long SOURCE_ARTIFACT_ATTEMPT_CHARGE = 256L;
    private static final long SOURCE_PIN_PATH_WORK_CHARGE = 393_216L;
    private static final long SOURCE_LIBRARY_ARENA_CHARGE = 16_384L;
    private static final long SOURCE_ARTIFACT_TOTAL_CHARGE = SOURCE_ARTIFACT_SCRATCH_CHARGE
        + SOURCE_ARTIFACT_TRANSFER_COUNT_CHARGE + SOURCE_ARTIFACT_OBSERVATION_CHARGE
        + SOURCE_ARTIFACT_ATTEMPT_CHARGE;
    private static final int MAX_PATH_WITH_TERMINATOR = 260;
    // CreateDirectoryW 保留追加 8.3 文件名的 12 字符空间。
    private static final int MAX_DIRECTORY_PATH_WITH_TERMINATOR = MAX_PATH_WITH_TERMINATOR - 12;
    private static final int MAX_WIN32_PATH_CODE_UNITS = 32766;
    private static final String EXTENDED_PATH_PREFIX = "\\\\?\\";
    private static final String EXTENDED_UNC_PREFIX = "\\\\?\\UNC\\";
    private static final String EXTENDED_VOLUME_PREFIX = "\\\\?\\Volume{";
    static final MemoryLayout FILE_ID = MemoryLayout.structLayout(
        ValueLayout.JAVA_LONG.withName("VolumeSerialNumber"),
        MemoryLayout.sequenceLayout(16, ValueLayout.JAVA_BYTE).withName("FileId"));
    static final MemoryLayout STANDARD = MemoryLayout.structLayout(
        ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT,
        ValueLayout.JAVA_BYTE, ValueLayout.JAVA_BYTE, MemoryLayout.paddingLayout(2));
    static final MemoryLayout ATTRIBUTE_TAG = MemoryLayout.structLayout(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT);
    static final MemoryLayout BASIC = MemoryLayout.structLayout(
        ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG,
        ValueLayout.JAVA_INT, MemoryLayout.paddingLayout(4));
    // WIN32_FIND_DATAW is 592 bytes on the supported Windows x64 ABI. FILETIME is two
    // DWORDs (four-byte alignment); only attributes, length, and the long filename are consumed.
    static final MemoryLayout WIN32_FIND_DATAW = MemoryLayout.structLayout(
        ValueLayout.JAVA_INT.withName("dwFileAttributes"),
        MemoryLayout.sequenceLayout(6, ValueLayout.JAVA_INT).withName("times"),
        ValueLayout.JAVA_INT.withName("nFileSizeHigh"), ValueLayout.JAVA_INT.withName("nFileSizeLow"),
        ValueLayout.JAVA_INT.withName("dwReserved0"), ValueLayout.JAVA_INT.withName("dwReserved1"),
        MemoryLayout.sequenceLayout(260, ValueLayout.JAVA_CHAR).withName("cFileName"),
        MemoryLayout.sequenceLayout(14, ValueLayout.JAVA_CHAR).withName("cAlternateFileName"));
    static final long WIN32_FIND_DATAW_SIZE = 592;
    static final long FIND_ATTRIBUTES_OFFSET = WIN32_FIND_DATAW.byteOffset(MemoryLayout.PathElement.groupElement("dwFileAttributes"));
    static final long FIND_SIZE_HIGH_OFFSET = WIN32_FIND_DATAW.byteOffset(MemoryLayout.PathElement.groupElement("nFileSizeHigh"));
    static final long FIND_SIZE_LOW_OFFSET = WIN32_FIND_DATAW.byteOffset(MemoryLayout.PathElement.groupElement("nFileSizeLow"));
    static final long FIND_NAME_OFFSET = WIN32_FIND_DATAW.byteOffset(MemoryLayout.PathElement.groupElement("cFileName"));
    static final MemoryLayout PROCESSENTRY32W = MemoryLayout.structLayout(
        ValueLayout.JAVA_INT.withName("dwSize"),
        ValueLayout.JAVA_INT.withName("cntUsage"),
        ValueLayout.JAVA_INT.withName("th32ProcessID"),
        MemoryLayout.paddingLayout(4),
        ValueLayout.JAVA_LONG.withName("th32DefaultHeapID"),
        ValueLayout.JAVA_INT.withName("th32ModuleID"),
        ValueLayout.JAVA_INT.withName("cntThreads"),
        ValueLayout.JAVA_INT.withName("th32ParentProcessID"),
        ValueLayout.JAVA_INT.withName("pcPriClassBase"),
        ValueLayout.JAVA_INT.withName("dwFlags"),
        MemoryLayout.sequenceLayout(260, ValueLayout.JAVA_CHAR).withName("szExeFile"),
        MemoryLayout.paddingLayout(4));
    static final long PROCESSENTRY32W_SIZE = 568;
    static final long PROCESSENTRY32W_DW_SIZE_OFFSET = PROCESSENTRY32W.byteOffset(MemoryLayout.PathElement.groupElement("dwSize"));
    static final long PROCESSENTRY32W_PID_OFFSET = PROCESSENTRY32W.byteOffset(MemoryLayout.PathElement.groupElement("th32ProcessID"));
    static final long PROCESSENTRY32W_IMAGE_OFFSET = PROCESSENTRY32W.byteOffset(MemoryLayout.PathElement.groupElement("szExeFile"));
    private static final int ERROR_INVALID_PARAMETER = 87;
    private static final int ERROR_NO_MORE_FILES = 18;
    private static final int ERROR_FILE_NOT_FOUND = 2;
    private static final int TH32CS_SNAPPROCESS = 0x00000002;
    private final Thread owner = Thread.currentThread();
    private final Arena libraryArena;
    private final SourceIoHolder sourceIoHolder;
    private final Map<String, MethodHandle> calls = new HashMap<>();
    private final Map<Handle, Boolean> owned = new IdentityHashMap<>();
    private final Map<FindHandle, Boolean> ownedFinds = new IdentityHashMap<>();
    private final MemoryLayout callState;
    private final long errorOffset;
    private final RegionReadSession regionSession;
    private final RegionArenaCloseAttempt arenaCloseAttempt;
    private final RegionHandleCloseAttempt handleCloseAttempt;
    private SourceForestAllocationBudget activeSourcePathAllocation;
    private int activeSourcePathDepth;
    private boolean closed;

    public WindowsMarkerIo() throws IOException {
        this.regionSession = null;
        this.sourceIoHolder = null;
        this.arenaCloseAttempt = Arena::close;
        this.handleCloseAttempt = (io, handle) -> { };
        requireWindowsNativeConfiguration();
        this.libraryArena = Arena.ofConfined();
        try {
            Kernel32Bindings bindings = linkKernel32(libraryArena, NO_REGION_INIT_FAULT);
            this.callState = bindings.callState();
            this.errorOffset = bindings.errorOffset();
            this.calls.putAll(bindings.calls());
        } catch (IOException | RuntimeException | Error failure) {
            try { libraryArena.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw ioFailure("link Kernel32", failure);
        }
    }

    WindowsMarkerIo(RegionReadSession session) throws IOException {
        this.regionSession = Objects.requireNonNull(session);
        this.sourceIoHolder = null;
        this.arenaCloseAttempt = session.arenaCloseAttempt();
        this.handleCloseAttempt = session.handleCloseAttempt();
        session.beginWindowsIoInitialization();
        requireWindowsNativeConfiguration();
        Arena arena = Arena.ofConfined();
        session.registerLibraryArena(arena);
        this.libraryArena = arena;
        Kernel32Bindings bindings;
        try {
            bindings = linkKernel32(arena, session.initFault());
        } catch (IOException | RuntimeException | Error failure) {
            session.recordInitializationFailure(failure);
            throw failure;
        }
        this.callState = bindings.callState();
        this.errorOffset = bindings.errorOffset();
        this.calls.putAll(bindings.calls());
    }

    private WindowsMarkerIo(SourceIoHolder holder, Arena arena, Kernel32Bindings bindings) {
        this.regionSession = null;
        this.sourceIoHolder = Objects.requireNonNull(holder);
        this.arenaCloseAttempt = Arena::close;
        this.handleCloseAttempt = (io, handle) -> { };
        this.libraryArena = Objects.requireNonNull(arena);
        this.callState = bindings.callState();
        this.errorOffset = bindings.errorOffset();
        this.calls.putAll(bindings.calls());
    }

    /**
     * Budgeted SOURCE native realm. The original IO owner retains this holder before calling initialize();
     * if Kernel32 linking fails, the confined library Arena stays reachable and charged here for explicit cleanup.
     */
    public static final class SourceIoHolder implements AutoCloseable {
        private enum State { RESERVED, INITIALIZING, READY, FAILED, CLOSING, CLOSE_FAILED, CLOSED }
        private final Thread owner = Thread.currentThread();
        private final SourceLimitProfile profile;
        private final SourceForestAllocationBudget allocation;
        private final RegionArenaCloseAttempt arenaCloseAttempt = Arena::close;
        private State state = State.RESERVED;
        private Arena libraryArena;
        private WindowsMarkerIo io;
        private Throwable failure;
        private boolean reservationHeld;
        private boolean closeAttempted;
        private boolean arenaCloseAttempted;
        private SourceRegionFilePin unresolvedRegionReadPin;
        private Arena unresolvedRegionReadArena;
        private byte[] unresolvedRegionReadBody;
        private SourceForestAllocationBudget.Account unresolvedRegionReadAllocation;
        private long unresolvedRegionReadCharge;
        private Throwable unresolvedRegionReadCause;
        private boolean unresolvedRegionReadCloseAttempted;

        private SourceIoHolder(SourceLimitProfile profile, SourceForestAllocationBudget allocation) {
            this.profile = profile;
            this.allocation = allocation;
        }

        private void checkOwner() {
            if (Thread.currentThread() != owner) throw new IllegalStateException("SOURCE native holder owner thread mismatch");
        }

        public static SourceIoHolder reserve(SourceLimitProfile exactProfile,
                SourceForestAllocationBudget.Account originalAllocation) throws IOException {
            Objects.requireNonNull(exactProfile, "exact SOURCE limit profile");
            Objects.requireNonNull(originalAllocation, "original SOURCE allocation Account");
            if (originalAllocation.exactProfile() != exactProfile)
                throw new IOException("SOURCE native holder requires the exact original limit profile instance");
            originalAllocation.reserveTemporary(SOURCE_LIBRARY_ARENA_CHARGE,
                "SOURCE confined Kernel32 library Arena retained holder BEFORE holder allocation/native allocation/link");
            try {
                SourceIoHolder holder = new SourceIoHolder(exactProfile, originalAllocation);
                holder.reservationHeld = true;
                return holder;
            } catch (Throwable primary) {
                try { originalAllocation.releaseTemporary(SOURCE_LIBRARY_ARENA_CHARGE,
                    "SOURCE native holder construction failed before native allocation/link"); }
                catch (Throwable cleanup) {
                    if (cleanup != primary) primary.addSuppressed(cleanup);
                }
                throw preserveIoFailure("construct precharged SOURCE native holder", primary);
            }
        }

        /**
         * FirstBinding6 的原 Full27 root 直接承担 pre-Main native 工作；不冒充已绑定的 SOURCE Account。
         * 此 factory 只提供有界 IO holder，不签发 source custody、writer 或普通读取资格。
         */
        public static SourceIoHolder reserveDbFirst(MySqlFormat3ReadBudget originalRoot) throws IOException {
            Objects.requireNonNull(originalRoot);
            if (originalRoot.isClosed() || originalRoot.isScopedReadBudget())
                throw new IOException("FirstBinding6 native IO requires the original complete Full27 root");
            originalRoot.requireDbFirstNativeRoot();
            SourceLimitProfile exact = originalRoot.profile();
            if (originalRoot.profile() != exact)
                throw new IOException("FirstBinding6 native profile/root identity differs");
            originalRoot.reserveTemporaryMemory(SOURCE_LIBRARY_ARENA_CHARGE,
                "FirstBinding6 confined Kernel32 holder BEFORE allocation/link");
            try {
                var allocation = new DbFirstAllocation(originalRoot);
                var holder = new SourceIoHolder(exact, allocation);
                holder.reservationHeld = true;
                return holder;
            } catch (Throwable primary) {
                try { originalRoot.releaseTemporaryMemory(SOURCE_LIBRARY_ARENA_CHARGE,
                    "unallocated FirstBinding6 native holder"); }
                catch (Throwable cleanup) { if (cleanup != primary) primary.addSuppressed(cleanup); }
                throw preserveIoFailure("reserve FirstBinding6 native holder", primary);
            }
        }

        public SourceForestAllocationBudget dbFirstAllocation() throws IOException {
            checkOwner();
            if (!(allocation instanceof DbFirstAllocation actual)
                || (state != State.RESERVED && state != State.READY))
                throw new IOException("FirstBinding6 allocation is unavailable on a SOURCE Account holder");
            actual.requireLive();
            return actual;
        }

        private static final class DbFirstAllocation implements SourceForestAllocationBudget {
            private final MySqlFormat3ReadBudget root;
            private final Thread owner = Thread.currentThread();
            private long held;
            private boolean retired;
            private DbFirstAllocation(MySqlFormat3ReadBudget root) {
                this.root = root;
                this.held = SOURCE_LIBRARY_ARENA_CHARGE;
            }
            private void requireLive() throws IOException {
                if (retired || Thread.currentThread() != owner || root.isClosed() || root.isScopedReadBudget())
                    throw new IOException("Original FirstBinding6 native root/worker required");
                root.requireDbFirstNativeRoot();
            }
            @Override public SourceLimitProfile exactProfile() { return root.profile(); }
            @Override public void reserveTemporary(long bytes, String context) throws IOException {
                requireLive();
                if (bytes < 0) throw new IOException("Negative FirstBinding6 native reservation");
                long next = Math.addExact(held, bytes);
                root.reserveTemporaryMemory(bytes, context); held = next;
            }
            @Override public void releaseTemporary(long bytes, String context) throws IOException {
                requireLive();
                if (bytes < 0 || bytes > held) throw new IOException("Foreign FirstBinding6 native reservation release");
                root.releaseTemporaryMemory(bytes, context); held -= bytes;
            }
        }

        /** Must be called only after the original IO job has retained this holder. */
        public WindowsMarkerIo initialize() throws IOException {
            checkOwner();
            if (state != State.RESERVED) throw new IllegalStateException("SOURCE native holder initialization is one-shot");
            state = State.INITIALIZING;
            try {
                requireWindowsNativeConfiguration();
                libraryArena = Arena.ofConfined();
                Kernel32Bindings bindings = linkKernel32(libraryArena, NO_REGION_INIT_FAULT);
                io = new WindowsMarkerIo(this, libraryArena, bindings);
                state = State.READY;
                return io;
            } catch (IOException | RuntimeException | Error primary) {
                recordFailure(primary);
                state = State.FAILED;
                throw preserveIoFailure("initialize budgeted SOURCE native realm", primary);
            }
        }

        public WindowsMarkerIo io() throws IOException {
            checkOwner();
            if (state != State.READY || io == null || unresolvedRegionReadArena != null)
                throw new IOException("A successfully initialized exact SOURCE native realm without unresolved owned workspaces is required",
                    unresolvedRegionReadCause != null ? unresolvedRegionReadCause : failure);
            return io;
        }


        public Optional<Throwable> failure() {
            checkOwner();
            return Optional.ofNullable(unresolvedRegionReadCause != null ? unresolvedRegionReadCause : failure);
        }

        public boolean ready() {
            checkOwner();
            return state == State.READY && io != null && unresolvedRegionReadArena == null;
        }

        private void recordFailure(Throwable next) {
            if (failure == null) failure = next;
            else if (failure != next) failure.addSuppressed(next);
        }

        private void requireAllocation(SourceLimitProfile exactProfile,
                SourceForestAllocationBudget exactAllocation) throws IOException {
            checkOwner();
            if (unresolvedRegionReadArena != null)
                throw new IOException("Original SOURCE owner retains an unclosed region read Arena/body and its full charge",
                    unresolvedRegionReadCause);
            if (state != State.READY || io == null || profile != exactProfile || allocation != exactAllocation)
                throw new IOException("Budgeted SOURCE operation requires this ready native holder and its exact original profile/Account", failure);
        }

        /** Transfers an Arena whose one allowed close attempt failed into the original retained IO owner. */
        private void retainUnclosedRegionRead(SourceRegionFilePin exactPin, Arena exactArena, byte[] exactBody,
                SourceForestAllocationBudget.Account exactAllocation, long exactCharge, Throwable exactCause) {
            checkOwner();
            if (unresolvedRegionReadArena != null)
                throw new IllegalStateException("A prior unresolved SOURCE region read workspace already owns this native holder",
                    unresolvedRegionReadCause);
            if (exactAllocation != allocation || exactCharge <= 0L)
                throw new IllegalStateException("Unresolved SOURCE region workspace must retain its exact precharged holder Account/fee");
            unresolvedRegionReadPin = Objects.requireNonNull(exactPin, "exact source region pin");
            unresolvedRegionReadArena = Objects.requireNonNull(exactArena, "unclosed source region read Arena");
            unresolvedRegionReadBody = Objects.requireNonNull(exactBody, "retained source region read body");
            unresolvedRegionReadAllocation = Objects.requireNonNull(exactAllocation, "original source allocation Account");
            unresolvedRegionReadCharge = exactCharge;
            unresolvedRegionReadCause = Objects.requireNonNull(exactCause, "original region read/close failure");
            unresolvedRegionReadCloseAttempted = true;
            recordFailure(exactCause);
        }

        private void preflightIoClose(WindowsMarkerIo exactIo) throws IOException {
            checkOwner();
            if (io != exactIo || exactIo.sourceIoHolder != this || state != State.READY)
                throw new IOException("Exact live SOURCE native realm from this retained holder required");
            if (allocation instanceof DbFirstAllocation actual && actual.held != SOURCE_LIBRARY_ARENA_CHARGE)
                throw new IOException("FirstBinding6 native work reservations remain owned before IO close");
            exactIo.checkOwner();
            if (unresolvedRegionReadArena != null)
                throw new IOException("SOURCE owner cannot close while its one-attempt region read Arena/body/full allocation charge remains unresolved",
                    unresolvedRegionReadCause);
            if (!exactIo.owned.isEmpty() || !exactIo.ownedFinds.isEmpty())
                throw new IOException("SOURCE native handles remain owned: file=" + exactIo.owned.size()
                    + " find=" + exactIo.ownedFinds.size());
            if (exactIo.retainedWitnessArena != null)
                throw new IOException("SOURCE witness native workspace remains unresolved", exactIo.retainedWitnessArenaFailure);
            if (exactIo.activeSourcePathDepth != 0 || exactIo.activeSourcePathAllocation != null)
                throw new IOException("SOURCE native path workspace remains retained during holder close");
        }

        @Override public void close() throws IOException {
            checkOwner();
            if (closeAttempted) throw new IllegalStateException("SOURCE native holder close already attempted");
            if (state == State.INITIALIZING || state == State.CLOSING)
                throw new IOException("SOURCE native holder is already changing state");
            if (state == State.READY) preflightIoClose(io);
            closeAttempted = true;
            state = State.CLOSING;
            if (libraryArena != null) {
                arenaCloseAttempted = true;
                try {
                    arenaCloseAttempt.close(libraryArena);
                    libraryArena = null;
                    if (io != null) io.closed = true;
                } catch (IOException | RuntimeException | Error cleanup) {
                    recordFailure(cleanup);
                    state = State.CLOSE_FAILED;
                    throw preserveIoFailure("close retained SOURCE Kernel32 library Arena", cleanup);
                }
            }
            if (reservationHeld) {
                try {
                    allocation.releaseTemporary(SOURCE_LIBRARY_ARENA_CHARGE,
                        "SOURCE Kernel32 library Arena holder released after confirmed native close");
                    reservationHeld = false;
                } catch (IOException | RuntimeException | Error cleanup) {
                    recordFailure(cleanup);
                    state = State.CLOSE_FAILED;
                    throw preserveIoFailure("release SOURCE native realm retained allocation", cleanup);
                }
            }
            state = State.CLOSED;
            if (allocation instanceof DbFirstAllocation actual) actual.retired = true;
        }
    }

    @FunctionalInterface
    interface RegionInitFault {
        void afterLibraryLookupBeforeFirstBind() throws IOException;
    }
    @FunctionalInterface
    interface RegionArenaCloseAttempt {
        void close(Arena exactArena) throws IOException;
    }
    @FunctionalInterface
    interface RegionHandleCloseAttempt {
        void beforeNativeClose(WindowsMarkerIo exactIo, Handle exactHandle) throws IOException;
    }
    private static final RegionInitFault NO_REGION_INIT_FAULT = () -> { };
    private record Kernel32Bindings(MemoryLayout callState, long errorOffset, Map<String, MethodHandle> calls) { }
    private static void requireWindowsNativeConfiguration() {
        if (Runtime.version().feature() != 25 || !System.getProperty("os.name", "").startsWith("Windows")
            || !(System.getProperty("os.arch", "").equals("amd64") || System.getProperty("os.arch", "").equals("x86_64"))
            || ValueLayout.ADDRESS.byteSize() != 8 || ByteOrder.nativeOrder() != ByteOrder.LITTLE_ENDIAN)
            throw new UnsupportedOperationException("Windows marker barrier requires Java 25 Windows x64");
        if (!WindowsMarkerIo.class.getModule().isNativeAccessEnabled())
            throw new UnsupportedOperationException("Enable native access for the actual WindowsMarkerIo caller module");
    }
    private static Kernel32Bindings linkKernel32(Arena exactArena, RegionInitFault initFault) throws IOException {
        Linker linker = Linker.nativeLinker();
        MemoryLayout callState = Linker.Option.captureStateLayout();
        long errorOffset = callState.byteOffset(MemoryLayout.PathElement.groupElement("GetLastError"));
        SymbolLookup library = SymbolLookup.libraryLookup("Kernel32.dll", exactArena);
        initFault.afterLibraryLookupBeforeFirstBind();
        Map<String, MethodHandle> calls = new HashMap<>();
        bindInto(calls, linker, library, "CreateFileW", ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.ADDRESS);
        for (String name : new String[]{"ReadFile", "WriteFile"})
            bindInto(calls, linker, library, name, ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
        for (String name : new String[]{"FlushFileBuffers", "CloseHandle", "GetFileType"})
            bindInto(calls, linker, library, name, ValueLayout.JAVA_INT, ValueLayout.ADDRESS);
        for (String name : new String[]{"GetFileInformationByHandleEx", "SetFileInformationByHandle"})
            bindInto(calls, linker, library, name, ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS, ValueLayout.JAVA_INT);
        bindInto(calls, linker, library, "MoveFileExW", ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT);
        // LARGE_INTEGER 是 Windows x64 按值传递的八字节 union。
        bindInto(calls, linker, library, "SetFilePointerEx", ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
            ValueLayout.ADDRESS, ValueLayout.JAVA_INT);
        bindInto(calls, linker, library, "CreateDirectoryW", ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
        bindInto(calls, linker, library, "GetCurrentProcessId", ValueLayout.JAVA_INT);
        bindInto(calls, linker, library, "GetCurrentProcess", ValueLayout.ADDRESS);
        bindInto(calls, linker, library, "OpenProcess", ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT);
        bindInto(calls, linker, library, "GetProcessTimes", ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
            ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
        bindInto(calls, linker, library, "WaitForSingleObject", ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT);
        bindInto(calls, linker, library, "CreateToolhelp32Snapshot", ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT);
        bindInto(calls, linker, library, "Process32FirstW", ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
        bindInto(calls, linker, library, "Process32NextW", ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
        bindInto(calls, linker, library, "GetFinalPathNameByHandleW", ValueLayout.JAVA_INT, ValueLayout.ADDRESS,
            ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT);
        bindInto(calls, linker, library, "FindFirstFileW", ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
        bindInto(calls, linker, library, "FindNextFileW", ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS);
        bindInto(calls, linker, library, "FindClose", ValueLayout.JAVA_INT, ValueLayout.ADDRESS);
        return new Kernel32Bindings(callState, errorOffset, Map.copyOf(calls));
    }
    private static void bindInto(Map<String, MethodHandle> calls, Linker linker, SymbolLookup library, String name,
                                 MemoryLayout result, MemoryLayout... arguments) {
        calls.put(name, linker.downcallHandle(library.find(name).orElseThrow(),
            FunctionDescriptor.of(result, arguments), Linker.Option.captureCallState("GetLastError")));
    }
    private record Result(Object value, int error) {}
    private Result call(String api, UUID operation, Object... args) throws IOException {
        checkOwner();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment state = arena.allocate(callState);
            Object[] actual = new Object[args.length + 1];
            actual[0] = state;
            System.arraycopy(args, 0, actual, 1, args.length);
            Object value = calls.get(api).invokeWithArguments(actual);
            // 必须在任何后续 native call 之前复制这个调用自己的 GetLastError。
            return new Result(value, state.get(ValueLayout.JAVA_INT, errorOffset));
        } catch (Throwable failure) { throw ioFailure(api + " operation=" + operation, failure); }
    }
    private void bool(String api, UUID operation, Object... args) throws IOException {
        Result result = call(api, operation, args);
        if ((Integer) result.value == 0) throw new NativeFailure(api, operation, result.error);
    }
    static IOException ioFailure(String stage, Throwable cause) {
        return new IOException("Windows marker boundary failed: " + stage, cause);
    }
    private void checkOwner() {
        if (Thread.currentThread() != owner) throw new IllegalStateException("Windows marker owner thread mismatch");
        if (closed) throw new IllegalStateException("Windows marker IO closed");
    }
    public record NativePathDiagnostic(int utf16CodeUnitsExcludingTerminator, String sha256Utf16LeExcludingTerminator) {
        public NativePathDiagnostic {
            if (utf16CodeUnitsExcludingTerminator < 0 || sha256Utf16LeExcludingTerminator == null
                || !sha256Utf16LeExcludingTerminator.matches("[0-9A-F]{64}"))
                throw new IllegalArgumentException("Exact native target diagnostic required");
        }
    }
    public static final class NativeFailure extends IOException {
        private final String api;
        private final UUID operation;
        private final int win32Error;
        private final NativePathDiagnostic nativePathDiagnostic;
        NativeFailure(String api, UUID operation, int error) { this(api, operation, error, null); }
        NativeFailure(String api, UUID operation, int error, NativePathDiagnostic nativePathDiagnostic) {
            super(api + " failed operation=" + operation + " win32=" + Integer.toUnsignedString(error)
                + (nativePathDiagnostic == null ? "" : " nativeTargetUtf16CodeUnits="
                    + nativePathDiagnostic.utf16CodeUnitsExcludingTerminator()
                    + " nativeTargetSha256Utf16Le="
                    + nativePathDiagnostic.sha256Utf16LeExcludingTerminator()));
            this.api = api; this.operation = operation; this.win32Error = error;
            this.nativePathDiagnostic = nativePathDiagnostic;
        }
        public String api() { return api; }
        public UUID operation() { return operation; }
        public int win32Error() { return win32Error; }
        public Optional<NativePathDiagnostic> nativePathDiagnostic() { return Optional.ofNullable(nativePathDiagnostic); }
    }
    public static final class FileIdentity {
        private final long volume;
        private final byte[] id;
        public FileIdentity(long volume, byte[] id) {
            if (id == null || id.length != 16) throw new IllegalArgumentException("FileId must contain 16 bytes");
            this.volume = volume; this.id = id.clone();
        }
        public long volumeSerialNumber() { return volume; }
        public byte[] fileId128() { return id.clone(); }
        public String versionedIdentity() {
            return "FP_WIN32_FILE_ID_V1;volume=" + HexFormat.of().toHexDigits(volume) + ";id=" + HexFormat.of().formatHex(id);
        }
        @Override public boolean equals(Object other) {
            return other instanceof FileIdentity that && volume == that.volume && Arrays.equals(id, that.id);
        }
        @Override public int hashCode() { return 31 * Long.hashCode(volume) + Arrays.hashCode(id); }
    }
    final class Handle implements AutoCloseable {
        final MemorySegment value;
        final UUID operation;
        final int access, share, disposition, flags;
        boolean closeAttempted;
        Handle(MemorySegment value, UUID operation) { this(value, operation, -1, -1, -1, -1); }
        Handle(MemorySegment value, UUID operation, int access, int share, int disposition, int flags) {
            this.value = value; this.operation = operation; this.access = access;
            this.share = share; this.disposition = disposition; this.flags = flags;
        }
        @Override public void close() throws IOException {
            checkOwner();
            if (closeAttempted) throw new IllegalStateException("CloseHandle already attempted");
            closeAttempted = true;
            if (regionSession != null) regionSession.handleCloseAttempted(this);
            try {
                if (regionSession != null) handleCloseAttempt.beforeNativeClose(WindowsMarkerIo.this, this);
                bool("CloseHandle", operation, value);
                if (regionSession != null) regionSession.handleCloseSucceeded(this);
                owned.remove(this); // false 时保留未确认释放的义务，不能伪称已关闭。
            } catch (IOException | RuntimeException | Error failure) {
                if (regionSession != null) {
                    try { regionSession.handleCloseFailed(this, failure); }
                    catch (RuntimeException | Error bookkeepingFailure) { failure.addSuppressed(bookkeepingFailure); }
                }
                throw failure;
            }
        }
    }
    /** Typed, owner-thread native pin for one exact source parent directory. No HANDLE escapes this wrapper. */
    public final class SourceDirectoryPin implements AutoCloseable {
        private final Handle handle;
        private final FileIdentity identity;
        private final String exactNativePath;
        private final SourceForestAllocationBudget retainedPathAllocation;
        private final long retainedPathCharge;
        private int children;
        private Throwable childCloseFailure;
        private boolean closeAttempted;
        private SourceArtifactWorkspace sourceArtifactWorkspace;
        private boolean sourceArtifactWorkspaceAttempted;
        private boolean sourceArtifactWorkspaceChargeAttempted, sourceArtifactWorkspaceChargeReturned;
        private boolean sourceArtifactWorkspaceChargeReleaseAttempted, sourceArtifactWorkspaceChargeReleased;
        private SourceForestAllocationBudget sourceArtifactWorkspaceAllocation;
        private WindowsNormalRecallSourceStore.ArtifactKind sourceArtifactWorkspaceKind;
        private UUID sourceArtifactWorkspaceWrite, sourceArtifactWorkspaceOperation;
        private String sourceArtifactWorkspaceContext;
        private Throwable sourceArtifactWorkspaceFailure;
        private SourceDirectoryPin(Handle handle, FileIdentity identity, String exactNativePath,
                SourceForestAllocationBudget retainedPathAllocation, long retainedPathCharge) {
            this.handle = handle; this.identity = identity; this.exactNativePath = exactNativePath;
            this.retainedPathAllocation = retainedPathAllocation; this.retainedPathCharge = retainedPathCharge;
        }
        public FileIdentity identity() throws IOException {
            requireLive();
            if (!identity.equals(WindowsMarkerIo.this.identity(handle)))
                throw new IOException("Pinned source parent directory identity changed");
            return identity;
        }
        private boolean issuedBy(WindowsMarkerIo expected) { return WindowsMarkerIo.this == expected; }
        private String exactNativePath() throws IOException { requireLive(); return exactNativePath; }
        private void requireLive() throws IOException {
            checkOwner();
            if (closeAttempted || handle.closeAttempted || !owned.containsKey(handle))
                throw new IOException("Exact live source parent directory pin required");
            if (childCloseFailure != null)
                throw new IOException("A source child HANDLE close remains unconfirmed", childCloseFailure);
            if (sourceIoHolder == null || sourceIoHolder.allocation != retainedPathAllocation)
                throw new IOException("Source directory pin must belong to a ready budgeted SOURCE native holder");
            sourceIoHolder.requireAllocation(retainedPathAllocation.exactProfile(), retainedPathAllocation);
        }
        private void reserveChild() throws IOException {
            requireLive();
            children = Math.addExact(children, 1);
        }
        private void releaseChild() {
            if (children <= 0) throw new IllegalStateException("Source directory child pin accounting underflow");
            children--;
        }
        private void retainChildCloseFailure(Throwable failure) {
            Objects.requireNonNull(failure);
            if (childCloseFailure == null) childCloseFailure = failure;
            else if (childCloseFailure != failure) childCloseFailure.addSuppressed(failure);
        }
        private void beginArtifactWorkspaceAttempt(WindowsNormalRecallSourceStore.ArtifactKind kind,
                UUID write, UUID operation, SourceForestAllocationBudget allocation, String context)
                throws IOException {
            requireLive();
            if (sourceArtifactWorkspaceAttempted || sourceArtifactWorkspace != null)
                throw new IOException("Exact source parent already retains an artifact workspace attempt",
                    sourceArtifactWorkspaceFailure);
            sourceArtifactWorkspaceAttempted=true;
            sourceArtifactWorkspaceKind=Objects.requireNonNull(kind);
            sourceArtifactWorkspaceWrite=Objects.requireNonNull(write);
            sourceArtifactWorkspaceOperation=Objects.requireNonNull(operation);
            sourceArtifactWorkspaceAllocation=Objects.requireNonNull(allocation);
            sourceArtifactWorkspaceContext=Objects.requireNonNull(context);
            sourceArtifactWorkspaceChargeAttempted=true;
            try {
                allocation.reserveTemporary(SOURCE_ARTIFACT_TOTAL_CHARGE,
                    context + " transfer scratch/count, observation and typed workspace attempt BEFORE allocation");
                sourceArtifactWorkspaceChargeReturned=true;
            } catch (Throwable failure) {
                retainArtifactWorkspaceFailure(failure);
                throw preserveIoFailure("reserve typed source artifact workspace", failure);
            }
        }
        private void registerArtifactWorkspace(SourceArtifactWorkspace actual) throws IOException {
            Objects.requireNonNull(actual);
            if (!sourceArtifactWorkspaceAttempted || !sourceArtifactWorkspaceChargeReturned
                || sourceArtifactWorkspace != null || actual.parent != this
                || actual.allocation != sourceArtifactWorkspaceAllocation
                || actual.kind != sourceArtifactWorkspaceKind || !actual.write.equals(sourceArtifactWorkspaceWrite)
                || !actual.operation.equals(sourceArtifactWorkspaceOperation))
                throw new IOException("Precharged exact source artifact workspace attempt is required before registration");
            sourceArtifactWorkspace=actual;
        }
        private SourceArtifactWorkspace retainedArtifactWorkspace() { return sourceArtifactWorkspace; }
        private void retainArtifactWorkspaceFailure(Throwable failure) {
            Objects.requireNonNull(failure);
            if (sourceArtifactWorkspaceFailure == null) sourceArtifactWorkspaceFailure=failure;
            else if (sourceArtifactWorkspaceFailure != failure) sourceArtifactWorkspaceFailure.addSuppressed(failure);
        }
        private void releaseArtifactWorkspaceRegistration(SourceArtifactWorkspace exactWorkspace) throws IOException {
            if (sourceArtifactWorkspace != exactWorkspace || !sourceArtifactWorkspaceAttempted
                || !exactWorkspace.attemptChargeReleased)
                throw new IOException("Only the exact fully charged/closed artifact workspace can leave its source parent");
            sourceArtifactWorkspace=null;
            clearArtifactWorkspaceAttempt();
        }
        private Throwable releaseArtifactWorkspaceWithoutOwner(Throwable primary, String context) {
            if (!sourceArtifactWorkspaceAttempted || sourceArtifactWorkspace != null
                || !sourceArtifactWorkspaceChargeReturned || sourceArtifactWorkspaceChargeReleaseAttempted)
                return primary;
            sourceArtifactWorkspaceChargeReleaseAttempted=true;
            try {
                sourceArtifactWorkspaceAllocation.releaseTemporary(SOURCE_ARTIFACT_TOTAL_CHARGE,
                    context + " workspace owner construction failed before Arena/native allocation");
                sourceArtifactWorkspaceChargeReleased=true;
                clearArtifactWorkspaceAttempt();
            } catch (Throwable cleanup) {
                retainArtifactWorkspaceFailure(cleanup);
                if (primary == null) primary=cleanup;
                else if (cleanup != primary) primary.addSuppressed(cleanup);
            }
            return primary;
        }
        private void clearArtifactWorkspaceAttempt() {
            sourceArtifactWorkspaceAttempted=false;
            sourceArtifactWorkspaceChargeAttempted=false;sourceArtifactWorkspaceChargeReturned=false;
            sourceArtifactWorkspaceChargeReleaseAttempted=false;sourceArtifactWorkspaceChargeReleased=false;
            sourceArtifactWorkspaceAllocation=null;sourceArtifactWorkspaceKind=null;
            sourceArtifactWorkspaceWrite=null;sourceArtifactWorkspaceOperation=null;
            sourceArtifactWorkspaceContext=null;sourceArtifactWorkspaceFailure=null;
        }
        @Override public void close() throws IOException {
            checkOwner();
            if (closeAttempted) throw new IllegalStateException("Source directory pin close already attempted");
            if (sourceArtifactWorkspaceAttempted || sourceArtifactWorkspace != null)
                throw new IOException("Cannot close a source parent while its exact artifact workspace attempt remains retained",
                    sourceArtifactWorkspaceFailure);
            if (childCloseFailure != null)
                throw new IOException("Cannot close a source parent with an unconfirmed child close", childCloseFailure);
            if (children != 0) throw new IOException("Cannot close a source parent while child pins remain: " + children);
            closeAttempted = true;
            handle.close();
            if (retainedPathAllocation != null) retainedPathAllocation.releaseTemporary(retainedPathCharge,
                "exact pinned source parent native path released after its last child/handle close");
        }
    }
    /** Typed native pin for one existing r.<x>.<z>.mca child under a pinned directory. */
    public final class SourceRegionFilePin implements AutoCloseable {
        private final SourceDirectoryPin parent;
        private final String basename;
        private final String exactNativePath;
        private final FileIdentity parentIdentity;
        private final Handle handle;
        private final FileIdentity identity;
        private final SourceForestAllocationBudget retainedPathAllocation;
        private final long retainedPathCharge;
        private boolean closeAttempted;
        private SourceRegionFilePin(SourceDirectoryPin parent, String basename, String exactNativePath,
                FileIdentity parentIdentity, Handle handle, FileIdentity identity, SourceForestAllocationBudget retainedPathAllocation,
                long retainedPathCharge) {
            this.parent = parent; this.basename = basename; this.exactNativePath = exactNativePath;
            this.parentIdentity = parentIdentity; this.handle = handle; this.identity = identity; this.retainedPathAllocation = retainedPathAllocation;
            this.retainedPathCharge = retainedPathCharge;
        }
        public FileIdentity identity() throws IOException {
            SourceFileObservation current = observation();
            return current.identity();
        }
        public SourceFileObservation observation() throws IOException {
            return refreshObservation();
        }
        public SourceFileObservation refreshObservation() throws IOException {
            requireLive();
            try (SourcePathWorkspace ignored = sourcePathWorkspace(parent.retainedPathAllocation, parent,
                    "source region parent/child path observation")) {
                SourceFileObservation observed = observeSourceFile(handle);
                if (observed.byteLength() < 8192L)
                    throw new IOException("Pinned source region file is shorter than its 8192-byte header");
                if (!identity.equals(observed.identity()))
                    throw new IOException("Pinned source region FILE_ID_INFO identity changed");
                if (!parentIdentity.equals(parent.identity()))
                    throw new IOException("Pinned source region parent identity changed");
                String expected = sourceRegionFilePath(parent, basename);
                if (!exactNativePath.equals(expected) || !expected.equals(finalPath(handle)))
                    throw new IOException("Pinned source region file is no longer the exact child of its pinned parent");
                return observed;
            }
        }
        /** Reads exactly the original 8192-byte region header from this held HANDLE, retaining its original-root fee. */
        public byte[] readHeader(com.wi.floatingpod.server.source.SourceForestAllocationBudget allocation) throws IOException {
            Objects.requireNonNull(allocation, "original source allocation account");
            requireLive();
            if (parent.retainedPathAllocation != allocation)
                throw new IOException("Original region header must use the exact native parent's source Account");
            sourceIoHolder.requireAllocation(allocation.exactProfile(), allocation);
            SourceFileObservation before = refreshObservation();
            if (before.byteLength() < 8192L)
                throw new IOException("Original region header is not completely present");
            final long totalCharge = 16_640L; // Returned 8192-byte header + 8192-byte native scratch + fixed bookkeeping.
            allocation.reserveTemporary(totalCharge, "source region original 8192-byte undo header and native read buffer BEFORE allocation");
            byte[] header = null;
            Arena arena = null;
            Throwable primary = null;
            boolean arenaClosed = false;
            boolean arenaCloseAttempted = false;
            try {
                header = new byte[8192];
                arena = Arena.ofConfined();
                MemorySegment buffer = arena.allocate(8192L);
                MemorySegment count = arena.allocate(ValueLayout.JAVA_INT);
                bool("SetFilePointerEx", handle.operation, handle.value, 0L, MemorySegment.NULL, 0);
                for (int offset = 0; offset < header.length;) {
                    int request = Math.min(8192, header.length - offset);
                    bool("ReadFile", handle.operation, handle.value, buffer, request, count, MemorySegment.NULL);
                    int read = progress(count.get(ValueLayout.JAVA_INT, 0), request);
                    MemorySegment.ofArray(header).asSlice(offset, read).copyFrom(buffer.asSlice(0L, read));
                    offset = Math.addExact(offset, read);
                }
                SourceFileObservation after = refreshObservation();
                if (!before.equals(after)) throw new IOException("Region identity, length, or raw FILETIME changed while reading the exact original header");
                arenaCloseAttempted = true;
                arena.close();
                arenaClosed = true;
            } catch (Throwable failure) {
                primary = failure;
            } finally {
                if (arena != null && !arenaCloseAttempted) {
                    arenaCloseAttempted = true;
                    try { arena.close(); arenaClosed = true; }
                    catch (Throwable cleanup) {
                        if (primary == null) primary = cleanup;
                        else if (cleanup != primary) primary.addSuppressed(cleanup);
                    }
                }
                if (arenaClosed || arena == null) {
                    try {
                        long release = primary == null ? 8_448L : totalCharge;
                        allocation.releaseTemporary(release, "source region header native scratch close / failed read");
                    } catch (Throwable cleanup) {
                        if (primary == null) primary = cleanup;
                        else if (cleanup != primary) primary.addSuppressed(cleanup);
                    }
                }
            }
            if (primary != null) throw preserveIoFailure("read source region header from exact pinned HANDLE", primary);
            return header;
        }
        /** Reads the complete bounded current region file from this same HANDLE and retains only its accounted byte[]. */
        public byte[] readAllBounded(SourceForestAllocationBudget.Account allocation, long maximum) throws IOException {
            Objects.requireNonNull(allocation, "original source holder account");
            requireLive();
            if (parent.retainedPathAllocation != allocation)
                throw new IOException("Complete original region capture must use the exact native parent's source Account");
            sourceIoHolder.requireAllocation(allocation.exactProfile(), allocation);
            SourceLimitProfile profile = allocation.exactProfile();
            if (maximum <= 0L || maximum > profile.maxRegionBytes())
                throw new IOException("Complete source region maximum must be positive and within the exact profile");
            allocation.reserveTemporary(SOURCE_ARTIFACT_OBSERVATION_CHARGE,
                "original source holder pinned region metadata observations BEFORE native query");
            SourceFileObservation before;
            try { before = refreshObservation(); }
            catch (Throwable primary) {
                try { allocation.releaseTemporary(SOURCE_ARTIFACT_OBSERVATION_CHARGE,
                    "original source holder failed initial region observation"); }
                catch (Throwable cleanup) { if (cleanup != primary) primary.addSuppressed(cleanup); }
                throw preserveIoFailure("observe exact source region before complete capture", primary);
            }
            long length = before.byteLength();
            if (length < 8192L || length > maximum || length > Integer.MAX_VALUE) {
                IOException failure = new IOException("Exact pinned source region length is outside the complete bounded byte-array range");
                try { allocation.releaseTemporary(SOURCE_ARTIFACT_OBSERVATION_CHARGE,
                    "original source holder rejected complete region length"); }
                catch (Throwable cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
                throw failure;
            }
            long bodyAndScratch, totalCharge;
            try {
                bodyAndScratch = Math.addExact(length, SOURCE_ARTIFACT_TRANSFER_BYTES);
                totalCharge = Math.addExact(bodyAndScratch, SOURCE_ARTIFACT_OBSERVATION_CHARGE);
            } catch (ArithmeticException overflow) {
                IOException failure = new IOException("Complete source region allocation charge overflow", overflow);
                try { allocation.releaseTemporary(SOURCE_ARTIFACT_OBSERVATION_CHARGE,
                    "original source holder complete region charge overflow"); }
                catch (Throwable cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
                throw failure;
            }
            try { allocation.reserveTemporary(bodyAndScratch,
                "original source holder complete native region capture plus returned bytes BEFORE allocation"); }
            catch (Throwable primary) {
                try { allocation.releaseTemporary(SOURCE_ARTIFACT_OBSERVATION_CHARGE,
                    "original source holder failed complete region reservation"); }
                catch (Throwable cleanup) { if (cleanup != primary) primary.addSuppressed(cleanup); }
                throw preserveIoFailure("reserve complete source region capture", primary);
            }
            byte[] bytes = null;
            Arena arena = null;
            Throwable primary = null;
            boolean arenaCloseAttempted = false;
            boolean arenaClosed = false;
            try {
                bytes = new byte[Math.toIntExact(length)];
                arena = Arena.ofConfined();
                MemorySegment buffer = arena.allocate(SOURCE_ARTIFACT_TRANSFER_BYTES);
                MemorySegment count = arena.allocate(ValueLayout.JAVA_INT);
                MemorySegment target = MemorySegment.ofArray(bytes);
                bool("SetFilePointerEx", handle.operation, handle.value, 0L, MemorySegment.NULL, 0);
                for (int offset = 0; offset < bytes.length;) {
                    int request = Math.min(SOURCE_ARTIFACT_TRANSFER_BYTES, bytes.length - offset);
                    bool("ReadFile", handle.operation, handle.value, buffer, request, count, MemorySegment.NULL);
                    int read = progress(count.get(ValueLayout.JAVA_INT, 0), request);
                    target.asSlice(offset, read).copyFrom(buffer.asSlice(0L, read));
                    offset = Math.addExact(offset, read);
                }
                bool("ReadFile", handle.operation, handle.value, buffer, 1, count, MemorySegment.NULL);
                if (count.get(ValueLayout.JAVA_INT, 0) != 0)
                    throw new IOException("Pinned source region grew beyond its captured exact length");
                SourceFileObservation after = refreshObservation();
                if (!before.equals(after))
                    throw new IOException("Source region identity, exact length, or raw FILETIME changed during complete capture");
                arenaCloseAttempted = true;
                arena.close();
                arenaClosed = true;
            } catch (Throwable failure) {
                primary = failure;
            } finally {
                if (arena != null && !arenaCloseAttempted) {
                    arenaCloseAttempted = true;
                    try { arena.close(); arenaClosed = true; }
                    catch (Throwable cleanup) {
                        if (primary == null) primary = cleanup;
                        else if (cleanup != primary) primary.addSuppressed(cleanup);
                    }
                }
                if (arena == null || arenaClosed) {
                    long release = primary == null ? totalCharge - length : totalCharge;
                    try { allocation.releaseTemporary(release,
                        "original source holder complete region capture native scratch/failed body release"); }
                    catch (Throwable cleanup) {
                        parent.retainChildCloseFailure(cleanup);
                        if (primary == null) primary = cleanup;
                        else if (cleanup != primary) primary.addSuppressed(cleanup);
                    }
                } else {
                    if (primary == null)
                        primary = new IOException("Complete source region capture Arena close returned without confirmation");
                    sourceIoHolder.retainUnclosedRegionRead(this, arena, bytes, allocation, totalCharge, primary);
                    parent.retainChildCloseFailure(primary);
                    // The original IO holder retains the real Arena, body, Account, full fee and cause; no close retry or refund.
                    arena = null;
                    bytes = null;
                }
            }
            if (primary != null) throw preserveIoFailure("read complete source region from exact pinned HANDLE", primary);
            return bytes;
        }
        private void requireLive() throws IOException {
            checkOwner();
            parent.requireLive();
            if (closeAttempted || handle.closeAttempted || !owned.containsKey(handle))
                throw new IOException("Exact live source region file pin required");
        }
        @Override public void close() throws IOException {
            checkOwner();
            if (closeAttempted) throw new IllegalStateException("Source region file pin close already attempted");
            closeAttempted = true;
            try {
                handle.close();
                parent.releaseChild();
                if (retainedPathAllocation != null) retainedPathAllocation.releaseTemporary(retainedPathCharge,
                    "exact pinned source region native path released after its final handle close");
            } catch (IOException | RuntimeException | Error failure) {
                parent.retainChildCloseFailure(failure);
                throw failure;
            }
        }
    }
    /** Typed temp artifact with CREATE_NEW/no-sharing semantics; only the result-core writer can obtain it. */
    public final class SourceResultTemporaryPin implements AutoCloseable {
        private final SourceDirectoryPin parent;
        private final UUID write;
        private final UUID publicationAttempt;
        private final Handle handle;
        private final FileIdentity fileIdentity;
        private boolean writeAttempted;
        private boolean closeAttempted;
        private SourceResultTemporaryPin(SourceDirectoryPin parent, UUID write, UUID publicationAttempt,
                Handle handle, FileIdentity fileIdentity) {
            this.parent = parent; this.write = write; this.publicationAttempt = publicationAttempt;
            this.handle = handle; this.fileIdentity = fileIdentity;
        }
        public FileIdentity identity() throws IOException {
            requireLive();
            try (SourcePathWorkspace ignored = sourcePathWorkspace(parent.retainedPathAllocation, parent,
                    "source result temporary identity exact-child verification")) {
                validateSourceResultChild(parent, handle, sourceResultTemporaryBasename(write, publicationAttempt), fileIdentity);
                return fileIdentity;
            }
        }
        public void write(byte[] exactCore, long explicitArtifactLimit) throws IOException {
            requireLive();
            if (writeAttempted) throw new IOException("Source result temporary core write is one-shot");
            writeAttempted = true;
            Objects.requireNonNull(exactCore, "complete source result core");
            witnessArtifactBound(exactCore.length, explicitArtifactLimit);
            long copyCharge = Math.addExact((long) exactCore.length, 1_024L);
            SourcePathWorkspace pathWorkspace = null;
            boolean copyChargeReserved = false;
            Throwable primary = null;
            try {
                pathWorkspace = sourcePathWorkspace(parent.retainedPathAllocation, parent,
                    "source result temporary write exact-child verification");
                validateSourceResultChild(parent, handle,
                    sourceResultTemporaryBasename(write, publicationAttempt), fileIdentity);
                if (retainedWitnessArena != null)
                    throw new IOException("Prior witness native workspace remains unresolved", retainedWitnessArenaFailure);
                parent.retainedPathAllocation.reserveTemporary(copyCharge,
                    "source result core native write copy workspace BEFORE Arena allocation");
                copyChargeReserved = true;
                writeWitnessArtifact(handle, exactCore, explicitArtifactLimit);
                metadata(handle, false, exactCore.length);
                validateSourceResultChild(parent, handle,
                    sourceResultTemporaryBasename(write, publicationAttempt), fileIdentity);
            } catch (Throwable failure) {
                primary = failure;
            } finally {
                if (copyChargeReserved) {
                    if (retainedWitnessArena == null) {
                        try { parent.retainedPathAllocation.releaseTemporary(copyCharge,
                            "source result core native write copy Arena confirmed closed"); }
                        catch (Throwable cleanup) {
                            parent.retainChildCloseFailure(cleanup);
                            if (primary == null) primary = cleanup;
                            else if (cleanup != primary) primary.addSuppressed(cleanup);
                        }
                    } else {
                        IOException unresolved = new IOException("Source result core native write Arena close remains unconfirmed",
                            retainedWitnessArenaFailure);
                        parent.retainChildCloseFailure(unresolved);
                        if (primary == null) primary = unresolved;
                        else if (unresolved != primary) primary.addSuppressed(unresolved);
                    }
                }
                if (pathWorkspace != null && !pathWorkspace.closeAttempted) {
                    try { pathWorkspace.close(); }
                    catch (Throwable cleanup) {
                        if (primary == null) primary = cleanup;
                        else if (cleanup != primary) primary.addSuppressed(cleanup);
                    }
            }
            }
            if (primary != null) throw preserveIoFailure("write source result temporary core", primary);
        }
        public void verifyLength(long exactLength) throws IOException {
            requireLive();
            try (SourcePathWorkspace ignored = sourcePathWorkspace(parent.retainedPathAllocation, parent,
                    "source result temporary length exact-child verification")) {
                validateSourceResultChild(parent, handle,
                    sourceResultTemporaryBasename(write, publicationAttempt), fileIdentity);
                metadata(handle, false, exactLength);
                validateSourceResultChild(parent, handle,
                    sourceResultTemporaryBasename(write, publicationAttempt), fileIdentity);
            }
        }
        public void flush() throws IOException {
            requireLive();
            try (SourcePathWorkspace ignored = sourcePathWorkspace(parent.retainedPathAllocation, parent,
                    "source result temporary flush exact-child verification")) {
                validateSourceResultChild(parent, handle,
                    sourceResultTemporaryBasename(write, publicationAttempt), fileIdentity);
                flushChecked(handle);
                validateSourceResultChild(parent, handle,
                    sourceResultTemporaryBasename(write, publicationAttempt), fileIdentity);
            }
        }
        private void requireLive() throws IOException {
            checkOwner(); parent.requireLive();
            if (closeAttempted || handle.closeAttempted || !owned.containsKey(handle))
                throw new IOException("Exact live result temporary pin required");
        }
        @Override public void close() throws IOException {
            checkOwner();
            if (closeAttempted) throw new IllegalStateException("Source result temporary close already attempted");
            closeAttempted = true;
            closeSourceResultChild(parent, handle, sourceResultTemporaryBasename(write, publicationAttempt), fileIdentity,
                "close source result temporary");
        }
    }
    /** Typed canonical artifact opened with READ|WRITE/share0 for flush and complete readback. */
    public final class SourceResultCanonicalPin implements AutoCloseable {
        private final SourceDirectoryPin parent;
        private final UUID write;
        private final Handle handle;
        private final FileIdentity fileIdentity;
        private boolean closeAttempted;
        private SourceResultCanonicalPin(SourceDirectoryPin parent, UUID write, Handle handle,
                FileIdentity fileIdentity) {
            this.parent = parent; this.write = write; this.handle = handle; this.fileIdentity = fileIdentity;
        }
        public FileIdentity identity() throws IOException {
            requireLive();
            try (SourcePathWorkspace ignored = sourcePathWorkspace(parent.retainedPathAllocation, parent,
                    "source result canonical identity exact-child verification")) {
                validateSourceResultChild(parent, handle, sourceResultCanonicalBasename(write), fileIdentity);
                return fileIdentity;
            }
        }
        public void verifyLength(long exactLength) throws IOException {
            requireLive();
            try (SourcePathWorkspace ignored = sourcePathWorkspace(parent.retainedPathAllocation, parent,
                    "source result canonical length exact-child verification")) {
                validateSourceResultChild(parent, handle, sourceResultCanonicalBasename(write), fileIdentity);
                metadata(handle, false, exactLength);
                validateSourceResultChild(parent, handle, sourceResultCanonicalBasename(write), fileIdentity);
            }
        }
        public void flush() throws IOException {
            requireLive();
            try (SourcePathWorkspace ignored = sourcePathWorkspace(parent.retainedPathAllocation, parent,
                    "source result canonical flush exact-child verification")) {
                validateSourceResultChild(parent, handle, sourceResultCanonicalBasename(write), fileIdentity);
                flushChecked(handle);
                validateSourceResultChild(parent, handle, sourceResultCanonicalBasename(write), fileIdentity);
            }
        }
        public void compareExact(byte[] exactCore, FileIdentity expectedIdentity, long explicitArtifactLimit) throws IOException {
            requireLive();
            Objects.requireNonNull(exactCore, "complete source result core");
            Objects.requireNonNull(expectedIdentity, "exact source result temporary FileIdentity");
            witnessArtifactBound(exactCore.length, explicitArtifactLimit);
            long scratchCharge = SOURCE_ARTIFACT_TRANSFER_BYTES + SOURCE_ARTIFACT_OBSERVATION_CHARGE;
            SourcePathWorkspace pathWorkspace = null;
            boolean scratchReserved = false;
            Throwable primary = null;
            try {
                pathWorkspace = sourcePathWorkspace(parent.retainedPathAllocation, parent,
                    "source result canonical compare exact-child verification");
                validateSourceResultChild(parent, handle, sourceResultCanonicalBasename(write), fileIdentity);
                if (!fileIdentity.equals(expectedIdentity))
                    throw new IOException("Canonical result FileIdentity differs from the exact created temporary");
                if (retainedWitnessArena != null)
                    throw new IOException("Prior witness native workspace remains unresolved", retainedWitnessArenaFailure);
                parent.retainedPathAllocation.reserveTemporary(scratchCharge,
                    "source result canonical compare native scratch BEFORE Arena allocation");
                scratchReserved = true;
                compareWitnessArtifactExact(handle, exactCore, expectedIdentity, explicitArtifactLimit);
                validateSourceResultChild(parent, handle, sourceResultCanonicalBasename(write), fileIdentity);
            } catch (Throwable failure) {
                primary = failure;
            } finally {
                if (scratchReserved) {
                    if (retainedWitnessArena == null) {
                        try { parent.retainedPathAllocation.releaseTemporary(scratchCharge,
                            "source result canonical compare Arena confirmed closed"); }
                        catch (Throwable cleanup) {
                            parent.retainChildCloseFailure(cleanup);
                            if (primary == null) primary = cleanup;
                            else if (cleanup != primary) primary.addSuppressed(cleanup);
                        }
                    } else {
                        IOException unresolved = new IOException("Source result canonical compare Arena close remains unconfirmed",
                            retainedWitnessArenaFailure);
                        parent.retainChildCloseFailure(unresolved);
                        if (primary == null) primary = unresolved;
                        else if (unresolved != primary) primary.addSuppressed(unresolved);
                    }
                }
                if (pathWorkspace != null && !pathWorkspace.closeAttempted) {
                    try { pathWorkspace.close(); }
                    catch (Throwable cleanup) {
                        if (primary == null) primary = cleanup;
                        else if (cleanup != primary) primary.addSuppressed(cleanup);
                    }
            }
            }
            if (primary != null) throw preserveIoFailure("compare source result canonical core", primary);
        }
        private void requireLive() throws IOException {
            checkOwner(); parent.requireLive();
            if (closeAttempted || handle.closeAttempted || !owned.containsKey(handle))
                throw new IOException("Exact live canonical result pin required");
        }
        @Override public void close() throws IOException {
            checkOwner();
            if (closeAttempted) throw new IllegalStateException("Source result canonical close already attempted");
            closeAttempted = true;
            closeSourceResultChild(parent, handle, sourceResultCanonicalBasename(write), fileIdentity,
                "close source result canonical");
        }
    }
    /** Fixed-role CREATE_NEW source body handle; it exposes a bounded stream, never its HANDLE. */
    public final class SourceArtifactCreatePin implements AutoCloseable {
        private final SourceDirectoryPin parent;
        private final WindowsNormalRecallSourceStore.ArtifactKind kind;
        private final UUID write;
        private final Handle handle;
        private final FileIdentity createdIdentity;
        private SourceArtifactWorkspace workspace;
        private final long explicitArtifactLimit;
        private final java.security.MessageDigest digest;
        private SourceArtifactBodyOutputStream stream;
        private long byteLength;
        private SourceArtifactWriteObservation completedObservation;
        private Throwable writeFailure;
        private boolean finishAttempted, finished, closeAttempted;

        private SourceArtifactCreatePin(SourceDirectoryPin parent,
                WindowsNormalRecallSourceStore.ArtifactKind kind, UUID write, Handle handle,
                FileIdentity createdIdentity, SourceArtifactWorkspace workspace, long explicitArtifactLimit,
                java.security.MessageDigest digest) {
            this.parent = parent; this.kind = kind; this.write = write; this.handle = handle;
            this.createdIdentity = createdIdentity; this.workspace = workspace;
            this.explicitArtifactLimit = explicitArtifactLimit; this.digest = digest;
        }

        public OutputStream outputStream() throws IOException {
            requireLive();
            if (finishAttempted || stream != null) throw new IOException("Source artifact body stream is one-shot");
            stream = new SourceArtifactBodyOutputStream(this);
            return stream;
        }

        /** Completes the only write flight, flushes the created file, and closes its native scratch arena. */
        public SourceArtifactWriteObservation finishBody() throws IOException {
            requireLive();
            if (finishAttempted) throw new IllegalStateException("Source artifact body finish already attempted");
            finishAttempted = true;
            Throwable primary = null;
            SourceArtifactWriteObservation observation = null;
            SourcePathWorkspace pathWorkspace = null;
            try {
                if (stream == null || !stream.closeAttempted)
                    throw new IOException("Source artifact producer stream must return and close before finish");
                if (writeFailure != null) throw preserveIoFailure("source artifact body writer", writeFailure);
                if (byteLength <= 0L || byteLength > explicitArtifactLimit)
                    throw new IOException("Source artifact body length is outside its explicit native limit");
                pathWorkspace = sourcePathWorkspace(parent.retainedPathAllocation, parent,
                    "source artifact CREATE_NEW final exact-child verification");
                String expectedPath = sourceArtifactPath(parent, kind, write);
                if (!expectedPath.equals(finalPath(handle)))
                    throw new IOException("Created source artifact HANDLE left its exact fixed parent child before flush");
                flushChecked(handle);
                metadata(handle, false, byteLength);
                SourceFileObservation actual = observeSourceFile(handle);
                FileIdentity parentIdentity = parent.identity();
                if (!createdIdentity.equals(actual.identity()) || actual.byteLength() != byteLength
                    || parentIdentity.volumeSerialNumber() != actual.identity().volumeSerialNumber()
                    || !expectedPath.equals(sourceArtifactPath(parent, kind, write))
                    || !expectedPath.equals(finalPath(handle)))
                    throw new IOException("Created source artifact identity, length, volume, or exact fixed child changed before close");
                observation = new SourceArtifactWriteObservation(parentIdentity, actual.identity(),
                    actual.byteLength(), actual.lastWriteFileTimeBits(), digest.digest());
            } catch (Throwable failure) {
                primary = failure;
            } finally {
                if (pathWorkspace != null && !pathWorkspace.closeAttempted) {
                    try { pathWorkspace.close(); }
                    catch (Throwable cleanup) {
                        if (primary == null) primary = cleanup;
                        else if (cleanup != primary) primary.addSuppressed(cleanup);
                    }
                }
                primary = workspace.closeScratch(parent, primary,
                    "source artifact write scratch/metadata cleanup");
            }
            if (primary != null) {
                writeFailure = primary;
                throw preserveIoFailure("finish source artifact body", primary);
            }
            completedObservation = Objects.requireNonNull(observation);
            finished = true;
            return completedObservation;
        }

        public SourceArtifactWriteObservation completedObservation() throws IOException {
            requireLive();
            if (!finished || completedObservation == null) throw new IOException("Complete source artifact write observation required");
            return completedObservation;
        }

        private void writeBytes(byte[] value, int offset, int length) throws IOException {
            requireLive();
            if (finishAttempted || stream == null || stream.closeAttempted || writeFailure != null)
                throw new IOException("Source artifact write is not accepting body bytes", writeFailure);
            Objects.checkFromIndexSize(offset, length, value.length);
            long next;
            try { next = Math.addExact(byteLength, length); }
            catch (ArithmeticException overflow) { throw recordWriteFailure(new IOException("Source artifact length overflow", overflow)); }
            if (next > explicitArtifactLimit)
                throw recordWriteFailure(new IOException("Source artifact exceeds the active native artifact bound"));
            try {
                int consumed = 0;
                while (consumed < length) {
                    int request = Math.min(SOURCE_ARTIFACT_TRANSFER_BYTES, length - consumed);
                    workspace.transferBuffer.asSlice(0L, request)
                        .copyFrom(MemorySegment.ofArray(value).asSlice(offset + consumed, request));
                    bool("WriteFile", handle.operation, handle.value, workspace.transferBuffer, request,
                        workspace.transferCount, MemorySegment.NULL);
                    int written = progress(workspace.transferCount.get(ValueLayout.JAVA_INT, 0), request);
                    digest.update(value, offset + consumed, written);
                    byteLength = Math.addExact(byteLength, written);
                    consumed = Math.addExact(consumed, written);
                }
            } catch (Throwable failure) { throw recordWriteFailure(failure); }
            if (byteLength != next) throw recordWriteFailure(new IOException("Source artifact byte count diverged from accepted write length"));
        }

        private void writeSingle(int value) throws IOException {
            byte[] one = {(byte) value};
            writeBytes(one, 0, 1);
        }

        private IOException recordWriteFailure(Throwable failure) {
            if (writeFailure == null) writeFailure = failure;
            else if (writeFailure != failure) writeFailure.addSuppressed(failure);
            return preserveIoFailure("source artifact body write", writeFailure);
        }

        private void requireLive() throws IOException {
            checkOwner();
            if (closeAttempted || handle.closeAttempted || !owned.containsKey(handle))
                throw new IOException("Exact live source artifact CREATE_NEW pin required");
            parent.requireLive();
        }

        @Override public void close() throws IOException {
            checkOwner();
            if (closeAttempted) throw new IllegalStateException("Source artifact CREATE_NEW close already attempted");
            closeAttempted = true;
            SourceArtifactWorkspace exactWorkspace = workspace;
            workspace = null;
            Throwable cleanup = exactWorkspace.closeScratch(parent, null, "source artifact write scratch close");
            boolean childCloseReturned = false;
            try { handle.close(); parent.releaseChild(); }
            catch (Throwable failure) {
                parent.retainChildCloseFailure(failure);
                if (cleanup == null) cleanup = failure; else if (cleanup != failure) cleanup.addSuppressed(failure);
            }
            if (!owned.containsKey(handle)) childCloseReturned = true;
            if (childCloseReturned)
                cleanup = exactWorkspace.finishCloseAfterChild(parent, cleanup,
                    "source artifact write observation/attempt release", true);
            if (cleanup == null && !finished && writeFailure == null)
                cleanup = new IOException("Source artifact CREATE_NEW pin closed before complete body finish");
            if (cleanup != null) throw preserveIoFailure("close source artifact CREATE_NEW pin", cleanup);
        }
    }

    /** Fixed-role OPEN_EXISTING source body pin; compared through the exact held HANDLE to EOF. */
    public final class SourceArtifactReadbackPin implements AutoCloseable {
        private final SourceDirectoryPin parent;
        private final WindowsNormalRecallSourceStore.ArtifactKind kind;
        private final UUID write;
        private final Handle handle;
        private final SourceArtifactWriteObservation expected;
        private final SourceFileObservation openedObservation;
        private SourceArtifactWorkspace workspace;
        private final long explicitArtifactLimit;
        private final java.security.MessageDigest digest;
        private SourceArtifactCompareOutputStream stream;
        private long comparedLength;
        private SourceFileObservation completedObservation;
        private Throwable compareFailure;
        private boolean compareAttempted, compared, closeAttempted;

        private SourceArtifactReadbackPin(SourceDirectoryPin parent,
                WindowsNormalRecallSourceStore.ArtifactKind kind, UUID write, Handle handle,
                SourceArtifactWriteObservation expected, SourceFileObservation openedObservation,
                SourceArtifactWorkspace workspace, long explicitArtifactLimit, java.security.MessageDigest digest) {
            this.parent = parent; this.kind = kind; this.write = write; this.handle = handle;
            this.expected = expected; this.openedObservation = openedObservation; this.workspace = workspace;
            this.explicitArtifactLimit = explicitArtifactLimit; this.digest = digest;
        }

        public OutputStream comparisonOutputStream() throws IOException {
            requireLive();
            if (compareAttempted || stream != null) throw new IOException("Source artifact full readback comparator is one-shot");
            compareAttempted = true;
            try (SourcePathWorkspace ignored = sourcePathWorkspace(parent.retainedPathAllocation, parent,
                    "source artifact OPEN_EXISTING compare-start exact-child verification")) {
                String expectedPath = sourceArtifactPath(parent, kind, write);
                if (!expectedPath.equals(finalPath(handle)))
                    throw new IOException("Reopened source artifact HANDLE left its exact fixed parent child before comparison");
                bool("SetFilePointerEx", handle.operation, handle.value, 0L, MemorySegment.NULL, 0);
                stream = new SourceArtifactCompareOutputStream(this);
                return stream;
            } catch (Throwable failure) { throw recordCompareFailure(failure); }
        }

        /** Requires the second immutable source stream to compare byte-for-byte, then proves EOF and same-handle identity/stamp. */
        public SourceFileObservation finishComparison() throws IOException {
            requireLive();
            if (!compareAttempted || stream == null || !stream.closeAttempted)
                throw new IOException("Exact source artifact comparator stream must return and close before finish");
            Throwable primary = compareFailure;
            SourceFileObservation actual = null;
            SourcePathWorkspace pathWorkspace = null;
            try {
                if (primary != null) throw preserveIoFailure("source artifact comparison", primary);
                pathWorkspace = sourcePathWorkspace(parent.retainedPathAllocation, parent,
                    "source artifact OPEN_EXISTING compare-finish exact-child verification");
                String path = sourceArtifactPath(parent, kind, write);
                if (!path.equals(finalPath(handle)))
                    throw new IOException("Source artifact readback HANDLE left its exact fixed parent child during comparison");
                bool("ReadFile", handle.operation, handle.value, workspace.transferBuffer, 1,
                    workspace.transferCount, MemorySegment.NULL);
                if (workspace.transferCount.get(ValueLayout.JAVA_INT, 0) != 0)
                    throw new IOException("Source artifact readback contains trailing bytes after complete body comparison");
                metadata(handle, false, expected.byteLength());
                actual = observeSourceFile(handle);
                if (!openedObservation.equals(actual)
                    || !expected.fileIdentity().equals(actual.identity())
                    || expected.byteLength() != actual.byteLength()
                    || comparedLength != expected.byteLength()
                    || !java.security.MessageDigest.isEqual(expected.contentSha256(), digest.digest()))
                    throw new IOException("Source artifact readback identity, length, FILETIME, or SHA differs from its exact CREATE_NEW body");
                if (!path.equals(sourceArtifactPath(parent, kind, write)) || !path.equals(finalPath(handle)))
                    throw new IOException("Source artifact readback HANDLE is no longer the exact fixed child of its pinned parent");
            } catch (Throwable failure) {
                if (primary == null) primary = failure;
                else if (failure != primary) primary.addSuppressed(failure);
            } finally {
                if (pathWorkspace != null && !pathWorkspace.closeAttempted) {
                    try { pathWorkspace.close(); }
                    catch (Throwable cleanup) {
                        if (primary == null) primary = cleanup;
                        else if (cleanup != primary) primary.addSuppressed(cleanup);
                    }
                }
                primary = workspace.closeScratch(parent, primary,
                    "source artifact readback scratch/metadata cleanup");
            }
            if (primary != null) {
                compareFailure = primary;
                throw preserveIoFailure("finish source artifact full readback", primary);
            }
            completedObservation = Objects.requireNonNull(actual);
            compared = true;
            return completedObservation;
        }

        public SourceFileObservation completedObservation() throws IOException {
            requireLive();
            if (!compared || completedObservation == null) throw new IOException("Complete source artifact readback observation required");
            return completedObservation;
        }

        private void compareBytes(byte[] value, int offset, int length) throws IOException {
            requireLive();
            if (!compareAttempted || stream == null || stream.closeAttempted || compareFailure != null)
                throw new IOException("Source artifact comparator is not accepting body bytes", compareFailure);
            Objects.checkFromIndexSize(offset, length, value.length);
            long next;
            try { next = Math.addExact(comparedLength, length); }
            catch (ArithmeticException overflow) { throw recordCompareFailure(new IOException("Source artifact comparison length overflow", overflow)); }
            if (next > expected.byteLength() || next > explicitArtifactLimit)
                throw recordCompareFailure(new IOException("Source artifact body exceeds its exact readback length"));
            try {
                int consumed = 0;
                while (consumed < length) {
                    int request = Math.min(SOURCE_ARTIFACT_TRANSFER_BYTES, length - consumed);
                    bool("ReadFile", handle.operation, handle.value, workspace.transferBuffer, request,
                        workspace.transferCount, MemorySegment.NULL);
                    int read = progress(workspace.transferCount.get(ValueLayout.JAVA_INT, 0), request);
                    for (int index = 0; index < read; index++)
                        if (workspace.transferBuffer.get(ValueLayout.JAVA_BYTE, index) != value[offset + consumed + index])
                            throw new IOException("Source artifact full readback byte differs from the retained immutable body");
                    digest.update(value, offset + consumed, read);
                    comparedLength = Math.addExact(comparedLength, read);
                    consumed = Math.addExact(consumed, read);
                }
            } catch (Throwable failure) { throw recordCompareFailure(failure); }
            if (comparedLength != next)
                throw recordCompareFailure(new IOException("Source artifact readback count diverged from the immutable body stream"));
        }

        private void compareSingle(int value) throws IOException {
            byte[] one = {(byte) value};
            compareBytes(one, 0, 1);
        }

        private IOException recordCompareFailure(Throwable failure) {
            if (compareFailure == null) compareFailure = failure;
            else if (compareFailure != failure) compareFailure.addSuppressed(failure);
            return preserveIoFailure("source artifact readback compare", compareFailure);
        }

        private void requireLive() throws IOException {
            checkOwner();
            if (closeAttempted || handle.closeAttempted || !owned.containsKey(handle))
                throw new IOException("Exact live source artifact OPEN_EXISTING pin required");
            parent.requireLive();
        }

        @Override public void close() throws IOException {
            checkOwner();
            if (closeAttempted) throw new IllegalStateException("Source artifact readback close already attempted");
            closeAttempted = true;
            SourceArtifactWorkspace exactWorkspace = workspace;
            workspace = null;
            Throwable cleanup = exactWorkspace.closeScratch(parent, null, "source artifact readback scratch close");
            boolean childCloseReturned = false;
            try { handle.close(); parent.releaseChild(); }
            catch (Throwable failure) {
                parent.retainChildCloseFailure(failure);
                if (cleanup == null) cleanup = failure; else if (cleanup != failure) cleanup.addSuppressed(failure);
            }
            if (!owned.containsKey(handle)) childCloseReturned = true;
            if (childCloseReturned)
                cleanup = exactWorkspace.finishCloseAfterChild(parent, cleanup,
                    "source artifact readback observation/attempt release", true);
            if (cleanup == null && !compared && compareFailure == null)
                cleanup = new IOException("Source artifact readback closed before complete compare/EOF verification");
            if (cleanup != null) throw preserveIoFailure("close source artifact readback pin", cleanup);
        }
    }

    private final class SourceArtifactWorkspace {
        private SourceDirectoryPin parent;
        private SourceForestAllocationBudget allocation;
        private WindowsNormalRecallSourceStore.ArtifactKind kind;
        private UUID write, operation;
        private String context;
        private Arena arena;
        private MemorySegment transferBuffer, transferCount;
        private boolean arenaCreationAttempted, arenaCreationReturned;
        private boolean scratchCloseAttempted, arenaClosed, scratchReleaseAttempted, scratchReleased;
        private boolean observationReleaseAttempted, observationReleased;
        private boolean attemptReleaseAttempted, attemptChargeReleased;
        private Throwable arenaCloseFailure, scratchReleaseFailure, observationReleaseFailure, attemptReleaseFailure;

        private SourceArtifactWorkspace(SourceDirectoryPin parent, SourceForestAllocationBudget allocation,
                WindowsNormalRecallSourceStore.ArtifactKind kind, UUID write, UUID operation, String context) {
            this.parent=Objects.requireNonNull(parent);this.allocation=Objects.requireNonNull(allocation);
            this.kind=Objects.requireNonNull(kind);this.write=Objects.requireNonNull(write);
            this.operation=Objects.requireNonNull(operation);this.context=Objects.requireNonNull(context);
        }

        private void initializeArena() throws IOException {
            if (arenaCreationAttempted) throw new IllegalStateException("Source artifact transfer Arena is one-shot");
            arenaCreationAttempted=true;
            try {
                arena=Arena.ofConfined();
                arenaCreationReturned=true;
                transferBuffer=arena.allocate(SOURCE_ARTIFACT_TRANSFER_BYTES);
                transferCount=arena.allocate(ValueLayout.JAVA_INT);
            } catch (Throwable failure) {
                parent.retainArtifactWorkspaceFailure(failure);
                throw preserveIoFailure("allocate registered source artifact workspace", failure);
            }
        }

        private Throwable closeScratch(SourceDirectoryPin exactParent, Throwable primary, String closeContext) {
            if (parent != exactParent) {
                IOException failure=new IOException("Exact registered source artifact workspace parent is required");
                exactParent.retainArtifactWorkspaceFailure(failure);
                return addCleanup(primary,failure);
            }
            if (!scratchCloseAttempted) {
                scratchCloseAttempted=true;
                if (arena == null) {
                    // No Arena was returned by the one-shot factory, so there is no handle to close.
                    if (!arenaCreationReturned) arenaClosed=true;
                } else {
                    try {
                        arena.close();
                        arenaClosed=true;
                        transferBuffer=null;transferCount=null;arena=null;
                    } catch (Throwable failure) {
                        arenaCloseFailure=failure;
                        exactParent.retainArtifactWorkspaceFailure(failure);
                    }
                }
            }
            if (!arenaClosed && arenaCloseFailure != null) primary=addCleanup(primary,arenaCloseFailure);
            if (arenaClosed && !scratchReleaseAttempted) {
                scratchReleaseAttempted=true;
                try {
                    allocation.releaseTemporary(SOURCE_ARTIFACT_SCRATCH_CHARGE+SOURCE_ARTIFACT_TRANSFER_COUNT_CHARGE,
                        closeContext + " confirmed-closed native transfer buffer and DWORD count");
                    scratchReleased=true;
                } catch (Throwable failure) {
                    scratchReleaseFailure=failure;
                    exactParent.retainArtifactWorkspaceFailure(failure);
                }
            }
            if (!scratchReleased && scratchReleaseFailure != null) primary=addCleanup(primary,scratchReleaseFailure);
            return primary;
        }

        private Throwable releaseObservationCharge(SourceDirectoryPin exactParent, Throwable primary, String releaseContext) {
            if (parent != exactParent || exactParent.retainedArtifactWorkspace() != this) {
                IOException failure=new IOException("Exact parent registration is required to release source artifact observation fees");
                exactParent.retainArtifactWorkspaceFailure(failure);
                return addCleanup(primary,failure);
            }
            if (!arenaClosed || !scratchReleased || observationReleaseAttempted) {
                if (!observationReleased && observationReleaseFailure != null)
                    primary=addCleanup(primary,observationReleaseFailure);
                return primary;
            }
            observationReleaseAttempted=true;
            try {
                allocation.releaseTemporary(SOURCE_ARTIFACT_OBSERVATION_CHARGE,releaseContext);
                observationReleased=true;
            } catch (Throwable failure) {
                observationReleaseFailure=failure;
                exactParent.retainArtifactWorkspaceFailure(failure);
                primary=addCleanup(primary,failure);
            }
            return primary;
        }

        private Throwable releaseAttemptChargeAndUnregister(SourceDirectoryPin exactParent, Throwable primary,
                String releaseContext) {
            if (parent != exactParent || exactParent.retainedArtifactWorkspace() != this) {
                IOException failure=new IOException("Exact parent registration is required to release source artifact attempt fees");
                exactParent.retainArtifactWorkspaceFailure(failure);
                return addCleanup(primary,failure);
            }
            if (!arenaClosed || !scratchReleased || !observationReleased || attemptReleaseAttempted) {
                if (!attemptChargeReleased && attemptReleaseFailure != null)
                    primary=addCleanup(primary,attemptReleaseFailure);
                return primary;
            }
            attemptReleaseAttempted=true;
            try {
                allocation.releaseTemporary(SOURCE_ARTIFACT_ATTEMPT_CHARGE,releaseContext);
                attemptChargeReleased=true;
                // Drop the last workspace/arena/segment references before the parent unlinks this attempt.
                allocation=null;parent=null;kind=null;write=null;operation=null;context=null;
                exactParent.releaseArtifactWorkspaceRegistration(this);
            } catch (Throwable failure) {
                attemptReleaseFailure=failure;
                exactParent.retainArtifactWorkspaceFailure(failure);
                primary=addCleanup(primary,failure);
            }
            return primary;
        }

        private Throwable closeUnpublished(SourceDirectoryPin exactParent, Throwable primary,
                String cleanupContext, boolean childCloseReturned) {
            primary=closeScratch(exactParent,primary,cleanupContext + " scratch");
            return finishCloseAfterChild(exactParent,primary,cleanupContext,childCloseReturned);
        }

        private Throwable finishCloseAfterChild(SourceDirectoryPin exactParent, Throwable primary,
                String cleanupContext, boolean childCloseReturned) {
            if (!childCloseReturned) return primary;
            primary=releaseObservationCharge(exactParent,primary,cleanupContext + " observation");
            if (observationReleased)
                primary=releaseAttemptChargeAndUnregister(exactParent,primary,cleanupContext + " attempt owner");
            return primary;
        }

        private static Throwable addCleanup(Throwable primary, Throwable cleanup) {
            if (primary == null) return cleanup;
            if (cleanup != primary) primary.addSuppressed(cleanup);
            return primary;
        }
    }

    /** Reuses one precharged Win32 maximum-path peak across nested checks in one lexical native operation. */
    private final class SourcePathWorkspace implements AutoCloseable {
        private final SourceForestAllocationBudget allocation;
        private final SourceDirectoryPin parent;
        private final String context;
        private boolean closeAttempted;
        private SourcePathWorkspace(SourceForestAllocationBudget allocation, SourceDirectoryPin parent, String context) {
            this.allocation = allocation; this.parent = parent; this.context = context;
        }
        @Override public void close() throws IOException {
            checkOwner();
            if (closeAttempted) throw new IllegalStateException("Source native path workspace close already attempted");
            closeAttempted = true;
            if (activeSourcePathAllocation != allocation || activeSourcePathDepth <= 0)
                throw new IllegalStateException("Exact active source native path workspace required");
            activeSourcePathDepth--;
            if (activeSourcePathDepth != 0) return;
            activeSourcePathAllocation = null;
            try { allocation.releaseTemporary(SOURCE_PIN_PATH_WORK_CHARGE,
                context + " Win32 final-path/native-string peak"); }
            catch (Throwable failure) {
                if (parent != null) parent.retainChildCloseFailure(failure);
                throw preserveIoFailure("release source native path workspace", failure);
            }
        }
    }

    private SourcePathWorkspace sourcePathWorkspace(SourceForestAllocationBudget allocation,
            SourceDirectoryPin parent, String context) throws IOException {
        checkOwner();
        Objects.requireNonNull(allocation, "original source allocation account for native path workspace");
        if (activeSourcePathDepth == 0) {
            allocation.reserveTemporary(SOURCE_PIN_PATH_WORK_CHARGE,
                context + " maximum Win32 path/native string peak BEFORE allocation");
            activeSourcePathAllocation = allocation;
        } else if (activeSourcePathAllocation != allocation) {
            throw new IOException("Nested source native path workspaces must use the exact same original allocation account");
        }
        activeSourcePathDepth = Math.addExact(activeSourcePathDepth, 1);
        return new SourcePathWorkspace(allocation, parent, context);
    }

    private final class SourceArtifactBodyOutputStream extends OutputStream {
        private final SourceArtifactCreatePin pin;
        private boolean closeAttempted;
        private SourceArtifactBodyOutputStream(SourceArtifactCreatePin pin) { this.pin = pin; }
        @Override public void write(int value) throws IOException {
            if (closeAttempted) throw new IOException("Source artifact body stream is closed");
            pin.writeSingle(value);
        }
        @Override public void write(byte[] value, int offset, int length) throws IOException {
            if (closeAttempted) throw new IOException("Source artifact body stream is closed");
            Objects.requireNonNull(value);
            pin.writeBytes(value, offset, length);
        }
        @Override public void flush() throws IOException { if (closeAttempted) throw new IOException("Source artifact body stream is closed"); }
        @Override public void close() throws IOException {
            if (closeAttempted) throw new IOException("Source artifact body stream close already attempted");
            closeAttempted = true;
        }
    }

    private final class SourceArtifactCompareOutputStream extends OutputStream {
        private final SourceArtifactReadbackPin pin;
        private boolean closeAttempted;
        private SourceArtifactCompareOutputStream(SourceArtifactReadbackPin pin) { this.pin = pin; }
        @Override public void write(int value) throws IOException {
            if (closeAttempted) throw new IOException("Source artifact compare stream is closed");
            pin.compareSingle(value);
        }
        @Override public void write(byte[] value, int offset, int length) throws IOException {
            if (closeAttempted) throw new IOException("Source artifact compare stream is closed");
            Objects.requireNonNull(value);
            pin.compareBytes(value, offset, length);
        }
        @Override public void flush() throws IOException { if (closeAttempted) throw new IOException("Source artifact compare stream is closed"); }
        @Override public void close() throws IOException {
            if (closeAttempted) throw new IOException("Source artifact compare stream close already attempted");
            closeAttempted = true;
        }
    }

    /** Immutable FILE_ID_INFO/length/raw FILETIME projection from the pinned file HANDLE. */
    public record SourceFileObservation(FileIdentity identity, long byteLength, long lastWriteFileTimeBits) {
        public SourceFileObservation {
            Objects.requireNonNull(identity, "source file identity");
            if (byteLength < 0L) throw new IllegalArgumentException("Source file length must be nonnegative");
        }
    }
    /** Native artifact DATA emitted only after same-HANDLE complete body write and flush. */
    public record SourceArtifactWriteObservation(FileIdentity parentDirectoryIdentity, FileIdentity fileIdentity,
            long byteLength, long lastWriteFileTimeBits, byte[] contentSha256) {
        public SourceArtifactWriteObservation {
            Objects.requireNonNull(parentDirectoryIdentity); Objects.requireNonNull(fileIdentity);
            if (parentDirectoryIdentity.volumeSerialNumber() != fileIdentity.volumeSerialNumber())
                throw new IllegalArgumentException("Source artifact parent and file must share one native volume");
            if (byteLength <= 0L) throw new IllegalArgumentException("Source artifact body must be nonempty");
            if (contentSha256 == null || contentSha256.length != 32)
                throw new IllegalArgumentException("Source artifact body SHA-256 must contain exactly 32 bytes");
            contentSha256 = contentSha256.clone();
        }
        @Override public byte[] contentSha256() { return contentSha256.clone(); }
    }
    /** A search handle has its own sticky FindClose duty; CloseHandle is never valid for it. */
    final class FindHandle implements AutoCloseable {
        final MemorySegment value;
        final UUID operation;
        boolean closeAttempted;
        FindHandle(MemorySegment value, UUID operation) { this.value = value; this.operation = operation; }
        @Override public void close() throws IOException {
            checkOwner();
            if (closeAttempted) throw new IllegalStateException("FindClose already attempted");
            closeAttempted = true;
            bool("FindClose", operation, value);
            ownedFinds.remove(this); // failure keeps the unconfirmed native obligation retained.
        }
    }
    record Full20WitnessFindEntry(String basename, long length, int attributes) {}
    /** schema5固定source的真实partial owner；调用者登记返回值后才可start。 */
    final class DeploymentReceiptFindSequence implements AutoCloseable {
        private final WindowsWorldProvisioner.DeploymentReceiptIoScope source;
        private final MySqlFormat3ReadBudget root;
        private Arena arena;
        private MemorySegment data,rawHandle;
        private FindHandle handle;
        private String pattern,nativePattern,name;
        private long length,count,retainedFee=256L,scratchFee,nameFee;
        private int attributes;
        private boolean started,firstPending,exhausted,entryPresent,closeAttempted,closed;
        private Throwable failure,cleanupFailure;
        private DeploymentReceiptFindSequence(WindowsWorldProvisioner.DeploymentReceiptIoScope source,
                MySqlFormat3ReadBudget root){this.source=source;this.root=root;}
        private void requireLive()throws IOException {
            checkOwner();
            if(closed||closeAttempted||failure!=null||cleanupFailure!=null||root.isClosed())
                throw new IOException("schema5 fixed-source find unavailable",failure!=null?failure:cleanupFailure);
            if(source.io()!=WindowsMarkerIo.this)throw new IOException("schema5 find source/native issuer differs");
            source.bindAllocationRoot(root);
        }
        void start()throws IOException {
            requireLive();
            if(started)throw new IOException("schema5 fixed-source FindFirst is one-shot");
            started=true;
            try {
                String directory=source.nativePath();
                String suffix="\\.floatingpod-b1-*";
                int patternLength=Math.addExact(directory.length(),suffix.length());
                long strings=Math.addExact(64L,Math.multiplyExact((long)patternLength,2L));
                root.reserveRetainedMemory(strings,"schema5 fixed find pattern BEFORE allocation");retainedFee=Math.addExact(retainedFee,strings);
                pattern=directory+suffix;
                int argumentLength=patternLength;
                if(!pattern.startsWith(EXTENDED_PATH_PREFIX)&&patternLength+1>MAX_PATH_WITH_TERMINATOR)
                    argumentLength=pattern.startsWith("\\\\")?Math.addExact(patternLength-2,EXTENDED_UNC_PREFIX.length()):Math.addExact(patternLength,EXTENDED_PATH_PREFIX.length());
                if(argumentLength!=patternLength){
                    long converted=Math.addExact(64L,Math.multiplyExact((long)argumentLength,2L));
                    root.reserveRetainedMemory(converted,"schema5 actual extended find pattern BEFORE conversion");retainedFee=Math.addExact(retainedFee,converted);
                }
                nativePattern=nativePathArgument(pattern);
                // 原find accounting的每pass固定2048含592-byte DATA、Arena/FindHandle及控制结构。
                long nativeFee=Math.addExact(2048L,Math.multiplyExact(nativePattern.length()+1L,2L));
                root.reserveTemporaryMemory(nativeFee,"schema5 actual FindHandle/592 DATA/Arena/UTF16 pattern BEFORE native acquisition");scratchFee=nativeFee;
                arena=Arena.ofConfined();data=arena.allocate(WIN32_FIND_DATAW);
                Result result=call("FindFirstFileW",source.operation(),wide(arena,nativePattern),data);
                MemorySegment actual=(MemorySegment)result.value;
                if(actual.address()==-1L||actual.address()==0L){
                    if(result.error==ERROR_FILE_NOT_FOUND){exhausted=true;return;}
                    throw new NativeFailure("FindFirstFileW",source.operation(),result.error,nativePathDiagnostic(pattern));
                }
                rawHandle=actual;
                handle=new FindHandle(rawHandle,source.operation());rawHandle=null;
                ownedFinds.put(handle,Boolean.TRUE);
                firstPending=true;
            }catch(Throwable cause){failure=cause;throw preserveIoFailure("schema5 FindFirstFileW",cause);}
        }
        boolean next()throws IOException {
            requireLive();
            if(!started)throw new IOException("schema5 find must start after original pending registration");
            try {
                dropName();
                if(exhausted)return false;
                if(!firstPending){
                    Result result=call("FindNextFileW",source.operation(),handle.value,data);
                    if((Integer)result.value==0){
                        if(result.error==ERROR_NO_MORE_FILES){exhausted=true;return false;}
                        throw new NativeFailure("FindNextFileW",source.operation(),result.error);
                    }
                }
                firstPending=false;
                if(count>=root.profile().maxMembers())throw new IOException("schema5 directory inventory exceeds actual member limit");
                int chars=0;
                while(chars<260&&data.get(ValueLayout.JAVA_CHAR,FIND_NAME_OFFSET+chars*2L)!='\0')chars++;
                if(chars==0||chars==260)throw new IOException("schema5 FindFirst/Next returned invalid filename");
                long entryFee=Math.addExact(256L,Math.multiplyExact((long)chars,4L));
                root.reserveTemporaryMemory(entryFee,"schema5 actual filename/builder BEFORE allocation");nameFee=entryFee;
                var builder=new StringBuilder(chars);
                for(int index=0;index<chars;index++)builder.append(data.get(ValueLayout.JAVA_CHAR,FIND_NAME_OFFSET+index*2L));
                name=builder.toString();
                attributes=data.get(ValueLayout.JAVA_INT,FIND_ATTRIBUTES_OFFSET);
                length=(Integer.toUnsignedLong(data.get(ValueLayout.JAVA_INT,FIND_SIZE_HIGH_OFFSET))<<32)|
                    Integer.toUnsignedLong(data.get(ValueLayout.JAVA_INT,FIND_SIZE_LOW_OFFSET));
                count++;entryPresent=true;return true;
            }catch(Throwable cause){failure=cause;throw preserveIoFailure("schema5 FindNextFileW/data",cause);}
        }
        private void requireEntry()throws IOException {requireLive();if(!entryPresent)throw new IOException("actual current schema5 find entry required");}
        String basename()throws IOException {requireEntry();return name;}
        long length()throws IOException {requireEntry();return length;}
        int attributes()throws IOException {requireEntry();return attributes;}
        Throwable retainedFailure(){return failure!=null?failure:cleanupFailure;}
        private void dropName()throws IOException {
            name=null;entryPresent=false;
            if(nameFee!=0L){root.releaseTemporaryMemory(nameFee,"schema5 actual filename dropped");nameFee=0L;}
        }
        @Override public void close()throws IOException {
            checkOwner();
            if(closeAttempted)throw new IOException("schema5 find close is one-shot",cleanupFailure);
            closeAttempted=true;
            Throwable primary=null;
            try {
                if(handle!=null){handle.close();handle=null;}
                else if(rawHandle!=null){bool("FindClose",source.operation(),rawHandle);rawHandle=null;}
            }catch(Throwable cause){primary=cause;}
            if(handle==null&&rawHandle==null&&arena!=null){
                try{arena.close();arena=null;data=null;}
                catch(Throwable cause){if(primary==null)primary=cause;else com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,cause);}
            }
            try{dropName();}catch(Throwable cause){if(primary==null)primary=cause;else com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(primary,cause);}
            if(primary==null){
                pattern=null;nativePattern=null;
                try {
                    if(scratchFee!=0L){root.releaseTemporaryMemory(scratchFee,"schema5 native find acquisition settled");scratchFee=0L;}
                    root.releaseRetainedMemory(retainedFee,"schema5 find holder/pattern references dropped");retainedFee=0L;closed=true;
                }catch(Throwable cause){primary=cause;}
            }
            if(primary!=null){cleanupFailure=primary;if(failure!=null)com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(failure,primary);
                throw preserveIoFailure("schema5 FindClose/arena retirement",primary);}
        }
    }
    DeploymentReceiptFindSequence allocateDeploymentReceiptWitnessDiscovery(
            WindowsWorldProvisioner.DeploymentReceiptIoScope source,
            MySqlFormat3ReadBudget root)throws IOException {
        checkOwner();Objects.requireNonNull(source);Objects.requireNonNull(root);
        if(source.io()!=this)throw new IOException("exact schema5 fixed-source native issuer required");
        source.bindAllocationRoot(root);
        root.reserveRetainedMemory(256L,"schema5 partial native find holder BEFORE allocation");
        return new DeploymentReceiptFindSequence(source,root);
    }
    final class Full20WitnessFindSequence implements AutoCloseable {
        private final UUID operation;
        private final Arena arena;
        private final MemorySegment data;
        private final WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit permit;
        private final com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting;
        private FindHandle handle;
        private boolean firstPending, exhausted, closeAttempted;
        private Full20WitnessFindSequence(UUID operation, Arena arena, MemorySegment data,
                FindHandle handle, boolean firstPending,
                WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit permit,
                com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting) {
            this.operation = operation; this.arena = arena; this.data = data;
            this.handle = handle; this.firstPending = firstPending; this.exhausted = handle == null;
            this.permit = permit; this.accounting = accounting;
        }
        Full20WitnessFindEntry next() throws IOException {
            checkOwner();
            if (exhausted) return null;
            if (!firstPending) {
                Result result = call("FindNextFileW", operation, handle.value, data);
                if ((Integer) result.value == 0) {
                    if (result.error == ERROR_NO_MORE_FILES) { exhausted = true; return null; }
                    throw new NativeFailure("FindNextFileW", operation, result.error);
                }
            }
            firstPending = false;
            int attributes = data.get(ValueLayout.JAVA_INT, FIND_ATTRIBUTES_OFFSET);
            long high = Integer.toUnsignedLong(data.get(ValueLayout.JAVA_INT, FIND_SIZE_HIGH_OFFSET));
            long low = Integer.toUnsignedLong(data.get(ValueLayout.JAVA_INT, FIND_SIZE_LOW_OFFSET));
            long length = (high << 32) | low;
            StringBuilder name = new StringBuilder(64);
            boolean terminated = false;
            for (int index = 0; index < 260; index++) {
                char value = data.get(ValueLayout.JAVA_CHAR, FIND_NAME_OFFSET + index * 2L);
                if (value == '\0') { terminated = true; break; }
                name.append(value);
            }
            if (!terminated || name.isEmpty()) throw new IOException("FindFirst/Next returned an invalid long filename");
            return new Full20WitnessFindEntry(name.toString(), length, attributes);
        }
        @Override public void close() throws IOException {
            checkOwner();
            if (closeAttempted) throw new IllegalStateException("Find sequence close already attempted");
            closeAttempted = true;
            Throwable primary = null;
            try {
                if (handle != null) { handle.close(); handle = null; }
            } catch (Throwable failure) { primary = failure; throw preserveIoFailure("FindClose", failure); }
            finally {
                if (handle == null) {
                    try { arena.close(); }
                    catch (Throwable cleanup) {
                        if (primary != null) { if (cleanup != primary) primary.addSuppressed(cleanup); }
                        else throw ioFailure("Find data arena close", cleanup);
                    }
                    if(primary==null)permit.findSequenceClosed(WindowsMarkerIo.this,accounting,this);
                }
            }
        }
    }
    private enum ProcessObservationState { LIVE, POISONED, CLOSING, EXIT_CONFIRMED, DISCARDED, CLOSE_FAILED }
    /**
     * 只保留一次实际 OpenProcess 得到的精确 HANDLE；不携带世界、owner 或启动权限。
     * 此对象和 HANDLE 只能由创建它的 WindowsMarkerIo owner thread 使用。
     */
    public static final class RetainedProcessObservation implements AutoCloseable {
        private final WindowsMarkerIo issuer;
        private final Handle handle;
        private final Thread owner;
        private final long pid;
        private final long creationFileTime;
        private final UUID operation;
        private ProcessObservationState state = ProcessObservationState.LIVE;
        private RetainedProcessObservation(WindowsMarkerIo issuer, Handle handle, long pid,
                                           long creationFileTime, UUID operation) {
            this.issuer = issuer; this.handle = handle; this.owner = issuer.owner;
            this.pid = pid; this.creationFileTime = creationFileTime; this.operation = operation;
        }
        /** 返回同一 HANDLE 当前是否已 signaled；WAIT_TIMEOUT 不释放或替换 HANDLE。 */
        public boolean isSignaled() throws IOException { return issuer.pollRetainedProcess(this); }
        /** 丢弃尚未确认的 observation；CloseHandle 未确认时保留 owned cleanup debt。 */
        @Override public void close() throws IOException { issuer.discardRetainedProcess(this); }
    }
    static final class ProcessExitAcknowledgement {
        private final WindowsMarkerIo issuer;
        private final RetainedProcessObservation observation;
        private final long pid, creationFileTime;
        private final UUID operation;
        private boolean claimed;
        private ProcessExitAcknowledgement(WindowsMarkerIo issuer, RetainedProcessObservation observation,
                                           long pid, long creationFileTime, UUID operation) {
            this.issuer = issuer; this.observation = observation; this.pid = pid;
            this.creationFileTime = creationFileTime; this.operation = operation;
        }
    }
    enum PreviousProcessEvidenceKind {
        EXACT_HANDLE_PROCESS_EXIT,
        PROCESS_ID_REUSED,
        OLD_LIFETIME_ABSENT_FROM_COMPLETE_SNAPSHOT
    }
    static abstract sealed class PreviousProcessLifetimeObservation
            permits ExactHandleProcessExit, ProcessIdReused, OldLifetimeEndedByCompleteSnapshot {
        private final WindowsMarkerIo issuer;
        private final long previousPid;
        private final long previousCreationFileTime;
        private final UUID operation;
        private boolean claimed;
        private PreviousProcessLifetimeObservation(WindowsMarkerIo issuer, long previousPid,
                long previousCreationFileTime, UUID operation) {
            this.issuer = issuer;
            this.previousPid = previousPid;
            this.previousCreationFileTime = previousCreationFileTime;
            this.operation = operation;
        }
        final PreviousProcessEvidenceKind kind() { return evidenceKind(); }
        final long previousPid() { return previousPid; }
        final long previousCreationFileTime() { return previousCreationFileTime; }
        final UUID operation() { return operation; }
        abstract PreviousProcessEvidenceKind evidenceKind();
    }
    static final class ExactHandleProcessExit extends PreviousProcessLifetimeObservation {
        private final Handle exactHandle;
        private ExactHandleProcessExit(WindowsMarkerIo issuer, Handle exactHandle, long pid,
                long creationFileTime, UUID operation) {
            super(issuer, pid, creationFileTime, operation);
            this.exactHandle = exactHandle;
        }
        @Override PreviousProcessEvidenceKind evidenceKind() {
            return PreviousProcessEvidenceKind.EXACT_HANDLE_PROCESS_EXIT;
        }
    }
    static final class ProcessIdReused extends PreviousProcessLifetimeObservation {
        private final long observedCreationFileTime;
        private final int waitResult;
        private ProcessIdReused(WindowsMarkerIo issuer, long pid, long previousCreationFileTime,
                long observedCreationFileTime, int waitResult, UUID operation) {
            super(issuer, pid, previousCreationFileTime, operation);
            this.observedCreationFileTime = observedCreationFileTime;
            this.waitResult = waitResult;
        }
        long observedCreationFileTime() { return observedCreationFileTime; }
        int waitResult() { return waitResult; }
        @Override PreviousProcessEvidenceKind evidenceKind() {
            return PreviousProcessEvidenceKind.PROCESS_ID_REUSED;
        }
    }
    static final class OldLifetimeEndedByCompleteSnapshot extends PreviousProcessLifetimeObservation {
        private final long enumeratedProcessCount;
        private final int terminalError;
        private OldLifetimeEndedByCompleteSnapshot(WindowsMarkerIo issuer, long pid,
                long creationFileTime, long enumeratedProcessCount, int terminalError, UUID operation) {
            super(issuer, pid, creationFileTime, operation);
            this.enumeratedProcessCount = enumeratedProcessCount;
            this.terminalError = terminalError;
        }
        long enumeratedProcessCount() { return enumeratedProcessCount; }
        int terminalError() { return terminalError; }
        @Override PreviousProcessEvidenceKind evidenceKind() {
            return PreviousProcessEvidenceKind.OLD_LIFETIME_ABSENT_FROM_COMPLETE_SNAPSHOT;
        }
    }
    private MemorySegment wide(Arena arena, Path path) {
        return wide(arena, nativePathArgument(path.toAbsolutePath().normalize().toString()));
    }
    private MemorySegment wideDirectory(Arena arena, Path path) {
        return wide(arena, nativeDirectoryPathArgument(path.toAbsolutePath().normalize().toString()));
    }
    /**
     * 将绝对、已规范化的 Java Windows 路径映射到 Win32 文件 API 参数。
     * MAX_PATH 包含结尾 NUL；目录 API 使用更窄界限，长路径使用扩展命名空间。
     */
    static String nativePathArgument(String absoluteNormalizedPath) {
        return nativePathArgument(absoluteNormalizedPath, MAX_PATH_WITH_TERMINATOR);
    }
    static String nativeDirectoryPathArgument(String absoluteNormalizedPath) {
        return nativePathArgument(absoluteNormalizedPath, MAX_DIRECTORY_PATH_WITH_TERMINATOR);
    }
    private static String nativePathArgument(String absoluteNormalizedPath, int legacyMaxPathWithTerminator) {
        Objects.requireNonNull(absoluteNormalizedPath);
        if (absoluteNormalizedPath.isEmpty() || absoluteNormalizedPath.indexOf('\0') >= 0
            || absoluteNormalizedPath.indexOf('/') >= 0 || absoluteNormalizedPath.length() > MAX_WIN32_PATH_CODE_UNITS)
            throw new IllegalArgumentException("Invalid absolute normalized Windows path");
        if (absoluteNormalizedPath.startsWith("\\\\.\\"))
            throw new IllegalArgumentException("Win32 device namespace paths are not file paths");

        if (absoluteNormalizedPath.startsWith(EXTENDED_PATH_PREFIX)) {
            if (isExtendedDrivePath(absoluteNormalizedPath) || isExtendedVolumePath(absoluteNormalizedPath))
                return absoluteNormalizedPath;
            if (absoluteNormalizedPath.regionMatches(true, 0, EXTENDED_UNC_PREFIX, 0, EXTENDED_UNC_PREFIX.length())) {
                requireUncShare(absoluteNormalizedPath.substring(EXTENDED_UNC_PREFIX.length()));
                return absoluteNormalizedPath;
            }
            throw new IllegalArgumentException("Unsupported extended Windows namespace path");
        }

        final String extended;
        if (isDriveAbsolutePath(absoluteNormalizedPath, 0)) {
            if (absoluteNormalizedPath.length() + 1 <= legacyMaxPathWithTerminator) return absoluteNormalizedPath;
            extended = EXTENDED_PATH_PREFIX + absoluteNormalizedPath;
        } else if (absoluteNormalizedPath.startsWith("\\\\")) {
            requireUncShare(absoluteNormalizedPath.substring(2));
            if (absoluteNormalizedPath.length() + 1 <= legacyMaxPathWithTerminator) return absoluteNormalizedPath;
            extended = EXTENDED_UNC_PREFIX + absoluteNormalizedPath.substring(2);
        } else {
            throw new IllegalArgumentException("Absolute drive, UNC, or volume-GUID path required");
        }
        if (extended.length() > MAX_WIN32_PATH_CODE_UNITS)
            throw new IllegalArgumentException("Extended Win32 path exceeds supported UTF-16 bound");
        return extended;
    }
    private static boolean isDriveAbsolutePath(String path, int start) {
        return path.length() >= start + 3 && isAsciiLetter(path.charAt(start))
            && path.charAt(start + 1) == ':' && path.charAt(start + 2) == '\\';
    }
    private static boolean isAsciiLetter(char value) {
        return (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z');
    }
    private static boolean isExtendedDrivePath(String path) {
        return path.startsWith(EXTENDED_PATH_PREFIX) && isDriveAbsolutePath(path, EXTENDED_PATH_PREFIX.length());
    }
    private static boolean isExtendedVolumePath(String path) {
        if (!path.regionMatches(true, 0, EXTENDED_VOLUME_PREFIX, 0, EXTENDED_VOLUME_PREFIX.length())) return false;
        int guidStart = EXTENDED_VOLUME_PREFIX.length(), guidEnd = guidStart + 36;
        if (path.length() < guidEnd + 2 || path.charAt(guidEnd) != '}' || path.charAt(guidEnd + 1) != '\\') return false;
        String guid = path.substring(guidStart, guidEnd);
        for (int i = 0; i < guid.length(); i++) {
            char value = guid.charAt(i);
            if (i == 8 || i == 13 || i == 18 || i == 23) {
                if (value != '-') return false;
            } else if (!isAsciiHex(value)) return false;
        }
        return true;
    }
    private static boolean isAsciiHex(char value) {
        return (value >= '0' && value <= '9') || (value >= 'A' && value <= 'F') || (value >= 'a' && value <= 'f');
    }
    private static void requireUncShare(String serverAndShare) {
        int serverEnd = serverAndShare.indexOf('\\');
        if (serverEnd <= 0) throw new IllegalArgumentException("UNC path requires a server and share");
        int shareStart = serverEnd + 1;
        int shareEnd = serverAndShare.indexOf('\\', shareStart);
        int actualShareEnd = shareEnd < 0 ? serverAndShare.length() : shareEnd;
        if (actualShareEnd == shareStart) throw new IllegalArgumentException("UNC path requires a non-empty share");
    }
    static MemorySegment wide(Arena arena, String value) {
        Objects.requireNonNull(value);
        if (value.indexOf('\0') >= 0 || value.length() > MAX_WIN32_PATH_CODE_UNITS) throw new IllegalArgumentException("Invalid UTF-16 native path");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.isHighSurrogate(c)) {
                if (++i >= value.length() || !Character.isLowSurrogate(value.charAt(i))) throw new IllegalArgumentException("Unpaired path surrogate");
            } else if (Character.isLowSurrogate(c)) throw new IllegalArgumentException("Unpaired path surrogate");
        }
        MemorySegment memory = arena.allocate((value.length() + 1L) * 2, 2);
        for (int i = 0; i < value.length(); i++) memory.set(ValueLayout.JAVA_CHAR, i * 2L, value.charAt(i));
        memory.set(ValueLayout.JAVA_CHAR, value.length() * 2L, '\0');
        return memory;
    }
    Handle open(Path path, int access, int share, int disposition, int flags, UUID operation) throws IOException {
        // 保持既有Path入口的absolute/normalize语义；native volume GUID不经过Java Path parser。
        return openNative(path.toAbsolutePath().normalize().toString(), access, share, disposition, flags, operation);
    }
    Handle openRegionReadHandle(Path path, boolean secondPathHandle, UUID operation) throws IOException {
        if (regionSession == null) throw new IllegalStateException("Region-only handle requires its registered read session");
        Handle handle = open(path, READ, SHARE_READ | SHARE_WRITE | SHARE_DELETE, OPEN_EXISTING, NORMAL | OPEN_REPARSE, operation);
        regionSession.registerRegionHandle(handle, secondPathHandle);
        return handle;
    }
    /** 仅B1历史固定槽presence probe；不提供任意String CREATE/MOVE/delete入口。 */
    Handle openExistingNativeLegacySlot(String exactNativeWorld, String filename, UUID operation) throws IOException {
        WindowsB1SourceAnchorStore.requireNativeWorldPath(exactNativeWorld);
        if (filename == null || !filename.matches("\\.floatingpod-b1-resume-v[123]\\.(0|1|tmp)"))
            throw new IllegalArgumentException("Exact legacy B1 slot filename required");
        return openNative(exactNativeWorld + "\\" + filename, READ, 0, OPEN_EXISTING, NORMAL | OPEN_REPARSE, operation);
    }
    /** Schema5 仅探测固定旧槽冲突；不开放通用 native String 路径操作。 */
    Handle openExistingNativeSchema5ConflictSlot(String exactNativeWorld, String filename, UUID operation) throws IOException {
        WindowsB1SourceAnchorStore.requireNativeWorldPath(exactNativeWorld);
        if (filename == null || !filename.matches("\\.floatingpod-b1-resume-v[1-5]\\.(0|1|tmp)"))
            throw new IllegalArgumentException("Exact schema5 conflicting B1 slot filename required");
        return openNative(exactNativeWorld + "\\" + filename, READ, 0, OPEN_EXISTING, NORMAL | OPEN_REPARSE, operation);
    }
    /** CREATE_NEW entry for the fixed result-core publisher; names derive only from the retained write/attempt IDs. */
    SourceResultTemporaryPin createSourceResultTemporary(SourceDirectoryPin exactParent,
            UUID write, UUID publicationAttempt, UUID operation) throws IOException {
        Objects.requireNonNull(exactParent, "exact source result parent pin");
        Objects.requireNonNull(write, "source result write UUID");
        Objects.requireNonNull(publicationAttempt, "source result publication attempt UUID");
        Objects.requireNonNull(operation, "source result native operation UUID");
        if (!exactParent.issuedBy(this)) throw new IOException("Source result parent pin belongs to another WindowsMarkerIo instance");
        SourcePathWorkspace pathWorkspace = sourcePathWorkspace(exactParent.retainedPathAllocation, exactParent,
            "source result CREATE_NEW exact path/open");
        boolean childReserved = false;
        Handle handle = null;
        try {
            String basename = sourceResultTemporaryBasename(write, publicationAttempt);
            String path = sourceResultPath(exactParent, basename);
            exactParent.reserveChild();
            childReserved = true;
            handle = openNative(path, READ | WRITE, 0, CREATE_NEW, NORMAL | OPEN_REPARSE, operation);
            metadata(handle, false, 0L);
            FileIdentity created = identity(handle);
            if (created.volumeSerialNumber() != exactParent.identity().volumeSerialNumber()
                || !path.equals(finalPath(handle)) || !path.equals(sourceResultPath(exactParent, basename)))
                throw new IOException("Source result temporary CREATE_NEW is not the exact fixed child of its pinned parent");
            pathWorkspace.close();
            return new SourceResultTemporaryPin(exactParent, write, publicationAttempt, handle, created);
        } catch (Throwable primary) {
            if (childReserved) releaseChildAfterFailedOpen(exactParent, handle, primary);
            if (!pathWorkspace.closeAttempted) {
                try { pathWorkspace.close(); }
                catch (Throwable cleanup) { if (cleanup != primary) primary.addSuppressed(cleanup); }
            }
            throw preserveIoFailure("create source result temporary", primary);
        }
    }
    /** Reopens only the one fixed canonical result name, with WRITE for FlushFileBuffers and no sharing. */
    SourceResultCanonicalPin openExistingSourceResultCanonical(SourceDirectoryPin exactParent,
            UUID write, UUID operation) throws IOException {
        Objects.requireNonNull(exactParent, "exact source result parent pin");
        Objects.requireNonNull(write, "source result write UUID");
        Objects.requireNonNull(operation, "source result native operation UUID");
        if (!exactParent.issuedBy(this)) throw new IOException("Source result parent pin belongs to another WindowsMarkerIo instance");
        SourcePathWorkspace pathWorkspace = sourcePathWorkspace(exactParent.retainedPathAllocation, exactParent,
            "source result canonical OPEN_EXISTING exact path/open");
        boolean childReserved = false;
        Handle handle = null;
        try {
            String basename = sourceResultCanonicalBasename(write);
            String path = sourceResultPath(exactParent, basename);
            exactParent.reserveChild();
            childReserved = true;
            handle = openNative(path, READ | WRITE, 0, OPEN_EXISTING, NORMAL | OPEN_REPARSE, operation);
            SourceFileObservation observed = observeSourceFile(handle);
            if (observed.byteLength() <= 0L
                || observed.identity().volumeSerialNumber() != exactParent.identity().volumeSerialNumber()
                || !path.equals(finalPath(handle)) || !path.equals(sourceResultPath(exactParent, basename)))
                throw new IOException("Source result canonical OPEN_EXISTING is not the exact non-empty fixed child");
            pathWorkspace.close();
            return new SourceResultCanonicalPin(exactParent, write, handle, observed.identity());
        } catch (Throwable primary) {
            if (childReserved) releaseChildAfterFailedOpen(exactParent, handle, primary);
            if (!pathWorkspace.closeAttempted) {
                try { pathWorkspace.close(); }
                catch (Throwable cleanup) { if (cleanup != primary) primary.addSuppressed(cleanup); }
            }
            throw preserveIoFailure("open source result canonical", primary);
        }
    }
    /** MoveFileExW with fixed same-parent result names and the contract's only permitted flag. */
    void moveSourceResultWriteThroughNoReplace(SourceDirectoryPin exactParent,
            UUID write, UUID publicationAttempt, UUID operation) throws IOException {
        Objects.requireNonNull(exactParent, "exact source result parent pin");
        Objects.requireNonNull(write, "source result write UUID");
        Objects.requireNonNull(publicationAttempt, "source result publication attempt UUID");
        Objects.requireNonNull(operation, "source result native operation UUID");
        if (!exactParent.issuedBy(this)) throw new IOException("Source result move parent pin belongs to another WindowsMarkerIo instance");
        try (SourcePathWorkspace ignored = sourcePathWorkspace(exactParent.retainedPathAllocation, exactParent,
                "source result no-replace move exact parent/path workspace")) {
            String parentBefore = sourceDirectoryPath(exactParent);
            String temporaryBasename = sourceResultTemporaryBasename(write, publicationAttempt);
            String canonicalBasename = sourceResultCanonicalBasename(write);
            String temporary = nativePathArgument(joinNativeChild(parentBefore, temporaryBasename));
            String canonical = nativePathArgument(joinNativeChild(parentBefore, canonicalBasename));
            if (!temporary.equals(sourceResultPath(exactParent, temporaryBasename))
                || !canonical.equals(sourceResultPath(exactParent, canonicalBasename)))
                throw new IOException("Source result move names do not resolve to the pinned exact parent");
            try (Arena arena = Arena.ofConfined()) {
                bool("MoveFileExW", operation, wide(arena, temporary), wide(arena, canonical), WRITE_THROUGH);
            }
            String parentAfter = sourceDirectoryPath(exactParent);
            if (!parentBefore.equals(parentAfter)
                || !canonical.equals(sourceResultPath(exactParent, canonicalBasename)))
                throw new IOException("Source result parent path changed across the no-replace move");
        }
    }
    static String sourceResultTemporaryBasename(UUID write, UUID publicationAttempt) {
        Objects.requireNonNull(write, "write UUID");
        Objects.requireNonNull(publicationAttempt, "publication attempt UUID");
        return ".floatingpod-source-result-" + compactUuid(write) + "-" + compactUuid(publicationAttempt) + ".tmp";
    }
    static String sourceResultCanonicalBasename(UUID write) {
        Objects.requireNonNull(write, "write UUID");
        return ".floatingpod-source-result-" + compactUuid(write) + ".core";
    }
    private static String compactUuid(UUID value) {
        return value.toString().replace("-", "");
    }
    private String sourceResultPath(SourceDirectoryPin exactParent, String fixedBasename) throws IOException {
        return nativePathArgument(joinNativeChild(sourceDirectoryPath(exactParent), fixedBasename));
    }
    private void validateSourceResultChild(SourceDirectoryPin exactParent, Handle handle,
            String fixedBasename, FileIdentity expectedIdentity) throws IOException {
        SourceFileObservation actual = observeSourceFile(handle);
        String expectedPath = sourceResultPath(exactParent, fixedBasename);
        if (!expectedIdentity.equals(actual.identity()) || !expectedPath.equals(finalPath(handle))
            || !expectedPath.equals(sourceResultPath(exactParent, fixedBasename)))
            throw new IOException("Pinned result core HANDLE is not the exact expected child of its source parent");
    }
    private void closeSourceResultChild(SourceDirectoryPin parent, Handle handle,
            String basename, FileIdentity expectedIdentity, String context) throws IOException {
        Throwable primary = null;
        try (SourcePathWorkspace ignored = sourcePathWorkspace(parent.retainedPathAllocation, parent,
                context + " exact-child verification")) {
            validateSourceResultChild(parent, handle, basename, expectedIdentity);
        } catch (Throwable failure) {
            primary = failure;
            parent.retainChildCloseFailure(failure);
        }
        try {
            handle.close();
            parent.releaseChild();
        } catch (Throwable failure) {
            parent.retainChildCloseFailure(failure);
            if (primary == null) primary = failure;
            else if (failure != primary) primary.addSuppressed(failure);
        }
        if (primary != null) throw preserveIoFailure(context, primary);
    }
    private String sourceArtifactPath(SourceDirectoryPin exactParent,
            WindowsNormalRecallSourceStore.ArtifactKind kind, UUID write) throws IOException {
        String basename = WindowsNormalRecallSourceStore.sourceArtifactBasename(kind, write);
        return nativePathArgument(joinNativeChild(sourceDirectoryPath(exactParent), basename));
    }
    private String sourceDirectoryPath(SourceDirectoryPin exactParent) throws IOException {
        checkOwner();
        if (exactParent == null || !exactParent.issuedBy(this))
            throw new IOException("Exact typed source directory pin from this WindowsMarkerIo required");
        exactParent.requireLive();
        if ((exactParent.handle.access & 0x80) == 0
            || exactParent.handle.share != (SHARE_READ | SHARE_WRITE)
            || exactParent.handle.disposition != OPEN_EXISTING
            || (exactParent.handle.flags & (BACKUP | OPEN_REPARSE)) != (BACKUP | OPEN_REPARSE))
            throw new IOException("Source directory pin lacks the required no-delete native open flags");
        try (SourcePathWorkspace ignored = sourcePathWorkspace(exactParent.retainedPathAllocation,
                exactParent, "source parent exact native final-path verification")) {
            metadata(exactParent.handle, true);
            if (!exactParent.identity.equals(identity(exactParent.handle)))
                throw new IOException("Pinned source directory identity changed");
            String current = finalPath(exactParent.handle);
            if (!exactParent.exactNativePath.equals(current))
                throw new IOException("Pinned source parent exact native final path changed");
            return current;
        }
    }
    private String sourceRegionFilePath(SourceDirectoryPin exactParent, String fixedRegionBasename) throws IOException {
        if (fixedRegionBasename == null || !fixedRegionBasename.matches("r\\.(-?\\d+)\\.(-?\\d+)\\.mca"))
            throw new IllegalArgumentException("Exact existing Anvil region basename required");
        return nativePathArgument(joinNativeChild(sourceDirectoryPath(exactParent), fixedRegionBasename));
    }
    private static String joinNativeChild(String exactParentPath, String fixedBasename) {
        if (fixedBasename == null || !(fixedBasename.matches("\\.floatingpod-source-result-[0-9a-f]{32}-[0-9a-f]{32}\\.tmp")
            || fixedBasename.matches("\\.floatingpod-source-result-[0-9a-f]{32}\\.core")
            || fixedBasename.matches("\\.floatingpod-source-(payload|undo)-[0-9a-f]{32}\\.bin")
            || fixedBasename.matches("r\\.(-?\\d+)\\.(-?\\d+)\\.mca")))
            throw new IllegalArgumentException("Fixed source-result or Anvil region basename required");
        return exactParentPath.endsWith("\\") ? exactParentPath + fixedBasename : exactParentPath + "\\" + fixedBasename;
    }
    private Handle openNative(String nativePath, int access, int share, int disposition, int flags, UUID operation) throws IOException {
        String actualNativePath = nativePathArgument(nativePath);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment target = wide(arena, actualNativePath);
            NativePathDiagnostic diagnostic = nativePathDiagnostic(actualNativePath);
            Result result = call("CreateFileW", operation, target, access, share, MemorySegment.NULL,
                disposition, flags, MemorySegment.NULL);
            MemorySegment handle = (MemorySegment) result.value;
            if (handle.address() == -1L || handle.address() == 0L)
                throw new NativeFailure("CreateFileW", operation, result.error, diagnostic);
            Handle ownedHandle = new Handle(handle, operation, access, share, disposition, flags);
            owned.put(ownedHandle, Boolean.TRUE);
            return ownedHandle;
        }
    }
    static NativePathDiagnostic nativePathDiagnostic(String nativePath) {
        Objects.requireNonNull(nativePath);
        byte[] exactUtf16Le = nativePath.getBytes(StandardCharsets.UTF_16LE);
        if (exactUtf16Le.length != nativePath.length() * 2L)
            throw new IllegalStateException("Native target UTF-16 encoding length changed");
        try {
            String digest = HexFormat.of().withUpperCase().formatHex(
                MessageDigest.getInstance("SHA-256").digest(exactUtf16Le));
            return new NativePathDiagnostic(nativePath.length(), digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Required SHA-256 unavailable for native target diagnostics", impossible);
        }
    }
    Handle createMarkerHandle(Path path, UUID operation) throws IOException {
        return open(path, READ | WRITE | DELETE, SHARE_DELETE, CREATE_NEW, NORMAL | OPEN_REPARSE, operation);
    }
    /** Fixed Full20 witness discovery only; the private issuer supplies the controlled directory and budget. */
    Full20WitnessFindSequence beginFull20WitnessDiscovery(
            WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit permit,
            com.wi.floatingpod.storage.mysql.MySqlFull20ColdReadAccounting accounting) throws IOException {
        checkOwner(); Objects.requireNonNull(permit); Objects.requireNonNull(accounting);
        permit.requireFor(this, accounting);
        Path directory = permit.directoryFor(this, accounting);
        UUID operation = permit.operationFor(this, accounting);
        // A permit cannot choose the pattern; all filenames are kept under the single private v1 prefix.
        String pattern = directory.toAbsolutePath().normalize() + "\\.floatingpod-b1-full20-v1-*";
        permit.beginFindAttempt(this, accounting, pattern.length());
        Arena arena = null;
        FindHandle findHandle = null;
        Full20WitnessFindSequence sequence = null;
        try {
            arena = Arena.ofConfined();
            MemorySegment data = arena.allocate(WIN32_FIND_DATAW);
            Result result = call("FindFirstFileW", operation, wide(arena, nativePathArgument(pattern)) , data);
            MemorySegment nativeHandle = (MemorySegment) result.value;
            if (nativeHandle.address() == -1L || nativeHandle.address() == 0L) {
                if (result.error == ERROR_FILE_NOT_FOUND) {
                    sequence = new Full20WitnessFindSequence(operation, arena, data, null, false, permit, accounting);
                    permit.registerFindSequence(this, accounting, sequence);
                    return sequence;
                }
                throw new NativeFailure("FindFirstFileW", operation, result.error,
                    nativePathDiagnostic(pattern));
            }
            findHandle = new FindHandle(nativeHandle, operation);
            ownedFinds.put(findHandle, Boolean.TRUE); // immediately retain the raw handle after native return.
            sequence = new Full20WitnessFindSequence(operation, arena, data, findHandle, true, permit, accounting);
            permit.registerFindSequence(this, accounting, sequence);
            return sequence;
        } catch (Throwable primary) {
            if(findHandle==null) {
                try { if(arena!=null)arena.close(); } catch (Throwable cleanup) { if (cleanup != primary) primary.addSuppressed(cleanup); }
            }
            permit.failFindAttempt(this, accounting, primary);
            throw preserveIoFailure("FindFirstFileW", primary);
        }
    }
    static void closeSuppressed(FindHandle handle, Throwable primary) {
        if (handle == null || handle.closeAttempted) return;
        try { handle.close(); } catch (Throwable cleanup) { if (cleanup != primary) primary.addSuppressed(cleanup); }
    }
    void closeChecked(Handle handle) throws IOException { handle.close(); }
    Handle directory(Path path, UUID operation) throws IOException {
        Handle handle = open(path, 0x80, SHARE_READ | SHARE_WRITE, OPEN_EXISTING, BACKUP | OPEN_REPARSE, operation);
        try { metadata(handle, true); return handle; }
        catch (Throwable primary) { closeSuppressed(handle, primary); throw ioFailure("directory validation", primary); }
    }

    /** Budgeted native path pin primitive; child pins retain this exact directory handle. */
    private SourceDirectoryPin pinSourceDirectoryPath(Path exactDirectory, UUID operation,
            SourceLimitProfile profile, SourceForestAllocationBudget allocation) throws IOException {
        Objects.requireNonNull(exactDirectory, "exact source directory");
        requireSourceArtifactAllocation(profile, allocation);
        requireSourceRealm(profile, allocation);
        SourcePathWorkspace pathWorkspace = sourcePathWorkspace(allocation, null, "pin exact source parent directory");
        Handle handle = null;
        SourceDirectoryPin pin = null;
        long retainedCharge = 0L;
        boolean retainedChargeReserved = false;
        Throwable primary = null;
        try {
            handle = open(exactDirectory, 0x80, SHARE_READ | SHARE_WRITE, OPEN_EXISTING,
                BACKUP | OPEN_REPARSE, operation);
            metadata(handle, true);
            FileIdentity identity = identity(handle);
            if (Arrays.equals(identity.fileId128(), new byte[16]))
                throw new IOException("Source parent directory HANDLE returned an unobservable all-zero FILE_ID_INFO identity");
            String exactNativePath = finalPath(handle);
            retainedCharge = retainedNativePathCharge(exactNativePath);
            allocation.reserveTemporary(retainedCharge,
                "original source parent exact final-path/identity retained representation BEFORE escape");
            retainedChargeReserved = true;
            pathWorkspace.close();
            pin = new SourceDirectoryPin(handle, identity, exactNativePath, allocation, retainedCharge);
            return pin;
        } catch (Throwable failure) {
            primary = failure;
        }
        if (handle != null && !handle.closeAttempted) {
            try { handle.close(); }
            catch (Throwable cleanup) { if (cleanup != primary) primary.addSuppressed(cleanup); }
        }
        if (retainedChargeReserved && pin == null) {
            try { allocation.releaseTemporary(retainedCharge, "failed exact source parent pin retained path"); }
            catch (Throwable cleanup) { if (cleanup != primary) primary.addSuppressed(cleanup); }
        }
        if (!pathWorkspace.closeAttempted) {
            try { pathWorkspace.close(); }
            catch (Throwable cleanup) { if (cleanup != primary) primary.addSuppressed(cleanup); }
        }
        throw preserveIoFailure("pin exact source directory under original account", primary);
    }

    private static long retainedNativePathCharge(String exactNativePath) throws IOException {
        Objects.requireNonNull(exactNativePath, "exact native path");
        if (exactNativePath.isEmpty() || exactNativePath.length() > MAX_WIN32_PATH_CODE_UNITS)
            throw new IOException("Exact native path exceeds the retained Win32 path bound");
        try { return Math.addExact(256L, Math.multiplyExact((long) exactNativePath.length(), 2L)); }
        catch (ArithmeticException overflow) { throw new IOException("Exact native path retained-byte charge overflow", overflow); }
    }
    /** Budgeted sibling pin for original source capture; path workspace is reserved before either native file HANDLE opens. */
    public SourceRegionFilePin pinSourceRegionFile(SourceDirectoryPin exactParent,
            String fixedRegionBasename, UUID operation, SourceLimitProfile profile,
            SourceForestAllocationBudget allocation) throws IOException {
        Objects.requireNonNull(exactParent, "exact source parent pin");
        Objects.requireNonNull(fixedRegionBasename, "exact source region basename");
        Objects.requireNonNull(operation, "native source region operation UUID");
        requireSourceArtifactAllocation(profile, allocation);
        requireSourceRealm(profile, allocation);
        if (!exactParent.issuedBy(this)) throw new IOException("Source directory pin belongs to another WindowsMarkerIo instance");
        if (exactParent.retainedPathAllocation != allocation)
            throw new IOException("Source region pin must retain the exact parent pin's original allocation account");
        exactParent.reserveChild();
        SourcePathWorkspace pathWorkspace = null;
        Handle handle = null;
        SourceRegionFilePin pin = null;
        long retainedCharge = 0L;
        boolean retainedChargeReserved = false;
        Throwable primary = null;
        try {
            pathWorkspace = sourcePathWorkspace(allocation, exactParent, "pin exact source region child");
            String path = sourceRegionFilePath(exactParent, fixedRegionBasename);
            FileIdentity parentIdentity = exactParent.identity();
            handle = openNative(path, READ, SHARE_READ | SHARE_WRITE, OPEN_EXISTING,
                NORMAL | OPEN_REPARSE, operation);
            SourceFileObservation observation = observeSourceFile(handle);
            if (observation.byteLength() < 8192L)
                throw new IOException("Existing source region file is shorter than its 8192-byte header");
            if (parentIdentity.volumeSerialNumber() != observation.identity().volumeSerialNumber()
                || !path.equals(finalPath(handle)) || !path.equals(sourceRegionFilePath(exactParent, fixedRegionBasename)))
                throw new IOException("Source region HANDLE is not the exact same-volume child of its pinned parent");
            retainedCharge = retainedNativePathCharge(path);
            allocation.reserveTemporary(retainedCharge,
                "exact source region child final-path/identity retained representation BEFORE escape");
            retainedChargeReserved = true;
            pathWorkspace.close();
            pin = new SourceRegionFilePin(exactParent, fixedRegionBasename, path, parentIdentity,
                handle, observation.identity(), allocation, retainedCharge);
            return pin;
        } catch (Throwable failure) {
            primary = failure;
        }
        if (handle != null) {
            try {
                if (!handle.closeAttempted) handle.close();
                if (!owned.containsKey(handle)) exactParent.releaseChild();
                else exactParent.retainChildCloseFailure(new IOException("Source region HANDLE remains owned after open failure"));
            } catch (Throwable cleanup) {
                exactParent.retainChildCloseFailure(cleanup);
                if (cleanup != primary) primary.addSuppressed(cleanup);
            }
        } else exactParent.releaseChild();
        if (retainedChargeReserved && pin == null) {
            try { allocation.releaseTemporary(retainedCharge, "failed exact source region pin retained path"); }
            catch (Throwable cleanup) { exactParent.retainChildCloseFailure(cleanup); if (cleanup != primary) primary.addSuppressed(cleanup); }
        }
        if (pathWorkspace != null && !pathWorkspace.closeAttempted) {
            try { pathWorkspace.close(); }
            catch (Throwable cleanup) { if (cleanup != primary) primary.addSuppressed(cleanup); }
        }
        throw preserveIoFailure("pin exact source region under original account", primary);
    }
    private void releaseChildAfterFailedOpen(SourceDirectoryPin exactParent, Handle handle, Throwable primary) {
        if (handle == null) {
            exactParent.releaseChild();
            return;
        }
        try {
            if (!handle.closeAttempted) handle.close();
            if (!owned.containsKey(handle)) exactParent.releaseChild();
            else exactParent.retainChildCloseFailure(new IOException("Source child HANDLE remains owned after close"));
        } catch (Throwable cleanup) {
            if (cleanup != primary) primary.addSuppressed(cleanup);
            exactParent.retainChildCloseFailure(cleanup);
        }
    }
    private boolean releaseArtifactChildAfterFailedOpen(SourceDirectoryPin exactParent,
            Handle handle, Throwable primary) {
        try {
            if (handle == null) {
                exactParent.releaseChild();
                return true;
            }
            if (!handle.closeAttempted) handle.close();
            if (!owned.containsKey(handle)) {
                exactParent.releaseChild();
                return true;
            }
            IOException unresolved=new IOException("Source artifact child HANDLE remains owned after failed-open close");
            exactParent.retainChildCloseFailure(unresolved);
            if (unresolved != primary) primary.addSuppressed(unresolved);
            return false;
        } catch (Throwable cleanup) {
            exactParent.retainChildCloseFailure(cleanup);
            if (cleanup != primary) primary.addSuppressed(cleanup);
            return false;
        }
    }
    /**
     * Opens only the store's role/write-derived source basename with CREATE_NEW and share=0.
     * Capacity for the confined transfer arena is charged before opening the native file.
     */
    SourceArtifactCreatePin createSourceArtifact(SourceDirectoryPin exactParent,
            WindowsNormalRecallSourceStore.ArtifactKind kind, UUID write, UUID operation,
            SourceLimitProfile profile, SourceForestAllocationBudget allocation) throws IOException {
        Objects.requireNonNull(exactParent, "exact source parent pin");
        Objects.requireNonNull(kind, "source artifact role");
        Objects.requireNonNull(write, "source write UUID");
        Objects.requireNonNull(operation, "native source artifact operation UUID");
        requireSourceArtifactAllocation(profile, allocation);
        requireSourceRealm(profile, allocation);
        if (!exactParent.issuedBy(this)) throw new IOException("Source artifact parent pin belongs to another WindowsMarkerIo instance");
        if (exactParent.retainedPathAllocation != allocation)
            throw new IOException("Source artifact CREATE_NEW must use the exact parent pin's original allocation account");
        exactParent.reserveChild();
        SourceArtifactWorkspace workspace = null;
        SourcePathWorkspace pathWorkspace = null;
        Handle handle = null;
        java.security.MessageDigest digest = null;
        try {
            workspace = sourceArtifactWorkspace(exactParent, allocation, kind, write, operation,
                "source artifact CREATE_NEW");
            digest = sourceArtifactDigest();
            pathWorkspace = sourcePathWorkspace(allocation, exactParent, "source artifact CREATE_NEW exact path/open");
            String path = sourceArtifactPath(exactParent, kind, write);
            handle = openNative(path, READ | WRITE, 0, CREATE_NEW, NORMAL | OPEN_REPARSE, operation);
            metadata(handle, false, 0L);
            FileIdentity identity = identity(handle);
            if (!path.equals(finalPath(handle)) || !identity.equals(identity(handle))
                || !path.equals(sourceArtifactPath(exactParent, kind, write)))
                throw new IOException("Source artifact CREATE_NEW handle is not the exact fixed child identity");
            pathWorkspace.close();
            return new SourceArtifactCreatePin(exactParent, kind, write, handle, identity,
                workspace, profile.maxNativeArtifactBytes(), digest);
        } catch (Throwable primary) {
            boolean childCloseReturned=releaseArtifactChildAfterFailedOpen(exactParent, handle, primary);
            if (pathWorkspace != null && !pathWorkspace.closeAttempted) {
                try { pathWorkspace.close(); }
                catch (Throwable cleanup) { if (cleanup != primary) primary.addSuppressed(cleanup); }
            }
            if (workspace == null) workspace=exactParent.retainedArtifactWorkspace();
            if (workspace != null) {
                primary=workspace.closeUnpublished(exactParent,primary,
                    "source artifact CREATE_NEW failed-open workspace",childCloseReturned);
            } else {
                primary=exactParent.releaseArtifactWorkspaceWithoutOwner(primary,
                    "source artifact CREATE_NEW failed before workspace owner registration");
            }
            throw preserveIoFailure("create fixed source artifact", primary);
        }
    }

    /** Reopens only the fixed role/write basename, validates its original native identity, and compares full body to EOF. */
    SourceArtifactReadbackPin openExistingSourceArtifactReadback(SourceDirectoryPin exactParent,
            WindowsNormalRecallSourceStore.ArtifactKind kind, UUID write, UUID operation,
            SourceArtifactWriteObservation expected, SourceLimitProfile profile,
            SourceForestAllocationBudget allocation) throws IOException {
        Objects.requireNonNull(exactParent, "exact source parent pin");
        Objects.requireNonNull(kind, "source artifact role");
        Objects.requireNonNull(write, "source write UUID");
        Objects.requireNonNull(operation, "native source artifact operation UUID");
        Objects.requireNonNull(expected, "complete original CREATE_NEW body observation");
        requireSourceArtifactAllocation(profile, allocation);
        requireSourceRealm(profile, allocation);
        if (!exactParent.issuedBy(this)) throw new IOException("Source artifact parent pin belongs to another WindowsMarkerIo instance");
        if (exactParent.retainedPathAllocation != allocation)
            throw new IOException("Source artifact readback must use the exact parent pin's original allocation account");
        if (!exactParent.identity().equals(expected.parentDirectoryIdentity()))
            throw new IOException("Source artifact parent identity differs from the exact CREATE_NEW body observation");
        if (expected.byteLength() > profile.maxNativeArtifactBytes())
            throw new IOException("Source artifact readback length exceeds the active native artifact limit");
        exactParent.reserveChild();
        SourceArtifactWorkspace workspace = null;
        SourcePathWorkspace pathWorkspace = null;
        Handle handle = null;
        java.security.MessageDigest digest = null;
        try {
            workspace = sourceArtifactWorkspace(exactParent, allocation, kind, write, operation,
                "source artifact OPEN_EXISTING readback");
            digest = sourceArtifactDigest();
            pathWorkspace = sourcePathWorkspace(allocation, exactParent, "source artifact OPEN_EXISTING exact path/open");
            String path = sourceArtifactPath(exactParent, kind, write);
            handle = openNative(path, READ | WRITE, 0, OPEN_EXISTING, NORMAL | OPEN_REPARSE, operation);
            metadata(handle, false, expected.byteLength());
            SourceFileObservation opened = observeSourceFile(handle);
            if (!expected.fileIdentity().equals(opened.identity()) || opened.byteLength() != expected.byteLength()
                || expected.lastWriteFileTimeBits() != opened.lastWriteFileTimeBits()
                || opened.identity().volumeSerialNumber() != expected.parentDirectoryIdentity().volumeSerialNumber()
                || !path.equals(finalPath(handle)) || !path.equals(sourceArtifactPath(exactParent, kind, write)))
                throw new IOException("Reopened source artifact identity, length, raw FILETIME, or exact parent-child path differs");
            flushChecked(handle);
            pathWorkspace.close();
            return new SourceArtifactReadbackPin(exactParent, kind, write, handle, expected, opened,
                workspace, profile.maxNativeArtifactBytes(), digest);
        } catch (Throwable primary) {
            boolean childCloseReturned=releaseArtifactChildAfterFailedOpen(exactParent, handle, primary);
            if (pathWorkspace != null && !pathWorkspace.closeAttempted) {
                try { pathWorkspace.close(); }
                catch (Throwable cleanup) { if (cleanup != primary) primary.addSuppressed(cleanup); }
            }
            if (workspace == null) workspace=exactParent.retainedArtifactWorkspace();
            if (workspace != null) {
                primary=workspace.closeUnpublished(exactParent,primary,
                    "source artifact readback failed-open workspace",childCloseReturned);
            } else {
                primary=exactParent.releaseArtifactWorkspaceWithoutOwner(primary,
                    "source artifact readback failed before workspace owner registration");
            }
            throw preserveIoFailure("open fixed source artifact readback", primary);
        }
    }

    private SourceArtifactWorkspace sourceArtifactWorkspace(SourceDirectoryPin parent,
            SourceForestAllocationBudget allocation, WindowsNormalRecallSourceStore.ArtifactKind kind,
            UUID write, UUID operation, String context) throws IOException {
        parent.beginArtifactWorkspaceAttempt(kind,write,operation,allocation,context);
        SourceArtifactWorkspace workspace=null;
        try {
            workspace=new SourceArtifactWorkspace(parent,allocation,kind,write,operation,context);
            parent.registerArtifactWorkspace(workspace);
            workspace.initializeArena();
            return workspace;
        } catch (Throwable primary) {
            if (workspace==null || parent.retainedArtifactWorkspace()!=workspace)
                primary=parent.releaseArtifactWorkspaceWithoutOwner(primary,
                    context + " workspace owner construction/registration failed before Arena allocation");
            throw preserveIoFailure(context + " workspace", primary);
        }
    }

    private static void requireSourceArtifactAllocation(SourceLimitProfile profile,
            SourceForestAllocationBudget allocation) throws IOException {
        Objects.requireNonNull(profile, "exact source limit profile");
        Objects.requireNonNull(allocation, "original source allocation account");
        if (allocation.exactProfile() != profile) throw new IOException("Exact original source profile instance required for artifact publication");
    }

    private void requireSourceRealm(SourceLimitProfile profile,
            SourceForestAllocationBudget allocation) throws IOException {
        if (sourceIoHolder == null)
            throw new IOException("Normal Recall source operations require the retained budgeted SOURCE native holder");
        sourceIoHolder.requireAllocation(profile, allocation);
    }

    private static java.security.MessageDigest sourceArtifactDigest() throws IOException {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException failure) { throw new IOException("SHA-256 unavailable for source artifact", failure); }
    }

    public FileIdentity captureDirectoryIdentity(Path path, UUID operation) throws IOException {
        try (Handle handle = directory(path, operation)) { return identity(handle); }
    }
    void metadata(Handle handle, boolean directory) throws IOException { metadata(handle, directory, 24); }
    void metadata(Handle handle, boolean directory, long expectedLength) throws IOException {
        Result type = call("GetFileType", handle.operation, handle.value);
        if ((Integer) type.value == 0 && type.error != 0) throw new NativeFailure("GetFileType", handle.operation, type.error);
        if ((Integer) type.value != 1) throw new IOException("Marker handle is not FILE_TYPE_DISK operation=" + handle.operation);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment attributes = arena.allocate(ATTRIBUTE_TAG);
            bool("GetFileInformationByHandleEx", handle.operation, handle.value, 9, attributes, 8);
            int flags = attributes.get(ValueLayout.JAVA_INT, 0);
            if ((flags & 0x400) != 0 || ((flags & 0x10) != 0) != directory)
                throw new IOException("Marker handle has wrong directory/reparse type operation=" + handle.operation);
            MemorySegment standard = arena.allocate(STANDARD);
            bool("GetFileInformationByHandleEx", handle.operation, handle.value, 1, standard, 24);
            if (standard.get(ValueLayout.JAVA_BYTE, 20) != 0 || (standard.get(ValueLayout.JAVA_BYTE, 21) != 0) != directory
                || (!directory && standard.get(ValueLayout.JAVA_LONG, 8) != expectedLength))
                throw new IOException("Marker handle has wrong size/deletion/type operation=" + handle.operation);
        }
    }
    FileIdentity identity(Handle handle) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment info = arena.allocate(FILE_ID);
            bool("GetFileInformationByHandleEx", handle.operation, handle.value, 18, info, 24);
            return new FileIdentity(info.get(ValueLayout.JAVA_LONG, 0), info.asSlice(8, 16).toArray(ValueLayout.JAVA_BYTE));
        }
    }
    record RegionObservation(FileIdentity identity, long size, long lastWriteTime) { }
    SourceFileObservation observeHeldAdoptionArtifact(Handle handle, long explicitLimit) throws IOException {
        if (explicitLimit <= 0L) throw new IOException("Positive FirstBinding6 artifact limit required");
        SourceFileObservation observation = observeSourceFile(handle);
        if (observation.byteLength() <= 0L || observation.byteLength() > explicitLimit
            || observation.byteLength() > Integer.MAX_VALUE)
            throw new IOException("FirstBinding6 artifact exceeds its exact native byte limit");
        metadata(handle, false, observation.byteLength());
        return observation;
    }
    private SourceFileObservation observeSourceFile(Handle handle) throws IOException {
        checkOwner();
        if (handle == null || !owned.containsKey(handle) || handle.closeAttempted)
            throw new IOException("Exact live source file HANDLE required");
        Result type = call("GetFileType", handle.operation, handle.value);
        if ((Integer) type.value == 0 && type.error != 0)
            throw new NativeFailure("GetFileType", handle.operation, type.error);
        if ((Integer) type.value != 1) throw new IOException("Source file HANDLE is not FILE_TYPE_DISK");
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment attributes = arena.allocate(ATTRIBUTE_TAG);
            bool("GetFileInformationByHandleEx", handle.operation, handle.value, 9, attributes, 8);
            int flags = attributes.get(ValueLayout.JAVA_INT, 0);
            if ((flags & 0x400) != 0 || (flags & 0x10) != 0)
                throw new IOException("Source file HANDLE is a reparse point or directory");
            MemorySegment standard = arena.allocate(STANDARD);
            bool("GetFileInformationByHandleEx", handle.operation, handle.value, 1, standard, 24);
            long size = standard.get(ValueLayout.JAVA_LONG, 8);
            if (size < 0L || standard.get(ValueLayout.JAVA_BYTE, 20) != 0
                || standard.get(ValueLayout.JAVA_BYTE, 21) != 0)
                throw new IOException("Source file HANDLE is negative-sized, delete-pending, or a directory");
            MemorySegment basic = arena.allocate(BASIC);
            bool("GetFileInformationByHandleEx", handle.operation, handle.value, 0, basic, 40);
            FileIdentity fileIdentity = identity(handle);
            if (Arrays.equals(fileIdentity.fileId128(), new byte[16]))
                throw new IOException("Source file HANDLE returned an unobservable all-zero FILE_ID_INFO identity");
            return new SourceFileObservation(fileIdentity, size, basic.get(ValueLayout.JAVA_LONG, 16));
        }
    }
    RegionObservation observeRegion(Handle handle) throws IOException {
        requireRegionHandle(handle);
        Result type = call("GetFileType", handle.operation, handle.value);
        if ((Integer) type.value == 0 && type.error != 0) throw new NativeFailure("GetFileType", handle.operation, type.error);
        if ((Integer) type.value != 1) throw new IOException("Region handle is not FILE_TYPE_DISK operation=" + handle.operation);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment attributes = arena.allocate(ATTRIBUTE_TAG);
            bool("GetFileInformationByHandleEx", handle.operation, handle.value, 9, attributes, 8);
            int flags = attributes.get(ValueLayout.JAVA_INT, 0);
            if ((flags & 0x400) != 0 || (flags & 0x10) != 0)
                throw new IOException("Region handle is a reparse point or directory operation=" + handle.operation);
            MemorySegment standard = arena.allocate(STANDARD);
            bool("GetFileInformationByHandleEx", handle.operation, handle.value, 1, standard, 24);
            long size = standard.get(ValueLayout.JAVA_LONG, 8);
            if (size < 0L || standard.get(ValueLayout.JAVA_BYTE, 20) != 0 || standard.get(ValueLayout.JAVA_BYTE, 21) != 0)
                throw new IOException("Region handle is negative-sized, delete-pending, or a directory operation=" + handle.operation);
            MemorySegment basic = arena.allocate(BASIC);
            bool("GetFileInformationByHandleEx", handle.operation, handle.value, 0, basic, 40);
            FileIdentity fileIdentity = identity(handle);
            if (Arrays.equals(fileIdentity.fileId128(), new byte[16]))
                throw new IOException("Region handle returned an unobservable all-zero FILE_ID_INFO identity operation=" + handle.operation);
            return new RegionObservation(fileIdentity, size, basic.get(ValueLayout.JAVA_LONG, 16));
        }
    }
    byte[] readRegionRangeExact(Handle handle, long offset, int length) throws IOException {
        requireRegionHandle(handle);
        if (offset < 0L || length <= 0 || Math.addExact(offset, (long) length) < offset)
            throw new IllegalArgumentException("Region range must be positive and non-overflowing");
        byte[] result = new byte[length];
        bool("SetFilePointerEx", handle.operation, handle.value, offset, MemorySegment.NULL, 0);
        try (Arena arena = Arena.ofConfined()) {
            int bufferSize = Math.min(length, 64 * 1024);
            MemorySegment buffer = arena.allocate(bufferSize);
            MemorySegment count = arena.allocate(ValueLayout.JAVA_INT);
            for (int written = 0; written < length;) {
                int request = Math.min(bufferSize, length - written);
                bool("ReadFile", handle.operation, handle.value, buffer, request, count, MemorySegment.NULL);
                int read = count.get(ValueLayout.JAVA_INT, 0);
                progress(read, request);
                System.arraycopy(buffer.asSlice(0L, read).toArray(ValueLayout.JAVA_BYTE), 0, result, written, read);
                written += read;
            }
        }
        return result;
    }
    private void requireRegionHandle(Handle handle) {
        checkOwner();
        if (regionSession == null || handle == null || !owned.containsKey(handle))
            throw new IllegalStateException("Exact live region handle owned by the registered Windows IO is required");
        regionSession.requireRegionHandle(handle);
    }
    boolean ownsRegionHandle(Handle handle) {
        checkOwner();
        if (regionSession == null || handle == null) throw new IllegalStateException("Region HANDLE ownership query requires its exact session");
        regionSession.requireRegionHandle(handle);
        return owned.containsKey(handle);
    }
    int ownedHandleCount() {
        checkOwner();
        return owned.size();
    }
    String finalPath(Handle handle) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buffer = arena.allocate(32768L * 2, 2);
            Result result = call("GetFinalPathNameByHandleW", handle.operation, handle.value, buffer, 32768, 1);
            int count = (Integer) result.value;
            if (count == 0) throw new NativeFailure("GetFinalPathNameByHandleW", handle.operation, result.error);
            if (count < 0 || count >= 32768) throw new IOException("Native final path exceeds supported bound");
            StringBuilder value = new StringBuilder(count);
            for (int i = 0; i < count; i++) value.append(buffer.get(ValueLayout.JAVA_CHAR, i * 2L));
            return value.toString();
        }
    }
    static int progress(int actual, int remaining) throws IOException {
        if (actual <= 0 || actual > remaining) throw new IOException("Invalid synchronous marker IO progress");
        return actual;
    }
    void writeAll(Handle handle, byte[] bytes) throws IOException {
        if (bytes.length != 24) throw new IllegalArgumentException("Marker is exactly 24 bytes");
        writePayload(handle, bytes);
    }
    static void payloadBound(int length) {
        if (length <= 0 || length > 65536) throw new IllegalArgumentException("Native receipt payload bound is 1..65536 bytes");
    }
    void writePayload(Handle handle, byte[] bytes) throws IOException {
        payloadBound(bytes.length);
        bool("SetFilePointerEx", handle.operation, handle.value, 0L, MemorySegment.NULL, 0);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buffer = arena.allocateFrom(ValueLayout.JAVA_BYTE, bytes);
            MemorySegment count = arena.allocate(ValueLayout.JAVA_INT);
            for (int offset = 0; offset < bytes.length;) {
                bool("WriteFile", handle.operation, handle.value, buffer.asSlice(offset), bytes.length - offset, count, MemorySegment.NULL);
                offset += progress(count.get(ValueLayout.JAVA_INT, 0), bytes.length - offset);
            }
        }
    }
    byte[] readBackExact(Handle handle) throws IOException { return readPayloadExact(handle, 24); }
    byte[] readPayloadExact(Handle handle, int length) throws IOException {
        payloadBound(length);
        bool("SetFilePointerEx", handle.operation, handle.value, 0L, MemorySegment.NULL, 0);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buffer = arena.allocate(length + 1L), count = arena.allocate(ValueLayout.JAVA_INT);
            for (int offset = 0; offset < length;) {
                bool("ReadFile", handle.operation, handle.value, buffer.asSlice(offset), length - offset, count, MemorySegment.NULL);
                offset += progress(count.get(ValueLayout.JAVA_INT, 0), length - offset);
            }
            bool("ReadFile", handle.operation, handle.value, buffer.asSlice(length), 1, count, MemorySegment.NULL);
            if (count.get(ValueLayout.JAVA_INT, 0) != 0) throw new IOException("Marker has trailing bytes");
            return buffer.asSlice(0, length).toArray(ValueLayout.JAVA_BYTE);
        }
    }
    /** 专用 witness artifact；调用者须先在原 actor root 预留全部传输及 native scratch。 */
    static void witnessArtifactBound(int length, long explicitArtifactLimit) {
        if(explicitArtifactLimit<=0 || length<=0 || length>explicitArtifactLimit || length>Integer.MAX_VALUE-8)
            throw new IllegalArgumentException("Witness artifact exceeds its original explicit bound");
    }
    private Arena retainedWitnessArena;
    private Throwable retainedWitnessArenaFailure;
    private Arena beginWitnessArena() throws IOException {
        checkOwner();
        if(retainedWitnessArena!=null)throw new IOException("Prior witness native workspace remains unresolved",retainedWitnessArenaFailure);
        retainedWitnessArena=Arena.ofConfined();return retainedWitnessArena;
    }
    private void finishWitnessArena(Arena exact,Throwable primary)throws IOException {
        if(exact!=retainedWitnessArena)throw new IllegalStateException("Exact witness workspace required");
        try {exact.close();retainedWitnessArena=null;}
        catch(Throwable cleanup) {
            retainedWitnessArenaFailure=cleanup;
            if(primary!=null) {if(cleanup!=primary)primary.addSuppressed(cleanup);}
            else throw ioFailure("witness native workspace close",cleanup);
        }
    }
    Throwable retainedWitnessWorkspaceFailure(){checkOwner();return retainedWitnessArenaFailure;}
    void writeWitnessArtifact(Handle handle, byte[] bytes, long explicitArtifactLimit) throws IOException {
        checkOwner();Objects.requireNonNull(bytes);witnessArtifactBound(bytes.length,explicitArtifactLimit);
        Arena arena=beginWitnessArena();Throwable primary=null;
        try {
            bool("SetFilePointerEx",handle.operation,handle.value,0L,MemorySegment.NULL,0);
            MemorySegment buffer=arena.allocateFrom(ValueLayout.JAVA_BYTE,bytes);
            MemorySegment count=arena.allocate(ValueLayout.JAVA_INT);
            for(int offset=0;offset<bytes.length;) {
                bool("WriteFile",handle.operation,handle.value,buffer.asSlice(offset),bytes.length-offset,count,MemorySegment.NULL);
                offset+=progress(count.get(ValueLayout.JAVA_INT,0),bytes.length-offset);
            }
        } catch(IOException|RuntimeException|Error failure) {primary=failure;throw failure;}
        finally {finishWitnessArena(arena,primary);}
    }
    byte[] readWitnessArtifactExact(Handle handle, int length, long explicitArtifactLimit) throws IOException {
        checkOwner();witnessArtifactBound(length,explicitArtifactLimit);
        Arena arena=beginWitnessArena();Throwable primary=null;
        try {
            bool("SetFilePointerEx",handle.operation,handle.value,0L,MemorySegment.NULL,0);
            MemorySegment buffer=arena.allocate(length+1L),count=arena.allocate(ValueLayout.JAVA_INT);
            for(int offset=0;offset<length;) {
                bool("ReadFile",handle.operation,handle.value,buffer.asSlice(offset),length-offset,count,MemorySegment.NULL);
                offset+=progress(count.get(ValueLayout.JAVA_INT,0),length-offset);
            }
            bool("ReadFile",handle.operation,handle.value,buffer.asSlice(length),1,count,MemorySegment.NULL);
            if(count.get(ValueLayout.JAVA_INT,0)!=0)throw new IOException("Witness artifact has trailing bytes");
            return buffer.asSlice(0,length).toArray(ValueLayout.JAVA_BYTE);
        } catch(IOException|RuntimeException|Error failure) {primary=failure;throw failure;}
        finally {finishWitnessArena(arena,primary);}
    }

    /** 同一真实READ handle完整流比较；不flush、不发行durability，不复制known payload。 */
    void compareWitnessArtifactExact(Handle handle, byte[] known, FileIdentity expectedIdentity,
                                     long explicitArtifactLimit) throws IOException {
        checkOwner(); Objects.requireNonNull(known); Objects.requireNonNull(expectedIdentity);
        witnessArtifactBound(known.length, explicitArtifactLimit);
        metadata(handle, false, known.length);
        if (!expectedIdentity.equals(identity(handle))) throw new IOException("Inventory native identity differs before complete read");
        Arena arena = beginWitnessArena(); Throwable primary = null;
        try {
            MemorySegment buffer = arena.allocate(65537L), count = arena.allocate(ValueLayout.JAVA_INT);
            bool("SetFilePointerEx", handle.operation, handle.value, 0L, MemorySegment.NULL, 0);
            for (int offset = 0; offset < known.length;) {
                int requested = Math.min(65536, known.length - offset);
                bool("ReadFile", handle.operation, handle.value, buffer, requested, count, MemorySegment.NULL);
                int actual = progress(count.get(ValueLayout.JAVA_INT, 0), requested);
                for (int index = 0; index < actual; index++)
                    if (buffer.get(ValueLayout.JAVA_BYTE, index) != known[offset + index])
                        throw new IOException("Inventory complete native byte differs");
                offset = Math.addExact(offset, actual);
            }
            bool("ReadFile", handle.operation, handle.value, buffer, 1, count, MemorySegment.NULL);
            if (count.get(ValueLayout.JAVA_INT, 0) != 0) throw new IOException("Inventory native payload has trailing bytes");
            metadata(handle, false, known.length);
            if (!expectedIdentity.equals(identity(handle))) throw new IOException("Inventory native identity differs after complete read");
        } catch (IOException | RuntimeException | Error failure) { primary = failure; throw failure; }
        finally { finishWitnessArena(arena, primary); }
    }
    void flushChecked(Handle handle) throws IOException { bool("FlushFileBuffers", handle.operation, handle.value); }
    /**
     * Adoption6 专用：将仍被独占持有的旧 marker 改名为同一父目录中的固定历史槽。
     * 调用方先持久保存 source/cutover intent，并预留 393216 字节 native/path 工作区。
     * 这是 handle-bound no-replace rename，不冒充 MoveFileEx WRITE_THROUGH 或掉电验收。
     * 失败后保留 handle 和既有文件；上层必须重新核对固定槽，不能盲目重放。
     */
    void renameHeldMarkerNoReplace(Handle old, Handle parent, String preservedBasename,
            FileIdentity expectedOld, FileIdentity expectedParent) throws IOException {
        checkOwner();
        Objects.requireNonNull(old); Objects.requireNonNull(parent);
        Objects.requireNonNull(expectedOld); Objects.requireNonNull(expectedParent);
        String fixed = ".floatingpod-adoption6-" + old.operation + ".old-binding.bin";
        if (!fixed.equals(preservedBasename) || !owned.containsKey(old) || !owned.containsKey(parent)
            || old.closeAttempted || parent.closeAttempted || (old.access & DELETE) == 0
            || old.share != 0 || (parent.share & SHARE_DELETE) != 0)
            throw new IOException("Adoption6 requires the exact exclusive old marker and pinned parent handles");
        metadata(old, false); metadata(parent, true);
        if (!expectedOld.equals(identity(old)) || !expectedParent.equals(identity(parent))
            || expectedOld.volumeSerialNumber() != expectedParent.volumeSerialNumber())
            throw new IOException("Adoption6 source or parent identity changed before preservation");
        String parentPath = finalPath(parent);
        String oldPath = finalPath(old);
        WindowsCanonicalMarkerPublicationBarrier.requireSameNativeParent(parentPath, oldPath);
        String prefix = parentPath.endsWith("\\") ? parentPath : parentPath + "\\";
        if (!oldPath.equals(prefix + "floatingpod-world-binding.bin"))
            throw new IOException("Adoption6 only preserves the exact canonical FP marker");
        byte[] before = readBackExact(old);
        String target = prefix + fixed;
        // FILE_RENAME_INFO x64: union@0, HANDLE@8, DWORD length@16, WCHAR name@20.
        // RootDirectory=NULL plus an absolute name is the documented Win32 form.
        // ReplaceIfExists remains FALSE; no POSIX/replacement/ignore-readonly flags.
        try (Arena arena = Arena.ofConfined()) {
            int nameBytes = Math.multiplyExact(target.length(), Character.BYTES);
            if (target.length() > MAX_WIN32_PATH_CODE_UNITS)
                throw new IOException("Adoption6 preserved marker path exceeds native bound");
            MemorySegment rename = arena.allocate(Math.addExact(22L, nameBytes), 8);
            rename.fill((byte) 0);
            rename.set(ValueLayout.ADDRESS, 8, MemorySegment.NULL);
            rename.set(ValueLayout.JAVA_INT, 16, nameBytes);
            for (int index = 0; index < target.length(); index++)
                rename.set(ValueLayout.JAVA_CHAR, 20L + index * 2L, target.charAt(index));
            bool("SetFileInformationByHandle", old.operation, old.value, 3, rename,
                Math.toIntExact(rename.byteSize()));
        }
        flushChecked(old);
        metadata(old, false); metadata(parent, true);
        WindowsCanonicalMarkerPublicationBarrier.requireExact(before, readBackExact(old));
        if (!expectedOld.equals(identity(old)) || !expectedParent.equals(identity(parent))
            || !parentPath.equals(finalPath(parent)) || !target.equals(finalPath(old)))
            throw new IOException("Adoption6 preserved marker identity/path readback differs; reconciliation required");
    }
    void moveWriteThroughNoReplace(Path temporary, Path canonical, UUID operation) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            bool("MoveFileExW", operation, wide(arena, temporary), wide(arena, canonical), WRITE_THROUGH);
        }
    }
    void deleteOwnedTemporary(Handle handle) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment disposition = arena.allocate(ValueLayout.JAVA_BYTE);
            disposition.set(ValueLayout.JAVA_BYTE, 0, (byte) 1);
            bool("SetFileInformationByHandle", handle.operation, handle.value, 4, disposition, 1);
        }
    }
    /** 仅供固定 inactive canonical store 的同 handle 全身份/bytes/链核对后使用。 */
    void deleteInactiveCanonicalChecked(Handle handle) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment disposition = arena.allocate(ValueLayout.JAVA_BYTE);
            disposition.set(ValueLayout.JAVA_BYTE, 0, (byte) 1);
            bool("SetFileInformationByHandle", handle.operation, handle.value, 4, disposition, 1);
        }
    }
    static void closeSuppressed(Handle handle, Throwable primary) {
        if (handle == null || handle.closeAttempted) return;
        try { handle.close(); } catch (Throwable cleanup) { primary.addSuppressed(cleanup); }
    }
    void createDirectoryNew(Path path, UUID operation) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            // CreateDirectoryW 只创建 final component；已有目录的 ERROR_ALREADY_EXISTS 必须传播。
            bool("CreateDirectoryW", operation, wideDirectory(arena, path), MemorySegment.NULL);
        }
    }
    long payloadLength(Handle handle) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment standard = arena.allocate(STANDARD);
            bool("GetFileInformationByHandleEx", handle.operation, handle.value, 1, standard, 24);
            if (standard.get(ValueLayout.JAVA_BYTE, 20) != 0 || standard.get(ValueLayout.JAVA_BYTE, 21) != 0)
                throw new IOException("Receipt/custody payload is deleted or directory");
            long length = standard.get(ValueLayout.JAVA_LONG, 8);
            if (length < 0 || length > 65536) throw new IOException("Receipt/custody payload exceeds bound");
            metadata(handle, false, length);
            return length;
        }
    }
    public static final class ProcessIdentity {
        private final long pid, creationFileTime;
        private final Instant startInstant;
        private ProcessIdentity(long pid, long creationFileTime) {
            validateProcessIdentity(pid, creationFileTime);
            this.pid = pid; this.creationFileTime = creationFileTime; this.startInstant = fileTimeInstant(creationFileTime);
        }
        public long pid() { return pid; }
        public long creationFileTime() { return creationFileTime; }
        public Instant startInstant() { return startInstant; }
    }
    static void validateProcessIdentity(long pid, long creationFileTime) {
        if (pid <= 0 || pid > 0xffffffffL || creationFileTime <= 0)
            throw new IllegalArgumentException("Exact Windows PID/FILETIME required");
        fileTimeInstant(creationFileTime);
    }
    static Instant fileTimeInstant(long ticks) {
        if (ticks <= 0) throw new IllegalArgumentException("Invalid process creation FILETIME");
        return Instant.ofEpochSecond(ticks / 10_000_000L - 11_644_473_600L, (ticks % 10_000_000L) * 100L);
    }
    private long processCreationTime(MemorySegment handle, UUID operation) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment creation = arena.allocate(ValueLayout.JAVA_LONG), exit = arena.allocate(ValueLayout.JAVA_LONG),
                kernel = arena.allocate(ValueLayout.JAVA_LONG), user = arena.allocate(ValueLayout.JAVA_LONG);
            bool("GetProcessTimes", operation, handle, creation, exit, kernel, user);
            long value = creation.get(ValueLayout.JAVA_LONG, 0);
            if (value <= 0) throw new IOException("Missing exact native process creation time");
            return value;
        }
    }
    public ProcessIdentity captureCurrentProcessIdentity(UUID operation) throws IOException {
        long pid = Integer.toUnsignedLong((Integer) call("GetCurrentProcessId", operation).value);
        MemorySegment pseudo = (MemorySegment) call("GetCurrentProcess", operation).value;
        // GetCurrentProcess 返回 borrowed pseudo-handle；不能放入 owned map 或 CloseHandle。
        return new ProcessIdentity(pid, processCreationTime(pseudo, operation));
    }
    /** 在读取世界 guard/receipt 前登记真实 live helper 的 OS 候选，并保留初始 WAIT_TIMEOUT 的同一 HANDLE。 */
    public RetainedProcessObservation observeLiveProcessCandidate(long candidatePid, UUID operation) throws IOException {
        checkOwner(); Objects.requireNonNull(operation);
        if (candidatePid <= 0 || candidatePid > 0xffffffffL) throw new IllegalArgumentException("Exact Windows PID required");
        long currentPid = Integer.toUnsignedLong((Integer) call("GetCurrentProcessId", operation).value);
        if (currentPid == candidatePid)
            throw new IOException("Same-process previous physical writer requires opaque actual IO-exit evidence; unavailable");
        Result result = call("OpenProcess", operation, 0x00100000 | 0x1000, 0, (int) candidatePid);
        MemorySegment process = (MemorySegment) result.value;
        if (process.address() == 0) throw new NativeFailure("OpenProcess", operation, result.error);

        // OpenProcess 返回后先登记 cleanup obligation，再调用任何依赖该 HANDLE 的 API。
        Handle retained = new Handle(process, operation);
        owned.put(retained, Boolean.TRUE);
        try {
            long creation = processCreationTime(process, operation);
            if (!processSignaled(retained, operation))
                return new RetainedProcessObservation(this, retained, candidatePid, creation, operation);
            throw new IOException("Process was not observed live at the initial native wait; no candidate issued");
        } catch (Throwable primary) {
            closeSuppressed(retained, primary);
            throw preserveIoFailure("observe live process candidate operation=" + operation, primary);
        }
    }

    void requireRetainedCandidate(RetainedProcessObservation observation, UUID expectedOperation) throws IOException {
        checkOwner();
        if (observation == null || observation.issuer != this || observation.owner != owner)
            throw new IOException("Exact retained process observation from this WindowsMarkerIo required");
        if (!observation.operation.equals(expectedOperation))
            throw new IOException("Retained process observation operation mismatch");
        if (observation.state != ProcessObservationState.LIVE || observation.handle.closeAttempted)
            throw new IOException("Retained process observation is not live and unconsumed");
    }

    private boolean processSignaled(Handle handle, UUID operation) throws IOException {
        Result wait = call("WaitForSingleObject", operation, handle.value, 0);
        int value = (Integer) wait.value;
        if (value == 0) return true;
        if (value == 258) return false;
        if (value == -1) throw new NativeFailure("WaitForSingleObject", operation, wait.error);
        throw new IOException("Unexpected process wait result; no exit proof");
    }

    private int processWaitResult(Handle handle, UUID operation) throws IOException {
        Result wait = call("WaitForSingleObject", operation, handle.value, 0);
        int value = (Integer) wait.value;
        if (value == 0 || value == 258) return value;
        if (value == -1) throw new NativeFailure("WaitForSingleObject", operation, wait.error);
        throw new IOException("Unexpected process wait result; no lifetime evidence");
    }

    /**
     * 返回低层 OS 观察，不单独授予世界恢复资格。调用者须以仍持有的原 guard、source 和 receipt
     * 在观察前后核对 durable owner，然后才能 claim 该不可伪造实例。
     */
    PreviousProcessLifetimeObservation observePreviousProcessLifetime(
            long previousPid, long previousCreationFileTime, UUID operation) throws IOException {
        checkOwner();
        Objects.requireNonNull(operation);
        validateProcessIdentity(previousPid, previousCreationFileTime);
        Result opened = call("OpenProcess", operation, 0x00100000 | 0x1000, 0, (int) previousPid);
        MemorySegment process = (MemorySegment) opened.value;
        if (process.address() == 0 || process.address() == -1L) {
            // 只有 PID 无效参数才尝试完整进程快照；ACCESS_DENIED 与其它错误不能推断进程缺席。
            if (opened.error == ERROR_INVALID_PARAMETER)
                return requireCompleteProcessSnapshotAbsence(previousPid, previousCreationFileTime, operation);
            throw new NativeFailure("OpenProcess", operation, opened.error);
        }

        Handle retained = new Handle(process, operation);
        owned.put(retained, Boolean.TRUE);
        PreviousProcessLifetimeObservation evidence = null;
        Throwable primary = null;
        try {
            long actualCreation = processCreationTime(process, operation);
            if (actualCreation < previousCreationFileTime)
                throw new IOException("Observed process creation FILETIME predates durable previous owner; no lifetime evidence");
            int wait = processWaitResult(retained, operation);
            if (actualCreation == previousCreationFileTime) {
                if (wait != 0) throw new IOException("Previous owner process is still live; no restart evidence");
                evidence = new ExactHandleProcessExit(this, retained, previousPid, actualCreation, operation);
            } else {
                // 上面的严格小于拒绝保证此分支只表示已核验 HANDLE 的创建时间晚于耐久 owner。
                evidence = new ProcessIdReused(this, previousPid, previousCreationFileTime, actualCreation, wait, operation);
            }
        } catch (Throwable failure) {
            primary = failure;
        }
        if (primary == null) {
            try {
                retained.close();
            } catch (Throwable closeFailure) {
                primary = closeFailure;
            }
        } else {
            closeSuppressed(retained, primary);
        }
        if (primary != null) throw preserveIoFailure("observe prior process lifetime operation=" + operation, primary);
        if (evidence == null) throw new IOException("Native prior-process observation produced no typed evidence");
        return evidence;
    }

    private PreviousProcessLifetimeObservation requireCompleteProcessSnapshotAbsence(
            long previousPid, long previousCreationFileTime, UUID operation) throws IOException {
        requireProcessEntry32WLayout();
        Result created = call("CreateToolhelp32Snapshot", operation, TH32CS_SNAPPROCESS, 0);
        MemorySegment snapshotValue = (MemorySegment) created.value;
        if (snapshotValue.address() == 0 || snapshotValue.address() == -1L)
            throw new NativeFailure("CreateToolhelp32Snapshot", operation, created.error);
        Handle snapshot = new Handle(snapshotValue, operation);
        owned.put(snapshot, Boolean.TRUE);

        long count = 0;
        int terminal = 0;
        boolean found = false;
        Throwable primary = null;
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment entry = arena.allocate(PROCESSENTRY32W);
            entry.set(ValueLayout.JAVA_INT, PROCESSENTRY32W_DW_SIZE_OFFSET, Math.toIntExact(PROCESSENTRY32W_SIZE));
            Result first = call("Process32FirstW", operation, snapshot.value, entry);
            if ((Integer) first.value == 0) {
                if (first.error != ERROR_NO_MORE_FILES) throw new NativeFailure("Process32FirstW", operation, first.error);
                terminal = first.error;
            } else {
                while (true) {
                    requireProcessEntry32WSize(entry);
                    long processId = Integer.toUnsignedLong(entry.get(ValueLayout.JAVA_INT, PROCESSENTRY32W_PID_OFFSET));
                    if (processId == previousPid) found = true;
                    try {
                        count = Math.addExact(count, 1L);
                    } catch (ArithmeticException overflow) {
                        throw new IOException("Toolhelp process enumeration count overflow", overflow);
                    }
                    entry.set(ValueLayout.JAVA_INT, PROCESSENTRY32W_DW_SIZE_OFFSET, Math.toIntExact(PROCESSENTRY32W_SIZE));
                    Result next = call("Process32NextW", operation, snapshot.value, entry);
                    if ((Integer) next.value == 0) {
                        if (next.error != ERROR_NO_MORE_FILES) throw new NativeFailure("Process32NextW", operation, next.error);
                        terminal = next.error;
                        break;
                    }
                }
            }
            if (terminal != ERROR_NO_MORE_FILES)
                throw new IOException("Toolhelp process enumeration did not reach ERROR_NO_MORE_FILES");
            if (count == 0)
                throw new IOException("Toolhelp process snapshot contained no entries; completeness is unverified");
            // First/Next 均以 ERROR_NO_MORE_FILES 终止，且完整系统快照必须包含当前调用者。
            if (found) throw new IOException("Previous owner PID remains in the complete process snapshot; no lifetime proof");
        } catch (Throwable failure) {
            primary = failure;
        }
        if (primary == null) {
            try {
                snapshot.close();
            } catch (Throwable closeFailure) {
                primary = closeFailure;
            }
        } else {
            closeSuppressed(snapshot, primary);
        }
        if (primary != null) throw preserveIoFailure("complete Toolhelp process snapshot operation=" + operation, primary);
        return new OldLifetimeEndedByCompleteSnapshot(this, previousPid, previousCreationFileTime, count, terminal, operation);
    }

    private static void requireProcessEntry32WLayout() throws IOException {
        if (PROCESSENTRY32W.byteSize() != PROCESSENTRY32W_SIZE
            || PROCESSENTRY32W_DW_SIZE_OFFSET != 0
            || PROCESSENTRY32W.byteOffset(MemoryLayout.PathElement.groupElement("cntUsage")) != 4
            || PROCESSENTRY32W_PID_OFFSET != 8
            || PROCESSENTRY32W.byteOffset(MemoryLayout.PathElement.groupElement("th32DefaultHeapID")) != 16
            || PROCESSENTRY32W.byteOffset(MemoryLayout.PathElement.groupElement("th32ModuleID")) != 24
            || PROCESSENTRY32W.byteOffset(MemoryLayout.PathElement.groupElement("cntThreads")) != 28
            || PROCESSENTRY32W.byteOffset(MemoryLayout.PathElement.groupElement("th32ParentProcessID")) != 32
            || PROCESSENTRY32W.byteOffset(MemoryLayout.PathElement.groupElement("pcPriClassBase")) != 36
            || PROCESSENTRY32W.byteOffset(MemoryLayout.PathElement.groupElement("dwFlags")) != 40
            || PROCESSENTRY32W_IMAGE_OFFSET != 44)
            throw new IOException("PROCESSENTRY32W does not match the reviewed Windows x64 ABI");
    }

    private static void requireProcessEntry32WSize(MemorySegment entry) throws IOException {
        if (entry.get(ValueLayout.JAVA_INT, PROCESSENTRY32W_DW_SIZE_OFFSET) != PROCESSENTRY32W_SIZE)
            throw new IOException("Toolhelp returned a PROCESSENTRY32W with an unexpected size");
    }

    void claimPreviousProcessLifetimeObservation(PreviousProcessLifetimeObservation observation,
            long expectedPid, long expectedCreationFileTime, UUID expectedOperation) throws IOException {
        checkOwner();
        validateProcessIdentity(expectedPid, expectedCreationFileTime);
        if (observation == null || observation.issuer != this || observation.claimed
            || observation.previousPid != expectedPid
            || observation.previousCreationFileTime != expectedCreationFileTime
            || !observation.operation.equals(expectedOperation))
            throw new IOException("Exact unclaimed prior-process lifetime observation required");
        observation.claimed = true;
    }

    void requireClaimedPreviousProcessLifetimeObservation(PreviousProcessLifetimeObservation observation,
            long expectedPid, long expectedCreationFileTime, UUID expectedOperation) throws IOException {
        checkOwner();
        if (observation == null || observation.issuer != this || !observation.claimed
            || observation.previousPid != expectedPid
            || observation.previousCreationFileTime != expectedCreationFileTime
            || !observation.operation.equals(expectedOperation))
            throw new IOException("Claimed prior-process lifetime observation is not exact");
    }

    private boolean pollRetainedProcess(RetainedProcessObservation observation) throws IOException {
        requireRetainedCandidate(observation, observation == null ? null : observation.operation);
        try {
            long actualCreation = processCreationTime(observation.handle.value, observation.operation);
            if (actualCreation != observation.creationFileTime) {
                observation.state = ProcessObservationState.POISONED;
                throw new IOException("Retained process HANDLE identity changed; no exit proof");
            }
            return processSignaled(observation.handle, observation.operation);
        } catch (IOException | RuntimeException | Error failure) {
            if (observation.state == ProcessObservationState.LIVE) observation.state = ProcessObservationState.POISONED;
            throw failure;
        }
    }

    private void discardRetainedProcess(RetainedProcessObservation observation) throws IOException {
        checkOwner();
        if (observation == null || observation.issuer != this || observation.owner != owner)
            throw new IOException("Exact retained process observation from this WindowsMarkerIo required");
        if (observation.state == ProcessObservationState.EXIT_CONFIRMED || observation.state == ProcessObservationState.DISCARDED
            || observation.state == ProcessObservationState.CLOSING || observation.state == ProcessObservationState.CLOSE_FAILED
            || observation.handle.closeAttempted)
            throw new IOException("Retained process observation is already consumed or has unresolved close debt");
        observation.state = ProcessObservationState.CLOSING;
        try {
            observation.handle.close();
            observation.state = ProcessObservationState.DISCARDED;
        } catch (IOException | RuntimeException | Error failure) {
            observation.state = ProcessObservationState.CLOSE_FAILED;
            throw failure;
        }
    }

    /**
     * 仅在 Provisioner 已从真实 receipt/guard 解出并匹配 PID/FILETIME 后调用。
     * WAIT_TIMEOUT 返回 null 并保留 observation；signaled 时对同一 HANDLE checked-close。
     */
    ProcessExitAcknowledgement confirmRetainedProcessExited(RetainedProcessObservation observation,
                                                               long expectedPid, long expectedCreationTime,
                                                               UUID operation) throws IOException {
        requireRetainedCandidate(observation, operation);
        validateProcessIdentity(expectedPid, expectedCreationTime);
        if (observation.pid != expectedPid || observation.creationFileTime != expectedCreationTime)
            throw new IOException("Durable previous process identity does not match retained native HANDLE");
        long actualCreation;
        try { actualCreation = processCreationTime(observation.handle.value, operation); }
        catch (IOException | RuntimeException | Error failure) {
            observation.state = ProcessObservationState.POISONED;
            throw failure;
        }
        if (actualCreation != expectedCreationTime) {
            observation.state = ProcessObservationState.POISONED;
            throw new IOException("Retained process HANDLE FILETIME differs from durable owner; no exit proof");
        }
        boolean signaled;
        try { signaled = processSignaled(observation.handle, operation); }
        catch (IOException | RuntimeException | Error failure) {
            observation.state = ProcessObservationState.POISONED;
            throw failure;
        }
        if (!signaled) return null;
        observation.state = ProcessObservationState.CLOSING;
        try {
            observation.handle.close();
            observation.state = ProcessObservationState.EXIT_CONFIRMED;
            return new ProcessExitAcknowledgement(this, observation, expectedPid, expectedCreationTime, operation);
        } catch (IOException | RuntimeException | Error failure) {
            observation.state = ProcessObservationState.CLOSE_FAILED;
            throw failure;
        }
    }

    void claimProcessExitAcknowledgement(ProcessExitAcknowledgement ack, RetainedProcessObservation observation,
                                         long expectedPid, long expectedCreationTime, UUID operation) throws IOException {
        checkOwner();
        if (ack == null || ack.issuer != this || ack.observation != observation || observation == null
            || observation.issuer != this || observation.state != ProcessObservationState.EXIT_CONFIRMED
            || ack.claimed || ack.pid != expectedPid || ack.creationFileTime != expectedCreationTime
            || !ack.operation.equals(operation) || !observation.operation.equals(operation))
            throw new IOException("Exact unconsumed native process exit acknowledgement required");
        ack.claimed = true;
    }

    private static IOException preserveIoFailure(String stage, Throwable primary) {
        if (primary instanceof IOException failure) return failure;
        if (primary instanceof RuntimeException failure) throw failure;
        if (primary instanceof Error failure) throw failure;
        return ioFailure(stage, primary);
    }

    /** Scalar PID/FILETIME can never prove process exit; callers must retain a live HANDLE first. */
    void requirePreviousProcessExited(long previousPid, long previousCreationTime, UUID operation) throws IOException {
        throw new IOException("Retained live process observation required; scalar process identity cannot authorize exit");
    }
    public static byte[] markerBytes(UUID binding, int format) {
        Objects.requireNonNull(binding);
        if (format != 1 && format != 2) throw new IllegalArgumentException("Unsupported physical format");
        return ByteBuffer.allocate(24).order(ByteOrder.BIG_ENDIAN).putInt(0x46505742).putInt(format)
            .putLong(binding.getMostSignificantBits()).putLong(binding.getLeastSignificantBits()).array();
    }
    @Override public void close() throws IOException {
        checkOwner();
        if (sourceIoHolder != null) {
            sourceIoHolder.close();
            return;
        }
        if (!owned.isEmpty() || !ownedFinds.isEmpty())
            throw new IOException("Unconfirmed native handle obligations remain: file=" + owned.size() + " find=" + ownedFinds.size());
        if (retainedWitnessArena != null) throw new IOException("Original witness workspace close remains unresolved", retainedWitnessArenaFailure);
        if (regionSession == null) libraryArena.close();
        else regionSession.closeInitializedArena(this, libraryArena, arenaCloseAttempt);
        closed = true;
    }
}
