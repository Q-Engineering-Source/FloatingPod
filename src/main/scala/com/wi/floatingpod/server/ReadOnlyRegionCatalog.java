package com.wi.floatingpod.server;

import net.minecraft.nbt.CompressedStreamTools;
import net.minecraft.nbt.NBTSizeTracker;
import net.minecraft.nbt.NBTTagCompound;
import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.source.SourceForestAllocationBudget;
import com.wi.floatingpod.server.save.ManagedSourceSaveLedger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.DigestException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;

/** 仅扫描调用方给出的 MCA 根目录；观察结果不代表完整世界覆盖或系统级路径身份。 */
public final class ReadOnlyRegionCatalog {
    private static final int SECTOR_BYTES = 4096;
    private static final int HEADER_BYTES = 8192;
    private static final int SLOT_COUNT = 1024;
    private static final int PATCHED_NBT_DEPTH_LIMIT = 512;
    private static final byte[] EMPTY_TARGET_BYTES = new byte[0];
    private static final Pattern REGION_NAME = Pattern.compile("^r\\.(-?\\d+)\\.(-?\\d+)\\.mca$");
    private static final FileSnapshotHook NO_FILE_HOOK = ignored -> { };

    private final FileSnapshotHook fileSnapshotHook;
    private final BoundedEntityNbtWalker entityWalker;
    private final CatalogReadResponsibility readResponsibility;

    public ReadOnlyRegionCatalog() {
        this(null, NO_FILE_HOOK);
    }

    ReadOnlyRegionCatalog(FileSnapshotHook fileSnapshotHook) {
        this(null, fileSnapshotHook);
    }

    public ReadOnlyRegionCatalog(CatalogReadResponsibility readResponsibility) {
        this(readResponsibility, NO_FILE_HOOK);
    }

    ReadOnlyRegionCatalog(CatalogReadResponsibility readResponsibility, FileSnapshotHook fileSnapshotHook) {
        if (fileSnapshotHook == null) {
            throw new IllegalArgumentException("file snapshot hook must not be null");
        }
        this.readResponsibility = readResponsibility;
        this.fileSnapshotHook = fileSnapshotHook;
        this.entityWalker = new BoundedEntityNbtWalker();
        if (readResponsibility != null) readResponsibility.attachReader(this);
    }

    private static void writeOwnedBytesTo(DataOutput destination, byte[] ownedBytes) throws IOException {
        for (int index = 0; index < ownedBytes.length; index++) destination.writeByte(ownedBytes[index]);
    }

    public CatalogSnapshot scan(CatalogInput input) throws CatalogException {
        if (input == null) {
            throw new IllegalArgumentException("explicit catalog input is required");
        }
        CatalogLocation initialLocation = input.roots().isEmpty()
            ? CatalogLocation.empty()
            : CatalogLocation.root(input.roots().get(0).identity(), input.roots().get(0).directory());
        if (Thread.currentThread() != input.ownerThread()) {
            throw failure("OWNER_THREAD", initialLocation, "scan must run on the explicitly supplied owner thread", null);
        }
        Limits limits = input.limits();
        if (input.roots().size() > limits.maxRoots()) {
            throw failure("CATALOG_LIMIT", initialLocation, "supplied region roots exceed the configured limit", null);
        }
        validateDistinctRootLocations(input.roots());

        ArrayList<RegionRootSnapshot> rootSnapshots = new ArrayList<>();
        ArrayList<RegionSnapshot> regions = new ArrayList<>();
        ArrayList<EntityOccurrence> occurrences = new ArrayList<>();
        long totalFiles = 0L;
        long totalChunks = 0L;
        long totalRegionBytes = 0L;
        long totalRetainedBytes = 0L;
        long totalEntityOccurrences = 0L;

        for (RegionRoot root : input.roots()) {
            CatalogLocation rootLocation = CatalogLocation.root(root.identity(), root.directory());
            DirectorySnapshot before;
            List<RegionFileEntry> beforeFiles;
            try {
                before = observeDirectory(root);
                beforeFiles = listRegionFiles(root, limits.maxFiles() - totalFiles);
            } catch (CatalogException failure) {
                throw failure;
            } catch (IOException | RuntimeException failure) {
                throw failure("REGION_ROOT_READ", rootLocation, "could not observe the supplied region root", failure);
            }
            totalFiles = addWithinLimit(totalFiles, beforeFiles.size(), limits.maxFiles(), rootLocation, "region file count exceeds the configured limit");
            Set<String> namesBefore = new HashSet<>();
            for (RegionFileEntry entry : beforeFiles) {
                namesBefore.add(entry.path().getFileName().toString());
            }

            for (RegionFileEntry entry : beforeFiles) {
                RegionScan result = scanRegionFile(root, entry, limits, input.ownerThread(), totalRetainedBytes,
                    limits.maxTotalRegionBytes() - totalRegionBytes, limits.maxChunks() - totalChunks, totalEntityOccurrences);
                totalRegionBytes = addWithinLimit(
                    totalRegionBytes,
                    result.region().byteLength(),
                    limits.maxTotalRegionBytes(),
                    result.location(),
                    "total region file bytes exceed the configured limit"
                );
                totalChunks = addWithinLimit(
                    totalChunks,
                    result.presentChunks(),
                    limits.maxChunks(),
                    result.location(),
                    "present chunk count exceeds the configured limit"
                );
                totalEntityOccurrences = addWithinLimit(totalEntityOccurrences, result.occurrences().size(), limits.maxTotalEntityOccurrences(), result.location(), "catalog entity occurrences exceed the configured total limit");
                totalRetainedBytes = addWithinLimit(
                    totalRetainedBytes,
                    result.retainedPayloadBytes(),
                    limits.maxRetainedPayloadBytes(),
                    result.location(),
                    "retained compressed, decoded, and entity payload bytes exceed the configured limit"
                );
                occurrences.addAll(result.occurrences());
                regions.add(result.region());
            }

            DirectorySnapshot after;
            List<RegionFileEntry> afterFiles;
            try {
                after = observeDirectory(root);
                afterFiles = listRegionFiles(root, beforeFiles.size());
            } catch (CatalogException failure) {
                throw failure;
            } catch (IOException | RuntimeException failure) {
                throw failure("REGION_ROOT_READ", rootLocation, "could not re-observe the supplied region root", failure);
            }
            Set<String> namesAfter = new HashSet<>();
            for (RegionFileEntry entry : afterFiles) {
                namesAfter.add(entry.path().getFileName().toString());
            }
            if (!before.sameObservation(after) || !namesBefore.equals(namesAfter)) {
                throw failure("REGION_ROOT_CHANGED", rootLocation, "region directory identity or visible .mca entry set changed during scan", null);
            }
            rootSnapshots.add(new RegionRootSnapshot(root, before, after, beforeFiles.size()));
        }

        return new CatalogSnapshot(
            input.worldIdentity(),
            input.generation(),
            input.manifestPin(),
            rootSnapshots,
            regions,
            occurrences,
            totalFiles,
            totalChunks,
            totalRegionBytes,
            totalRetainedBytes
        );
    }

    private RegionScan scanRegionFile(
        RegionRoot root,
        RegionFileEntry entry,
        Limits limits,
        Thread scanOwner,
        long alreadyRetainedBytes,
        long remainingRegionBytes,
        long remainingChunkCount,
        long alreadyEntityOccurrences
    ) throws CatalogException {
        CatalogLocation fileLocation = CatalogLocation.region(root.identity(), entry.path(), entry.regionX(), entry.regionZ());
        FileRead file;
        try {
            file = readRegionFile(root, entry, limits, remainingRegionBytes, scanOwner);
        } catch (CatalogException failure) {
            throw failure;
        } catch (IOException | RuntimeException failure) {
            throw failure("REGION_FILE_READ", fileLocation, "could not read a stable bounded region file", failure);
        }
        return scanRegionBytes(root, entry, limits, alreadyRetainedBytes, remainingChunkCount, alreadyEntityOccurrences, file);
    }

    /** Reuses the exact slot/compression/NBT/member parser for bytes read from the original RegionFile channel. */
    private RegionScan scanRegionBytes(
        RegionRoot root,
        RegionFileEntry entry,
        Limits limits,
        long alreadyRetainedBytes,
        long remainingChunkCount,
        long alreadyEntityOccurrences,
        FileRead file
    ) throws CatalogException {
        CatalogLocation fileLocation = CatalogLocation.region(root.identity(), entry.path(), entry.regionX(), entry.regionZ());
        byte[] bytes = file.bytes();
        if (bytes.length < HEADER_BYTES) {
            throw failure("REGION_HEADER", fileLocation, "region file is shorter than the complete two-sector header", null);
        }
        if (bytes.length % SECTOR_BYTES != 0) {
            throw failure("REGION_FILE_ALIGNMENT", fileLocation, "region file length is not sector-aligned", null);
        }
        int sectorCount = bytes.length / SECTOR_BYTES;
        boolean[] occupied = new boolean[sectorCount];
        occupied[0] = true;
        occupied[1] = true;
        ArrayList<SlotSnapshot> slots = new ArrayList<>(SLOT_COUNT);
        ArrayList<EntityOccurrence> found = new ArrayList<>();
        long chunkCount = 0L;
        long retained = 0L;

        for (int slotIndex = 0; slotIndex < SLOT_COUNT; slotIndex++) {
            int word = headerWord(bytes, slotIndex);
            int sectorOffset = (word >>> 8) & 0x00FFFFFF;
            int declaredCount = word & 0xFF;
            int localX = slotIndex & 31;
            int localZ = slotIndex >>> 5;
            int chunkX = checkedChunkCoordinate(entry.regionX(), localX, fileLocation, slotIndex);
            int chunkZ = checkedChunkCoordinate(entry.regionZ(), localZ, fileLocation, slotIndex);
            OptionalInt dimension = root.dimensionId();

            if (sectorOffset == 0 && declaredCount == 0) {
                slots.add(SlotSnapshot.empty(root.identity(), entry.path(), entry.regionX(), entry.regionZ(), slotIndex, localX, localZ, chunkX, chunkZ, dimension));
                continue;
            }
            CatalogLocation slotLocation = fileLocation.withSlot(slotIndex, chunkX, chunkZ);
            if (chunkCount >= remainingChunkCount) {
                throw failure("CATALOG_LIMIT", slotLocation, "present chunk count exceeds the remaining configured limit", null);
            }
            if (sectorOffset < 2 || declaredCount == 0) {
                throw failure("REGION_SLOT_BOUNDS", slotLocation, "non-empty slot must start after the header and declare a non-zero sector count", null);
            }
            long recordStart = Math.multiplyExact((long) sectorOffset, SECTOR_BYTES);
            if (recordStart + 4L > bytes.length) {
                throw failure("REGION_SLOT_BOUNDS", slotLocation, "slot length prefix is outside the region file", null);
            }
            int recordLength = ByteBuffer.wrap(bytes, Math.toIntExact(recordStart), 4).order(ByteOrder.BIG_ENDIAN).getInt();
            if (recordLength <= 0) {
                throw failure("REGION_RECORD_LENGTH", slotLocation, "stored chunk record length must be positive", null);
            }
            long recordEnd = checkedAdd(recordStart, checkedAdd(4L, recordLength, slotLocation, "record end overflow"), slotLocation, "record end overflow");
            if (recordEnd > bytes.length) {
                throw failure("REGION_RECORD_LENGTH", slotLocation, "stored chunk record exceeds the region file", null);
            }
            long sourceSpan = checkedAdd(((long) recordLength + 4L) / SECTOR_BYTES, 1L, slotLocation, "sector span overflow");
            long span = declaredCount == 255 ? sourceSpan : declaredCount;
            if ((declaredCount == 255 && sourceSpan < 255L) || (declaredCount != 255 && sourceSpan != declaredCount)) {
                throw failure("REGION_SECTOR_SPAN", slotLocation, "header sector count differs from the patched region record-length rule", null);
            }
            long endSector = checkedAdd(sectorOffset, span, slotLocation, "sector end overflow");
            if (endSector > sectorCount || recordLength + 4L > span * SECTOR_BYTES) {
                throw failure("REGION_SLOT_BOUNDS", slotLocation, "record sector span exceeds the region file or declared slot span", null);
            }
            for (long sector = sectorOffset; sector < endSector; sector++) {
                int index = Math.toIntExact(sector);
                if (occupied[index]) {
                    throw failure("REGION_SECTOR_OVERLAP", slotLocation, "region records overlap a header or another chunk sector", null);
                }
                occupied[index] = true;
            }

            int compressionId = bytes[Math.toIntExact(recordStart + 4L)] & 0xFF;
            int compressedStart = Math.toIntExact(recordStart + 5L);
            int compressedLength = recordLength - 1;
            if (compressedLength <= 0 || compressedLength > limits.maxCompressedBytesPerChunk()) {
                throw failure("COMPRESSED_LIMIT", slotLocation, "compressed chunk payload is empty or exceeds the configured per-slot limit", null);
            }
            byte[] compressed = Arrays.copyOfRange(bytes, compressedStart, compressedStart + compressedLength);
            long used = checkedAdd(alreadyRetainedBytes, retained, slotLocation, "retained payload byte count overflow");
            long afterCompressed = checkedAdd(used, compressed.length, slotLocation, "retained payload byte count overflow");
            if (afterCompressed >= limits.maxRetainedPayloadBytes()) {
                throw failure("CATALOG_LIMIT", slotLocation, "compressed chunk payload leaves no bounded room for decoded NBT", null);
            }
            long decodedBound = Math.min(limits.maxDecompressedBytesPerChunk(), limits.maxRetainedPayloadBytes() - afterCompressed);
            byte[] decoded = decompress(compressed, compressionId, decodedBound, slotLocation);
            NBTTagCompound chunkNbt = readNbt(decoded, limits, slotLocation);
            long slotRawRetained = checkedAdd(compressed.length, decoded.length, slotLocation, "retained payload byte count overflow");
            long remainingForEntities = limits.maxRetainedPayloadBytes() - used - slotRawRetained;
            if (remainingForEntities < 0L) {
                throw failure("CATALOG_LIMIT", slotLocation, "compressed and decoded chunk payload exceed the configured retained-byte limit", null);
            }
            ChunkSource source = new ChunkSource(
                root.identity(),
                entry.path(),
                entry.regionX(),
                entry.regionZ(),
                slotIndex,
                localX,
                localZ,
                chunkX,
                chunkZ,
                dimension
            );
            long remainingEntityCount = limits.maxTotalEntityOccurrences() - alreadyEntityOccurrences - found.size();
            int chunkEntityLimit = Math.toIntExact(Math.max(0L, Math.min(limits.maxEntityOccurrencesPerChunk(), remainingEntityCount)));
            List<EntityOccurrence> chunkOccurrences = entityWalker.walkWithinBudget(chunkNbt, source, limits, remainingForEntities, chunkEntityLimit);
            if (chunkOccurrences.size() > limits.maxEntityOccurrencesPerChunk()) {
                throw failure("ENTITY_COUNT_LIMIT", slotLocation, "chunk entity occurrence count exceeds the configured limit", null);
            }
            long entityBytes = 0L;
            for (EntityOccurrence occurrence : chunkOccurrences) {
                entityBytes = checkedAdd(entityBytes, occurrence.retainedPayloadBytes(), slotLocation, "entity payload byte count overflow");
            }
            retained = checkedAdd(retained, checkedAdd(slotRawRetained, entityBytes, slotLocation, "retained payload byte count overflow"), slotLocation, "retained payload byte count overflow");
            found.addAll(chunkOccurrences);
            chunkCount++;
            slots.add(SlotSnapshot.present(
                root.identity(),
                entry.path(),
                entry.regionX(),
                entry.regionZ(),
                slotIndex,
                localX,
                localZ,
                chunkX,
                chunkZ,
                dimension,
                sectorOffset,
                Math.toIntExact(span),
                compressionId,
                sha256Hex(compressed),
                compressed,
                decoded
            ));
        }

        RegionSnapshot snapshot = new RegionSnapshot(
            root.identity(),
            entry.path(),
            entry.regionX(),
            entry.regionZ(),
            root.dimensionId(),
            file.before(),
            file.after(),
            bytes.length,
            sha256Hex(bytes),
            slots
        );
        return new RegionScan(snapshot, found, chunkCount, retained, fileLocation);
    }

    /**
     * Parses one already bounded readback image; it performs no pathname read and grants no write capability.
     * The caller must retain and validate its native pin independently and close the returned snapshot on the same
     * owner thread.  Its child allocation scope reserves the configured retained-payload maximum, bounded decompressor
     * peak, NBT tracker, and count-derived slot/entity/preflight structures before this method allocates any parser data.
     * The returned DATA contains every slot after the same 255-sector, compression, decompressed NBT, and entity-member
     * checks used by catalog scans.  The supplied readback array remains the caller's separately charged buffer.
     */
    public RegionReadback inspectRegionReadback(String rootIdentity, Path regionPath, int regionX, int regionZ,
                                                 OptionalInt dimensionId, byte[] exactReadback, Limits limits,
                                                 int targetSlotIndex, SourceLimitProfile profile,
                                                SourceForestAllocationBudget allocation) throws CatalogException {
        if (rootIdentity == null || rootIdentity.isBlank() || regionPath == null || regionPath.getFileName() == null
            || regionPath.getParent() == null || !regionPath.isAbsolute()
            || !regionPath.equals(regionPath.normalize()) || dimensionId == null || exactReadback == null || limits == null
            || targetSlotIndex < 0 || targetSlotIndex >= SLOT_COUNT || profile == null || allocation == null) {
            throw new IllegalArgumentException("Exact normalized region path, coordinates, limits, bytes, target slot, profile, and owner allocation are required");
        }
        Matcher name = REGION_NAME.matcher(regionPath.getFileName().toString());
        if (!name.matches()) throw failure("REGION_NAME", CatalogLocation.file(rootIdentity, regionPath), "readback path is not a canonical MCA region basename", null);
        int namedX;
        int namedZ;
        try {
            namedX = Integer.parseInt(name.group(1));
            namedZ = Integer.parseInt(name.group(2));
        } catch (NumberFormatException cause) {
            throw failure("REGION_COORDINATE", CatalogLocation.file(rootIdentity, regionPath), "region basename coordinates exceed signed 32-bit bounds", cause);
        }
        CatalogLocation fileLocation = CatalogLocation.region(rootIdentity, regionPath, regionX, regionZ);
        if (namedX != regionX || namedZ != regionZ) throw failure("REGION_COORDINATE", fileLocation, "captured region coordinates differ from the exact MCA basename", null);
        if (exactReadback.length > limits.maxRegionFileBytes() || exactReadback.length < HEADER_BYTES || exactReadback.length % SECTOR_BYTES != 0) {
            throw failure("REGION_FILE_LIMIT", fileLocation, "exact readback length is outside the catalog bounds or sector alignment", null);
        }
        validateProfileBounds(profile, limits, fileLocation);
        SourceForestAllocationBudget.Scope workspace;
        try { workspace = SourceForestAllocationBudget.Scope.open(allocation, profile); }
        catch (UncheckedIOException cause) { throw failure("REGION_READBACK_BUDGET", fileLocation, "source owner could not reserve its readback workspace", cause); }
        Throwable primary = null;
        boolean transferred = false;
        try {
            workspace.charge(readbackWorkspaceBytes(exactReadback.length, limits), "actual bounded Region readback parser workspace BEFORE allocations");
        } catch (RuntimeException cause) {
            primary = cause;
            try { workspace.close(); } catch (Throwable cleanup) { suppressCleanup(primary, cleanup); }
            throw failure("REGION_READBACK_BUDGET", fileLocation, "source owner could not reserve its bounded Region readback parser peak", primary);
        } catch (Error cause) {
            primary = cause;
            try { workspace.close(); } catch (Throwable cleanup) { suppressCleanup(primary, cleanup); }
            throw cause;
        }
        try {
            RegionRoot root = new RegionRoot(rootIdentity, regionPath.getParent(), dimensionId);
            RegionFileEntry entry = new RegionFileEntry(regionPath, regionX, regionZ);
            RegionScan parsed = scanRegionBytes(root, entry, limits, 0L, limits.maxChunks(), 0L,
                new FileRead(exactReadback, null, null));
            RegionReadback result = new RegionReadback(parsed.region().byteLength(), parsed.region().sha256(), regionX, regionZ,
                targetSlotIndex, parsed.region().slots(), parsed.occurrences(), parsed.retainedPayloadBytes(), workspace);
            transferred = true;
            return result;
        } catch (CatalogException | RuntimeException | Error cause) {
            primary = cause;
            throw cause;
        } finally {
            if (!transferred) {
                try { workspace.close(); }
                catch (Throwable cleanup) {
                    if (primary != null) suppressCleanup(primary, cleanup);
                    else throw failure("REGION_READBACK_BUDGET", fileLocation, "source owner could not retire its failed Region readback workspace", cleanup);
                }
            }
        }
    }

    /**
     * Reads the complete caller-pinned MCA image as DATA, validates every original slot and allocated extent, and
     * fully decodes only the selected slot. The whole-image array is borrowed and must already be charged by its
     * original owner. Returned arrays/tree and parser metadata are valid only until the returned view is closed.
     */
    public SourceTargetReadback inspectSourceTarget(String rootIdentity, Path regionPath, int regionX, int regionZ,
            OptionalInt dimensionId, byte[] exactMcaBytes, int targetSlotIndex, SourceLimitProfile exactProfile,
            SourceForestAllocationBudget.Account originalAllocation) throws CatalogException {
        if (rootIdentity == null || rootIdentity.isBlank() || regionPath == null || regionPath.getFileName() == null
            || regionPath.getParent() == null || !regionPath.isAbsolute() || !regionPath.equals(regionPath.normalize())
            || dimensionId == null || exactMcaBytes == null || targetSlotIndex < 0 || targetSlotIndex >= SLOT_COUNT
            || exactProfile == null || originalAllocation == null) {
            throw new IllegalArgumentException("Exact normalized Region identity, complete MCA bytes, target slot, exact profile, and original allocation are required");
        }
        CatalogLocation fileLocation = CatalogLocation.region(rootIdentity, regionPath, regionX, regionZ);
        int fileLength = exactMcaBytes.length;
        if (fileLength < HEADER_BYTES || fileLength > exactProfile.maxRegionBytes() || fileLength % SECTOR_BYTES != 0) {
            throw failure("REGION_TARGET_FILE", fileLocation, "complete MCA image is outside the exact profile, header, or sector-alignment bound", null);
        }

        SourceForestAllocationBudget.Scope workspace;
        try {
            workspace = SourceForestAllocationBudget.Scope.open(originalAllocation, exactProfile);
        } catch (UncheckedIOException cause) {
            throw failure("REGION_TARGET_BUDGET", fileLocation, "original source owner could not open the target-reader workspace", cause);
        }

        Throwable primary = null;
        boolean transferred = false;
        byte[] targetCompressed = null;
        byte[] targetDecoded = null;
        long targetCompressedCharge = 0L;
        long targetDecodedCharge = 0L;
        BoundedNbtStructurePreflight.Node typedRoot = null;
        RegionHeaderSlot[] headerSlots = null;
        int[] recordLengths = null;
        int[] resolvedSpans = null;
        int[] compressionIds = null;
        int[] extentLengths = null;
        int[] extentHashOffsets = null;
        byte[] extentHashes = null;
        boolean[] occupied = null;
        try {
            workspace.charge(2048L, "target reader exact path/name/identity validation workspace BEFORE allocations");
            Matcher name = REGION_NAME.matcher(regionPath.getFileName().toString());
            if (!name.matches()) throw failure("REGION_NAME", fileLocation, "source target path is not a canonical MCA basename", null);
            try {
                if (Integer.parseInt(name.group(1)) != regionX || Integer.parseInt(name.group(2)) != regionZ) {
                    throw failure("REGION_COORDINATE", fileLocation, "source target region coordinates differ from the exact MCA basename", null);
                }
            } catch (NumberFormatException cause) {
                throw failure("REGION_COORDINATE", fileLocation, "source target basename coordinates exceed signed 32-bit bounds", cause);
            }

            workspace.charge(512L, "complete source MCA SHA-256 objects BEFORE allocation");
            String fileSha256 = sha256Hex(exactMcaBytes);
            workspace.charge(512L, "complete source MCA header SHA-256 objects BEFORE allocation");
            String headerSha256 = sha256HexRange(exactMcaBytes, 0, HEADER_BYTES);

            int sectorCount = fileLength / SECTOR_BYTES;
            int presentCount = 0;
            for (int slotIndex = 0; slotIndex < SLOT_COUNT; slotIndex++) {
                int word = readBigEndianInt(exactMcaBytes, slotIndex * Integer.BYTES);
                int sectorOffset = (word >>> 8) & 0x00FFFFFF;
                int declaredCount = word & 0xFF;
                if (sectorOffset != 0 || declaredCount != 0) presentCount++;
            }

            workspace.charge(sectorCount, "actual complete source MCA sector-overlap bitmap BEFORE allocation");
            occupied = new boolean[sectorCount];
            workspace.charge(Math.multiplyExact(8L, SLOT_COUNT), "actual 1024 source MCA header-slot references BEFORE allocation");
            headerSlots = new RegionHeaderSlot[SLOT_COUNT];
            workspace.charge(Math.multiplyExact(64L, SLOT_COUNT), "actual 1024 source MCA header-slot records BEFORE allocation");
            workspace.charge(Math.multiplyExact(4L, SLOT_COUNT), "actual source MCA record lengths BEFORE allocation");
            recordLengths = new int[SLOT_COUNT];
            workspace.charge(Math.multiplyExact(4L, SLOT_COUNT), "actual source MCA resolved record spans BEFORE allocation");
            resolvedSpans = new int[SLOT_COUNT];
            workspace.charge(Math.multiplyExact(4L, SLOT_COUNT), "actual source MCA compression identities BEFORE allocation");
            compressionIds = new int[SLOT_COUNT];
            workspace.charge(Math.multiplyExact(4L, SLOT_COUNT), "actual source MCA allocated extent lengths BEFORE allocation");
            extentLengths = new int[SLOT_COUNT];
            workspace.charge(Math.multiplyExact(4L, SLOT_COUNT), "actual source MCA extent digest offsets BEFORE allocation");
            extentHashOffsets = new int[SLOT_COUNT];
            Arrays.fill(extentHashOffsets, -1);
            long extentHashBytes = Math.multiplyExact((long) presentCount, 32L);
            workspace.charge(Math.max(16L, extentHashBytes), "actual non-target and target allocated-extent SHA-256 bytes BEFORE allocation");
            extentHashes = new byte[Math.multiplyExact(presentCount, 32)];

            MessageDigest extentDigest = null;
            if (presentCount > 0) {
                workspace.charge(256L, "single reused allocated-extent SHA-256 digest BEFORE allocation");
                try { extentDigest = MessageDigest.getInstance("SHA-256"); }
                catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException("JVM is missing required SHA-256", impossible); }
            }

            occupied[0] = true;
            occupied[1] = true;
            int presentOrdinal = 0;
            boolean targetPresent = false;
            int targetCompressedStart = 0;
            int targetCompressedLength = 0;
            int targetCompressionId = 0;

            for (int slotIndex = 0; slotIndex < SLOT_COUNT; slotIndex++) {
                int word = readBigEndianInt(exactMcaBytes, slotIndex * Integer.BYTES);
                int sectorOffset = (word >>> 8) & 0x00FFFFFF;
                int declaredCount = word & 0xFF;
                int timestamp = readBigEndianInt(exactMcaBytes, HEADER_BYTES / 2 + slotIndex * Integer.BYTES);
                if (sectorOffset == 0 && declaredCount == 0) {
                    headerSlots[slotIndex] = new RegionHeaderSlot(slotIndex, false, 0, 0, timestamp);
                    continue;
                }

                CatalogLocation slotLocation = fileLocation.withSlot(slotIndex,
                    checkedChunkCoordinate(regionX, slotIndex & 31, fileLocation, slotIndex),
                    checkedChunkCoordinate(regionZ, slotIndex >>> 5, fileLocation, slotIndex)
                );
                if (sectorOffset < 2 || declaredCount == 0) {
                    throw failure("REGION_SLOT_BOUNDS", slotLocation, "non-empty original MCA slot must begin after both header sectors and declare a non-zero span", null);
                }
                long recordStart = Math.multiplyExact((long) sectorOffset, SECTOR_BYTES);
                if (recordStart + Integer.BYTES > fileLength) {
                    throw failure("REGION_SLOT_BOUNDS", slotLocation, "MCA record-length prefix lies outside the complete pinned image", null);
                }
                int recordLength = readBigEndianInt(exactMcaBytes, Math.toIntExact(recordStart));
                if (recordLength <= 0) throw failure("REGION_RECORD_LENGTH", slotLocation, "stored MCA record length must be positive", null);
                long recordEnd = checkedAdd(recordStart,
                    checkedAdd(Integer.BYTES, recordLength, slotLocation, "MCA record end overflow"),
                    slotLocation, "MCA record end overflow");
                if (recordEnd > fileLength) throw failure("REGION_RECORD_LENGTH", slotLocation, "stored MCA record exceeds the complete pinned image", null);

                long completeRecordLength = checkedAdd((long) recordLength, Integer.BYTES, slotLocation,
                    "complete MCA target record length overflow");
                long resolvedSpan = validatePatchedTargetRecordSpan(completeRecordLength, sectorOffset,
                    declaredCount, sectorCount, slotLocation);
                long endSector = Math.addExact((long) sectorOffset, resolvedSpan);
                long extentByteLength = Math.multiplyExact(resolvedSpan, (long) SECTOR_BYTES);
                for (long sector = sectorOffset; sector < endSector; sector++) {
                    int actual = Math.toIntExact(sector);
                    if (occupied[actual]) throw failure("REGION_SECTOR_OVERLAP", slotLocation, "MCA allocated record extents overlap a header or another slot", null);
                    occupied[actual] = true;
                }

                int compressionId = exactMcaBytes[Math.toIntExact(recordStart + Integer.BYTES)] & 0xFF;
                int compressedLength = validateSourceCompressedBodyLength(recordLength, compressionId,
                    exactProfile, slotLocation);

                int extentBytes = Math.toIntExact(extentByteLength);
                int extentStart = Math.toIntExact(recordStart);
                int digestOffset = Math.multiplyExact(presentOrdinal, 32);
                extentDigest.reset();
                extentDigest.update(exactMcaBytes, extentStart, extentBytes);
                try {
                    if (extentDigest.digest(extentHashes, digestOffset, 32) != 32) {
                        throw new IllegalStateException("SHA-256 returned a non-canonical extent digest length");
                    }
                } catch (DigestException impossible) {
                    throw new IllegalStateException("Precharged allocated-extent digest output was too short", impossible);
                }

                headerSlots[slotIndex] = new RegionHeaderSlot(slotIndex, true, sectorOffset, declaredCount, timestamp);
                recordLengths[slotIndex] = recordLength;
                resolvedSpans[slotIndex] = Math.toIntExact(resolvedSpan);
                compressionIds[slotIndex] = compressionId;
                extentLengths[slotIndex] = extentBytes;
                extentHashOffsets[slotIndex] = digestOffset;
                presentOrdinal++;

                if (slotIndex == targetSlotIndex) {
                    targetPresent = true;
                    targetCompressedStart = Math.toIntExact(recordStart + Integer.BYTES + 1L);
                    targetCompressedLength = compressedLength;
                    targetCompressionId = compressionId;
                }
            }

            String compressedSha256 = "";
            String decodedSha256 = "";
            if (targetPresent) {
                try {
                    originalAllocation.reserveTemporary(targetCompressedLength,
                        "exact source target compressed body retained before target copy allocation");
                    targetCompressedCharge = targetCompressedLength;
                } catch (IOException cause) {
                    throw failure("REGION_TARGET_BUDGET", fileLocation, "original source account could not reserve the target compressed body", cause);
                }
                try {
                    targetCompressed = Arrays.copyOfRange(exactMcaBytes, targetCompressedStart,
                        Math.addExact(targetCompressedStart, targetCompressedLength));
                } catch (RuntimeException | Error cause) {
                    if (targetCompressedCharge > 0L) {
                        try { originalAllocation.releaseTemporary(targetCompressedCharge, "failed source target compressed body copy dropped"); }
                        catch (Throwable cleanup) { suppressCleanup(cause, cleanup); }
                        targetCompressedCharge = 0L;
                    }
                    throw cause;
                }

                targetDecoded = decompressSourceTarget(targetCompressed, targetCompressionId, exactProfile, originalAllocation, workspace,
                    fileLocation.withSlot(targetSlotIndex,
                        checkedChunkCoordinate(regionX, targetSlotIndex & 31, fileLocation, targetSlotIndex),
                        checkedChunkCoordinate(regionZ, targetSlotIndex >>> 5, fileLocation, targetSlotIndex)
                    ));
                targetDecodedCharge = targetDecoded.length;
                workspace.charge(512L, "target compressed SHA-256 string and digest workspace BEFORE allocation");
                compressedSha256 = sha256Hex(targetCompressed);
                workspace.charge(512L, "target decoded root SHA-256 string and digest workspace BEFORE allocation");
                decodedSha256 = sha256Hex(targetDecoded);
                CatalogLocation targetLocation = fileLocation.withSlot(targetSlotIndex,
                    checkedChunkCoordinate(regionX, targetSlotIndex & 31, fileLocation, targetSlotIndex),
                    checkedChunkCoordinate(regionZ, targetSlotIndex >>> 5, fileLocation, targetSlotIndex)
                );
                try {
                    typedRoot = BoundedNbtStructurePreflight.extract(targetDecoded, exactProfile,
                        exactProfile.maxRetainedBytes(), workspace);
                } catch (BoundedNbtStructurePreflight.PreflightException cause) {
                    throw failure("NBT_PRECHECK", targetLocation, "full typed source target NBT preflight rejected the complete decoded root", cause);
                } catch (UncheckedIOException cause) {
                    throw failure("REGION_TARGET_BUDGET", targetLocation, "original source account could not reserve the complete typed target NBT tree", cause);
                } catch (RuntimeException cause) {
                    throw failure("NBT_PRECHECK", targetLocation, "full typed source target NBT preflight failed", cause);
                }
            }

            workspace.charge(256L, "closeable source target DATA wrapper BEFORE allocation");
            SourceTargetReadback result = new SourceTargetReadback(rootIdentity, regionPath, regionX, regionZ, dimensionId,
                fileLength, fileSha256, headerSha256, targetSlotIndex, headerSlots, recordLengths, resolvedSpans,
                compressionIds, extentLengths, extentHashOffsets, extentHashes, targetCompressed, targetDecoded,
                targetCompressedCharge, targetDecodedCharge, compressedSha256, decodedSha256, typedRoot,
                workspace, originalAllocation);
            transferred = true;
            return result;
        } catch (CatalogException cause) {
            primary = cause;
            throw cause;
        } catch (IOException cause) {
            primary = cause;
            throw failure("REGION_TARGET_READ", fileLocation, "profile-native source target read failed", cause);
        } catch (UncheckedIOException cause) {
            primary = cause;
            throw failure("REGION_TARGET_BUDGET", fileLocation, "original source account rejected the profile-native target workspace", cause);
        } catch (RuntimeException cause) {
            primary = cause;
            throw cause;
        } catch (Error cause) {
            primary = cause;
            throw cause;
        } finally {
            if (!transferred) {
                targetCompressed = null;
                targetDecoded = null;
                typedRoot = null;
                headerSlots = null;
                recordLengths = null;
                resolvedSpans = null;
                compressionIds = null;
                extentLengths = null;
                extentHashOffsets = null;
                extentHashes = null;
                occupied = null;
                Throwable cleanupFailure = null;
                if (targetCompressedCharge > 0L) {
                    try {
                        originalAllocation.releaseTemporary(targetCompressedCharge, "failed source target compressed body dropped before refund");
                        targetCompressedCharge = 0L;
                    } catch (Throwable cleanup) { cleanupFailure = cleanup; }
                }
                if (targetDecodedCharge > 0L) {
                    try {
                        originalAllocation.releaseTemporary(targetDecodedCharge, "failed source target decoded root dropped before refund");
                        targetDecodedCharge = 0L;
                    } catch (Throwable cleanup) {
                        if (cleanupFailure == null) cleanupFailure = cleanup;
                        else suppressCleanup(cleanupFailure, cleanup);
                    }
                }
                try { workspace.close(); }
                catch (Throwable cleanup) {
                    if (cleanupFailure == null) cleanupFailure = cleanup;
                    else suppressCleanup(cleanupFailure, cleanup);
                }
                if (cleanupFailure != null) {
                    if (primary != null) suppressCleanup(primary, cleanupFailure);
                    else if (cleanupFailure instanceof IOException cause) {
                        throw failure("REGION_TARGET_BUDGET", fileLocation, "failed source target workspace could not be retired", cause);
                    } else if (cleanupFailure instanceof RuntimeException cause) throw cause;
                    else if (cleanupFailure instanceof Error cause) throw cause;
                    else throw new IllegalStateException("Unexpected source target cleanup failure", cleanupFailure);
                }
            }
        }
    }

    /**
     * Validates the bounded receipt projection of one exact target record. This proves only the
     * DATA visible in the supplied header/record; complete-image extent and overlap evidence stays
     * with the existing native reader.
     */
    static void validateSourceReceiptTargetRecordData(byte[] exactHeader, long exactAfterLength, int exactSlot,
            byte[] exactTargetRecord, byte[] exactDecodedRoot, SourceLimitProfile exactProfile,
            SourceForestAllocationBudget originalAllocation) throws CatalogException {
        CatalogLocation location = CatalogLocation.empty();
        if (exactHeader == null || exactHeader.length != HEADER_BYTES || exactAfterLength < HEADER_BYTES
            || exactSlot < 0 || exactSlot >= SLOT_COUNT || exactTargetRecord == null || exactDecodedRoot == null
            || exactProfile == null || originalAllocation == null
            || originalAllocation.exactProfile() != exactProfile) {
            throw failure("REGION_RECEIPT_ARGUMENT", location,
                "exact 8192-byte header, aligned after stamp, target slot/record, decoded root, profile, and original allocation are required", null);
        }
        location = location.withSlot(exactSlot, null, null);
        if (exactAfterLength > exactProfile.maxRegionBytes() || exactAfterLength % SECTOR_BYTES != 0L
            || exactDecodedRoot.length == 0 || (long) exactDecodedRoot.length > exactProfile.maxDecompressedBytes()
            || (long) exactDecodedRoot.length > exactProfile.maxRetainedBytes()) {
            throw failure("REGION_RECEIPT_PROFILE", location,
                "receipt target after length or decoded root is outside the exact region/decompression/retained profile", null);
        }
        if (exactTargetRecord.length <= 5 || exactTargetRecord.length > exactProfile.maxSqlPacketBytes()
            || exactTargetRecord.length > exactProfile.maxRetainedBytes()) {
            throw failure("REGION_RECEIPT_RECORD", location,
                "complete receipt target record is empty or exceeds the exact SQL/retained bound", null);
        }

        SourceForestAllocationBudget.Scope workspace;
        try { workspace = SourceForestAllocationBudget.Scope.open(originalAllocation, exactProfile); }
        catch (UncheckedIOException cause) {
            throw failure("REGION_TARGET_BUDGET", location,
                "original source allocation could not open the target-record validation workspace", cause);
        }
        byte[] decoded = null;
        long decodedCharge = 0L;
        Throwable primary = null;
        try (workspace) {
            workspace.charge(2_048L, "receipt target-record cursor and NBT wrapper workspace BEFORE allocation");
            workspace.charge(Math.multiplyExact(2L * SLOT_COUNT, Long.BYTES),
                "receipt target declared-sector interval table BEFORE allocation");
            long sectorCount = exactAfterLength / SECTOR_BYTES;
            long[] starts = new long[SLOT_COUNT];
            long[] ends = new long[SLOT_COUNT];
            int intervalCount = 0;
            long targetStart = -1L;
            int targetDeclaredCount = 0;
            for (int slotIndex = 0; slotIndex < SLOT_COUNT; slotIndex++) {
                int word = readBigEndianInt(exactHeader, slotIndex * Integer.BYTES);
                int sectorOffset = (word >>> 8) & 0x00FFFFFF;
                int declaredCount = word & 0xFF;
                if (sectorOffset == 0 && declaredCount == 0) continue;
                CatalogLocation slotLocation = location.withSlot(slotIndex, null, null);
                if (sectorOffset < 2 || declaredCount == 0)
                    throw failure("REGION_SLOT_BOUNDS", slotLocation,
                        "non-empty receipt MCA location must start after both headers and declare a non-zero span", null);
                long declaredEnd;
                try { declaredEnd = Math.addExact((long) sectorOffset, declaredCount); }
                catch (ArithmeticException cause) {
                    throw failure("REGION_SLOT_BOUNDS", slotLocation, "declared receipt MCA slot bound overflowed", cause);
                }
                if (declaredEnd > sectorCount)
                    throw failure("REGION_SLOT_BOUNDS", slotLocation,
                        "declared receipt MCA slot extent lies beyond the exact after-stamp file length", null);
                if (slotIndex == exactSlot) {
                    targetStart = sectorOffset;
                    targetDeclaredCount = declaredCount;
                    continue;
                }
                for (int prior = 0; prior < intervalCount; prior++) {
                    if ((long) sectorOffset < ends[prior] && starts[prior] < declaredEnd)
                        throw failure("REGION_SECTOR_OVERLAP", slotLocation,
                            "determinable declared receipt MCA extents overlap a prior slot", null);
                }
                starts[intervalCount] = sectorOffset;
                ends[intervalCount] = declaredEnd;
                intervalCount++;
            }
            if (targetStart < 0L || targetDeclaredCount == 0)
                throw failure("REGION_SLOT_ABSENT", location, "receipt target slot is absent from the exact after header", null);

            int recordLength = exactTargetRecord.length;
            int innerLength = readBigEndianInt(exactTargetRecord, 0);
            if (innerLength <= 1 || innerLength != recordLength - Integer.BYTES)
                throw failure("REGION_RECORD_LENGTH", location,
                    "receipt complete target record must contain exactly its positive inner length, compression tag, and body", null);
            int compressionId = exactTargetRecord[Integer.BYTES] & 0xFF;
            int compressedLength = validateSourceCompressedBodyLength(innerLength, compressionId,
                exactProfile, location);
            long sourceSpan = validatePatchedTargetRecordSpan(recordLength, Math.toIntExact(targetStart),
                targetDeclaredCount, sectorCount, location);
            long endSector;
            long allocatedBytes;
            long recordStart;
            long recordEnd;
            try {
                endSector = Math.addExact(targetStart, sourceSpan);
                allocatedBytes = Math.multiplyExact(sourceSpan, (long) SECTOR_BYTES);
                recordStart = Math.multiplyExact(targetStart, (long) SECTOR_BYTES);
                recordEnd = Math.addExact(recordStart, recordLength);
            } catch (ArithmeticException cause) {
                throw failure("REGION_SLOT_BOUNDS", location, "receipt target actual sector/record bound overflowed", cause);
            }
            if (endSector > sectorCount || recordEnd > exactAfterLength || recordLength > allocatedBytes)
                throw failure("REGION_SLOT_BOUNDS", location,
                    "complete receipt target record does not fit its checked actual allocated sector span", null);
            for (int prior = 0; prior < intervalCount; prior++) {
                if (targetStart < ends[prior] && starts[prior] < endSector)
                    throw failure("REGION_SECTOR_OVERLAP", location,
                        "receipt target actual span overlaps a determinable other declared slot extent", null);
            }

            workspace.charge(96L, "receipt target decompression range wrapper BEFORE use");
            try {
                decoded = decompressSourceTargetRange(exactTargetRecord, Integer.BYTES + 1,
                    compressedLength, compressionId, exactProfile, originalAllocation, workspace, location);
                decodedCharge = decoded.length;
            } catch (CatalogException cause) {
                throw cause;
            }
            if (!Arrays.equals(decoded, exactDecodedRoot))
                throw failure("REGION_READBACK_MISMATCH", location,
                    "complete decoded receipt target record differs byte-for-byte from readbackDecodedRootNbt", null);
            try {
                BoundedNbtStructurePreflight.extract(exactDecodedRoot, exactProfile,
                    exactProfile.maxRetainedBytes(), workspace);
            } catch (BoundedNbtStructurePreflight.PreflightException cause) {
                throw failure("NBT_PRECHECK", location,
                    "complete receipt target decoded root failed the existing bounded typed NBT preflight", cause);
            } catch (UncheckedIOException cause) {
                throw failure("REGION_TARGET_BUDGET", location,
                    "original source allocation rejected complete receipt target NBT preflight", cause);
            } catch (RuntimeException cause) {
                throw failure("NBT_PRECHECK", location,
                    "complete receipt target NBT preflight failed unexpectedly", cause);
            }
        } catch (CatalogException cause) {
            primary = cause;
            throw cause;
        } catch (UncheckedIOException cause) {
            primary = cause;
            throw failure("REGION_TARGET_BUDGET", location,
                "original source allocation rejected receipt target validation workspace", cause);
        } catch (RuntimeException cause) {
            primary = cause;
            throw failure("REGION_RECEIPT_RECORD", location,
                "receipt target record or bounded target validation failed", cause);
        } finally {
            decoded = null;
            if (decodedCharge > 0L) {
                try { originalAllocation.releaseTemporary(decodedCharge, "receipt target decoded root dropped after byte/NBT validation"); }
                catch (Throwable cleanup) {
                    if (primary != null) suppressCleanup(primary, cleanup);
                    else if (cleanup instanceof IOException io)
                        throw failure("REGION_TARGET_BUDGET", location,
                            "receipt target decoded root could not be retired from the original allocation", io);
                    else if (cleanup instanceof RuntimeException runtime) throw runtime;
                    else if (cleanup instanceof Error error) throw error;
                    else throw new IllegalStateException("Unexpected receipt target decoder cleanup failure", cleanup);
                }
            }
        }
    }

    private static long validatePatchedTargetRecordSpan(long completeRecordLength, int sectorOffset,
            int declaredCount, long sectorCount, CatalogLocation location) throws CatalogException {
        if (completeRecordLength <= 0L || sectorOffset < 2 || declaredCount < 1)
            throw failure("REGION_SLOT_BOUNDS", location,
                "complete MCA target record requires a positive length, offset after both headers, and non-zero declared count", null);
        final long sourceSpan, endSector, extentBytes;
        try {
            sourceSpan = Math.addExact(completeRecordLength / SECTOR_BYTES, 1L);
            if (declaredCount == 255) {
                if (sourceSpan < 255L)
                    throw failure("REGION_SECTOR_SPAN", location,
                        "declared count 255 does not resolve to at least 255 patched actual sectors", null);
            } else if (sourceSpan != declaredCount) {
                throw failure("REGION_SECTOR_SPAN", location,
                    "header sector count differs from the patched floor(R/4096)+1 target record span", null);
            }
            endSector = Math.addExact((long) sectorOffset, sourceSpan);
            extentBytes = Math.multiplyExact(sourceSpan, (long) SECTOR_BYTES);
        } catch (ArithmeticException cause) {
            throw failure("REGION_SLOT_BOUNDS", location,
                "complete MCA target record sector/span arithmetic overflowed", cause);
        }
        if (endSector > sectorCount || completeRecordLength > extentBytes)
            throw failure("REGION_SLOT_BOUNDS", location,
                "complete MCA target record does not fit its checked patched sector extent", null);
        return sourceSpan;
    }

    private static int validateSourceCompressedBodyLength(int lengthField, int compressionId,
            SourceLimitProfile profile, CatalogLocation location) throws CatalogException {
        if (compressionId != 1 && compressionId != 2)
            throw failure("REGION_COMPRESSION", location,
                "only patched compression ids 1 (gzip) and 2 (zlib) are supported", null);
        int compressedLength = lengthField - 1;
        if (lengthField <= 1 || compressedLength <= 0 || (long) compressedLength > profile.maxChunkBytes())
            throw failure("COMPRESSED_LIMIT", location,
                "compressed MCA target body is empty or exceeds the exact source profile chunk bound", null);
        return compressedLength;
    }

    private static byte[] decompressSourceTarget(byte[] compressed, int compressionId, SourceLimitProfile profile,
            SourceForestAllocationBudget allocation, SourceForestAllocationBudget.Scope workspace,
            CatalogLocation location) throws CatalogException {
        return decompressSourceTargetRange(compressed, 0, compressed.length, compressionId,
            profile, allocation, workspace, location);
    }

    private static byte[] decompressSourceTargetRange(byte[] compressed, int offset, int length,
            int compressionId, SourceLimitProfile profile, SourceForestAllocationBudget allocation,
            SourceForestAllocationBudget.Scope workspace, CatalogLocation location) throws CatalogException {
        if (profile.maxDecompressedBytes() <= 0L) {
            throw failure("REGION_PROFILE", location, "source target decompression requires a positive profile bound", null);
        }
        try {
            workspace.charge(64L, "accounted source target decoder output wrapper BEFORE allocation");
            AccountedTargetOutput output = new AccountedTargetOutput(allocation);
            try (output) {
                if (compressionId == 1) {
                    workspace.charge(8704L, "gzip target decoder, compressed input stream, and internal buffer BEFORE allocation");
                    ByteArrayInputStream source = new ByteArrayInputStream(compressed, offset, length);
                    try (GZIPInputStream input = new GZIPInputStream(source, 8192)) {
                        copyBoundedTarget(input, output, profile.maxDecompressedBytes(), workspace);
                    }
                } else if (compressionId == 2) {
                    inflateZlibTarget(compressed, offset, length, output, profile.maxDecompressedBytes(), workspace);
                } else {
                    throw failure("REGION_COMPRESSION", location, "only patched compression ids 1 (gzip) and 2 (zlib) are supported", null);
                }
                return output.finish();
            }
        } catch (CatalogException cause) {
            throw cause;
        } catch (TargetBudgetIOException cause) {
            throw failure("REGION_TARGET_BUDGET", location, "original source account rejected target decoder storage", cause);
        } catch (UncheckedIOException cause) {
            throw failure("REGION_TARGET_BUDGET", location, "original source account rejected target decoder workspace", cause);
        } catch (IOException | DataFormatException | RuntimeException cause) {
            throw failure("REGION_DECOMPRESS", location, "compressed target record is malformed, truncated, overlong, or exceeds its exact profile bound", cause);
        }
    }

    private static void copyBoundedTarget(InputStream input, AccountedTargetOutput output, long maximumBytes,
            SourceForestAllocationBudget.Scope workspace) throws IOException {
        workspace.charge(8192L, "gzip target decoder copy buffer BEFORE allocation");
        byte[] buffer = new byte[8192];
        long total = 0L;
        while (true) {
            int read = input.read(buffer);
            if (read < 0) return;
            if (read == 0) continue;
            total = Math.addExact(total, read);
            if (total > maximumBytes) throw new IOException("decompressed target chunk exceeds the exact source profile bound");
            output.write(buffer, 0, read);
        }
    }

    private static void inflateZlibTarget(byte[] compressed, int offset, int length,
            AccountedTargetOutput output, long maximumBytes,
            SourceForestAllocationBudget.Scope workspace) throws IOException, DataFormatException {
        workspace.charge(256L, "target zlib inflater wrapper BEFORE allocation");
        Inflater inflater = new Inflater(false);
        try {
            inflater.setInput(compressed, offset, length);
            workspace.charge(8192L, "target zlib decompression buffer BEFORE allocation");
            byte[] buffer = new byte[8192];
            long total = 0L;
            while (!inflater.finished()) {
                int produced = inflater.inflate(buffer);
                if (produced > 0) {
                    total = Math.addExact(total, produced);
                    if (total > maximumBytes) throw new IOException("decompressed zlib target chunk exceeds the exact source profile bound");
                    output.write(buffer, 0, produced);
                    continue;
                }
                if (inflater.needsDictionary()) throw new DataFormatException("zlib target stream requires an unsupported preset dictionary");
                if (inflater.needsInput()) throw new DataFormatException("zlib target stream ended before its checksum and end marker");
                throw new DataFormatException("zlib target inflater made no progress");
            }
            if (inflater.getRemaining() != 0) throw new DataFormatException("zlib target payload has trailing bytes after its checksum");
        } catch (ArithmeticException cause) {
            throw new IOException("decompressed zlib target byte count overflowed", cause);
        } finally {
            inflater.end();
        }
    }

    private static int readBigEndianInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24) | ((bytes[offset + 1] & 0xFF) << 16)
            | ((bytes[offset + 2] & 0xFF) << 8) | (bytes[offset + 3] & 0xFF);
    }

    private static String sha256HexRange(byte[] bytes, int offset, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(bytes, offset, length);
            return hexDigest(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("JVM is missing required SHA-256", impossible);
        }
    }

    private static String hexDigest(byte[] digest) {
        StringBuilder result = new StringBuilder(Math.multiplyExact(digest.length, 2));
        for (byte value : digest) {
            result.append(Character.forDigit((value >>> 4) & 0xF, 16));
            result.append(Character.forDigit(value & 0xF, 16));
        }
        return result.toString().toUpperCase(Locale.ROOT);
    }

    private static final class AccountedTargetOutput extends OutputStream {
        private final SourceForestAllocationBudget allocation;
        private byte[] bytes;
        private int count;
        private long heldBytes;
        private boolean closed;
        private Throwable cleanupFailure;

        private AccountedTargetOutput(SourceForestAllocationBudget allocation) {
            this.allocation = allocation;
        }

        @Override public void write(int value) throws IOException {
            requireOpen();
            ensureCapacity(Math.addExact(count, 1));
            bytes[count++] = (byte) value;
        }

        @Override public void write(byte[] source, int offset, int length) throws IOException {
            requireOpen();
            Objects.checkFromIndexSize(offset, length, source.length);
            if (length == 0) return;
            ensureCapacity(Math.addExact(count, length));
            System.arraycopy(source, offset, bytes, count, length);
            count += length;
        }

        private void ensureCapacity(int minimum) throws IOException {
            if (minimum <= (bytes == null ? 0 : bytes.length)) return;
            long nextCapacity = bytes == null || bytes.length == 0 ? 32L : Math.min((long) Integer.MAX_VALUE, (long) bytes.length * 2L);
            nextCapacity = Math.max(nextCapacity, minimum);
            if (nextCapacity > Integer.MAX_VALUE) throw new IOException("decoded target chunk cannot fit a bounded byte array");
            int capacity = Math.toIntExact(nextCapacity);
            try { allocation.reserveTemporary(capacity, "actual target decoder output growth BEFORE allocation"); }
            catch (IOException cause) { throw new TargetBudgetIOException("target decoder output reservation failed", cause); }

            byte[] replacement;
            try { replacement = new byte[capacity]; }
            catch (RuntimeException | Error cause) {
                try { allocation.releaseTemporary(capacity, "failed target decoder output allocation dropped"); }
                catch (Throwable cleanup) { suppressCleanup(cause, cleanup); }
                throw cause;
            }
            int previousCapacity = bytes == null ? 0 : bytes.length;
            if (previousCapacity > 0) System.arraycopy(bytes, 0, replacement, 0, count);
            byte[] previous = bytes;
            bytes = replacement;
            heldBytes = capacity;
            previous = null;
            if (previousCapacity > 0) {
                try { allocation.releaseTemporary(previousCapacity, "superseded target decoder output dropped after growth"); }
                catch (IOException cause) { throw new TargetBudgetIOException("superseded target decoder output refund failed", cause); }
            }
        }

        private byte[] finish() throws IOException {
            requireOpen();
            if (count == 0) {
                int previousCapacity = bytes == null ? 0 : bytes.length;
                bytes = null;
                heldBytes = 0L;
                closed = true;
                if (previousCapacity > 0) {
                    try { allocation.releaseTemporary(previousCapacity, "empty target decoder output dropped before refund"); }
                    catch (IOException cause) { cleanupFailure = cause; throw new TargetBudgetIOException("empty target decoder output refund failed", cause); }
                }
                return EMPTY_TARGET_BYTES;
            }
            if (bytes.length == count) {
                byte[] result = bytes;
                bytes = null;
                heldBytes = 0L;
                closed = true;
                return result;
            }
            try { allocation.reserveTemporary(count, "exact decoded target root copy BEFORE allocation"); }
            catch (IOException cause) { throw new TargetBudgetIOException("exact decoded target root copy reservation failed", cause); }
            byte[] exact;
            try { exact = new byte[count]; }
            catch (RuntimeException | Error cause) {
                try { allocation.releaseTemporary(count, "failed exact decoded target root allocation dropped"); }
                catch (Throwable cleanup) { suppressCleanup(cause, cleanup); }
                throw cause;
            }
            System.arraycopy(bytes, 0, exact, 0, count);
            int previousCapacity = bytes.length;
            bytes = null;
            heldBytes = 0L;
            closed = true;
            try { allocation.releaseTemporary(previousCapacity, "superseded target decoder output dropped after exact copy"); }
            catch (IOException cause) { cleanupFailure = cause; throw new TargetBudgetIOException("superseded target decoder output refund failed", cause); }
            return exact;
        }

        @Override public void close() throws IOException {
            if (closed) {
                if (cleanupFailure != null) throw new IOException("Target decoder output cleanup previously failed", cleanupFailure);
                return;
            }
            long refund = heldBytes;
            bytes = null;
            heldBytes = 0L;
            closed = true;
            if (refund == 0L) return;
            try { allocation.releaseTemporary(refund, "target decoder scratch output dropped before refund"); }
            catch (IOException | RuntimeException | Error cause) { cleanupFailure = cause; throw cause; }
        }

        private void requireOpen() {
            if (closed || cleanupFailure != null) throw new IllegalStateException("Open accounted target decoder output required", cleanupFailure);
        }
    }

    private static final class TargetBudgetIOException extends IOException {
        private TargetBudgetIOException(String message, Throwable cause) { super(message, cause); }
    }

    private static void validateProfileBounds(SourceLimitProfile profile, Limits limits, CatalogLocation location) throws CatalogException {
        if (limits.maxRegionFileBytes() > profile.maxRegionBytes() || limits.maxCompressedBytesPerChunk() > profile.maxChunkBytes()
            || limits.maxDecompressedBytesPerChunk() > profile.maxDecompressedBytes() || limits.maxNbtDepth() > profile.maxNbtDepth()
            || limits.maxNbtTags() > profile.maxMembers() || limits.maxRetainedPayloadBytes() > profile.maxRetainedBytes()) {
            throw failure("REGION_PROFILE", location, "catalog bounds exceed the exact source owner profile", null);
        }
    }

    private static long readbackWorkspaceBytes(long fileLength, Limits limits) {
        long sectors = fileLength / SECTOR_BYTES;
        long retained = limits.maxRetainedPayloadBytes();
        long decoderPeak = Math.multiplyExact(4L, limits.maxDecompressedBytesPerChunk());
        long compressedScratch = limits.maxCompressedBytesPerChunk();
        long nbtTree = limits.maxNbtTrackerBytes();
        long preflightNodes = Math.multiplyExact(32L, limits.maxNbtTags());
        long slotMetadata = Math.multiplyExact(64L, SLOT_COUNT);
        long entityMetadata = Math.multiplyExact(64L, limits.maxTotalEntityOccurrences());
        return Math.addExact(retained, Math.addExact(decoderPeak, Math.addExact(compressedScratch,
            Math.addExact(nbtTree, Math.addExact(preflightNodes, Math.addExact(slotMetadata,
                Math.addExact(entityMetadata, sectors)))))));
    }

    private static void suppressCleanup(Throwable primary, Throwable cleanup) {
        if (primary == cleanup) return;
        try { primary.addSuppressed(cleanup); }
        catch (Throwable association) {
            if (association != primary && association != cleanup) {
                try { cleanup.addSuppressed(association); } catch (Throwable ignored) { }
            }
        }
    }

    /**
     * Validates all original pinned header entries before RegionFile construction can normalize or extend them.
     * The caller obtains/charges {@code exactHeader} from the native pin; this method charges its sector bitmap and
     * all 1024 retained slot observations under a child scope released only by closing the returned snapshot.
     */
    public RegionHeaderSnapshot validateRegionHeader(String rootIdentity, Path regionPath, int regionX, int regionZ,
                                                      byte[] exactHeader, long fileLength, Limits limits,
                                                      int targetSlotIndex, SourceLimitProfile profile,
                                                      SourceForestAllocationBudget allocation) throws CatalogException {
        if (rootIdentity == null || rootIdentity.isBlank() || regionPath == null || regionPath.getFileName() == null
            || regionPath.getParent() == null || !regionPath.isAbsolute()
            || !regionPath.equals(regionPath.normalize()) || exactHeader == null || exactHeader.length != HEADER_BYTES
            || limits == null || targetSlotIndex < 0 || targetSlotIndex >= SLOT_COUNT || profile == null || allocation == null) {
            throw new IllegalArgumentException("Exact normalized region path, 8192-byte header, length, limits, target slot, profile, and owner allocation are required");
        }
        CatalogLocation location = CatalogLocation.region(rootIdentity, regionPath, regionX, regionZ);
        Matcher name = REGION_NAME.matcher(regionPath.getFileName().toString());
        if (!name.matches()) throw failure("REGION_NAME", location, "pinned source path is not a canonical MCA region basename", null);
        try {
            if (Integer.parseInt(name.group(1)) != regionX || Integer.parseInt(name.group(2)) != regionZ)
                throw failure("REGION_COORDINATE", location, "captured region coordinates differ from the exact MCA basename", null);
        } catch (NumberFormatException cause) {
            throw failure("REGION_COORDINATE", location, "region basename coordinates exceed signed 32-bit bounds", cause);
        }
        if (fileLength < HEADER_BYTES || fileLength > limits.maxRegionFileBytes() || fileLength % SECTOR_BYTES != 0) {
            throw failure("REGION_HEADER", location, "pinned original length is outside the complete-header, alignment, or profile bound", null);
        }
        validateProfileBounds(profile, limits, location);
        long sectorCount = fileLength / SECTOR_BYTES;
        SourceForestAllocationBudget.Scope workspace;
        try { workspace = SourceForestAllocationBudget.Scope.open(allocation, profile); }
        catch (UncheckedIOException cause) { throw failure("REGION_HEADER_BUDGET", location, "source owner could not reserve its exact original header workspace", cause); }
        Throwable primary = null;
        boolean transferred = false;
        try {
            workspace.charge(Math.addExact(sectorCount, Math.addExact(Math.multiplyExact(64L, SLOT_COUNT), 256L)),
                "actual pinned Region header slots/sector validation workspace BEFORE allocation");
            boolean[] occupied = new boolean[Math.toIntExact(sectorCount)];
            occupied[0] = true;
            occupied[1] = true;
            ArrayList<RegionHeaderSlot> slots = new ArrayList<>(SLOT_COUNT);
            for (int slotIndex = 0; slotIndex < SLOT_COUNT; slotIndex++) {
                int word = headerWord(exactHeader, slotIndex);
                int offset = (word >>> 8) & 0x00FFFFFF;
                int sectors = word & 0xFF;
                int timestamp = ByteBuffer.wrap(exactHeader, HEADER_BYTES / 2 + slotIndex * 4, 4).order(ByteOrder.BIG_ENDIAN).getInt();
                if (offset == 0 && sectors == 0) {
                    slots.add(new RegionHeaderSlot(slotIndex, false, 0, 0, timestamp));
                    continue;
                }
                CatalogLocation slot = location.withSlot(slotIndex,
                    checkedChunkCoordinate(regionX, slotIndex & 31, location, slotIndex),
                    checkedChunkCoordinate(regionZ, slotIndex >>> 5, location, slotIndex));
                if (offset < 2 || sectors == 0) throw failure("REGION_SLOT_BOUNDS", slot, "non-empty original header entry must start after both header sectors and declare sectors", null);
                long end = (long) offset + sectors;
                if (end > sectorCount) throw failure("REGION_SLOT_BOUNDS", slot, "original header entry extends past the pinned source length", null);
                for (long sector = offset; sector < end; sector++) {
                    int actual = Math.toIntExact(sector);
                    if (occupied[actual]) throw failure("REGION_SECTOR_OVERLAP", slot, "original header entries overlap a header or another declared sector span", null);
                    occupied[actual] = true;
                }
                slots.add(new RegionHeaderSlot(slotIndex, true, offset, sectors, timestamp));
            }
            RegionHeaderSnapshot result = new RegionHeaderSnapshot(fileLength, regionX, regionZ, sha256Hex(exactHeader),
                targetSlotIndex, slots, workspace);
            transferred = true;
            return result;
        } catch (CatalogException | RuntimeException cause) {
            primary = cause;
            throw cause;
        } catch (Error cause) {
            primary = cause;
            throw cause;
        } finally {
            if (!transferred) {
                try { workspace.close(); }
                catch (Throwable cleanup) {
                    if (primary != null) suppressCleanup(primary, cleanup);
                    else throw failure("REGION_HEADER_BUDGET", location, "source owner could not retire failed original header workspace", cleanup);
                }
            }
        }
    }

    private static int headerWord(byte[] bytes, int slotIndex) {
        return ByteBuffer.wrap(bytes, slotIndex * 4, 4).order(ByteOrder.BIG_ENDIAN).getInt();
    }

    private FileRead readRegionFile(
        RegionRoot root,
        RegionFileEntry entry,
        Limits limits,
        long remainingRegionBytes,
        Thread scanOwner
    ) throws IOException, CatalogException {
        if (System.getProperty("os.name", "").startsWith("Windows")) {
            return readRegionFileWindows(root, entry, limits, remainingRegionBytes, scanOwner);
        }
        return readRegionFileNio(root, entry, limits, remainingRegionBytes);
    }

    private FileRead readRegionFileNio(RegionRoot root, RegionFileEntry entry, Limits limits, long remainingRegionBytes) throws IOException, CatalogException {
        CatalogLocation location = CatalogLocation.region(root.identity(), entry.path(), entry.regionX(), entry.regionZ());
        FileObservation before = observeRegularFile(root, entry.path(), limits, location);
        long length = before.size();
        if (length < HEADER_BYTES) {
            throw failure("REGION_HEADER", location, "region file is shorter than the complete two-sector header", null);
        }
        if (length > limits.maxRegionFileBytes() || length > remainingRegionBytes || length > Integer.MAX_VALUE) {
            throw failure("REGION_FILE_LIMIT", location, "region file length exceeds the explicit per-file or remaining total byte bound", null);
        }
        byte[] content = new byte[Math.toIntExact(length)];
        try (FileChannel channel = FileChannel.open(entry.path(), StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            if (channel.size() != length) {
                throw failure("REGION_CHANGED", location, "region file size changed between attribute observation and open", null);
            }
            ByteBuffer target = ByteBuffer.wrap(content);
            long offset = 0L;
            while (target.hasRemaining()) {
                int read = channel.read(target, offset);
                if (read < 0) {
                    throw failure("REGION_CHANGED", location, "region file became shorter while being read", null);
                }
                if (read == 0) {
                    throw failure("REGION_READ_STALLED", location, "file channel made no progress during bounded read", null);
                }
                offset = checkedAdd(offset, read, location, "file read offset overflow");
            }
            fileSnapshotHook.afterFileRead(entry.path());
            byte[] initialHeader = Arrays.copyOf(content, HEADER_BYTES);
            byte[] openHandleHeader = readHeader(channel, location);
            if (!Arrays.equals(initialHeader, openHandleHeader) || channel.size() != length) {
                throw failure("REGION_CHANGED", location, "region header or size changed on the original read handle", null);
            }

            FileObservation reopenedBefore = observeRegularFile(root, entry.path(), limits, location);
            if (!before.sameObservation(reopenedBefore)) {
                throw failure("REGION_CHANGED", location, "region path identity, length, or timestamp changed before the second read", null);
            }
            try (FileChannel pathChannel = FileChannel.open(entry.path(), StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
                if (pathChannel.size() != length) {
                    throw failure("REGION_CHANGED", location, "region file size changed on the second read handle", null);
                }
                byte[] pathHeader = readHeader(pathChannel, location);
                if (!Arrays.equals(initialHeader, pathHeader) || pathChannel.size() != length) {
                    throw failure("REGION_CHANGED", location, "region header or size differs on the second path read", null);
                }
                FileObservation after = observeRegularFile(root, entry.path(), limits, location);
                if (!before.sameObservation(after) || channel.size() != length || pathChannel.size() != length) {
                    throw failure("REGION_CHANGED", location, "region file identity, length, or timestamp changed during read", null);
                }
                return new FileRead(content, before, after);
            }
        }
    }

    private static byte[] readHeader(FileChannel channel, CatalogLocation location) throws IOException, CatalogException {
        byte[] header = new byte[HEADER_BYTES];
        ByteBuffer target = ByteBuffer.wrap(header);
        long offset = 0L;
        while (target.hasRemaining()) {
            int read = channel.read(target, offset);
            if (read < 0) {
                throw failure("REGION_CHANGED", location, "region file became shorter while its header was re-read", null);
            }
            if (read == 0) {
                throw failure("REGION_READ_STALLED", location, "file channel made no progress while re-reading the region header", null);
            }
            offset += read;
        }
        return header;
    }

    private FileRead readRegionFileWindows(
        RegionRoot root,
        RegionFileEntry entry,
        Limits limits,
        long remainingRegionBytes,
        Thread scanOwner
    ) throws CatalogException {
        CatalogLocation location = CatalogLocation.region(root.identity(), entry.path(), entry.regionX(), entry.regionZ());
        if (readResponsibility == null) {
            throw failure("REGION_SESSION", location,
                "Windows native region reads require an explicitly attached caller-owned responsibility", null);
        }
        try {
            readResponsibility.requireAttachedReader(this, scanOwner);
        } catch (RuntimeException failure) {
            throw failure("REGION_SESSION", location, "the exact reader and owner thread are not registered", failure);
        }

        FileObservation before;
        try {
            before = observeWindowsPath(root, entry.path(), limits, location);
        } catch (CatalogException failure) {
            throw failure;
        } catch (IOException | RuntimeException failure) {
            throw failure("REGION_FILE_ATTRIBUTES", location, "could not observe the Windows region path without following links", failure);
        }
        long length = before.size();
        if (length < HEADER_BYTES) {
            throw failure("REGION_HEADER", location, "region file is shorter than the complete two-sector header", null);
        }
        if (length > limits.maxRegionFileBytes() || length > remainingRegionBytes || length > Integer.MAX_VALUE) {
            throw failure("REGION_FILE_LIMIT", location, "region file length exceeds the explicit per-file or remaining total byte bound", null);
        }

        RegionReadSession session;
        try {
            session = readResponsibility.register(this, scanOwner, UUID.randomUUID(), entry.path());
        } catch (RuntimeException failure) {
            throw failure("REGION_SESSION", location, "the single region resource slot is not available", failure);
        }
        Throwable primary = null;
        FileRead result = null;
        try {
            session.initializeWindowsIo();
            session.beginRead();
            WindowsMarkerIo io = session.initializedIo();
            WindowsMarkerIo.Handle h1 = io.openRegionReadHandle(entry.path(), false, session.attempt());
            WindowsMarkerIo.RegionObservation h1Before = io.observeRegion(h1);
            if (h1Before.size() != length) {
                throw failure("REGION_CHANGED", location, "the H1 native handle size differs from the path observation", null);
            }

            byte[] content = io.readRegionRangeExact(h1, 0L, Math.toIntExact(length));
            byte[] initialHeader = Arrays.copyOf(content, HEADER_BYTES);
            session.recordH1Initial(h1Before, initialHeader);
            fileSnapshotHook.afterFileRead(entry.path());

            byte[] h1Header = io.readRegionRangeExact(h1, 0L, HEADER_BYTES);
            WindowsMarkerIo.RegionObservation h1After = io.observeRegion(h1);
            session.recordH1Final(h1After, h1Header);
            if (!Arrays.equals(initialHeader, h1Header) || !h1Before.equals(h1After) || h1After.size() != length) {
                throw failure("REGION_CHANGED", location, "the H1 native handle identity, size, last-write time, or complete header changed", null);
            }

            WindowsMarkerIo.Handle h2 = io.openRegionReadHandle(entry.path(), true, session.attempt());
            WindowsMarkerIo.RegionObservation h2Before = io.observeRegion(h2);
            byte[] h2Header = io.readRegionRangeExact(h2, 0L, HEADER_BYTES);
            session.recordH2Initial(h2Before, h2Header);
            WindowsMarkerIo.RegionObservation h2After = io.observeRegion(h2);
            session.recordH2Final(h2After);
            FileObservation after = observeWindowsPath(root, entry.path(), limits, location);
            if (!h1Before.equals(h2Before) || !h2Before.equals(h2After) || !Arrays.equals(initialHeader, h2Header)
                || h2After.size() != length || !before.sameObservation(after)) {
                throw failure("REGION_CHANGED", location,
                    "the second native path handle identity/header or surrounding path attributes changed", null);
            }
            result = new FileRead(content, before, after);
        } catch (IOException | RuntimeException | Error failure) {
            primary = failure;
        }

        Throwable finalFailure = session.cleanupAfter(primary);
        if (finalFailure != null) {
            if (finalFailure instanceof CatalogException catalogFailure) throw catalogFailure;
            if (finalFailure instanceof Error error) throw error;
            throw failure("REGION_FILE_READ", location, "native region read or its single cleanup attempt failed", finalFailure);
        }
        if (result == null) {
            throw failure("REGION_FILE_READ", location, "native region read completed without a validated result", null);
        }
        return result;
    }

    private static FileObservation observeWindowsPath(
        RegionRoot root,
        Path path,
        Limits limits,
        CatalogLocation location
    ) throws IOException, CatalogException {
        rejectVisibleSymlinkComponents(path, root.identity());
        BasicFileAttributes attributes;
        try {
            attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException failure) {
            throw failure("REGION_FILE_ATTRIBUTES", location, "could not read Windows region path attributes without following links", failure);
        }
        if (!attributes.isRegularFile() || attributes.isSymbolicLink() || attributes.isOther()) {
            throw failure("PATH_IDENTITY", location, "region path is not an observed ordinary regular file", null);
        }
        if (attributes.size() < 0L || attributes.size() > limits.maxRegionFileBytes() || attributes.size() > Integer.MAX_VALUE) {
            throw failure("REGION_FILE_LIMIT", location, "region file length exceeds the explicit per-file bound", null);
        }
        return new FileObservation(attributes.size(), attributes.lastModifiedTime(), fileKey(attributes), true);
    }

    private static NBTTagCompound readNbt(byte[] decoded, Limits limits, CatalogLocation location) throws CatalogException {
        try {
            BoundedNbtStructurePreflight.validate(decoded, limits);
        } catch (BoundedNbtStructurePreflight.PreflightException preflightFailure) {
            throw failure("NBT_PRECHECK", location, "bounded NBT structure preflight rejected the decoded chunk", preflightFailure);
        } catch (RuntimeException preflightFailure) {
            throw failure("NBT_PRECHECK", location, "bounded NBT structure preflight failed", preflightFailure);
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(decoded))) {
            NBTTagCompound result = CompressedStreamTools.read(input, new NBTSizeTracker(limits.maxNbtTrackerBytes()));
            if (input.available() != 0) {
                throw failure("NBT_TRAILING_BYTES", location, "decoded chunk contains bytes after the root NBT compound", null);
            }
            return result;
        } catch (CatalogException failure) {
            throw failure;
        } catch (IOException | RuntimeException failure) {
            throw failure("NBT_PARSE", location, "bounded Cleanroom NBT parser rejected the decoded chunk", failure);
        }
    }

    private static byte[] decompress(byte[] compressed, int compressionId, long maximumBytes, CatalogLocation location) throws CatalogException {
        if (maximumBytes <= 0L || maximumBytes > Integer.MAX_VALUE) {
            throw failure("CATALOG_LIMIT", location, "decompression limit must be explicit and fit a bounded byte array", null);
        }
        ByteArrayOutputStream decoded = new ByteArrayOutputStream();
        try {
            if (compressionId == 1) {
                try (InputStream input = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
                    copyBounded(input, decoded, maximumBytes);
                }
            } else if (compressionId == 2) {
                inflateZlib(compressed, decoded, maximumBytes);
            } else {
                throw failure("REGION_COMPRESSION", location, "only patched compression ids 1 (gzip) and 2 (zlib) are supported", null);
            }
            return decoded.toByteArray();
        } catch (CatalogException failure) {
            throw failure;
        } catch (IOException | DataFormatException | RuntimeException failure) {
            throw failure("REGION_DECOMPRESS", location, "compressed chunk is malformed, truncated, overlong, or exceeds its decompression bound", failure);
        }
    }

    private static void copyBounded(InputStream input, ByteArrayOutputStream output, long maximumBytes) throws IOException {
        byte[] buffer = new byte[8192];
        long total = 0L;
        while (true) {
            int read = input.read(buffer);
            if (read < 0) {
                return;
            }
            if (read == 0) {
                continue;
            }
            total = Math.addExact(total, read);
            if (total > maximumBytes) {
                throw new IOException("decompressed chunk exceeds the explicit bound");
            }
            output.write(buffer, 0, read);
        }
    }

    private static void inflateZlib(byte[] compressed, ByteArrayOutputStream output, long maximumBytes) throws IOException, DataFormatException {
        Inflater inflater = new Inflater(false);
        try {
            inflater.setInput(compressed);
            byte[] buffer = new byte[8192];
            long total = 0L;
            while (!inflater.finished()) {
                int produced = inflater.inflate(buffer);
                if (produced > 0) {
                    total = Math.addExact(total, produced);
                    if (total > maximumBytes) {
                        throw new IOException("decompressed zlib chunk exceeds the explicit bound");
                    }
                    output.write(buffer, 0, produced);
                    continue;
                }
                if (inflater.needsDictionary()) {
                    throw new DataFormatException("zlib stream requires an unsupported preset dictionary");
                }
                if (inflater.needsInput()) {
                    throw new DataFormatException("zlib stream ended before its checksum and end marker");
                }
                throw new DataFormatException("zlib inflater made no progress");
            }
            if (inflater.getRemaining() != 0) {
                throw new DataFormatException("zlib payload has trailing bytes after its checksum");
            }
        } catch (ArithmeticException failure) {
            throw new IOException("decompressed byte count overflowed", failure);
        } finally {
            inflater.end();
        }
    }

    private static List<RegionFileEntry> listRegionFiles(RegionRoot root, long remainingFileCount) throws IOException, CatalogException {
        CatalogLocation location = CatalogLocation.root(root.identity(), root.directory());
        ArrayList<RegionFileEntry> files = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(root.directory())) {
            for (Path path : stream) {
                String name = path.getFileName().toString();
                if (!name.endsWith(".mca")) {
                    continue;
                }
                if (files.size() >= remainingFileCount) {
                    throw failure("CATALOG_LIMIT", CatalogLocation.file(root.identity(), path), "visible region files exceed the remaining configured file count", null);
                }
                Matcher matcher = REGION_NAME.matcher(name);
                if (!matcher.matches()) {
                    throw failure("REGION_FILENAME", CatalogLocation.file(root.identity(), path), "visible .mca filename is not an exact r.<x>.<z>.mca region name", null);
                }
                try {
                    int regionX = Integer.parseInt(matcher.group(1));
                    int regionZ = Integer.parseInt(matcher.group(2));
                    String canonicalName = "r." + regionX + "." + regionZ + ".mca";
                    if (!name.equals(canonicalName)) {
                        throw failure("REGION_FILENAME", CatalogLocation.file(root.identity(), path), "region filename uses a non-canonical coordinate spelling", null);
                    }
                    files.add(new RegionFileEntry(path.toAbsolutePath().normalize(), regionX, regionZ));
                } catch (NumberFormatException failure) {
                    throw failure("REGION_FILENAME", CatalogLocation.file(root.identity(), path), "region coordinate is outside the signed 32-bit range", failure);
                }
            }
        }
        files.sort(Comparator.comparingInt(RegionFileEntry::regionX).thenComparingInt(RegionFileEntry::regionZ).thenComparing(entry -> entry.path().getFileName().toString()));
        return List.copyOf(files);
    }

    private static void validateDistinctRootLocations(List<RegionRoot> roots) throws CatalogException {
        for (RegionRoot root : roots) {
            try {
                observeDirectory(root);
            } catch (CatalogException failure) {
                throw failure;
            } catch (IOException failure) {
                throw failure("PATH_ALIAS_OBSERVATION", CatalogLocation.root(root.identity(), root.directory()),
                    "could not observe supplied root before alias checks", failure);
            } catch (RuntimeException failure) {
                throw failure("PATH_ALIAS_OBSERVATION", CatalogLocation.root(root.identity(), root.directory()),
                    "runtime rejected supplied root observation", failure);
            }
        }
        for (int left = 0; left < roots.size(); left++) {
            for (int right = left + 1; right < roots.size(); right++) {
                RegionRoot first = roots.get(left);
                RegionRoot second = roots.get(right);
                try {
                    if (Files.isSameFile(first.directory(), second.directory())) {
                        throw failure("PATH_ALIAS", CatalogLocation.root(second.identity(), second.directory()),
                            "two supplied roots resolve to the same observed directory", null);
                    }
                } catch (CatalogException failure) {
                    throw failure;
                } catch (IOException failure) {
                    throw failure("PATH_ALIAS_OBSERVATION", CatalogLocation.root(second.identity(), second.directory()),
                        "could not compare supplied root locations", failure);
                } catch (RuntimeException failure) {
                    throw failure("PATH_ALIAS_OBSERVATION", CatalogLocation.root(second.identity(), second.directory()),
                        "runtime rejected supplied root comparison", failure);
                }
            }
        }
    }
    private static DirectorySnapshot observeDirectory(RegionRoot root) throws IOException, CatalogException {
        Path directory = root.directory();
        rejectVisibleSymlinkComponents(directory, root.identity());
        BasicFileAttributes attributes;
        try {
            attributes = Files.readAttributes(directory, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException failure) {
            throw failure("REGION_ROOT_ATTRIBUTES", CatalogLocation.root(root.identity(), directory), "could not read supplied region-root attributes without following links", failure);
        }
        if (!attributes.isDirectory() || attributes.isSymbolicLink() || attributes.isOther()) {
            throw failure("PATH_IDENTITY", CatalogLocation.root(root.identity(), directory), "supplied region root is not an observed ordinary directory", null);
        }
        return new DirectorySnapshot(attributes.size(), attributes.lastModifiedTime(), fileKey(attributes), attributes.isDirectory());
    }

    private static FileObservation observeRegularFile(RegionRoot root, Path path, Limits limits, CatalogLocation location) throws IOException, CatalogException {
        rejectVisibleSymlinkComponents(path, root.identity());
        BasicFileAttributes attributes;
        try {
            attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException failure) {
            throw failure("REGION_FILE_ATTRIBUTES", location, "could not read region-file attributes without following links", failure);
        }
        if (!attributes.isRegularFile() || attributes.isSymbolicLink() || attributes.isOther()) {
            throw failure("PATH_IDENTITY", location, "region path is not an observed ordinary regular file", null);
        }
        if (attributes.size() < 0L || attributes.size() > limits.maxRegionFileBytes() || attributes.size() > Integer.MAX_VALUE) {
            throw failure("REGION_FILE_LIMIT", location, "region file length exceeds the explicit per-file bound", null);
        }
        String observedFileKey = fileKey(attributes);
        if (observedFileKey == null) {
            throw failure("PATH_IDENTITY", location, "file system did not expose a stable region-file identity", null);
        }
        return new FileObservation(attributes.size(), attributes.lastModifiedTime(), observedFileKey, attributes.isRegularFile());
    }

    private static void rejectVisibleSymlinkComponents(Path absolutePath, String rootIdentity) throws CatalogException {
        if (!absolutePath.isAbsolute() || !absolutePath.equals(absolutePath.normalize())) {
            throw failure("PATH_IDENTITY", CatalogLocation.file(rootIdentity, absolutePath), "all paths must be absolute and lexically normalized", null);
        }
        Path current = absolutePath.getRoot();
        for (Path component : absolutePath) {
            current = current.resolve(component);
            if (Files.isSymbolicLink(current)) {
                throw failure("PATH_IDENTITY", CatalogLocation.file(rootIdentity, current), "a visible symbolic-link path component is not accepted", null);
            }
        }
    }

    private static String fileKey(BasicFileAttributes attributes) {
        Object key = attributes.fileKey();
        return key == null ? null : key.toString();
    }

    private static long addWithinLimit(long current, long addition, long maximum, CatalogLocation location, String message) throws CatalogException {
        long result = checkedAdd(current, addition, location, message);
        if (result > maximum) {
            throw failure("CATALOG_LIMIT", location, message, null);
        }
        return result;
    }

    private static long checkedAdd(long left, long right, CatalogLocation location, String message) throws CatalogException {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException failure) {
            throw failure("CATALOG_LIMIT", location, message, failure);
        }
    }

    private static int checkedChunkCoordinate(int region, int local, CatalogLocation location, int slotIndex) throws CatalogException {
        try {
            return Math.addExact(Math.multiplyExact(region, 32), local);
        } catch (ArithmeticException failure) {
            throw failure("REGION_COORDINATE", location.withSlot(slotIndex, null, null), "chunk coordinate overflows signed 32-bit region coordinates", failure);
        }
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte value : digest) {
                result.append(Character.forDigit((value >>> 4) & 0xF, 16));
                result.append(Character.forDigit(value & 0xF, 16));
            }
            return result.toString().toUpperCase(Locale.ROOT);
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("JVM is missing required SHA-256", failure);
        }
    }

    static CatalogException failure(String stage, CatalogLocation location, String detail, Throwable cause) {
        return new CatalogException(stage, location, detail, cause);
    }

    public enum Coverage {
        SUPPLIED_ROOTS_ONLY
    }

    public static final class Limits {
        private final int maxRoots;
        private final int maxFiles;
        private final int maxChunks;
        private final long maxRegionFileBytes;
        private final long maxTotalRegionBytes;
        private final long maxCompressedBytesPerChunk;
        private final long maxDecompressedBytesPerChunk;
        private final long maxNbtTrackerBytes;
        private final long maxNbtTags;
        private final int maxNbtDepth;
        private final int maxPassengerDepth;
        private final int maxEntityOccurrencesPerChunk;
        private final int maxTotalEntityOccurrences;
        private final long maxRetainedPayloadBytes;

        public Limits(
            int maxRoots,
            int maxFiles,
            int maxChunks,
            long maxRegionFileBytes,
            long maxTotalRegionBytes,
            long maxCompressedBytesPerChunk,
            long maxDecompressedBytesPerChunk,
            long maxNbtTrackerBytes,
            long maxNbtTags,
            int maxNbtDepth,
            int maxPassengerDepth,
            int maxEntityOccurrencesPerChunk,
            int maxTotalEntityOccurrences,
            long maxRetainedPayloadBytes
        ) {
            if (maxRoots < 1 || maxFiles < 1 || maxChunks < 1 || maxRegionFileBytes < HEADER_BYTES || maxRegionFileBytes > Integer.MAX_VALUE
                || maxTotalRegionBytes < HEADER_BYTES || maxCompressedBytesPerChunk < 1 || maxCompressedBytesPerChunk > Integer.MAX_VALUE
                || maxDecompressedBytesPerChunk < 1 || maxDecompressedBytesPerChunk > Integer.MAX_VALUE
                || maxNbtTrackerBytes < 1 || maxNbtTags < 1 || maxNbtDepth < 1 || maxNbtDepth > PATCHED_NBT_DEPTH_LIMIT || maxPassengerDepth < 0
                || maxEntityOccurrencesPerChunk < 1 || maxTotalEntityOccurrences < 1 || maxRetainedPayloadBytes < 1) {
                throw new IllegalArgumentException("all catalog limits must be explicit positive bounds within the patched reader limits");
            }
            this.maxRoots = maxRoots;
            this.maxFiles = maxFiles;
            this.maxChunks = maxChunks;
            this.maxRegionFileBytes = maxRegionFileBytes;
            this.maxTotalRegionBytes = maxTotalRegionBytes;
            this.maxCompressedBytesPerChunk = maxCompressedBytesPerChunk;
            this.maxDecompressedBytesPerChunk = maxDecompressedBytesPerChunk;
            this.maxNbtTrackerBytes = maxNbtTrackerBytes;
            this.maxNbtTags = maxNbtTags;
            this.maxNbtDepth = maxNbtDepth;
            this.maxPassengerDepth = maxPassengerDepth;
            this.maxEntityOccurrencesPerChunk = maxEntityOccurrencesPerChunk;
            this.maxTotalEntityOccurrences = maxTotalEntityOccurrences;
            this.maxRetainedPayloadBytes = maxRetainedPayloadBytes;
        }

        public int maxRoots() { return maxRoots; }
        public int maxFiles() { return maxFiles; }
        public int maxChunks() { return maxChunks; }
        public long maxRegionFileBytes() { return maxRegionFileBytes; }
        public long maxTotalRegionBytes() { return maxTotalRegionBytes; }
        public long maxCompressedBytesPerChunk() { return maxCompressedBytesPerChunk; }
        public long maxDecompressedBytesPerChunk() { return maxDecompressedBytesPerChunk; }
        public long maxNbtTrackerBytes() { return maxNbtTrackerBytes; }
        public long maxNbtTags() { return maxNbtTags; }
        public int maxNbtDepth() { return maxNbtDepth; }
        public int maxPassengerDepth() { return maxPassengerDepth; }
        public int maxEntityOccurrencesPerChunk() { return maxEntityOccurrencesPerChunk; }
        public int maxTotalEntityOccurrences() { return maxTotalEntityOccurrences; }
        public long maxRetainedPayloadBytes() { return maxRetainedPayloadBytes; }
    }

    public static final class RegionRoot {
        private final String identity;
        private final Path directory;
        private final OptionalInt dimensionId;

        public RegionRoot(String identity, Path absoluteDirectory, OptionalInt dimensionId) {
            if (identity == null || identity.isBlank() || absoluteDirectory == null || !absoluteDirectory.isAbsolute()
                || !absoluteDirectory.equals(absoluteDirectory.normalize()) || dimensionId == null) {
                throw new IllegalArgumentException("region root requires an identity, normalized absolute path, and explicit optional dimension");
            }
            this.identity = identity;
            this.directory = absoluteDirectory;
            this.dimensionId = dimensionId;
        }

        public String identity() { return identity; }
        public Path directory() { return directory; }
        public OptionalInt dimensionId() { return dimensionId; }
    }

    public static final class ManifestPin {
        private final String reference;
        private final long byteLength;
        private final byte[] sha256;

        public ManifestPin(String reference, long byteLength, byte[] sha256) {
            if (reference == null || reference.isBlank() || byteLength < 0L || sha256 == null || sha256.length != 32) {
                throw new IllegalArgumentException("manifest pin requires a reference, byte length, and exact SHA-256 bytes");
            }
            this.reference = reference;
            this.byteLength = byteLength;
            this.sha256 = sha256.clone();
        }

        public String reference() { return reference; }
        public long byteLength() { return byteLength; }
        public byte[] sha256() { return sha256.clone(); }
    }

    public static final class CatalogInput {
        private final String worldIdentity;
        private final UUID generation;
        private final Thread ownerThread;
        private final List<RegionRoot> roots;
        private final ManifestPin manifestPin;
        private final Limits limits;

        public CatalogInput(String worldIdentity, UUID generation, Thread ownerThread, List<RegionRoot> roots, ManifestPin manifestPin, Limits limits) {
            if (worldIdentity == null || worldIdentity.isBlank() || generation == null || ownerThread == null || roots == null || roots.isEmpty() || manifestPin == null || limits == null) {
                throw new IllegalArgumentException("world identity, generation, owner thread, at least one root, manifest pin, and limits are required");
            }
            ArrayList<RegionRoot> rootCopy = new ArrayList<>(roots.size());
            HashSet<String> identities = new HashSet<>();
            HashSet<Path> directories = new HashSet<>();
            for (RegionRoot root : roots) {
                if (root == null || !identities.add(root.identity()) || !directories.add(root.directory())) {
                    throw new IllegalArgumentException("supplied roots must be non-null and have unique identities and normalized directories");
                }
                rootCopy.add(root);
            }
            this.worldIdentity = worldIdentity;
            this.generation = generation;
            this.ownerThread = ownerThread;
            this.roots = List.copyOf(rootCopy);
            this.manifestPin = manifestPin;
            this.limits = limits;
        }

        public String worldIdentity() { return worldIdentity; }
        public UUID generation() { return generation; }
        public Thread ownerThread() { return ownerThread; }
        public List<RegionRoot> roots() { return roots; }
        public ManifestPin manifestPin() { return manifestPin; }
        public Limits limits() { return limits; }
    }

    @FunctionalInterface
    interface FileSnapshotHook {
        void afterFileRead(Path path) throws IOException;
    }

    public static final class CatalogSnapshot {
        private final String worldIdentity;
        private final UUID generation;
        private final ManifestPin manifestPin;
        private final List<RegionRootSnapshot> roots;
        private final List<RegionSnapshot> regions;
        private final List<EntityOccurrence> occurrences;
        private final long fileCount;
        private final long chunkCount;
        private final long regionBytes;
        private final long retainedPayloadBytes;

        private CatalogSnapshot(String worldIdentity, UUID generation, ManifestPin manifestPin, List<RegionRootSnapshot> roots,
                                List<RegionSnapshot> regions, List<EntityOccurrence> occurrences, long fileCount, long chunkCount,
                                long regionBytes, long retainedPayloadBytes) {
            this.worldIdentity = worldIdentity;
            this.generation = generation;
            this.manifestPin = manifestPin;
            this.roots = List.copyOf(roots);
            this.regions = List.copyOf(regions);
            this.occurrences = List.copyOf(occurrences);
            this.fileCount = fileCount;
            this.chunkCount = chunkCount;
            this.regionBytes = regionBytes;
            this.retainedPayloadBytes = retainedPayloadBytes;
        }

        public String worldIdentity() { return worldIdentity; }
        public UUID generation() { return generation; }
        public ManifestPin manifestPin() { return manifestPin; }
        public Coverage coverage() { return Coverage.SUPPLIED_ROOTS_ONLY; }
        public List<RegionRootSnapshot> roots() { return roots; }
        public List<RegionSnapshot> regions() { return regions; }
        public List<EntityOccurrence> occurrences() { return occurrences; }
        public long fileCount() { return fileCount; }
        public long chunkCount() { return chunkCount; }
        public long regionBytes() { return regionBytes; }
        public long retainedPayloadBytes() { return retainedPayloadBytes; }
    }

    public static final class RegionRootSnapshot {
        private final RegionRoot root;
        private final DirectorySnapshot before;
        private final DirectorySnapshot after;
        private final int visibleRegionFiles;

        private RegionRootSnapshot(RegionRoot root, DirectorySnapshot before, DirectorySnapshot after, int visibleRegionFiles) {
            this.root = root;
            this.before = before;
            this.after = after;
            this.visibleRegionFiles = visibleRegionFiles;
        }

        public RegionRoot root() { return root; }
        public int visibleRegionFiles() { return visibleRegionFiles; }
        public boolean rootObservationStableDuringScan() { return before.sameObservation(after); }
    }

    public static final class RegionSnapshot {
        private final String rootIdentity;
        private final Path path;
        private final int regionX;
        private final int regionZ;
        private final OptionalInt dimensionId;
        private final FileObservation before;
        private final FileObservation after;
        private final long byteLength;
        private final String sha256;
        private final List<SlotSnapshot> slots;
        private final int presentSlotCount;

        private RegionSnapshot(String rootIdentity, Path path, int regionX, int regionZ, OptionalInt dimensionId,
                               FileObservation before, FileObservation after, long byteLength, String sha256, List<SlotSnapshot> slots) {
            this.rootIdentity = rootIdentity;
            this.path = path;
            this.regionX = regionX;
            this.regionZ = regionZ;
            this.dimensionId = dimensionId;
            this.before = before;
            this.after = after;
            this.byteLength = byteLength;
            this.sha256 = sha256;
            this.slots = List.copyOf(slots);
            this.presentSlotCount = Math.toIntExact(slots.stream().filter(SlotSnapshot::present).count());
        }

        public String rootIdentity() { return rootIdentity; }
        public Path path() { return path; }
        public int regionX() { return regionX; }
        public int regionZ() { return regionZ; }
        public OptionalInt dimensionId() { return dimensionId; }
        public long byteLength() { return byteLength; }
        public String sha256() { return sha256; }
        public List<SlotSnapshot> slots() { return slots; }
        public int presentSlotCount() { return presentSlotCount; }
        public int emptySlotCount() { return SLOT_COUNT - presentSlotCount; }
        public boolean fileObservationStableDuringRead() { return before != null && before.sameObservation(after); }

        public SlotSnapshot slot(int localX, int localZ) {
            if (localX < 0 || localX >= 32 || localZ < 0 || localZ >= 32) {
                throw new IndexOutOfBoundsException("local chunk coordinates must be in [0, 31]");
            }
            return slots.get(localZ * 32 + localX);
        }
    }

    public static final class SlotSnapshot {
        private final String rootIdentity;
        private final Path regionPath;
        private final int regionX;
        private final int regionZ;
        private final int slotIndex;
        private final int localX;
        private final int localZ;
        private final int chunkX;
        private final int chunkZ;
        private final OptionalInt dimensionId;
        private final boolean present;
        private final int sectorOffset;
        private final int spanSectors;
        private final int compressionId;
        private final String compressedSha256;
        private final byte[] compressedBytes;
        private final byte[] decodedRootBytes;

        private SlotSnapshot(String rootIdentity, Path regionPath, int regionX, int regionZ, int slotIndex, int localX, int localZ,
                             int chunkX, int chunkZ, OptionalInt dimensionId, boolean present, int sectorOffset, int spanSectors,
                             int compressionId, String compressedSha256, byte[] compressedBytes, byte[] decodedRootBytes) {
            this.rootIdentity = rootIdentity;
            this.regionPath = regionPath;
            this.regionX = regionX;
            this.regionZ = regionZ;
            this.slotIndex = slotIndex;
            this.localX = localX;
            this.localZ = localZ;
            this.chunkX = chunkX;
            this.chunkZ = chunkZ;
            this.dimensionId = dimensionId;
            this.present = present;
            this.sectorOffset = sectorOffset;
            this.spanSectors = spanSectors;
            this.compressionId = compressionId;
            this.compressedSha256 = compressedSha256;
            this.compressedBytes = compressedBytes.clone();
            this.decodedRootBytes = decodedRootBytes.clone();
        }

        private static SlotSnapshot empty(String rootIdentity, Path path, int regionX, int regionZ, int slotIndex, int localX, int localZ,
                                          int chunkX, int chunkZ, OptionalInt dimensionId) {
            return new SlotSnapshot(rootIdentity, path, regionX, regionZ, slotIndex, localX, localZ, chunkX, chunkZ,
                dimensionId, false, 0, 0, 0, "", new byte[0], new byte[0]);
        }

        private static SlotSnapshot present(String rootIdentity, Path path, int regionX, int regionZ, int slotIndex, int localX, int localZ,
                                            int chunkX, int chunkZ, OptionalInt dimensionId, int sectorOffset, int spanSectors,
                                            int compressionId, String compressedSha256, byte[] compressed, byte[] decoded) {
            return new SlotSnapshot(rootIdentity, path, regionX, regionZ, slotIndex, localX, localZ, chunkX, chunkZ,
                dimensionId, true, sectorOffset, spanSectors, compressionId, compressedSha256, compressed, decoded);
        }

        public String rootIdentity() { return rootIdentity; }
        public Path regionPath() { return regionPath; }
        public int regionX() { return regionX; }
        public int regionZ() { return regionZ; }
        public int slotIndex() { return slotIndex; }
        public int localX() { return localX; }
        public int localZ() { return localZ; }
        public int chunkX() { return chunkX; }
        public int chunkZ() { return chunkZ; }
        public OptionalInt dimensionId() { return dimensionId; }
        public boolean present() { return present; }
        public int sectorOffset() { return sectorOffset; }
        public int spanSectors() { return spanSectors; }
        public int compressionId() { return compressionId; }
        public String compressedSha256() { return compressedSha256; }
        public byte[] compressedBytes() { return compressedBytes.clone(); }
        public byte[] decodedRootBytes() { return decodedRootBytes.clone(); }

        /** Retains exact typed rollback data beyond the parser workspace without exposing its backing array. */
        public RetainedUndoSlot retainUndoSlot(SourceForestAllocationBudget.Account exactAccount) throws IOException {
            Objects.requireNonNull(exactAccount, "exact original retained source account");
            long charge = Math.addExact(256L, Math.addExact(compressedBytes.length, decodedRootBytes.length));
            exactAccount.reserveTemporary(charge, "retained exact before-image Region slot compressed undo and decoded NBT DATA BEFORE allocation");
            try {
                return new RetainedUndoSlot(exactAccount, slotIndex, present, sectorOffset, spanSectors,
                    compressionId, compressedSha256, compressedBytes.clone(), decodedRootBytes.clone(), charge);
            } catch (RuntimeException | Error cause) {
                try { exactAccount.releaseTemporary(charge, "failed exact before-image Region slot compressed/decoded DATA dropped"); }
                catch (Throwable cleanup) { ManagedSourceSaveLedger.suppressSecondary(cause, cleanup); }
                throw cause;
            }
        }
    }

    /** Immutable compressed undo and decoded-root DATA retained after parser cleanup under the original source account. */
    public static final class RetainedUndoSlot implements AutoCloseable {
        private final SourceForestAllocationBudget.Account allocation;
        private final int slotIndex;
        private final boolean present;
        private final int sectorOffset, spanSectors, compressionId;
        private final String compressedSha256;
        private final String decodedRootSha256;
        private final long chargedBytes;
        private byte[] compressedBytes;
        private byte[] decodedRootBytes;
        private boolean closed;
        private Throwable cleanupFailure;

        private RetainedUndoSlot(SourceForestAllocationBudget.Account allocation, int slotIndex, boolean present,
                int sectorOffset, int spanSectors, int compressionId, String compressedSha256,
                byte[] compressedBytes, byte[] decodedRootBytes, long chargedBytes) {
            this.allocation = Objects.requireNonNull(allocation, "exact original retained source account");
            if (slotIndex < 0 || slotIndex >= SLOT_COUNT || compressedBytes == null || decodedRootBytes == null || compressedSha256 == null
                || chargedBytes < 0L) {
                throw new IllegalArgumentException("Exact retained Region slot undo DATA is required");
            }
            if (present) {
                if (sectorOffset < 2 || spanSectors < 1 || (compressionId != 1 && compressionId != 2)
                    || compressedBytes.length < 1 || decodedRootBytes.length < 1 || !compressedSha256.matches("[0-9A-F]{64}")) {
                    throw new IllegalArgumentException("Present slot requires its exact compressed FULL_CHUNK body, decoded root, and descriptor");
                }
            } else if (sectorOffset != 0 || spanSectors != 0 || compressionId != 0
                || compressedBytes.length != 0 || decodedRootBytes.length != 0 || !compressedSha256.isEmpty()) {
                throw new IllegalArgumentException("Absent slot cannot carry a compressed body or decoded FULL_CHUNK root");
            }
            long expectedCharge = Math.addExact(256L, Math.addExact(compressedBytes.length, decodedRootBytes.length));
            if (chargedBytes != expectedCharge)
                throw new IllegalArgumentException("Retained slot DATA charge differs from its exact body sizes");
            if (present && !sha256Hex(compressedBytes).equals(compressedSha256))
                throw new IllegalArgumentException("Retained compressed FULL_CHUNK bytes differ from their original digest");
            this.slotIndex = slotIndex;
            this.present = present;
            this.sectorOffset = sectorOffset;
            this.spanSectors = spanSectors;
            this.compressionId = compressionId;
            this.compressedSha256 = compressedSha256;
            this.decodedRootSha256 = sha256Hex(decodedRootBytes);
            this.compressedBytes = compressedBytes;
            this.decodedRootBytes = decodedRootBytes;
            this.chargedBytes = chargedBytes;
        }

        public int slotIndex() { requireOpen(); return slotIndex; }
        public boolean present() { requireOpen(); return present; }
        public int sectorOffset() { requireOpen(); return sectorOffset; }
        public int spanSectors() { requireOpen(); return spanSectors; }
        public int compressionId() { requireOpen(); return compressionId; }
        public String compressedSha256() { requireOpen(); return compressedSha256; }
        public int compressedByteLength() { requireOpen(); return compressedBytes.length; }
        public String decodedRootSha256() { requireOpen(); return decodedRootSha256; }
        public int decodedRootByteLength() { requireOpen(); return decodedRootBytes.length; }
        /** Repeatedly writes only the original compressed body (without MCA length/tag/padding); never closes the destination. */
        public void writeCompressedBytesTo(DataOutput destination) throws IOException {
            requireOpen();
            if (!present) throw new IllegalStateException("SLOT_ABSENT has no compressed FULL_CHUNK body");
            writeOwnedBytesTo(Objects.requireNonNull(destination, "borrowed undo DATA destination"), compressedBytes);
        }
        /** Repeatedly writes the exact original uncompressed root compound bytes; never closes the destination. */
        public void writeDecodedRootBytesTo(DataOutput destination) throws IOException {
            requireOpen();
            if (!present) throw new IllegalStateException("SLOT_ABSENT has no decoded FULL_CHUNK root");
            writeOwnedBytesTo(Objects.requireNonNull(destination, "borrowed decoded-root DATA destination"), decodedRootBytes);
        }
        @Override public void close() throws IOException {
            requireOpen();
            compressedBytes = null;
            decodedRootBytes = null;
            closed = true;
            try {
                allocation.releaseTemporary(chargedBytes, "exact before-image Region slot compressed/decoded DATA dropped before refund");
            } catch (IOException | RuntimeException | Error cause) {
                cleanupFailure = cause;
                throw cause;
            }
        }
        private void requireOpen() { if (closed) throw new IllegalStateException("Exact retained before-image Region slot undo DATA is closed", cleanupFailure); }
    }

    /** Parsed DATA from an already pinned full-file readback; not an observation of filesystem identity. */
    public static final class RegionReadback implements AutoCloseable {
        private final long byteLength;
        private final String sha256;
        private final int regionX;
        private final int regionZ;
        private final int targetSlotIndex;
        private List<SlotSnapshot> slots;
        private List<EntityOccurrence> occurrences;
        private final long retainedPayloadBytes;
        private final SourceForestAllocationBudget.Scope workspace;
        private final Thread owner = Thread.currentThread();
        private boolean closed;
        private Throwable cleanupFailure;

        private RegionReadback(long byteLength, String sha256, int regionX, int regionZ, int targetSlotIndex, List<SlotSnapshot> slots,
                               List<EntityOccurrence> occurrences, long retainedPayloadBytes, SourceForestAllocationBudget.Scope workspace) {
            this.byteLength = byteLength;
            this.sha256 = sha256;
            this.regionX = regionX;
            this.regionZ = regionZ;
            this.targetSlotIndex = targetSlotIndex;
            this.slots = List.copyOf(slots);
            this.occurrences = List.copyOf(occurrences);
            this.retainedPayloadBytes = retainedPayloadBytes;
            this.workspace = workspace;
        }

        private void requireOpen() { if (closed || Thread.currentThread() != owner) throw new IllegalStateException("Exact Region readback owner scope required", cleanupFailure); }
        public long byteLength() { return byteLength; }
        public String sha256() { return sha256; }
        public int regionX() { return regionX; }
        public int regionZ() { return regionZ; }
        public int targetSlotIndex() { return targetSlotIndex; }
        public List<SlotSnapshot> slots() { requireOpen(); return slots; }
        public SlotSnapshot slot(int index) {
            requireOpen();
            if (index < 0 || index >= SLOT_COUNT) throw new IndexOutOfBoundsException("slot index must be in [0, 1023]");
            return slots.get(index);
        }
        public SlotSnapshot targetSlot() { return slot(targetSlotIndex); }
        public int presentSlotCount() { requireOpen(); return (int) slots.stream().filter(SlotSnapshot::present).count(); }
        public List<EntityOccurrence> occurrences() { requireOpen(); return occurrences; }
        public long retainedPayloadBytes() { return retainedPayloadBytes; }
        @Override public void close() {
            requireOpen();
            slots = null;
            occurrences = null;
            closed = true;
            try { workspace.close(); }
            catch (RuntimeException | Error cause) { cleanupFailure = cause; throw cause; }
        }
    }

    /** Complete structural evidence and the only decoded slot, scoped to the original source allocation. */
    public static final class SourceTargetReadback implements AutoCloseable {
        private final String rootIdentity;
        private final Path regionPath;
        private final int regionX;
        private final int regionZ;
        private final OptionalInt dimensionId;
        private final long byteLength;
        private String sha256;
        private String headerSha256;
        private final int targetSlotIndex;
        private RegionHeaderSlot[] headerSlots;
        private int[] recordLengths;
        private int[] resolvedSpans;
        private int[] compressionIds;
        private int[] extentLengths;
        private int[] extentHashOffsets;
        private byte[] extentHashes;
        private byte[] targetCompressed;
        private byte[] targetDecoded;
        private long targetCompressedCharge;
        private long targetDecodedCharge;
        private String targetCompressedSha256;
        private String targetDecodedSha256;
        private BoundedNbtStructurePreflight.Node typedRoot;
        private final SourceForestAllocationBudget.Scope workspace;
        private final SourceForestAllocationBudget.Account allocation;
        private final Thread owner = Thread.currentThread();
        private boolean closed;
        private Throwable cleanupFailure;

        private SourceTargetReadback(String rootIdentity, Path regionPath, int regionX, int regionZ, OptionalInt dimensionId,
                long byteLength, String sha256, String headerSha256, int targetSlotIndex, RegionHeaderSlot[] headerSlots,
                int[] recordLengths, int[] resolvedSpans, int[] compressionIds, int[] extentLengths,
                int[] extentHashOffsets, byte[] extentHashes, byte[] targetCompressed, byte[] targetDecoded,
                long targetCompressedCharge, long targetDecodedCharge, String targetCompressedSha256,
                String targetDecodedSha256, BoundedNbtStructurePreflight.Node typedRoot,
                SourceForestAllocationBudget.Scope workspace, SourceForestAllocationBudget.Account allocation) {
            this.rootIdentity = rootIdentity;
            this.regionPath = regionPath;
            this.regionX = regionX;
            this.regionZ = regionZ;
            this.dimensionId = dimensionId;
            this.byteLength = byteLength;
            this.sha256 = sha256;
            this.headerSha256 = headerSha256;
            this.targetSlotIndex = targetSlotIndex;
            this.headerSlots = headerSlots;
            this.recordLengths = recordLengths;
            this.resolvedSpans = resolvedSpans;
            this.compressionIds = compressionIds;
            this.extentLengths = extentLengths;
            this.extentHashOffsets = extentHashOffsets;
            this.extentHashes = extentHashes;
            this.targetCompressed = targetCompressed;
            this.targetDecoded = targetDecoded;
            this.targetCompressedCharge = targetCompressedCharge;
            this.targetDecodedCharge = targetDecodedCharge;
            this.targetCompressedSha256 = targetCompressedSha256;
            this.targetDecodedSha256 = targetDecodedSha256;
            this.typedRoot = typedRoot;
            this.workspace = workspace;
            this.allocation = allocation;
        }

        private void requireOpen() {
            if (closed || Thread.currentThread() != owner) {
                throw new IllegalStateException("Open original-owner source target readback required", cleanupFailure);
            }
        }

        private static void requireSlotIndex(int slotIndex) {
            if (slotIndex < 0 || slotIndex >= SLOT_COUNT) throw new IndexOutOfBoundsException("slot index must be in [0, 1023]");
        }

        public String rootIdentity() { requireOpen(); return rootIdentity; }
        public Path regionPath() { requireOpen(); return regionPath; }
        public int regionX() { requireOpen(); return regionX; }
        public int regionZ() { requireOpen(); return regionZ; }
        public OptionalInt dimensionId() { requireOpen(); return dimensionId; }
        public long byteLength() { requireOpen(); return byteLength; }
        public String sha256() { requireOpen(); return sha256; }
        public String headerSha256() { requireOpen(); return headerSha256; }
        public int targetSlotIndex() { requireOpen(); return targetSlotIndex; }
        public boolean targetPresent() { requireOpen(); return headerSlots[targetSlotIndex].present(); }
        public int slotCount() { requireOpen(); return SLOT_COUNT; }

        public RegionHeaderSlot headerSlot(int slotIndex) {
            requireOpen();
            requireSlotIndex(slotIndex);
            return headerSlots[slotIndex];
        }

        public int recordLength(int slotIndex) {
            requireOpen();
            requireSlotIndex(slotIndex);
            return recordLengths[slotIndex];
        }

        public int resolvedSpanSectors(int slotIndex) {
            requireOpen();
            requireSlotIndex(slotIndex);
            return resolvedSpans[slotIndex];
        }

        public int compressionId(int slotIndex) {
            requireOpen();
            requireSlotIndex(slotIndex);
            return compressionIds[slotIndex];
        }

        public int extentLength(int slotIndex) {
            requireOpen();
            requireSlotIndex(slotIndex);
            return extentLengths[slotIndex];
        }

        /** Copies an already computed full allocated-extent digest into caller-precharged storage. */
        public void copyExtentSha256To(int slotIndex, byte[] destination, int destinationOffset) {
            requireOpen();
            requireSlotIndex(slotIndex);
            Objects.requireNonNull(destination, "caller-precharged extent digest destination");
            int digestOffset = extentHashOffsets[slotIndex];
            if (digestOffset < 0) throw new IllegalStateException("Absent source slot has no allocated extent digest");
            Objects.checkFromIndexSize(destinationOffset, 32, destination.length);
            System.arraycopy(extentHashes, digestOffset, destination, destinationOffset, 32);
        }

        /** Compares every non-target header word/timestamp/resolved record and full allocated extent hash. */
        public boolean sameNonTargetLayoutAndExtents(SourceTargetReadback other) {
            requireOpen();
            Objects.requireNonNull(other, "complete source target after-readback").requireOpen();
            if (!rootIdentity.equals(other.rootIdentity) || !regionPath.equals(other.regionPath)
                || regionX != other.regionX || regionZ != other.regionZ || !dimensionId.equals(other.dimensionId)
                || targetSlotIndex != other.targetSlotIndex) return false;
            for (int slotIndex = 0; slotIndex < SLOT_COUNT; slotIndex++) {
                if (slotIndex == targetSlotIndex) continue;
                RegionHeaderSlot before = headerSlots[slotIndex];
                RegionHeaderSlot after = other.headerSlots[slotIndex];
                if (!before.equals(after) || recordLengths[slotIndex] != other.recordLengths[slotIndex]
                    || resolvedSpans[slotIndex] != other.resolvedSpans[slotIndex]
                    || compressionIds[slotIndex] != other.compressionIds[slotIndex]
                    || extentLengths[slotIndex] != other.extentLengths[slotIndex]) return false;
                if (!before.present()) continue;
                int beforeDigest = extentHashOffsets[slotIndex];
                int afterDigest = other.extentHashOffsets[slotIndex];
                if (beforeDigest < 0 || afterDigest < 0) return false;
                for (int byteIndex = 0; byteIndex < 32; byteIndex++) {
                    if (extentHashes[beforeDigest + byteIndex] != other.extentHashes[afterDigest + byteIndex]) return false;
                }
            }
            return true;
        }

        public int targetCompressionId() { requireOpen(); requireTargetPresent(); return compressionIds[targetSlotIndex]; }
        public int targetRecordLength() { requireOpen(); return recordLengths[targetSlotIndex]; }
        public int targetResolvedSpanSectors() { requireOpen(); return resolvedSpans[targetSlotIndex]; }
        public int targetCompressedByteLength() { requireOpen(); requireTargetPresent(); return targetCompressed.length; }
        public String targetCompressedSha256() { requireOpen(); requireTargetPresent(); return targetCompressedSha256; }
        public int targetDecodedRootByteLength() { requireOpen(); requireTargetPresent(); return targetDecoded.length; }
        public String targetDecodedRootSha256() { requireOpen(); requireTargetPresent(); return targetDecodedSha256; }

        /** Writes only the original compressed body; it does not close or flush the caller's destination. */
        public void writeTargetCompressedBytesTo(DataOutput destination) throws IOException {
            requireOpen();
            requireTargetPresent();
            writeOwnedBytesTo(Objects.requireNonNull(destination, "borrowed target compressed DATA destination"), targetCompressed);
        }

        /** Writes the exact complete uncompressed root compound; it does not close or flush the caller's destination. */
        public void writeTargetDecodedRootBytesTo(DataOutput destination) throws IOException {
            requireOpen();
            requireTargetPresent();
            writeOwnedBytesTo(Objects.requireNonNull(destination, "borrowed target decoded-root DATA destination"), targetDecoded);
        }

        /** Retains exact rollback bytes under the original account before either clone is allocated. */
        public RetainedUndoSlot retainTargetUndoSlot() throws IOException {
            requireOpen();
            requireTargetPresent();
            long charge = Math.addExact(256L, Math.addExact(targetCompressed.length, targetDecoded.length));
            allocation.reserveTemporary(charge, "retained exact source target compressed undo and decoded root BEFORE allocation");
            try {
                return new RetainedUndoSlot(allocation, targetSlotIndex, true, headerSlots[targetSlotIndex].sectorOffset(),
                    resolvedSpans[targetSlotIndex], compressionIds[targetSlotIndex], targetCompressedSha256,
                    targetCompressed.clone(), targetDecoded.clone(), charge);
            } catch (RuntimeException | Error cause) {
                try { allocation.releaseTemporary(charge, "failed retained source target body and decoded root dropped"); }
                catch (Throwable cleanup) { suppressCleanup(cause, cleanup); }
                throw cause;
            }
        }

        /** A close-guarded typed facade; it never returns the preflight Node or its backing tree. */
        public SourceTargetNode typedRoot() {
            requireOpen();
            requireTargetPresent();
            workspace.charge(32L, "source target typed-root scoped facade BEFORE allocation");
            return new SourceTargetNode(this, null, -1);
        }

        private void requireTargetPresent() {
            if (!headerSlots[targetSlotIndex].present()) throw new IllegalStateException("SLOT_ABSENT source target has no compressed or decoded root DATA");
        }

        @Override public void close() throws IOException {
            requireOpen();
            targetCompressed = null;
            targetDecoded = null;
            targetCompressedSha256 = null;
            targetDecodedSha256 = null;
            typedRoot = null;
            headerSlots = null;
            recordLengths = null;
            resolvedSpans = null;
            compressionIds = null;
            extentLengths = null;
            extentHashOffsets = null;
            extentHashes = null;
            sha256 = null;
            headerSha256 = null;
            closed = true;

            Throwable cleanup = null;
            if (targetCompressedCharge > 0L) {
                try {
                    allocation.releaseTemporary(targetCompressedCharge, "source target compressed body dropped before refund");
                    targetCompressedCharge = 0L;
                } catch (IOException | RuntimeException | Error cause) { cleanup = cause; }
            }
            if (targetDecodedCharge > 0L) {
                try {
                    allocation.releaseTemporary(targetDecodedCharge, "source target decoded root dropped before refund");
                    targetDecodedCharge = 0L;
                } catch (IOException | RuntimeException | Error cause) {
                    if (cleanup == null) cleanup = cause;
                    else suppressCleanup(cleanup, cause);
                }
            }
            try { workspace.close(); }
            catch (RuntimeException | Error cause) {
                if (cleanup == null) cleanup = cause;
                else suppressCleanup(cleanup, cause);
            }
            if (cleanup != null) {
                cleanupFailure = cleanup;
                if (cleanup instanceof IOException cause) throw cause;
                if (cleanup instanceof RuntimeException cause) throw cause;
                if (cleanup instanceof Error cause) throw cause;
                throw new IOException("Source target readback close failed", cleanup);
            }
        }
    }

    /** A typed-node handle that resolves through its owning readback on every access and becomes unusable on close. */
    public static final class SourceTargetNode {
        private final SourceTargetReadback readback;
        private final SourceTargetNode parent;
        private final int childIndex;

        private SourceTargetNode(SourceTargetReadback readback, SourceTargetNode parent, int childIndex) {
            this.readback = readback;
            this.parent = parent;
            this.childIndex = childIndex;
        }

        private BoundedNbtStructurePreflight.Node resolve() {
            readback.requireOpen();
            if (parent == null) return readback.typedRoot;
            return parent.resolve().children().get(childIndex);
        }

        public int type() { return resolve().type(); }
        public String name() { return resolve().name(); }
        public int childCount() { return resolve().children().size(); }
        public boolean hasNamedHeader() { return resolve().hasNamedHeader(); }
        public int originalHeaderLength() { return resolve().originalHeaderLength(); }
        public int originalPayloadLength() { return resolve().originalPayloadLength(); }
        public int listElementType() { return resolve().listElementType(); }
        public void writeOriginalHeaderTo(DataOutput output) throws IOException { resolve().writeOriginalHeaderTo(output); }
        public void writeOriginalPayloadTo(DataOutput output) throws IOException { resolve().writeOriginalPayloadTo(output); }

        public SourceTargetNode child(int index) {
            BoundedNbtStructurePreflight.Node node = resolve();
            if (index < 0 || index >= node.children().size()) throw new IndexOutOfBoundsException("NBT child index outside the exact typed node");
            readback.workspace.charge(32L, "source target typed child scoped facade BEFORE allocation");
            return new SourceTargetNode(readback, this, index);
        }

        public SourceTargetNode child(String exactName) {
            Objects.requireNonNull(exactName, "exact NBT child name");
            BoundedNbtStructurePreflight.Node node = resolve();
            for (int index = 0; index < node.children().size(); index++) {
                if (node.children().get(index).name().equals(exactName)) return child(index);
            }
            return null;
        }

        public List<SourceTargetNode> children() {
            readback.requireOpen();
            readback.workspace.charge(64L, "source target typed child-list facade BEFORE allocation");
            return new SourceTargetNodeList(this);
        }

        public long integerBits() { return resolve().integerBits(); }
        public int intBits() { return resolve().intBits(); }
        /** If retained past readback close, the caller must separately reserve its source-account lifetime first. */
        public String stringValue() throws IOException { return resolve().stringValue(); }
        /** If retained past readback close, the caller must separately reserve its source-account lifetime first. */
        public byte[] payloadBytes() { return resolve().payloadBytes(); }
        public boolean matchesStandaloneCompound(byte[] standalone) { return resolve().matchesStandaloneCompound(standalone); }
    }

    private static final class SourceTargetNodeList extends AbstractList<SourceTargetNode> {
        private final SourceTargetNode parent;
        private SourceTargetNodeList(SourceTargetNode parent) { this.parent = parent; }
        @Override public SourceTargetNode get(int index) { return parent.child(index); }
        @Override public int size() { return parent.childCount(); }
    }

    public record RegionHeaderSlot(int slotIndex, boolean present, int sectorOffset, int declaredSectorCount, int timestamp) {
        public RegionHeaderSlot {
            if (slotIndex < 0 || slotIndex >= SLOT_COUNT || sectorOffset < 0 || declaredSectorCount < 0
                || (present && (sectorOffset < 2 || declaredSectorCount == 0))
                || (!present && (sectorOffset != 0 || declaredSectorCount != 0)) ) {
                throw new IllegalArgumentException("Region header slot fields are inconsistent");
            }
        }
    }

    /** Parsed target header DATA from the exact native pin; the full 1024-entry header was checked before return. */
    public static final class RegionHeaderSnapshot implements AutoCloseable {
        private final long byteLength;
        private final int regionX;
        private final int regionZ;
        private final String headerSha256;
        private final int targetSlotIndex;
        private List<RegionHeaderSlot> slots;
        private final SourceForestAllocationBudget.Scope workspace;
        private final Thread owner = Thread.currentThread();
        private boolean closed;

        private RegionHeaderSnapshot(long byteLength, int regionX, int regionZ, String headerSha256,
                                     int targetSlotIndex, List<RegionHeaderSlot> slots, SourceForestAllocationBudget.Scope workspace) {
            this.byteLength = byteLength;
            this.regionX = regionX;
            this.regionZ = regionZ;
            this.headerSha256 = headerSha256;
            this.targetSlotIndex = targetSlotIndex;
            this.slots = List.copyOf(slots);
            this.workspace = workspace;
        }

        private void requireOpen() { if (closed || Thread.currentThread() != owner) throw new IllegalStateException("Exact Region header owner scope required"); }
        public long byteLength() { return byteLength; }
        public int regionX() { return regionX; }
        public int regionZ() { return regionZ; }
        public String headerSha256() { return headerSha256; }
        public List<RegionHeaderSlot> slots() { requireOpen(); return slots; }
        public RegionHeaderSlot slot(int index) {
            requireOpen();
            if (index < 0 || index >= SLOT_COUNT) throw new IndexOutOfBoundsException("slot index must be in [0, 1023]");
            return slots.get(index);
        }
        public RegionHeaderSlot targetSlot() { return slot(targetSlotIndex); }
        @Override public void close() { requireOpen(); workspace.close(); closed=true; slots=List.of(); }
    }

    public static final class ChunkSource {
        private final String rootIdentity;
        private final Path regionPath;
        private final int regionX;
        private final int regionZ;
        private final int slotIndex;
        private final int localX;
        private final int localZ;
        private final int chunkX;
        private final int chunkZ;
        private final OptionalInt dimensionId;

        private ChunkSource(String rootIdentity, Path regionPath, int regionX, int regionZ, int slotIndex, int localX, int localZ,
                            int chunkX, int chunkZ, OptionalInt dimensionId) {
            this.rootIdentity = rootIdentity;
            this.regionPath = regionPath;
            this.regionX = regionX;
            this.regionZ = regionZ;
            this.slotIndex = slotIndex;
            this.localX = localX;
            this.localZ = localZ;
            this.chunkX = chunkX;
            this.chunkZ = chunkZ;
            this.dimensionId = dimensionId;
        }

        public String rootIdentity() { return rootIdentity; }
        public Path regionPath() { return regionPath; }
        public int regionX() { return regionX; }
        public int regionZ() { return regionZ; }
        public int slotIndex() { return slotIndex; }
        public int localX() { return localX; }
        public int localZ() { return localZ; }
        public int chunkX() { return chunkX; }
        public int chunkZ() { return chunkZ; }
        public OptionalInt dimensionId() { return dimensionId; }
        CatalogLocation location(List<Integer> entityIndexPath) {
            return CatalogLocation.region(rootIdentity, regionPath, regionX, regionZ).withSlot(slotIndex, chunkX, chunkZ).withEntityPath(entityIndexPath);
        }
    }

    public static final class EntityOccurrence {
        private final ChunkSource source;
        private final List<Integer> entityIndexPath;
        private final String rawType;
        private final RawUuidRepresentation rawUuid;
        private final Optional<UUID> parsedUuid;
        private final byte[] subtreeBytes;
        private final String subtreeSha256;

        EntityOccurrence(ChunkSource source, List<Integer> entityIndexPath, String rawType, RawUuidRepresentation rawUuid,
                         Optional<UUID> parsedUuid, byte[] subtreeBytes) {
            this.source = source;
            this.entityIndexPath = List.copyOf(entityIndexPath);
            this.rawType = rawType;
            this.rawUuid = rawUuid;
            this.parsedUuid = parsedUuid;
            this.subtreeBytes = subtreeBytes;
            this.subtreeSha256 = sha256Hex(subtreeBytes);
        }

        public ChunkSource source() { return source; }
        public String rootIdentity() { return source.rootIdentity(); }
        public Path regionPath() { return source.regionPath(); }
        public int regionX() { return source.regionX(); }
        public int regionZ() { return source.regionZ(); }
        public int slotIndex() { return source.slotIndex(); }
        public int chunkX() { return source.chunkX(); }
        public int chunkZ() { return source.chunkZ(); }
        public OptionalInt dimensionId() { return source.dimensionId(); }
        public List<Integer> entityIndexPath() { return entityIndexPath; }
        public List<Integer> parentIndexPath() {
            return entityIndexPath.isEmpty() ? List.of() : List.copyOf(entityIndexPath.subList(0, entityIndexPath.size() - 1));
        }
        public String rawType() { return rawType; }
        public RawUuidRepresentation rawUuidRepresentation() { return rawUuid; }
        public Optional<UUID> parsedUuid() { return parsedUuid; }
        public byte[] subtreeBytes() { return subtreeBytes.clone(); }
        public String subtreeSha256() { return subtreeSha256; }
        public long retainedPayloadBytes() { return subtreeBytes.length; }
    }

    public static final class RawUuidRepresentation {
        private final int mostTagType;
        private final OptionalLong mostValue;
        private final int leastTagType;
        private final OptionalLong leastValue;

        RawUuidRepresentation(int mostTagType, OptionalLong mostValue, int leastTagType, OptionalLong leastValue) {
            this.mostTagType = mostTagType;
            this.mostValue = mostValue;
            this.leastTagType = leastTagType;
            this.leastValue = leastValue;
        }

        public int mostTagType() { return mostTagType; }
        public OptionalLong mostValue() { return mostValue; }
        public int leastTagType() { return leastTagType; }
        public OptionalLong leastValue() { return leastValue; }
    }

    public static final class CatalogException extends IOException {
        private final String stage;
        private final CatalogLocation location;

        private CatalogException(String stage, CatalogLocation location, String detail, Throwable cause) {
            super(formatFailure(stage, location, detail), cause);
            this.stage = stage;
            this.location = location;
        }

        public String stage() { return stage; }
        public String rootIdentity() { return location.rootIdentity(); }
        public Path regionPath() { return location.regionPath(); }
        public OptionalInt regionX() { return location.hasRegionCoordinates() ? OptionalInt.of(location.regionX()) : OptionalInt.empty(); }
        public OptionalInt regionZ() { return location.hasRegionCoordinates() ? OptionalInt.of(location.regionZ()) : OptionalInt.empty(); }
        public OptionalInt slotIndex() { return location.slotIndex(); }
        public OptionalInt chunkX() { return location.chunkX(); }
        public OptionalInt chunkZ() { return location.chunkZ(); }
        public List<Integer> entityIndexPath() { return location.entityIndexPath(); }

        private static String formatFailure(String stage, CatalogLocation location, String detail) {
            StringBuilder message = new StringBuilder(stage).append(": ").append(detail).append(" [root=").append(location.rootIdentity());
            if (location.regionPath() != null) {
                message.append(", path=").append(location.regionPath());
            }
            if (location.hasRegionCoordinates()) {
                message.append(", region=").append(location.regionX()).append(',').append(location.regionZ());
            }
            if (location.slotIndex().isPresent()) {
                message.append(", slot=").append(location.slotIndex().getAsInt());
            }
            if (location.chunkX().isPresent() && location.chunkZ().isPresent()) {
                message.append(", chunk=").append(location.chunkX().getAsInt()).append(',').append(location.chunkZ().getAsInt());
            }
            if (!location.entityIndexPath().isEmpty()) {
                message.append(", entityPath=").append(location.entityIndexPath());
            }
            return message.append(']').toString();
        }
    }

    private static final class CatalogLocation {
        private final String rootIdentity;
        private final Path regionPath;
        private final boolean hasRegionCoordinates;
        private final int regionX;
        private final int regionZ;
        private final Integer slotIndex;
        private final Integer chunkX;
        private final Integer chunkZ;
        private final List<Integer> entityIndexPath;

        private CatalogLocation(String rootIdentity, Path regionPath, boolean hasRegionCoordinates, int regionX, int regionZ,
                                Integer slotIndex, Integer chunkX, Integer chunkZ, List<Integer> entityIndexPath) {
            this.rootIdentity = rootIdentity;
            this.regionPath = regionPath;
            this.hasRegionCoordinates = hasRegionCoordinates;
            this.regionX = regionX;
            this.regionZ = regionZ;
            this.slotIndex = slotIndex;
            this.chunkX = chunkX;
            this.chunkZ = chunkZ;
            this.entityIndexPath = List.copyOf(entityIndexPath);
        }

        private static CatalogLocation empty() {
            return new CatalogLocation("<none>", null, false, 0, 0, null, null, null, List.of());
        }

        private static CatalogLocation root(String rootIdentity, Path directory) {
            return new CatalogLocation(rootIdentity, directory, false, 0, 0, null, null, null, List.of());
        }

        private static CatalogLocation file(String rootIdentity, Path path) {
            return new CatalogLocation(rootIdentity, path, false, 0, 0, null, null, null, List.of());
        }

        private static CatalogLocation region(String rootIdentity, Path path, int regionX, int regionZ) {
            return new CatalogLocation(rootIdentity, path, true, regionX, regionZ, null, null, null, List.of());
        }

        private CatalogLocation withSlot(int slotIndex, Integer chunkX, Integer chunkZ) {
            return new CatalogLocation(rootIdentity, regionPath, hasRegionCoordinates, regionX, regionZ, slotIndex, chunkX, chunkZ, entityIndexPath);
        }

        private CatalogLocation withEntityPath(List<Integer> entityIndexPath) {
            return new CatalogLocation(rootIdentity, regionPath, hasRegionCoordinates, regionX, regionZ, slotIndex, chunkX, chunkZ, entityIndexPath);
        }

        private String rootIdentity() { return rootIdentity; }
        private Path regionPath() { return regionPath; }
        private boolean hasRegionCoordinates() { return hasRegionCoordinates; }
        private int regionX() { return regionX; }
        private int regionZ() { return regionZ; }
        private OptionalInt slotIndex() { return slotIndex == null ? OptionalInt.empty() : OptionalInt.of(slotIndex); }
        private OptionalInt chunkX() { return chunkX == null ? OptionalInt.empty() : OptionalInt.of(chunkX); }
        private OptionalInt chunkZ() { return chunkZ == null ? OptionalInt.empty() : OptionalInt.of(chunkZ); }
        private List<Integer> entityIndexPath() { return entityIndexPath; }
    }

    private record DirectorySnapshot(long size, FileTime modified, String fileKey, boolean directory) {
        private boolean sameObservation(DirectorySnapshot other) {
            return other != null && size == other.size && modified.equals(other.modified)
                && java.util.Objects.equals(fileKey, other.fileKey) && directory == other.directory;
        }
    }

    private record FileObservation(long size, FileTime modified, String fileKey, boolean regularFile) {
        private boolean sameObservation(FileObservation other) {
            return other != null && size == other.size && modified.equals(other.modified)
                && java.util.Objects.equals(fileKey, other.fileKey) && regularFile == other.regularFile;
        }
    }

    private record RegionFileEntry(Path path, int regionX, int regionZ) {
    }

    private record FileRead(byte[] bytes, FileObservation before, FileObservation after) {
    }

    private record RegionScan(RegionSnapshot region, List<EntityOccurrence> occurrences, long presentChunks,
                              long retainedPayloadBytes, CatalogLocation location) {
        private RegionScan {
            occurrences = List.copyOf(occurrences);
        }
    }
}
