package com.wi.floatingpod.server;

import net.minecraft.nbt.CompressedStreamTools;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.nbt.NBTTagList;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.zip.DeflaterOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PersistedCatalogChunkLookupTest {
    private static final int SECTOR_BYTES = 4096;
    private static final String ROOT_IDENTITY = "overworld-region";
    private static final UUID GENERATION = UUID.fromString("00000000-0000-0000-0000-000000000010");

    @TempDir
    Path temporaryDirectory;

    @Test
    void mapsExactRootAndNegativeChunkCoordinatesWithoutPromotingEmptySlotsToAbsence() throws Exception {
        Path regionRoot = Files.createDirectory(temporaryDirectory.resolve("regions"));
        Path zeroHeader = regionRoot.resolve("r.0.0.mca");
        Files.write(zeroHeader, new byte[SECTOR_BYTES * 2]);
        Path negativeRegion = regionRoot.resolve("r.-1.-1.mca");
        writeSingleZlibChunk(negativeRegion, 31, 0, chunkNbt(-1, -32));
        byte[] zeroHeaderBytes = Files.readAllBytes(zeroHeader);
        byte[] negativeRegionBytes = Files.readAllBytes(negativeRegion);
        long zeroHeaderSize = Files.size(zeroHeader);
        long negativeRegionSize = Files.size(negativeRegion);
        FileTime zeroHeaderTime = Files.getLastModifiedTime(zeroHeader, LinkOption.NOFOLLOW_LINKS);
        FileTime negativeRegionTime = Files.getLastModifiedTime(negativeRegion, LinkOption.NOFOLLOW_LINKS);

        ReadOnlyRegionCatalog.CatalogSnapshot snapshot = scan(regionRoot);
        assertArrayEquals(zeroHeaderBytes, Files.readAllBytes(zeroHeader));
        assertArrayEquals(negativeRegionBytes, Files.readAllBytes(negativeRegion));
        assertEquals(zeroHeaderSize, Files.size(zeroHeader));
        assertEquals(negativeRegionSize, Files.size(negativeRegion));
        assertEquals(zeroHeaderTime, Files.getLastModifiedTime(zeroHeader, LinkOption.NOFOLLOW_LINKS));
        assertEquals(negativeRegionTime, Files.getLastModifiedTime(negativeRegion, LinkOption.NOFOLLOW_LINKS));
        PersistedCatalogChunkLookup.SlotObservation presentObservation = PersistedCatalogChunkLookup.lookup(
            snapshot,
            ROOT_IDENTITY,
            -1,
            -32
        );

        assertTrue(presentObservation instanceof PersistedCatalogChunkLookup.ValidatedSlot);
        PersistedCatalogChunkLookup.ValidatedSlot present = (PersistedCatalogChunkLookup.ValidatedSlot) presentObservation;
        assertSame(snapshot, present.snapshot());
        assertSame(
            snapshot.regions().stream().filter(region -> region.path().equals(negativeRegion)).findFirst().orElseThrow(),
            present.region()
        );
        assertSame(present.region().slot(31, 0), present.slot());
        assertEquals("world-fixture-1", present.worldIdentity());
        assertEquals(GENERATION, present.generation());
        assertEquals("fixture-manifest", present.manifestPin().reference());
        assertEquals("overworld-region", present.slot().rootIdentity());
        assertEquals(-1, present.slot().chunkX());
        assertEquals(-32, present.slot().chunkZ());
        assertEquals(31, present.slot().localX());
        assertEquals(0, present.slot().localZ());
        assertTrue(present.slot().present());

        PersistedCatalogChunkLookup.SlotObservation emptyObservation = PersistedCatalogChunkLookup.lookup(
            snapshot,
            ROOT_IDENTITY,
            0,
            0
        );
        assertTrue(emptyObservation instanceof PersistedCatalogChunkLookup.ValidatedSlot);
        PersistedCatalogChunkLookup.ValidatedSlot empty = (PersistedCatalogChunkLookup.ValidatedSlot) emptyObservation;
        assertSame(snapshot, empty.snapshot());
        assertSame(empty.region().slot(0, 0), empty.slot());
        assertEquals(0, empty.region().presentSlotCount());
        assertTrue(empty.region().fileObservationStableDuringRead());
        assertTrue(!empty.slot().present());

        PersistedCatalogChunkLookup.SlotObservation unknownRoot = PersistedCatalogChunkLookup.lookup(
            snapshot,
            "unknown-root",
            -1,
            -32
        );
        assertTrue(unknownRoot instanceof PersistedCatalogChunkLookup.Unobserved);
        assertEquals(
            PersistedCatalogChunkLookup.UnobservedReason.ROOT_IDENTITY_UNKNOWN,
            ((PersistedCatalogChunkLookup.Unobserved) unknownRoot).reason()
        );
        assertSame(snapshot, unknownRoot.snapshot());
        assertEquals("unknown-root", ((PersistedCatalogChunkLookup.Unobserved) unknownRoot).rootIdentity());

        PersistedCatalogChunkLookup.SlotObservation missingRegion = PersistedCatalogChunkLookup.lookup(
            snapshot,
            ROOT_IDENTITY,
            32,
            0
        );
        assertTrue(missingRegion instanceof PersistedCatalogChunkLookup.Unobserved);
        assertEquals(
            PersistedCatalogChunkLookup.UnobservedReason.REGION_NOT_OBSERVED,
            ((PersistedCatalogChunkLookup.Unobserved) missingRegion).reason()
        );
        assertSame(snapshot, missingRegion.snapshot());
        assertEquals(ROOT_IDENTITY, ((PersistedCatalogChunkLookup.Unobserved) missingRegion).rootIdentity());
    }

    @Test
    void rejectsHeaderChangeEvenWhenModificationTimeIsRestored() throws Exception {
        Path regionRoot = Files.createDirectory(temporaryDirectory.resolve("changing"));
        Path regionFile = regionRoot.resolve("r.0.0.mca");
        byte[] original = new byte[SECTOR_BYTES * 2];
        Files.write(regionFile, original);
        FileTime originalTime = Files.getLastModifiedTime(regionFile, LinkOption.NOFOLLOW_LINKS);

        ReadOnlyRegionCatalog.FileSnapshotHook mutator = path -> {
            byte[] changed = Files.readAllBytes(path);
            changed[0] = 1;
            Files.write(path, changed, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            Files.setLastModifiedTime(path, originalTime);
        };
        ReadOnlyRegionCatalog.CatalogException failure = assertThrows(
            ReadOnlyRegionCatalog.CatalogException.class,
            () -> reader(mutator).scan(catalogInput(regionRoot))
        );

        assertEquals("REGION_CHANGED", failure.stage());
        byte[] changedOnDisk = Files.readAllBytes(regionFile);
        assertEquals(1, changedOnDisk[0]);
        assertEquals(originalTime, Files.getLastModifiedTime(regionFile, LinkOption.NOFOLLOW_LINKS));
    }

    @Test
    void corruptCatalogScanFailsAndDoesNotRepairTheRegionFile() throws Exception {
        Path regionRoot = Files.createDirectory(temporaryDirectory.resolve("corrupt"));
        Path regionFile = regionRoot.resolve("r.0.0.mca");
        byte[] corrupt = new byte[SECTOR_BYTES * 2];
        putInt(corrupt, 0, (2 << 8) | 1);
        Files.write(regionFile, corrupt);

        ReadOnlyRegionCatalog.CatalogException failure = assertThrows(
            ReadOnlyRegionCatalog.CatalogException.class,
            () -> reader().scan(catalogInput(regionRoot))
        );

        assertEquals("REGION_SLOT_BOUNDS", failure.stage());
        assertArrayEquals(corrupt, Files.readAllBytes(regionFile));
    }

    @Test
    void retainsOriginalIOExceptionCauseFromTheReadHook() throws Exception {
        Path regionRoot = Files.createDirectory(temporaryDirectory.resolve("hook-failure"));
        Files.write(regionRoot.resolve("r.0.0.mca"), new byte[SECTOR_BYTES * 2]);
        IOException originalFailure = new IOException("fixture hook failure");

        ReadOnlyRegionCatalog.CatalogException failure = assertThrows(
            ReadOnlyRegionCatalog.CatalogException.class,
            () -> reader(path -> {
                throw originalFailure;
            }).scan(catalogInput(regionRoot))
        );

        assertEquals("REGION_FILE_READ", failure.stage());
        assertSame(originalFailure, failure.getCause());
    }

    private ReadOnlyRegionCatalog.CatalogSnapshot scan(Path regionRoot) throws IOException {
        return reader().scan(catalogInput(regionRoot));
    }

    private ReadOnlyRegionCatalog reader() {
        return new ReadOnlyRegionCatalog(new CatalogReadResponsibility(Thread.currentThread()));
    }

    private ReadOnlyRegionCatalog reader(ReadOnlyRegionCatalog.FileSnapshotHook hook) {
        return new ReadOnlyRegionCatalog(new CatalogReadResponsibility(Thread.currentThread()), hook);
    }

    private ReadOnlyRegionCatalog.CatalogInput catalogInput(Path regionRoot) {
        return new ReadOnlyRegionCatalog.CatalogInput(
            "world-fixture-1",
            GENERATION,
            Thread.currentThread(),
            List.of(new ReadOnlyRegionCatalog.RegionRoot(
                ROOT_IDENTITY,
                regionRoot.toAbsolutePath().normalize(),
                OptionalInt.of(0)
            )),
            new ReadOnlyRegionCatalog.ManifestPin("fixture-manifest", 32L, new byte[32]),
            new ReadOnlyRegionCatalog.Limits(
                2,
                16,
                4096,
                16_000_000L,
                32_000_000L,
                8_000_000L,
                16_000_000L,
                16_000_000L,
                100_000L,
                128,
                64,
                128,
                4096,
                32_000_000L
            )
        );
    }

    private static byte[] chunkNbt(int chunkX, int chunkZ) throws IOException {
        NBTTagCompound level = new NBTTagCompound();
        level.setInteger("xPos", chunkX);
        level.setInteger("zPos", chunkZ);
        level.setTag("Entities", new NBTTagList());
        NBTTagCompound root = new NBTTagCompound();
        root.setTag("Level", level);

        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(raw)) {
            CompressedStreamTools.write(root, output);
        }
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (DeflaterOutputStream zlib = new DeflaterOutputStream(compressed)) {
            zlib.write(raw.toByteArray());
        }
        return compressed.toByteArray();
    }

    private static void writeSingleZlibChunk(Path file, int localX, int localZ, byte[] compressed) throws IOException {
        int slotIndex = localZ * 32 + localX;
        int recordLength = Math.addExact(compressed.length, 1);
        long span = Math.addExact(((long) recordLength + 4L) / SECTOR_BYTES, 1L);
        int headerCount = Math.toIntExact(span);
        if (headerCount > 255) {
            throw new IllegalArgumentException("test chunk must use the ordinary region header count");
        }
        byte[] region = new byte[Math.toIntExact((2L + span) * SECTOR_BYTES)];
        putInt(region, slotIndex * 4, (2 << 8) | headerCount);
        int recordOffset = SECTOR_BYTES * 2;
        putInt(region, recordOffset, recordLength);
        region[recordOffset + 4] = 2;
        System.arraycopy(compressed, 0, region, recordOffset + 5, compressed.length);
        Files.write(file, region, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    private static void putInt(byte[] bytes, int offset, int value) {
        ByteBuffer.wrap(bytes, offset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(value);
    }
}
