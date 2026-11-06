package com.wi.floatingpod.server;

import net.minecraft.nbt.CompressedStreamTools;
import net.minecraft.nbt.NBTSizeTracker;
import net.minecraft.nbt.NBTTagByteArray;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.nbt.NBTTagInt;
import net.minecraft.nbt.NBTTagList;
import net.minecraft.nbt.NBTTagLongArray;
import net.minecraft.nbt.NBTTagString;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ReadOnlyRegionCatalogTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void readsGzipAndZlibRecordsAndKeepsEmptySlots() throws Exception {
        Path region = Files.createDirectory(temporaryDirectory.resolve("region"));
        RegionFixture.write(region.resolve("r.0.0.mca"), Map.of(
            0, RegionFixture.payload((byte) 1, nbtBytes(chunk(0, 0, new NBTTagList()))),
            1, RegionFixture.payload((byte) 2, nbtBytes(chunk(1, 0, entities(entity("floatingpod:floating_pod", uuid(1), null)))))
        ));

        ReadOnlyRegionCatalog.CatalogSnapshot result = scan(region, 0, 0, OptionalInt.of(7), limits());
        ReadOnlyRegionCatalog.RegionSnapshot file = result.regions().get(0);

        assertEquals(ReadOnlyRegionCatalog.Coverage.SUPPLIED_ROOTS_ONLY, result.coverage());
        assertEquals(1024, file.slots().size());
        assertEquals(2, file.presentSlotCount());
        assertEquals(1022, file.emptySlotCount());
        assertEquals(1, file.slot(0, 0).compressionId());
        assertEquals(2, file.slot(1, 0).compressionId());
        assertEquals(0, file.slot(2, 0).compressedBytes().length);
        assertEquals(1, result.occurrences().size());
        assertEquals("floatingpod:floating_pod", result.occurrences().get(0).rawType());
        assertEquals(uuid(1), result.occurrences().get(0).parsedUuid().orElseThrow());
        assertEquals(OptionalInt.of(7), file.dimensionId());
    }

    @Test
    void keepsNegativeRegionCoordinatesAndUnknownDimensionExplicit() throws Exception {
        Path region = Files.createDirectory(temporaryDirectory.resolve("negative"));
        int regionX = -2;
        int regionZ = -1;
        int localX = 31;
        int localZ = 0;
        int chunkX = regionX * 32 + localX;
        int chunkZ = regionZ * 32 + localZ;
        RegionFixture.write(region.resolve("r.-2.-1.mca"), Map.of(
            RegionFixture.slot(localX, localZ),
            RegionFixture.payload((byte) 2, nbtBytes(chunk(chunkX, chunkZ, entities(entity("minecraft:pig", uuid(2), null)))))
        ));

        ReadOnlyRegionCatalog.CatalogSnapshot result = scan(region, regionX, regionZ, OptionalInt.empty(), limits());
        ReadOnlyRegionCatalog.SlotSnapshot slot = result.regions().get(0).slot(localX, localZ);

        assertEquals(-33, slot.chunkX());
        assertEquals(-32, slot.chunkZ());
        assertEquals(OptionalInt.empty(), slot.dimensionId());
        assertEquals(OptionalInt.empty(), result.occurrences().get(0).dimensionId());
    }

    @Test
    void derivesExtendedForge255SpanFromRecordLengthAndRetainsCompressedBytes() throws Exception {
        Path region = Files.createDirectory(temporaryDirectory.resolve("sentinel"));
        byte[] incompressible = new byte[1_060_000];
        new Random(77123L).nextBytes(incompressible);
        NBTTagCompound large = entity("minecraft:chest_minecart", uuid(3), null);
        large.setTag("FuturePayload", new NBTTagByteArray(incompressible));
        byte[] rawNbt = nbtBytes(chunk(0, 0, entities(large)));
        RegionFixture.Payload payload = RegionFixture.payload((byte) 2, rawNbt);
        assertTrue(RegionFixture.allocatedSectors(payload) > 255);
        Path file = region.resolve("r.0.0.mca");
        RegionFixture.write(file, Map.of(0, payload));

        ReadOnlyRegionCatalog.SlotSnapshot slot = scan(region, 0, 0, OptionalInt.of(0), largeLimits())
            .regions().get(0).slot(0, 0);

        assertEquals(255, RegionFixture.headerCount(file, 0));
        assertEquals(RegionFixture.allocatedSectors(payload), slot.spanSectors());
        assertArrayEquals(payload.compressed(), slot.compressedBytes());
        assertEquals(RegionFixture.sha256(payload.compressed()), slot.compressedSha256());
        assertArrayEquals(rawNbt, slot.decodedRootBytes());
    }

    @Test
    void retainsDuplicateUuidsAcrossTopLevelAndPassengerTypes() throws Exception {
        Path region = Files.createDirectory(temporaryDirectory.resolve("duplicates"));
        UUID duplicate = uuid(44);
        NBTTagCompound nested = entity("minecraft:armor_stand", duplicate, null);
        NBTTagCompound parent = entity("floatingpod:floating_pod", duplicate, passengers(nested));
        NBTTagCompound second = entity("minecraft:pig", duplicate, null);
        RegionFixture.write(region.resolve("r.0.0.mca"), Map.of(
            0, RegionFixture.payload((byte) 1, nbtBytes(chunk(0, 0, entities(parent, second))))
        ));

        List<ReadOnlyRegionCatalog.EntityOccurrence> found =
            scan(region, 0, 0, OptionalInt.of(0), limits()).occurrences();

        assertEquals(3, found.size());
        assertEquals(List.of(0), found.get(0).entityIndexPath());
        assertEquals(List.of(0, 0), found.get(1).entityIndexPath());
        assertEquals(List.of(0), found.get(1).parentIndexPath());
        assertEquals(List.of(1), found.get(2).entityIndexPath());
        assertEquals("minecraft:armor_stand", found.get(1).rawType());
        for (ReadOnlyRegionCatalog.EntityOccurrence occurrence : found) {
            assertEquals(duplicate, occurrence.parsedUuid().orElseThrow());
            assertEquals(4, occurrence.rawUuidRepresentation().mostTagType());
            assertEquals(OptionalLong.of(duplicate.getMostSignificantBits()), occurrence.rawUuidRepresentation().mostValue());
            assertEquals(OptionalLong.of(duplicate.getLeastSignificantBits()), occurrence.rawUuidRepresentation().leastValue());
        }
    }

    @Test
    void retainsUnknownTagsAndCompletePassengerSubtrees() throws Exception {
        Path region = Files.createDirectory(temporaryDirectory.resolve("unknown-tags"));
        NBTTagCompound caps = new NBTTagCompound();
        caps.setInteger("FutureNumericTag", 0x13579);
        caps.setTag("NestedUnknown", new NBTTagString("kept"));
        byte[] unknown = new byte[] { 0, 1, (byte) 0xFE, (byte) 0xFF };
        NBTTagCompound parent = entity("floatingpod:floating_pod", uuid(54), passengers(entity("example:carrier", uuid(55), null)));
        parent.setTag("ForgeCaps", caps);
        parent.setTag("MysteryBytes", new NBTTagByteArray(unknown));
        String modifiedUtf = "\u0000雪\uD83D\uDE80";
        caps.setString("FutureModifiedUtf", modifiedUtf);
        caps.setTag("FutureLongArray", new NBTTagLongArray(new long[] { Long.MIN_VALUE, 17L, Long.MAX_VALUE }));
        byte[] rootBytes = nbtBytes(chunk(0, 0, entities(parent)));
        byte[] compressed = RegionFixture.payload((byte) 1, rootBytes).compressed();
        RegionFixture.write(region.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 1, rootBytes)));

        ReadOnlyRegionCatalog.CatalogSnapshot result = scan(region, 0, 0, OptionalInt.of(0), limits());
        ReadOnlyRegionCatalog.SlotSnapshot slot = result.regions().get(0).slot(0, 0);
        NBTTagCompound decodedSlot = readNbt(slot.decodedRootBytes());
        NBTTagCompound decodedParent = ((NBTTagList) ((NBTTagCompound) decodedSlot.getTag("Level")).getTag("Entities")).getCompoundTagAt(0);
        NBTTagCompound decodedEntity = readNbt(result.occurrences().get(0).subtreeBytes());
        NBTTagCompound decodedCaps = (NBTTagCompound) decodedEntity.getTag("ForgeCaps");

        assertArrayEquals(compressed, slot.compressedBytes());
        assertEquals(caps.getTag("FutureNumericTag"), decodedCaps.getTag("FutureNumericTag"));
        assertEquals(caps.getTag("NestedUnknown"), decodedCaps.getTag("NestedUnknown"));
        assertEquals(modifiedUtf, ((NBTTagString) decodedCaps.getTag("FutureModifiedUtf")).getString());
        assertEquals(caps.getTag("FutureLongArray"), decodedCaps.getTag("FutureLongArray"));
        assertArrayEquals(unknown, ((NBTTagByteArray) decodedEntity.getTag("MysteryBytes")).getByteArray());
        assertEquals(decodedParent.getTag("ForgeCaps"), decodedEntity.getTag("ForgeCaps"));
        assertEquals(2, result.occurrences().size());
        assertEquals("example:carrier", readNbt(result.occurrences().get(1).subtreeBytes()).getString("id"));
    }

    @Test
    void rejectsChunkCoordinateMismatchAndWrongEntitiesOrPassengerTypes() throws Exception {
        Path mismatch = Files.createDirectory(temporaryDirectory.resolve("mismatch"));
        RegionFixture.write(mismatch.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 2, nbtBytes(chunk(1, 0, new NBTTagList())))));
        assertStage(mismatch, "NBT_CHUNK_IDENTITY");

        Path wrongEntities = Files.createDirectory(temporaryDirectory.resolve("wrong-entities"));
        NBTTagCompound root = chunk(0, 0, null);
        ((NBTTagCompound) root.getTag("Level")).setTag("Entities", new NBTTagString("not-list"));
        RegionFixture.write(wrongEntities.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 1, nbtBytes(root))));
        assertStage(wrongEntities, "NBT_ENTITIES");

        Path wrongPassengers = Files.createDirectory(temporaryDirectory.resolve("wrong-passengers"));
        NBTTagCompound entity = entity("minecraft:pig", uuid(6), null);
        NBTTagList passengerTags = new NBTTagList();
        passengerTags.appendTag(new NBTTagInt(7));
        entity.setTag("Passengers", passengerTags);
        RegionFixture.write(wrongPassengers.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 2, nbtBytes(chunk(0, 0, entities(entity))))));
        assertStage(wrongPassengers, "NBT_PASSENGERS");
    }

    @Test
    void rejectsTinyArrayPayloadsBeforeLegacyTrackerMultiplicationCanOverflow() throws Exception {
        int[][] cases = {
            { 7, 0x20000000 },
            { 11, 0x08000000 },
            { 12, 0x04000000 }
        };
        for (int index = 0; index < cases.length; index++) {
            int tagId = cases[index][0];
            int declaredCount = cases[index][1];
            Path region = Files.createDirectory(temporaryDirectory.resolve("array-overflow-" + index));
            byte[] malformed = rawNbtWithLevelTag(tagId, "DeclaredArray", output -> output.writeInt(declaredCount));
            RegionFixture.write(region.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 2, malformed)));

            assertPreflightFailure(region, limits(), BoundedNbtStructurePreflight.Code.ARRAY_TRACKER_MULTIPLICATION_OVERFLOW);
        }
    }

    @Test
    void rejectsNegativeTruncatedUnknownAndInvalidListStructureDuringPreflight() throws Exception {
        Path negativeArray = Files.createDirectory(temporaryDirectory.resolve("negative-array-length"));
        RegionFixture.write(negativeArray.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 1,
            rawNbtWithLevelTag(7, "BadArray", output -> output.writeInt(-1)))));
        assertPreflightFailure(negativeArray, limits(), BoundedNbtStructurePreflight.Code.NEGATIVE_ARRAY_LENGTH);

        Path truncatedArray = Files.createDirectory(temporaryDirectory.resolve("truncated-array"));
        RegionFixture.write(truncatedArray.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 2,
            rawNbtWithLevelTag(7, "ShortArray", output -> {
                output.writeInt(8);
                output.write(new byte[] { 1, 2 });
            }))));
        assertPreflightFailure(truncatedArray, limits(), BoundedNbtStructurePreflight.Code.TRUNCATED_ARRAY);

        Path negativeList = Files.createDirectory(temporaryDirectory.resolve("negative-list-count"));
        RegionFixture.write(negativeList.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 1,
            rawNbtWithLevelTag(9, "BadList", output -> {
                output.writeByte(1);
                output.writeInt(-1);
            }))));
        assertPreflightFailure(negativeList, limits(), BoundedNbtStructurePreflight.Code.NEGATIVE_LIST_COUNT);

        Path endList = Files.createDirectory(temporaryDirectory.resolve("end-list-with-count"));
        RegionFixture.write(endList.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 2,
            rawNbtWithLevelTag(9, "BadEndList", output -> {
                output.writeByte(0);
                output.writeInt(1);
            }))));
        assertPreflightFailure(endList, limits(), BoundedNbtStructurePreflight.Code.TAG_END_LIST_WITH_ELEMENTS);

        Path unknownType = Files.createDirectory(temporaryDirectory.resolve("unknown-nbt-tag-type"));
        RegionFixture.write(unknownType.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 1,
            rawNbtWithLevelTag(13, "UnknownTag", output -> output.writeByte(0)))));
        assertPreflightFailure(unknownType, limits(), BoundedNbtStructurePreflight.Code.UNKNOWN_TAG);

        Path malformedUtf = Files.createDirectory(temporaryDirectory.resolve("malformed-modified-utf"));
        RegionFixture.write(malformedUtf.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 2,
            rawNbtWithLevelTag(8, "BadUtf", output -> {
                output.writeShort(2);
                output.writeByte(0xC0);
                output.writeByte(0x20);
            }))));
        assertPreflightFailure(malformedUtf, limits(), BoundedNbtStructurePreflight.Code.MALFORMED_MODIFIED_UTF);

        Path trailing = Files.createDirectory(temporaryDirectory.resolve("trailing-root-bytes"));
        byte[] validRoot = rawNbtWithLevelTag(9, "EmptyList", output -> {
            output.writeByte(0);
            output.writeInt(0);
        });
        byte[] rootWithTrailing = new byte[validRoot.length + 1];
        System.arraycopy(validRoot, 0, rootWithTrailing, 0, validRoot.length);
        rootWithTrailing[rootWithTrailing.length - 1] = 0x7F;
        RegionFixture.write(trailing.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 1, rootWithTrailing)));
        assertPreflightFailure(trailing, limits(), BoundedNbtStructurePreflight.Code.TRAILING_BYTES);
    }

    @Test
    void prechargesListTagBudgetAndRejectsNbtDepthBeforeMaterialization() throws Exception {
        Path tagBudget = Files.createDirectory(temporaryDirectory.resolve("tag-budget"));
        byte[] hugeList = rawNbtWithLevelTag(9, "ManyElements", output -> {
            output.writeByte(1);
            output.writeInt(Integer.MAX_VALUE);
        });
        RegionFixture.write(tagBudget.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 2, hugeList)));
        ReadOnlyRegionCatalog.Limits smallTagBudget =
            limits(4, 64, 8192, 16_000_000L, 256_000_000L, 8_000_000L, 16_000_000L, 16_000_000L, 5L, 128, 64, 4096, 50_000, 256_000_000L);
        assertPreflightFailure(tagBudget, smallTagBudget, BoundedNbtStructurePreflight.Code.TAG_BUDGET_EXCEEDED);

        Path deepTree = Files.createDirectory(temporaryDirectory.resolve("nbt-depth-preflight"));
        NBTTagCompound root = chunk(0, 0, new NBTTagList());
        NBTTagCompound level = (NBTTagCompound) root.getTag("Level");
        NBTTagCompound outer = new NBTTagCompound();
        NBTTagCompound inner = new NBTTagCompound();
        outer.setTag("Inner", inner);
        inner.setInteger("TooDeep", 1);
        level.setTag("Outer", outer);
        RegionFixture.write(deepTree.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 1, nbtBytes(root))));
        ReadOnlyRegionCatalog.Limits shallow =
            limits(4, 64, 8192, 16_000_000L, 256_000_000L, 8_000_000L, 16_000_000L, 16_000_000L, 100_000L, 3, 64, 4096, 50_000, 256_000_000L);
        assertPreflightFailure(deepTree, shallow, BoundedNbtStructurePreflight.Code.DEPTH_LIMIT);
    }

    @Test
    void appliesEntityLimitPerChunkAndKeepsGlobalLimitAcrossChunks() throws Exception {
        Path region = Files.createDirectory(temporaryDirectory.resolve("per-chunk-entity-limits"));
        RegionFixture.write(region.resolve("r.0.0.mca"), Map.of(
            0, RegionFixture.payload((byte) 1, nbtBytes(chunk(0, 0, entities(
                entity("minecraft:pig", uuid(60), null),
                entity("minecraft:cow", uuid(61), null)
            )))),
            1, RegionFixture.payload((byte) 2, nbtBytes(chunk(1, 0, entities(
                entity("minecraft:sheep", uuid(62), null),
                entity("minecraft:chicken", uuid(63), null)
            ))))
        ));

        ReadOnlyRegionCatalog.Limits perChunkTwoGlobalFour =
            limits(4, 64, 8192, 16_000_000L, 256_000_000L, 8_000_000L, 16_000_000L, 16_000_000L, 100_000L, 128, 64, 2, 4, 256_000_000L);
        ReadOnlyRegionCatalog.CatalogSnapshot result = scan(region, 0, 0, OptionalInt.of(0), perChunkTwoGlobalFour);
        assertEquals(4, result.occurrences().size());
        assertEquals(2, result.regions().get(0).presentSlotCount());

        ReadOnlyRegionCatalog.Limits perChunkTwoGlobalThree =
            limits(4, 64, 8192, 16_000_000L, 256_000_000L, 8_000_000L, 16_000_000L, 16_000_000L, 100_000L, 128, 64, 2, 3, 256_000_000L);
        ReadOnlyRegionCatalog.CatalogException failure = assertThrows(
            ReadOnlyRegionCatalog.CatalogException.class,
            () -> scan(region, 0, 0, OptionalInt.of(0), perChunkTwoGlobalThree)
        );
        assertEquals("ENTITY_COUNT_LIMIT", failure.stage());
    }

    @Test
    void rejectsMalformedUuidWordsWithoutInventingAnIdentity() throws Exception {
        Path region = Files.createDirectory(temporaryDirectory.resolve("bad-uuid"));
        NBTTagCompound malformed = new NBTTagCompound();
        malformed.setString("id", "minecraft:pig");
        malformed.setInteger("UUIDMost", 5);
        malformed.setLong("UUIDLeast", 6L);
        RegionFixture.write(region.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 1, nbtBytes(chunk(0, 0, entities(malformed))))));

        ReadOnlyRegionCatalog.CatalogException failure = assertThrows(
            ReadOnlyRegionCatalog.CatalogException.class,
            () -> scan(region, 0, 0, OptionalInt.of(0), limits())
        );
        assertEquals("NBT_ENTITY_UUID", failure.stage());
        assertEquals(OptionalInt.of(0), failure.slotIndex());
    }

    @Test
    void rejectsShortHeaderHeaderOffsetsBeyondFileAndOverlaps() throws Exception {
        Path region = Files.createDirectory(temporaryDirectory.resolve("bad-header"));
        Path file = region.resolve("r.0.0.mca");
        Files.write(file, new byte[8191]);
        assertStage(region, "REGION_HEADER");

        writeRaw(file, RegionFixture.raw(3, 1, 1, 0, 0));
        assertStage(region, "REGION_SLOT_BOUNDS");
        writeRaw(file, RegionFixture.raw(3, 4, 1, 0, 0));
        assertStage(region, "REGION_SLOT_BOUNDS");

        RegionFixture.Payload valid = RegionFixture.payload(
            (byte) 1,
            nbtBytes(chunk(0, 0, new NBTTagList()))
        );
        byte[] compressedNbt = valid.compressed();
        byte[] record = new byte[compressedNbt.length + 1];
        record[0] = 1;
        System.arraycopy(compressedNbt, 0, record, 1, compressedNbt.length);

        byte[] overlap = RegionFixture.raw(4, 2, 1, 2, 1);
        RegionFixture.putRecord(overlap, 2, record);
        writeRaw(file, overlap);
        assertStage(region, "REGION_SECTOR_OVERLAP");
    }

    @Test
    void rejectsInvalidRecordLengthUnknownCompressionAndMalformedStreamsWithCause() throws Exception {
        Path region = Files.createDirectory(temporaryDirectory.resolve("bad-record"));
        Path file = region.resolve("r.0.0.mca");
        byte[] overlong = RegionFixture.raw(3, 2, 1, 0, 0);
        RegionFixture.putInt(overlong, 2 * 4096, 4096);
        writeRaw(file, overlong);
        assertStage(region, "REGION_RECORD_LENGTH");

        RegionFixture.write(file, Map.of(0, RegionFixture.payload((byte) 9, new byte[] { 1, 2, 3 })));
        assertStage(region, "REGION_COMPRESSION");

        RegionFixture.write(file, Map.of(0, RegionFixture.rawPayload((byte) 1, new byte[] { 0x1F, (byte) 0x8B, 1 })));
        ReadOnlyRegionCatalog.CatalogException gzip = assertThrows(
            ReadOnlyRegionCatalog.CatalogException.class,
            () -> scan(region, 0, 0, OptionalInt.of(0), limits())
        );
        assertEquals("REGION_DECOMPRESS", gzip.stage());
        assertNotNull(gzip.getCause());

        RegionFixture.write(file, Map.of(0, RegionFixture.rawPayload((byte) 2, new byte[] { 0, 0, 1 })));
        ReadOnlyRegionCatalog.CatalogException zlib = assertThrows(
            ReadOnlyRegionCatalog.CatalogException.class,
            () -> scan(region, 0, 0, OptionalInt.of(0), limits())
        );
        assertEquals("REGION_DECOMPRESS", zlib.stage());
        assertNotNull(zlib.getCause());
    }

    @Test
    void rejectsGzipAndZlibChecksumCorruptionWithOriginalCauses() throws Exception {
        Path region = Files.createDirectory(temporaryDirectory.resolve("bad-checksum"));
        Path file = region.resolve("r.0.0.mca");
        byte[] rawNbt = nbtBytes(chunk(0, 0, new NBTTagList()));

        byte[] gzip = RegionFixture.payload((byte) 1, rawNbt).compressed();
        gzip[gzip.length - 8] ^= 1;
        RegionFixture.write(file, Map.of(0, RegionFixture.rawPayload((byte) 1, gzip)));
        ReadOnlyRegionCatalog.CatalogException gzipFailure = assertThrows(
            ReadOnlyRegionCatalog.CatalogException.class,
            () -> scan(region, 0, 0, OptionalInt.of(0), limits())
        );
        assertEquals("REGION_DECOMPRESS", gzipFailure.stage());
        assertNotNull(gzipFailure.getCause());

        byte[] zlib = RegionFixture.payload((byte) 2, rawNbt).compressed();
        zlib[zlib.length - 1] ^= 1;
        RegionFixture.write(file, Map.of(0, RegionFixture.rawPayload((byte) 2, zlib)));
        ReadOnlyRegionCatalog.CatalogException zlibFailure = assertThrows(
            ReadOnlyRegionCatalog.CatalogException.class,
            () -> scan(region, 0, 0, OptionalInt.of(0), limits())
        );
        assertEquals("REGION_DECOMPRESS", zlibFailure.stage());
        assertNotNull(zlibFailure.getCause());
    }
    @Test
    void enforcesRootFileChunkAndRegionByteLimits() throws Exception {
        Path first = Files.createDirectory(temporaryDirectory.resolve("limit-a"));
        Path second = Files.createDirectory(temporaryDirectory.resolve("limit-b"));
        RegionFixture.write(first.resolve("r.0.0.mca"), Map.of(
            0, RegionFixture.payload((byte) 2, nbtBytes(chunk(0, 0, entities(entity("minecraft:pig", uuid(8), null))))),
            1, RegionFixture.payload((byte) 2, nbtBytes(chunk(1, 0, new NBTTagList())))
        ));
        RegionFixture.write(first.resolve("r.1.0.mca"), Map.of(0, RegionFixture.payload((byte) 1, nbtBytes(chunk(32, 0, new NBTTagList())))));
        RegionFixture.write(second.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 1, nbtBytes(chunk(0, 0, new NBTTagList())))));

        ReadOnlyRegionCatalog.Limits base = limits();
        assertThrows(ReadOnlyRegionCatalog.CatalogException.class, () -> new ReadOnlyRegionCatalog().scan(input(
            List.of(root("a", first), root("b", second)), limits(1, 64, 8192, 16_000_000L, 256_000_000L, 8_000_000L, 16_000_000L, 16_000_000L, 1_000_000L, 128, 64, 4096, 50_000, 256_000_000L)
        )));
        assertLimitsFail(first, limits(2, 1, 8192, 16_000_000L, 256_000_000L, 8_000_000L, 16_000_000L, 16_000_000L, 1_000_000L, 128, 64, 4096, 50_000, 256_000_000L));
        assertLimitsFail(first, limits(2, 64, 1, 16_000_000L, 256_000_000L, 8_000_000L, 16_000_000L, 16_000_000L, 1_000_000L, 128, 64, 4096, 50_000, 256_000_000L));
        assertLimitsFail(first, limits(2, 64, 8192, 8192L, 256_000_000L, 8_000_000L, 16_000_000L, 16_000_000L, 1_000_000L, 128, 64, 4096, 50_000, 256_000_000L));
        assertLimitsFail(first, limits(2, 64, 8192, 16_000_000L, 8192L, 8_000_000L, 16_000_000L, 16_000_000L, 1_000_000L, 128, 64, 4096, 50_000, 256_000_000L));
    }

    @Test
    void enforcesCompressedDecompressedAndNbtTrackerLimits() throws Exception {
        Path region = Files.createDirectory(temporaryDirectory.resolve("payload-limits"));
        byte[] large = new byte[64_000];
        new Random(13L).nextBytes(large);
        NBTTagCompound entity = entity("minecraft:pig", uuid(9), null);
        entity.setTag("LargePayload", new NBTTagByteArray(large));
        RegionFixture.write(region.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 2, nbtBytes(chunk(0, 0, entities(entity))))));

        assertLimitsFail(region, limits(2, 4, 64, 16_000_000L, 64_000_000L, 512L, 256_000L, 256_000L, 4_096L, 64, 32, 100, 500, 1_000_000L));
        assertLimitsFail(region, limits(2, 4, 64, 16_000_000L, 64_000_000L, 256_000L, 512L, 256_000L, 4_096L, 64, 32, 100, 500, 1_000_000L));
        assertLimitsFail(region, limits(2, 4, 64, 16_000_000L, 64_000_000L, 256_000L, 256_000L, 128L, 64L, 64, 32, 100, 500, 1_000_000L));
        assertThrows(IllegalArgumentException.class, () ->
            limits(2, 4, 64, 16_000_000L, 64_000_000L, 256_000L, 256_000L, 128L, 0L, 64, 32, 100, 500, 1_000_000L)
        );
    }

    @Test
    void enforcesStructurePassengerOccurrenceAndRetainedPayloadLimits() throws Exception {
        Path region = Files.createDirectory(temporaryDirectory.resolve("depth-limits"));
        NBTTagCompound child = entity("minecraft:pig", uuid(10), null);
        NBTTagCompound parent = entity("floatingpod:floating_pod", uuid(11), passengers(child));
        RegionFixture.write(region.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 2, nbtBytes(chunk(0, 0, entities(parent))))));

        assertLimitsFail(region, limits(2, 4, 64, 16_000_000L, 64_000_000L, 2_000_000L, 4_000_000L, 4_000_000L, 250_000L, 2, 32, 100, 500, 64_000_000L));
        assertLimitsFail(region, limits(2, 4, 64, 16_000_000L, 64_000_000L, 2_000_000L, 4_000_000L, 4_000_000L, 250_000L, 64, 0, 100, 500, 64_000_000L));
        assertLimitsFail(region, limits(2, 4, 64, 16_000_000L, 64_000_000L, 2_000_000L, 4_000_000L, 4_000_000L, 250_000L, 64, 32, 1, 500, 64_000_000L));
        assertLimitsFail(region, limits(2, 4, 64, 16_000_000L, 64_000_000L, 2_000_000L, 4_000_000L, 4_000_000L, 250_000L, 64, 32, 100, 1, 64_000_000L));
        assertLimitsFail(region, limits(2, 4, 64, 16_000_000L, 64_000_000L, 2_000_000L, 4_000_000L, 4_000_000L, 250_000L, 64, 32, 100, 500, 1L));
    }

    @Test
    void rejectsVisibleSymlinkRootsAndLexicalAliases() throws Exception {
        Path real = Files.createDirectory(temporaryDirectory.resolve("real"));
        RegionFixture.write(real.resolve("r.0.0.mca"), Map.of());
        Path link = temporaryDirectory.resolve("link");
        Files.createSymbolicLink(link, real);
        ReadOnlyRegionCatalog.CatalogException failure = assertThrows(
            ReadOnlyRegionCatalog.CatalogException.class,
            () -> scan(link, 0, 0, OptionalInt.of(0), limits())
        );
        assertEquals("PATH_IDENTITY", failure.stage());
        assertThrows(IllegalArgumentException.class, () -> input(List.of(root("a", real), root("b", real.normalize())), limits()));
    }

    @Test
    void rejectsRegionFileObservedChangingDuringItsRead() throws Exception {
        Path region = Files.createDirectory(temporaryDirectory.resolve("changed"));
        Path file = region.resolve("r.0.0.mca");
        RegionFixture.write(file, Map.of(0, RegionFixture.payload((byte) 1, nbtBytes(chunk(0, 0, new NBTTagList())))));
        ReadOnlyRegionCatalog.FileSnapshotHook mutator = path -> {
            byte[] bytes = Files.readAllBytes(path);
            bytes[bytes.length - 1] ^= 1;
            Files.write(path, bytes, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            FileTime time = Files.getLastModifiedTime(path, LinkOption.NOFOLLOW_LINKS);
            Files.setLastModifiedTime(path, FileTime.fromMillis(time.toMillis() + 2_000L));
        };

        ReadOnlyRegionCatalog.CatalogException failure = assertThrows(
            ReadOnlyRegionCatalog.CatalogException.class,
            () -> new ReadOnlyRegionCatalog(mutator).scan(catalogInput(region, limits()))
        );

        assertEquals("REGION_CHANGED", failure.stage());
        assertTrue(failure.getMessage().contains("r.0.0.mca"));
    }

    @Test
    void returnsImmutableCollectionsAndDefensiveByteCopies() throws Exception {
        Path region = Files.createDirectory(temporaryDirectory.resolve("immutable"));
        byte[] raw = nbtBytes(chunk(0, 0, entities(entity("floatingpod:floating_pod", uuid(12), null))));
        byte[] compressed = RegionFixture.payload((byte) 1, raw).compressed();
        RegionFixture.write(region.resolve("r.0.0.mca"), Map.of(0, RegionFixture.payload((byte) 1, raw)));
        ReadOnlyRegionCatalog.CatalogSnapshot result = scan(region, 0, 0, OptionalInt.of(0), limits());
        ReadOnlyRegionCatalog.SlotSnapshot slot = result.regions().get(0).slot(0, 0);
        byte[] beforeCompressed = slot.compressedBytes();
        byte[] beforeDecoded = slot.decodedRootBytes();
        byte[] beforeSubtree = result.occurrences().get(0).subtreeBytes();

        slot.compressedBytes()[0] ^= 0x7F;
        slot.decodedRootBytes()[0] ^= 0x7F;
        result.occurrences().get(0).subtreeBytes()[0] ^= 0x7F;

        assertArrayEquals(beforeCompressed, slot.compressedBytes());
        assertArrayEquals(beforeDecoded, slot.decodedRootBytes());
        assertArrayEquals(beforeSubtree, result.occurrences().get(0).subtreeBytes());
        assertThrows(UnsupportedOperationException.class, () -> result.regions().clear());
        assertThrows(UnsupportedOperationException.class, () -> result.occurrences().clear());
        assertThrows(UnsupportedOperationException.class, () -> result.occurrences().get(0).entityIndexPath().clear());
        assertArrayEquals(compressed, slot.compressedBytes());
    }

    private void assertStage(Path region, String stage) {
        ReadOnlyRegionCatalog.CatalogException failure = assertThrows(
            ReadOnlyRegionCatalog.CatalogException.class,
            () -> scan(region, 0, 0, OptionalInt.of(0), limits())
        );
        assertEquals(stage, failure.stage());
        assertNotNull(failure.regionPath());
    }

    private ReadOnlyRegionCatalog.CatalogException assertPreflightFailure(
        Path region,
        ReadOnlyRegionCatalog.Limits configured,
        BoundedNbtStructurePreflight.Code expectedCode
    ) {
        ReadOnlyRegionCatalog.CatalogException failure = assertThrows(
            ReadOnlyRegionCatalog.CatalogException.class,
            () -> scan(region, 0, 0, OptionalInt.of(0), configured)
        );
        assertEquals("NBT_PRECHECK", failure.stage());
        assertTrue(failure.getCause() instanceof BoundedNbtStructurePreflight.PreflightException);
        assertEquals(expectedCode, ((BoundedNbtStructurePreflight.PreflightException) failure.getCause()).code());
        return failure;
    }

    private void assertLimitsFail(Path region, ReadOnlyRegionCatalog.Limits configured) {
        assertThrows(ReadOnlyRegionCatalog.CatalogException.class, () -> scan(region, 0, 0, OptionalInt.of(0), configured));
    }

    private ReadOnlyRegionCatalog.CatalogSnapshot scan(Path directory, int regionX, int regionZ, OptionalInt dimension, ReadOnlyRegionCatalog.Limits configured) throws IOException {
        Files.createDirectories(directory);
        Path file = directory.resolve("r." + regionX + "." + regionZ + ".mca");
        if (!Files.exists(file, LinkOption.NOFOLLOW_LINKS)) {
            RegionFixture.write(file, Map.of());
        }
        return new ReadOnlyRegionCatalog().scan(catalogInput(directory, configured, regionX, regionZ, dimension));
    }

    private ReadOnlyRegionCatalog.CatalogInput catalogInput(Path directory, ReadOnlyRegionCatalog.Limits configured) {
        return catalogInput(directory, configured, 0, 0, OptionalInt.of(0));
    }

    private ReadOnlyRegionCatalog.CatalogInput catalogInput(Path directory, ReadOnlyRegionCatalog.Limits configured, int regionX, int regionZ, OptionalInt dimension) {
        return input(List.of(root("region-" + regionX + "-" + regionZ, directory, dimension)), configured);
    }

    private ReadOnlyRegionCatalog.CatalogInput input(List<ReadOnlyRegionCatalog.RegionRoot> roots, ReadOnlyRegionCatalog.Limits configured) {
        return new ReadOnlyRegionCatalog.CatalogInput(
            "world-fixture-1",
            UUID.fromString("00000000-0000-0000-0000-000000000010"),
            Thread.currentThread(),
            roots,
            new ReadOnlyRegionCatalog.ManifestPin("fixture-manifest", 32L, new byte[32]),
            configured
        );
    }

    private ReadOnlyRegionCatalog.RegionRoot root(String id, Path path) {
        return root(id, path, OptionalInt.of(0));
    }

    private ReadOnlyRegionCatalog.RegionRoot root(String id, Path path, OptionalInt dimension) {
        return new ReadOnlyRegionCatalog.RegionRoot(id, path.toAbsolutePath().normalize(), dimension);
    }

    private static ReadOnlyRegionCatalog.Limits limits() {
        return limits(4, 64, 8192, 16_000_000L, 256_000_000L, 8_000_000L, 16_000_000L, 16_000_000L, 1_000_000L, 128, 64, 4096, 50_000, 256_000_000L);
    }

    private static ReadOnlyRegionCatalog.Limits largeLimits() {
        return limits(4, 64, 8192, 16_000_000L, 256_000_000L, 4_000_000L, 4_000_000L, 16_000_000L, 1_000_000L, 128, 64, 4096, 50_000, 256_000_000L);
    }

    private static ReadOnlyRegionCatalog.Limits limits(int roots, int files, int chunks, long regionBytes, long totalRegionBytes, long compressedBytes, long decompressedBytes, long trackerBytes, long nbtTags, int nbtDepth, int passengerDepth, int perChunkEntities, int totalEntities, long retainedBytes) {
        return new ReadOnlyRegionCatalog.Limits(roots, files, chunks, regionBytes, totalRegionBytes, compressedBytes, decompressedBytes, trackerBytes, nbtTags, nbtDepth, passengerDepth, perChunkEntities, totalEntities, retainedBytes);
    }

    private static NBTTagCompound chunk(int x, int z, NBTTagList entities) {
        NBTTagCompound level = new NBTTagCompound();
        level.setInteger("xPos", x);
        level.setInteger("zPos", z);
        if (entities != null) {
            level.setTag("Entities", entities);
        }
        NBTTagCompound root = new NBTTagCompound();
        root.setTag("Level", level);
        return root;
    }

    private static NBTTagCompound entity(String id, UUID uuid, NBTTagList passengers) {
        NBTTagCompound result = new NBTTagCompound();
        result.setString("id", id);
        result.setLong("UUIDMost", uuid.getMostSignificantBits());
        result.setLong("UUIDLeast", uuid.getLeastSignificantBits());
        if (passengers != null) {
            result.setTag("Passengers", passengers);
        }
        return result;
    }

    private static NBTTagList entities(NBTTagCompound... entities) {
        NBTTagList list = new NBTTagList();
        for (NBTTagCompound entity : entities) {
            list.appendTag(entity);
        }
        return list;
    }

    private static NBTTagList passengers(NBTTagCompound... passengers) {
        return entities(passengers);
    }

    private static byte[] nbtBytes(NBTTagCompound root) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            CompressedStreamTools.write(root, output);
        }
        return bytes.toByteArray();
    }

    private static byte[] rawNbtWithLevelTag(int tagId, String name, RawNbtPayload payload) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeByte(10);
            output.writeUTF("");
            output.writeByte(10);
            output.writeUTF("Level");
            output.writeByte(3);
            output.writeUTF("xPos");
            output.writeInt(0);
            output.writeByte(3);
            output.writeUTF("zPos");
            output.writeInt(0);
            output.writeByte(tagId);
            output.writeUTF(name);
            payload.write(output);
            output.writeByte(0);
            output.writeByte(0);
        }
        return bytes.toByteArray();
    }

    @FunctionalInterface
    private interface RawNbtPayload {
        void write(DataOutputStream output) throws IOException;
    }

    private static NBTTagCompound readNbt(byte[] bytes) throws IOException {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes))) {
            return CompressedStreamTools.read(input, new NBTSizeTracker(8_000_000L));
        }
    }

    private static UUID uuid(long value) {
        return new UUID(0x0102030405060708L, value);
    }

    private static void writeRaw(Path path, byte[] bytes) throws IOException {
        Files.write(path, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
    }

    private static final class RegionFixture {
        private static final int SECTOR = 4096;
        private static final int HEADER = 8192;

        private static final class Payload {
            private final byte compression;
            private final byte[] compressed;

            private Payload(byte compression, byte[] compressed) {
                this.compression = compression;
                this.compressed = compressed.clone();
            }

            private byte[] compressed() {
                return compressed.clone();
            }
        }

        private static Payload payload(byte compression, byte[] nbt) throws IOException {
            return new Payload(compression, compress(compression, nbt));
        }

        private static Payload rawPayload(byte compression, byte[] compressed) {
            return new Payload(compression, compressed);
        }

        private static byte[] compress(byte compression, byte[] raw) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            if (compression == 1) {
                try (GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
                    gzip.write(raw);
                }
            } else if (compression == 2) {
                try (DeflaterOutputStream zlib = new DeflaterOutputStream(bytes)) {
                    zlib.write(raw);
                }
            } else {
                return raw.clone();
            }
            return bytes.toByteArray();
        }

        private static int allocatedSectors(Payload payload) {
            long recordLength = Math.addExact((long) payload.compressed.length, 1L);
            return Math.toIntExact(Math.addExact((recordLength + 4L) / SECTOR, 1L));
        }

        private static int slot(int x, int z) {
            return x + z * 32;
        }

        private static void write(Path file, Map<Integer, Payload> chunks) throws IOException {
            TreeMap<Integer, Payload> ordered = new TreeMap<>(chunks);
            List<Allocation> allocations = new ArrayList<>();
            long next = 2L;
            for (Map.Entry<Integer, Payload> entry : ordered.entrySet()) {
                int slot = entry.getKey();
                if (slot < 0 || slot >= 1024) {
                    throw new IllegalArgumentException("fixture slot out of range");
                }
                int sectors = allocatedSectors(entry.getValue());
                allocations.add(new Allocation(slot, Math.toIntExact(next), sectors, entry.getValue()));
                next = Math.addExact(next, sectors);
            }
            byte[] bytes = new byte[Math.toIntExact(Math.multiplyExact(next, SECTOR))];
            for (Allocation allocation : allocations) {
                int encoded = (allocation.offset << 8) | Math.min(255, allocation.sectors);
                int at = allocation.slot * 4;
                bytes[at] = (byte) (encoded >>> 24);
                bytes[at + 1] = (byte) (encoded >>> 16);
                bytes[at + 2] = (byte) (encoded >>> 8);
                bytes[at + 3] = (byte) encoded;
                int start = allocation.offset * SECTOR;
                putInt(bytes, start, allocation.payload.compressed.length + 1);
                bytes[start + 4] = allocation.payload.compression;
                System.arraycopy(allocation.payload.compressed, 0, bytes, start + 5, allocation.payload.compressed.length);
            }
            Files.write(file, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        }

        private static byte[] raw(int sectors, int offsetA, int countA, int offsetB, int countB) {
            byte[] bytes = new byte[Math.multiplyExact(sectors, SECTOR)];
            putOffset(bytes, 0, offsetA, countA);
            if (offsetB != 0 || countB != 0) {
                putOffset(bytes, 1, offsetB, countB);
            }
            return bytes;
        }

        private static void putOffset(byte[] bytes, int slot, int sector, int count) {
            int value = (sector << 8) | count;
            int at = slot * 4;
            bytes[at] = (byte) (value >>> 24);
            bytes[at + 1] = (byte) (value >>> 16);
            bytes[at + 2] = (byte) (value >>> 8);
            bytes[at + 3] = (byte) value;
        }

        private static void putRecord(byte[] bytes, int sector, byte[] record) {
            int start = sector * SECTOR;
            putInt(bytes, start, record.length);
            System.arraycopy(record, 0, bytes, start + 4, record.length);
        }

        private static void putInt(byte[] bytes, int offset, int value) {
            ByteBuffer.wrap(bytes, offset, 4).order(ByteOrder.BIG_ENDIAN).putInt(value);
        }

        private static int headerCount(Path file, int slot) throws IOException {
            byte[] header = new byte[4];
            try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
                ByteBuffer target = ByteBuffer.wrap(header);
                long offset = slot * 4L;
                while (target.hasRemaining()) {
                    int count = channel.read(target, offset);
                    if (count < 0) {
                        throw new IOException("short region fixture");
                    }
                    offset += count;
                }
            }
            int value = ((header[0] & 0xFF) << 24) | ((header[1] & 0xFF) << 16) | ((header[2] & 0xFF) << 8) | (header[3] & 0xFF);
            return value & 0xFF;
        }

        private static String sha256(byte[] bytes) throws Exception {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte value : digest) {
                result.append(Character.toUpperCase(Character.forDigit((value >>> 4) & 0xF, 16)));
                result.append(Character.toUpperCase(Character.forDigit(value & 0xF, 16)));
            }
            return result.toString();
        }

        private static final class Allocation {
            private final int slot;
            private final int offset;
            private final int sectors;
            private final Payload payload;

            private Allocation(int slot, int offset, int sectors, Payload payload) {
                this.slot = slot;
                this.offset = offset;
                this.sectors = sectors;
                this.payload = payload;
            }
        }
    }
}
