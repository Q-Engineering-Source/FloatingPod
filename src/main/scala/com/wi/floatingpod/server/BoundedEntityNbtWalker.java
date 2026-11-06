package com.wi.floatingpod.server;

import net.minecraft.nbt.CompressedStreamTools;
import net.minecraft.nbt.NBTBase;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.nbt.NBTTagList;
import net.minecraft.nbt.NBTTagLong;
import net.minecraft.nbt.NBTTagString;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

/** 在限定深度与字节预算内保留实体 NBT 子树，不创建或修改游戏实体。 */
final class BoundedEntityNbtWalker {
    List<ReadOnlyRegionCatalog.EntityOccurrence> walkWithinBudget(
        NBTTagCompound chunkNbt,
        ReadOnlyRegionCatalog.ChunkSource source,
        ReadOnlyRegionCatalog.Limits limits,
        long remainingRetainedBytes,
        int maximumEntityOccurrences
    ) throws ReadOnlyRegionCatalog.CatalogException {
        if (chunkNbt == null || source == null || limits == null) {
            throw new IllegalArgumentException("chunk NBT, source coordinates, and explicit limits are required");
        }
        if (remainingRetainedBytes < 0L || maximumEntityOccurrences < 0 || maximumEntityOccurrences > limits.maxEntityOccurrencesPerChunk()) {
            throw ReadOnlyRegionCatalog.failure("CATALOG_LIMIT", source.location(List.of()), "no retained payload budget remains", null);
        }

        validateRootIdentity(chunkNbt, source);
        validateTreeDepth(chunkNbt, source, limits);
        NBTBase entitiesTag = ((NBTTagCompound) chunkNbt.getTag("Level")).getTag("Entities");
        if (entitiesTag == null) {
            return List.of();
        }
        if (entitiesTag.getId() != 9) {
            throw ReadOnlyRegionCatalog.failure("NBT_ENTITIES", source.location(List.of()), "Level.Entities is present but is not an NBTTagList", null);
        }

        NBTTagList entities = (NBTTagList) entitiesTag;
        if (entities.tagCount() > 0 && entities.getTagType() != 10) {
            throw ReadOnlyRegionCatalog.failure("NBT_ENTITIES", source.location(List.of()), "Level.Entities list entries are not compounds", null);
        }

        ArrayList<ReadOnlyRegionCatalog.EntityOccurrence> result = new ArrayList<>();
        long[] retained = { 0L };
        for (int index = 0; index < entities.tagCount(); index++) {
            NBTBase tag = entities.get(index);
            if (tag.getId() != 10) {
                throw ReadOnlyRegionCatalog.failure("NBT_ENTITIES", source.location(List.of(index)), "Level.Entities contains a non-compound entry", null);
            }
            visitEntity((NBTTagCompound) tag, source, limits, remainingRetainedBytes, maximumEntityOccurrences, List.of(index), result, retained);
        }
        return List.copyOf(result);
    }

    private static void validateRootIdentity(
        NBTTagCompound chunkNbt,
        ReadOnlyRegionCatalog.ChunkSource source
    ) throws ReadOnlyRegionCatalog.CatalogException {
        NBTBase levelTag = chunkNbt.getTag("Level");
        if (levelTag == null || levelTag.getId() != 10) {
            throw ReadOnlyRegionCatalog.failure("NBT_LEVEL", source.location(List.of()), "chunk root has no compound Level tag", null);
        }
        NBTTagCompound level = (NBTTagCompound) levelTag;
        NBTBase xTag = level.getTag("xPos");
        NBTBase zTag = level.getTag("zPos");
        if (xTag == null || xTag.getId() != 3 || zTag == null || zTag.getId() != 3) {
            throw ReadOnlyRegionCatalog.failure("NBT_CHUNK_IDENTITY", source.location(List.of()), "Level.xPos and Level.zPos must be integer tags", null);
        }
        int observedX = ((net.minecraft.nbt.NBTTagInt) xTag).getInt();
        int observedZ = ((net.minecraft.nbt.NBTTagInt) zTag).getInt();
        if (observedX != source.chunkX() || observedZ != source.chunkZ()) {
            throw ReadOnlyRegionCatalog.failure(
                "NBT_CHUNK_IDENTITY",
                source.location(List.of()),
                "Level chunk coordinates " + observedX + "," + observedZ + " differ from region slot " + source.chunkX() + "," + source.chunkZ(),
                null
            );
        }
    }

    private static void validateTreeDepth(
        NBTTagCompound root,
        ReadOnlyRegionCatalog.ChunkSource source,
        ReadOnlyRegionCatalog.Limits limits
    ) throws ReadOnlyRegionCatalog.CatalogException {
        ArrayDeque<TagDepth> pending = new ArrayDeque<>();
        pending.push(new TagDepth(root, 1));
        while (!pending.isEmpty()) {
            TagDepth current = pending.pop();
            if (current.depth() > limits.maxNbtDepth()) {
                throw ReadOnlyRegionCatalog.failure("NBT_DEPTH_LIMIT", source.location(List.of()), "decoded NBT tree exceeds the configured depth limit", null);
            }
            NBTBase tag = current.tag();
            if (tag.getId() == 10) {
                NBTTagCompound compound = (NBTTagCompound) tag;
                for (String key : compound.getKeySet()) {
                    NBTBase child = compound.getTag(key);
                    if (child != null && (child.getId() == 9 || child.getId() == 10)) {
                        pending.push(new TagDepth(child, Math.addExact(current.depth(), 1)));
                    }
                }
            } else if (tag.getId() == 9) {
                NBTTagList list = (NBTTagList) tag;
                for (int index = 0; index < list.tagCount(); index++) {
                    NBTBase child = list.get(index);
                    if (child.getId() == 9 || child.getId() == 10) {
                        pending.push(new TagDepth(child, Math.addExact(current.depth(), 1)));
                    }
                }
            }
        }
    }

    private static void visitEntity(
        NBTTagCompound entity,
        ReadOnlyRegionCatalog.ChunkSource source,
        ReadOnlyRegionCatalog.Limits limits,
        long remainingRetainedBytes,
        int maximumEntityOccurrences,
        List<Integer> indexPath,
        ArrayList<ReadOnlyRegionCatalog.EntityOccurrence> result,
        long[] retained
    ) throws ReadOnlyRegionCatalog.CatalogException {
        if (indexPath.size() - 1 > limits.maxPassengerDepth()) {
            throw ReadOnlyRegionCatalog.failure("PASSENGER_DEPTH_LIMIT", source.location(indexPath), "passenger nesting exceeds the configured limit", null);
        }
        if (result.size() >= maximumEntityOccurrences) {
            throw ReadOnlyRegionCatalog.failure("ENTITY_COUNT_LIMIT", source.location(indexPath), "chunk entity occurrences exceed the configured limit", null);
        }

        NBTBase idTag = entity.getTag("id");
        if (idTag == null || idTag.getId() != 8) {
            throw ReadOnlyRegionCatalog.failure("NBT_ENTITY_ID", source.location(indexPath), "entity id must be a string tag", null);
        }
        String rawType = ((NBTTagString) idTag).getString();
        if (rawType.isEmpty()) {
            throw ReadOnlyRegionCatalog.failure("NBT_ENTITY_ID", source.location(indexPath), "entity id string must not be empty", null);
        }

        NBTBase mostTag = entity.getTag("UUIDMost");
        NBTBase leastTag = entity.getTag("UUIDLeast");
        int mostType = mostTag == null ? 0 : mostTag.getId();
        int leastType = leastTag == null ? 0 : leastTag.getId();
        OptionalLong most = mostType == 4 ? OptionalLong.of(((NBTTagLong) mostTag).getLong()) : OptionalLong.empty();
        OptionalLong least = leastType == 4 ? OptionalLong.of(((NBTTagLong) leastTag).getLong()) : OptionalLong.empty();
        ReadOnlyRegionCatalog.RawUuidRepresentation rawUuid =
            new ReadOnlyRegionCatalog.RawUuidRepresentation(mostType, most, leastType, least);
        if (mostType != 4 || leastType != 4) {
            throw ReadOnlyRegionCatalog.failure("NBT_ENTITY_UUID", source.location(indexPath), "entity UUIDMost and UUIDLeast must both be exact long tags", null);
        }
        UUID parsedUuid = new UUID(most.getAsLong(), least.getAsLong());

        long entityBudget = remainingRetainedBytes - retained[0];
        if (entityBudget <= 0L) {
            throw ReadOnlyRegionCatalog.failure("CATALOG_LIMIT", source.location(indexPath), "no retained payload budget remains for this entity subtree", null);
        }
        byte[] subtreeBytes;
        try {
            subtreeBytes = serializeBounded(entity, Math.min(entityBudget, limits.maxDecompressedBytesPerChunk()));
        } catch (IOException failure) {
            throw ReadOnlyRegionCatalog.failure("NBT_ENTITY_SERIALIZE", source.location(indexPath), "could not preserve complete entity subtree within the configured bound", failure);
        }

        result.add(new ReadOnlyRegionCatalog.EntityOccurrence(
            source,
            indexPath,
            rawType,
            rawUuid,
            Optional.of(parsedUuid),
            subtreeBytes
        ));
        try {
            retained[0] = Math.addExact(retained[0], subtreeBytes.length);
        } catch (ArithmeticException failure) {
            throw ReadOnlyRegionCatalog.failure("CATALOG_LIMIT", source.location(indexPath), "entity payload byte count overflowed", failure);
        }

        NBTBase passengerTag = entity.getTag("Passengers");
        if (passengerTag == null) {
            return;
        }
        if (passengerTag.getId() != 9) {
            throw ReadOnlyRegionCatalog.failure("NBT_PASSENGERS", source.location(indexPath), "Passengers is present but is not an NBTTagList", null);
        }
        NBTTagList passengers = (NBTTagList) passengerTag;
        if (passengers.tagCount() > 0 && passengers.getTagType() != 10) {
            throw ReadOnlyRegionCatalog.failure("NBT_PASSENGERS", source.location(indexPath), "Passengers list entries are not compounds", null);
        }
        for (int index = 0; index < passengers.tagCount(); index++) {
            NBTBase passenger = passengers.get(index);
            if (passenger.getId() != 10) {
                throw ReadOnlyRegionCatalog.failure("NBT_PASSENGERS", source.location(indexPath), "Passengers contains a non-compound entry", null);
            }
            ArrayList<Integer> childPath = new ArrayList<>(indexPath);
            childPath.add(index);
            visitEntity((NBTTagCompound) passenger, source, limits, remainingRetainedBytes, maximumEntityOccurrences, List.copyOf(childPath), result, retained);
        }
    }

    private static byte[] serializeBounded(NBTTagCompound entity, long maximumBytes) throws IOException {
        if (maximumBytes <= 0L) {
            throw new IOException("no entity subtree payload budget remains");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(new BoundedOutputStream(bytes, maximumBytes))) {
            CompressedStreamTools.write(entity, output);
        }
        return bytes.toByteArray();
    }

    private record TagDepth(NBTBase tag, int depth) {
    }

    private static final class BoundedOutputStream extends OutputStream {
        private final ByteArrayOutputStream target;
        private final long limit;

        private BoundedOutputStream(ByteArrayOutputStream target, long limit) {
            this.target = target;
            this.limit = limit;
        }

        @Override
        public void write(int value) throws IOException {
            ensureCapacity(1);
            target.write(value);
        }

        @Override
        public void write(byte[] source, int offset, int length) throws IOException {
            if (source == null) {
                throw new NullPointerException("source");
            }
            if (offset < 0 || length < 0 || offset > source.length - length) {
                throw new IndexOutOfBoundsException();
            }
            ensureCapacity(length);
            target.write(source, offset, length);
        }

        private void ensureCapacity(int additional) throws IOException {
            if ((long) target.size() + additional > limit) {
                throw new IOException("entity subtree exceeds the explicit payload bound");
            }
        }
    }
}