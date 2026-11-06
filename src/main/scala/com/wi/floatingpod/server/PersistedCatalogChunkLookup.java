package com.wi.floatingpod.server;

import java.util.UUID;

/** 将完整 catalog 中已观察到的 region slot 映射到精确 chunk 坐标。 */
public final class PersistedCatalogChunkLookup {
    private PersistedCatalogChunkLookup() {
    }

    public static SlotObservation lookup(
        ReadOnlyRegionCatalog.CatalogSnapshot snapshot,
        String rootIdentity,
        int chunkX,
        int chunkZ
    ) {
        if (snapshot == null || rootIdentity == null || rootIdentity.isBlank()) {
            throw new IllegalArgumentException("a completed catalog snapshot and explicit root identity are required");
        }

        int regionX = Math.floorDiv(chunkX, 32);
        int regionZ = Math.floorDiv(chunkZ, 32);
        int localX = Math.floorMod(chunkX, 32);
        int localZ = Math.floorMod(chunkZ, 32);

        boolean suppliedRoot = snapshot.roots().stream()
            .anyMatch(root -> root.root().identity().equals(rootIdentity));
        if (!suppliedRoot) {
            return new Unobserved(snapshot, rootIdentity, chunkX, chunkZ, UnobservedReason.ROOT_IDENTITY_UNKNOWN);
        }

        for (ReadOnlyRegionCatalog.RegionSnapshot region : snapshot.regions()) {
            if (region.rootIdentity().equals(rootIdentity) && region.regionX() == regionX && region.regionZ() == regionZ) {
                return new ValidatedSlot(snapshot, region, region.slot(localX, localZ));
            }
        }
        return new Unobserved(snapshot, rootIdentity, chunkX, chunkZ, UnobservedReason.REGION_NOT_OBSERVED);
    }

    public enum UnobservedReason {
        ROOT_IDENTITY_UNKNOWN,
        REGION_NOT_OBSERVED
    }

    public sealed interface SlotObservation permits ValidatedSlot, Unobserved {
        ReadOnlyRegionCatalog.CatalogSnapshot snapshot();

        default String worldIdentity() {
            return snapshot().worldIdentity();
        }

        default UUID generation() {
            return snapshot().generation();
        }

        default ReadOnlyRegionCatalog.ManifestPin manifestPin() {
            return snapshot().manifestPin();
        }
    }

    public static final class ValidatedSlot implements SlotObservation {
        private final ReadOnlyRegionCatalog.CatalogSnapshot snapshot;
        private final ReadOnlyRegionCatalog.RegionSnapshot region;
        private final ReadOnlyRegionCatalog.SlotSnapshot slot;

        private ValidatedSlot(
            ReadOnlyRegionCatalog.CatalogSnapshot snapshot,
            ReadOnlyRegionCatalog.RegionSnapshot region,
            ReadOnlyRegionCatalog.SlotSnapshot slot
        ) {
            this.snapshot = snapshot;
            this.region = region;
            this.slot = slot;
        }

        @Override
        public ReadOnlyRegionCatalog.CatalogSnapshot snapshot() {
            return snapshot;
        }

        public ReadOnlyRegionCatalog.RegionSnapshot region() {
            return region;
        }

        public ReadOnlyRegionCatalog.SlotSnapshot slot() {
            return slot;
        }
    }

    public static final class Unobserved implements SlotObservation {
        private final ReadOnlyRegionCatalog.CatalogSnapshot snapshot;
        private final String rootIdentity;
        private final int chunkX;
        private final int chunkZ;
        private final UnobservedReason reason;

        private Unobserved(
            ReadOnlyRegionCatalog.CatalogSnapshot snapshot,
            String rootIdentity,
            int chunkX,
            int chunkZ,
            UnobservedReason reason
        ) {
            this.snapshot = snapshot;
            this.rootIdentity = rootIdentity;
            this.chunkX = chunkX;
            this.chunkZ = chunkZ;
            this.reason = reason;
        }

        @Override
        public ReadOnlyRegionCatalog.CatalogSnapshot snapshot() {
            return snapshot;
        }

        public int chunkX() {
            return chunkX;
        }

        public String rootIdentity() {
            return rootIdentity;
        }

        public int chunkZ() {
            return chunkZ;
        }

        public UnobservedReason reason() {
            return reason;
        }
    }
}
