package com.wi.floatingpod.entity;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import net.minecraft.nbt.NBTTagCompound;

/** 物理artifact身份子树的严格编解码器。 */
public final class PodArtifactNbtCodec {
    public static final String ROOT_KEY = "FloatingPodArtifact";

    private static final int FORMAT_VERSION = 1;
    private static final int TAG_INT = 3;
    private static final int TAG_STRING = 8;
    private static final int TAG_COMPOUND = 10;
    private static final Set<String> EXACT_FIELDS = Set.of("Format", "Binding", "Attempt", "Origin");

    private PodArtifactNbtCodec() {
    }

    /**
     * 解码artifact子树。仅根key缺失属于旧格式；任何已存在但畸形的值都明确失败。
     */
    public static Optional<PodArtifactIdentity> read(NBTTagCompound root) {
        Objects.requireNonNull(root, "root");

        if (!root.hasKey(ROOT_KEY)) {
            return Optional.empty();
        }
        int rootType = root.getTagId(ROOT_KEY);
        if (rootType != TAG_COMPOUND) {
            throw new IllegalArgumentException("FloatingPodArtifact must be an NBT compound");
        }

        NBTTagCompound artifact = root.getCompoundTag(ROOT_KEY);
        for (String field : EXACT_FIELDS) {
            if (!artifact.hasKey(field)) {
                throw new IllegalArgumentException("FloatingPodArtifact is missing required field " + field);
            }
        }
        if (artifact.getKeySet().size() != EXACT_FIELDS.size()) {
            throw new IllegalArgumentException("FloatingPodArtifact contains an unsupported field");
        }

        requireType(artifact, "Format", TAG_INT);
        int format = artifact.getInteger("Format");
        if (format != FORMAT_VERSION) {
            throw new IllegalArgumentException("Unsupported FloatingPodArtifact format: " + format);
        }

        requireType(artifact, "Binding", TAG_STRING);
        requireType(artifact, "Attempt", TAG_STRING);
        requireType(artifact, "Origin", TAG_STRING);

        UUID binding = canonicalUuid("Binding", artifact.getString("Binding"));
        UUID attempt = canonicalUuid("Attempt", artifact.getString("Attempt"));
        PodArtifactIdentity.Origin origin = origin(artifact.getString("Origin"));
        return Optional.of(new PodArtifactIdentity(binding, attempt, origin));
    }

    /** 先验证输入与现有子树，再一次性安装完整的新子树。 */
    public static void write(NBTTagCompound root, PodArtifactIdentity identity) {
        Objects.requireNonNull(root, "root");
        if (identity == null) {
            throw new IllegalArgumentException("identity must not be null");
        }

        Optional<PodArtifactIdentity> current = read(root);
        if (current.isPresent() && !current.orElseThrow().equals(identity)) {
            throw new IllegalArgumentException("FloatingPodArtifact already contains a different identity");
        }

        NBTTagCompound artifact = new NBTTagCompound();
        artifact.setInteger("Format", FORMAT_VERSION);
        artifact.setString("Binding", identity.bindingUuid().toString());
        artifact.setString("Attempt", identity.attemptUuid().toString());
        artifact.setString("Origin", originWire(identity.origin()));
        root.setTag(ROOT_KEY, artifact);
    }

    private static void requireType(NBTTagCompound compound, String key, int expectedType) {
        int actualType = compound.getTagId(key);
        if (actualType != expectedType) {
            throw new IllegalArgumentException("FloatingPodArtifact." + key + " has NBT type " + actualType
                    + "; expected " + expectedType);
        }
    }

    private static UUID canonicalUuid(String field, String value) {
        UUID parsed;
        try {
            parsed = UUID.fromString(value);
        } catch (IllegalArgumentException cause) {
            throw new IllegalArgumentException("FloatingPodArtifact." + field + " is not a valid UUID", cause);
        }
        if (!parsed.toString().equals(value)) {
            throw new IllegalArgumentException("FloatingPodArtifact." + field + " is not a canonical UUID");
        }
        return parsed;
    }

    private static String originWire(PodArtifactIdentity.Origin origin) {
        return switch (origin) {
            case OWNED_RELEASE -> "OWNED_RELEASE";
            case UNBOUND_ADOPTION -> "UNBOUND_ADOPTION";
            case DEBUG_ITEM_CREATE -> "DEBUG_ITEM_CREATE";
        };
    }

    private static PodArtifactIdentity.Origin origin(String wireValue) {
        return switch (wireValue) {
            case "OWNED_RELEASE" -> PodArtifactIdentity.Origin.OWNED_RELEASE;
            case "UNBOUND_ADOPTION" -> PodArtifactIdentity.Origin.UNBOUND_ADOPTION;
            case "DEBUG_ITEM_CREATE" -> PodArtifactIdentity.Origin.DEBUG_ITEM_CREATE;
            default -> throw new IllegalArgumentException("FloatingPodArtifact.Origin has an unknown wire value");
        };
    }
}
