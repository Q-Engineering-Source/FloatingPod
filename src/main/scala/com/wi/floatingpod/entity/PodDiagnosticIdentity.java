package com.wi.floatingpod.entity;

import java.util.Objects;
import java.util.UUID;
import net.minecraft.nbt.NBTTagCompound;

/** 世界旧壳的原Pod与独立物理体身份；不构成数据库归属、活动或ticket资格。 */
public record PodDiagnosticIdentity(UUID podUuid, UUID bodyUuid) {
    public static final String ROOT_KEY = "FloatingPodDiagnostic";

    public PodDiagnosticIdentity {
        Objects.requireNonNull(podUuid, "Original diagnostic Pod UUID");
        Objects.requireNonNull(bodyUuid, "Diagnostic physical UUID");
        if (podUuid.equals(bodyUuid)) throw new IllegalArgumentException("Diagnostic body must not occupy its original Pod UUID");
    }

    /** vanilla已读取外层UUID，尚未加入世界索引；首次迁移只改变物理UUID，重载沿用已保存身份。 */
    public static PodDiagnosticIdentity fromWorldNbt(NBTTagCompound root, UUID loadedBody) {
        Objects.requireNonNull(root);Objects.requireNonNull(loadedBody);
        requireOuterUuid(root, loadedBody.getMostSignificantBits(), loadedBody.getLeastSignificantBits());
        if (!root.hasKey(ROOT_KEY)) return new PodDiagnosticIdentity(loadedBody, UUID.randomUUID());
        requireValid(root);
        NBTTagCompound tag = root.getCompoundTag(ROOT_KEY);
        UUID body = new UUID(tag.getLong("BodyMost"), tag.getLong("BodyLeast"));
        if (!body.equals(loadedBody)) throw new IllegalArgumentException("Diagnostic body differs from loaded vanilla UUID");
        return new PodDiagnosticIdentity(new UUID(tag.getLong("PodMost"), tag.getLong("PodLeast")), body);
    }

    /** 保存前核外层physical UUID；不能把原Pod字段覆盖成新值或给活动快照添加诊断身份。 */
    public void write(NBTTagCompound root) {
        requireOuterUuid(root, bodyUuid.getMostSignificantBits(), bodyUuid.getLeastSignificantBits());
        if (root.hasKey(ROOT_KEY)) {
            requireValid(root);
            if (!matches(root, this)) throw new IllegalArgumentException("Diagnostic identity cannot be replaced");
        }
        NBTTagCompound tag = new NBTTagCompound();
        tag.setInteger("Format", 1);
        tag.setLong("PodMost", podUuid.getMostSignificantBits());tag.setLong("PodLeast", podUuid.getLeastSignificantBits());
        tag.setLong("BodyMost", bodyUuid.getMostSignificantBits());tag.setLong("BodyLeast", bodyUuid.getLeastSignificantBits());
        root.setTag(ROOT_KEY, tag);
    }

    /** 最终payload逐条核验，不分配UUID/身份对象；缺key才表示非诊断体。 */
    public static void requireValid(NBTTagCompound root) {
        Objects.requireNonNull(root);
        if (!root.hasKey(ROOT_KEY)) return;
        if (root.getTagId(ROOT_KEY) != 10) throw new IllegalArgumentException("Diagnostic identity must be a compound");
        NBTTagCompound tag = root.getCompoundTag(ROOT_KEY);
        if (tag.getKeySet().size()!=5 || tag.getTagId("Format")!=3 || tag.getInteger("Format")!=1
                || tag.getTagId("PodMost")!=4 || tag.getTagId("PodLeast")!=4
                || tag.getTagId("BodyMost")!=4 || tag.getTagId("BodyLeast")!=4)
            throw new IllegalArgumentException("Exact diagnostic identity fields/types/version required");
        if (tag.getLong("PodMost")==tag.getLong("BodyMost") && tag.getLong("PodLeast")==tag.getLong("BodyLeast"))
            throw new IllegalArgumentException("Diagnostic identity cannot occupy business Pod UUID");
        requireOuterUuid(root, tag.getLong("BodyMost"), tag.getLong("BodyLeast"));
    }

    /** requireValid之后比对实际旧壳；null要求payload没有诊断身份。 */
    public static boolean matches(NBTTagCompound root, PodDiagnosticIdentity expected) {
        if (expected==null) return !root.hasKey(ROOT_KEY);
        if (root.getTagId(ROOT_KEY)!=10) return false;
        NBTTagCompound tag=root.getCompoundTag(ROOT_KEY);
        return tag.getLong("PodMost")==expected.podUuid.getMostSignificantBits()
                && tag.getLong("PodLeast")==expected.podUuid.getLeastSignificantBits()
                && tag.getLong("BodyMost")==expected.bodyUuid.getMostSignificantBits()
                && tag.getLong("BodyLeast")==expected.bodyUuid.getLeastSignificantBits();
    }

    private static void requireOuterUuid(NBTTagCompound root, long most, long least) {
        if (root.getTagId("UUIDMost")!=4 || root.getTagId("UUIDLeast")!=4
                || root.getLong("UUIDMost")!=most || root.getLong("UUIDLeast")!=least)
            throw new IllegalArgumentException("Diagnostic identity differs from exact outer physical UUID");
    }
}
