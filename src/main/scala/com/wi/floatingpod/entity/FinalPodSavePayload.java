package com.wi.floatingpod.entity;

import java.util.UUID;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.nbt.NBTTagList;

/** 最终世界保存的物理身份核对；artifact只描述来源，不授予业务资格。 */
public final class FinalPodSavePayload {
    private FinalPodSavePayload() { }

    public static void requireValid(NBTTagCompound body, String podId) {
        if (body.getTagId("id") != 8 || !podId.equals(body.getString("id"))
                || body.getTagId("UUIDMost") != 4 || body.getTagId("UUIDLeast") != 4)
            throw new IllegalStateException("Exact final FloatingPod id/UUID required");
        NBTTagList pos = list(body, "Pos", 6, 3), rotation = list(body, "Rotation", 5, 2);
        for (int i=0;i<3;i++) if (!Double.isFinite(pos.getDoubleAt(i)))
            throw new IllegalStateException("Finite final FloatingPod position required");
        for (int i=0;i<2;i++) if (!Float.isFinite(rotation.getFloatAt(i)))
            throw new IllegalStateException("Finite final FloatingPod rotation required");
        PodDiagnosticIdentity.requireValid(body);
        if (body.getTag(PodArtifactNbtCodec.ROOT_KEY) == null) return; // 无标记legacy诊断壳仍保持无标记。
        if (body.getTagId(PodArtifactNbtCodec.ROOT_KEY) != 10)
            throw new IllegalStateException("Exact final artifact compound required");
        NBTTagCompound artifact = body.getCompoundTag(PodArtifactNbtCodec.ROOT_KEY);
        if (artifact.getKeySet().size()!=4 || artifact.getTagId("Format")!=3 || artifact.getInteger("Format")!=1
                || artifact.getTagId("Binding")!=8 || artifact.getTagId("Attempt")!=8 || artifact.getTagId("Origin")!=8)
            throw new IllegalStateException("Exact final artifact fields/types required");
        requireUuid(artifact.getString("Binding")); requireUuid(artifact.getString("Attempt"));
        String origin=artifact.getString("Origin");
        if (!origin.equals("OWNED_RELEASE") && !origin.equals("UNBOUND_ADOPTION") && !origin.equals("DEBUG_ITEM_CREATE"))
            throw new IllegalStateException("Final artifact origin invalid");
    }

    /** requireValid之后可反复比对；不反复分配UUID、artifact或key-set视图。 */
    public static boolean matches(NBTTagCompound body, UUID pod, double x, double y, double z,
            float yaw, float pitch, PodArtifactIdentity artifact) {
        return matches(body,pod,x,y,z,yaw,pitch,artifact,null);
    }

    public static boolean matches(NBTTagCompound body, UUID pod, double x, double y, double z,
            float yaw, float pitch, PodArtifactIdentity artifact, PodDiagnosticIdentity diagnostic) {
        if (!PodDiagnosticIdentity.matches(body,diagnostic)) return false;
        if (body.getLong("UUIDMost")!=pod.getMostSignificantBits() || body.getLong("UUIDLeast")!=pod.getLeastSignificantBits()) return false;
        NBTTagList pos=body.getTagList("Pos",6), rotation=body.getTagList("Rotation",5);
        if (pos.getDoubleAt(0)!=x || pos.getDoubleAt(1)!=y || pos.getDoubleAt(2)!=z
                || rotation.getFloatAt(0)!=yaw || rotation.getFloatAt(1)!=pitch) return false;
        if (artifact==null) return body.getTag(PodArtifactNbtCodec.ROOT_KEY)==null;
        if (body.getTagId(PodArtifactNbtCodec.ROOT_KEY)!=10) return false;
        NBTTagCompound saved=body.getCompoundTag(PodArtifactNbtCodec.ROOT_KEY);
        return uuidMatches(saved.getString("Binding"),artifact.bindingUuid())
                && uuidMatches(saved.getString("Attempt"),artifact.attemptUuid())
                && saved.getString("Origin").equals(artifact.origin().name());
    }

    private static NBTTagList list(NBTTagCompound body,String key,int type,int size) {
        if (body.getTagId(key)!=9) throw new IllegalStateException("Exact final pose list required");
        NBTTagList result=body.getTagList(key,type);
        if (result.tagCount()!=size || result.getTagType()!=type)
            throw new IllegalStateException("Exact final pose element type/count required");
        return result;
    }
    private static void requireUuid(String value) {
        if (value.length()!=36) throw new IllegalStateException("Canonical final artifact UUID required");
        for (int i=0;i<36;i++) {
            char c=value.charAt(i);boolean dash=i==8||i==13||i==18||i==23;
            if (dash ? c!='-' : !((c>='0'&&c<='9')||(c>='a'&&c<='f')))
                throw new IllegalStateException("Canonical final artifact UUID required");
        }
    }
    private static boolean uuidMatches(String value,UUID expected) {
        // 格式已经严格验证；用原bits核对，不为每次候选匹配创建UUID/String。
        long most=0,least=0;int digits=0;
        for (int i=0;i<36;i++) {
            char c=value.charAt(i);if(c=='-')continue;
            int n=c<='9'?c-'0':c-'a'+10;
            if(digits++<16)most=(most<<4)|n;else least=(least<<4)|n;
        }
        return most==expected.getMostSignificantBits() && least==expected.getLeastSignificantBits();
    }
}
