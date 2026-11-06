package com.wi.floatingpod.entity;

import java.util.UUID;
import net.minecraft.nbt.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** 真实NBT/codec与最终保存身份核对；不构造游戏World或冒充Main保存许可。 */
final class FinalPodSavePayloadTest {
    @Test void diagnosticPayloadMatchesOnlyExactPhysicalAndOriginalPodIdentity(){
        var identity=new PodDiagnosticIdentity(ATTEMPT,POD);var raw=body(null);identity.write(raw);
        FinalPodSavePayload.requireValid(raw,ID);
        assertFalse(matches(raw,null));
        assertTrue(FinalPodSavePayload.matches(raw,POD,-33.25,70.5,20.75,-179.5f,30f,null,identity));
        assertFalse(FinalPodSavePayload.matches(raw,POD,-33.25,70.5,20.75,-179.5f,30f,null,new PodDiagnosticIdentity(BINDING,POD)));
        assertFalse(FinalPodSavePayload.matches(body(null),POD,-33.25,70.5,20.75,-179.5f,30f,null,identity));
    }
    @Test void malformedDiagnosticPayloadCannotPassAsUnmarkedLegacy(){
        var malformed=body(null);malformed.setString(PodDiagnosticIdentity.ROOT_KEY,"invalid");
        assertThrows(IllegalArgumentException.class,()->FinalPodSavePayload.requireValid(malformed,ID));
        var identity=new PodDiagnosticIdentity(ATTEMPT,POD);var raw=body(null);identity.write(raw);
        raw.setUniqueId("UUID",BINDING);
        var mismatched=raw;
        assertThrows(IllegalArgumentException.class,()->FinalPodSavePayload.requireValid(mismatched,ID));
    }
    private static final String ID="floatingpod:floatingpod";
    private static final UUID POD=UUID.fromString("81234567-89ab-cdef-fedc-ba9876543210");
    private static final UUID BINDING=UUID.fromString("00000000-0000-0000-0000-000000000000");
    private static final UUID ATTEMPT=UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");
    private static NBTTagCompound body(PodArtifactIdentity artifact){
        NBTTagCompound result=new NBTTagCompound();result.setString("id",ID);
        result.setLong("UUIDMost",POD.getMostSignificantBits());result.setLong("UUIDLeast",POD.getLeastSignificantBits());
        NBTTagList pos=new NBTTagList();pos.appendTag(new NBTTagDouble(-33.25));pos.appendTag(new NBTTagDouble(70.5));pos.appendTag(new NBTTagDouble(20.75));
        NBTTagList rot=new NBTTagList();rot.appendTag(new NBTTagFloat(-179.5f));rot.appendTag(new NBTTagFloat(30f));
        result.setTag("Pos",pos);result.setTag("Rotation",rot);
        if(artifact!=null)PodArtifactNbtCodec.write(result,artifact);
        return result;
    }
    private static boolean matches(NBTTagCompound body,PodArtifactIdentity artifact){
        return FinalPodSavePayload.matches(body,POD,-33.25,70.5,20.75,-179.5f,30f,artifact);
    }
    @Test void unmarkedDiagnosticShellRemainsUnmarkedAndHasNoInventedArtifact(){
        var raw=body(null);FinalPodSavePayload.requireValid(raw,ID);assertTrue(matches(raw,null));
        assertTrue(PodArtifactNbtCodec.read(raw).isEmpty());
        assertFalse(matches(raw,new PodArtifactIdentity(BINDING,ATTEMPT,PodArtifactIdentity.Origin.OWNED_RELEASE)));
    }
    @Test void everyOriginalArtifactOriginMatchesExactBitsWithoutCreatingNewAuthority(){
        for(var origin:PodArtifactIdentity.Origin.values()){
            var identity=new PodArtifactIdentity(BINDING,ATTEMPT,origin);var raw=body(identity);
            FinalPodSavePayload.requireValid(raw,ID);assertTrue(matches(raw,identity));
            assertEquals(identity,PodArtifactNbtCodec.read(raw).orElseThrow());
            assertFalse(matches(raw,null));
            assertFalse(matches(raw,new PodArtifactIdentity(ATTEMPT,BINDING,origin)));
            var other=origin==PodArtifactIdentity.Origin.OWNED_RELEASE?PodArtifactIdentity.Origin.UNBOUND_ADOPTION:PodArtifactIdentity.Origin.OWNED_RELEASE;
            assertFalse(matches(raw,new PodArtifactIdentity(BINDING,ATTEMPT,other)));
        }
    }
    @Test void changedUuidOrPoseCannotMatchOriginalBody(){
        var raw=body(null);FinalPodSavePayload.requireValid(raw,ID);
        assertFalse(FinalPodSavePayload.matches(raw,ATTEMPT,-33.25,70.5,20.75,-179.5f,30f,null));
        assertFalse(FinalPodSavePayload.matches(raw,POD,-33.0,70.5,20.75,-179.5f,30f,null));
        assertFalse(FinalPodSavePayload.matches(raw,POD,-33.25,70.5,20.75,-179f,30f,null));
        assertFalse(FinalPodSavePayload.matches(raw,POD,-33.25,70.5,20.75,-179.5f,31f,null));
    }
    @Test void malformedArtifactCannotBeTreatedAsUnmarkedLegacy(){
        var identity=new PodArtifactIdentity(BINDING,ATTEMPT,PodArtifactIdentity.Origin.DEBUG_ITEM_CREATE);
        var wrongType=body(null);wrongType.setString(PodArtifactNbtCodec.ROOT_KEY,"bad");
        assertThrows(IllegalStateException.class,()->FinalPodSavePayload.requireValid(wrongType,ID));
        var extra=body(identity);extra.getCompoundTag(PodArtifactNbtCodec.ROOT_KEY).setInteger("Unexpected",1);
        assertThrows(IllegalStateException.class,()->FinalPodSavePayload.requireValid(extra,ID));
        var invalid=body(identity);invalid.getCompoundTag(PodArtifactNbtCodec.ROOT_KEY).setString("Binding","FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF");
        assertThrows(IllegalStateException.class,()->FinalPodSavePayload.requireValid(invalid,ID));
        var origin=body(identity);origin.getCompoundTag(PodArtifactNbtCodec.ROOT_KEY).setString("Origin","UNKNOWN");
        assertThrows(IllegalStateException.class,()->FinalPodSavePayload.requireValid(origin,ID));
        var missing=body(identity);missing.getCompoundTag(PodArtifactNbtCodec.ROOT_KEY).removeTag("Attempt");
        assertThrows(IllegalStateException.class,()->FinalPodSavePayload.requireValid(missing,ID));
    }
    @Test void foreignEntityWrongTypesAndNonfinitePoseAreExplicitFailures(){
        var foreign=body(null);foreign.setString("id","minecraft:pig");
        assertThrows(IllegalStateException.class,()->FinalPodSavePayload.requireValid(foreign,ID));
        var wrongUuid=body(null);wrongUuid.setString("UUIDMost",POD.toString());
        assertThrows(IllegalStateException.class,()->FinalPodSavePayload.requireValid(wrongUuid,ID));
        var nan=body(null);nan.getTagList("Pos",6).set(0,new NBTTagDouble(Double.NaN));
        assertThrows(IllegalStateException.class,()->FinalPodSavePayload.requireValid(nan,ID));
        var badRotation=body(null);badRotation.getTagList("Rotation",5).set(0,new NBTTagFloat(Float.POSITIVE_INFINITY));
        assertThrows(IllegalStateException.class,()->FinalPodSavePayload.requireValid(badRotation,ID));
        var truncated=body(null);truncated.getTagList("Pos",6).removeTag(2);
        assertThrows(IllegalStateException.class,()->FinalPodSavePayload.requireValid(truncated,ID));
    }
}
