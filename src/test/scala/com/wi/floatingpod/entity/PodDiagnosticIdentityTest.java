package com.wi.floatingpod.entity;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.UUID;
import net.minecraft.nbt.CompressedStreamTools;
import net.minecraft.nbt.NBTSizeTracker;
import net.minecraft.nbt.NBTTagCompound;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** 实际身份/NBT模块的纯行为测试；不构造World、实体或假造数据库/业务资格。 */
class PodDiagnosticIdentityTest {
    private static final UUID POD=new UUID(4,5), BODY=new UUID(6,7), OTHER=new UUID(8,9);
    private static NBTTagCompound root(UUID body) {
        var nbt=new NBTTagCompound();nbt.setUniqueId("UUID",body);
        var caps=new NBTTagCompound();caps.setByteArray("opaque",new byte[]{1,0,-1});
        nbt.setTag("ForgeCaps",caps);nbt.setString("unknown","x\u0000雪");
        return nbt;
    }

    @Test void firstWorldLoadSeparatesPhysicalIdWithoutChangingOriginalInputOrArtifact() {
        var raw=root(POD);
        var artifact=new PodArtifactIdentity(BODY,OTHER,PodArtifactIdentity.Origin.DEBUG_ITEM_CREATE);
        PodArtifactNbtCodec.write(raw,artifact);
        var before=raw.copy();
        var diagnostic=PodDiagnosticIdentity.fromWorldNbt(raw,POD);
        assertEquals(POD,diagnostic.podUuid());assertNotEquals(POD,diagnostic.bodyUuid());
        assertEquals(before,raw,"load observation must not rewrite source payload");
        assertEquals(artifact,PodArtifactNbtCodec.read(raw).orElseThrow());
    }

    @Test void duplicateOldBodiesRetainSameSourcePodButHaveIndependentPhysicalIds() {
        var raw=root(POD);
        var first=PodDiagnosticIdentity.fromWorldNbt(raw,POD);
        var second=PodDiagnosticIdentity.fromWorldNbt(raw,POD);
        assertEquals(first.podUuid(),second.podUuid());assertNotEquals(first.bodyUuid(),second.bodyUuid());
        assertNotEquals(POD,first.bodyUuid());assertNotEquals(POD,second.bodyUuid());
    }

    @Test void savedDiagnosticIdentitySurvivesBinaryNbtAndReloadWithoutReminting() throws Exception {
        var identity=new PodDiagnosticIdentity(POD,BODY);var saved=root(BODY);
        identity.write(saved);
        var bytes=new ByteArrayOutputStream();CompressedStreamTools.write(saved,new DataOutputStream(bytes));
        var reloaded=CompressedStreamTools.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())),new NBTSizeTracker(1000000L));
        assertEquals(identity,PodDiagnosticIdentity.fromWorldNbt(reloaded,BODY));
        assertEquals(BODY,reloaded.getUniqueId("UUID"));
        assertEquals("x\u0000雪",reloaded.getString("unknown"));
        assertArrayEquals(new byte[]{1,0,-1},reloaded.getCompoundTag("ForgeCaps").getByteArray("opaque"));
        assertTrue(PodArtifactNbtCodec.read(reloaded).isEmpty(),"unmarked legacy does not invent an artifact");
        identity.write(reloaded);assertEquals(saved,reloaded);
    }

    @Test void malformedExistingIdentityCannotFallBackToLegacyOrAcquireNewPhysicalId() {
        var valid=root(BODY);new PodDiagnosticIdentity(POD,BODY).write(valid);
        var wrongType=valid.copy();wrongType.setString(PodDiagnosticIdentity.ROOT_KEY,"bad");
        var missing=valid.copy();missing.getCompoundTag(PodDiagnosticIdentity.ROOT_KEY).removeTag("PodMost");
        var wrongField=valid.copy();wrongField.getCompoundTag(PodDiagnosticIdentity.ROOT_KEY).setInteger("BodyMost",6);
        var version=valid.copy();version.getCompoundTag(PodDiagnosticIdentity.ROOT_KEY).setInteger("Format",2);
        var extra=valid.copy();extra.getCompoundTag(PodDiagnosticIdentity.ROOT_KEY).setLong("extra",0);
        var occupied=valid.copy();occupied.getCompoundTag(PodDiagnosticIdentity.ROOT_KEY).setLong("PodMost",BODY.getMostSignificantBits());
        occupied.getCompoundTag(PodDiagnosticIdentity.ROOT_KEY).setLong("PodLeast",BODY.getLeastSignificantBits());
        var outer=valid.copy();outer.setUniqueId("UUID",OTHER);
        for(var bad:new NBTTagCompound[]{wrongType,missing,wrongField,version,extra,occupied,outer}) {
            assertThrows(IllegalArgumentException.class,()->PodDiagnosticIdentity.fromWorldNbt(bad,BODY));
            assertThrows(IllegalArgumentException.class,()->PodDiagnosticIdentity.requireValid(bad));
        }
        assertThrows(IllegalArgumentException.class,()->PodDiagnosticIdentity.fromWorldNbt(valid,OTHER));
    }

    @Test void writeCannotReplaceSourcePodOrMismatchOuterPhysicalBody() {
        var identity=new PodDiagnosticIdentity(POD,BODY);var raw=root(BODY);identity.write(raw);
        var before=raw.copy();
        assertThrows(IllegalArgumentException.class,()->new PodDiagnosticIdentity(OTHER,BODY).write(raw));
        assertEquals(before,raw);
        assertThrows(IllegalArgumentException.class,()->identity.write(root(OTHER)));
        assertThrows(IllegalArgumentException.class,()->new PodDiagnosticIdentity(POD,POD));
    }

    @Test void payloadMatchDistinguishesDiagnosticFromActiveAndPreservesExactSourceIdentity() {
        var identity=new PodDiagnosticIdentity(POD,BODY);var raw=root(BODY);identity.write(raw);
        PodDiagnosticIdentity.requireValid(raw);
        assertTrue(PodDiagnosticIdentity.matches(raw,identity));
        assertFalse(PodDiagnosticIdentity.matches(raw,null));
        assertFalse(PodDiagnosticIdentity.matches(raw,new PodDiagnosticIdentity(OTHER,BODY)));
        assertFalse(PodDiagnosticIdentity.matches(root(BODY),identity));
        assertTrue(PodDiagnosticIdentity.matches(root(BODY),null));
    }
}
