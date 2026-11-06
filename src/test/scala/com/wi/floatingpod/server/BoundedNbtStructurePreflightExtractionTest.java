package com.wi.floatingpod.server;

import com.wi.floatingpod.config.SourceLimitProfile;
import org.junit.jupiter.api.Test;
import java.io.*;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

final class BoundedNbtStructurePreflightExtractionTest {
    static SourceLimitProfile limits(long members,long depth,long retention){return SourceLimitProfile.of(1_000_000,100_000,100_000,depth,members,retention,1_000_000,65536);}
    @Test void oneExistingCursorPreservesAllTwelveTypesCapsModifiedUtfAndRawBits()throws Exception {
        byte[] bytes=allTypes();var root=BoundedNbtStructurePreflight.extract(bytes,limits(4096,64,10_000_000));
        assertEquals(12,root.children().size());for(int type=1;type<=12;type++)assertEquals(type,root.child("t"+type).type());
        assertEquals(0x7ff8000000000042L,root.child("t6").integerBits());assertEquals(0x80000000,root.child("t5").intBits());assertEquals("x\u0000\u2603",root.child("t8").stringValue());
        byte[] source=root.payloadBytes();bytes[3]=0;assertArrayEquals(source,root.payloadBytes());assertThrows(UnsupportedOperationException.class,()->root.children().clear());
        byte[] standalone=new byte[source.length+3];standalone[0]=10;System.arraycopy(source,0,standalone,3,source.length);assertTrue(root.matchesStandaloneCompound(standalone));standalone[standalone.length-1]=1;assertFalse(root.matchesStandaloneCompound(standalone));
    }
    @Test void exactMemberTagBudgetBoundaryRejectsBeforeExtraSemanticNode()throws Exception {
        byte[] bytes=twoBytes();assertEquals(2,BoundedNbtStructurePreflight.extract(bytes,limits(3,64,100_000)).children().size());
        var failure=assertThrows(BoundedNbtStructurePreflight.PreflightException.class,()->BoundedNbtStructurePreflight.extract(bytes,limits(2,64,100_000)));
        assertEquals(BoundedNbtStructurePreflight.Code.TAG_BUDGET_EXCEEDED,failure.code());
    }
    @Test void duplicateNamesHaveNoAmbiguousSemanticGraphWhileLegacyValidationRemainsCompatible()throws Exception {
        byte[] duplicate=namedBytes(true);ReadOnlyRegionCatalog.Limits old=legacy();BoundedNbtStructurePreflight.validate(duplicate,old);
        var failure=assertThrows(BoundedNbtStructurePreflight.PreflightException.class,()->BoundedNbtStructurePreflight.extract(duplicate,limits(100,64,100_000)));
        assertEquals(BoundedNbtStructurePreflight.Code.DUPLICATE_COMPOUND_NAME,failure.code());
    }
    @Test void legacyApiAndSemanticExtractionBothRejectTruncationTrailingAndNegativeArrays()throws Exception {
        byte[] good=allTypes();BoundedNbtStructurePreflight.validate(good,legacy());
        byte[] trailing=Arrays.copyOf(good,good.length+1);assertThrows(BoundedNbtStructurePreflight.PreflightException.class,()->BoundedNbtStructurePreflight.validate(trailing,legacy()));assertThrows(BoundedNbtStructurePreflight.PreflightException.class,()->BoundedNbtStructurePreflight.extract(trailing,limits(4096,64,10_000_000)));
        byte[] shortInput=Arrays.copyOf(good,good.length-1);assertThrows(BoundedNbtStructurePreflight.PreflightException.class,()->BoundedNbtStructurePreflight.extract(shortInput,limits(4096,64,10_000_000)));
        ByteArrayOutputStream bytes=new ByteArrayOutputStream();DataOutputStream out=new DataOutputStream(bytes);out.writeByte(10);out.writeUTF("");tag(out,7,"bad");out.writeInt(-1);out.writeByte(0);
        var failure=assertThrows(BoundedNbtStructurePreflight.PreflightException.class,()->BoundedNbtStructurePreflight.extract(bytes.toByteArray(),limits(100,64,100_000)));assertEquals(BoundedNbtStructurePreflight.Code.NEGATIVE_ARRAY_LENGTH,failure.code());
    }
    @Test void depthAndRemainingWorkspaceCannotBeExtendedByCaller()throws Exception {
        ByteArrayOutputStream bytes=new ByteArrayOutputStream();DataOutputStream out=new DataOutputStream(bytes);out.writeByte(10);out.writeUTF("");tag(out,10,"nested");out.writeByte(0);out.writeByte(0);
        var failure=assertThrows(BoundedNbtStructurePreflight.PreflightException.class,()->BoundedNbtStructurePreflight.extract(bytes.toByteArray(),limits(100,1,100_000)));assertEquals(BoundedNbtStructurePreflight.Code.DEPTH_LIMIT,failure.code());
        byte[] data=twoBytes();SourceLimitProfile profile=limits(100,64,100_000);
        assertThrows(IllegalArgumentException.class,()->BoundedNbtStructurePreflight.extract(data,profile,100_001));
        assertThrows(BoundedNbtStructurePreflight.PreflightException.class,()->BoundedNbtStructurePreflight.extract(data,profile,2L*data.length));
    }
    private static ReadOnlyRegionCatalog.Limits legacy(){return new ReadOnlyRegionCatalog.Limits(2,2,2,1_000_000,2_000_000,100_000,100_000,1_000_000,4096,64,64,100,200,10_000_000);}
    private static byte[] twoBytes()throws IOException{return namedBytes(false);}
    private static byte[] namedBytes(boolean duplicate)throws IOException{ByteArrayOutputStream bytes=new ByteArrayOutputStream();DataOutputStream out=new DataOutputStream(bytes);out.writeByte(10);out.writeUTF("");tag(out,1,"a");out.writeByte(1);tag(out,1,duplicate?"a":"b");out.writeByte(2);out.writeByte(0);return bytes.toByteArray();}
    private static void tag(DataOutputStream out,int type,String name)throws IOException{out.writeByte(type);out.writeUTF(name);}
    private static byte[] allTypes()throws IOException {
        ByteArrayOutputStream bytes=new ByteArrayOutputStream();DataOutputStream out=new DataOutputStream(bytes);out.writeByte(10);out.writeUTF("");
        tag(out,1,"t1");out.writeByte(-1);tag(out,2,"t2");out.writeShort(Short.MIN_VALUE);tag(out,3,"t3");out.writeInt(Integer.MIN_VALUE);tag(out,4,"t4");out.writeLong(Long.MIN_VALUE);
        tag(out,5,"t5");out.writeInt(0x80000000);tag(out,6,"t6");out.writeLong(0x7ff8000000000042L);
        tag(out,7,"t7");out.writeInt(2);out.write(new byte[]{0,-1});tag(out,8,"t8");out.writeUTF("x\u0000\u2603");
        tag(out,9,"t9");out.writeByte(3);out.writeInt(2);out.writeInt(1);out.writeInt(2);
        tag(out,10,"t10");tag(out,8,"Caps");out.writeUTF("opaque");out.writeByte(0);
        tag(out,11,"t11");out.writeInt(1);out.writeInt(17);tag(out,12,"t12");out.writeInt(1);out.writeLong(-1);out.writeByte(0);return bytes.toByteArray();
    }
}
