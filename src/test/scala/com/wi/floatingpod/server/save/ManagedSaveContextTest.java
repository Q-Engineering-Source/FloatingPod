package com.wi.floatingpod.server.save;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicReference;
import com.wi.floatingpod.config.SourceLimitProfile;
import net.minecraft.nbt.CompressedStreamTools;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.nbt.NBTTagList;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** actual NBT/DataOutput production逻辑；不冒Region/native/Mixin transformed运行。 */
final class ManagedSaveContextTest {
    @Test void completeFinalRootPreservesPassengersAndUnrelatedVanillaData()throws Exception {
        NBTTagCompound root=new NBTTagCompound(),level=new NBTTagCompound(),pig=new NBTTagCompound();
        root.setTag("Level",level);root.setString("ModOther","must-preserve");level.setByteArray("Blocks",new byte[]{1,2,3});
        NBTTagList passengers=new NBTTagList();pig.setString("id","minecraft:pig");pig.setString("UnrelatedPayload","retained");passengers.appendTag(pig);level.setTag("Passengers",passengers);
        byte[] bytes=ManagedSaveContext.encode(root,ManagedSourceSaveLedgerTest.profile(4096,8192));
        NBTTagCompound decoded=CompressedStreamTools.read(new DataInputStream(new ByteArrayInputStream(bytes)));
        assertEquals(root,decoded);assertEquals("must-preserve",decoded.getString("ModOther"));assertEquals("minecraft:pig",decoded.getCompoundTag("Level").getTagList("Passengers",10).getCompoundTagAt(0).getString("id"));
    }
    @Test void actualCompleteEncoderRejectsDepthAndByteBudgetWithoutTruncationSuccess()throws Exception {
        NBTTagCompound root=new NBTTagCompound();root.setByteArray("FullData",new byte[128]);
        assertThrows(IOException.class,()->ManagedSaveContext.encode(root,ManagedSourceSaveLedgerTest.profile(16,1024)));
        NBTTagCompound current=root;
        for(int i=0;i<33;i++){NBTTagCompound child=new NBTTagCompound();current.setTag("Nested",child);current=child;}
        assertThrows(IOException.class,()->ManagedSaveContext.encode(root,ManagedSourceSaveLedgerTest.profile(4096,8192)));
    }
    @Test void actualCountingOutputForwardsModifiedUtfAndPreservesRealBytes()throws Exception {
        try(var f=new ManagedSourceSaveLedgerTest.Fixture(ManagedSourceSaveLedgerTest.profile(64,1024),new ManagedSourceSaveLedger.Capacity(4,1,4))) {
            Object compound=new Object();f.enqueue(compound,new byte[]{1});
            f.onIo(()->{
                var a=f.removeActual();f.ledger.reserveOutput(a);
                ByteArrayOutputStream buffer=new ByteArrayOutputStream();DataOutputStream actual=new DataOutputStream(buffer);
                try(ManagedSaveContext scope=ManagedSaveContext.writer(f.ledger)) {
                    scope.attempt=a;scope.opened(actual);ManagedCountingDataOutput bounded=new ManagedCountingDataOutput(actual,f.ledger,a);
                    bounded.writeUTF("a\0\u0800");bounded.writeInt(123);scope.closing(actual);actual.close();scope.closeReturned();
                }
                ByteArrayOutputStream expected=new ByteArrayOutputStream();try(DataOutputStream out=new DataOutputStream(expected)){out.writeUTF("a\0\u0800");out.writeInt(123);}
                assertArrayEquals(expected.toByteArray(),buffer.toByteArray());assertEquals(12,f.ledger.snapshot().attempts().getFirst().encoderSubmittedBytes());f.ledger.endFlight(a);
            });
            assertEquals(0,f.ledger.snapshot().outputReservations());assertEquals(ManagedSourceSaveLedger.Resolution.UNRESOLVED,f.ledger.snapshot().attempts().getFirst().resolution());
        }
    }
    @Test void actualBoundedForwardingFailureKeepsStreamAndOriginalCause()throws Exception {
        try(var f=new ManagedSourceSaveLedgerTest.Fixture(ManagedSourceSaveLedgerTest.profile(8,128),new ManagedSourceSaveLedger.Capacity(4,1,4))) {
            Object compound=new Object();f.enqueue(compound,new byte[]{1});
            f.onIo(()->{
                var a=f.removeActual();f.ledger.reserveOutput(a);ByteArrayOutputStream buffer=new ByteArrayOutputStream();DataOutputStream actual=new DataOutputStream(buffer);
                try(ManagedSaveContext scope=ManagedSaveContext.writer(f.ledger)) {
                    scope.attempt=a;scope.opened(actual);ManagedCountingDataOutput output=new ManagedCountingDataOutput(actual,f.ledger,a);output.writeLong(7);
                    IOException cause=assertThrows(IOException.class,()->output.writeByte(8));f.ledger.failure(a,cause);scope.retainUnclosedOutput();assertSame(cause,f.ledger.snapshot().primary());
                }
                assertEquals(8,buffer.size());assertEquals(1,f.ledger.snapshot().attempts().getFirst().retainedOutputCount());f.ledger.endFlight(a);
            });
        }
    }
    @Test void explicitMemberBoundaryPreservesFullPassengersAndRejectsAdditionalMember()throws Exception {
        NBTTagCompound root=new NBTTagCompound(),pig=new NBTTagCompound();NBTTagList passengers=new NBTTagList();
        pig.setString("id","minecraft:pig");passengers.appendTag(pig);root.setTag("Passengers",passengers);root.setString("OtherMod","complete");
        SourceLimitProfile limits=SourceLimitProfile.of(8192,8192,4096,32,5,8192,8192,8192);
        byte[] complete=ManagedSaveContext.encode(root,limits);
        assertEquals(root,CompressedStreamTools.read(new DataInputStream(new ByteArrayInputStream(complete))));
        root.setString("SixthMember","must-not-be-truncated");
        IOException outside=assertThrows(IOException.class,()->ManagedSaveContext.encode(root,limits));
        assertEquals("Full NBT observation exceeds explicit member budget",outside.getMessage());
        SourceLimitProfile six=SourceLimitProfile.of(8192,8192,4096,32,6,8192,8192,8192);
        byte[] all=ManagedSaveContext.encode(root,six);
        assertEquals(root,CompressedStreamTools.read(new DataInputStream(new ByteArrayInputStream(all))));
    }
    @Test void threeActualFaultsWithCauseCapacityOneKeepEachOriginalAndActualStream()throws Exception {
        try(var f=new ManagedSourceSaveLedgerTest.Fixture(ManagedSourceSaveLedgerTest.profile(64,1024),new ManagedSourceSaveLedger.Capacity(4,1,1))) {
            Object compound=new Object();f.enqueue(compound,new byte[]{1});
            f.onIo(()->{
                var a=f.removeActual();f.ledger.reserveOutput(a);int[] closes={0};
                DataOutputStream actual=new DataOutputStream(new OutputStream(){@Override public void write(int value){} @Override public void close(){closes[0]++;}});
                try(ManagedSaveContext scope=ManagedSaveContext.writer(f.ledger)) {
                    scope.attempt=a;scope.opened(actual);
                    IOException first=new IOException("actual-first"),second=new IOException("actual-second"),third=new IOException("actual-third");
                    StackTraceElement[] firstStack=first.getStackTrace(),secondStack=second.getStackTrace(),thirdStack=third.getStackTrace();
                    assertSame(first,assertThrows(Throwable.class,()->scope.failed(first)));
                    assertSame(second,assertThrows(Throwable.class,()->scope.failed(second)));
                    assertSame(third,assertThrows(Throwable.class,()->scope.failed(third)));
                    assertArrayEquals(firstStack,first.getStackTrace());assertArrayEquals(secondStack,second.getStackTrace());assertArrayEquals(thirdStack,third.getStackTrace());
                    assertSame(first,f.ledger.snapshot().primary());assertEquals(1,first.getSuppressed().length);
                    assertSame(first.getSuppressed()[0],second.getSuppressed()[0]);assertSame(first.getSuppressed()[0],third.getSuppressed()[0]);
                    assertNull(first.getSuppressed()[0].getCause());assertAcyclic(first);assertAcyclic(second);assertAcyclic(third);
                    assertEquals(0,closes[0]);assertEquals(1,f.ledger.snapshot().outputReservations());assertEquals(1,f.ledger.snapshot().attempts().getFirst().retainedOutputCount());
                }
                f.ledger.endFlight(a);
            });
            assertThrows(IllegalStateException.class,()->f.ledger.begin(f.loader,new ManagedSourceSaveLedger.Slot(7,-4)));
            assertThrows(IllegalStateException.class,f.ledger::sealObservation);
        }
    }
    @Test void secondaryRetentionFailureCannotReplaceOriginalOrCreateCauseCycle()throws Exception {
        try(var f=new ManagedSourceSaveLedgerTest.Fixture(ManagedSourceSaveLedgerTest.profile(64,1024),new ManagedSourceSaveLedger.Capacity(4,1,1))) {
            Object compound=new Object();f.enqueue(compound,new byte[]{1});
            f.onIo(()->{
                var a=f.removeActual();f.ledger.reserveOutput(a);int[] closes={0};
                DataOutputStream actual=new DataOutputStream(new OutputStream(){@Override public void write(int value){} @Override public void close(){closes[0]++;}});
                try(ManagedSaveContext scope=ManagedSaveContext.writer(f.ledger)) {
                    scope.attempt=a;scope.opened(actual);scope.closing(actual);actual.close();scope.closeReturned();
                    IOException original=new IOException("actual-after-close-fault");
                    assertSame(original,assertThrows(Throwable.class,()->scope.failed(original)));
                    assertTrue(original.getSuppressed().length>0);assertAcyclic(original);assertEquals(1,closes[0]);
                    IllegalStateException backlink=new IllegalStateException("observer already references actual",original);
                    int before=original.getSuppressed().length;ManagedSourceSaveLedger.suppressSecondary(original,backlink);
                    assertEquals(before,original.getSuppressed().length);assertAcyclic(original);
                }
                f.ledger.endFlight(a);
            });
        }
    }
    @Test void anotherAcceptedFlightStaysFaultedAfterCauseCapacityOverflowAndEndFlight()throws Exception {
        try(var f=new ManagedSourceSaveLedgerTest.Fixture(ManagedSourceSaveLedgerTest.profile(64,1024),new ManagedSourceSaveLedger.Capacity(4,1,1))) {
            Object firstPayload=new Object(),secondPayload=new Object();f.enqueue(firstPayload,new byte[]{1});
            AtomicReference<ManagedSourceSaveLedger.Attempt> firstFlight=new AtomicReference<>();
            f.onIo(()->firstFlight.set(f.removeActual()));
            f.enqueue(secondPayload,new byte[]{2});
            f.onIo(()->{
                var first=firstFlight.get();var second=f.removeActual();Throwable primary=null;
                try {
                    IOException firstCause=new IOException("first-accepted-flight"),secondCause=new IOException("second-accepted-flight");
                    StackTraceElement[] originalStack=secondCause.getStackTrace();
                    try(ManagedSaveContext scope=ManagedSaveContext.writer(f.ledger)) {
                        scope.attempt=first;assertSame(firstCause,assertThrows(Throwable.class,()->scope.failed(firstCause)));
                    }
                    try(ManagedSaveContext scope=ManagedSaveContext.writer(f.ledger)) {
                        scope.attempt=second;assertSame(secondCause,assertThrows(Throwable.class,()->scope.failed(secondCause)));
                    }
                    assertArrayEquals(originalStack,secondCause.getStackTrace());assertAcyclic(secondCause);
                } catch(Throwable cause) {
                    primary=cause;if(cause instanceof Error error)throw error;if(cause instanceof Exception error)throw error;throw new AssertionError(cause);
                } finally {
                    Throwable cleanup=null;
                    try{f.ledger.endFlight(second);}catch(Throwable cause){cleanup=cause;}
                    try{f.ledger.endFlight(first);}catch(Throwable cause){if(cleanup==null)cleanup=cause;else ManagedSourceSaveLedger.suppressSecondary(cleanup,cause);}
                    if(cleanup!=null) {
                        if(primary!=null)ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);
                        else if(cleanup instanceof Error error)throw error;
                        else if(cleanup instanceof Exception error)throw error;
                        else throw new AssertionError(cleanup);
                    }
                }
                var rows=f.ledger.snapshot().attempts();
                assertEquals(1,rows.get(0).acceptedGeneration());assertEquals(2,rows.get(1).acceptedGeneration());
                assertEquals(ManagedSourceSaveLedger.Progress.FAULTED,rows.get(0).progress());
                assertEquals(ManagedSourceSaveLedger.Progress.FAULTED,rows.get(1).progress());
                assertNull(rows.get(1).primary(),"Cause not retained under bounded ledger; FAULTED is unresolved, never CALLS_RETURNED");
                assertEquals(ManagedSourceSaveLedger.Resolution.UNRESOLVED,rows.get(1).resolution());assertEquals(0,f.ledger.snapshot().flights());
            });
            assertThrows(IllegalStateException.class,()->f.ledger.begin(f.loader,new ManagedSourceSaveLedger.Slot(7,-4)));
        }
    }
    private static void assertAcyclic(Throwable root) { visit(root,new IdentityHashMap<>(),new IdentityHashMap<>()); }
    private static void visit(Throwable node,IdentityHashMap<Throwable,Boolean> active,IdentityHashMap<Throwable,Boolean> complete) {
        if(complete.containsKey(node))return;assertFalse(active.containsKey(node),"Cause/suppressed graph cycle");active.put(node,Boolean.TRUE);
        if(node.getCause()!=null)visit(node.getCause(),active,complete);for(Throwable child:node.getSuppressed())visit(child,active,complete);
        active.remove(node);complete.put(node,Boolean.TRUE);
    }
}
