package com.wi.floatingpod.server.source;

import com.wi.floatingpod.config.SourceLimitProfile;
import com.wi.floatingpod.server.save.ManagedSaveContext;
import net.minecraft.nbt.CompressedStreamTools;
import net.minecraft.nbt.NBTTagCompound;
import net.minecraft.nbt.NBTTagList;
import net.minecraft.util.datafix.DataFixer;
import net.minecraft.util.datafix.FixTypes;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** 真实NBT/read/state逻辑；controlled decoder事件不是Forge factory/transformed/world验收。 */
final class SourceReadOperationTest {
    static SourceLimitProfile limits(){return SourceLimitProfile.of(1<<20,1<<16,1<<16,64,4096,1<<24,1<<16,65536);}
    static NBTTagCompound root(){
        NBTTagCompound root=new NBTTagCompound(),level=new NBTTagCompound(),first=new NBTTagCompound(),child=new NBTTagCompound();
        root.setInteger("DataVersion",1343);first.setString("id","example:root");first.setString("UUID","same-observed-text");
        child.setString("id","example:child");child.setString("UUID","same-observed-text");child.setByteArray("opaque",new byte[]{0,-1,3});
        NBTTagCompound caps=new NBTTagCompound();caps.setLong("raw",Long.MIN_VALUE);child.setTag("ForgeCaps",caps);
        NBTTagList passengers=new NBTTagList();passengers.appendTag(child);first.setTag("Passengers",passengers);
        NBTTagList entities=new NBTTagList();entities.appendTag(first);level.setTag("Entities",entities);level.setInteger("xPos",2);level.setInteger("zPos",-3);root.setTag("Level",level);return root;
    }
    static final class Fixture {
        final Object loader=new Object(),world=new Object();final SourceLoadManifest holder=SourceLoadManifest.observedFixture(loader,world,limits());
        SourceReadOperation queued(NBTTagCompound raw)throws IOException {
            SourceReadOperation op=holder.beginRead(loader,world,2,-3);
            try(var scope=op.enterRead()){NBTTagCompound actual=op.queuedRoot(raw,raw,7);op.returnedAsync(new Object[]{null,actual});op.readEnded(null);}
            holder.rawFinished(op,null);return op;
        }
        NBTTagCompound level(SourceReadOperation op){return ((NBTTagCompound)op.exactReturnedRoot()).getCompoundTag("Level");}
        SourceReadOperation main(SourceReadOperation op){assertSame(op,holder.beginDecode(loader,world,level(op),2,-3));return op;}
        SourceReadOperation.Observation finish(SourceReadOperation op,Throwable cause){op.mainEnded(cause);holder.decoded(op,cause);return holder.drainObserved(1).getFirst();}
    }
    @Test void stoppingRejectsNewReadsButRetainsAcceptedAsyncEntryUntilItCloses()throws Exception {
        Fixture f=new Fixture();
        var entered=new java.util.concurrent.CountDownLatch(1);var proceed=new java.util.concurrent.CountDownLatch(1);
        var rawDone=new java.util.concurrent.CountDownLatch(1);var close=new java.util.concurrent.CountDownLatch(1);
        AtomicReference<Throwable> failure=new AtomicReference<>();
        Thread reader=new Thread(()->{
            try(var entry=f.holder.enterReadEntry(f.loader,f.world)){
                entered.countDown();assertTrue(proceed.await(5,java.util.concurrent.TimeUnit.SECONDS));
                var operation=f.holder.beginRead(f.loader,f.world,2,-3);
                assertThrows(IllegalStateException.class,()->f.holder.beginRead(f.loader,f.world,2,-3));
                try(var scope=operation.enterRead()){operation.returnedAsync(null);operation.readEnded(null);}
                f.holder.rawFinished(operation,null);rawDone.countDown();
                assertTrue(close.await(5,java.util.concurrent.TimeUnit.SECONDS));
            }catch(Throwable cause){failure.set(cause);entered.countDown();rawDone.countDown();}
        });
        reader.start();
        try{
            assertTrue(entered.await(5,java.util.concurrent.TimeUnit.SECONDS));
            f.holder.beginStopping(f.loader);
            assertThrows(IllegalStateException.class,()->f.holder.enterReadEntry(f.loader,f.world));
            assertThrows(IllegalStateException.class,()->f.holder.beginRead(f.loader,f.world,2,-3));
            assertThrows(IllegalStateException.class,()->f.holder.requireStoppedReads(f.loader));
            assertFalse(f.holder.pollStoppedReads(f.loader));
            proceed.countDown();assertTrue(rawDone.await(5,java.util.concurrent.TimeUnit.SECONDS));
            assertThrows(IllegalStateException.class,()->f.holder.requireStoppedReads(f.loader));
            assertFalse(f.holder.pollStoppedReads(f.loader));
        }finally{proceed.countDown();close.countDown();reader.join(5000);}
        assertFalse(reader.isAlive());assertNull(failure.get());f.holder.requireStoppedReads(f.loader);
        assertTrue(f.holder.pollStoppedReads(f.loader));
        assertEquals(1,f.holder.drainObserved(1).size(),"quiescence must retain original read observations");
    }
    @Test void stoppingStillRequiresOriginalMainDecodeAndRetainsItsFailure()throws Exception {
        Fixture f=new Fixture();var operation=f.queued(root());f.holder.beginStopping(f.loader);
        assertThrows(IllegalStateException.class,()->f.holder.requireStoppedReads(f.loader));
        f.main(operation);
        assertThrows(IllegalStateException.class,()->f.holder.requireStoppedReads(f.loader));
        try(var scope=operation.enterMain(f.world,f.level(operation))){}
        var cause=new IOException("original decode failure");f.finish(operation,cause);
        assertSame(cause,assertThrows(IllegalStateException.class,()->f.holder.requireStoppedReads(f.loader)).getCause());
    }
    @Test void stoppedReadCheckDoesNotDiscardSuccessfullyDecodedObservation()throws Exception {
        Fixture f=new Fixture();var operation=f.queued(root());f.holder.beginStopping(f.loader);f.main(operation);
        try(var scope=operation.enterMain(f.world,f.level(operation))){}
        operation.mainEnded(null);f.holder.decoded(operation,null);f.holder.requireStoppedReads(f.loader);
        assertNotNull(f.holder.drainObserved(1).getFirst().raw());
        assertThrows(IllegalStateException.class,()->f.holder.beginStopping(new Object()));
    }
    @Test void stoppedManifestRejectsUnknownMainDecodeWithoutPoisoningAcceptedHandoff()throws Exception {
        Fixture f=new Fixture();var operation=f.queued(root());f.holder.beginStopping(f.loader);
        assertThrows(IllegalStateException.class,()->f.holder.beginDecode(f.loader,f.world,new NBTTagCompound(),2,-3));
        assertFalse(f.holder.pollStoppedReads(f.loader));f.main(operation);
        try(var scope=operation.enterMain(f.world,f.level(operation))){}
        operation.mainEnded(null);f.holder.decoded(operation,null);assertTrue(f.holder.pollStoppedReads(f.loader));
        assertThrows(IllegalStateException.class,()->f.holder.beginDecode(f.loader,f.world,f.level(operation),2,-3));
        assertTrue(f.holder.pollStoppedReads(f.loader));
    }
    @Test void actualDiskReaderAndStandardCurrentFixerKeepFullRawAndSameNodeRelations()throws Exception {
        Fixture f=new Fixture();var op=f.holder.beginRead(f.loader,f.world,2,-3);byte[] bytes=ManagedSaveContext.encode(root(),limits());
        DataInputStream input=new DataInputStream(new ByteArrayInputStream(bytes));
        try(var scope=op.enterRead()){
            op.openedDiskStream(input);NBTTagCompound raw=CompressedStreamTools.read(op.ownedDiskInput(input));op.diskRootReturned(raw);
            op.fixerBeginning(raw);NBTTagCompound fixed=new DataFixer(1343).process(FixTypes.CHUNK,raw);op.fixerReturned(raw,fixed);
            assertSame(raw,fixed);op.actualStreamClosing(input);input.close();op.returnedAsync(new Object[]{null,fixed});op.readEnded(null);
        }
        f.holder.rawFinished(op,null);f.main(op);try(var scope=op.enterMain(f.world,f.level(op))){}
        var observed=f.finish(op,null);assertArrayEquals(bytes,observed.raw().bytes().copy());assertEquals(2,observed.raw().occurrences().size());
        assertEquals(SourceReadOperation.Lineage.SAME_ACTUAL_NODE,observed.fixerLineage());assertEquals(4,observed.nodeRelations().size());
        assertTrue(observed.unresolved().contains("DISK_NATIVE_IDENTITY_UNOBSERVED"));assertNull(SourceReadOperation.currentRead());assertNull(SourceReadOperation.currentMain());
    }
    @Test void queuedActualCopyPreservesPassengersCapsAndRemainsMutatorUnproven()throws Exception {
        Fixture f=new Fixture();NBTTagCompound raw=root();var op=f.queued(raw);NBTTagCompound copied=(NBTTagCompound)op.exactReturnedRoot();assertNotSame(raw,copied);
        raw.getCompoundTag("Level").getTagList("Entities",10).getCompoundTagAt(0).setString("changed","external-mutator");
        f.main(op);try(var scope=op.enterMain(f.world,f.level(op))){}var observed=f.finish(op,null);
        assertEquals(7,observed.acceptedSaveGeneration());assertEquals(2,observed.actualDecodeInput().occurrences().size());
        assertTrue(observed.unresolved().contains("QUEUED_MUTATOR_EXCLUSIVITY_UNPROVEN"));assertTrue(observed.unresolved().contains("QUEUED_NODE_COPY_MAPPING_UNOBSERVED"));
        assertFalse(copied.getCompoundTag("Level").getTagList("Entities",10).getCompoundTagAt(0).hasKey("changed"));
        assertArrayEquals(new byte[]{0,-1,3},copied.getCompoundTag("Level").getTagList("Entities",10).getCompoundTagAt(0).getTagList("Passengers",10).getCompoundTagAt(0).getByteArray("opaque"));
    }
    @Test void replacementFixerNeverInfersMappingFromSameUuidOrBytes()throws Exception {
        Fixture f=new Fixture();var op=f.holder.beginRead(f.loader,f.world,2,-3);byte[] bytes=ManagedSaveContext.encode(root(),limits());DataInputStream input=new DataInputStream(new ByteArrayInputStream(bytes));
        try(var scope=op.enterRead()){
            op.openedDiskStream(input);NBTTagCompound raw=CompressedStreamTools.read(op.ownedDiskInput(input));op.diskRootReturned(raw);op.fixerBeginning(raw);NBTTagCompound replacement=raw.copy();op.fixerReturned(raw,replacement);op.returnedAsync(new Object[]{null,replacement});op.actualStreamClosing(input);input.close();op.readEnded(null);
        }
        f.holder.rawFinished(op,null);f.main(op);try(var scope=op.enterMain(f.world,f.level(op))){}var observed=f.finish(op,null);
        assertEquals(SourceReadOperation.Lineage.UNPROVEN,observed.fixerLineage());assertTrue(observed.nodeRelations().stream().noneMatch(r->r.from()==SourceReadOperation.Stage.RAW));
    }
    @Test void originalReadFailureRetainsPartialBodyAndSwallowedHelperNull()throws Exception {
        Fixture f=new Fixture();var op=f.queued(root());f.main(op);IOException original=new IOException("actual virtual read fault"),secondary=new IOException("later helper fault");Object registry=new Object(),entry=new Object(),body=new Object();
        try(var phase=op.enterMain(f.world,f.level(op))){NBTTagCompound node=f.level(op).getTagList("Entities",10).getCompoundTagAt(0);
            try(var decoder=op.enterDecoder(node,f.world);var inner=SourceReadOperation.enterEntityList(node,f.world);var lookup=SourceReadOperation.enterLookup("example:root",f.world)){
                assertSame(decoder,SourceReadOperation.lookup());decoder.registry(registry,"example:root",entry,"example:root","controlled-class",SourceForestCodec.Classification.OTHER);
                decoder.factoryBeginning(entry,f.world);decoder.factoryReturned(body);decoder.readBeginning(body,node);decoder.readFailed(original);decoder.helperFailed(secondary);decoder.helperReturned(null);
            }
        }
        var observed=f.finish(op,null);var member=observed.actualDecodeInput().occurrences().getFirst();assertSame(original,observed.primary());assertSame(original,member.originalCause());
        assertEquals(SourceReadOperation.DecodeOutcome.READ_FAILED,member.outcome());assertTrue(member.partialBodyRetained());assertArrayEquals(new Throwable[]{secondary},original.getSuppressed());assertFalse(original.getStackTrace().length==0);
        assertNotNull(member.registryObservation().registryReference());assertNotNull(member.registryObservation().entryReference());assertNull(SourceReadOperation.decoder());assertNull(SourceReadOperation.inner());assertNull(SourceReadOperation.lookup());
    }
    @Test void unknownRegistryAndRegisteredNullFactoryAreDistinct()throws Exception {
        Fixture f=new Fixture();var op=f.queued(root());f.main(op);
        try(var phase=op.enterMain(f.world,f.level(op))){var nodes=f.level(op).getTagList("Entities",10);var first=nodes.getCompoundTagAt(0);var child=first.getTagList("Passengers",10).getCompoundTagAt(0);
            try(var decoder=op.enterDecoder(first,f.world)){decoder.registry(new Object(),"example:root",null,"","",SourceForestCodec.Classification.UNKNOWN);decoder.helperReturned(null);}
            Object entry=new Object();try(var decoder=op.enterDecoder(child,f.world)){decoder.registry(new Object(),"example:child",entry,"example:child","controlled",SourceForestCodec.Classification.OTHER);decoder.factoryBeginning(entry,f.world);decoder.factoryReturned(null);decoder.helperReturned(null);}
        }
        var rows=f.finish(op,null).actualDecodeInput().occurrences();assertEquals(SourceReadOperation.DecodeOutcome.UNKNOWN_ENTITY_ID,rows.get(0).outcome());assertEquals(SourceReadOperation.DecodeOutcome.REGISTERED_FACTORY_NULL,rows.get(1).outcome());
    }
    @Test void nestedForeignAndRepeatedFactoriesCannotBorrowSelectedDecoder()throws Exception {
        Fixture f=new Fixture();var op=f.queued(root());f.main(op);
        try(var phase=op.enterMain(f.world,f.level(op))){var node=f.level(op).getTagList("Entities",10).getCompoundTagAt(0);try(var decoder=op.enterDecoder(node,f.world);var inner=SourceReadOperation.enterEntityList(node,f.world)){
            try(var foreign=SourceReadOperation.enterEntityList(node.copy(),f.world)){assertNull(SourceReadOperation.inner());}assertSame(decoder,SourceReadOperation.inner());
            try(var lookup=SourceReadOperation.enterLookup("example:root",f.world)){assertSame(decoder,SourceReadOperation.lookup());try(var nested=SourceReadOperation.enterLookup("example:root",f.world)){assertNull(SourceReadOperation.lookup());}}
            Object entry=new Object();decoder.registry(new Object(),"example:root",entry,"example:root","controlled",SourceForestCodec.Classification.OTHER);assertThrows(IllegalStateException.class,()->decoder.factoryBeginning(new Object(),f.world));
        }}f.finish(op,null);
    }
    @Test void mainScopeAcquisitionFailureStillProducesRetainedFailureObservation()throws Exception {
        Fixture f=new Fixture();var op=f.queued(root());f.main(op);Throwable cause;
        try{op.enterMain(new Object(),f.level(op));throw new AssertionError("foreign world must fail");}catch(IllegalStateException actual){cause=actual;op.recordFailure(actual);}
        var observed=f.finish(op,cause);assertSame(cause,observed.primary());assertNotNull(observed.raw());assertNotNull(observed.fixed());assertNull(observed.actualDecodeInput());assertTrue(f.holder.drainObserved(1).isEmpty());
    }
    @Test void sourceStreamCleanupPreservesOriginalAndNeverRetriesActualClose()throws Exception {
        Fixture f=new Fixture();var op=f.holder.beginRead(f.loader,f.world,2,-3);IOException original=new IOException("actual read fault"),cleanup=new IOException("actual close fault");int[] closes={0};
        DataInputStream input=new DataInputStream(new ByteArrayInputStream(new byte[0])){@Override public void close()throws IOException{closes[0]++;throw cleanup;}};
        try(var phase=op.enterRead()){op.openedDiskStream(input);op.readEnded(original);}f.holder.rawFinished(op,original);
        assertEquals(1,closes[0]);assertSame(original,f.holder.drainObserved(1).getFirst().primary());assertArrayEquals(new Throwable[]{cleanup},original.getSuppressed());
        Object world=new Object(),loader=new Object();var second=SourceLoadManifest.observedFixture(loader,world,limits());var other=second.beginRead(loader,world,0,0);
        try(var phase=other.enterRead()){other.openedDiskStream(input);other.actualStreamClosing(input);assertThrows(IOException.class,input::close);other.readEnded(cleanup);}second.rawFinished(other,cleanup);assertEquals(2,closes[0]);
    }
    @Test void foreignMainThreadCannotStealAwaitingRootAndItsCauseIsObservable()throws Exception {
        Fixture f=new Fixture();var op=f.queued(root());CountDownLatch done=new CountDownLatch(1);AtomicReference<Throwable> rejected=new AtomicReference<>();
        Thread actor=new Thread(()->{try{f.holder.beginDecode(f.loader,f.world,f.level(op),2,-3);}catch(Throwable cause){rejected.set(cause);}finally{done.countDown();}},"source-foreign-main");
        try{actor.start();assertTrue(done.await(3,TimeUnit.SECONDS));assertInstanceOf(IllegalStateException.class,rejected.get());}
        finally{actor.join(3000);assertFalse(actor.isAlive(),"foreign actor leaked");}
        f.main(op);try(var scope=op.enterMain(f.world,f.level(op))){}f.finish(op,null);
    }
    @Test void noRootAndExhaustedReservationNeverBecomeAbsenceOrPartialSuccess()throws Exception {
        Object loader=new Object(),world=new Object();SourceLimitProfile tiny=SourceLimitProfile.of(1000,1000,64,8,32,4480,1000,1000);var holder=SourceLoadManifest.observedFixture(loader,world,tiny);
        var op=holder.beginRead(loader,world,0,0);try(var scope=op.enterRead()){op.returnedAsync(null);op.readEnded(null);}holder.rawFinished(op,null);
        assertThrows(IllegalStateException.class,()->holder.beginRead(loader,world,1,1));var observed=holder.drainObserved(1).getFirst();assertTrue(observed.unresolved().contains("NO_ROOT_RETURN_IS_NOT_ABSENCE"));assertNull(observed.raw());
    }
    @Test void actualAsyncReadHandsExactRootToOnlyRetainedMain()throws Exception {
        Fixture f=new Fixture();AtomicReference<SourceReadOperation> operation=new AtomicReference<>();AtomicReference<Throwable> failure=new AtomicReference<>();CountDownLatch done=new CountDownLatch(1);
        Thread reader=new Thread(()->{try{operation.set(f.queued(root()));}catch(Throwable cause){failure.set(cause);}finally{done.countDown();}},"source-actual-async-reader");
        try{reader.start();assertTrue(done.await(3,TimeUnit.SECONDS));assertNull(failure.get());}
        finally{reader.join(3000);assertFalse(reader.isAlive(),"async read actor leak");}
        var op=operation.get();assertThrows(IllegalStateException.class,op::enterRead);f.main(op);try(var scope=op.enterMain(f.world,f.level(op))){}assertNotNull(f.finish(op,null).actualDecodeInput());
    }
    @Test void serializationEventRetainsActualReturnedNbtSeparatelyFromDecoderInput()throws Exception {
        Fixture f=new Fixture();var op=f.queued(root());f.main(op);Object entry=new Object(),body=new Object();NBTTagCompound actualBody=new NBTTagCompound();actualBody.setLong("postReadDefault",47);NBTTagCompound caps=new NBTTagCompound();caps.setByteArray("cap",new byte[]{3,4});actualBody.setTag("ForgeCaps",caps);
        try(var phase=op.enterMain(f.world,f.level(op))){NBTTagCompound node=f.level(op).getTagList("Entities",10).getCompoundTagAt(0);try(var decoder=op.enterDecoder(node,f.world)){
            decoder.registry(new Object(),"example:root",entry,"example:root","controlled",SourceForestCodec.Classification.OTHER);decoder.factoryBeginning(entry,f.world);decoder.factoryReturned(body);decoder.readBeginning(body,node);decoder.readReturned();
            decoder.serializationBeginning(body,new NBTTagCompound());decoder.serializationReturned(body,actualBody);decoder.helperReturned(body);
        }}
        var observed=f.finish(op,null);var member=observed.actualDecodeInput().occurrences().getFirst();assertEquals(SourceReadOperation.DecodeOutcome.BODY_DECODED_NOT_PLACED,member.outcome());assertEquals(1,member.actualBody().serializationCalls());assertEquals(Thread.currentThread().threadId(),member.actualBody().actualMainThreadId());
        var decoded=CompressedStreamTools.read(new DataInputStream(new ByteArrayInputStream(member.actualBody().actualBodyNbt().copy())));assertEquals(47,decoded.getLong("postReadDefault"));assertArrayEquals(new byte[]{3,4},decoded.getCompoundTag("ForgeCaps").getByteArray("cap"));
        assertFalse(member.rawSubtree().equals(member.actualBody().actualBodyNbt()));actualBody.setLong("postReadDefault",99);assertEquals(47,decoded.getLong("postReadDefault"));
    }
    @Test void serializationFailureRetainsExactBodyAndCauseWithoutSuccessfulActualBodyBytes()throws Exception {
        Fixture f=new Fixture();var op=f.queued(root());f.main(op);Object entry=new Object(),body=new Object();IOException original=new IOException("actual body serialization fault");
        try(var phase=op.enterMain(f.world,f.level(op))){NBTTagCompound node=f.level(op).getTagList("Entities",10).getCompoundTagAt(0);try(var decoder=op.enterDecoder(node,f.world)){
            decoder.registry(new Object(),"example:root",entry,"example:root","controlled",SourceForestCodec.Classification.OTHER);decoder.factoryBeginning(entry,f.world);decoder.factoryReturned(body);decoder.readBeginning(body,node);decoder.readReturned();decoder.serializationBeginning(body,new NBTTagCompound());decoder.serializationFailed(original);decoder.helperReturned(null);
        }}var member=f.finish(op,null).actualDecodeInput().occurrences().getFirst();assertEquals(SourceReadOperation.DecodeOutcome.SERIALIZATION_FAILED,member.outcome());assertTrue(member.partialBodyRetained());assertSame(original,member.originalCause());assertSame(original,member.actualBody().serializationFailure());assertNull(member.actualBody().actualBodyNbt());
    }
    @Test void duplicateExactAsyncRootDoesNotOverwriteOlderAcceptedOperation()throws Exception {
        Fixture f=new Fixture();var first=f.queued(root());var second=f.holder.beginRead(f.loader,f.world,2,-3);
        try(var scope=second.enterRead()){NBTTagCompound root=(NBTTagCompound)first.exactReturnedRoot();second.returnedAsync(new Object[]{null,root});second.readEnded(null);}
        var cause=assertThrows(IllegalStateException.class,()->f.holder.rawFinished(second,null));
        var failure=f.holder.drainObserved(1).getFirst();assertSame(cause,failure.primary());
        f.main(first); // 原accepted operation没有被覆盖，但同loader共享root冲突已封住后续decode。
        assertSame(cause,assertThrows(IllegalStateException.class,()->first.enterMain(f.world,f.level(first))).getCause());
        f.holder.beginStopping(f.loader);
        assertSame(cause,assertThrows(IllegalStateException.class,()->f.holder.requireStoppedReads(f.loader)).getCause());
    }
    @Test void actualFactoryFailureIsNotConvertedToUnknownIdOrSuccessfulBody()throws Exception {
        Fixture f=new Fixture();var op=f.queued(root());f.main(op);Object entry=new Object();IllegalStateException cause=new IllegalStateException("actual factory failure");
        try(var phase=op.enterMain(f.world,f.level(op))){NBTTagCompound node=f.level(op).getTagList("Entities",10).getCompoundTagAt(0);try(var decoder=op.enterDecoder(node,f.world)){
            decoder.registry(new Object(),"example:root",entry,"example:root","controlled",SourceForestCodec.Classification.OTHER);decoder.factoryBeginning(entry,f.world);decoder.factoryFailed(cause);decoder.helperReturned(null);
        }}var member=f.finish(op,null).actualDecodeInput().occurrences().getFirst();assertSame(cause,member.originalCause());assertEquals(SourceReadOperation.DecodeOutcome.FACTORY_FAILED,member.outcome());assertEquals(1,member.factoryCalls());assertEquals(0,member.readCalls());assertNull(member.actualBody());
    }
    @Test void drainingFailureObservationDoesNotReleaseManifestCapacityOrPermitDuplicateDelivery()throws Exception {
        Object loader=new Object(),world=new Object();var tiny=SourceLimitProfile.of(1000,1000,64,8,32,4480,1000,1000);
        var holder=SourceLoadManifest.observedFixture(loader,world,tiny);var op=holder.beginRead(loader,world,0,0);
        IOException original=new IOException("actual async read failed");
        try(var scope=op.enterRead()){op.readEnded(original);}holder.rawFinished(op,original);
        var observations=holder.drainObserved(1);assertEquals(1,observations.size());assertSame(original,observations.getFirst().primary());
        assertTrue(holder.drainObserved(1).isEmpty());assertThrows(UnsupportedOperationException.class,observations::clear);
        assertThrows(IllegalStateException.class,()->holder.rawFinished(op,original));
        assertSame(original,assertThrows(IllegalStateException.class,()->holder.beginRead(loader,world,1,1)).getCause());
        assertThrows(IllegalArgumentException.class,()->holder.drainObserved(0));
    }

    @Test void successfulNoRootDrainKeepsReservationUntilActualResponsibilitySettlement()throws Exception {
        Object loader=new Object(),world=new Object();var tiny=SourceLimitProfile.of(1000,1000,64,8,32,4480,1000,1000);
        var holder=SourceLoadManifest.observedFixture(loader,world,tiny);var op=holder.beginRead(loader,world,0,0);
        try(var scope=op.enterRead()){op.returnedAsync(null);op.readEnded(null);}holder.rawFinished(op,null);
        var observed=holder.drainObserved(1).getFirst();assertNull(observed.primary());assertNull(observed.raw());
        assertTrue(observed.unresolved().contains("NO_ROOT_RETURN_IS_NOT_ABSENCE"));assertTrue(holder.drainObserved(1).isEmpty());
        var exhausted=assertThrows(IllegalStateException.class,()->holder.beginRead(loader,world,1,1));
        assertEquals("Source read operation reservation exhausted",exhausted.getMessage());assertNull(exhausted.getCause());
    }

}
