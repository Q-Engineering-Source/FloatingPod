package com.wi.floatingpod.server.save;

import com.wi.floatingpod.config.SourceLimitProfile;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** 真实ledger/Map/线程行为，非transformed Forge/native durability验收。显式预算仅测试fixture。 */
final class ManagedSourceSaveLedgerTest {
    private static final ManagedSourceSaveLedger.Slot SLOT=new ManagedSourceSaveLedger.Slot(7,-4);
    static SourceLimitProfile profile(long payload,long retained) {
        return SourceLimitProfile.of(1_048_576,4096,payload,32,256,retained,4096,65536);
    }
    @FunctionalInterface interface Work { void run()throws Exception; }
    static final class Fixture implements AutoCloseable {
        private static final Runnable STOP=()->{};
        final Object loader=new Object();
        final Map<ManagedSourceSaveLedger.Slot,Object> actualMap=new ConcurrentHashMap<>();
        final LinkedBlockingQueue<Runnable> tasks=new LinkedBlockingQueue<>();
        final Thread io;
        final ManagedSourceSaveLedger ledger;
        final ManagedSaveQueueObservation queue;
        Fixture(){this(profile(1024,65536),new ManagedSourceSaveLedger.Capacity(32,4,16));}
        Fixture(SourceLimitProfile profile,ManagedSourceSaveLedger.Capacity capacity) {
            io=new Thread(()->{
                try { while(true){Runnable task=tasks.take();if(task==STOP)return;task.run();} }
                catch(InterruptedException cause){Thread.currentThread().interrupt();}
            },"managed-ledger-actual-io");
            ledger=new ManagedSourceSaveLedger(UUID.randomUUID(),Thread.currentThread(),loader,io,profile,capacity);
            queue=new ManagedSaveQueueObservation(ledger);io.start();
        }
        ManagedSourceSaveLedger.Attempt enqueue(Object compound,byte[] bytes) {
            var a=ledger.begin(loader,SLOT);Throwable primary=null;
            try{ledger.finalReference(a,compound);ledger.finalNbt(a,compound,bytes);try(var guard=queue.enter()){ledger.queuePutBeginning(a,compound);ledger.putReturned(a,compound,actualMap.put(SLOT,compound));}return a;}
            catch(Throwable cause){primary=cause;throw cause;}
            finally{try{ledger.endProducer(a);}catch(Throwable cleanup){if(primary!=null){if(primary!=cleanup)primary.addSuppressed(cleanup);}else throw cleanup;}}
        }
        void onIo(Work work)throws Exception {
            AtomicReference<Throwable> failure=new AtomicReference<>();CountDownLatch done=new CountDownLatch(1);
            tasks.add(()->{try{work.run();}catch(Throwable cause){failure.set(cause);}finally{done.countDown();}});
            assertTrue(done.await(3,TimeUnit.SECONDS),"actual IO work deadline expired");
            Throwable cause=failure.get();if(cause instanceof Error e)throw e;if(cause instanceof Exception e)throw e;if(cause!=null)throw new AssertionError(cause);
        }
        ManagedSourceSaveLedger.Attempt removeActual(){try(var guard=queue.enter()){ledger.queueRemoveBeginning(loader,SLOT);return ledger.removed(loader,SLOT,actualMap.remove(SLOT));}}
        ManagedSourceSaveLedger.Attempt consume(Object compound)throws Exception {
            AtomicReference<ManagedSourceSaveLedger.Attempt> consumed=new AtomicReference<>();
            onIo(()->consumed.set(removeActual()));return consumed.get();
        }
        @Override public void close()throws Exception {
            tasks.add(STOP);Throwable primary=null;
            try{io.join(3000);}catch(Throwable cause){primary=cause;}
            if(io.isAlive()) {
                io.interrupt();
                try{io.join(3000);}catch(Throwable cleanup){if(primary==null)primary=cleanup;else if(primary!=cleanup)primary.addSuppressed(cleanup);}
            }
            if(io.isAlive()){AssertionError leak=new AssertionError("actual non-daemon fixture worker leaked");if(primary==null)primary=leak;else primary.addSuppressed(leak);}
            if(primary instanceof Error e)throw e;if(primary instanceof Exception e)throw e;if(primary!=null)throw new AssertionError(primary);
        }
    }
    @Test void detachWaitsForOriginalLexicalEntryAndQueuedReadLeaseThenRejectsLateWork()throws Exception {
        try(Fixture f=new Fixture()) {
            assertThrows(IllegalStateException.class,()->f.ledger.tryDetachStoppedConsumer(f.loader,0,0));
            try(var entry=f.ledger.enterProducerAllocation(f.loader)){
                f.ledger.beginStoppingObservation();
                try(var guard=f.queue.enter()){assertFalse(f.ledger.tryDetachStoppedConsumer(f.loader,f.actualMap.size(),0));}
                assertThrows(IllegalStateException.class,()->f.ledger.enterProducerAllocation(f.loader));
            }
            try(var guard=f.queue.enter();var read=f.ledger.observeSourceQueuedGet(f.loader,SLOT,null)){
                assertFalse(f.ledger.tryDetachStoppedConsumer(f.loader,f.actualMap.size(),0));
            }
            try(var guard=f.queue.enter()){assertTrue(f.ledger.tryDetachStoppedConsumer(f.loader,f.actualMap.size(),0));}
            assertThrows(IllegalStateException.class,()->f.ledger.tryDetachStoppedConsumer(f.loader,0,0));
            assertThrows(IllegalStateException.class,()->f.ledger.begin(f.loader,SLOT));
            assertThrows(IllegalStateException.class,()->f.ledger.observeSourceQueuedGet(f.loader,SLOT,null));
            f.onIo(()->assertThrows(IllegalStateException.class,()->f.ledger.enterConsumerAllocation(f.loader)));
            assertTrue(f.ledger.snapshot().attempts().isEmpty());
        }
    }
    @Test void stoppedQueueDrainsThroughOriginalIoAndDetachRetainsUnresolvedPayload()throws Exception {
        try(Fixture f=new Fixture()) {
            Object compound=new Object();f.enqueue(compound,new byte[]{2,7});f.ledger.beginStoppingObservation();
            try(var guard=f.queue.enter()){assertFalse(f.ledger.tryDetachStoppedConsumer(f.loader,f.actualMap.size(),0));}
            var attempt=f.consume(compound);
            try(var guard=f.queue.enter()){assertFalse(f.ledger.tryDetachStoppedConsumer(f.loader,f.actualMap.size(),1));}
            f.onIo(()->{try(var entry=f.ledger.enterConsumerAllocation(f.loader)){f.ledger.endFlight(attempt);}});
            var before=f.ledger.snapshot();
            try(var guard=f.queue.enter()){assertTrue(f.ledger.tryDetachStoppedConsumer(f.loader,f.actualMap.size(),0));}
            var after=f.ledger.snapshot();assertEquals(before,after);
            assertEquals(ManagedSourceSaveLedger.Resolution.UNRESOLVED,after.attempts().getFirst().resolution());
            assertNotNull(after.attempts().getFirst().finalPayload());assertTrue(after.retainedPayloadBytes()>0);
        }
    }
    @Test void detachRejectsActualQueueMismatchAndRetainsOriginalFailure()throws Exception {
        try(Fixture f=new Fixture()) {
            f.ledger.beginStoppingObservation();
            try(var guard=f.queue.enter()){
                assertThrows(IllegalStateException.class,()->f.ledger.tryDetachStoppedConsumer(f.loader,1,0));
            }
            var cause=new IOException("original unresolved save failure");f.ledger.closeFailure(cause);
            assertSame(cause,assertThrows(IllegalStateException.class,()->f.ledger.tryDetachStoppedConsumer(f.loader,0,0)).getCause());
        }
    }
    @Test void actualReplacementRetainsPreviousExactPayloadAndAcceptedGenerations()throws Exception {
        try(Fixture f=new Fixture()) {
            Object old=new Object(),fresh=new Object();byte[] bytes={1,2,3};
            f.enqueue(old,bytes);bytes[0]=99;f.enqueue(fresh,new byte[]{4,5});
            var rows=f.ledger.snapshot().attempts();assertEquals(2,rows.size());
            assertEquals(1,rows.get(0).acceptedGeneration());assertEquals(2,rows.get(1).acceptedGeneration());
            assertEquals(ManagedSourceSaveLedger.Progress.SUPERSEDED,rows.get(0).progress());
            assertArrayEquals(new byte[]{1,2,3},rows.get(0).finalPayload().bytes());
            byte[] escaped=rows.get(0).finalPayload().bytes();escaped[0]=88;
            assertArrayEquals(new byte[]{1,2,3},rows.get(0).finalPayload().bytes());
            assertEquals(ManagedSourceSaveLedger.Resolution.UNRESOLVED,rows.get(0).resolution());
        }
    }
    @Test void containsNoPutHasNoAcceptedGenerationButPreservesFullPayload()throws Exception {
        try(Fixture f=new Fixture()) {
            Object compound=new Object();var a=f.ledger.begin(f.loader,SLOT);
            f.ledger.finalReference(a,compound);f.ledger.finalNbt(a,compound,new byte[]{9,8});f.ledger.noPut(a,compound);f.ledger.endProducer(a);
            var row=f.ledger.snapshot().attempts().getFirst();assertEquals(0,row.acceptedGeneration());
            assertEquals(ManagedSourceSaveLedger.Progress.NO_PUT,row.progress());assertArrayEquals(new byte[]{9,8},row.finalPayload().bytes());assertTrue(f.actualMap.isEmpty());
        }
    }
    @Test void exactRemovedCompoundMovesOnlyItsGenerationAndForeignReferenceFaultIsRetained()throws Exception {
        try(Fixture f=new Fixture()) {
            Object actual=new Object();var accepted=f.enqueue(actual,new byte[]{1});
            f.onIo(()->{
                var flight=f.removeActual();assertSame(accepted,flight);
                assertEquals(1,f.ledger.snapshot().flights());
                f.ledger.beforeWrite(flight,actual,new byte[]{1});f.ledger.endFlight(flight);
            });
            var snapshot=f.ledger.snapshot();assertTrue(f.actualMap.isEmpty());assertNull(snapshot.primary());
            assertEquals(1,snapshot.attempts().size());assertEquals(1,snapshot.attempts().getFirst().acceptedGeneration());
            assertEquals(ManagedSourceSaveLedger.Progress.CALLS_RETURNED,snapshot.attempts().getFirst().progress());assertEquals(0,snapshot.flights());
        }
        try(Fixture f=new Fixture()) {
            Object actual=new Object(),foreign=new Object();f.enqueue(actual,new byte[]{1});
            var accepted=f.ledger.snapshot().attempts().getFirst();AtomicReference<Throwable> original=new AtomicReference<>();
            f.onIo(()->{
                try(var guard=f.queue.enter()) {
                    f.ledger.queueRemoveBeginning(f.loader,SLOT,f.actualMap.get(SLOT));
                    assertSame(actual,f.actualMap.put(SLOT,foreign));
                    Object removed=f.actualMap.remove(SLOT);assertSame(foreign,removed);
                    original.set(assertThrows(IllegalStateException.class,()->f.ledger.removed(f.loader,SLOT,removed)));
                    assertEquals(0,f.ledger.snapshot().flights());
                }
            });
            var snapshot=f.ledger.snapshot();assertTrue(f.actualMap.isEmpty());assertEquals(1,snapshot.attempts().size());
            var retained=snapshot.attempts().getFirst();assertEquals(accepted.attempt(),retained.attempt());
            assertEquals(1,retained.acceptedGeneration());assertSame(accepted.finalPayload(),retained.finalPayload());assertArrayEquals(new byte[]{1},retained.finalPayload().bytes());
            assertEquals(ManagedSourceSaveLedger.Progress.QUEUED,retained.progress());assertEquals(ManagedSourceSaveLedger.Resolution.UNRESOLVED,retained.resolution());
            assertEquals(0,snapshot.flights());assertSame(original.get(),snapshot.primary());assertFalse(snapshot.observationSealed());
            assertSame(original.get(),assertThrows(IllegalStateException.class,()->f.ledger.begin(f.loader,SLOT)).getCause());
            f.ledger.beginStoppingObservation();assertThrows(IllegalStateException.class,f.ledger::sealObservation);
            assertSame(original.get(),f.ledger.snapshot().primary());
        }
    }
    @Test void oldFlightCompletionCannotCompleteNewlyAcceptedGeneration()throws Exception {
        try(Fixture f=new Fixture()) {
            Object old=new Object(),fresh=new Object();f.enqueue(old,new byte[]{1});
            CountDownLatch consumed=new CountDownLatch(1),release=new CountDownLatch(1),done=new CountDownLatch(1);
            AtomicReference<Throwable> failure=new AtomicReference<>();
            f.tasks.add(()->{try{var flight=f.removeActual();consumed.countDown();assertTrue(release.await(3,TimeUnit.SECONDS));f.ledger.beforeWrite(flight,old,new byte[]{1});f.ledger.endFlight(flight);}catch(Throwable cause){failure.set(cause);}finally{consumed.countDown();done.countDown();}});
            Throwable primary=null;
            try {
                assertTrue(consumed.await(3,TimeUnit.SECONDS));f.enqueue(fresh,new byte[]{2});release.countDown();assertTrue(done.await(3,TimeUnit.SECONDS));assertNull(failure.get());
                var rows=f.ledger.snapshot().attempts();assertEquals(ManagedSourceSaveLedger.Progress.CALLS_RETURNED,rows.get(0).progress());assertEquals(ManagedSourceSaveLedger.Progress.QUEUED,rows.get(1).progress());assertEquals(2,rows.get(1).acceptedGeneration());
            } catch(Throwable cause){primary=cause;throw cause;}
            finally {
                release.countDown();
                try{assertTrue(done.await(3,TimeUnit.SECONDS),"old-flight helper not finished");}
                catch(Throwable cleanup){if(primary!=null){if(primary!=cleanup)primary.addSuppressed(cleanup);}else throw cleanup;}
            }
        }
    }
    @Test void swallowedOriginalCauseAndCleanupCauseKeepIdentityAndDoNotSeal()throws Exception {
        try(Fixture f=new Fixture()) {
            Object compound=new Object();var a=f.ledger.begin(f.loader,SLOT);IOException original=new IOException("actual encoder failure"),cleanup=new IOException("actual later cleanup failure");
            f.ledger.failure(a,original);f.ledger.failure(a,cleanup);f.ledger.failure(a,cleanup);f.ledger.endProducer(a);
            assertSame(original,f.ledger.snapshot().primary());assertSame(original,f.ledger.snapshot().attempts().getFirst().primary());assertArrayEquals(new Throwable[]{cleanup},original.getSuppressed());
            f.ledger.beginStoppingObservation();assertThrows(IllegalStateException.class,f.ledger::sealObservation);
            assertThrows(IllegalStateException.class,()->f.ledger.begin(f.loader,SLOT));
        }
    }
    @Test void activeEncoderAndAcceptedFlightBothRejectSealAndReturnedCallsRemainUnresolved()throws Exception {
        try(Fixture f=new Fixture()) {
            Object c=new Object();var a=f.ledger.begin(f.loader,SLOT);f.ledger.beginStoppingObservation();assertThrows(IllegalStateException.class,f.ledger::sealObservation);
            f.ledger.finalReference(a,c);f.ledger.finalNbt(a,c,new byte[]{1});f.ledger.putReturned(a,c,f.actualMap.put(SLOT,c));f.ledger.endProducer(a);
            assertThrows(IllegalStateException.class,f.ledger::sealObservation);
            f.onIo(()->{var flight=f.removeActual();assertThrows(IllegalStateException.class,f.ledger::sealObservation);f.ledger.endFlight(flight);});
            var observationSealed=f.ledger.sealObservation();assertTrue(observationSealed.observationSealed());assertEquals(ManagedSourceSaveLedger.Resolution.UNRESOLVED,observationSealed.attempts().getFirst().resolution());
        }
    }
    @Test void stoppingRejectsLateProducerAndRetainsLateObservation()throws Exception {
        try(Fixture f=new Fixture()) {
            f.ledger.beginStoppingObservation();assertThrows(IllegalStateException.class,()->f.ledger.begin(f.loader,SLOT));
            var snapshot=f.ledger.snapshot();assertEquals(1,snapshot.attempts().size());assertEquals(0,snapshot.producers());assertNotNull(snapshot.attempts().getFirst().primary());assertFalse(snapshot.observationSealed());
        }
    }
    @Test void lruCloseAndNewRegionGenerationDoNotDeclareStoppingOrReviveOldRegion()throws Exception {
        try(Fixture f=new Fixture()) {
            Object oldRegion=new Object(),newRegion=new Object(),compound=new Object();f.enqueue(compound,new byte[]{1});
            f.onIo(()->{
                f.ledger.bindRegion(oldRegion);f.ledger.closeBeginning(oldRegion);f.ledger.closeReturned(oldRegion);f.ledger.bindRegion(newRegion);
                var flight=f.removeActual();f.ledger.regionWriteBeginning(newRegion,flight);
                assertFalse(f.ledger.snapshot().closeStarted());
                assertThrows(IllegalStateException.class,()->f.ledger.regionWriteBeginning(oldRegion,flight));f.ledger.endFlight(flight);
            });
            var regions=f.ledger.snapshot().regions();assertEquals(2,regions.size());assertEquals(1,regions.get(0).generation());assertEquals(2,regions.get(1).generation());assertTrue(regions.get(0).closeBegun());assertFalse(regions.get(1).closeBegun());
        }
    }
    @Test void encoderFailureRetainsActualStreamWithoutCloseAndReservationPreventsAnotherOpening()throws Exception {
        try(Fixture f=new Fixture(profile(1024,65536),new ManagedSourceSaveLedger.Capacity(32,1,16))) {
            Object first=new Object(),second=new Object();f.enqueue(first,new byte[]{1});
            f.onIo(()->{
                var a=f.removeActual();
                f.ledger.reserveOutput(a);
                int[] closeCalls={0};DataOutputStream stream=new DataOutputStream(new ByteArrayOutputStream()){@Override public void close()throws IOException{closeCalls[0]++;super.close();}};
                try(ManagedSaveContext c=ManagedSaveContext.writer(f.ledger)) {
                    c.attempt=a;c.opened(stream);IOException original=new IOException("actual observed encoder failure");f.ledger.failure(a,original);c.retainUnclosedOutput();
                }
                assertEquals(0,closeCalls[0]);f.ledger.endFlight(a);
            });
            assertEquals(1,f.ledger.snapshot().outputReservations());assertEquals(1024,f.ledger.snapshot().reservedOutputBytes());assertEquals(1,f.ledger.snapshot().attempts().getFirst().retainedOutputCount());
            assertThrows(IllegalStateException.class,()->f.enqueue(second,new byte[]{2}));
            f.ledger.beginStoppingObservation();assertThrows(IllegalStateException.class,f.ledger::sealObservation);
        }
    }
    @Test void unchangedPayloadRequiredAndLargeDisabledReturnNeverSignsSuccess()throws Exception {
        try(Fixture f=new Fixture()) {
            Object c=new Object();f.enqueue(c,new byte[]{1});f.onIo(()->{
                var a=f.removeActual();f.ledger.beforeWrite(a,c,new byte[]{1});f.ledger.largeDisabledReturn(a);f.ledger.regionReturned(a,0);f.ledger.endFlight(a);
            });
            var row=f.ledger.snapshot().attempts().getFirst();assertTrue(row.largeDisabledReturn());assertEquals(0,row.dataWritesReturned());assertEquals(ManagedSourceSaveLedger.Resolution.UNRESOLVED,row.resolution());
        }
        try(Fixture f=new Fixture()) {
            Object c=new Object();f.enqueue(c,new byte[]{1});f.onIo(()->{var a=f.removeActual();assertThrows(IllegalStateException.class,()->f.ledger.beforeWrite(a,c,new byte[]{2}));f.ledger.endFlight(a);});assertNotNull(f.ledger.snapshot().primary());
        }
    }
    @Test void exactInstallerAndDeclaredIoThreadAreRequired()throws Exception {
        try(Fixture f=new Fixture()) {
            assertThrows(IllegalStateException.class,()->f.ledger.requireIo(f.loader));assertThrows(IllegalStateException.class,()->f.ledger.assertInstallation(new Object()));
            f.onIo(()->{assertThrows(IllegalStateException.class,()->f.ledger.begin(f.loader,SLOT));f.ledger.requireIo(f.loader);});
            assertTrue(f.ledger.snapshot().attempts().isEmpty());
        }
    }
    @Test void actualMapPreviousMismatchRetainsBothAcceptedRecordsAndOriginalReferenceObligation()throws Exception {
        try(Fixture f=new Fixture()) {
            Object old=new Object(),fresh=new Object();f.enqueue(old,new byte[]{1});var a=f.ledger.begin(f.loader,SLOT);f.ledger.finalReference(a,fresh);f.ledger.finalNbt(a,fresh,new byte[]{2});
            Object actualPrevious=f.actualMap.put(SLOT,fresh);assertSame(old,actualPrevious);
            assertThrows(IllegalStateException.class,()->f.ledger.putReturned(a,fresh,new Object()));f.ledger.endProducer(a);
            assertEquals(2,f.ledger.snapshot().attempts().get(1).acceptedGeneration());assertSame(fresh,f.actualMap.get(SLOT));assertNotNull(f.ledger.snapshot().primary());assertEquals(2,f.ledger.snapshot().attempts().size());
        }
    }
    @Test void encodedRetentionBudgetRejectsBeforePutAndAttemptBudgetFreezesAdmission()throws Exception {
        try(Fixture f=new Fixture(profile(8,4),new ManagedSourceSaveLedger.Capacity(2,1,4))) {
            Object c=new Object();var a=f.ledger.begin(f.loader,SLOT);f.ledger.finalReference(a,c);
            assertThrows(IllegalStateException.class,()->f.ledger.finalNbt(a,c,new byte[5]));f.ledger.endProducer(a);
            assertTrue(f.actualMap.isEmpty());assertEquals(0,f.ledger.snapshot().retainedPayloadBytes());assertNotNull(f.ledger.snapshot().primary());assertThrows(IllegalStateException.class,()->f.ledger.begin(f.loader,SLOT));
        }
        try(Fixture f=new Fixture(profile(8,64),new ManagedSourceSaveLedger.Capacity(1,1,4))) {
            var a=f.ledger.begin(f.loader,SLOT);Object c=new Object();f.ledger.finalReference(a,c);f.ledger.finalNbt(a,c,new byte[]{1});f.ledger.noPut(a,c);f.ledger.endProducer(a);
            assertThrows(IllegalStateException.class,()->f.ledger.begin(f.loader,SLOT));assertEquals(1,f.ledger.snapshot().attempts().size());
        }
    }
    @Test void boundedCauseRetentionKeepsOriginalPrimaryAndOverflowOriginalCause()throws Exception {
        try(Fixture f=new Fixture(profile(8,64),new ManagedSourceSaveLedger.Capacity(2,1,1))) {
            var a=f.ledger.begin(f.loader,SLOT);IOException first=new IOException("first"),overflow=new IOException("overflow actual cause");
            StackTraceElement[] originalStack=overflow.getStackTrace();f.ledger.failure(a,first);f.ledger.failure(a,first);
            var boundary=assertThrows(IllegalStateException.class,()->f.ledger.failure(a,overflow));assertNull(boundary.getCause());assertEquals(0,boundary.getSuppressed().length);
            assertArrayEquals(originalStack,overflow.getStackTrace());assertNull(overflow.getCause());assertEquals(0,overflow.getSuppressed().length);
            assertSame(first,f.ledger.snapshot().primary());assertSame(first,f.ledger.snapshot().attempts().getFirst().primary());
            assertArrayEquals(new Throwable[]{boundary},first.getSuppressed());
            assertSame(boundary,assertThrows(IllegalStateException.class,()->f.ledger.failure(a,overflow)));
            assertSame(boundary,assertThrows(IllegalStateException.class,()->f.ledger.failure(a,boundary)));
            f.ledger.endProducer(a);
            var snapshot=f.ledger.snapshot();assertSame(first,snapshot.primary());assertSame(first,snapshot.attempts().getFirst().primary());
            assertEquals(ManagedSourceSaveLedger.Progress.FAULTED,snapshot.attempts().getFirst().progress());assertEquals(ManagedSourceSaveLedger.Resolution.UNRESOLVED,snapshot.attempts().getFirst().resolution());
            assertSame(first,assertThrows(IllegalStateException.class,()->f.ledger.begin(f.loader,SLOT)).getCause());
            assertArrayEquals(new Throwable[]{boundary},first.getSuppressed());assertEquals(0,boundary.getSuppressed().length);
        }
    }
    @Test void regionCloseFailureAdmitsCauseBeforeRetainingRegionPrimary()throws Exception {
        try(Fixture f=new Fixture(profile(8,64),new ManagedSourceSaveLedger.Capacity(2,1,1))) {
            Object firstRegion=new Object(),secondRegion=new Object();IOException first=new IOException("first close"),unadmitted=new IOException("second close");
            f.onIo(()->f.ledger.bindRegion(firstRegion));
            UUID firstRegionId=f.ledger.snapshot().regions().getFirst().id();
            f.onIo(()->f.ledger.bindRegion(secondRegion));
            UUID secondRegionId=f.ledger.snapshot().regions().stream().filter(r->!r.id().equals(firstRegionId)).findFirst().orElseThrow().id();
            f.ledger.closeBeginning(firstRegion);f.ledger.closeBeginning(secondRegion);
            f.ledger.closeFailure(firstRegion,first);f.ledger.closeFailure(firstRegion,first);
            var boundary=assertThrows(IllegalStateException.class,()->f.ledger.closeFailure(secondRegion,unadmitted));
            assertSame(boundary,assertThrows(IllegalStateException.class,()->f.ledger.closeFailure(secondRegion,boundary)));
            var snapshot=f.ledger.snapshot();assertSame(first,snapshot.primary());
            var firstObservation=snapshot.regions().stream().filter(r->r.id().equals(firstRegionId)).findFirst().orElseThrow();
            var secondObservation=snapshot.regions().stream().filter(r->r.id().equals(secondRegionId)).findFirst().orElseThrow();
            assertSame(first,firstObservation.primary());assertNull(secondObservation.primary());
            assertTrue(secondObservation.closeBegun());assertEquals(0,secondObservation.closeCallsReturned());assertEquals(ManagedSourceSaveLedger.Resolution.UNRESOLVED,secondObservation.resolution());
            assertArrayEquals(new Throwable[]{boundary},first.getSuppressed());assertNull(boundary.getCause());assertEquals(0,boundary.getSuppressed().length);
            assertNull(unadmitted.getCause());assertEquals(0,unadmitted.getSuppressed().length);
            assertSame(first,assertThrows(IllegalStateException.class,()->f.ledger.begin(f.loader,SLOT)).getCause());
            f.ledger.beginStoppingObservation();assertThrows(IllegalStateException.class,f.ledger::sealObservation);
        }
    }

    @Test void actualConcurrentMapRemoveCannotPassPutObservationRecord()throws Exception {
        try(Fixture f=new Fixture()) {
            Object compound=new Object();var a=f.ledger.begin(f.loader,SLOT);f.ledger.finalReference(a,compound);f.ledger.finalNbt(a,compound,new byte[]{1});
            CountDownLatch requested=new CountDownLatch(1),done=new CountDownLatch(1);AtomicReference<Throwable> failure=new AtomicReference<>();Throwable primary=null;
            try {
                try(var guard=f.queue.enter()) {
                    f.ledger.queuePutBeginning(a,compound);Object previous=f.actualMap.put(SLOT,compound);
                    f.tasks.add(()->{requested.countDown();try{var flight=f.removeActual();f.ledger.beforeWrite(flight,compound,new byte[]{1});f.ledger.endFlight(flight);}catch(Throwable cause){failure.set(cause);}finally{done.countDown();}});
                    assertTrue(requested.await(3,TimeUnit.SECONDS));long started=System.nanoTime();
                    while(!f.queue.observesQueuedThread(f.io) && done.getCount()!=0 && System.nanoTime()-started<TimeUnit.SECONDS.toNanos(3))Thread.onSpinWait();
                    assertTrue(f.queue.observesQueuedThread(f.io),"Actual IO must wait at the queue lock, before ledger put record");assertEquals(1,done.getCount());
                    f.ledger.putReturned(a,compound,previous);
                }
                f.ledger.endProducer(a);assertTrue(done.await(3,TimeUnit.SECONDS));assertNull(failure.get());assertNull(f.ledger.snapshot().primary());assertEquals(1,f.ledger.snapshot().attempts().getFirst().acceptedGeneration());
            } catch(Throwable cause){primary=cause;throw cause;}
            finally {
                try{assertTrue(done.await(3,TimeUnit.SECONDS),"Queue guard failed to release actual IO helper");}
                catch(Throwable cleanup){if(primary!=null){if(primary!=cleanup)primary.addSuppressed(cleanup);}else throw cleanup;}
            }
        }
    }

    @Test void queueAndFlightAbaRevokesCaptureEvenWhenActualCountsReturnToZero()throws Exception {
        try(Fixture f=new Fixture()) {
            var initial=f.ledger.captureGeneration(f.loader,0,0);f.ledger.requireGeneration(f.loader,initial,0,0);
            Object compound=new Object();f.enqueue(compound,new byte[]{1,2});
            assertThrows(IllegalStateException.class,()->f.ledger.captureGeneration(f.loader,0,0));
            var queued=f.ledger.captureGeneration(f.loader,1,0);
            f.onIo(()->{var a=f.removeActual();assertNotNull(a);f.ledger.endFlight(a);});
            assertTrue(f.actualMap.isEmpty());
            assertThrows(IllegalStateException.class,()->f.ledger.requireGeneration(f.loader,initial,0,0));
            assertThrows(IllegalStateException.class,()->f.ledger.requireGeneration(f.loader,queued,0,0));
            var current=f.ledger.captureGeneration(f.loader,0,0);f.ledger.requireGeneration(f.loader,current,0,0);
            assertEquals(ManagedSourceSaveLedger.Resolution.UNRESOLVED,f.ledger.snapshot().attempts().getFirst().resolution());
        }
    }
    @Test void generationRejectionPreservesOriginalProducerFailure()throws Exception {
        try(Fixture f=new Fixture()) {
            var initial=f.ledger.captureGeneration(f.loader,0,0);var attempt=f.ledger.begin(f.loader,SLOT);
            IOException original=new IOException("actual producer failed before queue");f.ledger.failure(attempt,original);f.ledger.endProducer(attempt);
            var rejected=assertThrows(IllegalStateException.class,()->f.ledger.requireGeneration(f.loader,initial,0,0));
            assertSame(original,rejected.getCause());assertSame(original,f.ledger.snapshot().primary());
            assertSame(original,assertThrows(IllegalStateException.class,()->f.ledger.captureGeneration(f.loader,0,0)).getCause());
            assertEquals(ManagedSourceSaveLedger.Resolution.UNRESOLVED,f.ledger.snapshot().attempts().getFirst().resolution());
        }
    }

}
