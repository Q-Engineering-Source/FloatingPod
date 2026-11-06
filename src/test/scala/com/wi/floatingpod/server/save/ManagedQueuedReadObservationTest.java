package com.wi.floatingpod.server.save;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.MethodInsnNode;

/** 实际sharedqueue/ledger/异actor线程；不把read lease授予保存IO资格或mutable copy完整性。 */
final class ManagedQueuedReadObservationTest {
    private static final ManagedSourceSaveLedger.Slot SLOT=new ManagedSourceSaveLedger.Slot(7,-4);
    @Test void actualSourceMixinBytecodeDoesNotInvertForeignLoaderMonitorAndQueueGuard()throws Exception {
        ClassNode saves=compiledMixin("ManagedAnvilSaveMixin");
        var consumer=saves.methods.stream().filter(m->m.name.equals("floatingpod$next")).findFirst().orElseThrow();
        for(var instruction:consumer.instructions)
            if(instruction instanceof MethodInsnNode call)
                assertFalse(call.name.equals("holdQueueGuard"),"writeNextIO must not prelock queue before UT synchronized fetch");
        ClassNode reads=compiledMixin("SourceAnvilReadMixin");
        var reader=reads.methods.stream().filter(m->m.name.equals("floatingpod$queueRead")).findFirst().orElseThrow();
        boolean monitorEntered=false,guardSeen=false;
        for(var instruction:reader.instructions){
            if(instruction.getOpcode()==Opcodes.MONITORENTER)monitorEntered=true;
            if(instruction instanceof MethodInsnNode call&&call.name.equals("floatingpod$beginSourceQueuedRead")){
                assertTrue(monitorEntered,"actual queued get must acquire loader monitor before queue guard");guardSeen=true;
            }
        }
        assertTrue(guardSeen,"actual source queue observation must remain installed");
    }
    private static ClassNode compiledMixin(String name)throws Exception{
        try(var input=ManagedQueuedReadObservationTest.class.getResourceAsStream("/com/wi/floatingpod/mixin/"+name+".class")){
            assertNotNull(input,"actual production mixin class required");ClassNode node=new ClassNode();
            new ClassReader(input).accept(node,0);return node;
        }
    }
    @Test void sourceIoWaitingForUtLoaderMonitorLeavesQueueAvailableToActualMainProducer()throws Exception {
        try(var f=new ManagedSourceSaveLedgerTest.Fixture()){
            Object first=new Object(),latest=new Object();f.enqueue(first,new byte[]{1});
            CountDownLatch scheduled=new CountDownLatch(1),done=new CountDownLatch(1);AtomicReference<Throwable> failure=new AtomicReference<>();
            synchronized(f.loader){
                f.tasks.add(()->{scheduled.countDown();try{
                    synchronized(f.loader){var flight=f.removeActual();f.ledger.beforeWrite(flight,latest,new byte[]{2});f.ledger.endFlight(flight);}
                }catch(Throwable cause){failure.set(cause);}finally{done.countDown();}});
                assertTrue(scheduled.await(3,TimeUnit.SECONDS));
                long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
                while(f.io.getState()!=Thread.State.BLOCKED&&System.nanoTime()-deadline<0)Thread.onSpinWait();
                assertEquals(Thread.State.BLOCKED,f.io.getState(),"actual File IO must wait for UT loader monitor");
                assertFalse(f.queue.observesQueuedThread(f.io),"File IO must not hold/wait queue before loader monitor");
                f.enqueue(latest,new byte[]{2}); // Main 持 UT monitor 时仍可替换原 queue token；不能死锁。
            }
            assertTrue(done.await(3,TimeUnit.SECONDS),"actual source flight must finish after loader monitor release");
            assertNull(failure.get());
        }
    }
    @Test void asyncActorGetRetainsExactAcceptedGenerationButCannotBecomeIoWriter()throws Exception {
        try(var f=new ManagedSourceSaveLedgerTest.Fixture()){
            Object payload=new Object();f.enqueue(payload,new byte[]{1,2,3});AtomicReference<Throwable> failure=new AtomicReference<>();CountDownLatch done=new CountDownLatch(1);
            Thread reader=new Thread(()->{
                try{ManagedSourceSaveLedger.QueuedReadObservation lease;
                    try(var guard=f.queue.enterSourceReadObservation()){Object actual=f.actualMap.get(SLOT);lease=f.ledger.observeSourceQueuedGet(f.loader,SLOT,actual);}
                    try(lease){assertTrue(lease.matchesActual(payload));assertEquals(1,lease.acceptedGeneration());assertNotNull(lease.acceptedAttempt());assertArrayEquals(new byte[]{1,2,3},lease.acceptedPayload().bytes());assertThrows(IllegalStateException.class,()->f.ledger.removed(f.loader,SLOT,payload));}
                }catch(Throwable cause){failure.set(cause);}finally{done.countDown();}
            },"actual-forge-like-read-actor");
            try{reader.start();assertTrue(done.await(3,TimeUnit.SECONDS),"read actor deadline");assertNull(failure.get());}
            finally{reader.join(3000);assertFalse(reader.isAlive(),"read actor leak");}
        }
    }
    @Test void getAndGenerationRecordStayBeforeCompetingActualRemove()throws Exception {
        try(var f=new ManagedSourceSaveLedgerTest.Fixture()){
            Object payload=new Object();f.enqueue(payload,new byte[]{1});CountDownLatch held=new CountDownLatch(1),release=new CountDownLatch(1),finished=new CountDownLatch(1);
            AtomicReference<Throwable> failure=new AtomicReference<>();AtomicReference<ManagedSourceSaveLedger.QueuedReadObservation> lease=new AtomicReference<>();
            Thread reader=new Thread(()->{
                try{try(var guard=f.queue.enterSourceReadObservation()){
                        Object actual=f.actualMap.get(SLOT);held.countDown();assertTrue(release.await(3,TimeUnit.SECONDS));lease.set(f.ledger.observeSourceQueuedGet(f.loader,SLOT,actual));}
                    try(var captured=lease.get()){assertEquals(1,captured.acceptedGeneration());assertTrue(captured.matchesActual(payload));}
                }catch(Throwable cause){failure.set(cause);}finally{held.countDown();finished.countDown();}
            },"queue-read-before-ledger-record");
            Throwable primary=null;
            try{
                reader.start();assertTrue(held.await(3,TimeUnit.SECONDS));CountDownLatch removed=new CountDownLatch(1);
                f.tasks.add(()->{try{var a=f.removeActual();f.ledger.endFlight(a);}catch(Throwable cause){failure.compareAndSet(null,cause);}finally{removed.countDown();}});
                long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);while(!f.queue.observesQueuedThread(f.io)&&System.nanoTime()-deadline<0)Thread.onSpinWait();
                assertTrue(f.queue.observesQueuedThread(f.io),"actual remove must wait behind get/record lock");assertEquals(1,removed.getCount());
                release.countDown();assertTrue(finished.await(3,TimeUnit.SECONDS));assertTrue(removed.await(3,TimeUnit.SECONDS));assertNull(failure.get());assertEquals(ManagedSourceSaveLedger.Progress.CALLS_RETURNED,f.ledger.snapshot().attempts().getFirst().progress());
            }catch(Throwable cause){primary=cause;throw cause;}
            finally{release.countDown();try{reader.join(3000);assertFalse(reader.isAlive(),"queue reader leaked");}catch(Throwable cleanup){if(primary==null)throw cleanup;ManagedSourceSaveLedger.suppressSecondary(primary,cleanup);}}
        }
    }
    @Test void leaseDoesNotProveCopyAtomicityAndWrongThreadCannotReleaseItsReservation()throws Exception {
        try(var f=new ManagedSourceSaveLedgerTest.Fixture()){
            Object payload=new Object();f.enqueue(payload,new byte[]{1});ManagedSourceSaveLedger.QueuedReadObservation lease;
            try(var guard=f.queue.enterSourceReadObservation()){lease=f.ledger.observeSourceQueuedGet(f.loader,SLOT,f.actualMap.get(SLOT));}
            try(lease){
                AtomicReference<Throwable> rejected=new AtomicReference<>();Thread actor=new Thread(()->{try{lease.close();}catch(Throwable cause){rejected.set(cause);}},"foreign-lease-close");
                try{actor.start();actor.join(3000);assertFalse(actor.isAlive());assertInstanceOf(IllegalStateException.class,rejected.get());}
                finally{actor.join(3000);assertFalse(actor.isAlive(),"foreign close leak");}
                f.onIo(()->{var a=f.removeActual();f.ledger.endFlight(a);});f.ledger.beginStoppingObservation();assertThrows(IllegalStateException.class,f.ledger::sealObservation);
                assertEquals(1,lease.acceptedGeneration());assertTrue(f.actualMap.isEmpty()); // immutable historical get, not current map/copy qualification
            }
            assertTrue(f.ledger.sealObservation().observationSealed());assertThrows(IllegalStateException.class,lease::close);
        }
    }
    @Test void foreignQueuedReferenceIsExplicitlyUnobservedInsteadOfBorrowingLatestGeneration()throws Exception {
        try(var f=new ManagedSourceSaveLedgerTest.Fixture()){
            Object accepted=new Object(),foreign=new Object();f.enqueue(accepted,new byte[]{1});
            try(var guard=f.queue.enterSourceReadObservation();var lease=f.ledger.observeSourceQueuedGet(f.loader,SLOT,foreign)){
                assertEquals(0,lease.acceptedGeneration());assertNull(lease.acceptedAttempt());assertNull(lease.acceptedPayload());assertTrue(lease.matchesActual(foreign));
                assertThrows(IllegalArgumentException.class,()->f.ledger.observeSourceQueuedGet(new Object(),SLOT,accepted));
            }
        }
    }
}
