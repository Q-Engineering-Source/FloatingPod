package com.wi.floatingpod.server.save;

import java.util.HashMap;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/** 直接执行真实 owner；有界线程、真实有限队列、显式 timeout 失败。 */
public final class ManagedOrdinarySaveAdmissionTest {
    @org.junit.jupiter.api.Test
    void finalSealingDefersPreviouslyAllowedProviderEffectsBeforeProducerAllocation(){
        var actual=new ManagedOrdinarySaveAdmission(Thread.currentThread(),4,ordinaryPolicy());
        var allowed=actual.restrictProviderDecision(ManagedOrdinarySaveAdmission.ProviderDecision.ALLOW,null);
        check(allowed==ManagedOrdinarySaveAdmission.ProviderDecision.ALLOW,"ordinary provider allowed before sealing");
        actual.beginFinalSealing();
        for(var operation:ManagedOrdinarySaveAdmission.ProviderOperation.values()){
            check(actual.restrictProviderDecision(allowed,null)==ManagedOrdinarySaveAdmission.ProviderDecision.DEFER,
                    "previous ALLOW must defer before "+operation+" effects during final-source handoff");
        }
        check(actual.restrictProviderDecision(ManagedOrdinarySaveAdmission.ProviderDecision.HELD,null)==
                ManagedOrdinarySaveAdmission.ProviderDecision.HELD,"existing protected responsibility remains held");
        denied(actual::candidate);
        Object sourceOwner=new Object();
        check(actual.tryReserveFinalSource(sourceOwner,640L)!=null,"actual preparation escrow expected");
        check(actual.restrictProviderDecision(allowed,null)==ManagedOrdinarySaveAdmission.ProviderDecision.DEFER,
                "escrow is not final-source activation");
        check(!actual.isFinalActive(),"ordinary deferral cannot mint a final permit");
    }
    @org.junit.jupiter.api.Test
    void finalProviderChargeCannotBorrowPreparationEscrowBeforeActualActivation(){
        var actual=owner();Object original=new Object();
        denied(()->actual.retainFinalProviderTemporary(256L,"provider scope"));
        actual.beginFinalSealing();
        check(actual.tryReserveFinalSource(original,640L)!=null,"real escrow expected");
        denied(()->actual.retainFinalProviderTemporary(256L,"provider scope"));
        check(!actual.isFinalActive(),"escrow is not final-source activation");
    }
    private static ManagedOrdinarySaveAdmission.OrdinaryProviderPolicy ordinaryPolicy(){
        return new ManagedOrdinarySaveAdmission.OrdinaryProviderPolicy(){
            public ManagedOrdinarySaveAdmission.ProviderDecision decide(net.minecraft.world.gen.ChunkProviderServer p,
                    net.minecraft.world.WorldServer w,net.minecraft.world.chunk.Chunk c,
                    ManagedOrdinarySaveAdmission.ProviderOperation op){throw new AssertionError("no provider in pure drain test");}
            public AutoCloseable retainTemporary(long bytes,String purpose){throw new AssertionError("no provider allocation in pure drain test");}
        };
    }
    @org.junit.jupiter.api.Test
    void ordinaryDrainWaitsForTokensAndEmptyFlightThenClosesEntrances() throws Exception {
        var owner=new ManagedOrdinarySaveAdmission(Thread.currentThread(),4,ordinaryPolicy());
        var token=owner.candidate();check(!owner.trySealOrdinaryAfterDrain(),"unsettled token must block seal");owner.noPut(token);
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var failure=new AtomicReference<Throwable>();
        Thread io=Thread.ofPlatform().start(()->{try(var flight=owner.flight()){entered.countDown();await(release);}catch(Throwable cause){failure.set(cause);}});
        try{await(entered);check(!owner.trySealOrdinaryAfterDrain(),"actual empty flight must block seal");}
        finally{release.countDown();io.join(5000);}
        check(!io.isAlive()&&failure.get()==null,"original flight must return normally");
        check(owner.trySealOrdinaryAfterDrain(),"empty original ordinary responsibility must seal");
        check(owner.trySealOrdinaryAfterDrain(),"same sealed responsibility remains sealed");
        denied(owner::candidate);denied(owner::reserveMutation);denied(()->owner.mainFlush(new Object()));
        Thread late=Thread.ofPlatform().start(()->{try{owner.flight();failure.set(new AssertionError("late flight accepted"));}
            catch(IllegalStateException expected){}catch(Throwable cause){failure.set(cause);}});
        late.join(5000);check(!late.isAlive()&&failure.get()==null,"late IO entrance must reject after seal");
    }
    @org.junit.jupiter.api.Test
    void ordinaryDrainDoesNotConvertPendingSuccessorOrFailureStopToCompletion(){
        var owner=new ManagedOrdinarySaveAdmission(Thread.currentThread(),4,ordinaryPolicy());
        var mutation=owner.reserveMutation();check(!owner.trySealOrdinaryAfterDrain(),"reserved mutation must block seal");
        owner.submitted(mutation,true);check(!owner.trySealOrdinaryAfterDrain(),"pending successor must block seal");
        var stopped=new ManagedOrdinarySaveAdmission(Thread.currentThread(),4,ordinaryPolicy());
        stopped.stop(null);denied(stopped::trySealOrdinaryAfterDrain);
        var failed=new ManagedOrdinarySaveAdmission(Thread.currentThread(),4,ordinaryPolicy());
        var cause=new IllegalStateException("original save failure");failed.stop(cause);
        try{failed.trySealOrdinaryAfterDrain();throw new AssertionError("UNKNOWN accepted");}
        catch(IllegalStateException actual){check(actual.getCause()==cause,"original cause must remain attached");}
    }
    @org.junit.jupiter.api.Test
    void failureBoundaryRejectsLateProviderBeforeTemporaryBudgetAndRetainsUnknownToken() throws Exception {
        var temporaryCalls=new java.util.concurrent.atomic.AtomicInteger();
        var policy=new ManagedOrdinarySaveAdmission.OrdinaryProviderPolicy(){
            public ManagedOrdinarySaveAdmission.ProviderDecision decide(net.minecraft.world.gen.ChunkProviderServer p,
                    net.minecraft.world.WorldServer w,net.minecraft.world.chunk.Chunk c,
                    ManagedOrdinarySaveAdmission.ProviderOperation op){throw new AssertionError("no provider decision after primary failure");}
            public AutoCloseable retainTemporary(long bytes,String purpose){temporaryCalls.incrementAndGet();return ()->{};}
        };
        var owner=new ManagedOrdinarySaveAdmission(Thread.currentThread(),4,policy);
        var token=owner.candidate();
        var primary=new IllegalStateException("original main failure");
        owner.unknownAtBoundary(primary);
        try{owner.beginProviderScope(new Object(),new Object());throw new AssertionError("late provider scope accepted");}
        catch(IllegalStateException failure){check(failure.getCause()==primary,"original failure must remain the cause");}
        check(temporaryCalls.get()==0,"late provider callback must fail before temporary budget");
        check(owner.outstanding()==1&&token.phase()==ManagedOrdinarySaveAdmission.Phase.CANDIDATE,
                "owner failure must retain the original candidate responsibility without inventing a token failure");
    }
    /** 保留原独立入口，同时让 Gradle/JUnit 实际执行既有线程与队列行为验证。 */
    @org.junit.jupiter.api.Test
    void existingBehavioralSuite() throws Throwable {
        main(new String[0]);
    }
    @org.junit.jupiter.api.Test
    void confirmedSuccessorRequiresOriginalPolicyAndActualDrain() {
        class Policy implements ManagedOrdinarySaveAdmission.OrdinaryProviderPolicy {
            RuntimeException refusal=new IllegalStateException("unconfirmed successor");
            int checks;
            public ManagedOrdinarySaveAdmission.ProviderDecision decide(net.minecraft.world.gen.ChunkProviderServer p,
                    net.minecraft.world.WorldServer w,net.minecraft.world.chunk.Chunk c,
                    ManagedOrdinarySaveAdmission.ProviderOperation op){throw new AssertionError("no game provider in pure test");}
            public AutoCloseable retainTemporary(long bytes,String purpose){throw new AssertionError("no provider allocation in pure test");}
            public void requireConfirmedSuccessor(){checks++;if(refusal!=null)throw refusal;}
        }
        var policy=new Policy();var owner=new ManagedOrdinarySaveAdmission(Thread.currentThread(),4,policy);
        owner.requireMainReadAvailable();
        denied(()->owner.resumeConfirmedSuccessor(policy));
        var token=owner.candidate();var mutation=owner.reserveMutation();
        org.junit.jupiter.api.Assertions.assertThrows(ManagedOrdinarySaveAdmission.MainReadPending.class,owner::requireMainReadAvailable);
        owner.submitted(mutation,true);
        check(owner.awaitingOrdinarySuccessor(),"accepted generation must retain its single successor slot");
        org.junit.jupiter.api.Assertions.assertThrows(ManagedOrdinarySaveAdmission.MainReadPending.class,owner::requireMainReadAvailable);
        denied(owner::reserveMutation);
        denied(()->owner.resumeConfirmedSuccessor(policy));
        check(policy.checks==0,"unsettled token must refuse before consulting successor issuer");
        owner.noPut(token);
        denied(()->owner.resumeConfirmedSuccessor(new Policy()));
        try{owner.resumeConfirmedSuccessor(policy);throw new AssertionError("unconfirmed issuer accepted");}
        catch(IllegalStateException cause){check(cause==policy.refusal,"original successor refusal lost");}
        denied(owner::candidate);
        policy.refusal=null;owner.resumeConfirmedSuccessor(policy);
        check(!owner.awaitingOrdinarySuccessor(),"actual confirmed resume must release successor capacity");
        owner.requireMainReadAvailable();
        owner.noPut(owner.candidate());
        denied(()->owner.resumeConfirmedSuccessor(policy));
        check(policy.checks==2,"only original drained resume may consult issuer");
    }
    @org.junit.jupiter.api.Test
    void finalSealingCompletesOnlyOriginalSuccessorsAndKeepsOrdinarySavesClosed() throws Throwable {
        class Policy implements ManagedOrdinarySaveAdmission.OrdinaryProviderPolicy {
            RuntimeException refusal=new IllegalStateException("original stop successor not confirmed");
            int checks;
            public ManagedOrdinarySaveAdmission.ProviderDecision decide(net.minecraft.world.gen.ChunkProviderServer p,
                    net.minecraft.world.WorldServer w,net.minecraft.world.chunk.Chunk c,
                    ManagedOrdinarySaveAdmission.ProviderOperation op){throw new AssertionError("no game provider in pure test");}
            public AutoCloseable retainTemporary(long bytes,String purpose){throw new AssertionError("no provider allocation in pure test");}
            public void requireConfirmedSuccessor(){checks++;if(refusal!=null)throw refusal;}
        }
        for(boolean acceptedBeforeSeal:new boolean[]{true,false}){
            var policy=new Policy();var owner=new ManagedOrdinarySaveAdmission(Thread.currentThread(),4,policy);
            Object sourceOwner=new Object();var token=owner.candidate();var mutation=owner.reserveMutation();
            if(acceptedBeforeSeal)owner.submitted(mutation,true);
            owner.beginFinalSealing();
            if(!acceptedBeforeSeal)owner.submitted(mutation,true);
            denied(()->owner.resumeConfirmedSuccessor(policy));
            check(policy.checks==0,"old token must block original successor validation");
            owner.noPut(token);
            var escrow=owner.tryReserveFinalSource(sourceOwner,640L);
            check(escrow!=null,"escrow may retain source preparation while original successor is pending");
            check(!owner.tryFenceFinalOldGeneration(escrow,sourceOwner),"retired successor is not a final fence receipt");
            denied(()->owner.resumeConfirmedSuccessor(new Policy()));
            try{owner.resumeConfirmedSuccessor(policy);throw new AssertionError("unconfirmed stop successor accepted");}
            catch(IllegalStateException actual){check(actual==policy.refusal,"original stop successor cause lost");}
            check(owner.awaitingOrdinarySuccessor(),"refused successor must remain original pending responsibility");
            policy.refusal=null;owner.resumeConfirmedSuccessor(policy);
            owner.requireMainReadAvailable();
            denied(owner::candidate);
            org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                    ()->owner.producer(new Object(),0,0,()->{}));
            // 同一stop内下一项真实mutation仍可提交；封普通producer不等于封掉停服SQL收尾。
            var next=owner.reserveMutation();owner.submitted(next,true);
            check(!owner.tryFenceFinalOldGeneration(escrow,sourceOwner),"next original successor must also block fence");
            owner.resumeConfirmedSuccessor(policy);owner.requireMainReadAvailable();
            check(policy.checks==3,"each actual successor must consult original issuer exactly once");
            check(owner.tryFenceFinalOldGeneration(escrow,sourceOwner),"settled original successors must permit final fence");
            denied(owner::candidate);denied(owner::reserveMutation);
            denied(()->owner.resumeConfirmedSuccessor(policy));
            check(!owner.isFinalActive(),"admission fence alone cannot activate final world-save source");
        }
    }

    @org.junit.jupiter.api.Test
    void ordinaryMutationWaitsForActualEmptyFlight() throws Exception {
        var policy=new ManagedOrdinarySaveAdmission.OrdinaryProviderPolicy(){
            public ManagedOrdinarySaveAdmission.ProviderDecision decide(net.minecraft.world.gen.ChunkProviderServer p,
                    net.minecraft.world.WorldServer w,net.minecraft.world.chunk.Chunk c,
                    ManagedOrdinarySaveAdmission.ProviderOperation op){throw new AssertionError("no game provider in pure test");}
            public AutoCloseable retainTemporary(long bytes,String purpose){throw new AssertionError("no provider allocation in pure test");}
        };
        var owner=new ManagedOrdinarySaveAdmission(Thread.currentThread(),4,policy);
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var repository=new CountDownLatch(1);
        var error=new AtomicReference<Throwable>();
        Thread io=Thread.ofPlatform().start(()->{try(var flight=owner.flight()){entered.countDown();await(release);}catch(Throwable cause){error.set(cause);}});
        Thread worker=null;
        try{
            await(entered);var mutation=owner.reserveMutation();owner.submitted(mutation,true);
            worker=Thread.ofPlatform().start(()->{try{mutation.awaitBeforeRepository();repository.countDown();}catch(Throwable cause){error.set(cause);}});
            blocked(repository);release.countDown();await(repository);join(worker);join(io);
            check(error.get()==null,"actual empty Flight barrier failure "+error.get());
        }finally{release.countDown();io.interrupt();join(io);if(worker!=null){worker.interrupt();join(worker);}}
    }
    static void check(boolean value,String message){if(!value)throw new AssertionError(message);}
    static void await(CountDownLatch latch)throws Exception{check(latch.await(3,TimeUnit.SECONDS),"latch timeout");}
    static void blocked(CountDownLatch latch)throws Exception{check(!latch.await(60,TimeUnit.MILLISECONDS),"worker escaped barrier");}
    static void join(Thread t)throws Exception{t.join(3000);check(!t.isAlive(),"thread timeout");}
    static void denied(Runnable body){try{body.run();throw new AssertionError("expected denial");}catch(IllegalStateException expected){}}
    static ManagedOrdinarySaveAdmission owner(){return new ManagedOrdinarySaveAdmission(Thread.currentThread(),4);}
    static void rejectedReopens(){var o=owner();var r=o.reserveMutation();denied(o::candidate);o.submitted(r,false);o.noPut(o.candidate());check(o.outstanding()==0,"rejected responsibility");}
    static void earlyWorkerAndFlight()throws Exception{
        var o=owner();var t=o.candidate();Object a=new Object(),p=new Object();o.beforePut(t,a,p);o.putReturned(t,1,null);
        var r=o.reserveMutation();var entered=new CountDownLatch(1);var sql=new CountDownLatch(1);var remove=new CountDownLatch(1);var finish=new CountDownLatch(1);var flight=new CountDownLatch(1);var errors=new AtomicReference<Throwable>();
        Thread worker=Thread.ofPlatform().start(()->{try{entered.countDown();r.awaitBeforeRepository();sql.countDown();}catch(Throwable x){errors.set(x);}});
        Thread io=Thread.ofPlatform().start(()->{try{await(remove);o.removed(t,a,1,p);flight.countDown();await(finish);o.flightReturned(t);}catch(Throwable x){errors.set(x);}});
        try{await(entered);blocked(sql);o.submitted(r,true);denied(o::candidate);blocked(sql);remove.countDown();await(flight);blocked(sql);finish.countDown();await(sql);join(worker);join(io);check(errors.get()==null,"worker failure "+errors.get());check(o.outstanding()==0,"flight retained");}
        finally{remove.countDown();finish.countDown();worker.interrupt();io.interrupt();join(worker);join(io);}
    }
    static void replacementAndActualQueue()throws Exception{
        var o=owner();var old=o.candidate();var fresh=o.candidate();Object oldA=new Object(),oldP=new Object(),newA=new Object(),newP=new Object();o.beforePut(old,oldA,oldP);o.putReturned(old,1,null);
        var guardRef=new AtomicReference<ManagedSaveQueueObservation>();var map=new HashMap<Integer,Object>();map.put(7,oldP);var visible=new CountDownLatch(1);var attempted=new CountDownLatch(1);var consumed=new CountDownLatch(1);var errors=new AtomicReference<Throwable>();
        Thread io=Thread.ofPlatform().unstarted(()->{try{await(visible);attempted.countDown();try(var guard=guardRef.get().enter()){Object removed=map.remove(7);check(removed==newP,"actual queue payload");o.removed(fresh,newA,2,removed);}o.flightReturned(fresh);consumed.countDown();}catch(Throwable x){errors.set(x);}});
        var ledger=new ManagedSourceSaveLedger(java.util.UUID.randomUUID(),Thread.currentThread(),new Object(),io,com.wi.floatingpod.config.SourceLimitProfile.of(4096,2048,1024,64,1000,1000000,4096,4096),new ManagedSourceSaveLedger.Capacity(8,4,8));
        var queue=new ManagedSaveQueueObservation(ledger);guardRef.set(queue);io.start();
        try{try(var guard=queue.enter()){o.beforePut(fresh,newA,newP);Object previous=map.put(7,newP);check(previous==oldP,"actual previous");visible.countDown();await(attempted);long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);while(!queue.observesQueuedThread(io)&&System.nanoTime()<deadline)Thread.onSpinWait();check(queue.observesQueuedThread(io),"IO never queued on actual guard");blocked(consumed);check(fresh.phase()==ManagedOrdinarySaveAdmission.Phase.PUTTING,"generation bound before actual putReturned");check(old.phase()==ManagedOrdinarySaveAdmission.Phase.QUEUED,"old premature completion");o.putReturned(fresh,2,old);}await(consumed);join(io);check(errors.get()==null,"queue thread failure "+errors.get());check(old.phase()==ManagedOrdinarySaveAdmission.Phase.COMPLETE&&o.outstanding()==0,"replacement responsibility");}
        finally{visible.countDown();io.interrupt();join(io);}
    }
    static void mutationDuringEvent(){var o=owner();var t=o.candidate();var r=o.reserveMutation();o.submitted(r,true);denied(()->o.beforePut(t,new Object(),new Object()));o.noPut(t);check(o.outstanding()==0,"rejected candidate not released");}
    static void failurePreservesCause()throws Exception{
        var o=owner();var t=o.candidate();var r=o.reserveMutation();var cause=new RuntimeException("actual physical failure");o.submitted(r,true);o.failed(t,cause);var error=new AtomicReference<Throwable>();Thread w=Thread.ofPlatform().start(()->{try{r.awaitBeforeRepository();}catch(Throwable x){error.set(x);}});join(w);check(error.get()!=null&&error.get().getCause()==cause,"original cause identity");check(o.outstanding()==1&&t.phase()==ManagedOrdinarySaveAdmission.Phase.UNKNOWN,"unknown silently dropped");
    }
    static void submitThrowUnknown(){var o=owner();var r=o.reserveMutation();var x=new RuntimeException("submit uncertain");o.submissionUnknown(r,x);try{o.candidate();throw new AssertionError();}catch(IllegalStateException e){check(e.getCause()==x,"submission cause");}}
    static void foreignToken(){var one=owner();var two=owner();var t=one.candidate();denied(()->two.noPut(t));check(one.outstanding()==1&&two.outstanding()==0,"foreign ownership");one.noPut(t);}
    static void finiteQueueReject(){var o=owner();var queue=new ArrayBlockingQueue<Object>(1);check(queue.offer(new Object()),"queue initial");var r=o.reserveMutation();boolean accepted=queue.offer(r);check(!accepted,"bounded actual queue must reject");o.submitted(r,accepted);o.noPut(o.candidate());}
    static void alreadyFlyingBeforeMutation()throws Exception{
        var o=owner();var token=o.candidate();Object attempt=new Object(),payload=new Object();o.beforePut(token,attempt,payload);o.putReturned(token,1,null);
        var inFlight=new CountDownLatch(1);var release=new CountDownLatch(1);var sql=new CountDownLatch(1);var errors=new AtomicReference<Throwable>();
        Thread io=Thread.ofPlatform().start(()->{try{o.removed(token,attempt,1,payload);inFlight.countDown();await(release);o.flightReturned(token);}catch(Throwable x){errors.set(x);}});
        Thread worker=null;
        try{await(inFlight);var mutation=o.reserveMutation();o.submitted(mutation,true);worker=Thread.ofPlatform().start(()->{try{mutation.awaitBeforeRepository();sql.countDown();}catch(Throwable x){errors.set(x);}});blocked(sql);release.countDown();await(sql);join(worker);join(io);check(errors.get()==null,"prior flight failure "+errors.get());}
        finally{release.countDown();io.interrupt();if(worker!=null){worker.interrupt();join(worker);}join(io);}
    }
    static void stopFailureWakesAcceptedWorker()throws Exception{
        var o=owner();var token=o.candidate();var mutation=o.reserveMutation();o.submitted(mutation,true);var entered=new CountDownLatch(1);var done=new CountDownLatch(1);var result=new AtomicReference<Throwable>();
        Thread worker=Thread.ofPlatform().start(()->{entered.countDown();try{mutation.awaitBeforeRepository();}catch(Throwable x){result.set(x);}finally{done.countDown();}});
        try{await(entered);blocked(done);var original=new IllegalStateException("actual stopping failure");o.stop(original);await(done);join(worker);check(result.get()!=null&&result.get().getCause()==original,"stop cause identity");check(o.outstanding()==1,"stop dropped pending responsibility");denied(o::candidate);o.noPut(token);}
        finally{worker.interrupt();join(worker);}
    }
    static void replacementDoesNotReleaseOldFlight()throws Exception{
        var o=owner();var first=o.candidate();Object a=new Object(),p=new Object();o.beforePut(first,a,p);o.putReturned(first,1,null);
        var flying=new CountDownLatch(1);var release=new CountDownLatch(1);var error=new AtomicReference<Throwable>();
        Thread io=Thread.ofPlatform().start(()->{try{o.removed(first,a,1,p);flying.countDown();await(release);o.flightReturned(first);}catch(Throwable x){error.set(x);}});
        try{await(flying);var second=o.candidate();o.beforePut(second,new Object(),new Object());o.putReturned(second,2,null);check(o.outstanding()==2,"new put released unrelated old flight");release.countDown();join(io);check(error.get()==null&&o.outstanding()==1,"old flight completion lost new queued obligation");}
        finally{release.countDown();io.interrupt();join(io);}
    }
    static void liveOnlyQueueAndProducerLifecycle()throws Throwable{
        var o=owner();Object loader=new Object();var tasks=new ArrayBlockingQueue<Runnable>(1);var errors=new AtomicReference<Throwable>();Runnable stop=()->{};
        Thread io=Thread.ofPlatform().unstarted(()->{try{for(;;){Runnable task=tasks.take();if(task==stop)return;task.run();}}catch(Throwable x){errors.set(x);}});
        var guard=new ManagedSaveQueueObservation(Thread.currentThread(),io);var actual=new HashMap<Integer,Object>();io.start();
        try{
            for(int n=0;n<24;n++){
                Object payload=new Object();var checked=new java.util.concurrent.atomic.AtomicInteger();
                try(var p=o.producer(loader,7,8,checked::incrementAndGet)){
                    p.beforePut(loader,7,8,payload);
                    try(var lock=guard.enter()){p.putReturned(actual.put(7,payload));}
                }
                check(checked.get()==1,"actual put recheck missing");
                var done=new CountDownLatch(1);
                tasks.put(()->{try(var f=o.flight()){try(var lock=guard.enter()){f.removed(loader,7,8,actual.remove(7));}f.beforeWrite(loader,7,8,payload);}finally{done.countDown();}});
                await(done);check(errors.get()==null,"live IO failure "+errors.get());check(o.outstanding()==0,"live token/history retained");
            }
            try(var noPut=o.producer(loader,9,9,()->{})){check(o.currentProducer()==noPut,"missing current producer");}
            check(o.currentProducer()==null&&o.outstanding()==0,"no-put producer leak");
        }finally{tasks.offer(stop);io.interrupt();join(io);}
    }
    static void liveEventMutationRecheck()throws Throwable{
        var o=owner();Object loader=new Object();var r=new AtomicReference<ManagedOrdinarySaveAdmission.Mutation>();
        try(var p=o.producer(loader,1,2,()->{var value=o.reserveMutation();o.submitted(value,true);r.set(value);})){try{p.beforePut(loader,1,2,new Object());throw new AssertionError("mutation recheck accepted save");}catch(IllegalStateException expected){}}
        check(o.outstanding()==0,"recheck refusal did not release candidate");
        var result=new AtomicReference<Throwable>();Thread worker=Thread.ofPlatform().start(()->{try{r.get().awaitBeforeRepository();}catch(Throwable x){result.set(x);}});join(worker);check(result.get()==null,"released pre-put candidate blocked SQL");
    }
    static void mainFlushLexicalScope()throws Throwable{
        var o=owner();Object loader=new Object(),other=new Object();denied(()->o.flight(loader));
        try(var outer=o.mainFlush(loader)){
            try(var nested=o.mainFlush(loader)){try(var f=o.flight(loader)){}denied(()->o.flight(other));}
            try(var f=o.flight(loader)){}
            var actual=new RuntimeException("original flush failure");
            try(var nested=o.mainFlush(loader)){throw actual;}catch(RuntimeException seen){check(seen==actual,"flush cause replaced");}
            try(var f=o.flight(loader)){}
        }
        denied(()->o.flight(loader));
        var foreign=new AtomicReference<Throwable>();Thread t=Thread.ofPlatform().start(()->{try{o.mainFlush(loader);}catch(Throwable x){foreign.set(x);}});join(t);check(foreign.get() instanceof IllegalStateException,"foreign Main flush admitted");
    }
    static void mainFlushDoesNotReleaseFileFlight()throws Throwable{
        var o=owner();Object loader=new Object(),old=new Object(),fresh=new Object();
        try(var p=o.producer(loader,1,2,()->{})){p.beforePut(loader,1,2,old);p.putReturned(null);}
        var inFlight=new CountDownLatch(1);var release=new CountDownLatch(1);var error=new AtomicReference<Throwable>();
        Thread io=Thread.ofPlatform().start(()->{try(var f=o.flight(loader)){f.removed(loader,1,2,old);inFlight.countDown();await(release);f.beforeWrite(loader,1,2,old);}catch(Throwable x){error.set(x);}});
        try{await(inFlight);try(var p=o.producer(loader,3,4,()->{})){p.beforePut(loader,3,4,fresh);p.putReturned(null);}
            var mutation=o.reserveMutation();o.submitted(mutation,true);
            try(var scope=o.mainFlush(loader);var f=o.flight(loader)){f.removed(loader,3,4,fresh);f.beforeWrite(loader,3,4,fresh);}
            check(o.outstanding()==1,"Main flush released concurrent File IO flight");release.countDown();join(io);check(error.get()==null&&o.outstanding()==0,"File IO completion failed "+error.get());
        }finally{release.countDown();io.interrupt();join(io);}
    }
    static void quarantinedLoaderDoesNotKillHealthyLoader()throws Throwable{
        var o=owner();Object bad=new Object(),healthy=new Object(),badPayload=new Object(),goodPayload=new Object();
        try(var p=o.producer(bad,1,2,()->{})){p.beforePut(bad,1,2,badPayload);p.putReturned(null);}
        try(var p=o.producer(healthy,3,4,()->{})){p.beforePut(healthy,3,4,goodPayload);p.putReturned(null);}
        var cause=new RuntimeException("actual bad loader physical write");var diagnostic=new RuntimeException("diagnostic failed");var results=new AtomicReference<Throwable>();var globalCompleted=new java.util.concurrent.atomic.AtomicInteger();var logged=new AtomicReference<Throwable>();
        Thread io=Thread.ofPlatform().start(()->{try{
            boolean badMore=o.executeLoader(bad,()->{var f=o.currentFlight();f.removed(bad,1,2,badPayload);throw cause;},x->{logged.set(x);throw diagnostic;});
            check(!badMore,"bad loader not removed from scheduler");globalCompleted.incrementAndGet();
            check(o.schedulerQueuedCount(healthy)==1,"another loader failure stranded already QUEUED healthy payload");
            o.requireSchedulerQueuedPayload(healthy,3,4,goodPayload);
            boolean goodMore=o.executeLoader(healthy,()->{var f=o.currentFlight();f.removed(healthy,3,4,goodPayload);f.beforeWrite(healthy,3,4,goodPayload);return false;},x->{throw new AssertionError("healthy loader logged fault",x);});
            check(!goodMore,"healthy loader unexpectedly retained");globalCompleted.incrementAndGet();
        }catch(Throwable x){results.set(x);}});
        join(io);check(results.get()==null,"shared worker died "+results.get());check(globalCompleted.get()==2,"scheduler counters not complete");check(logged.get()==cause,"wrong logged cause");check(cause.getSuppressed().length==1&&cause.getSuppressed()[0]==diagnostic,"diagnostic cause lost");check(o.outstanding()==1,"failed token silently dropped");
        try{o.requireHealthyAfterFlush();throw new AssertionError("global done erased loader failure");}catch(IllegalStateException actual){check(actual.getCause()==cause,"flush lost original cause");}
        denied(()->o.mainFlush(healthy));denied(o::candidate);
    }
    static void failureAfterActualPutRemainsUnknown()throws Throwable{
        var o=owner();Object loader=new Object();var cause=new AssertionError("queueIO failed after actual Map.put");
        try(var p=o.producer(loader,1,2,()->{})){p.beforePut(loader,1,2,new Object());p.putReturned(null);p.failedAfterAdmission(cause);}
        check(o.outstanding()==1,"post-put failure silently dropped token");
        try{o.requireHealthyAfterFlush();throw new AssertionError("post-put failure became success");}catch(IllegalStateException actual){check(actual.getCause()==cause,"post-put cause lost");}
    }
    static void interruptedWaitCannotBecomeFlushSuccess()throws Throwable{
        var o=owner();Object loader=new Object(),payload=new Object();Thread actualMain=Thread.currentThread();
        try(var p=o.producer(loader,1,2,()->{})){p.beforePut(loader,1,2,payload);p.putReturned(null);}
        var inFlight=new CountDownLatch(1);var releaseFlight=new CountDownLatch(1);var waitEntered=new CountDownLatch(1);var neverFinishedWait=new CountDownLatch(1);
        var ioFailure=new AtomicReference<Throwable>();var interrupterFailure=new AtomicReference<Throwable>();var originalInterrupt=new AtomicReference<InterruptedException>();
        Thread io=Thread.ofPlatform().start(()->{try(var f=o.flight(loader)){f.removed(loader,1,2,payload);inFlight.countDown();await(releaseFlight);f.beforeWrite(loader,1,2,payload);}catch(Throwable cause){ioFailure.set(cause);}});
        Thread interrupter=Thread.ofPlatform().unstarted(()->{try{await(waitEntered);long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(3);
            while(actualMain.getState()!=Thread.State.TIMED_WAITING&&actualMain.getState()!=Thread.State.WAITING&&System.nanoTime()<deadline)Thread.onSpinWait();
            check(actualMain.getState()==Thread.State.TIMED_WAITING||actualMain.getState()==Thread.State.WAITING,"flush wait never actually blocked");actualMain.interrupt();
        }catch(Throwable cause){interrupterFailure.set(cause);}});
        try{
            await(inFlight);interrupter.start();boolean returned=false;Throwable seen=null;
            try{
                o.flush(()->{
                    try{o.waitForFlush(()->{waitEntered.countDown();try{check(neverFinishedWait.await(3,TimeUnit.SECONDS),"wait was not interrupted");}catch(InterruptedException actual){originalInterrupt.set(actual);throw actual;}});}
                    catch(InterruptedException swallowedByVanilla){throw new AssertionError("managed wait reached vanilla swallowing catch",swallowedByVanilla);}
                },o::requireHealthyAfterFlush);
                returned=true;
            }catch(ManagedOrdinarySaveAdmission.FlushWaitInterrupted actual){seen=actual;}
            check(!returned&&seen!=null,"interrupted flush returned success");check(Thread.currentThread().isInterrupted(),"managed interrupt status not restored");
            check(seen.getCause()==originalInterrupt.get()&&originalInterrupt.get()!=null,"original InterruptedException identity lost");
            check(o.outstanding()==1,"interrupted wait dropped unfinished flight");
            check(seen.getSuppressed().length==1&&seen.getSuppressed()[0].getCause()==originalInterrupt.get(),"post-flush diagnostic did not retain original cause as suppressed");
            Thread.interrupted();join(interrupter);check(interrupterFailure.get()==null,"interrupter failure "+interrupterFailure.get());
            releaseFlight.countDown();join(io);check(ioFailure.get()==null&&o.outstanding()==0,"real flight did not complete independently "+ioFailure.get());
            try{o.requireHealthyAfterFlush();throw new AssertionError("later IO completion erased interrupted flush failure");}catch(IllegalStateException actual){check(actual.getCause()==originalInterrupt.get(),"late observation lost original interrupt");}
        }finally{Thread.interrupted();releaseFlight.countDown();neverFinishedWait.countDown();io.interrupt();if(interrupter.getState()!=Thread.State.NEW){interrupter.interrupt();join(interrupter);}join(io);}
    }
    static void flushPrimarySurvivesDiagnosticFailure()throws Throwable{
        var o=owner();var primary=new AssertionError("actual original flush failure");var diagnostic=new IllegalStateException("actual outcome diagnostic failure");
        try{o.flush(()->{throw primary;},()->{throw diagnostic;});throw new AssertionError("flush reported success");}
        catch(AssertionError actual){check(actual==primary,"flush primary replaced");check(actual.getSuppressed().length==1&&actual.getSuppressed()[0]==diagnostic,"diagnostic failure lost");}
    }
    static void freshSourceBoundaryWaitsForOriginalTokensAndCannotBeReminted(){
        var o=owner();var original=o.candidate();
        check(o.tryFinishFreshSourceGeneration()==null,"live original token was treated as settled");
        o.noPut(original);
        var boundary=o.tryFinishFreshSourceGeneration();check(boundary!=null,"completed original token not observed");
        boundary.requireOriginal(o);
        denied(o::reserveMutation);
        denied(o::tryFinishFreshSourceGeneration);
        var lateSave=o.candidate();
        check(o.outstanding()==1,"late save still has its own real responsibility");
        o.noPut(lateSave);boundary.requireOriginal(o);
        try{boundary.requireOriginal(owner());throw new AssertionError("foreign save owner accepted");}
        catch(IllegalArgumentException expected){}
    }
    static void freshSourceBoundaryCannotClearUnknownSaveFailure(){
        var o=owner();var token=o.candidate();var cause=new IllegalStateException("original unresolved save");
        o.failed(token,cause);
        try{o.tryFinishFreshSourceGeneration();throw new AssertionError("UNKNOWN save issued a boundary");}
        catch(IllegalStateException actual){check(actual.getCause()==cause,"original save cause changed");}
        check(o.outstanding()==1,"failed save token was discarded");
    }
    static void freshSourceBoundaryWaitsForEmptyExecutingLoader()throws Throwable{
        var o=owner();Object loader=new Object();
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        var error=new AtomicReference<Throwable>();var result=new AtomicReference<Boolean>();
        Thread io=Thread.ofPlatform().start(()->{
            try{result.set(o.executeLoader(loader,()->{entered.countDown();await(release);return false;},cause->{throw new AssertionError("unexpected loader diagnostic",cause);}));}
            catch(Throwable cause){error.set(cause);}
        });
        try{
            await(entered);
            check(o.outstanding()==0,"counterexample requires no token");
            check(o.tryFinishFreshSourceGeneration()==null,"empty executing loader issued Fresh boundary before actual Flight close");
            release.countDown();join(io);
            check(error.get()==null,"loader failure "+error.get());
            check(Boolean.FALSE.equals(result.get()),"original empty loader return changed");
            var boundary=o.tryFinishFreshSourceGeneration();
            check(boundary!=null,"actual empty Flight close did not permit boundary");boundary.requireOriginal(o);
        }finally{release.countDown();join(io);}
    }
    static void lateFreshGenerationKeepsActualPutAndFlight()throws Throwable{
        var o=owner();Object loader=new Object(),payload=new Object();var boundary=o.tryFinishFreshSourceGeneration();
        var checks=new java.util.concurrent.atomic.AtomicInteger();
        try(var producer=o.producer(loader,3,4,checks::incrementAndGet)){producer.beforePut(loader,3,4,payload);producer.putReturned(null);}
        var error=new AtomicReference<Throwable>();
        Thread io=Thread.ofPlatform().start(()->{try{
            check(!o.executeLoader(loader,()->{var f=o.currentFlight();f.removed(loader,3,4,payload);f.beforeWrite(loader,3,4,payload);return false;},x->{throw new AssertionError("late loader failure",x);}),"late return changed");
        }catch(Throwable cause){error.set(cause);}});
        join(io);check(error.get()==null,"late generation failed "+error.get());check(checks.get()==1&&o.outstanding()==0,"late current check or settlement lost");
        boundary.requireOriginal(o);denied(o::tryFinishFreshSourceGeneration);
    }
    static void emptyFlightBodyFailurePreservesCause()throws Throwable{
        var o=owner();var cause=new AssertionError("actual empty loader failure");var seen=new AtomicReference<Throwable>();
        Thread io=Thread.ofPlatform().start(()->{try{check(!o.executeLoader(new Object(),()->{throw cause;},seen::set),"failed loader continued");}catch(Throwable error){seen.set(error);}});
        join(io);check(seen.get()==cause,"empty loader original cause changed");
        try{o.tryFinishFreshSourceGeneration();throw new AssertionError("failed empty flight minted boundary");}
        catch(IllegalStateException actual){check(actual.getCause()==cause,"empty flight failure cause lost");}
    }
    static void invalidFlightClosePreservesOriginalRefusal()throws Throwable{
        var o=owner();var seen=new AtomicReference<Throwable>();
        Thread io=Thread.ofPlatform().start(()->{
            var first=o.flight(new Object());var second=o.flight(new Object());
            try{first.close();throw new AssertionError("out of order close accepted");}catch(IllegalStateException cause){seen.set(cause);}
            second.close();first.close();denied(first::close);
        });join(io);
        check(seen.get()!=null,"missing close failure");
        check(o.tryFinishFreshSourceGeneration()!=null,"invalid external close changed healthy owner semantics");
    }
    static void tokenBodyFailureKeepsOriginalWithoutCloseCycle()throws Throwable{
        var o=owner();Object loader=new Object(),payload=new Object();
        try(var producer=o.producer(loader,1,2,()->{})){producer.beforePut(loader,1,2,payload);producer.putReturned(null);}
        var cause=new AssertionError("actual token body failure");var seen=new AtomicReference<Throwable>();var restored=new AtomicReference<Boolean>();
        Thread io=Thread.ofPlatform().start(()->{try{
            check(!o.executeLoader(loader,()->{o.currentFlight().removed(loader,1,2,payload);throw cause;},seen::set),"failed token loader continued");
            restored.set(o.currentFlight()==null);
        }catch(Throwable error){seen.set(error);}});
        join(io);check(seen.get()==cause,"token body original object changed");
        check(cause.getCause()==null&&cause.getSuppressed().length==0,"close added a cause cycle/wrapper");
        check(Boolean.TRUE.equals(restored.get())&&o.outstanding()==1,"failed token responsibility or TLS lost");
        try{o.tryFinishFreshSourceGeneration();throw new AssertionError("UNKNOWN token issued boundary");}
        catch(IllegalStateException actual){check(actual.getCause()==cause,"token boundary rejection lost original cause");}
    }
    static void schedulerPendingRequiresExactQueuedPayload()throws Throwable{
        var o=owner();Object loader=new Object(),payload=new Object();
        check(o.schedulerQueuedCount(loader)==0,"empty queued responsibility");
        try(var producer=o.producer(loader,3,4,()->{})){producer.beforePut(loader,3,4,payload);producer.putReturned(null);}
        check(o.schedulerQueuedCount(loader)==1,"late actual put missing from scheduler responsibility");
        o.requireSchedulerQueuedPayload(loader,3,4,payload);
        denied(()->o.requireSchedulerQueuedPayload(loader,3,4,new Object()));
        denied(()->o.requireSchedulerQueuedPayload(loader,4,3,payload));
        denied(()->o.requireSchedulerQueuedPayload(new Object(),3,4,payload));
        var errors=new AtomicReference<Throwable>();
        Thread io=Thread.ofPlatform().start(()->{try{
            check(o.executeLoader(loader,()->{o.currentFlight().removed(loader,3,4,payload);return true;},errors::set),"real queued write refused");
        }catch(Throwable cause){errors.set(cause);}});
        join(io);check(errors.get()==null&&o.schedulerQueuedCount(loader)==0&&o.outstanding()==0,"queued responsibility did not complete");
    }
    static void schedulerQuarantineRetainsOriginalCauseAndStopsRequeue()throws Throwable{
        var o=owner();Object loader=new Object(),payload=new Object();
        try(var producer=o.producer(loader,1,2,()->{})){producer.beforePut(loader,1,2,payload);producer.putReturned(null);}
        var cause=new IllegalStateException("actual scheduler handoff failure");o.schedulerFailed(loader,cause);
        check(o.schedulerQueuedCount(loader)==-1,"known quarantine must retire without requeue or new wrapper");
        try{o.requireHealthyAfterFlush();throw new AssertionError("quarantine lost failure");}
        catch(IllegalStateException seen){check(seen.getCause()==cause,"scheduler cause identity lost");}
        var errors=new AtomicReference<Throwable>();
        Thread io=Thread.ofPlatform().start(()->{try{
            check(!o.executeLoader(loader,()->{throw new AssertionError("quarantined original executed");},errors::set),"quarantine continued");
        }catch(Throwable x){errors.set(x);}});
        join(io);check(errors.get()==null&&o.outstanding()==1,"quarantine lost payload responsibility");
    }
    static void schedulerSeesLatePutAfterEmptyFlightReturns()throws Throwable{
        var o=owner();Object loader=new Object(),payload=new Object();
        var emptyReturned=new CountDownLatch(1);var latePut=new CountDownLatch(1);var errors=new AtomicReference<Throwable>();
        Thread io=Thread.ofPlatform().start(()->{try{
            check(!o.executeLoader(loader,()->false,errors::set),"empty loader must return false");
            emptyReturned.countDown();await(latePut);
            check(o.schedulerQueuedCount(loader)==1,"late put not visible at retirement");
            o.requireSchedulerQueuedPayload(loader,7,8,payload);
            check(o.executeLoader(loader,()->{o.currentFlight().removed(loader,7,8,payload);return true;},errors::set),"late payload not consumable");
        }catch(Throwable cause){errors.set(cause);}});
        try{await(emptyReturned);try(var producer=o.producer(loader,7,8,()->{})){producer.beforePut(loader,7,8,payload);producer.putReturned(null);}latePut.countDown();join(io);
            check(errors.get()==null&&o.outstanding()==0,"late put failure "+errors.get());
        }finally{latePut.countDown();io.interrupt();join(io);}
    }
    static void schedulerRecordingFailureCannotEscapeOriginalBoundary()throws Throwable{
        var o=owner();Object firstLoader=new Object(),secondLoader=new Object();
        var primary=new IllegalStateException("original scheduler primary");
        var association=new IllegalStateException("actual secondary getCause failure");
        var associationUnderMonitor=new AtomicReference<Boolean>(false);
        var secondary=new IllegalStateException("actual scheduler secondary"){
            @Override public synchronized Throwable getCause(){associationUnderMonitor.set(Thread.holdsLock(o));throw association;}
        };
        o.schedulerFailed(firstLoader,primary);
        o.schedulerFailed(secondLoader,secondary);
        o.associateSchedulerFailure(secondary);
        check(!associationUnderMonitor.get(),"Throwable association ran under Admission monitor");
        check(o.schedulerRecordingFailure()==association,"actual association failure not retained");
        check(o.schedulerQueuedCount(secondLoader)==-1,"recording failure reopened scheduler");
        try{o.requireHealthyAfterFlush();throw new AssertionError("primary lost");}
        catch(IllegalStateException actual){check(actual.getCause()==primary,"original primary replaced");}
        try{o.executeLoader(secondLoader,()->{throw new AssertionError("quarantined body executed");},cause->{});throw new AssertionError("Main quarantine not observed");}
        catch(IllegalStateException actual){check(actual==secondary,"original secondary lost");}
    }
    static void finalSealingDrainsActualOldTokensAndFencesOnlyAfterActualFlight()throws Throwable{
        var o=owner();Object loader=new Object(),payload=new Object(),sourceOwner=new Object();var queue=new HashMap<Object,Object>();
        try(var producer=o.producer(loader,3,4,()->{})){
            producer.beforePut(loader,3,4,payload);producer.putReturned(queue.put("slot",payload));
        }
        o.beginFinalSealing();denied(o::candidate);
        check(o.tryReserveFinalSource(sourceOwner,640L)==null,"old queued token must drain before source graph allocation");
        var observed=new AtomicReference<Throwable>();
        Thread io=new Thread(()->{
            try{o.executeLoader(loader,()->{
                var actual=queue.remove("slot");var flight=o.currentFlight();flight.removed(loader,3,4,actual);flight.beforeWrite(loader,3,4,actual);return false;
            },cause->{throw new AssertionError("healthy old flight diagnosed",cause);});}
            catch(Throwable cause){observed.set(cause);}
        },"final-old-generation-pure-io");
        io.start();join(io);if(observed.get()!=null)throw observed.get();
        check(o.outstanding()==0,"original actual flight must settle token");
        var escrow=o.tryReserveFinalSource(sourceOwner,640L);check(escrow!=null,"actual old lexical exit must admit source");
        check(o.tryFenceFinalOldGeneration(escrow,sourceOwner),"empty original Admission fence did not complete");
        // 仅此Admission层状态；不是scheduler membership/Minecraft ACK正例。
        check(!o.executeLoader(loader,()->{throw new AssertionError("fenced old empty body executed");},cause->{}),"fenced empty schedule must retire");
        o.requireHealthyAfterFlush();check(!o.isFinalActive(),"fence must not activate final source");
    }
    static void finalEscrowKeepsOriginalBudgetAndExactOwner(){
        var tiny=new ManagedOrdinarySaveAdmission(Thread.currentThread(),1);tiny.beginFinalSealing();
        denied(()->tiny.tryReserveFinalSource(new Object(),640L));
        var o=owner();Object origin=new Object();o.beginFinalSealing();var escrow=o.tryReserveFinalSource(origin,640L);
        denied(()->o.tryGrowFinalSourceEscrow(escrow,new Object(),800L,200L));
        denied(()->o.tryGrowFinalSourceEscrow(escrow,origin,2048L,1L));
        check(o.tryGrowFinalSourceEscrow(escrow,origin,800L,200L),"actual common budget growth failed");
        denied(()->o.tryReserveFinalSource(origin,640L));
        check(!o.isFinalActive(),"escrow cannot authorize a source");
    }
    static void finalEscrowPendingRetainsActualOldResponsibility(){
        var o=new ManagedOrdinarySaveAdmission(Thread.currentThread(),2);Object origin=new Object();
        var first=o.candidate();var second=o.candidate();o.beginFinalSealing();
        check(o.tryReserveFinalSource(origin,640L)==null,"full original tokens must keep phase pending before allocation");
        o.noPut(first);o.noPut(second);
        check(o.tryReserveFinalSource(origin,640L)!=null,"actual token completion must release source budget opportunity");
    }
    static void finalEscrowWaitsForActualEmptyFlightBeforeNearBudgetAllocation()throws Throwable{
        var o=owner();Object loader=new Object(),origin=new Object();
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var error=new AtomicReference<Throwable>();
        Thread io=new Thread(()->{
            try{o.executeLoader(loader,()->{entered.countDown();await(release);return false;},cause->{throw new AssertionError("healthy empty flight diagnosed",cause);});}
            catch(Throwable cause){error.set(cause);}
        },"final-escrow-empty-flight");
        io.start();await(entered);
        try{
            o.beginFinalSealing();check(o.outstanding()==0,"counterexample requires no token");
            check(o.tryReserveFinalSource(origin,2048L)==null,"unpaid old empty Flight overlapped near-B source");
        }finally{release.countDown();join(io);}
        if(error.get()!=null)throw error.get();
        var escrow=o.tryReserveFinalSource(origin,2048L);check(escrow!=null,"actual empty Flight exit did not free source admission");
        Thread late=new Thread(()->{
            try{check(!o.executeLoader(loader,()->{throw new AssertionError("new old Flight entered after source escrow");},cause->{throw new AssertionError(cause);}),"old empty scheduler entry must retire");}
            catch(Throwable cause){error.set(cause);}
        },"final-escrow-late-empty");
        late.start();join(late);if(error.get()!=null)throw error.get();
        denied(()->o.mainFlush(loader));
        // 整B已由本case的source占用，Mutation在真实容量门拒绝，不是escrow期一概禁止。
        denied(o::reserveMutation);
        o.requireHealthyAfterFlush();
    }
    static void finalEscrowWaitsForOriginalMainFlushAndCompletedProducer()throws Throwable{
        var flushOwner=owner();Object loader=new Object(),origin=new Object();
        try(var flush=flushOwner.mainFlush(loader)){
            flushOwner.beginFinalSealing();
            check(flushOwner.tryReserveFinalSource(origin,2048L)==null,"old MainFlush overlapped source");
        }
        check(flushOwner.tryReserveFinalSource(origin,2048L)!=null,"actual old MainFlush close must allow source");
        var o=owner();Object payload=new Object();var queue=new HashMap<Object,Object>();var error=new AtomicReference<Throwable>();
        try(var producer=o.producer(loader,5,6,()->{})){
            producer.beforePut(loader,5,6,payload);producer.putReturned(queue.put("slot",payload));
            Thread io=new Thread(()->{
                try{o.executeLoader(loader,()->{Object actual=queue.remove("slot");var flight=o.currentFlight();flight.removed(loader,5,6,actual);flight.beforeWrite(loader,5,6,actual);return false;},cause->{throw new AssertionError(cause);});}
                catch(Throwable cause){error.set(cause);}
            },"final-escrow-live-producer");
            io.start();join(io);if(error.get()!=null)throw error.get();
            check(o.outstanding()==0,"real producer token must already be complete");o.beginFinalSealing();
            check(o.tryReserveFinalSource(origin,2048L)==null,"completed token must not erase live old Producer scope");
        }
        check(o.tryReserveFinalSource(origin,2048L)!=null,"actual old Producer close must allow source");
    }
    static void finalEscrowAllowsRequiredMutationAndKeepsActualWorkCharge()throws Throwable{
        var o=owner();Object origin=new Object();o.beginFinalSealing();
        var escrow=o.tryReserveFinalSource(origin,1800L);check(escrow!=null,"actual source escrow required");
        var mutation=o.reserveMutation();
        var entered=new CountDownLatch(1);var finished=new CountDownLatch(1);var error=new AtomicReference<Throwable>();
        Thread worker=new Thread(()->{
            try{entered.countDown();mutation.awaitBeforeRepository();finished.countDown();}
            catch(Throwable cause){error.set(cause);}
        },"final-preparation-repository-barrier");
        worker.start();await(entered);o.submitted(mutation,true);await(finished);join(worker);
        if(error.get()!=null)throw error.get();
        // 2048-1800=248，真实56保费后只余192；await返回不是Work结束ACK。
        denied(()->o.tryGrowFinalSourceEscrow(escrow,origin,1993L,0L));
        check(o.tryGrowFinalSourceEscrow(escrow,origin,1992L,0L),"source grow lost actual 56 retained charge");
        denied(o::reserveMutation); // source1992+已付56已满整个B，重复申请是明确capacity failure。
        check(o.tryFenceFinalOldGeneration(escrow,origin),"original resolved mutation/empty physical fence required");
        try{o.reserveMutation();throw new AssertionError("fenced mutation entered");}
        catch(IllegalStateException cause){check("Source generation is closed to repository mutations".equals(cause.getMessage()),"final fence guard did not dominate capacity");}
        o.requireHealthyAfterFlush();
    }
    static void finalEscrowRejectedMutationsRetainChargeUntilActualGenerationEnd(){
        var o=owner();Object origin=new Object();o.beginFinalSealing();
        var escrow=o.tryReserveFinalSource(origin,1900L);check(escrow!=null,"actual source escrow required");
        var first=o.reserveMutation();o.submitted(first,false);
        var second=o.reserveMutation();o.submitted(second,false);
        denied(o::reserveMutation); // 1900+112+56超过2048，不能以Rejected假退款或永远pending。
        denied(()->o.tryGrowFinalSourceEscrow(escrow,origin,1937L,0L));
        check(o.tryGrowFinalSourceEscrow(escrow,origin,1936L,0L),"grow must preserve both actual allocated Mutation charges");
    }
    public static void main(String[] args)throws Throwable{
        if(args.length==1&&args[0].equals("scheduler-recording")){schedulerRecordingFailureCannotEscapeOriginalBoundary();System.out.println("PASS selected scheduler-recording behavioral case");return;}
        if(args.length==1&&args[0].equals("fresh-empty-flight")){freshSourceBoundaryWaitsForEmptyExecutingLoader();System.out.println("PASS selected fresh-empty-flight behavioral case");return;}
        rejectedReopens();earlyWorkerAndFlight();replacementAndActualQueue();mutationDuringEvent();failurePreservesCause();submitThrowUnknown();foreignToken();finiteQueueReject();
        alreadyFlyingBeforeMutation();stopFailureWakesAcceptedWorker();replacementDoesNotReleaseOldFlight();
        liveOnlyQueueAndProducerLifecycle();liveEventMutationRecheck();
        mainFlushLexicalScope();mainFlushDoesNotReleaseFileFlight();quarantinedLoaderDoesNotKillHealthyLoader();
        failureAfterActualPutRemainsUnknown();
        interruptedWaitCannotBecomeFlushSuccess();flushPrimarySurvivesDiagnosticFailure();
        freshSourceBoundaryWaitsForOriginalTokensAndCannotBeReminted();freshSourceBoundaryCannotClearUnknownSaveFailure();
        freshSourceBoundaryWaitsForEmptyExecutingLoader();lateFreshGenerationKeepsActualPutAndFlight();emptyFlightBodyFailurePreservesCause();invalidFlightClosePreservesOriginalRefusal();
        tokenBodyFailureKeepsOriginalWithoutCloseCycle();
        schedulerPendingRequiresExactQueuedPayload();schedulerQuarantineRetainsOriginalCauseAndStopsRequeue();schedulerSeesLatePutAfterEmptyFlightReturns();
        schedulerRecordingFailureCannotEscapeOriginalBoundary();
        finalSealingDrainsActualOldTokensAndFencesOnlyAfterActualFlight();finalEscrowKeepsOriginalBudgetAndExactOwner();finalEscrowPendingRetainsActualOldResponsibility();
        finalEscrowWaitsForActualEmptyFlightBeforeNearBudgetAllocation();finalEscrowWaitsForOriginalMainFlushAndCompletedProducer();
        finalEscrowAllowsRequiredMutationAndKeepsActualWorkCharge();finalEscrowRejectedMutationsRetainChargeUntilActualGenerationEnd();
        System.out.println("PASS 37 behavioral cases; real threads/finite queue; no Minecraft/SQL/runtime acceptance");
    }
}
