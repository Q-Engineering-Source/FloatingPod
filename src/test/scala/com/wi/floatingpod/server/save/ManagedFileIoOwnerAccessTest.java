package com.wi.floatingpod.server.save;

import org.junit.jupiter.api.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

/** 实际Thread/start因果逻辑测试；不代表Mixin已转换或FileIO已安装。 */
final class ManagedFileIoOwnerAccessTest {
    private static final class Running implements AutoCloseable {
        final CountDownLatch release=new CountDownLatch(1);
        final Thread thread=new Thread(() -> {
            try { release.await(); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        }, "managed-owner-observation-test");
        @Override public void close() throws InterruptedException {
            release.countDown();
            joinActual(thread);
        }
    }
    private static void joinActual(Thread thread) throws InterruptedException {
        InterruptedException primary=null;
        try { thread.join(3000); }
        catch(InterruptedException cause) { primary=cause; }
        if(thread.isAlive()) {
            thread.interrupt();
            try { thread.join(3000); }
            catch(InterruptedException cause) {
                if(primary==null)primary=cause;else primary.addSuppressed(cause);
            }
        }
        if(thread.isAlive()) {
            AssertionError unresolved=new AssertionError("Actual test thread failed bounded cleanup");
            if(primary==null)throw unresolved;else primary.addSuppressed(unresolved);
        }
        if(primary!=null)throw primary;
    }
    @Test void onlyActualReturnedStartExposesExactLiveReceiver() throws Exception {
        try(Running actual=new Running()) {
            var observation=new ManagedFileIoOwnerAccess.StartObservation(actual.thread);
            assertThrows(IllegalStateException.class, observation::actualStartedThread);
            actual.thread.start();
            observation.startReturned();
            assertSame(actual.thread, observation.actualStartedThread());
            assertThrows(IllegalStateException.class, observation::startReturned);
            actual.release.countDown();
            actual.thread.join(3000);
            assertFalse(actual.thread.isAlive(), "Actual termination deadline");
            assertThrows(IllegalStateException.class, observation::actualStartedThread);
        }
    }
    @Test void actualDoubleStartFailureRetainsOriginalCauseAndNeverRearms() throws Exception {
        try(Running actual=new Running()) {
            var observation=new ManagedFileIoOwnerAccess.StartObservation(actual.thread);
            actual.thread.start();
            IllegalThreadStateException original=assertThrows(IllegalThreadStateException.class, actual.thread::start);
            observation.startFailed(original);
            IllegalStateException unavailable=assertThrows(IllegalStateException.class, observation::actualStartedThread);
            assertSame(original, unavailable.getCause());
            assertThrows(IllegalStateException.class, observation::startReturned);
            assertSame(original, assertThrows(IllegalStateException.class, observation::actualStartedThread).getCause());
        }
    }
    @Test void wrongCallerCannotConsumeCreatorStartObservation() throws Exception {
        try(Running actual=new Running()) {
            var observation=new ManagedFileIoOwnerAccess.StartObservation(actual.thread);
            AtomicReference<Throwable> failure=new AtomicReference<>();
            CountDownLatch done=new CountDownLatch(1);
            Thread foreign=new Thread(() -> {
                try { observation.startReturned(); }
                catch(Throwable cause) { failure.set(cause); }
                finally { done.countDown(); }
            }, "managed-owner-wrong-caller");
            try(AutoCloseable cleanup=() -> joinActual(foreign)) {
                foreign.start();
                assertTrue(done.await(3, TimeUnit.SECONDS), "Actual foreign caller deadline");
                assertInstanceOf(IllegalStateException.class, failure.get());
                actual.thread.start();
                observation.startReturned();
                assertSame(actual.thread, observation.actualStartedThread());
            }
        }
    }
    @Test void nonNewAndConstructorCallerAreRejectedWithoutInventingAnOwner() throws Exception {
        assertThrows(NullPointerException.class, () -> new ManagedFileIoOwnerAccess.StartObservation(null));
        assertThrows(IllegalStateException.class, () -> new ManagedFileIoOwnerAccess.StartObservation(Thread.currentThread()));
        try(Running actual=new Running()) {
            actual.thread.start();
            assertThrows(IllegalStateException.class, () -> new ManagedFileIoOwnerAccess.StartObservation(actual.thread));
            actual.release.countDown();
            actual.thread.join(3000);
            assertFalse(actual.thread.isAlive(), "Actual termination deadline");
            assertThrows(IllegalStateException.class, () -> new ManagedFileIoOwnerAccess.StartObservation(actual.thread));
        }
    }
}
