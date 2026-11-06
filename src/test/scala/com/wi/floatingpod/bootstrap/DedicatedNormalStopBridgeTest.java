package com.wi.floatingpod.bootstrap;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

/** 只验证停服请求的实际线程/实例调度；不发行或模拟 Main closure、native ACK、停止许可。 */
class DedicatedNormalStopBridgeTest {
    @Test void freshInvocationEntryIsExactMainAndOneShot() throws Exception {
        Object server=new Object();
        var bridge=new DedicatedNormalStopBridge(server,Thread.currentThread(),UUID.randomUUID(),1L);
        bridge.request(server);var request=bridge.pollOnMain();
        bridge.beginFreshInvocation(request);
        var foreignResult=new AtomicReference<Boolean>();
        Thread foreign=new Thread(()->foreignResult.set(bridge.consumeOriginalEntry(server)),"foreign-original-stop");
        foreign.start();foreign.join(3000L);assertFalse(foreign.isAlive());
        assertEquals(Boolean.FALSE,foreignResult.get());
        assertFalse(bridge.consumeOriginalEntry(new Object()));
        assertTrue(bridge.consumeOriginalEntry(server));
        assertFalse(bridge.consumeOriginalEntry(server));
        bridge.finishFreshInvocation(request,true);
        bridge.requireFreshInvocation(request);
        assertThrows(IllegalStateException.class,()->bridge.beginFreshInvocation(request));
    }

    @Test void missingActualHookCannotBecomeCompletionOrRetry() {
        Object server=new Object();
        var bridge=new DedicatedNormalStopBridge(server,Thread.currentThread(),UUID.randomUUID(),1L);
        bridge.request(server);var request=bridge.pollOnMain();
        bridge.beginFreshInvocation(request);
        assertThrows(IllegalStateException.class,()->bridge.finishFreshInvocation(request,false));
        assertThrows(IllegalStateException.class,()->bridge.requireFreshInvocation(request));
        assertThrows(IllegalStateException.class,()->bridge.beginFreshInvocation(request));
        assertFalse(bridge.consumeOriginalEntry(server));
    }

    @Test void concurrentRequestsCoalesceBeforeMainConsumesExactlyOnce() throws Exception {
        Object server = new Object();
        var bridge = new DedicatedNormalStopBridge(server, Thread.currentThread(), UUID.randomUUID(), 1L);
        var failure = new AtomicReference<Throwable>();
        var callers = new ArrayList<Thread>();
        for (int i = 0; i < 12; i++) {
            Thread caller = new Thread(() -> {
                try { bridge.request(server); }
                catch (Throwable cause) { failure.compareAndSet(null, cause); }
            }, "stop-request-" + i);
            callers.add(caller);
            caller.start();
        }
        for (Thread caller : callers) {
            caller.join(3000L);
            assertFalse(caller.isAlive(), "request must not wait for Main or resource closure");
        }
        assertNull(failure.get());
        var request = bridge.pollOnMain();
        assertNotNull(request);
        assertNull(bridge.pollOnMain());
        bridge.request(server);
        assertNull(bridge.pollOnMain(), "repeat request must not start another responsibility census");
        bridge.requireOriginalOnMain(request);
    }

    @Test void foreignServerAndForeignMainCannotConsumeTheOriginalRequest() throws Exception {
        Object server = new Object();
        var bridge = new DedicatedNormalStopBridge(server, Thread.currentThread(), UUID.randomUUID(), 2L);
        assertThrows(IllegalArgumentException.class, () -> bridge.request(new Object()));
        assertNull(bridge.pollOnMain());
        bridge.request(server);
        var failure = new AtomicReference<Throwable>();
        Thread foreign = new Thread(() -> {
            try { bridge.pollOnMain(); }
            catch (Throwable cause) { failure.set(cause); }
        }, "foreign-stop-consumer");
        foreign.start();
        foreign.join(3000L);
        assertFalse(foreign.isAlive());
        assertInstanceOf(IllegalStateException.class, failure.get());
        assertNotNull(bridge.pollOnMain(), "foreign consumer must not consume the pending request");
    }

    @Test void equalBootDataCannotReplaceTheOriginalBridgeRequest() {
        Object server = new Object();
        UUID boot = UUID.randomUUID();
        var first = new DedicatedNormalStopBridge(server, Thread.currentThread(), boot, 3L);
        var second = new DedicatedNormalStopBridge(server, Thread.currentThread(), boot, 3L);
        first.request(server);
        second.request(server);
        var original = first.pollOnMain();
        var other = second.pollOnMain();
        first.requireOriginalOnMain(original);
        assertThrows(IllegalArgumentException.class, () -> first.requireOriginalOnMain(other));
        assertThrows(IllegalArgumentException.class, () -> first.requireOriginalOnMain(null));
    }
}
