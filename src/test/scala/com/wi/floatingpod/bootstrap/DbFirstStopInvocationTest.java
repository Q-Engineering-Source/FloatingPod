package com.wi.floatingpod.bootstrap;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** 直接执行生产调用序列；不授SQL/native关闭资格，不冒充FML/Mixin运行验收。 */
class DbFirstStopInvocationTest {
    @Test void onlyOriginalConsumedInvocationAndBothNormalReturnsCanEndGeneration(){
        Object server=new Object();var actual=new DbFirstStopInvocation(server,Thread.currentThread());
        assertThrows(IllegalStateException.class,()->actual.requireFinalGenerationEnd(server));
        actual.issue(server);actual.begin(server);
        assertTrue(actual.inFlight(server));
        assertThrows(IllegalStateException.class,()->actual.invocationReturned(server));
        actual.consumeOriginalEntry(server);
        assertFalse(actual.inFlight(server));
        assertThrows(IllegalStateException.class,()->actual.consumeOriginalEntry(server));
        actual.invocationReturned(server);
        assertTrue(actual.stopServerPending(server));
        assertThrows(IllegalStateException.class,()->actual.requireStoppedDispatchEntry(server));
        actual.requireStopServerEntry(server);actual.stopServerReturned(server);
        assertFalse(actual.stopServerPending(server));assertTrue(actual.stoppedDispatchPending(server));
        actual.requireStoppedDispatchEntry(server);
        assertThrows(IllegalStateException.class,()->actual.requireFinalGenerationEnd(server),"event entry is not full dispatch return");
        actual.stoppedDispatchReturned(server);actual.requireFinalGenerationEnd(server);actual.finalGenerationEnded(server);
        assertFalse(actual.stoppedDispatchPending(server));
        assertThrows(IllegalStateException.class,()->actual.finalGenerationEnded(server));
        assertThrows(IllegalStateException.class,()->actual.issue(server));
    }
    @Test void failureBetweenOriginalReturnsPreservesFirstCauseAndPreventsEnd(){
        Object server=new Object();var actual=new DbFirstStopInvocation(server,Thread.currentThread());
        actual.issue(server);actual.begin(server);actual.consumeOriginalEntry(server);actual.invocationReturned(server);
        var first=new IllegalStateException("original stopServer failure");var cleanup=new IllegalStateException("later dispatch cleanup failure");
        actual.retainFailure(server,first);actual.retainFailure(server,cleanup);actual.retainFailure(server,cleanup);
        assertFalse(actual.stopServerPending(server));assertFalse(actual.stoppedDispatchPending(server));
        assertSame(first,assertThrows(IllegalStateException.class,()->actual.stopServerReturned(server)).getCause());
        assertSame(first,assertThrows(IllegalStateException.class,()->actual.requireFinalGenerationEnd(server)).getCause());
        assertArrayEquals(new Throwable[]{cleanup},first.getSuppressed());
    }
    @Test void originalMainAndServerCannotBeReplacedAtAnyInvocationBoundary()throws Exception{
        Object server=new Object();var actual=new DbFirstStopInvocation(server,Thread.currentThread());
        assertThrows(IllegalStateException.class,()->actual.issue(new Object()));
        actual.issue(server);
        var failure=new AtomicReference<Throwable>();
        var foreign=Thread.ofPlatform().unstarted(()->{
            try{
                assertThrows(IllegalStateException.class,()->actual.begin(server));
                assertThrows(IllegalStateException.class,()->actual.inFlight(server));
                assertThrows(IllegalStateException.class,()->actual.retainFailure(server,new IllegalStateException("foreign")));
            }catch(Throwable cause){failure.set(cause);}
        });
        foreign.start();foreign.join(5000);assertFalse(foreign.isAlive());if(failure.get()!=null)throw new AssertionError(failure.get());
        actual.begin(server);actual.consumeOriginalEntry(server);actual.invocationReturned(server);assertTrue(actual.stopServerPending(server));
    }
    @Test void stoppedDispatchFailureCannotBeRetriedAsFinalGenerationSuccess(){
        Object server=new Object();var actual=new DbFirstStopInvocation(server,Thread.currentThread());
        actual.issue(server);actual.begin(server);actual.consumeOriginalEntry(server);actual.invocationReturned(server);actual.stopServerReturned(server);
        var cause=new IllegalStateException("original full Stopped dispatch failure");actual.retainFailure(server,cause);
        assertSame(cause,assertThrows(IllegalStateException.class,()->actual.stoppedDispatchReturned(server)).getCause());
        assertSame(cause,assertThrows(IllegalStateException.class,()->actual.finalGenerationEnded(server)).getCause());
    }
}
