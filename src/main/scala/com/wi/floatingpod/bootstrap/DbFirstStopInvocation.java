package com.wi.floatingpod.bootstrap;

import java.util.Objects;

/** 原runtime私有持有的正常停服调用序列；单独的实例/阶段不构成SQL关闭或生产许可。 */
public final class DbFirstStopInvocation {
    private enum Phase { NEW, ISSUED, IN_FLIGHT, ENTERED, RETURNED, STOP_RETURNED, DISPATCH_RETURNED, COMPLETED }
    private final Object server;
    private final Thread main;
    private Phase phase=Phase.NEW;
    private Throwable failure;
    public DbFirstStopInvocation(Object actualServer,Thread actualMain){
        server=Objects.requireNonNull(actualServer);main=Objects.requireNonNull(actualMain);owner(server);
    }
    private void owner(Object actual){
        if(actual!=server||Thread.currentThread()!=main)throw new IllegalStateException("Original schema6 stop server/Main required");
    }
    private void require(Object actual,Phase expected){
        owner(actual);
        if(failure!=null)throw new IllegalStateException("Original schema6 stop invocation failed",failure);
        if(phase!=expected)throw new IllegalStateException("Original schema6 stop stage required: "+expected+"; actual="+phase);
    }
    public void issue(Object actual){require(actual,Phase.NEW);phase=Phase.ISSUED;}
    public void begin(Object actual){require(actual,Phase.ISSUED);phase=Phase.IN_FLIGHT;}
    public boolean inFlight(Object actual){owner(actual);return failure==null&&phase==Phase.IN_FLIGHT;}
    public void consumeOriginalEntry(Object actual){require(actual,Phase.IN_FLIGHT);phase=Phase.ENTERED;}
    public void invocationReturned(Object actual){require(actual,Phase.ENTERED);phase=Phase.RETURNED;}
    public boolean stopServerPending(Object actual){owner(actual);return failure==null&&phase==Phase.RETURNED;}
    public void requireStopServerEntry(Object actual){require(actual,Phase.RETURNED);}
    public void stopServerReturned(Object actual){require(actual,Phase.RETURNED);phase=Phase.STOP_RETURNED;}
    public boolean stoppedDispatchPending(Object actual){owner(actual);return failure==null&&phase==Phase.STOP_RETURNED;}
    public void requireStoppedDispatchEntry(Object actual){require(actual,Phase.STOP_RETURNED);}
    public void stoppedDispatchReturned(Object actual){require(actual,Phase.STOP_RETURNED);phase=Phase.DISPATCH_RETURNED;}
    public void requireFinalGenerationEnd(Object actual){require(actual,Phase.DISPATCH_RETURNED);}
    public void finalGenerationEnded(Object actual){require(actual,Phase.DISPATCH_RETURNED);phase=Phase.COMPLETED;}
    public void retainFailure(Object actual,Throwable cause){
        owner(actual);Objects.requireNonNull(cause);
        if(failure==null)failure=cause;
        else if(failure!=cause)com.wi.floatingpod.server.save.ManagedSourceSaveLedger.suppressSecondary(failure,cause);
    }
}
