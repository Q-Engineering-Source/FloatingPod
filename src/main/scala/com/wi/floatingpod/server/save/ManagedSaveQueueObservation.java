package com.wi.floatingpod.server.save;

import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/** 每loader短内存queue锁：queueLock -> ledgerLock。不能跨encoder/Region/cache/native/IO/logsink。 */
public final class ManagedSaveQueueObservation {
    private final ManagedSourceSaveLedger ledger;
    private final Thread main,io;
    private final ReentrantLock lock=new ReentrantLock();
    public ManagedSaveQueueObservation(ManagedSourceSaveLedger ledger){this.ledger=Objects.requireNonNull(ledger);main=null;io=null;}
    /** 普通保存只保留 live token，不强制安装永久 forensic history。 */
    public ManagedSaveQueueObservation(Thread exactMain,Thread exactIo){
        ledger=null;main=Objects.requireNonNull(exactMain);io=Objects.requireNonNull(exactIo);
        if(main==io)throw new IllegalArgumentException("Distinct Main and File IO required");
        if(Thread.currentThread()!=main)throw new IllegalStateException("Exact Main installs queue guard");
    }
    public Guard enter() {
        if(ledger!=null)ledger.assertQueueActor();
        else if(Thread.currentThread()!=main&&Thread.currentThread()!=io)throw new IllegalStateException("Exact Main or File IO queue actor required");
        lock.lock();return new Guard();
    }
    /** Forge async reader仅观察 actual get→generation；不取得 Main/保存 IO actor资格。
     * Caller must hold no ledger/native/IO lock, and release before copy/encoder/factory. */
    public Guard enterSourceReadObservation(){lock.lock();return new Guard();}
    public void assertHeldForSourceGet(){if(!lock.isHeldByCurrentThread())throw new IllegalStateException("Actual short queue get/record lock required");}
    /** 直接回归观察actual ReentrantLock waiter，包内只读，不授业务权利。 */
    boolean observesQueuedThread(Thread thread){return lock.hasQueuedThread(thread);}
    public final class Guard implements AutoCloseable {
        private final Thread owner=Thread.currentThread();private boolean closed;
        private Guard(){ }
        @Override public void close(){if(closed || Thread.currentThread()!=owner)throw new IllegalStateException("Exact once queue guard owner required");closed=true;lock.unlock();}
    }
}
