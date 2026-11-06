package com.wi.floatingpod.server.save;

import java.util.Objects;

/** 仅实际ThreadedFileIOBase上的Mixin field签发owner观察；不是quiescence/native证明。 */
public interface ManagedFileIoOwnerAccess {
    Thread floatingpod$actualStartedIoThread();
    boolean floatingpod$tryFenceFinalSource(ManagedOrdinarySaveAdmission admission,
        ManagedOrdinarySaveAdmission.SourceEscrow escrow,Object sourceOwner,Iterable<?> actualLoaders);
    void floatingpod$endFinalSource(ManagedOrdinarySaveAdmission admission,Iterable<?> actualLoaders);
    boolean floatingpod$trySealOrdinarySaves(ManagedOrdinarySaveAdmission admission,Iterable<?> actualLoaders);
    void floatingpod$requireFinalEndLock();
    /** 在真实 scheduler List 短锁内初始化，不能从缺少通知推断 membership。 */
    void floatingpod$installOrdinarySchedulerMembership(ManagedSaveAccess loader, ManagedOrdinarySaveAdmission admission);
    final class StartObservation {
        private enum State { SELECTED, RETURNED, FAILED }
        private final Thread constructorOwner=Thread.currentThread();
        private final Thread exactStartReceiver;
        private volatile State state=State.SELECTED;
        private volatile Throwable failure;
        public StartObservation(Thread actualStartReceiver) {
            exactStartReceiver=Objects.requireNonNull(actualStartReceiver);
            if(actualStartReceiver==constructorOwner || actualStartReceiver.getState()!=Thread.State.NEW)
                throw new IllegalStateException("Actual constructor Thread.start receiver must be NEW and different from constructor owner");
        }
        public void startReturned() { selectedOwner();state=State.RETURNED; }
        public void startFailed(Throwable actualCause) { selectedOwner();failure=Objects.requireNonNull(actualCause);state=State.FAILED; }
        public Thread actualStartedThread() {
            if(state!=State.RETURNED)throw new IllegalStateException("Actual original Thread.start has not returned successfully",failure);
            if(!exactStartReceiver.isAlive())throw new IllegalStateException("Actual started File IO Thread is not alive; no quiescence proof");
            return exactStartReceiver;
        }
        private void selectedOwner() {
            if(Thread.currentThread()!=constructorOwner || state!=State.SELECTED)
                throw new IllegalStateException("Exact constructor caller and one terminal start observation required",failure);
        }
    }
}
