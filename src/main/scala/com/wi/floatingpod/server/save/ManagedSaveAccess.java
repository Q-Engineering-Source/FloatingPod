package com.wi.floatingpod.server.save;

/** 每 actual loader 一次安装；普通 live admission 与 forensic observer 互斥。 */
public interface ManagedSaveAccess {
    void floatingpod$installOrdinaryAdmission(ManagedOrdinarySaveAdmission owner);
    void floatingpod$installSaveLedger(ManagedSourceSaveLedger ledger);
    /** 实际queue/IO停止后解除同一consumer；false仅表示原工作仍在途，不退款。 */
    boolean floatingpod$tryDetachSaveLedger(ManagedSourceSaveLedger ledger);
    boolean floatingpod$hasOrdinaryAdmission();
    void floatingpod$initializeOrdinaryScheduler(Object scheduler, ManagedOrdinarySaveAdmission admission, boolean member);
    void floatingpod$ordinarySchedulerAdded(Object scheduler);
    boolean floatingpod$ordinarySchedulerRemoved(Object scheduler);
    void floatingpod$ordinarySchedulerFailed(Object scheduler, Throwable cause);
    void floatingpod$reportOrdinarySchedulerFailure();
    /** 仅原scheduler List锁内观察实际空queue和原membership，不授游戏保存资格。 */
    boolean floatingpod$finalSourceQueueEmpty(Object scheduler,ManagedOrdinarySaveAdmission admission);
    ManagedSourceSaveLedger.GenerationCapture floatingpod$captureSourceGeneration();
    void floatingpod$requireSourceGeneration(ManagedSourceSaveLedger.GenerationCapture original);
    /** 只给实际 queued get 的短观察锁；不授保存 IO/write owner。 */
    default ManagedSaveQueueObservation.Guard floatingpod$beginSourceQueuedRead(){return null;}
    default ManagedSourceSaveLedger.QueuedReadObservation floatingpod$observeSourceQueuedGet(int x,int z,Object actualCompound){return null;}

}
