package com.wi.floatingpod.server.save;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;

/** 仅实际RegionFile自身装配边界使用，不是Main/native proof API。 */
public interface ManagedRegionAccess {
    void floatingpod$bindSaveLedger(ManagedSourceSaveLedger ledger);
    FileDescriptor floatingpod$originalDescriptor() throws IOException;
    File floatingpod$originalFileName();
    void floatingpod$cacheSwallowed(Throwable originalCloseCause,Throwable actualCaughtCause)throws Throwable;
}
