package com.wi.floatingpod.storage.mysql;

import com.wi.floatingpod.server.WindowsMarkerIo;
import com.wi.floatingpod.server.WindowsWorldProvisioner;
import java.util.Objects;

/** 原 session 的 native 收口权限；不是 Main 停服许可，也不代表 close 已成功。 */
public final class MySqlCertifiedFreshExitPermit {
    private final MySqlOwnedDatabaseSession session;
    private final WindowsWorldProvisioner.ProvisionedWorld world;
    private final WindowsMarkerIo io;
    private final Thread worker;
    private boolean claimed;

    private MySqlCertifiedFreshExitPermit(MySqlOwnedDatabaseSession session,
            WindowsWorldProvisioner.ProvisionedWorld world, WindowsMarkerIo io) {
        this.session = Objects.requireNonNull(session);
        this.world = Objects.requireNonNull(world);
        this.io = Objects.requireNonNull(io);
        worker = Thread.currentThread();
    }

    static MySqlCertifiedFreshExitPermit issue(MySqlOwnedDatabaseSession session,
            WindowsWorldProvisioner.ProvisionedWorld world, WindowsMarkerIo io) {
        return new MySqlCertifiedFreshExitPermit(session, world, io);
    }

    /** 再核原 session 的实际释放状态；同值对象或另一个 worker 不能消费。 */
    public void claimFor(WindowsWorldProvisioner.ProvisionedWorld actualWorld, WindowsMarkerIo actualIo) {
        if (Thread.currentThread() != worker || actualWorld != world || actualIo != io || claimed)
            throw new IllegalStateException("Original unconsumed Fresh native exit permit required");
        session.requireCertifiedFreshNativeExitPermit(this);
        claimed = true; // native 核验或 close 失败也不能重用。
    }
}
