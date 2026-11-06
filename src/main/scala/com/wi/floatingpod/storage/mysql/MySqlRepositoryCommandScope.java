package com.wi.floatingpod.storage.mysql;

import com.wi.floatingpod.server.io.IoOperationTag;
import java.util.Objects;

/** Java 私有构造器保证 command scope 在 JVM 边界保持不透明。 */
public final class MySqlRepositoryCommandScope {
    private final MySqlOwnedRepositoryBorrow issuer;
    private final IoOperationTag tag;
    private final long borrowGeneration;
    private final MySqlAuditBootScope auditScope;
    private final HeldSessionBarrier heldSessionBarrier;
    private final MySqlBusinessAdmission businessAdmission;
    private final MySqlOwnedRepositoryOperationPlan operationPlan;
    private final Thread ownerThread;

    private MySqlRepositoryCommandScope(MySqlOwnedRepositoryBorrow issuer, IoOperationTag tag,
            long borrowGeneration, MySqlAuditBootScope auditScope, HeldSessionBarrier heldSessionBarrier,
            MySqlBusinessAdmission businessAdmission,
            MySqlOwnedRepositoryOperationPlan operationPlan, Thread ownerThread) {
        this.issuer = Objects.requireNonNull(issuer, "actual active borrow issuer");
        this.tag = Objects.requireNonNull(tag, "exact Main operation tag");
        if (borrowGeneration <= 0) throw new IllegalArgumentException("positive borrow generation required");
        this.borrowGeneration = borrowGeneration;
        this.auditScope = Objects.requireNonNull(auditScope, "exact audit session scope");
        this.heldSessionBarrier = heldSessionBarrier;
        this.businessAdmission = businessAdmission;
        if ((heldSessionBarrier == null) != (businessAdmission == null)) {
            throw new IllegalArgumentException("held session and business admission must be retained together");
        }
        this.operationPlan = operationPlan;
        this.ownerThread = Objects.requireNonNull(ownerThread, "actual owner thread");
    }

    static MySqlRepositoryCommandScope issue(MySqlOwnedRepositoryBorrow issuer, IoOperationTag tag,
            long borrowGeneration, MySqlAuditBootScope auditScope, HeldSessionBarrier heldSessionBarrier,
            MySqlBusinessAdmission businessAdmission,
            MySqlOwnedRepositoryOperationPlan operationPlan, Thread ownerThread) {
        return new MySqlRepositoryCommandScope(issuer, tag, borrowGeneration, auditScope,
                heldSessionBarrier, businessAdmission, operationPlan, ownerThread);
    }

    public IoOperationTag tag() { return tag; }
    public long borrowGeneration() { return borrowGeneration; }
    public boolean hasOperationPlan() { return operationPlan != null; }
    public MySqlOwnedRepositoryOperationPlan operationPlan() {
        if (operationPlan == null) throw new IllegalStateException("Main-retained repository operation plan is required");
        return operationPlan;
    }

    public void requireCurrent(MySqlAuditBootScope expectedAuditScope) {
        if (expectedAuditScope == null || expectedAuditScope != auditScope || Thread.currentThread() != ownerThread) {
            throw new IllegalStateException("repository command scope differs from its exact active IO/session owner");
        }
        issuer.requireIssuedCommandScope(this);
        if (!tag.bootId().equals(auditScope.bootUuid()) || tag.bootGeneration() != auditScope.bootGeneration()
                || tag.connectionGeneration() != auditScope.connectionGeneration()) {
            throw new IllegalStateException("repository command tag no longer matches its captured database generation");
        }
    }

    public void requireCurrent(MySqlAuditBootScope expectedAuditScope,
            MySqlOwnedRepositoryOperationPlan expectedPlan) {
        requireCurrent(expectedAuditScope);
        if (expectedPlan == null || operationPlan != expectedPlan) {
            throw new IllegalStateException("repository command plan differs from its exact cached Main command");
        }
        expectedPlan.requireTag(tag);
    }

    HeldSessionBarrier requireHeldSessionBarrier(MySqlAuditBootScope expectedAuditScope,
            MySqlOwnedRepositoryOperationPlan expectedPlan) {
        requireCurrent(expectedAuditScope, expectedPlan);
        if (heldSessionBarrier == null) {
            throw new IllegalStateException("actual held MySQL session-barrier receipt is unavailable");
        }
        return heldSessionBarrier;
    }

    MySqlBusinessAdmission requireBusinessAdmission(MySqlAuditBootScope expectedAuditScope,
            MySqlOwnedRepositoryOperationPlan expectedPlan) {
        requireCurrent(expectedAuditScope, expectedPlan);
        if (businessAdmission == null) {
            throw new IllegalStateException("actual current MySQL business admission is unavailable");
        }
        return businessAdmission;
    }

    MySqlAuditBootScope exactAuditScope() { return auditScope; }
}
