package com.wi.floatingpod.server;

import com.wi.floatingpod.storage.mysql.MySqlDbFirstRecallRequestRecord;
import java.util.UUID;
import scala.collection.immutable.Vector;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 直接调用实际JVM公开入口；普通Java可构造metadata，但不能因此取得Boot完成资格。 */
public final class BootRefusalJavaCaller {
    private BootRefusalJavaCaller() {}

    public static void assertUnissuedRestoredTerminalRejected(DbFirstBootRecallRequestCustody custody,
            MySqlDbFirstRecallRequestRecord original) {
        assertThrows(IllegalArgumentException.class, () -> custody.completeRestoredTerminal(original, null));
    }

    public static void assertUnissuedCommittedRejected(DbFirstBootRecallRequestCustody custody,
            MySqlDbFirstRecallRequestRecord original) {
        assertThrows(IllegalArgumentException.class, () -> custody.completeCommitted(original, null));
    }

    public static void assertUnissuedSettlementRejected(DbFirstBootRecallRequestCustody custody,
            MySqlDbFirstRecallRequestRecord original, com.wi.floatingpod.domain.RecallRefusalReason reason) {
        assertThrows(IllegalArgumentException.class, () -> custody.completeRefusal(original,
                reason, null));
    }

    public static void assertUnissuedPassRejected(DbFirstBootRecallRequestCustody custody,
            Vector<MySqlDbFirstRecallRequestRecord> records, UUID binding) {
        var pass = new DbFirstBootActiveRecovery.TerminalRecoveryPass(
                custody, records, Thread.currentThread(), binding, UUID.randomUUID());
        pass.requireOriginal(custody, records, Thread.currentThread());
        assertThrows(IllegalArgumentException.class, () -> custody.completeResults(pass, null));
    }
}
