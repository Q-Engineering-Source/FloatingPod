package com.wi.floatingpod.storage.mysql

import java.nio.file.Path
import java.time.Duration
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 前置拒绝只验证生产入口；不伪造 verified connection/audit/native 成功。 */
final class MySqlDeploymentReceiptSessionTest:
  @Test def rejectsMissingTypedInputsBeforeAnyResourceAllocation(): Unit =
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlDeploymentReceiptSession.allocateOrdinary(null, null, null, null, Duration.ofSeconds(1),null,null,null,null))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlDeploymentReceiptSession.allocateFresh(null, null, null, null, Duration.ofSeconds(1)))

  @Test def rejectsLegacyOrMissingFreshPlanInsteadOfCoercingIt(): Unit =
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlDeploymentReceiptSession.FreshRequest(Path.of("D:/fp-schema5-test/world"), null, null, null,null,null))

  @Test def missingQualificationCannotPassPreNativeGate(): Unit =
    assertThrows(classOf[IllegalStateException], () =>
      MySqlDeploymentReceiptSession.requireFreshQualification(null, null, null, null))
