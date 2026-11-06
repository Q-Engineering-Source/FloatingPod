package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID

/** Admission match rule only; constructed immutable rows are not an actual SQL/admission proof. */
class MySqlOwnedSnapshotAdmissionRulesTest:
  private val boot = UUID.randomUUID()
  private val binding = UUID.randomUUID()
  private val now = LocalDateTime.of(2026, 10, 7, 0, 0)
  private val lease = now.plusSeconds(30)
  private val admission = MySqlOwnedDatabaseAdmission(boot, 2L, 3L, binding, 4L)
  private def snapshot(): MySqlRecoverySnapshot =
    val hash = Vector.fill(32)(0.toByte)
    val manifest = MySqlSnapshotManifest(1, "MYSQL", 2, hash, UUID.randomUUID(), "READY", UUID.randomUUID(),
      binding, None, "NONE", None, None, None, Some(hash))
    val writer = MySqlSnapshotWriter(1, binding, 4L, Some(boot), lease, None, None)
    MySqlRecoverySnapshot(boot, 2L, 3L, manifest, writer, Vector.empty, Vector.empty,
      MySqlRecoveryGraph(Some(PhysicalRecoveryBinding(binding, PhysicalRecoveryCutoverState.Certified, 2)),
        Vector.empty, Vector.empty, Vector.empty), Vector.empty, now, now.plusSeconds(1), "0".repeat(64),
      MySqlRecoveryObservationState.CertifiedSchemaObserved, MySqlObservedWriterLease.OwnedUnexpired)
  private def validate(value: MySqlRecoverySnapshot): Unit =
    MySqlOwnedSnapshotAdmissionRules.validate(value, admission, lease, 2)

  @Test def exactOwnedSnapshotTupleAndDbLeasePassPureRule(): Unit = validate(snapshot())

  @Test def foreignBootGenerationBindingAndEpochAreRejected(): Unit =
    val value = snapshot()
    assertThrows(classOf[IllegalArgumentException], () => validate(value.copy(bootUuid = UUID.randomUUID())))
    assertThrows(classOf[IllegalArgumentException], () => validate(value.copy(connectionGeneration = 4L)))
    assertThrows(classOf[IllegalArgumentException], () => validate(value.copy(writer = value.writer.copy(bindingUuid = UUID.randomUUID()))))
    assertThrows(classOf[IllegalArgumentException], () => validate(value.copy(writer = value.writer.copy(epoch = 5L))))

  @Test def bindingAbsentLegacyAndForeignOwnerNeverBecomeNormalAdmission(): Unit =
    val value = snapshot()
    assertThrows(classOf[IllegalArgumentException], () =>
      validate(value.copy(state = MySqlRecoveryObservationState.BindingAbsent, recovery = value.recovery.copy(binding = None))))
    assertThrows(classOf[IllegalArgumentException], () =>
      validate(value.copy(recovery = value.recovery.copy(binding = Some(PhysicalRecoveryBinding(binding, PhysicalRecoveryCutoverState.LegacyUncertified, 2))))))
    assertThrows(classOf[IllegalArgumentException], () =>
      validate(value.copy(writer = value.writer.copy(ownerBootUuid = Some(UUID.randomUUID())))))

  @Test def leaseMismatchExpiryAndBackwardsDatabaseTimeAreRejected(): Unit =
    val value = snapshot()
    assertThrows(classOf[IllegalArgumentException], () => validate(value.copy(writer = value.writer.copy(leaseUntil = lease.plusSeconds(1)))))
    assertThrows(classOf[IllegalArgumentException], () => validate(value.copy(databaseTimeAfter = lease)))
    assertThrows(classOf[IllegalArgumentException], () => validate(value.copy(databaseTimeAfter = now.minusSeconds(1))))
    assertThrows(classOf[IllegalArgumentException], () => validate(value.copy(writerLease = MySqlObservedWriterLease.OwnedExpired)))

