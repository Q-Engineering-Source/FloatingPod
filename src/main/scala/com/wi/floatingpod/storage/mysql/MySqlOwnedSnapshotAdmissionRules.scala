package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import java.time.LocalDateTime

/** 对真实reader输出再核同 admitted scope；纯数据规则，不生成admission。 */
private[mysql] object MySqlOwnedSnapshotAdmissionRules:
  def validate(snapshot: MySqlRecoverySnapshot, admission: MySqlOwnedDatabaseAdmission,
      exactLeaseUntil: LocalDateTime, physicalFormat: Int): Unit =
    require(snapshot != null && admission != null && exactLeaseUntil != null, "snapshot/admission/lease required")
    require(snapshot.bootUuid == admission.bootId && snapshot.bootGeneration == admission.bootGeneration &&
      snapshot.connectionGeneration == admission.connectionGeneration, "snapshot owned generation differs")
    require(snapshot.manifest.bindingUuid == admission.bindingId && snapshot.writer.bindingUuid == admission.bindingId &&
      snapshot.writer.epoch == admission.writerEpoch && snapshot.writer.ownerBootUuid == Some(admission.bootId),
      "snapshot writer exact owner/binding/epoch differs")
    require(snapshot.state == MySqlRecoveryObservationState.CertifiedSchemaObserved &&
      snapshot.recovery.binding.contains(PhysicalRecoveryBinding(admission.bindingId, PhysicalRecoveryCutoverState.Certified, physicalFormat)),
      "snapshot is not exact CERTIFIED binding")
    require(snapshot.writer.leaseUntil == exactLeaseUntil && snapshot.writerLease == MySqlObservedWriterLease.OwnedUnexpired &&
      !snapshot.databaseTimeAfter.isBefore(snapshot.databaseTimeBefore) &&
      exactLeaseUntil.isAfter(snapshot.databaseTimeBefore) && exactLeaseUntil.isAfter(snapshot.databaseTimeAfter),
      "snapshot DB-time lease is expired or differs from admission")

