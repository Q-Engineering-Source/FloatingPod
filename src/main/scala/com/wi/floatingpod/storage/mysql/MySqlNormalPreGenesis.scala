package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{B1ResumeRecord, WindowsB1TwoSlotStore, WindowsWorldProvisioner}

/** 已有 durable record 不能转作初始 seed；保存原 records/inspection，后续 settlement 另门。 */
final class MySqlNormalPreGenesisUnresolved(val records: WindowsB1TwoSlotStore.ResumeSelectedRecords,
    val inspection: WindowsWorldProvisioner.ResumeInspection)
  extends java.sql.SQLNonTransientConnectionException("Normal anchored source has durable history; controlled settlement remains unresolved"):
  require(records != null && inspection != null, "actual unresolved native observations required")

final case class MySqlNormalPreGenesisObservation(
    plan: Option[MySqlNormalPreGenesisCapabilities.PlannedNormalPreGenesis],
    draftRecord: Option[B1ResumeRecord], nativeAck: Option[WindowsB1TwoSlotStore.DurablePreGenesisNativeAck],
    installedWorld: Option[WindowsWorldProvisioner.ProvisionedWorld],
    prepare: MySqlFreshPrepareAttemptObservation, certify: MySqlFreshCertifyObservation):
  def retainedResponsibility: Boolean = plan.nonEmpty || draftRecord.nonEmpty || nativeAck.nonEmpty ||
    installedWorld.nonEmpty || prepare.retainedResponsibility || certify.plan.nonEmpty || certify.intent.nonEmpty ||
    certify.draftRecord.nonEmpty || certify.markerPublicationAttempted || certify.executionAttempted
  override def toString: String = "MySqlNormalPreGenesisObservation[redacted; not Ready/settlement/TERM]"
