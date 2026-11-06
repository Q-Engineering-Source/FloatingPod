package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{WindowsB1TwoSlotStore, WindowsB1V5WitnessRecordStore}

/** Ordinary restart permissions are actor-issued, identity-bound workflow capabilities. */
private[mysql] object MySqlCertifiedOrdinaryCapabilities:
  sealed trait OrdinaryNativeHandoffAcknowledgement
  final case class V4NativeHandoffAcknowledgement(
      private[mysql] val value: WindowsB1TwoSlotStore.DurableHandoffNativeAck
  ) extends OrdinaryNativeHandoffAcknowledgement:
    require(value != null, "actual V4 native HANDOFF ACK required")
  final case class V5NativeHandoffAcknowledgement(
      private[mysql] val value: WindowsB1V5WitnessRecordStore.NativeV5RecordDurability
  ) extends OrdinaryNativeHandoffAcknowledgement:
    require(value != null, "actual V5 native HANDOFF ACK required")

  sealed trait OrdinaryNativeSettledAcknowledgement
  final case class V4SettledNativeAcknowledgement(
      private[mysql] val value: WindowsB1TwoSlotStore.NativeRecordDurability
  ) extends OrdinaryNativeSettledAcknowledgement:
    require(value != null, "actual V4 settled DB_ACQUIRED ACK required")
  final case class V5SettledNativeAcknowledgement(
      private[mysql] val value: WindowsB1V5WitnessRecordStore.NativeV5RecordDurability
  ) extends OrdinaryNativeSettledAcknowledgement:
    require(value != null, "actual V5 settled DB_ACQUIRED ACK required")

  type VerifiedOriginalCertifiedSource = MySqlCertifiedRecoveryInspectionActor.VerifiedOriginalCertifiedSource
  type PlannedCertifiedHandoff = MySqlCertifiedRecoveryInspectionActor.PlannedCertifiedHandoff
  type CommittedCertifiedHandoff = MySqlCertifiedRecoveryInspectionActor.CommittedCertifiedHandoff
  type CompletedCertifiedHandoff = MySqlCertifiedRecoveryInspectionActor.CompletedCertifiedHandoff
  type RecallRecoveryAdmission = MySqlCertifiedRecoveryInspectionActor.RecallRecoveryAdmission
private[mysql] type MySqlOrdinaryPreAnvilProjection = MySqlCertifiedRecoveryInspectionActor.MySqlOrdinaryPreAnvilProjection
