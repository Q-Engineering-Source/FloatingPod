package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.DbFirstWorldAdoptionPlanData
import com.wi.floatingpod.storage.PhysicalRecoveryCutoverState

import java.util.UUID

/** Schema6 Full27 的独立 typed facade；只接收 schema6 recipe 与显式 quota 预期。 */
private[mysql] object MySqlDbFirstBootstrapGraph:
  private[mysql] type PrepareBeforeWriter = MySqlB1BootstrapGraph.PrepareBeforeWriter
  private val Kernel = MySqlB1BootstrapGraph.dbFirstFull27

  private[mysql] def prepareBeforeEpoch(writer: PrepareBeforeWriter): Long =
    MySqlB1BootstrapGraph.prepareBeforeEpoch(writer)

  def prepareTarget(
      before: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: DbFirstWorldAdoptionPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      beforeWriter: PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget,
      expectedQuotaMaximumRecords: Long
  ): MySqlScriptSchemaSnapshot =
    requirePositiveQuotaMaximum(expectedQuotaMaximumRecords)
    Kernel.prepareTarget(before, inputs, originalRoot, plan, sourceWorldIdentity, provisionReceiptSha256,
      beforeWriter, targetWriter, Some(expectedQuotaMaximumRecords))

  def certifyTarget(
      before: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: DbFirstWorldAdoptionPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      markerAckSha256: Vector[Byte],
      beforeWriter: PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget,
      expectedQuotaMaximumRecords: Long
  ): MySqlScriptSchemaSnapshot =
    requirePositiveQuotaMaximum(expectedQuotaMaximumRecords)
    Kernel.certifyTarget(before, inputs, originalRoot, plan, sourceWorldIdentity, provisionReceiptSha256,
      markerAckSha256, beforeWriter, targetWriter, Some(expectedQuotaMaximumRecords))

  def requirePrepareTransition(
      before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: DbFirstWorldAdoptionPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      beforeWriter: PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget,
      expectedQuotaMaximumRecords: Long
  ): Unit =
    requirePositiveQuotaMaximum(expectedQuotaMaximumRecords)
    Kernel.requirePrepareTransition(before, target, inputs, originalRoot, plan, sourceWorldIdentity,
      provisionReceiptSha256, beforeWriter, targetWriter, Some(expectedQuotaMaximumRecords))

  def requireCertifyTransition(
      before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: DbFirstWorldAdoptionPlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      markerAckSha256: Vector[Byte],
      beforeWriter: PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget,
      expectedQuotaMaximumRecords: Long
  ): Unit =
    requirePositiveQuotaMaximum(expectedQuotaMaximumRecords)
    Kernel.requireCertifyTransition(before, target, inputs, originalRoot, plan, sourceWorldIdentity,
      provisionReceiptSha256, markerAckSha256, beforeWriter, targetWriter, Some(expectedQuotaMaximumRecords))

  /** 上下文推导前重新核对模式、manifest、配额和允许存在的 Full27 行。 */
  def contextForTransition(stage: String, before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      plan: DbFirstWorldAdoptionPlanData, source: String, receipt: Vector[Byte],
      writer: MySqlFreshFormat3WriterTarget, expectedQuotaMaximumRecords: Long): MySqlOperationContext =
    requirePositiveQuotaMaximum(expectedQuotaMaximumRecords)
    Kernel.contextForTransition(stage, before, target, plan, source, receipt, writer,
      Some(expectedQuotaMaximumRecords))

  private[mysql] def requireWriterMatches(snapshot: MySqlScriptSchemaSnapshot,
      expected: MySqlBusinessWriterRow): Unit = Kernel.requireWriterMatches(snapshot, expected)

  private[mysql] def requireBinding(snapshot: MySqlScriptSchemaSnapshot,
      state: PhysicalRecoveryCutoverState, bindingUuid: UUID): Unit =
    Kernel.requireBinding(snapshot, state, bindingUuid)

  private def requirePositiveQuotaMaximum(value: Long): Unit =
    if value <= 0L then throw new java.sql.SQLDataException("DB-first Full27 requires an explicit positive quota maximum")
