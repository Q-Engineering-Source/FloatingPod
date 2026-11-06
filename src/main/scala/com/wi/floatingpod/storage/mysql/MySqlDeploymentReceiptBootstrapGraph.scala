package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.WindowsWorldProvisionReceiptDataCodec
import com.wi.floatingpod.storage.PhysicalRecoveryCutoverState

import java.util.UUID

/** 冻结的 schema5 Full26 facade，不接受 schema6 数据。 */
private[mysql] object MySqlDeploymentReceiptBootstrapGraph:
  private[mysql] type PrepareBeforeWriter = MySqlB1BootstrapGraph.PrepareBeforeWriter
  private val Kernel = MySqlB1BootstrapGraph.deploymentReceiptFull26

  private[mysql] def prepareBeforeEpoch(writer: PrepareBeforeWriter): Long =
    MySqlB1BootstrapGraph.prepareBeforeEpoch(writer)

  def prepareTarget(
      before: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: WindowsWorldProvisionReceiptDataCodec.PlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      beforeWriter: PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget
  ): MySqlScriptSchemaSnapshot =
    Kernel.prepareTarget(before, inputs, originalRoot, plan, sourceWorldIdentity, provisionReceiptSha256,
      beforeWriter, targetWriter, None)

  def certifyTarget(
      before: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: WindowsWorldProvisionReceiptDataCodec.PlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      markerAckSha256: Vector[Byte],
      beforeWriter: PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget
  ): MySqlScriptSchemaSnapshot =
    Kernel.certifyTarget(before, inputs, originalRoot, plan, sourceWorldIdentity, provisionReceiptSha256,
      markerAckSha256, beforeWriter, targetWriter, None)

  def requirePrepareTransition(
      before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: WindowsWorldProvisionReceiptDataCodec.PlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      beforeWriter: PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget
  ): Unit =
    Kernel.requirePrepareTransition(before, target, inputs, originalRoot, plan, sourceWorldIdentity,
      provisionReceiptSha256, beforeWriter, targetWriter, None)

  def requireCertifyTransition(
      before: MySqlScriptSchemaSnapshot,
      target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget,
      plan: WindowsWorldProvisionReceiptDataCodec.PlanData,
      sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte],
      markerAckSha256: Vector[Byte],
      beforeWriter: PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget
  ): Unit =
    Kernel.requireCertifyTransition(before, target, inputs, originalRoot, plan, sourceWorldIdentity,
      provisionReceiptSha256, markerAckSha256, beforeWriter, targetWriter, None)

  def contextForTransition(stage: String, before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      plan: WindowsWorldProvisionReceiptDataCodec.PlanData, source: String, receipt: Vector[Byte],
      writer: MySqlFreshFormat3WriterTarget): MySqlOperationContext =
    Kernel.contextForTransition(stage, before, target, plan, source, receipt, writer, None)

  private[mysql] def requireWriterMatches(snapshot: MySqlScriptSchemaSnapshot,
      expected: MySqlBusinessWriterRow): Unit = Kernel.requireWriterMatches(snapshot, expected)

  private[mysql] def requireBinding(snapshot: MySqlScriptSchemaSnapshot,
      state: PhysicalRecoveryCutoverState, bindingUuid: UUID): Unit =
    Kernel.requireBinding(snapshot, state, bindingUuid)
