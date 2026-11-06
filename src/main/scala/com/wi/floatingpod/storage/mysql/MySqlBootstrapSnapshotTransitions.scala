package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}

import java.util.UUID

/** B1 两阶段 bootstrap 在完整九表图上的确定状态转换；调用者仍须持有真实 opaque world proof。 */
private[mysql] object MySqlBootstrapSnapshotTransitions:
  def prepareFresh(
      before: MySqlFullRepositorySnapshot,
      plan: MySqlBootstrapPlanIdentity,
      sourceWorldIdentity: String,
      receiptSha256: Vector[Byte]
  ): MySqlFullRepositorySnapshot =
    require(before != null && plan != null, "fresh bootstrap snapshot and exact plan are required")
    require(sourceWorldIdentity != null && sourceWorldIdentity.nonEmpty, "opaque source world identity is required")
    require(receiptSha256 != null && receiptSha256.size == 32, "durable provision receipt hash must contain 32 bytes")
    before.validate()
    if !before.exactlyEquals(MySqlFullRepositorySnapshot.empty) then
      reject("fresh bootstrap requires an empty domain and physical recovery graph")
    val binding = PhysicalRecoveryBinding(plan.bindingUuid, PhysicalRecoveryCutoverState.LegacyUncertified, 2)
    val intent = MySqlBootstrapIntentData(
      1,
      plan.bootstrapOperationUuid,
      plan.prepareWriteUuid,
      plan.certifyWriteUuid,
      plan.bindingUuid,
      2,
      plan.provisioningUuid,
      sourceWorldIdentity,
      receiptSha256,
      MySqlBootstrapIntentStage.Prepared,
      None
    )
    val prepared = before.copy(
      recovery = before.recovery.copy(binding = Some(binding)),
      bootstrapIntents = Vector(intent)
    )
    prepared.validate()
    prepared

  /** 重启续做只接受与持久 PREPARED 完全一致的 proof；绝不重写其 operation 或 source 身份。 */
  def requirePreparedMatches(
      prepared: MySqlFullRepositorySnapshot,
      plan: MySqlBootstrapPlanIdentity,
      sourceWorldIdentity: String,
      receiptSha256: Vector[Byte]
  ): MySqlBootstrapIntentData =
    require(prepared != null && plan != null, "prepared bootstrap snapshot and exact plan are required")
    prepared.validate()
    val binding = prepared.recovery.binding.getOrElse(reject("prepared bootstrap binding is absent"))
    val intent = prepared.bootstrapIntents match
      case Vector(single) => single
      case _ => reject("prepared bootstrap requires exactly its singleton B1 intent")
    if binding.bindingUuid != plan.bindingUuid || binding.cutoverState != PhysicalRecoveryCutoverState.LegacyUncertified ||
      binding.formatVersion != 2 || intent.singletonFixed1 != 1 ||
      intent.bootstrapOperationUuid != plan.bootstrapOperationUuid || intent.prepareWriteUuid != plan.prepareWriteUuid ||
      intent.certifyWriteUuid != plan.certifyWriteUuid || intent.bindingUuid != plan.bindingUuid ||
      intent.bindingFormatVersion != 2 || intent.provisioningUuid != plan.provisioningUuid ||
      intent.sourceWorldIdentity != sourceWorldIdentity || intent.provisionReceiptSha256 != receiptSha256 ||
      intent.stage != MySqlBootstrapIntentStage.Prepared || intent.markerAckSha256.nonEmpty then
      reject("persisted B1 PREPARED history differs from the exact fresh provision proof")
    intent

  /** Marker ACK 只推进同一 exact PREPARED 行和其实际 LEGACY binding。 */
  def complete(
      prepared: MySqlFullRepositorySnapshot,
      plan: MySqlBootstrapPlanIdentity,
      sourceWorldIdentity: String,
      receiptSha256: Vector[Byte],
      markerAckSha256: Vector[Byte]
  ): MySqlFullRepositorySnapshot =
    requirePreparedMatches(prepared, plan, sourceWorldIdentity, receiptSha256)
    require(markerAckSha256 != null && markerAckSha256.size == 32, "durable marker acknowledgement hash must contain 32 bytes")
    val intent = prepared.bootstrapIntents.head
    val completed = prepared.copy(
      recovery = prepared.recovery.copy(
        binding = Some(intentBinding(intent).copy(cutoverState = PhysicalRecoveryCutoverState.Certified))
      ),
      bootstrapIntents = Vector(intent.copy(
        stage = MySqlBootstrapIntentStage.Complete,
        markerAckSha256 = Some(markerAckSha256)
      ))
    )
    completed.validate()
    completed

  def requireCompleteMatches(
      completed: MySqlFullRepositorySnapshot,
      plan: MySqlBootstrapPlanIdentity,
      sourceWorldIdentity: String,
      receiptSha256: Vector[Byte],
      markerAckSha256: Vector[Byte]
  ): MySqlBootstrapIntentData =
    require(completed != null && plan != null, "completed bootstrap snapshot and exact plan are required")
    completed.validate()
    val binding = completed.recovery.binding.getOrElse(reject("completed bootstrap binding is absent"))
    val intent = completed.bootstrapIntents match
      case Vector(single) => single
      case _ => reject("completed bootstrap requires exactly its singleton B1 intent")
    if binding.bindingUuid != plan.bindingUuid || binding.cutoverState != PhysicalRecoveryCutoverState.Certified ||
      binding.formatVersion != 2 || intent.singletonFixed1 != 1 ||
      intent.bootstrapOperationUuid != plan.bootstrapOperationUuid || intent.prepareWriteUuid != plan.prepareWriteUuid ||
      intent.certifyWriteUuid != plan.certifyWriteUuid || intent.bindingUuid != plan.bindingUuid ||
      intent.bindingFormatVersion != 2 || intent.provisioningUuid != plan.provisioningUuid ||
      intent.sourceWorldIdentity != sourceWorldIdentity || intent.provisionReceiptSha256 != receiptSha256 ||
      intent.stage != MySqlBootstrapIntentStage.Complete || !intent.markerAckSha256.contains(markerAckSha256) then
      reject("persisted B1 COMPLETE history differs from the exact provision proof and durable marker acknowledgement")
    intent

  private def intentBinding(intent: MySqlBootstrapIntentData): PhysicalRecoveryBinding =
    PhysicalRecoveryBinding(intent.bindingUuid, PhysicalRecoveryCutoverState.LegacyUncertified, intent.bindingFormatVersion)

  private def reject(message: String): Nothing =
    throw new java.sql.SQLTransactionRollbackException("MySQL B1 bootstrap state transition rejected: " + message)

/** 可公开比较的数据库计划字段，不包含来源认证能力；认证只由 opaque proof 转换器提供。 */
private[mysql] final case class MySqlBootstrapPlanIdentity(
    creationOperationUuid: UUID,
    bootstrapOperationUuid: UUID,
    prepareWriteUuid: UUID,
    certifyWriteUuid: UUID,
    bindingUuid: UUID,
    mysqlServerUuid: UUID,
    schema: String,
    schemaFormat: Int,
    physicalFormat: Int,
    ddlSha256: String,
    provisioningUuid: UUID
):
  require(creationOperationUuid != null && bootstrapOperationUuid != null && prepareWriteUuid != null &&
    certifyWriteUuid != null && bindingUuid != null && mysqlServerUuid != null && provisioningUuid != null,
    "B1 plan UUIDs are required")
  require(prepareWriteUuid != certifyWriteUuid, "B1 prepare/certify write UUIDs must differ")
  require(bindingUuid.version() == 4, "B1 format 2 binding must be the explicitly supplied UUIDv4")
  require(schema != null && MySqlSchemaAuditor.SchemaIdentifier.matches(schema), "B1 schema must be a fixed safe identifier")
  require(schemaFormat == 3 && physicalFormat == 2, "fresh B1 requires schema format 3 and physical format 2")
  require(ddlSha256 != null && MySqlSchemaAuditor.Sha256Hex.matches(ddlSha256), "B1 DDL digest must be SHA-256")
  val canonicalDdlSha256: String = ddlSha256.toUpperCase(java.util.Locale.ROOT)
