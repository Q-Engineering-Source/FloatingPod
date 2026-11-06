package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.*
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.LocalDateTime
import java.util.{HexFormat, UUID}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** B1 DATA 只承诺完整 typed graph 与独立证书，不签发 SQL 或运行资格。 */
private[mysql] object MySqlScriptSchemaDataTestFixture:
  val Schema: String = MySqlScriptSchemaProfile.Schema
  val Connection: MySqlFormat3ConnectionObservation = MySqlFormat3ConnectionObservation(
    UUID.fromString("00000000-0000-0000-0000-000000000124"), "9.7.2", Schema,
    "b1-data@test", "InnoDB", 1, 1L, "READ-COMMITTED", "REPEATABLE-READ",
    "STRICT_ALL_TABLES", "+00:00", "TLSv1.3", "TLS_AES_256_GCM_SHA384")
  val Scope: MySqlFormat3ObservationScope = MySqlFormat3ObservationScope(
    UUID.fromString("00000000-0000-0000-0000-000000000125"), 1L, 1L)
  val SourceProfile: SourceLimitProfile = SourceLimitProfile.of(
    1_000_000L, 1_000_000L, 2_000_000L, 64L, 2_000_000L,
    16_000_000L, 4_000_000L, 2_000_000L)
  val Inputs: MySqlScriptSchemaInputs = MySqlScriptSchemaInputs.create(SourceProfile,
    MySqlScriptSchemaProfile.Budget(100_000L, 100_000L, 100_000L, 100_000L,
      1_000_000L, 100_000L, 200L, 16_000_000L, 2_000_000L))
  val Full26DdlSha256: String = HexFormat.of().formatHex(Array.fill[Byte](32)(0x5a.toByte))
  val Full26Inputs: MySqlScriptSchemaInputs = MySqlScriptSchemaInputs.createDeploymentReceiptFull26(
    Schema, Full26DdlSha256, SourceProfile, Inputs.budget,
    MySqlDeploymentReceiptSchemaDefinition.Budget(evidenceBytes = 1_000_000L, diagnosticsBytes = 1_000_000L))

  val BindingUuid: UUID = UUID.fromString("00000000-0000-0000-0000-000000000201")
  val ProvisioningUuid: UUID = UUID.fromString("00000000-0000-0000-0000-000000000202")

  def root(): MySqlFormat3ReadBudget = Inputs.newAllocationRoot(Inputs.budget.packetBytes,
    new MySqlFormat3AllocationLedger(SourceProfile.maxRetainedBytes()))

  def root(inputs: MySqlScriptSchemaInputs,
      ledger: MySqlFormat3AllocationLedger = new MySqlFormat3AllocationLedger(SourceProfile.maxRetainedBytes())): MySqlFormat3ReadBudget =
    inputs.newAllocationRoot(inputs.budget.packetBytes, ledger)

  def uuidBytes(value: UUID): Array[Byte] =
    java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()

  def binary(value: Array[Byte]): MySqlFormat3Cell =
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(value.clone()))

  def digest(value: Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA-256").digest(value)

  def defaultRow(shape: MySqlFormat3RelationShape): Vector[MySqlFormat3Cell] = shape.columns.map { column =>
    if column.nullable then MySqlFormat3Cell.SqlNull
    else column.kind match
      case MySqlFormat3SqlKind.Integer => MySqlFormat3Cell.SignedInteger(0L)
      case MySqlFormat3SqlKind.Floating => MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(0.0d))
      case MySqlFormat3SqlKind.DateTime => MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 23, 0, 0))
      case MySqlFormat3SqlKind.Text => MySqlFormat3Cell.Text("")
      case MySqlFormat3SqlKind.Binary => binary(new Array[Byte](column.maximumLength.getOrElse(0)))
  }

  def row(table: String, values: (String, MySqlFormat3Cell)*): Vector[MySqlFormat3Cell] =
    val shape = MySqlScriptSchemaInputs.forTable(table)
    values.foldLeft(defaultRow(shape)) { case (current, (name, value)) =>
      val index = shape.columns.indexWhere(_.name == name)
      require(index >= 0, s"$table.$name must belong to the closed full24 layout")
      current.updated(index, value)
    }

  def relations(rows: Map[String, Vector[Vector[MySqlFormat3Cell]]]): Vector[MySqlFormat3RelationSnapshot] =
    MySqlScriptSchemaInputs.canonicalOrder.map(shape =>
      MySqlFormat3RelationSnapshot(shape.table, rows.getOrElse(shape.table, Vector.empty)))

  def genesisRelations(snapshotBytes: Array[Byte] = Array.emptyByteArray,
      catalogRevision: Long = 0L): Vector[MySqlFormat3RelationSnapshot] =
    val profileBytes = SourceProfile.canonicalBytes()
    val provenance = Array[Byte](0x31, 0x32, 0x33)
    val manifest = row("fp_schema_manifest",
      "singleton_fixed1" -> MySqlFormat3Cell.SignedInteger(1L),
      "backend" -> MySqlFormat3Cell.Text("MYSQL"),
      "format_version" -> MySqlFormat3Cell.SignedInteger(MySqlScriptSchemaProfile.SchemaFormat.toLong),
      "ddl_sha256" -> binary(HexFormat.of().parseHex(MySqlScriptSchemaProfile.SqlSha256)),
      "provisioning_uuid" -> binary(uuidBytes(ProvisioningUuid)),
      "bootstrap_state" -> MySqlFormat3Cell.Text("READY"),
      "expected_server_uuid" -> binary(uuidBytes(Connection.serverUuid)),
      "expected_binding_uuid" -> binary(uuidBytes(BindingUuid)),
      "import_state" -> MySqlFormat3Cell.Text("NONE"),
      "guard_manifest_sha256" -> binary(Array.fill[Byte](32)(0x22.toByte)),
      "configuration_sha256" -> binary(Array.fill[Byte](32)(0x33.toByte)),
      "source_provenance_kind" -> MySqlFormat3Cell.Text("FRESH_PLAN"),
      "source_provenance_bytes" -> binary(provenance),
      "source_provenance_sha256" -> binary(digest(provenance)),
      "limit_profile_encoding" -> MySqlFormat3Cell.Text("FP_SOURCE_LIMIT_PROFILE_BE_V1"),
      "limit_profile_bytes" -> binary(profileBytes),
      "limit_profile_sha256" -> binary(digest(profileBytes)),
      "max_region_bytes" -> MySqlFormat3Cell.SignedInteger(SourceProfile.maxRegionBytes()),
      "max_chunk_bytes" -> MySqlFormat3Cell.SignedInteger(SourceProfile.maxChunkBytes()),
      "max_decompressed_bytes" -> MySqlFormat3Cell.SignedInteger(SourceProfile.maxDecompressedBytes()),
      "max_nbt_depth" -> MySqlFormat3Cell.SignedInteger(SourceProfile.maxNbtDepth().toLong),
      "max_members" -> MySqlFormat3Cell.SignedInteger(SourceProfile.maxMembers()),
      "max_retained_bytes" -> MySqlFormat3Cell.SignedInteger(SourceProfile.maxRetainedBytes()),
      "max_sql_packet_bytes" -> MySqlFormat3Cell.SignedInteger(SourceProfile.maxSqlPacketBytes()),
      "max_native_artifact_bytes" -> MySqlFormat3Cell.SignedInteger(SourceProfile.maxNativeArtifactBytes()))
    val binding = row("physical_recovery_binding",
      "singleton_fixed1" -> MySqlFormat3Cell.SignedInteger(1L),
      "binding_uuid" -> binary(uuidBytes(BindingUuid)),
      "cutover_state" -> MySqlFormat3Cell.Text("LEGACY_UNCERTIFIED"),
      "format_version" -> MySqlFormat3Cell.SignedInteger(MySqlScriptSchemaProfile.PhysicalBindingFormat.toLong))
    val intent = row("binding_bootstrap_intent",
      "singleton_fixed1" -> MySqlFormat3Cell.SignedInteger(1L),
      "bootstrap_operation_uuid" -> binary(uuidBytes(UUID.fromString("00000000-0000-0000-0000-000000000203"))),
      "prepare_write_uuid" -> binary(uuidBytes(UUID.fromString("00000000-0000-0000-0000-000000000204"))),
      "certify_write_uuid" -> binary(uuidBytes(UUID.fromString("00000000-0000-0000-0000-000000000205"))),
      "binding_uuid" -> binary(uuidBytes(BindingUuid)),
      "binding_format_version" -> MySqlFormat3Cell.SignedInteger(MySqlScriptSchemaProfile.PhysicalBindingFormat.toLong),
      "provisioning_uuid" -> binary(uuidBytes(ProvisioningUuid)),
      "source_world_identity" -> MySqlFormat3Cell.Text("world://source/primary"),
      "provision_receipt_sha256" -> binary(Array.fill[Byte](32)(0x44.toByte)),
      "stage" -> MySqlFormat3Cell.Text("PREPARED"))
    val writer = row("fp_runtime_writer",
      "singleton_fixed1" -> MySqlFormat3Cell.SignedInteger(1L),
      "binding_uuid" -> binary(uuidBytes(BindingUuid)))
    val catalog = row("fp_script_catalog",
      "binding_uuid" -> binary(uuidBytes(BindingUuid)),
      "catalog_revision" -> MySqlFormat3Cell.SignedInteger(catalogRevision),
      "snapshot_bytes" -> binary(snapshotBytes),
      "snapshot_sha256" -> binary(digest(snapshotBytes)),
      "next_version" -> MySqlFormat3Cell.SignedInteger(1L))
    val values = Map(
      "fp_schema_manifest" -> Vector(manifest),
      "physical_recovery_binding" -> Vector(binding),
      "binding_bootstrap_intent" -> Vector(intent),
      "fp_runtime_writer" -> Vector(writer),
      "fp_script_catalog" -> Vector(catalog))
    relations(values)

  def completeRelations: Vector[MySqlFormat3RelationSnapshot] =
    MySqlScriptSchemaInputs.canonicalOrder.map(shape =>
      MySqlFormat3RelationSnapshot(shape.table, Vector(defaultRow(shape))))

  private def replaceCell(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape,
      column: String, value: MySqlFormat3Cell): Vector[MySqlFormat3Cell] =
    val index = shape.columns.indexWhere(_.name == column)
    require(index >= 0, s"${shape.table}.$column must belong to its frozen shape")
    row.updated(index, value)

  private final case class DeploymentFixture(
      operationRow: Vector[MySqlFormat3Cell],
      historyRows: Vector[Vector[MySqlFormat3Cell]])

  def encodeGenericContext(context: MySqlOperationContext): Array[Byte] =
    val bytes = new ByteArrayOutputStream()
    val output = new DataOutputStream(bytes)
    try
      MySqlModeOperationEffectCodec.writeOperationContext(output, context)
      output.flush()
      bytes.toByteArray
    finally output.close()

  private def recoveryHistoryRow(operation: UUID, workflow: UUID, step: String,
      boot: UUID, bootGeneration: Long, connectionGeneration: UUID, connectionId: Long, fenceEpoch: Long,
      requestIdentity: Array[Byte], target: Array[Byte], preimage: Array[Byte],
      expectedDomain: Option[Long], previous: Option[UUID], diagnostic: Option[Array[Byte]],
      state: String = "COMMITTED", settledBy: Option[UUID] = None,
      reserve: Option[UUID] = None): Vector[MySqlFormat3Cell] =
    val scope = s"scope:$step:$operation".getBytes(StandardCharsets.US_ASCII)
    val phaseFingerprint = digest(s"phase:$step:$operation".getBytes(StandardCharsets.US_ASCII))
    val context = MySqlOperationContext(boot, fenceEpoch, BindingUuid, operation,
      Vector(MySqlExpectedEffect(s"fp_deployment_operation/$operation/$step",
        expectedDomain.getOrElse(0L), digest(target).toVector)))
    val contextBytes = encodeGenericContext(context)
    // 这里只覆盖 DATA 中的不透明 witness 字节与摘要，不构造或断言 producer authority。
    val witness = if state == "RESERVED" then None
      else Some(s"opaque durable witness:$step:$operation".getBytes(StandardCharsets.US_ASCII))
    val planned = LocalDateTime.of(2026, 10, 23, 12, 0).plusSeconds(fenceEpoch)
    val resultAt = if state == "RESERVED" then MySqlFormat3Cell.SqlNull
      else MySqlFormat3Cell.DateTime(planned.plusSeconds(30))
    row("fp_recovery_operation",
      "binding_uuid" -> binary(uuidBytes(BindingUuid)),
      "operation_uuid" -> binary(uuidBytes(operation)),
      "workflow_uuid" -> binary(uuidBytes(workflow)),
      "step" -> MySqlFormat3Cell.Text(step),
      "operation_state" -> MySqlFormat3Cell.Text(state),
      "boot_uuid" -> binary(uuidBytes(boot)),
      "boot_generation" -> MySqlFormat3Cell.SignedInteger(bootGeneration),
      "connection_generation_uuid" -> binary(uuidBytes(connectionGeneration)),
      "connection_id" -> MySqlFormat3Cell.SignedInteger(connectionId),
      "fence_epoch" -> MySqlFormat3Cell.SignedInteger(fenceEpoch),
      "request_identity_bytes" -> binary(requestIdentity),
      "request_identity_sha256" -> binary(digest(requestIdentity)),
      "expected_domain_version" -> expectedDomain.map(MySqlFormat3Cell.SignedInteger.apply).getOrElse(MySqlFormat3Cell.SqlNull),
      "context_encoding" -> MySqlFormat3Cell.Text("FP_RECOVERY_OPERATION_BE_V1"),
      "original_context_bytes" -> binary(contextBytes),
      "original_context_sha256" -> binary(digest(contextBytes)),
      "scope_bytes" -> binary(scope),
      "scope_sha256" -> binary(digest(scope)),
      "preimage_bytes" -> binary(preimage),
      "preimage_sha256" -> binary(digest(preimage)),
      "target_bytes" -> binary(target),
      "target_sha256" -> binary(digest(target)),
      "reserve_operation_uuid" -> reserve.map(id => binary(uuidBytes(id))).getOrElse(MySqlFormat3Cell.SqlNull),
      "previous_step_operation_uuid" -> previous.map(id => binary(uuidBytes(id))).getOrElse(MySqlFormat3Cell.SqlNull),
      "settled_by_operation_uuid" -> settledBy.map(id => binary(uuidBytes(id))).getOrElse(MySqlFormat3Cell.SqlNull),
      "commit_witness_bytes" -> witness.map(binary).getOrElse(MySqlFormat3Cell.SqlNull),
      "commit_witness_sha256" -> witness.map(value => binary(digest(value))).getOrElse(MySqlFormat3Cell.SqlNull),
      "original_diagnostic_identity" -> diagnostic.map(binary).getOrElse(MySqlFormat3Cell.SqlNull),
      "original_diagnostic_sha256" -> diagnostic.map(value => binary(digest(value))).getOrElse(MySqlFormat3Cell.SqlNull),
      "planned_at" -> MySqlFormat3Cell.DateTime(planned),
      "fixed_lease_until" -> MySqlFormat3Cell.DateTime(planned.plusMinutes(1)),
      "result_at" -> resultAt,
      "operation_fingerprint" -> binary(phaseFingerprint))

  private def deploymentFixture(actor: UUID, operation: UUID, workflow: UUID, unknown: Boolean,
      registerStep: UUID, startStep: UUID, finalizeStep: UUID, reconcileStep: Option[UUID],
      createdBoot: UUID, currentBoot: UUID, currentBootGeneration: Long, currentWriterEpoch: Long): DeploymentFixture =
    import MySqlDeploymentOperationRows.ColumnValue.*
    val expected = new ExpectedCommandVersions(UUID.fromString("00000000-0000-0000-0000-000000000501"),
      1, UUID.fromString("00000000-0000-0000-0000-000000000601"), 2, 3, 4, 5)
    val request = new DeployRequest(operation,
      UUID.fromString("00000000-0000-0000-0000-000000000301"),
      UUID.fromString("00000000-0000-0000-0000-000000000401"), 1, expected)
    val receipt = if unknown then
      new MutationReceipt(operation, request.podUuid(), request.moduleInstanceId(), request.moduleGeneration(),
        MutationKind.DEPLOY, expected, OperationPhase.UNKNOWN, ReceiptReason.OUTCOME_UNKNOWN,
        ActualEffectStatus.UNKNOWN, CriticalCommitStatus.UNKNOWN, ReadinessStatus.NOT_EVALUATED, CleanupStatus.NOT_REQUIRED)
    else
      new MutationReceipt(operation, request.podUuid(), request.moduleInstanceId(), request.moduleGeneration(),
        MutationKind.DEPLOY, expected, OperationPhase.APPLIED, ReceiptReason.NONE,
        ActualEffectStatus.APPLIED_CONFIRMED, CriticalCommitStatus.COMMITTED, ReadinessStatus.QUALIFIED, CleanupStatus.NOT_REQUIRED)
    val requestBytes = DeploymentCommandWireCodec.encodeRequest(request)
    val receiptBytes = DeploymentCommandWireCodec.encodeResponse(receipt)
    val queuedReceipt = new MutationReceipt(operation, request.podUuid(), request.moduleInstanceId(), request.moduleGeneration(),
      MutationKind.DEPLOY, expected, OperationPhase.QUEUED, ReceiptReason.NONE,
      ActualEffectStatus.NOT_STARTED, CriticalCommitStatus.NOT_ATTEMPTED,
      ReadinessStatus.NOT_EVALUATED, CleanupStatus.NOT_REQUIRED)
    val queuedReceiptBytes = DeploymentCommandWireCodec.encodeResponse(queuedReceipt)
    val executingReceipt = new MutationReceipt(operation, request.podUuid(), request.moduleInstanceId(), request.moduleGeneration(),
      MutationKind.DEPLOY, expected, OperationPhase.EXECUTING, ReceiptReason.NONE,
      ActualEffectStatus.NOT_STARTED, CriticalCommitStatus.NOT_ATTEMPTED,
      ReadinessStatus.NOT_EVALUATED, CleanupStatus.NOT_REQUIRED)
    val executingReceiptBytes = DeploymentCommandWireCodec.encodeResponse(executingReceipt)
    val finalizedReceiptBytes = receiptBytes
    val enqueueEvidence = s"enqueue:$operation".getBytes(StandardCharsets.US_ASCII)
    val startEvidence = s"step:$operation:START".getBytes(StandardCharsets.US_ASCII)
    val finalizeEvidence = s"step:$operation:FINALIZE".getBytes(StandardCharsets.US_ASCII)
    val reconcileEvidence = s"step:$operation:RECONCILE".getBytes(StandardCharsets.US_ASCII)
    val lastStepCode = if reconcileStep.isDefined then 4L else 3L
    val lastStep = reconcileStep.getOrElse(finalizeStep)
    val lastStepEvidence = if reconcileStep.isDefined then reconcileEvidence else finalizeEvidence
    val lastToken = MySqlDeploymentReceiptSchemaDefinition.RecoverySteps.find(_._1.toLong == lastStepCode)
      .map(_._2).getOrElse(throw new IllegalArgumentException("fixture requires a closed deployment step code"))
    val diagnostic = if unknown then Some(Array[Byte](7, 8, 9)) else None
    val diagnosticValues = diagnostic match
      case Some(bytes) => Vector(Integer(1L), Binary(bytes), Binary(digest(bytes)))
      case None => Vector(SqlNull, SqlNull, SqlNull)
    val values = Vector(
      Binary(Array[Byte](70, 80, 68, 67)), Binary(uuidBytes(operation)), Binary(uuidBytes(actor)),
      Binary(uuidBytes(BindingUuid)), Integer(1L), Binary(requestBytes), Binary(digest(requestBytes)),
      Integer(1L), Binary(receiptBytes), Binary(digest(receiptBytes)), Integer(if reconcileStep.isDefined then 3L else 2L),
      Binary(uuidBytes(createdBoot)), Integer(1L), Integer(1L),
      Binary(uuidBytes(currentBoot)), Integer(currentWriterEpoch),
      Binary(uuidBytes(registerStep)), Binary(uuidBytes(lastStep)),
      Integer(lastStepCode), Binary(digest(s"phase:$lastToken:$lastStep".getBytes(StandardCharsets.US_ASCII))),
      Integer(10L), Integer(20L),
      Integer(1L), Binary(enqueueEvidence), Binary(digest(enqueueEvidence)),
      Integer(1L), Binary(lastStepEvidence), Binary(digest(lastStepEvidence))) ++ diagnosticValues
    val operationRow = values.map {
      case Binary(bytes) => MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes))
      case Integer(value) => MySqlFormat3Cell.SignedInteger(value)
      case SqlNull => MySqlFormat3Cell.SqlNull
    }
    val namespace = HexFormat.of().parseHex(MySqlDeploymentReceiptSchemaDefinition.NamespaceHex)
    val actorBytes = uuidBytes(actor)
    val requestIdentity = namespace ++ actorBytes ++ requestBytes
    val registerHistory = recoveryHistoryRow(registerStep, workflow,
      "DEPLOYMENT_REGISTER", createdBoot, 1L,
      UUID.fromString("00000000-0000-0000-0000-000000000911"), 901L, 1L,
      requestIdentity, enqueueEvidence, Array.emptyByteArray, Some(0L), None, None)
    val startHistory = recoveryHistoryRow(startStep, workflow,
      "DEPLOYMENT_START", createdBoot, 1L,
      UUID.fromString("00000000-0000-0000-0000-000000000912"), 902L, 1L,
      requestIdentity, startEvidence, queuedReceiptBytes, Some(0L), Some(registerStep), None)
    val finalizeBoot = if reconcileStep.isDefined then
      UUID.fromString("00000000-0000-0000-0000-000000000713") else currentBoot
    val finalizeGeneration = if reconcileStep.isDefined then currentBootGeneration - 1L else currentBootGeneration
    val finalizeEpoch = if reconcileStep.isDefined then currentWriterEpoch - 1L else currentWriterEpoch
    val finalizeHistory = recoveryHistoryRow(finalizeStep, workflow,
      "DEPLOYMENT_FINALIZE", finalizeBoot, finalizeGeneration,
      UUID.fromString("00000000-0000-0000-0000-000000000913"), 903L, finalizeEpoch,
      requestIdentity, finalizeEvidence, executingReceiptBytes, Some(1L), Some(startStep), diagnostic)
    val history = Vector(registerHistory, startHistory, finalizeHistory) ++ reconcileStep.map { reconcileId =>
      recoveryHistoryRow(reconcileId, workflow,
        "DEPLOYMENT_RECONCILE", currentBoot, currentBootGeneration,
        UUID.fromString("00000000-0000-0000-0000-000000000914"), 904L, currentWriterEpoch,
        requestIdentity, reconcileEvidence, finalizedReceiptBytes, Some(2L), Some(finalizeStep), diagnostic)
    }
    require(lastToken == (if reconcileStep.isDefined then "DEPLOYMENT_RECONCILE" else "DEPLOYMENT_FINALIZE"),
      "fixture's terminal step token must match its actual public phase transition")
    DeploymentFixture(operationRow, history)

  private def quotaRow(used: Long): Vector[MySqlFormat3Cell] =
    Vector(MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(
      HexFormat.of().parseHex(MySqlDeploymentReceiptSchemaDefinition.NamespaceHex))),
      MySqlFormat3Cell.SignedInteger(8L), MySqlFormat3Cell.SignedInteger(used),
      MySqlFormat3Cell.SignedInteger(11L))

  def full26GenesisRelations(inputs: MySqlScriptSchemaInputs, catalogPresent: Boolean): Vector[MySqlFormat3RelationSnapshot] =
    val baseRows = genesisRelations().iterator.map(relation => relation.table -> relation).toMap
    val manifestShape = inputs.forTable("fp_schema_manifest")
    val oldManifest = baseRows("fp_schema_manifest").rows.head
    val manifest = replaceCell(replaceCell(oldManifest, manifestShape, "format_version",
      MySqlFormat3Cell.SignedInteger(inputs.schemaFormat.toLong)), manifestShape, "ddl_sha256",
      binary(HexFormat.of().parseHex(inputs.ddlSha256)))
    // This fixture accepts explicit inputs; profile bytes and scalar projections must come from those inputs.
    val profile = inputs.sourceLimitProfile
    val profileFields: Map[String, MySqlFormat3Cell] = Map(
      "limit_profile_encoding" -> MySqlFormat3Cell.Text(com.wi.floatingpod.config.SourceLimitProfile.ENCODING),
      "limit_profile_bytes" -> binary(profile.canonicalBytes()),
      "limit_profile_sha256" -> binary(profile.sha256()),
      "max_region_bytes" -> MySqlFormat3Cell.SignedInteger(profile.maxRegionBytes()),
      "max_chunk_bytes" -> MySqlFormat3Cell.SignedInteger(profile.maxChunkBytes()),
      "max_decompressed_bytes" -> MySqlFormat3Cell.SignedInteger(profile.maxDecompressedBytes()),
      "max_nbt_depth" -> MySqlFormat3Cell.SignedInteger(profile.maxNbtDepth().toLong),
      "max_members" -> MySqlFormat3Cell.SignedInteger(profile.maxMembers()),
      "max_retained_bytes" -> MySqlFormat3Cell.SignedInteger(profile.maxRetainedBytes()),
      "max_sql_packet_bytes" -> MySqlFormat3Cell.SignedInteger(profile.maxSqlPacketBytes()),
      "max_native_artifact_bytes" -> MySqlFormat3Cell.SignedInteger(profile.maxNativeArtifactBytes()))
    val exactManifest = manifestShape.columns.zipWithIndex.map { (column, index) =>
      profileFields.getOrElse(column.name, manifest(index))
    }
    val catalog = if catalogPresent then baseRows("fp_script_catalog").rows else Vector.empty
    inputs.canonicalOrder.map { shape =>
      if shape.table == "fp_schema_manifest" then MySqlFormat3RelationSnapshot(shape.table, Vector(exactManifest))
      else if shape.table == "fp_script_catalog" then MySqlFormat3RelationSnapshot(shape.table, catalog)
      else if shape.table == MySqlDeploymentReceiptSchemaDefinition.OperationTable then
        MySqlFormat3RelationSnapshot(shape.table, Vector.empty)
      else if shape.table == MySqlDeploymentReceiptSchemaDefinition.QuotaTable then
        MySqlFormat3RelationSnapshot(shape.table, Vector(quotaRow(0L)))
      else baseRows(shape.table)
    }

  def full26OrdinaryRelations(inputs: MySqlScriptSchemaInputs): Vector[MySqlFormat3RelationSnapshot] =
    val terminal = deploymentFixture(
      UUID.fromString("00000000-0000-0000-0000-000000000101"),
      UUID.fromString("00000000-0000-0000-0000-000000000201"),
      UUID.fromString("00000000-0000-0000-0000-000000000601"), false,
      UUID.fromString("00000000-0000-0000-0000-000000000811"),
      UUID.fromString("00000000-0000-0000-0000-000000000812"),
      UUID.fromString("00000000-0000-0000-0000-000000000813"), None,
      UUID.fromString("00000000-0000-0000-0000-000000000511"),
      UUID.fromString("00000000-0000-0000-0000-000000000711"),
      currentBootGeneration = 2L, currentWriterEpoch = 2L)
    val unknown = deploymentFixture(
      UUID.fromString("00000000-0000-0000-0000-000000000102"),
      UUID.fromString("00000000-0000-0000-0000-000000000202"),
      UUID.fromString("00000000-0000-0000-0000-000000000602"), true,
      UUID.fromString("00000000-0000-0000-0000-000000000821"),
      UUID.fromString("00000000-0000-0000-0000-000000000822"),
      UUID.fromString("00000000-0000-0000-0000-000000000823"),
      Some(UUID.fromString("00000000-0000-0000-0000-000000000824")),
      UUID.fromString("00000000-0000-0000-0000-000000000512"),
      UUID.fromString("00000000-0000-0000-0000-000000000712"),
      currentBootGeneration = 3L, currentWriterEpoch = 3L)
    val genesisRows = full26GenesisRelations(inputs, catalogPresent = true).iterator
      .map(relation => relation.table -> relation).toMap
    val ordinaryRows = completeRelations.iterator.map(relation => relation.table -> relation).toMap
    val bindingShape = inputs.forTable("physical_recovery_binding")
    val binding = replaceCell(genesisRows("physical_recovery_binding").rows.head, bindingShape,
      "cutover_state", MySqlFormat3Cell.Text("CERTIFIED"))
    val intentShape = inputs.forTable("binding_bootstrap_intent")
    val oldIntent = genesisRows("binding_bootstrap_intent").rows.head
    val intent = replaceCell(replaceCell(oldIntent, intentShape, "stage", MySqlFormat3Cell.Text("COMPLETE")),
      intentShape, "marker_ack_sha256", binary(digest("ordinary full26 marker acknowledgement".getBytes(StandardCharsets.US_ASCII))))
    val writerShape = inputs.forTable("fp_runtime_writer")
    val oldWriter = genesisRows("fp_runtime_writer").rows.head
    val writer = replaceCell(replaceCell(replaceCell(oldWriter, writerShape, "owner_boot_uuid",
      binary(uuidBytes(UUID.fromString("00000000-0000-0000-0000-000000000712")))),
      writerShape, "writer_epoch", MySqlFormat3Cell.SignedInteger(3L)),
      writerShape, "lease_until", MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 23, 13, 0)))
    val history = terminal.historyRows ++ unknown.historyRows
    inputs.canonicalOrder.map { shape =>
      if shape.table == "fp_schema_manifest" then
        genesisRows(shape.table)
      else if shape.table == MySqlDeploymentReceiptSchemaDefinition.OperationTable then
        MySqlFormat3RelationSnapshot(shape.table, Vector(terminal.operationRow, unknown.operationRow))
      else if shape.table == MySqlDeploymentReceiptSchemaDefinition.QuotaTable then
        MySqlFormat3RelationSnapshot(shape.table, Vector(quotaRow(2L)))
      else if shape.table == "fp_recovery_operation" then
        MySqlFormat3RelationSnapshot(shape.table, history)
      else if shape.table == "physical_recovery_binding" then
        MySqlFormat3RelationSnapshot(shape.table, Vector(binding))
      else if shape.table == "binding_bootstrap_intent" then
        MySqlFormat3RelationSnapshot(shape.table, Vector(intent))
      else if shape.table == "fp_runtime_writer" then
        MySqlFormat3RelationSnapshot(shape.table, Vector(writer))
      else ordinaryRows.getOrElse(shape.table, genesisRows(shape.table))
    }

  def snapshot(root: MySqlFormat3ReadBudget,
      rows: Vector[MySqlFormat3RelationSnapshot]): MySqlScriptSchemaSnapshot =
    MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
      Inputs.budget.packetBytes, Inputs, rows, root)

  def snapshot(inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget,
      rows: Vector[MySqlFormat3RelationSnapshot]): MySqlScriptSchemaSnapshot =
    MySqlScriptSchemaSnapshot.captureMaterialized(Schema, Connection, Scope,
      inputs.budget.packetBytes, inputs, rows, root)

  def replace(relations: Vector[MySqlFormat3RelationSnapshot], table: String, column: String,
      value: MySqlFormat3Cell): Vector[MySqlFormat3RelationSnapshot] =
    relations.map { relation =>
      if relation.table != table then relation
      else
        val shape = MySqlScriptSchemaInputs.forTable(table)
        val index = shape.columns.indexWhere(_.name == column)
        require(index >= 0, s"$table.$column must belong to the closed full24 layout")
        relation.copy(rows = relation.rows.updated(0, relation.rows.head.updated(index, value)))
    }

  def replaceRowCell(relations: Vector[MySqlFormat3RelationSnapshot], table: String,
      rowIndex: Int, column: String, value: MySqlFormat3Cell): Vector[MySqlFormat3RelationSnapshot] =
    relations.map { relation =>
      if relation.table != table then relation
      else
        val shape = table match
          case MySqlDeploymentReceiptSchemaDefinition.OperationTable => MySqlDeploymentReceiptRelationShapes.operation
          case MySqlDeploymentReceiptSchemaDefinition.QuotaTable => MySqlDeploymentReceiptRelationShapes.quota
          case _ => MySqlFormat3RelationShapes.forTable(table)
        val index = shape.columns.indexWhere(_.name == column)
        require(index >= 0 && rowIndex >= 0 && rowIndex < relation.rows.size,
          s"$table.$column row $rowIndex must belong to its frozen shape")
        relation.copy(rows = relation.rows.updated(rowIndex, relation.rows(rowIndex).updated(index, value)))
    }

  def appendRecoveryRows(relations: Vector[MySqlFormat3RelationSnapshot],
      additions: Vector[Vector[MySqlFormat3Cell]]): Vector[MySqlFormat3RelationSnapshot] =
    relations.map(relation => if relation.table == "fp_recovery_operation" then
      relation.copy(rows = relation.rows ++ additions) else relation)

  def legacyHistoryRow(operation: UUID, step: String, previous: Option[UUID] = None,
      reserve: Option[UUID] = None, state: String = "COMMITTED",
      settledBy: Option[UUID] = None): Vector[MySqlFormat3Cell] =
    recoveryHistoryRow(operation, UUID.fromString("00000000-0000-0000-0000-000000000631"), step,
      UUID.fromString("00000000-0000-0000-0000-000000000632"), 4L,
      UUID.fromString("00000000-0000-0000-0000-000000000633"), 903L, 4L,
      Array[Byte](1, 2, 3), Array[Byte](4, 5, 6), Array[Byte](7), None, previous, None,
      state, settledBy, reserve)

final class MySqlScriptSchemaB1GraphTest:
  import MySqlScriptSchemaDataTestFixture.*

  private def testBytes(data: MySqlScriptSchemaB1Graph.Data): Array[Byte] =
    Array.tabulate[Byte](data.byteLength)(data.bytes.byteAt)

  private def testBytes(payload: MySqlScriptSchemaCanonicalPayload): Array[Byte] =
    Array.tabulate[Byte](payload.byteLength)(payload.bytes.byteAt)

  private def assertTransitionImages(data: MySqlScriptSchemaB1Graph.Data,
      before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      expectedDomain: String, allocation: MySqlFormat3ReadBudget): Unit =
    var beforePayload: MySqlScriptSchemaCanonicalPayload = null
    var targetPayload: MySqlScriptSchemaCanonicalPayload = null
    try
      beforePayload = MySqlScriptSchemaSnapshotCodec.encode(before, allocation)
      targetPayload = MySqlScriptSchemaSnapshotCodec.encode(target, allocation)
      val transitionBytes = testBytes(data)
      assertArrayEquals(digest(transitionBytes), data.sha256.toArray)
      assertEquals(HexFormat.of().withUpperCase().formatHex(data.sha256.toArray), data.sha256Hex)
      val input = new DataInputStream(new ByteArrayInputStream(transitionBytes))
      val domainLength = input.readInt()
      val domain = new String(input.readNBytes(domainLength), java.nio.charset.StandardCharsets.UTF_8)
      assertEquals(expectedDomain, domain)
      assertEquals(data.kind.ordinal, input.readUnsignedByte())
      val beforeLength = input.readLong()
      assertEquals(before.canonicalByteLength, beforeLength)
      assertArrayEquals(testBytes(beforePayload), input.readNBytes(beforeLength.toInt))
      val targetLength = input.readLong()
      assertEquals(target.canonicalByteLength, targetLength)
      assertArrayEquals(testBytes(targetPayload), input.readNBytes(targetLength.toInt))
      assertEquals(-1, input.read())
    finally
      if targetPayload != null then targetPayload.storageBudget.closeScopedReadBudget()
      if beforePayload != null then beforePayload.storageBudget.closeScopedReadBudget()

  private def operationFacts(row: Vector[MySqlFormat3Cell], allocation: MySqlFormat3ReadBudget): (UUID, OperationPhase, Boolean) =
    val shape = MySqlDeploymentReceiptRelationShapes.operation
    val budget = allocation.newCanonicalDataScope()
    var primary: Throwable = null
    try
      val values = row.zip(shape.columns).map { (cell, column) =>
        cell match
          case MySqlFormat3Cell.SqlNull => MySqlDeploymentOperationRows.ColumnValue.SqlNull
          case MySqlFormat3Cell.SignedInteger(value) => MySqlDeploymentOperationRows.ColumnValue.Integer(value)
          case MySqlFormat3Cell.Binary(value) => MySqlDeploymentOperationRows.ColumnValue.Binary(
            value.copyBytes(budget, s"B1 test ${shape.table}.${column.name} actual decoder input"))
          case _ => throw new AssertionError(s"unexpected operation cell ${shape.table}.${column.name}")
      }
      def uuid(column: String): UUID =
        val index = MySqlDeploymentOperationRows.columnNames.indexOf(column)
        values(index) match
          case MySqlDeploymentOperationRows.ColumnValue.Binary(bytes) =>
            val buffer = java.nio.ByteBuffer.wrap(bytes)
            new UUID(buffer.getLong(), buffer.getLong())
          case _ => throw new AssertionError(s"operation $column must be binary")
      val actor = uuid("actor_uuid")
      val operation = uuid("operation_uuid")
      val decoded = MySqlDeploymentOperationRows.decode(
        MySqlDeploymentOperationRows.ColumnValues(values), actor, operation, budget)
      val diagnostic = decoded.copyBinary("diagnostic_bytes", budget)
      (decoded.actorUuid, decoded.receipt.phase(), diagnostic.isDefined)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(() => budget.closeScopedReadBudget()))

  private def replaceLayoutCell(rows: Vector[MySqlFormat3RelationSnapshot], inputs: MySqlScriptSchemaInputs,
      table: String, column: String, value: MySqlFormat3Cell): Vector[MySqlFormat3RelationSnapshot] =
    val shape = inputs.forTable(table)
    rows.map { relation =>
      if relation.table != table then relation
      else relation.copy(rows = relation.rows.updated(0,
        replaceLayoutRow(relation.rows.head, shape, column, value)))
    }

  private def replaceLayoutRow(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape,
      column: String, value: MySqlFormat3Cell): Vector[MySqlFormat3Cell] =
    val index = shape.columns.indexWhere(_.name == column)
    require(index >= 0, s"${shape.table}.$column must belong to its frozen shape")
    row.updated(index, value)

  private def advanceWriterEpoch(rows: Vector[MySqlFormat3RelationSnapshot],
      inputs: MySqlScriptSchemaInputs): Vector[MySqlFormat3RelationSnapshot] =
    val shape = inputs.forTable("fp_runtime_writer")
    val epochIndex = shape.columns.indexWhere(_.name == "writer_epoch")
    val current = rows.find(_.table == shape.table).flatMap(_.rows.headOption)
      .map(_(epochIndex)) match
        case Some(MySqlFormat3Cell.SignedInteger(value)) => value
        case _ => throw new IllegalStateException("writer transition fixture requires an integer before epoch")
    val next = Math.addExact(current, 1L)
    rows.map { relation =>
      if relation.table == shape.table then
        relation.copy(rows = Vector(relation.rows.head.updated(epochIndex, MySqlFormat3Cell.SignedInteger(next))))
      else relation
    }

  @Test def freshGenesisBuildsIndependentCertificateFromExactBeforeAndTargetImages(): Unit =
    val allocation = root()
    var beforePayload: MySqlScriptSchemaCanonicalPayload = null
    var targetPayload: MySqlScriptSchemaCanonicalPayload = null
    var data: MySqlScriptSchemaB1Graph.Data = null
    try
      val before = snapshot(allocation, genesisRelations().map { relation =>
        if relation.table == "fp_script_catalog" then relation.copy(rows = Vector.empty) else relation
      })
      val target = snapshot(allocation, genesisRelations())
      beforePayload = MySqlScriptSchemaSnapshotCodec.encode(before, allocation)
      targetPayload = MySqlScriptSchemaSnapshotCodec.encode(target, allocation)

      MySqlScriptSchemaB1Graph.requireFreshGenesisTransition(before, target)
      data = MySqlScriptSchemaB1Graph.genesisData(before, target, allocation)

      assertEquals(MySqlScriptSchemaB1Graph.TransitionKind.FreshGenesis, data.kind)
      assertSame(Inputs, data.inputs)
      assertSame(before, data.beforeSnapshot)
      assertSame(target, data.targetSnapshot)
      assertSame(allocation, data.originalBudget)
      assertTrue(data.storageBudget.sharesAllocationLedger(allocation))
      assertArrayEquals(digest(testBytes(data)), data.sha256.toArray)
      assertEquals(HexFormat.of().withUpperCase().formatHex(data.sha256.toArray), data.sha256Hex)

      val input = new DataInputStream(new ByteArrayInputStream(testBytes(data)))
      val domainLength = input.readInt()
      val domain = new String(input.readNBytes(domainLength), java.nio.charset.StandardCharsets.UTF_8)
      assertEquals("floatingpod.mysql.script-publication1-b1-transition/v1", domain)
      assertEquals(MySqlScriptSchemaB1Graph.TransitionKind.FreshGenesis.ordinal, input.readUnsignedByte())
      val beforeLength = input.readLong()
      assertEquals(before.canonicalByteLength, beforeLength)
      assertArrayEquals(testBytes(beforePayload), input.readNBytes(beforeLength.toInt))
      val targetLength = input.readLong()
      assertEquals(target.canonicalByteLength, targetLength)
      assertArrayEquals(testBytes(targetPayload), input.readNBytes(targetLength.toInt))
      assertEquals(-1, input.read())
    finally
      if data != null then data.storageBudget.closeScopedReadBudget()
      if targetPayload != null then targetPayload.storageBudget.closeScopedReadBudget()
      if beforePayload != null then beforePayload.storageBudget.closeScopedReadBudget()
      allocation.closeRootReadBudget()

  @Test def genesisRejectsCatalogBytesOutsideTheFrozenRevisionZeroEmptyImage(): Unit =
    val allocation = root()
    try
      val beforeRelations = genesisRelations().map { relation =>
        if relation.table == "fp_script_catalog" then relation.copy(rows = Vector.empty) else relation
      }
      val before = snapshot(allocation, beforeRelations)
      val nonemptyCatalog = snapshot(allocation, genesisRelations(snapshotBytes = Array[Byte](0x41)))
      val error = assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlScriptSchemaB1Graph.requireFreshGenesisTransition(before, nonemptyCatalog))
      assertTrue(error.getMessage.contains("snapshot_bytes differs"), error.getMessage)

      val nonzeroRevision = snapshot(allocation, genesisRelations(catalogRevision = 1L))
      val revisionError = assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlScriptSchemaB1Graph.requireFreshGenesisTransition(before, nonzeroRevision))
      assertTrue(revisionError.getMessage.contains("catalog_revision differs"), revisionError.getMessage)
    finally allocation.closeRootReadBudget()

  @Test def ordinaryHandoffComparesAllOtherTwentyThreeTypedRelationsAndCertifiesOnlyWriterChange(): Unit =
    val allocation = root()
    var data: MySqlScriptSchemaB1Graph.Data = null
    try
      val beforeRelations = completeRelations
      val writerShape = MySqlScriptSchemaInputs.forTable("fp_runtime_writer")
      val writerIndex = writerShape.columns.indexWhere(_.name == "writer_epoch")
      val targetRelations = beforeRelations.map { relation =>
        if relation.table == "fp_runtime_writer" then
          relation.copy(rows = Vector(relation.rows.head.updated(writerIndex, MySqlFormat3Cell.SignedInteger(1L))))
        else relation
      }
      val before = snapshot(allocation, beforeRelations)
      val target = snapshot(allocation, targetRelations)
      MySqlScriptSchemaB1Graph.requireOnlyWriterChanged(before, target)
      data = MySqlScriptSchemaB1Graph.ordinaryWriterHandoffData(before, target, allocation)
      assertEquals(MySqlScriptSchemaB1Graph.TransitionKind.OrdinaryWriterHandoff, data.kind)
      assertTrue(data.byteLength > before.canonicalByteLength + target.canonicalByteLength)

      val unrelatedChange = replace(targetRelations, "physical_recovery_binding", "binding_uuid",
        binary(uuidBytes(BindingUuid)))
      val unrelatedTarget = snapshot(allocation, unrelatedChange)
      val error = assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlScriptSchemaB1Graph.requireOnlyWriterChanged(before, unrelatedTarget))
      assertTrue(error.getMessage.contains("changed unrelated relation physical_recovery_binding"), error.getMessage)
    finally
      if data != null then data.storageBudget.closeScopedReadBudget()
      allocation.closeRootReadBudget()

  @Test def everyScriptRelationValueChangesTheFull24CommitmentAndBreaksWriterOnlyAdmission(): Unit =
    val allocation = root()
    try
      val before = snapshot(allocation, completeRelations)
      val binding = binary(uuidBytes(BindingUuid))
      Vector("fp_script_catalog", "fp_script_operation", "fp_script_candidate", "fp_script_version").foreach { table =>
        val changed = replace(completeRelations, table, "binding_uuid", binding)
        val target = snapshot(allocation, changed)
        assertFalse(before.exactlyEquals(target), s"$table must be part of the full24 snapshot equality")
        assertNotEquals(before.canonicalSha256, target.canonicalSha256,
          s"$table changes must alter the independently computed complete snapshot commitment")
        val error = assertThrows(classOf[java.sql.SQLDataException], () =>
          MySqlScriptSchemaB1Graph.requireOnlyWriterChanged(before, target))
        assertTrue(error.getMessage.contains(s"changed unrelated relation $table"), error.getMessage)
      }
    finally allocation.closeRootReadBudget()

  @Test def deploymentReceiptFull26GenesisUsesItsOwnDomainAndRetainsExactSnapshots(): Unit =
    val allocation = root(Full26Inputs)
    var data: MySqlScriptSchemaB1Graph.Data = null
    try
      val before = snapshot(Full26Inputs, allocation, full26GenesisRelations(Full26Inputs, catalogPresent = false))
      val target = snapshot(Full26Inputs, allocation, full26GenesisRelations(Full26Inputs, catalogPresent = true))
      MySqlScriptSchemaB1Graph.requireDeploymentReceiptFull26FreshGenesisTransition(before, target)
      data = MySqlScriptSchemaB1Graph.deploymentReceiptFull26GenesisData(before, target, allocation)

      assertEquals(5, data.inputs.schemaFormat)
      assertEquals(Full26DdlSha256, data.inputs.ddlSha256)
      assertSame(before, data.beforeSnapshot)
      assertSame(target, data.targetSnapshot)
      assertSame(allocation, data.originalBudget)
      assertTrue(data.storageBudget.sharesAllocationLedger(allocation))
      assertEquals(0, target.relation(MySqlDeploymentReceiptSchemaDefinition.OperationTable).rows.size)
      val quota = target.relation(MySqlDeploymentReceiptSchemaDefinition.QuotaTable).rows
      assertEquals(1, quota.size)
      assertEquals(0L, quota.head(2).asInstanceOf[MySqlFormat3Cell.SignedInteger].value)
      assertEquals(11L, quota.head(3).asInstanceOf[MySqlFormat3Cell.SignedInteger].value)
      assertTransitionImages(data, before, target,
        "floatingpod.mysql.script-publication1-deployment-receipt1-b1-transition/v1", allocation)

      val wrongFormatBefore = snapshot(Full26Inputs, allocation, replaceLayoutCell(
        full26GenesisRelations(Full26Inputs, catalogPresent = false), Full26Inputs,
        "fp_schema_manifest", "format_version", MySqlFormat3Cell.SignedInteger(4L)))
      val wrongFormatTarget = snapshot(Full26Inputs, allocation, replaceLayoutCell(
        full26GenesisRelations(Full26Inputs, catalogPresent = true), Full26Inputs,
        "fp_schema_manifest", "format_version", MySqlFormat3Cell.SignedInteger(4L)))
      val formatError = assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlScriptSchemaB1Graph.requireDeploymentReceiptFull26FreshGenesisTransition(
          wrongFormatBefore, wrongFormatTarget))
      assertTrue(formatError.getMessage.contains("format_version differs"), formatError.getMessage)

      val wrongDdlBefore = snapshot(Full26Inputs, allocation, replaceLayoutCell(
        full26GenesisRelations(Full26Inputs, catalogPresent = false), Full26Inputs,
        "fp_schema_manifest", "ddl_sha256", binary(Array.fill[Byte](32)(0x11.toByte))))
      val wrongDdlTarget = snapshot(Full26Inputs, allocation, replaceLayoutCell(
        full26GenesisRelations(Full26Inputs, catalogPresent = true), Full26Inputs,
        "fp_schema_manifest", "ddl_sha256", binary(Array.fill[Byte](32)(0x11.toByte))))
      val ddlError = assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlScriptSchemaB1Graph.requireDeploymentReceiptFull26FreshGenesisTransition(wrongDdlBefore, wrongDdlTarget))
      assertTrue(ddlError.getMessage.contains("ddl_sha256 differs"), ddlError.getMessage)
    finally
      if data != null then data.storageBudget.closeScopedReadBudget()
      allocation.closeRootReadBudget()

  @Test def deploymentReceiptFull26OrdinaryHandoffRetainsAllRelationsUnknownRowsAndQuota(): Unit =
    val allocation = root(Full26Inputs)
    var data: MySqlScriptSchemaB1Graph.Data = null
    try
      val beforeRelations = full26OrdinaryRelations(Full26Inputs)
      val targetRelations = advanceWriterEpoch(beforeRelations, Full26Inputs)
      val before = snapshot(Full26Inputs, allocation, beforeRelations)
      val target = snapshot(Full26Inputs, allocation, targetRelations)
      MySqlScriptSchemaB1Graph.requireDeploymentReceiptFull26OnlyWriterChanged(before, target)
      data = MySqlScriptSchemaB1Graph.deploymentReceiptFull26OrdinaryWriterHandoffData(before, target, allocation)

      assertEquals(MySqlScriptSchemaB1Graph.TransitionKind.OrdinaryWriterHandoff, data.kind)
      assertSame(before, data.beforeSnapshot)
      assertSame(target, data.targetSnapshot)
      assertEquals(26, before.relations.size)
      assertEquals(467, Full26Inputs.canonicalOrder.flatMap(_.columns).size)
      val operation = before.relation(MySqlDeploymentReceiptSchemaDefinition.OperationTable).rows
      assertEquals(MySqlDeploymentOperationRows.columnNames,
        MySqlDeploymentReceiptRelationShapes.operation.columns.map(_.name))
      assertEquals(31, operation.head.size)
      val terminal = operationFacts(operation(0), allocation)
      val unknown = operationFacts(operation(1), allocation)
      assertEquals(OperationPhase.APPLIED, terminal._2)
      assertFalse(terminal._3)
      assertEquals(OperationPhase.UNKNOWN, unknown._2)
      assertTrue(unknown._3)
      assertNotEquals(terminal._1, unknown._1, "terminal and UNKNOWN history belong to different actors")
      assertEquals(2L, before.relation(MySqlDeploymentReceiptSchemaDefinition.QuotaTable).rows.head(2)
        .asInstanceOf[MySqlFormat3Cell.SignedInteger].value)
      assertEquals(11L, before.relation(MySqlDeploymentReceiptSchemaDefinition.QuotaTable).rows.head(3)
        .asInstanceOf[MySqlFormat3Cell.SignedInteger].value,
        "quota_revision remains independent of operation count")
      assertTransitionImages(data, before, target,
        "floatingpod.mysql.script-publication1-deployment-receipt1-b1-transition/v1", allocation)

      val alteredQuota = replaceLayoutCell(beforeRelations, Full26Inputs,
        MySqlDeploymentReceiptSchemaDefinition.QuotaTable, "quota_revision", MySqlFormat3Cell.SignedInteger(12L))
      val quotaTarget = snapshot(Full26Inputs, allocation, alteredQuota)
      val error = assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlScriptSchemaB1Graph.requireDeploymentReceiptFull26OnlyWriterChanged(before, quotaTarget))
      assertTrue(error.getMessage.contains("changed unrelated relation fp_deployment_quota"), error.getMessage)
    finally
      if data != null then data.storageBudget.closeScopedReadBudget()
      allocation.closeRootReadBudget()

  @Test def full26EntryRejectsLegacyMixingAndSameLedgerSiblingRoot(): Unit =
    val ledger = new MySqlFormat3AllocationLedger(SourceProfile.maxRetainedBytes())
    val allocation = root(Full26Inputs, ledger)
    val sibling = root(Full26Inputs, ledger)
    val legacyRoot = root()
    try
      val exactBefore = snapshot(Full26Inputs, allocation, full26OrdinaryRelations(Full26Inputs))
      val exactTargetRelations = advanceWriterEpoch(exactBefore.relations, Full26Inputs)
      val exactTarget = snapshot(Full26Inputs, allocation, exactTargetRelations)

      val legacy = assertThrows(classOf[java.sql.SQLDataException], () =>
        {
          MySqlScriptSchemaB1Graph.genesisData(exactBefore, exactBefore, allocation)
          ()
        })
      assertTrue(legacy.getMessage.contains("explicit ScriptPublication1 layout"), legacy.getMessage)
      val legacyOrdinary = assertThrows(classOf[java.sql.SQLDataException], () =>
        {
          MySqlScriptSchemaB1Graph.ordinaryWriterHandoffData(exactBefore, exactTarget, allocation)
          ()
        })
      assertTrue(legacyOrdinary.getMessage.contains("explicit ScriptPublication1 layout"), legacyOrdinary.getMessage)
      val mismatched = assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlScriptSchemaB1Graph.requireDeploymentReceiptFull26OnlyWriterChanged(exactBefore, exactBefore))
      assertTrue(mismatched.getMessage.contains("did not change any writer"), mismatched.getMessage)

      val siblingBefore = snapshot(Full26Inputs, sibling, full26OrdinaryRelations(Full26Inputs))
      val siblingTargetRelations = advanceWriterEpoch(siblingBefore.relations, Full26Inputs)
      val siblingTarget = snapshot(Full26Inputs, sibling, siblingTargetRelations)
      val wrongRoot = assertThrows(classOf[java.sql.SQLDataException], () =>
        {
          MySqlScriptSchemaB1Graph.deploymentReceiptFull26OrdinaryWriterHandoffData(
            siblingBefore, siblingTarget, allocation)
          ()
        })
      assertTrue(wrongRoot.getMessage.contains("exact original root"), wrongRoot.getMessage)

      val legacySnapshot = snapshot(legacyRoot, completeRelations)
      val mixed = assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlScriptSchemaB1Graph.requireDeploymentReceiptFull26OnlyWriterChanged(exactBefore, legacySnapshot))
      assertTrue(mixed.getMessage.contains("explicit ScriptPublication1DeploymentReceipt1 layout"), mixed.getMessage)
    finally
      legacyRoot.closeRootReadBudget()
      sibling.closeRootReadBudget()
      allocation.closeRootReadBudget()

  @Test def full26CertificateBudgetFailureClosesCanonicalDataScopeDebt(): Unit =
    val allocation = root(Full26Inputs)
    try
      val before = snapshot(Full26Inputs, allocation, full26OrdinaryRelations(Full26Inputs))
      val targetRelations = advanceWriterEpoch(before.relations, Full26Inputs)
      val target = snapshot(Full26Inputs, allocation, targetRelations)
      val beforeLength = before.canonicalByteLength
      val targetLength = target.canonicalByteLength
      val framingBytes = Math.addExact(21L, MySqlFormat3Utf8.byteLength(
        "floatingpod.mysql.script-publication1-deployment-receipt1-b1-transition/v1"))
      val transitionLength = Math.addExact(Math.addExact(beforeLength, targetLength), framingBytes)
      // 256 child metadata + 512 writer workspace + 32 references + (DATA length + 192 holder) need length+992.
      // Leave one byte less so the ledger rejects the final DATA reserve after the scope is entered.
      val remainingAtFinalData = Math.addExact(transitionLength, 991L)
      val remaining = allocation.profile.maxRetainedBytes() - allocation.accountedMemoryBytes
      assertTrue(remaining > remainingAtFinalData,
        s"fixture must leave room to reach the Full26 B1 final DATA boundary, got $remaining, need $remainingAtFinalData")
      allocation.reserveRetainedMemory(remaining - remainingAtFinalData,
        "controlled Full26 B1 final DATA budget boundary")
      val beforeAttempt = allocation.accountedMemoryBytes
      val childrenBeforeAttempt = allocation.retainedCanonicalDataChildCount
      val error = assertThrows(classOf[java.sql.SQLDataException], () =>
        {
          MySqlScriptSchemaB1Graph.deploymentReceiptFull26OrdinaryWriterHandoffData(before, target, allocation)
          ()
        })
      assertEquals("ScriptPublication1 B1 final DATA and holder exceeds the shared explicit retained-memory budget",
        error.getMessage)
      assertEquals(beforeAttempt, allocation.accountedMemoryBytes,
        "failed canonical scope must return its temporary and child debt while retaining original snapshot debt")
      assertEquals(childrenBeforeAttempt, allocation.retainedCanonicalDataChildCount,
        "failed canonical scope must unlink its child from the original allocation root")
    finally allocation.closeRootReadBudget()
