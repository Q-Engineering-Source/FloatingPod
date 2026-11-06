package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.FloatingPodPose
import com.wi.floatingpod.storage.PhysicalRecoveryChunk
import com.wi.floatingpod.server.io.IoOperationTag
import java.io.DataOutputStream
import java.nio.file.Path
import java.sql.SQLDataException
import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID
import scala.collection.IndexedSeq
import MySqlModeOperationEffectCodec.{allocationBudget,limitProfile,relations,rowCount,value}

/** Recall 的 DATA 编码。所有载荷计入原 root；编码字节从不代替 Main 或 SQL 许可。 */
private[mysql] object MySqlNormalRecallCanonical:
  val PrepareDomain = "FP_NORMAL_RECALL_PREPARE_BE_V1"
  val RemovalDomain = "FP_NORMAL_RECALL_REMOVAL_BE_V1"
  val TicketDomain = "FP_NORMAL_RECALL_TICKET_BE_V1"
  val CheckpointDomain = "FP_NORMAL_RECALL_CHECKPOINT_BE_V1"
  val RecoveryDomain = "FP_NORMAL_RECALL_RECOVERY_BE_V1"
  val DerivedCellDomain = "FP_NORMAL_RECALL_DERIVED_CELL_V1"
  private val ContextColumns = Set("original_context_bytes", "original_context_sha256")

  /** 历史scope仅为严格解出的DATA，刻意不构造当前command/session identity。 */
  final case class DecodedScope(bytes: MySqlFormat3Binary, tag: IoOperationTag, workflow: UUID,
      reserve: UUID, binding: UUID, epoch: Long, connectionId: Long, lease: LocalDateTime,
      server: UUID, schema: String, stableSession: DecodedSession)
  final case class DecodedSession(barrier: UUID, namespace: String, serverVersion: String,
      effectiveUser: String, provisioning: UUID, ddl: String, manifest: String,
      configuration: MySqlFormat3Binary, provenanceKind: String, provenance: MySqlFormat3Binary,
      schemaFormat: Int, physicalFormat: Int)

  /** 共同历史scope字段严格DATA入口；不捕获当前session/command身份。 */
  def decodeScopeFields(input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget): DecodedScope =
    decodeData(input, root)(_.scopeData())

  /** 独立 schema5 DATA 入口；旧 decoder 仍只接受原 format3/physical2。 */
  private[mysql] def decodeDeploymentReceiptScopeFields(input: MySqlFormat3Binary,
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget): DecodedScope =
    require(inputs!=null && root!=null && !root.isClosed &&
      inputs.dataLayout==MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      (inputs.sourceLimitProfile eq root.profile),"exact Full26 scope inputs/profile required")
    MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root).requireHistoricalPayloadInputs(inputs)
    val scope=decodePhysical(input,root,4096L,Some(inputs)) { _=>
      val budget=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
      new DataReader(input,0,input.length,root,budget,new MemberCount).scopeData(5)
    }
    require(scope.schema==inputs.schema && scope.stableSession.ddl.equalsIgnoreCase(inputs.ddlSha256),
      "historical schema5 scope differs from exact schema/DDL inputs")
    scope
  final case class DecodedPose(x: Long, y: Long, z: Long, yaw: Int, pitch: Int):
    require(Vector(x, y, z).forall(v => java.lang.Double.isFinite(java.lang.Double.longBitsToDouble(v))) &&
      Vector(yaw, pitch).forall(v => java.lang.Float.isFinite(java.lang.Float.intBitsToFloat(v))), "finite historical pose required")
  final case class DecodedHeader(scope: DecodedScope, stop: UUID, previous: Option[UUID], pod: UUID,
      owner: Option[UUID], attempt: UUID, footprint: UUID, version: Long, locatorRevision: Long,
      dirtyRevision: Long, source: MySqlNormalRecallSourceIdentity, dimension: Int,
      committed: DecodedPose, finalPose: DecodedPose, chunks: Vector[PhysicalRecoveryChunk])
  final case class DecodedPrepare(header: DecodedHeader, purpose: MySqlNormalRecallPurpose,
      domain: Vector[MySqlFormat3RelationSnapshot], locator: Vector[MySqlFormat3RelationSnapshot],
      originalPrepare: Option[UUID], originalCheckpoint: Option[UUID], settlement: Option[UUID],
      predecessor: Option[MySqlRecoveryOperationRawEnvelope], sources: Vector[MySqlFormat3RelationSnapshot])
  final case class DecodedCheckpoint(header: DecodedHeader, prepare: DecodedPrepare,
      prepareBytes: MySqlFormat3Binary, removal: MySqlNormalRecallRemovalFacts,
      ticket: MySqlNormalRecallTicketFacts, afterVersion: Long, afterLocator: Long, receipt: UUID,
      beforeObligations: Vector[MySqlFormat3RelationSnapshot], targetObligations: Vector[MySqlFormat3RelationSnapshot],
      targetDomainLocator: Vector[MySqlFormat3RelationSnapshot])

  private val SourceWriteTable = "physical_source_write"
  private val SourceObligationRevisionDomain = "FP_SOURCE_WRITE_OBLIGATIONS_BE_V1"
  private val SourceChildReserveRequestDomain = "FP_SOURCE_CHILD_RESERVE_REQUEST_BE_V1"
  private val SourceChildReserveScopeDomain = "FP_SOURCE_CHILD_RESERVE_SCOPE_BE_V1"
  private val SourceChildReserveDerivedFieldDomain = "FP_SOURCE_CHILD_RESERVE_DERIVED_BE_V1"
  private val SourceChildReserveEffectDomain = "FP_SOURCE_CHILD_RESERVE_EFFECT_BE_V1"
  private val SourceChildReserveFingerprintDomain = "FP_SOURCE_CHILD_RESERVE_FINGERPRINT_BE_V1"
  private val SourceChildReserveWitnessDomain = "FP_SOURCE_CHILD_RESERVE_WITNESS_BE_V1"
  private val SourceChildReserveOperationContextEncoding = "FP_RECOVERY_OPERATION_BE_V1"
  private val SourceChildReserveOperationTable = "fp_recovery_operation"
  private val SourceChildReserveWriterTable = "fp_runtime_writer"
  private val SourceScopeDomainPrefix = "FP_SOURCE_"
  private val SourceChildReserveOperationDerivedColumns = Set(
    "original_context_bytes", "original_context_sha256", "target_bytes", "target_sha256",
    "commit_witness_bytes", "commit_witness_sha256", "operation_fingerprint")
  private val SourceWriteImmutableColumns = Vector(
    "binding_uuid", "write_uuid", "workflow_uuid", "resource_kind", "carrier_key_hash", "carrier_key",
    "save_identity", "loader_identity", "region_identity", "slot_x", "slot_z", "write_generation",
    "payload_descriptor_bytes", "payload_descriptor_sha256", "payload_length", "payload_sha256",
    "payload_artifact_identity", "undo_kind", "undo_descriptor_bytes", "undo_descriptor_sha256", "undo_length",
    "undo_content_sha256", "undo_artifact_identity", "member_coverage_bytes", "member_coverage_sha256",
    "obligation_revisions_bytes", "obligation_revisions_sha256", "limit_profile_sha256", "receipt_codec",
    "prepared_operation_uuid")

  /** Source-write SQL row DATA only; this value never issues WritePermit or native completion. */
  final class DecodedSourceWrite private[MySqlNormalRecallCanonical] (
      val row: Vector[MySqlFormat3Cell], val binding: UUID, val write: UUID, val workflow: UUID,
      val resourceKey: MySqlRecoveryResourceKey, val saveIdentity: String, val loaderIdentity: String,
      val regionIdentity: String, val slotX: Int, val slotZ: Int, val writeGeneration: Long,
      val payloadDescriptor: MySqlFormat3Binary, val payloadDescriptorSha256: MySqlFormat3Binary,
      val payloadLength: Long, val payloadSha256: MySqlFormat3Binary, val payloadArtifactIdentity: String,
      val undoKind: String, val undoDescriptor: MySqlFormat3Binary,
      val undoDescriptorSha256: MySqlFormat3Binary, val undoLength: Long,
      val undoContentSha256: Option[MySqlFormat3Binary], val undoArtifactIdentity: String,
      val memberCoverage: MySqlFormat3Binary, val memberCoverageSha256: MySqlFormat3Binary,
      val obligationRevisions: MySqlFormat3Binary, val obligationRevisionsSha256: MySqlFormat3Binary,
      val obligationRevisionData: DecodedSourceObligationRevisions,
      val limitProfileSha256: MySqlFormat3Binary, val preparedOperation: UUID, val state: String,
      val resolvedOperation: Option[UUID], val durableReceipt: Option[MySqlFormat3Binary],
      val durableReceiptSha256: Option[MySqlFormat3Binary], val failureDiagnostic: Option[MySqlFormat3Binary],
      val failureDiagnosticSha256: Option[MySqlFormat3Binary], val revision: Long, val lastMutation: UUID,
      private[mysql] val immutableCells: Vector[MySqlFormat3Cell]):
    def hasSameImmutableIdentity(other: DecodedSourceWrite): Boolean =
      other != null && binding == other.binding && write == other.write && workflow == other.workflow &&
        immutableCells == other.immutableCells

  /** 严格解出既有SQL行；opaque artifact bytes 仍需其原owner codec/issuer核验。 */
  def decodeSourceWrite(row: Vector[MySqlFormat3Cell], root: MySqlFormat3ReadBudget): DecodedSourceWrite =
    require(row != null && root != null && !root.isClosed && !root.isScopedReadBudget,
      "original open full26 root and source-write row required")
    val checked = MySqlControlledDebugRows.update(row, SourceWriteTable, Map.empty, root)
    def cell(name: String): MySqlFormat3Cell = MySqlModeOperationEffectCodec.value(checked, SourceWriteTable, name)
    def uuid(name: String): UUID = cell(name) match
      case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 =>
        val raw = bytes.copyBytes(root, "source-write UUID " + name)
        val buffer = java.nio.ByteBuffer.wrap(raw)
        val value = new UUID(buffer.getLong(), buffer.getLong())
        require(value != new UUID(0L, 0L), "source-write identity UUID must be nonzero: " + name)
        value
      case _ => throw new SQLDataException("source-write UUID column is absent or malformed: " + name)
    def text(name: String): String = cell(name) match
      case MySqlFormat3Cell.Text(value) if value.nonEmpty => value
      case _ => throw new SQLDataException("source-write text column is absent or empty: " + name)
    def number(name: String): Long = cell(name) match
      case MySqlFormat3Cell.SignedInteger(value) => value
      case _ => throw new SQLDataException("source-write integer column is absent: " + name)
    def binary(name: String): MySqlFormat3Binary = cell(name) match
      case MySqlFormat3Cell.Binary(value) => value
      case _ => throw new SQLDataException("source-write binary column is absent: " + name)
    def optionalBinary(name: String): Option[MySqlFormat3Binary] = cell(name) match
      case MySqlFormat3Cell.SqlNull => None
      case MySqlFormat3Cell.Binary(value) => Some(value)
      case _ => throw new SQLDataException("source-write optional binary column is malformed: " + name)
    def digestMatches(value: MySqlFormat3Binary, digest: MySqlFormat3Binary, context: String): Unit =
      require(value != null && digest != null && digest.length == 32, context + " SHA-256 shape differs")
      val sha = java.security.MessageDigest.getInstance("SHA-256")
      var index = 0
      while index < value.length do
        sha.update(value.byteAt(index)); index += 1
      require(digest.equalsBytes(sha.digest()), context + " SHA-256 differs")
    val binding = uuid("binding_uuid")
    val write = uuid("write_uuid")
    val workflow = uuid("workflow_uuid")
    val prepared = uuid("prepared_operation_uuid")
    val save = text("save_identity")
    val loader = text("loader_identity")
    val region = text("region_identity")
    val slotX = Math.toIntExact(number("slot_x"))
    val slotZ = Math.toIntExact(number("slot_z"))
    val generation = number("write_generation")
    val key = MySqlRecoveryResourceKey.carrier(save, loader, region, slotX, slotZ, root.profile)
    require(text("resource_kind") == "CARRIER", "source-write must bind one CARRIER reservation")
    require(binary("carrier_key").equalsBytes(key.copyCanonicalBytes(root, "source-write full CARRIER key")) &&
      binary("carrier_key_hash").equalsVector(key.keyHashSha256),
      "source-write full CARRIER key/hash differs from its typed identity")
    require(generation >= 0L, "source-write generation must be nonnegative")
    val payloadDescriptor = binary("payload_descriptor_bytes")
    val payloadDescriptorSha = binary("payload_descriptor_sha256")
    digestMatches(payloadDescriptor, payloadDescriptorSha, "source-write payload descriptor")
    val payloadLength = number("payload_length")
    val payloadSha = binary("payload_sha256")
    require(payloadLength > 0L && payloadSha.length == 32, "source-write payload identity is incomplete")
    val payloadArtifact = text("payload_artifact_identity")
    val undoKind = text("undo_kind")
    require(undoKind == "FULL_CHUNK" || undoKind == "SLOT_ABSENT", "unsupported source-write undo kind")
    val undoDescriptor = binary("undo_descriptor_bytes")
    val undoDescriptorSha = binary("undo_descriptor_sha256")
    digestMatches(undoDescriptor, undoDescriptorSha, "source-write undo descriptor")
    val undoLength = number("undo_length")
    val undoContentSha = optionalBinary("undo_content_sha256")
    require((undoKind, undoLength, undoContentSha) match
      case ("FULL_CHUNK", n, Some(hash)) => n > 0L && hash.length == 32
      case ("SLOT_ABSENT", 0L, None) => true
      case _ => false, "source-write undo content identity differs from its kind")
    val undoArtifact = text("undo_artifact_identity")
    val memberCoverage = binary("member_coverage_bytes")
    val memberCoverageSha = binary("member_coverage_sha256")
    digestMatches(memberCoverage, memberCoverageSha, "source-write member coverage")
    val obligationRevisions = binary("obligation_revisions_bytes")
    val obligationRevisionsSha = binary("obligation_revisions_sha256")
    digestMatches(obligationRevisions, obligationRevisionsSha, "source-write obligation revisions")
    val obligationRevisionData = decodeSourceObligationRevisions(obligationRevisions, binding, key, root)
    val limitProfileSha = binary("limit_profile_sha256")
    require(limitProfileSha.equalsBytes(root.profile.sha256()), "source-write limit profile differs from the original root")
    require(text("receipt_codec") == "FP_SOURCE_WRITE_RECEIPT_BE_V1", "source-write receipt codec differs")
    val state = text("write_state")
    val resolved = optionalBinary("resolved_operation_uuid").map { bytes =>
      require(bytes.length == 16, "source-write resolved operation UUID length differs")
      val raw = bytes.copyBytes(root, "source-write resolved operation UUID")
      val buffer = java.nio.ByteBuffer.wrap(raw)
      val value = new UUID(buffer.getLong(), buffer.getLong())
      require(value != new UUID(0L, 0L), "source-write resolved operation UUID must be nonzero")
      value
    }
    val durable = optionalBinary("durable_receipt_bytes")
    val durableSha = optionalBinary("durable_receipt_sha256")
    require(durable.isDefined == durableSha.isDefined, "source-write durable receipt bytes/hash must be paired")
    durable.zip(durableSha).foreach((bytes, sha) => digestMatches(bytes, sha, "source-write durable receipt"))
    val diagnostic = optionalBinary("failure_diagnostic_identity")
    val diagnosticSha = optionalBinary("failure_diagnostic_sha256")
    require(diagnostic.isDefined == diagnosticSha.isDefined, "source-write failure diagnostic bytes/hash must be paired")
    diagnostic.zip(diagnosticSha).foreach((bytes, sha) => digestMatches(bytes, sha, "source-write failure diagnostic"))
    val revision = number("write_revision")
    val lastMutation = uuid("last_mutation_operation_uuid")
    require(revision >= 0L, "source-write revision must be nonnegative")
    state match
      case "PREPARED" =>
        require(resolved.isEmpty && durable.isEmpty && diagnostic.isEmpty && lastMutation == prepared,
          "prepared source-write row cannot contain a result or another mutation owner")
      case "DURABLE" =>
        require(resolved.nonEmpty && durable.exists(_.length > 0) && lastMutation == resolved.get,
          "durable source-write row needs its original resolved operation and receipt")
      case "NO_PUT" | "SUPERSEDED" =>
        require(resolved.nonEmpty && durable.isEmpty && lastMutation == resolved.get,
          "source-write no-put/superseded row must retain its resolved operation without a durable receipt")
      case "FAILED_NEEDS_RECONCILIATION" =>
        require(resolved.nonEmpty && durable.isEmpty && diagnostic.exists(_.length > 0) && lastMutation == resolved.get,
          "failed source-write row needs its original operation and diagnostic identity")
      case _ => throw new SQLDataException("unknown source-write state")
    val immutable = SourceWriteImmutableColumns.map(name => MySqlModeOperationEffectCodec.value(checked, SourceWriteTable, name))
    root.reserveRetainedMemory(Math.addExact(1024L, Math.multiplyExact(immutable.size.toLong, 64L)),
      "decoded immutable source-write tuple DATA")
    new DecodedSourceWrite(checked, binding, write, workflow, key, save, loader, region, slotX, slotZ, generation,
      payloadDescriptor, payloadDescriptorSha, payloadLength, payloadSha, payloadArtifact, undoKind,
      undoDescriptor, undoDescriptorSha, undoLength, undoContentSha, undoArtifact, memberCoverage,
      memberCoverageSha, obligationRevisions, obligationRevisionsSha, obligationRevisionData,
      limitProfileSha, prepared, state,
      resolved, durable, durableSha, diagnostic, diagnosticSha, revision, lastMutation, immutable)

  /** Source obligation 主键与捕获revision的严格DATA；不生成写权限或discharge proof。 */
  final class SourceObligationRevision private[MySqlNormalRecallCanonical] (
      val binding: UUID, val unit: UUID, val forest: UUID, val carrierKeyHash: Vector[Byte],
      val carrierKey: MySqlFormat3Binary, val purpose: String, val revision: Long)

  final class DecodedSourceObligationRevisions private[MySqlNormalRecallCanonical] (
      val values: Vector[SourceObligationRevision], val canonical: MySqlRecoveryCanonicalBytes)

  def sourceObligationRevision(binding: UUID, unit: UUID, forest: UUID,
      carrierKeyHash: Vector[Byte], carrierKey: MySqlFormat3Binary,
      purpose: String, revision: Long): SourceObligationRevision =
    new SourceObligationRevision(binding, unit, forest, carrierKeyHash, carrierKey, purpose, revision)

  private def compareUuid(left: UUID, right: UUID): Int =
    val high = java.lang.Long.compareUnsigned(left.getMostSignificantBits, right.getMostSignificantBits)
    if high != 0 then high else java.lang.Long.compareUnsigned(left.getLeastSignificantBits, right.getLeastSignificantBits)

  private def compareUnsigned(left: Vector[Byte], right: Vector[Byte]): Int =
    var index = 0
    while index < Math.min(left.size, right.size) do
      val compared = Integer.compare(left(index) & 255, right(index) & 255)
      if compared != 0 then return compared
      index += 1
    Integer.compare(left.size, right.size)

  private def compareAscii(left: String, right: String): Int =
    var index = 0
    while index < Math.min(left.length, right.length) do
      val compared = Integer.compare(left.charAt(index).toInt, right.charAt(index).toInt)
      if compared != 0 then return compared
      index += 1
    Integer.compare(left.length, right.length)

  private def compareObligationRevision(left: SourceObligationRevision, right: SourceObligationRevision): Int =
    val binding = compareUuid(left.binding, right.binding)
    if binding != 0 then binding
    else
      val unit = compareUuid(left.unit, right.unit)
      if unit != 0 then unit
      else
        val forest = compareUuid(left.forest, right.forest)
        if forest != 0 then forest
        else
          val key = compareUnsigned(left.carrierKeyHash, right.carrierKeyHash)
          if key != 0 then key else compareAscii(left.purpose, right.purpose)

  private def writeSourceObligationRevisions(output: DataOutputStream,
      values: Vector[SourceObligationRevision]): Unit =
    require(values != null, "typed source obligation revisions are required")
    output.writeInt(values.size)
    values.foreach { value =>
      uuid(output, value.binding); uuid(output, value.unit); uuid(output, value.forest)
      hash(output, value.carrierKeyHash)
      output.writeInt(value.carrierKey.length); value.carrierKey.writeTo(output)
      text(output, value.purpose); output.writeLong(value.revision)
    }

  def encodeSourceObligationRevisions(values: Vector[SourceObligationRevision],
      binding: UUID, resourceKey: MySqlRecoveryResourceKey,
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    requireCanonicalRoot(root)
    require(values != null && MySqlNormalRecallRequest.nonZero(binding) && resourceKey != null &&
      resourceKey.resourceKind == "CARRIER" && resourceKey.exactProfile == root.profile &&
      values.size.toLong <= root.profile.maxMembers(),
      "exact original profile, binding, CARRIER and bounded obligation DATA required")
    root.reserveRetainedMemory(Math.addExact(1024L, Math.multiplyExact(values.size.toLong, 256L)),
      "source-write canonical obligation revision DATA")
    val expectedKey = MySqlFormat3Binary.takeOwned(resourceKey.copyCanonicalBytes(root,
      "source-write obligation full CARRIER key"), root)
    var previous: SourceObligationRevision = null
    values.foreach { value =>
      require(value != null && value.binding == binding && MySqlNormalRecallRequest.nonZero(value.unit) &&
        MySqlNormalRecallRequest.nonZero(value.forest) && value.carrierKeyHash != null &&
        value.carrierKeyHash.size == 32 && value.carrierKey != null &&
        value.carrierKeyHash == resourceKey.keyHashSha256 &&
        (value.carrierKey == expectedKey) &&
        Set("OLD_HOLD", "TARGET_STAGED", "CURRENT_HOLD", "OLD_RETIRE", "UNDO").contains(value.purpose) &&
        value.revision >= 0L, "source obligation revision identity/purpose/revision differs")
      require(previous == null || compareObligationRevision(previous, value) < 0,
        "source obligation revision keys must be unique and in canonical SQL-PK order")
      previous = value
    }
    encode(SourceObligationRevisionDomain, root)(writeSourceObligationRevisions(_, values))

  private def decodeSourceObligationRevisions(input: MySqlFormat3Binary, binding: UUID,
      resourceKey: MySqlRecoveryResourceKey, root: MySqlFormat3ReadBudget): DecodedSourceObligationRevisions =
    require(input != null && MySqlNormalRecallRequest.nonZero(binding) && resourceKey != null,
      "source-write obligation bytes and exact identity required")
    decodeData(input, root) { reader =>
      reader.domain(SourceObligationRevisionDomain)
      val count = reader.int32()
      require(count >= 0 && count.toLong <= root.profile.maxMembers(),
        "source obligation revision count exceeds original profile")
      root.reserveRetainedMemory(Math.addExact(1024L, Math.multiplyExact(count.toLong, 256L)),
        "decoded source obligation revision DATA")
      val values = Vector.newBuilder[SourceObligationRevision]
      var previous: SourceObligationRevision = null
      var index = 0
      while index < count do
        val itemBinding = reader.uuid(); val unit = reader.uuid(); val forest = reader.uuid()
        val keyHash = Vector.fill(32)(reader.octet().toByte)
        val keyFrame = reader.dataFrame()
        val key = keyFrame.copyAll(); keyFrame.finish()
        val purpose = reader.textData(); val revision = reader.int64()
        val item = new SourceObligationRevision(itemBinding, unit, forest, keyHash, key, purpose, revision)
        require(item.binding == binding && keyHash == resourceKey.keyHashSha256 &&
          key.equalsBytes(resourceKey.copyCanonicalBytes(root, "decoded obligation full CARRIER key")) &&
          Set("OLD_HOLD", "TARGET_STAGED", "CURRENT_HOLD", "OLD_RETIRE", "UNDO").contains(purpose) && revision >= 0L,
          "decoded source obligation revision differs from write binding/CARRIER")
        require(previous == null || compareObligationRevision(previous, item) < 0,
          "duplicate or unsorted source obligation revision primary key")
        values += item; previous = item; index += 1
      reader.finish()
      val result = values.result()
      val canonical = encodeSourceObligationRevisions(result, binding, resourceKey, root)
      var matches = false
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        matches = canonical.exactlyMatchesCell(MySqlFormat3Cell.Binary(input))
      }
      require(matches, "noncanonical source obligation revision DATA")
      new DecodedSourceObligationRevisions(result, canonical)
    }

  /** Complete source-write O nested scope DATA; it is not a command/custody capability. */
  final class SourceWriteUnitStamp private[MySqlNormalRecallCanonical] (
      val unit: UUID, val unitRevision: Long, val epoch: Long, val currentForest: UUID)

  final class SourceWriteObligationStamp private[MySqlNormalRecallCanonical] (
      val key: SourceObligationRevision, val beforeState: String)

  final class SourceWriteCarrierIdentityData private[MySqlNormalRecallCanonical] (
      val resourceKey: MySqlRecoveryResourceKey, val saveIdentity: String, val loaderIdentity: String,
      val regionIdentity: String, val slotX: Int, val slotZ: Int,
      val canonicalBytes: MySqlFormat3Binary)

  /** PREPARE 前的 source scope 纯 DATA，不含捕获、签发或 permit 权限。 */
  class DecodedSourceWriteScopeRequest private[MySqlNormalRecallCanonical] (
      val binding: UUID, val childWorkflow: UUID, val parentRecallWorkflow: UUID,
      val parentP2Operation: UUID, val write: UUID, val carrierIdentity: SourceWriteCarrierIdentityData,
      val writeGeneration: Long, val units: Vector[SourceWriteUnitStamp],
      val obligations: Vector[SourceWriteObligationStamp], val canonical: MySqlRecoveryCanonicalBytes):
    val carrierKey: MySqlRecoveryResourceKey = carrierIdentity.resourceKey
    def saveIdentity: String = carrierIdentity.saveIdentity
    def loaderIdentity: String = carrierIdentity.loaderIdentity
    def regionIdentity: String = carrierIdentity.regionIdentity
    def slotX: Int = carrierIdentity.slotX
    def slotZ: Int = carrierIdentity.slotZ

  /** G 的 LOADED/COLD 持久观察绑定；这是 DATA，私有owner引用仍由调用方保留。 */
  final class SourceWriteObservationBindingData private[MySqlNormalRecallCanonical] (
      val phase: String,
      val observationId: UUID,
      val bootId: UUID,
      val bootGeneration: Long,
      val connectionGeneration: Long,
      val domainSessionId: UUID,
      val domainObservedGeneration: Long,
      val domainRegistryGeneration: Long,
      val saveNamespace: String,
      val loaderNamespace: String,
      val dimension: Int,
      val providerType: String,
      val dimensionDirectory: String,
      val regionDirectory: String,
      val completeCensusEpoch: Option[Long],
      val observerGeneration: Long)

  /** 具名新域包住完整原 FP_SOURCE_WRITE_SCOPE_BE_V1，不向旧 V1 追加尾字段。 */
  final class DecodedSourceWriteCaptureContext private[MySqlNormalRecallCanonical] (
      val scope: DecodedSourceWriteScopeRequest,
      val observer: SourceWriteObservationBindingData,
      val canonical: MySqlRecoveryCanonicalBytes)

  /** 一项受限 SOURCE reservation 的完整 Full26 前像；None 表示精确不存在。 */
  final class SourceChildReserveResourceExpectation private[MySqlNormalRecallCanonical] (
      val resource: MySqlRecoveryResourceKey,
      val before: Option[Vector[MySqlFormat3Cell]])

  /** One-use root or exact per-plan child charge transferred into an H1 request and its enclosing plan DATA. */
  final class SourceChildReserveRequestDataPrecharge private[MySqlNormalRecallCanonical] (
      private[MySqlNormalRecallCanonical] val root: MySqlFormat3ReadBudget,
      private[MySqlNormalRecallCanonical] val dataBudget: MySqlFormat3ReadBudget,
      private[MySqlNormalRecallCanonical] val unitCount: Int,
      private[MySqlNormalRecallCanonical] val obligationCount: Int,
      private[MySqlNormalRecallCanonical] val resourceCount: Int,
      private[MySqlNormalRecallCanonical] val bytes: Long):
    private var requestBuilt = false
    private var committed = false

    private[MySqlNormalRecallCanonical] def requireAvailable(actualRoot: MySqlFormat3ReadBudget,
        actualDataBudget: MySqlFormat3ReadBudget,
        actualUnitCount: Int, actualObligationCount: Int, actualResourceCount: Int): Unit = synchronized {
      require(!requestBuilt && !committed && (root eq actualRoot) && unitCount == actualUnitCount &&
        (dataBudget eq actualDataBudget) && obligationCount == actualObligationCount && resourceCount == actualResourceCount,
        "one-use exact-account SOURCE R0 request precharge and item counts required")
    }

    private[MySqlNormalRecallCanonical] def markRequestBuilt(actualRoot: MySqlFormat3ReadBudget,
        actualDataBudget: MySqlFormat3ReadBudget,
        actualUnitCount: Int, actualObligationCount: Int, actualResourceCount: Int): Unit = synchronized {
      requireAvailable(actualRoot, actualDataBudget, actualUnitCount, actualObligationCount, actualResourceCount)
      requestBuilt = true
    }

    private[mysql] def commit(): Unit = synchronized {
      require(requestBuilt && !committed, "built SOURCE R0 request precharge must transfer once into its plan")
      committed = true
    }

    private[mysql] def releaseIfUncommitted(): Unit = synchronized {
      if !committed then
        committed = true
        dataBudget.releaseRetainedMemory(bytes, "unused SOURCE R0 request precharge")
    }

  /** H1 的完整R0请求 DATA；obligation候选仍须由原历史解释器验证，且不表示R0已提交。 */
  final class SourceChildReserveRequestData private[MySqlNormalRecallCanonical] (
      val binding: UUID,
      val childWorkflow: UUID,
      val parentRecallWorkflow: UUID,
      val parentP2Operation: UUID,
      val reserveOperation: UUID,
      val plannedWrite: UUID,
      val writeGeneration: Long,
      val carrierIdentity: SourceWriteCarrierIdentityData,
      val units: Vector[SourceWriteUnitStamp],
      val obligations: Vector[SourceWriteObligationStamp],
      val resources: Vector[SourceChildReserveResourceExpectation],
      val canonical: MySqlRecoveryCanonicalBytes)

  /** H1 R0 scope DATA；会话身份仍引用本次实际当前捕获的身份对象。 */
  private[mysql] final class SourceChildReserveScopeData private[MySqlNormalRecallCanonical] (
      val contextIdentity: MySqlRepositoryOperationContextIdentity,
      val request: SourceChildReserveRequestData,
      val inputs: MySqlScriptSchemaInputs,
      val canonical: MySqlRecoveryCanonicalBytes)

  /** Recovered H1 identity is DATA parsed from the envelope; it is never returned as a live session identity. */
  private[mysql] final class HistoricalSourceChildReserveScopeData private[MySqlNormalRecallCanonical] (
      val envelope: MySqlFormat3Binary,
      val request: SourceChildReserveRequestData,
      val commonScope: DecodedScope,
      val inputs: MySqlScriptSchemaInputs,
      private[MySqlNormalRecallCanonical] val typedScope: SourceChildReserveScopeData,
      val root: MySqlFormat3ReadBudget,
      val dataBudget: MySqlFormat3ReadBudget):
    private[mysql] def typedScopeData: SourceChildReserveScopeData = typedScope

  /** Strict source-reserve receipt candidate. Recovery publishes ReceiptData only after full history qualification. */
  private[mysql] final class HistoricalSourceChildReserveReceiptData private[MySqlNormalRecallCanonical] (
      val row: Vector[MySqlFormat3Cell],
      val scope: HistoricalSourceChildReserveScopeData,
      val request: SourceChildReserveRequestData,
      val preimage: SourceChildReserveEffectData,
      val projectedTarget: SourceChildReserveEffectData,
      val fingerprint: SourceChildReservePhaseFingerprintData,
      val originalContext: SourceChildReserveOriginalContextData,
      val witness: SourceChildReserveWitnessData,
      val root: MySqlFormat3ReadBudget,
      val dataBudget: MySqlFormat3ReadBudget)

  private def hasDomainToken(input: MySqlFormat3Binary, prefix: String, exact: Boolean): Boolean =
    if input == null || prefix == null || prefix.isEmpty || input.length < 4 + prefix.length then false
    else
      val length = ((input.byteAt(0).toInt & 255) << 24) |
        ((input.byteAt(1).toInt & 255) << 16) |
        ((input.byteAt(2).toInt & 255) << 8) | (input.byteAt(3).toInt & 255)
      length >= prefix.length && (!exact || length == prefix.length) &&
        input.length >= 4 + length && prefix.indices.forall(index =>
          input.byteAt(4 + index) == prefix.charAt(index).toByte)

  /** Exact H1 routing check; malformed bytes after this domain still go through the strict decoder and fail. */
  private[mysql] def isHistoricalSourceChildReserveScope(input: MySqlFormat3Binary): Boolean =
    hasDomainToken(input, SourceChildReserveScopeDomain, exact = true)

  /** Unknown and unsupported SOURCE_* formats never fall through to the generic historical receipt decoder. */
  private[mysql] def isSourceHistoryDomain(input: MySqlFormat3Binary): Boolean =
    hasDomainToken(input, SourceScopeDomainPrefix, exact = false)

  /** H1 TARGET 单字段派生标记；只保留确切请求、列主键和值，不含提交或授权能力。 */
  private[mysql] final class SourceChildReserveDerivedFieldData private[MySqlNormalRecallCanonical] (
      val request: SourceChildReserveRequestData,
      val inputs: MySqlScriptSchemaInputs,
      val image: MySqlOperationEffectImage,
      val tableName: String,
      val primaryKey: Vector[MySqlFormat3Cell],
      val columnName: String,
      val columnSqlDeclaration: String,
      val canonical: MySqlRecoveryCanonicalBytes)

  /** H1 效果图中的Cell仍是原SQL Cell；仅确定的TARGET字段可携带既有派生DATA。 */
  private[mysql] enum SourceChildReserveEffectCell:
    case Literal(value: MySqlFormat3Cell)
    case Derived(value: SourceChildReserveDerivedFieldData)

  private[mysql] final case class SourceChildReserveEffectRelationData(
      table: String, rows: Vector[Vector[SourceChildReserveEffectCell]])

  /** H1 纯DATA全26表效果图；保留原root、精确输入与R0 DATA child，不授予SQL能力。 */
  private[mysql] final class SourceChildReserveEffectData private[MySqlNormalRecallCanonical] (
      val request: SourceChildReserveRequestData,
      val inputs: MySqlScriptSchemaInputs,
      val root: MySqlFormat3ReadBudget,
      val dataBudget: MySqlFormat3ReadBudget,
      val image: MySqlOperationEffectImage,
      val relations: Vector[SourceChildReserveEffectRelationData],
      val canonical: MySqlRecoveryCanonicalBytes)

  /** H1 phase fingerprint DATA；完整绑定同一 R0 scope、BEFORE 与 projected TARGET。 */
  private[mysql] final class SourceChildReservePhaseFingerprintData private[MySqlNormalRecallCanonical] (
      val scope: SourceChildReserveScopeData,
      val preimage: SourceChildReserveEffectData,
      val projectedTarget: SourceChildReserveEffectData,
      val root: MySqlFormat3ReadBudget,
      val dataBudget: MySqlFormat3ReadBudget,
      val canonical: MySqlRecoveryCanonicalBytes)

  /** H1 原始单阶段 R0 context DATA；context canonical bytes 复用原通用 codec。 */
  private[mysql] final class SourceChildReserveOriginalContextData private[MySqlNormalRecallCanonical] (
      val fingerprint: SourceChildReservePhaseFingerprintData,
      val context: MySqlOperationContext,
      val root: MySqlFormat3ReadBudget,
      val dataBudget: MySqlFormat3ReadBudget,
      val canonical: MySqlRecoveryCanonicalBytes)

  /** H1 witness DATA；resultAt 来自调用者真实值并须等于 projected TARGET O.result_at。 */
  private[mysql] final class SourceChildReserveWitnessData private[MySqlNormalRecallCanonical] (
      val originalContext: SourceChildReserveOriginalContextData,
      val resultAt: LocalDateTime,
      val root: MySqlFormat3ReadBudget,
      val dataBudget: MySqlFormat3ReadBudget,
      val canonical: MySqlRecoveryCanonicalBytes)

  final class DecodedSourceWriteScope private[MySqlNormalRecallCanonical] (
      binding: UUID, childWorkflow: UUID, parentRecallWorkflow: UUID, parentP2Operation: UUID,
      write: UUID, carrierIdentity: SourceWriteCarrierIdentityData, writeGeneration: Long,
      units: Vector[SourceWriteUnitStamp], obligations: Vector[SourceWriteObligationStamp],
      canonical: MySqlRecoveryCanonicalBytes)
      extends DecodedSourceWriteScopeRequest(binding, childWorkflow, parentRecallWorkflow, parentP2Operation,
        write, carrierIdentity, writeGeneration, units, obligations, canonical)

  def sourceWriteCarrierIdentity(resourceKey: MySqlRecoveryResourceKey, saveIdentity: String,
      loaderIdentity: String, regionIdentity: String, slotX: Int, slotZ: Int,
      root: MySqlFormat3ReadBudget): SourceWriteCarrierIdentityData =
    requireCanonicalRoot(root)
    require(resourceKey != null && resourceKey.resourceKind == "CARRIER" &&
      resourceKey.exactProfile == root.profile && saveIdentity != null && loaderIdentity != null &&
      regionIdentity != null,
      "exact original-profile CARRIER key and native source identities required")
    val reconstructed = MySqlRecoveryResourceKey.carrier(saveIdentity, loaderIdentity,
      regionIdentity, slotX, slotZ, root.profile)
    require(resourceKey.exactlyEquals(reconstructed),
      "native save/loader/region/slot fields differ from the complete canonical CARRIER key")
    val keyBytes = MySqlFormat3Binary.takeOwned(resourceKey.copyCanonicalBytes(root,
      "source-write typed CARRIER key bytes"), root)
    new SourceWriteCarrierIdentityData(resourceKey, saveIdentity, loaderIdentity,
      regionIdentity, slotX, slotZ, keyBytes)

  /** H3 retained carrier identity is copied into the exact issuer-validated plan DATA child. */
  private[mysql] def sourceWriteCarrierIdentity(resourceKey: MySqlRecoveryResourceKey, saveIdentity: String,
      loaderIdentity: String, regionIdentity: String, slotX: Int, slotZ: Int,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): SourceWriteCarrierIdentityData =
    requireSourcePlanDataBudget(root, dataBudget)
    require(resourceKey != null && resourceKey.resourceKind == "CARRIER" &&
      resourceKey.exactProfile == root.profile && saveIdentity != null && loaderIdentity != null &&
      regionIdentity != null,
      "exact original-profile CARRIER key and native source identities required")
    var reconstructedMatches = false
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val reconstructed = MySqlRecoveryResourceKey.carrier(saveIdentity, loaderIdentity,
        regionIdentity, slotX, slotZ, root.profile)
      reconstructedMatches = resourceKey.exactlyEquals(reconstructed)
    }
    require(reconstructedMatches,
      "native save/loader/region/slot fields differ from the complete canonical CARRIER key")
    val keyBytes = MySqlFormat3Binary.takeOwned(resourceKey.copyCanonicalBytes(dataBudget,
      "source-write typed CARRIER key bytes"), dataBudget)
    dataBudget.reserveRetainedMemory(256L, "SOURCE R0 carrier identity DATA before allocation")
    new SourceWriteCarrierIdentityData(resourceKey, saveIdentity, loaderIdentity,
      regionIdentity, slotX, slotZ, keyBytes)

  private def sourceWriteCarrierIdentityForHistoricalData(resourceKey: MySqlRecoveryResourceKey,
      saveIdentity: String, loaderIdentity: String, regionIdentity: String, slotX: Int, slotZ: Int,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): SourceWriteCarrierIdentityData =
    requireSourceDataBudget(root, dataBudget)
    require(resourceKey != null && resourceKey.resourceKind == "CARRIER" &&
      resourceKey.exactProfile == root.profile && saveIdentity != null && loaderIdentity != null &&
      regionIdentity != null,
      "historical SOURCE R0 requires the exact original-profile CARRIER identity fields")
    withSourceChildReserveTemporary(dataBudget, 256L, "historical SOURCE R0 CARRIER key comparison") {
      val reconstructed = MySqlRecoveryResourceKey.carrier(saveIdentity, loaderIdentity,
        regionIdentity, slotX, slotZ, root.profile)
      require(resourceKey.exactlyEquals(reconstructed),
        "historical SOURCE R0 full CARRIER key differs from its complete identity fields")
    }
    val keyBytes = MySqlFormat3Binary.takeOwned(resourceKey.copyCanonicalBytes(dataBudget,
      "historical SOURCE R0 typed CARRIER key bytes"), dataBudget)
    dataBudget.reserveRetainedMemory(256L, "historical SOURCE R0 carrier identity DATA before allocation")
    new SourceWriteCarrierIdentityData(resourceKey, saveIdentity, loaderIdentity,
      regionIdentity, slotX, slotZ, keyBytes)

  private def sourceChildReserveResourceExpectationForHistoricalData(resource: MySqlRecoveryResourceKey,
      before: Option[Vector[MySqlFormat3Cell]], inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): SourceChildReserveResourceExpectation =
    requireSourceDataBudget(root, dataBudget)
    val reservationShape = sourceChildReservationShape(inputs, root)
    require(resource != null && resource.exactProfile == root.profile &&
      Set("CARRIER", "SOURCE_UNIT").contains(resource.resourceKind) && before != null,
      "historical SOURCE R0 resource requires its original-profile full CARRIER/SOURCE_UNIT key")
    before.foreach(validateSourceChildReservationRowShape(_, reservationShape, root))
    dataBudget.reserveRetainedMemory(192L, "historical SOURCE R0 resource expectation DATA before allocation")
    new SourceChildReserveResourceExpectation(resource, before)

  def sourceWriteUnitStamp(unit: UUID, unitRevision: Long, epoch: Long,
      currentForest: UUID): SourceWriteUnitStamp =
    new SourceWriteUnitStamp(unit, unitRevision, epoch, currentForest)

  def sourceWriteObligationStamp(key: SourceObligationRevision, beforeState: String): SourceWriteObligationStamp =
    new SourceWriteObligationStamp(key, beforeState)

  private def compareSourceWriteUnit(left: SourceWriteUnitStamp, right: SourceWriteUnitStamp): Int =
    compareUuid(left.unit, right.unit)

  private def compareSourceWriteObligation(left: SourceWriteObligationStamp,
      right: SourceWriteObligationStamp): Int =
    val unit = compareUuid(left.key.unit, right.key.unit)
    if unit != 0 then unit
    else
      val forest = compareUuid(left.key.forest, right.key.forest)
      if forest != 0 then forest
      else
        val hash = compareUnsigned(left.key.carrierKeyHash, right.key.carrierKeyHash)
        if hash != 0 then hash else compareAscii(left.key.purpose, right.key.purpose)

  private def requireSourceWriteScopeData(binding: UUID, childWorkflow: UUID, writeUuid: UUID,
      carrierIdentity: SourceWriteCarrierIdentityData, writeGeneration: Long,
      expectedObligations: Vector[SourceObligationRevision], parentRecallWorkflow: UUID,
      parentP2Operation: UUID, units: Vector[SourceWriteUnitStamp],
      obligations: Vector[SourceWriteObligationStamp], root: MySqlFormat3ReadBudget): Unit =
    requireCanonicalRoot(root)
    val resourceKey = if carrierIdentity == null then null else carrierIdentity.resourceKey
    require(MySqlNormalRecallRequest.nonZero(binding) && MySqlNormalRecallRequest.nonZero(childWorkflow) &&
      MySqlNormalRecallRequest.nonZero(writeUuid) && MySqlNormalRecallRequest.nonZero(parentRecallWorkflow) &&
      MySqlNormalRecallRequest.nonZero(parentP2Operation) && units != null && obligations != null &&
      expectedObligations != null && resourceKey != null && resourceKey.resourceKind == "CARRIER" &&
      carrierIdentity.canonicalBytes != null && resourceKey.exactProfile == root.profile &&
      writeGeneration >= 0L,
      "exact CARRIER source request and original parent Recall/P2 lineage required")
    require(units.nonEmpty && units.size.toLong <= root.profile.maxMembers() &&
      obligations.size == expectedObligations.size && obligations.size.toLong <= root.profile.maxMembers(),
      "complete bounded source-write unit and obligation scopes required")
    root.reserveRetainedMemory(Math.addExact(1024L, Math.addExact(
      Math.multiplyExact(units.size.toLong, 128L), Math.multiplyExact(obligations.size.toLong, 256L))),
      "typed source-write nested scope DATA")
    val expectedCarrierKey = carrierIdentity.canonicalBytes
    var previousUnit: SourceWriteUnitStamp = null
    units.foreach { item =>
      require(item != null && MySqlNormalRecallRequest.nonZero(item.unit) && item.unitRevision >= 0L &&
        item.epoch >= 0L && MySqlNormalRecallRequest.nonZero(item.currentForest),
        "source-write UnitStamp requires exact unit revision, epoch and current forest")
      require(previousUnit == null || compareSourceWriteUnit(previousUnit, item) < 0,
        "source-write UnitStamps must be unique and sorted by unsigned UUID")
      previousUnit = item
    }
    val affectedUnits = units.map(_.unit).toSet
    var previousObligation: SourceWriteObligationStamp = null
    obligations.zip(expectedObligations).foreach { (item, expected) =>
      require(item != null && item.key != null && item.key.binding == binding &&
        item.key.carrierKeyHash == resourceKey.keyHashSha256 &&
        item.key.carrierKey.compareTo(expectedCarrierKey) == 0 &&
        affectedUnits.contains(item.key.unit) && MySqlNormalRecallRequest.nonZero(item.key.forest) &&
        item.key.purpose != null &&
        Set("OLD_HOLD", "TARGET_STAGED", "CURRENT_HOLD", "OLD_RETIRE", "UNDO").contains(item.key.purpose) &&
        item.key.revision == expected.revision && item.key.unit == expected.unit &&
        item.key.forest == expected.forest && item.key.purpose == expected.purpose &&
        item.key.carrierKeyHash == expected.carrierKeyHash &&
        item.key.carrierKey.compareTo(expected.carrierKey) == 0 &&
        (item.beforeState == "OPEN" || item.beforeState == "DISCHARGED"),
        "source-write nested obligation scope differs from the immutable SQL row PK/revisions")
      require(previousObligation == null || compareSourceWriteObligation(previousObligation, item) < 0,
        "source-write obligation stamps must be unique and in canonical full-PK order")
      previousObligation = item
    }

  private def writeSourceWriteScope(output: DataOutputStream, binding: UUID, childWorkflow: UUID,
      writeUuid: UUID, carrierIdentity: SourceWriteCarrierIdentityData, writeGeneration: Long,
      parentRecallWorkflow: UUID, parentP2Operation: UUID, units: Vector[SourceWriteUnitStamp],
      obligations: Vector[SourceWriteObligationStamp], root: MySqlFormat3ReadBudget): Unit =
    uuid(output, binding); uuid(output, childWorkflow); uuid(output, parentRecallWorkflow)
    uuid(output, parentP2Operation); uuid(output, writeUuid)
    output.writeInt(carrierIdentity.canonicalBytes.length); carrierIdentity.canonicalBytes.writeTo(output)
    output.writeLong(writeGeneration); hash(output, root.profile.sha256().toVector)
    output.writeInt(units.size)
    units.foreach { item =>
      uuid(output, item.unit); output.writeLong(item.unitRevision); output.writeLong(item.epoch); uuid(output, item.currentForest)
    }
    output.writeInt(obligations.size)
    obligations.foreach { item =>
      uuid(output, item.key.unit); uuid(output, item.key.forest); text(output, "CARRIER")
      output.writeInt(item.key.carrierKey.length); item.key.carrierKey.writeTo(output)
      text(output, item.key.purpose); output.writeLong(item.key.revision); text(output, item.beforeState)
    }

  def encodeSourceWriteScopeRequest(request: DecodedSourceWriteScopeRequest,
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    require(request != null, "typed PREPARE-before source-write scope request required")
    requireSourceWriteScopeData(request.binding, request.childWorkflow, request.write, request.carrierIdentity,
      request.writeGeneration, request.obligations.map(_.key), request.parentRecallWorkflow,
      request.parentP2Operation, request.units, request.obligations, root)
    encode("FP_SOURCE_WRITE_SCOPE_BE_V1", root) { output =>
      writeSourceWriteScope(output, request.binding, request.childWorkflow, request.write,
        request.carrierIdentity, request.writeGeneration, request.parentRecallWorkflow,
        request.parentP2Operation, request.units, request.obligations, root)
    }

  def sourceWriteScopeRequest(binding: UUID, childWorkflow: UUID, parentRecallWorkflow: UUID,
      parentP2Operation: UUID, write: UUID, carrierIdentity: SourceWriteCarrierIdentityData,
      writeGeneration: Long, units: Vector[SourceWriteUnitStamp],
      obligations: Vector[SourceWriteObligationStamp], root: MySqlFormat3ReadBudget): DecodedSourceWriteScopeRequest =
    require(obligations != null && obligations.forall(item => item != null && item.key != null),
      "complete source-write obligation vector required")
    requireSourceWriteScopeData(binding, childWorkflow, write, carrierIdentity, writeGeneration,
      obligations.map(_.key), parentRecallWorkflow, parentP2Operation, units, obligations, root)
    val canonical = encode("FP_SOURCE_WRITE_SCOPE_BE_V1", root) { output =>
      writeSourceWriteScope(output, binding, childWorkflow, write, carrierIdentity, writeGeneration,
        parentRecallWorkflow, parentP2Operation, units, obligations, root)
    }
    new DecodedSourceWriteScopeRequest(binding, childWorkflow, parentRecallWorkflow, parentP2Operation,
      write, carrierIdentity, writeGeneration, units, obligations, canonical)

  private val SourceReservationTable = "fp_recovery_reservation"

  private def sourceChildReservationShape(inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget): MySqlFormat3RelationShape =
    requireCanonicalRoot(root)
    require(inputs != null && inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      (inputs.sourceLimitProfile eq root.profile),
      "exact original schema5 Full26 inputs/profile required for SOURCE R0")
    root.requireScriptDataInputs(inputs)
    val shape = inputs.forTable(SourceReservationTable)
    require(shape != null && shape.table == SourceReservationTable && shape.columns.size == 11,
      "SOURCE R0 FullReservationRow must use the actual closed eleven-column Full26 shape")
    shape

  private def requireSourceChildBinaryOwner(value: MySqlFormat3Binary,
      root: MySqlFormat3ReadBudget, label: String): Unit =
    val owner = if value == null then null else value.storageBudget
    require(owner != null && !owner.isClosed && owner.sharesAllocationLedger(root) &&
      ((owner eq root) || owner.isCanonicalChildOf(root)),
      s"$label must be precharged to the same original Full26 root")

  private def requireSourceDataBudget(root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): Unit =
    requireCanonicalRoot(root)
    require(dataBudget != null && !dataBudget.isClosed && (dataBudget.profile eq root.profile) &&
      dataBudget.sharesAllocationLedger(root) && ((dataBudget eq root) || dataBudget.isCanonicalChildOf(root)),
      "SOURCE DATA allocation must remain on the same original root or one direct child")

  private def requireSourcePlanDataBudget(root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): Unit =
    requireSourceDataBudget(root, dataBudget)
    require(dataBudget ne root,
      "H3 SOURCE plan DATA requires its exact issuer-validated canonical data child")

  /** Parent-P2 recursion may reuse only the actual root comparison child or its exact DATA child. */
  private[mysql] def requireHistoryComparisonOwner(root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, activeComparisonBudget: MySqlFormat3ReadBudget): Unit =
    requireSourceDataBudget(root, dataBudget)
    val exactOwner =
      if dataBudget eq root then activeComparisonBudget != null && activeComparisonBudget.isCanonicalComparisonChildOf(root)
      else activeComparisonBudget eq dataBudget
    require(activeComparisonBudget != null && !activeComparisonBudget.isClosed &&
      (activeComparisonBudget.profile eq root.profile) && activeComparisonBudget.sharesAllocationLedger(root) &&
      exactOwner && (MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root) eq activeComparisonBudget),
      "historical parent P2 must reuse its exact active root comparison or direct DATA owner")

  private def sourceResourceKeyMatches(key: MySqlRecoveryResourceKey,
      actual: MySqlFormat3Binary): Boolean =
    if key == null || actual == null || key.canonicalLength != actual.length then false
    else
      var position = 0
      var matches = true
      val sink = new java.io.OutputStream:
        override def write(value: Int): Unit =
          if position >= actual.length || actual.byteAt(position) != value.toByte then matches = false
          position = Math.addExact(position, 1)
        override def write(values: Array[Byte], offset: Int, length: Int): Unit =
          if values == null || offset < 0 || length < 0 || offset > values.length - length then
            throw new IndexOutOfBoundsException("resource-key canonical comparison slice is invalid")
          var index = 0
          while index < length do
            write(values(offset + index).toInt)
            index += 1
      val output = new DataOutputStream(sink)
      key.writeCanonicalTo(output)
      output.flush()
      matches && position == actual.length

  private def writeSourceChildReservationRow(output: DataOutputStream,
      row: Vector[MySqlFormat3Cell], inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget): Unit =
    val shape = sourceChildReservationShape(inputs, root)
    require(shape.columns.size == 11 && row != null && row.size == shape.columns.size,
      "SOURCE R0 FullReservationRow must use the exact closed eleven-column shape")
    validateSourceChildReservationRowShape(row, shape, root)
    output.writeInt(shape.columns.size)
    shape.columns.foreach { column => text(output, column.name); text(output, column.sqlDeclaration) }
    row.foreach(cell(output, _))

  private def validateSourceChildReservationRowShape(row: Vector[MySqlFormat3Cell],
      shape: MySqlFormat3RelationShape, root: MySqlFormat3ReadBudget): Unit =
    require(shape != null && shape.table == SourceReservationTable && shape.columns.size == 11 &&
      row != null && row.size == 11 && row.forall(_ != null),
      "SOURCE R0 FullReservationRow must use the exact closed eleven-column shape")
    shape.columns.zip(row).foreach { (column, value) =>
      MySqlFormat3SnapshotValidation.validateCell(value, column, SourceReservationTable)
      value match
        case MySqlFormat3Cell.Binary(bytes) =>
          requireSourceChildBinaryOwner(bytes, root, s"SOURCE R0 ${column.name}")
        case _ => ()
    }

  private def validateSourceChildReservationRow(row: Vector[MySqlFormat3Cell],
      key: MySqlRecoveryResourceKey, binding: UUID, child: UUID, reserveOperation: UUID,
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget): Unit =
    val table = SourceReservationTable
    val shape = sourceChildReservationShape(inputs, root)
    require(shape.columns.size == 11 && row != null && row.size == shape.columns.size,
      "SOURCE R0 FullReservationRow must have its exact eleven-column shape")
    validateSourceChildReservationRowShape(row, shape, root)
    val unitReferenceMatches = key.sourceUnitUuid match
      case Some(unit) => MySqlModeOperationEffectCodec.uuidEquals(value(row, table, "resource_unit_uuid"), unit)
      case None => value(row, table, "resource_unit_uuid") == MySqlFormat3Cell.SqlNull
    require(MySqlModeOperationEffectCodec.uuidEquals(value(row, table, "binding_uuid"), binding) &&
      value(row, table, "resource_kind") == MySqlFormat3Cell.Text(key.resourceKind) &&
      (value(row, table, "resource_key") match
        case MySqlFormat3Cell.Binary(actual) => sourceResourceKeyMatches(key, actual)
        case _ => false) &&
      (value(row, table, "resource_key_hash") match
        case MySqlFormat3Cell.Binary(hash) => hash.equalsVector(key.keyHashSha256)
        case _ => false) &&
      value(row, table, "resource_pod_uuid") == MySqlFormat3Cell.SqlNull &&
      unitReferenceMatches &&
      value(row, table, "last_mutation_operation_uuid") == value(row, table, "operation_uuid"),
      "R0 request reservation before row differs from its complete full-key ownership tuple")
    val revision = value(row, table, "reservation_revision") match
      case MySqlFormat3Cell.SignedInteger(number) if number >= 0L => number
      case _ => throw new SQLDataException("R0 request reservation revision is absent or negative")
    val operation = uuidValue(value(row, table, "operation_uuid"), root, "R0 reservation predecessor")
    val workflow = uuidValue(value(row, table, "workflow_uuid"), root, "R0 reservation workflow")
    value(row, table, "reservation_state") match
      case MySqlFormat3Cell.Text("RELEASED") => ()
      case MySqlFormat3Cell.Text("HELD") =>
        require(workflow == child && operation == reserveOperation && revision == 0L,
          "same-child HELD R0 reservation is only valid as its exact revision-zero recovery")
      case _ => throw new SQLDataException("R0 request existing reservation must be RELEASED or exact-R0 HELD")

  private def uuidValue(cell: MySqlFormat3Cell, root: MySqlFormat3ReadBudget, label: String): UUID = cell match
    case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 =>
      val raw = bytes.copyBytes(root, label)
      val buffer = java.nio.ByteBuffer.wrap(raw)
      val id = new UUID(buffer.getLong(), buffer.getLong())
      require(MySqlNormalRecallRequest.nonZero(id), label + " must be nonzero")
      id
    case _ => throw new SQLDataException(label + " must be a 16-byte UUID")

  def sourceChildReserveResourceExpectation(resource: MySqlRecoveryResourceKey,
      before: Option[Vector[MySqlFormat3Cell]], inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget): SourceChildReserveResourceExpectation =
    val reservationShape = sourceChildReservationShape(inputs, root)
    require(resource != null && resource.exactProfile == root.profile &&
      Set("CARRIER", "SOURCE_UNIT").contains(resource.resourceKind) && before != null,
      "original-profile CARRIER/SOURCE_UNIT key and explicit full-row presence required")
    before.foreach(validateSourceChildReservationRowShape(_, reservationShape, root))
    root.reserveRetainedMemory(192L, "SOURCE R0 resource expectation DATA")
    new SourceChildReserveResourceExpectation(resource, before)

  /** H3 plan DATA ownership; root remains the Full26 authority for schema/row validation. */
  private[mysql] def sourceChildReserveResourceExpectation(resource: MySqlRecoveryResourceKey,
      before: Option[Vector[MySqlFormat3Cell]], inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): SourceChildReserveResourceExpectation =
    requireSourcePlanDataBudget(root, dataBudget)
    val reservationShape = sourceChildReservationShape(inputs, root)
    require(resource != null && resource.exactProfile == root.profile &&
      Set("CARRIER", "SOURCE_UNIT").contains(resource.resourceKind) && before != null,
      "original-profile CARRIER/SOURCE_UNIT key and explicit full-row presence required")
    before.foreach(validateSourceChildReservationRowShape(_, reservationShape, root))
    dataBudget.reserveRetainedMemory(192L, "SOURCE R0 resource expectation DATA")
    new SourceChildReserveResourceExpectation(resource, before)

  private def requireSourceChildReserveRequestData(binding: UUID, childWorkflow: UUID,
      parentRecallWorkflow: UUID, parentP2Operation: UUID, reserveOperation: UUID, plannedWrite: UUID,
      writeGeneration: Long, carrierIdentity: SourceWriteCarrierIdentityData,
      units: Vector[SourceWriteUnitStamp], obligations: Vector[SourceWriteObligationStamp],
      resources: Vector[SourceChildReserveResourceExpectation], inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget): Unit =
    requireSourceChildReserveRequestDataCore(binding, childWorkflow, parentRecallWorkflow,
      parentP2Operation, reserveOperation, plannedWrite, writeGeneration, carrierIdentity,
      units, obligations, resources, inputs, root, null)

  private def requireSourceChildReserveRequestData(binding: UUID, childWorkflow: UUID,
      parentRecallWorkflow: UUID, parentP2Operation: UUID, reserveOperation: UUID, plannedWrite: UUID,
      writeGeneration: Long, carrierIdentity: SourceWriteCarrierIdentityData,
      units: Vector[SourceWriteUnitStamp], obligations: Vector[SourceWriteObligationStamp],
      resources: Vector[SourceChildReserveResourceExpectation], inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): Unit =
    requireSourceDataBudget(root, dataBudget)
    val scratch = Math.addExact(4096L, Math.addExact(Math.multiplyExact(units.size.toLong, 256L),
      Math.addExact(Math.multiplyExact(obligations.size.toLong, 512L),
        Math.multiplyExact(resources.size.toLong, 512L))))
    withSourceChildReserveTemporary(dataBudget, scratch,
      "historical SOURCE R0 request validation candidates") {
      MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
        requireSourceChildReserveRequestDataCore(binding, childWorkflow, parentRecallWorkflow,
          parentP2Operation, reserveOperation, plannedWrite, writeGeneration, carrierIdentity,
          units, obligations, resources, inputs, root, dataBudget)
      }
    }

  private def sourceChildReserveRequestComparison[A](root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget)(body: => A): A =
    if dataBudget == null then
      var result: A = null.asInstanceOf[A]
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) { result = body }
      result
    else body

  private def requireSourceChildReserveRequestDataCore(binding: UUID, childWorkflow: UUID,
      parentRecallWorkflow: UUID, parentP2Operation: UUID, reserveOperation: UUID, plannedWrite: UUID,
      writeGeneration: Long, carrierIdentity: SourceWriteCarrierIdentityData,
      units: Vector[SourceWriteUnitStamp], obligations: Vector[SourceWriteObligationStamp],
      resources: Vector[SourceChildReserveResourceExpectation], inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, validationDataBudget: MySqlFormat3ReadBudget): Unit =
    sourceChildReservationShape(inputs, root)
    require(Vector(binding, childWorkflow, parentRecallWorkflow, parentP2Operation,
      reserveOperation, plannedWrite).forall(MySqlNormalRecallRequest.nonZero) &&
      writeGeneration >= 0L && carrierIdentity != null && carrierIdentity.resourceKey != null &&
      carrierIdentity.resourceKey.resourceKind == "CARRIER" &&
      carrierIdentity.resourceKey.exactProfile == root.profile && units != null && units.nonEmpty &&
      obligations != null && resources != null,
      "complete restricted SOURCE R0 identity, CARRIER and original Full26 root required")
    val totalItems = Math.addExact(Math.addExact(units.size.toLong, obligations.size.toLong), resources.size.toLong)
    require(totalItems <= root.profile.maxMembers(), "SOURCE R0 aggregate list items exceed the original member bound")
    var carrierIdentityMatches = false
    sourceChildReserveRequestComparison(root, validationDataBudget) {
      val comparisonBudget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
      comparisonBudget.reserveRetainedMemory(256L, "SOURCE R0 carrier-key comparison DATA")
      val expectedCarrier = MySqlRecoveryResourceKey.carrier(carrierIdentity.saveIdentity,
        carrierIdentity.loaderIdentity, carrierIdentity.regionIdentity, carrierIdentity.slotX,
        carrierIdentity.slotZ, root.profile)
      carrierIdentityMatches = carrierIdentity.resourceKey.exactlyEquals(expectedCarrier)
    }
    require(carrierIdentityMatches,
      "SOURCE R0 full CARRIER key differs from its complete identity fields")
    var previousUnit: SourceWriteUnitStamp = null
    units.foreach { item =>
      require(item != null && MySqlNormalRecallRequest.nonZero(item.unit) && item.unitRevision >= 0L &&
        item.epoch >= 0L && MySqlNormalRecallRequest.nonZero(item.currentForest) &&
        (previousUnit == null || compareSourceWriteUnit(previousUnit, item) < 0),
        "SOURCE R0 UnitExpectations must be complete and unsigned-UUID sorted")
      previousUnit = item
    }
    val unitIds = units.map(_.unit).toSet
    require(unitIds.size == units.size, "SOURCE R0 UnitExpectation repeats a unit")
    var openUnitIds = Set.empty[UUID]
    var previousObligation: SourceWriteObligationStamp = null
    obligations.foreach { item =>
      require(item != null && item.key != null && item.key.binding == binding &&
        item.key.carrierKeyHash == carrierIdentity.resourceKey.keyHashSha256 &&
        item.key.carrierKey != null && (item.key.carrierKey == carrierIdentity.canonicalBytes) &&
        MySqlNormalRecallRequest.nonZero(item.key.unit) && MySqlNormalRecallRequest.nonZero(item.key.forest) &&
        Set("OLD_HOLD", "TARGET_STAGED", "CURRENT_HOLD", "OLD_RETIRE", "UNDO").contains(item.key.purpose) &&
        item.key.revision >= 0L && (item.beforeState == "OPEN" || item.beforeState == "DISCHARGED") &&
        (previousObligation == null || compareSourceWriteObligation(previousObligation, item) < 0),
        "SOURCE R0 must retain every complete carrier obligation expectation in full-PK order")
      requireSourceChildBinaryOwner(item.key.carrierKey, root, "SOURCE R0 obligation full CARRIER key")
      if item.beforeState == "OPEN" then openUnitIds += item.key.unit
      previousObligation = item
    }
    require(openUnitIds.nonEmpty && unitIds == openUnitIds,
      "SOURCE R0 UnitExpectations must equal the nonempty OPEN obligation unit set")
    require(resources.forall(entry => entry != null && entry.resource != null && entry.before != null),
      "SOURCE R0 resource expectation entries and keys must be present")
    var resourceKeysMatch = false
    sourceChildReserveRequestComparison(root, validationDataBudget) {
      val comparisonBudget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
      val expectedCount = Math.addExact(units.size.toLong, 1L)
      comparisonBudget.reserveRetainedMemory(Math.addExact(512L, Math.multiplyExact(expectedCount, 192L)),
        "SOURCE R0 lock-key comparison vectors")
      val actualKeys = resources.map(_.resource)
      val expectedKeys = MySqlRecoveryResourceKey.orderForLocks(
        Vector(carrierIdentity.resourceKey) ++ units.map(unit => MySqlRecoveryResourceKey.sourceUnit(unit.unit, root.profile)))
      val sortedKeys = MySqlRecoveryResourceKey.orderForLocks(actualKeys)
      resourceKeysMatch = sortedKeys.size == resources.size && expectedKeys.size == resources.size &&
        sortedKeys.zip(expectedKeys).forall((actual, expected) => actual.exactlyEquals(expected)) &&
        resources.zip(sortedKeys).forall((entry, key) => entry.resource.exactlyEquals(key))
    }
    require(resourceKeysMatch,
      "SOURCE R0 ResourceExpectations must be the exact unique CARRIER+SOURCE_UNIT full-key lock set")
    resources.foreach { entry =>
      require(entry.resource.exactProfile == root.profile &&
        Set("CARRIER", "SOURCE_UNIT").contains(entry.resource.resourceKind),
        "SOURCE R0 resources cannot contain POD or another resource kind")
      entry.before.foreach(validateSourceChildReservationRow(_, entry.resource, binding, childWorkflow,
        reserveOperation, inputs, root))
    }

  private def writeSourceChildReserveRequestData(output: DataOutputStream,
      binding: UUID, childWorkflow: UUID, parentRecallWorkflow: UUID, parentP2Operation: UUID,
      reserveOperation: UUID, plannedWrite: UUID, writeGeneration: Long,
      carrierIdentity: SourceWriteCarrierIdentityData, units: Vector[SourceWriteUnitStamp],
      obligations: Vector[SourceWriteObligationStamp], resources: Vector[SourceChildReserveResourceExpectation],
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget,
      itemScratchBudget: MySqlFormat3ReadBudget): Unit =
    uuid(output, binding); uuid(output, childWorkflow)
    uuid(output, parentRecallWorkflow); uuid(output, parentP2Operation)
    uuid(output, reserveOperation); uuid(output, plannedWrite)
    output.writeLong(writeGeneration)
    text(output, carrierIdentity.saveIdentity); text(output, carrierIdentity.loaderIdentity)
    text(output, carrierIdentity.regionIdentity)
    output.writeInt(carrierIdentity.slotX); output.writeInt(carrierIdentity.slotZ)
    output.writeInt(carrierIdentity.canonicalBytes.length)
    carrierIdentity.canonicalBytes.writeTo(output)
    output.writeInt(units.size)
    units.foreach { item => writeSourceChildItem(output, root, itemScratchBudget) { itemOutput =>
      uuid(itemOutput, item.unit); itemOutput.writeLong(item.unitRevision); itemOutput.writeLong(item.epoch)
      uuid(itemOutput, item.currentForest)
    }}
    output.writeInt(obligations.size)
    obligations.foreach { item => writeSourceChildItem(output, root, itemScratchBudget) { itemOutput =>
      uuid(itemOutput, item.key.binding); uuid(itemOutput, item.key.unit); uuid(itemOutput, item.key.forest)
      hash(itemOutput, item.key.carrierKeyHash)
      itemOutput.writeInt(item.key.carrierKey.length); item.key.carrierKey.writeTo(itemOutput)
      text(itemOutput, item.key.purpose); itemOutput.writeLong(item.key.revision); text(itemOutput, item.beforeState)
    }}
    output.writeInt(resources.size)
    resources.foreach { entry =>
      writeSourceChildItem(output, root, itemScratchBudget) { itemOutput =>
        writeSourceChildResourceItem(itemOutput, entry, inputs, root, itemScratchBudget)
      }
    }

  private def writeSourceChildItem(output: DataOutputStream, root: MySqlFormat3ReadBudget,
      itemScratchBudget: MySqlFormat3ReadBudget)
      (write: DataOutputStream => Unit): Unit =
    requireSourceDataBudget(root, itemScratchBudget)
    val itemLength = measure(itemScratchBudget)(write)
    output.writeInt(Math.toIntExact(itemLength))
    write(output)

  private def writeSourceChildResourceItem(output: DataOutputStream,
      entry: SourceChildReserveResourceExpectation, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, itemScratchBudget: MySqlFormat3ReadBudget): Unit =
    text(output, entry.resource.resourceKind)
    output.writeInt(entry.resource.canonicalLength)
    entry.resource.writeCanonicalTo(output)
    output.writeByte(if entry.before.isDefined then 1 else 0)
    entry.before.foreach { row => writeSourceChildItem(output, root, itemScratchBudget)(rowOutput =>
      writeSourceChildReservationRow(rowOutput, row, inputs, root))
    }

  private def buildSourceChildReserveRequestData(binding: UUID, childWorkflow: UUID,
      parentRecallWorkflow: UUID, parentP2Operation: UUID, reserveOperation: UUID, plannedWrite: UUID,
      writeGeneration: Long, carrierIdentity: SourceWriteCarrierIdentityData,
      units: Vector[SourceWriteUnitStamp], obligations: Vector[SourceWriteObligationStamp],
      resources: Vector[SourceChildReserveResourceExpectation], inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      itemScratchBudget: MySqlFormat3ReadBudget,
      chargeWrapperMemory: Boolean,
      validationDataBudget: MySqlFormat3ReadBudget = null): SourceChildReserveRequestData =
    requireSourceDataBudget(root, dataBudget)
    requireSourceDataBudget(root, itemScratchBudget)
    if validationDataBudget == null then
      requireSourceChildReserveRequestData(binding, childWorkflow, parentRecallWorkflow, parentP2Operation,
        reserveOperation, plannedWrite, writeGeneration, carrierIdentity, units, obligations, resources, inputs, root)
    else
      requireSourceChildReserveRequestData(binding, childWorkflow, parentRecallWorkflow, parentP2Operation,
        reserveOperation, plannedWrite, writeGeneration, carrierIdentity, units, obligations, resources,
        inputs, root, validationDataBudget)
    if chargeWrapperMemory then
      dataBudget.reserveRetainedMemory(Math.addExact(2048L, Math.addExact(
        Math.multiplyExact(units.size.toLong, 192L), Math.addExact(Math.multiplyExact(obligations.size.toLong, 384L),
          Math.multiplyExact(resources.size.toLong, 2048L)))), "SOURCE R0 typed request DATA")
    val canonical = MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      encode(SourceChildReserveRequestDomain, root) { output =>
        writeSourceChildReserveRequestData(output, binding, childWorkflow, parentRecallWorkflow,
          parentP2Operation, reserveOperation, plannedWrite, writeGeneration, carrierIdentity,
          units, obligations, resources, inputs, root, itemScratchBudget)
      }
    }
    new SourceChildReserveRequestData(binding, childWorkflow, parentRecallWorkflow,
      parentP2Operation, reserveOperation, plannedWrite, writeGeneration, carrierIdentity, units,
      obligations, resources, canonical)

  private[mysql] def sourceChildReserveRequestDataRetainedCharge(unitCount: Int,
      obligationCount: Int, resourceCount: Int): Long =
    require(unitCount > 0 && obligationCount > 0 && resourceCount > 0,
      "nonempty SOURCE R0 request item counts required before allocation")
    Math.addExact(2048L, Math.addExact(
      Math.multiplyExact(unitCount.toLong, 192L), Math.addExact(
        Math.multiplyExact(obligationCount.toLong, 384L),
        Math.multiplyExact(resourceCount.toLong, 2048L))))

  /** Charge the exact H1 retained wrapper before constructing its unit/obligation/resource DATA. */
  private[mysql] def prechargeSourceChildReserveRequestData(root: MySqlFormat3ReadBudget,
      unitCount: Int, obligationCount: Int, resourceCount: Int): SourceChildReserveRequestDataPrecharge =
    requireCanonicalRoot(root)
    val totalItems = Math.addExact(Math.addExact(unitCount.toLong, obligationCount.toLong), resourceCount.toLong)
    require(totalItems <= root.profile.maxMembers(),
      "SOURCE R0 aggregate list items exceed the original member bound")
    val bytes = Math.addExact(sourceChildReserveRequestDataRetainedCharge(unitCount,
      obligationCount, resourceCount), 1024L)
    root.reserveRetainedMemory(bytes, "SOURCE R0 typed request and plan DATA BEFORE item allocation")
    try new SourceChildReserveRequestDataPrecharge(root, root, unitCount, obligationCount, resourceCount, bytes)
    catch
      case cause: Throwable =>
        try root.releaseRetainedMemory(bytes, "SOURCE R0 request precharge wrapper allocation failure")
        catch case cleanup: Throwable => if (cleanup ne cause) && !cause.getSuppressed.contains(cleanup) then cause.addSuppressed(cleanup)
        throw cause

  /** Same H1 request precharge, charged to the exact Fenced-issued H3 plan DATA child. */
  private[mysql] def prechargeSourceChildReserveRequestData(root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, unitCount: Int, obligationCount: Int,
      resourceCount: Int): SourceChildReserveRequestDataPrecharge =
    requireSourcePlanDataBudget(root, dataBudget)
    val totalItems = Math.addExact(Math.addExact(unitCount.toLong, obligationCount.toLong), resourceCount.toLong)
    require(totalItems <= root.profile.maxMembers(),
      "SOURCE R0 aggregate list items exceed the original member bound")
    val bytes = Math.addExact(sourceChildReserveRequestDataRetainedCharge(unitCount,
      obligationCount, resourceCount), 1024L)
    dataBudget.reserveRetainedMemory(bytes, "SOURCE R0 typed request and plan DATA BEFORE item allocation")
    try new SourceChildReserveRequestDataPrecharge(root, dataBudget, unitCount,
      obligationCount, resourceCount, bytes)
    catch
      case cause: Throwable =>
        try dataBudget.releaseRetainedMemory(bytes, "SOURCE R0 request precharge wrapper allocation failure")
        catch case cleanup: Throwable => if (cleanup ne cause) && !cause.getSuppressed.contains(cleanup) then cause.addSuppressed(cleanup)
        throw cause

  /** Marks the exact precharge as request DATA; callers commit it only after transferring that request into a plan. */
  private[mysql] def sourceChildReserveRequestDataPrecharged(binding: UUID, childWorkflow: UUID,
      parentRecallWorkflow: UUID, parentP2Operation: UUID, reserveOperation: UUID, plannedWrite: UUID,
      writeGeneration: Long, carrierIdentity: SourceWriteCarrierIdentityData,
      units: Vector[SourceWriteUnitStamp], obligations: Vector[SourceWriteObligationStamp],
      resources: Vector[SourceChildReserveResourceExpectation], inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, precharge: SourceChildReserveRequestDataPrecharge):
      SourceChildReserveRequestData =
    require(precharge != null && units != null && obligations != null && resources != null,
      "precharged SOURCE R0 request DATA and complete item vectors required")
    precharge.requireAvailable(root, root, units.size, obligations.size, resources.size)
    try
      val request = buildSourceChildReserveRequestData(binding, childWorkflow, parentRecallWorkflow,
        parentP2Operation, reserveOperation, plannedWrite, writeGeneration, carrierIdentity,
        units, obligations, resources, inputs, root,
        MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root), root, chargeWrapperMemory = false)
      precharge.markRequestBuilt(root, root, units.size, obligations.size, resources.size)
      request
    catch
      case cause: Throwable =>
        try precharge.releaseIfUncommitted()
        catch case cleanup: Throwable => if (cleanup ne cause) && !cause.getSuppressed.contains(cleanup) then cause.addSuppressed(cleanup)
        throw cause

  /** H3 plan DATA overload; exact original root validates while the holder-issued child owns all request bytes. */
  private[mysql] def sourceChildReserveRequestDataPrecharged(binding: UUID, childWorkflow: UUID,
      parentRecallWorkflow: UUID, parentP2Operation: UUID, reserveOperation: UUID, plannedWrite: UUID,
      writeGeneration: Long, carrierIdentity: SourceWriteCarrierIdentityData,
      units: Vector[SourceWriteUnitStamp], obligations: Vector[SourceWriteObligationStamp],
      resources: Vector[SourceChildReserveResourceExpectation], inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      precharge: SourceChildReserveRequestDataPrecharge): SourceChildReserveRequestData =
    requireSourcePlanDataBudget(root, dataBudget)
    require(precharge != null && units != null && obligations != null && resources != null,
      "precharged SOURCE R0 request DATA and complete item vectors required")
    precharge.requireAvailable(root, dataBudget, units.size, obligations.size, resources.size)
    try
      val request = buildSourceChildReserveRequestData(binding, childWorkflow, parentRecallWorkflow,
        parentP2Operation, reserveOperation, plannedWrite, writeGeneration, carrierIdentity,
        units, obligations, resources, inputs, root, dataBudget, dataBudget, chargeWrapperMemory = false)
      precharge.markRequestBuilt(root, dataBudget, units.size, obligations.size, resources.size)
      request
    catch
      case cause: Throwable =>
        try precharge.releaseIfUncommitted()
        catch case cleanup: Throwable => if (cleanup ne cause) && !cause.getSuppressed.contains(cleanup) then cause.addSuppressed(cleanup)
        throw cause

  def sourceChildReserveRequestData(binding: UUID, childWorkflow: UUID,
      parentRecallWorkflow: UUID, parentP2Operation: UUID, reserveOperation: UUID, plannedWrite: UUID,
      writeGeneration: Long, carrierIdentity: SourceWriteCarrierIdentityData,
      units: Vector[SourceWriteUnitStamp], obligations: Vector[SourceWriteObligationStamp],
      resources: Vector[SourceChildReserveResourceExpectation], inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget):
      SourceChildReserveRequestData =
    buildSourceChildReserveRequestData(binding, childWorkflow, parentRecallWorkflow, parentP2Operation,
      reserveOperation, plannedWrite, writeGeneration, carrierIdentity, units, obligations, resources,
      inputs, root, MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root), root,
      chargeWrapperMemory = true)

  def encodeSourceChildReserveRequestData(request: SourceChildReserveRequestData,
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    require(request != null, "typed SOURCE R0 request DATA required")
    requireSourceChildReserveRequestData(request.binding, request.childWorkflow, request.parentRecallWorkflow,
      request.parentP2Operation, request.reserveOperation, request.plannedWrite, request.writeGeneration,
      request.carrierIdentity, request.units, request.obligations, request.resources, inputs, root)
    require(request.canonical != null && request.canonical.encoding == SourceChildReserveRequestDomain,
      "typed SOURCE R0 request canonical bytes require their exact domain")
    requireSourceChildBinaryOwner(request.canonical.borrowedBinary, root, "SOURCE R0 canonical request bytes")
    require(request.canonical.matchesWrittenBytes { output =>
        token(output, SourceChildReserveRequestDomain)
        writeSourceChildReserveRequestData(output, request.binding, request.childWorkflow,
          request.parentRecallWorkflow, request.parentP2Operation, request.reserveOperation,
          request.plannedWrite, request.writeGeneration, request.carrierIdentity, request.units,
          request.obligations, request.resources, inputs, root, root)
      }, "typed SOURCE R0 request DATA differs from its exact canonical bytes")
    request.canonical

  def decodeSourceChildReserveRequestData(input: MySqlFormat3Binary, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget): SourceChildReserveRequestData =
    decodeSourceChildReserveRequestDataInternal(input, inputs, root, root, explicitDataOwner = false)

  /** Historical H1 overload: every retained request/frame copy belongs to the exact same-ledger plan child. */
  private[mysql] def decodeSourceChildReserveRequestData(input: MySqlFormat3Binary,
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): SourceChildReserveRequestData =
    decodeSourceChildReserveRequestDataInternal(input, inputs, root, dataBudget, explicitDataOwner = true)

  private def decodeSourceChildReserveRequestDataInternal(input: MySqlFormat3Binary,
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, explicitDataOwner: Boolean): SourceChildReserveRequestData =
    requireSourceDataBudget(root, dataBudget)
    require(input != null && root != null && !root.isClosed && !root.isScopedReadBudget,
      "SOURCE R0 request bytes and original open root required")
    sourceChildReservationShape(inputs, root)
    requireSourceChildBinaryOwner(input, root, "SOURCE R0 request bytes")
    decodeData(input, root, dataBudget, Some(inputs)) { reader =>
      reader.domain(SourceChildReserveRequestDomain)
      dataBudget.reserveRetainedMemory(2048L, "decoded SOURCE R0 request wrapper")
      val binding = reader.uuid(); val childWorkflow = reader.uuid()
      val parentRecallWorkflow = reader.uuid(); val parentP2Operation = reader.uuid()
      val reserveOperation = reader.uuid(); val plannedWrite = reader.uuid()
      val writeGeneration = reader.int64()
      val saveIdentity = reader.textData(); val loaderIdentity = reader.textData(); val regionIdentity = reader.textData()
      val slotX = reader.int32(); val slotZ = reader.int32()
      val carrierFrame = reader.dataFrame()
      val carrierKey = MySqlRecoveryResourceKey.carrier(saveIdentity, loaderIdentity, regionIdentity,
        slotX, slotZ, root.profile)
      val carrierIdentity = if explicitDataOwner then
        sourceWriteCarrierIdentityForHistoricalData(carrierKey, saveIdentity, loaderIdentity, regionIdentity,
          slotX, slotZ, root, dataBudget)
      else sourceWriteCarrierIdentity(carrierKey, saveIdentity, loaderIdentity, regionIdentity,
        slotX, slotZ, root)
      carrierFrame.requireExactBinary(carrierIdentity.canonicalBytes)

      val unitCount = reader.boundedItemCount("SOURCE R0 UnitExpectations", minimum = 1)
      dataBudget.reserveRetainedMemory(Math.multiplyExact(unitCount.toLong, 192L),
        "decoded SOURCE R0 UnitExpectation vector")
      val units = Vector.newBuilder[SourceWriteUnitStamp]
      var previousUnit: SourceWriteUnitStamp = null
      var unitIndex = 0
      while unitIndex < unitCount do
        val itemReader = reader.dataFrame()
        val item = new SourceWriteUnitStamp(itemReader.uuid(), itemReader.int64(),
          itemReader.int64(), itemReader.uuid())
        itemReader.finish()
        require(item.unitRevision >= 0L && item.epoch >= 0L &&
          (previousUnit == null || compareSourceWriteUnit(previousUnit, item) < 0),
          "decoded SOURCE R0 UnitExpectations are invalid, duplicated or unsorted")
        units += item
        previousUnit = item
        unitIndex += 1
      val unitValues = units.result()

      val obligationCount = reader.boundedItemCount("SOURCE R0 ObligationExpectations", minimum = 0)
      dataBudget.reserveRetainedMemory(Math.multiplyExact(obligationCount.toLong, 384L),
        "decoded SOURCE R0 ObligationExpectation vector")
      val obligations = Vector.newBuilder[SourceWriteObligationStamp]
      var previousObligation: SourceWriteObligationStamp = null
      var obligationIndex = 0
      while obligationIndex < obligationCount do
        val itemReader = reader.dataFrame()
        val itemBinding = itemReader.uuid(); val unit = itemReader.uuid(); val forest = itemReader.uuid()
        val keyHash = Vector.fill(32)(itemReader.octet().toByte)
        val keyFrame = itemReader.dataFrame()
        keyFrame.requireExactBinary(carrierIdentity.canonicalBytes)
        val purpose = itemReader.textData(); val revision = itemReader.int64(); val beforeState = itemReader.textData()
        itemReader.finish()
        val key = new SourceObligationRevision(itemBinding, unit, forest, keyHash,
          carrierIdentity.canonicalBytes, purpose, revision)
        val item = new SourceWriteObligationStamp(key, beforeState)
        require(itemBinding == binding && keyHash == carrierIdentity.resourceKey.keyHashSha256 &&
          (previousObligation == null || compareSourceWriteObligation(previousObligation, item) < 0),
          "decoded SOURCE R0 ObligationExpectations differ from binding/carrier or are not full-PK sorted")
        obligations += item
        previousObligation = item
        obligationIndex += 1
      val obligationValues = obligations.result()

      val expectedKeyCount = Math.addExact(unitValues.size.toLong, 1L)
      if explicitDataOwner then dataBudget.reserveRetainedMemory(
        Math.addExact(512L, Math.multiplyExact(expectedKeyCount, 192L)),
        "decoded SOURCE R0 expected resource-key ordering DATA before allocation")
      val expectedKeys = MySqlRecoveryResourceKey.orderForLocks(
        Vector(carrierIdentity.resourceKey) ++ unitValues.map(unit => MySqlRecoveryResourceKey.sourceUnit(unit.unit, root.profile)))
      val resourceCount = reader.boundedItemCount("SOURCE R0 ResourceExpectations", minimum = expectedKeys.size)
      require(resourceCount == expectedKeys.size,
        "decoded SOURCE R0 resources must contain exactly one CARRIER and all SOURCE_UNIT full keys")
      dataBudget.reserveRetainedMemory(Math.multiplyExact(resourceCount.toLong, 2048L),
        "decoded SOURCE R0 ResourceExpectation vector")
      val resources = Vector.newBuilder[SourceChildReserveResourceExpectation]
      var resourceIndex = 0
      while resourceIndex < resourceCount do
        val itemReader = reader.dataFrame()
        val resourceKind = itemReader.textData()
        val resource = expectedKeys(resourceIndex)
        require(resourceKind == resource.resourceKind,
          "decoded SOURCE R0 resource kind differs from original lock order")
        val keyFrame = itemReader.dataFrame()
        keyFrame.requireExactResourceKey(resource, "decoded SOURCE R0 full resource key")
        val before = itemReader.octet() match
          case 0 => None
          case 1 => Some(itemReader.dataFrame().sourceChildReservationRow())
          case _ => throw new SQLDataException("SOURCE R0 before-presence flag must be exactly zero or one")
        itemReader.finish()
        resources += (if explicitDataOwner then
          sourceChildReserveResourceExpectationForHistoricalData(resource, before, inputs, root, dataBudget)
        else new SourceChildReserveResourceExpectation(resource, before))
        resourceIndex += 1
      val resourceValues = resources.result()
      reader.finish()

      val decoded = buildSourceChildReserveRequestData(binding, childWorkflow, parentRecallWorkflow,
        parentP2Operation, reserveOperation, plannedWrite, writeGeneration, carrierIdentity, unitValues,
        obligationValues, resourceValues, inputs, root, dataBudget, dataBudget,
        chargeWrapperMemory = false, validationDataBudget = if explicitDataOwner then dataBudget else null)
      if explicitDataOwner then
        require(decoded.canonical.borrowedBinary.storageBudget eq dataBudget,
          "historical SOURCE R0 request canonical bytes must remain in the explicit DATA owner")
      def requireExactRequestBytes(): Unit =
        require(decoded.canonical.exactlyMatchesCell(MySqlFormat3Cell.Binary(input)),
          "noncanonical SOURCE R0 request DATA")
      if explicitDataOwner then MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
        requireExactRequestBytes()
      }
      else MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        requireExactRequestBytes()
      }
      decoded
    }

  private def requireSourceChildReserveScopeData(contextIdentity: MySqlRepositoryOperationContextIdentity,
      request: SourceChildReserveRequestData, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, requirePlanDataChild: Boolean = true): Unit =
    if requirePlanDataChild then requireSourcePlanDataBudget(root, dataBudget)
    else requireSourceDataBudget(root, dataBudget)
    require(inputs != null && inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      (inputs.sourceLimitProfile eq root.profile),
      "H1 scope requires its exact original Full26 inputs and root profile")
    root.requireScriptDataInputs(inputs)
    require(contextIdentity != null && request != null && request.canonical != null &&
      request.canonical.encoding == SourceChildReserveRequestDomain &&
      MySqlNormalRecallRequest.nonZero(request.binding) && MySqlNormalRecallRequest.nonZero(request.childWorkflow) &&
      MySqlNormalRecallRequest.nonZero(request.reserveOperation) && MySqlNormalRecallRequest.nonZero(request.plannedWrite) &&
      request.writeGeneration >= 0L && contextIdentity.tag != null &&
      contextIdentity.tag.operationId == request.reserveOperation &&
      contextIdentity.reserveOperationUuid == request.reserveOperation &&
      contextIdentity.workflowUuid == request.childWorkflow && contextIdentity.bindingUuid == request.binding &&
      contextIdentity.requestKind == "RUN" && contextIdentity.requestIdentitySha256 == request.canonical.sha256 &&
      contextIdentity.schemaFormat == inputs.schemaFormat && contextIdentity.schemaFormat == 5 &&
      contextIdentity.physicalFormat == 2 && contextIdentity.schema == inputs.schema &&
      contextIdentity.ddlSha256.equalsIgnoreCase(inputs.ddlSha256) &&
      (contextIdentity.exactLimitProfile eq root.profile) &&
      contextIdentity.limitProfileSha256 == root.profile.sha256().toVector,
      "H1 scope requires the exact actual RUN identity for this Full26 R0 request and original root")
    requireSourceChildBinaryOwner(request.canonical.borrowedBinary, root, "H1 complete R0 request scope")

  private def writeSourceChildReserveScopeBody(output: DataOutputStream,
      contextIdentity: MySqlRepositoryOperationContextIdentity, request: SourceChildReserveRequestData): Unit =
    contextIdentity.writeScopeIdentityFields(output)
    output.writeInt(request.canonical.length)
    request.canonical.writeTo(output)

  private def writeSourceChildReserveScopeCanonical(output: DataOutputStream,
      contextIdentity: MySqlRepositoryOperationContextIdentity, request: SourceChildReserveRequestData): Unit =
    token(output, SourceChildReserveScopeDomain)
    writeSourceChildReserveScopeBody(output, contextIdentity, request)

  private def newSourceChildReserveScopeData(contextIdentity: MySqlRepositoryOperationContextIdentity,
      request: SourceChildReserveRequestData, inputs: MySqlScriptSchemaInputs,
      canonical: MySqlRecoveryCanonicalBytes,
      dataBudget: MySqlFormat3ReadBudget): SourceChildReserveScopeData =
    dataBudget.reserveRetainedMemory(256L, "H1 exact source reserve scope DATA before allocation")
    try new SourceChildReserveScopeData(contextIdentity, request, inputs, canonical)
    catch
      case cause: Throwable =>
        try dataBudget.releaseRetainedMemory(256L, "H1 source reserve scope wrapper allocation failure")
        catch case cleanup: Throwable => if (cleanup ne cause) && !cause.getSuppressed.contains(cleanup) then cause.addSuppressed(cleanup)
        throw cause

  /** H1 scope 按原捕获身份helper及完整R0 request进行严格编码。 */
  private[mysql] def sourceChildReserveScopeData(contextIdentity: MySqlRepositoryOperationContextIdentity,
      request: SourceChildReserveRequestData, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): SourceChildReserveScopeData =
    requireSourceChildReserveScopeData(contextIdentity, request, inputs, root, dataBudget)
    val canonical = MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      encode(SourceChildReserveScopeDomain, root) { output =>
        writeSourceChildReserveScopeBody(output, contextIdentity, request)
      }
    }
    newSourceChildReserveScopeData(contextIdentity, request, inputs, canonical, dataBudget)

  private[mysql] def encodeSourceChildReserveScopeData(scope: SourceChildReserveScopeData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    sourceChildReserveScopeDataCanonical(scope, root, dataBudget, requirePlanDataChild = true)

  private def sourceChildReserveScopeDataCanonical(scope: SourceChildReserveScopeData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean): MySqlRecoveryCanonicalBytes =
    require(scope != null, "typed H1 source reserve scope DATA required")
    requireSourceChildReserveScopeData(scope.contextIdentity, scope.request, scope.inputs,
      root, dataBudget, requirePlanDataChild)
    require(scope.canonical != null && scope.canonical.encoding == SourceChildReserveScopeDomain &&
      scope.canonical.matchesWrittenBytes(output => writeSourceChildReserveScopeCanonical(output,
        scope.contextIdentity, scope.request)), "typed H1 source reserve scope differs from exact identity/request bytes")
    scope.canonical

  private def requireSourceReserveScopeIdentityBytes(reader: DataReader,
      contextIdentity: MySqlRepositoryOperationContextIdentity): Unit =
    var matches = true
    val sink = new java.io.OutputStream:
      override def write(value: Int): Unit =
        if reader.octet() != (value & 0xff) then matches = false
      override def write(values: Array[Byte], offset: Int, length: Int): Unit =
        if values == null || offset < 0 || length < 0 || offset > values.length - length then
          throw new IndexOutOfBoundsException("H1 scope identity comparison slice is invalid")
        var index = 0
        while index < length do
          write(values(offset + index).toInt)
          index += 1
    val output = new DataOutputStream(sink)
    contextIdentity.writeScopeIdentityFields(output)
    output.flush()
    require(matches, "H1 scope fields differ from the exact captured operation-context identity")

  /** 解码绑定调用方传入的确切捕获身份与request，不重建会话身份。 */
  private[mysql] def decodeSourceChildReserveScopeData(input: MySqlFormat3Binary,
      contextIdentity: MySqlRepositoryOperationContextIdentity, request: SourceChildReserveRequestData,
      inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): SourceChildReserveScopeData =
    requireSourceChildReserveScopeData(contextIdentity, request, inputs, root, dataBudget)
    requireSourceChildBinaryOwner(input, root, "H1 source reserve scope input")
    decodeData(input, root, dataBudget, Some(inputs)) { reader =>
      reader.domain(SourceChildReserveScopeDomain)
      requireSourceReserveScopeIdentityBytes(reader, contextIdentity)
      val requestFrame = reader.dataFrame()
      requestFrame.requireExactBinary(request.canonical.borrowedBinary)
      reader.finish()
    }
    val canonical = MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      encode(SourceChildReserveScopeDomain, root) { output =>
        writeSourceChildReserveScopeBody(output, contextIdentity, request)
      }
    }
    MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      require(canonical.exactlyMatchesCell(MySqlFormat3Cell.Binary(input)),
        "noncanonical H1 source reserve scope DATA")
    }
    newSourceChildReserveScopeData(contextIdentity, request, inputs, canonical, dataBudget)

  /** Parse the complete historical H1 scope envelope, then rebuild only a DATA-private identity for exact canonical comparison. */
  private[mysql] def decodeHistoricalSourceChildReserveScope(input: MySqlFormat3Binary,
      request: SourceChildReserveRequestData, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): HistoricalSourceChildReserveScopeData =
    requireSourceDataBudget(root, dataBudget)
    require(input != null && isHistoricalSourceChildReserveScope(input) && request != null &&
      inputs != null && inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      (inputs.sourceLimitProfile eq root.profile),
      "exact historical H1 scope envelope, typed request, Full26 inputs and original root required")
    root.requireScriptDataInputs(inputs)
    require(request.canonical != null && request.canonical.encoding == SourceChildReserveRequestDomain &&
      (request.canonical.borrowedBinary.storageBudget eq dataBudget),
      "historical H1 scope request must retain its exact explicitly-owned DATA bytes")
    val parsed = decodeData(input, root, dataBudget, Some(inputs)) { reader =>
      reader.domain(SourceChildReserveScopeDomain)
      val common = reader.scopeDataPrefix(5)
      val requestFrame = reader.dataFrame()
      val embeddedRequest = requestFrame.copyAllToBudget()
      requestFrame.finish()
      reader.finish()
      (common, embeddedRequest)
    }
    val decodedRequest = decodeSourceChildReserveRequestData(parsed._2, inputs, root, dataBudget)
    require(decodedRequest.binding == request.binding && decodedRequest.childWorkflow == request.childWorkflow &&
      decodedRequest.reserveOperation == request.reserveOperation &&
      decodedRequest.canonical.borrowedBinary == request.canonical.borrowedBinary,
      "historical H1 scope embedded request differs from the exact O request identity")
    val common = parsed._1
    val stable = common.stableSession
    require(common.tag.operationId == request.reserveOperation && common.workflow == request.childWorkflow &&
      common.reserve == request.reserveOperation && common.binding == request.binding &&
      stable.schemaFormat == 5 && stable.physicalFormat == 2 && common.schema == inputs.schema &&
      stable.ddl.equalsIgnoreCase(inputs.ddlSha256) &&
      stable.configuration.length == 32 && stable.provenance.length == 32,
      "historical H1 common scope identity differs from exact Full26/R0 request fields")
    dataBudget.reserveRetainedMemory(512L, "historical H1 DATA-only operation identity before allocation")
    val identity = new MySqlRepositoryOperationContextIdentity(common.tag, common.workflow,
      common.reserve, stable.barrier, common.connectionId, stable.namespace, common.binding,
      common.epoch, common.lease, common.server, stable.serverVersion, common.schema,
      stable.effectiveUser, stable.provisioning, stable.schemaFormat, stable.physicalFormat,
      stable.ddl, stable.manifest, stable.configuration.copyBytes(dataBudget,
        "historical H1 configuration identity").toVector, stable.provenanceKind,
      stable.provenance.copyBytes(dataBudget, "historical H1 provenance identity").toVector,
      root.profile.sha256().toVector, "RUN", request.canonical.sha256, root.profile)
    val typed = decodeHistoricalScopeData(input, identity, request, inputs, root, dataBudget)
    val envelopeBytes = input.copyBytes(dataBudget, "historical H1 complete scope envelope")
    val envelope = MySqlFormat3Binary.takeOwned(envelopeBytes, dataBudget)
    dataBudget.reserveRetainedMemory(384L, "historical H1 scope DATA wrapper before allocation")
    new HistoricalSourceChildReserveScopeData(envelope, request, common, inputs, typed, root, dataBudget)

  private def decodeHistoricalScopeData(input: MySqlFormat3Binary,
      identity: MySqlRepositoryOperationContextIdentity, request: SourceChildReserveRequestData,
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): SourceChildReserveScopeData =
    requireSourceChildReserveScopeData(identity, request, inputs, root, dataBudget,
      requirePlanDataChild = false)
    decodeData(input, root, dataBudget, Some(inputs)) { reader =>
      reader.domain(SourceChildReserveScopeDomain)
      requireSourceReserveScopeIdentityBytes(reader, identity)
      val requestFrame = reader.dataFrame()
      requestFrame.requireExactBinary(request.canonical.borrowedBinary)
      reader.finish()
    }
    val canonical = MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      encode(SourceChildReserveScopeDomain, root) { output =>
        writeSourceChildReserveScopeBody(output, identity, request)
      }
    }
    MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      require(canonical.exactlyMatchesCell(MySqlFormat3Cell.Binary(input)),
        "noncanonical or trailing historical H1 scope DATA")
    }
    newSourceChildReserveScopeData(identity, request, inputs, canonical, dataBudget)

  private def requireSourceChildReserveDerivedInputs(inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): Unit =
    if requirePlanDataChild then requireSourcePlanDataBudget(root, dataBudget)
    else requireSourceDataBudget(root, dataBudget)
    require(inputs != null && inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      (inputs.sourceLimitProfile eq root.profile),
      "H1 derived DATA requires exact original Full26 inputs and root profile")
    root.requireScriptDataInputs(inputs)

  private def requireSourceChildReserveRequestForDerived(request: SourceChildReserveRequestData,
      root: MySqlFormat3ReadBudget): Unit =
    require(request != null && request.canonical != null &&
      request.canonical.encoding == SourceChildReserveRequestDomain &&
      MySqlNormalRecallRequest.nonZero(request.binding) && MySqlNormalRecallRequest.nonZero(request.childWorkflow) &&
      MySqlNormalRecallRequest.nonZero(request.reserveOperation) && MySqlNormalRecallRequest.nonZero(request.plannedWrite) &&
      request.writeGeneration >= 0L,
      "complete exact H1 request and R0 identities required for a derived field")
    requireSourceChildBinaryOwner(request.canonical.borrowedBinary, root, "H1 derived field request identity")

  private def sourceChildReserveDerivedColumn(inputs: MySqlScriptSchemaInputs,
      tableName: String, columnName: String): MySqlFormat3ColumnShape =
    val operationColumn = tableName == SourceChildReserveOperationTable &&
      SourceChildReserveOperationDerivedColumns.contains(columnName)
    val writerColumn = tableName == SourceChildReserveWriterTable && columnName == "last_commit_fingerprint"
    require(operationColumn || writerColumn,
      "H1 typed derived fields are restricted to exact current R0 operation and singleton writer columns")
    val shape = inputs.forTable(tableName)
    val columnIndex = shape.columns.indexWhere(_.name == columnName)
    require(columnIndex >= 0, "H1 typed derived column must exist in the exact Full26 shape")
    val column = shape.columns(columnIndex)
    require(column.kind == MySqlFormat3SqlKind.Binary,
      "H1 typed derived payload columns must use their original binary SQL type")
    if columnName.endsWith("_sha256") || columnName == "operation_fingerprint" ||
      columnName == "last_commit_fingerprint" then
      require(column.maximumLength.contains(32), "H1 derived digest columns must retain their exact 32-byte shape")
    column

  private def sourceReserveRowCell(row: Vector[MySqlFormat3Cell], shape: MySqlFormat3RelationShape,
      name: String): MySqlFormat3Cell =
    val index = shape.columns.indexWhere(_.name == name)
    require(index >= 0 && index < row.size, "H1 target row is missing an exact closed-layout field")
    row(index)

  private def sourceChildReserveDerivedTargetRow(snapshot: MySqlScriptSchemaSnapshot,
      request: SourceChildReserveRequestData, inputs: MySqlScriptSchemaInputs,
      image: MySqlOperationEffectImage, tableName: String, columnName: String,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    requireSourceChildReserveDerivedInputs(inputs, root, dataBudget)
    requireSourceChildReserveRequestForDerived(request, root)
    require(image == MySqlOperationEffectImage.Target,
      "H1 derived markers are legal only in the exact TARGET image")
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot, inputs, root)
    val derivedColumn = sourceChildReserveDerivedColumn(inputs, tableName, columnName)
    val shape = inputs.forTable(tableName)
    val relation = snapshot.relation(tableName)
    val row =
      if tableName == SourceChildReserveOperationTable then
        require(shape.primaryKey == Vector("binding_uuid", "operation_uuid"),
          "H1 current operation must retain the original two-column Full26 primary-key order")
        var selected: Vector[MySqlFormat3Cell] = null
        relation.rows.foreach { candidate =>
          if MySqlModeOperationEffectCodec.uuidEquals(sourceReserveRowCell(candidate, shape, "binding_uuid"), request.binding) &&
            MySqlModeOperationEffectCodec.uuidEquals(sourceReserveRowCell(candidate, shape, "operation_uuid"), request.reserveOperation) then
            require(selected == null, "H1 TARGET must contain one exact current O(binding,R0) row")
            selected = candidate
        }
        require(selected != null, "H1 TARGET must contain its exact current O(binding,R0) row")
        require(MySqlModeOperationEffectCodec.uuidEquals(sourceReserveRowCell(selected, shape, "workflow_uuid"), request.childWorkflow) &&
          sourceReserveRowCell(selected, shape, "step") == MySqlFormat3Cell.Text("RESERVE") &&
          sourceReserveRowCell(selected, shape, "operation_state") == MySqlFormat3Cell.Text("COMMITTED") &&
          sourceReserveRowCell(selected, shape, "reserve_operation_uuid") == MySqlFormat3Cell.SqlNull &&
          sourceReserveRowCell(selected, shape, "context_encoding") == MySqlFormat3Cell.Text("FP_RECOVERY_OPERATION_BE_V1") &&
          sourceReserveRowCell(selected, shape, "result_at").isInstanceOf[MySqlFormat3Cell.DateTime],
          "H1 TARGET derived operation must be the committed standalone R0 RESERVE row")
        val requestBytes = sourceReserveRowCell(selected, shape, "request_identity_bytes") match
          case MySqlFormat3Cell.Binary(value) => value
          case _ => throw new SQLDataException("H1 TARGET current operation request bytes must be binary")
        val requestHash = sourceReserveRowCell(selected, shape, "request_identity_sha256") match
          case MySqlFormat3Cell.Binary(value) => value
          case _ => throw new SQLDataException("H1 TARGET current operation request digest must be binary")
        require(requestBytes == request.canonical.borrowedBinary && requestHash.equalsVector(request.canonical.sha256),
          "H1 TARGET current operation request identity differs from the complete typed R0 request")
        selected
      else
        require(tableName == SourceChildReserveWriterTable &&
          shape.primaryKey == Vector("singleton_fixed1") && relation.rows.size == 1,
          "H1 TARGET writer must use its exact singleton Full26 row and primary key")
        val singleton = relation.rows.head
        require(sourceReserveRowCell(singleton, shape, "singleton_fixed1") == MySqlFormat3Cell.SignedInteger(1L) &&
          MySqlModeOperationEffectCodec.uuidEquals(sourceReserveRowCell(singleton, shape, "binding_uuid"), request.binding) &&
          MySqlModeOperationEffectCodec.uuidEquals(sourceReserveRowCell(singleton, shape, "last_commit_operation_uuid"), request.reserveOperation),
          "H1 TARGET singleton writer row must retain the exact binding and R0 operation identity")
        singleton
    val derivedValue = sourceReserveRowCell(row, shape, columnName)
    MySqlFormat3SnapshotValidation.validateCell(derivedValue, derivedColumn, tableName)
    val expectedLength = if columnName.endsWith("_sha256") || columnName == "operation_fingerprint" ||
      columnName == "last_commit_fingerprint" then 32 else -1
    derivedValue match
      case MySqlFormat3Cell.Binary(bytes) =>
        require(bytes.length > 0 && (expectedLength < 0 || bytes.length == expectedLength),
          "H1 TARGET derived field must retain its exact nonempty binary payload shape")
      case _ => throw new SQLDataException("H1 TARGET derived field must carry an actual binary value")
    row

  private def copySourceChildReserveDerivedPrimaryKey(shape: MySqlFormat3RelationShape,
      row: Vector[MySqlFormat3Cell], dataBudget: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    dataBudget.reserveRetainedMemory(Math.addExact(128L, Math.multiplyExact(shape.primaryKey.size.toLong, 64L)),
      "H1 typed derived original primary-key vector before allocation")
    shape.primaryKey.map { name =>
      val value = sourceReserveRowCell(row, shape, name)
      value match
        case MySqlFormat3Cell.Binary(bytes) =>
          MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(
            bytes.copyBytes(dataBudget, "H1 typed derived primary-key binary"), dataBudget))
        case integer: MySqlFormat3Cell.SignedInteger => integer
        case _ => throw new SQLDataException("H1 derived original primary key must use the exact binary/integer Full26 cell")
    }

  private def sourceChildReserveProjectedTargetPrimaryKey(request: SourceChildReserveRequestData,
      inputs: MySqlScriptSchemaInputs, tableName: String, columnName: String,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): Vector[MySqlFormat3Cell] =
    requireSourceChildReserveDerivedInputs(inputs, root, dataBudget, requirePlanDataChild)
    requireSourceChildReserveRequestForDerived(request, root)
    val derivedColumn = sourceChildReserveDerivedColumn(inputs, tableName, columnName)
    require(derivedColumn.kind == MySqlFormat3SqlKind.Binary,
      "H1 projected TARGET derived column must retain its original binary SQL type")
    val shape = inputs.forTable(tableName)
    val operationRow = tableName == SourceChildReserveOperationTable
    if operationRow then
      require(shape.primaryKey == Vector("binding_uuid", "operation_uuid"),
        "H1 projected TARGET operation must retain the original two-column Full26 primary-key order")
    else
      require(tableName == SourceChildReserveWriterTable && shape.primaryKey == Vector("singleton_fixed1"),
        "H1 projected TARGET writer must retain its exact singleton Full26 primary key")
    val keyCount = shape.primaryKey.size
    val binaryCount = if operationRow then 2L else 0L
    val charge = Math.addExact(128L, Math.addExact(Math.multiplyExact(keyCount.toLong, 64L),
      Math.multiplyExact(binaryCount, 80L)))
    dataBudget.reserveRetainedMemory(charge, "H1 projected TARGET real derived primary keys before allocation")
    try
      val primaryKey =
        if operationRow then
          def uuidCell(value: UUID): MySqlFormat3Cell =
            val buffer = java.nio.ByteBuffer.allocate(16)
            buffer.putLong(value.getMostSignificantBits)
            buffer.putLong(value.getLeastSignificantBits)
            MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(buffer.array(), dataBudget))
          Vector(uuidCell(request.binding), uuidCell(request.reserveOperation))
        else Vector(MySqlFormat3Cell.SignedInteger(1L))
      require(primaryKey.size == shape.primaryKey.size,
        "H1 projected TARGET primary-key cells differ from the exact original Full26 shape")
      shape.primaryKey.zip(primaryKey).foreach { (name, value) =>
        val index = shape.columns.indexWhere(_.name == name)
        require(index >= 0, "H1 projected TARGET primary-key column is absent from its original shape")
        MySqlFormat3SnapshotValidation.validateCell(value, shape.columns(index), tableName)
      }
      primaryKey
    catch
      case cause: Throwable =>
        try dataBudget.releaseRetainedMemory(charge, "failed H1 projected TARGET primary-key DATA")
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
        throw cause

  private def writeSourceChildReserveDerivedFieldBody(output: DataOutputStream,
      request: SourceChildReserveRequestData, inputs: MySqlScriptSchemaInputs,
      tableName: String, primaryKey: Vector[MySqlFormat3Cell], columnName: String): Unit =
    val shape = inputs.forTable(tableName)
    text(output, tableName)
    output.writeInt(shape.primaryKey.size)
    shape.primaryKey.zip(primaryKey).foreach { (name, value) => text(output, name); cell(output, value) }
    text(output, columnName)
    text(output, sourceChildReserveDerivedColumn(inputs, tableName, columnName).sqlDeclaration)
    uuid(output, request.reserveOperation)

  private def writeSourceChildReserveDerivedFieldCanonical(output: DataOutputStream,
      field: SourceChildReserveDerivedFieldData): Unit =
    token(output, SourceChildReserveDerivedFieldDomain)
    writeSourceChildReserveDerivedFieldBody(output, field.request, field.inputs,
      field.tableName, field.primaryKey, field.columnName)

  private def newSourceChildReserveDerivedFieldData(request: SourceChildReserveRequestData,
      inputs: MySqlScriptSchemaInputs, image: MySqlOperationEffectImage, tableName: String,
      primaryKey: Vector[MySqlFormat3Cell], columnName: String,
      canonical: MySqlRecoveryCanonicalBytes, dataBudget: MySqlFormat3ReadBudget): SourceChildReserveDerivedFieldData =
    dataBudget.reserveRetainedMemory(256L, "H1 typed derived field DATA before allocation")
    try new SourceChildReserveDerivedFieldData(request, inputs, image, tableName, primaryKey,
      columnName, sourceChildReserveDerivedColumn(inputs, tableName, columnName).sqlDeclaration, canonical)
    catch
      case cause: Throwable =>
        try dataBudget.releaseRetainedMemory(256L, "H1 typed derived field wrapper allocation failure")
        catch case cleanup: Throwable => if (cleanup ne cause) && !cause.getSuppressed.contains(cleanup) then cause.addSuppressed(cleanup)
        throw cause

  private def buildSourceChildReserveDerivedFieldData(request: SourceChildReserveRequestData,
      inputs: MySqlScriptSchemaInputs, image: MySqlOperationEffectImage, tableName: String,
      primaryKey: Vector[MySqlFormat3Cell], columnName: String,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): SourceChildReserveDerivedFieldData =
    requireSourceChildReserveDerivedInputs(inputs, root, dataBudget, requirePlanDataChild)
    requireSourceChildReserveRequestForDerived(request, root)
    require(image == MySqlOperationEffectImage.Target, "H1 derived field DATA must describe the TARGET image")
    val shape = inputs.forTable(tableName)
    require(primaryKey != null && primaryKey.size == shape.primaryKey.size,
      "H1 derived field DATA requires exact original Full26 primary-key cells")
    val derivedColumn = sourceChildReserveDerivedColumn(inputs, tableName, columnName)
    require(derivedColumn.kind == MySqlFormat3SqlKind.Binary,
      "H1 derived field DATA column must retain its original binary SQL type")
    val canonical = MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      encode(SourceChildReserveDerivedFieldDomain, root) { output =>
        writeSourceChildReserveDerivedFieldBody(output, request, inputs, tableName, primaryKey, columnName)
      }
    }
    val field = newSourceChildReserveDerivedFieldData(request, inputs, image, tableName,
      primaryKey, columnName, canonical, dataBudget)
    requireSourceChildReserveDerivedFieldData(field, root, dataBudget, requirePlanDataChild)
    field

  /** 为projected TARGET生成纯DATA派生标记；主键只来自绑定/R0或singleton=1，不含派生值或SQL能力。 */
  private[mysql] def sourceChildReserveProjectedTargetDerivedFieldData(request: SourceChildReserveRequestData,
      inputs: MySqlScriptSchemaInputs, image: MySqlOperationEffectImage,
      tableName: String, columnName: String, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): SourceChildReserveDerivedFieldData =
    require(image == MySqlOperationEffectImage.Target,
      "H1 projected TARGET derived field DATA cannot describe BEFORE")
    val primaryKey = sourceChildReserveProjectedTargetPrimaryKey(request, inputs, tableName,
      columnName, root, dataBudget, requirePlanDataChild)
    buildSourceChildReserveDerivedFieldData(request, inputs, image, tableName,
      primaryKey, columnName, root, dataBudget, requirePlanDataChild)

  private def requireSourceChildReserveDerivedFieldData(field: SourceChildReserveDerivedFieldData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): Unit =
    require(field != null, "typed H1 source reserve derived field DATA required")
    requireSourceChildReserveDerivedInputs(field.inputs, root, dataBudget, requirePlanDataChild)
    requireSourceChildReserveRequestForDerived(field.request, root)
    require(field.image == MySqlOperationEffectImage.Target && field.tableName != null &&
      field.columnName != null && field.canonical != null &&
      field.canonical.encoding == SourceChildReserveDerivedFieldDomain &&
      (field.canonical.borrowedBinary.storageBudget eq dataBudget),
      "H1 typed derived DATA must retain its exact TARGET source request and child-owned canonical bytes")
    val column = sourceChildReserveDerivedColumn(field.inputs, field.tableName, field.columnName)
    val shape = field.inputs.forTable(field.tableName)
    require(field.columnSqlDeclaration == column.sqlDeclaration && field.primaryKey != null &&
      field.primaryKey.size == shape.primaryKey.size,
      "H1 typed derived DATA column declaration or original primary-key shape differs")
    shape.primaryKey.zip(field.primaryKey).foreach { (name, value) =>
      val keyColumn = shape.columns(shape.columns.indexWhere(_.name == name))
      MySqlFormat3SnapshotValidation.validateCell(value, keyColumn, field.tableName)
      value match
        case MySqlFormat3Cell.Binary(bytes) =>
          require(bytes.storageBudget eq dataBudget,
            "H1 typed derived primary-key binary must be retained by the exact plan child")
        case _: MySqlFormat3Cell.SignedInteger => ()
        case _ => throw new SQLDataException("H1 typed derived primary key has an unsupported original Cell kind")
    }
    if field.tableName == SourceChildReserveOperationTable then
      require(shape.primaryKey == Vector("binding_uuid", "operation_uuid") &&
        (field.primaryKey(0) match
          case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(field.request.binding)
          case _ => false) && (field.primaryKey(1) match
          case MySqlFormat3Cell.Binary(bytes) => bytes.equalsUuid(field.request.reserveOperation)
          case _ => false),
        "H1 typed derived operation primary key differs from the exact current binding/R0")
    else
      require(field.tableName == SourceChildReserveWriterTable && shape.primaryKey == Vector("singleton_fixed1") &&
        field.primaryKey.head == MySqlFormat3Cell.SignedInteger(1L),
        "H1 typed derived writer primary key must be its exact singleton row")

  /** H1 真实TARGET重投影用的派生DATA；要求原始Full26图已有真实派生列值，不造预投影或占位值，也不表示提交结果。 */
  private[mysql] def sourceChildReserveDerivedFieldData(snapshot: MySqlScriptSchemaSnapshot,
      request: SourceChildReserveRequestData, inputs: MySqlScriptSchemaInputs,
      image: MySqlOperationEffectImage, tableName: String, columnName: String,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): SourceChildReserveDerivedFieldData =
    val row = sourceChildReserveDerivedTargetRow(snapshot, request, inputs, image,
      tableName, columnName, root, dataBudget)
    val shape = inputs.forTable(tableName)
    val primaryKey = copySourceChildReserveDerivedPrimaryKey(shape, row, dataBudget)
    buildSourceChildReserveDerivedFieldData(request, inputs, image, tableName,
      primaryKey, columnName, root, dataBudget, requirePlanDataChild)

  private[mysql] def encodeSourceChildReserveDerivedFieldData(field: SourceChildReserveDerivedFieldData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): MySqlRecoveryCanonicalBytes =
    requireSourceChildReserveDerivedFieldData(field, root, dataBudget, requirePlanDataChild)
    require(field.canonical.matchesWrittenBytes(output =>
      writeSourceChildReserveDerivedFieldCanonical(output, field)),
      "typed H1 derived field differs from its exact Full26 table/column/primary-key/R0 bytes")
    field.canonical

  private def requireSourceReserveDerivedPrimaryKeyCell(reader: DataReader,
      column: MySqlFormat3ColumnShape, expected: MySqlFormat3Cell, tableName: String): Unit = expected match
    case MySqlFormat3Cell.Binary(bytes) =>
      require(column.kind == MySqlFormat3SqlKind.Binary && !column.nullable && reader.octet() == 2,
        "H1 derived binary primary-key Cell tag or shape differs")
      val frame = reader.dataFrame()
      require(column.maximumLength.forall(_ >= frame.end - frame.start),
        "H1 derived binary primary-key Cell exceeds its original column shape")
      frame.requireExactBinary(bytes)
    case MySqlFormat3Cell.SignedInteger(value) =>
      require(column.kind == MySqlFormat3SqlKind.Integer && !column.nullable && reader.octet() == 3,
        "H1 derived integer primary-key Cell tag or shape differs")
      val actual = reader.int64()
      column.requireSignedInteger(actual, s"H1 derived $tableName.${column.name} primary key")
      require(actual == value, "H1 derived integer primary-key Cell differs from its exact target row")
    case _ => throw new SQLDataException("H1 derived primary-key Cell kind is outside the exact operation/writer shapes")

  private def decodeSourceChildReserveDerivedFieldCanonical(input: MySqlFormat3Binary,
      request: SourceChildReserveRequestData, inputs: MySqlScriptSchemaInputs,
      image: MySqlOperationEffectImage, tableName: String, columnName: String,
      primaryKey: Vector[MySqlFormat3Cell], root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): MySqlRecoveryCanonicalBytes =
    requireSourceChildReserveDerivedInputs(inputs, root, dataBudget, requirePlanDataChild)
    requireSourceChildReserveRequestForDerived(request, root)
    require(image == MySqlOperationEffectImage.Target,
      "H1 derived field decoder cannot describe the BEFORE image")
    require(primaryKey != null, "H1 derived field decoder requires exact expected primary-key cells")
    val shape = inputs.forTable(tableName)
    require(primaryKey.size == shape.primaryKey.size,
      "H1 derived field primary-key values differ from the exact Full26 shape")
    val derivedColumn = sourceChildReserveDerivedColumn(inputs, tableName, columnName)
    requireSourceChildBinaryOwner(input, root, "H1 typed derived field input")
    decodeData(input, root, dataBudget, Some(inputs)) { reader =>
      reader.domain(SourceChildReserveDerivedFieldDomain)
      reader.domain(tableName)
      require(reader.int32() == shape.primaryKey.size,
        "H1 derived field primary-key count differs from exact Full26 shape")
      shape.primaryKey.zip(primaryKey).foreach { (name, expected) =>
        reader.domain(name)
        val keyColumnIndex = shape.columns.indexWhere(_.name == name)
        require(keyColumnIndex >= 0, "H1 derived field primary-key column is absent from exact Full26 shape")
        requireSourceReserveDerivedPrimaryKeyCell(reader, shape.columns(keyColumnIndex), expected, tableName)
      }
      reader.domain(columnName)
      reader.domain(derivedColumn.sqlDeclaration)
      require(reader.uuid() == request.reserveOperation,
        "H1 derived field R0 differs from the exact current source request")
      reader.finish()
    }
    val canonical = MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      encode(SourceChildReserveDerivedFieldDomain, root) { output =>
        writeSourceChildReserveDerivedFieldBody(output, request, inputs, tableName, primaryKey, columnName)
      }
    }
    require(canonical.exactlyMatchesCell(MySqlFormat3Cell.Binary(input)),
      "noncanonical H1 typed derived field DATA")
    canonical

  /** H1 派生Field严格解码绑定原Full26图和确切请求，拒绝错序、错列、错R0及尾部字节。 */
  private[mysql] def decodeSourceChildReserveDerivedFieldData(input: MySqlFormat3Binary,
      snapshot: MySqlScriptSchemaSnapshot, request: SourceChildReserveRequestData,
      inputs: MySqlScriptSchemaInputs, image: MySqlOperationEffectImage,
      tableName: String, columnName: String, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): SourceChildReserveDerivedFieldData =
    val row = sourceChildReserveDerivedTargetRow(snapshot, request, inputs, image,
      tableName, columnName, root, dataBudget)
    val shape = inputs.forTable(tableName)
    val primaryKey = copySourceChildReserveDerivedPrimaryKey(shape, row, dataBudget)
    val canonical = decodeSourceChildReserveDerivedFieldCanonical(input, request, inputs,
      image, tableName, columnName, primaryKey, root, dataBudget)
    val field = newSourceChildReserveDerivedFieldData(request, inputs, image, tableName,
      primaryKey, columnName, canonical, dataBudget)
    requireSourceChildReserveDerivedFieldData(field, root, dataBudget)
    field

  /** 严格解码projected TARGET派生标记；只按绑定/R0或singleton=1解键，不读取派生值或赋予SQL能力。 */
  private[mysql] def decodeSourceChildReserveProjectedTargetDerivedFieldData(input: MySqlFormat3Binary,
      request: SourceChildReserveRequestData, inputs: MySqlScriptSchemaInputs,
      image: MySqlOperationEffectImage, tableName: String, columnName: String,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): SourceChildReserveDerivedFieldData =
    require(image == MySqlOperationEffectImage.Target,
      "H1 projected TARGET derived field decoder cannot describe the BEFORE image")
    val primaryKey = sourceChildReserveProjectedTargetPrimaryKey(request, inputs, tableName,
      columnName, root, dataBudget, requirePlanDataChild)
    val canonical = decodeSourceChildReserveDerivedFieldCanonical(input, request, inputs,
      image, tableName, columnName, primaryKey, root, dataBudget, requirePlanDataChild)
    val field = newSourceChildReserveDerivedFieldData(request, inputs, image, tableName,
      primaryKey, columnName, canonical, dataBudget)
    requireSourceChildReserveDerivedFieldData(field, root, dataBudget, requirePlanDataChild)
    field

  private def sourceChildReserveEffectShapes(inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): Vector[MySqlFormat3RelationShape] =
    requireSourceChildReserveDerivedInputs(inputs, root, dataBudget, requirePlanDataChild)
    val shapes = inputs.canonicalOrder
    require(shapes == MySqlDeploymentReceiptRelationShapes.canonicalOrder && shapes.size == 26 &&
      shapes.iterator.map(_.columns.size.toLong).sum == 467L,
      "H1 effect DATA requires the exact original Full26/467 shape and table order")
    Vector(SourceChildReserveOperationTable -> SourceChildReserveOperationDerivedColumns.toVector,
      SourceChildReserveWriterTable -> Vector("last_commit_fingerprint")).foreach { (table, columns) =>
      val shape = inputs.forTable(table)
      val lastPrimaryKeyIndex = shape.primaryKey.map(name => shape.columns.indexWhere(_.name == name)).max
      require(columns.forall { name =>
        val derivedIndex = shape.columns.indexWhere(_.name == name)
        derivedIndex > lastPrimaryKeyIndex
      }, s"H1 effect $table original primary-key columns must precede every DerivedField")
    }
    shapes

  private def requireSourceChildReserveEffectRequest(request: SourceChildReserveRequestData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): Unit =
    if requirePlanDataChild then requireSourcePlanDataBudget(root, dataBudget)
    else requireSourceDataBudget(root, dataBudget)
    requireSourceChildReserveRequestForDerived(request, root)
    require(request.canonical.borrowedBinary.storageBudget eq dataBudget,
      "H1 effect request bytes must remain in the exact issuer-validated plan child")

  private def sourceChildReserveEffectCellValue(row: IndexedSeq[SourceChildReserveEffectCell],
      index: Int, label: String): MySqlFormat3Cell =
    require(row != null && index >= 0 && index < row.size, s"$label cell index is outside the exact row")
    row(index) match
      case SourceChildReserveEffectCell.Literal(value) => value
      case SourceChildReserveEffectCell.Derived(_) =>
        throw new SQLDataException(s"$label identity or primary-key cell cannot be derived")

  private def sourceChildReserveEffectCurrentOperationRow(row: IndexedSeq[SourceChildReserveEffectCell],
      shape: MySqlFormat3RelationShape, request: SourceChildReserveRequestData): Boolean =
    if shape.table != SourceChildReserveOperationTable ||
      shape.primaryKey != Vector("binding_uuid", "operation_uuid") then false
    else
      val bindingIndex = shape.columns.indexWhere(_.name == "binding_uuid")
      val operationIndex = shape.columns.indexWhere(_.name == "operation_uuid")
      if bindingIndex < 0 || operationIndex < 0 then false
      else
        (sourceChildReserveEffectCellValue(row, bindingIndex, "H1 current O binding"),
          sourceChildReserveEffectCellValue(row, operationIndex, "H1 current O operation")) match
          case (MySqlFormat3Cell.Binary(binding), MySqlFormat3Cell.Binary(operation)) =>
            binding.equalsUuid(request.binding) && operation.equalsUuid(request.reserveOperation)
          case _ => false

  private def sourceChildReserveEffectCurrentWriterRow(row: IndexedSeq[SourceChildReserveEffectCell],
      shape: MySqlFormat3RelationShape): Boolean =
    if shape.table != SourceChildReserveWriterTable || shape.primaryKey != Vector("singleton_fixed1") then false
    else
      val index = shape.columns.indexWhere(_.name == "singleton_fixed1")
      index >= 0 && sourceChildReserveEffectCellValue(row, index, "H1 singleton W primary key") ==
        MySqlFormat3Cell.SignedInteger(1L)

  private def sourceChildReserveEffectDerivedPosition(row: IndexedSeq[SourceChildReserveEffectCell],
      shape: MySqlFormat3RelationShape, column: MySqlFormat3ColumnShape,
      request: SourceChildReserveRequestData, image: MySqlOperationEffectImage): Boolean =
    if image != MySqlOperationEffectImage.Target then false
    else if shape.table == SourceChildReserveOperationTable &&
      SourceChildReserveOperationDerivedColumns.contains(column.name) then
      sourceChildReserveEffectCurrentOperationRow(row, shape, request)
    else if shape.table == SourceChildReserveWriterTable && column.name == "last_commit_fingerprint" then
      sourceChildReserveEffectCurrentWriterRow(row, shape)
    else false

  private def compareSourceChildReserveEffectPrimaryKeyCell(left: MySqlFormat3Cell,
      right: MySqlFormat3Cell, table: String, column: String): Int = (left, right) match
    case (MySqlFormat3Cell.Binary(a), MySqlFormat3Cell.Binary(b)) => a.compareTo(b)
    case (MySqlFormat3Cell.Text(a), MySqlFormat3Cell.Text(b)) =>
      var leftOffset = 0
      var rightOffset = 0
      while leftOffset < a.length && rightOffset < b.length do
        val leftCodePoint = a.codePointAt(leftOffset)
        val rightCodePoint = b.codePointAt(rightOffset)
        val compared = Integer.compare(leftCodePoint, rightCodePoint)
        if compared != 0 then return compared
        leftOffset += Character.charCount(leftCodePoint)
        rightOffset += Character.charCount(rightCodePoint)
      Integer.compare(a.length - leftOffset, b.length - rightOffset)
    case (MySqlFormat3Cell.SignedInteger(a), MySqlFormat3Cell.SignedInteger(b)) => java.lang.Long.compare(a, b)
    case (MySqlFormat3Cell.DateTime(a), MySqlFormat3Cell.DateTime(b)) => a.compareTo(b)
    case _ => throw new SQLDataException(s"H1 effect $table.$column primary key has an unsupported or NULL Cell")

  private def compareSourceChildReserveEffectPrimaryKey(left: Vector[SourceChildReserveEffectCell],
      right: Vector[SourceChildReserveEffectCell], shape: MySqlFormat3RelationShape): Int =
    shape.primaryKey.iterator.map { name =>
      val index = shape.columns.indexWhere(_.name == name)
      require(index >= 0, s"H1 effect ${shape.table} primary-key column is absent")
      compareSourceChildReserveEffectPrimaryKeyCell(
        sourceChildReserveEffectCellValue(left, index, s"H1 effect ${shape.table} left primary key"),
        sourceChildReserveEffectCellValue(right, index, s"H1 effect ${shape.table} right primary key"),
        shape.table, name)
    }.find(_ != 0).getOrElse(0)

  private def sourceChildReserveEffectRowIndex(relations: Vector[SourceChildReserveEffectRelationData],
      tableName: String): Int =
    val index = relations.indexWhere(_.table == tableName)
    require(index >= 0, s"H1 effect exact Full26 table $tableName is missing")
    index

  private def sourceChildReserveEffectColumnValue(row: IndexedSeq[SourceChildReserveEffectCell],
      shape: MySqlFormat3RelationShape, columnName: String, label: String): MySqlFormat3Cell =
    val index = shape.columns.indexWhere(_.name == columnName)
    require(index >= 0, s"$label exact column $columnName is missing")
    sourceChildReserveEffectCellValue(row, index, label)

  private def validateSourceChildReserveEffectSemantics(request: SourceChildReserveRequestData,
      inputs: MySqlScriptSchemaInputs, image: MySqlOperationEffectImage,
      relations: Vector[SourceChildReserveEffectRelationData]): Unit =
    val operationShape = inputs.forTable(SourceChildReserveOperationTable)
    val operationRows = relations(sourceChildReserveEffectRowIndex(relations, SourceChildReserveOperationTable)).rows
    val currentOperations = operationRows.filter(row => sourceChildReserveEffectCurrentOperationRow(row, operationShape, request))
    val writerShape = inputs.forTable(SourceChildReserveWriterTable)
    val writerRows = relations(sourceChildReserveEffectRowIndex(relations, SourceChildReserveWriterTable)).rows
    require(writerShape.primaryKey == Vector("singleton_fixed1") && writerRows.size == 1 &&
      sourceChildReserveEffectCurrentWriterRow(writerRows.head, writerShape),
      "H1 effect must retain the exact singleton W row and original singleton primary key")
    val derivedCount = relations.iterator.flatMap(_.rows.iterator).flatMap(_.iterator).count {
      case SourceChildReserveEffectCell.Derived(_) => true
      case _ => false
    }
    image match
      case MySqlOperationEffectImage.Before =>
        require(derivedCount == 0 && currentOperations.isEmpty,
          "H1 BEFORE must retain only literal values and exclude the current R0 operation row")
        val writer = writerRows.head
        val lastCommit = sourceChildReserveEffectColumnValue(writer, writerShape,
          "last_commit_operation_uuid", "H1 BEFORE singleton W")
        require(!MySqlModeOperationEffectCodec.uuidEquals(lastCommit, request.reserveOperation),
          "H1 BEFORE singleton W cannot name the current R0 operation")
      case MySqlOperationEffectImage.Target =>
        require(currentOperations.size == 1 && derivedCount == 8,
          "H1 TARGET requires one exact current O(binding,R0) row and exactly eight typed derived fields")
        val operation = currentOperations.head
        require(MySqlModeOperationEffectCodec.uuidEquals(
          sourceChildReserveEffectColumnValue(operation, operationShape, "workflow_uuid", "H1 TARGET current O"),
          request.childWorkflow) &&
          sourceChildReserveEffectColumnValue(operation, operationShape, "step", "H1 TARGET current O") ==
            MySqlFormat3Cell.Text("RESERVE") &&
          sourceChildReserveEffectColumnValue(operation, operationShape, "operation_state", "H1 TARGET current O") ==
            MySqlFormat3Cell.Text("COMMITTED") &&
          sourceChildReserveEffectColumnValue(operation, operationShape, "reserve_operation_uuid", "H1 TARGET current O") ==
            MySqlFormat3Cell.SqlNull &&
          sourceChildReserveEffectColumnValue(operation, operationShape, "context_encoding", "H1 TARGET current O") ==
            MySqlFormat3Cell.Text("FP_RECOVERY_OPERATION_BE_V1") &&
          sourceChildReserveEffectColumnValue(operation, operationShape, "result_at", "H1 TARGET current O")
            .isInstanceOf[MySqlFormat3Cell.DateTime],
          "H1 TARGET current O must be the exact committed standalone R0 RESERVE operation")
        val requestBytes = sourceChildReserveEffectColumnValue(operation, operationShape,
          "request_identity_bytes", "H1 TARGET current O") match
          case MySqlFormat3Cell.Binary(bytes) => bytes
          case _ => throw new SQLDataException("H1 TARGET current O request identity must be binary")
        val requestHash = sourceChildReserveEffectColumnValue(operation, operationShape,
          "request_identity_sha256", "H1 TARGET current O") match
          case MySqlFormat3Cell.Binary(bytes) => bytes
          case _ => throw new SQLDataException("H1 TARGET current O request digest must be binary")
        require(requestBytes == request.canonical.borrowedBinary && requestHash.equalsVector(request.canonical.sha256),
          "H1 TARGET current O request identity differs from the exact typed R0 request")
        require(MySqlModeOperationEffectCodec.uuidEquals(
          sourceChildReserveEffectColumnValue(writerRows.head, writerShape, "binding_uuid", "H1 TARGET singleton W"),
          request.binding) && MySqlModeOperationEffectCodec.uuidEquals(
          sourceChildReserveEffectColumnValue(writerRows.head, writerShape, "last_commit_operation_uuid", "H1 TARGET singleton W"),
          request.reserveOperation),
          "H1 TARGET singleton W must retain the exact binding and R0 operation identity")

  private def validateSourceChildReserveEffectRelations(request: SourceChildReserveRequestData,
      inputs: MySqlScriptSchemaInputs, image: MySqlOperationEffectImage,
      relations: Vector[SourceChildReserveEffectRelationData], root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, requireChildBinary: Boolean,
      requirePlanDataChild: Boolean = true): Unit =
    val shapes = sourceChildReserveEffectShapes(inputs, root, dataBudget, requirePlanDataChild)
    require(request != null && image != null && relations != null && relations.size == shapes.size &&
      relations.forall(_ != null),
      "H1 effect requires the exact request, image and complete 26-table typed graph")
    requireSourceChildReserveEffectRequest(request, root, dataBudget, requirePlanDataChild)
    require(relations.map(_.table) == shapes.map(_.table),
      "H1 effect relation names/order differ from the original canonical Full26 layout")
    var totalRows = 0L
    relations.zip(shapes).foreach { (relation, shape) =>
      require(relation != null && relation.rows != null,
        s"H1 effect ${shape.table} relation and rows are required")
      require(shape.primaryKey.forall { name =>
        val index = shape.columns.indexWhere(_.name == name)
        index >= 0 && !shape.columns(index).nullable &&
          (shape.columns(index).kind != MySqlFormat3SqlKind.Text ||
            shape.columns(index).sqlDeclaration.contains("COLLATE utf8mb4_0900_bin"))
      }, s"H1 effect ${shape.table} primary-key shape must preserve its exact non-NULL binary ordering")
      totalRows = Math.addExact(totalRows, relation.rows.size.toLong)
      require(totalRows <= inputs.budget.rows && totalRows <= root.profile.maxMembers(),
        "H1 effect Full26 rows exceed the exact publication/member bounds")
      var previous: Vector[SourceChildReserveEffectCell] = null
      relation.rows.foreach { row =>
        require(row != null && row.size == shape.columns.size,
          s"H1 effect ${shape.table} row must retain every original Full26 column")
        shape.columns.zipWithIndex.foreach { (column, columnIndex) =>
          row(columnIndex) match
            case SourceChildReserveEffectCell.Literal(value) =>
              require(value != null, s"H1 effect ${shape.table}.${column.name} literal Cell is required")
              MySqlFormat3SnapshotValidation.validateCell(value, column, shape.table)
              value match
                case MySqlFormat3Cell.Binary(bytes) =>
                  if requireChildBinary then require(bytes.storageBudget eq dataBudget,
                    s"H1 effect ${shape.table}.${column.name} binary must remain in the exact plan child")
                  else requireSourceChildBinaryOwner(bytes, root,
                    s"H1 effect ${shape.table}.${column.name} original Cell")
                case _ => ()
              require(!sourceChildReserveEffectDerivedPosition(row, shape, column, request, image),
                s"H1 effect ${shape.table}.${column.name} requires its exact typed TARGET DerivedField")
            case SourceChildReserveEffectCell.Derived(field) =>
              require(sourceChildReserveEffectDerivedPosition(row, shape, column, request, image),
                s"H1 DerivedField is forbidden at ${shape.table}.${column.name} for this image/primary key")
              requireSourceChildReserveDerivedFieldData(field, root, dataBudget, requirePlanDataChild)
              val primaryKey = shape.primaryKey.map { name =>
                val index = shape.columns.indexWhere(_.name == name)
                sourceChildReserveEffectCellValue(row, index, s"H1 effect ${shape.table} DerivedField primary key")
              }
              require((field.request eq request) && (field.inputs eq inputs) &&
                field.image == MySqlOperationEffectImage.Target && field.tableName == shape.table &&
                field.columnName == column.name && field.columnSqlDeclaration == column.sqlDeclaration &&
                field.primaryKey == primaryKey,
                s"H1 DerivedField does not bind its exact request, Full26 row key, column and declaration")
              encodeSourceChildReserveDerivedFieldData(field, root, dataBudget, requirePlanDataChild)
        }
        shape.primaryKey.foreach { name =>
          val index = shape.columns.indexWhere(_.name == name)
          sourceChildReserveEffectCellValue(row, index, s"H1 effect ${shape.table} original primary key")
        }
        require(previous == null || compareSourceChildReserveEffectPrimaryKey(previous, row, shape) < 0,
          s"H1 effect ${shape.table} rows must be strictly ordered by the original primary key")
        previous = row
      }
    }
    validateSourceChildReserveEffectSemantics(request, inputs, image, relations)

  private def retainSourceChildReserveEffectCell(value: SourceChildReserveEffectCell,
      dataBudget: MySqlFormat3ReadBudget, table: String, column: String): SourceChildReserveEffectCell = value match
    case SourceChildReserveEffectCell.Derived(field) => SourceChildReserveEffectCell.Derived(field)
    case SourceChildReserveEffectCell.Literal(MySqlFormat3Cell.Binary(bytes)) =>
      val copied = bytes.copyBytes(dataBudget, s"H1 effect $table.$column binary literal")
      SourceChildReserveEffectCell.Literal(MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(copied, dataBudget)))
    case SourceChildReserveEffectCell.Literal(MySqlFormat3Cell.Text(value)) =>
      val charge = Math.addExact(Math.multiplyExact(value.length.toLong, 2L), 64L)
      dataBudget.reserveRetainedMemory(charge, s"H1 effect $table.$column immutable text literal")
      SourceChildReserveEffectCell.Literal(MySqlFormat3Cell.Text(value))
    case literal => literal

  private def sourceChildReserveEffectCandidateScratchBytes(inputs: MySqlScriptSchemaInputs,
      rowCounts: Vector[Int]): Long =
    val shapes = inputs.canonicalOrder
    require(rowCounts != null && rowCounts.size == shapes.size,
      "H1 effect candidate scratch charge requires every exact Full26 relation row count")
    var total = 0L
    shapes.zip(rowCounts).foreach { (shape, rowCount) =>
      require(rowCount >= 0, s"H1 effect ${shape.table} candidate row count cannot be negative")
      val perRow = Math.addExact(192L, Math.multiplyExact(shape.columns.size.toLong, 64L))
      total = Math.addExact(total, Math.addExact(256L, Math.multiplyExact(rowCount.toLong, perRow)))
    }
    total

  private def withSourceChildReserveEffectCandidateScratch[A](root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, bytes: Long, label: String,
      requirePlanDataChild: Boolean = true)(body: => A): A =
    if requirePlanDataChild then requireSourcePlanDataBudget(root, dataBudget)
    else requireSourceDataBudget(root, dataBudget)
    require(bytes >= 0L && label != null && label.nonEmpty,
      "H1 effect candidate scratch byte count and label are required")
    dataBudget.reserveTemporaryMemory(bytes, label)
    var primary: Throwable = null
    try body
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try dataBudget.releaseTemporaryMemory(bytes, label)
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  private def writeSourceChildReserveEffectCanonical(output: DataOutputStream,
      effect: SourceChildReserveEffectData): Unit =
    token(output, SourceChildReserveEffectDomain)
    writeSourceChildReserveEffectBody(output, effect.request, effect.inputs, effect.image, effect.relations)

  private def newSourceChildReserveEffectData(request: SourceChildReserveRequestData,
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, image: MySqlOperationEffectImage,
      relations: Vector[SourceChildReserveEffectRelationData],
      canonical: MySqlRecoveryCanonicalBytes): SourceChildReserveEffectData =
    dataBudget.reserveRetainedMemory(512L, "H1 typed effect DATA wrapper before allocation")
    try new SourceChildReserveEffectData(request, inputs, root, dataBudget, image, relations, canonical)
    catch
      case cause: Throwable =>
        try dataBudget.releaseRetainedMemory(512L, "failed H1 typed effect DATA wrapper")
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
        throw cause

  private def buildSourceChildReserveEffectData(request: SourceChildReserveRequestData,
      inputs: MySqlScriptSchemaInputs, image: MySqlOperationEffectImage,
      candidate: Vector[SourceChildReserveEffectRelationData], root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, valuesAlreadyRetained: Boolean = false,
      requirePlanDataChild: Boolean = true): SourceChildReserveEffectData =
    val shapes = sourceChildReserveEffectShapes(inputs, root, dataBudget, requirePlanDataChild)
    validateSourceChildReserveEffectRelations(request, inputs, image, candidate, root, dataBudget,
      requireChildBinary = valuesAlreadyRetained, requirePlanDataChild = requirePlanDataChild)
    val relations = if valuesAlreadyRetained then candidate else
      candidate.zip(shapes).map { (relation, shape) =>
        val retainedWrapperBytes = Math.addExact(256L, Math.addExact(
          Math.multiplyExact(relation.rows.size.toLong, 128L),
          Math.multiplyExact(relation.rows.size.toLong, Math.multiplyExact(shape.columns.size.toLong, 40L))))
        dataBudget.reserveRetainedMemory(retainedWrapperBytes,
          s"H1 effect ${shape.table} typed rows/cells before allocation")
        val rows = relation.rows.map { row =>
          row.zip(shape.columns).map { (value, column) =>
            retainSourceChildReserveEffectCell(value, dataBudget, shape.table, column.name)
          }
        }
        SourceChildReserveEffectRelationData(shape.table, rows)
      }
    if valuesAlreadyRetained then
      ()
    else validateSourceChildReserveEffectRelations(request, inputs, image, relations, root, dataBudget,
      requireChildBinary = true, requirePlanDataChild = requirePlanDataChild)
    val canonical = MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      encode(SourceChildReserveEffectDomain, root) { output =>
        writeSourceChildReserveEffectBody(output, request, inputs, image, relations)
      }
    }
    newSourceChildReserveEffectData(request, inputs, root, dataBudget, image, relations, canonical)

  private def writeSourceChildReserveEffectBody(output: DataOutputStream,
      request: SourceChildReserveRequestData, inputs: MySqlScriptSchemaInputs,
      image: MySqlOperationEffectImage, relations: Vector[SourceChildReserveEffectRelationData]): Unit =
    output.writeByte(if image == MySqlOperationEffectImage.Before then 1 else 2)
    uuid(output, request.binding); uuid(output, request.childWorkflow); uuid(output, request.reserveOperation)
    val shapes = inputs.canonicalOrder
    output.writeInt(shapes.size)
    relations.zip(shapes).foreach { (relation, shape) =>
      text(output, shape.table); output.writeInt(shape.columns.size)
      shape.columns.foreach { column => text(output, column.name); text(output, column.sqlDeclaration) }
      output.writeInt(relation.rows.size)
      relation.rows.foreach { row =>
        shape.columns.zip(row).foreach { pair => pair._2 match
          case SourceChildReserveEffectCell.Literal(actual) => cell(output, actual)
          case SourceChildReserveEffectCell.Derived(field) =>
            writeSourceChildReserveDerivedFieldCanonical(output, field)
        }
      }
    }

  private def requireSourceChildReserveEffectData(effect: SourceChildReserveEffectData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): Unit =
    require(effect != null && (effect.root eq root) && (effect.dataBudget eq dataBudget) &&
      effect.canonical != null && effect.canonical.encoding == SourceChildReserveEffectDomain &&
      (effect.canonical.borrowedBinary.storageBudget eq dataBudget),
      "H1 effect DATA must retain its exact original root and canonical plan child")
    sourceChildReserveEffectShapes(effect.inputs, root, dataBudget, requirePlanDataChild)
    requireSourceChildReserveRequestForDerived(effect.request, root)
    validateSourceChildReserveEffectRelations(effect.request, effect.inputs, effect.image,
      effect.relations, root, dataBudget, requireChildBinary = true,
      requirePlanDataChild = requirePlanDataChild)

  private def sourceChildReserveEffectDataCanonical(effect: SourceChildReserveEffectData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): MySqlRecoveryCanonicalBytes =
    requireSourceChildReserveEffectData(effect, root, dataBudget, requirePlanDataChild)
    require(effect.canonical.matchesWrittenBytes(output => writeSourceChildReserveEffectCanonical(output, effect)),
      "typed H1 effect DATA differs from its exact Full26 image/Cell/DerivedField bytes")
    effect.canonical

  /** H1 projected TARGET只接收真实普通Cell及已绑定的DerivedField节点；不构造临时snapshot或派生值。 */
  private[mysql] def sourceChildReserveProjectedTargetEffectData(request: SourceChildReserveRequestData,
      inputs: MySqlScriptSchemaInputs, relations: Vector[SourceChildReserveEffectRelationData],
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): SourceChildReserveEffectData =
    buildSourceChildReserveEffectData(request, inputs, MySqlOperationEffectImage.Target,
      relations, root, dataBudget, requirePlanDataChild = requirePlanDataChild)

  /** H1 BEFORE从原始Full26 image完整保留literal Cell；current R0须缺席。 */
  private[mysql] def sourceChildReserveBeforeEffectData(snapshot: MySqlScriptSchemaSnapshot,
      request: SourceChildReserveRequestData, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): SourceChildReserveEffectData =
    sourceChildReserveEffectShapes(inputs, root, dataBudget, requirePlanDataChild)
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot, inputs, root)
    inputs.validateRelations(snapshot.relations, snapshot.maxAllowedPacketBytes, dataBudget)
    val countScratch = Math.addExact(512L, Math.multiplyExact(inputs.canonicalOrder.size.toLong, 32L))
    def buildBeforeEffect(): SourceChildReserveEffectData =
      val rowCounts = snapshot.relations.map(_.rows.size)
      val scratchBytes = sourceChildReserveEffectCandidateScratchBytes(inputs, rowCounts)
      withSourceChildReserveEffectCandidateScratch(root, dataBudget, scratchBytes,
        "H1 BEFORE Full26 typed candidate rows/cells", requirePlanDataChild) {
        val relations = inputs.canonicalOrder.map { shape =>
          val relation = snapshot.relation(shape.table)
          SourceChildReserveEffectRelationData(shape.table,
            relation.rows.map(_.map(SourceChildReserveEffectCell.Literal(_))))
        }
        buildSourceChildReserveEffectData(request, inputs, MySqlOperationEffectImage.Before,
          relations, root, dataBudget, requirePlanDataChild = requirePlanDataChild)
      }
    if requirePlanDataChild then buildBeforeEffect()
    else withSourceChildReserveTemporary(dataBudget, countScratch, "historical H1 BEFORE relation-count vector") {
      buildBeforeEffect()
    }

  /** H1 effect canonical编码复核；只返回纯DATA bytes。 */
  private[mysql] def encodeSourceChildReserveEffectData(effect: SourceChildReserveEffectData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    sourceChildReserveEffectDataCanonical(effect, root, dataBudget)

  /** 有限投影检查：比较普通Cell与Derived身份，不验证其外部预期payload值。 */
  private[mysql] def sourceChildReserveActualTargetEffectData(snapshot: MySqlScriptSchemaSnapshot,
      projected: SourceChildReserveEffectData, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): SourceChildReserveEffectData =
    sourceChildReserveActualTargetEffectDataCore(snapshot, projected, null, root, dataBudget)

  /**
    * 完整H1 actual TARGET读回：先用witness的typed链核对O七项/W一项真实payload，再做Full26重投影。
    * 这里只消费已存在的真实snapshot，不创建precommit target或成功占位。
    */
  private[mysql] def sourceChildReserveVerifiedActualTargetEffectData(snapshot: MySqlScriptSchemaSnapshot,
      witness: SourceChildReserveWitnessData, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): SourceChildReserveEffectData =
    require(witness != null, "complete H1 witness typed DATA is required for verified actual TARGET readback")
    sourceChildReserveActualTargetEffectDataCore(snapshot,
      witness.originalContext.fingerprint.projectedTarget, witness, root, dataBudget)

  private[mysql] def sourceChildReserveVerifiedHistoricalActualTargetEffectData(
      snapshot: MySqlScriptSchemaSnapshot, witness: SourceChildReserveWitnessData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): SourceChildReserveEffectData =
    require(witness != null, "complete historical H1 witness typed DATA is required for actual TARGET readback")
    sourceChildReserveActualTargetEffectDataCore(snapshot,
      witness.originalContext.fingerprint.projectedTarget, witness, root, dataBudget,
      requirePlanDataChild = false)

  private def sourceChildReserveActualTargetOperationRow(snapshot: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs, request: SourceChildReserveRequestData): Vector[MySqlFormat3Cell] =
    val shape = inputs.forTable(SourceChildReserveOperationTable)
    require(shape.primaryKey == Vector("binding_uuid", "operation_uuid"),
      "H1 actual TARGET O must retain its exact original Full26 primary-key order")
    var selected: Vector[MySqlFormat3Cell] = null
    snapshot.relation(SourceChildReserveOperationTable).rows.foreach { candidate =>
      if MySqlModeOperationEffectCodec.uuidEquals(sourceReserveRowCell(candidate, shape, "binding_uuid"), request.binding) &&
        MySqlModeOperationEffectCodec.uuidEquals(sourceReserveRowCell(candidate, shape, "operation_uuid"), request.reserveOperation) then
        require(selected == null, "H1 actual TARGET must contain one exact current O(binding,R0) row")
        selected = candidate
    }
    require(selected != null, "H1 actual TARGET is missing its exact current O(binding,R0) row")
    selected

  private def sourceChildReserveActualBinary(row: Vector[MySqlFormat3Cell],
      shape: MySqlFormat3RelationShape, column: String, label: String): MySqlFormat3Binary =
    sourceReserveRowCell(row, shape, column) match
      case MySqlFormat3Cell.Binary(bytes) => bytes
      case _ => throw new SQLDataException(s"$label must be an actual binary Cell")

  private def requireSourceChildReserveActualPayload(row: Vector[MySqlFormat3Cell],
      shape: MySqlFormat3RelationShape, bytesColumn: String, digestColumn: String,
      expected: MySqlRecoveryCanonicalBytes, label: String): Unit =
    require(expected != null &&
      sourceChildReserveActualBinary(row, shape, bytesColumn, label) == expected.borrowedBinary &&
      sourceChildReserveActualBinary(row, shape, digestColumn, label).equalsVector(expected.sha256),
      s"$label actual bytes/hash differ from the exact H1 typed DATA")

  private def requireSourceChildReserveActualDigest(row: Vector[MySqlFormat3Cell],
      shape: MySqlFormat3RelationShape, column: String, expected: Vector[Byte], label: String): Unit =
    require(expected != null && expected.size == 32 &&
      sourceChildReserveActualBinary(row, shape, column, label).equalsVector(expected),
      s"$label actual digest differs from the exact H1 typed DATA")

  private def requireSourceChildReserveActualDerivedValues(snapshot: MySqlScriptSchemaSnapshot,
      witness: SourceChildReserveWitnessData): Unit =
    val originalContext = witness.originalContext
    val fingerprint = originalContext.fingerprint
    val scope = fingerprint.scope
    val request = scope.request
    val inputs = scope.inputs
    val operationShape = inputs.forTable(SourceChildReserveOperationTable)
    val operation = sourceChildReserveActualTargetOperationRow(snapshot, inputs, request)
    require(MySqlModeOperationEffectCodec.uuidEquals(
      sourceReserveRowCell(operation, operationShape, "workflow_uuid"), request.childWorkflow) &&
      sourceReserveRowCell(operation, operationShape, "step") == MySqlFormat3Cell.Text("RESERVE") &&
      sourceReserveRowCell(operation, operationShape, "operation_state") == MySqlFormat3Cell.Text("COMMITTED") &&
      sourceReserveRowCell(operation, operationShape, "reserve_operation_uuid") == MySqlFormat3Cell.SqlNull &&
      sourceReserveRowCell(operation, operationShape, "context_encoding") ==
        MySqlFormat3Cell.Text(SourceChildReserveOperationContextEncoding) &&
      sourceReserveRowCell(operation, operationShape, "result_at") == MySqlFormat3Cell.DateTime(witness.resultAt),
      "H1 actual TARGET O must retain the exact committed R0 identity, encoding and witness resultAt")
    requireSourceChildReserveActualPayload(operation, operationShape,
      "request_identity_bytes", "request_identity_sha256", request.canonical,
      "H1 actual TARGET O request")
    requireSourceChildReserveActualPayload(operation, operationShape,
      "scope_bytes", "scope_sha256", scope.canonical,
      "H1 actual TARGET O scope")
    requireSourceChildReserveActualPayload(operation, operationShape,
      "preimage_bytes", "preimage_sha256", fingerprint.preimage.canonical,
      "H1 actual TARGET O preimage")
    requireSourceChildReserveActualPayload(operation, operationShape,
      "original_context_bytes", "original_context_sha256", originalContext.canonical,
      "H1 actual TARGET O original context")
    requireSourceChildReserveActualPayload(operation, operationShape,
      "target_bytes", "target_sha256", fingerprint.projectedTarget.canonical,
      "H1 actual TARGET O projected target")
    requireSourceChildReserveActualPayload(operation, operationShape,
      "commit_witness_bytes", "commit_witness_sha256", witness.canonical,
      "H1 actual TARGET O commit witness")
    requireSourceChildReserveActualDigest(operation, operationShape, "operation_fingerprint",
      fingerprint.canonical.sha256, "H1 actual TARGET O phase fingerprint")

    val writerShape = inputs.forTable(SourceChildReserveWriterTable)
    val writerRows = snapshot.relation(SourceChildReserveWriterTable).rows
    require(writerShape.primaryKey == Vector("singleton_fixed1") && writerRows.size == 1,
      "H1 actual TARGET W must retain its exact singleton Full26 row")
    val writer = writerRows.head
    require(sourceReserveRowCell(writer, writerShape, "singleton_fixed1") == MySqlFormat3Cell.SignedInteger(1L) &&
      MySqlModeOperationEffectCodec.uuidEquals(sourceReserveRowCell(writer, writerShape, "binding_uuid"), request.binding) &&
      MySqlModeOperationEffectCodec.uuidEquals(sourceReserveRowCell(writer, writerShape, "last_commit_operation_uuid"),
        request.reserveOperation) &&
      originalContext.context.canonicalFingerprintSha256 == originalContext.canonical.sha256,
      "H1 actual TARGET W must retain the exact binding/R0 and original context fingerprint identity")
    requireSourceChildReserveActualDigest(writer, writerShape, "last_commit_fingerprint",
      originalContext.context.canonicalFingerprintSha256, "H1 actual TARGET W context fingerprint")

  private def sourceChildReserveActualTargetEffectDataCore(snapshot: MySqlScriptSchemaSnapshot,
      projected: SourceChildReserveEffectData, witness: SourceChildReserveWitnessData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): SourceChildReserveEffectData =
    requireSourceChildReserveEffectData(projected, root, dataBudget, requirePlanDataChild)
    require(projected.image == MySqlOperationEffectImage.Target,
      "H1 actual-target reprojection requires the exact projected TARGET effect DATA")
    if witness != null then
      requireSourceChildReserveWitnessData(witness, root, dataBudget, requirePlanDataChild)
      require(witness.originalContext.fingerprint.projectedTarget eq projected,
        "verified H1 actual TARGET must use the exact projected effect retained by its witness chain")
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot, projected.inputs, root)
    projected.inputs.validateRelations(snapshot.relations, snapshot.maxAllowedPacketBytes, dataBudget)
    if witness != null then
      withSourceChildReserveTemporary(dataBudget, 1024L, "H1 actual TARGET external-value comparison workspace") {
        requireSourceChildReserveActualDerivedValues(snapshot, witness)
      }
    val scratchBytes = sourceChildReserveEffectCandidateScratchBytes(projected.inputs,
      projected.relations.map(_.rows.size))
    withSourceChildReserveEffectCandidateScratch(root, dataBudget, scratchBytes,
      "H1 actual TARGET Full26 typed candidate rows/cells", requirePlanDataChild) {
      val shapes = projected.inputs.canonicalOrder
      val actualRelations = Vector.tabulate(shapes.size) { relationIndex =>
        val shape = shapes(relationIndex)
        val relation = projected.relations(relationIndex)
        val actual = snapshot.relation(shape.table)
        require(actual.rows.size == relation.rows.size,
          s"H1 actual TARGET ${shape.table} row count differs from its projected graph")
        val rows = Vector.tabulate(actual.rows.size) { rowIndex =>
          val actualRow = actual.rows(rowIndex)
          val projectedRow = relation.rows(rowIndex)
          require(actualRow.size == projectedRow.size && actualRow.size == shape.columns.size,
            s"H1 actual TARGET ${shape.table} row $rowIndex differs from the original Full26 width")
          Vector.tabulate(shape.columns.size) { columnIndex =>
            val actualCell = actualRow(columnIndex)
            val projectedCell = projectedRow(columnIndex)
            val column = shape.columns(columnIndex)
            projectedCell match
              case SourceChildReserveEffectCell.Literal(_) => SourceChildReserveEffectCell.Literal(actualCell)
              case SourceChildReserveEffectCell.Derived(field) =>
                val actualField = sourceChildReserveDerivedFieldData(snapshot, projected.request, projected.inputs,
                  MySqlOperationEffectImage.Target, shape.table, column.name, root, dataBudget,
                  requirePlanDataChild)
                require(actualField.primaryKey == field.primaryKey,
                  s"H1 actual TARGET ${shape.table}.${column.name} primary key differs from its projected DerivedField")
                if requirePlanDataChild then MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
                  require(actualField.canonical.exactlyMatchesCell(
                    MySqlFormat3Cell.Binary(field.canonical.borrowedBinary)),
                    s"H1 actual TARGET ${shape.table}.${column.name} differs from its projected DerivedField")
                }
                else MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
                  require(actualField.canonical.exactlyMatchesCell(
                    MySqlFormat3Cell.Binary(field.canonical.borrowedBinary)),
                    s"H1 actual TARGET ${shape.table}.${column.name} differs from its projected DerivedField")
                }
                SourceChildReserveEffectCell.Derived(field)
          }
        }
        SourceChildReserveEffectRelationData(shape.table, rows)
      }
      val actualEffect = buildSourceChildReserveEffectData(projected.request, projected.inputs,
        MySqlOperationEffectImage.Target, actualRelations, root, dataBudget,
        requirePlanDataChild = requirePlanDataChild)
      if requirePlanDataChild then MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        require(actualEffect.canonical.exactlyMatchesCell(
          MySqlFormat3Cell.Binary(projected.canonical.borrowedBinary)),
          "H1 actual TARGET complete literal/DerivedField reproject differs from projected TARGET bytes")
      }
      else MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
        require(actualEffect.canonical.exactlyMatchesCell(
          MySqlFormat3Cell.Binary(projected.canonical.borrowedBinary)),
          "H1 actual TARGET complete literal/DerivedField reproject differs from projected TARGET bytes")
      }
      actualEffect
    }

  /** 严格解码H1全26表Effect；Derived/Literal的选择只由image、table、column和已读原PK决定。 */
  private[mysql] def decodeSourceChildReserveEffectData(input: MySqlFormat3Binary,
      request: SourceChildReserveRequestData, inputs: MySqlScriptSchemaInputs,
      image: MySqlOperationEffectImage, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): SourceChildReserveEffectData =
    decodeSourceChildReserveEffectDataInternal(input, request, inputs, image, root, dataBudget,
      requirePlanDataChild = true)

  private[mysql] def decodeHistoricalSourceChildReserveEffectData(input: MySqlFormat3Binary,
      request: SourceChildReserveRequestData, inputs: MySqlScriptSchemaInputs,
      image: MySqlOperationEffectImage, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): SourceChildReserveEffectData =
    decodeSourceChildReserveEffectDataInternal(input, request, inputs, image, root, dataBudget,
      requirePlanDataChild = false)

  private def decodeSourceChildReserveEffectDataInternal(input: MySqlFormat3Binary,
      request: SourceChildReserveRequestData, inputs: MySqlScriptSchemaInputs,
      image: MySqlOperationEffectImage, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, requirePlanDataChild: Boolean): SourceChildReserveEffectData =
    sourceChildReserveEffectShapes(inputs, root, dataBudget, requirePlanDataChild)
    requireSourceChildReserveEffectRequest(request, root, dataBudget, requirePlanDataChild)
    require(image != null, "H1 effect decoder requires an explicit BEFORE or TARGET image")
    requireSourceChildBinaryOwner(input, root, "H1 complete effect input")
    val decoded = decodePhysical(input, root, 4096L, dataBudget, Some(inputs)) { _ =>
      val reader = new DataReader(input, 0, input.length, root, dataBudget, new MemberCount, Some(inputs))
      val result = reader.sourceChildReserveEffectData(request, inputs, image, requirePlanDataChild)
      reader.finish()
      result
    }
    val compare = () =>
      require(decoded.canonical.exactlyMatchesCell(MySqlFormat3Cell.Binary(input)),
        "noncanonical or trailing H1 Full26 effect DATA")
    if requirePlanDataChild then MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      compare()
    }
    else MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      compare()
    }
    decoded

  private def withSourceChildReserveTemporary[A](budget: MySqlFormat3ReadBudget, bytes: Long,
      label: String)(body: => A): A =
    require(budget != null && bytes > 0L && label != null && label.nonEmpty,
      "H1 canonical temporary workspace inputs are required")
    budget.reserveTemporaryMemory(bytes, label + " before allocation")
    var primary: Throwable = null
    try body
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      try budget.releaseTemporaryMemory(bytes, label + " release")
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  private def requireSourceChildReserveOwnedBinary(input: MySqlFormat3Binary,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget, label: String): Unit =
    requireSourceChildBinaryOwner(input, root, label)
    require(input.storageBudget eq dataBudget,
      s"$label must remain in the exact H1 plan DATA child")

  private def writeSourceChildReserveCanonicalFrame(output: DataOutputStream,
      value: MySqlRecoveryCanonicalBytes): Unit =
    require(output != null && value != null, "complete H1 canonical B frame required")
    output.writeInt(value.length)
    value.writeTo(output)

  private def sourceChildReserveCurrentOperationRow(effect: SourceChildReserveEffectData,
      label: String): Vector[SourceChildReserveEffectCell] =
    val shape = effect.inputs.forTable(SourceChildReserveOperationTable)
    val rows = effect.relations(sourceChildReserveEffectRowIndex(effect.relations,
      SourceChildReserveOperationTable)).rows
    val current = rows.filter(row => sourceChildReserveEffectCurrentOperationRow(row, shape, effect.request))
    require(current.size == 1, s"$label requires its exact current O(binding,R0) row")
    current.head

  private def sourceChildReserveEffectBinary(effect: SourceChildReserveEffectData,
      row: IndexedSeq[SourceChildReserveEffectCell], columnName: String, label: String): MySqlFormat3Binary =
    sourceChildReserveEffectColumnValue(row, effect.inputs.forTable(SourceChildReserveOperationTable),
      columnName, label) match
      case MySqlFormat3Cell.Binary(bytes) => bytes
      case _ => throw new SQLDataException(s"$label must be a complete literal binary Cell")

  private def requireSourceChildReserveEffectPayload(effect: SourceChildReserveEffectData,
      row: IndexedSeq[SourceChildReserveEffectCell], bytesColumn: String, digestColumn: String,
      expected: MySqlRecoveryCanonicalBytes, label: String): Unit =
    require(expected != null &&
      sourceChildReserveEffectBinary(effect, row, bytesColumn, label) == expected.borrowedBinary &&
      sourceChildReserveEffectBinary(effect, row, digestColumn, label).equalsVector(expected.sha256),
      s"$label literal bytes/hash differ from the exact H1 DATA frame")

  private def requireSourceChildReserveRequestForPhase(request: SourceChildReserveRequestData,
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): Unit =
    requireSourceChildReserveRequestData(request.binding, request.childWorkflow,
      request.parentRecallWorkflow, request.parentP2Operation, request.reserveOperation,
      request.plannedWrite, request.writeGeneration, request.carrierIdentity, request.units,
      request.obligations, request.resources, inputs, root, dataBudget)
    require(request.canonical != null && request.canonical.encoding == SourceChildReserveRequestDomain,
      "H1 phase fingerprint requires the original canonical R0 request frame")
    requireSourceChildReserveOwnedBinary(request.canonical.borrowedBinary, root, dataBudget,
      "H1 phase fingerprint complete request")
    withSourceChildReserveTemporary(dataBudget, 256L, "H1 phase request canonical comparison") {
      require(request.canonical.matchesWrittenBytes { output =>
        token(output, SourceChildReserveRequestDomain)
        writeSourceChildReserveRequestData(output, request.binding, request.childWorkflow,
          request.parentRecallWorkflow, request.parentP2Operation, request.reserveOperation,
          request.plannedWrite, request.writeGeneration, request.carrierIdentity, request.units,
          request.obligations, request.resources, inputs, root, dataBudget)
      }, "H1 phase request differs from its exact request identity and DATA child bytes")
    }

  private def requireSourceChildReservePhaseInputs(scope: SourceChildReserveScopeData,
      preimage: SourceChildReserveEffectData, projectedTarget: SourceChildReserveEffectData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): Unit =
    if requirePlanDataChild then requireSourcePlanDataBudget(root, dataBudget)
    else requireSourceDataBudget(root, dataBudget)
    require(scope != null && preimage != null && projectedTarget != null &&
      scope.request != null && scope.inputs != null &&
      (preimage.request eq scope.request) && (projectedTarget.request eq scope.request) &&
      (preimage.inputs eq scope.inputs) && (projectedTarget.inputs eq scope.inputs) &&
      (preimage.root eq root) && (projectedTarget.root eq root) &&
      (preimage.dataBudget eq dataBudget) && (projectedTarget.dataBudget eq dataBudget) &&
      preimage.image == MySqlOperationEffectImage.Before &&
      projectedTarget.image == MySqlOperationEffectImage.Target,
      "H1 phase fingerprint requires the same exact request/root/Full26 inputs/plan child and BEFORE/TARGET images")
    val request = scope.request
    val inputs = scope.inputs
    requireSourceChildReserveRequestForPhase(request, inputs, root, dataBudget)
    val requestBytes = request.canonical
    requireSourceChildReserveOwnedBinary(requestBytes.borrowedBinary, root, dataBudget,
      "H1 phase fingerprint request")
    val scopeBytes = if requirePlanDataChild then encodeSourceChildReserveScopeData(scope, root, dataBudget)
      else sourceChildReserveScopeDataCanonical(scope, root, dataBudget, requirePlanDataChild = false)
    requireSourceChildReserveOwnedBinary(scopeBytes.borrowedBinary, root, dataBudget,
      "H1 phase fingerprint exact identity scope")
    val preimageBytes = sourceChildReserveEffectDataCanonical(preimage, root, dataBudget, requirePlanDataChild)
    requireSourceChildReserveOwnedBinary(preimageBytes.borrowedBinary, root, dataBudget,
      "H1 phase fingerprint BEFORE preimage")
    val targetBytes = sourceChildReserveEffectDataCanonical(projectedTarget, root, dataBudget, requirePlanDataChild)
    requireSourceChildReserveOwnedBinary(targetBytes.borrowedBinary, root, dataBudget,
      "H1 phase fingerprint projected TARGET")
    val operation = sourceChildReserveCurrentOperationRow(projectedTarget,
      "H1 phase fingerprint projected TARGET")
    require(sourceChildReserveEffectColumnValue(operation, inputs.forTable(SourceChildReserveOperationTable),
      "context_encoding", "H1 projected TARGET current O") ==
        MySqlFormat3Cell.Text(SourceChildReserveOperationContextEncoding),
      "H1 projected TARGET current O must retain the original generic context encoding")
    requireSourceChildReserveEffectPayload(projectedTarget, operation,
      "request_identity_bytes", "request_identity_sha256", requestBytes,
      "H1 projected TARGET current O request")
    requireSourceChildReserveEffectPayload(projectedTarget, operation,
      "scope_bytes", "scope_sha256", scopeBytes,
      "H1 projected TARGET current O scope")
    requireSourceChildReserveEffectPayload(projectedTarget, operation,
      "preimage_bytes", "preimage_sha256", preimageBytes,
      "H1 projected TARGET current O preimage")

  private def writeSourceChildReservePhaseFingerprintCanonical(output: DataOutputStream,
      scope: SourceChildReserveScopeData, preimage: SourceChildReserveEffectData,
      projectedTarget: SourceChildReserveEffectData): Unit =
    token(output, SourceChildReserveFingerprintDomain)
    uuid(output, scope.request.binding)
    uuid(output, scope.request.childWorkflow)
    uuid(output, scope.request.reserveOperation)
    writeSourceChildReserveCanonicalFrame(output, scope.request.canonical)
    writeSourceChildReserveCanonicalFrame(output, scope.canonical)
    writeSourceChildReserveCanonicalFrame(output, preimage.canonical)
    writeSourceChildReserveCanonicalFrame(output, projectedTarget.canonical)

  private def newSourceChildReservePhaseFingerprintData(scope: SourceChildReserveScopeData,
      preimage: SourceChildReserveEffectData, projectedTarget: SourceChildReserveEffectData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      canonical: MySqlRecoveryCanonicalBytes): SourceChildReservePhaseFingerprintData =
    dataBudget.reserveRetainedMemory(384L, "H1 phase fingerprint DATA before allocation")
    try new SourceChildReservePhaseFingerprintData(scope, preimage, projectedTarget, root, dataBudget, canonical)
    catch
      case cause: Throwable =>
        try dataBudget.releaseRetainedMemory(384L, "H1 phase fingerprint DATA wrapper allocation failure")
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
        throw cause

  private def requireSourceChildReservePhaseFingerprintData(fingerprint: SourceChildReservePhaseFingerprintData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): Unit =
    require(fingerprint != null && (fingerprint.root eq root) && (fingerprint.dataBudget eq dataBudget) &&
      fingerprint.canonical != null && fingerprint.canonical.encoding == SourceChildReserveFingerprintDomain &&
      (fingerprint.canonical.borrowedBinary.storageBudget eq dataBudget),
      "H1 phase fingerprint DATA must retain its exact root and canonical plan child")
    requireSourceChildReservePhaseInputs(fingerprint.scope, fingerprint.preimage,
      fingerprint.projectedTarget, root, dataBudget, requirePlanDataChild)
    withSourceChildReserveTemporary(dataBudget, 256L, "H1 phase fingerprint canonical comparison") {
      require(fingerprint.canonical.matchesWrittenBytes(output =>
        writeSourceChildReservePhaseFingerprintCanonical(output, fingerprint.scope,
          fingerprint.preimage, fingerprint.projectedTarget)),
        "typed H1 phase fingerprint differs from its exact request/scope/BEFORE/TARGET bytes")
    }

  /** H1 O.operation_fingerprint 的完整阶段 DATA；不填充任何 TARGET 派生列。 */
  private[mysql] def sourceChildReservePhaseFingerprintData(scope: SourceChildReserveScopeData,
      preimage: SourceChildReserveEffectData, projectedTarget: SourceChildReserveEffectData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): SourceChildReservePhaseFingerprintData =
    requireSourceChildReservePhaseInputs(scope, preimage, projectedTarget, root, dataBudget, requirePlanDataChild)
    val canonical = MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      MySqlRecoveryCanonicalBytes.encode(SourceChildReserveFingerprintDomain, root.profile) { output =>
        writeSourceChildReservePhaseFingerprintCanonical(output, scope, preimage, projectedTarget)
      }
    }
    val result = newSourceChildReservePhaseFingerprintData(scope, preimage,
      projectedTarget, root, dataBudget, canonical)
    requireSourceChildReservePhaseFingerprintData(result, root, dataBudget, requirePlanDataChild)
    result

  private[mysql] def encodeSourceChildReservePhaseFingerprintData(
      fingerprint: SourceChildReservePhaseFingerprintData, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    requireSourceChildReservePhaseFingerprintData(fingerprint, root, dataBudget)
    fingerprint.canonical

  /** 严格解码并逐字重编码完整 H1 phase fingerprint，拒绝错绑、非规范帧与尾部字节。 */
  private[mysql] def decodeSourceChildReservePhaseFingerprintData(input: MySqlFormat3Binary,
      scope: SourceChildReserveScopeData, preimage: SourceChildReserveEffectData,
      projectedTarget: SourceChildReserveEffectData, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): SourceChildReservePhaseFingerprintData =
    requireSourceChildReservePhaseInputs(scope, preimage, projectedTarget, root, dataBudget)
    requireSourceChildReserveOwnedBinary(input, root, dataBudget, "H1 phase fingerprint input")
    val request = scope.request
    val inputs = scope.inputs
    decodeData(input, root, dataBudget, Some(inputs)) { reader =>
      reader.domain(SourceChildReserveFingerprintDomain)
      require(reader.uuid() == request.binding && reader.uuid() == request.childWorkflow &&
        reader.uuid() == request.reserveOperation, "H1 phase fingerprint identity differs from exact R0 request")
      reader.dataFrame().requireExactBinary(request.canonical.borrowedBinary)
      reader.dataFrame().requireExactBinary(scope.canonical.borrowedBinary)
      reader.dataFrame().requireExactBinary(preimage.canonical.borrowedBinary)
      reader.dataFrame().requireExactBinary(projectedTarget.canonical.borrowedBinary)
      reader.finish()
    }
    val canonical = MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      MySqlRecoveryCanonicalBytes.encode(SourceChildReserveFingerprintDomain, root.profile) { output =>
        writeSourceChildReservePhaseFingerprintCanonical(output, scope, preimage, projectedTarget)
      }
    }
    require(canonical.exactlyMatchesCell(MySqlFormat3Cell.Binary(input)),
      "noncanonical or trailing H1 phase fingerprint DATA")
    val result = newSourceChildReservePhaseFingerprintData(scope, preimage,
      projectedTarget, root, dataBudget, canonical)
    requireSourceChildReservePhaseFingerprintData(result, root, dataBudget)
    result

  private def sourceChildReserveExpectedVersion(request: SourceChildReserveRequestData,
      entry: SourceChildReserveResourceExpectation, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget): Long =
    entry.before match
      case None => 0L
      case Some(row) =>
        validateSourceChildReservationRow(row, entry.resource, request.binding,
          request.childWorkflow, request.reserveOperation, inputs, root)
        sourceReserveRowCell(row, sourceChildReservationShape(inputs, root), "reservation_revision") match
          case MySqlFormat3Cell.SignedInteger(value) if value >= 0L => value
          case _ => throw new SQLDataException("H1 original context expectedVersion requires the full nonnegative reservation_revision")

  private def sourceChildReserveExpectedEffects(fingerprint: SourceChildReservePhaseFingerprintData,
      root: MySqlFormat3ReadBudget): Vector[MySqlExpectedEffect] =
    val request = fingerprint.scope.request
    val inputs = fingerprint.scope.inputs
    request.resources.map { entry =>
      val resource = entry.resource
      require(resource != null, "H1 original context resource identity is required")
      val keyHashBytes = resource.keyHashSha256
      require(keyHashBytes != null && keyHashBytes.size == 32,
        "H1 original context target identity requires the exact full-key SHA-256 bytes")
      val keyHash = java.util.HexFormat.of().withLowerCase().formatHex(keyHashBytes.toArray)
      require(keyHash != null && keyHash.length == 64 && keyHash.forall(character =>
        (character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')),
        "H1 original context target identity requires the canonical lower-hex full-key hash")
      MySqlExpectedEffect("SOURCE_RESERVE/" + resource.resourceKind + "/" + keyHash,
        sourceChildReserveExpectedVersion(request, entry, inputs, root), fingerprint.canonical.sha256)
    }.sortBy(_.targetIdentity)

  private def writeSourceChildReserveOperationContext(output: DataOutputStream,
      context: MySqlOperationContext): Unit =
    MySqlModeOperationEffectCodec.writeOperationContext(output, context)

  private def newSourceChildReserveOriginalContextData(
      fingerprint: SourceChildReservePhaseFingerprintData, context: MySqlOperationContext,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      canonical: MySqlRecoveryCanonicalBytes): SourceChildReserveOriginalContextData =
    dataBudget.reserveRetainedMemory(320L, "H1 original operation context DATA before allocation")
    try new SourceChildReserveOriginalContextData(fingerprint, context, root, dataBudget, canonical)
    catch
      case cause: Throwable =>
        try dataBudget.releaseRetainedMemory(320L, "H1 original operation context wrapper allocation failure")
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
        throw cause

  private def requireSourceChildReserveOriginalContextData(originalContext: SourceChildReserveOriginalContextData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): Unit =
    require(originalContext != null && (originalContext.root eq root) &&
      (originalContext.dataBudget eq dataBudget) && originalContext.context != null &&
      originalContext.canonical != null &&
      originalContext.canonical.encoding == SourceChildReserveOperationContextEncoding &&
      (originalContext.canonical.borrowedBinary.storageBudget eq dataBudget),
      "H1 original operation context DATA must retain its exact root and canonical plan child")
    requireSourceChildReservePhaseFingerprintData(originalContext.fingerprint, root, dataBudget,
      requirePlanDataChild)
    val identity = originalContext.fingerprint.scope.contextIdentity
    val request = originalContext.fingerprint.scope.request
    val context = originalContext.context
    require(context.bootUuid == identity.tag.bootId && context.writerEpoch == identity.writerEpoch &&
      context.bindingUuid == request.binding && context.operationUuid == request.reserveOperation,
      "H1 original operation context must retain the exact captured boot/fence/binding/R0 identity")
    withSourceChildReserveTemporary(dataBudget,
      Math.addExact(512L, Math.multiplyExact(request.resources.size.toLong, 384L)),
      "H1 original context expected-effect comparison") {
      require(context.expectedEffects == sourceChildReserveExpectedEffects(originalContext.fingerprint, root),
        "H1 original operation context expected effects differ from exact request resources and before revisions")
    }
    require(context.canonicalFingerprintSha256 == originalContext.canonical.sha256 &&
      originalContext.canonical.matchesWrittenBytes(output => writeSourceChildReserveOperationContext(output, context)),
      "H1 original operation context canonical bytes/fingerprint differ from the exact generic writer")

  /** H1 context 使用原通用 FP_RECOVERY_OPERATION_BE_V1 writer，不复制 P/C 或 generic 编码规则。 */
  private[mysql] def sourceChildReserveOriginalContextData(
      fingerprint: SourceChildReservePhaseFingerprintData, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): SourceChildReserveOriginalContextData =
    requireSourceChildReservePhaseFingerprintData(fingerprint, root, dataBudget, requirePlanDataChild)
    val request = fingerprint.scope.request
    val identity = fingerprint.scope.contextIdentity
    val resourceCount = request.resources.size.toLong
    val retained = Math.addExact(512L, Math.multiplyExact(resourceCount, 512L))
    val temporary = Math.addExact(1024L, Math.multiplyExact(resourceCount, 384L))
    var retainedCharged = false
    var retainedTransferred = false
    var temporaryCharged = false
    var primary: Throwable = null
    try
      dataBudget.reserveRetainedMemory(retained, "H1 original context effects/identities before allocation")
      retainedCharged = true
      dataBudget.reserveTemporaryMemory(temporary, "H1 original context sort/digest workspace before allocation")
      temporaryCharged = true
      val effects = sourceChildReserveExpectedEffects(fingerprint, root)
      val context = MySqlOperationContext(identity.tag.bootId, identity.writerEpoch,
        request.binding, request.reserveOperation, effects)
      val canonical = MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
        MySqlModeOperationEffectCodec.operationContextCanonicalBytes(context, root.profile)
      }
      require(canonical.encoding == SourceChildReserveOperationContextEncoding &&
        (canonical.borrowedBinary.storageBudget eq dataBudget) &&
        context.canonicalFingerprintSha256 == canonical.sha256,
        "H1 original context generic codec and canonical context fingerprint must agree")
      val result = newSourceChildReserveOriginalContextData(fingerprint, context,
        root, dataBudget, canonical)
      requireSourceChildReserveOriginalContextData(result, root, dataBudget, requirePlanDataChild)
      retainedTransferred = true
      result
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      if retainedCharged && !retainedTransferred then
        try dataBudget.releaseRetainedMemory(retained, "failed H1 original context effects/identities")
        catch case cleanup: Throwable =>
          if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
      if temporaryCharged then
        try dataBudget.releaseTemporaryMemory(temporary, "H1 original context sort/digest workspace release")
        catch case cleanup: Throwable =>
          if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  private[mysql] def encodeSourceChildReserveOriginalContextData(
      originalContext: SourceChildReserveOriginalContextData, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    requireSourceChildReserveOriginalContextData(originalContext, root, dataBudget)
    originalContext.canonical

  /** 原 context 没有新 wire 或自制 decoder；按完整原 helper 结果逐字验证输入。 */
  private[mysql] def decodeSourceChildReserveOriginalContextData(input: MySqlFormat3Binary,
      fingerprint: SourceChildReservePhaseFingerprintData, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): SourceChildReserveOriginalContextData =
    requireSourceChildReserveOwnedBinary(input, root, dataBudget, "H1 original context input")
    val decoded = sourceChildReserveOriginalContextData(fingerprint, root, dataBudget, requirePlanDataChild)
    require(input == decoded.canonical.borrowedBinary,
      "H1 original context input differs from its complete generic FP_RECOVERY_OPERATION_BE_V1 re-encoding")
    decoded

  private def sourceChildReserveProjectedResultAt(target: SourceChildReserveEffectData): LocalDateTime =
    val row = sourceChildReserveCurrentOperationRow(target, "H1 witness projected TARGET")
    sourceChildReserveEffectColumnValue(row, target.inputs.forTable(SourceChildReserveOperationTable),
      "result_at", "H1 witness projected TARGET current O") match
      case MySqlFormat3Cell.DateTime(value) => value
      case _ => throw new SQLDataException("H1 witness requires the real current R0 O.result_at DateTime Cell")

  private def requireSourceChildReserveWitnessInputs(originalContext: SourceChildReserveOriginalContextData,
      resultAt: LocalDateTime, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, requirePlanDataChild: Boolean = true): Unit =
    requireSourceChildReserveOriginalContextData(originalContext, root, dataBudget, requirePlanDataChild)
    require(resultAt != null &&
      resultAt == sourceChildReserveProjectedResultAt(originalContext.fingerprint.projectedTarget),
      "H1 witness must use the caller-supplied real resultAt equal to projected TARGET current R0 O.result_at")

  private def writeSourceChildReserveWitnessCanonical(output: DataOutputStream,
      originalContext: SourceChildReserveOriginalContextData, resultAt: LocalDateTime): Unit =
    val fingerprint = originalContext.fingerprint
    val scope = fingerprint.scope
    token(output, SourceChildReserveWitnessDomain)
    uuid(output, scope.request.binding)
    uuid(output, scope.request.childWorkflow)
    uuid(output, scope.request.reserveOperation)
    cell(output, MySqlFormat3Cell.DateTime(resultAt))
    writeSourceChildReserveCanonicalFrame(output, scope.canonical)
    writeSourceChildReserveCanonicalFrame(output, fingerprint.canonical)
    writeSourceChildReserveCanonicalFrame(output, originalContext.canonical)
    writeSourceChildReserveCanonicalFrame(output, fingerprint.preimage.canonical)
    writeSourceChildReserveCanonicalFrame(output, fingerprint.projectedTarget.canonical)

  private def newSourceChildReserveWitnessData(originalContext: SourceChildReserveOriginalContextData,
      resultAt: LocalDateTime, root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      canonical: MySqlRecoveryCanonicalBytes): SourceChildReserveWitnessData =
    dataBudget.reserveRetainedMemory(320L, "H1 witness DATA before allocation")
    try new SourceChildReserveWitnessData(originalContext, resultAt, root, dataBudget, canonical)
    catch
      case cause: Throwable =>
        try dataBudget.releaseRetainedMemory(320L, "H1 witness DATA wrapper allocation failure")
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
        throw cause

  private def requireSourceChildReserveWitnessData(witness: SourceChildReserveWitnessData,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): Unit =
    require(witness != null && (witness.root eq root) && (witness.dataBudget eq dataBudget) &&
      witness.canonical != null && witness.canonical.encoding == SourceChildReserveWitnessDomain &&
      (witness.canonical.borrowedBinary.storageBudget eq dataBudget),
      "H1 witness DATA must retain its exact original root and canonical plan child")
    requireSourceChildReserveWitnessInputs(witness.originalContext, witness.resultAt, root, dataBudget,
      requirePlanDataChild)
    withSourceChildReserveTemporary(dataBudget, 256L, "H1 witness canonical comparison") {
      require(witness.canonical.matchesWrittenBytes(output =>
        writeSourceChildReserveWitnessCanonical(output, witness.originalContext, witness.resultAt)),
        "typed H1 witness differs from its full resultAt/scope/fingerprint/context/BEFORE/TARGET bytes")
    }

  /** H1 witness 只使用真实调用方 resultAt；不会构造成功占位、snapshot 或 SQL 许可。 */
  private[mysql] def sourceChildReserveWitnessData(
      originalContext: SourceChildReserveOriginalContextData, resultAt: LocalDateTime,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): SourceChildReserveWitnessData =
    requireSourceChildReserveWitnessInputs(originalContext, resultAt, root, dataBudget, requirePlanDataChild)
    val canonical = MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      MySqlRecoveryCanonicalBytes.encode(SourceChildReserveWitnessDomain, root.profile) { output =>
        writeSourceChildReserveWitnessCanonical(output, originalContext, resultAt)
      }
    }
    val result = newSourceChildReserveWitnessData(originalContext, resultAt, root, dataBudget, canonical)
    requireSourceChildReserveWitnessData(result, root, dataBudget, requirePlanDataChild)
    result

  private[mysql] def encodeSourceChildReserveWitnessData(witness: SourceChildReserveWitnessData,
      root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    requireSourceChildReserveWitnessData(witness, root, dataBudget)
    witness.canonical

  /** 严格解码 H1 witness；日期复用普通 Cell DATETIME(6) 的真实纳秒帧并完整重编码。 */
  private[mysql] def decodeSourceChildReserveWitnessData(input: MySqlFormat3Binary,
      originalContext: SourceChildReserveOriginalContextData, resultAt: LocalDateTime,
      root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget,
      requirePlanDataChild: Boolean = true): SourceChildReserveWitnessData =
    requireSourceChildReserveWitnessInputs(originalContext, resultAt, root, dataBudget,
      requirePlanDataChild)
    requireSourceChildReserveOwnedBinary(input, root, dataBudget, "H1 witness input")
    val fingerprint = originalContext.fingerprint
    val scope = fingerprint.scope
    val inputs = scope.inputs
    decodeData(input, root, dataBudget, Some(inputs)) { reader =>
      reader.domain(SourceChildReserveWitnessDomain)
      require(reader.uuid() == scope.request.binding && reader.uuid() == scope.request.childWorkflow &&
        reader.uuid() == scope.request.reserveOperation,
        "H1 witness binding/child/R0 identity differs from exact source reserve DATA")
      require(reader.sourceChildReserveWitnessDateTime(inputs) == resultAt,
        "H1 witness DateTime Cell differs from the exact caller-supplied resultAt")
      reader.dataFrame().requireExactBinary(scope.canonical.borrowedBinary)
      reader.dataFrame().requireExactBinary(fingerprint.canonical.borrowedBinary)
      reader.dataFrame().requireExactBinary(originalContext.canonical.borrowedBinary)
      reader.dataFrame().requireExactBinary(fingerprint.preimage.canonical.borrowedBinary)
      reader.dataFrame().requireExactBinary(fingerprint.projectedTarget.canonical.borrowedBinary)
      reader.finish()
    }
    val canonical = MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      MySqlRecoveryCanonicalBytes.encode(SourceChildReserveWitnessDomain, root.profile) { output =>
        writeSourceChildReserveWitnessCanonical(output, originalContext, resultAt)
      }
    }
    require(canonical.exactlyMatchesCell(MySqlFormat3Cell.Binary(input)),
      "noncanonical or trailing H1 witness DATA")
    val result = newSourceChildReserveWitnessData(originalContext, resultAt, root, dataBudget, canonical)
    requireSourceChildReserveWitnessData(result, root, dataBudget, requirePlanDataChild)
    result

  /** Strict R0 historical candidate DATA. Recovery must still prove the anchored O/W chain and earlier RELEASED edges. */
  private[mysql] def decodeHistoricalSourceChildReserveReceipt(raw: Vector[MySqlFormat3Cell],
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): HistoricalSourceChildReserveReceiptData =
    requireSourceDataBudget(root, dataBudget)
    require(raw != null && inputs != null &&
      inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      (inputs.sourceLimitProfile eq root.profile),
      "historical H1 receipt requires its complete original O row, Full26 inputs and root")
    root.requireScriptDataInputs(inputs)
    MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(raw, root.profile, root, Some(inputs))
    val operationShape = inputs.forTable(SourceChildReserveOperationTable)
    require(raw.size == operationShape.columns.size && operationShape.primaryKey == Vector("binding_uuid", "operation_uuid"),
      "historical H1 receipt must be the exact full26 O row with its original two-column primary key")
    def cell(name: String): MySqlFormat3Cell = MySqlModeOperationEffectCodec.value(raw,
      SourceChildReserveOperationTable, name)
    def binary(name: String): MySqlFormat3Binary = cell(name) match
      case MySqlFormat3Cell.Binary(value) => value
      case _ => throw new SQLDataException("historical H1 O binary field is absent: " + name)
    def digest(name: String, expected: Vector[Byte]): Unit =
      require(expected != null && expected.size == 32 && (cell(name) match
        case MySqlFormat3Cell.Binary(value) => value.equalsVector(expected)
        case _ => false), "historical H1 O digest differs from its recomputed canonical payload: " + name)
    def owned(value: MySqlFormat3Binary, label: String): MySqlFormat3Binary =
      requireSourceChildBinaryOwner(value, root, label)
      MySqlFormat3Binary.takeOwned(value.copyBytes(dataBudget, label), dataBudget)
    def uuid(name: String): UUID = cell(name) match
      case MySqlFormat3Cell.Binary(value) if value.length == 16 =>
        var high = 0L; var low = 0L; var index = 0
        while index < 8 do
          high = (high << 8) | (value.byteAt(index).toLong & 255L)
          low = (low << 8) | (value.byteAt(index + 8).toLong & 255L)
          index += 1
        new UUID(high, low)
      case _ => throw new SQLDataException("historical H1 O UUID field is absent: " + name)
    require(uuid("binding_uuid") != new UUID(0L, 0L) &&
      cell("step") == MySqlFormat3Cell.Text("RESERVE") &&
      cell("operation_state") == MySqlFormat3Cell.Text("COMMITTED") &&
      cell("reserve_operation_uuid") == MySqlFormat3Cell.SqlNull &&
      cell("context_encoding") == MySqlFormat3Cell.Text(SourceChildReserveOperationContextEncoding),
      "historical H1 receipt must be a committed standalone generic-context R0 RESERVE row")

    val requestBytes = owned(binary("request_identity_bytes"), "historical H1 O request identity bytes")
    val request = decodeSourceChildReserveRequestData(requestBytes, inputs, root, dataBudget)
    require(uuid("binding_uuid") == request.binding && uuid("workflow_uuid") == request.childWorkflow &&
      uuid("operation_uuid") == request.reserveOperation &&
      (cell("request_identity_sha256") match
        case MySqlFormat3Cell.Binary(value) => value.equalsVector(request.canonical.sha256)
        case _ => false),
      "historical H1 O primary identity or request digest differs from its exact decoded R0 request")
    digest("request_identity_sha256", request.canonical.sha256)

    val scope = decodeHistoricalSourceChildReserveScope(
      owned(binary("scope_bytes"), "historical H1 O scope bytes"), request, inputs, root, dataBudget)
    digest("scope_sha256", scope.typedScope.canonical.sha256)
    val preimage = decodeHistoricalSourceChildReserveEffectData(
      owned(binary("preimage_bytes"), "historical H1 O BEFORE effect bytes"), request, inputs,
      MySqlOperationEffectImage.Before, root, dataBudget)
    digest("preimage_sha256", preimage.canonical.sha256)
    val projectedTarget = decodeHistoricalSourceChildReserveEffectData(
      owned(binary("target_bytes"), "historical H1 O projected TARGET bytes"), request, inputs,
      MySqlOperationEffectImage.Target, root, dataBudget)
    val fingerprint = sourceChildReservePhaseFingerprintData(scope.typedScope, preimage,
      projectedTarget, root, dataBudget, requirePlanDataChild = false)
    digest("operation_fingerprint", fingerprint.canonical.sha256)
    val originalContext = decodeSourceChildReserveOriginalContextData(
      owned(binary("original_context_bytes"), "historical H1 O original context bytes"),
      fingerprint, root, dataBudget, requirePlanDataChild = false)
    digest("original_context_sha256", originalContext.canonical.sha256)
    val resultAt = cell("result_at") match
      case MySqlFormat3Cell.DateTime(value) => value
      case _ => throw new SQLDataException("historical H1 O result_at must retain its exact DateTime(6) value")
    val witness = decodeSourceChildReserveWitnessData(
      owned(binary("commit_witness_bytes"), "historical H1 O commit witness bytes"),
      originalContext, resultAt, root, dataBudget, requirePlanDataChild = false)
    digest("commit_witness_sha256", witness.canonical.sha256)
    digest("target_sha256", projectedTarget.canonical.sha256)
    dataBudget.reserveRetainedMemory(512L, "historical H1 source reserve receipt candidate before allocation")
    new HistoricalSourceChildReserveReceiptData(raw, scope, request, preimage,
      projectedTarget, fingerprint, originalContext, witness, root, dataBudget)

  def sourceWriteObservationBinding(phase: String, observationId: UUID, bootId: UUID,
      bootGeneration: Long, connectionGeneration: Long, domainSessionId: UUID,
      domainObservedGeneration: Long, domainRegistryGeneration: Long, saveNamespace: String,
      loaderNamespace: String, dimension: Int, providerType: String, dimensionDirectory: String,
      regionDirectory: String, completeCensusEpoch: Option[Long], observerGeneration: Long,
      scope: DecodedSourceWriteScopeRequest, root: MySqlFormat3ReadBudget): SourceWriteObservationBindingData =
    require(scope != null && root != null && !root.isClosed && !root.isScopedReadBudget &&
      scope.carrierKey.exactProfile == root.profile,
      "original typed source scope and Full26 root required for the observation binding")
    require(phase == "LOADED" || phase == "COLD", "source observation phase must be explicitly LOADED or COLD")
    require(MySqlNormalRecallRequest.nonZero(observationId) && MySqlNormalRecallRequest.nonZero(bootId) &&
      bootGeneration > 0L && connectionGeneration > 0L && MySqlNormalRecallRequest.nonZero(domainSessionId) &&
      domainObservedGeneration >= 0L && domainRegistryGeneration >= 0L && observerGeneration >= 0L &&
      completeCensusEpoch != null && completeCensusEpoch.forall(_ >= 0L),
      "source observation requires exact original boot/domain generations and explicit census Option")
    require(phase != "COLD" || completeCensusEpoch.nonEmpty,
      "COLD observation requires its complete original census epoch")
    require(saveNamespace != null && loaderNamespace != null &&
      saveNamespace == scope.saveIdentity && loaderNamespace == scope.loaderIdentity &&
      providerType != null && providerType.nonEmpty,
      "source namespaces/provider must match the complete original CARRIER identity")
    requireCanonicalSourceDirectory(dimensionDirectory, "dimension root")
    requireCanonicalSourceDirectory(regionDirectory, "region root")
    val dimensionPath = Path.of(dimensionDirectory)
    require(Path.of(regionDirectory).startsWith(dimensionPath),
      "verified region root must remain below its exact dimension root")
    root.reserveRetainedMemory(Math.addExact(512L, Math.multiplyExact(
      (saveNamespace.length.toLong + loaderNamespace.length.toLong + providerType.length.toLong +
        dimensionDirectory.length.toLong + regionDirectory.length.toLong), 4L)),
      "typed source observer binding DATA")
    new SourceWriteObservationBindingData(phase, observationId, bootId, bootGeneration,
      connectionGeneration, domainSessionId, domainObservedGeneration, domainRegistryGeneration,
      saveNamespace, loaderNamespace, dimension, providerType, dimensionDirectory, regionDirectory,
      completeCensusEpoch, observerGeneration)

  private def requireCanonicalSourceDirectory(value: String, label: String): Unit =
    require(value != null && value.nonEmpty, "source " + label + " is required")
    val path = Path.of(value)
    require(path.isAbsolute && path.normalize().toString == value,
      "source " + label + " must be a normalized absolute path")

  private def requireSourceObservationBinding(scope: DecodedSourceWriteScopeRequest,
      observer: SourceWriteObservationBindingData, root: MySqlFormat3ReadBudget): Unit =
    require(scope != null && observer != null && root != null && !root.isClosed && !root.isScopedReadBudget &&
      scope.carrierKey.exactProfile == root.profile &&
      (observer.phase == "LOADED" || observer.phase == "COLD") &&
      MySqlNormalRecallRequest.nonZero(observer.observationId) && MySqlNormalRecallRequest.nonZero(observer.bootId) &&
      observer.bootGeneration > 0L && observer.connectionGeneration > 0L &&
      MySqlNormalRecallRequest.nonZero(observer.domainSessionId) && observer.domainObservedGeneration >= 0L &&
      observer.domainRegistryGeneration >= 0L && observer.observerGeneration >= 0L &&
      observer.completeCensusEpoch != null && observer.completeCensusEpoch.forall(_ >= 0L) &&
      (observer.phase != "COLD" || observer.completeCensusEpoch.nonEmpty) &&
      observer.saveNamespace == scope.saveIdentity && observer.loaderNamespace == scope.loaderIdentity &&
      observer.providerType != null && observer.providerType.nonEmpty,
      "typed LOADED/COLD observer binding differs from the original source scope")
    requireCanonicalSourceDirectory(observer.dimensionDirectory, "dimension root")
    requireCanonicalSourceDirectory(observer.regionDirectory, "region root")
    require(Path.of(observer.regionDirectory).startsWith(Path.of(observer.dimensionDirectory)),
      "verified region root escaped its exact dimension root")

  private def writeSourceObservationBinding(output: DataOutputStream,
      observer: SourceWriteObservationBindingData): Unit =
    output.writeByte(if observer.phase == "LOADED" then 1 else 2)
    uuid(output, observer.observationId); uuid(output, observer.bootId)
    output.writeLong(observer.bootGeneration); output.writeLong(observer.connectionGeneration)
    uuid(output, observer.domainSessionId); output.writeLong(observer.domainObservedGeneration)
    output.writeLong(observer.domainRegistryGeneration)
    text(output, observer.saveNamespace); text(output, observer.loaderNamespace)
    output.writeInt(observer.dimension); text(output, observer.providerType)
    text(output, observer.dimensionDirectory); text(output, observer.regionDirectory)
    output.writeByte(if observer.completeCensusEpoch.isDefined then 1 else 0)
    observer.completeCensusEpoch.foreach(output.writeLong)
    output.writeLong(observer.observerGeneration)

  def encodeSourceWriteCaptureContext(scope: DecodedSourceWriteScopeRequest,
      observer: SourceWriteObservationBindingData, root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    requireSourceObservationBinding(scope, observer, root)
    require(scope.canonical.encoding == "FP_SOURCE_WRITE_SCOPE_BE_V1",
      "capture context must embed the complete original SOURCE_WRITE_SCOPE_BE_V1 frame")
    encode("FP_SOURCE_WRITE_CAPTURE_CONTEXT_BE_V1", root) { output =>
      output.writeInt(scope.canonical.length); scope.canonical.writeTo(output)
      writeSourceObservationBinding(output, observer)
    }

  def sourceWriteCaptureContext(scope: DecodedSourceWriteScopeRequest,
      observer: SourceWriteObservationBindingData, root: MySqlFormat3ReadBudget): DecodedSourceWriteCaptureContext =
    val canonical = encodeSourceWriteCaptureContext(scope, observer, root)
    new DecodedSourceWriteCaptureContext(scope, observer, canonical)

  /** Strict cold DATA decode: requires the independently typed original BE_V1 child scope. */
  def decodeSourceWriteCaptureContext(input: MySqlFormat3Binary,
      expectedScope: DecodedSourceWriteScopeRequest, root: MySqlFormat3ReadBudget): DecodedSourceWriteCaptureContext =
    require(input != null && expectedScope != null && root != null && !root.isClosed && !root.isScopedReadBudget,
      "source capture context, exact typed BE_V1 scope and retained Full26 root are required")
    require(expectedScope.canonical.encoding == "FP_SOURCE_WRITE_SCOPE_BE_V1",
      "capture context decoder requires the exact original SOURCE_WRITE_SCOPE_BE_V1 frame")
    val decoded = decodeData(input, root) { reader =>
      reader.domain("FP_SOURCE_WRITE_CAPTURE_CONTEXT_BE_V1")
      reader.dataFrame().requireExactBinary(expectedScope.canonical.borrowedBinary)
      val phase = reader.octet() match
        case 1 => "LOADED"
        case 2 => "COLD"
        case _ => throw new SQLDataException("unknown typed source observer phase")
      val observationId = reader.uuid(); val bootId = reader.uuid()
      val bootGeneration = reader.int64(); val connectionGeneration = reader.int64()
      val domainSessionId = reader.uuid(); val domainObservedGeneration = reader.int64()
      val domainRegistryGeneration = reader.int64()
      val saveNamespace = reader.textData(); val loaderNamespace = reader.textData()
      val dimension = reader.int32(); val providerType = reader.textData()
      val dimensionDirectory = reader.textData(); val regionDirectory = reader.textData()
      val censusEpoch = reader.octet() match
        case 0 => None
        case 1 => Some(reader.int64())
        case _ => throw new SQLDataException("noncanonical source census epoch Option")
      val observerGeneration = reader.int64()
      val observer = sourceWriteObservationBinding(phase, observationId, bootId, bootGeneration,
        connectionGeneration, domainSessionId, domainObservedGeneration, domainRegistryGeneration,
        saveNamespace, loaderNamespace, dimension, providerType, dimensionDirectory, regionDirectory,
        censusEpoch, observerGeneration, expectedScope, root)
      new DecodedSourceWriteCaptureContext(expectedScope, observer, null)
    }
    val canonical = encodeSourceWriteCaptureContext(decoded.scope, decoded.observer, root)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      require(canonical.exactlyMatchesCell(MySqlFormat3Cell.Binary(input)),
        "noncanonical source capture context DATA")
    }
    new DecodedSourceWriteCaptureContext(decoded.scope, decoded.observer, canonical)

  def encodeSourceWriteScope(write: DecodedSourceWrite, parentRecallWorkflow: UUID,
      parentP2Operation: UUID, units: Vector[SourceWriteUnitStamp],
      obligations: Vector[SourceWriteObligationStamp], root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    require(write != null, "exact decoded source-write row required")
    val carrierIdentity = sourceWriteCarrierIdentity(write.resourceKey, write.saveIdentity,
      write.loaderIdentity, write.regionIdentity, write.slotX, write.slotZ, root)
    requireSourceWriteScopeData(write.binding, write.workflow, write.write, carrierIdentity,
      write.writeGeneration, write.obligationRevisionData.values, parentRecallWorkflow,
      parentP2Operation, units, obligations, root)
    encodeSourceWriteScopeRequest(sourceWriteScopeRequest(write.binding, write.workflow,
      parentRecallWorkflow, parentP2Operation, write.write, carrierIdentity,
      write.writeGeneration, units, obligations, root), root)

  /** Decode requires the exact immutable SQL write row so the CARRIER key is never hash-only decoded. */
  def decodeSourceWriteScopeRequest(input: MySqlFormat3Binary, binding: UUID, childWorkflow: UUID,
      parentRecallWorkflow: UUID, parentP2Operation: UUID, writeUuid: UUID,
      carrierIdentity: SourceWriteCarrierIdentityData, writeGeneration: Long,
      root: MySqlFormat3ReadBudget): DecodedSourceWriteScopeRequest =
    decodeSourceWriteScopeRequestInternal(input, binding, childWorkflow,
      Some((parentRecallWorkflow, parentP2Operation)), writeUuid, carrierIdentity, writeGeneration, root)

  private def decodeSourceWriteScopeRequestInternal(input: MySqlFormat3Binary, expectedBinding: UUID,
      expectedChildWorkflow: UUID, expectedParent: Option[(UUID, UUID)], expectedWriteUuid: UUID,
      expectedCarrierIdentity: SourceWriteCarrierIdentityData, expectedWriteGeneration: Long,
      root: MySqlFormat3ReadBudget): DecodedSourceWriteScopeRequest =
    require(input != null && root != null && !root.isClosed,
      "source-write nested scope request and original root required")
    require(MySqlNormalRecallRequest.nonZero(expectedBinding) && MySqlNormalRecallRequest.nonZero(expectedChildWorkflow) &&
      expectedParent != null && expectedParent.forall((parent, p2) =>
        MySqlNormalRecallRequest.nonZero(parent) && MySqlNormalRecallRequest.nonZero(p2)) &&
      MySqlNormalRecallRequest.nonZero(expectedWriteUuid) && expectedCarrierIdentity != null &&
      expectedCarrierIdentity.resourceKey != null && expectedCarrierIdentity.resourceKey.resourceKind == "CARRIER" &&
      expectedCarrierIdentity.resourceKey.exactProfile == root.profile &&
      expectedWriteGeneration >= 0L,
      "exact source-write request lineage and CARRIER identity required")
    decodeData(input, root) { reader =>
      reader.domain("FP_SOURCE_WRITE_SCOPE_BE_V1")
      val decodedBinding = reader.uuid(); val decodedWorkflow = reader.uuid(); val parentWorkflow = reader.uuid()
      val parentP2 = reader.uuid(); val decodedWriteUuid = reader.uuid()
      val keyFrame = reader.dataFrame()
      keyFrame.requireExactBinary(expectedCarrierIdentity.canonicalBytes)
      val generation = reader.int64()
      val profileSha = Vector.fill(32)(reader.octet().toByte)
      val unitCount = reader.int32()
      require(unitCount > 0 && unitCount.toLong <= root.profile.maxMembers(),
        "source-write UnitStamp count exceeds the original profile")
      root.reserveRetainedMemory(Math.addExact(512L, Math.multiplyExact(unitCount.toLong, 128L)),
        "decoded source-write UnitStamp vector")
      val units = Vector.newBuilder[SourceWriteUnitStamp]
      var previousUnit: SourceWriteUnitStamp = null
      var unitIndex = 0
      while unitIndex < unitCount do
        val item = new SourceWriteUnitStamp(reader.uuid(), reader.int64(), reader.int64(), reader.uuid())
        require(MySqlNormalRecallRequest.nonZero(item.unit) && item.unitRevision >= 0L && item.epoch >= 0L &&
          MySqlNormalRecallRequest.nonZero(item.currentForest) &&
          (previousUnit == null || compareSourceWriteUnit(previousUnit, item) < 0),
          "decoded source-write UnitStamps are invalid, duplicated or unsorted")
        units += item; previousUnit = item; unitIndex += 1
      val obligationCount = reader.int32()
      require(obligationCount >= 0 && obligationCount.toLong <= root.profile.maxMembers(),
        "source-write obligation stamp count exceeds the original profile")
      root.reserveRetainedMemory(Math.addExact(512L, Math.multiplyExact(obligationCount.toLong, 256L)),
        "decoded source-write obligation stamp vector")
      val obligations = Vector.newBuilder[SourceWriteObligationStamp]
      var previousObligation: SourceWriteObligationStamp = null
      var obligationIndex = 0
      while obligationIndex < obligationCount do
        val unit = reader.uuid(); val forest = reader.uuid(); val kind = reader.textData()
        val keyFrame = reader.dataFrame()
        keyFrame.requireExactBinary(expectedCarrierIdentity.canonicalBytes)
        val purpose = reader.textData(); val revision = reader.int64(); val state = reader.textData()
        val itemKey = new SourceObligationRevision(expectedBinding, unit, forest,
          expectedCarrierIdentity.resourceKey.keyHashSha256, expectedCarrierIdentity.canonicalBytes, purpose, revision)
        val item = new SourceWriteObligationStamp(itemKey, state)
        require(kind == "CARRIER" && revision >= 0L &&
          (state == "OPEN" || state == "DISCHARGED") &&
          (previousObligation == null || compareSourceWriteObligation(previousObligation, item) < 0),
          "decoded source-write obligation stamps are invalid, duplicated or unsorted")
        obligations += item; previousObligation = item; obligationIndex += 1
      reader.finish()
      val unitValues = units.result(); val obligationValues = obligations.result()
      require(decodedBinding == expectedBinding && decodedWorkflow == expectedChildWorkflow &&
        decodedWriteUuid == expectedWriteUuid && generation == expectedWriteGeneration &&
        profileSha == root.profile.sha256().toVector &&
        expectedCarrierIdentity.resourceKey.exactProfile == root.profile,
        "source-write nested scope header differs from its exact immutable row/profile")
      require(expectedParent.forall((parent, p2) => parentWorkflow == parent && parentP2 == p2),
        "source-write nested scope parent lineage differs from its exact request")
      requireSourceWriteScopeData(decodedBinding, decodedWorkflow, decodedWriteUuid, expectedCarrierIdentity, generation,
        obligationValues.map(_.key), parentWorkflow, parentP2, unitValues, obligationValues, root)
      val canonical = encode("FP_SOURCE_WRITE_SCOPE_BE_V1", root) { output =>
        writeSourceWriteScope(output, decodedBinding, decodedWorkflow, decodedWriteUuid, expectedCarrierIdentity, generation,
          parentWorkflow, parentP2, unitValues, obligationValues, root)
      }
      var matches = false
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        matches = canonical.exactlyMatchesCell(MySqlFormat3Cell.Binary(input))
      }
      require(matches, "source-write nested scope does not round-trip to its exact canonical bytes")
      new DecodedSourceWriteScopeRequest(decodedBinding, decodedWorkflow, parentWorkflow, parentP2,
        decodedWriteUuid, expectedCarrierIdentity, generation, unitValues, obligationValues, canonical)
    }

  def decodeSourceWriteScope(input: MySqlFormat3Binary, write: DecodedSourceWrite,
      root: MySqlFormat3ReadBudget): DecodedSourceWriteScope =
    require(write != null && root != null && !root.isClosed, "exact decoded source-write row required")
    val carrierIdentity = sourceWriteCarrierIdentity(write.resourceKey, write.saveIdentity,
      write.loaderIdentity, write.regionIdentity, write.slotX, write.slotZ, root)
    val decoded = decodeSourceWriteScopeRequestInternal(input, write.binding, write.workflow,
      None, write.write, carrierIdentity, write.writeGeneration, root)
    require(decoded.obligations.size == write.obligationRevisionData.values.size &&
      decoded.obligations.map(_.key).zip(write.obligationRevisionData.values).forall { (actual, expected) =>
        actual.binding == expected.binding && actual.unit == expected.unit && actual.forest == expected.forest &&
          actual.carrierKeyHash == expected.carrierKeyHash && actual.carrierKey.compareTo(expected.carrierKey) == 0 &&
          actual.purpose == expected.purpose && actual.revision == expected.revision
      }, "source-write nested scope differs from its exact immutable obligation PK/revision set")
    new DecodedSourceWriteScope(decoded.binding, decoded.childWorkflow, decoded.parentRecallWorkflow,
      decoded.parentP2Operation, decoded.write, decoded.carrierIdentity, decoded.writeGeneration,
      decoded.units, decoded.obligations, decoded.canonical)

  /**
   * 在原SQL root 与 source allocation account 下同步核验 transfer forest/index；consumer 返回后图视图即失效。
   * consumer 若需保留图或 index，必须先通过同一 source owner account 预留其完整保留费用。
   */
  def consumeVerifiedSourceTransferForest(forestBytes: MySqlFormat3Binary, forestSha256: MySqlFormat3Binary,
      indexBytes: MySqlFormat3Binary, indexSha256: MySqlFormat3Binary,
      expected: com.wi.floatingpod.server.source.SourceForestMembershipCodec.ForestExpectation,
      allocation: com.wi.floatingpod.server.source.SourceForestAllocationBudget,
      root: MySqlFormat3ReadBudget)(consumer: java.util.function.BiConsumer[
        com.wi.floatingpod.server.source.SourceForestCodec.Encoded,
        com.wi.floatingpod.server.source.SourceForestMembershipCodec.IndexData]): Unit =
    requireCanonicalRoot(root)
    require(forestBytes != null && forestSha256 != null && indexBytes != null && indexSha256 != null &&
      expected != null && allocation != null && allocation.exactProfile() == root.profile && consumer != null &&
      forestSha256.length == 32 && indexSha256.length == 32,
      "exact transfer Full26 forest/index identities and source owner allocation account required")
    val limit = Math.min(Math.min(root.profile.maxSqlPacketBytes(), root.profile.maxRetainedBytes()),
      MySqlFormat3JvmArrayLimit.MaximumLength)
    require(forestBytes.length > 0 && indexBytes.length > 0 &&
      forestBytes.length.toLong <= limit && indexBytes.length.toLong <= limit,
      "transfer forest/index bytes exceed the exact original profile")
    val workspace = Math.addExact(512L, Math.addExact(
      Math.addExact(forestBytes.length.toLong, indexBytes.length.toLong), 64L))
    root.reserveTemporaryMemory(workspace, "Full26 transfer forest/index handoff arrays before allocation")
    var primary: Throwable = null
    try
      def copy(value: MySqlFormat3Binary, context: String): Array[Byte] =
        val result = new Array[Byte](MySqlFormat3JvmArrayLimit.requireLength(value.length.toLong, context))
        var index = 0
        while index < result.length do
          result(index) = value.byteAt(index); index += 1
        result
      val forest = copy(forestBytes, "transfer full forest")
      val forestSha = copy(forestSha256, "transfer forest SHA-256")
      val index = copy(indexBytes, "transfer full membership index")
      val indexSha = copy(indexSha256, "transfer membership-index SHA-256")
      com.wi.floatingpod.server.source.SourceForestMembershipCodec.consumeVerifiedGraph(
        forest, forestSha, index, indexSha, root.profile, allocation, expected, consumer)
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      try root.releaseTemporaryMemory(workspace, "Full26 transfer forest/index handoff arrays dropped")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  enum EffectCell:
    case Scalar(value: MySqlFormat3Cell)
    case OperationDerived
    case CompletionDerived
    case HistoricalPayload(length: Long, digest: MySqlFormat3Binary)
  final case class EffectRelation(table: String, rows: Vector[Vector[EffectCell]])
  final case class DecodedEffect(phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage,
      header: DecodedHeader, receipt: Option[UUID], relations: Vector[EffectRelation],
      deploymentInputs:Option[MySqlScriptSchemaInputs]=None)

  def decodeEffect(input: MySqlFormat3Binary, header: DecodedHeader,
      phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage, receipt: Option[UUID],
      root: MySqlFormat3ReadBudget,deploymentInputs:Option[MySqlScriptSchemaInputs]=None): DecodedEffect =
    require(header != null && phase != null && image != null && receipt != null &&
      receipt.forall(MySqlNormalRecallRequest.nonZero), "complete historical effect identity required")
    require(header.scope.stableSession.schemaFormat==(if deploymentInputs.nonEmpty then 5 else 3) &&
      header.scope.stableSession.physicalFormat==2,"Recall effect/header complete-image family differs")
    decodeData(input, root,deploymentInputs) { reader =>
      val result = reader.effectData(header, phase, image, receipt)
      reader.finish()
      requireDecodedEffectCanonical(input, result, root)
      result
    }

  /** H3 history decode: root validates the original graph while the exact plan child owns retained DATA. */
  private[mysql] def decodeEffect(input: MySqlFormat3Binary, header: DecodedHeader,
      phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage, receipt: Option[UUID],
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      deploymentInputs: Option[MySqlScriptSchemaInputs]): DecodedEffect =
    requireSourcePlanDataBudget(root, dataBudget)
    require(header != null && phase != null && image != null && receipt != null &&
      receipt.forall(MySqlNormalRecallRequest.nonZero), "complete historical effect identity required")
    require(header.scope.stableSession.schemaFormat == (if deploymentInputs.nonEmpty then 5 else 3) &&
      header.scope.stableSession.physicalFormat == 2, "Recall effect/header complete-image family differs")
    decodeData(input, root, dataBudget, deploymentInputs) { reader =>
      val result = reader.effectData(header, phase, image, receipt)
      reader.finish()
      requireDecodedEffectCanonical(input, result, root)
      result
    }

  /** Parent-P2 recursion decodes and re-encodes under the exact owner already active at its caller. */
  private[mysql] def decodeEffectWithActiveHistoryComparisonOwner(input: MySqlFormat3Binary,
      header: DecodedHeader, phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage,
      receipt: Option[UUID], root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      activeComparisonBudget: MySqlFormat3ReadBudget,
      deploymentInputs: Option[MySqlScriptSchemaInputs]): DecodedEffect =
    requireHistoryComparisonOwner(root, dataBudget, activeComparisonBudget)
    require(header != null && phase != null && image != null && receipt != null &&
      receipt.forall(MySqlNormalRecallRequest.nonZero), "complete historical effect identity required")
    require(header.scope.stableSession.schemaFormat == (if deploymentInputs.nonEmpty then 5 else 3) &&
      header.scope.stableSession.physicalFormat == 2, "Recall effect/header complete-image family differs")
    decodeData(input, root, activeComparisonBudget, deploymentInputs, activeComparisonBudget) { reader =>
      val result = reader.effectData(header, phase, image, receipt)
      reader.finish()
      requireDecodedEffectCanonical(input, result, root, activeComparisonBudget)
      result
    }

  private def requireDecodedEffectCanonical(input: MySqlFormat3Binary, result: DecodedEffect,
      root: MySqlFormat3ReadBudget,
      activeComparisonBudget: MySqlFormat3ReadBudget = null): Unit =
    val compare = () => require(encode("FP_NORMAL_RECALL_EFFECT_BE_V2", root)(writeDecodedEffect(_, result))
      .exactlyMatchesCell(MySqlFormat3Cell.Binary(input)), "noncanonical Recall full20 effect DATA")
    if activeComparisonBudget == null then MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      compare()
    }
    else MySqlRecoveryCanonicalBytes.withAllocationBudget(activeComparisonBudget) {
      compare()
    }

  private def effectPhase(phase: MySqlOperationEffectPhase): String =
    if phase == MySqlOperationEffectPhase.Reserve then "RESERVE" else "EXECUTE"
  private def effectImage(image: MySqlOperationEffectImage): String =
    if image == MySqlOperationEffectImage.Before then "BEFORE" else "TARGET"
  private val HistoricalColumns = Map("request_identity_bytes" -> "request_identity_sha256",
    "original_context_bytes" -> "original_context_sha256", "scope_bytes" -> "scope_sha256",
    "preimage_bytes" -> "preimage_sha256", "target_bytes" -> "target_sha256",
    "commit_witness_bytes" -> "commit_witness_sha256", "original_diagnostic_identity" -> "original_diagnostic_sha256")
  private def effectScalar(row: Vector[EffectCell], shape: MySqlFormat3RelationShape, name: String): MySqlFormat3Cell =
    val index = shape.columns.indexWhere(_.name == name)
    require(index >= 0 && index < row.size, "effect identity column must precede derived payload")
    row(index) match
      case EffectCell.Scalar(value) => value
      case _ => throw new SQLDataException("derived effect identity column is forbidden")
  private def effectCurrent(row: Vector[EffectCell], shape: MySqlFormat3RelationShape, h: DecodedHeader,
      phase: MySqlOperationEffectPhase): (Boolean, Boolean) =
    val same = MySqlModeOperationEffectCodec.uuidEquals(effectScalar(row, shape, "binding_uuid"), h.scope.binding)
    val operation = effectScalar(row, shape, "operation_uuid")
    (same && MySqlModeOperationEffectCodec.uuidEquals(operation, h.scope.reserve),
      same && MySqlModeOperationEffectCodec.uuidEquals(operation, h.scope.tag.operationId))
  private def effectOperationMarker(row: Vector[EffectCell], shape: MySqlFormat3RelationShape,
      column: String, h: DecodedHeader, phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage): Boolean =
    if shape.table == "fp_runtime_writer" then image == MySqlOperationEffectImage.Target && column == "last_commit_fingerprint"
    else if shape.table != "fp_recovery_operation" || !Set("original_context_bytes", "original_context_sha256",
      "preimage_bytes", "preimage_sha256", "target_bytes", "target_sha256", "commit_witness_bytes",
      "commit_witness_sha256", "operation_fingerprint").contains(column) then false
    else
      val (reserve, execute) = effectCurrent(row, shape, h, phase)
      val shared = ContextColumns ++ Set("target_bytes", "target_sha256", "operation_fingerprint")
      val preimage = Set("preimage_bytes", "preimage_sha256")
      val witness = Set("commit_witness_bytes", "commit_witness_sha256")
      phase match
        case MySqlOperationEffectPhase.Reserve => image == MySqlOperationEffectImage.Target &&
          ((reserve && (shared ++ witness).contains(column)) || (execute && (shared ++ preimage).contains(column)))
        case MySqlOperationEffectPhase.Execute => execute &&
          ((shared ++ preimage).contains(column) || (image == MySqlOperationEffectImage.Target && witness.contains(column)))
  private def effectCompletionMarker(row: Vector[EffectCell], shape: MySqlFormat3RelationShape,
      column: String, h: DecodedHeader, phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage,
      receipt: Option[UUID]): Boolean =
    shape.table == "fp_lifecycle_completion" && phase == MySqlOperationEffectPhase.Execute &&
      image == MySqlOperationEffectImage.Target && ContextColumns.contains(column) && receipt.exists { id =>
        MySqlModeOperationEffectCodec.uuidEquals(effectScalar(row, shape, "binding_uuid"), h.scope.binding) &&
          MySqlModeOperationEffectCodec.uuidEquals(effectScalar(row, shape, "receipt_uuid"), id)
      }
  private def effectHistorical(row: Vector[EffectCell], shape: MySqlFormat3RelationShape, column: String,
      h: DecodedHeader, phase: MySqlOperationEffectPhase): Boolean =
    if shape.table != "fp_recovery_operation" || !HistoricalColumns.contains(column) then false
    else
      val (reserve, execute) = effectCurrent(row, shape, h, phase)
      val state = effectScalar(row, shape, "operation_state")
      (state == MySqlFormat3Cell.Text("COMMITTED") || state == MySqlFormat3Cell.Text("CANCELLED")) &&
        !execute && !(phase == MySqlOperationEffectPhase.Reserve && reserve)

  private def writeDecodedEffect(out: DataOutputStream, effect: DecodedEffect): Unit =
    val h = effect.header
    token(out, effectPhase(effect.phase)); token(out, effectImage(effect.image))
    Vector(h.scope.binding, h.scope.workflow, h.scope.reserve, h.scope.tag.operationId,
      h.pod, h.attempt, h.footprint).foreach(uuid(out, _))
    out.writeInt(effect.relations.size)
    effect.relations.foreach { relation =>
      val shape = effect.deploymentInputs.map(_.forTable(relation.table)).getOrElse(MySqlFormat3RelationShapes.forTable(relation.table))
      text(out, shape.table); out.writeInt(shape.columns.size)
      shape.columns.foreach { column => text(out, column.name); text(out, column.sqlDeclaration) }
      out.writeInt(relation.rows.size)
      relation.rows.foreach { row =>
        shape.columns.zip(row).foreach { (column, value) => value match
          case EffectCell.Scalar(actual) => cell(out, actual)
          case EffectCell.HistoricalPayload(length, digest) =>
            out.writeByte(7); token(out, MySqlNormalRecallHistoricalPayload.Domain)
            text(out, column.name); text(out, column.sqlDeclaration); out.writeLong(length); digest.writeTo(out)
          case EffectCell.OperationDerived =>
            token(out, "FP_NORMAL_RECALL_OPERATION_DERIVED_V1")
            token(out, effectPhase(effect.phase)); token(out, effectImage(effect.image)); text(out, shape.table)
            out.writeInt(shape.primaryKey.size)
            shape.primaryKey.foreach { key => text(out, key); cell(out, effectScalar(row, shape, key)) }
            text(out, column.name); text(out, column.sqlDeclaration)
          case EffectCell.CompletionDerived =>
            token(out, DerivedCellDomain); token(out, "EXECUTE"); token(out, "FP_LIFECYCLE_COMPLETION")
            token(out, "BINDING_UUID"); uuid(out, h.scope.binding); token(out, "RECEIPT_UUID"); uuid(out, effect.receipt.get)
            token(out, column.name.toUpperCase(java.util.Locale.ROOT)); text(out, column.sqlDeclaration)
            uuid(out, h.scope.tag.operationId); token(out, "C_ORIGINAL_CONTEXT")
            out.writeByte(if column.name == "original_context_bytes" then 1 else 2)
        }
      }
    }

  def decodePrepare(input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget,
      deploymentInputs:Option[MySqlScriptSchemaInputs]=None): DecodedPrepare =
    decodeData(input, root,deploymentInputs) { reader =>
      val result = reader.prepareData()
      reader.finish()
      requireDecodedPrepareCanonical(input, result, root)
      result
    }

  /** H3 history decode with an explicit retained DATA owner; comparisons remain rooted separately. */
  private[mysql] def decodePrepare(input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, deploymentInputs: Option[MySqlScriptSchemaInputs]): DecodedPrepare =
    requireSourcePlanDataBudget(root, dataBudget)
    decodeData(input, root, dataBudget, deploymentInputs) { reader =>
      val result = reader.prepareData()
      reader.finish()
      requireDecodedPrepareCanonical(input, result, root)
      result
    }

  /** Parent-P2 history keeps its exact retained or active root-comparison owner through decode and comparison. */
  private[mysql] def decodePrepareWithActiveHistoryComparisonOwner(input: MySqlFormat3Binary,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      deploymentInputs: Option[MySqlScriptSchemaInputs],
      activeComparisonBudget: MySqlFormat3ReadBudget): DecodedPrepare =
    requireHistoryComparisonOwner(root, dataBudget, activeComparisonBudget)
    decodeData(input, root, activeComparisonBudget, deploymentInputs, activeComparisonBudget) { reader =>
      val result = reader.prepareData()
      reader.finish()
      requireDecodedPrepareCanonical(input, result, root, activeComparisonBudget)
      result
    }

  private def requireDecodedPrepareCanonical(input: MySqlFormat3Binary, result: DecodedPrepare,
      root: MySqlFormat3ReadBudget,
      activeComparisonBudget: MySqlFormat3ReadBudget = null): Unit =
    val compare = () => require(encode(PrepareDomain, root)(writeDecodedPrepare(_, result, root))
      .exactlyMatchesCell(MySqlFormat3Cell.Binary(input)), "noncanonical recall P DATA")
    if activeComparisonBudget == null then MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      compare()
    }
    else MySqlRecoveryCanonicalBytes.withAllocationBudget(activeComparisonBudget) {
      compare()
    }

  def decodeCheckpoint(input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget,
      deploymentInputs:Option[MySqlScriptSchemaInputs]=None): DecodedCheckpoint =
    decodeData(input, root,deploymentInputs) { reader =>
      reader.domain(CheckpointDomain)
      val header = reader.headerData()
      val nested = reader.dataFrame()
      val prepared = nested.prepareData()
      nested.finish()
      reader.exactDigest(nested)
      val pBytes = nested.copyAll()
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        require(encode(PrepareDomain, root)(writeDecodedPrepare(_, prepared, root))
          .exactlyMatchesCell(MySqlFormat3Cell.Binary(pBytes)), "noncanonical C nested P DATA")
      }
      val removedBytes = reader.dataFrame()
      val removed = removedBytes.removal()
      removedBytes.finish()
      reader.exactDigest(removedBytes)
      val ticketBytes = reader.dataFrame()
      val released = decodeTicketWithInputs(ticketBytes.copyAll(),root,deploymentInputs)
      reader.exactDigest(ticketBytes)
      val version = reader.int64(); val locator = reader.int64(); val receipt = reader.uuid()
      require(version == Math.incrementExact(header.version) && locator == Math.incrementExact(header.locatorRevision),
        "C versions differ from the exact original base")
      val before = reader.selected(Set("physical_recovery_obligation"), Some(header.pod))
      val target = reader.selected(Set("physical_recovery_obligation"), Some(header.pod))
      val domainLocator = reader.selected(Set("floating_pod_state", "physical_recovery_locator"), Some(header.pod))
      requireBaseRows(header, domainLocator, active = false, checkpoint = true)
      reader.finish()
      val p = prepared.header
      require(prepared.purpose == MySqlNormalRecallPurpose.NormalRecall && header.previous.contains(p.scope.tag.operationId) &&
        header.scope.workflow == p.scope.workflow && header.scope.binding == p.scope.binding && header.stop == p.stop &&
        header.pod == p.pod && header.owner == p.owner && header.attempt == p.attempt && header.footprint == p.footprint &&
        header.version == p.version && header.locatorRevision == p.locatorRevision && header.dirtyRevision == p.dirtyRevision &&
        header.source == p.source && header.dimension == p.dimension && header.committed == p.committed &&
        header.finalPose == p.finalPose && header.chunks == p.chunks &&
        header.scope.tag.bootId == p.scope.tag.bootId && header.scope.tag.bootGeneration == p.scope.tag.bootGeneration &&
        header.scope.tag.connectionGeneration == p.scope.tag.connectionGeneration && header.scope.epoch == p.scope.epoch &&
        header.scope.connectionId == p.scope.connectionId && header.scope.server == p.scope.server && header.scope.schema == p.scope.schema &&
        header.scope.stableSession == p.scope.stableSession,
        "C nested P belongs to another exact recall")
      require(removed.tag == p.scope.tag && removed.podUuid == header.pod && removed.attemptUuid == header.attempt &&
        removed.expectedVersion == header.version && removed.dimensionId == header.dimension &&
        released.removal == removed && released.stopRequest == header.stop && released.footprint == header.chunks,
        "C physical DATA differs from original P")
      val result = DecodedCheckpoint(header, prepared, pBytes, removed, released, version, locator, receipt, before, target, domainLocator)
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        require(encode(CheckpointDomain, root) { out =>
          writeDecodedHeader(out, header, root)
          framedBinary(out, pBytes); framedWithHash(out, removal(removed, root)); framedWithHash(out, ticket(released, root))
          out.writeLong(version); out.writeLong(locator); uuid(out, receipt)
          writeDecodedRelations(out, before); writeDecodedRelations(out, target); writeDecodedRelations(out, domainLocator)
        }.exactlyMatchesCell(MySqlFormat3Cell.Binary(input)), "noncanonical recall C DATA")
      }
      result
    }

  /** H3 checkpoint decode; nested P/ticket and selected relation bytes remain on the plan child. */
  private[mysql] def decodeCheckpoint(input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, deploymentInputs: Option[MySqlScriptSchemaInputs]): DecodedCheckpoint =
    requireSourcePlanDataBudget(root, dataBudget)
    decodeData(input, root, dataBudget, deploymentInputs) { reader =>
      reader.domain(CheckpointDomain)
      val header = reader.headerData()
      val nested = reader.dataFrame()
      val prepared = nested.prepareData()
      nested.finish()
      reader.exactDigest(nested)
      val pBytes = nested.copyAll()
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        require(encode(PrepareDomain, root)(writeDecodedPrepare(_, prepared, root))
          .exactlyMatchesCell(MySqlFormat3Cell.Binary(pBytes)), "noncanonical C nested P DATA")
      }
      val removedBytes = reader.dataFrame()
      val removed = removedBytes.removal()
      removedBytes.finish()
      reader.exactDigest(removedBytes)
      val ticketBytes = reader.dataFrame()
      val released = decodeTicketWithInputs(ticketBytes.copyAll(), root, deploymentInputs, dataBudget)
      reader.exactDigest(ticketBytes)
      val version = reader.int64(); val locator = reader.int64(); val receipt = reader.uuid()
      require(version == Math.incrementExact(header.version) && locator == Math.incrementExact(header.locatorRevision),
        "C versions differ from the exact original base")
      val before = reader.selected(Set("physical_recovery_obligation"), Some(header.pod))
      val target = reader.selected(Set("physical_recovery_obligation"), Some(header.pod))
      val domainLocator = reader.selected(Set("floating_pod_state", "physical_recovery_locator"), Some(header.pod))
      requireBaseRows(header, domainLocator, active = false, checkpoint = true)
      reader.finish()
      val p = prepared.header
      require(prepared.purpose == MySqlNormalRecallPurpose.NormalRecall && header.previous.contains(p.scope.tag.operationId) &&
        header.scope.workflow == p.scope.workflow && header.scope.binding == p.scope.binding && header.stop == p.stop &&
        header.pod == p.pod && header.owner == p.owner && header.attempt == p.attempt && header.footprint == p.footprint &&
        header.version == p.version && header.locatorRevision == p.locatorRevision && header.dirtyRevision == p.dirtyRevision &&
        header.source == p.source && header.dimension == p.dimension && header.committed == p.committed &&
        header.finalPose == p.finalPose && header.chunks == p.chunks &&
        header.scope.tag.bootId == p.scope.tag.bootId && header.scope.tag.bootGeneration == p.scope.tag.bootGeneration &&
        header.scope.tag.connectionGeneration == p.scope.tag.connectionGeneration && header.scope.epoch == p.scope.epoch &&
        header.scope.connectionId == p.scope.connectionId && header.scope.server == p.scope.server && header.scope.schema == p.scope.schema &&
        header.scope.stableSession == p.scope.stableSession, "C nested P belongs to another exact recall")
      require(removed.tag == p.scope.tag && removed.podUuid == header.pod && removed.attemptUuid == header.attempt &&
        removed.expectedVersion == header.version && removed.dimensionId == header.dimension &&
        released.removal == removed && released.stopRequest == header.stop && released.footprint == header.chunks,
        "C physical DATA differs from original P")
      val result = DecodedCheckpoint(header, prepared, pBytes, removed, released, version, locator, receipt, before, target, domainLocator)
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        require(encode(CheckpointDomain, root) { out =>
          writeDecodedHeader(out, header, root)
          framedBinary(out, pBytes); framedWithHash(out, removal(removed, root)); framedWithHash(out, ticket(released, root))
          out.writeLong(version); out.writeLong(locator); uuid(out, receipt)
          writeDecodedRelations(out, before); writeDecodedRelations(out, target); writeDecodedRelations(out, domainLocator)
        }.exactlyMatchesCell(MySqlFormat3Cell.Binary(input)), "noncanonical recall C DATA")
      }
      result
    }

  /** Parent-P2 nested C decode retains its validated DATA/comparison owner through every nested reader and re-encode. */
  private[mysql] def decodeCheckpointWithActiveHistoryComparisonOwner(input: MySqlFormat3Binary,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget,
      deploymentInputs: Option[MySqlScriptSchemaInputs],
      activeComparisonBudget: MySqlFormat3ReadBudget): DecodedCheckpoint =
    requireHistoryComparisonOwner(root, dataBudget, activeComparisonBudget)
    decodeData(input, root, activeComparisonBudget, deploymentInputs, activeComparisonBudget) { reader =>
      reader.domain(CheckpointDomain)
      val header = reader.headerData()
      val nested = reader.dataFrame()
      val prepared = nested.prepareData()
      nested.finish()
      reader.exactDigest(nested)
      val pBytes = nested.copyAll()
      MySqlRecoveryCanonicalBytes.withAllocationBudget(activeComparisonBudget) {
        require(encode(PrepareDomain, root)(writeDecodedPrepare(_, prepared, root))
          .exactlyMatchesCell(MySqlFormat3Cell.Binary(pBytes)), "noncanonical C nested P DATA")
      }
      val removedBytes = reader.dataFrame()
      val removed = removedBytes.removal()
      removedBytes.finish()
      reader.exactDigest(removedBytes)
      val ticketBytes = reader.dataFrame()
      val released = decodeTicketWithActiveHistoryComparisonOwner(
        ticketBytes.copyAll(), root, deploymentInputs, dataBudget, activeComparisonBudget)
      reader.exactDigest(ticketBytes)
      val version = reader.int64(); val locator = reader.int64(); val receipt = reader.uuid()
      require(version == Math.incrementExact(header.version) && locator == Math.incrementExact(header.locatorRevision),
        "C versions differ from the exact original base")
      val before = reader.selected(Set("physical_recovery_obligation"), Some(header.pod))
      val target = reader.selected(Set("physical_recovery_obligation"), Some(header.pod))
      val domainLocator = reader.selected(Set("floating_pod_state", "physical_recovery_locator"), Some(header.pod))
      requireBaseRows(header, domainLocator, active = false, checkpoint = true)
      reader.finish()
      val p = prepared.header
      require(prepared.purpose == MySqlNormalRecallPurpose.NormalRecall && header.previous.contains(p.scope.tag.operationId) &&
        header.scope.workflow == p.scope.workflow && header.scope.binding == p.scope.binding && header.stop == p.stop &&
        header.pod == p.pod && header.owner == p.owner && header.attempt == p.attempt && header.footprint == p.footprint &&
        header.version == p.version && header.locatorRevision == p.locatorRevision && header.dirtyRevision == p.dirtyRevision &&
        header.source == p.source && header.dimension == p.dimension && header.committed == p.committed &&
        header.finalPose == p.finalPose && header.chunks == p.chunks &&
        header.scope.tag.bootId == p.scope.tag.bootId && header.scope.tag.bootGeneration == p.scope.tag.bootGeneration &&
        header.scope.tag.connectionGeneration == p.scope.tag.connectionGeneration && header.scope.epoch == p.scope.epoch &&
        header.scope.connectionId == p.scope.connectionId && header.scope.server == p.scope.server && header.scope.schema == p.scope.schema &&
        header.scope.stableSession == p.scope.stableSession, "C nested P belongs to another exact recall")
      require(removed.tag == p.scope.tag && removed.podUuid == header.pod && removed.attemptUuid == header.attempt &&
        removed.expectedVersion == header.version && removed.dimensionId == header.dimension &&
        released.removal == removed && released.stopRequest == header.stop && released.footprint == header.chunks,
        "C physical DATA differs from original P")
      val result = DecodedCheckpoint(header, prepared, pBytes, removed, released, version, locator, receipt, before, target, domainLocator)
      MySqlRecoveryCanonicalBytes.withAllocationBudget(activeComparisonBudget) {
        require(encode(CheckpointDomain, root) { out =>
          writeDecodedHeader(out, header, root)
          framedBinary(out, pBytes); framedWithHash(out, removal(removed, root)); framedWithHash(out, ticket(released, root))
          out.writeLong(version); out.writeLong(locator); uuid(out, receipt)
          writeDecodedRelations(out, before); writeDecodedRelations(out, target); writeDecodedRelations(out, domainLocator)
        }.exactlyMatchesCell(MySqlFormat3Cell.Binary(input)), "noncanonical recall C DATA")
      }
      result
    }

  private def decodeData[A](input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget,
      deploymentInputs:Option[MySqlScriptSchemaInputs]=None)(body: DataReader => A): A =
    decodeData(input, root, MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root), deploymentInputs)(body)
  private def decodeData[A](input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, deploymentInputs: Option[MySqlScriptSchemaInputs])
      (body: DataReader => A): A = decodeData(input, root, dataBudget, deploymentInputs, null)(body)
  private def decodeData[A](input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget, deploymentInputs: Option[MySqlScriptSchemaInputs],
      activeComparisonBudget: MySqlFormat3ReadBudget)(body: DataReader => A): A =
    decodePhysical(input, root, 4096L, dataBudget, deploymentInputs) { _ =>
      body(new DataReader(input,0,input.length,root,dataBudget,new MemberCount,deploymentInputs,activeComparisonBudget))
    }
  private final class MemberCount:
    var value = 0L

  private final class DataReader(input: MySqlFormat3Binary, begin: Int, limit: Int,
      root: MySqlFormat3ReadBudget, budget: MySqlFormat3ReadBudget, members: MemberCount,
      deploymentInputs:Option[MySqlScriptSchemaInputs]=None,
      activeComparisonBudget: MySqlFormat3ReadBudget = null)
      extends PhysicalReader(input, begin, limit):
    def dataFrame(): DataReader =
      val count = int32(); need(count.toLong)
      val result = new DataReader(input,offset,offset+count,root,budget,members,deploymentInputs,activeComparisonBudget)
      offset += count
      result
    def boundedItemCount(label: String, minimum: Int): Int =
      require(label != null && label.nonEmpty && minimum >= 0, "bounded DATA item count label and minimum required")
      val count = int32()
      require(count >= minimum && count.toLong <= root.profile.maxMembers(),
        s"$label count is outside the original member bound")
      members.value = Math.addExact(members.value, count.toLong)
      require(members.value <= root.profile.maxMembers(), s"$label aggregate count exceeds the original member bound")
      count
    def copyAll(): MySqlFormat3Binary =
      val size = end - start
      budget.reserveRetainedMemory(Math.addExact(size.toLong, 64L), "recall decoded bounded frame")
      val bytes = new Array[Byte](MySqlFormat3JvmArrayLimit.requireLength(size.toLong, "recall frame"))
      var i = 0
      while i < size do
        bytes(i) = input.byteAt(start + i)
        i += 1
      MySqlFormat3Binary.takeOwned(bytes)
    private def copyRange(from: Int, until: Int, label: String): MySqlFormat3Binary =
      require(from >= start && until >= from && until <= end && label != null && label.nonEmpty,
        "recall decoded prefix range must remain inside its bounded frame")
      val size = until - from
      val reservation = Math.addExact(size.toLong, 64L)
      budget.reserveRetainedMemory(reservation, label + " before allocation")
      try
        val bytes = new Array[Byte](MySqlFormat3JvmArrayLimit.requireLength(size.toLong, label))
        var index = 0
        while index < size do
          bytes(index) = input.byteAt(from + index)
          index += 1
        MySqlFormat3Binary.takeOwned(bytes, budget)
      catch
        case cause: Throwable =>
          try budget.releaseRetainedMemory(reservation, label + " allocation failure")
          catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
          throw cause
    def requireExactBinary(expected: MySqlFormat3Binary): Unit =
      require(expected != null && end.toLong - start.toLong == expected.length.toLong,
        "recall bounded binary frame length differs from its exact typed identity")
      var index = 0
      while index < expected.length do
        require(input.byteAt(start + index) == expected.byteAt(index),
          "recall bounded binary frame differs from its exact typed identity")
        index += 1
      offset = end
    def copyAllToBudget(): MySqlFormat3Binary =
      val size = end - start
      val reservation = Math.addExact(size.toLong, 64L)
      budget.reserveRetainedMemory(reservation, "H1 effect bounded SQL binary Cell")
      try
        val bytes = new Array[Byte](MySqlFormat3JvmArrayLimit.requireLength(size.toLong, "H1 effect binary Cell"))
        var index = 0
        while index < size do
          bytes(index) = input.byteAt(start + index)
          index += 1
        MySqlFormat3Binary.takeOwned(bytes, budget)
      catch
        case cause: Throwable =>
          try budget.releaseRetainedMemory(reservation, "failed H1 effect bounded binary Cell")
          catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
          throw cause
    def requireNextExact(expected: MySqlFormat3Binary): Unit =
      require(expected != null, "H1 exact DerivedField frame is required")
      need(expected.length.toLong)
      var index = 0
      while index < expected.length do
        require(input.byteAt(offset + index) == expected.byteAt(index),
          "H1 DerivedField frame differs from its position-selected exact descriptor")
        index += 1
      offset = Math.addExact(offset, expected.length)
    def requireNoSourceChildDerivedFieldStart(): Unit =
      val length = SourceChildReserveDerivedFieldDomain.length
      val frameLength = 4L + length.toLong
      if end.toLong - offset.toLong >= frameLength then
        val actualLength = ((input.byteAt(offset).toInt & 255) << 24) |
          ((input.byteAt(offset + 1).toInt & 255) << 16) |
          ((input.byteAt(offset + 2).toInt & 255) << 8) |
          (input.byteAt(offset + 3).toInt & 255)
        if actualLength == length then
          var index = 0
          var matches = true
          while index < length && matches do
            matches = input.byteAt(offset + 4 + index) == SourceChildReserveDerivedFieldDomain.charAt(index).toByte
            index += 1
          if matches then throw new SQLDataException(
            "H1 DerivedField frame is forbidden at this literal image/table/column/primary-key position")
    def requireExactResourceKey(expected: MySqlRecoveryResourceKey, label: String): Unit =
      require(expected != null && label != null && label.nonEmpty &&
        end.toLong - start.toLong == expected.canonicalLength.toLong,
        s"$label length differs from its exact typed full-key identity")
      var position = 0
      val sink = new java.io.OutputStream:
        override def write(value: Int): Unit =
          require(position < expected.canonicalLength,
            s"$label canonical encoder exceeded its declared full-key length")
          if input.byteAt(start + position) != value.toByte then
            throw new SQLDataException(s"$label differs from its exact typed full-key identity")
          position = Math.addExact(position, 1)
        override def write(values: Array[Byte], offset: Int, length: Int): Unit =
          if values == null || offset < 0 || length < 0 || offset > values.length - length then
            throw new IndexOutOfBoundsException("resource-key canonical comparison slice is invalid")
          var index = 0
          while index < length do
            write(values(offset + index).toInt)
            index += 1
      val output = new DataOutputStream(sink)
      expected.writeCanonicalTo(output)
      output.flush()
      require(position == expected.canonicalLength,
        s"$label canonical encoder did not write its declared full-key length")
      offset = end
    def textData(): String =
      val count = int32(); need(count.toLong)
      val scratch = Math.addExact(Math.multiplyExact(count.toLong, 3L), 384L)
      budget.reserveRetainedMemory(Math.addExact(Math.multiplyExact(count.toLong, 2L), 64L), "recall strict UTF8 String")
      budget.reserveTemporaryMemory(scratch, "recall strict UTF8 scratch")
      var primary: Throwable = null
      try
        val bytes = new Array[Byte](MySqlFormat3JvmArrayLimit.requireLength(count.toLong, "recall UTF8"))
        var i = 0
        while i < count do
          bytes(i) = octet().toByte
          i += 1
        java.nio.charset.StandardCharsets.UTF_8.newDecoder()
          .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
          .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT).decode(java.nio.ByteBuffer.wrap(bytes)).toString
      catch
        case cause: Throwable => primary = cause; throw cause
      finally
        try budget.releaseTemporaryMemory(scratch, "recall strict UTF8 scratch")
        catch case cleanup: Throwable => if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    def bool(): Boolean = octet() match
      case 0 => false
      case 1 => true
      case _ => throw new SQLDataException("noncanonical recall boolean")
    def optionalUuid(): Option[UUID] = if bool() then Some(this.uuid()) else None
    private def scalarData(column: MySqlFormat3ColumnShape): MySqlFormat3Cell = octet() match
      case 0 => require(column.nullable, "unexpected effect SQL NULL"); MySqlFormat3Cell.SqlNull
      case 1 =>
        require(column.kind == MySqlFormat3SqlKind.Text, "effect SQL text kind differs")
        MySqlFormat3Cell.Text(textData())
      case 2 =>
        require(column.kind == MySqlFormat3SqlKind.Binary, "effect SQL binary kind differs")
        val frame = dataFrame()
        require(column.maximumLength.forall(_ >= frame.end - frame.start), "effect binary exceeds declared length")
        MySqlFormat3Cell.Binary(frame.copyAll())
      case 3 => require(column.kind == MySqlFormat3SqlKind.Integer, "effect SQL integer kind differs"); MySqlFormat3Cell.SignedInteger(int64())
      case 4 => require(column.kind == MySqlFormat3SqlKind.Floating, "effect SQL floating kind differs"); MySqlFormat3Cell.Floating(int64())
      case 5 =>
        require(column.kind == MySqlFormat3SqlKind.DateTime, "effect SQL datetime kind differs")
        MySqlFormat3Cell.DateTime(LocalDateTime.of(int32(), octet(), octet(), octet(), octet(), octet(), int32()))
      case _ => throw new SQLDataException("unknown effect SQL cell tag")
    private def sourceChildReserveEffectScalarData(column: MySqlFormat3ColumnShape,
        table: String): MySqlFormat3Cell =
      val value = octet() match
        case 0 => require(column.nullable, "unexpected H1 effect SQL NULL"); MySqlFormat3Cell.SqlNull
        case 1 =>
          require(column.kind == MySqlFormat3SqlKind.Text, "H1 effect SQL text kind differs")
          MySqlFormat3Cell.Text(textData())
        case 2 =>
          require(column.kind == MySqlFormat3SqlKind.Binary, "H1 effect SQL binary kind differs")
          val frame = dataFrame()
          require(column.maximumLength.forall(_ >= frame.end - frame.start),
            "H1 effect binary Cell exceeds its original column shape")
          MySqlFormat3Cell.Binary(frame.copyAllToBudget())
        case 3 =>
          require(column.kind == MySqlFormat3SqlKind.Integer, "H1 effect SQL integer kind differs")
          MySqlFormat3Cell.SignedInteger(int64())
        case 4 =>
          require(column.kind == MySqlFormat3SqlKind.Floating, "H1 effect SQL floating kind differs")
          MySqlFormat3Cell.Floating(int64())
        case 5 =>
          require(column.kind == MySqlFormat3SqlKind.DateTime, "H1 effect SQL datetime kind differs")
          MySqlFormat3Cell.DateTime(sourceChildReserveEffectDateTime6())
        case _ => throw new SQLDataException("unknown H1 effect SQL Cell tag")
      MySqlFormat3SnapshotValidation.validateCell(value, column, table)
      value
    def sourceChildReserveWitnessDateTime(inputs: MySqlScriptSchemaInputs): LocalDateTime =
      require(inputs != null && inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26,
        "H1 witness DateTime Cell requires the exact Full26 schema")
      val shape = inputs.forTable(SourceChildReserveOperationTable)
      val index = shape.columns.indexWhere(_.name == "result_at")
      require(index >= 0 && shape.columns(index).kind == MySqlFormat3SqlKind.DateTime,
        "H1 witness result_at must retain its original DATETIME(6) column shape")
      sourceChildReserveEffectScalarData(shape.columns(index), SourceChildReserveOperationTable) match
        case MySqlFormat3Cell.DateTime(value) => value
        case _ => throw new SQLDataException("H1 witness requires an ordinary non-NULL DateTime Cell")
    private def sourceChildReserveEffectDateTime6(): LocalDateTime =
      val year = int32(); val month = octet(); val day = octet()
      val hour = octet(); val minute = octet(); val second = octet()
      val nanos = int32()
      require(year >= 1000 && year <= 9999 && nanos >= 0 && nanos < 1000000000 && nanos % 1000 == 0,
        "invalid H1 effect SQL DATETIME(6) nanosecond frame")
      LocalDateTime.of(year, month, day, hour, minute, second, nanos)
    def sourceChildReserveEffectData(request: SourceChildReserveRequestData,
        inputs: MySqlScriptSchemaInputs, expectedImage: MySqlOperationEffectImage,
        requirePlanDataChild: Boolean = true): SourceChildReserveEffectData =
      sourceChildReserveEffectShapes(inputs, root, budget, requirePlanDataChild)
      requireSourceChildReserveRequestForDerived(request, root)
      domain(SourceChildReserveEffectDomain)
      val image = octet() match
        case 1 => MySqlOperationEffectImage.Before
        case 2 => MySqlOperationEffectImage.Target
        case _ => throw new SQLDataException("unknown H1 effect image tag")
      require(image == expectedImage, "H1 effect image differs from its exact decoder context")
      require(this.uuid() == request.binding && this.uuid() == request.childWorkflow &&
        this.uuid() == request.reserveOperation, "H1 effect binding/child/R0 identity differs from its exact request")
      val shapes = inputs.canonicalOrder
      require(int32() == shapes.size, "H1 effect must contain the exact original 26-table layout")
      budget.reserveRetainedMemory(8192L, "H1 effect typed Full26 relation wrappers")
      var totalRows = 0L
      val relations = shapes.map { shape =>
        domain(shape.table)
        require(int32() == shape.columns.size, s"H1 effect ${shape.table} column count differs")
        shape.columns.foreach { column =>
          domain(column.name)
          domain(column.sqlDeclaration)
        }
        val count = int32()
        require(count >= 0 && count.toLong <= root.profile.maxMembers(),
          s"H1 effect ${shape.table} row count is outside the original member bound")
        totalRows = Math.addExact(totalRows, count.toLong)
        members.value = Math.addExact(members.value, count.toLong)
        require(totalRows <= inputs.budget.rows && members.value <= root.profile.maxMembers(),
          "H1 effect aggregate rows exceed the exact Full26/member bounds")
        need(Math.multiplyExact(count.toLong, shape.columns.size.toLong))
        val rowReservation = Math.addExact(128L, Math.addExact(
          Math.multiplyExact(count.toLong, 128L),
          Math.multiplyExact(count.toLong, Math.multiplyExact(shape.columns.size.toLong, 40L))))
        budget.reserveRetainedMemory(rowReservation,
          s"H1 effect ${shape.table} decoded typed rows/cells")
        val rows = Vector.fill(count) {
          val row = scala.collection.mutable.ArrayBuffer.empty[SourceChildReserveEffectCell]
          shape.columns.foreach { column =>
            val effectCell =
              if sourceChildReserveEffectDerivedPosition(row, shape, column, request, image) then
                val field = sourceChildReserveProjectedTargetDerivedFieldData(request, inputs, image,
                  shape.table, column.name, root, budget, requirePlanDataChild)
                val rowPrimaryKey = shape.primaryKey.map { name =>
                  val index = shape.columns.indexWhere(_.name == name)
                  sourceChildReserveEffectCellValue(row, index, s"H1 effect ${shape.table} current primary key")
                }
                require(field.primaryKey == rowPrimaryKey,
                  s"H1 effect ${shape.table}.${column.name} descriptor key differs from the already-read original PK")
                requireNextExact(field.canonical.borrowedBinary)
                SourceChildReserveEffectCell.Derived(field)
              else
                requireNoSourceChildDerivedFieldStart()
                SourceChildReserveEffectCell.Literal(sourceChildReserveEffectScalarData(column, shape.table))
            row += effectCell
          }
          row.toVector
        }
        SourceChildReserveEffectRelationData(shape.table, rows)
      }
      buildSourceChildReserveEffectData(request, inputs, image, relations, root, budget,
        valuesAlreadyRetained = true, requirePlanDataChild = requirePlanDataChild)
    def sourceChildReservationRow(): Vector[MySqlFormat3Cell] =
      val inputs = deploymentInputs.getOrElse(
        throw new SQLDataException("SOURCE R0 FullReservationRow requires exact Full26 DATA inputs"))
      val shape = sourceChildReservationShape(inputs, root)
      require(shape.columns.size == 11 && int32() == 11,
        "SOURCE R0 FullReservationRow must declare the exact eleven-column shape")
      shape.columns.foreach { column =>
        require(textData() == column.name && textData() == column.sqlDeclaration,
          "SOURCE R0 FullReservationRow descriptor differs from the original closed table shape")
      }
      val row = shape.columns.map(scalarData)
      validateSourceChildReservationRowShape(row, shape, root)
      finish()
      row
    def effectData(h: DecodedHeader, phase: MySqlOperationEffectPhase, image: MySqlOperationEffectImage,
        receipt: Option[UUID]): DecodedEffect =
      domain("FP_NORMAL_RECALL_EFFECT_BE_V2"); domain(effectPhase(phase)); domain(effectImage(image))
      Vector(h.scope.binding, h.scope.workflow, h.scope.reserve, h.scope.tag.operationId,
        h.pod, h.attempt, h.footprint).foreach(id => require(this.uuid() == id, "effect original header identity differs"))
      val shapes=deploymentInputs.map(_.canonicalOrder).getOrElse(MySqlFormat3RelationShapes.canonicalOrder)
      require(int32()==shapes.size,"complete effect closed image required")
      budget.reserveRetainedMemory(8192L, "effect decoded full20 wrappers")
      val relations = shapes.map { shape =>
        domain(shape.table); require(int32() == shape.columns.size, "effect column count differs")
        shape.columns.foreach { column => domain(column.name); domain(column.sqlDeclaration) }
        val count = int32()
        need(Math.multiplyExact(count.toLong, shape.columns.size.toLong))
        members.value = Math.addExact(members.value, count.toLong)
        require(count >= 0 && members.value <= root.profile.maxMembers(), "effect aggregate member budget exceeded")
        budget.reserveRetainedMemory(Math.multiplyExact(count.toLong, 128L + shape.columns.size * 128L), "effect typed rows and validation views")
        val rows = Vector.fill(count) {
          var row = Vector.empty[EffectCell]
          shape.columns.foreach { column =>
            need(1L)
            val value =
              if effectCompletionMarker(row, shape, column.name, h, phase, image, receipt) then
                domain(DerivedCellDomain); domain("EXECUTE"); domain("FP_LIFECYCLE_COMPLETION")
                domain("BINDING_UUID"); require(this.uuid() == h.scope.binding, "completion marker binding differs")
                domain("RECEIPT_UUID"); require(this.uuid() == receipt.get, "completion marker receipt differs")
                domain(column.name.toUpperCase(java.util.Locale.ROOT)); domain(column.sqlDeclaration)
                require(this.uuid() == h.scope.tag.operationId, "completion marker C differs")
                domain("C_ORIGINAL_CONTEXT")
                require(octet() == (if column.name == "original_context_bytes" then 1 else 2), "completion marker kind differs")
                EffectCell.CompletionDerived
              else if effectOperationMarker(row, shape, column.name, h, phase, image) then
                domain("FP_NORMAL_RECALL_OPERATION_DERIVED_V1"); domain(effectPhase(phase)); domain(effectImage(image)); domain(shape.table)
                require(int32() == shape.primaryKey.size, "operation marker PK count differs")
                shape.primaryKey.foreach { key =>
                  domain(key)
                  require(scalarData(shape.columns.find(_.name == key).get) == effectScalar(row, shape, key), "operation marker PK differs")
                }
                domain(column.name); domain(column.sqlDeclaration)
                EffectCell.OperationDerived
              else if effectHistorical(row, shape, column.name, h, phase) && input.byteAt(offset) == 7.toByte then
                require(octet() == 7, "historical payload tag differs")
                domain(MySqlNormalRecallHistoricalPayload.Domain); domain(column.name); domain(column.sqlDeclaration)
                val length = int64()
                require(length >= 0L && length <= Math.min(Math.min(root.profile.maxSqlPacketBytes(), root.profile.maxRetainedBytes()),
                  MySqlFormat3JvmArrayLimit.MaximumLength), "historical payload length exceeds original profile")
                need(32L)
                val digest = new DataReader(input, offset, offset + 32, root, budget, members,
                  deploymentInputs, activeComparisonBudget).copyAll()
                offset += 32
                EffectCell.HistoricalPayload(length, digest)
              else
                val actual = scalarData(column)
                require(!effectHistorical(row, shape, column.name, h, phase) || actual == MySqlFormat3Cell.SqlNull,
                  "terminal historical payload must retain its exact descriptor")
                EffectCell.Scalar(actual)
            row = row :+ value
          }
          shape.columns.zip(row).foreach { (column, value) => value match
            case EffectCell.HistoricalPayload(_, digest) =>
              require(effectScalar(row, shape, HistoricalColumns(column.name)) == MySqlFormat3Cell.Binary(digest),
                "historical descriptor digest differs from original paired column")
            case EffectCell.Scalar(MySqlFormat3Cell.SqlNull) if effectHistorical(row, shape, column.name, h, phase) =>
              require(effectScalar(row, shape, HistoricalColumns(column.name)) == MySqlFormat3Cell.SqlNull,
                "historical absent payload retains a digest")
            case _ => ()
          }
          row
        }
        EffectRelation(shape.table, rows)
      }
      // 仅验证固定SQL schema与PK顺序。marker临时类型视图不构造快照、不作为真实值返回。
      val validationBinary = MySqlFormat3Binary.takeOwned(new Array[Byte](32))
      val validationRelations=relations.map { relation =>
        MySqlFormat3RelationSnapshot(relation.table, relation.rows.map(_.map {
          case EffectCell.Scalar(value) => value
          case _ => MySqlFormat3Cell.Binary(validationBinary)
        }))
      }
      deploymentInputs match
        case None=>MySqlFormat3SnapshotValidation.validateRelations(validationRelations)
        case Some(_)=>MySqlFormat3SnapshotValidation.validateScriptPublication1DeploymentReceipt1Relations(validationRelations)
      DecodedEffect(phase,image,h,receipt,relations,deploymentInputs)
    private def date6(): LocalDateTime =
      val year = int32(); val month = octet(); val day = octet(); val hour = octet(); val minute = octet(); val second = octet()
      val micros = int32()
      require(year >= 1000 && year <= 9999 && micros >= 0 && micros < 1000000, "invalid recall DATETIME(6)")
      LocalDateTime.of(year, month, day, hour, minute, second, micros * 1000)
    private def digestFrame(expected: Option[Array[Byte]] = None): Unit =
      need(32L) // 原scope的SHA字段为固定32字节，没有length前缀。
      var i = 0
      while i < 32 do
        val b = octet().toByte
        expected.foreach(hash => require(b == hash(i), "recall scope profile SHA differs"))
        i += 1
    def scopeData(expectedSchemaFormat: Int = 3): DecodedScope =
      scopeDataFields(expectedSchemaFormat, finishFrame = true, prefixFromCurrentOffset = false)
    /** H1 DATA envelope prefix; caller must parse the remainder and finish the original frame. */
    def scopeDataPrefix(expectedSchemaFormat: Int): DecodedScope =
      scopeDataFields(expectedSchemaFormat, finishFrame = false, prefixFromCurrentOffset = true)
    private def scopeDataFields(expectedSchemaFormat: Int, finishFrame: Boolean,
        prefixFromCurrentOffset: Boolean): DecodedScope =
      val scopeStart = if prefixFromCurrentOffset then offset else start
      require(expectedSchemaFormat==3 || expectedSchemaFormat==5,"unsupported historical scope family")
      val tag = IoOperationTag(this.uuid(), int64(), int64(), this.uuid())
      val workflow = this.uuid(); val reserve = this.uuid(); val barrier = this.uuid()
      val connection = int64()
      val namespace = textData()
      require(connection > 0L && namespace.matches("fpb1:[A-Za-z0-9_-]{43}"), "invalid recall held-session identity")
      val binding = this.uuid(); val epoch = int64(); val lease = date6()
      require(epoch > 0L, "invalid recall writer epoch")
      val server = this.uuid()
      val serverVersion = textData()
      require(serverVersion.nonEmpty, "missing recall server version")
      val schema = textData()
      val effectiveUser = textData()
      require(schema.matches("[a-z][a-z0-9_]{0,63}") && effectiveUser.nonEmpty, "invalid recall schema/effective user")
      val provisioning = this.uuid()
      val schemaFormat=int32();val physicalFormat=int32()
      require(schemaFormat==expectedSchemaFormat && physicalFormat==2,"recall scope format differs")
      val ddl = textData(); val manifest = textData()
      require(ddl.matches("(?i)[0-9a-f]{64}") && manifest.matches("(?i)[0-9a-f]{64}"), "invalid recall approved hashes")
      def hashData(): MySqlFormat3Binary =
        need(32L)
        val frame = new DataReader(input, offset, offset + 32, root, budget, members,
          deploymentInputs, activeComparisonBudget)
        offset += 32
        frame.copyAll()
      val config = hashData(); val provenanceKind = textData()
      require(provenanceKind.nonEmpty, "missing recall provenance kind")
      val provenance = hashData()
      digestFrame(Some(root.profile.sha256()))
      val scopeEnd = offset
      if finishFrame then finish()
      DecodedScope(copyRange(scopeStart, scopeEnd, "recall decoded common scope prefix"), tag, workflow, reserve, binding, epoch, connection, lease, server, schema,
        DecodedSession(barrier, namespace, serverVersion, effectiveUser, provisioning, ddl, manifest, config, provenanceKind, provenance,
          schemaFormat,physicalFormat))
    private def poseData(): DecodedPose = DecodedPose(int64(), int64(), int64(), int32(), int32())
    def headerData(): DecodedHeader =
      val workflow = this.uuid(); val stop = this.uuid(); val binding = this.uuid(); val boot = this.uuid()
      val generation = int64(); val connectionGeneration = int64(); val epoch = int64(); val connection = int64()
      val scope = dataFrame().scopeData(if deploymentInputs.nonEmpty then 5 else 3)
      val seconds = int64(); val micros = int32()
      require(micros >= 0 && micros < 1000000, "invalid recall lease microseconds")
      val lease = LocalDateTime.ofEpochSecond(seconds, micros * 1000, ZoneOffset.UTC)
      val operation = this.uuid(); val reserve = this.uuid(); val previous = optionalUuid(); val pod = this.uuid(); val owner = optionalUuid()
      val attempt = this.uuid(); val footprint = this.uuid(); val version = int64(); val locator = int64(); val dirty = int64()
      domain("NORMAL")
      val world = textData()
      val sourceReader = dataFrame()
      // source identity uses framed UTF8 content rather than another nested length.
      val sourceBinary = sourceReader.copyAll()
      val sourceText = strictSourceText(sourceBinary, budget)
      exactDigest(sourceReader)
      val source = MySqlNormalRecallSourceIdentity(world, sourceText)
      val dimension = int32(); val committed = poseData(); val finalPose = poseData()
      require(int32() == 25 && root.profile.maxMembers() >= 25L, "exact recall footprint count required")
      members.value = Math.addExact(members.value, 25L)
      require(members.value <= root.profile.maxMembers(), "recall aggregate footprint/member budget exceeded")
      budget.reserveRetainedMemory(2048L, "recall decoded exact footprint")
      val chunks = Vector.fill(25)(PhysicalRecoveryChunk(int32(), int32(), int32()))
      require(chunks == chunks.sortBy(c => (c.dimensionId, c.chunkX, c.chunkZ)) && chunks.distinct.size == 25,
        "noncanonical recall footprint order or duplicate")
      val cx = MySqlNormalRecallRequest.chunk(java.lang.Double.longBitsToDouble(finalPose.x)).toLong
      val cz = MySqlNormalRecallRequest.chunk(java.lang.Double.longBitsToDouble(finalPose.z)).toLong
      require(chunks.forall(c => c.dimensionId == dimension && c.chunkX.toLong >= cx - 2 && c.chunkX.toLong <= cx + 2 &&
        c.chunkZ.toLong >= cz - 2 && c.chunkZ.toLong <= cz + 2) &&
        MySqlNormalRecallRequest.chunk(java.lang.Double.longBitsToDouble(committed.x)).toLong == cx &&
        MySqlNormalRecallRequest.chunk(java.lang.Double.longBitsToDouble(committed.z)).toLong == cz, "recall footprint differs from original poses")
      val profile = dataFrame()
      require(profile.copyAll().equalsBytes(root.profile.canonicalBytes()), "recall explicit profile differs")
      exactDigest(profile)
      require(scope.tag == IoOperationTag(boot, generation, connectionGeneration, operation) && scope.workflow == workflow &&
        scope.reserve == reserve && scope.binding == binding && scope.epoch == epoch && scope.connectionId == connection && scope.lease == lease &&
        operation != reserve && !previous.contains(operation) && !previous.contains(reserve) && owner.nonEmpty &&
        version >= 0L && locator >= 0L && dirty >= 0L, "recall duplicated header/scope fields differ")
      DecodedHeader(scope, stop, previous, pod, owner, attempt, footprint, version, locator, dirty, source, dimension, committed, finalPose, chunks)
    def selected(tables: Set[String], pod: Option[UUID]): Vector[MySqlFormat3RelationSnapshot] =
      require(int32() == tables.size, "recall selected relation count differs")
      val result = MySqlFormat3RelationShapes.canonicalOrder.filter(s => tables.contains(s.table)).map { shape =>
        domain(shape.table)
        require(int32() == shape.columns.size, "recall selected column count differs")
        shape.columns.foreach { c => domain(c.name); domain(c.sqlDeclaration) }
        val count = int32()
        need(Math.multiplyExact(count.toLong, shape.columns.size.toLong))
        members.value = Math.addExact(members.value, count.toLong)
        require(count >= 0 && members.value <= root.profile.maxMembers(), "recall aggregate member budget exceeded")
        budget.reserveRetainedMemory(Math.multiplyExact(count.toLong, 64L + shape.columns.size * 64L), "recall decoded typed rows")
        val rows = Vector.fill(count) { shape.columns.map { column =>
          octet() match
            case 0 => require(column.nullable, "unexpected recall SQL NULL"); MySqlFormat3Cell.SqlNull
            case 1 =>
              require(column.kind == MySqlFormat3SqlKind.Text, "recall SQL text kind differs")
              MySqlFormat3Cell.Text(textData())
            case 2 =>
              require(column.kind == MySqlFormat3SqlKind.Binary, "recall SQL binary kind differs")
              val frame = dataFrame()
              require(column.maximumLength.forall(_ >= frame.end - frame.start), "recall binary exceeds declared length")
              MySqlFormat3Cell.Binary(frame.copyAll())
            case 3 => require(column.kind == MySqlFormat3SqlKind.Integer, "recall SQL integer kind differs"); MySqlFormat3Cell.SignedInteger(int64())
            case 4 => require(column.kind == MySqlFormat3SqlKind.Floating, "recall SQL floating kind differs"); MySqlFormat3Cell.Floating(int64())
            case 5 =>
              require(column.kind == MySqlFormat3SqlKind.DateTime, "recall SQL datetime kind differs")
              MySqlFormat3Cell.DateTime(LocalDateTime.of(int32(), octet(), octet(), octet(), octet(), octet(), int32()))
            case _ => throw new SQLDataException("unknown recall SQL cell tag")
        } }
        pod.foreach { id =>
          val key = shape.columns.indexWhere(_.name == (if shape.table == "floating_pod_state" then "uuid" else "pod_uuid"))
          require(key >= 0 && rows.forall(r => MySqlModeOperationEffectCodec.uuidEquals(r(key), id)), "foreign Pod in recall selected relations")
        }
        MySqlFormat3RelationSnapshot(shape.table, rows)
      }
      // 只复用完整shape/PK验证器；填空关系是验证DATA，没有构造snapshot/admission。
      MySqlFormat3SnapshotValidation.validateRelations(MySqlFormat3RelationShapes.canonicalOrder.map { shape =>
        result.find(_.table == shape.table).getOrElse(MySqlFormat3RelationSnapshot(shape.table, Vector.empty))
      })
      result
    def prepareData(): DecodedPrepare =
      domain(PrepareDomain)
      val header = headerData()
      val purpose = octet() match
        case 1 => MySqlNormalRecallPurpose.NormalRecall
        case 2 => MySqlNormalRecallPurpose.RestoreCommittedActive
        case 3 => MySqlNormalRecallPurpose.FinishCommittedInactive
        case _ => throw new SQLDataException("unknown recall purpose")
      val domainRows = selected(Set("floating_pod_state"), Some(header.pod))
      val locator = selected(Set("physical_recovery_locator"), Some(header.pod))
      requireBaseRows(header, domainRows ++ locator, purpose != MySqlNormalRecallPurpose.FinishCommittedInactive, checkpoint = false)
      val original = optionalUuid(); val checkpoint = optionalUuid(); val settlement = optionalUuid()
      val predecessor = if bool() then
        val frame = dataFrame(); val bytes = frame.copyAll()
        val copied = bytes.copyBytes(budget, "recall original RAW decode input")
        Some(MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
          MySqlRecoveryOperationRawEnvelopeCodec.decode(copied, java.security.MessageDigest.getInstance("SHA-256").digest(copied).toVector, root.profile, deploymentInputs)
        })
      else None
      if purpose == MySqlNormalRecallPurpose.NormalRecall then
        require(original.isEmpty && checkpoint.isEmpty && settlement.isEmpty && predecessor.isEmpty && header.previous.isEmpty,
          "initial P cannot borrow predecessor")
      else
        require(original.nonEmpty && predecessor.nonEmpty && header.previous.contains(
          MySqlModeOperationEffectCodec.rowOperationUuid(predecessor.get.row)), "recovery P lost its original RAW predecessor")
      requirePrepareLineage(header.scope.binding,header.scope.workflow,header.pod,header.attempt,header.footprint,
        header.previous,header.scope.tag.operationId,header.scope.reserve,purpose,original,checkpoint,settlement,
        predecessor,root,budget,activeComparisonBudget)
      val sources = selected(Set("physical_recovery_obligation", "physical_source_unit", "physical_source_forest",
        "physical_source_write", "physical_source_transfer", "physical_source_obligation"), None)
      DecodedPrepare(header, purpose, domainRows, locator, original, checkpoint, settlement, predecessor, sources)

  /** 解出的每份完整原行必须对应header；不是从字段看似合理推导数据库commit许可。 */
  private def requireBaseRows(h: DecodedHeader, relations: Vector[MySqlFormat3RelationSnapshot],
      active: Boolean, checkpoint: Boolean): Unit =
    def row(table: String): Vector[MySqlFormat3Cell] =
      val rows = relations.find(_.table == table).get.rows
      require(rows.size == 1, "recall requires one exact domain/locator row")
      rows.head
    def v(row: Vector[MySqlFormat3Cell], table: String, column: String): MySqlFormat3Cell =
      MySqlModeOperationEffectCodec.value(row, table, column)
    def id(row: Vector[MySqlFormat3Cell], table: String, column: String, expected: UUID): Boolean =
      MySqlModeOperationEffectCodec.uuidEquals(v(row, table, column), expected)
    val domain = row("floating_pod_state"); val locator = row("physical_recovery_locator")
    val d = "floating_pod_state"; val l = "physical_recovery_locator"
    val version = if checkpoint then Math.incrementExact(h.version) else h.version
    val revision = if checkpoint then Math.incrementExact(h.locatorRevision) else h.locatorRevision
    val pose = if checkpoint then h.finalPose else h.committed
    require(id(domain, d, "uuid", h.pod) && id(domain, d, "binding_uuid", h.scope.binding) &&
      id(domain, d, "owner_uuid", h.owner.get) && v(domain, d, "state_version") == MySqlFormat3Cell.SignedInteger(version) &&
      v(domain, d, "is_active") == MySqlFormat3Cell.SignedInteger(if active then 1 else 0) &&
      v(domain, d, "operating_mode") == MySqlFormat3Cell.Text("NORMAL") &&
      v(domain, d, "dimension_id") == MySqlFormat3Cell.SignedInteger(h.dimension) &&
      v(domain, d, "pos_x") == MySqlFormat3Cell.Floating(pose.x) &&
      v(domain, d, "pos_y") == MySqlFormat3Cell.Floating(pose.y) &&
      v(domain, d, "pos_z") == MySqlFormat3Cell.Floating(pose.z) &&
      v(domain, d, "yaw") == MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(java.lang.Float.intBitsToFloat(pose.yaw).toDouble)) &&
      v(domain, d, "pitch") == MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(java.lang.Float.intBitsToFloat(pose.pitch).toDouble)),
      "recall domain DATA differs from exact header/base/pose")
    require(id(locator, l, "pod_uuid", h.pod) && id(locator, l, "binding_uuid", h.scope.binding) &&
      id(locator, l, "current_attempt_uuid", h.attempt) && id(locator, l, "current_footprint_uuid", h.footprint) &&
      v(locator, l, "domain_version") == MySqlFormat3Cell.SignedInteger(version) &&
      v(locator, l, "target_dimension_id") == MySqlFormat3Cell.SignedInteger(h.dimension) &&
      v(locator, l, "locator_revision") == MySqlFormat3Cell.SignedInteger(revision) &&
      v(locator, l, "locator_state") == MySqlFormat3Cell.Text(if active then "ROW_ACTIVE" else "ROW_INACTIVE_PENDING"),
      "recall locator DATA differs from exact header/base")
    if checkpoint then require(id(domain, d, "last_mutation_operation_uuid", h.scope.tag.operationId) &&
      id(locator, l, "last_mutation_operation_uuid", h.scope.tag.operationId), "C rows must retain original execute identity")

  private def strictSourceText(bytes: MySqlFormat3Binary, budget: MySqlFormat3ReadBudget): String =
    // F14B当前identity语法为固定ASCII长度；逐字节保留，拒绝BOM/UTF8替代字符等别名。
    require(bytes.length == 79, "recall native source identity length differs")
    budget.reserveRetainedMemory(256L, "recall full source identity String")
    val chars = new Array[Char](79)
    var i = 0
    while i < chars.length do
      val b = bytes.byteAt(i) & 255
      require(b < 128, "recall native source identity is not canonical ASCII")
      chars(i) = b.toChar; i += 1
    new String(chars)
  private def writeDecodedPrepare(out: DataOutputStream, value: DecodedPrepare, root: MySqlFormat3ReadBudget): Unit =
    writeDecodedHeader(out, value.header, root); out.writeByte(value.purpose.wire)
    writeDecodedRelations(out, value.domain); writeDecodedRelations(out, value.locator)
    optionUuid(out, value.originalPrepare); optionUuid(out, value.originalCheckpoint); optionUuid(out, value.settlement)
    out.writeBoolean(value.predecessor.nonEmpty)
    value.predecessor.foreach(p => { out.writeInt(p.length); p.canonical.writeTo(out) })
    writeDecodedRelations(out, value.sources)
  private def writeDecodedHeader(out: DataOutputStream, h: DecodedHeader, root: MySqlFormat3ReadBudget): Unit =
    val s = h.scope
    uuid(out, s.workflow); uuid(out, h.stop); uuid(out, s.binding); uuid(out, s.tag.bootId)
    out.writeLong(s.tag.bootGeneration); out.writeLong(s.tag.connectionGeneration); out.writeLong(s.epoch); out.writeLong(s.connectionId)
    out.writeInt(s.bytes.length); s.bytes.writeTo(out)
    out.writeLong(s.lease.toEpochSecond(ZoneOffset.UTC)); out.writeInt(s.lease.getNano / 1000)
    uuid(out, s.tag.operationId); uuid(out, s.reserve); optionUuid(out, h.previous); uuid(out, h.pod); optionUuid(out, h.owner)
    uuid(out, h.attempt); uuid(out, h.footprint); out.writeLong(h.version); out.writeLong(h.locatorRevision); out.writeLong(h.dirtyRevision)
    token(out, "NORMAL"); text(out, h.source.worldIdentity)
    val source = h.source.sourceIdentity.getBytes(java.nio.charset.StandardCharsets.UTF_8)
    out.writeInt(source.length); out.write(source); out.write(java.security.MessageDigest.getInstance("SHA-256").digest(source))
    out.writeInt(h.dimension)
    Vector(h.committed, h.finalPose).foreach { p => out.writeLong(p.x); out.writeLong(p.y); out.writeLong(p.z); out.writeInt(p.yaw); out.writeInt(p.pitch) }
    footprint(out, h.chunks)
    val profile = root.profile.canonicalBytes()
    out.writeInt(profile.length); out.write(profile); out.write(root.profile.sha256())
  private def writeDecodedRelations(out: DataOutputStream, values: Vector[MySqlFormat3RelationSnapshot]): Unit =
    out.writeInt(values.size)
    values.foreach { relation =>
      val shape = MySqlFormat3RelationShapes.forTable(relation.table)
      text(out, relation.table); out.writeInt(shape.columns.size)
      shape.columns.foreach(c => { text(out, c.name); text(out, c.sqlDeclaration) })
      out.writeInt(relation.rows.size)
      relation.rows.foreach(_.foreach(cell(out, _)))
    }
  private def framedBinary(out: DataOutputStream, value: MySqlFormat3Binary): Unit =
    out.writeInt(value.length); value.writeTo(out)
    val digest = java.security.MessageDigest.getInstance("SHA-256")
    var i = 0
    while i < value.length do
      digest.update(value.byteAt(i)); i += 1
    out.write(digest.digest())

  /** 严格DATA解码；借原不可变输入，不发行旧boot的opaque ACK或恢复许可。 */
  def decodeRemoval(input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget): MySqlNormalRecallRemovalFacts =
    decodeRemovalWithInputs(input,root,None)

  private[mysql] def decodeDeploymentReceiptRemoval(input:MySqlFormat3Binary,
      inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget):MySqlNormalRecallRemovalFacts =
    decodeRemovalWithInputs(input,root,Some(inputs))

  private def decodeRemovalWithInputs(input:MySqlFormat3Binary,root:MySqlFormat3ReadBudget,
      inputs:Option[MySqlScriptSchemaInputs]):MySqlNormalRecallRemovalFacts =
    decodePhysical(input, root, 1024L,inputs) { reader =>
      val result = reader.removal()
      reader.finish()
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        require(removal(result, root).exactlyMatchesCell(MySqlFormat3Cell.Binary(input)), "noncanonical recall removal")
      }
      result
    }

  def decodeTicket(input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget): MySqlNormalRecallTicketFacts =
    decodeTicketWithInputs(input,root,None)

  private[mysql] def decodeDeploymentReceiptTicket(input:MySqlFormat3Binary,
      inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget):MySqlNormalRecallTicketFacts =
    decodeTicketWithInputs(input,root,Some(inputs))

  private def decodeTicketWithInputs(input:MySqlFormat3Binary,root:MySqlFormat3ReadBudget,
      inputs:Option[MySqlScriptSchemaInputs]):MySqlNormalRecallTicketFacts =
    decodeTicketWithInputs(input, root, inputs, MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root))
  private def decodeTicketWithInputs(input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget,
      inputs: Option[MySqlScriptSchemaInputs], dataBudget: MySqlFormat3ReadBudget): MySqlNormalRecallTicketFacts =
    decodeTicketWithHistoryOwners(input, root, inputs, dataBudget, null)

  private def decodeTicketWithActiveHistoryComparisonOwner(input: MySqlFormat3Binary,
      root: MySqlFormat3ReadBudget, inputs: Option[MySqlScriptSchemaInputs],
      dataBudget: MySqlFormat3ReadBudget,
      activeComparisonBudget: MySqlFormat3ReadBudget): MySqlNormalRecallTicketFacts =
    decodeTicketWithHistoryOwners(input, root, inputs, dataBudget, activeComparisonBudget)

  private def decodeTicketWithHistoryOwners(input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget,
      inputs: Option[MySqlScriptSchemaInputs], dataBudget: MySqlFormat3ReadBudget,
      activeComparisonBudget: MySqlFormat3ReadBudget): MySqlNormalRecallTicketFacts =
    if activeComparisonBudget != null then
      requireHistoryComparisonOwner(root, dataBudget, activeComparisonBudget)
    val physicalBudget = if activeComparisonBudget == null then dataBudget else activeComparisonBudget
    decodePhysical(input, root, 8192L, physicalBudget, inputs) { reader =>
      require(root.profile.maxMembers() >= 25L, "original profile cannot retain the exact footprint")
      reader.domain(TicketDomain)
      val nested = reader.framed()
      val removed = nested.removal()
      nested.finish()
      reader.exactDigest(nested)
      require(reader.int32() == 25, "ticket footprint count must be exactly twenty-five")
      val chunks = Vector.fill(25)(PhysicalRecoveryChunk(reader.int32(), reader.int32(), reader.int32()))
      require(chunks == chunks.sortBy(c => (c.dimensionId, c.chunkX, c.chunkZ)), "ticket footprint is not in canonical order")
      require(reader.uuid() == removed.tag.bootId, "ticket boot differs from original removal")
      val stop = reader.uuid()
      require(reader.uuid() == removed.tag.operationId && reader.uuid() == removed.podUuid &&
        reader.uuid() == removed.attemptUuid, "ticket nested removal identities differ")
      require(reader.octet() == 1 && reader.octet() == 1 && reader.octet() == 1,
        "ticket registry/forced/requested absence results must all be exact")
      reader.finish()
      val result = MySqlNormalRecallTicketFacts(removed, stop, chunks)
      val compare = () =>
        require(ticket(result, root).exactlyMatchesCell(MySqlFormat3Cell.Binary(input)), "noncanonical recall ticket")
      if activeComparisonBudget == null then MySqlRecoveryCanonicalBytes.withComparisonBudget(root) { compare() }
      else MySqlRecoveryCanonicalBytes.withAllocationBudget(activeComparisonBudget) { compare() }
      result
    }

  private def decodePhysical[A](input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget, retained: Long,
      deploymentInputs:Option[MySqlScriptSchemaInputs]=None)
      (read: PhysicalReader => A): A =
    decodePhysical(input, root, retained, MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root), deploymentInputs)(read)
  private def decodePhysical[A](input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget, retained: Long,
      dataBudget: MySqlFormat3ReadBudget, deploymentInputs: Option[MySqlScriptSchemaInputs])
      (read: PhysicalReader => A): A =
    deploymentInputs match
      case None=>requireRoot(root)
      case Some(inputs)=>
        require(root!=null && !root.isClosed && !root.isScopedReadBudget &&
          inputs.dataLayout==MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
          (inputs.sourceLimitProfile eq root.profile),"original open Full26 scope root required")
        dataBudget.requireHistoricalPayloadInputs(inputs)
    val budget = dataBudget
    require(budget.sharesAllocationLedger(root) && ((budget eq root) || budget.isCanonicalChildOf(root)),
      "physical decoder cannot switch original allocation root")
    require(input != null && input.length > 0 && input.length.toLong <= Math.min(
      Math.min(root.profile.maxSqlPacketBytes(), root.profile.maxRetainedBytes()), MySqlFormat3JvmArrayLimit.MaximumLength),
      "bounded original recall physical input required")
    budget.reserveRetainedMemory(retained, "recall decoded physical DATA")
    budget.reserveTemporaryMemory(8192L, "recall physical reader/digest/footprint validation")
    var primary: Throwable = null
    try read(new PhysicalReader(input, 0, input.length))
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      try budget.releaseTemporaryMemory(8192L, "recall physical reader/digest/footprint validation")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  private class PhysicalReader(input: MySqlFormat3Binary, val start: Int, val end: Int):
    protected var offset = start
    protected def need(count: Long): Unit =
      if count < 0L || count > end.toLong - offset.toLong then
        throw new SQLDataException("negative/overflow/truncated recall physical framing")
    def octet(): Int =
      need(1L)
      val result = input.byteAt(offset) & 255
      offset += 1
      result
    def int32(): Int = (octet() << 24) | (octet() << 16) | (octet() << 8) | octet()
    def int64(): Long =
      var result = 0L
      var n = 0
      while n < 8 do
        result = (result << 8) | octet().toLong
        n += 1
      result
    def uuid(): UUID =
      val result = new UUID(int64(), int64())
      require(MySqlNormalRecallRequest.nonZero(result), "zero physical identity is not canonical")
      result
    def domain(expected: String): Unit =
      require(int32() == expected.length, "recall physical domain length differs")
      expected.foreach(c => require(octet() == c.toInt, "recall physical domain differs"))
    def framed(): PhysicalReader =
      val size = int32()
      need(size.toLong)
      val nested = new PhysicalReader(input, offset, offset + size)
      offset += size
      nested
    def exactDigest(nested: PhysicalReader): Unit =
      need(32L)
      val digest = java.security.MessageDigest.getInstance("SHA-256")
      var n = nested.start
      while n < nested.end do
        digest.update(input.byteAt(n))
        n += 1
      val expected = digest.digest()
      expected.foreach(value => require(octet().toByte == value, "nested recall removal digest differs"))
    def removal(): MySqlNormalRecallRemovalFacts =
      domain(RemovalDomain)
      val tag = IoOperationTag(uuid(), int64(), int64(), uuid())
      MySqlNormalRecallRemovalFacts(tag, uuid(), uuid(), int64(), int32(), int32(), int64(), int64(), int64())
    def finish(): Unit = require(offset == end, "unconsumed recall physical tail")

  def removal(facts: MySqlNormalRecallRemovalFacts, root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    require(facts != null, "original removal facts required")
    encode(RemovalDomain, root) { output => writeRemoval(output, facts) }

  def ticket(facts: MySqlNormalRecallTicketFacts, root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    require(facts != null, "original ticket facts required")
    val removed = removal(facts.removal, root)
    encode(TicketDomain, root) { output =>
      framedWithHash(output, removed)
      footprint(output, facts.footprint)
      uuid(output, facts.removal.tag.bootId); uuid(output, facts.stopRequest)
      uuid(output, facts.removal.tag.operationId); uuid(output, facts.removal.podUuid)
      uuid(output, facts.removal.attemptUuid)
      // 此载荷只有成功结果类型；权限仍由原 release holder/opaque ACK 验证。
      output.writeByte(1); output.writeByte(1); output.writeByte(1)
    }

  def prepare(header: MySqlNormalRecallHeader, purpose: MySqlNormalRecallPurpose,
      original: MySqlModeOperationEffectCodec.Image, predecessor: Option[MySqlRecoveryOperationRawEnvelope],
      originalPrepare: Option[UUID], originalCheckpoint: Option[UUID], settlement: Option[UUID],
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    requireHeader(header, root)
    requireSnapshot(original, root,header.context.schemaFormat)
    require(purpose != null && predecessor != null && !predecessor.exists(_ == null) &&
      originalPrepare != null && originalCheckpoint != null && settlement != null &&
      (originalPrepare ++ originalCheckpoint ++ settlement).forall(MySqlNormalRecallRequest.nonZero),
      "complete fixed recall predecessor identities required")
    if purpose == MySqlNormalRecallPurpose.NormalRecall then
      require(originalPrepare.isEmpty && originalCheckpoint.isEmpty && settlement.isEmpty &&
        predecessor.isEmpty && header.previousOperation.isEmpty, "initial recall cannot borrow a predecessor")
    else
      require(originalPrepare.nonEmpty && predecessor.nonEmpty && header.previousOperation.nonEmpty,
        "recovery prepare requires its original committed or settled predecessor")
    requirePrepareLineage(header.context.bindingUuid,header.context.workflowUuid,header.request.podUuid,
      header.request.attemptUuid,header.request.footprintUuid,header.previousOperation,
      header.context.tag.operationId,header.context.reserveOperationUuid,purpose,
      originalPrepare,originalCheckpoint,settlement,predecessor,root)
    encode(PrepareDomain, root) { output =>
      writeHeader(output, header, root)
      output.writeByte(purpose.wire)
      writeSelected(output, original, Set("floating_pod_state"), Some(header.request.podUuid))
      writeSelected(output, original, Set("physical_recovery_locator"), Some(header.request.podUuid))
      optionUuid(output, originalPrepare); optionUuid(output, originalCheckpoint); optionUuid(output, settlement)
      output.writeBoolean(predecessor.nonEmpty)
      predecessor.foreach { raw =>
        val actual = MySqlModeOperationEffectCodec.rowOperationUuid(raw.row)
        require(header.previousOperation.contains(actual), "recovery predecessor row differs")
        output.writeInt(raw.length); raw.canonical.writeTo(output)
      }
      writeSelected(output, original, Set("physical_recovery_obligation", "physical_source_unit",
        "physical_source_forest", "physical_source_write", "physical_source_transfer", "physical_source_obligation"), None)
    }

  /** P2原始行的封闭DATA关联；不证明negative语义、共同锁或发行恢复资格。 */
  private def requirePrepareLineage(binding:UUID,workflow:UUID,pod:UUID,attempt:UUID,footprint:UUID,
      previous:Option[UUID],operation:UUID,reserve:UUID,purpose:MySqlNormalRecallPurpose,original:Option[UUID],checkpoint:Option[UUID],
      settlement:Option[UUID],predecessor:Option[MySqlRecoveryOperationRawEnvelope],root:MySqlFormat3ReadBudget,
      dataBudget:MySqlFormat3ReadBudget = null,
      activeComparisonBudget:MySqlFormat3ReadBudget = null):Unit =
    if purpose!=MySqlNormalRecallPurpose.NormalRecall then
      def compare(): Unit =
        val budget=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
        budget.reserveRetainedMemory(4096L,"P2 complete predecessor lineage comparison wrappers BEFORE allocation")
        require(original.nonEmpty && predecessor.nonEmpty,"P2 requires its original P and complete predecessor")
        val originalIds=original.toVector++checkpoint.toVector++settlement.toVector
        require(originalIds.distinct.size==originalIds.size,"P2 original P/C/S identities must be distinct")
        require(!originalIds.contains(operation) && !originalIds.contains(reserve),"P2 cannot reuse an original P/C/S operation identity")
        val row=predecessor.get.row
        val shape=MySqlFormat3RelationShapes.forTable("fp_recovery_operation")
        def cell(name:String)=row(shape.columns.indexWhere(_.name==name))
        def matches(name:String,id:UUID)=cell(name) match
          case MySqlFormat3Cell.Binary(bytes)=>bytes.equalsUuid(id)
          case _=>false
        Vector("binding_uuid"->binding,"workflow_uuid"->workflow,"pod_uuid"->pod,
          "attempt_uuid"->attempt,"footprint_uuid"->footprint).foreach { (column,id)=>
          require(matches(column,id),"P2 predecessor belongs to another exact "+column)
        }
        require(previous.exists(matches("operation_uuid",_)) && cell("operation_state")==MySqlFormat3Cell.Text("COMMITTED"),
          "P2 requires its exact COMMITTED predecessor")
        purpose match
          case MySqlNormalRecallPurpose.RestoreCommittedActive=>
            checkpoint match
              case None=>
                require(settlement.isEmpty && matches("operation_uuid",original.get) &&
                  cell("step")==MySqlFormat3Cell.Text("RECALL_PREPARE"),"active P2 without C must follow original P without S")
              case Some(c)=>
                require(settlement.nonEmpty && matches("operation_uuid",settlement.get) &&
                  matches("previous_step_operation_uuid",c) && cell("step")==MySqlFormat3Cell.Text("CANCEL"),
                  "active P2 with C must follow its exact committed S")
          case MySqlNormalRecallPurpose.FinishCommittedInactive=>
            require(checkpoint.nonEmpty && settlement.isEmpty && matches("operation_uuid",checkpoint.get) &&
              matches("previous_step_operation_uuid",original.get) && cell("step")==MySqlFormat3Cell.Text("RECALL_CHECKPOINT"),
              "inactive P2 must follow original committed C without S")
          case MySqlNormalRecallPurpose.NormalRecall=>()
      if activeComparisonBudget == null then MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        compare()
      }
      else
        requireHistoryComparisonOwner(root, dataBudget, activeComparisonBudget)
        MySqlRecoveryCanonicalBytes.withAllocationBudget(activeComparisonBudget) {
          compare()
        }
  def checkpoint(header: MySqlNormalRecallHeader, prepared: MySqlRecoveryCanonicalBytes,
      removed: MySqlNormalRecallRemovalFacts, released: MySqlNormalRecallTicketFacts,
      receipt: UUID, before: MySqlModeOperationEffectCodec.Image, target: MySqlModeOperationEffectCodec.Image,
      root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    requireHeader(header, root); requireSnapshot(before, root,header.context.schemaFormat); requireSnapshot(target, root,header.context.schemaFormat)
    checkpointRelations(header, prepared, removed, released, receipt, before, target.relations, root)

  /** 未完成 C 草稿只能编码 selected DATA，不能取得 snapshot 资格。 */
  private[mysql] def checkpointConstruction(header: MySqlNormalRecallHeader, prepared: MySqlRecoveryCanonicalBytes,
      removed: MySqlNormalRecallRemovalFacts, released: MySqlNormalRecallTicketFacts,
      receipt: UUID, before: MySqlModeOperationEffectCodec.Image, base: MySqlModeOperationEffectCodec.Image,
      targetRelations: Vector[MySqlFormat3RelationSnapshot], root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    requireHeader(header, root); requireSnapshot(before, root, header.context.schemaFormat)
    requireSnapshot(base, root, header.context.schemaFormat)
    require(base eq before, "checkpoint construction must retain its exact original validated base")
    MySqlNormalRecallOperationEffectCodec.requireConstructionRelations(base, targetRelations, root)
    checkpointRelations(header, prepared, removed, released, receipt, before, targetRelations, root)

  private def checkpointRelations(header: MySqlNormalRecallHeader, prepared: MySqlRecoveryCanonicalBytes,
      removed: MySqlNormalRecallRemovalFacts, released: MySqlNormalRecallTicketFacts,
      receipt: UUID, before: MySqlModeOperationEffectCodec.Image,
      targetRelations: Vector[MySqlFormat3RelationSnapshot], root: MySqlFormat3ReadBudget): MySqlRecoveryCanonicalBytes =
    require(prepared != null && prepared.encoding == PrepareDomain && removed != null && released != null &&
      released.removal == removed && released.stopRequest == header.request.stopRequest &&
      released.footprint.toSet == header.request.originalFootprint.toSet && MySqlNormalRecallRequest.nonZero(receipt),
      "original prepare and matching physical facts required")
    val request = header.request
    require(header.previousOperation.contains(removed.tag.operationId) &&
      removed.tag.bootId == header.context.tag.bootId &&
      removed.tag.bootGeneration == header.context.tag.bootGeneration &&
      removed.tag.connectionGeneration == header.context.tag.connectionGeneration &&
      removed.podUuid == request.podUuid && removed.attemptUuid == request.attemptUuid &&
      removed.expectedVersion == request.criticalBase.stateVersion && removed.dimensionId == request.finalPose.dimensionId,
      "late ACK must retain original P tag, attempt, version and current owner generation")
    val removalBytes = removal(removed, root)
    val ticketBytes = ticket(released, root)
    encode(CheckpointDomain, root) { output =>
      writeHeader(output, header, root)
      framedWithHash(output, prepared); framedWithHash(output, removalBytes); framedWithHash(output, ticketBytes)
      output.writeLong(Math.addExact(request.criticalBase.stateVersion, 1L))
      output.writeLong(Math.addExact(header.locatorRevision, 1L)); uuid(output, receipt)
      writeSelected(output, before, Set("physical_recovery_obligation"), Some(request.podUuid))
      writeSelectedRelations(output, targetRelations, Set("physical_recovery_obligation"), Some(request.podUuid))
      writeSelectedRelations(output, targetRelations, Set("floating_pod_state", "physical_recovery_locator"), Some(request.podUuid))
    }

  /** 两个明确派生列；旧行、preimage和其他 completion 字段没有 marker 资格。 */
  def writeDerivedCell(output: DataOutputStream, phase: MySqlOperationEffectPhase,
      image: MySqlOperationEffectImage, table: String, key: Vector[MySqlFormat3Cell],
      column: String, sqlType: String, actual: MySqlFormat3Cell, binding: UUID,
      receipt: UUID, operation: UUID): Unit =
    require(output != null && phase == MySqlOperationEffectPhase.Execute && image == MySqlOperationEffectImage.Target &&
      table == "fp_lifecycle_completion" && ContextColumns.contains(column) && actual != null &&
      actual != MySqlFormat3Cell.SqlNull && MySqlNormalRecallRequest.nonZero(binding) &&
      MySqlNormalRecallRequest.nonZero(receipt) && MySqlNormalRecallRequest.nonZero(operation),
      "only exact current completion context target cells may be derived")
    val shape = MySqlFormat3RelationShapes.forTable(table)
    val expectedType = shape.columns.find(_.name == column).get.sqlDeclaration
    val validValue = actual match
      case MySqlFormat3Cell.Binary(value) =>
        if column == "original_context_sha256" then value.length == 32 else value.length > 0
      case _ => false
    require(sqlType == expectedType && key != null && key.size == 2 &&
      MySqlModeOperationEffectCodec.uuidEquals(key.head, binding) &&
      MySqlModeOperationEffectCodec.uuidEquals(key(1), receipt) && validValue, "derived completion key/type differs")
    token(output, DerivedCellDomain); token(output, "EXECUTE"); token(output, table.toUpperCase(java.util.Locale.ROOT))
    token(output, "BINDING_UUID"); uuid(output, binding); token(output, "RECEIPT_UUID"); uuid(output, receipt)
    token(output, column.toUpperCase(java.util.Locale.ROOT)); text(output, sqlType); uuid(output, operation)
    token(output, "C_ORIGINAL_CONTEXT"); output.writeByte(if column == "original_context_bytes" then 1 else 2)

  /** 验证只保留固定 SHA/context，不物化完整比较 payload。 */
  private[mysql] final case class StreamedCanonical(length: Int, sha256: Vector[Byte], matches: Boolean)

  private[mysql] def fingerprintStream(domain: String, root: MySqlFormat3ReadBudget)
      (write: DataOutputStream => Unit): StreamedCanonical =
    requireCanonicalRoot(root)
    val budget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    require(budget.sharesAllocationLedger(root) && ((budget eq root) || budget.isCanonicalChildOf(root)),
      "recall streaming fingerprint cannot switch allocation owner")
    budget.reserveRetainedMemory(640L, "Recall streaming fingerprint SHA/context")
    try scanCanonical(domain, root, None)(write)
    catch
      case cause: Throwable =>
        try budget.releaseRetainedMemory(640L, "failed Recall streaming fingerprint")
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
        throw cause

  private[mysql] def matchesStream(domain: String, expected: MySqlFormat3Binary,
      root: MySqlFormat3ReadBudget)(write: DataOutputStream => Unit): Boolean =
    require(expected != null, "original canonical comparison payload required")
    scanCanonical(domain, root, Some(expected))(write).matches

  private def scanCanonical(domain: String, root: MySqlFormat3ReadBudget,
      expected: Option[MySqlFormat3Binary])(write: DataOutputStream => Unit): StreamedCanonical =
    requireCanonicalRoot(root)
    val budget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    require(budget.sharesAllocationLedger(root) && ((budget eq root) || budget.isCanonicalChildOf(root)),
      "recall streaming comparison cannot switch allocation owner")
    val limit = Math.min(Math.min(root.profile.maxSqlPacketBytes(), root.profile.maxRetainedBytes()),
      MySqlFormat3JvmArrayLimit.MaximumLength)
    budget.reserveTemporaryMemory(2048L, "Recall streaming digest/comparison workspace")
    var primary: Throwable = null
    try
      val digest = java.security.MessageDigest.getInstance("SHA-256")
      var length = 0L
      var equal = true
      val sink = new java.io.OutputStream:
        override def write(value: Int): Unit =
          val next = Math.addExact(length, 1L)
          if next > limit then throw new SQLDataException(
            s"recall canonical payload exceeds original profile: domain=$domain bytes=$next limit=$limit")
          expected.foreach { original =>
            if length >= original.length || original.byteAt(length.toInt) != value.toByte then equal = false
          }
          digest.update(value.toByte)
          length = next
        override def write(bytes: Array[Byte], offset: Int, count: Int): Unit =
          java.util.Objects.checkFromIndexSize(offset, count, bytes.length)
          val next = Math.addExact(length, count.toLong)
          if next > limit then throw new SQLDataException(
            s"recall canonical payload exceeds original profile: domain=$domain bytes=$next limit=$limit")
          expected.foreach { original =>
            var index = 0
            while index < count do
              val position = length + index
              if position >= original.length || original.byteAt(position.toInt) != bytes(offset + index) then equal = false
              index += 1
          }
          digest.update(bytes, offset, count)
          length = next
      val output = new DataOutputStream(sink)
      token(output, domain); write(output); output.flush()
      StreamedCanonical(length.toInt, digest.digest().toVector,
        equal && expected.forall(_.length.toLong == length))
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      try budget.releaseTemporaryMemory(2048L, "Recall streaming workspace release")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  private[mysql] def encode(domain: String, root: MySqlFormat3ReadBudget)
      (write: DataOutputStream => Unit): MySqlRecoveryCanonicalBytes =
    requireCanonicalRoot(root)
    val budget = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    require(budget.sharesAllocationLedger(root) && ((budget eq root) || budget.isCanonicalChildOf(root)),
      "recall encoding cannot switch allocation ledger or sibling")
    MySqlRecoveryCanonicalBytes.withAllocationBudget(budget) {
      val length = measure(budget) { output => token(output, domain); write(output) }
      val limit = Math.min(Math.min(root.profile.maxSqlPacketBytes(), root.profile.maxRetainedBytes()),
        MySqlFormat3JvmArrayLimit.MaximumLength)
      if length > limit then throw new SQLDataException(
        s"recall canonical payload exceeds original profile: domain=$domain bytes=$length limit=$limit")
      MySqlRecoveryCanonicalBytes.encodeExact(domain, root.profile, length) { output =>
        token(output, domain); write(output)
      }
    }

  private def writeHeader(output: DataOutputStream, header: MySqlNormalRecallHeader,
      root: MySqlFormat3ReadBudget): Unit =
    val request = header.request
    val context = header.context
    uuid(output, context.workflowUuid); uuid(output, request.stopRequest); uuid(output, context.bindingUuid)
    uuid(output, context.tag.bootId); output.writeLong(context.tag.bootGeneration)
    output.writeLong(context.tag.connectionGeneration); output.writeLong(context.writerEpoch)
    output.writeLong(context.actualConnectionId)
    // 原 session/barrier 身份不包含尚未生成的 request/context fingerprint。
    val sessionLength = measure(root)(context.writeScopeIdentityFields)
    output.writeInt(Math.toIntExact(sessionLength)); context.writeScopeIdentityFields(output)
    output.writeLong(context.fixedLeaseUntil.toEpochSecond(ZoneOffset.UTC))
    output.writeInt(context.fixedLeaseUntil.getNano / 1000)
    uuid(output, context.tag.operationId); uuid(output, context.reserveOperationUuid)
    optionUuid(output, header.previousOperation); uuid(output, request.podUuid)
    optionUuid(output, request.criticalBase.ownerUuid); uuid(output, request.attemptUuid); uuid(output, request.footprintUuid)
    output.writeLong(request.criticalBase.stateVersion); output.writeLong(header.locatorRevision)
    output.writeLong(request.dirtyRevision); token(output, request.criticalBase.operatingMode.databaseValue)
    text(output, request.source.worldIdentity)
    // 冻结 F14B：全文 UTF-8 无 BOM，不把 DB capture provenance 当作世界来源。
    val sourceBytes = request.source.sourceIdentity.getBytes(java.nio.charset.StandardCharsets.UTF_8)
    output.writeInt(sourceBytes.length); output.write(sourceBytes)
    output.write(java.security.MessageDigest.getInstance("SHA-256").digest(sourceBytes))
    output.writeInt(request.finalPose.dimensionId); pose(output, request.committedPose); pose(output, request.finalPose)
    footprint(output, request.originalFootprint)
    val profileBytes = root.profile.canonicalBytes()
    output.writeInt(profileBytes.length); output.write(profileBytes); hash(output, root.profile.sha256().toVector)

  private[mysql] def writeRemoval(output: DataOutputStream, facts: MySqlNormalRecallRemovalFacts): Unit =
    uuid(output, facts.tag.bootId); output.writeLong(facts.tag.bootGeneration)
    output.writeLong(facts.tag.connectionGeneration); uuid(output, facts.tag.operationId)
    uuid(output, facts.podUuid); uuid(output, facts.attemptUuid); output.writeLong(facts.expectedVersion)
    output.writeInt(facts.dimensionId); output.writeInt(facts.entityId)
    output.writeLong(facts.capturedEnd); output.writeLong(facts.requestedEnd); output.writeLong(facts.observedEnd)

  private def writeSelected(output: DataOutputStream, snapshot: MySqlModeOperationEffectCodec.Image,
      tables: Set[String], pod: Option[UUID]): Unit =
    writeSelectedRelations(output, snapshot.relations, tables, pod)

  private def writeSelectedRelations(output: DataOutputStream, completeRelations: Vector[MySqlFormat3RelationSnapshot],
      tables: Set[String], pod: Option[UUID]): Unit =
    require(tables.forall(table => completeRelations.exists(_.table == table)), "complete typed recall relations required")
    output.writeInt(tables.size)
    completeRelations.iterator.filter(relation => tables.contains(relation.table)).foreach { relation =>
      val shape = MySqlFormat3RelationShapes.forTable(relation.table)
      val podColumn = if relation.table == "floating_pod_state" then "uuid" else "pod_uuid"
      val index = shape.columns.indexWhere(_.name == podColumn)
      require(pod.isEmpty || index >= 0, "pod relation key required")
      def included(row: Vector[MySqlFormat3Cell]): Boolean =
        pod.forall(value => MySqlModeOperationEffectCodec.uuidEquals(row(index), value))
      text(output, relation.table); output.writeInt(shape.columns.size)
      shape.columns.foreach { column => text(output, column.name); text(output, column.sqlDeclaration) }
      output.writeInt(relation.rows.count(included))
      relation.rows.iterator.filter(included).foreach(_.foreach(cell(output, _)))
    }

  private[mysql] def cell(output: DataOutputStream, value: MySqlFormat3Cell): Unit = value match
    case MySqlFormat3Cell.SqlNull => output.writeByte(0)
    case MySqlFormat3Cell.Text(v) => output.writeByte(1); text(output, v)
    case MySqlFormat3Cell.Binary(v) => output.writeByte(2); output.writeInt(v.length); v.writeTo(output)
    case MySqlFormat3Cell.SignedInteger(v) => output.writeByte(3); output.writeLong(v)
    case MySqlFormat3Cell.Floating(bits) => output.writeByte(4); output.writeLong(bits)
    case MySqlFormat3Cell.DateTime(v) =>
      output.writeByte(5); output.writeInt(v.getYear); output.writeByte(v.getMonthValue); output.writeByte(v.getDayOfMonth)
      output.writeByte(v.getHour); output.writeByte(v.getMinute); output.writeByte(v.getSecond); output.writeInt(v.getNano)

  private def requireCanonicalRoot(root:MySqlFormat3ReadBudget):Unit =
    require(root != null && !root.isClosed && !root.isScopedReadBudget, "original open recall root required")

  private def requireRoot(root: MySqlFormat3ReadBudget): Unit =
    requireCanonicalRoot(root)
    root.requireFormat3Full20Layout()
  private def requireHeader(header: MySqlNormalRecallHeader, root: MySqlFormat3ReadBudget): Unit =
    requireCanonicalRoot(root)
    require(header != null && (header.context.exactLimitProfile eq root.profile), "exact recall context profile required")
    header.context.schemaFormat match
      case 3=>root.requireFormat3Full20Layout()
      case 5=>()
      case _=>throw new SQLDataException("recall header has unsupported complete-image schema family")
  private def requireSnapshot(snapshot: MySqlModeOperationEffectCodec.Image, root: MySqlFormat3ReadBudget,
      schemaFormat:Int): Unit =
    require(snapshot != null && !snapshot.allocationBudget.isClosed &&
      snapshot.allocationBudget.sharesAllocationLedger(root) && (snapshot.limitProfile eq root.profile) &&
      snapshot.rowCount <= root.profile.maxMembers(), "complete bounded original full20 image required")
    snapshot match
      case _:MySqlFormat3FullSnapshot=>
        require(schemaFormat==3,"legacy recall image requires its original schema3 header")
        root.requireFormat3Full20Layout()
      case deployment:MySqlScriptSchemaSnapshot=>
        require(schemaFormat==5,"Full26 recall image requires its original schema5 header")
        MySqlFormat3QueryProjection.requireDeploymentSource(deployment,deployment.inputs,root)
  private def measure(root: MySqlFormat3ReadBudget)(write: DataOutputStream => Unit): Long =
    root.reserveTemporaryMemory(512L, "recall canonical count")
    var primary: Throwable = null
    try
      val count = new MySqlFormat3CountingOutputStream
      val output = new DataOutputStream(count)
      write(output); output.flush(); count.byteLength
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      try root.releaseTemporaryMemory(512L, "recall canonical count")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
  private def footprint(output: DataOutputStream, values: Vector[PhysicalRecoveryChunk]): Unit =
    output.writeInt(25)
    values.sortBy(c => (c.dimensionId, c.chunkX, c.chunkZ)).foreach { c =>
      output.writeInt(c.dimensionId); output.writeInt(c.chunkX); output.writeInt(c.chunkZ)
    }
  private def pose(output: DataOutputStream, value: FloatingPodPose): Unit =
    output.writeLong(java.lang.Double.doubleToRawLongBits(value.posX)); output.writeLong(java.lang.Double.doubleToRawLongBits(value.posY))
    output.writeLong(java.lang.Double.doubleToRawLongBits(value.posZ)); output.writeInt(java.lang.Float.floatToRawIntBits(value.yaw))
    output.writeInt(java.lang.Float.floatToRawIntBits(value.pitch))
  private def framedWithHash(output: DataOutputStream, value: MySqlRecoveryCanonicalBytes): Unit =
    output.writeInt(value.length); value.writeTo(output); hash(output, value.sha256)
  private def hash(output: DataOutputStream, value: Vector[Byte]): Unit =
    require(value != null && value.size == 32, "SHA-256 requires 32 original bytes")
    value.foreach(output.writeByte(_))
  private def bytes(output: DataOutputStream, value: Vector[Byte]): Unit =
    output.writeInt(value.size); value.foreach(output.writeByte(_))
  private def token(output: DataOutputStream, value: String): Unit = MySqlRecoveryCanonicalBytes.writeToken(output, value)
  private def text(output: DataOutputStream, value: String): Unit = MySqlRecoveryCanonicalBytes.writeText(output, value)
  private def uuid(output: DataOutputStream, value: UUID): Unit = MySqlRecoveryCanonicalBytes.writeUuid(output, value)
  private def optionUuid(output: DataOutputStream, value: Option[UUID]): Unit = MySqlRecoveryCanonicalBytes.writeOptionUuid(output, value)
