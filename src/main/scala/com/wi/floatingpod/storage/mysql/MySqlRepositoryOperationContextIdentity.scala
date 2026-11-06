package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.IoOperationTag
import java.io.DataOutputStream
import java.sql.{SQLDataException, SQLNonTransientConnectionException}
import java.time.LocalDateTime
import java.util.UUID

/** 只由实际 command scope、held session 与 admission 捕获的连接/操作身份；不含效果或目标摘要。 */
private[mysql] final class MySqlRepositoryOperationContextIdentity private[mysql] (
    val tag: IoOperationTag,
    val workflowUuid: UUID,
    val reserveOperationUuid: UUID,
    val barrierGenerationUuid: UUID,
    val actualConnectionId: Long,
    val barrierNamespace: String,
    val bindingUuid: UUID,
    val writerEpoch: Long,
    val fixedLeaseUntil: LocalDateTime,
    val serverUuid: UUID,
    val serverVersion: String,
    val schema: String,
    val effectiveUser: String,
    val provisioningUuid: UUID,
    val schemaFormat: Int,
    val physicalFormat: Int,
    val ddlSha256: String,
    val approvedObjectManifestSha256: String,
    val configurationSha256: Vector[Byte],
    val sourceProvenanceKind: String,
    val sourceProvenanceSha256: Vector[Byte],
    val limitProfileSha256: Vector[Byte],
    val requestKind: String,
    val requestIdentitySha256: Vector[Byte],
    private val profile: com.wi.floatingpod.config.SourceLimitProfile
):
  import MySqlRepositoryOperationContextIdentity.{Encoding, hex, writeBytes, writeDateTime6}

  require(tag != null && workflowUuid != null && reserveOperationUuid != null && barrierGenerationUuid != null &&
    actualConnectionId > 0L && barrierNamespace != null && bindingUuid != null && writerEpoch > 0L && fixedLeaseUntil != null &&
    serverUuid != null && serverVersion != null && schema != null && effectiveUser != null && provisioningUuid != null &&
    (schemaFormat == 3 || schemaFormat == 5) && physicalFormat == 2 && ddlSha256 != null && approvedObjectManifestSha256 != null &&
    configurationSha256 != null && configurationSha256.size == 32 && sourceProvenanceKind != null &&
    sourceProvenanceSha256 != null && sourceProvenanceSha256.size == 32 && limitProfileSha256 != null &&
    limitProfileSha256.size == 32 && requestKind != null && requestIdentitySha256 != null &&
    requestIdentitySha256.size == 32 && profile != null, "complete non-self-referential operation context identity is required")
  require(nonZero(tag.bootId) && nonZero(tag.operationId) && nonZero(workflowUuid) && nonZero(reserveOperationUuid) &&
    nonZero(barrierGenerationUuid) && nonZero(bindingUuid) && nonZero(serverUuid) && nonZero(provisioningUuid),
    "operation context UUID identities must be non-zero")
  require(fixedLeaseUntil.getYear >= 1000 && fixedLeaseUntil.getYear <= 9999 && fixedLeaseUntil.getNano % 1000 == 0,
    "MySQL DATETIME(6) fixed lease timestamp must retain exact microsecond precision")
  require(barrierNamespace.matches("fpb1:[A-Za-z0-9_-]{43}"), "exact held barrier namespace is malformed")
  require(requestKind == "RUN" || requestKind == "TRANSACTION", "fixed original request kind is unsupported")
  require(ddlSha256.matches("(?i)[0-9a-f]{64}") && approvedObjectManifestSha256.matches("(?i)[0-9a-f]{64}"),
    "approved schema and object manifest digests must be exact SHA-256 values")
  require(profile.sha256().toVector == limitProfileSha256, "operation context profile bytes differ from their exact digest")

  private[mysql] def exactLimitProfile = profile
  def canonical: MySqlRecoveryCanonicalBytes = MySqlRecoveryCanonicalBytes.encode(Encoding, profile)(writeCanonical)
  def canonicalSha256: Vector[Byte] = canonical.sha256
  def canonicalLength: Int = canonical.length

  private[mysql] def writeIdentityFields(output: DataOutputStream): Unit =
    MySqlRecoveryCanonicalBytes.writeToken(output, Encoding)
    MySqlModeEnvelopeV2Census.byte(output, 1)
    MySqlRecoveryCanonicalBytes.writeUuid(output, tag.bootId)
    MySqlModeEnvelopeV2Census.long(output, tag.bootGeneration)
    MySqlModeEnvelopeV2Census.long(output, tag.connectionGeneration)
    MySqlRecoveryCanonicalBytes.writeUuid(output, tag.operationId)
    MySqlRecoveryCanonicalBytes.writeUuid(output, workflowUuid)
    MySqlRecoveryCanonicalBytes.writeUuid(output, reserveOperationUuid)
    MySqlRecoveryCanonicalBytes.writeUuid(output, barrierGenerationUuid)
    MySqlModeEnvelopeV2Census.long(output, actualConnectionId)
    MySqlModeEnvelopeV2Census.text(output, barrierNamespace)
    MySqlRecoveryCanonicalBytes.writeUuid(output, bindingUuid)
    MySqlModeEnvelopeV2Census.long(output, writerEpoch)
    writeDateTime6(output, fixedLeaseUntil)
    MySqlRecoveryCanonicalBytes.writeUuid(output, serverUuid)
    MySqlModeEnvelopeV2Census.text(output, serverVersion)
    MySqlModeEnvelopeV2Census.text(output, schema)
    MySqlModeEnvelopeV2Census.text(output, effectiveUser)
    MySqlRecoveryCanonicalBytes.writeUuid(output, provisioningUuid)
    MySqlModeEnvelopeV2Census.int(output, schemaFormat)
    MySqlModeEnvelopeV2Census.int(output, physicalFormat)
    MySqlModeEnvelopeV2Census.text(output, ddlSha256)
    MySqlModeEnvelopeV2Census.text(output, approvedObjectManifestSha256)
    MySqlRecoveryCanonicalBytes.writeToken(output, requestKind)
    writeBytes(output, requestIdentitySha256)
    writeBytes(output, configurationSha256)
    MySqlModeEnvelopeV2Census.text(output, sourceProvenanceKind)
    writeBytes(output, sourceProvenanceSha256)
    writeBytes(output, limitProfileSha256)

  /** Scope carries stable identities and resource versions, never request/context/effect digests. */
  private[mysql] def writeScopeIdentityFields(output: DataOutputStream): Unit =
    MySqlRecoveryCanonicalBytes.writeUuid(output, tag.bootId)
    MySqlModeEnvelopeV2Census.long(output, tag.bootGeneration)
    MySqlModeEnvelopeV2Census.long(output, tag.connectionGeneration)
    MySqlRecoveryCanonicalBytes.writeUuid(output, tag.operationId)
    MySqlRecoveryCanonicalBytes.writeUuid(output, workflowUuid)
    MySqlRecoveryCanonicalBytes.writeUuid(output, reserveOperationUuid)
    MySqlRecoveryCanonicalBytes.writeUuid(output, barrierGenerationUuid)
    MySqlModeEnvelopeV2Census.long(output, actualConnectionId)
    MySqlModeEnvelopeV2Census.text(output, barrierNamespace)
    MySqlRecoveryCanonicalBytes.writeUuid(output, bindingUuid)
    MySqlModeEnvelopeV2Census.long(output, writerEpoch)
    writeDateTime6(output, fixedLeaseUntil)
    MySqlRecoveryCanonicalBytes.writeUuid(output, serverUuid)
    MySqlModeEnvelopeV2Census.text(output, serverVersion)
    MySqlModeEnvelopeV2Census.text(output, schema)
    MySqlModeEnvelopeV2Census.text(output, effectiveUser)
    MySqlRecoveryCanonicalBytes.writeUuid(output, provisioningUuid)
    MySqlModeEnvelopeV2Census.int(output, schemaFormat)
    MySqlModeEnvelopeV2Census.int(output, physicalFormat)
    MySqlModeEnvelopeV2Census.text(output, ddlSha256)
    MySqlModeEnvelopeV2Census.text(output, approvedObjectManifestSha256)
    writeBytes(output, configurationSha256)
    MySqlModeEnvelopeV2Census.text(output, sourceProvenanceKind)
    writeBytes(output, sourceProvenanceSha256)
    writeBytes(output, limitProfileSha256)

  private def writeCanonical(output: DataOutputStream): Unit = writeIdentityFields(output)

  private def nonZero(value: UUID): Boolean =
    value != null && (value.getMostSignificantBits | value.getLeastSignificantBits) != 0L

  override def toString: String =
    s"MySqlRepositoryOperationContextIdentity[boot=${tag.bootId},operation=${tag.operationId},epoch=$writerEpoch,requestSha256=${hex(requestIdentitySha256)}]"

private[mysql] object MySqlRepositoryOperationContextIdentity:
  private val Encoding = "FP_MYSQL_REPOSITORY_OPERATION_CONTEXT_BE_V1"

  /** 重新核实际 session barrier 与当前 admission；DETACHED 标量对象不能签发此身份。 */
  def captureCurrent(
      commandScope: MySqlRepositoryCommandScope,
      verified: VerifiedMySqlConnection,
      expectation: MySqlSchemaAuditExpectation
  ): MySqlRepositoryOperationContextIdentity =
    if commandScope == null || verified == null || expectation == null then
      throw new IllegalArgumentException("actual command scope, verified connection, and approved expectation are required")
    val plan = commandScope.operationPlan()
    val auditScope = commandScope.exactAuditScope()
    commandScope.requireCurrent(auditScope, plan)
    val tag = commandScope.tag()
    plan.requireTag(tag)
    val barrier = commandScope.requireHeldSessionBarrier(auditScope, plan)
    val admission = commandScope.requireBusinessAdmission(auditScope, plan)
    if !auditScope.verifiedConnection().contains(verified) || (auditScope.connectionToken() ne verified.connection()) ||
      (auditScope.connectionIdentity() ne verified.identity()) then
      throw new SQLNonTransientConnectionException("operation context differs from the exact verified MySQL session")
    if (expectation.profile != MySqlSchemaAuditProfile.Format3 &&
      expectation.profile != MySqlSchemaAuditProfile.DeploymentReceipt1) ||
      expectation.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 then
      throw new SQLNonTransientConnectionException("operation context requires an exact format3 or schema5 physical2 audit target")
    val token = admission.token
    val qualification = token.qualification
    val receipt = admission.receipt
    val now = System.nanoTime()
    if (token.verified ne verified) || (token.scope ne auditScope) || token.bootUuid != tag.bootId ||
      token.bootGeneration != tag.bootGeneration || token.connectionGeneration != tag.connectionGeneration ||
      token.bindingUuid != expectation.bindingUuid || token.writerEpoch <= 0L ||
      token.leaseUntil != token.fence.leaseUntil || token.fence.ownerBootUuid != tag.bootId then
      throw new SQLNonTransientConnectionException("operation context business admission differs from its exact Main/session identities")
    if !qualification.isCurrent(tag.bootId, tag.bootGeneration, tag.connectionGeneration,
      expectation.serverUuid, expectation.schema, expectation.bindingUuid, expectation.profile,
      expectation.expectedPhysicalFormat, verified, Thread.currentThread(), now) then
      throw new SQLNonTransientConnectionException("operation context schema qualification is stale or mismatched")
    receipt.assertCurrent(tag.bootId, tag.bootGeneration, tag.connectionGeneration,
      expectation.serverUuid, expectation.schema, expectation.bindingUuid, expectation.profile,
      expectation.expectedPhysicalFormat, verified, Some(token.fence), now)
    barrier.readmit(verified, expectation, auditScope)
    if barrier.actualConnectionId <= 0L || barrier.connectionGenerationId == null ||
      barrier.namespace == null || barrier.namespace.lockName() == null then
      throw new SQLNonTransientConnectionException("operation context held-session identity is malformed")
    val identity = verified.identity
    if identity.serverUuid != expectation.serverUuid || identity.serverVersion != expectation.serverVersion ||
      identity.catalog != expectation.schema || identity.effectiveUser != expectation.effectiveUser then
      throw new SQLNonTransientConnectionException("operation context connection identity differs from approved target")
    expectation.profile match
      case MySqlSchemaAuditProfile.Format3 =>
        val format3 = expectation.format3Inputs.getOrElse(
          throw new SQLNonTransientConnectionException("format3 operation context inputs are unavailable"))
        if format3.limitProfile == null || format3.serverUuid != expectation.serverUuid ||
          format3.schema != expectation.schema || format3.bindingUuid != expectation.bindingUuid ||
          format3.provisioningUuid != expectation.provisioningUuid || !format3.ddlSha256.equalsIgnoreCase(expectation.ddlSha256) then
          throw new SQLNonTransientConnectionException("format3 operation context independent identity differs from its approved target")
        new MySqlRepositoryOperationContextIdentity(tag, plan.workflowUuid, plan.reserveOperationUuid,
          barrier.connectionGenerationId, barrier.actualConnectionId, barrier.namespace.lockName(), expectation.bindingUuid,
          token.writerEpoch, token.leaseUntil, identity.serverUuid, identity.serverVersion, identity.catalog,
          identity.effectiveUser, expectation.provisioningUuid, expectation.profile.schemaFormat,
          expectation.expectedPhysicalFormat.version, expectation.ddlSha256, expectation.objects.canonicalSha256,
          format3.configurationSha256.bytes.toVector, format3.sourceProvenanceKind,
          format3.sourceProvenanceSha256.bytes.toVector, format3.limitProfile.sha256().toVector,
          plan.requestKind, plan.requestIdentitySha256, format3.limitProfile)
      case MySqlSchemaAuditProfile.DeploymentReceipt1 =>
        val deployment = expectation.deploymentReceiptInputs.getOrElse(
          throw new SQLNonTransientConnectionException("schema5 operation context inputs are unavailable"))
        deployment.requireIdentity(expectation.serverUuid,expectation.serverVersion,expectation.schema,
          expectation.effectiveUser,expectation.callerRoles,expectation.provisioningUuid,expectation.bindingUuid,
          expectation.ddlSha256,expectation.auditDefiner)
        val profile=deployment.limitProfile
        if profile==null || (plan.exactLimitProfile ne profile) then
          throw new SQLNonTransientConnectionException("schema5 operation context lost exact original static/plan profile")
        new MySqlRepositoryOperationContextIdentity(tag, plan.workflowUuid, plan.reserveOperationUuid,
          barrier.connectionGenerationId, barrier.actualConnectionId, barrier.namespace.lockName(), expectation.bindingUuid,
          token.writerEpoch, token.leaseUntil, identity.serverUuid, identity.serverVersion, identity.catalog,
          identity.effectiveUser, expectation.provisioningUuid, expectation.profile.schemaFormat,
          expectation.expectedPhysicalFormat.version, expectation.ddlSha256, expectation.objects.canonicalSha256,
          deployment.configurationSha256.bytes.toVector,deployment.sourceProvenanceKind,
          deployment.sourceProvenanceSha256.bytes.toVector,profile.sha256().toVector,
          plan.requestKind,plan.requestIdentitySha256,profile)
      case _ => throw new SQLNonTransientConnectionException("unsupported operation context schema family")

  private def writeDateTime6(output: DataOutputStream, value: LocalDateTime): Unit =
    MySqlModeEnvelopeV2Census.int(output, value.getYear)
    MySqlModeEnvelopeV2Census.byte(output, value.getMonthValue)
    MySqlModeEnvelopeV2Census.byte(output, value.getDayOfMonth)
    MySqlModeEnvelopeV2Census.byte(output, value.getHour)
    MySqlModeEnvelopeV2Census.byte(output, value.getMinute)
    MySqlModeEnvelopeV2Census.byte(output, value.getSecond)
    MySqlModeEnvelopeV2Census.int(output, value.getNano / 1000)

  private def writeBytes(output: DataOutputStream, bytes: Vector[Byte]): Unit =
    var index = 0
    while index < bytes.size do
      MySqlModeEnvelopeV2Census.byte(output, bytes(index))
      index += 1

  private def hex(value: Vector[Byte]): String = java.util.HexFormat.of().withUpperCase().formatHex(value.toArray)

/** Reservation effects bind only to identity/resource/version fields; no context or target digest is nested here. */
private[mysql] final case class MySqlRecoveryResourceRevision(
    resource: MySqlRecoveryResourceKey,
    expectedRevision: Long,
    attemptUuid: Option[UUID]
):
  require(resource != null && expectedRevision >= 0L && attemptUuid != null && !attemptUuid.exists(_ == null),
    "exact recovery resource and non-negative revision are required")

private[mysql] object MySqlRecoveryOperationScope:
  private val Encoding = "FP_MYSQL_RECOVERY_OPERATION_SCOPE_BE_V1"

  def encode(
      context: MySqlRepositoryOperationContextIdentity,
      resources: Vector[MySqlRecoveryResourceRevision]
  ): MySqlRecoveryCanonicalBytes =
    if context == null || resources == null || resources.exists(_ == null) then
      throw new IllegalArgumentException("actual non-self-referential context and explicit resource revisions are required")
    if resources.exists(_.resource.exactProfile != context.exactLimitProfile) then
      throw new SQLNonTransientConnectionException("operation scope resource keys use a different explicit limit profile")
    val orderedKeys = MySqlRecoveryResourceKey.orderForLocks(resources.map(_.resource))
    if orderedKeys.size != resources.size then throw new SQLDataException("operation scope lost a resource key")
    val byKey = resources.map(value => value.resource -> value).toMap
    val ordered = orderedKeys.map(key => byKey.getOrElse(key,
      throw new SQLDataException("operation scope resource key comparison did not resolve its full value")))
    MySqlRecoveryCanonicalBytes.encode(Encoding, context.exactLimitProfile) { output =>
      writeOrdered(output, context, ordered)
    }

  private[mysql] def writeOrdered(output: DataOutputStream, context: MySqlRepositoryOperationContextIdentity,
      ordered: Vector[MySqlRecoveryResourceRevision]): Unit =
      MySqlRecoveryCanonicalBytes.writeToken(output, Encoding)
      MySqlModeEnvelopeV2Census.byte(output, 1)
      // Deliberately write identity fields inline: the scope never stores a context/target digest.
      context.writeScopeIdentityFields(output)
      MySqlModeEnvelopeV2Census.int(output, ordered.size)
      ordered.foreach { value =>
        MySqlRecoveryCanonicalBytes.writeToken(output, value.resource.resourceKind)
        MySqlModeEnvelopeV2Census.int(output, value.resource.canonicalLength)
        value.resource.writeCanonicalTo(output)
        MySqlModeEnvelopeV2Census.long(output, value.expectedRevision)
        MySqlRecoveryCanonicalBytes.writeOptionUuid(output, value.attemptUuid)
      }
