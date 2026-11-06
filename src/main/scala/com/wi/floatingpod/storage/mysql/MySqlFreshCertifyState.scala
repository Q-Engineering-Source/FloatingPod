package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{B1ResumeRecord, B1ResumeRecordCanonical, WindowsB1TwoSlotStore, WindowsCanonicalMarkerPublicationBarrier, WindowsMarkerIo}
import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.sql.SQLNonTransientConnectionException

enum MySqlFreshCertifyPhase:
  case New, PublishingMarker, MarkerAcknowledged, Planning, Planned, PublishingIntent, IntentDurable, Executing, SqlAcknowledged, ReadbackConfirmed, CertifiedConfirmed, Auditing, Admitted, Frozen

/** 事实观察保留固定plan/native阶段和原失败；boolean不签发任何能力。 */
final case class MySqlFreshCertifyObservation(
    phase: MySqlFreshCertifyPhase, markerPublicationAttempted: Boolean, intentPublicationAttempted: Boolean,
    executionAttempted: Boolean, sqlCommitAcknowledged: Boolean, fullReadbackConfirmed: Boolean,
    nativeConfirmationAcknowledged: Boolean, failureStage: Option[String], failure: Option[Throwable],
    plan: Option[MySqlFreshCertifyCapabilities.PlannedFreshCertify],
    intent: Option[MySqlFreshCertifyCapabilities.DurableCertifyIntent], draftRecord: Option[B1ResumeRecord],
    checkedRetirements: Vector[WindowsB1TwoSlotStore.RetiredInactiveSlot]
):
  override def toString: String = "FreshCertifyObservation[retained responsibility; not settlement/Ready]"

private[mysql] final class MySqlFreshCertifyLifecycle(owner: Thread):
  require(owner != null, "actual certify owner required")
  private var phase = MySqlFreshCertifyPhase.New
  private var primary: Throwable = null
  private var stage: Option[String] = None
  private var marker = false
  private var intent = false
  private var execution = false
  private var commit = false
  private var readback = false
  private var nativeConfirmation = false
  private def owned(): Unit = require(Thread.currentThread() eq owner, "exact certify owned IO thread required")
  private def next(from: MySqlFreshCertifyPhase, to: MySqlFreshCertifyPhase): Unit =
    owned()
    if primary != null then throw primary
    if phase != from then throw new SQLNonTransientConnectionException("Fresh certify cannot be retried or skipped")
    phase = to
  def beginMarker(): Unit =
    next(MySqlFreshCertifyPhase.New, MySqlFreshCertifyPhase.PublishingMarker); marker = true
  def markerAcknowledged(): Unit = next(MySqlFreshCertifyPhase.PublishingMarker, MySqlFreshCertifyPhase.MarkerAcknowledged)
  def beginPlanning(): Unit = next(MySqlFreshCertifyPhase.MarkerAcknowledged, MySqlFreshCertifyPhase.Planning)
  def planned(): Unit = next(MySqlFreshCertifyPhase.Planning, MySqlFreshCertifyPhase.Planned)
  def beginIntent(): Unit =
    next(MySqlFreshCertifyPhase.Planned, MySqlFreshCertifyPhase.PublishingIntent); intent = true
  def intentDurable(): Unit = next(MySqlFreshCertifyPhase.PublishingIntent, MySqlFreshCertifyPhase.IntentDurable)
  def beginExecution(): Unit =
    next(MySqlFreshCertifyPhase.IntentDurable, MySqlFreshCertifyPhase.Executing); execution = true
  def sqlAcknowledged(): Unit =
    next(MySqlFreshCertifyPhase.Executing, MySqlFreshCertifyPhase.SqlAcknowledged); commit = true
  def readbackConfirmed(): Unit =
    next(MySqlFreshCertifyPhase.SqlAcknowledged, MySqlFreshCertifyPhase.ReadbackConfirmed); readback = true
  def nativeConfirmed(): Unit =
    next(MySqlFreshCertifyPhase.ReadbackConfirmed, MySqlFreshCertifyPhase.CertifiedConfirmed); nativeConfirmation = true
  def beginAudit(): Unit = next(MySqlFreshCertifyPhase.CertifiedConfirmed, MySqlFreshCertifyPhase.Auditing)
  def admitted(): Unit = next(MySqlFreshCertifyPhase.Auditing, MySqlFreshCertifyPhase.Admitted)
  def fail(at: String, cause: Throwable): Unit =
    owned(); require(at != null && at.nonEmpty && cause != null)
    if primary == null then
      primary = cause; stage = Some(at)
    else if primary ne cause then MySqlFencedTransactionSupport.addSuppressed(primary, cause)
    phase = MySqlFreshCertifyPhase.Frozen
  def observe(plan: Option[MySqlFreshCertifyCapabilities.PlannedFreshCertify],
      pending: Option[MySqlFreshCertifyCapabilities.DurableCertifyIntent], draft: Option[B1ResumeRecord],
      retirements: Vector[WindowsB1TwoSlotStore.RetiredInactiveSlot] = Vector.empty): MySqlFreshCertifyObservation =
    owned()
    MySqlFreshCertifyObservation(phase, marker, intent, execution, commit, readback, nativeConfirmation, stage, Option(primary), plan, pending, draft, retirements)

private[mysql] object MySqlFreshCertifyRules:
  /** Exact current facts only; native read/rebarrier remains the real store's responsibility. */
  def requireRecord(expectedBytes: Array[Byte], actual: WindowsB1TwoSlotStore.StoredRecordFacts): Unit =
    if actual == null then throw new SQLNonTransientConnectionException("Current planned native record absent")
    MySqlFreshPrepareRules.exactBytes(expectedBytes, actual.canonicalBytes())
    MySqlFreshPrepareRules.exactBytes(expectedBytes, B1ResumeRecordCanonical.encode(actual.record()))

  /** 原 native ACK 的公开完整typed facts编码；本编码不是新增native ACK或授权。 */
  def markerFacts(ack: WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck,
      custodyGeneration: Long): B1ResumeRecord.MarkerAssertions =
    require(ack != null && custodyGeneration > 0, "actual current marker ACK required")
    val bytes = WindowsMarkerIo.markerBytes(ack.bindingUuid(), ack.physicalFormat())
    val evidence = new ByteArrayOutputStream()
    val out = new DataOutputStream(evidence)
    try
      def text(value: String): Unit =
        val raw = value.getBytes(java.nio.charset.StandardCharsets.UTF_8)
        out.writeInt(raw.length); out.write(raw)
      def uuid(value: java.util.UUID): Unit =
        out.writeLong(value.getMostSignificantBits); out.writeLong(value.getLeastSignificantBits)
      text("FP_B1_ACTUAL_MARKER_ACK_PUBLIC_FACTS_BE_V1")
      uuid(ack.operationUuid()); uuid(ack.bindingUuid()); out.writeInt(ack.physicalFormat())
      text(ack.canonicalPath().toString); text(ack.parentIdentity().versionedIdentity()); text(ack.fileIdentity().versionedIdentity())
      text(ack.markerSha256()); text(ack.durabilityProtocol()); out.writeBoolean(ack.retainedExisting()); out.writeLong(custodyGeneration)
    finally out.close()
    new B1ResumeRecord.MarkerAssertions(bytes, ack.fileIdentity().versionedIdentity(), ack.parentIdentity().versionedIdentity(),
      ack.durabilityProtocol(), custodyGeneration, evidence.toByteArray)

  def requireSameMarker(original: WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck,
      current: WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck): Unit =
    if original == null || current == null || !current.retainedExisting() ||
      original.operationUuid() != current.operationUuid() || original.bindingUuid() != current.bindingUuid() ||
      original.physicalFormat() != current.physicalFormat() || original.canonicalPath() != current.canonicalPath() ||
      original.parentIdentity() != current.parentIdentity() || original.fileIdentity() != current.fileIdentity() ||
      original.markerSha256() != current.markerSha256() || original.durabilityProtocol() != current.durabilityProtocol() then
      throw new SQLNonTransientConnectionException("Current native marker rebarrier differs from original exact ACK")

  /** One fixed full snapshot per successor. No receipt/scope is rewritten to the certify context. */
  def successor(previous: B1ResumeRecord, kind: B1ResumeRecord.Kind, state: B1ResumeRecord.FullStateAssertions): B1ResumeRecord =
    require(previous != null && state != null)
    val result = new B1ResumeRecord(Math.addExact(previous.sequence(), 1),
      java.security.MessageDigest.getInstance("SHA-256").digest(B1ResumeRecordCanonical.encode(previous)), kind,
      previous.sourceAnchorSha256(), previous.originalReceipt(), previous.barrierNamespace(), state)
    result // immutable caller assertions; actor pre-encodes both records before issuing the opaque plan
