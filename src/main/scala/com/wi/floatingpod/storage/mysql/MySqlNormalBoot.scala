package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{B1ResumeRecord, WindowsB1SourceAnchorStore, WindowsB1TwoSlotStore, WindowsCanonicalMarkerPublicationBarrier, WindowsMarkerIo, WindowsWorldProvisioner}
import com.wi.floatingpod.config.MySqlRuntimeMetadata
import java.nio.file.Path
import java.util.UUID

/** 固定原 creation/phase 身份来自独立 control；不能从现目录或数据库学习批准值。 */
final case class MySqlNormalBootRequest(requestedWorldPath: Path, plan: WindowsWorldProvisioner.Plan,
    boot: WindowsWorldProvisioner.BootIdentity, controlledMetadata: MySqlFreshControlledMetadata):
  require(requestedWorldPath != null && requestedWorldPath.isAbsolute && requestedWorldPath.getParent != null &&
    requestedWorldPath.getFileName != null && plan != null && boot != null && controlledMetadata != null,
    "explicit Normal path/original plan/target boot/controlled metadata required")
  require(!controlledMetadata.metadataPath.normalize().startsWith(requestedWorldPath.normalize()),
    "controlled metadata must be outside world; actual native checks remain mandatory")
  override def toString: String = "MySqlNormalBootRequest[redacted; no custody or authority]"

final case class MySqlNormalNativeObservation(constructionAttempted: Boolean, libraryConstructed: Boolean,
    sourceCapture: Option[MySqlFreshSourceCaptureFacts], anchor: Option[MySqlFreshSourceAnchorFacts],
    inspection: Option[WindowsWorldProvisioner.ResumeInspection],
    installedWorld: Option[WindowsWorldProvisioner.ProvisionedWorld],
    marker: Option[MySqlFreshMarkerFacts], closeAttempted: Boolean, closeReturned: Boolean,
    failure: Option[Throwable]):
  def retainedResponsibility: Boolean = inspection.nonEmpty || installedWorld.nonEmpty ||
    (libraryConstructed && !closeReturned) || failure.nonEmpty
  override def toString: String = "MySqlNormalNativeObservation[redacted; not IOExit/TERM]"

final case class MySqlNormalBootstrapResponsibility(request: MySqlNormalBootRequest,
    native: MySqlNormalNativeObservation, execution: Option[MySqlNormalPreGenesisObservation],
    lastKnownOperation: Option[MySqlOperationContext], sessionBarrierAcquireAttempted: Boolean):
  def retainedResponsibility: Boolean = native.retainedResponsibility || execution.exists(_.retainedResponsibility)
  override def toString: String = "MySqlNormalBootstrapResponsibility[redacted; complete retained observations]"

final case class MySqlNormalBootFacts(originalSourceIdentity: String, originalReceiptSha256: String,
    originalReceiptCanonicalBytes: Vector[Byte], previousOwner: B1ResumeRecord.NativeOwnerAssertions,
    currentOwner: B1ResumeRecord.NativeOwnerAssertions, marker: MySqlFreshMarkerFacts,
    approvedBaselineIdentity: MySqlApprovedBaselineIdentity, approvedBaselineFileSha256: String):
  override def toString: String = "MySqlNormalBootFacts[redacted; anchored initial seed only; not GameReady]"

private[mysql] object MySqlNormalBootRules:
  def validate(request: MySqlNormalBootRequest, baseline: MySqlApprovedBaselineIdentity,
               expectedImport: MySqlExpectedImportState, physical: MySqlExpectedPhysicalFormat,
               bootId: UUID, bootGeneration: Long, metadata: MySqlRuntimeMetadata): Unit =
    require(request != null && baseline != null && bootId != null && bootGeneration > 0L,
      "explicit Normal pre-genesis request and approved baseline identity required")
    require(expectedImport == MySqlExpectedNoImport && physical == MySqlExpectedPhysicalFormat.Format2 &&
      baseline.schemaFormat == 3 && baseline.coverage == MySqlAuditCoverage.Format3,
      "Normal pre-genesis requires exact schema3/physical2/no-import approved target")
    require(request.boot.bootId() == bootId && request.boot.generation() == bootGeneration,
      "Normal pre-genesis boot identity differs from owned IO envelope")
    require(metadata != null, "parsed original runtime metadata required before resource allocation")
    val configuration = WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(metadata)
    require(configuration.configurationSha256().equalsIgnoreCase(request.controlledMetadata.approvedConfigurationSha256),
      "controlled configuration differs from independently approved request hash")
    require(metadata.expectedServerUuid() == baseline.serverUuid && metadata.schema() == baseline.schema &&
      metadata.expectedBindingUuid() == baseline.bindingUuid && metadata.baseline().provisioningUuid() == baseline.provisioningUuid &&
      metadata.baseline().ddlSha256().equalsIgnoreCase(baseline.ddlSha256) && metadata.baseline().schemaFormat() == 3 &&
      metadata.baseline().coverage() == baseline.coverage && metadata.baseline().format() == baseline.format &&
      metadata.baseline().serverVersion() == baseline.serverVersion && metadata.baseline().auditDefiner() == baseline.auditDefiner &&
      metadata.baseline().sourceCaptureSha256().equalsIgnoreCase(baseline.sourceCaptureSha256),
      "parsed original metadata differs from approved Normal pre-genesis baseline")
    val plan = request.plan
    require(plan.mysqlServerUuid() == baseline.serverUuid && plan.schema() == baseline.schema &&
      plan.schemaFormat() == baseline.schemaFormat && plan.schemaFormat() == 3 && plan.physicalFormat() == physical.version &&
      plan.provisioningUuid() == baseline.provisioningUuid && plan.bindingUuid() == baseline.bindingUuid &&
      plan.ddlSha256().equalsIgnoreCase(baseline.ddlSha256), "fixed Normal pre-genesis plan differs from independently approved baseline")

/** 注册到 session 后才允许 native inspection；不创建世界、不改旧 guard、无同进程退休替身。 */
private[mysql] final class MySqlNormalNativeHolder(owner: Thread, val request: MySqlNormalBootRequest):
  require(owner != null && request != null, "actual Normal owner/request required")
  private var io: WindowsMarkerIo = null
  private var factory: WindowsWorldProvisioner = null
  private var publisher: WindowsCanonicalMarkerPublicationBarrier = null
  private var anchors: WindowsB1SourceAnchorStore = null
  private var location: WindowsB1SourceAnchorStore.ControlledAnchorLocation = null
  private var source: WindowsB1SourceAnchorStore.DurableB1SourceAnchor = null
  private var slots: WindowsB1TwoSlotStore = null
  private var inspected: WindowsWorldProvisioner.ResumeInspection = null
  private var world: WindowsWorldProvisioner.ProvisionedWorld = null
  private var constructionAttempted = false
  private var captured: Option[MySqlFreshSourceCaptureFacts] = None
  private var anchorFacts: Option[MySqlFreshSourceAnchorFacts] = None
  private var marker: Option[MySqlFreshMarkerFacts] = None
  private var actualMarker: WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck = null
  private var closeAttempted = false
  private var closeReturned = false
  private var primary: Option[Throwable] = None
  private def owned(): Unit = require(Thread.currentThread() eq owner, "exact Normal native IO owner required")

  def beginInspection(metadata: MySqlRuntimeMetadata): Unit =
    owned(); require(!constructionAttempted && metadata != null, "one-shot parsed original Normal metadata required")
    val config = WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(metadata)
    require(config.configurationSha256().equalsIgnoreCase(request.controlledMetadata.approvedConfigurationSha256),
      "independently approved configuration mismatch before native construction")
    constructionAttempted = true
    try
      io = new WindowsMarkerIo()
      factory = new WindowsWorldProvisioner(io); publisher = new WindowsCanonicalMarkerPublicationBarrier(io)
      anchors = new WindowsB1SourceAnchorStore(io); slots = new WindowsB1TwoSlotStore(io)
      location = anchors.captureLocation(request.controlledMetadata.metadataPath, config, request.plan.creationOperationUuid())
      require(location.configurationSha256().equalsIgnoreCase(request.controlledMetadata.approvedConfigurationSha256),
        "same-handle approved configuration mismatch")
      captured = Some(MySqlFreshSourceCaptureFacts(location.canonicalPath(), location.configurationSha256(), location.parentIdentity()))
      val expected = anchors.readExpectedSource(location)
      require(expected.expectedPlan() == request.plan, "original anchor plan differs from independently approved fixed plan")
      source = anchors.rebarrierExisting(location, expected)
      anchorFacts = Some(MySqlFreshSourceAnchorFacts(source.anchorSha256(), source.fileIdentity(), source.durabilityProtocol()))
      inspected = factory.beginResumeInspection(request.requestedWorldPath, request.plan, expected.expectedReceiptSha256(), request.boot)
      require(inspected.nativeDirectoryPath() == expected.worldNativePath() &&
        inspected.sourceWorldIdentity() == expected.originalSourceIdentity(), "original anchor/source directory identity differs")
      MySqlFreshPrepareRules.exactBytes(expected.originalReceiptCanonicalBytes(), inspected.freshProvisioningProof().receiptCanonicalBytes())
      inspected.validateInspection()
    catch case cause: Throwable =>
      primary = MySqlOwnedSessionLifecycle.combine(primary, cause); throw cause

  private def ready(): Unit =
    owned(); require(inspected != null && source != null, "actual complete Normal inspection/source required")
  def provisioner: WindowsWorldProvisioner = { ready(); factory }
  def markerPublisher: WindowsCanonicalMarkerPublicationBarrier = { ready(); publisher }
  def inspection: WindowsWorldProvisioner.ResumeInspection = { ready(); inspected }
  def anchorStore: WindowsB1SourceAnchorStore = { ready(); anchors }
  def anchorLocation: WindowsB1SourceAnchorStore.ControlledAnchorLocation = { ready(); location }
  def sourceAnchor: WindowsB1SourceAnchorStore.DurableB1SourceAnchor = { ready(); source }
  def slotStore: WindowsB1TwoSlotStore = { ready(); slots }
  def observeInstalled(value: WindowsWorldProvisioner.ProvisionedWorld): Unit =
    ready(); require(world == null && value != null && !value.createdThisBoot() &&
      (value.freshProvisioningProof() eq inspected.freshProvisioningProof()), "exact Normal installed world required")
    world = value // 强持 process-lifetime custody，后续失败/退出不释放。
  def recordMarker(ack: WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck): Unit =
    ready(); require(world != null && ack != null && marker.isEmpty, "actual one-shot Normal marker ACK required")
    actualMarker = ack
    marker = Some(MySqlFreshMarkerFacts(ack.canonicalPath(), ack.bindingUuid(), ack.physicalFormat(), ack.operationUuid(),
      ack.parentIdentity().versionedIdentity(), ack.fileIdentity().versionedIdentity(), ack.markerSha256(), ack.retainedExisting(), ack.durabilityProtocol()))
  def facts(baseline: MySqlApprovedStaticBaseline): MySqlNormalBootFacts =
    ready(); require(world != null && marker.nonEmpty && actualMarker != null, "actual Normal native completion facts required")
    val original = world.freshProvisioningProof()
    MySqlNormalBootFacts(original.sourceWorldIdentity(), original.receiptSha256(), original.receiptCanonicalBytes().toVector,
      inspected.previousOwner(), inspected.targetOwner(), marker.get, baseline.identity, baseline.fileSha256)
  def observation: MySqlNormalNativeObservation =
    owned(); MySqlNormalNativeObservation(constructionAttempted, io != null, captured, anchorFacts,
      Option(inspected), Option(world), marker, closeAttempted, closeReturned, primary)
  def settleForExit(): Unit =
    owned()
    // inspection也持原share0 handles；io.close会拒绝，不能把此责任伪成已关闭或释放。
    if io != null && inspected == null && !closeAttempted then
      closeAttempted = true
      try { io.close(); closeReturned = true }
      catch case cause: Throwable =>
        primary = MySqlOwnedSessionLifecycle.combine(primary, cause); throw cause
