package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{WindowsB1SourceAnchorStore, WindowsB1TwoSlotStore, WindowsCanonicalMarkerPublicationBarrier, WindowsMarkerIo, WindowsWorldProvisioner}
import com.wi.floatingpod.config.MySqlRuntimeMetadata
import java.nio.file.Path
import java.time.Instant
import java.util.UUID

/** control显式批准的无秘密配置位置；纯identity，不证明文件ACL或实际source。 */
final case class MySqlFreshControlledMetadata(metadataPath: Path, approvedConfigurationSha256: String):
  require(metadataPath != null && metadataPath.isAbsolute && metadataPath.getParent != null &&
    metadataPath.getFileName != null, "explicit absolute controlled metadata file path required")
  require(approvedConfigurationSha256 != null && approvedConfigurationSha256.matches("[0-9a-fA-F]{64}"),
    "independently approved configuration SHA-256 required")
  override def toString: String = "MySqlFreshControlledMetadata[redacted; not ACL authority]"

/** control从actual DedicatedWorldContext提取path，不把server/converter传入worker。 */
final case class MySqlFreshBootRequest(requestedWorldPath: Path, plan: WindowsWorldProvisioner.Plan,
                                     boot: WindowsWorldProvisioner.BootIdentity, controlledMetadata: MySqlFreshControlledMetadata):
  require(requestedWorldPath != null && requestedWorldPath.isAbsolute &&
    requestedWorldPath.getParent != null && requestedWorldPath.getFileName != null, "explicit absolute world path with parent required")
  require(plan != null && boot != null && controlledMetadata != null, "fixed Fresh plan, boot and controlled metadata required")
  require(!controlledMetadata.metadataPath.toAbsolutePath.normalize().startsWith(requestedWorldPath.toAbsolutePath.normalize()),
    "controlled metadata must be lexically outside the requested world; native source checks remain mandatory")
  override def toString: String = "MySqlFreshBootRequest[redacted; no native proof]"

final case class MySqlFreshCreationFacts(request: MySqlFreshBootRequest, provisionedDirectory: Path,
    sourceWorldIdentity: String, guardFileIdentity: String, receiptFileIdentity: String,
    receiptSha256: String, receiptCanonicalBytes: Vector[Byte], receiptEncoding: String,
    creationProcessPid: Long, creationProcessStart: Instant):
  override def toString: String = "MySqlFreshCreationFacts[redacted; immutable native observations]"

final case class MySqlFreshMarkerFacts(canonicalPath: Path, bindingUuid: UUID,
    physicalFormat: Int, operationUuid: UUID, parentIdentity: String, fileIdentity: String,
    markerSha256: String, retainedExisting: Boolean, durabilityProtocol: String):
  override def toString: String = "MySqlFreshMarkerFacts[redacted; actual native ACK facts]"

final case class MySqlFreshBootFacts(creation: MySqlFreshCreationFacts, marker: MySqlFreshMarkerFacts,
    approvedBaselineIdentity: MySqlApprovedBaselineIdentity, approvedBaselineFileSha256: String):
  override def toString: String = "MySqlFreshBootFacts[redacted; not GameReady]"

enum MySqlFreshNativeRetention:
  case NotOpened
  case LibraryConstructionFailed(cause: Throwable)
  case NativeOwned
  case ProcessLifetimeCustodyRetained
  case NativeCloseReturned
  case CertifiedCompletionClosed
  case NativeCloseFailed(cause: Throwable)

/** close返回不是独立native/physical close证明，process-lifetime custody有意保持。 */
final case class MySqlFreshSourceCaptureFacts(canonicalAnchorPath: Path, configurationSha256: String,
    parentIdentity: String):
  override def toString: String = "MySqlFreshSourceCaptureFacts[redacted; actual controlled location only]"

final case class MySqlFreshSourceAnchorFacts(anchorSha256: String, fileIdentity: String, durabilityProtocol: String):
  override def toString: String = "MySqlFreshSourceAnchorFacts[redacted; actual ACK only]"

final case class MySqlFreshNativeObservation(retention: MySqlFreshNativeRetention,
    libraryConstructionAttempted: Boolean, libraryConstructed: Boolean,
    closeAttempted: Boolean, closeReturned: Boolean,
    creation: Option[MySqlFreshCreationFacts], marker: Option[MySqlFreshMarkerFacts],
    creationAttempted: Boolean = false,
    sourceCaptureAttempted: Boolean = false, sourceCapture: Option[MySqlFreshSourceCaptureFacts] = None,
    sourcePublicationAttempted: Boolean = false, sourceAnchor: Option[MySqlFreshSourceAnchorFacts] = None):
  def retainedResponsibility: Boolean =
    if retention == MySqlFreshNativeRetention.CertifiedCompletionClosed then false
    else if creationAttempted || sourcePublicationAttempted then true
    else retention match
      case MySqlFreshNativeRetention.NotOpened | MySqlFreshNativeRetention.NativeCloseReturned => false
      case _ => true
  override def toString: String = "MySqlFreshNativeObservation[redacted; not IOExitProof]"

final case class MySqlFreshBootstrapResponsibility(request: MySqlFreshBootRequest,
    prepare: Option[MySqlFreshPrepareAttemptObservation], certify: Option[MySqlFreshCertifyObservation],
    lastKnownOperation: Option[MySqlOperationContext], native: MySqlFreshNativeObservation,
    sessionBarrierAcquireAttempted: Boolean, certifiedWitnessesReleased: Boolean = false):
  def retainedResponsibility: Boolean = native.retainedResponsibility || (!certifiedWitnessesReleased &&
    (prepare.exists(_.retainedResponsibility) || certify.exists(value => value.plan.nonEmpty || value.intent.nonEmpty || value.draftRecord.nonEmpty ||
      value.markerPublicationAttempted || value.intentPublicationAttempted || value.executionAttempted)))
  override def toString: String = "MySqlFreshBootstrapResponsibility[redacted; complete prepare/certify/native obligations]"

/** 只比较caller批准的静态值；不从DB或目录观测填入目标、不授予Fresh。 */
private[mysql] object MySqlFreshBootRules:
  def validate(request: MySqlFreshBootRequest, baseline: MySqlApprovedBaselineIdentity,
               expectedImport: MySqlExpectedImportState, physical: MySqlExpectedPhysicalFormat,
               bootId: UUID, bootGeneration: Long, metadata: MySqlRuntimeMetadata): Unit =
    require(request != null && baseline != null && bootId != null && bootGeneration > 0L,
      "explicit Fresh request and approved baseline identity required")
    require(expectedImport == MySqlExpectedNoImport && physical == MySqlExpectedPhysicalFormat.Format2 &&
      baseline.schemaFormat == 3 && baseline.coverage == MySqlAuditCoverage.Format3,
      "Fresh requires exact schema3/physical2/no-import approved target")
    require(request.boot.bootId() == bootId && request.boot.generation() == bootGeneration,
      "Fresh boot identity differs from owned IO envelope")
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
      "parsed original metadata differs from approved Fresh baseline")
    val plan = request.plan
    require(plan.mysqlServerUuid() == baseline.serverUuid && plan.schema() == baseline.schema &&
      plan.schemaFormat() == baseline.schemaFormat && plan.schemaFormat() == 3 && plan.physicalFormat() == physical.version &&
      plan.provisioningUuid() == baseline.provisioningUuid && plan.bindingUuid() == baseline.bindingUuid &&
      plan.ddlSha256().equalsIgnoreCase(baseline.ddlSha256), "fixed Fresh plan differs from independently approved baseline")

  def validateV2(request: MySqlFreshBootRequest, baseline: MySqlApprovedBaselineIdentity,
               expectedImport: MySqlExpectedImportState, physical: MySqlExpectedPhysicalFormat,
               bootId: UUID, bootGeneration: Long, metadata: MySqlRuntimeMetadata.FullV2): Unit =
    require(request != null && baseline != null && bootId != null && bootGeneration > 0L,
      "explicit Fresh request and approved baseline identity required")
    require(expectedImport == MySqlExpectedNoImport && physical == MySqlExpectedPhysicalFormat.Format2 &&
      baseline.schemaFormat == 3 && baseline.coverage == MySqlAuditCoverage.Format3,
      "Fresh requires exact schema3/physical2/no-import approved target")
    require(request.boot.bootId() == bootId && request.boot.generation() == bootGeneration,
      "Fresh boot identity differs from owned IO envelope")
    require(metadata != null, "parsed original runtime metadata required before resource allocation")
    val configuration = WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(metadata)
    require(configuration.configurationSha256().equalsIgnoreCase(request.controlledMetadata.approvedConfigurationSha256),
      "controlled configuration differs from independently approved request hash")
    require(metadata.expectedServerUuid() == baseline.serverUuid && metadata.schema() == baseline.schema &&
      metadata.expectedBindingUuid() == baseline.bindingUuid && metadata.baseline().provisioningUuid() == baseline.provisioningUuid &&
      metadata.baseline().ddlSha256().equalsIgnoreCase(baseline.ddlSha256) && metadata.baseline().schemaFormat() == 3 &&
      metadata.baseline().coverage() == baseline.coverage && metadata.baseline().format() == baseline.format &&
      metadata.baseline().serverVersion() == baseline.serverVersion && metadata.baseline().auditDefiner() == baseline.auditDefiner &&
      metadata.baseline().sourceCaptureSha256().equalsIgnoreCase(baseline.sourceCaptureSha256),
      "parsed original metadata differs from approved Fresh baseline")
    val plan = request.plan
    require(plan.mysqlServerUuid() == baseline.serverUuid && plan.schema() == baseline.schema &&
      plan.schemaFormat() == baseline.schemaFormat && plan.schemaFormat() == 3 && plan.physicalFormat() == physical.version &&
      plan.provisioningUuid() == baseline.provisioningUuid && plan.bindingUuid() == baseline.bindingUuid &&
      plan.ddlSha256().equalsIgnoreCase(baseline.ddlSha256), "fixed Fresh plan differs from independently approved baseline")

  /** 相位观察不发行admission；调用方仍须核actual coordinator的opaque admission。 */
  def requireAdmittedPhase(value: MySqlFreshCertifyObservation): Unit =
    require(value != null && value.phase == MySqlFreshCertifyPhase.Admitted && value.failure.isEmpty,
      "actual planned certify Admitted observation required")

/** 本holder在session登记后才允许构造NativeIO；原factory及全部proof始终留actual worker。 */
private[mysql] final class MySqlFreshNativeHolder(owner: Thread, val request: MySqlFreshBootRequest):
  require(owner != null && request != null, "Fresh native owner/request required")
  private var io: WindowsMarkerIo = null
  private var factory: WindowsWorldProvisioner = null
  private var publisher: WindowsCanonicalMarkerPublicationBarrier = null
  private var world: WindowsWorldProvisioner.ProvisionedWorld = null
  private var anchors: WindowsB1SourceAnchorStore = null
  private var location: WindowsB1SourceAnchorStore.ControlledAnchorLocation = null
  private var sourceAck: WindowsB1SourceAnchorStore.DurableB1SourceAnchor = null
  private var slots: WindowsB1TwoSlotStore = null
  private var sourceCaptureAttempted = false
  private var sourcePublicationAttempted = false
  private var sourceCapture = Option.empty[MySqlFreshSourceCaptureFacts]
  private var anchorFacts = Option.empty[MySqlFreshSourceAnchorFacts]
  private var creation = Option.empty[MySqlFreshCreationFacts]
  private var marker = Option.empty[MySqlFreshMarkerFacts]
  private var actualMarkerAck: WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck = null
  private var constructionAttempted = false
  private var creationAttempted = false
  private var retention: MySqlFreshNativeRetention = MySqlFreshNativeRetention.NotOpened
  private var closeAttempted = false
  private var closeReturned = false

  private def requireOwner(): Unit =
    require(Thread.currentThread() eq owner, "Fresh native responsibility requires exact actual IO worker")

  def createFresh(metadata: MySqlRuntimeMetadata): Unit =
    requireOwner()
    require(!constructionAttempted, "Fresh native factory construction is one-shot")
    require(metadata != null, "parsed original metadata is mandatory")
    val configuration = WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(metadata)
    createWithConfiguration(configuration)

  def createFreshV2(metadata: MySqlRuntimeMetadata.FullV2): Unit =
    requireOwner()
    require(!constructionAttempted, "Fresh native factory construction is one-shot")
    require(metadata != null,"typed FullV2 metadata is mandatory")
    createWithConfiguration(WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(metadata))

  private def createWithConfiguration(configuration: WindowsB1SourceAnchorStore.AnchorConfiguration): Unit =
    require(configuration.configurationSha256().equalsIgnoreCase(request.controlledMetadata.approvedConfigurationSha256),
      "controlled metadata hash mismatch before native construction")
    constructionAttempted = true
    try io = new WindowsMarkerIo()
    catch
      case cause: Throwable =>
        retention = MySqlFreshNativeRetention.LibraryConstructionFailed(cause)
        throw cause
    retention = MySqlFreshNativeRetention.NativeOwned
    factory = new WindowsWorldProvisioner(io)
    publisher = new WindowsCanonicalMarkerPublicationBarrier(io)
    anchors = new WindowsB1SourceAnchorStore(io)
    slots = new WindowsB1TwoSlotStore(io)
    sourceCaptureAttempted = true
    location = anchors.captureLocation(request.controlledMetadata.metadataPath, configuration, request.plan.creationOperationUuid())
    require(location.configurationSha256().equalsIgnoreCase(request.controlledMetadata.approvedConfigurationSha256),
      "same-handle controlled source configuration differs")
    sourceCapture = Some(MySqlFreshSourceCaptureFacts(location.canonicalPath(), location.configurationSha256(), location.parentIdentity()))
    creationAttempted = true // 成败未知的目录/receipt副作用即使arena close返回也不能当作不存在。
    world = factory.createFresh(request.requestedWorldPath, request.plan, request.boot)
    // createFresh成功已经把原share0 handles登记到native process-lifetime list；下面任何失败都不能close这些句柄。
    retention = MySqlFreshNativeRetention.ProcessLifetimeCustodyRetained
    val fresh = world.freshProvisioningProof()
    val custody = world.bootWorldCustodyProof()
    require(world.createdThisBoot() && fresh != null && custody != null &&
      (custody.freshProvisioningProof() eq fresh) && fresh.plan() == request.plan &&
      fresh.creationBoot() == request.boot && custody.bootId() == request.boot.bootId() &&
      custody.generation() == request.boot.generation(), "actual native Fresh proof differs from fixed request")
    custody.validateLive()
    creation = Some(MySqlFreshCreationFacts(request, custody.directory(), fresh.sourceWorldIdentity(),
      fresh.guardFileIdentity(), fresh.receiptFileIdentity(), fresh.receiptSha256(),
      fresh.receiptCanonicalBytes().toVector, fresh.receiptEncoding(), fresh.creationProcessPid(), fresh.creationProcessStartInstant()))
    sourcePublicationAttempted = true
    sourceAck = anchors.publishCreatedSource(location, world)
    anchorFacts = Some(MySqlFreshSourceAnchorFacts(sourceAck.anchorSha256(), sourceAck.fileIdentity(), sourceAck.durabilityProtocol()))

  private[mysql] def dropRuntimeConfigurationForExit(): Unit =
    requireOwner()
    require(!observation.retainedResponsibility,"Fresh native/configuration responsibility remains retained")
    location=null;sourceAck=null

  def provisioner: WindowsWorldProvisioner =
    requireOwner()
    require(world != null, "actual Fresh world has not been created")
    factory
  def markerPublisher: WindowsCanonicalMarkerPublicationBarrier =
    requireOwner()
    require(world != null, "actual Fresh world has not been created")
    publisher
  def provisionedWorld: WindowsWorldProvisioner.ProvisionedWorld =
    requireOwner()
    require(world != null, "actual Fresh world has not been created")
    world

  def nativeIo: WindowsMarkerIo =
    requireOwner(); require(sourceAck != null, "actual source anchor ACK required"); io
  def anchorStore: WindowsB1SourceAnchorStore =
    requireOwner(); require(sourceAck != null, "actual source anchor ACK required"); anchors
  def anchorLocation: WindowsB1SourceAnchorStore.ControlledAnchorLocation =
    requireOwner(); require(sourceAck != null, "actual source anchor ACK required"); location
  def sourceAnchor: WindowsB1SourceAnchorStore.DurableB1SourceAnchor =
    requireOwner(); require(sourceAck != null, "actual source anchor ACK required"); sourceAck
  def slotStore: WindowsB1TwoSlotStore =
    requireOwner(); require(sourceAck != null, "actual source anchor ACK required"); slots

  def recordMarker(ack: WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck): Unit =
    requireOwner()
    require(world != null && ack != null && marker.isEmpty, "one actual marker ACK required")
    actualMarkerAck = ack // 原opaque ACK强引用与immutable事实分开，退出不伪造native释放。
    marker = Some(MySqlFreshMarkerFacts(ack.canonicalPath(), ack.bindingUuid(), ack.physicalFormat(),
      ack.operationUuid(), ack.parentIdentity().versionedIdentity(), ack.fileIdentity().versionedIdentity(),
      ack.markerSha256(), ack.retainedExisting(), ack.durabilityProtocol()))

  def bootFacts: (MySqlFreshCreationFacts, MySqlFreshMarkerFacts) =
    requireOwner()
    require(creation.nonEmpty && marker.nonEmpty && actualMarkerAck != null, "actual native creation and publication facts required")
    (creation.get, marker.get)

  def observation: MySqlFreshNativeObservation =
    requireOwner()
    MySqlFreshNativeObservation(retention, constructionAttempted, io != null,
      closeAttempted, closeReturned, creation, marker, creationAttempted,
      sourceCaptureAttempted, sourceCapture, sourcePublicationAttempted, anchorFacts)

  /** 不提前释放成功custody；失败partial handles/arena的close异常保留原实际io强引用。 */
  def settleForExit(): MySqlFreshNativeObservation =
    requireOwner()
    if world != null then
      retention = MySqlFreshNativeRetention.ProcessLifetimeCustodyRetained
    else if io != null && !closeAttempted then
      closeAttempted = true
      try
        io.close()
        closeReturned = true
        retention = MySqlFreshNativeRetention.NativeCloseReturned
      catch
        case cause: Throwable =>
          retention = MySqlFreshNativeRetention.NativeCloseFailed(cause)
          throw cause
    observation

  /** 正常 session finalizer 的单独路径；失败出口仍保留 process-lifetime custody。 */
  private[mysql] def closeCertifiedPreparation(permit: MySqlCertifiedFreshExitPermit): MySqlFreshNativeObservation =
    requireOwner()
    require(permit != null && world != null && io != null && !closeAttempted,
      "original admitted Fresh native close is one-shot")
    closeAttempted = true
    try
      factory.closeCertifiedFreshCustody(world, permit)
      io.close() // 原 owned 集合中任何其他未确认 handle/arena 都会明确失败。
      closeReturned = true
      retention = MySqlFreshNativeRetention.CertifiedCompletionClosed
    catch
      case cause: Throwable =>
        retention = MySqlFreshNativeRetention.NativeCloseFailed(cause)
        throw cause
    observation
