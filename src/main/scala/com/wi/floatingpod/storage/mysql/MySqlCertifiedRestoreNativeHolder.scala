package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{WindowsMarkerIo, WindowsWorldProvisioner, WindowsB1SourceAnchorStore,
  WindowsB1TwoSlotStore, WindowsB1ColdResumeLookup, WindowsB1Full20WitnessStore}
import java.util.HexFormat
import scala.jdk.CollectionConverters.*

/** Session先注册此holder；所有创建、原scope、部分native失败都留在同actual worker。 */
private[mysql] final class MySqlCertifiedRestoreNativeHolder(
    owner: Thread, val request: CertifiedSourceRequest):
  private var io: WindowsMarkerIo = null
  private var factory: WindowsWorldProvisioner = null
  private var anchors: WindowsB1SourceAnchorStore = null
  private var slots: WindowsB1TwoSlotStore = null
  private var location: WindowsB1SourceAnchorStore.ControlledAnchorLocation = null
  private var source: WindowsB1SourceAnchorStore.DurableB1SourceAnchor = null
  private var inspection: WindowsWorldProvisioner.ResumeInspection = null
  private var coldScope: WindowsB1TwoSlotStore.ColdResumeSourceScope = null
  private var selected: WindowsB1TwoSlotStore.ResumeSelectedRecords = null
  private var lookup: WindowsB1ColdResumeLookup = null
  private var historicalDiscovery: WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit = null
  private var historicalArtifacts = Vector.empty[WindowsB1Full20WitnessStore.VerifiedHistoricalArtifact]
  private var verifiedCurrentV5: WindowsB1TwoSlotStore.VerifiedWritableCurrentV5 = null
  private var recallV5History: com.wi.floatingpod.server.WindowsB1V5HistoricalLookup = null
  private var verifiedV5Flight: com.wi.floatingpod.server.WindowsB1V5WitnessRecordStore.VerifiedHandoffRetirementFlight = null
  private var v5Store: com.wi.floatingpod.server.WindowsB1V5WitnessRecordStore = null
  private var ordinaryV5Transfer: MySqlFull20NativeTransfer = null
  private var ordinaryHandoffArtifact: WindowsB1Full20WitnessStore.NativeArtifactDurability = null
  private var ordinaryFirstSlot: com.wi.floatingpod.server.WindowsB1V5WitnessRecordStore.FirstSlotAvailability = null
  private var ordinaryV5Handoff: com.wi.floatingpod.server.WindowsB1V5WitnessRecordStore.NativeV5RecordDurability = null
  private var ordinaryHandoffAck: com.wi.floatingpod.server.WindowsB1TwoSlotStore.DurableHandoffNativeAck = null
  private var ordinaryV4Resume: com.wi.floatingpod.server.WindowsB1TwoSlotStore.ResumeSourceScope = null
  private var ordinaryV4Predecessor: com.wi.floatingpod.server.WindowsB1TwoSlotStore.ResumeNativeRecordDurability = null
  private var ordinaryV4WitnessScope: com.wi.floatingpod.server.WindowsB1TwoSlotStore.NativeWitnessScope = null
  private var ordinaryV4FirstRetirement: com.wi.floatingpod.server.WindowsB1TwoSlotStore.ResumeRetiredInactiveSlot = null
  private var ordinaryV4FinalWitnessScope: com.wi.floatingpod.server.WindowsB1TwoSlotStore.NativeWitnessScope = null
  private var ordinaryInstalledScope: com.wi.floatingpod.server.WindowsB1TwoSlotStore.InstalledResumeRecordScope = null
  private var ordinaryInstalledWorld: WindowsWorldProvisioner.ProvisionedWorld = null
  private var ordinarySettledV5: com.wi.floatingpod.server.WindowsB1V5WitnessRecordStore.NativeV5RecordDurability = null
  private var ordinarySettledV4: com.wi.floatingpod.server.WindowsB1TwoSlotStore.NativeRecordDurability = null
  private var originalAccounting: MySqlFull20ColdReadAccounting = null
  private var constructionAttempted = false
  private var closeAttempted = false
  private var closeReturned = false
  private var primary: Option[Throwable] = None
  private def owned(): Unit = require(Thread.currentThread() eq owner, "exact certified inspection native worker required")

  private def inspectionRequest: MySqlCertifiedRestoreRequest = request match
    case CertifiedSourceRequest.Inspection(value) => value
    case _: CertifiedSourceRequest.Ordinary => throw new IllegalStateException("ordinary source request cannot enter inspection-only holder")

  private def ordinaryRequest: MySqlOrdinaryCertifiedBootRequest = request match
    case CertifiedSourceRequest.Ordinary(value) => value
    case _: CertifiedSourceRequest.Inspection => throw new IllegalStateException("inspection source request cannot enter ordinary issuer")

  private[mysql] def dropRuntimeConfigurationForExit(): Unit =
    owned()
    require(!retainedResponsibility,"certified native/configuration responsibility remains retained")
    location=null;source=null

  def begin(inputs: MySqlRuntimeInputs, accounting: MySqlFull20ColdReadAccounting): Unit =
    owned()
    require(!constructionAttempted && inputs != null && accounting != null, "registered certified inspection begins once")
    accounting.requireRegisteredLookupParent()
    require(originalAccounting == null || (originalAccounting eq accounting),
      "exact original actor accounting parent is required")
    originalAccounting = accounting
    val metadata = inputs.originalMetadata
    val inspectionRequest = this.inspectionRequest
    require(inspectionRequest.boot.bootId() == inputs.bootId && inspectionRequest.boot.generation() == inputs.bootGeneration,
      "request boot differs from actual Session")
    val config = WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(metadata)
    require(config.configurationSha256().equalsIgnoreCase(inspectionRequest.controlledMetadata.approvedConfigurationSha256),
      "approved configuration differs before native construction")
    constructionAttempted = true
    try
      io = new WindowsMarkerIo()
      factory = new WindowsWorldProvisioner(io)
      anchors = new WindowsB1SourceAnchorStore(io)
      slots = new WindowsB1TwoSlotStore(io)
      location = anchors.captureLocation(inspectionRequest.controlledMetadata.metadataPath, config, inspectionRequest.plan.creationOperationUuid())
      require(location.configurationSha256().equalsIgnoreCase(inspectionRequest.controlledMetadata.approvedConfigurationSha256),
        "same-handle configuration differs")
      val expected = anchors.readExpectedSource(location)
      require(expected.expectedPlan() == inspectionRequest.plan, "existing anchor exact plan differs")
      source = anchors.rebarrierExisting(location, expected)
      inspection = factory.beginResumeInspection(inspectionRequest.requestedWorldPath, inspectionRequest.plan,
        expected.expectedReceiptSha256(), inspectionRequest.boot)
      require(inspection.nativeDirectoryPath() == expected.worldNativePath() &&
        inspection.sourceWorldIdentity() == expected.originalSourceIdentity(), "existing source directory identity differs")
      MySqlFreshPrepareRules.exactBytes(expected.originalReceiptCanonicalBytes(),
        inspection.freshProvisioningProof().receiptCanonicalBytes())
      inspection.validateInspection()
      coldScope = slots.bindResumeSourceForColdLookup(anchors, location, source, inspection)
      selected = slots.readCurrentForColdLookup(coldScope, accounting)
      var lookupFailure: Throwable = null
      try lookup = slots.inspectExistingV5(coldScope, selected.current(), accounting)
      catch
        case cause: Throwable =>
          lookupFailure = cause
          throw cause
      finally
        // 原getter失败也不能覆盖原lookup失败；已登记store仍由holder强持。
        try
          val retained = slots.retainedColdLookup(coldScope)
          if retained != null then lookup = retained
        catch
          case cleanup: Throwable =>
            if lookupFailure != null then MySqlOwnedSessionLifecycle.suppress(lookupFailure, cleanup)
            else throw cleanup
    catch
      case cause: Throwable =>
        primary = MySqlOwnedSessionLifecycle.combine(primary, cause)
        throw cause

  /** Ordinary source DATA is compared first; actual resume handles/process retirement are issued by the original native boundary. */
  def beginOrdinary(inputs: MySqlRuntimeInputs, accounting: MySqlFull20ColdReadAccounting): Unit =
    owned()
    require(!constructionAttempted && inputs != null && accounting != null, "registered ordinary source inspection begins once")
    accounting.requireRegisteredLookupParent()
    require(originalAccounting == null || (originalAccounting eq accounting),
      "exact original actor accounting parent is required")
    originalAccounting = accounting
    val request = ordinaryRequest
    val approved = request.approvedOriginalCreation
    require(request.boot.bootId() == inputs.bootId && request.boot.generation() == inputs.bootGeneration,
      "ordinary request boot differs from actual Session")
    val config =
      if inputs.hasV2Origin then WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(inputs.originalMetadataV2)
      else WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(inputs.originalMetadata)
    require(config.configurationSha256().equalsIgnoreCase(request.controlledMetadata.approvedConfigurationSha256) &&
      config.configurationSha256().equalsIgnoreCase(approved.configurationSha256()),
      "approved original configuration differs before native construction")
    require(request.requestedWorldPath.toAbsolutePath.normalize() == approved.worldPath(),
      "ordinary requested world differs from pinned original source path")
    constructionAttempted = true
    try
      io = new WindowsMarkerIo()
      factory = new WindowsWorldProvisioner(io)
      anchors = new WindowsB1SourceAnchorStore(io)
      slots = new WindowsB1TwoSlotStore(io)
      location = anchors.captureLocation(request.controlledMetadata.metadataPath, config, approved.plan().creationOperationUuid())
      require(location.configurationSha256().equalsIgnoreCase(approved.configurationSha256()) &&
        location.canonicalPath() == approved.anchorPath(), "actual controlled anchor path/configuration differs from approval")
      val expected = anchors.readExpectedSource(location)
      require(expected.expectedPlan() == approved.plan() &&
        expected.anchorSha256().equalsIgnoreCase(approved.anchorSha256()),
        "actual original anchor plan/path/content differs from approved source")
      source = anchors.rebarrierExisting(location, expected)
      require(source.fileIdentity() == approved.anchorFileIdentity(), "actual original anchor native identity differs from approval")
      val expectedOriginal = new WindowsWorldProvisioner.ExpectedOriginalCreation(approved.plan(),
        approved.creationBootUuid(), approved.creationBootGeneration(), approved.worldPath(),
        approved.canonicalReceiptLength(), approved.canonicalReceiptSha256(), approved.sourceWorldIdentity(),
        approved.guardFileIdentity(), approved.receiptFileIdentity(), approved.anchorFileIdentity(),
        approved.anchorPath(), approved.anchorSha256())
      inspection = factory.beginCertifiedResumeInspection(request.requestedWorldPath, expectedOriginal, request.boot)
      require(inspection.nativeDirectoryPath() == expected.worldNativePath() &&
        inspection.sourceWorldIdentity() == approved.sourceWorldIdentity(),
        "ordinary current source directory differs from its pinned native identity")
      MySqlFreshPrepareRules.exactBytes(expected.originalReceiptCanonicalBytes(),
        inspection.freshProvisioningProof().receiptCanonicalBytes())
      inspection.validateInspection()
      coldScope = slots.bindResumeSourceForColdLookup(anchors, location, source, inspection)
      selected = slots.readCurrentForColdLookup(coldScope, accounting)
      var lookupFailure: Throwable = null
      try lookup = slots.inspectExistingV5(coldScope, selected.current(), accounting)
      catch
        case cause: Throwable =>
          lookupFailure = cause
          throw cause
      finally
        try
          val retained = slots.retainedColdLookup(coldScope)
          if retained != null then lookup = retained
        catch
          case cleanup: Throwable =>
            if lookupFailure != null then MySqlOwnedSessionLifecycle.suppress(lookupFailure, cleanup)
            else throw cleanup
      if lookup != null && lookup.observation().status() == WindowsB1ColdResumeLookup.Status.UNRESOLVED_DATA &&
          lookup.observation().unresolvedReason() == WindowsB1ColdResumeLookup.UnresolvedReason.NO_V5 then
        // The V4-only fixed-pattern locator is issued only for the actual NO_V5 branch.
        historicalDiscovery = slots.issueFull20HistoricalDiscoveryPermit(coldScope, selected.current(), accounting)
        historicalArtifacts = slots.discoverFull20HistoricalArtifacts(historicalDiscovery, accounting).asScala.toVector
    catch
      case cause: Throwable =>
        primary = MySqlOwnedSessionLifecycle.combine(primary, cause)
        throw cause

  /** Actual existing V5 source is independently reopened before the actor plans/retains its fixed transfer. */
  def captureSettledV5Source(accounting: MySqlFull20ColdReadAccounting): WindowsB1TwoSlotStore.VerifiedWritableCurrentV5 =
    owned();requireSource()
    require(ordinaryRequest != null && verifiedCurrentV5 == null &&
      lookup.observation().status() == WindowsB1ColdResumeLookup.Status.RESOLVED_DATA &&
      lookup.observation().currentRecord() != null, "exact actual settled V5 ordinary source required once")
    verifiedCurrentV5 = slots.resumeVerifiedCurrentV5ForHandoff(coldScope,selected.current(),accounting,null)
    verifiedCurrentV5

  /** Complete earlier V5 provenance is retained by the same original cold owner. */
  def captureRecallV5History(accounting: MySqlFull20ColdReadAccounting): Unit =
    owned(); requireSource()
    require(verifiedCurrentV5 != null && recallV5History == null && (originalAccounting eq accounting),
      "one exact preinstall Recall history reader required")
    recallV5History = slots.readVerifiedV5History(verifiedCurrentV5, accounting)
    recallV5History.consume(accounting)

  def consumedRecallV5History(accounting: MySqlFull20ColdReadAccounting): java.util.List[com.wi.floatingpod.server.B1ResumeRecordV5] =
    owned(); requireSource()
    require(recallV5History != null && (originalAccounting eq accounting), "original consumed Recall history required")
    recallV5History.consumedRecords(accounting)

  /** Binds the actor-planned full20 transfer to the exact preinstall source and retains the private flight/store. */
  def beginExistingV5Handoff(accounting: MySqlFull20ColdReadAccounting,
      transfer: MySqlFull20NativeTransfer): com.wi.floatingpod.server.WindowsB1V5WitnessRecordStore.VerifiedHandoffRetirementFlight =
    owned();requireSource()
    require(verifiedCurrentV5 != null && verifiedV5Flight == null && transfer != null,
      "actual preinstall V5 source and one fixed planned HANDOFF transfer required")
    require(ordinaryV5Transfer == null, "fixed ordinary V5 transfer is one-shot")
    ordinaryV5Transfer=transfer
    verifiedV5Flight = slots.beginVerifiedV5Handoff(verifiedCurrentV5,transfer)
    v5Store = verifiedV5Flight.store()
    verifiedV5Flight

  /** V4 continuation is available only from the actual NO_V5 discovery permit and exact settled current. */
  def beginExistingV4Handoff(
      transfer: MySqlFull20NativeTransfer
  ): MySqlCertifiedOrdinaryCapabilities.V4NativeHandoffAcknowledgement =
    owned();requireSource()
    require(transfer != null && ordinaryV5Transfer == null && historicalDiscovery != null &&
      historicalArtifacts.nonEmpty && ordinaryHandoffArtifact == null && ordinaryHandoffAck == null &&
      lookup.observation().status() == WindowsB1ColdResumeLookup.Status.UNRESOLVED_DATA &&
      lookup.observation().unresolvedReason() == WindowsB1ColdResumeLookup.UnresolvedReason.NO_V5,
      "actual complete NO_V5 source and one planned ordinary HANDOFF transfer required")
    ordinaryV5Transfer=transfer
    try
      ordinaryV4Resume=slots.resumeVerifiedCurrentV4ForOrdinaryHandoff(historicalDiscovery,parentForNative())
      ordinaryV4Predecessor=slots.rebarrierCurrent(ordinaryV4Resume,selected.current())
      ordinaryV4WitnessScope=slots.witnessScopeForOrdinaryV4Handoff(ordinaryV4Resume,ordinaryV4Predecessor,transfer)
      val store=slots.full20WitnessStore(ordinaryV4WitnessScope,transfer)
      val artifact=store.publish(transfer)
      store.readCold(artifact)
      transfer.completeColdComparison()
      ordinaryHandoffArtifact=artifact
      if selected.inactive()!=null then
        ordinaryV4FirstRetirement=slots.retireInactiveSlot(ordinaryV4Resume,ordinaryV4Predecessor,selected.inactive())
        ordinaryV4Predecessor=ordinaryV4FirstRetirement.retainedRecord()
      ordinaryHandoffAck=slots.publishHandoffPrepared(ordinaryV4Resume,ordinaryV4Predecessor,
        transfer.originalRecord().assertions())
      MySqlCertifiedOrdinaryCapabilities.V4NativeHandoffAcknowledgement(ordinaryHandoffAck)
    catch
      case cause: Throwable =>
        primary=MySqlOwnedSessionLifecycle.combine(primary,cause)
        throw cause

  private def parentForNative(): MySqlFull20ColdReadAccounting =
    if originalAccounting == null then throw new IllegalStateException("Original actor full20 accounting root is unavailable")
    originalAccounting.requireRegisteredLookupParent()
    originalAccounting

  def publishOrdinaryHandoffArtifact(transfer: MySqlFull20NativeTransfer): WindowsB1Full20WitnessStore.NativeArtifactDurability =
    owned()
    require(v5Store != null && verifiedV5Flight != null && ordinaryV5Transfer != null &&
      (transfer eq ordinaryV5Transfer) && ordinaryHandoffArtifact == null,
      "exact preinstall flight and one actor-planned full20 HANDOFF transfer required")
    try
      val store = v5Store.beginHandoffArtifactStore(verifiedV5Flight)
      val acknowledgement = store.publish(transfer)
      store.readCold(acknowledgement)
      transfer.completeColdComparison()
      v5Store.retainVerifiedHandoffArtifact(verifiedV5Flight,acknowledgement)
      ordinaryHandoffArtifact=acknowledgement
      acknowledgement
    catch
      case cause: Throwable =>
        primary = MySqlOwnedSessionLifecycle.combine(primary,cause)
        throw cause

  def retireOrdinaryFirstV5Slot(): com.wi.floatingpod.server.WindowsB1V5WitnessRecordStore.FirstSlotAvailability =
    owned()
    require(v5Store != null && verifiedV5Flight != null && ordinaryHandoffArtifact != null && ordinaryFirstSlot == null,
      "actual compared HANDOFF artifact required before one first-slot retirement")
    try
      ordinaryFirstSlot=v5Store.retireVerifiedInactiveV5(verifiedV5Flight)
      ordinaryFirstSlot
    catch
      case cause: Throwable =>
        primary = MySqlOwnedSessionLifecycle.combine(primary,cause)
        throw cause

  def publishOrdinaryV5Handoff(): com.wi.floatingpod.server.WindowsB1V5WitnessRecordStore.NativeV5RecordDurability =
    owned()
    require(v5Store != null && verifiedV5Flight != null && ordinaryFirstSlot != null && ordinaryV5Handoff == null,
      "actual one-shot first-slot availability and exact V5 HANDOFF flight required")
    try
      ordinaryV5Handoff=v5Store.publishVerifiedHandoffPrepared(verifiedV5Flight,ordinaryFirstSlot)
      ordinaryV5Handoff
    catch
      case cause: Throwable =>
        primary = MySqlOwnedSessionLifecycle.combine(primary,cause)
        throw cause

  /** Uses the original inspection's one-shot install; a failure keeps the installed custody in that holder. */
  def installOrdinaryHandoff(): com.wi.floatingpod.server.WindowsB1TwoSlotStore.InstalledResumeRecordScope =
    owned();requireSource()
    require(ordinaryInstalledScope == null,
      "ordinary handoff installation is one-shot")
    if ordinaryHandoffAck == null then
      ordinaryHandoffAck =
        if verifiedV5Flight != null then
          require(ordinaryV5Handoff != null, "actual durable V5 HANDOFF record is required before owner installation")
          slots.handoffAcknowledgementForInstall(verifiedV5Flight, ordinaryV5Handoff)
        else if ordinaryV4Resume != null && ordinaryHandoffAck != null then ordinaryHandoffAck
        else throw new IllegalStateException("An actual V4 or V5 HANDOFF acknowledgement is required before owner installation")
    try
      ordinaryInstalledScope = factory.installResumeOwnerForRecords(inspection, ordinaryHandoffAck)
      ordinaryInstalledWorld = ordinaryInstalledScope.installedWorld()
      ordinaryInstalledScope
    catch
      case cause: Throwable =>
        primary = MySqlOwnedSessionLifecycle.combine(primary, cause)
        throw cause

  /** Called by the actor's opaque post-commit capability before either retirement/publication continuation. */
  private[mysql] def requireOrdinaryInstalledContinuation(
      installed: com.wi.floatingpod.server.WindowsB1TwoSlotStore.InstalledResumeRecordScope,
      expectedV5Flight: com.wi.floatingpod.server.WindowsB1V5WitnessRecordStore.VerifiedHandoffRetirementFlight
  ): Unit =
    owned()
    require(installed != null && (ordinaryInstalledScope eq installed) && ordinaryInstalledWorld != null &&
      (installed.installedWorld() eq ordinaryInstalledWorld) && ordinaryHandoffAck != null,
      "exact actual installed ordinary HANDOFF custody scope is required")
    if expectedV5Flight != null then
      require((verifiedV5Flight eq expectedV5Flight) && ordinaryV5Handoff != null,
        "exact original actor V5 retirement flight is required")
      val actual = verifiedV5Flight.actualHandoffRecord()
      require(actual eq ordinaryV5Handoff, "installed V5 custody no longer has its exact HANDOFF ACK")
    else
      val selected = slots.readCurrent(installed)
      val actual = selected.current()
      require(actual != null && ordinaryHandoffAck.record().kind() ==
        com.wi.floatingpod.server.B1ResumeRecord.Kind.HANDOFF_PREPARED &&
        java.util.Arrays.equals(ordinaryHandoffAck.canonicalBytes(), actual.canonicalBytes()) &&
        java.util.Arrays.equals(com.wi.floatingpod.server.B1ResumeRecordCanonical.encode(ordinaryHandoffAck.record()),
          com.wi.floatingpod.server.B1ResumeRecordCanonical.encode(actual.record())),
        "installed V4 fixed slot must retain the exact durable HANDOFF ACK bytes")

  private def requireOrdinaryInstalledPublication(
      committed: MySqlCertifiedOrdinaryCapabilities.CommittedCertifiedHandoff
  ): Unit =
    owned()
    require(originalAccounting != null && committed != null && primary.isEmpty &&
      ordinaryInstalledScope != null && ordinaryInstalledWorld != null && ordinaryHandoffAck != null,
      "exact original accounting, post-audit commit, and installed HANDOFF custody are required")
    originalAccounting.requireOpen()
    require(ordinaryInstalledScope.installedWorld() eq ordinaryInstalledWorld,
      "exact installed ordinary world custody is required")
    ordinaryInstalledScope.requireCurrent()
    committed.requireForNativeContinuation(verifiedV5Flight, ordinaryInstalledScope)

  /** Writes the exact actor-settled DB_ACQUIRED successor after post-audit and installed-custody retirement. */
  def publishOrdinaryDbAcquired(
      committed: MySqlCertifiedOrdinaryCapabilities.CommittedCertifiedHandoff,
      settled: com.wi.floatingpod.server.B1ResumeRecord
  ): MySqlCertifiedOrdinaryCapabilities.OrdinaryNativeSettledAcknowledgement =
    owned()
    try
      requireOrdinaryInstalledPublication(committed)
      require(settled != null && ordinarySettledV4 == null && ordinarySettledV5 == null,
        "exact installed/post-audit ordinary plan and one DB_ACQUIRED publication are required")
      if verifiedV5Flight != null then
        v5Store.retireOriginalCurrentV5AfterInstall(verifiedV5Flight, committed)
        ordinarySettledV5=v5Store.publishSettledDbAcquired(verifiedV5Flight,committed,settled)
        v5Store.readCold(ordinarySettledV5)
        MySqlCertifiedOrdinaryCapabilities.V5SettledNativeAcknowledgement(ordinarySettledV5)
      else
        require(ordinaryV4Resume != null && ordinaryV4WitnessScope != null && ordinaryHandoffArtifact != null,
          "exact installed V4 source, actual HANDOFF witness scope, and cold-compared artifact required")
        slots.retireOrdinaryV4SourceAfterCommit(ordinaryInstalledScope,committed)
        ordinaryV4FinalWitnessScope=slots.witnessScope(ordinaryInstalledScope)
        val finalStore=slots.v5WitnessRecordStore(ordinaryV4FinalWitnessScope,ordinaryV5Transfer)
        val state=settled.assertions()
        val prepare=historicalReference(state.originalPrepareWitness(),ordinaryV4FinalWitnessScope)
        val certify=historicalReference(state.acknowledgedCertifyWitness(),ordinaryV4FinalWitnessScope)
        ordinarySettledV5=finalStore.publishNext(null,com.wi.floatingpod.server.B1ResumeRecord.Kind.DB_ACQUIRED,
          state,prepare,null,ordinaryHandoffArtifact,certify)
        finalStore.readCold(ordinarySettledV5)
        MySqlCertifiedOrdinaryCapabilities.V5SettledNativeAcknowledgement(ordinarySettledV5)
    catch
      case cause: Throwable =>
        primary = MySqlOwnedSessionLifecycle.combine(primary,cause)
        throw cause

  def preinstallV5Source: WindowsB1TwoSlotStore.VerifiedWritableCurrentV5 =
    owned();requireSource();verifiedCurrentV5
  def ordinaryV5Flight: com.wi.floatingpod.server.WindowsB1V5WitnessRecordStore.VerifiedHandoffRetirementFlight =
    owned();if(verifiedV5Flight==null)throw new IllegalStateException("No retained ordinary V5 flight");verifiedV5Flight
  def ordinaryV5Store: com.wi.floatingpod.server.WindowsB1V5WitnessRecordStore =
    owned();if(v5Store==null)throw new IllegalStateException("No retained ordinary V5 store");v5Store

  private def historicalReference(
      attempt: com.wi.floatingpod.server.B1ResumeRecord.AttemptAssertions,
      installed: com.wi.floatingpod.server.WindowsB1TwoSlotStore.NativeWitnessScope
  ): com.wi.floatingpod.server.WindowsB1Full20WitnessStore.VerifiedArtifactReference =
    if attempt==null then throw new IllegalStateException("Exact retained settled historical attempt is required")
    val matches=historicalArtifacts.filter(_.reference().attemptId()==attempt.attemptId())
    if matches.size!=1 then throw new IllegalStateException("Exact unique NO_V5 full20 artifact for settled history is required")
    matches.head.requireExactAttempt(attempt)
    matches.head.reissueUnderInstalledScope(installed,ordinaryV5Transfer)

  def requireSource(): Unit =
    owned()
    require(inspection != null && source != null && selected != null && lookup != null,
      "actual original source and registered cold lookup are required")
    inspection.validateInspection()

  def selectedRecord: WindowsB1TwoSlotStore.ResumeStoredRecordFacts =
    requireSource(); selected.current()
  def coldObservation: WindowsB1ColdResumeLookup.Observation =
    requireSource(); lookup.observation()
  def historicalFull20Artifacts: Vector[WindowsB1Full20WitnessStore.VerifiedHistoricalArtifact] =
    requireSource()
    historicalArtifacts
  /** 仅原六项不可变来源DATA可在真实安装后读取，不授namespace或native操作资格。 */
  private def requireOriginalSourceFacts(): Unit =
    owned()
    primary.foreach(throw _)
    require(inspection != null && source != null && selected != null && lookup != null,
      "actual original source and registered cold lookup are required")
    val installedOrPartial = inspection.ownerPublicationAttempted() ||
      ordinaryInstalledScope != null || ordinaryInstalledWorld != null ||
      inspection.installedRecordScope() != null || inspection.installedWorld() != null
    if installedOrPartial then
      require(ordinaryInstalledScope != null && ordinaryInstalledWorld != null && ordinaryHandoffAck != null &&
        (inspection.installedRecordScope() eq ordinaryInstalledScope) &&
        (inspection.installedHandoffAcknowledgement() eq ordinaryHandoffAck) &&
        (inspection.installedWorld() eq ordinaryInstalledWorld) &&
        (ordinaryInstalledScope.installedWorld() eq ordinaryInstalledWorld),
        "exact installed original source facts require the retained scope/world/HANDOFF ACK")
      ordinaryInstalledScope.requireCurrent()
    else requireSource()

  def originalSourceIdentity: String =
    requireOriginalSourceFacts(); inspection.sourceWorldIdentity()
  def originalReceiptSha256: String =
    requireOriginalSourceFacts(); inspection.freshProvisioningProof().receiptSha256()
  def originalGuardFileIdentity: String =
    requireOriginalSourceFacts(); inspection.freshProvisioningProof().guardFileIdentity()
  def originalReceiptFileIdentity: String =
    requireOriginalSourceFacts(); inspection.freshProvisioningProof().receiptFileIdentity()
  def originalAnchorFileIdentity: String =
    requireOriginalSourceFacts(); source.fileIdentity()
  def originalAnchorSha256: String =
    requireOriginalSourceFacts(); source.anchorSha256()
  def previousNativeOwner: com.wi.floatingpod.server.B1ResumeRecord.NativeOwnerAssertions =
    requireSource(); inspection.previousOwner()
  def targetNativeOwner: com.wi.floatingpod.server.B1ResumeRecord.NativeOwnerAssertions =
    requireSource(); inspection.targetOwner()
  def originalOwnerCanonicalBytes: Array[Byte] =
    requireSource(); inspection.originalOwnerCanonicalBytes()
  def requireReceipt(bytes: Array[Byte]): Unit =
    requireSource()
    MySqlFreshPrepareRules.exactBytes(bytes, inspection.freshProvisioningProof().receiptCanonicalBytes())
  def namespace: String =
    requireSource(); source.verifiedInput().barrierNamespace()
  def retainedResponsibility: Boolean =
    owned(); io != null && !closeReturned
  def failure: Option[Throwable] =
    owned(); primary

  private[mysql] def issueOrdinaryExitPermit(session: MySqlOwnedDatabaseSession,
      actor: MySqlCertifiedRecoveryInspectionActor, root: MySqlFormat3ReadBudget,
      state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): MySqlCertifiedOrdinaryNativeExitPermit =
    owned()
    if primary.nonEmpty then throw primary.get
    actor.requireOrdinaryExitState(state)
    if ordinaryInstalledScope == null || ordinaryInstalledWorld == null || ordinaryHandoffAck == null ||
        ordinaryHandoffArtifact == null || ordinaryV5Transfer == null || inspection == null || io == null ||
        closeAttempted || closeReturned then throw new IllegalStateException("original settled Ordinary native owner required")
    ordinaryInstalledScope.requireCurrent()
    root.reserveRetainedMemory(96L,"原Session九引用Ordinary native permit在new前预付；失败同槽保管")
    new MySqlCertifiedOrdinaryNativeExitPermit(session,actor,this,root,inspection,ordinaryInstalledWorld,
      ordinaryInstalledScope,ordinaryHandoffAck,io)

  private[mysql] def closeOrdinaryForExit(permit: MySqlCertifiedOrdinaryNativeExitPermit,
      state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): Unit =
    owned()
    if primary.nonEmpty then throw primary.get
    permit.requireHolder(this)
    if closeAttempted || closeReturned then throw new IllegalStateException("original Ordinary native close is one-shot")
    val actor=permit.originalActorForHolder(this)
    closeAttempted=true
    try
      if recallV5History != null then recallV5History.endOrdinaryConsumers(actor,state)
      if verifiedV5Flight != null then
        if v5Store == null then throw new IllegalStateException("original V5 flight store missing")
        v5Store.endOrdinaryDataConsumers(actor,state,ordinaryInstalledScope,ordinaryV5Transfer)
      else
        val bridge=slots.originalOrdinaryBridgeStore(ordinaryV4FinalWitnessScope,ordinaryInstalledScope,ordinaryV5Transfer)
        bridge.endOrdinaryDataConsumers(actor,state,ordinaryInstalledScope,ordinaryV5Transfer)
      var h=0
      while h<historicalArtifacts.size do
        historicalArtifacts(h).endOrdinaryConsumers(actor,state)
        h+=1
      historicalArtifacts=Vector.empty
      // 全部V5/history消费者退出后才封Full20 ACK；最终source活核不重新读取这些DATA。
      ordinaryHandoffArtifact.beginOrdinaryConsumerEnd(actor,state,ordinaryInstalledScope)
      ordinaryHandoffArtifact.finishOrdinaryConsumerEnd(actor,state)
      slots.requireOrdinaryTerminalInstalled(ordinaryInstalledScope,permit)
      factory.closeCertifiedOrdinaryCustody(inspection,ordinaryInstalledWorld,ordinaryInstalledScope,ordinaryHandoffAck,permit)
      io.close()
      permit.nativeCloseReturned(this)
      if verifiedV5Flight != null then
        slots.detachOrdinaryV5FlightIdentity(verifiedV5Flight,actor,state,ordinaryV5Transfer)
      else
        slots.detachOrdinaryWitnessIdentity(ordinaryV4FinalWitnessScope,actor,state,ordinaryV5Transfer)
        slots.detachOrdinaryWitnessIdentity(ordinaryV4WitnessScope,actor,state,ordinaryV5Transfer)
      slots.detachOrdinaryInstalledIdentity(ordinaryInstalledScope,permit)
      ordinaryV5Transfer.detachOrdinaryTerminal(actor,state)
      originalAccounting=null;ordinaryV5Transfer=null;ordinaryHandoffArtifact=null;ordinaryHandoffAck=null
      ordinaryV5Handoff=null;ordinarySettledV5=null;ordinarySettledV4=null;ordinaryFirstSlot=null
      ordinaryV4Resume=null;ordinaryV4Predecessor=null;ordinaryV4WitnessScope=null;ordinaryV4FinalWitnessScope=null
      ordinaryV4FirstRetirement=null;ordinaryInstalledScope=null;ordinaryInstalledWorld=null
      verifiedV5Flight=null;verifiedCurrentV5=null;v5Store=null;recallV5History=null
      historicalDiscovery=null;lookup=null;selected=null;coldScope=null;inspection=null
      slots=null;anchors=null;location=null;source=null;factory=null;io=null;closeReturned=true
    catch
      case cause: Throwable =>
        primary=MySqlOwnedSessionLifecycle.combine(primary,cause)
        throw cause

  def settleForExit(): Unit =
    owned()
    // Existing inspection保share0/source handles；不能当成无资源或释放custody。
    if io != null && inspection == null && !closeAttempted then
      closeAttempted = true
      try
        io.close()
        closeReturned = true
      catch
        case cause: Throwable =>
          primary = MySqlOwnedSessionLifecycle.combine(primary, cause)
          throw cause
