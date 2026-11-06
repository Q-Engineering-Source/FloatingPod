package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.MySqlRuntimeMetadata
import com.wi.floatingpod.server.{B1ResumeRecord, B1ResumeRecordV6, WindowsB1Full26WitnessStore, WindowsB1V6WitnessRecordStore, WindowsB1SourceAnchorStore, WindowsCanonicalMarkerPublicationBarrier, WindowsMarkerIo, WindowsWorldProvisioner, WindowsWorldProvisionReceiptDataCodec}
import com.wi.floatingpod.bootstrap.ProductionBootstrapControl
import java.io.ByteArrayOutputStream
import java.nio.file.Path
import java.util.UUID

/** 实际 native5 caller 的永久责任 holder；不是 SQL admission、私有 plan 或 Ready。 */
private[mysql] final class MySqlDeploymentReceiptFreshNativeOwner private (
    private val root: MySqlFormat3ReadBudget,
    private var graph: MySqlRuntimeInputs.DeploymentReceiptStaticGraph,
    private var worldPath: Path,
    private var plan: WindowsWorldProvisionReceiptDataCodec.PlanData,
    private var boot: WindowsWorldProvisioner.BootIdentity,
    private var controlled: MySqlFreshControlledMetadata,
    private var original: Option[ProductionBootstrapControl.OriginalSourcePins] = None
) extends AutoCloseable:
  private val thread = Thread.currentThread()
  private var lease: MySqlRuntimeInputs.ConsumerLease = null
  private var metadata: MySqlRuntimeMetadata.DeploymentReceiptStatic = null
  private var configuration: WindowsB1SourceAnchorStore.AnchorConfiguration = null
  private var io: WindowsMarkerIo = null
  private var provisioner: WindowsWorldProvisioner = null
  private var anchors: WindowsB1SourceAnchorStore = null
  private var location: WindowsB1SourceAnchorStore.ControlledAnchorLocation = null
  private var world: WindowsWorldProvisioner.DeploymentReceiptWorld = null
  private var ack: WindowsB1SourceAnchorStore.DurableB1SourceAnchor = null
  private var inspection: WindowsWorldProvisioner.DeploymentReceiptInspection = null
  private var previousExit: WindowsWorldProvisioner.DeploymentReceiptPreviousOwnerExit = null
  private var source: WindowsWorldProvisioner.DeploymentReceiptIoScope = null
  private var artifacts: WindowsB1Full26WitnessStore = null
  private var records: WindowsB1V6WitnessRecordStore = null
  private var markerLocation: WindowsWorldProvisioner.DeploymentCanonicalMarkerLocation = null
  private var marker: WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck = null
  private var originalMarker:WindowsWorldProvisioner.DeploymentReceiptMarkerInspection=null
  private var attempted = false
  private var construction = false
  private var capture = false
  private var creation = false
  private var publication = false
  private var closed = false
  private var primary: Throwable = null
  private var cleanupFailure: Throwable = null
  private var retainedFee = 0L
  private var workspaceFee = 0L
  private var markerInspectionFee=0L
  private var retirementAttempted=false
  private var nativeCloseReturned=false
  // 两个 Win32 长路径的规范化副本，以及有界 metadata map/Properties/UTF16 控制峰值。
  // 必须在 toString/normalize/completeCanonicalFields 之前预付，不能由后面的持久费倒补。
  private val validationPeak = 64L * MySqlRuntimeMetadata.MAX_METADATA_BYTES + 32L * 32768L
  def failure: Option[Throwable] = Option(primary)
  def failedCleanup: Option[Throwable] = Option(cleanupFailure)
  def attemptedCreation: Boolean = creation
  def attemptedSourceCapture: Boolean = capture
  def attemptedPublication: Boolean = publication
  def attemptedNativeConstruction: Boolean = construction
  def hasUnsettledResources: Boolean = !closed
  private def requireThread(): Unit =
    if Thread.currentThread() ne thread then throw new IllegalStateException("exact native5 IO owner thread required")
  private def requireLive(): Unit =
    requireThread()
    MySqlManifestConfigurationIdentity.requireRoot(root)
    if closed || cleanupFailure != null then throw new IllegalStateException("native5 owner closed or cleanup failed",cleanupFailure)
  private def recordCleanup(action: => Unit): Unit =
    try action
    catch
      case failure: Throwable =>
        if cleanupFailure == null then cleanupFailure = failure
        else MySqlOwnedIoAdapter.suppressExitCleanup(cleanupFailure,failure)
        throw failure

  /** 必须先由 caller 保留此 holder。每一实际副作用入口均在调用前登记一次性 attempt。 */
  def createFresh(): Unit =
    require(original.isEmpty,"Fresh original native mode required")
    openNativeSource()

  def inspectOrdinary(): Unit =
    require(original.nonEmpty,"Ordinary original native source pins required")
    openNativeSource()

  private def openNativeSource(): Unit =
    requireLive()
    if attempted then throw new IllegalStateException("native5 Fresh owner is one-shot; no blind retry",primary)
    attempted = true
    var validationReserved = false
    try
      root.reserveTemporaryMemory(validationPeak,"native5 pre-native validation/control peak")
      validationReserved = true
      if graph == null || plan == null || boot == null || controlled == null || worldPath == null ||
          !worldPath.isAbsolute || worldPath.getParent == null || worldPath.getFileName == null ||
          plan.schemaFormat() != 5 || plan.physicalFormat() != 2 || boot.generation() <= 0L || boot.bootId() == new UUID(0L,0L) then
        throw new IllegalArgumentException("explicit native5 path/PlanData5/actual boot/static graph/control required")
      val worldText = worldPath.toString
      val metadataText = controlled.metadataPath.toString
      if worldText.length > 32768 || metadataText.length > 32768 then
        throw new IllegalArgumentException("native5 paths exceed bounded Win32 path input")
      if controlled.metadataPath.toAbsolutePath.normalize().startsWith(worldPath.toAbsolutePath.normalize()) then
        throw new IllegalArgumentException("native5 controlled metadata must be lexically outside world; actual native checks remain mandatory")
      lease = graph.borrowForNative(root)
      metadata = graph.nativeMetadata(lease,root)
      graph.requireNativePlan(lease,root,plan)
      val id = graph.approvedBaselineIdentity
      if id.schemaFormat != 5 || id.serverUuid != plan.mysqlServerUuid() || id.schema != plan.schema() ||
          id.bindingUuid != plan.bindingUuid() || id.provisioningUuid != plan.provisioningUuid() ||
          !id.ddlSha256.equalsIgnoreCase(plan.ddlSha256()) then throw new IllegalArgumentException("actual native5 plan differs from approved static baseline")
      val fields = metadata.completeCanonicalFields().entrySet().iterator()
      var chars = 0L
      while fields.hasNext do
        val field = fields.next()
        chars = Math.addExact(chars,Math.addExact(field.getKey.length.toLong,field.getValue.length.toLong))
      val pathChars = Math.addExact(worldText.length.toLong,metadataText.length.toLong)
      // A/config/location/receipt/proof/owner/verified-anchor 的有界持久副本及直接 holder。
      val fee = Math.addExact(16384L + 12L * MySqlRuntimeMetadata.MAX_METADATA_BYTES,
        Math.addExact(Math.multiplyExact(chars,16L),Math.multiplyExact(pathChars,16L)))
      root.reserveRetainedMemory(fee,"native5 bounded persistent metadata/native proof/receipt/anchor owners BEFORE allocation")
      retainedFee = fee
      configuration = WindowsB1SourceAnchorStore.AnchorConfiguration.ofDeploymentReceiptMetadata(metadata)
      if !configuration.configurationSha256().equalsIgnoreCase(controlled.approvedConfigurationSha256) then
        throw new IllegalArgumentException("approved complete schema5 A differs before native construction")
      // 覆盖 bounded Properties/UTF8/reencode/readback/receipt/anchor 副本及 Win32 extended paths 的峰值。
      val peak = Math.addExact(64L * MySqlRuntimeMetadata.MAX_METADATA_BYTES,
        Math.addExact(16L * 32768L,Math.multiplyExact(pathChars,16L)))
      root.reserveTemporaryMemory(peak,"native5 bounded native/config/receipt/anchor call peak BEFORE construction")
      workspaceFee = peak
      construction = true
      io = new WindowsMarkerIo()
      provisioner = new WindowsWorldProvisioner(io)
      anchors = new WindowsB1SourceAnchorStore(io)
      capture = true
      location = anchors.captureLocation(controlled.metadataPath,configuration,plan.creationOperationUuid())
      original match
        case Some(pins) =>
          inspection=provisioner.inspectOriginalSource(pins,boot)
          previousExit=provisioner.verifyPreviousOwnerExit(inspection)
          val existing=anchors.readExpectedSource(location)
          ack=anchors.rebarrierExisting(location,existing)
          source=provisioner.bindDeploymentReceiptInspection(inspection,previousExit,anchors,location,ack)
        case None =>
          creation = true
          world = provisioner.createFreshDeploymentReceipt(worldPath,plan,boot)
      // 成功 native5 factory 已永久保留 share0 custody；之后任何失败都不能 close 它。
      if original.isEmpty then
        val actual = world.creationProof()
        if actual.plan() != plan || actual.creationBoot() != boot || (world.custodyProof().creationProof() ne actual) then
          throw new IllegalStateException("actual native5 result differs from exact original request")
        world.custodyProof().validateLive()
        publication = true
        ack = anchors.publishCreatedDeploymentReceiptSource(location,world)
        if ack.verifiedInput().expectedDeploymentReceiptPlan() != plan ||
            !ack.verifiedInput().expectedReceiptSha256().equalsIgnoreCase(actual.receiptSha256()) then
          throw new IllegalStateException("actual native5 durable source anchor differs from original creation")
        world.custodyProof().validateLive()
        source=provisioner.bindDeploymentReceiptSource(world,anchors,location,ack)
      artifacts=new WindowsB1Full26WitnessStore(source,root)
      records=new WindowsB1V6WitnessRecordStore(source,artifacts,root)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      // Native 失败可能留下 arena/handle/磁盘责任；峰值保留为未结责任，不能假定局部 close 已清空。
      try MySqlFormat3BudgetCleanup.run(primary,Vector(
        () => recordCleanup {
          if workspaceFee != 0L && (!construction || primary == null) then
            root.releaseTemporaryMemory(workspaceFee,"native5 completed bounded call peak")
            workspaceFee = 0L
        },
        () => recordCleanup {
          if validationReserved then root.releaseTemporaryMemory(validationPeak,"native5 pre-native validation/control peak")
        }))
      catch
        case failure: Throwable =>
          if primary == null then primary = failure
          throw failure

  /** 仅返回真实 opaque native 结果；borrow/root/owner 必须仍活，不能据此签发 SQL。 */
  def retainedWorld: Option[WindowsWorldProvisioner.DeploymentReceiptWorld] =
    requireLive()
    if lease != null then graph.nativeMetadata(lease,root)
    if world != null then world.custodyProof().validateLive()
    Option(world)
  def retainedAnchor: Option[WindowsB1SourceAnchorStore.DurableB1SourceAnchor] =
    retainedWorld
    Option(ack)
  def controlledLocation: Option[WindowsB1SourceAnchorStore.ControlledAnchorLocation] =
    requireLive()
    if lease != null then graph.nativeMetadata(lease,root)
    Option(location)

  private[mysql] def withSource[A](consume: WindowsWorldProvisioner.DeploymentReceiptIoScope => A): A =
    requireLive()
    if source==null || primary!=null then throw new IllegalStateException("actual original native source scope unavailable",primary)
    source.validateLive();consume(source)
  private[mysql] def withRetirementIdentity[A](session:MySqlDeploymentReceiptSession,
      intent:MySqlDeploymentReceiptSession.TerminalIntent5)
      (consume:(WindowsWorldProvisioner.DeploymentReceiptIoScope,WindowsMarkerIo)=>A):A =
    session.requireTerminalNativeOwner(this,intent)
    requireLive()
    if source==null || io==null || retirementAttempted then throw new IllegalStateException("original live schema5 retirement identity required")
    source.validateLive();consume(source,io)
  private[mysql] def requireActualNativeCloseReturn(permit:MySqlDeploymentReceiptSession.NativeRetirementPermit5):Unit =
    permit.requireOwner(this)
    if !retirementAttempted || !nativeCloseReturned then throw new IllegalStateException("actual original schema5 custody and IO close must return before identity detach")

  private[mysql] def publishBody(record: B1ResumeRecord,body: MySqlFull26WitnessCodec.Owner): WindowsB1Full26WitnessStore.DurableFull26Ack =
    withSource { _ =>
      val size=body.byteLength
      val fee=Math.addExact(validationPeak,Math.multiplyExact(size.toLong,4L))
      root.reserveTemporaryMemory(fee,"Full26 body native transfer and actual native call peak BEFORE allocation")
      var failure:Throwable=null
      try
        body.withData { (original,_,_,_,_) =>
          if !java.util.Arrays.equals(com.wi.floatingpod.server.B1ResumeRecordCanonical.encodeSchema5Data(original),
              com.wi.floatingpod.server.B1ResumeRecordCanonical.encodeSchema5Data(record)) then
            throw new IllegalArgumentException("exact original body record required")
        }
        val output=new ByteArrayOutputStream(size)
        body.writeBodyTo(output)
        val bytes=output.toByteArray
        var configuration=Array.emptyByteArray
        body.withData { (_,before,_,_,_) =>
          val i=before.inputs.forTable("fp_schema_manifest").columns.indexWhere(_.name=="configuration_sha256")
          before.relation("fp_schema_manifest").rows.head(i) match
            case MySqlFormat3Cell.Binary(value) => configuration=Array.tabulate(value.length)(value.byteAt)
            case _ => throw new IllegalStateException("actual Full26 manifest C required")
        }
        val actual=artifacts.publish(record,bytes,configuration)
        body.requireReference(actual.reference());actual
      catch
        case cause:Throwable =>
          failure=cause
          primary=cause
          throw cause
      finally
        if failure==null then root.releaseTemporaryMemory(fee,"Full26 native transfer completed")
    }

  private[mysql] def publishGenesis(record:B1ResumeRecordV6,artifact:WindowsB1Full26WitnessStore.DurableFull26Ack):WindowsB1V6WitnessRecordStore.DurableV6Ack =
    withSource(_=>records.publishGenesis(record,java.util.List.of(artifact)))
  private[mysql] def publishNext(previous:WindowsB1V6WitnessRecordStore.DurableV6Ack,record:B1ResumeRecordV6,
      artifact:Option[WindowsB1Full26WitnessStore.DurableFull26Ack]):WindowsB1V6WitnessRecordStore.DurableV6Ack =
    withSource(_=>records.publishNext(previous,record,artifact.map(java.util.List.of(_)).getOrElse(java.util.List.of())))
  private[mysql] def reserveConfirmation(plan:MySqlDeploymentReceiptBootstrapActor.Planned5,
      previous:WindowsB1V6WitnessRecordStore.DurableV6Ack)(construct: =>B1ResumeRecordV6):WindowsB1V6WitnessRecordStore.ConfirmationReservation =
    withSource(_=>records.reserveConfirmation(plan,previous,()=>construct))
  private[mysql] def publishConfirmation(plan:MySqlDeploymentReceiptBootstrapActor.Planned5,
      previous:WindowsB1V6WitnessRecordStore.DurableV6Ack,reserved:WindowsB1V6WitnessRecordStore.ConfirmationReservation):WindowsB1V6WitnessRecordStore.DurableV6Ack =
    withSource(_=>records.publishConfirmation(reserved,plan,previous))
  private[mysql] def discoverCurrent():WindowsB1V6WitnessRecordStore.CurrentInventory = withSource(_=>records.discoverCurrent())
  private[mysql] def retainCurrentInventory(expected:WindowsB1V6WitnessRecordStore.DurableV6Ack):WindowsB1V6WitnessRecordStore.CurrentInventory =
    withSource(_=>records.retainCurrentInventory(expected))
  private[mysql] def withArtifact(reference:com.wi.floatingpod.server.B1Full26WitnessReference)(consume:Array[Byte]=>Unit):Unit =
    withSource(_=>artifacts.withReadBody(reference,bytes=>consume(bytes)))
  private[mysql] def publishPreparedMarker():WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck =
    withSource { scope =>
      if marker!=null then throw new IllegalStateException("original schema5 marker publication is one-shot")
      markerLocation=provisioner.ensureDeploymentCanonicalMarkerLocation(scope)
      marker=new WindowsCanonicalMarkerPublicationBarrier(io).publish(markerLocation.barrierRequest())
      marker
    }
  private[mysql] def rebarrierMarker(actual:WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck):Unit =
    withSource { _ =>
      if actual==null || (actual ne marker) || markerLocation==null then throw new IllegalArgumentException("exact native schema5 marker ACK required")
      new WindowsCanonicalMarkerPublicationBarrier(io).rebarrierExisting(markerLocation.barrierRequest(),actual)
      ()
    }
  private[mysql] def installOrdinary(handoff:WindowsB1V6WitnessRecordStore.DurableV6Ack):Unit =
    withSource { scope =>
      if original.isEmpty || inspection==null || world!=null then throw new IllegalStateException("exact uninstalled schema5 Ordinary source required")
      world=provisioner.installCurrentOwner(inspection,scope,handoff)
    }
  private[mysql] def installSuccessorPrepare(permit:MySqlDeploymentReceiptBootstrapActor.SuccessorPrepareInstallPermit5):Unit =
    withSource { scope=>
      if original.isEmpty || inspection==null || world!=null || permit==null then
        throw new IllegalStateException("exact uninstalled schema5 Ordinary source and successor permit required")
      world=provisioner.installCurrentOwner(inspection,scope,permit)
    }
  private[mysql] def rebarrierOriginalMarker(assertions:B1ResumeRecord.MarkerAssertions):Unit =
    withSource { scope=>
      root.reserveRetainedMemory(512L,"schema5 original marker inspection partial holder BEFORE native attempt")
      markerInspectionFee=Math.addExact(markerInspectionFee,512L)
      root.reserveTemporaryMemory(validationPeak,"schema5 original marker native path/call peak")
      var failure:Throwable=null
      try
        originalMarker=provisioner.rebarrierDeploymentReceiptMarker(scope,assertions)
        originalMarker.requireCurrent()
      catch
        case cause:Throwable=>
          failure=cause;primary=cause
          throw cause
      finally
        if failure==null then root.releaseTemporaryMemory(validationPeak,"schema5 original marker native rebarrier complete")
    }

  /** 普通 close 仍禁止；仅原 Session 实际 SQL 结算后的专用许可可退 native custody。 */
  private[mysql] def retireAfterSqlSettlement(permit:MySqlDeploymentReceiptSession.NativeRetirementPermit5):Unit =
    if permit==null then throw new IllegalArgumentException("original schema5 native retirement permit required")
    permit.requireOwner(this)
    requireThread()
    if retirementAttempted || closed || primary!=null || cleanupFailure!=null || source==null || world==null || io==null then
      throw new IllegalStateException("original installed native5 retirement is one-shot",Option(primary).getOrElse(cleanupFailure))
    retirementAttempted=true
    try
      if records!=null then
        records.retireAfterSqlSettlement(permit);records=null
      if artifacts!=null then
        artifacts.retireAfterSqlSettlement(permit);artifacts=null
      provisioner.closeDeploymentReceiptCustody(source,world,permit)
      io.close()
      nativeCloseReturned=true
      permit.detachNativeIdentityAfterClose(this)
      // 实际 native close 返回后才撤原强边；lease/费用失败仍保留相应原槽。
      originalMarker=null;marker=null;markerLocation=null;source=null;world=null;inspection=null;previousExit=null
      ack=null;location=null;anchors=null;provisioner=null;io=null
      graph=null;metadata=null;configuration=null;worldPath=null;plan=null;boot=null;controlled=null;original=None
      if lease!=null then
        lease.close();lease=null
      if workspaceFee!=0L then throw new IllegalStateException("native5 retirement retains uncertain native workspace fee")
      if markerInspectionFee!=0L then
        root.releaseRetainedMemory(markerInspectionFee,"schema5 original marker inspection graph retired");markerInspectionFee=0L
      if retainedFee!=0L then
        root.releaseRetainedMemory(retainedFee,"native5 actual static/native graph retired");retainedFee=0L
      root.releaseRetainedMemory(512L,"native5 retired original caller holder")
      closed=true
    catch
      case cause:Throwable=>cleanupFailure=cause;throw cause

  /** 只允许证明尚未尝试 NativeIO 的本地回收；native5 成功/失败均保留实际责任，不 ordinary-close custody。 */
  override def close(): Unit =
    requireThread()
    if construction then throw new IllegalStateException("native5 process-lifetime or uncertain native responsibility retained; ordinary close forbidden",primary)
    if cleanupFailure != null then throw new IllegalStateException("native5 local cleanup failed; no automatic retry",cleanupFailure)
    if !closed then
      graph = null; metadata = null; configuration = null; worldPath = null; plan = null; boot = null; controlled = null
      try
        if lease != null then
          lease.close()
          lease = null
        if retainedFee != 0L then
          root.releaseRetainedMemory(retainedFee,"native5 detached pre-native persistent reservation")
          retainedFee = 0L
        root.releaseRetainedMemory(512L,"native5 detached caller holder")
        closed = true
      catch
        case failure: Throwable =>
          cleanupFailure = failure
          if primary != null then MySqlOwnedIoAdapter.suppressExitCleanup(primary,failure)
          throw failure
  override def toString: String = "DeploymentReceiptFreshNativeOwner[actual responsibility; no admission]"

private[mysql] object MySqlDeploymentReceiptFreshNativeOwner:
  def beginOrdinary(root:MySqlFormat3ReadBudget,graph:MySqlRuntimeInputs.DeploymentReceiptStaticGraph,
      original:ProductionBootstrapControl.OriginalSourcePins,boot:WindowsWorldProvisioner.BootIdentity,
      controlled:MySqlFreshControlledMetadata):MySqlDeploymentReceiptFreshNativeOwner =
    MySqlManifestConfigurationIdentity.requireRoot(root)
    require(original!=null,"original schema5 source pins required")
    root.reserveRetainedMemory(512L,"native5 original Ordinary partial holder BEFORE allocation")
    try new MySqlDeploymentReceiptFreshNativeOwner(root,graph,original.worldPath(),original.plan(),boot,controlled,Some(original))
    catch
      case failure:Throwable =>
        try root.releaseRetainedMemory(512L,"unpublished Ordinary native5 holder")
        catch case cleanup:Throwable=>MySqlOwnedIoAdapter.suppressExitCleanup(failure,cleanup)
        throw failure
  def begin(root: MySqlFormat3ReadBudget, graph: MySqlRuntimeInputs.DeploymentReceiptStaticGraph,
      worldPath: Path, plan: WindowsWorldProvisionReceiptDataCodec.PlanData, boot: WindowsWorldProvisioner.BootIdentity,
      controlled: MySqlFreshControlledMetadata): MySqlDeploymentReceiptFreshNativeOwner =
    MySqlManifestConfigurationIdentity.requireRoot(root)
    root.reserveRetainedMemory(512L,"native5 caller-retained partial holder BEFORE allocation")
    try new MySqlDeploymentReceiptFreshNativeOwner(root,graph,worldPath,plan,boot,controlled)
    catch
      case failure: Throwable =>
        try root.releaseRetainedMemory(512L,"unpublished native5 holder")
        catch case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
        throw failure
