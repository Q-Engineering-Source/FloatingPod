package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{RecallRefusalReason,PodOperatingMode}
import com.wi.floatingpod.storage.UnboundIntentOrigin

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.entity.{DbFirstCaptureAllocation, DbFirstPodSnapshot}
import java.io.IOException
import java.sql.SQLException
import com.wi.floatingpod.server.io.{IoOperationTag, IoSubmission}
import com.wi.floatingpod.server.{ExactBodyRemovalAcknowledgement,TicketReleaseAcknowledgement}
import java.util.UUID
import java.time.LocalDateTime
import scala.collection.mutable

/** 无JDBC/世界/Ready能力的opaque原reservation引用；只有实际session issuer可建立。 */
private[floatingpod] final class MySqlRetainedPlanHandle private[mysql] (
    private[mysql] val issuer: MySqlOwnedRetainedPlanIssuer,
    private[mysql] val serial: Long, val tag: IoOperationTag,
    private[mysql] val reservationCore: MySqlRetainedPlanReservationCore)

/** 仅原 entry 的一次 capture admission；没有 plan/SQL 转换能力。 */
private[floatingpod] final class MySqlDbFirstCaptureReservation private[mysql] (
    private[mysql] val core: MySqlRetainedPlanReservationCore,
    private[mysql] val handle: MySqlRetainedPlanHandle, val profile: SourceLimitProfile,
    val purpose: DbFirstPodSnapshot.Purpose = DbFirstPodSnapshot.Purpose.RECALL):
  val tag: IoOperationTag = handle.tag

/** 原Main一次领取的opaque确认；只含不可变metadata，不是DATA收据的权限转换。 */
private[floatingpod] final class MySqlDbFirstRecallConfirmation private (
    private[mysql] val core:MySqlRetainedPlanReservationCore,private[mysql] val handle:MySqlRetainedPlanHandle,
    val bindingUuid:UUID,val podUuid:UUID,val ownerUuid:Option[UUID],val attemptUuid:UUID,
    val capturedStateVersion:Long,val committedStateVersion:Long,val pose:DbFirstPodSnapshot.Pose,
    val retainedFailure:Option[Throwable],
    qualificationInput:MySqlDbFirstSession.FailedRecallCleanupQualification):
  private val retainedMode=qualificationInput!=null
  @volatile private var qualification=qualificationInput
  val tag:IoOperationTag=handle.tag
  def isRetained:Boolean=retainedMode
  private[mysql] def retainedQualification:MySqlDbFirstSession.FailedRecallCleanupQualification=qualification
  /** 原core在Main实际交接时撤边；模式永久保留，撤资格不能转换成普通confirmation。 */
  private[mysql] def detachRetainedQualification(original:MySqlDbFirstSession.FailedRecallCleanupQualification):Unit =
    require(retainedMode && original!=null && (qualification eq original),"original retained qualification detach is one-shot")
    qualification=null
  def requireOriginal(reservation:MySqlDbFirstCaptureReservation):Unit =
    if isRetained then core.requireRetainedDbFirstConfirmation(this,reservation)
    else core.requireDbFirstConfirmation(this,reservation)
private[mysql] object MySqlDbFirstRecallConfirmation:
  val HolderBytes=512L
  def issue(core:MySqlRetainedPlanReservationCore,handle:MySqlRetainedPlanHandle,
      request:MySqlDbFirstRecallRequest):MySqlDbFirstRecallConfirmation =
    new MySqlDbFirstRecallConfirmation(core,handle,request.bindingUuid,request.podUuid,request.ownerUuid,
      request.attemptUuid,request.capturedStateVersion,Math.addExact(request.capturedStateVersion,1L),request.pose,None,null)
  def issueRetained(core:MySqlRetainedPlanReservationCore,handle:MySqlRetainedPlanHandle,
      request:MySqlDbFirstRecallRequest,cause:Option[Throwable],
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):MySqlDbFirstRecallConfirmation =
    new MySqlDbFirstRecallConfirmation(core,handle,request.bindingUuid,request.podUuid,request.ownerUuid,
      request.attemptUuid,request.capturedStateVersion,Math.addExact(request.capturedStateVersion,1L),request.pose,
      cause,qualification)
/** 原Main一次领取的opaque确认；只含不可变metadata，不是DATA收据的权限转换。 */
private[floatingpod] final class MySqlDbFirstCheckpointConfirmation private (
    private[mysql] val core:MySqlRetainedPlanReservationCore,private[mysql] val handle:MySqlRetainedPlanHandle,
    val bindingUuid:UUID,val podUuid:UUID,val ownerUuid:Option[UUID],val attemptUuid:UUID,
    val capturedStateVersion:Long,val committedStateVersion:Long,val pose:DbFirstPodSnapshot.Pose):
  val tag:IoOperationTag=handle.tag
  def requireOriginal(reservation:MySqlDbFirstCaptureReservation):Unit=core.requireDbFirstCheckpointConfirmation(this,reservation)
private[mysql] object MySqlDbFirstCheckpointConfirmation:
  val HolderBytes=512L
  def issue(core:MySqlRetainedPlanReservationCore,handle:MySqlRetainedPlanHandle,
      request:MySqlDbFirstCheckpointRequest):MySqlDbFirstCheckpointConfirmation =
    new MySqlDbFirstCheckpointConfirmation(core,handle,request.bindingUuid,request.podUuid,request.ownerUuid,
      request.attemptUuid,request.capturedStateVersion,Math.addExact(request.capturedStateVersion,1L),request.pose)
/** 原Main一次领取的opaque确认；只含不可变metadata，不是DATA收据的权限转换。 */
private[floatingpod] final class MySqlDbFirstInitialActivationConfirmation private (
    private[mysql] val core:MySqlRetainedPlanReservationCore,private[mysql] val handle:MySqlRetainedPlanHandle,
    val creationOperationUuid:UUID,val bindingUuid:UUID,val podUuid:UUID,val ownerUuid:Option[UUID],val attemptUuid:UUID,
    val capturedStateVersion:Long,val committedStateVersion:Long,val pose:DbFirstPodSnapshot.Pose):
  val tag:IoOperationTag=handle.tag
  def requireOriginal(reservation:MySqlDbFirstCaptureReservation):Unit=core.requireDbFirstInitialConfirmation(this,reservation)
private[mysql] object MySqlDbFirstInitialActivationConfirmation:
  val HolderBytes=512L
  def issue(core:MySqlRetainedPlanReservationCore,handle:MySqlRetainedPlanHandle,
      request:MySqlDbFirstInitialActivationRequest):MySqlDbFirstInitialActivationConfirmation =
    new MySqlDbFirstInitialActivationConfirmation(core,handle,request.creationOperationUuid,request.bindingUuid,request.podUuid,request.ownerUuid,
      request.attemptUuid,request.capturedStateVersion,Math.addExact(request.capturedStateVersion,1L),request.pose)
/** 只确认原准备请求已完成SQL读回与SQL退休，不授予实体放置/激活或Ready。 */
private[floatingpod] final class MySqlDbFirstInitialPreparationConfirmation private (
    private[mysql] val core:MySqlRetainedPlanReservationCore,private[mysql] val handle:MySqlRetainedPlanHandle,
    val request:MySqlControlledDebugPrepareRequest):
  val tag:IoOperationTag=handle.tag
  def requireOriginal(reservation:MySqlDbFirstInitialPreparationReservation):Unit=
    core.requireDbFirstPreparationConfirmation(this,reservation)
  def preparedData(reservation:MySqlDbFirstInitialPreparationReservation):MySqlDbFirstInitialPreparationDataView =
    core.preparationConfirmedData(this,reservation)
private[mysql] object MySqlDbFirstInitialPreparationConfirmation:
  val HolderBytes=512L
  def issue(core:MySqlRetainedPlanReservationCore,handle:MySqlRetainedPlanHandle,
      request:MySqlControlledDebugPrepareRequest):MySqlDbFirstInitialPreparationConfirmation =
    new MySqlDbFirstInitialPreparationConfirmation(core,handle,request)
/** Main真实detach后发行；只由原core登记的exact引用可退休。 */
private[floatingpod] final class MySqlDbFirstRecallDropReceipt private (
    private[mysql] val core:MySqlRetainedPlanReservationCore,private[mysql] val handle:MySqlRetainedPlanHandle,
    private[mysql] val retainedConfirmation:MySqlDbFirstRecallConfirmation,
    val removalAck:ExactBodyRemovalAcknowledgement,val ticketAck:TicketReleaseAcknowledgement):
  val tag:IoOperationTag=handle.tag
  def isRetained:Boolean=retainedConfirmation!=null
  @volatile private var retainedRetired=false
  private[floatingpod] def retainedEntryRetired:Boolean=retainedRetired
  private[mysql] def markRetainedEntryRetired():Unit =
    require(isRetained && !retainedRetired,"original retained Entry retirement marker is one-shot")
    retainedRetired=true
private[mysql] object MySqlDbFirstRecallDropReceipt:
  val HolderBytes=128L
  def issue(core:MySqlRetainedPlanReservationCore,handle:MySqlRetainedPlanHandle):MySqlDbFirstRecallDropReceipt =
    new MySqlDbFirstRecallDropReceipt(core,handle,null,null,null)
  def issueRetained(core:MySqlRetainedPlanReservationCore,handle:MySqlRetainedPlanHandle,
      confirmation:MySqlDbFirstRecallConfirmation,removal:ExactBodyRemovalAcknowledgement,
      ticket:TicketReleaseAcknowledgement):MySqlDbFirstRecallDropReceipt =
    require(confirmation!=null && confirmation.isRetained && removal!=null && ticket!=null &&
      (ticket.removal() eq removal),"actual retained confirmation and exact native ACKs required")
    new MySqlDbFirstRecallDropReceipt(core,handle,confirmation,removal,ticket)

/** Main完成初建发布并解除capture引用后发行；只由原core登记的exact引用可退休。 */
/** 已接受但确定precommit拒绝的原Main确认；不授予Prepared或成功资格。 */
private[floatingpod] final class MySqlDbFirstPreparationRejectionConfirmation private (
    private[mysql] val core:MySqlRetainedPlanReservationCore,private[mysql] val handle:MySqlRetainedPlanHandle,
    private[mysql] val observation:MySqlDbFirstSession.DbFirstInitialPreparationRejection):
  val tag:IoOperationTag=handle.tag
  def cause:Throwable=observation.cause
  def requireOriginal(reservation:MySqlDbFirstInitialPreparationReservation):Unit=
    core.requirePreparationRejectionConfirmation(this,reservation)
private[mysql] object MySqlDbFirstPreparationRejectionConfirmation:
  val HolderBytes=128L
  def issue(core:MySqlRetainedPlanReservationCore,handle:MySqlRetainedPlanHandle,
      observation:MySqlDbFirstSession.DbFirstInitialPreparationRejection):MySqlDbFirstPreparationRejectionConfirmation=
    new MySqlDbFirstPreparationRejectionConfirmation(core,handle,observation)

/** 仅原明确未接受请求的Main detach回执；不能释放Accepted/UNKNOWN。 */
private[floatingpod] final class MySqlDbFirstRejectedPreparationReceipt private[mysql] (
    private[mysql] val core:MySqlRetainedPlanReservationCore,private[mysql] val handle:MySqlRetainedPlanHandle):
  val tag:IoOperationTag=handle.tag

private[floatingpod] final class MySqlDbFirstPreparationFinalReceipt private (
    private[mysql] val core:MySqlRetainedPlanReservationCore,private[mysql] val handle:MySqlRetainedPlanHandle):
  val tag:IoOperationTag=handle.tag
private[mysql] object MySqlDbFirstPreparationFinalReceipt:
  val HolderBytes=128L
  def issue(core:MySqlRetainedPlanReservationCore,handle:MySqlRetainedPlanHandle):MySqlDbFirstPreparationFinalReceipt =
    new MySqlDbFirstPreparationFinalReceipt(core,handle)

private[floatingpod] final class MySqlDbFirstCheckpointFinalReceipt private (
    private[mysql] val core:MySqlRetainedPlanReservationCore,private[mysql] val handle:MySqlRetainedPlanHandle):
  val tag:IoOperationTag=handle.tag
private[mysql] object MySqlDbFirstCheckpointFinalReceipt:
  val HolderBytes=128L
  def issue(core:MySqlRetainedPlanReservationCore,handle:MySqlRetainedPlanHandle):MySqlDbFirstCheckpointFinalReceipt =
    new MySqlDbFirstCheckpointFinalReceipt(core,handle)

private[floatingpod] final class MySqlDbFirstInitialPublicationReceipt private (
    private[mysql] val core:MySqlRetainedPlanReservationCore,private[mysql] val handle:MySqlRetainedPlanHandle):
  val tag:IoOperationTag=handle.tag
private[mysql] object MySqlDbFirstInitialPublicationReceipt:
  val HolderBytes=128L
  def issue(core:MySqlRetainedPlanReservationCore,handle:MySqlRetainedPlanHandle):MySqlDbFirstInitialPublicationReceipt =
    new MySqlDbFirstInitialPublicationReceipt(core,handle)

/** 阶段分类仅是路由策略；真实局部资格必须由原Core/receipt另行核验。 */
private[mysql] object MySqlDbFirstRetirementFailure:
  enum Stage:
    case Unqualified, KnownSqlRetiredPreflight, RootCloseAttempted
  enum Disposition:
    case Local, Shared
  def classify(stage: Stage): Disposition = stage match
    case Stage.KnownSqlRetiredPreflight => Disposition.Local
    case _ => Disposition.Shared

private[mysql] object MySqlDbFirstLocalRetirementException:
  def fromCore(core: MySqlRetainedPlanReservationCore, receipt: MySqlDbFirstRecallDropReceipt,
      cause: Throwable): MySqlDbFirstLocalRetirementException =
    require(core != null, "actual retirement core required")
    core.requireLocalRetirementFailure(receipt, cause)
    new MySqlDbFirstLocalRetirementException(receipt, cause)

private[mysql] object MySqlDbFirstRefusalFinalLocalRetirementException:
  def fromCore(core:MySqlRetainedPlanReservationCore,receipt:MySqlDbFirstRecallRefusalFinalReceipt,
      cause:Throwable):MySqlDbFirstRefusalFinalLocalRetirementException =
    core.requireRefusalFinalLocalRetirementFailure(receipt,cause)
    new MySqlDbFirstRefusalFinalLocalRetirementException(receipt,cause)

/** 只封闭双SQL已退休且RAW未动的原Entry preflight失败；不归还槽。 */
private[mysql] final class MySqlDbFirstRefusalFinalLocalRetirementException private (
    val receipt:MySqlDbFirstRecallRefusalFinalReceipt,cause:Throwable)
    extends RuntimeException("confirmed refusal final retirement preflight failed",cause)

private[mysql] object MySqlDbFirstJournalCaptureLocalRetirementException:
  def fromCore(core:MySqlRetainedPlanReservationCore,transfer:MySqlDbFirstRecallJournalCaptureTransfer,
      cause:Throwable):MySqlDbFirstJournalCaptureLocalRetirementException =
    core.requireJournalCaptureLocalRetirementFailure(transfer,cause)
    new MySqlDbFirstJournalCaptureLocalRetirementException(transfer,cause)

/** 只封闭原J的已证实SQL退休后preflight失败；不签发C激活或槽释放资格。 */
private[mysql] final class MySqlDbFirstJournalCaptureLocalRetirementException private (
    val transfer:MySqlDbFirstRecallJournalCaptureTransfer,cause:Throwable)
    extends RuntimeException("confirmed journal/C retirement preflight failed",cause)

/** 只表示原Entry失败可封闭，不签发释放ACK，也不归还slot。 */
private[mysql] final class MySqlDbFirstLocalRetirementException private (
    val receipt: MySqlDbFirstRecallDropReceipt, cause: Throwable)
    extends RuntimeException("confirmed DB-first Entry retirement preflight failed", cause)

private[mysql] object MySqlDbFirstPreparationLocalRetirementException:
  def fromCore(core: MySqlRetainedPlanReservationCore, receipt: MySqlDbFirstPreparationFinalReceipt,
      cause: Throwable): MySqlDbFirstPreparationLocalRetirementException =
    require(core != null, "actual retirement core required")
    core.requirePreparationLocalRetirementFailure(receipt, cause)
    new MySqlDbFirstPreparationLocalRetirementException(receipt, cause)

/** 只表示原Entry失败可封闭，不签发释放ACK，也不归还slot。 */
private[mysql] final class MySqlDbFirstPreparationLocalRetirementException private (
    val receipt: MySqlDbFirstPreparationFinalReceipt, cause: Throwable)
    extends RuntimeException("confirmed DB-first Entry retirement preflight failed", cause)

private[mysql] object MySqlDbFirstCheckpointLocalRetirementException:
  def fromCore(core: MySqlRetainedPlanReservationCore, receipt: MySqlDbFirstCheckpointFinalReceipt,
      cause: Throwable): MySqlDbFirstCheckpointLocalRetirementException =
    require(core != null, "actual retirement core required")
    core.requireCheckpointLocalRetirementFailure(receipt, cause)
    new MySqlDbFirstCheckpointLocalRetirementException(receipt, cause)

/** 只表示原Entry失败可封闭，不签发释放ACK，也不归还slot。 */
private[mysql] final class MySqlDbFirstCheckpointLocalRetirementException private (
    val receipt: MySqlDbFirstCheckpointFinalReceipt, cause: Throwable)
    extends RuntimeException("confirmed DB-first Entry retirement preflight failed", cause)

private[mysql] object MySqlDbFirstInitialLocalRetirementException:
  def fromCore(core: MySqlRetainedPlanReservationCore, receipt: MySqlDbFirstInitialPublicationReceipt,
      cause: Throwable): MySqlDbFirstInitialLocalRetirementException =
    require(core != null, "actual retirement core required")
    core.requireInitialLocalRetirementFailure(receipt, cause)
    new MySqlDbFirstInitialLocalRetirementException(receipt, cause)

/** 只表示原Entry失败可封闭，不签发释放ACK，也不归还slot。 */
private[mysql] final class MySqlDbFirstInitialLocalRetirementException private (
    val receipt: MySqlDbFirstInitialPublicationReceipt, cause: Throwable)
    extends RuntimeException("confirmed DB-first Entry retirement preflight failed", cause)

/** Main实际drop边界的opaque一次receipt；不携plan/payload，不授SQL或物理资格。 */
final class MySqlRetainedPlanDropReceipt private[mysql] (
    private[mysql] val handle: MySqlRetainedPlanHandle):
  val tag: IoOperationTag = handle.tag
  override def toString: String = "MySqlRetainedPlanDropReceipt[exact owned reference]"

/** 只观察有界内存责任，不代表SQL/Boot/closed；Throwable只供受控诊断，不打印payload。 */
final case class MySqlRetainedPlanState(tag: IoOperationTag, accepted: Boolean, workerObserved: Boolean,
    dropped: Boolean, cause: Option[Throwable]):
  override def toString: String = "MySqlRetainedPlanState[bounded owned responsibility]"
final case class MySqlRetainedAllocationObservation(accountedBytes: Long,
    plans: Vector[MySqlRetainedPlanState], failure: Option[Throwable]):
  override def toString: String = "MySqlRetainedAllocationObservation[not admission or durability]"

private[mysql] enum MySqlRetainedPlanReleaseKind:
  case Ordinary, ModeProjection, DriftProjection

/** 生产唯一holder；core只是内存账，actualSession/profile/ledger资格仍在本层严格核验。 */
private[mysql] final class MySqlOwnedRetainedPlanIssuer private[mysql] (
    sessionInput: MySqlOwnedDatabaseSession, identity: MySqlOwnedIoIdentity, worker: Thread, initialMain: Thread,
    profileInput: SourceLimitProfile, ledgerInput: MySqlFormat3AllocationLedger, capacity: Int,
    deploymentInput: MySqlDeploymentReceiptSession = null,
    deploymentInputsInput: Option[MySqlScriptSchemaInputs] = None,
    dbFirstInput: MySqlDbFirstSession = null,
    dbFirstInputsInput: Option[MySqlScriptSchemaInputs] = None):
  private enum SessionOwner:
    case Legacy(session:MySqlOwnedDatabaseSession)
    case DeploymentReceipt(session:MySqlDeploymentReceiptSession)
    case DbFirst(session:MySqlDbFirstSession)
  require(Seq(sessionInput, deploymentInput, dbFirstInput).count(_ != null) == 1,"one exact typed retained issuer Session owner required")
  private var sessionOwner:SessionOwner =
    if sessionInput!=null then SessionOwner.Legacy(sessionInput)
    else if deploymentInput!=null then SessionOwner.DeploymentReceipt(deploymentInput)
    else SessionOwner.DbFirst(dbFirstInput)
  private var dbFirstInputs = dbFirstInputsInput
  private var profile = profileInput
  private var ledger = ledgerInput
  require(profile != null && ledger != null, "actual session/profile/shared ledger required")
  require(deploymentInputsInput!=null && !deploymentInputsInput.exists(_==null) &&
    (deploymentInput!=null)==deploymentInputsInput.nonEmpty,"exact retained issuer family inputs required")
  require(dbFirstInputsInput != null && !dbFirstInputsInput.exists(_ == null) &&
    (dbFirstInput != null) == dbFirstInputsInput.nonEmpty, "exact schema6 retained issuer inputs required")
  private var core: MySqlRetainedPlanReservationCore = new MySqlRetainedPlanReservationCore(identity, worker, initialMain,
    profile, ledger, capacity, Some(this),deploymentInputsInput,dbFirstInputsInput)
  // 私有core为空只表示原图已实际撤销；不能用它发行closed/native成功。
  private def liveCore: MySqlRetainedPlanReservationCore =
    val actual = core
    if actual == null then throw new IllegalStateException("retained issuer graph was retired")
    actual
  private[mysql] def requireSession(session: MySqlOwnedDatabaseSession,
      actualProfile: SourceLimitProfile, actualLedger: MySqlFormat3AllocationLedger): Unit = synchronized {
    liveCore
    require(Thread.currentThread() eq worker, "session reservation requires actual owned worker")
    val exact=sessionOwner match
      case SessionOwner.Legacy(value)=>session eq value
      case _=>false
    require(exact && (actualProfile eq profile) && (actualLedger eq ledger),
      "retained issuer differs from actual session/profile/shared ledger")
  }
  private[mysql] def requireDeploymentReceiptSession(session:MySqlDeploymentReceiptSession,
      actualProfile:SourceLimitProfile,actualLedger:MySqlFormat3AllocationLedger):Unit = synchronized {
    liveCore
    require(Thread.currentThread() eq worker,"schema5 reservation requires actual owned worker")
    val exact=sessionOwner match
      case SessionOwner.DeploymentReceipt(value)=>session eq value
      case _=>false
    require(exact && (actualProfile eq profile) && (actualLedger eq ledger),"retained issuer differs from actual schema5 Session/profile/shared ledger")
  }
  private[mysql] def requireDbFirstSession(session: MySqlDbFirstSession,
      actualInputs: MySqlScriptSchemaInputs, actualProfile: SourceLimitProfile,
      actualLedger: MySqlFormat3AllocationLedger): Unit = synchronized {
    liveCore
    require(Thread.currentThread() eq worker, "schema6 retained owner requires actual owned worker")
    val exact = sessionOwner match
      case SessionOwner.DbFirst(value) => session eq value
      case _ => false
    require(exact && dbFirstInputs.exists(_ eq actualInputs) &&
      (actualProfile eq profile) && (actualLedger eq ledger),
      "retained issuer differs from actual schema6 Session/inputs/profile/shared ledger")
  }
  private[mysql] def requireRecoveryCore(actual:MySqlRetainedPlanReservationCore):Unit = synchronized {
    require(actual!=null && (actual eq liveCore),"actual privately registered retained core required")
  }
  private[mysql] def transferDbFirstServiceOwner(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      adapter:MySqlOwnedIoAdapter,previousSession:MySqlDbFirstSession,nextSession:MySqlDbFirstSession,
      actualInputs:MySqlScriptSchemaInputs,actualProfile:SourceLimitProfile,
      actualLedger:MySqlFormat3AllocationLedger,previous:MySqlOwnedIoIdentity,next:MySqlOwnedIoIdentity):Boolean = synchronized {
    requireDbFirstSession(previousSession,actualInputs,actualProfile,actualLedger)
    require(source!=null && adapter!=null && nextSession!=null,"actual service recovery owner/adapter/Session required")
    source.requireServiceIssuerTransfer(adapter,this,liveCore,previous,next)
    val nextOwner=SessionOwner.DbFirst(nextSession) // 在core发生任何改变之前保留新的typed owner。
    if liveCore.transferDbFirstServiceIdentity(source,adapter,this,previous,next) then
      sessionOwner=nextOwner
      true
    else false
  }
  private[mysql] def detachDbFirstReferencesForExit(session: MySqlDbFirstSession,
      actualInputs: MySqlScriptSchemaInputs, actualProfile: SourceLimitProfile,
      actualLedger: MySqlFormat3AllocationLedger): Unit = synchronized {
    requireDbFirstSession(session, actualInputs, actualProfile, actualLedger)
    liveCore.stop()
    liveCore.requireStoppedEmptyForTerminal()
    liveCore.retireSettledMemoryGraph()
    core=null;sessionOwner=null;profile=null;ledger=null;dbFirstInputs=None
  }
  private[mysql] def detachDeploymentReceiptReferencesForExit(session:MySqlDeploymentReceiptSession,
      actualProfile:SourceLimitProfile,actualLedger:MySqlFormat3AllocationLedger):Unit = synchronized {
    requireDeploymentReceiptSession(session,actualProfile,actualLedger)
    liveCore.requireStoppedEmptyForTerminal()
    liveCore.retireSettledMemoryGraph()
    core=null;sessionOwner=null;profile=null;ledger=null
  }
  private[mysql] def beginBootRecallTakeover(session:MySqlDbFirstSession,intent:MySqlDbFirstBootRecallTakeoverIntent,
      inputs:MySqlScriptSchemaInputs):MySqlDbFirstBootRecallTakeoverWork = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger);liveCore.beginBootRecallTakeover(intent)
  }
  private[mysql] def finishBootRecallTakeover(work:MySqlDbFirstBootRecallTakeoverWork):MySqlDbFirstCaptureReservation =
    synchronized { liveCore.finishBootRecallTakeover(work) }
  private[mysql] def failBootRecallTakeover(intent:MySqlDbFirstBootRecallTakeoverIntent,cause:Throwable):Unit =
    synchronized { liveCore.failBootRecallTakeover(intent,cause) }
  private[mysql] def activateBootRecallCapture(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      decision:MySqlDbFirstBootRecallMainDecision,reservation:MySqlDbFirstCaptureReservation):Unit =
    liveCore.activateBootRecallCapture(owner,decision,reservation)
  private[mysql] def requireBootRecallTakeoverResult(intent:MySqlDbFirstBootRecallTakeoverIntent,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized { liveCore.requireBootRecallTakeoverResult(intent,reservation) }
  private[mysql] def beginBootRecallMainDecision(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    liveCore.beginBootRecallMainDecision(owner,intent,reservation)
  }
  private[mysql] def finishBootRecallMainDecision(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation,
      reason:Option[RecallRefusalReason]):MySqlDbFirstBootRecallMainDecision = synchronized {
    liveCore.finishBootRecallMainDecision(owner,intent,reservation,reason)
  }
  def reserveDbFirstCapture(tag: IoOperationTag,
      purpose: DbFirstPodSnapshot.Purpose = DbFirstPodSnapshot.Purpose.RECALL): MySqlDbFirstCaptureReservation =
    synchronized { liveCore.reserveDbFirstCapture(tag, purpose) }
  def retainRecoveredInitialData(reservation:MySqlDbFirstCaptureReservation,
      data:MySqlDbFirstInitialPreparationDataView):MySqlDbFirstInitialPreparationDataView =
    synchronized { liveCore.retainRecoveredInitialData(reservation,data) }
  private[mysql] def beginDbFirstRecallRefusal(session:MySqlDbFirstSession,
      exclusion:MySqlDbFirstRecallRefusalSource,inputs:MySqlScriptSchemaInputs):MySqlDbFirstRecallRefusalWork = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.beginRecallRefusal(exclusion)
  }
  private[mysql] def requireExecutingDbFirstRecallRefusal(session:MySqlDbFirstSession,
      work:MySqlDbFirstRecallRefusalWork,inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.requireExecutingRecallRefusal(work)
  }
  def excludeRecallBeforeCapture(reservation:MySqlDbFirstRecallJournalReservation,cause:Throwable,
      reason:Option[RecallRefusalReason]=None):MySqlDbFirstRecallPreCaptureExclusion = synchronized {
    liveCore.excludeRecallBeforeCapture(reservation,cause,reason)
  }
  def requireRecallRefusalSubmission(exclusion:MySqlDbFirstRecallRefusalSource):Unit = synchronized { liveCore.requireRecallRefusalSubmission(exclusion) }
  def submittedRecallRefusal(exclusion:MySqlDbFirstRecallRefusalSource,result:IoSubmission):Unit = synchronized { liveCore.submittedRecallRefusal(exclusion,result) }
  def submissionUnknownRecallRefusal(exclusion:MySqlDbFirstRecallRefusalSource,cause:Throwable):Unit = synchronized { liveCore.submissionUnknownRecallRefusal(exclusion,cause) }
  def claimRecallRefusalConfirmation(exclusion:MySqlDbFirstRecallRefusalSource,
      observation:MySqlDbFirstSession.DbFirstRecallRefusalObservation):MySqlDbFirstRecallRefusalConfirmation = synchronized {
    liveCore.claimRecallRefusalConfirmation(exclusion,observation)
  }
  private[mysql] def requireRecallRefusalConfirmation(confirmation:MySqlDbFirstRecallRefusalConfirmation,
      exclusion:MySqlDbFirstRecallRefusalSource):Unit = synchronized { liveCore.requireRecallRefusalConfirmation(confirmation,exclusion) }
  private[mysql] def beginRefusalReadRelease(session:MySqlDbFirstSession,confirmation:MySqlDbFirstRecallRefusalConfirmation,
      inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger);liveCore.beginRefusalReadRelease(confirmation)
  }
  private[mysql] def finishRefusalReadRelease(confirmation:MySqlDbFirstRecallRefusalConfirmation):Unit = synchronized { liveCore.finishRefusalReadRelease(confirmation) }
  private[mysql] def requireRefusalReadReleased(confirmation:MySqlDbFirstRecallRefusalConfirmation,
      exclusion:MySqlDbFirstRecallRefusalSource):Unit = synchronized { liveCore.requireRefusalReadReleased(confirmation,exclusion) }
  private[mysql] def acknowledgeRefusalReadReleased(confirmation:MySqlDbFirstRecallRefusalConfirmation,
      exclusion:MySqlDbFirstRecallRefusalSource):Unit = synchronized { liveCore.acknowledgeRefusalReadReleased(confirmation,exclusion) }
  def reserveDbFirstRecallJournal(tag:IoOperationTag,original:IoOperationTag,binding:UUID,actor:UUID,pod:UUID,
      version:Long,client:Option[UUID]):MySqlDbFirstRecallJournalReservation = synchronized {
    liveCore.reserveDbFirstRecallJournal(tag,original,binding,actor,pod,version,client)
  }
  def requireDbFirstRecallJournalSubmission(reservation:MySqlDbFirstRecallJournalReservation):Unit = synchronized {
    liveCore.requireDbFirstRecallJournalSubmission(reservation)
  }
  def submittedDbFirstRecallJournal(reservation:MySqlDbFirstRecallJournalReservation,result:IoSubmission):Unit = synchronized {
    liveCore.submittedDbFirstRecallJournal(reservation,result)
  }
  def submissionUnknownDbFirstRecallJournal(reservation:MySqlDbFirstRecallJournalReservation,cause:Throwable):Unit = synchronized {
    liveCore.submissionUnknownDbFirstRecallJournal(reservation,cause)
  }
  def recordRejectedJournalDrop(reservation:MySqlDbFirstRecallJournalReservation):MySqlDbFirstRejectedJournalReceipt = synchronized {
    liveCore.recordRejectedJournalDrop(reservation)
  }
  def cancelRejectedJournal(receipt:MySqlDbFirstRejectedJournalReceipt):Unit = synchronized {
    liveCore.cancelRejectedJournal(receipt)
  }
  private[mysql] def beginDbFirstRecallJournal(session:MySqlDbFirstSession,
      reservation:MySqlDbFirstRecallJournalReservation,inputs:MySqlScriptSchemaInputs):MySqlDbFirstRecallJournalWork = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.beginDbFirstRecallJournal(reservation)
  }
  private[mysql] def requireExecutingDbFirstRecallJournal(session:MySqlDbFirstSession,
      work:MySqlDbFirstRecallJournalWork,inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.requireExecutingDbFirstRecallJournal(work)
  }
  def claimDbFirstRecallJournalConfirmation(reservation:MySqlDbFirstRecallJournalReservation,
      observation:MySqlDbFirstSession.DbFirstRecallJournalObservation):MySqlDbFirstRecallJournalConfirmation = synchronized {
    liveCore.claimDbFirstRecallJournalConfirmation(reservation,observation)
  }
  private[mysql] def beginJournalReadRelease(session:MySqlDbFirstSession,
      confirmation:MySqlDbFirstRecallJournalConfirmation,inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.beginJournalReadRelease(confirmation)
  }
  private[mysql] def finishJournalReadRelease(confirmation:MySqlDbFirstRecallJournalConfirmation):Unit = synchronized {
    liveCore.finishJournalReadRelease(confirmation)
  }
  private[mysql] def requireJournalReadReleased(confirmation:MySqlDbFirstRecallJournalConfirmation,
      reservation:MySqlDbFirstRecallJournalReservation):Unit = synchronized {
    liveCore.requireJournalReadReleased(confirmation,reservation)
  }
  private[mysql] def acknowledgeJournalReadReleased(confirmation:MySqlDbFirstRecallJournalConfirmation,
      reservation:MySqlDbFirstRecallJournalReservation):Unit = synchronized {
    liveCore.acknowledgeJournalReadReleased(confirmation,reservation)
  }
  private[mysql] def recordRefusalFinal(confirmation:MySqlDbFirstRecallRefusalConfirmation,
      exclusion:MySqlDbFirstRecallRefusalSource):MySqlDbFirstRecallRefusalFinalReceipt = synchronized {
    liveCore.recordRefusalFinal(confirmation,exclusion)
  }
  private[mysql] def requireRefusalFinalSubmission(receipt:MySqlDbFirstRecallRefusalFinalReceipt):Unit = synchronized {
    liveCore.requireRefusalFinalSubmission(receipt)
  }
  private[mysql] def submittedRefusalFinal(receipt:MySqlDbFirstRecallRefusalFinalReceipt,result:IoSubmission):Unit = synchronized {
    liveCore.submittedRefusalFinal(receipt,result)
  }
  private[mysql] def submissionUnknownRefusalFinal(receipt:MySqlDbFirstRecallRefusalFinalReceipt,cause:Throwable):Unit = synchronized {
    liveCore.submissionUnknownRefusalFinal(receipt,cause)
  }
  private[mysql] def retireRefusalFinal(session:MySqlDbFirstSession,receipt:MySqlDbFirstRecallRefusalFinalReceipt,
      inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger);liveCore.retireRefusalFinal(receipt)
  }
  private[mysql] def failRefusalFinal(receipt:MySqlDbFirstRecallRefusalFinalReceipt,cause:Throwable):Throwable = synchronized {
    liveCore.failRefusalFinal(receipt,cause)
  }
  private[mysql] def requireRefusalFinalRetired(receipt:MySqlDbFirstRecallRefusalFinalReceipt):Unit = synchronized {
    liveCore.requireRefusalFinalRetired(receipt)
  }
  private[mysql] def acknowledgeRefusalFinal(receipt:MySqlDbFirstRecallRefusalFinalReceipt):Unit = synchronized {
    liveCore.acknowledgeRefusalFinal(receipt)
  }

  private[mysql] def prepareJournalCaptureTransfer(reservation:MySqlDbFirstRecallJournalReservation,
      confirmation:MySqlDbFirstRecallJournalConfirmation):MySqlDbFirstRecallJournalCaptureTransfer = synchronized {
    liveCore.prepareJournalCaptureTransfer(reservation,confirmation)
  }
  private[mysql] def retireJournalCaptureTransfer(session:MySqlDbFirstSession,
      transfer:MySqlDbFirstRecallJournalCaptureTransfer,inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.retireJournalCaptureTransfer(transfer)
  }
  private[mysql] def publishJournalCaptureTransfer(transfer:MySqlDbFirstRecallJournalCaptureTransfer):Unit = synchronized {
    liveCore.publishJournalCaptureTransfer(transfer)
  }
  private[mysql] def requireJournalCaptureTransferSubmission(t:MySqlDbFirstRecallJournalCaptureTransfer):Unit = synchronized {
    liveCore.requireJournalCaptureTransferSubmission(t)
  }
  private[mysql] def submittedJournalCaptureTransfer(t:MySqlDbFirstRecallJournalCaptureTransfer,result:IoSubmission):Unit = synchronized {
    liveCore.submittedJournalCaptureTransfer(t,result)
  }
  private[mysql] def submissionUnknownJournalCaptureTransfer(t:MySqlDbFirstRecallJournalCaptureTransfer,cause:Throwable):Unit = synchronized {
    liveCore.submissionUnknownJournalCaptureTransfer(t,cause)
  }
  private[mysql] def requireJournalCaptureTransferRetired(t:MySqlDbFirstRecallJournalCaptureTransfer):Unit = synchronized {
    liveCore.requireJournalCaptureTransferRetired(t)
  }
  private[mysql] def journalCaptureTransferFor(r:MySqlDbFirstCaptureReservation):Option[MySqlDbFirstRecallJournalCaptureTransfer] = synchronized {
    liveCore.journalCaptureTransferFor(r)
  }
  private[mysql] def classifyJournalCaptureTransferFailure(t:MySqlDbFirstRecallJournalCaptureTransfer,cause:Throwable):Throwable = synchronized {
    liveCore.classifyJournalCaptureTransferFailure(t,cause)
  }
  private[mysql] def activateJournalCaptureTransfer(transfer:MySqlDbFirstRecallJournalCaptureTransfer):MySqlDbFirstCaptureReservation = synchronized {
    liveCore.activateJournalCaptureTransfer(transfer)
  }
  private[mysql] def failJournalCaptureTransfer(transfer:MySqlDbFirstRecallJournalCaptureTransfer,cause:Throwable):Unit = synchronized {
    liveCore.failJournalCaptureTransfer(transfer,cause)
  }
  def recordRejectedPreparationDrop(reservation:MySqlDbFirstInitialPreparationReservation):MySqlDbFirstRejectedPreparationReceipt = synchronized {
    liveCore.recordRejectedPreparationDrop(reservation)
  }
  def cancelRejectedPreparation(receipt:MySqlDbFirstRejectedPreparationReceipt):Unit = synchronized {
    liveCore.cancelRejectedPreparation(receipt)
  }
  def reserveDbFirstInitialPreparation(tag:IoOperationTag,request:MySqlControlledDebugPrepareRequest,
      activeLimit:Int):MySqlDbFirstInitialPreparationReservation = synchronized {
    liveCore.reserveDbFirstInitialPreparation(tag,request,activeLimit)
  }
  def requireDbFirstInitialPreparationSubmission(reservation:MySqlDbFirstInitialPreparationReservation):Unit = synchronized {
    liveCore.requireDbFirstInitialPreparationSubmission(reservation)
  }
  def submittedDbFirstInitialPreparation(reservation:MySqlDbFirstInitialPreparationReservation,result:IoSubmission):Unit = synchronized {
    liveCore.submittedDbFirstInitialPreparation(reservation,result)
  }
  def submissionUnknownDbFirstInitialPreparation(reservation:MySqlDbFirstInitialPreparationReservation,cause:Throwable):Unit = synchronized {
    liveCore.submissionUnknownDbFirstInitialPreparation(reservation,cause)
  }
  private[mysql] def beginDbFirstInitialPreparation(session:MySqlDbFirstSession,
      reservation:MySqlDbFirstInitialPreparationReservation,inputs:MySqlScriptSchemaInputs):MySqlDbFirstInitialPreparationWork = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.beginDbFirstInitialPreparation(reservation)
  }
  private[mysql] def requireExecutingDbFirstInitialPreparation(session:MySqlDbFirstSession,
      work:MySqlDbFirstInitialPreparationWork,inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.requireExecutingDbFirstInitialPreparation(work)
  }
  private[mysql] def beginCheckpointReadRelease(session:MySqlDbFirstSession,
      confirmation:MySqlDbFirstCheckpointConfirmation,inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.beginCheckpointReadRelease(confirmation)
  }
  private[mysql] def finishCheckpointReadRelease(confirmation:MySqlDbFirstCheckpointConfirmation):Unit = synchronized {
    liveCore.finishCheckpointReadRelease(confirmation)
  }
  private[mysql] def requireCheckpointReadReleased(confirmation:MySqlDbFirstCheckpointConfirmation,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    liveCore.requireCheckpointReadReleased(confirmation,reservation)
  }

  private[mysql] def acknowledgeCheckpointReadReleased(confirmation:MySqlDbFirstCheckpointConfirmation,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    liveCore.acknowledgeCheckpointReadReleased(confirmation,reservation)
  }
  private[mysql] def beginPreparationReadRelease(session:MySqlDbFirstSession,
      confirmation:MySqlDbFirstInitialPreparationConfirmation,inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.beginPreparationReadRelease(confirmation)
  }
  private[mysql] def finishPreparationReadRelease(confirmation:MySqlDbFirstInitialPreparationConfirmation):Unit = synchronized {
    liveCore.finishPreparationReadRelease(confirmation)
  }
  private[mysql] def requirePreparationReadReleased(confirmation:MySqlDbFirstInitialPreparationConfirmation,
      reservation:MySqlDbFirstInitialPreparationReservation):Unit = synchronized {
    liveCore.requirePreparationReadReleased(confirmation,reservation)
  }

  def claimDbFirstInitialPreparationConfirmation(reservation:MySqlDbFirstInitialPreparationReservation,
      observation:MySqlDbFirstSession.DbFirstInitialPreparationObservation):MySqlDbFirstInitialPreparationConfirmation = synchronized {
    liveCore.claimDbFirstInitialPreparationConfirmation(reservation,observation)
  }
  def claimPreparationRejection(reservation:MySqlDbFirstInitialPreparationReservation,
      observation:MySqlDbFirstSession.DbFirstInitialPreparationRejection):MySqlDbFirstPreparationRejectionConfirmation = synchronized {
    liveCore.claimPreparationRejection(reservation,observation)
  }
  private[mysql] def beginRejectedPreparationReadRelease(session:MySqlDbFirstSession,
      confirmation:MySqlDbFirstPreparationRejectionConfirmation,inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.beginRejectedPreparationReadRelease(confirmation)
  }
  private[mysql] def finishRejectedPreparationReadRelease(confirmation:MySqlDbFirstPreparationRejectionConfirmation):Unit = synchronized {
    liveCore.finishRejectedPreparationReadRelease(confirmation)
  }
  private[mysql] def requireRejectedPreparationReadReleased(confirmation:MySqlDbFirstPreparationRejectionConfirmation,
      reservation:MySqlDbFirstInitialPreparationReservation):Unit = synchronized {
    liveCore.requireRejectedPreparationReadReleased(confirmation,reservation)
  }
  private[floatingpod] def detachRejectedPreparation(confirmation:MySqlDbFirstPreparationRejectionConfirmation):Unit = synchronized {
    liveCore.detachRejectedPreparation(confirmation)
  }
  private[mysql] def retireRejectedPreparation(session:MySqlDbFirstSession,
      confirmation:MySqlDbFirstPreparationRejectionConfirmation,inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.retireRejectedPreparation(confirmation)
  }
  private[mysql] def failDbFirstInitialPreparation(work:MySqlDbFirstInitialPreparationWork,cause:Throwable):Unit = synchronized {
    liveCore.failDbFirstInitialPreparation(work,cause)
  }
  def requireDbFirstCaptureReservation(reservation: MySqlDbFirstCaptureReservation): Unit =
    synchronized { liveCore.requireDbFirstCaptureReservation(reservation) }
  def captureDbFirstSnapshot(reservation: MySqlDbFirstCaptureReservation)
      (capture: DbFirstCaptureAllocation => DbFirstPodSnapshot): DbFirstPodSnapshot =
    synchronized { liveCore.captureDbFirstSnapshot(reservation)(capture) }
  def dbFirstInitialActivationRequest(reservation: MySqlDbFirstCaptureReservation,
      snapshot: DbFirstPodSnapshot): MySqlDbFirstInitialActivationRequest =
    synchronized { liveCore.dbFirstInitialActivationRequest(reservation,snapshot) }
  def dbFirstRecallRequest(reservation: MySqlDbFirstCaptureReservation, snapshot: DbFirstPodSnapshot): MySqlDbFirstRecallRequest =
    synchronized { liveCore.dbFirstRecallRequest(reservation,snapshot) }
  def dbFirstCheckpointRequest(reservation: MySqlDbFirstCaptureReservation, snapshot: DbFirstPodSnapshot): MySqlDbFirstCheckpointRequest =
    synchronized { liveCore.dbFirstCheckpointRequest(reservation,snapshot) }

  private[mysql] def materializeDbFirstRecall(reservation: MySqlDbFirstCaptureReservation,
      request: MySqlDbFirstRecallRequest, before: MySqlScriptSchemaSnapshot, connectionId: Long,
      barrierGeneration: UUID, plannedAt: LocalDateTime, resultAt: LocalDateTime): MySqlDbFirstRecallMaterialization = synchronized {
    liveCore.materializeDbFirstRecall(reservation,request,before,connectionId,barrierGeneration,plannedAt,resultAt)
  }
  private[mysql] def materializeDbFirstCheckpoint(reservation: MySqlDbFirstCaptureReservation,
      request: MySqlDbFirstCheckpointRequest, before: MySqlScriptSchemaSnapshot, connectionId: Long,
      barrierGeneration: UUID, plannedAt: LocalDateTime, resultAt: LocalDateTime): MySqlDbFirstRecallMaterialization = synchronized {
    liveCore.materializeDbFirstCheckpoint(reservation,request,before,connectionId,barrierGeneration,plannedAt,resultAt)
  }
  def claimDbFirstRecallConfirmation(reservation:MySqlDbFirstCaptureReservation,
      observation:MySqlDbFirstSession.DbFirstRecallObservation):MySqlDbFirstRecallConfirmation = synchronized {
    liveCore.claimDbFirstRecallConfirmation(reservation,observation)
  }
  def claimRetainedDbFirstRecallConfirmation(reservation:MySqlDbFirstCaptureReservation,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):MySqlDbFirstRecallConfirmation = synchronized {
    liveCore.claimRetainedDbFirstRecallConfirmation(reservation,qualification)
  }
  def claimDbFirstCheckpointConfirmation(reservation:MySqlDbFirstCaptureReservation,
      observation:MySqlDbFirstSession.DbFirstCheckpointObservation):MySqlDbFirstCheckpointConfirmation = synchronized {
    liveCore.claimDbFirstCheckpointConfirmation(reservation,observation)
  }
  def claimDbFirstInitialActivationConfirmation(reservation:MySqlDbFirstCaptureReservation,
      observation:MySqlDbFirstSession.DbFirstInitialActivationObservation):MySqlDbFirstInitialActivationConfirmation = synchronized {
    liveCore.claimDbFirstInitialActivationConfirmation(reservation,observation)
  }
  def recordDbFirstMainDrop(confirmation:MySqlDbFirstRecallConfirmation):MySqlDbFirstRecallDropReceipt = synchronized {
    liveCore.recordDbFirstMainDrop(confirmation)
  }
  def recordRetainedDbFirstMainDrop(confirmation:MySqlDbFirstRecallConfirmation,
      removal:ExactBodyRemovalAcknowledgement,ticket:TicketReleaseAcknowledgement):MySqlDbFirstRecallDropReceipt = synchronized {
    liveCore.recordRetainedDbFirstMainDrop(confirmation,removal,ticket)
  }
  def handoffRetainedDbFirstMainDrop(receipt:MySqlDbFirstRecallDropReceipt,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):Unit = synchronized {
    liveCore.handoffRetainedDbFirstMainDrop(receipt,qualification)
  }
  private[mysql] def requireDbFirstDrop(session:MySqlDbFirstSession,receipt:MySqlDbFirstRecallDropReceipt,
      inputs:MySqlScriptSchemaInputs):MySqlDbFirstRecallTransaction = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.requireDbFirstDrop(receipt)
  }
  /** 原失败路径的退休预检；实际Session/barrier检查由调用者在monitor外完成。 */
  private[mysql] def requireRetainedDbFirstDrop(session:MySqlDbFirstSession,
      receipt:MySqlDbFirstRecallDropReceipt,qualification:MySqlDbFirstSession.FailedRecallCleanupQualification,
      inputs:MySqlScriptSchemaInputs):MySqlDbFirstRecallTransaction = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    require(receipt!=null && receipt.isRetained && qualification!=null,"original retained native drop/qualification required")
    qualification.requireSession(session)
    liveCore.requireRetainedDbFirstDrop(receipt,qualification)
  }
  private[mysql] def retireDbFirstDrop(receipt:MySqlDbFirstRecallDropReceipt):Unit = synchronized {
    liveCore.retireDbFirstDrop(receipt)
  }
  private[mysql] def beginRetainedDbFirstDrop(session:MySqlDbFirstSession,receipt:MySqlDbFirstRecallDropReceipt,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification,inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.beginRetainedDbFirstDrop(receipt,qualification)
  }
  private[mysql] def detachRetainedDbFirstPayload(session:MySqlDbFirstSession,receipt:MySqlDbFirstRecallDropReceipt,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification,inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.detachRetainedDbFirstPayload(receipt,qualification)
  }
  private[mysql] def retireRetainedDbFirstDrop(session:MySqlDbFirstSession,receipt:MySqlDbFirstRecallDropReceipt,
      inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.retireRetainedDbFirstDrop(receipt)
  }
  def requireRetainedDbFirstReleaseSubmission(receipt:MySqlDbFirstRecallDropReceipt,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):Unit = synchronized {
    liveCore.requireRetainedDbFirstReleaseSubmission(receipt,qualification)
  }
  private[mysql] def failDbFirstDrop(receipt:MySqlDbFirstRecallDropReceipt,cause:Throwable):Throwable = synchronized {
    liveCore.failDbFirstDrop(receipt,cause)
  }
  def recordDbFirstPreparationFinal(confirmation:MySqlDbFirstInitialPreparationConfirmation):MySqlDbFirstPreparationFinalReceipt = synchronized {
    liveCore.recordDbFirstPreparationFinal(confirmation)
  }
  private[mysql] def requireDbFirstPreparationFinal(session:MySqlDbFirstSession,receipt:MySqlDbFirstPreparationFinalReceipt,
      inputs:MySqlScriptSchemaInputs):MySqlDbFirstInitialPreparationTransaction = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.requireDbFirstPreparationFinal(receipt)
  }
  private[mysql] def retireDbFirstPreparationFinal(receipt:MySqlDbFirstPreparationFinalReceipt):Unit = synchronized {
    liveCore.retireDbFirstPreparationFinal(receipt)
  }
  private[mysql] def failDbFirstPreparationFinal(receipt:MySqlDbFirstPreparationFinalReceipt,cause:Throwable):Throwable = synchronized {
    liveCore.failDbFirstPreparationFinal(receipt,cause)
  }
  def recordDbFirstCheckpointFinal(confirmation:MySqlDbFirstCheckpointConfirmation):MySqlDbFirstCheckpointFinalReceipt = synchronized {
    liveCore.recordDbFirstCheckpointFinal(confirmation)
  }
  private[mysql] def requireDbFirstCheckpointFinal(session:MySqlDbFirstSession,receipt:MySqlDbFirstCheckpointFinalReceipt,
      inputs:MySqlScriptSchemaInputs):MySqlDbFirstCheckpointTransaction = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.requireDbFirstCheckpointFinal(receipt)
  }
  private[mysql] def retireDbFirstCheckpointFinal(receipt:MySqlDbFirstCheckpointFinalReceipt):Unit = synchronized {
    liveCore.retireDbFirstCheckpointFinal(receipt)
  }
  private[mysql] def failDbFirstCheckpointFinal(receipt:MySqlDbFirstCheckpointFinalReceipt,cause:Throwable):Throwable = synchronized {
    liveCore.failDbFirstCheckpointFinal(receipt,cause)
  }
  def recordDbFirstInitialPublication(confirmation:MySqlDbFirstInitialActivationConfirmation):MySqlDbFirstInitialPublicationReceipt = synchronized {
    liveCore.recordDbFirstInitialPublication(confirmation)
  }
  private[mysql] def requireDbFirstInitialPublication(session:MySqlDbFirstSession,receipt:MySqlDbFirstInitialPublicationReceipt,
      inputs:MySqlScriptSchemaInputs):MySqlDbFirstInitialActivationTransaction = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.requireDbFirstInitialPublication(receipt)
  }
  private[mysql] def retireDbFirstInitialPublication(receipt:MySqlDbFirstInitialPublicationReceipt):Unit = synchronized {
    liveCore.retireDbFirstInitialPublication(receipt)
  }
  private[mysql] def failDbFirstInitialPublication(receipt:MySqlDbFirstInitialPublicationReceipt,cause:Throwable):Throwable = synchronized {
    liveCore.failDbFirstInitialPublication(receipt,cause)
  }
  def requireDbFirstRecallSubmission(reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    liveCore.requireDbFirstRecallSubmission(reservation)
  }
  def submittedDbFirstRecall(reservation:MySqlDbFirstCaptureReservation,result:IoSubmission):Unit = synchronized {
    liveCore.submittedDbFirstRecall(reservation,result)
  }
  def submissionUnknownDbFirstRecall(reservation:MySqlDbFirstCaptureReservation,cause:Throwable):Unit = synchronized {
    liveCore.submissionUnknownDbFirstRecall(reservation,cause)
  }
  private[mysql] def beginDbFirstRecall(session:MySqlDbFirstSession,reservation:MySqlDbFirstCaptureReservation,
      inputs:MySqlScriptSchemaInputs):MySqlDbFirstRecallTransaction = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.beginDbFirstRecall(reservation)
  }
  private[mysql] def requireExecutingDbFirstRecall(session:MySqlDbFirstSession,runner:MySqlDbFirstRecallTransaction,
      inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.requireExecutingDbFirstRecall(runner)
  }
  private[mysql] def requireFailedDbFirstRecallObservation(session:MySqlDbFirstSession,
      runner:MySqlDbFirstRecallTransaction,inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.requireFailedDbFirstRecallObservation(runner)
  }
  private[mysql] def settleFailedDbFirstRecallReadback(session:MySqlDbFirstSession,
      runner:MySqlDbFirstRecallTransaction,observed:MySqlDbFirstSession.FailedRecallReadback,
      inputs:MySqlScriptSchemaInputs):Unit = {
    // 实际JDBC/barrier与独立DATA检查必须在issuer/core monitor之外。
    requireDbFirstSession(session,inputs,profile,ledger)
    session.requireFailedRecallSqlSettlement(runner,observed)
    synchronized { liveCore.settleFailedDbFirstRecallReadback(runner,observed) }
  }
  /** 真实Session在monitor外重核barrier/audit/lease，core只登记opaque原资格。 */
  private[mysql] def recordFailedDbFirstRecallQualification(session:MySqlDbFirstSession,
      runner:MySqlDbFirstRecallTransaction,qualification:MySqlDbFirstSession.FailedRecallCleanupQualification,
      inputs:MySqlScriptSchemaInputs):Unit =
    requireDbFirstSession(session,inputs,profile,ledger)
    session.requireFailedRecallCleanupQualification(runner,qualification)
    synchronized { liveCore.recordFailedDbFirstRecallQualification(session,runner,qualification) }

  private[mysql] def requireRecordedFailedRecallQualification(runner:MySqlDbFirstRecallTransaction,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):Unit = synchronized {
    liveCore.requireRecordedFailedRecallQualification(runner,qualification)
  }
  def retainedDbFirstRecallFailure(reservation:MySqlDbFirstCaptureReservation,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):Throwable = synchronized {
    liveCore.retainedDbFirstRecallFailure(reservation,qualification)
  }

  /** 原Entry的SQL结算DATA；不能用作业务confirmation或新writer准入。 */
  def failedDbFirstRecallSqlReadback(reservation:MySqlDbFirstCaptureReservation)
      :Option[MySqlDbFirstSession.FailedRecallReadback] = synchronized {
    liveCore.failedDbFirstRecallSqlReadback(reservation)
  }
  private[mysql] def detachFailedDbFirstRecallDraft(session:MySqlDbFirstSession,
      runner:MySqlDbFirstRecallTransaction,observed:MySqlDbFirstSession.FailedRecallReadback,
      inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.detachFailedDbFirstRecallDraft(runner,observed)
  }
  private[mysql] def detachConfirmedDbFirstSnapshotDraft(session:MySqlDbFirstSession,
      runner:MySqlDbFirstSnapshotTransaction[?,?],inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.detachConfirmedDbFirstSnapshotDraft(runner)
  }
  private[mysql] def detachConfirmedDbFirstInitialDraft(session:MySqlDbFirstSession,
      runner:MySqlDbFirstInitialActivationTransaction,inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.detachConfirmedDbFirstInitialDraft(runner)
  }
  private[mysql] def materializeExecutingDbFirstRecall(runner:MySqlDbFirstRecallTransaction,
      before:MySqlScriptSchemaSnapshot,connectionId:Long,barrierGeneration:UUID,
      plannedAt:LocalDateTime,resultAt:LocalDateTime):MySqlDbFirstRecallMaterialization = synchronized {
    liveCore.materializeExecutingDbFirstRecall(runner,before,connectionId,barrierGeneration,plannedAt,resultAt)
  }
  private[mysql] def failDbFirstRecall(runner:MySqlDbFirstRecallTransaction,cause:Throwable):Unit = synchronized {
    liveCore.failDbFirstRecall(runner,cause)
  }
  def requireDbFirstCheckpointSubmission(reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    liveCore.requireDbFirstCheckpointSubmission(reservation)
  }
  def submittedDbFirstCheckpoint(reservation:MySqlDbFirstCaptureReservation,result:IoSubmission):Unit = synchronized {
    liveCore.submittedDbFirstCheckpoint(reservation,result)
  }
  def submissionUnknownDbFirstCheckpoint(reservation:MySqlDbFirstCaptureReservation,cause:Throwable):Unit = synchronized {
    liveCore.submissionUnknownDbFirstCheckpoint(reservation,cause)
  }
  private[mysql] def beginDbFirstCheckpoint(session:MySqlDbFirstSession,reservation:MySqlDbFirstCaptureReservation,
      inputs:MySqlScriptSchemaInputs):MySqlDbFirstCheckpointTransaction = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.beginDbFirstCheckpoint(reservation)
  }
  private[mysql] def requireExecutingDbFirstCheckpoint(session:MySqlDbFirstSession,runner:MySqlDbFirstCheckpointTransaction,
      inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.requireExecutingDbFirstCheckpoint(runner)
  }
  private[mysql] def materializeExecutingDbFirstCheckpoint(runner:MySqlDbFirstCheckpointTransaction,
      before:MySqlScriptSchemaSnapshot,connectionId:Long,barrierGeneration:UUID,
      plannedAt:LocalDateTime,resultAt:LocalDateTime):MySqlDbFirstRecallMaterialization = synchronized {
    liveCore.materializeExecutingDbFirstCheckpoint(runner,before,connectionId,barrierGeneration,plannedAt,resultAt)
  }
  private[mysql] def failDbFirstCheckpoint(runner:MySqlDbFirstCheckpointTransaction,cause:Throwable):Unit = synchronized {
    liveCore.failDbFirstCheckpoint(runner,cause)
  }
  def requireDbFirstInitialActivationSubmission(reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    liveCore.requireDbFirstInitialActivationSubmission(reservation)
  }
  def submittedDbFirstInitialActivation(reservation:MySqlDbFirstCaptureReservation,result:IoSubmission):Unit = synchronized {
    liveCore.submittedDbFirstInitialActivation(reservation,result)
  }
  def submissionUnknownDbFirstInitialActivation(reservation:MySqlDbFirstCaptureReservation,cause:Throwable):Unit = synchronized {
    liveCore.submissionUnknownDbFirstInitialActivation(reservation,cause)
  }
  private[mysql] def beginDbFirstInitialActivation(session:MySqlDbFirstSession,reservation:MySqlDbFirstCaptureReservation,
      inputs:MySqlScriptSchemaInputs,activeLimit:Int):MySqlDbFirstInitialActivationTransaction = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.beginDbFirstInitialActivation(reservation,activeLimit)
  }
  private[mysql] def requireExecutingDbFirstInitialActivation(session:MySqlDbFirstSession,runner:MySqlDbFirstInitialActivationTransaction,
      inputs:MySqlScriptSchemaInputs):Unit = synchronized {
    requireDbFirstSession(session,inputs,profile,ledger)
    liveCore.requireExecutingDbFirstInitialActivation(runner)
  }
  private[mysql] def materializeExecutingDbFirstInitialActivation(runner:MySqlDbFirstInitialActivationTransaction,
      before:MySqlScriptSchemaSnapshot,connectionId:Long,barrierGeneration:UUID,
      plannedAt:LocalDateTime,resultAt:LocalDateTime,activeLimit:Int):MySqlDbFirstInitialActivationMaterialization = synchronized {
    liveCore.materializeExecutingDbFirstInitialActivation(runner,before,connectionId,barrierGeneration,plannedAt,resultAt,activeLimit)
  }
  private[mysql] def failDbFirstInitialActivation(runner:MySqlDbFirstInitialActivationTransaction,cause:Throwable):Unit = synchronized {
    liveCore.failDbFirstInitialActivation(runner,cause)
  }
  def reserveSingle(tag: IoOperationTag, workflow: UUID, reserve: UUID, operation: MySqlRepositoryOperation): MySqlOwnedRepositoryOperationPlan =
    synchronized { liveCore.reserveSingle(tag, workflow, reserve, operation) }
  def reserveTransaction(tag: IoOperationTag, workflow: UUID, reserve: UUID, operations: Vector[MySqlRepositoryOperation]): MySqlOwnedRepositoryOperationPlan =
    synchronized { liveCore.reserveTransaction(tag, workflow, reserve, operations) }
  def requireSingle(plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation): Unit = synchronized { liveCore.requireSingle(plan, operation) }
  def requireTransaction(plan: MySqlOwnedRepositoryOperationPlan, operations: Vector[MySqlRepositoryOperation]): Unit = synchronized { liveCore.requireTransaction(plan, operations) }
  def submitted(plan: MySqlOwnedRepositoryOperationPlan, result: IoSubmission): Unit = synchronized { liveCore.submitted(plan, result) }
  def beginExecution(plan: MySqlOwnedRepositoryOperationPlan): MySqlFormat3ReadBudget = synchronized { liveCore.beginExecution(plan) }
  def markUnknown(plan: MySqlOwnedRepositoryOperationPlan, cause: Throwable): Unit = synchronized { liveCore.markUnknown(plan, cause) }
  def recordMainDrop(handle: MySqlRetainedPlanHandle): MySqlRetainedPlanDropReceipt = synchronized { liveCore.recordMainDrop(handle) }
  def cancelExplicitlyRejected(handle: MySqlRetainedPlanHandle): Unit = synchronized { liveCore.cancelExplicitlyRejected(handle) }
  private[mysql] def requireDropReceipt(receipt: MySqlRetainedPlanDropReceipt): Unit = synchronized { liveCore.requireDropReceipt(receipt) }
  private[mysql] def requireReleaseKind(receipt: MySqlRetainedPlanDropReceipt, kind: MySqlRetainedPlanReleaseKind): Unit = synchronized { liveCore.requireReleaseKind(receipt, kind) }
  private[mysql] def dropWorkerPlanReferences(receipt: MySqlRetainedPlanDropReceipt): Unit = synchronized { liveCore.dropWorkerPlanReferences(receipt) }
  private[mysql] def markReleaseFailed(receipt: MySqlRetainedPlanDropReceipt, cause: Throwable): Unit = synchronized { liveCore.markReleaseFailed(receipt, cause) }
  private[mysql] def releaseDropped(receipt: MySqlRetainedPlanDropReceipt): Unit = synchronized { liveCore.releaseDropped(receipt) }
  def suspendMain(): Unit = synchronized { liveCore.suspendMain() }
  def claimMain(actual: Thread): Unit = synchronized { liveCore.claimMain(actual) }
  /** core 只在成功 stopped-empty 退休后清空；重复 Unit stop 不发行资源关闭或退出资格。 */
  def stop(): Unit = synchronized {
    val actual = core
    if actual != null then actual.stop()
  }
  private[mysql] def stopEmptyDbFirstAfterFailure():Boolean = synchronized {
    liveCore.stopEmptyDbFirstAfterFailure()
  }
  private[mysql] def sealUnusedForFreshCompletion(): Unit = synchronized { liveCore.sealUnusedForFreshCompletion() }
  private[mysql] def requireFreshCompletionSeal(): Unit = synchronized { liveCore.requireFreshCompletionSeal() }
  private[mysql] def requireOrdinaryTerminalPrepared(): Unit = synchronized { liveCore.requireStoppedEmptyForTerminal() }
  private[mysql] def detachOrdinaryReferencesForExit(session: MySqlOwnedDatabaseSession,
      actualProfile: SourceLimitProfile, actualLedger: MySqlFormat3AllocationLedger): Unit = synchronized {
    requireSession(session,actualProfile,actualLedger)
    liveCore.requireStoppedEmptyForTerminal()
    liveCore.retireSettledMemoryGraph()
    core=null;sessionOwner=null;profile=null;ledger=null
  }
  def freeze(cause: Throwable): Unit = synchronized { liveCore.freeze(cause) }
  private[mysql] def rootForOriginal(plan: MySqlOwnedRepositoryOperationPlan): MySqlFormat3ReadBudget = synchronized { liveCore.rootForOriginal(plan) }
  private[mysql] def validationBudget(handle: MySqlRetainedPlanHandle): MySqlFormat3ReadBudget = synchronized { liveCore.validationBudget(handle) }
  private[mysql] def retainedRoots: Int = synchronized { liveCore.retainedRoots }
  private[mysql] def accountedBytes: Long = synchronized { liveCore.accountedBytes }
  def observation: MySqlRetainedAllocationObservation = synchronized { liveCore.observation }

  /** 只撤销零历史issuer的实际图；Fresh/native综合资格由原Session私有worker入口另核。 */
  private[mysql] def detachUnusedFreshReferencesForExit(session: MySqlOwnedDatabaseSession,
      actualProfile: SourceLimitProfile, actualLedger: MySqlFormat3AllocationLedger): Unit = synchronized {
    requireSession(session, actualProfile, actualLedger)
    liveCore.requireFreshCompletionSeal()
    liveCore.retireSettledMemoryGraph()
    // 原seal禁止后续发行；零serial意味着没有可外逃的handle/core引用。
    core = null
    sessionOwner = null
    profile = null
    ledger = null
  }

/** 仅内存actor/原plan/root责任；不含Session、JDBC、Boot或任何SQL admission发行。 */
private[mysql] final class MySqlRetainedPlanReservationCore private[mysql] (
    private var identity: MySqlOwnedIoIdentity, worker: Thread, initialMain: Thread,
    profileInput: SourceLimitProfile, ledgerInput: MySqlFormat3AllocationLedger, capacity: Int,
    ownerIssuerInput: Option[MySqlOwnedRetainedPlanIssuer],
    deploymentInputsInput: Option[MySqlScriptSchemaInputs] = None,
    dbFirstInputsInput: Option[MySqlScriptSchemaInputs] = None):
  private var profile = profileInput
  private var ledger = ledgerInput
  private var ownerIssuer = ownerIssuerInput
  private var deploymentInputs=deploymentInputsInput
  private var dbFirstInputs=dbFirstInputsInput
  require(ownerIssuer != null && !ownerIssuer.exists(_ == null) && identity != null && worker != null && initialMain != null && (worker ne initialMain) &&
    profile != null && ledger != null && capacity > 0, "actual bounded session allocator identities required")
  require(deploymentInputs!=null && !deploymentInputs.exists(_==null) && deploymentInputs.forall(inputs=>
    inputs.dataLayout==MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 && (inputs.sourceLimitProfile eq profile)),
    "retained Full26 roots require exact original immutable inputs/profile")
  require(dbFirstInputs != null && !dbFirstInputs.exists(_ == null) && dbFirstInputs.forall(inputs =>
    inputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27 && (inputs.sourceLimitProfile eq profile)) &&
    !(dbFirstInputs.nonEmpty && deploymentInputs.nonEmpty),
    "retained Full27 roots require exact original immutable inputs/profile and distinct family")
  private final class Entry(val handle: MySqlRetainedPlanHandle, val root: MySqlFormat3ReadBudget, val releaseKind: MySqlRetainedPlanReleaseKind):
    var plan = Option.empty[MySqlOwnedRepositoryOperationPlan]
    var journalOriginalTag=Option.empty[IoOperationTag]
    var journalReservation=Option.empty[MySqlDbFirstRecallJournalReservation]
    var journalWork=Option.empty[MySqlDbFirstRecallJournalWork]
    var journalObservation=Option.empty[MySqlDbFirstSession.DbFirstRecallJournalObservation]
    var journalConfirmation=Option.empty[MySqlDbFirstRecallJournalConfirmation]
    var journalReadReleaseStarted=false
    var journalReadReleased=false
    var journalReadAcknowledged=false
    var recallRefusalObservation=Option.empty[MySqlDbFirstSession.DbFirstRecallRefusalObservation]
    var recallRefusalConfirmation=Option.empty[MySqlDbFirstRecallRefusalConfirmation]
    var recallRefusalReadReleaseStarted=false
    var recallRefusalReadReleased=false
    var recallRefusalReadAcknowledged=false
    var refusalOriginalTag=Option.empty[IoOperationTag]
    var recallRefusalFinal=Option.empty[MySqlDbFirstRecallRefusalFinalReceipt]
    var recallRefusalFinalSubmissionAttempted=false
    var recallRefusalFinalSubmission=Option.empty[IoSubmission]
    var recallRefusalFinalWorkerStarted=false
    var recallRefusalFinalPayloadDetachStarted=false
    var recallRefusalFinalRetirementStage=MySqlDbFirstRetirementFailure.Stage.Unqualified
    var recallRefusalFinalRootCloseAttempted=false
    var recallRefusalFinalWorkerRetired=false
    var recallRefusalSubmissionAttempted=false
    var recallRefusalSubmission=Option.empty[IoSubmission]
    var recallRefusalWorkerStarted=false
    var recallRefusalWork=Option.empty[MySqlDbFirstRecallRefusalWork]
    var journalPreCaptureExclusionAttempted=false
    var journalPreCaptureExclusionCause:Throwable=null
    var journalPreCaptureExclusionFee=0L
    var journalPreCaptureExclusion=Option.empty[MySqlDbFirstRecallPreCaptureExclusion]
    var journalCaptureTransferAttempted=false
    var journalCaptureRoot=Option.empty[MySqlFormat3ReadBudget]
    var journalCaptureSuccessor=Option.empty[Entry]
    var journalCaptureTransfer=Option.empty[MySqlDbFirstRecallJournalCaptureTransfer]
    var journalCaptureWorkerStarted=false
    var journalCaptureSubmissionAttempted=false
    var journalCaptureSubmission=Option.empty[IoSubmission]
    var journalCaptureRetirementStage=MySqlDbFirstRetirementFailure.Stage.Unqualified
    var journalCaptureWorkerRetired=false
    var journalCaptureRootCloseAttempted=false
    var journalSubmissionAttempted=false
    var journalSubmissionRecorded=false
    var journalRejectedDrop=Option.empty[MySqlDbFirstRejectedJournalReceipt]
    var preparationReservation=Option.empty[MySqlDbFirstInitialPreparationReservation]
    var preparationBacking=Option.empty[MySqlRepositoryRequestIdentity]
    var preparationSubmissionAttempted=false
    var preparationSubmissionRecorded=false
    var preparationReadReleaseStarted=false
    var preparationRejectedDrop=Option.empty[MySqlDbFirstRejectedPreparationReceipt]
    var preparationReadReleased=false
    var preparationFinal=Option.empty[MySqlDbFirstPreparationFinalReceipt]
    var preparationReleaseAttempted=false
    var preparationRootCloseAttempted=false
    var preparationRetirementStage=MySqlDbFirstRetirementFailure.Stage.Unqualified
    var preparationConfirmation=Option.empty[MySqlDbFirstInitialPreparationConfirmation]
    var preparationRejectionConfirmation=Option.empty[MySqlDbFirstPreparationRejectionConfirmation]
    var preparationObservation=Option.empty[MySqlDbFirstSession.DbFirstInitialPreparationObservation]
    var preparationWork=Option.empty[MySqlDbFirstInitialPreparationWork]
    var captureReservation = Option.empty[MySqlDbFirstCaptureReservation]
    var captureJournalLink=Option.empty[MySqlDbFirstRecallJournalLink]
    var captureJournalTransfer=Option.empty[MySqlDbFirstRecallJournalCaptureTransfer]
    var captureJournalActivated=false
    var bootTakeover=Option.empty[MySqlDbFirstBootRecallTakeoverWork]
    var bootTakeoverPrepared=false
    var bootMainDecisionAttempted=false
    var bootMainDecision=Option.empty[MySqlDbFirstBootRecallMainDecision]
    var bootCaptureExcluded=false
    var bootCaptureActivationAttempted=false
    var bootCaptureActivated=false
    var captureAttempted = false
    var recoveredInitialDataAttempted=false
    var captured = Option.empty[DbFirstPodSnapshot]
    var checkpointRequest = Option.empty[MySqlDbFirstCheckpointRequest]
    var checkpointBacking = Option.empty[MySqlRepositoryRequestIdentity]
    var checkpointRunner=Option.empty[MySqlDbFirstCheckpointTransaction]
    var checkpointObservation=Option.empty[MySqlDbFirstSession.DbFirstCheckpointObservation]
    var checkpointConfirmation=Option.empty[MySqlDbFirstCheckpointConfirmation]
    var checkpointReadReleaseStarted=false
    var checkpointReadReleased=false
    var checkpointReadAcknowledged=false
    var checkpointFinal=Option.empty[MySqlDbFirstCheckpointFinalReceipt]
    var checkpointReleaseAttempted=false
    var checkpointRootCloseAttempted=false
    var checkpointRetirementStage=MySqlDbFirstRetirementFailure.Stage.Unqualified
    var checkpointSubmissionAttempted=false
    var checkpointSubmissionRecorded=false
    var initialActivationRequest = Option.empty[MySqlDbFirstInitialActivationRequest]
    var initialActivationBacking = Option.empty[MySqlRepositoryRequestIdentity]
    var dbFirstRequest = Option.empty[MySqlDbFirstRecallRequest]
    var dbFirstRequestBacking = Option.empty[MySqlRepositoryRequestIdentity]
    var recallObservation=Option.empty[MySqlDbFirstSession.DbFirstRecallObservation]
    var recallSqlSettlementAttempted=false
    var recallSqlSettlement=Option.empty[MySqlDbFirstSession.FailedRecallReadback]
    var recallCleanupQualification=Option.empty[MySqlDbFirstSession.FailedRecallCleanupQualification]
    var recallConfirmation=Option.empty[MySqlDbFirstRecallConfirmation]
    var recallDrop=Option.empty[MySqlDbFirstRecallDropReceipt]
    var recallRetainedMainHandedOff=false
    var recallRetainedPayloadDetached=false
    var recallReleaseAttempted=false
    var recallRootCloseAttempted=false
    var recallRetirementStage=MySqlDbFirstRetirementFailure.Stage.Unqualified
    var recallSubmissionAttempted = false
    var recallSubmissionRecorded = false
    var recallRunner = Option.empty[MySqlDbFirstRecallTransaction]
    var initialSubmissionAttempted=false
    var initialSubmissionRecorded=false
    var initialRunner=Option.empty[MySqlDbFirstInitialActivationTransaction]
    var initialConfirmation=Option.empty[MySqlDbFirstInitialActivationConfirmation]
    var initialObservation=Option.empty[MySqlDbFirstSession.DbFirstInitialActivationObservation]
    var initialPublication=Option.empty[MySqlDbFirstInitialPublicationReceipt]
    var initialReleaseAttempted=false
    var initialRootCloseAttempted=false
    var initialRetirementStage=MySqlDbFirstRetirementFailure.Stage.Unqualified
    var materializationAttempted = false
    var initialActivationDraft = Option.empty[MySqlDbFirstInitialActivationDraft]
    var dbFirstDraft = Option.empty[MySqlDbFirstRecallDraft]
    var accepted = false
    var explicitlyRejected = false
    var workerObserved = false
    var dropped = false
    var dropReceipt = Option.empty[MySqlRetainedPlanDropReceipt]
    var cause = Option.empty[Throwable]
  private val entries = mutable.LinkedHashMap.empty[IoOperationTag, Entry]
  private var nextSerial = 0L
  private var main = Option(initialMain)
  private var frozen = Option.empty[Throwable]
  private var stopped = false
  private var freshCompletionSealed = false
  private val imageHandoffCapability = new Object()

  private def requireLive(): Unit =
    if profile == null then throw new IllegalStateException("retained core memory graph was retired")

  private def mainOwner(): Unit = require(main.exists(_ eq Thread.currentThread()), "retained plan requires exact current Main")
  private def workerOwner(): Unit = require(Thread.currentThread() eq worker, "retained plan worker action left actual owned IO")
  private def current(handle: MySqlRetainedPlanHandle): Entry =
    require(handle != null && (handle.reservationCore eq this), "foreign retained plan issuer")
    val entry = entries.getOrElse(handle.tag, throw new IllegalStateException("retained plan was released or never issued"))
    require(entry.handle eq handle, "retained plan serial/reference differs")
    entry
  private def original(plan: MySqlOwnedRepositoryOperationPlan): Entry =
    require(plan != null, "original retained plan required")
    val handle = plan.retainedHandle.getOrElse(throw new IllegalArgumentException("DATA plan has no owned session reservation"))
    val entry = current(handle)
    require(entry.plan.exists(_ eq plan), "equal DATA plan cannot replace actual reserved plan reference")
    entry

  /** 原C与内部J共同占位；其他capture/preparation不能抢走尚未终局的原业务身份。 */
  private def tagAvailable(tag:IoOperationTag):Boolean =
    !entries.contains(tag) && !entries.valuesIterator.exists(_.journalOriginalTag.contains(tag))

  def reserveDbFirstRecallJournal(tag:IoOperationTag,original:IoOperationTag,binding:UUID,actor:UUID,pod:UUID,
      version:Long,client:Option[UUID]):MySqlDbFirstRecallJournalReservation = synchronized {
    requireLive();mainOwner();identity.requireTag(tag);identity.requireTag(original)
    require(dbFirstInputs.nonEmpty,"actual schema6 journal inputs required")
    MySqlDbFirstRecallJournalRequest.validate(tag,original,binding,actor,pod,version,client)
    frozen.foreach(throw _)
    require(!stopped && tagAvailable(tag) && tagAvailable(original) && entries.size<capacity,
      "original journal/business identity or capacity unavailable; no remint")
    val inputs=dbFirstInputs.get
    val root=MySqlFormat3ReadBudget.retainedDbFirstRootOnLedger(inputs,
      Math.min(profile.maxSqlPacketBytes(),inputs.budget.packetBytes),ledger,imageHandoffCapability,worker)
    nextSerial=Math.addExact(nextSerial,1L)
    val handle=new MySqlRetainedPlanHandle(ownerIssuer.orNull,nextSerial,tag,this)
    val entry=new Entry(handle,root,MySqlRetainedPlanReleaseKind.Ordinary)
    entry.journalOriginalTag=Some(original)
    entries.put(tag,entry)
    try
      root.reserveRetainedMemory(1536L,"journal Entry/reservation/rejected receipt BEFORE allocation")
      val request=MySqlDbFirstRecallJournalRequest.fresh(tag,original,binding,actor,pod,version,client,root)
      val reservation=new MySqlDbFirstRecallJournalReservation(this,handle,request)
      entry.journalReservation=Some(reservation)
      reservation
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private def journalEntry(reservation:MySqlDbFirstRecallJournalReservation):Entry =
    requireLive()
    require(reservation!=null && (reservation.core eq this),"original journal core required")
    val entry=current(reservation.handle)
    require(dbFirstInputs.nonEmpty && entry.journalReservation.exists(_ eq reservation) &&
      entry.journalOriginalTag.contains(reservation.originalTag) && entry.plan.isEmpty &&
      entry.captureReservation.isEmpty && entry.preparationReservation.isEmpty && !entry.root.isClosed && !entry.dropped,
      "original live journal-only Entry required")
    entry
  def requireDbFirstRecallJournalSubmission(reservation:MySqlDbFirstRecallJournalReservation):Unit = synchronized {
    mainOwner();val entry=journalEntry(reservation);frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && !entry.journalSubmissionAttempted && !entry.workerObserved && !entry.accepted,
      "original journal submission is one-shot")
    entry.journalSubmissionAttempted=true
  }
  def submittedDbFirstRecallJournal(reservation:MySqlDbFirstRecallJournalReservation,result:IoSubmission):Unit = synchronized {
    mainOwner();val entry=journalEntry(reservation)
    require(result!=null && entry.journalSubmissionAttempted && !entry.journalSubmissionRecorded,
      "original journal submission result required")
    if result!=IoSubmission.Accepted then require(!entry.workerObserved && !entry.accepted,"observed journal cannot become rejected")
    entry.journalSubmissionRecorded=true
    if result==IoSubmission.Accepted then entry.accepted=true else entry.explicitlyRejected=true
  }
  def submissionUnknownDbFirstRecallJournal(reservation:MySqlDbFirstRecallJournalReservation,cause:Throwable):Unit = synchronized {
    mainOwner();val entry=journalEntry(reservation)
    require(entry.journalSubmissionAttempted && cause!=null,"actual original journal submission uncertainty required")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
    frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
  }
  private[mysql] def beginDbFirstRecallJournal(reservation:MySqlDbFirstRecallJournalReservation):MySqlDbFirstRecallJournalWork = synchronized {
    workerOwner();val entry=journalEntry(reservation);frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && entry.journalSubmissionAttempted && !entry.explicitlyRejected && !entry.workerObserved && entry.journalWork.isEmpty,
      "original journal worker entry is one-shot")
    try
      entry.root.bindRetainedImageWorker(imageHandoffCapability)
      entry.workerObserved=true;entry.accepted=true
      entry.root.reserveRetainedMemory(MySqlDbFirstRecallJournalWork.HolderBytes,"journal worker holder BEFORE allocation")
      val work=new MySqlDbFirstRecallJournalWork(reservation,entry.root)
      entry.journalWork=Some(work)
      work
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private[mysql] def requireExecutingDbFirstRecallJournal(work:MySqlDbFirstRecallJournalWork):Unit = synchronized {
    workerOwner();require(work!=null,"original journal work required")
    val entry=journalEntry(work.reservation);frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && entry.journalWork.exists(_ eq work) && entry.workerObserved && entry.accepted,
      "actual registered journal worker responsibility required")
  }
  private[mysql] def failDbFirstRecallJournal(work:MySqlDbFirstRecallJournalWork,cause:Throwable):Unit = synchronized {
    workerOwner();require(work!=null && cause!=null,"actual original journal failure required")
    val entry=journalEntry(work.reservation)
    require(entry.journalWork.exists(_ eq work),"foreign journal failure")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
  }
  def claimDbFirstRecallJournalConfirmation(reservation:MySqlDbFirstRecallJournalReservation,
      observation:MySqlDbFirstSession.DbFirstRecallJournalObservation):MySqlDbFirstRecallJournalConfirmation = synchronized {
    mainOwner();val entry=journalEntry(reservation);frozen.foreach(throw _)
    require(!stopped && entry.cause.isEmpty && entry.journalSubmissionRecorded && entry.journalConfirmation.isEmpty,
      "healthy original journal confirmation is one-shot")
    val work=entry.journalWork.getOrElse(throw new IllegalStateException("original journal work missing"))
    work.retainedTransaction.requireConfirmedObservation(observation)
    try
      val confirmation=MySqlDbFirstRecallJournalConfirmation.issue(this,entry.handle,reservation.request,observation.canonicalSha256)
      entry.journalObservation=Some(observation);entry.journalConfirmation=Some(confirmation)
      confirmation
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private[mysql] def requireDbFirstRecallJournalConfirmation(confirmation:MySqlDbFirstRecallJournalConfirmation,
      reservation:MySqlDbFirstRecallJournalReservation):Unit = synchronized {
    mainOwner();val entry=journalEntry(reservation);frozen.foreach(throw _)
    require(confirmation!=null && (confirmation.core eq this) && (confirmation.handle eq entry.handle) &&
      entry.journalConfirmation.exists(_ eq confirmation) && entry.cause.isEmpty && !stopped,
      "original Main journal confirmation reference required")
    entry.journalWork.get.retainedTransaction.requireConfirmedObservation(entry.journalObservation.get)
  }
  private[mysql] def beginJournalReadRelease(confirmation:MySqlDbFirstRecallJournalConfirmation):Unit = synchronized {
    requireLive();workerOwner();frozen.foreach(throw _)
    require(confirmation!=null && (confirmation.core eq this),"original journal confirmation required")
    val entry=current(confirmation.handle)
    require(entry.journalConfirmation.exists(_ eq confirmation) && entry.cause.isEmpty && !entry.dropped &&
      !stopped && !entry.journalReadReleaseStarted,"healthy original journal read release is one-shot")
    entry.journalWork.get.retainedTransaction.requireConfirmedObservation(entry.journalObservation.get)
    entry.journalReadReleaseStarted=true
  }
  private[mysql] def finishJournalReadRelease(confirmation:MySqlDbFirstRecallJournalConfirmation):Unit = synchronized {
    workerOwner();val entry=current(confirmation.handle)
    require((confirmation.core eq this) && entry.journalConfirmation.exists(_ eq confirmation) &&
      entry.journalReadReleaseStarted && !entry.journalReadReleased && entry.cause.isEmpty,
      "original started journal read release required")
    entry.journalReadReleased=true
  }
  private[mysql] def requireJournalReadReleased(confirmation:MySqlDbFirstRecallJournalConfirmation,
      reservation:MySqlDbFirstRecallJournalReservation):Unit = synchronized {
    requireDbFirstRecallJournalConfirmation(confirmation,reservation)
    require(journalEntry(reservation).journalReadReleased,"actual worker journal read retirement required")
  }
  private[mysql] def acknowledgeJournalReadReleased(confirmation:MySqlDbFirstRecallJournalConfirmation,
      reservation:MySqlDbFirstRecallJournalReservation):Unit = synchronized {
    requireJournalReadReleased(confirmation,reservation)
    val entry=journalEntry(reservation)
    require(!entry.journalReadAcknowledged,"original journal Main read ACK is one-shot")
    entry.journalReadAcknowledged=true
  }
  /** 只封从未转交capture的原C；先不可逆封路和保原因，再预收/登记holder。
    * J仍可完成原次SQL或保持UNKNOWN；封锁本身不返回终局结果，不释放J/C占位。
    */
  private[mysql] def excludeRecallBeforeCapture(reservation:MySqlDbFirstRecallJournalReservation,
      cause:Throwable,reason:Option[RecallRefusalReason]=None):MySqlDbFirstRecallPreCaptureExclusion = synchronized {
    mainOwner();val entry=journalEntry(reservation);frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(cause!=null && reason!=null && reason.forall(_!=null) && !stopped && !entry.journalPreCaptureExclusionAttempted &&
      !entry.journalCaptureTransferAttempted && entry.journalCaptureTransfer.isEmpty &&
      entry.journalCaptureSuccessor.isEmpty && !entries.contains(reservation.originalTag),
      "original untransferred C may be excluded once before capture; no replacement attempt")
    entry.journalPreCaptureExclusionAttempted=true
    entry.journalPreCaptureExclusionCause=cause
    try
      entry.root.reserveRetainedMemory(MySqlDbFirstRecallPreCaptureExclusion.HolderBytes,
        "original pre-capture C execution exclusion BEFORE allocation")
      entry.journalPreCaptureExclusionFee=MySqlDbFirstRecallPreCaptureExclusion.HolderBytes
      val exclusion=new MySqlDbFirstRecallPreCaptureExclusion(this,reservation,cause,reason)
      entry.journalPreCaptureExclusion=Some(exclusion)
      exclusion
    catch case failure:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,failure)
      throw failure
  }
  /** 真实注册封锁仅关闭本地执行路线，不能单凭该对象导出SQL/not-applied资格。 */
  private[mysql] def requireRecallExcludedBeforeCapture(exclusion:MySqlDbFirstRecallPreCaptureExclusion):Unit = synchronized {
    requireLive()
    require((Thread.currentThread() eq worker) || main.exists(_ eq Thread.currentThread()),
      "original exclusion actor required")
    require(exclusion!=null && (exclusion.core eq this),"original registered exclusion required")
    val entry=journalEntry(exclusion.reservation)
    frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && entry.journalPreCaptureExclusionAttempted &&
      entry.journalPreCaptureExclusionFee==MySqlDbFirstRecallPreCaptureExclusion.HolderBytes &&
      entry.journalPreCaptureExclusion.exists(_ eq exclusion) &&
      (entry.journalPreCaptureExclusionCause eq exclusion.cause) &&
      !entry.journalCaptureTransferAttempted && entry.journalCaptureTransfer.isEmpty &&
      entry.journalCaptureSuccessor.isEmpty && !entries.contains(exclusion.originalTag),
      "actual original C exclusion/cause required; no capture route may have escaped")
  }
  private def confirmedExcludedRecallEntry(exclusion:MySqlDbFirstRecallPreCaptureExclusion):Entry =
    requireRecallExcludedBeforeCapture(exclusion)
    val entry=journalEntry(exclusion.reservation)
    require(entry.journalSubmissionRecorded && entry.accepted && entry.workerObserved &&
      entry.journalReadReleased && entry.journalReadAcknowledged && entry.journalConfirmation.nonEmpty &&
      entry.journalObservation.nonEmpty && entry.journalWork.nonEmpty,
      "actual durable J confirmation and Main read ACK required before excluded-C settlement")
    entry.journalWork.get.retainedTransaction.requireConfirmedObservation(entry.journalObservation.get)
    entry
  /** worker后续结算还必须核已确认J/读代ACK；这也不取代当前barrier/audit/完整库存。 */
  private[mysql] def requireConfirmedRecallExcludedBeforeCapture(exclusion:MySqlDbFirstRecallPreCaptureExclusion):Unit = synchronized {
    workerOwner();confirmedExcludedRecallEntry(exclusion);()
  }
  private def refusalEntry(exclusion:MySqlDbFirstRecallRefusalSource):Entry =
    require(exclusion!=null && (exclusion.core eq this),"original registered refusal source required")
    exclusion match
      case live:MySqlDbFirstRecallPreCaptureExclusion=>
        val entry=journalEntry(live.reservation)
        require(entry.journalPreCaptureExclusion.exists(_ eq live) &&
          (entry.journalPreCaptureExclusionCause eq live.cause),"exact original live refusal holder/cause required")
        entry
      case boot:MySqlDbFirstBootRecallMainDecision=>
        val entry=current(boot.handle)
        require(entry.bootMainDecision.exists(_ eq boot) && entry.bootTakeover.exists(_.intent eq boot.intent) &&
          entry.captureReservation.exists(_.tag==boot.executionTag),"exact original Boot refusal decision required")
        entry
      case _=>throw new IllegalArgumentException("unregistered refusal source kind")
  private def confirmedRefusalEntry(exclusion:MySqlDbFirstRecallRefusalSource):Entry =
    exclusion match
      case live:MySqlDbFirstRecallPreCaptureExclusion=>confirmedExcludedRecallEntry(live)
      case boot:MySqlDbFirstBootRecallMainDecision=>
        requireBootRecallMainDecision(boot)
        val entry=refusalEntry(boot)
        require(boot.refusalReason.nonEmpty && entry.bootCaptureExcluded && !entry.bootCaptureActivated &&
          !entry.captureAttempted && entry.plan.isEmpty && !entry.root.isClosed,
          "actual excluded Boot decision with typed reason required; positive is not refusal authority")
        entry
      case _=>throw new IllegalArgumentException("actual live or Boot refusal source required")
  /** live须实际J确认/读代ACK，Boot须实际注册拒绝决定；队列前登记一次attempt，Rejected也不重发。 */
  private[mysql] def requireRecallRefusalSubmission(exclusion:MySqlDbFirstRecallRefusalSource):Unit = synchronized {
    mainOwner();val entry=confirmedRefusalEntry(exclusion)
    require(!entry.recallRefusalSubmissionAttempted && !entry.recallRefusalWorkerStarted && entry.recallRefusalWork.isEmpty,
      "original C refusal submission is one-shot")
    entry.recallRefusalSubmissionAttempted=true
  }
  private[mysql] def submittedRecallRefusal(exclusion:MySqlDbFirstRecallRefusalSource,result:IoSubmission):Unit = synchronized {
    mainOwner();val entry=refusalEntry(exclusion)
    require(entry.recallRefusalSubmissionAttempted && entry.recallRefusalSubmission.isEmpty && result!=null,
      "actual original refusal submission result required once")
    entry.recallRefusalSubmission=Some(result)
    if result!=IoSubmission.Accepted && entry.recallRefusalWorkerStarted then
      val cause=new IllegalStateException("rejected original refusal was already observed by worker")
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
      throw cause
    // Accepted回执可晚于worker；记录本身不清worker failure或UNKNOWN，不确认结算。
  }
  private[mysql] def submissionUnknownRecallRefusal(exclusion:MySqlDbFirstRecallRefusalSource,cause:Throwable):Unit = synchronized {
    mainOwner();val entry=refusalEntry(exclusion)
    require(entry.recallRefusalSubmissionAttempted && cause!=null,"actual original refusal submission uncertainty required")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
    frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
  }
  private[mysql] def beginRecallRefusal(exclusion:MySqlDbFirstRecallRefusalSource):MySqlDbFirstRecallRefusalWork = synchronized {
    workerOwner();val entry=confirmedRefusalEntry(exclusion)
    require(entry.recallRefusalSubmissionAttempted && !entry.recallRefusalWorkerStarted && entry.recallRefusalWork.isEmpty &&
      entry.recallRefusalSubmission.forall(_==IoSubmission.Accepted),"original refusal worker attempt required once")
    entry.recallRefusalWorkerStarted=true
    try
      entry.root.reserveRetainedMemory(MySqlDbFirstRecallRefusalWork.HolderBytes,"original refusal worker holder BEFORE allocation")
      val work=new MySqlDbFirstRecallRefusalWork(exclusion,entry.root)
      entry.recallRefusalWork=Some(work)
      work
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private[mysql] def requireExecutingRecallRefusal(work:MySqlDbFirstRecallRefusalWork):Unit = synchronized {
    workerOwner();require(work!=null,"actual original refusal worker required")
    val entry=confirmedRefusalEntry(work.exclusion)
    require(entry.recallRefusalSubmissionAttempted && entry.recallRefusalWorkerStarted &&
      entry.recallRefusalSubmission.forall(_==IoSubmission.Accepted) &&
      entry.recallRefusalWork.exists(_ eq work) && (entry.root eq work.root),
      "exact registered original refusal worker/root required")
  }
  private[mysql] def failRecallRefusal(work:MySqlDbFirstRecallRefusalWork,cause:Throwable):Unit = synchronized {
    workerOwner();require(work!=null && cause!=null,"actual original refusal failure required")
    val entry=refusalEntry(work.exclusion)
    require(entry.recallRefusalWork.exists(_ eq work) && (entry.root eq work.root),"foreign refusal failure")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
  }
  def claimRecallRefusalConfirmation(exclusion:MySqlDbFirstRecallRefusalSource,
      observation:MySqlDbFirstSession.DbFirstRecallRefusalObservation):MySqlDbFirstRecallRefusalConfirmation = synchronized {
    mainOwner();val entry=confirmedRefusalEntry(exclusion)
    require(entry.recallRefusalSubmission.contains(IoSubmission.Accepted) && entry.recallRefusalWorkerStarted &&
      entry.recallRefusalConfirmation.isEmpty,"actual accepted original refusal confirmation is one-shot")
    val work=entry.recallRefusalWork.getOrElse(throw new IllegalStateException("original refusal Work missing"))
    work.retainedTransaction.requireConfirmedObservation(observation)
    try
      val confirmation=MySqlDbFirstRecallRefusalConfirmation.issue(this,entry.handle,work.request,observation.canonicalSha256)
      entry.recallRefusalObservation=Some(observation);entry.recallRefusalConfirmation=Some(confirmation)
      confirmation
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private[mysql] def requireRecallRefusalConfirmation(confirmation:MySqlDbFirstRecallRefusalConfirmation,
      exclusion:MySqlDbFirstRecallRefusalSource):Unit = synchronized {
    mainOwner();val entry=confirmedRefusalEntry(exclusion)
    require(confirmation!=null && (confirmation.core eq this) && (confirmation.handle eq entry.handle) &&
      entry.recallRefusalConfirmation.exists(_ eq confirmation),"exact original Main refusal confirmation required")
    entry.recallRefusalWork.get.retainedTransaction.requireConfirmedObservation(entry.recallRefusalObservation.get)
  }
  private[mysql] def beginRefusalReadRelease(confirmation:MySqlDbFirstRecallRefusalConfirmation):Unit = synchronized {
    requireLive();workerOwner();frozen.foreach(throw _)
    require(confirmation!=null && (confirmation.core eq this),"original refusal confirmation required")
    val entry=current(confirmation.handle)
    require(entry.recallRefusalConfirmation.exists(_ eq confirmation) && entry.cause.isEmpty && !entry.dropped &&
      !stopped && !entry.recallRefusalReadReleaseStarted,"healthy original refusal read release is one-shot")
    entry.recallRefusalWork.get.retainedTransaction.requireConfirmedObservation(entry.recallRefusalObservation.get)
    entry.recallRefusalReadReleaseStarted=true
  }
  private[mysql] def finishRefusalReadRelease(confirmation:MySqlDbFirstRecallRefusalConfirmation):Unit = synchronized {
    workerOwner();val entry=current(confirmation.handle)
    require((confirmation.core eq this) && entry.recallRefusalConfirmation.exists(_ eq confirmation) &&
      entry.recallRefusalReadReleaseStarted && !entry.recallRefusalReadReleased && entry.cause.isEmpty,
      "original started refusal read release required")
    entry.recallRefusalReadReleased=true
  }
  private[mysql] def requireRefusalReadReleased(confirmation:MySqlDbFirstRecallRefusalConfirmation,
      exclusion:MySqlDbFirstRecallRefusalSource):Unit = synchronized {
    requireRecallRefusalConfirmation(confirmation,exclusion)
    require(refusalEntry(exclusion).recallRefusalReadReleased,"actual worker refusal read retirement required")
  }
  private[mysql] def acknowledgeRefusalReadReleased(confirmation:MySqlDbFirstRecallRefusalConfirmation,
      exclusion:MySqlDbFirstRecallRefusalSource):Unit = synchronized {
    requireRefusalReadReleased(confirmation,exclusion)
    val entry=refusalEntry(exclusion)
    require(!entry.recallRefusalReadAcknowledged,"original refusal Main read ACK is one-shot")
    entry.recallRefusalReadAcknowledged=true
  }
  /** 仅发行独立metadata。实际调用方须跨END断开Main/router旧RAW，再提交worker退休。 */
  private[mysql] def recordRefusalFinal(confirmation:MySqlDbFirstRecallRefusalConfirmation,
      exclusion:MySqlDbFirstRecallRefusalSource):MySqlDbFirstRecallRefusalFinalReceipt = synchronized {
    requireRefusalReadReleased(confirmation,exclusion)
    val entry=refusalEntry(exclusion);frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && entry.recallRefusalReadAcknowledged && entry.recallRefusalFinal.isEmpty,
      "actual Main refusal read ACK required before one-shot final receipt")
    try
      val receipt=MySqlDbFirstRecallRefusalFinalReceipt.issue(confirmation)
      entry.refusalOriginalTag=Some(receipt.originalTag);entry.recallRefusalFinal=Some(receipt);entry.dropped=true
      receipt
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private def refusalFinalEntry(receipt:MySqlDbFirstRecallRefusalFinalReceipt):Entry =
    requireLive()
    require(receipt!=null && (receipt.core eq this),"original refusal final receipt required")
    val entry=current(receipt.handle)
    require(entry.recallRefusalFinal.exists(_ eq receipt) && entry.dropped &&
      entry.refusalOriginalTag.contains(receipt.originalTag),"exact registered refusal final receipt required")
    entry
  private[mysql] def requireRefusalFinalSubmission(receipt:MySqlDbFirstRecallRefusalFinalReceipt):Unit = synchronized {
    mainOwner();val entry=refusalFinalEntry(receipt);frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && !entry.recallRefusalFinalSubmissionAttempted && !entry.recallRefusalFinalWorkerStarted,
      "original refusal final submission is one-shot")
    entry.recallRefusalFinalSubmissionAttempted=true
  }
  private[mysql] def submittedRefusalFinal(receipt:MySqlDbFirstRecallRefusalFinalReceipt,result:IoSubmission):Unit = synchronized {
    mainOwner();val entry=refusalFinalEntry(receipt)
    require(entry.recallRefusalFinalSubmissionAttempted && entry.recallRefusalFinalSubmission.isEmpty && result!=null,
      "original refusal final submission result required once")
    entry.recallRefusalFinalSubmission=Some(result)
    if result!=IoSubmission.Accepted && entry.recallRefusalFinalWorkerStarted then
      val cause=new IllegalStateException("rejected refusal final was already observed by worker")
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
      throw cause
  }
  private[mysql] def submissionUnknownRefusalFinal(receipt:MySqlDbFirstRecallRefusalFinalReceipt,cause:Throwable):Unit = synchronized {
    mainOwner();val entry=refusalFinalEntry(receipt)
    require(entry.recallRefusalFinalSubmissionAttempted && cause!=null,"actual refusal final submission uncertainty required")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
    frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
  }
  private def startedRefusalFinalEntry(receipt:MySqlDbFirstRecallRefusalFinalReceipt):Entry =
    workerOwner();val entry=refusalFinalEntry(receipt);frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && entry.recallRefusalReadAcknowledged && entry.recallRefusalFinalWorkerStarted &&
      !entry.recallRefusalFinalWorkerRetired && !entry.recallRefusalFinalRootCloseAttempted,
      "original started refusal retirement before root close required")
    entry
  private[mysql] def requireRefusalFinalPayload(receipt:MySqlDbFirstRecallRefusalFinalReceipt,
      runner:MySqlDbFirstRecallJournalTransaction):Unit = synchronized {
    val entry=startedRefusalFinalEntry(receipt)
    require(runner!=null && entry.journalWork.exists(_.retainedTransaction eq runner),"actual original J payload runner required")
    runner.requireConfirmedObservation(entry.journalObservation.get)
  }
  private[mysql] def requireRefusalFinalPayload(receipt:MySqlDbFirstRecallRefusalFinalReceipt,
      runner:MySqlDbFirstRecallRefusalTransaction):Unit = synchronized {
    val entry=startedRefusalFinalEntry(receipt)
    require(runner!=null && entry.recallRefusalWork.exists(_.retainedTransaction eq runner),"actual original refusal payload runner required")
    runner.requireConfirmedObservation(entry.recallRefusalObservation.get)
  }
  private[mysql] def requireBootRefusalFinalPayload(receipt:MySqlDbFirstRecallRefusalFinalReceipt,
      takeover:MySqlDbFirstBootRecallTakeoverWork):Unit = synchronized {
    val entry=startedRefusalFinalEntry(receipt)
    require(takeover!=null && entry.bootTakeover.exists(_ eq takeover) && entry.bootMainDecision.nonEmpty &&
      entry.bootCaptureExcluded && takeover.resourcesRetired,"actual SQL-retired Boot takeover required")
  }
  private[mysql] def retireRefusalFinal(receipt:MySqlDbFirstRecallRefusalFinalReceipt):Unit = synchronized {
    workerOwner();val entry=refusalFinalEntry(receipt);frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && entry.recallRefusalReadAcknowledged && entry.recallRefusalFinalSubmissionAttempted &&
      entry.recallRefusalFinalSubmission.forall(_==IoSubmission.Accepted) &&
      !entry.recallRefusalFinalWorkerStarted && !entry.root.isClosed,
      "healthy original refusal final worker is one-shot")
    entry.recallRefusalFinalWorkerStarted=true
    try
      val refusal=entry.recallRefusalWork.get.retainedTransaction
      val boot=entry.bootMainDecision.nonEmpty
      requireRefusalFinalPayload(receipt,refusal)
      if boot then
        require(entry.journalWork.isEmpty && entry.journalReservation.isEmpty,"Boot refusal must never fabricate live J credentials")
        requireBootRefusalFinalPayload(receipt,entry.bootTakeover.get)
      else requireRefusalFinalPayload(receipt,entry.journalWork.get.retainedTransaction)
      entry.recallRefusalFinalRetirementStage=MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight
      require(!entry.root.hasActiveImmutableBorrows && entry.root.retainedCanonicalDataChildCount==0,
        "original refusal Entry still has immutable borrowers/DATA children")
      entry.recallRefusalFinalPayloadDetachStarted=true
      if boot then
        val takeover=entry.bootTakeover.get
        takeover.retireRefusalMetadata(receipt)
        require(takeover.isMetadataRetired && takeover.resourcesRetired,"actual Boot takeover metadata/SQL must retire")
      else
        val journal=entry.journalWork.get.retainedTransaction
        journal.retireRefusalFinalPayload(receipt)
        require(journal.payloadRetired && journal.resourcesRetired,"actual live J payload/SQL must retire")
      refusal.retireRefusalFinalPayload(receipt)
      require(refusal.payloadRetired && refusal.resourcesRetired,"actual refusal payload/SQL must retire")
      entry.journalWork=None;entry.journalReservation=None;entry.journalObservation=None;entry.journalConfirmation=None
      entry.journalPreCaptureExclusion=None
      entry.bootTakeover=None;entry.bootMainDecision=None;entry.captureReservation=None;entry.captureJournalLink=None
      entry.recallRefusalWork=None;entry.recallRefusalObservation=None;entry.recallRefusalConfirmation=None
      entry.recallRefusalFinalRetirementStage=MySqlDbFirstRetirementFailure.Stage.RootCloseAttempted
      entry.recallRefusalFinalRootCloseAttempted=true
      entry.root.closeRootReadBudget()
      entry.recallRefusalFinalWorkerRetired=true
      // 原J Entry/C别名及metadata receipt仍保留，直到实际Main终局ACK。
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      if entry.recallRefusalFinalRootCloseAttempted then frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
      throw cause
  }
  private[mysql] def failRefusalFinal(receipt:MySqlDbFirstRecallRefusalFinalReceipt,cause:Throwable):Throwable = synchronized {
    workerOwner();val entry=refusalFinalEntry(receipt)
    require(cause!=null,"actual original refusal final failure required")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
    if entry.recallRefusalFinalRetirementStage==MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight &&
        !entry.recallRefusalFinalPayloadDetachStarted && !entry.recallRefusalFinalRootCloseAttempted &&
        scala.util.control.NonFatal(cause) && frozen.isEmpty then
      MySqlDbFirstRefusalFinalLocalRetirementException.fromCore(this,receipt,cause)
    else
      frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
      cause
  }
  private[mysql] def requireRefusalFinalLocalRetirementFailure(receipt:MySqlDbFirstRecallRefusalFinalReceipt,
      cause:Throwable):Unit = synchronized {
    workerOwner();val entry=refusalFinalEntry(receipt)
    require(cause!=null && scala.util.control.NonFatal(cause) && entry.cause.exists(_ eq cause) &&
      entry.recallRefusalFinalWorkerStarted && !entry.recallRefusalFinalPayloadDetachStarted &&
      !entry.recallRefusalFinalRootCloseAttempted && !entry.recallRefusalFinalWorkerRetired && frozen.isEmpty &&
      entry.recallRefusalFinalRetirementStage==MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight &&
      (if entry.bootMainDecision.nonEmpty then entry.bootTakeover.exists(_.resourcesRetired) && entry.journalWork.isEmpty
        else entry.journalWork.exists(_.retainedTransaction.resourcesRetired)) &&
      entry.recallRefusalWork.exists(_.retainedTransaction.resourcesRetired),
      "only actual source SQL-retired untouched-payload preflight failure is local")
  }
  private[mysql] def requireRefusalFinalRetired(receipt:MySqlDbFirstRecallRefusalFinalReceipt):Unit = synchronized {
    mainOwner();val entry=refusalFinalEntry(receipt);frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && entry.recallRefusalFinalSubmission.contains(IoSubmission.Accepted) &&
      entry.recallRefusalFinalWorkerRetired && entry.root.isClosed && entry.journalWork.isEmpty &&
      entry.journalReservation.isEmpty && entry.journalObservation.isEmpty && entry.journalConfirmation.isEmpty &&
      entry.journalPreCaptureExclusion.isEmpty && entry.recallRefusalWork.isEmpty &&
      entry.recallRefusalObservation.isEmpty && entry.recallRefusalConfirmation.isEmpty && entry.bootTakeover.isEmpty &&
      entry.bootMainDecision.isEmpty && entry.captureReservation.isEmpty && entry.captureJournalLink.isEmpty,
      "actual Accepted original refusal worker retirement required")
  }
  /** adapter须先核实际taken exact final ACK；核心不把结果DATA当ACK。 */
  private[mysql] def acknowledgeRefusalFinal(receipt:MySqlDbFirstRecallRefusalFinalReceipt):Unit = synchronized {
    requireRefusalFinalRetired(receipt)
    require(entries.remove(receipt.tag).exists(_.recallRefusalFinal.exists(_ eq receipt)),"original refusal final Entry changed")
    ()
  }

  /** 原J的business slot继续占位，独立C root在同一账本预收且登记后才创建successor metadata。
    * root可暂有两个，业务Entry仍只有原J一个；RAM上限由同一真实ledger约束。
    */
  private[mysql] def prepareJournalCaptureTransfer(reservation:MySqlDbFirstRecallJournalReservation,
      confirmation:MySqlDbFirstRecallJournalConfirmation):MySqlDbFirstRecallJournalCaptureTransfer = synchronized {
    requireDbFirstRecallJournalConfirmation(confirmation,reservation)
    val entry=journalEntry(reservation)
    require(entry.journalReadAcknowledged && !entry.journalPreCaptureExclusionAttempted && !entry.journalCaptureTransferAttempted &&
      !entries.contains(reservation.originalTag),"original Main read ACK and untransferred C responsibility required")
    entry.journalCaptureTransferAttempted=true
    try
      val inputs=dbFirstInputs.get
      val root=MySqlFormat3ReadBudget.retainedDbFirstRootOnLedger(inputs,
        Math.min(profile.maxSqlPacketBytes(),inputs.budget.packetBytes),ledger,imageHandoffCapability,worker)
      entry.journalCaptureRoot=Some(root) // 分配或metadata失败也不丢失新root custody。
      root.reserveRetainedMemory(512L+MySqlDbFirstRecallJournalCaptureTransfer.HolderBytes,
        "original journal/C transfer and successor metadata BEFORE allocation")
      val original=reservation.request
      def copyUuid(id:UUID)=new UUID(id.getMostSignificantBits,id.getLeastSignificantBits)
      def copyTag(tag:IoOperationTag)=IoOperationTag(copyUuid(tag.bootId),tag.bootGeneration,tag.connectionGeneration,copyUuid(tag.operationId))
      val c=copyTag(original.originalTag)
      val link=MySqlDbFirstRecallJournalLink(copyTag(original.journalTag),copyUuid(original.actorUuid),
        original.clientRequestUuid.map(copyUuid),Vector.tabulate(32)(original.canonical.sha256(_)))
      link.requireFor(c,Some(link.actorUuid))
      nextSerial=Math.addExact(nextSerial,1L)
      val handle=new MySqlRetainedPlanHandle(ownerIssuer.orNull,nextSerial,c,this)
      val successor=new Entry(handle,root,MySqlRetainedPlanReleaseKind.Ordinary)
      entry.journalCaptureSuccessor=Some(successor)
      val capture=new MySqlDbFirstCaptureReservation(this,handle,profile,DbFirstPodSnapshot.Purpose.RECALL)
      successor.captureReservation=Some(capture);successor.captureJournalLink=Some(link)
      val transfer=new MySqlDbFirstRecallJournalCaptureTransfer(this,entry.handle,capture,
        copyUuid(original.bindingUuid),link.actorUuid,copyUuid(original.podUuid),original.expectedStateVersion)
      successor.captureJournalTransfer=Some(transfer);entry.journalCaptureTransfer=Some(transfer)
      transfer
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private def journalTransferEntry(transfer:MySqlDbFirstRecallJournalCaptureTransfer):Entry =
    requireLive()
    require(transfer!=null && (transfer.core eq this),"original journal/C transfer required")
    val entry=current(transfer.handle)
    require(entry.journalCaptureTransfer.exists(_ eq transfer) && entry.journalCaptureSuccessor.exists(s=>
      s.captureReservation.exists(_ eq transfer.capture) && s.captureJournalTransfer.exists(_ eq transfer) &&
        entry.journalCaptureRoot.exists(_ eq s.root)) && entry.journalOriginalTag.contains(transfer.originalTag),
      "exact registered journal/C transfer and charged successor required")
    entry
  private[mysql] def requireJournalCaptureTransferPayload(transfer:MySqlDbFirstRecallJournalCaptureTransfer,
      runner:MySqlDbFirstRecallJournalTransaction):Unit = synchronized {
    workerOwner();val entry=journalTransferEntry(transfer);frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && entry.journalReadAcknowledged && entry.journalCaptureWorkerStarted &&
      !entry.journalCaptureWorkerRetired && !entry.journalCaptureRootCloseAttempted && runner!=null &&
      entry.journalWork.exists(_.retainedTransaction eq runner),"original started worker journal/C retirement required")
    runner.requireConfirmedObservation(entry.journalObservation.get)
  }
  private[mysql] def requireJournalCaptureTransferSubmission(transfer:MySqlDbFirstRecallJournalCaptureTransfer):Unit = synchronized {
    mainOwner();val entry=journalTransferEntry(transfer);frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && !entry.journalCaptureSubmissionAttempted && !entry.journalCaptureWorkerStarted,
      "original journal/C control submission is one-shot")
    entry.journalCaptureSubmissionAttempted=true
  }
  private[mysql] def submittedJournalCaptureTransfer(transfer:MySqlDbFirstRecallJournalCaptureTransfer,result:IoSubmission):Unit = synchronized {
    mainOwner();val entry=journalTransferEntry(transfer)
    require(entry.journalCaptureSubmissionAttempted && entry.journalCaptureSubmission.isEmpty && result!=null,
      "actual original journal/C submission result required once")
    entry.journalCaptureSubmission=Some(result)
    if result!=IoSubmission.Accepted && entry.journalCaptureWorkerStarted then
      val cause=new IllegalStateException("rejected journal/C control was already observed by worker")
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
      throw cause
    // Rejected也继续保管原J/C、两root及原槽，不把拒绝当作退休完成。
  }
  private[mysql] def submissionUnknownJournalCaptureTransfer(transfer:MySqlDbFirstRecallJournalCaptureTransfer,cause:Throwable):Unit = synchronized {
    mainOwner();val entry=journalTransferEntry(transfer)
    require(entry.journalCaptureSubmissionAttempted && cause!=null,"actual journal/C submission uncertainty required")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
    frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
  }
  private[mysql] def requireJournalCaptureTransferRetired(transfer:MySqlDbFirstRecallJournalCaptureTransfer):Unit = synchronized {
    mainOwner();val entry=journalTransferEntry(transfer);frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && entry.journalCaptureSubmission.contains(IoSubmission.Accepted) &&
      entry.journalCaptureWorkerRetired && entry.root.isClosed && entry.journalWork.isEmpty &&
      entry.journalReservation.isEmpty && entry.journalObservation.isEmpty && entry.journalConfirmation.isEmpty,
      "actual Accepted original journal/C worker retirement required")
  }
  private[mysql] def journalCaptureTransferFor(reservation:MySqlDbFirstCaptureReservation):Option[MySqlDbFirstRecallJournalCaptureTransfer] = synchronized {
    mainOwner();val entry=captureEntry(reservation);frozen.foreach(throw _);entry.cause.foreach(throw _)
    entry.captureJournalTransfer
  }
  private[mysql] def classifyJournalCaptureTransferFailure(transfer:MySqlDbFirstRecallJournalCaptureTransfer,cause:Throwable):Throwable = synchronized {
    workerOwner();val entry=journalTransferEntry(transfer)
    require(cause!=null,"actual journal/C worker failure required")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
    if entry.journalCaptureRetirementStage==MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight &&
        scala.util.control.NonFatal(cause) && frozen.isEmpty then
      MySqlDbFirstJournalCaptureLocalRetirementException.fromCore(this,transfer,cause)
    else
      frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
      cause
  }
  private[mysql] def requireJournalCaptureLocalRetirementFailure(transfer:MySqlDbFirstRecallJournalCaptureTransfer,cause:Throwable):Unit = synchronized {
    workerOwner();val entry=journalTransferEntry(transfer)
    require(cause!=null && scala.util.control.NonFatal(cause) && entry.cause.exists(_ eq cause) &&
      entry.journalCaptureWorkerStarted && !entry.journalCaptureRootCloseAttempted && frozen.isEmpty &&
      entry.journalCaptureRetirementStage==MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight &&
      entry.journalWork.exists(_.retainedTransaction.resourcesRetired),
      "only original SQL-retired journal/C preflight failure is local")
  }
  private[mysql] def retireJournalCaptureTransfer(transfer:MySqlDbFirstRecallJournalCaptureTransfer):Unit = synchronized {
    workerOwner();val entry=journalTransferEntry(transfer);frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && entry.journalReadAcknowledged && entry.journalCaptureSubmissionAttempted &&
      entry.journalCaptureSubmission.forall(_==IoSubmission.Accepted) && !entry.journalCaptureWorkerStarted &&
      !entry.root.isClosed && entry.journalCaptureRoot.exists(!_.isClosed),
      "original healthy journal/C worker retirement is one-shot")
    entry.journalCaptureWorkerStarted=true
    try
      val runner=entry.journalWork.get.retainedTransaction
      requireJournalCaptureTransferPayload(transfer,runner)
      entry.journalCaptureRetirementStage=MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight
      require(!entry.root.hasActiveImmutableBorrows && entry.root.retainedCanonicalDataChildCount==0,
        "original confirmed journal still has immutable borrowers/DATA children")
      runner.retireEntryPayload(transfer)
      require(runner.payloadRetired && runner.resourcesRetired,"actual original journal payload/SQL must retire")
      entry.journalWork=None;entry.journalObservation=None;entry.journalConfirmation=None;entry.journalReservation=None
      entry.journalCaptureRootCloseAttempted=true
      entry.journalCaptureRetirementStage=MySqlDbFirstRetirementFailure.Stage.RootCloseAttempted
      entry.root.closeRootReadBudget()
      entry.journalCaptureWorkerRetired=true
      // J Entry及原C别名仍在；worker不能清掉Main尚未消费的转移责任。
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      if entry.journalCaptureRootCloseAttempted then frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
      throw cause
  }
  /** Main实际消费worker ACK后先发布不可capture的C；router迁移原slot成功后再activate。 */
  private[mysql] def publishJournalCaptureTransfer(transfer:MySqlDbFirstRecallJournalCaptureTransfer):Unit = synchronized {
    requireJournalCaptureTransferRetired(transfer)
    mainOwner();val entry=journalTransferEntry(transfer);frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && entry.journalCaptureWorkerRetired && entry.root.isClosed &&
      entry.journalWork.isEmpty && entry.journalReservation.isEmpty && entry.journalObservation.isEmpty &&
      entry.journalConfirmation.isEmpty && !entries.contains(transfer.originalTag),
      "exact retired journal worker ACK required before C publication")
    val successor=entry.journalCaptureSuccessor.get
    require(!successor.root.isClosed && !successor.captureJournalActivated,"live inactive C successor required")
    try
      // put先于remove；即使内存容器更新异常，也不出现原C无人占位的状态。
      entries.put(transfer.originalTag,successor)
      require(entries.remove(transfer.tag).exists(_ eq entry),"original J registration changed during C publication")
      entry.journalCaptureSuccessor=None;entry.journalCaptureRoot=None;entry.journalCaptureTransfer=None
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      successor.cause=MySqlOwnedSessionLifecycle.combine(successor.cause,cause)
      frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
      throw cause
  }
  private[mysql] def activateJournalCaptureTransfer(transfer:MySqlDbFirstRecallJournalCaptureTransfer):MySqlDbFirstCaptureReservation = synchronized {
    requireLive();mainOwner();frozen.foreach(throw _)
    require(transfer!=null && (transfer.core eq this),"original journal/C activation required")
    val entry=current(transfer.capture.handle);entry.cause.foreach(throw _)
    require(!stopped && entry.captureJournalTransfer.exists(_ eq transfer) &&
      entry.captureReservation.exists(_ eq transfer.capture) && entry.captureJournalLink.nonEmpty &&
      !entry.captureJournalActivated && !entry.root.isClosed && !entries.contains(transfer.tag),
      "published original C requires one Main routing ACK before capture activation")
    entry.captureJournalActivated=true
    transfer.capture
  }
  private[mysql] def failJournalCaptureTransfer(transfer:MySqlDbFirstRecallJournalCaptureTransfer,cause:Throwable):Unit = synchronized {
    requireLive()
    require((Thread.currentThread() eq worker) || main.exists(_ eq Thread.currentThread()),"original transfer actor required")
    require(transfer!=null && (transfer.core eq this) && cause!=null,"actual original transfer failure required")
    val entry=entries.get(transfer.tag).filter(_.journalCaptureTransfer.exists(_ eq transfer))
      .orElse(entries.get(transfer.originalTag).filter(_.captureJournalTransfer.exists(_ eq transfer)))
      .getOrElse(throw new IllegalArgumentException("unregistered original transfer failure"))
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
  }
  def recordRejectedJournalDrop(reservation:MySqlDbFirstRecallJournalReservation):MySqlDbFirstRejectedJournalReceipt = synchronized {
    mainOwner();val entry=journalEntry(reservation);frozen.foreach(throw _)
    require(!stopped && entry.explicitlyRejected && entry.journalSubmissionRecorded && !entry.accepted &&
      !entry.workerObserved && entry.journalWork.isEmpty && entry.cause.isEmpty && entry.journalRejectedDrop.isEmpty,
      "only explicitly rejected unobserved original journal can detach")
    val receipt=new MySqlDbFirstRejectedJournalReceipt(this,entry.handle)
    entry.journalRejectedDrop=Some(receipt);entry.dropped=true
    receipt
  }
  def cancelRejectedJournal(receipt:MySqlDbFirstRejectedJournalReceipt):Unit = synchronized {
    requireLive();mainOwner();frozen.foreach(throw _)
    require(receipt!=null && (receipt.core eq this),"original rejected journal receipt required")
    val entry=current(receipt.handle)
    require(!stopped && entry.journalRejectedDrop.exists(_ eq receipt) && entry.dropped && entry.explicitlyRejected &&
      !entry.accepted && !entry.workerObserved && entry.journalWork.isEmpty && entry.cause.isEmpty,
      "actual unaccepted/unobserved journal retirement required")
    try
      entry.journalReservation=None
      entry.root.closeRootReadBudget()
      entries.remove(receipt.tag)
      ()
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
      throw cause
  }

  def reserveDbFirstInitialPreparation(tag:IoOperationTag,request:MySqlControlledDebugPrepareRequest,
      activeLimit:Int):MySqlDbFirstInitialPreparationReservation = synchronized {
    requireLive();mainOwner();identity.requireTag(tag)
    require(dbFirstInputs.nonEmpty && request != null && activeLimit>=1 && activeLimit<=256,
      "actual schema6 preparation request and configured capacity required")
    frozen.foreach(throw _)
    require(!stopped && tagAvailable(tag) && entries.size<capacity,"preparation capacity/identity unavailable; no remint")
    val inputs=dbFirstInputs.get
    val root=MySqlFormat3ReadBudget.retainedDbFirstRootOnLedger(inputs,
      Math.min(profile.maxSqlPacketBytes(),inputs.budget.packetBytes),ledger,imageHandoffCapability,worker)
    nextSerial=Math.addExact(nextSerial,1L)
    val handle=new MySqlRetainedPlanHandle(ownerIssuer.orNull,nextSerial,tag,this)
    val entry=new Entry(handle,root,MySqlRetainedPlanReleaseKind.Ordinary)
    entries.put(tag,entry)
    try
      root.reserveRetainedMemory(1152L,"initial preparation Entry/reservation/rejected receipt BEFORE allocation")
      val reservation=new MySqlDbFirstInitialPreparationReservation(this,handle,request,activeLimit)
      entry.preparationReservation=Some(reservation)
      // 原operation DATA编码，不发行旧计划；容量和tag由原reservation固定，后续完整scope仍须编码。
      entry.preparationBacking=Some(MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        MySqlRepositoryRequestIdentity.single(request.repositoryOperation,profile)
      })
      reservation
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }

  def recordRejectedPreparationDrop(reservation:MySqlDbFirstInitialPreparationReservation):MySqlDbFirstRejectedPreparationReceipt = synchronized {
    requireLive();mainOwner();frozen.foreach(throw _)
    val entry=preparationEntry(reservation)
    require(!stopped && entry.explicitlyRejected && entry.preparationSubmissionRecorded && !entry.accepted &&
      !entry.workerObserved && entry.preparationWork.isEmpty && entry.cause.isEmpty && entry.preparationRejectedDrop.isEmpty,
      "only original explicitly rejected unobserved preparation may detach")
    val receipt=new MySqlDbFirstRejectedPreparationReceipt(this,entry.handle)
    entry.preparationRejectedDrop=Some(receipt);entry.dropped=true
    receipt
  }
  def cancelRejectedPreparation(receipt:MySqlDbFirstRejectedPreparationReceipt):Unit = synchronized {
    requireLive();mainOwner();frozen.foreach(throw _)
    require(receipt!=null && (receipt.core eq this),"original rejected preparation receipt required")
    val entry=current(receipt.handle)
    require(!stopped && entry.preparationRejectedDrop.exists(_ eq receipt) && entry.dropped && entry.explicitlyRejected &&
      !entry.accepted && !entry.workerObserved && entry.preparationWork.isEmpty && entry.cause.isEmpty,
      "original unaccepted unobserved preparation required")
    try
      entry.preparationReservation=None;entry.preparationBacking=None
      entry.root.closeRootReadBudget() // 未bind worker，仍由实际Main关闭其原分配。
      entries.remove(receipt.tag)
      ()
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
      throw cause
  }

  private def preparationEntry(reservation:MySqlDbFirstInitialPreparationReservation):Entry =
    requireLive()
    require(reservation != null && (reservation.core eq this),"original preparation core required")
    val entry=current(reservation.handle)
    require(dbFirstInputs.nonEmpty && entry.preparationReservation.exists(_ eq reservation) &&
      entry.preparationBacking.nonEmpty && entry.plan.isEmpty && entry.captureReservation.isEmpty &&
      !entry.root.isClosed && !entry.dropped,"original live preparation Entry required")
    entry

  def requireDbFirstInitialPreparationSubmission(reservation:MySqlDbFirstInitialPreparationReservation):Unit = synchronized {
    mainOwner();val entry=preparationEntry(reservation)
    frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && !entry.preparationSubmissionAttempted && !entry.workerObserved && !entry.accepted,
      "initial preparation submission is one-shot")
    entry.preparationSubmissionAttempted=true
  }
  def submittedDbFirstInitialPreparation(reservation:MySqlDbFirstInitialPreparationReservation,result:IoSubmission):Unit = synchronized {
    mainOwner();val entry=preparationEntry(reservation)
    require(entry.preparationSubmissionAttempted && !entry.preparationSubmissionRecorded && result != null,
      "original preparation submission result required")
    entry.preparationSubmissionRecorded=true
    if result == IoSubmission.Accepted then
      entry.accepted=true;entry.explicitlyRejected=false
    else
      require(!entry.workerObserved && !entry.accepted,"observed preparation cannot become rejected")
      entry.explicitlyRejected=true
  }
  def submissionUnknownDbFirstInitialPreparation(reservation:MySqlDbFirstInitialPreparationReservation,cause:Throwable):Unit = synchronized {
    mainOwner();val entry=preparationEntry(reservation)
    require(entry.preparationSubmissionAttempted && cause != null,"original preparation submission uncertainty required")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
    frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
  }
  private[mysql] def beginDbFirstInitialPreparation(reservation:MySqlDbFirstInitialPreparationReservation):MySqlDbFirstInitialPreparationWork = synchronized {
    workerOwner();val entry=preparationEntry(reservation)
    frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && entry.preparationSubmissionAttempted && !entry.explicitlyRejected &&
      !entry.workerObserved && entry.preparationWork.isEmpty,"original preparation worker entry is one-shot")
    try
      entry.root.bindRetainedImageWorker(imageHandoffCapability)
      entry.root.reserveRetainedMemory(MySqlDbFirstInitialPreparationWork.HolderBytes,"initial preparation worker holder BEFORE allocation")
      val work=new MySqlDbFirstInitialPreparationWork(reservation,entry.root,entry.preparationBacking.get)
      entry.preparationWork=Some(work)
      entry.workerObserved=true;entry.accepted=true
      work
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private[mysql] def requireExecutingDbFirstInitialPreparation(work:MySqlDbFirstInitialPreparationWork):Unit = synchronized {
    workerOwner();require(work != null,"registered preparation work required")
    val entry=preparationEntry(work.reservation)
    frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && entry.preparationWork.exists(_ eq work) && entry.workerObserved && entry.accepted,
      "original preparation worker responsibility required")
  }
  /** 已失败Work只能清理原责任，不能重新取得执行或成功确认资格。 */
  private[mysql] def requireRejectedPreparationCleanup(work:MySqlDbFirstInitialPreparationWork,
      cause:MySqlDbFirstPreparationRejected):Unit = synchronized {
    workerOwner();require(work!=null && cause!=null,"original rejected preparation required")
    val entry=preparationEntry(work.reservation)
    frozen.foreach(throw _)
    require(!stopped && !entry.dropped && entry.preparationWork.exists(_ eq work) &&
      entry.workerObserved && entry.accepted && entry.cause.exists(_ eq cause) && cause.getSuppressed.isEmpty,
      "exact retained rejected worker responsibility required")
  }
  private[mysql] def requireRejectedPayloadRetirement(work:MySqlDbFirstInitialPreparationWork,
      cause:MySqlDbFirstPreparationRejected):Unit = synchronized {
    workerOwner();require(work!=null && cause!=null,"original rejected payload required")
    val entry=current(work.reservation.handle)
    frozen.foreach(throw _)
    require(!stopped && entry.dropped && entry.preparationReadReleased && entry.preparationReleaseAttempted &&
      entry.preparationRootCloseAttempted && entry.preparationWork.exists(_ eq work) &&
      entry.preparationRejectionConfirmation.exists(_.cause eq cause) && entry.cause.exists(_ eq cause) &&
      cause.getSuppressed.isEmpty,"actual original rejected payload retirement required")
  }
  private[mysql] def failDbFirstInitialPreparation(work:MySqlDbFirstInitialPreparationWork,cause:Throwable):Unit = synchronized {
    workerOwner();require(work != null && cause != null,"original preparation failure required")
    val entry=preparationEntry(work.reservation)
    require(entry.preparationWork.exists(_ eq work),"foreign preparation failure")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
  }

  def claimDbFirstInitialPreparationConfirmation(reservation:MySqlDbFirstInitialPreparationReservation,
      observation:MySqlDbFirstSession.DbFirstInitialPreparationObservation):MySqlDbFirstInitialPreparationConfirmation = synchronized {
    mainOwner()
    val entry=preparationEntry(reservation)
    frozen.foreach(throw _)
    require(!stopped && entry.cause.isEmpty && !entry.dropped && entry.preparationConfirmation.isEmpty,
      "original healthy preparation confirmation is one-shot")
    val work=entry.preparationWork.getOrElse(throw new IllegalStateException("original preparation work missing"))
    work.retainedTransaction.requireConfirmedObservation(observation)
    entry.preparationObservation=Some(observation)
    try
      // 固定确认槽已由原worker预扣；Main不改worker root账本。
      val confirmation=MySqlDbFirstInitialPreparationConfirmation.issue(this,entry.handle,reservation.request)
      entry.preparationConfirmation=Some(confirmation)
      confirmation
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  def claimPreparationRejection(reservation:MySqlDbFirstInitialPreparationReservation,
      observation:MySqlDbFirstSession.DbFirstInitialPreparationRejection):MySqlDbFirstPreparationRejectionConfirmation = synchronized {
    mainOwner();val entry=preparationEntry(reservation);frozen.foreach(throw _)
    require(!stopped && !entry.dropped && entry.accepted && entry.workerObserved &&
      entry.preparationRejectionConfirmation.isEmpty && entry.preparationConfirmation.isEmpty &&
      observation!=null && observation.tag==reservation.tag && observation.podUuid==reservation.request.podUuid &&
      observation.cause!=null && entry.cause.exists(_ eq observation.cause),"original rejected preparation confirmation is one-shot")
    entry.preparationWork.get.retainedTransaction.requireSettledRejection(observation)
    val confirmation=MySqlDbFirstPreparationRejectionConfirmation.issue(this,entry.handle,observation)
    entry.preparationRejectionConfirmation=Some(confirmation)
    confirmation
  }
  private[mysql] def requirePreparationRejectionConfirmation(confirmation:MySqlDbFirstPreparationRejectionConfirmation,
      reservation:MySqlDbFirstInitialPreparationReservation):Unit = synchronized {
    mainOwner();val entry=preparationEntry(reservation);frozen.foreach(throw _)
    require(confirmation!=null && (confirmation.core eq this) && (confirmation.handle eq entry.handle) &&
      entry.preparationRejectionConfirmation.exists(_ eq confirmation) && !stopped && !entry.dropped &&
      entry.accepted && entry.workerObserved && entry.cause.exists(_ eq confirmation.cause),
      "exact registered rejection confirmation required")
    entry.preparationWork.get.retainedTransaction.requireSettledRejection(confirmation.observation)
  }

  private def rejectedPreparationEntry(confirmation:MySqlDbFirstPreparationRejectionConfirmation):Entry =
    require(confirmation!=null && (confirmation.core eq this),"original rejection confirmation required")
    val entry=current(confirmation.handle)
    frozen.foreach(throw _)
    require(!stopped && entry.accepted && entry.workerObserved &&
      entry.preparationRejectionConfirmation.exists(_ eq confirmation) && entry.cause.exists(_ eq confirmation.cause),
      "registered original settled rejection required")
    entry.preparationWork.get.retainedTransaction.requireSettledRejection(confirmation.observation)
    entry

  private[mysql] def beginRejectedPreparationReadRelease(confirmation:MySqlDbFirstPreparationRejectionConfirmation):Unit = synchronized {
    requireLive();workerOwner()
    val entry=rejectedPreparationEntry(confirmation)
    require(!entry.dropped && !entry.preparationReadReleaseStarted,"rejected read release is one-shot")
    entry.preparationReadReleaseStarted=true
  }
  private[mysql] def finishRejectedPreparationReadRelease(confirmation:MySqlDbFirstPreparationRejectionConfirmation):Unit = synchronized {
    workerOwner();val entry=rejectedPreparationEntry(confirmation)
    require(!entry.dropped && entry.preparationReadReleaseStarted && !entry.preparationReadReleased,
      "original started rejected read release required")
    entry.preparationReadReleased=true
  }
  private[mysql] def requireRejectedPreparationReadReleased(confirmation:MySqlDbFirstPreparationRejectionConfirmation,
      reservation:MySqlDbFirstInitialPreparationReservation):Unit = synchronized {
    requirePreparationRejectionConfirmation(confirmation,reservation)
    require(preparationEntry(reservation).preparationReadReleased,"actual rejected worker read retirement required")
  }
  private[floatingpod] def detachRejectedPreparation(confirmation:MySqlDbFirstPreparationRejectionConfirmation):Unit = synchronized {
    requireLive();mainOwner();val entry=rejectedPreparationEntry(confirmation)
    require(!entry.dropped && entry.preparationReadReleased,"rejected Main detach requires actual worker read retirement")
    entry.dropped=true
  }
  private[mysql] def retireRejectedPreparation(confirmation:MySqlDbFirstPreparationRejectionConfirmation):Unit = synchronized {
    requireLive();workerOwner();val entry=rejectedPreparationEntry(confirmation)
    require(entry.dropped && entry.preparationReadReleased && !entry.preparationReleaseAttempted,
      "original rejected Main detach is one-shot")
    entry.preparationReleaseAttempted=true
    try
      require(!entry.root.hasActiveImmutableBorrows && entry.root.retainedCanonicalDataChildCount==0,
        "rejected Entry still has immutable borrowers or DATA children")
      entry.preparationRootCloseAttempted=true
      entry.preparationWork.get.retainedTransaction.retireRejectedEntryPayload(confirmation.observation)
      entry.preparationWork=None;entry.preparationBacking=None;entry.preparationReservation=None
      entry.preparationRejectionConfirmation=None
      entry.root.closeRootReadBudget()
      entries.remove(confirmation.tag)
      ()
    catch case cleanup:Throwable=>
      MySqlOwnedSessionLifecycle.suppress(confirmation.cause,cleanup)
      frozen=MySqlOwnedSessionLifecycle.combine(frozen,confirmation.cause)
      throw confirmation.cause
  }

  private[mysql] def beginPreparationReadRelease(confirmation:MySqlDbFirstInitialPreparationConfirmation):Unit = synchronized {
    requireLive();workerOwner();frozen.foreach(throw _)
    require(confirmation!=null && (confirmation.core eq this),"original preparation confirmation required")
    val entry=current(confirmation.handle)
    require(entry.preparationConfirmation.exists(_ eq confirmation) && entry.cause.isEmpty && !entry.dropped &&
      !stopped && !entry.preparationReadReleaseStarted,"original healthy preparation read release is one-shot")
    entry.preparationWork.get.retainedTransaction.requireConfirmedObservation(entry.preparationObservation.get)
    entry.preparationReadReleaseStarted=true
  }
  private[mysql] def finishPreparationReadRelease(confirmation:MySqlDbFirstInitialPreparationConfirmation):Unit = synchronized {
    workerOwner();val entry=current(confirmation.handle)
    require((confirmation.core eq this) && entry.preparationConfirmation.exists(_ eq confirmation) &&
      entry.preparationReadReleaseStarted && !entry.preparationReadReleased && entry.cause.isEmpty,
      "original started preparation read release required")
    entry.preparationReadReleased=true
  }
  private[mysql] def requirePreparationReadReleased(confirmation:MySqlDbFirstInitialPreparationConfirmation,
      reservation:MySqlDbFirstInitialPreparationReservation):Unit = synchronized {
    requireDbFirstPreparationConfirmation(confirmation,reservation)
    require(preparationEntry(reservation).preparationReadReleased,"original worker preparation read retirement required")
  }

  private[mysql] def beginCheckpointReadRelease(confirmation:MySqlDbFirstCheckpointConfirmation):Unit = synchronized {
    requireLive();workerOwner();frozen.foreach(throw _)
    require(confirmation!=null && (confirmation.core eq this),"original checkpoint confirmation required")
    val entry=current(confirmation.handle)
    require(entry.checkpointConfirmation.exists(_ eq confirmation) && entry.cause.isEmpty && !entry.dropped &&
      !stopped && !entry.checkpointReadReleaseStarted,"original healthy checkpoint read release is one-shot")
    entry.checkpointRunner.get.requireConfirmedObservation(entry.checkpointObservation.get)
    entry.checkpointReadReleaseStarted=true
  }
  private[mysql] def finishCheckpointReadRelease(confirmation:MySqlDbFirstCheckpointConfirmation):Unit = synchronized {
    workerOwner();val entry=current(confirmation.handle)
    require((confirmation.core eq this) && entry.checkpointConfirmation.exists(_ eq confirmation) &&
      entry.checkpointReadReleaseStarted && !entry.checkpointReadReleased && entry.cause.isEmpty,
      "original started checkpoint read release required")
    entry.checkpointReadReleased=true
  }
  private[mysql] def requireCheckpointReadReleased(confirmation:MySqlDbFirstCheckpointConfirmation,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    requireDbFirstCheckpointConfirmation(confirmation,reservation)
    require(checkpointEntry(reservation).checkpointReadReleased,"original worker checkpoint read retirement required")
  }

  private[mysql] def acknowledgeCheckpointReadReleased(confirmation:MySqlDbFirstCheckpointConfirmation,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    requireCheckpointReadReleased(confirmation,reservation)
    val entry=checkpointEntry(reservation)
    require(!entry.checkpointReadAcknowledged,"original checkpoint read ACK is one-shot")
    entry.checkpointReadAcknowledged=true
  }

  private[mysql] def preparationConfirmedData(confirmation:MySqlDbFirstInitialPreparationConfirmation,
      reservation:MySqlDbFirstInitialPreparationReservation):MySqlDbFirstInitialPreparationDataView = synchronized {
    requireDbFirstPreparationConfirmation(confirmation,reservation)
    val entry=preparationEntry(reservation)
    entry.preparationWork.getOrElse(throw new IllegalStateException("original preparation work missing"))
      .retainedTransaction.confirmedData(entry.preparationObservation.getOrElse(
        throw new IllegalStateException("original preparation observation missing")))
  }

  private[mysql] def requireDbFirstPreparationConfirmation(confirmation:MySqlDbFirstInitialPreparationConfirmation,
      reservation:MySqlDbFirstInitialPreparationReservation):Unit = synchronized {
    mainOwner()
    val entry=preparationEntry(reservation)
    frozen.foreach(throw _)
    require(confirmation!=null && (confirmation.core eq this) && (confirmation.handle eq entry.handle) &&
      entry.preparationConfirmation.exists(_ eq confirmation) && !entry.dropped && entry.cause.isEmpty && !stopped,
      "original Main preparation confirmation reference required")
  }

  private[mysql] def requireExecutingPreparationDraft(draft:MySqlDbFirstInitialPreparationDraft):Unit = synchronized {
    workerOwner();require(draft != null,"registered preparation draft required")
    val entry=preparationEntry(draft.reservation)
    val work=entry.preparationWork.getOrElse(throw new IllegalStateException("original preparation work absent"))
    requireExecutingDbFirstInitialPreparation(work)
    require(work.ownsDraft(draft) && (draft.root eq entry.root),"original registered preparation draft/root required")
  }

  private[mysql] def requirePreparationDraftSource(draft:MySqlDbFirstInitialPreparationDraft,
      source:MySqlScriptSchemaSnapshot):Unit = synchronized {
    requireExecutingPreparationDraft(draft)
    val work=preparationEntry(draft.reservation).preparationWork.get
    require(source != null && (work.validatedDataSource eq source),"exact original preparation preflight source required")
  }

  private[mysql] def requireExecutingPreparationTransition(transition:MySqlDbFirstInitialPreparationTransition):Unit = synchronized {
    workerOwner();require(transition != null,"registered preparation transition required")
    val work=transition.work
    requireExecutingDbFirstInitialPreparation(work)
    require(work.ownsTransition(transition),"original registered preparation transition required")
    requireExecutingPreparationDraft(transition.draft)
  }

  private var bootRootConstructionIntent:MySqlDbFirstBootRecallTakeoverIntent=null
  private[mysql] def requireBootTakeoverRootConstruction(intent:MySqlDbFirstBootRecallTakeoverIntent,
      inputs:MySqlScriptSchemaInputs,actualLedger:MySqlFormat3AllocationLedger,capability:AnyRef,actualWorker:Thread):Unit = synchronized {
    workerOwner()
    require(intent!=null && (bootRootConstructionIntent eq intent) && dbFirstInputs.exists(_ eq inputs) &&
      (actualLedger eq ledger) && (capability eq imageHandoffCapability) && (actualWorker eq worker) && !stopped,
      "actual core private Boot root construction required")
  }
  private[mysql] def beginBootRecallTakeover(intent:MySqlDbFirstBootRecallTakeoverIntent):MySqlDbFirstBootRecallTakeoverWork = synchronized {
    requireLive();workerOwner();require(intent!=null && dbFirstInputs.nonEmpty,"actual schema6 Boot takeover required")
    identity.requireTag(intent.executionTag);identity.requireTag(intent.controlTag)
    frozen.foreach(throw _)
    require(!stopped && tagAvailable(intent.executionTag) && entries.size<capacity,"original Boot C capacity unavailable; no remint")
    val inputs=dbFirstInputs.get
    require(bootRootConstructionIntent==null,"original Boot root allocation is not reentrant")
    bootRootConstructionIntent=intent
    val root=try MySqlFormat3ReadBudget.retainedBootTakeoverRootOnWorker(inputs,
      Math.min(profile.maxSqlPacketBytes(),inputs.budget.packetBytes),ledger,imageHandoffCapability,worker,this,intent)
      finally bootRootConstructionIntent=null
    nextSerial=Math.addExact(nextSerial,1L)
    val entry=new Entry(new MySqlRetainedPlanHandle(ownerIssuer.orNull,nextSerial,intent.executionTag,this),root,MySqlRetainedPlanReleaseKind.Ordinary)
    entries.put(intent.executionTag,entry)
    try
      root.reserveRetainedMemory(8192L,"inactive Boot takeover Entry/work BEFORE allocation")
      val work=new MySqlDbFirstBootRecallTakeoverWork(intent,root,worker)
      entry.bootTakeover=Some(work)
      work
    catch case cause:Throwable=>
      entry.cause=Some(cause)
      throw cause
  }
  private[mysql] def finishBootRecallTakeover(work:MySqlDbFirstBootRecallTakeoverWork):MySqlDbFirstCaptureReservation = synchronized {
    workerOwner();require(work!=null,"actual Boot work required")
    val entry=entries.getOrElse(work.intent.executionTag,throw new IllegalArgumentException("Boot C Entry missing"))
    require(entry.bootTakeover.exists(_ eq work) && !entry.bootTakeoverPrepared && entry.captureReservation.isEmpty &&
      entry.cause.isEmpty && !stopped,"same inactive Boot C Entry required")
    val link=work.requireCompleted()
    entry.root.reserveRetainedMemory(512L,"inactive Boot C reservation BEFORE allocation")
    val reservation=new MySqlDbFirstCaptureReservation(this,entry.handle,profile)
    entry.captureJournalLink=Some(link);entry.captureReservation=Some(reservation);entry.bootTakeoverPrepared=true
    reservation
  }
  private[mysql] def failBootRecallTakeover(intent:MySqlDbFirstBootRecallTakeoverIntent,cause:Throwable):Unit = synchronized {
    workerOwner();require(intent!=null && cause!=null,"actual Boot takeover failure required")
    entries.get(intent.executionTag).foreach(entry=>entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause))
  }
  private[mysql] def requireBootRecallTakeoverResult(intent:MySqlDbFirstBootRecallTakeoverIntent,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    mainOwner();require(intent!=null && reservation!=null,"actual Boot C result required")
    val entry=current(reservation.handle)
    require(entry.bootTakeover.exists(_.intent eq intent) && entry.bootTakeoverPrepared && !entry.bootCaptureActivated &&
      entry.captureReservation.exists(_ eq reservation) && entry.cause.isEmpty && !stopped,
      "same actual inactive Boot C result required")
  }

  /** 预收费/attempt先于Main观察；中途失败仍保原Entry，不重试或替换决定。 */
  private[mysql] def beginBootRecallMainDecision(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    mainOwner();require(owner!=null,"actual Boot Main decision issuer required")
    owner.requireRegisteringBootMainDecision(intent,reservation)
    requireBootRecallTakeoverResult(intent,reservation)
    val entry=current(reservation.handle)
    frozen.foreach(throw _)
    require(!entry.bootMainDecisionAttempted && entry.bootMainDecision.isEmpty && !entry.bootCaptureExcluded &&
      !entry.captureAttempted && entry.plan.isEmpty,"original inactive C Main decision is one-shot")
    entry.bootMainDecisionAttempted=true
    try entry.root.reserveRetainedMemory(1024L,"Boot Main decision/observation metadata BEFORE observation")
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private[mysql] def finishBootRecallMainDecision(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      intent:MySqlDbFirstBootRecallTakeoverIntent,reservation:MySqlDbFirstCaptureReservation,
      reason:Option[RecallRefusalReason]):MySqlDbFirstBootRecallMainDecision = synchronized {
    mainOwner();require(owner!=null && reason!=null && reason.forall(_!=null),"actual Boot decision/reason required")
    owner.requireCompletingBootMainDecision(intent,reservation,reason)
    requireBootRecallTakeoverResult(intent,reservation)
    val entry=current(reservation.handle)
    frozen.foreach(throw _)
    require(entry.bootMainDecisionAttempted && entry.bootMainDecision.isEmpty && !entry.bootCaptureExcluded &&
      !entry.captureAttempted && entry.plan.isEmpty,"same registered inactive Main decision attempt required")
    // 正常拒绝先不可逆封capture；对象构造失败仍不能重新进入capture。
    entry.bootCaptureExcluded=reason.nonEmpty
    try
      val decision=new MySqlDbFirstBootRecallMainDecision(this,intent,reason,entry.handle)
      entry.bootMainDecision=Some(decision)
      decision
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private[mysql] def activateBootRecallCapture(owner:com.wi.floatingpod.server.DbFirstBootActiveRecovery,
      decision:MySqlDbFirstBootRecallMainDecision,reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    mainOwner();require(owner!=null,"actual Boot capture issuer required")
    owner.requireActivatingBootCapture(decision,reservation)
    requireBootRecallMainDecision(decision)
    requireBootRecallTakeoverResult(decision.intent,reservation)
    val entry=current(reservation.handle)
    require(decision.refusalReason.isEmpty && !entry.bootCaptureActivationAttempted && !entry.bootCaptureActivated &&
      !entry.bootCaptureExcluded && !entry.captureAttempted && entry.plan.isEmpty && !entry.accepted,
      "same positive inactive Boot C capture activation required once")
    entry.bootCaptureActivationAttempted=true
    try
      entry.root.reserveRetainedMemory(8192L,"Boot positive Main entry/activation/native END graph BEFORE capture")
      entry.bootCaptureActivated=true
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private[mysql] def requireBootRecallMainDecision(decision:MySqlDbFirstBootRecallMainDecision):Unit = synchronized {
    requireLive()
    require((Thread.currentThread() eq worker) || main.exists(_ eq Thread.currentThread()),"original Boot decision actor required")
    require(decision!=null && (decision.core eq this),"actual registered Boot Main decision required")
    val entry=entries.getOrElse(decision.tag,throw new IllegalArgumentException("original Boot decision Entry missing"))
    frozen.foreach(throw _);entry.cause.foreach(throw _)
    require(!stopped && !entry.dropped && entry.bootMainDecisionAttempted && entry.bootMainDecision.exists(_ eq decision) &&
      entry.bootTakeover.exists(_.intent eq decision.intent) && entry.bootCaptureExcluded==decision.refusalReason.nonEmpty,
      "same original Entry registered Main decision required; equal metadata is not authority")
  }

  def reserveDbFirstCapture(tag: IoOperationTag,
      purpose: DbFirstPodSnapshot.Purpose = DbFirstPodSnapshot.Purpose.RECALL): MySqlDbFirstCaptureReservation = synchronized {
    requireLive(); mainOwner(); identity.requireTag(tag)
    require(purpose != null && (purpose == DbFirstPodSnapshot.Purpose.RECALL || dbFirstInputs.nonEmpty),
      "initial activation/checkpoint capture requires actual schema6 owner")
    frozen.foreach(throw _)
    require(!stopped && tagAvailable(tag) && entries.size < capacity,
      "capture capacity/identity unavailable; no remint")
    val root = dbFirstInputs match
      case Some(inputs) => MySqlFormat3ReadBudget.retainedDbFirstRootOnLedger(inputs,
        Math.min(profile.maxSqlPacketBytes(), inputs.budget.packetBytes), ledger, imageHandoffCapability, worker)
      case None => deploymentInputs match
        case Some(inputs) => MySqlFormat3ReadBudget.retainedDeploymentReceiptRootOnLedger(inputs,
          Math.min(profile.maxSqlPacketBytes(), inputs.budget.packetBytes), ledger, imageHandoffCapability, worker)
        case None => MySqlFormat3ReadBudget.retainedRootOnLedger(
          profile, profile.maxSqlPacketBytes(), ledger, imageHandoffCapability, worker)
    nextSerial = Math.addExact(nextSerial, 1L)
    val handle = new MySqlRetainedPlanHandle(ownerIssuer.orNull, nextSerial, tag, this)
    val entry = new Entry(handle, root, MySqlRetainedPlanReleaseKind.Ordinary)
    entries.put(tag, entry)
    try
      root.reserveRetainedMemory(512L, "capture entry/reservation/allocation fixed ownership")
      val reservation = new MySqlDbFirstCaptureReservation(this, handle, profile, purpose)
      entry.captureReservation = Some(reservation)
      reservation
    catch
      case cause: Throwable => entry.cause = Some(cause); throw cause
  }

  private def captureEntry(reservation: MySqlDbFirstCaptureReservation): Entry =
    requireLive(); mainOwner()
    require(reservation != null && (reservation.core eq this) && (reservation.profile eq profile),
      "foreign capture reservation/core/profile")
    val entry = current(reservation.handle)
    require(entry.captureReservation.exists(_ eq reservation) && entry.plan.isEmpty && !entry.root.isClosed,
      "capture requires its exact original entry; not an SQL plan")
    require(!entry.bootCaptureExcluded,"original Boot Main refusal permanently excludes capture")
    require(entry.bootTakeover.isEmpty || entry.bootCaptureActivated,"actual Boot Main capture activation required")
    require(entry.captureJournalTransfer.isEmpty || entry.captureJournalActivated,
      "original journal/C routing ACK is required before capture")
    entry

  def requireDbFirstCaptureReservation(reservation: MySqlDbFirstCaptureReservation): Unit = synchronized {
    val entry = captureEntry(reservation)
    frozen.foreach(throw _)
    require(!stopped && !entry.captureAttempted && entry.cause.isEmpty,
      "capture reservation stopped/already attempted/faulted")
  }

  /** 在当前读代退出前，将纯业务metadata计入真实新capture Entry；不铸原P confirmation。 */
  def retainRecoveredInitialData(reservation:MySqlDbFirstCaptureReservation,
      data:MySqlDbFirstInitialPreparationDataView):MySqlDbFirstInitialPreparationDataView = synchronized {
    val entry=captureEntry(reservation)
    frozen.foreach(throw _)
    require(!stopped && !entry.captureAttempted && entry.cause.isEmpty && !entry.recoveredInitialDataAttempted &&
      reservation.purpose==DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION && data!=null &&
      data.receipt.origin==UnboundIntentOrigin.DebugItemCreate &&
      data.receipt.observedIdentity.isEmpty && data.receipt.createOperationUuid.nonEmpty &&
      data.state.uuid==data.receipt.podUuid && data.state.stateVersion==0L && data.receipt.preparedVersion==0L &&
      !data.state.isActive && data.state.ownerUuid.isEmpty &&
      data.state.operatingMode==PodOperatingMode.WorldIdle &&
      MySqlControlledDebugWorkflowMaterializer.exactPose(MySqlPreparedUnboundGraph.poseOf(data.state),data.receipt.targetPose),
      "one actual new initial capture and original prepared-zero metadata required")
    entry.recoveredInitialDataAttempted=true
    try
      val text=MySqlFormat3Utf8.byteLength(data.state.worldName)+MySqlFormat3Utf8.byteLength(data.receipt.targetPose.worldName)
      entry.root.reserveRetainedMemory(Math.addExact(4096L,text),"recovered initial Main metadata BEFORE retention")
      data
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }

  def captureDbFirstSnapshot(reservation: MySqlDbFirstCaptureReservation)
      (capture: DbFirstCaptureAllocation => DbFirstPodSnapshot): DbFirstPodSnapshot = synchronized {
    val entry = captureEntry(reservation)
    frozen.foreach(throw _)
    require(!stopped && !entry.captureAttempted && entry.cause.isEmpty && capture != null,
      "capture is unavailable/already attempted; no reuse")
    entry.captureAttempted = true
    final class Allocation extends DbFirstCaptureAllocation:
      var active = true
      var begun = false
      var retained = 0L
      var temporary = 0L
      private def check(): Unit =
        mainOwner()
        require(active && !entry.root.isClosed, "capture allocation has left its original callback")
      private def accounting(body: => Unit): Unit =
        try body
        catch case cause: SQLException =>
          throw new IOException("DB-first shared capture allocation accounting failed", cause)
      override def profile(): SourceLimitProfile = reservation.profile
      override def beginCapture(): Unit =
        check(); require(!begun, "capture allocation already consumed"); begun = true
      override def reserveRetained(bytes: Long, responsibility: String): Unit =
        check(); require(bytes >= 0L && entry.captured.isEmpty, "capture retained ownership already transferred")
        val next = Math.addExact(retained, bytes)
        accounting { entry.root.reserveRetainedMemory(bytes, responsibility) }; retained = next
      override def releaseRetained(bytes: Long, responsibility: String): Unit =
        check(); require(bytes >= 0L && bytes <= retained && entry.captured.isEmpty, "capture retained refund exceeds untransferred owner")
        accounting { entry.root.releaseRetainedMemory(bytes, responsibility) }; retained -= bytes
      override def reserveTemporary(bytes: Long, responsibility: String): Unit =
        check(); require(bytes >= 0L && entry.captured.isEmpty, "capture has transferred")
        val next = Math.addExact(temporary, bytes)
        accounting { entry.root.reserveTemporaryMemory(bytes, responsibility) }; temporary = next
      override def releaseTemporary(bytes: Long, responsibility: String): Unit =
        check(); require(bytes >= 0L && bytes <= temporary, "capture temporary refund exceeds original owner")
        accounting { entry.root.releaseTemporaryMemory(bytes, responsibility) }; temporary -= bytes
      override def retainSnapshot(snapshot: DbFirstPodSnapshot): Unit =
        check()
        require(begun && entry.captured.isEmpty && snapshot != null && snapshot.belongsTo(this) &&
          snapshot.operationUuid() == reservation.tag.operationId && snapshot.purpose() == reservation.purpose &&
          (snapshot.limitProfile() eq reservation.profile) &&
          snapshot.retainedBytes() == retained && temporary == 0L, "capture transfer differs from original charged snapshot")
        entry.captureJournalTransfer.foreach { transfer=>
          require(snapshot.bindingUuid()==transfer.bindingUuid && snapshot.podUuid()==transfer.podUuid &&
            snapshot.ownerUuid()==transfer.actorUuid && snapshot.stateVersion()==transfer.expectedStateVersion,
            "original journal actor/pod/binding/version must match actual capture")
        }
        entry.captured = Some(snapshot)
    val allocation = new Allocation
    var primary: Throwable = null
    try
      val snapshot = capture(allocation)
      require(entry.captured.exists(_ eq snapshot), "callback must return its exact transferred snapshot")
      require(entry.dbFirstRequest.isEmpty && entry.dbFirstRequestBacking.isEmpty &&
        entry.initialActivationRequest.isEmpty && entry.initialActivationBacking.isEmpty &&
        entry.checkpointRequest.isEmpty && entry.checkpointBacking.isEmpty,
        "original capture request cannot be reminted")
      entry.root.reserveTemporaryMemory(128L,"DB-first backing publication callback BEFORE allocation")
      var publicationFailure: Throwable=null
      try
        reservation.purpose match
          case DbFirstPodSnapshot.Purpose.RECALL =>
            entry.dbFirstRequest=Some(MySqlDbFirstRecallRequest.capture(reservation.tag,snapshot,entry.root,entry.captureJournalLink) { backing =>
              require(entry.dbFirstRequestBacking.isEmpty,"original capture backing cannot be replaced")
              entry.dbFirstRequestBacking=Some(backing)
            })
          case DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION =>
            entry.initialActivationRequest=Some(MySqlDbFirstInitialActivationRequest.capture(reservation.tag,snapshot,entry.root) { backing =>
              require(entry.initialActivationBacking.isEmpty,"original initial capture backing cannot be replaced")
              entry.initialActivationBacking=Some(backing)
            })
          case DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT =>
            entry.checkpointRequest=Some(MySqlDbFirstCheckpointRequest.capture(reservation.tag,snapshot,entry.root) { backing =>
              require(entry.checkpointBacking.isEmpty,"original checkpoint backing cannot be replaced")
              entry.checkpointBacking=Some(backing)
            })
      catch case cause: Throwable =>
        publicationFailure=cause
        throw cause
      finally
        try entry.root.releaseTemporaryMemory(128L,"DB-first backing publication callback detached")
        catch case cleanup: Throwable =>
          if publicationFailure == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(publicationFailure,cleanup)
      snapshot
    catch
      case cause: Throwable => primary = cause; entry.cause = Some(cause); throw cause
    finally
      try
        if allocation.temporary != 0L then allocation.releaseTemporary(allocation.temporary, "failed capture temporary owner")
        if entry.captured.isEmpty && allocation.retained != 0L then
          allocation.releaseRetained(allocation.retained, "failed untransferred capture owner")
      catch
        case cleanup: Throwable =>
          if primary != null then MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
          else
            entry.cause = Some(cleanup)
            throw cleanup
      finally allocation.active = false
  }

  def dbFirstInitialActivationRequest(reservation: MySqlDbFirstCaptureReservation,
      snapshot: DbFirstPodSnapshot): MySqlDbFirstInitialActivationRequest = synchronized {
    val entry=captureEntry(reservation)
    frozen.foreach(throw _)
    require(reservation.purpose == DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION && !stopped &&
      entry.cause.isEmpty && entry.captureAttempted && snapshot != null && entry.captured.exists(_ eq snapshot),
      "exact original completed initial capture request required")
    entry.initialActivationRequest.getOrElse(throw new IllegalStateException("original initial request was not completed"))
  }

  def dbFirstRecallRequest(reservation: MySqlDbFirstCaptureReservation, snapshot: DbFirstPodSnapshot): MySqlDbFirstRecallRequest = synchronized {
    val entry=captureEntry(reservation)
    frozen.foreach(throw _)
    require(reservation.purpose == DbFirstPodSnapshot.Purpose.RECALL &&
      !stopped && entry.cause.isEmpty && entry.captureAttempted && snapshot != null &&
      entry.captured.exists(_ eq snapshot),"exact original completed capture request required")
    entry.dbFirstRequest.getOrElse(throw new IllegalStateException("original capture request was not completed"))
  }

  def dbFirstCheckpointRequest(reservation: MySqlDbFirstCaptureReservation,
      snapshot: DbFirstPodSnapshot): MySqlDbFirstCheckpointRequest = synchronized {
    val entry=captureEntry(reservation)
    frozen.foreach(throw _)
    require(reservation.purpose == DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT && !stopped && entry.cause.isEmpty &&
      entry.captureAttempted && snapshot != null && entry.captured.exists(_ eq snapshot),
      "exact original completed active checkpoint capture required")
    entry.checkpointRequest.getOrElse(throw new IllegalStateException("original checkpoint request was not completed"))
  }

  private def recallEntry(reservation:MySqlDbFirstCaptureReservation):Entry =
    recallEntryBound(reservation,false)
  private def retainedRecallEntry(reservation:MySqlDbFirstCaptureReservation):Entry =
    recallEntryBound(reservation,true)
  /** Main撤边前后都只认同一登记drop；撤边不会授普通confirmation或解除原失败。 */
  private def hasOriginalRetainedDrop(entry:Entry):Boolean =
    entry.recallDrop.exists { receipt=>
      receipt.isRetained && (receipt.core eq this) && (receipt.handle eq entry.handle) &&
        (receipt.retainedConfirmation.core eq this) && (receipt.retainedConfirmation.handle eq entry.handle) &&
        entry.recallCleanupQualification.nonEmpty &&
        receipt.retainedConfirmation.retainedFailure.exists(cause=>entry.cause.exists(_ eq cause)) &&
        (if entry.recallRetainedMainHandedOff then
          entry.recallConfirmation.isEmpty && receipt.retainedConfirmation.retainedQualification==null
        else entry.recallConfirmation.exists(_ eq receipt.retainedConfirmation) &&
          entry.recallCleanupQualification.exists(_ eq receipt.retainedConfirmation.retainedQualification))
    }
  private def recallEntryBound(reservation:MySqlDbFirstCaptureReservation,allowOriginalRetainedDrop:Boolean):Entry =
    requireLive()
    require(reservation != null && (reservation.core eq this) && (reservation.profile eq profile),
      "exact original DB-first reservation required")
    val entry=current(reservation.handle)
    require(dbFirstInputs.nonEmpty && reservation.purpose == DbFirstPodSnapshot.Purpose.RECALL &&
      entry.captureReservation.exists(_ eq reservation) &&
      entry.dbFirstRequest.nonEmpty && entry.captured.nonEmpty && entry.dbFirstRequestBacking.nonEmpty &&
      entry.plan.isEmpty && !entry.root.isClosed && (!entry.dropped ||
        (allowOriginalRetainedDrop && hasOriginalRetainedDrop(entry))),
      "completed original schema6 capture or exact retained native drop required")
    entry
  private def checkpointEntry(reservation:MySqlDbFirstCaptureReservation):Entry =
    requireLive()
    require(reservation != null && (reservation.core eq this) && (reservation.profile eq profile),
      "exact original DB-first reservation required")
    val entry=current(reservation.handle)
    require(dbFirstInputs.nonEmpty && reservation.purpose == DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT &&
      entry.captureReservation.exists(_ eq reservation) &&
      entry.checkpointRequest.nonEmpty && entry.captured.nonEmpty && entry.checkpointBacking.nonEmpty &&
      entry.plan.isEmpty && !entry.root.isClosed && !entry.dropped,"completed original schema6 capture required")
    entry
  private def initialEntry(reservation:MySqlDbFirstCaptureReservation):Entry =
    requireLive()
    require(reservation != null && (reservation.core eq this) && (reservation.profile eq profile),
      "exact original DB-first reservation required")
    val entry=current(reservation.handle)
    require(dbFirstInputs.nonEmpty && reservation.purpose == DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION &&
      entry.captureReservation.exists(_ eq reservation) &&
      entry.initialActivationRequest.nonEmpty && entry.captured.nonEmpty && entry.initialActivationBacking.nonEmpty &&
      entry.plan.isEmpty && !entry.root.isClosed && !entry.dropped,"completed original schema6 capture required")
    entry
  def claimDbFirstRecallConfirmation(reservation:MySqlDbFirstCaptureReservation,
      observation:MySqlDbFirstSession.DbFirstRecallObservation):MySqlDbFirstRecallConfirmation = synchronized {
    mainOwner()
    val entry=recallEntry(reservation)
    frozen.foreach(throw _)
    require(!stopped && entry.cause.isEmpty && !entry.dropped && entry.recallConfirmation.isEmpty,
      "original healthy recall confirmation is one-shot")
    val runner=entry.recallRunner.getOrElse(throw new IllegalStateException("original recall runner missing"))
    runner.requireConfirmedObservation(observation)
    entry.recallObservation=Some(observation)
    try
      // 原worker begin已预扣此固定metadata槽；Main不跨线程改root账本。
      val confirmation=MySqlDbFirstRecallConfirmation.issue(this,entry.handle,entry.dbFirstRequest.get)
      entry.recallConfirmation=Some(confirmation)
      confirmation
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  def claimDbFirstCheckpointConfirmation(reservation:MySqlDbFirstCaptureReservation,
      observation:MySqlDbFirstSession.DbFirstCheckpointObservation):MySqlDbFirstCheckpointConfirmation = synchronized {
    mainOwner()
    val entry=checkpointEntry(reservation)
    frozen.foreach(throw _)
    require(!stopped && entry.cause.isEmpty && !entry.dropped && entry.checkpointConfirmation.isEmpty,
      "original healthy checkpoint confirmation is one-shot")
    val runner=entry.checkpointRunner.getOrElse(throw new IllegalStateException("original checkpoint runner missing"))
    runner.requireConfirmedObservation(observation)
    entry.checkpointObservation=Some(observation)
    try
      // 原worker begin已预扣此固定metadata槽；Main不跨线程改root账本。
      val confirmation=MySqlDbFirstCheckpointConfirmation.issue(this,entry.handle,entry.checkpointRequest.get)
      entry.checkpointConfirmation=Some(confirmation)
      confirmation
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  /** 与普通确认共享原一次性预扣槽；只消费原Entry的opaque受限资格，不恢复普通writer。 */
  def claimRetainedDbFirstRecallConfirmation(reservation:MySqlDbFirstCaptureReservation,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):MySqlDbFirstRecallConfirmation = synchronized {
    mainOwner()
    val entry=recallEntry(reservation)
    frozen.foreach(throw _)
    require(!stopped && entry.recallConfirmation.isEmpty && entry.recallObservation.isEmpty &&
      entry.accepted && entry.workerObserved && entry.dbFirstDraft.isEmpty,
      "original retained confirmation requires unused one-shot slot and actual settled failed Entry")
    requireRecordedRecallQualification(entry,qualification)
    qualification.requireOriginal(entry.recallRunner.get,entry.recallSqlSettlement.get,entry.cause.get)
    try
      val confirmation=MySqlDbFirstRecallConfirmation.issueRetained(this,entry.handle,entry.dbFirstRequest.get,
        entry.cause,qualification)
      entry.recallConfirmation=Some(confirmation)
      confirmation
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  /** 每次Main使用仍核同一原窗口；领取不是永久清理许可。 */
  private[mysql] def requireRetainedDbFirstConfirmation(confirmation:MySqlDbFirstRecallConfirmation,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    mainOwner()
    val entry=recallEntry(reservation)
    frozen.foreach(throw _)
    require(!stopped && confirmation!=null && confirmation.isRetained &&
      (confirmation.core eq this) && (confirmation.handle eq entry.handle) &&
      entry.recallConfirmation.exists(_ eq confirmation) && !entry.dropped && entry.recallObservation.isEmpty &&
      entry.cause.nonEmpty && confirmation.retainedFailure.exists(cause=>entry.cause.exists(_ eq cause)) &&
      entry.recallCleanupQualification.exists(_ eq confirmation.retainedQualification) && entry.recallSqlSettlement.nonEmpty,
      "exact original retained Main/confirmation/qualification/cause required")
    confirmation.retainedQualification.requireOriginal(entry.recallRunner.get,entry.recallSqlSettlement.get,entry.cause.get)
  }

  private[mysql] def requireDbFirstConfirmation(confirmation:MySqlDbFirstRecallConfirmation,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    mainOwner()
    val entry=recallEntry(reservation)
    require(confirmation != null && !confirmation.isRetained && confirmation.retainedFailure.isEmpty &&
      (confirmation.core eq this) && (confirmation.handle eq entry.handle) &&
      entry.recallConfirmation.exists(_ eq confirmation) && !entry.dropped && entry.cause.isEmpty && !stopped,
      "original Main/entry/confirmation reference required")
  }
  private[mysql] def requireDbFirstCheckpointConfirmation(confirmation:MySqlDbFirstCheckpointConfirmation,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    mainOwner()
    val entry=checkpointEntry(reservation)
    require(confirmation != null && (confirmation.core eq this) && (confirmation.handle eq entry.handle) &&
      entry.checkpointConfirmation.exists(_ eq confirmation) && !entry.dropped && entry.cause.isEmpty && !stopped,
      "original Main/entry/confirmation reference required")
  }
  def claimDbFirstInitialActivationConfirmation(reservation:MySqlDbFirstCaptureReservation,
      observation:MySqlDbFirstSession.DbFirstInitialActivationObservation):MySqlDbFirstInitialActivationConfirmation = synchronized {
    mainOwner()
    val entry=initialEntry(reservation)
    frozen.foreach(throw _)
    require(!stopped && entry.cause.isEmpty && !entry.dropped && entry.initialConfirmation.isEmpty,
      "original healthy recall confirmation is one-shot")
    val runner=entry.initialRunner.getOrElse(throw new IllegalStateException("original recall runner missing"))
    runner.requireConfirmedObservation(observation)
    entry.initialObservation=Some(observation)
    try
      // 原worker begin已预扣此固定metadata槽；Main不跨线程改root账本。
      val confirmation=MySqlDbFirstInitialActivationConfirmation.issue(this,entry.handle,entry.initialActivationRequest.get)
      entry.initialConfirmation=Some(confirmation)
      confirmation
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private[mysql] def requireDbFirstInitialConfirmation(confirmation:MySqlDbFirstInitialActivationConfirmation,
      reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    mainOwner()
    val entry=initialEntry(reservation)
    require(confirmation != null && (confirmation.core eq this) && (confirmation.handle eq entry.handle) &&
      entry.initialConfirmation.exists(_ eq confirmation) && !entry.dropped && entry.cause.isEmpty && !stopped,
      "original Main/entry/confirmation reference required")
  }
  def recordDbFirstMainDrop(confirmation:MySqlDbFirstRecallConfirmation):MySqlDbFirstRecallDropReceipt = synchronized {
    requireLive();mainOwner()
    require(confirmation != null && (confirmation.core eq this),"original DB-first confirmation required")
    val entry=current(confirmation.handle)
    requireDbFirstConfirmation(confirmation,entry.captureReservation.getOrElse(
      throw new IllegalStateException("original capture was detached")))
    require(entry.recallDrop.isEmpty,"DB-first Main drop is one-shot")
    try
      // 与confirmation一起在原worker begin预扣；此处只发行已保留的固定槽。
      val receipt=MySqlDbFirstRecallDropReceipt.issue(this,entry.handle)
      entry.recallDrop=Some(receipt);entry.dropped=true
      receipt
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  /** Main资源frame实际撤引用后，登记同一原操作的本体与ticket ACK；不提交worker退休。 */
  def recordRetainedDbFirstMainDrop(confirmation:MySqlDbFirstRecallConfirmation,
      removal:ExactBodyRemovalAcknowledgement,ticket:TicketReleaseAcknowledgement):MySqlDbFirstRecallDropReceipt = synchronized {
    requireLive();mainOwner()
    require(confirmation!=null && confirmation.isRetained && (confirmation.core eq this),"original retained confirmation required")
    val entry=current(confirmation.handle)
    requireRetainedDbFirstConfirmation(confirmation,entry.captureReservation.get)
    val request=entry.dbFirstRequest.get
    require(entry.recallDrop.isEmpty && removal!=null && ticket!=null && (ticket.removal() eq removal) &&
      removal.bootId()==entry.handle.tag.bootId && removal.bootGeneration()==entry.handle.tag.bootGeneration &&
      removal.connectionGeneration()==entry.handle.tag.connectionGeneration && removal.operationId()==entry.handle.tag.operationId &&
      removal.podId()==request.podUuid && removal.attemptId()==request.attemptUuid &&
      removal.expectedStateVersion()==request.capturedStateVersion && removal.dimensionId()==request.pose.dimension() &&
      ticket.originalFootprint().size()==25,"exact original retained native completion and unused drop slot required")
    val receipt=MySqlDbFirstRecallDropReceipt.issueRetained(this,entry.handle,confirmation,removal,ticket)
    entry.recallDrop=Some(receipt);entry.dropped=true
    receipt
  }
  /** 仅撤Main confirmation到原资格图的引用；Session/Entry/adapter继续原保管，全部费用不动。 */
  def handoffRetainedDbFirstMainDrop(receipt:MySqlDbFirstRecallDropReceipt,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):Unit = synchronized {
    requireLive();mainOwner()
    require(receipt!=null && receipt.isRetained && (receipt.core eq this),"original retained native drop required")
    val entry=current(receipt.handle)
    require(entry.dropped && entry.recallDrop.exists(_ eq receipt) && hasOriginalRetainedDrop(entry) &&
      !entry.recallRetainedMainHandedOff && !entry.recallReleaseAttempted && !entry.recallRootCloseAttempted,
      "original Main retained graph handoff is one-shot")
    // 已完成native后的撤引用不续窗口；即使过期也保原cause和费用，不授新的副作用。
    requireRecordedRecallQualification(entry,qualification)
    receipt.retainedConfirmation.detachRetainedQualification(qualification)
    entry.recallConfirmation=None
    entry.recallRetainedMainHandedOff=true
  }
  private[mysql] def requireDbFirstDrop(receipt:MySqlDbFirstRecallDropReceipt):MySqlDbFirstRecallTransaction = synchronized {
    requireLive();workerOwner()
    require(receipt != null && !receipt.isRetained && (receipt.core eq this),"original DB-first Main drop receipt required")
    val entry=current(receipt.handle)
    require(entry.recallDrop.exists(_ eq receipt) && entry.recallConfirmation.exists(c=> !c.isRetained) && entry.dropped &&
      entry.accepted && entry.workerObserved && entry.cause.isEmpty && !entry.recallReleaseAttempted,
      "original confirmed DB-first Main drop is unavailable, faulted or replayed")
    val runner=entry.recallRunner.getOrElse(throw new IllegalStateException("original recall runner missing"))
    runner.requireConfirmedObservation(entry.recallObservation.getOrElse(
      throw new IllegalStateException("original confirmed observation missing")))
    entry.recallReleaseAttempted=true
    runner
  }
  /** 只核对原native drop与仍有效的失败资格，不清引用、不退款、不消费退休槽。 */
  private[mysql] def requireRetainedDbFirstDrop(receipt:MySqlDbFirstRecallDropReceipt,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):MySqlDbFirstRecallTransaction = synchronized {
    requireLive();workerOwner();frozen.foreach(throw _)
    require(receipt!=null && receipt.isRetained && (receipt.core eq this),
      "original retained DB-first Main drop receipt required")
    val entry=current(receipt.handle)
    require(!stopped && entry.accepted && entry.workerObserved && entry.dropped &&
      entry.recallDrop.exists(_ eq receipt) && entry.recallRetainedMainHandedOff && hasOriginalRetainedDrop(entry) &&
      entry.recallCleanupQualification.exists(_ eq qualification) &&
      entry.recallObservation.isEmpty && entry.dbFirstDraft.isEmpty &&
      !entry.recallReleaseAttempted && !entry.recallRootCloseAttempted,
      "exact original retained native drop before worker retirement required")
    val runner=entry.recallRunner.getOrElse(throw new IllegalStateException("original retained recall runner missing"))
    requireRecordedFailedRecallQualification(runner,qualification)
    require(receipt.retainedConfirmation.retainedFailure.exists(cause=>entry.cause.exists(_ eq cause)),
      "original retained native drop failure differs")
    runner.requireFailedRecallSqlSettlement(entry.recallSqlSettlement.get,entry.cause.get)
    require(!entry.root.hasActiveImmutableBorrows && entry.root.retainedCanonicalDataChildCount==0,
      "retained Entry still has local immutable borrowers/DATA children")
    runner
  }
  /** 专用控制提交只复用原已消费业务slot；Main不能执行Session/JDBC核验。 */
  def requireRetainedDbFirstReleaseSubmission(receipt:MySqlDbFirstRecallDropReceipt,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):Unit = synchronized {
    requireLive();mainOwner();frozen.foreach(throw _)
    require(receipt!=null && receipt.isRetained && (receipt.core eq this),"original retained native drop required")
    val entry=current(receipt.handle)
    require(!stopped && entry.recallDrop.exists(_ eq receipt) && hasOriginalRetainedDrop(entry) &&
      entry.recallRetainedMainHandedOff && !entry.recallReleaseAttempted && !entry.recallRootCloseAttempted,
      "original retained release submission is unavailable or already attempted")
    requireRecordedRecallQualification(entry,qualification)
    qualification.requireOriginal(entry.recallRunner.get,entry.recallSqlSettlement.get,entry.cause.get)
  }
  private[mysql] def beginRetainedDbFirstDrop(receipt:MySqlDbFirstRecallDropReceipt,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):Unit = synchronized {
    requireRetainedDbFirstDrop(receipt,qualification)
    val entry=current(receipt.handle)
    entry.recallReleaseAttempted=true
    entry.recallRetirementStage=MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight
  }
  /** 只承认已登记的Boot原C和实际SQL退休/失败资格；source metadata不授退休权限。 */
  private[mysql] def requireBootRecallFinalPayload(receipt:MySqlDbFirstRecallDropReceipt,
      takeover:MySqlDbFirstBootRecallTakeoverWork):Unit = synchronized {
    requireLive();workerOwner()
    require(receipt!=null && (receipt.core eq this),"actual Boot original C receipt core required")
    val entry=current(receipt.handle)
    require(entry.recallDrop.exists(_ eq receipt) && entry.dropped && entry.accepted && entry.workerObserved &&
      entry.recallReleaseAttempted && !entry.recallRootCloseAttempted && entry.captureAttempted &&
      entry.bootCaptureActivated && !entry.bootCaptureExcluded && takeover!=null &&
      entry.bootTakeover.exists(_ eq takeover) && entry.bootMainDecision.exists(decision=>
        decision.refusalReason.isEmpty && (decision.intent eq takeover.intent)) && takeover.resourcesRetired,
      "same registered positive Boot Entry and actually retired takeover SQL required")
    if receipt.isRetained then
      require(entry.recallRetainedMainHandedOff && hasOriginalRetainedDrop(entry) && entry.cause.nonEmpty &&
        entry.recallSqlSettlement.nonEmpty && entry.recallCleanupQualification.nonEmpty,
        "actual retained Boot C native handoff and original failed SQL qualification required")
      entry.recallCleanupQualification.get.requireDetached(entry.recallSqlSettlement.get,entry.cause.get)
    else
      require(entry.cause.isEmpty && entry.recallConfirmation.exists(c=> !c.isRetained) &&
        entry.recallRunner.exists(_.resourcesRetired),"actual normal Boot C SQL-retired runner required")
  }
  private def detachBootRecallPayload(entry:Entry,receipt:MySqlDbFirstRecallDropReceipt):Unit =
    entry.bootTakeover match
      case Some(takeover)=>
        takeover.retireRecallMetadata(receipt)
        entry.bootTakeover=None;entry.bootMainDecision=None
      case None=>require(entry.bootMainDecision.isEmpty,"Boot decision cannot outlive its actual takeover registration")
    entry.captureJournalLink=None

  /** 原worker已开始退休；撤payload的Unit frame必须先返回，随后才可关原root。 */
  private[mysql] def detachRetainedDbFirstPayload(receipt:MySqlDbFirstRecallDropReceipt,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):Unit = synchronized {
    requireLive();workerOwner()
    require(receipt!=null && receipt.isRetained && (receipt.core eq this),"original retained payload receipt required")
    val entry=current(receipt.handle)
    require(entry.recallDrop.exists(_ eq receipt) && entry.recallRetainedMainHandedOff &&
      entry.recallReleaseAttempted && !entry.recallRootCloseAttempted && !entry.recallRetainedPayloadDetached &&
      entry.recallConfirmation.isEmpty && entry.recallCleanupQualification.exists(_ eq qualification),
      "original started retained payload retirement required once")
    qualification.requireDetached(entry.recallSqlSettlement.get,entry.cause.get)
    entry.bootTakeover.foreach(takeover=>requireBootRecallFinalPayload(receipt,takeover))
    entry.recallRunner.get.retireFailedEntryPayload(entry.recallSqlSettlement.get,entry.cause.get)
    detachBootRecallPayload(entry,receipt)
    entry.captured=None;entry.dbFirstRequest=None;entry.dbFirstRequestBacking=None
    entry.dbFirstDraft=None;entry.recallObservation=None;entry.recallRunner=None
    entry.captureReservation=None;entry.recallCleanupQualification=None
    entry.recallRetainedPayloadDetached=true
  }
  private[mysql] def retireRetainedDbFirstDrop(receipt:MySqlDbFirstRecallDropReceipt):Unit = synchronized {
    requireLive();workerOwner()
    require(receipt!=null && receipt.isRetained && (receipt.core eq this),"original retained retirement receipt required")
    val entry=current(receipt.handle)
    require(entry.recallDrop.exists(_ eq receipt) && entry.recallRetainedMainHandedOff && entry.recallReleaseAttempted &&
      entry.recallRetainedPayloadDetached && !entry.recallRootCloseAttempted && entry.cause.nonEmpty &&
      entry.recallRunner.isEmpty && entry.recallCleanupQualification.isEmpty && entry.recallConfirmation.isEmpty &&
      entry.captured.isEmpty && entry.dbFirstRequest.isEmpty && entry.dbFirstRequestBacking.isEmpty &&
      entry.dbFirstDraft.isEmpty && entry.captureReservation.isEmpty && entry.bootTakeover.isEmpty &&
      entry.bootMainDecision.isEmpty && entry.captureJournalLink.isEmpty,
      "actual original retained payload graph must detach before root close")
    require(!entry.root.hasActiveImmutableBorrows && entry.root.retainedCanonicalDataChildCount==0,
      "retained Entry still has immutable borrowers/DATA children")
    entry.recallRetirementStage=MySqlDbFirstRetirementFailure.Stage.RootCloseAttempted
    entry.recallRootCloseAttempted=true
    try
      entry.root.closeRootReadBudget()
      require(entry.root.isClosed,"retained Entry root close did not complete")
      entries.remove(receipt.tag)
      receipt.markRetainedEntryRetired()
    catch case cause:Throwable=>
      MySqlOwnedIoAdapter.suppressExitCleanup(entry.cause.get,cause)
      frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
      throw cause
  }
  private[mysql] def retireDbFirstDrop(receipt:MySqlDbFirstRecallDropReceipt):Unit = synchronized {
    requireLive();workerOwner()
    require(receipt != null && !receipt.isRetained && (receipt.core eq this),"original DB-first retirement receipt required")
    val entry=current(receipt.handle)
    require(entry.recallDrop.exists(_ eq receipt) && entry.recallReleaseAttempted && entry.cause.isEmpty &&
      entry.recallConfirmation.exists(c=> !c.isRetained) &&
      entry.recallRunner.exists(_.resourcesRetired),"original runner resources must retire before Entry root")
    // 可定位的本Entry借用阻挡不触碰共享ledger，不能拖住其他健康Pod。
    entry.recallRetirementStage=MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight
    require(!entry.root.hasActiveImmutableBorrows && entry.root.retainedCanonicalDataChildCount == 0,
      "confirmed Entry still has local immutable borrowers/DATA children")
    entry.recallRetirementStage=MySqlDbFirstRetirementFailure.Stage.RootCloseAttempted
    entry.bootTakeover.foreach(takeover=>requireBootRecallFinalPayload(receipt,takeover))
    entry.recallRunner.get.retireEntryPayload()
    detachBootRecallPayload(entry,receipt)
    entry.captured=None;entry.dbFirstRequest=None;entry.dbFirstRequestBacking=None
    entry.dbFirstDraft=None;entry.recallObservation=None;entry.recallRunner=None
    entry.captureReservation=None;entry.recallConfirmation=None
    entry.recallRootCloseAttempted=true
    entry.root.closeRootReadBudget()
    entries.remove(receipt.tag)
    ()
  }
  private[mysql] def failDbFirstDrop(receipt:MySqlDbFirstRecallDropReceipt,cause:Throwable):Throwable = synchronized {
    requireLive();workerOwner()
    require(receipt != null && (receipt.core eq this) && cause != null,"original DB-first retirement failure required")
    val entry=current(receipt.handle)
    require(entry.recallDrop.exists(_ eq receipt),"foreign DB-first retirement failure")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
    // 本地preflight失败尚未触共享账本；真正root close/releaseAll失败则不能声称共享账本健康。
    if MySqlDbFirstRetirementFailure.classify(entry.recallRetirementStage) ==
        MySqlDbFirstRetirementFailure.Disposition.Local && scala.util.control.NonFatal(cause) then
      MySqlDbFirstLocalRetirementException.fromCore(this,receipt,cause)
    else
      frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
      cause
  }
  private[mysql] def requireLocalRetirementFailure(receipt:MySqlDbFirstRecallDropReceipt,cause:Throwable):Unit = synchronized {
    requireLive();workerOwner()
    require(receipt != null && (receipt.core eq this) && cause != null && scala.util.control.NonFatal(cause),
      "actual nonfatal Entry failure required")
    val entry=current(receipt.handle)
    require(entry.recallDrop.exists(_ eq receipt) && entry.recallReleaseAttempted && entry.dropped &&
      entry.recallRunner.exists(_.resourcesRetired) && entry.cause.exists(_ eq cause) &&
      entry.recallRetirementStage == MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight &&
      !entry.recallRootCloseAttempted && frozen.isEmpty,
      "only original confirmed SQL-retired preflight failure is local")
  }
  def recordDbFirstCheckpointFinal(confirmation:MySqlDbFirstCheckpointConfirmation):MySqlDbFirstCheckpointFinalReceipt = synchronized {
    requireLive();mainOwner()
    require(confirmation != null && (confirmation.core eq this),"original DB-first confirmation required")
    val entry=current(confirmation.handle)
    requireDbFirstCheckpointConfirmation(confirmation,entry.captureReservation.getOrElse(
      throw new IllegalStateException("original capture was detached")))
    require(entry.checkpointReadAcknowledged && entry.checkpointFinal.isEmpty,"DB-first Main drop is one-shot")
    try
      // 与confirmation一起在原worker begin预扣；此处只发行已保留的固定槽。
      val receipt=MySqlDbFirstCheckpointFinalReceipt.issue(this,entry.handle)
      entry.checkpointFinal=Some(receipt);entry.dropped=true
      receipt
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private[mysql] def requireDbFirstCheckpointFinal(receipt:MySqlDbFirstCheckpointFinalReceipt):MySqlDbFirstCheckpointTransaction = synchronized {
    requireLive();workerOwner()
    require(receipt != null && (receipt.core eq this),"original DB-first Main drop receipt required")
    val entry=current(receipt.handle)
    require(entry.checkpointFinal.exists(_ eq receipt) && entry.checkpointConfirmation.nonEmpty && entry.dropped &&
      entry.accepted && entry.workerObserved && entry.cause.isEmpty && !entry.checkpointReleaseAttempted,
      "original confirmed DB-first Main drop is unavailable, faulted or replayed")
    val runner=entry.checkpointRunner.getOrElse(throw new IllegalStateException("original recall runner missing"))
    runner.requireConfirmedObservation(entry.checkpointObservation.getOrElse(
      throw new IllegalStateException("original confirmed observation missing")))
    entry.checkpointReleaseAttempted=true
    runner
  }
  private[mysql] def retireDbFirstCheckpointFinal(receipt:MySqlDbFirstCheckpointFinalReceipt):Unit = synchronized {
    requireLive();workerOwner()
    require(receipt != null && (receipt.core eq this),"original DB-first retirement receipt required")
    val entry=current(receipt.handle)
    require(entry.checkpointFinal.exists(_ eq receipt) && entry.checkpointReleaseAttempted && entry.cause.isEmpty &&
      entry.checkpointRunner.exists(_.resourcesRetired),"original runner resources must retire before Entry root")
    // 可定位的本Entry借用阻挡不触碰共享ledger，不能拖住其他健康Pod。
    entry.checkpointRetirementStage=MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight
    require(!entry.root.hasActiveImmutableBorrows && entry.root.retainedCanonicalDataChildCount == 0,
      "confirmed Entry still has local immutable borrowers/DATA children")
    entry.checkpointRetirementStage=MySqlDbFirstRetirementFailure.Stage.RootCloseAttempted
    entry.checkpointRunner.get.retireEntryPayload()
    entry.captured=None;entry.checkpointRequest=None;entry.checkpointBacking=None
    entry.dbFirstDraft=None;entry.checkpointObservation=None;entry.checkpointRunner=None
    entry.captureReservation=None;entry.checkpointConfirmation=None
    entry.checkpointRootCloseAttempted=true
    entry.root.closeRootReadBudget()
    entries.remove(receipt.tag)
    ()
  }
  private[mysql] def failDbFirstCheckpointFinal(receipt:MySqlDbFirstCheckpointFinalReceipt,cause:Throwable):Throwable = synchronized {
    requireLive();workerOwner()
    require(receipt != null && (receipt.core eq this) && cause != null,"original DB-first retirement failure required")
    val entry=current(receipt.handle)
    require(entry.checkpointFinal.exists(_ eq receipt),"foreign DB-first retirement failure")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
    // 本地preflight失败尚未触共享账本；真正root close/releaseAll失败则不能声称共享账本健康。
    if MySqlDbFirstRetirementFailure.classify(entry.checkpointRetirementStage) ==
        MySqlDbFirstRetirementFailure.Disposition.Local && scala.util.control.NonFatal(cause) then
      MySqlDbFirstCheckpointLocalRetirementException.fromCore(this,receipt,cause)
    else
      frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
      cause
  }
  private[mysql] def requireCheckpointLocalRetirementFailure(receipt:MySqlDbFirstCheckpointFinalReceipt,cause:Throwable):Unit = synchronized {
    requireLive();workerOwner()
    require(receipt != null && (receipt.core eq this) && cause != null && scala.util.control.NonFatal(cause),
      "actual nonfatal Entry failure required")
    val entry=current(receipt.handle)
    require(entry.checkpointFinal.exists(_ eq receipt) && entry.checkpointReleaseAttempted && entry.dropped &&
      entry.checkpointRunner.exists(_.resourcesRetired) && entry.cause.exists(_ eq cause) &&
      entry.checkpointRetirementStage == MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight &&
      !entry.checkpointRootCloseAttempted && frozen.isEmpty,
      "only original confirmed SQL-retired preflight failure is local")
  }
  def recordDbFirstPreparationFinal(confirmation:MySqlDbFirstInitialPreparationConfirmation):MySqlDbFirstPreparationFinalReceipt = synchronized {
    requireLive();mainOwner()
    require(confirmation != null && (confirmation.core eq this),"original DB-first confirmation required")
    val entry=current(confirmation.handle)
    requireDbFirstPreparationConfirmation(confirmation,entry.preparationReservation.getOrElse(
      throw new IllegalStateException("original preparation was detached")))
    require(entry.preparationReadReleased && entry.preparationFinal.isEmpty,"preparation final payload detach is one-shot")
    try
      // 与confirmation一起在原worker begin预扣；此处只发行已保留的固定槽。
      val receipt=MySqlDbFirstPreparationFinalReceipt.issue(this,entry.handle)
      entry.preparationFinal=Some(receipt);entry.dropped=true
      receipt
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private[mysql] def requireDbFirstPreparationFinal(receipt:MySqlDbFirstPreparationFinalReceipt):MySqlDbFirstInitialPreparationTransaction = synchronized {
    requireLive();workerOwner()
    require(receipt != null && (receipt.core eq this),"original preparation final payload detach receipt required")
    val entry=current(receipt.handle)
    require(entry.preparationFinal.exists(_ eq receipt) && entry.preparationConfirmation.nonEmpty && entry.dropped &&
      entry.accepted && entry.workerObserved && entry.cause.isEmpty && !entry.preparationReleaseAttempted,
      "original confirmed preparation final payload detach is unavailable, faulted or replayed")
    val runner=entry.preparationWork.getOrElse(throw new IllegalStateException("original preparation work missing")).retainedTransaction
    runner.requireConfirmedObservation(entry.preparationObservation.getOrElse(
      throw new IllegalStateException("original confirmed observation missing")))
    entry.preparationReleaseAttempted=true
    runner
  }
  private[mysql] def retireDbFirstPreparationFinal(receipt:MySqlDbFirstPreparationFinalReceipt):Unit = synchronized {
    requireLive();workerOwner()
    require(receipt != null && (receipt.core eq this),"original DB-first retirement receipt required")
    val entry=current(receipt.handle)
    require(entry.preparationFinal.exists(_ eq receipt) && entry.preparationReleaseAttempted && entry.cause.isEmpty &&
      entry.preparationWork.exists(_.retainedTransaction.resourcesRetired),"original runner resources must retire before Entry root")
    // 可定位的本Entry借用阻挡不触碰共享ledger，不能拖住其他健康Pod。
    entry.preparationRetirementStage=MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight
    require(!entry.root.hasActiveImmutableBorrows && entry.root.retainedCanonicalDataChildCount == 0,
      "confirmed Entry still has local immutable borrowers/DATA children")
    entry.preparationRetirementStage=MySqlDbFirstRetirementFailure.Stage.RootCloseAttempted
    entry.preparationWork.get.retainedTransaction.retireEntryPayload()
    entry.preparationWork=None;entry.preparationBacking=None;entry.preparationObservation=None
    entry.preparationReservation=None;entry.preparationConfirmation=None
    entry.preparationRootCloseAttempted=true
    entry.root.closeRootReadBudget()
    entries.remove(receipt.tag)
    ()
  }
  private[mysql] def failDbFirstPreparationFinal(receipt:MySqlDbFirstPreparationFinalReceipt,cause:Throwable):Throwable = synchronized {
    requireLive();workerOwner()
    require(receipt != null && (receipt.core eq this) && cause != null,"original DB-first retirement failure required")
    val entry=current(receipt.handle)
    require(entry.preparationFinal.exists(_ eq receipt),"foreign DB-first retirement failure")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
    // 本地preflight失败尚未触共享账本；真正root close/releaseAll失败则不能声称共享账本健康。
    if MySqlDbFirstRetirementFailure.classify(entry.preparationRetirementStage) ==
        MySqlDbFirstRetirementFailure.Disposition.Local && scala.util.control.NonFatal(cause) then
      MySqlDbFirstPreparationLocalRetirementException.fromCore(this,receipt,cause)
    else
      frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
      cause
  }
  private[mysql] def requirePreparationLocalRetirementFailure(receipt:MySqlDbFirstPreparationFinalReceipt,cause:Throwable):Unit = synchronized {
    requireLive();workerOwner()
    require(receipt != null && (receipt.core eq this) && cause != null && scala.util.control.NonFatal(cause),
      "actual nonfatal Entry failure required")
    val entry=current(receipt.handle)
    require(entry.preparationFinal.exists(_ eq receipt) && entry.preparationReleaseAttempted && entry.dropped &&
      entry.preparationWork.exists(_.retainedTransaction.resourcesRetired) && entry.cause.exists(_ eq cause) &&
      entry.preparationRetirementStage == MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight &&
      !entry.preparationRootCloseAttempted && frozen.isEmpty,
      "only original confirmed SQL-retired preflight failure is local")
  }
  def recordDbFirstInitialPublication(confirmation:MySqlDbFirstInitialActivationConfirmation):MySqlDbFirstInitialPublicationReceipt = synchronized {
    requireLive();mainOwner()
    require(confirmation != null && (confirmation.core eq this),"original DB-first confirmation required")
    val entry=current(confirmation.handle)
    requireDbFirstInitialConfirmation(confirmation,entry.captureReservation.getOrElse(
      throw new IllegalStateException("original capture was detached")))
    require(entry.initialPublication.isEmpty,"initial publication payload detach is one-shot")
    try
      // 与confirmation一起在原worker begin预扣；此处只发行已保留的固定槽。
      val receipt=MySqlDbFirstInitialPublicationReceipt.issue(this,entry.handle)
      entry.initialPublication=Some(receipt);entry.dropped=true
      receipt
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private[mysql] def requireDbFirstInitialPublication(receipt:MySqlDbFirstInitialPublicationReceipt):MySqlDbFirstInitialActivationTransaction = synchronized {
    requireLive();workerOwner()
    require(receipt != null && (receipt.core eq this),"original initial publication payload detach receipt required")
    val entry=current(receipt.handle)
    require(entry.initialPublication.exists(_ eq receipt) && entry.initialConfirmation.nonEmpty && entry.dropped &&
      entry.accepted && entry.workerObserved && entry.cause.isEmpty && !entry.initialReleaseAttempted,
      "original confirmed initial publication payload detach is unavailable, faulted or replayed")
    val runner=entry.initialRunner.getOrElse(throw new IllegalStateException("original initial runner missing"))
    runner.requireConfirmedObservation(entry.initialObservation.getOrElse(
      throw new IllegalStateException("original confirmed observation missing")))
    entry.initialReleaseAttempted=true
    runner
  }
  private[mysql] def retireDbFirstInitialPublication(receipt:MySqlDbFirstInitialPublicationReceipt):Unit = synchronized {
    requireLive();workerOwner()
    require(receipt != null && (receipt.core eq this),"original DB-first retirement receipt required")
    val entry=current(receipt.handle)
    require(entry.initialPublication.exists(_ eq receipt) && entry.initialReleaseAttempted && entry.cause.isEmpty &&
      entry.initialRunner.exists(_.resourcesRetired),"original runner resources must retire before Entry root")
    // 可定位的本Entry借用阻挡不触碰共享ledger，不能拖住其他健康Pod。
    entry.initialRetirementStage=MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight
    require(!entry.root.hasActiveImmutableBorrows && entry.root.retainedCanonicalDataChildCount == 0,
      "confirmed Entry still has local immutable borrowers/DATA children")
    entry.initialRetirementStage=MySqlDbFirstRetirementFailure.Stage.RootCloseAttempted
    entry.initialRunner.get.retireEntryPayload()
    entry.captured=None;entry.initialActivationRequest=None;entry.initialActivationBacking=None
    entry.initialActivationDraft=None;entry.initialObservation=None;entry.initialRunner=None
    entry.captureReservation=None;entry.initialConfirmation=None
    entry.initialRootCloseAttempted=true
    entry.root.closeRootReadBudget()
    entries.remove(receipt.tag)
    ()
  }
  private[mysql] def failDbFirstInitialPublication(receipt:MySqlDbFirstInitialPublicationReceipt,cause:Throwable):Throwable = synchronized {
    requireLive();workerOwner()
    require(receipt != null && (receipt.core eq this) && cause != null,"original DB-first retirement failure required")
    val entry=current(receipt.handle)
    require(entry.initialPublication.exists(_ eq receipt),"foreign DB-first retirement failure")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
    // 本地preflight失败尚未触共享账本；真正root close/releaseAll失败则不能声称共享账本健康。
    if MySqlDbFirstRetirementFailure.classify(entry.initialRetirementStage) ==
        MySqlDbFirstRetirementFailure.Disposition.Local && scala.util.control.NonFatal(cause) then
      MySqlDbFirstInitialLocalRetirementException.fromCore(this,receipt,cause)
    else
      frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
      cause
  }
  private[mysql] def requireInitialLocalRetirementFailure(receipt:MySqlDbFirstInitialPublicationReceipt,cause:Throwable):Unit = synchronized {
    requireLive();workerOwner()
    require(receipt != null && (receipt.core eq this) && cause != null && scala.util.control.NonFatal(cause),
      "actual nonfatal Entry failure required")
    val entry=current(receipt.handle)
    require(entry.initialPublication.exists(_ eq receipt) && entry.initialReleaseAttempted && entry.dropped &&
      entry.initialRunner.exists(_.resourcesRetired) && entry.cause.exists(_ eq cause) &&
      entry.initialRetirementStage == MySqlDbFirstRetirementFailure.Stage.KnownSqlRetiredPreflight &&
      !entry.initialRootCloseAttempted && frozen.isEmpty,
      "only original confirmed SQL-retired preflight failure is local")
  }
  def requireDbFirstRecallSubmission(reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    mainOwner()
    val entry=recallEntry(reservation)
    frozen.foreach(throw _)
    require(!stopped && entry.cause.isEmpty && !entry.recallSubmissionAttempted &&
      !entry.materializationAttempted && !entry.workerObserved && !entry.accepted && entry.recallRunner.isEmpty,
      "DB-first submission is one-shot")
    entry.recallSubmissionAttempted=true
  }
  def submittedDbFirstRecall(reservation:MySqlDbFirstCaptureReservation,result:IoSubmission):Unit = synchronized {
    mainOwner()
    val entry=recallEntry(reservation)
    require(entry.recallSubmissionAttempted && !entry.recallSubmissionRecorded && result != null,"registered one-shot DB-first submission result required")
    entry.recallSubmissionRecorded=true
    if result == IoSubmission.Accepted then
      entry.accepted=true
      entry.explicitlyRejected=false
    else
      require(!entry.workerObserved && !entry.accepted,"DB-first rejected submission was already observed; responsibility retained")
      entry.explicitlyRejected=true
  }
  def submissionUnknownDbFirstRecall(reservation:MySqlDbFirstCaptureReservation,cause:Throwable):Unit = synchronized {
    mainOwner()
    val entry=recallEntry(reservation)
    require(entry.recallSubmissionAttempted && cause != null,"original DB-first submission uncertainty required")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
    frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
  }
  private[mysql] def beginDbFirstRecall(reservation:MySqlDbFirstCaptureReservation):MySqlDbFirstRecallTransaction = synchronized {
    workerOwner()
    val entry=recallEntry(reservation)
    frozen.foreach(throw _)
    require(!stopped && entry.cause.isEmpty && entry.recallSubmissionAttempted && !entry.explicitlyRejected &&
      !entry.workerObserved && !entry.materializationAttempted && entry.recallRunner.isEmpty,"DB-first execution is one-shot")
    try
      entry.root.bindRetainedImageWorker(imageHandoffCapability)
      entry.root.reserveRetainedMemory(Math.addExact(MySqlDbFirstRecallTransaction.HolderBytes,
        MySqlDbFirstRecallConfirmation.HolderBytes+MySqlDbFirstRecallDropReceipt.HolderBytes),
        "DB-first runner and one confirmation/drop metadata slot BEFORE allocation")
      val runner=new MySqlDbFirstRecallTransaction(reservation,entry.dbFirstRequest.get,entry.root)
      entry.recallRunner=Some(runner) // 登记先于所有audit、reader、materialization和SQL。
      entry.workerObserved=true;entry.accepted=true
      runner
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private[mysql] def requireExecutingDbFirstRecall(runner:MySqlDbFirstRecallTransaction):Unit = synchronized {
    workerOwner()
    require(runner != null,"registered DB-first execution required")
    val entry=recallEntry(runner.reservation)
    frozen.foreach(throw _)
    entry.cause.foreach(throw _)
    require(!stopped && entry.recallRunner.exists(_ eq runner) && entry.workerObserved && entry.accepted,
      "original DB-first execution was revoked or replaced")
  }
  private[mysql] def materializeExecutingDbFirstRecall(runner:MySqlDbFirstRecallTransaction,
      before:MySqlScriptSchemaSnapshot,connectionId:Long,barrierGeneration:UUID,
      plannedAt:LocalDateTime,resultAt:LocalDateTime):MySqlDbFirstRecallMaterialization = synchronized {
    workerOwner()
    require(runner != null,"registered DB-first transaction required")
    val entry=recallEntry(runner.reservation)
    require(entry.recallRunner.exists(_ eq runner) && entry.workerObserved && entry.accepted,
      "original executing DB-first transaction required")
    materializeDbFirstRecall(runner.reservation,runner.request,before,connectionId,barrierGeneration,plannedAt,resultAt)
  }
  private[mysql] def requireFailedDbFirstRecallObservation(runner:MySqlDbFirstRecallTransaction):Unit = synchronized {
    workerOwner()
    require(runner != null,"original failed DB-first transaction required")
    val entry=retainedRecallEntry(runner.reservation)
    require(entry.recallRunner.exists(_ eq runner) && entry.workerObserved && entry.accepted &&
      entry.cause.nonEmpty && (entry.root eq runner.root),
      "only retained original failed Entry/runner/root may inspect; no execution admission")
    runner.requireFailedCommitObservation()
  }
  /** 正向SQL读回登记到确切原Entry；保留失败责任和原root，不发行普通completion。 */
  private[mysql] def settleFailedDbFirstRecallReadback(runner:MySqlDbFirstRecallTransaction,
      observed:MySqlDbFirstSession.FailedRecallReadback):Unit = synchronized {
    requireFailedDbFirstRecallObservation(runner)
    frozen.foreach(throw _)
    val entry=recallEntry(runner.reservation)
    require(!stopped && !entry.recallSqlSettlementAttempted && entry.recallSqlSettlement.isEmpty &&
      entry.dbFirstDraft.isEmpty && entry.recallConfirmation.isEmpty && entry.recallObservation.isEmpty,
      "original failed Entry SQL settlement is one-shot and cannot issue confirmation")
    runner.requireFailedRecallSqlSettlement(observed,entry.cause.get)
    entry.recallSqlSettlementAttempted=true
    try
      entry.root.reserveRetainedMemory(128L,"original failed recall Entry SQL settlement slot BEFORE allocation")
      entry.recallSqlSettlement=Some(observed)
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  /** 只登记实际Session封存且仍有效的原资格；不清原cause或发行普通确认。 */
  private[mysql] def recordFailedDbFirstRecallQualification(session:MySqlDbFirstSession,
      runner:MySqlDbFirstRecallTransaction,qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):Unit = synchronized {
    requireFailedDbFirstRecallObservation(runner)
    frozen.foreach(throw _)
    val entry=recallEntry(runner.reservation)
    require(!stopped && qualification!=null && entry.recallSqlSettlement.nonEmpty &&
      entry.recallCleanupQualification.isEmpty && entry.recallConfirmation.isEmpty && entry.recallObservation.isEmpty,
      "original settled failed Entry requires one exact opaque qualification")
    qualification.requireSession(session)
    qualification.requireOriginal(runner,entry.recallSqlSettlement.get,entry.cause.get)
    entry.root.reserveRetainedMemory(128L,"original failed recall Entry qualification slot BEFORE allocation")
    entry.recallCleanupQualification=Some(qualification)
  }

  private def requireRecordedRecallQualification(entry:Entry,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):Unit =
    require(qualification!=null && entry.recallCleanupQualification.exists(_ eq qualification) &&
      entry.recallSqlSettlement.nonEmpty && entry.cause.nonEmpty &&
      (!entry.dropped || hasOriginalRetainedDrop(entry)) &&
      (entry.recallConfirmation.isEmpty || entry.recallConfirmation.exists(c=>c.isRetained &&
        (c.retainedQualification eq qualification) && c.retainedFailure.exists(cause=>entry.cause.exists(_ eq cause)))),
      "original retained failed Entry qualification required")
    qualification.requireDiagnosticOriginal(entry.recallRunner.get,entry.recallSqlSettlement.get,entry.cause.get)

  private[mysql] def requireRecordedFailedRecallQualification(runner:MySqlDbFirstRecallTransaction,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):Unit = synchronized {
    requireFailedDbFirstRecallObservation(runner)
    val entry=retainedRecallEntry(runner.reservation)
    require(!stopped,"stopped worker cannot maintain recall qualification")
    requireRecordedRecallQualification(entry,qualification)
    qualification.requireOriginal(runner,entry.recallSqlSettlement.get,entry.cause.get)
  }
  /** 晚到retained reply仍报告原失败；只核身份，不用过期/撤销窗口掩盖cause，不兑换confirmation。 */
  def retainedDbFirstRecallFailure(reservation:MySqlDbFirstCaptureReservation,
      qualification:MySqlDbFirstSession.FailedRecallCleanupQualification):Throwable = synchronized {
    mainOwner()
    val entry=retainedRecallEntry(reservation)
    requireRecordedRecallQualification(entry,qualification)
    entry.cause.get
  }

  /** 只返回已登记的原scalar；Main不得以此清cause、退款或绕过正常确认门禁。 */
  def failedDbFirstRecallSqlReadback(reservation:MySqlDbFirstCaptureReservation)
      :Option[MySqlDbFirstSession.FailedRecallReadback] = synchronized {
    mainOwner()
    val entry=recallEntry(reservation)
    entry.recallSqlSettlement
  }
  /** DATA已由原Session独立持有；不退款Entry预算，不清cause或发行任何确认。 */
  private[mysql] def detachConfirmedDbFirstInitialDraft(runner:MySqlDbFirstInitialActivationTransaction):Unit = synchronized {
    requireLive();workerOwner();frozen.foreach(throw _)
    require(runner!=null,"original initial runner required")
    val entry=initialEntry(runner.reservation)
    require(!stopped && entry.initialRunner.exists(_ eq runner) && entry.workerObserved && entry.accepted &&
      entry.cause.isEmpty && (entry.root eq runner.root),"original healthy initial readback Entry required")
    val draft=entry.initialActivationDraft.getOrElse(throw new IllegalStateException("original initial draft missing"))
    runner.requireConfirmedReadbackDraft(draft)
    entry.initialActivationDraft=None
    runner.recordConfirmedDraftDetached(draft)
  }
  private[mysql] def detachConfirmedDbFirstSnapshotDraft(runner:MySqlDbFirstSnapshotTransaction[?,?]):Unit = synchronized {
    requireLive();workerOwner();frozen.foreach(throw _)
    val entry=runner match
      case recall:MySqlDbFirstRecallTransaction=>
        val found=recallEntry(recall.reservation)
        require(found.recallRunner.exists(_ eq recall),"original confirmed recall runner required")
        found
      case checkpoint:MySqlDbFirstCheckpointTransaction=>
        val found=checkpointEntry(checkpoint.reservation)
        require(found.checkpointRunner.exists(_ eq checkpoint),"original confirmed checkpoint runner required")
        found
      case _=>throw new IllegalArgumentException("original recall/checkpoint runner required")
    require(!stopped && entry.workerObserved && entry.accepted && entry.cause.isEmpty &&
      (entry.root eq runner.root),"original healthy readback Entry required before draft detach")
    val draft=entry.dbFirstDraft.getOrElse(throw new IllegalStateException("original confirmed draft missing"))
    runner.requireConfirmedReadbackDraft(draft)
    entry.dbFirstDraft=None
    runner.recordConfirmedDraftDetached(draft)
  }
  /** DATA已由原Session独立持有；不退款Entry预算，不清cause或发行任何确认。 */
  private[mysql] def detachFailedDbFirstRecallDraft(runner:MySqlDbFirstRecallTransaction,
      observed:MySqlDbFirstSession.FailedRecallReadback):Unit = synchronized {
    requireFailedDbFirstRecallObservation(runner)
    val entry=recallEntry(runner.reservation)
    val draft=entry.dbFirstDraft.getOrElse(throw new IllegalStateException("original recall materialization draft missing"))
    runner.requireFailedRecallDraft(draft,observed)
    entry.dbFirstDraft=None // 必须先于runner.before/target来源reader的关闭与退款。
  }
  private[mysql] def failDbFirstRecall(runner:MySqlDbFirstRecallTransaction,cause:Throwable):Unit = synchronized {
    workerOwner()
    require(runner != null && cause != null,"original transaction failure required")
    val entry=recallEntry(runner.reservation)
    require(entry.recallRunner.exists(_ eq runner),"foreign DB-first failure")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
  }

  def requireDbFirstCheckpointSubmission(reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    mainOwner()
    val entry=checkpointEntry(reservation)
    frozen.foreach(throw _)
    require(!stopped && entry.cause.isEmpty && !entry.checkpointSubmissionAttempted &&
      !entry.materializationAttempted && !entry.workerObserved && !entry.accepted && entry.checkpointRunner.isEmpty,
      "DB-first submission is one-shot")
    entry.checkpointSubmissionAttempted=true
  }
  def submittedDbFirstCheckpoint(reservation:MySqlDbFirstCaptureReservation,result:IoSubmission):Unit = synchronized {
    mainOwner()
    val entry=checkpointEntry(reservation)
    require(entry.checkpointSubmissionAttempted && !entry.checkpointSubmissionRecorded && result != null,"registered one-shot DB-first submission result required")
    entry.checkpointSubmissionRecorded=true
    if result == IoSubmission.Accepted then
      entry.accepted=true
      entry.explicitlyRejected=false
    else
      require(!entry.workerObserved && !entry.accepted,"DB-first rejected submission was already observed; responsibility retained")
      entry.explicitlyRejected=true
  }
  def submissionUnknownDbFirstCheckpoint(reservation:MySqlDbFirstCaptureReservation,cause:Throwable):Unit = synchronized {
    mainOwner()
    val entry=checkpointEntry(reservation)
    require(entry.checkpointSubmissionAttempted && cause != null,"original DB-first submission uncertainty required")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
    frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
  }
  private[mysql] def beginDbFirstCheckpoint(reservation:MySqlDbFirstCaptureReservation):MySqlDbFirstCheckpointTransaction = synchronized {
    workerOwner()
    val entry=checkpointEntry(reservation)
    frozen.foreach(throw _)
    require(!stopped && entry.cause.isEmpty && entry.checkpointSubmissionAttempted && !entry.explicitlyRejected &&
      !entry.workerObserved && !entry.materializationAttempted && entry.checkpointRunner.isEmpty,"DB-first execution is one-shot")
    try
      entry.root.bindRetainedImageWorker(imageHandoffCapability)
      entry.root.reserveRetainedMemory(Math.addExact(MySqlDbFirstCheckpointTransaction.HolderBytes,
        MySqlDbFirstCheckpointConfirmation.HolderBytes+MySqlDbFirstCheckpointFinalReceipt.HolderBytes),
        "DB-first runner and one confirmation/drop metadata slot BEFORE allocation")
      val runner=new MySqlDbFirstCheckpointTransaction(reservation,entry.checkpointRequest.get,entry.root)
      entry.checkpointRunner=Some(runner) // 登记先于所有audit、reader、materialization和SQL。
      entry.workerObserved=true;entry.accepted=true
      runner
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private[mysql] def requireExecutingDbFirstCheckpoint(runner:MySqlDbFirstCheckpointTransaction):Unit = synchronized {
    workerOwner()
    require(runner != null,"registered DB-first execution required")
    val entry=checkpointEntry(runner.reservation)
    frozen.foreach(throw _)
    entry.cause.foreach(throw _)
    require(!stopped && entry.checkpointRunner.exists(_ eq runner) && entry.workerObserved && entry.accepted,
      "original DB-first execution was revoked or replaced")
  }
  private[mysql] def materializeExecutingDbFirstCheckpoint(runner:MySqlDbFirstCheckpointTransaction,
      before:MySqlScriptSchemaSnapshot,connectionId:Long,barrierGeneration:UUID,
      plannedAt:LocalDateTime,resultAt:LocalDateTime):MySqlDbFirstRecallMaterialization = synchronized {
    workerOwner()
    require(runner != null,"registered DB-first transaction required")
    val entry=checkpointEntry(runner.reservation)
    require(entry.checkpointRunner.exists(_ eq runner) && entry.workerObserved && entry.accepted,
      "original executing DB-first transaction required")
    materializeDbFirstCheckpoint(runner.reservation,runner.request,before,connectionId,barrierGeneration,plannedAt,resultAt)
  }
  private[mysql] def failDbFirstCheckpoint(runner:MySqlDbFirstCheckpointTransaction,cause:Throwable):Unit = synchronized {
    workerOwner()
    require(runner != null && cause != null,"original transaction failure required")
    val entry=checkpointEntry(runner.reservation)
    require(entry.checkpointRunner.exists(_ eq runner),"foreign DB-first failure")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
  }

  def requireDbFirstInitialActivationSubmission(reservation:MySqlDbFirstCaptureReservation):Unit = synchronized {
    mainOwner()
    val entry=initialEntry(reservation)
    frozen.foreach(throw _)
    require(!stopped && entry.cause.isEmpty && !entry.initialSubmissionAttempted &&
      !entry.materializationAttempted && !entry.workerObserved && !entry.accepted && entry.initialRunner.isEmpty,
      "DB-first submission is one-shot")
    entry.initialSubmissionAttempted=true
  }
  def submittedDbFirstInitialActivation(reservation:MySqlDbFirstCaptureReservation,result:IoSubmission):Unit = synchronized {
    mainOwner()
    val entry=initialEntry(reservation)
    require(entry.initialSubmissionAttempted && !entry.initialSubmissionRecorded && result != null,"registered one-shot DB-first submission result required")
    entry.initialSubmissionRecorded=true
    if result == IoSubmission.Accepted then
      entry.accepted=true
      entry.explicitlyRejected=false
    else
      require(!entry.workerObserved && !entry.accepted,"DB-first rejected submission was already observed; responsibility retained")
      entry.explicitlyRejected=true
  }
  def submissionUnknownDbFirstInitialActivation(reservation:MySqlDbFirstCaptureReservation,cause:Throwable):Unit = synchronized {
    mainOwner()
    val entry=initialEntry(reservation)
    require(entry.initialSubmissionAttempted && cause != null,"original DB-first submission uncertainty required")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
    frozen=MySqlOwnedSessionLifecycle.combine(frozen,cause)
  }
  private[mysql] def beginDbFirstInitialActivation(reservation:MySqlDbFirstCaptureReservation,activeLimit:Int):MySqlDbFirstInitialActivationTransaction = synchronized {
    workerOwner()
    val entry=initialEntry(reservation)
    frozen.foreach(throw _)
    require(activeLimit>=1 && activeLimit<=256,"explicit configured active limit required")
    require(!stopped && entry.cause.isEmpty && entry.initialSubmissionAttempted && !entry.explicitlyRejected &&
      !entry.workerObserved && !entry.materializationAttempted && entry.initialRunner.isEmpty,"DB-first execution is one-shot")
    try
      entry.root.bindRetainedImageWorker(imageHandoffCapability)
      entry.root.reserveRetainedMemory(Math.addExact(MySqlDbFirstInitialActivationTransaction.HolderBytes,MySqlDbFirstInitialActivationConfirmation.HolderBytes+MySqlDbFirstInitialPublicationReceipt.HolderBytes),
        "initial transaction runner BEFORE allocation")
      val runner=new MySqlDbFirstInitialActivationTransaction(reservation,entry.initialActivationRequest.get,entry.root,activeLimit)
      entry.initialRunner=Some(runner) // 登记先于所有audit、reader、materialization和SQL。
      entry.workerObserved=true;entry.accepted=true
      runner
    catch case cause:Throwable=>
      entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
      throw cause
  }
  private[mysql] def requireExecutingDbFirstInitialActivation(runner:MySqlDbFirstInitialActivationTransaction):Unit = synchronized {
    workerOwner()
    require(runner != null,"registered DB-first execution required")
    val entry=initialEntry(runner.reservation)
    frozen.foreach(throw _)
    entry.cause.foreach(throw _)
    require(!stopped && entry.initialRunner.exists(_ eq runner) && entry.workerObserved && entry.accepted,
      "original DB-first execution was revoked or replaced")
  }
  private[mysql] def materializeExecutingDbFirstInitialActivation(runner:MySqlDbFirstInitialActivationTransaction,
      before:MySqlScriptSchemaSnapshot,connectionId:Long,barrierGeneration:UUID,
      plannedAt:LocalDateTime,resultAt:LocalDateTime,activeLimit:Int):MySqlDbFirstInitialActivationMaterialization = synchronized {
    workerOwner()
    require(runner != null,"registered DB-first transaction required")
    val entry=initialEntry(runner.reservation)
    require(entry.initialRunner.exists(_ eq runner) && entry.workerObserved && entry.accepted && runner.activeLimit==activeLimit,
      "original executing DB-first transaction required")
    materializeDbFirstInitialActivation(runner.reservation,runner.request,before,connectionId,barrierGeneration,plannedAt,resultAt,activeLimit)
  }
  private[mysql] def failDbFirstInitialActivation(runner:MySqlDbFirstInitialActivationTransaction,cause:Throwable):Unit = synchronized {
    workerOwner()
    require(runner != null && cause != null,"original transaction failure required")
    val entry=initialEntry(runner.reservation)
    require(entry.initialRunner.exists(_ eq runner),"foreign DB-first failure")
    entry.cause=MySqlOwnedSessionLifecycle.combine(entry.cause,cause)
  }

  /** 只有原worker的DATA物化；没有queue接受、SQL/admission或Main完成语义。 */
  private[mysql] def materializeDbFirstRecall(reservation: MySqlDbFirstCaptureReservation,
      request: MySqlDbFirstRecallRequest, before: MySqlScriptSchemaSnapshot, connectionId: Long,
      barrierGeneration: UUID, plannedAt: LocalDateTime, resultAt: LocalDateTime): MySqlDbFirstRecallMaterialization = synchronized {
    requireLive();workerOwner()
    frozen.foreach(throw _)
    require(reservation != null && (reservation.core eq this) && (reservation.profile eq profile) && request != null,
      "DB-first materialization requires original capture reservation/request")
    val entry=current(reservation.handle)
    require(dbFirstInputs.nonEmpty && entry.captureReservation.exists(_ eq reservation) &&
      entry.dbFirstRequest.exists(_ eq request) && entry.captured.nonEmpty && entry.dbFirstRequestBacking.nonEmpty &&
      entry.plan.isEmpty && !entry.root.isClosed && entry.cause.isEmpty && !stopped && !entry.dropped &&
      !entry.materializationAttempted &&
      ((!entry.workerObserved && !entry.accepted && entry.recallRunner.isEmpty) ||
        (entry.workerObserved && entry.accepted && entry.recallRunner.exists(r => (r.request eq request) && (r.reservation eq reservation)))),
      "DB-first original capture is unavailable, foreign, replayed or already submitted")
    require(before != null && dbFirstInputs.exists(_ eq before.inputs) &&
      !before.allocationBudget.isClosed && before.allocationBudget.sharesAllocationLedger(entry.root),
      "DB-first before requires exact Full27 inputs and original shared ledger")
    entry.materializationAttempted=true
    try
      entry.root.reserveRetainedMemory(1024L,"DB-first caller-retained materialization draft BEFORE allocation")
      val draft=new MySqlDbFirstRecallDraft(entry.root)
      entry.dbFirstDraft=Some(draft)
      if entry.recallRunner.isEmpty then entry.root.bindRetainedImageWorker(imageHandoffCapability)
      MySqlDbFirstRecallMaterializer.materialize(request,entry.captured.get,entry.dbFirstRequestBacking.get,
        before,connectionId,barrierGeneration,plannedAt,resultAt,draft)
    catch
      case cause: Throwable =>
        entry.cause=Some(cause)
        throw cause
  }

  /** checkpoint只在原worker上生成并保留完整DATA；不授queue接受、SQL、Main完成或清理权限。 */
  private[mysql] def materializeDbFirstCheckpoint(reservation: MySqlDbFirstCaptureReservation,
      request: MySqlDbFirstCheckpointRequest, before: MySqlScriptSchemaSnapshot, connectionId: Long,
      barrierGeneration: UUID, plannedAt: LocalDateTime, resultAt: LocalDateTime): MySqlDbFirstRecallMaterialization = synchronized {
    requireLive();workerOwner()
    frozen.foreach(throw _)
    require(reservation != null && (reservation.core eq this) && (reservation.profile eq profile) &&
      reservation.purpose==DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT && request != null,
      "checkpoint materialization requires original capture reservation/request")
    val entry=current(reservation.handle)
    require(dbFirstInputs.nonEmpty && entry.captureReservation.exists(_ eq reservation) &&
      entry.checkpointRequest.exists(_ eq request) && entry.captured.nonEmpty && entry.checkpointBacking.nonEmpty &&
      entry.plan.isEmpty && !entry.root.isClosed && entry.cause.isEmpty && !stopped && !entry.dropped &&
      !entry.materializationAttempted && entry.recallRunner.isEmpty && entry.initialRunner.isEmpty &&
      ((!entry.workerObserved && !entry.accepted && entry.checkpointRunner.isEmpty) ||
        (entry.workerObserved && entry.accepted && entry.checkpointRunner.exists(r =>
          (r.request eq request) && (r.reservation eq reservation)))) ,
      "original checkpoint capture is unavailable, foreign, replayed or already submitted")
    require(before != null && dbFirstInputs.exists(_ eq before.inputs) &&
      !before.allocationBudget.isClosed && before.allocationBudget.sharesAllocationLedger(entry.root),
      "checkpoint before requires exact Full27 inputs and original shared ledger")
    entry.materializationAttempted=true
    try
      entry.root.reserveRetainedMemory(1024L,"checkpoint caller-retained materialization draft BEFORE allocation")
      val draft=new MySqlDbFirstRecallDraft(entry.root)
      entry.dbFirstDraft=Some(draft)
      if entry.checkpointRunner.isEmpty then entry.root.bindRetainedImageWorker(imageHandoffCapability)
      MySqlDbFirstRecallMaterializer.materialize(request,entry.captured.get,entry.checkpointBacking.get,
        before,connectionId,barrierGeneration,plannedAt,resultAt,draft)
    catch
      case cause: Throwable =>
        entry.cause=Some(cause)
        throw cause
  }

  /** 原初建Entry的完整DATA物化；尚不发行SQL或Main活动资格。 */
  private[mysql] def materializeDbFirstInitialActivation(reservation: MySqlDbFirstCaptureReservation,
      request: MySqlDbFirstInitialActivationRequest, before: MySqlScriptSchemaSnapshot, connectionId: Long,
      barrierGeneration: UUID, plannedAt: LocalDateTime, resultAt: LocalDateTime, activeLimit: Int): MySqlDbFirstInitialActivationMaterialization = synchronized {
    requireLive();workerOwner()
    frozen.foreach(throw _)
    require(reservation != null && (reservation.core eq this) && (reservation.profile eq profile) &&
      reservation.purpose==DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION && request != null,
      "initial materialization requires original capture reservation/request")
    val entry=current(reservation.handle)
    require(dbFirstInputs.nonEmpty && entry.captureReservation.exists(_ eq reservation) &&
      entry.initialActivationRequest.exists(_ eq request) && entry.captured.nonEmpty && entry.initialActivationBacking.nonEmpty &&
      entry.plan.isEmpty && !entry.root.isClosed && entry.cause.isEmpty && !stopped && !entry.dropped &&
      !entry.materializationAttempted &&
      ((!entry.workerObserved && !entry.accepted && entry.initialRunner.isEmpty) ||
        (entry.workerObserved && entry.accepted && entry.initialRunner.exists(r =>
          (r.request eq request) && (r.reservation eq reservation) && r.activeLimit==activeLimit))),
      "original initial capture is unavailable, foreign, replayed or already submitted")
    require(before != null && dbFirstInputs.exists(_ eq before.inputs) &&
      !before.allocationBudget.isClosed && before.allocationBudget.sharesAllocationLedger(entry.root),
      "initial before requires exact Full27 inputs and original shared ledger")
    entry.materializationAttempted=true
    try
      entry.root.reserveRetainedMemory(1024L,"initial caller-retained materialization draft BEFORE allocation")
      val draft=new MySqlDbFirstInitialActivationDraft(entry.root)
      entry.initialActivationDraft=Some(draft)
      if entry.initialRunner.isEmpty then entry.root.bindRetainedImageWorker(imageHandoffCapability)
      MySqlDbFirstInitialActivationMaterializer.materialize(request,entry.captured.get,entry.initialActivationBacking.get,
        before,connectionId,barrierGeneration,plannedAt,resultAt,draft,activeLimit)
    catch
      case cause: Throwable =>
        entry.cause=Some(cause)
        throw cause
  }

  private enum Shape:
    case Single, Transaction

  def reserveSingle(tag: IoOperationTag, workflow: UUID, reserve: UUID,
      operation: MySqlRepositoryOperation): MySqlOwnedRepositoryOperationPlan = synchronized {
    requireLive()
    require(operation != null, "original single operation required")
    reserveOperations(tag, workflow, reserve, Vector(operation), Shape.Single)
  }

  def reserveTransaction(tag: IoOperationTag, workflow: UUID, reserve: UUID,
      operations: Vector[MySqlRepositoryOperation]): MySqlOwnedRepositoryOperationPlan = synchronized {
    requireLive()
    require(operations != null && operations.nonEmpty && !operations.exists(_ == null),
      "original immutable transaction operations required")
    reserveOperations(tag, workflow, reserve, operations, Shape.Transaction)
  }

  /** 先留固定slot与表示预算，再调用现有canonical sink；失败保IDs，不签任何SQL结果。 */
  private def reserveOperations(tag: IoOperationTag, workflow: UUID, reserve: UUID,
      operations: Vector[MySqlRepositoryOperation], shape: Shape): MySqlOwnedRepositoryOperationPlan = synchronized {
    requireLive()
    require(dbFirstInputs.isEmpty, "schema6 capture owner cannot issue schema5 repository plans")
    mainOwner(); identity.requireTag(tag)
    frozen.foreach(throw _)
    require(!stopped && !entries.contains(tag) && entries.size < capacity,
      "retained plan capacity/identity is unavailable; no remint")
    var deploymentOriginal = false
    var operationIndex = 0
    while operationIndex < operations.size do
      operations(operationIndex) match
        case MySqlRepositoryOperation.FindOriginalDeploymentOperation(_, _) => deploymentOriginal = true
        case _ => ()
      operationIndex += 1
    if shape == Shape.Transaction && deploymentOriginal then
      throw new IllegalArgumentException("deployment-original read cannot reserve a transaction root")
    val root = if shape == Shape.Single && deploymentOriginal then
      MySqlFormat3ReadBudget.retainedDeploymentOriginalRootOnLedger(
        profile, profile.maxSqlPacketBytes(), ledger, imageHandoffCapability, worker)
    else deploymentInputs match
      case Some(inputs)=>MySqlFormat3ReadBudget.retainedDeploymentReceiptRootOnLedger(inputs,
        Math.min(profile.maxSqlPacketBytes(),inputs.budget.packetBytes),ledger,imageHandoffCapability,worker)
      case None=>MySqlFormat3ReadBudget.retainedRootOnLedger(
        profile, profile.maxSqlPacketBytes(), ledger, imageHandoffCapability, worker)
    nextSerial = Math.addExact(nextSerial, 1L)
    val handle = new MySqlRetainedPlanHandle(ownerIssuer.orNull, nextSerial, tag, this)
    val releaseKind = operations match
      case Vector(MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(_, _, _, _, _, _, _)) => MySqlRetainedPlanReleaseKind.ModeProjection
      case Vector(MySqlRepositoryOperation.CommitTrackedActiveDrift(_, _, _, _)) => MySqlRetainedPlanReleaseKind.DriftProjection
      case _ => MySqlRetainedPlanReleaseKind.Ordinary
    val entry = new Entry(handle, root, releaseKind)
    entries.put(tag, entry)
    try
      root.reserveRetainedMemory(256L, "actual retained plan slot/identity")
      val plan = MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        shape match
          case Shape.Single => MySqlOwnedRepositoryOperationPlan.singleRetained(
            tag, workflow, reserve, operations.head, profile, handle)
          case Shape.Transaction => MySqlOwnedRepositoryOperationPlan.transactionRetained(
            tag, workflow, reserve, operations, profile, handle)
      }
      entry.plan = Some(plan)
      plan
    catch
      case cause: Throwable =>
        entry.cause = Some(cause) // 原ID/失败责任仍保留；无plan可提交。
        throw cause
  }

  def requireSingle(plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation): Unit = synchronized {
    requireLive()
    require((Thread.currentThread() eq worker) || main.exists(_ eq Thread.currentThread()), "exact issuer actor required")
    val entry = original(plan)
    require(!entry.dropped && entry.cause.isEmpty && !entry.root.isClosed, "retained plan is revoked/faulted")
    MySqlRecoveryCanonicalBytes.withAllocationBudget(entry.root) { plan.requireSingle(operation, profile) }
  }

  def requireTransaction(plan: MySqlOwnedRepositoryOperationPlan,
      operations: Vector[MySqlRepositoryOperation]): Unit = synchronized {
    requireLive()
    require((Thread.currentThread() eq worker) || main.exists(_ eq Thread.currentThread()), "exact issuer actor required")
    val entry = original(plan)
    require(!entry.dropped && entry.cause.isEmpty && !entry.root.isClosed, "retained transaction is revoked/faulted")
    MySqlRecoveryCanonicalBytes.withAllocationBudget(entry.root) { plan.requireTransaction(operations, profile) }
  }

  def submitted(plan: MySqlOwnedRepositoryOperationPlan, result: IoSubmission): Unit = synchronized {
    requireLive()
    mainOwner()
    val entry = original(plan)
    require(result != null && !entry.dropped, "exact actual submission result required")
    if result == IoSubmission.Accepted then
      entry.accepted = true
      entry.explicitlyRejected = false
    else
      require(!entry.workerObserved && !entry.accepted, "worker observed a supposedly rejected submission; responsibility is unresolved")
      entry.explicitlyRejected = true
  }

  /** 包内实际session planned borrow先验admission后调用，不能由Main签SQL资格。 */
  def beginExecution(plan: MySqlOwnedRepositoryOperationPlan): MySqlFormat3ReadBudget = synchronized {
    requireLive()
    workerOwner()
    val entry = original(plan)
    require(!entry.workerObserved && !entry.dropped && entry.cause.isEmpty && !stopped,
      "exact original plan cannot replay/execute after revoke")
    frozen.foreach(throw _)
    entry.root.bindRetainedImageWorker(imageHandoffCapability)
    entry.workerObserved = true
    entry.accepted = true // 实际worker入口本身证实actor接受；Main submit返回可能稍后才到。
    entry.root
  }

  def markUnknown(plan: MySqlOwnedRepositoryOperationPlan, cause: Throwable): Unit = synchronized {
    requireLive()
    workerOwner()
    require(cause != null, "original uncertainty cause required")
    val entry = original(plan)
    entry.cause = MySqlOwnedSessionLifecycle.combine(entry.cause, cause)
    frozen = MySqlOwnedSessionLifecycle.combine(frozen, cause)
  }

  /** 仅实际Main Entry移除其plan/completion/projection引用后调用；无caller boolean。 */
  def recordMainDrop(handle: MySqlRetainedPlanHandle): MySqlRetainedPlanDropReceipt = synchronized {
    requireLive()
    mainOwner()
    val entry = current(handle)
    require(entry.journalOriginalTag.isEmpty && entry.preparationReservation.isEmpty && entry.captureReservation.isEmpty && entry.recallRunner.isEmpty,"DB-first capture requires its dedicated confirmed cleanup")
    require(entry.accepted && entry.workerObserved && !entry.dropped && entry.cause.isEmpty && !entry.root.isClosed,
      "only known original accepted responsibility may issue a one-shot drop receipt")
    val receipt = new MySqlRetainedPlanDropReceipt(handle)
    entry.dropped = true
    entry.dropReceipt = Some(receipt)
    receipt
  }

  private[mysql] def requireDropReceipt(receipt: MySqlRetainedPlanDropReceipt): Unit = synchronized {
    requireLive()
    workerOwner()
    require(receipt != null, "actual Main drop receipt required")
    val entry = current(receipt.handle)
    require(entry.journalOriginalTag.isEmpty && entry.preparationReservation.isEmpty && entry.captureReservation.isEmpty && entry.recallRunner.isEmpty,"generic drop cannot retire DB-first capture")
    require(entry.dropped && entry.dropReceipt.exists(_ eq receipt) && entry.accepted &&
      entry.workerObserved && entry.cause.isEmpty, "foreign/replayed/unknown drop receipt cannot release responsibility")
  }

  private[mysql] def requireReleaseKind(receipt: MySqlRetainedPlanDropReceipt, kind: MySqlRetainedPlanReleaseKind): Unit = synchronized {
    requireLive()
    requireDropReceipt(receipt)
    require(kind != null && current(receipt.handle).releaseKind == kind,
      "release purpose differs from original fixed repository operation; cannot bypass projection responsibility")
  }

  private[mysql] def dropWorkerPlanReferences(receipt: MySqlRetainedPlanDropReceipt): Unit = synchronized {
    requireLive()
    requireDropReceipt(receipt)
    current(receipt.handle).plan = None
  }

  private[mysql] def markReleaseFailed(receipt: MySqlRetainedPlanDropReceipt, cause: Throwable): Unit = synchronized {
    requireLive()
    workerOwner(); require(cause != null && receipt != null, "original release failure required")
    val entry = current(receipt.handle)
    entry.cause = MySqlOwnedSessionLifecycle.combine(entry.cause, cause)
    frozen = MySqlOwnedSessionLifecycle.combine(frozen, cause)
  }

  /** Main真实drop后仅取消明确Rejected；未submit、Accepted/Unknown/退休均不能借此门清账。 */
  def cancelExplicitlyRejected(handle: MySqlRetainedPlanHandle): Unit = synchronized {
    requireLive()
    mainOwner()
    frozen.foreach(throw _)
    val entry = current(handle)
    require(entry.journalOriginalTag.isEmpty && entry.preparationReservation.isEmpty && entry.captureReservation.isEmpty && entry.recallRunner.isEmpty,"generic cancellation cannot retire DB-first capture")
    require(!stopped && entry.explicitlyRejected && !entry.accepted && !entry.workerObserved &&
      !entry.dropped && entry.cause.isEmpty, "only exact unaccepted explicitly Rejected responsibility may settle")
    entry.plan = None
    try
      entry.root.closeRootReadBudget()
      entries.remove(handle.tag)
      ()
    catch
      case cause: Throwable =>
        entry.cause = MySqlOwnedSessionLifecycle.combine(entry.cause, cause)
        frozen = MySqlOwnedSessionLifecycle.combine(frozen, cause)
        throw cause
  }

  /** Worker先丢原canonical plan强引用，再释放同root预算；失败保持原slot/cause。 */
  private[mysql] def releaseDropped(receipt: MySqlRetainedPlanDropReceipt): Unit = synchronized {
    requireLive()
    requireDropReceipt(receipt)
    val entry = current(receipt.handle)
    entry.plan = None
    try
      entry.root.closeRootReadBudget()
      entries.remove(receipt.tag)
      ()
    catch
      case cause: Throwable =>
        entry.cause = MySqlOwnedSessionLifecycle.combine(entry.cause, cause)
        frozen = MySqlOwnedSessionLifecycle.combine(frozen, cause)
        throw cause
  }

  /** 只在真实IO handoff成功后同步Main引用；不改变worker/预算/原reservation。 */
  def suspendMain(): Unit = synchronized {
    requireLive()
    mainOwner(); main = None
  }
  def claimMain(actual: Thread): Unit = synchronized {
    requireLive()
    require(actual != null && (Thread.currentThread() eq actual) && (actual ne worker) && main.isEmpty,
      "actual successful IO target claim required")
    main = Some(actual)
  }
  def stop(): Unit = synchronized {
    requireLive()
    stopped = true
  }
  /** 仅封空schema6 issuer的生产入口；不证明SQL/maintenance终止，既有failure永不清除。 */
  private[mysql] def stopEmptyDbFirstAfterFailure():Boolean = synchronized {
    requireLive();mainOwner()
    if dbFirstInputs.nonEmpty && frozen.isEmpty && entries.isEmpty then
      stopped=true
      true
    else false
  }
  /** 原 Fresh producer 已证明未发布业务；这里只封原 issuer 的未使用历史，不签停服成功。 */
  private[mysql] def sealUnusedForFreshCompletion(): Unit = synchronized {
    requireLive()
    mainOwner()
    frozen.foreach(throw _)
    require(!stopped && !freshCompletionSealed && nextSerial == 0L && entries.isEmpty,
      "Fresh completion requires the original unused retained-plan issuer")
    freshCompletionSealed = true
    stopped = true
  }
  private[mysql] def requireFreshCompletionSeal(): Unit = synchronized {
    requireLive()
    workerOwner()
    frozen.foreach(throw _)
    require(freshCompletionSealed && stopped && nextSerial == 0L && entries.isEmpty,
      "original Main Fresh issuer seal is missing or has retained responsibilities")
  }
  def freeze(cause: Throwable): Unit = synchronized {
    requireLive()
    require(cause != null, "actual freeze cause required")
    frozen = MySqlOwnedSessionLifecycle.combine(frozen, cause)
  }
  private[mysql] def rootForOriginal(plan: MySqlOwnedRepositoryOperationPlan): MySqlFormat3ReadBudget = synchronized {
    requireLive()
    workerOwner(); original(plan).root
  }
  private[mysql] def retainedRoots: Int = synchronized {
    requireLive()
    // 已关闭但尚未收口/失败的注册root仍负责任；暂存C root也不能被单business slot掩盖。
    entries.size+entries.valuesIterator.map(_.journalCaptureRoot.size).sum
  }
  private[mysql] def accountedBytes: Long = synchronized {
    requireLive()
    ledger.accountedBytes
  }
  def observation: MySqlRetainedAllocationObservation = synchronized {
    requireLive()
    MySqlRetainedAllocationObservation(ledger.accountedBytes,
      entries.valuesIterator.map(entry => MySqlRetainedPlanState(entry.handle.tag, entry.accepted,
        entry.workerObserved, entry.dropped, entry.cause)).toVector, frozen)
  }


  private[mysql] def validationBudget(handle: MySqlRetainedPlanHandle): MySqlFormat3ReadBudget = synchronized {
    requireLive()
    require((Thread.currentThread() eq worker) || main.exists(_ eq Thread.currentThread()), "validation requires actual current actor")
    val entry = current(handle)
    require(!entry.dropped && entry.cause.isEmpty && !entry.root.isClosed, "retained validation budget is revoked/faulted")
    entry.root
  }

  /** 只在实际Main旧路由排空与Recovery私有登记后迁移原空core；不清failure或丢弃Entry。 */
  private[mysql] def transferDbFirstServiceIdentity(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      adapter:MySqlOwnedIoAdapter,actualIssuer:MySqlOwnedRetainedPlanIssuer,
      previous:MySqlOwnedIoIdentity,next:MySqlOwnedIoIdentity):Boolean = synchronized {
    requireLive();workerOwner()
    require(source!=null && adapter!=null && actualIssuer!=null && ownerIssuer.exists(_ eq actualIssuer) &&
      (identity eq previous) && next!=null && next.bootId==identity.bootId &&
      next.bootGeneration==identity.bootGeneration && next.connectionGeneration>identity.connectionGeneration,
      "actual old issuer/core and new same-boot SQL generation required")
    source.requireServiceIssuerTransfer(adapter,actualIssuer,this,previous,next)
    frozen.foreach(throw _)
    require(!stopped && !freshCompletionSealed && main.nonEmpty && dbFirstInputs.nonEmpty && deploymentInputs.isEmpty,
      "actual active DB-first core/Main ownership required; terminal core cannot resume")
    if entries.nonEmpty then false // 真实旧责任继续由原owner处理；不为恢复服务清空它们。
    else
      identity=next
      true
  }

  /** 只读必要条件；权限来自实际worker hook/Main回执，不由此查询签发。 */
  private[mysql] def requireStoppedEmptyForTerminal(): Unit = synchronized {
    requireLive()
    workerOwner()
    frozen match
      case Some(cause) => throw cause
      case None => ()
    if !stopped || entries.nonEmpty then throw new IllegalStateException("original stopped core retains terminal responsibilities")
  }

  /** 仅撤指定内存强边；原root关闭及ACK必须已由既有结算路径完成，不签业务退出。 */
  private[mysql] def retireSettledMemoryGraph(): Unit = synchronized {
    requireLive()
    workerOwner()
    frozen.foreach(throw _)
    require(stopped && entries.isEmpty, "stopped original core still has retained responsibilities")
    main = None
    ownerIssuer = None
    deploymentInputs=None
    dbFirstInputs=None
    ledger = null
    profile = null
  }
