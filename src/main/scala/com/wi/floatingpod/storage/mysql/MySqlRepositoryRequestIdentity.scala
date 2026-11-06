package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PlayerEntitlement, PodOperatingMode}
import com.wi.floatingpod.entity.DbFirstPodSnapshot
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{ObservedUnboundPod, ObservedUnboundUniquenessReceipt, PhysicalRecoveryBinding, PhysicalRecoveryChunk, PhysicalRecoveryLocator, PhysicalRecoveryCutoverState, PhysicalRecoveryLocatorState, StoredPodRow}
import java.io.DataOutputStream
import java.sql.SQLDataException
import java.util.UUID

/** 原始不可变 Main DTO 的完整规范身份；它只是请求身份，不是能力或世界证明。 */
final class MySqlRepositoryRequestIdentity private[mysql] (
    private[mysql] val canonical: MySqlRecoveryCanonicalBytes,
    val requestKind: String
):
  require(canonical != null && requestKind != null, "complete immutable request identity is required")
  def canonicalLength: Int = canonical.length
  def sha256: Vector[Byte] = canonical.sha256
  def sha256Hex: String = canonical.hexSha256

  private[mysql] def exactlyEquals(other: MySqlRepositoryRequestIdentity): Boolean =
    other != null && requestKind == other.requestKind && canonical.exactlyEquals(other.canonical)

  override def equals(other: Any): Boolean = other match
    case value: MySqlRepositoryRequestIdentity => exactlyEquals(value)
    case _ => false
  override def hashCode(): Int = (requestKind, sha256).hashCode
  override def toString: String = s"MySqlRepositoryRequestIdentity[$requestKind,sha256=$sha256Hex]"

object MySqlRepositoryRequestIdentity:
  private val Encoding = "FP_MYSQL_REPOSITORY_REQUEST_BE_V1"

  private[mysql] def dbFirstRecall(tag: IoOperationTag, binding: UUID, pod: UUID,
      owner: Option[UUID], attempt: UUID, version: Long, format: Int, nbtLength: Int, nbtSha: Vector[Byte],
      profileBytes: Vector[Byte], profileSha: Vector[Byte], pose: DbFirstPodSnapshot.Pose,
      root: MySqlFormat3ReadBudget,journal:Option[MySqlDbFirstRecallJournalLink]=None): MySqlRepositoryRequestIdentity =
    require(root != null && !root.isClosed && !root.isScopedReadBudget && tag != null && pose != null &&
      owner != null && version >= 0L && format == DbFirstPodSnapshot.FORMAT_VERSION &&
      nbtLength > 0 && nbtSha.size == 32 && profileSha.size == 32 && profileBytes.nonEmpty,
      "complete original DB-first capture identity required")
    require(journal!=null,"explicit recall journal option required")
    journal.foreach(_.requireFor(tag,owner))
    root.reserveTemporaryMemory(1024L,"DB-first request canonical sink/closure BEFORE allocation")
    var primary: Throwable=null
    try MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val encoded=MySqlRecoveryCanonicalBytes.encode(Encoding,root.profile) { output =>
        writeDbFirstRecallData(output,tag,binding,pod,owner,attempt,version,format,nbtLength,nbtSha,profileBytes,profileSha,pose,journal)
      }
      new MySqlRepositoryRequestIdentity(encoded,"RUN")
    }
    catch case cause: Throwable =>
      primary=cause
      throw cause
    finally
      try root.releaseTemporaryMemory(1024L,"DB-first request canonical sink/closure detached")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)

  /** 初建请求包含原创建操作；与收回使用独立domain，不能互换请求身份。 */
  private[mysql] def dbFirstInitialActivation(snapshot: DbFirstPodSnapshot, tag: IoOperationTag,
      root: MySqlFormat3ReadBudget): MySqlRepositoryRequestIdentity =
    require(snapshot != null && snapshot.purpose() == DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION &&
      snapshot.initialCreationOperationUuid() != null && snapshot.ownerUuid() == null && snapshot.stateVersion() == 0L &&
      tag != null && snapshot.operationUuid() == tag.operationId && root != null && !root.isClosed &&
      !root.isScopedReadBudget && (root.profile eq snapshot.limitProfile()), "original initial capture required")
    root.reserveTemporaryMemory(4096L,"initial capture canonical metadata/digest/profile workspace")
    var primary: Throwable=null
    try MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val encoded=MySqlRecoveryCanonicalBytes.encode(Encoding,root.profile) { output =>
        writeDbFirstSnapshotData(output,tag,snapshot.bindingUuid(),snapshot.podUuid(),None,snapshot.attemptUuid(),
          snapshot.stateVersion(),snapshot.formatVersion(),snapshot.byteLength(),snapshot.sha256().toVector,
          root.profile.canonicalBytes().toVector,root.profile.sha256().toVector,snapshot.pose(),
          Some(snapshot.initialCreationOperationUuid()),DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION)
      }
      new MySqlRepositoryRequestIdentity(encoded,"RUN")
    }
    catch case cause: Throwable =>
      primary=cause
      throw cause
    finally
      try root.releaseTemporaryMemory(4096L,"initial capture canonical workspace detached")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)

  private[mysql] def writeDbFirstRecallData(output: DataOutputStream, tag: IoOperationTag,
      binding: UUID, pod: UUID, owner: Option[UUID], attempt: UUID, version: Long, format: Int,
      nbtLength: Int, nbtSha: Vector[Byte], profileBytes: Vector[Byte], profileSha: Vector[Byte],
      pose: DbFirstPodSnapshot.Pose,journal:Option[MySqlDbFirstRecallJournalLink]=None): Unit =
    writeDbFirstSnapshotData(output,tag,binding,pod,owner,attempt,version,format,nbtLength,nbtSha,profileBytes,profileSha,pose,None,
      DbFirstPodSnapshot.Purpose.RECALL,journal)

  private[mysql] def writeDbFirstInitialActivationData(output: DataOutputStream,
      request: MySqlDbFirstInitialActivationRequest): Unit =
    writeDbFirstSnapshotData(output,request.tag,request.bindingUuid,request.podUuid,request.ownerUuid,
      request.attemptUuid,request.capturedStateVersion,request.snapshotFormatVersion,request.nbtByteLength,
      request.nbtSha256,request.profileBytes,request.profileSha256,request.pose,Some(request.creationOperationUuid),
      DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION)

  /** 活动完整快照使用独立canonical domain；旧recall/initial编码保持逐字节原义。 */
  private[mysql] def dbFirstCheckpoint(snapshot: DbFirstPodSnapshot, tag: IoOperationTag,
      root: MySqlFormat3ReadBudget): MySqlRepositoryRequestIdentity =
    require(snapshot != null && snapshot.purpose() == DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT &&
      tag != null && snapshot.operationUuid() == tag.operationId && root != null && !root.isClosed &&
      !root.isScopedReadBudget && (root.profile eq snapshot.limitProfile()), "original active checkpoint capture required")
    root.reserveTemporaryMemory(4096L,"checkpoint canonical metadata/digest/profile workspace")
    var primary: Throwable=null
    try MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val encoded=MySqlRecoveryCanonicalBytes.encode(Encoding,root.profile) { output =>
        writeDbFirstSnapshotData(output,tag,snapshot.bindingUuid(),snapshot.podUuid(),Option(snapshot.ownerUuid()),
          snapshot.attemptUuid(),snapshot.stateVersion(),snapshot.formatVersion(),snapshot.byteLength(),snapshot.sha256().toVector,
          root.profile.canonicalBytes().toVector,root.profile.sha256().toVector,snapshot.pose(),None,
          DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT)
      }
      new MySqlRepositoryRequestIdentity(encoded,"RUN")
    }
    catch case cause: Throwable =>
      primary=cause
      throw cause
    finally
      try root.releaseTemporaryMemory(4096L,"checkpoint canonical workspace detached")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)

  private[mysql] def writeDbFirstCheckpointData(output: DataOutputStream,
      request: MySqlDbFirstCheckpointRequest): Unit =
    writeDbFirstSnapshotData(output,request.tag,request.bindingUuid,request.podUuid,request.ownerUuid,
      request.attemptUuid,request.capturedStateVersion,request.snapshotFormatVersion,request.nbtByteLength,
      request.nbtSha256,request.profileBytes,request.profileSha256,request.pose,None,
      DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT)

  private def writeDbFirstSnapshotData(output: DataOutputStream, tag: IoOperationTag,
      binding: UUID, pod: UUID, owner: Option[UUID], attempt: UUID, version: Long, format: Int,
      nbtLength: Int, nbtSha: Vector[Byte], profileBytes: Vector[Byte], profileSha: Vector[Byte],
      pose: DbFirstPodSnapshot.Pose, initialCreation: Option[UUID], purpose: DbFirstPodSnapshot.Purpose,
      journal:Option[MySqlDbFirstRecallJournalLink]=None): Unit =
    require(journal!=null && (journal.isEmpty || purpose==DbFirstPodSnapshot.Purpose.RECALL),"journal link is recall-only")
    journal.foreach(_.requireFor(tag,owner))
    token(output,Encoding);output.writeByte(1);output.writeInt(1)
    require(initialCreation.nonEmpty == (purpose == DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION),
      "snapshot request purpose/creation identity disagree")
    token(output,purpose match
      case DbFirstPodSnapshot.Purpose.RECALL => if journal.exists(_.recoveredOriginal.nonEmpty) then "DB_FIRST_RECALL_V3" else if journal.nonEmpty then "DB_FIRST_RECALL_V2" else "DB_FIRST_RECALL_V1"
      case DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION => "DB_FIRST_INITIAL_ACTIVATION_V1"
      case DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT => "DB_FIRST_ACTIVE_CHECKPOINT_V1")
    uuidValue(output,tag.bootId);output.writeLong(tag.bootGeneration);output.writeLong(tag.connectionGeneration)
    uuidValue(output,tag.operationId)
    initialCreation.foreach(uuidValue(output,_))
    uuidValue(output,binding);uuidValue(output,pod);optionUuid(output,owner);uuidValue(output,attempt)
    output.writeLong(version);output.writeInt(format);output.writeInt(nbtLength)
    def bytes(value: Vector[Byte]): Unit =
      output.writeInt(value.size)
      value.foreach(b=>output.writeByte(b.toInt))
    bytes(nbtSha);bytes(profileBytes);bytes(profileSha)
    MySqlRecoveryCanonicalBytes.writeText(output,pose.worldName())
    output.writeInt(pose.dimension())
    output.writeLong(java.lang.Double.doubleToRawLongBits(pose.x()))
    output.writeLong(java.lang.Double.doubleToRawLongBits(pose.y()))
    output.writeLong(java.lang.Double.doubleToRawLongBits(pose.z()))
    output.writeInt(java.lang.Float.floatToRawIntBits(pose.yaw()))
    output.writeInt(java.lang.Float.floatToRawIntBits(pose.pitch()))
    journal.foreach { link=>
      uuidValue(output,link.journalTag.bootId);output.writeLong(link.journalTag.bootGeneration)
      output.writeLong(link.journalTag.connectionGeneration);uuidValue(output,link.journalTag.operationId)
      uuidValue(output,link.actorUuid);optionUuid(output,link.clientRequestUuid);bytes(link.journalRequestSha256)
      link.recoveredOriginal.foreach { original=>
        uuidValue(output,original.bootId);output.writeLong(original.bootGeneration)
        output.writeLong(original.connectionGeneration);uuidValue(output,original.operationId)
      }
    }

  def single(operation: MySqlRepositoryOperation, profile: SourceLimitProfile): MySqlRepositoryRequestIdentity =
    if operation == null then throw new IllegalArgumentException("original immutable repository operation is required")
    val encoded = MySqlRecoveryCanonicalBytes.encode(Encoding, profile) { output =>
      writeSingle(output, operation)
    }
    new MySqlRepositoryRequestIdentity(encoded, "RUN")

  private[mysql] def writeSingle(output: DataOutputStream, operation: MySqlRepositoryOperation): Unit =
    MySqlRecoveryCanonicalBytes.writeToken(output, Encoding)
    output.writeByte(1)
    output.writeInt(1)
    writeOperation(output, operation)

  def transaction(operations: Vector[MySqlRepositoryOperation], profile: SourceLimitProfile): MySqlRepositoryRequestIdentity =
    if operations == null || operations.isEmpty || operations.exists(_ == null) then
      throw new IllegalArgumentException("non-empty immutable repository transaction is required")
    val encoded = MySqlRecoveryCanonicalBytes.encode(Encoding, profile) { output =>
      MySqlRecoveryCanonicalBytes.writeToken(output, Encoding)
      output.writeByte(1)
      output.writeInt(operations.size)
      operations.foreach(writeOperation(output, _))
    }
    new MySqlRepositoryRequestIdentity(encoded, "TRANSACTION")

  private[mysql] def writeOperation(output: DataOutputStream, operation: MySqlRepositoryOperation): Unit =
    if output == null || operation == null then throw new SQLDataException("typed repository request is missing")
    operation match
      case MySqlRepositoryOperation.FindOriginalDeploymentOperation(actor, original) =>
        token(output, "FIND_ORIGINAL_DEPLOYMENT_OPERATION_V1")
        token(output, "FPDC"); uuidValue(output, actor); uuidValue(output, original)
      case MySqlRepositoryOperation.CheckpointNormalRecall(request) =>
        if request == null then throw new SQLDataException("original normal checkpoint request required")
        token(output, "CHECKPOINT_NORMAL_RECALL_F3")
        writeOperation(output, MySqlRepositoryOperation.PrepareNormalRecall(request.original))
        MySqlNormalRecallCanonical.writeRemoval(output, request.removal)
        uuidValue(output, request.ticket.stopRequest); chunkSet(output, request.ticket.footprint.toSet)
        uuidValue(output, request.receiptUuid)
      case MySqlRepositoryOperation.PrepareNormalRecall(request) =>
        if request == null then throw new SQLDataException("original normal recall request required")
        token(output, "PREPARE_NORMAL_RECALL_F3")
        uuidValue(output, request.stopRequest); bindingValue(output, request.expectedBinding)
        stateValue(output, request.criticalBase); poseValue(output, request.finalPose)
        output.writeLong(request.dirtyRevision); uuidValue(output, request.attemptUuid)
        uuidValue(output, request.footprintUuid); chunkSet(output, request.originalFootprint.toSet)
        MySqlRecoveryCanonicalBytes.writeText(output, request.source.worldIdentity)
        MySqlRecoveryCanonicalBytes.writeText(output, request.source.sourceIdentity)
        output.writeByte(request.purpose.wire)
      case MySqlRepositoryOperation.Insert(state) => token(output, "INSERT"); stateValue(output, state)
      case MySqlRepositoryOperation.Update(state) => token(output, "UPDATE"); stateValue(output, state)
      case MySqlRepositoryOperation.UpdateInactiveDomain(request) =>
        if request == null then throw new SQLDataException("complete inactive request required")
        token(output,"UPDATE_INACTIVE_DOMAIN_F3"); bindingValue(output,request.expectedBinding)
        uuidValue(output,request.podUuid); optionUuid(output,request.expectedOwner)
        output.writeLong(request.expectedVersion); stateValue(output,request.desired)
      case MySqlRepositoryOperation.FindByUuid(uuid) => token(output, "FIND_BY_UUID"); uuidValue(output, uuid)
      case MySqlRepositoryOperation.FindByOwner(uuid) => token(output, "FIND_BY_OWNER"); uuidValue(output, uuid)
      case MySqlRepositoryOperation.FindAllActive => token(output, "FIND_ALL_ACTIVE")
      case MySqlRepositoryOperation.FindAll => token(output, "FIND_ALL")
      case MySqlRepositoryOperation.Delete(uuid) => token(output, "DELETE"); uuidValue(output, uuid)
      case MySqlRepositoryOperation.FindByPlayerUuid(uuid) => token(output, "FIND_BY_PLAYER_UUID"); uuidValue(output, uuid)
      case MySqlRepositoryOperation.FindDesiredActiveRows => token(output, "FIND_DESIRED_ACTIVE_ROWS")
      case MySqlRepositoryOperation.FindStoredByUuid(uuid) => token(output, "FIND_STORED_BY_UUID"); uuidValue(output, uuid)
      case MySqlRepositoryOperation.FindAllRecoverable => token(output, "FIND_ALL_RECOVERABLE")
      case MySqlRepositoryOperation.BackfillMissingActiveDimension(uuid, version, dimension) =>
        token(output, "BACKFILL_MISSING_ACTIVE_DIMENSION"); uuidValue(output, uuid); output.writeLong(version); output.writeInt(dimension)
      case MySqlRepositoryOperation.ActivateIfAdmissible(target, owner, version, dimension, limit) =>
        token(output, "ACTIVATE_IF_ADMISSIBLE"); stateValue(output, target); uuidValue(output, owner)
        output.writeLong(version); output.writeInt(dimension); output.writeInt(limit)
      case MySqlRepositoryOperation.DeactivateIfExpectedActive(uuid, owner, version) =>
        token(output, "DEACTIVATE_IF_EXPECTED_ACTIVE"); uuidValue(output, uuid); optionUuid(output, owner); output.writeLong(version)
      case MySqlRepositoryOperation.FindPhysicalRecoveryBinding => token(output, "FIND_PHYSICAL_RECOVERY_BINDING")
      case MySqlRepositoryOperation.CreateOrRetainLegacyUncertifiedBinding(uuid, format) =>
        token(output, "CREATE_OR_RETAIN_LEGACY_UNCERTIFIED_BINDING"); uuidValue(output, uuid); output.writeInt(format)
      case MySqlRepositoryOperation.CertifyPhysicalRecoveryBinding(uuid, format) =>
        token(output, "CERTIFY_PHYSICAL_RECOVERY_BINDING"); uuidValue(output, uuid); output.writeInt(format)
      case MySqlRepositoryOperation.PrepareObservedUnbound(observation, binding, attempt, proof) =>
        token(output, "PREPARE_OBSERVED_UNBOUND"); observed(output, observation); bindingValue(output, binding)
        uuidValue(output, attempt); uniquenessValue(output, proof)
      case MySqlRepositoryOperation.CommitObservedUnbound(binding, pod, attempt, version, pose, limit) =>
        token(output, "COMMIT_OBSERVED_UNBOUND"); bindingValue(output, binding); uuidValue(output, pod)
        uuidValue(output, attempt); output.writeLong(version); poseValue(output, pose); output.writeInt(limit)
      case MySqlRepositoryOperation.PrepareUnboundCreate(binding, pod, attempt, pose, operation) =>
        token(output, "PREPARE_UNBOUND_CREATE"); bindingValue(output, binding); uuidValue(output, pod)
        uuidValue(output, attempt); poseValue(output, pose); uuidValue(output, operation)
      case MySqlRepositoryOperation.CommitUnboundCreate(binding, pod, attempt, version, pose, operation, limit) =>
        token(output, "COMMIT_UNBOUND_CREATE"); bindingValue(output, binding); uuidValue(output, pod)
        uuidValue(output, attempt); output.writeLong(version); poseValue(output, pose); uuidValue(output, operation); output.writeInt(limit)
      case MySqlRepositoryOperation.PrepareControlledDebugCreate(binding, pod, attempt, footprint, pose, create) =>
        token(output, "PREPARE_CONTROLLED_DEBUG_CREATE_F3"); bindingValue(output, binding)
        uuidValue(output, pod); uuidValue(output, attempt); uuidValue(output, footprint)
        poseValue(output, pose); uuidValue(output, create)
      case MySqlRepositoryOperation.CommitControlledDebugCreate(binding, pod, attempt, footprint, version, pose, create, limit) =>
        token(output, "COMMIT_CONTROLLED_DEBUG_CREATE_F3"); bindingValue(output, binding)
        uuidValue(output, pod); uuidValue(output, attempt); uuidValue(output, footprint)
        output.writeLong(version); poseValue(output, pose); uuidValue(output, create); output.writeInt(limit)
      case MySqlRepositoryOperation.FindUnboundPreparation(pod) => token(output, "FIND_UNBOUND_PREPARATION"); uuidValue(output, pod)
      case MySqlRepositoryOperation.PrepareCreateLocator(pod, version, attempt, dimension, chunks) =>
        token(output, "PREPARE_CREATE_LOCATOR"); uuidValue(output, pod); output.writeLong(version)
        uuidValue(output, attempt); output.writeInt(dimension); chunkSet(output, chunks)
      case MySqlRepositoryOperation.ActivatePreparedIfAdmissible(target, owner, version, dimension, limit, attempt) =>
        token(output, "ACTIVATE_PREPARED_IF_ADMISSIBLE"); stateValue(output, target); uuidValue(output, owner)
        output.writeLong(version); output.writeInt(dimension); output.writeInt(limit); uuidValue(output, attempt)
      case MySqlRepositoryOperation.DeactivateTrackedIfExpectedActive(pod, owner, version, attempt) =>
        token(output, "DEACTIVATE_TRACKED_IF_EXPECTED_ACTIVE"); uuidValue(output, pod); optionUuid(output, owner)
        output.writeLong(version); uuidValue(output, attempt)
      case MySqlRepositoryOperation.CommitTrackedActiveDrift(binding, base, desired, attempt) =>
        token(output, "COMMIT_TRACKED_ACTIVE_DRIFT"); bindingValue(output, binding)
        stateValue(output, base); stateValue(output, desired); uuidValue(output, attempt)
      case MySqlRepositoryOperation.UpdateTrackedActiveDrift(state, attempt) =>
        token(output, "UPDATE_TRACKED_ACTIVE_DRIFT"); stateValue(output, state); uuidValue(output, attempt)
      case MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(binding, pod, owner, version, attempt, mode, pose) =>
        token(output, "SET_OPERATING_MODE_TRACKED_IF_OWNED"); bindingValue(output, binding); uuidValue(output, pod)
        uuidValue(output, owner); output.writeLong(version); uuidValue(output, attempt); modeValue(output, mode); poseValue(output, pose)
      case MySqlRepositoryOperation.FindPhysicalRecoveryLocator(pod) => token(output, "FIND_PHYSICAL_RECOVERY_LOCATOR"); uuidValue(output, pod)
      case MySqlRepositoryOperation.FindAllPhysicalRecoveryLocators => token(output, "FIND_ALL_PHYSICAL_RECOVERY_LOCATORS")
      case MySqlRepositoryOperation.VerifyInactiveAbsence(pod, version, attempt) =>
        token(output, "VERIFY_INACTIVE_ABSENCE"); uuidValue(output, pod); output.writeLong(version); uuidValue(output, attempt)

  private def token(output: DataOutputStream, value: String): Unit = MySqlRecoveryCanonicalBytes.writeToken(output, value)
  private def uuidValue(output: DataOutputStream, value: UUID): Unit = MySqlRecoveryCanonicalBytes.writeUuid(output, value)
  private def optionUuid(output: DataOutputStream, value: Option[UUID]): Unit = MySqlRecoveryCanonicalBytes.writeOptionUuid(output, value)

  private def stateValue(output: DataOutputStream, value: FloatingPodState): Unit =
    if value == null then throw new SQLDataException("repository state DTO is missing")
    uuidValue(output, value.uuid); optionUuid(output, value.ownerUuid); output.writeLong(value.stateVersion)
    MySqlRecoveryCanonicalBytes.writeText(output, value.worldName)
    output.writeLong(java.lang.Double.doubleToRawLongBits(value.posX)); output.writeLong(java.lang.Double.doubleToRawLongBits(value.posY))
    output.writeLong(java.lang.Double.doubleToRawLongBits(value.posZ)); output.writeInt(java.lang.Float.floatToRawIntBits(value.yaw))
    output.writeInt(java.lang.Float.floatToRawIntBits(value.pitch)); output.writeByte(if value.isActive then 1 else 0)
    output.writeLong(value.lastUpdated); optionInt(output, value.dimensionId); modeValue(output, value.operatingMode)

  private def storedRow(output: DataOutputStream, value: StoredPodRow): Unit =
    if value == null then throw new SQLDataException("stored row DTO is missing")
    uuidValue(output, value.uuid); optionUuid(output, value.ownerUuid); output.writeLong(value.stateVersion)
    MySqlRecoveryCanonicalBytes.writeText(output, value.worldName)
    output.writeLong(java.lang.Double.doubleToRawLongBits(value.posX)); output.writeLong(java.lang.Double.doubleToRawLongBits(value.posY))
    output.writeLong(java.lang.Double.doubleToRawLongBits(value.posZ)); output.writeInt(java.lang.Float.floatToRawIntBits(value.yaw))
    output.writeInt(java.lang.Float.floatToRawIntBits(value.pitch)); output.writeByte(if value.isActive then 1 else 0)
    output.writeLong(value.lastUpdated); optionInt(output, value.dimensionId); modeValue(output, value.operatingMode)

  private def poseValue(output: DataOutputStream, value: FloatingPodPose): Unit =
    if value == null then throw new SQLDataException("repository pose DTO is missing")
    MySqlRecoveryCanonicalBytes.writeText(output, value.worldName)
    output.writeLong(java.lang.Double.doubleToRawLongBits(value.posX)); output.writeLong(java.lang.Double.doubleToRawLongBits(value.posY))
    output.writeLong(java.lang.Double.doubleToRawLongBits(value.posZ)); output.writeInt(java.lang.Float.floatToRawIntBits(value.yaw))
    output.writeInt(java.lang.Float.floatToRawIntBits(value.pitch)); output.writeInt(value.dimensionId)

  private def modeValue(output: DataOutputStream, value: PodOperatingMode): Unit =
    if value == null then throw new SQLDataException("operating mode DTO is missing")
    token(output, value.databaseValue)

  private def optionInt(output: DataOutputStream, value: Option[Int]): Unit =
    if value == null then throw new SQLDataException("integer option is malformed")
    value match
      case None => output.writeByte(0)
      case Some(number) => output.writeByte(1); output.writeInt(number)

  private def bindingValue(output: DataOutputStream, value: PhysicalRecoveryBinding): Unit =
    if value == null then throw new SQLDataException("recovery binding DTO is missing")
    uuidValue(output, value.bindingUuid)
    value.cutoverState match
      case PhysicalRecoveryCutoverState.LegacyUncertified => token(output, "LEGACY_UNCERTIFIED")
      case PhysicalRecoveryCutoverState.Certified => token(output, "CERTIFIED")
    output.writeInt(value.formatVersion)

  private def chunkSet(output: DataOutputStream, chunks: Set[PhysicalRecoveryChunk]): Unit =
    if chunks == null || chunks.exists(_ == null) then throw new SQLDataException("recovery chunk set is malformed")
    val ordered = chunks.toVector.sortBy(chunk => (chunk.dimensionId, chunk.chunkX, chunk.chunkZ))
    output.writeInt(ordered.size)
    ordered.foreach { chunk => output.writeInt(chunk.dimensionId); output.writeInt(chunk.chunkX); output.writeInt(chunk.chunkZ) }

  private def observed(output: DataOutputStream, value: ObservedUnboundPod): Unit =
    if value == null || value.identity == null then throw new SQLDataException("observed unbound DTO is malformed")
    uuidValue(output, value.podUuid)
    value.identity.sourceKind match
      case com.wi.floatingpod.storage.ObservedUnboundSourceKind.FreshArtifact => token(output, "FRESH_ARTIFACT")
      case com.wi.floatingpod.storage.ObservedUnboundSourceKind.LegacyUnregistered => token(output, "LEGACY_UNREGISTERED")
    uuidValue(output, value.identity.catalogGeneration)
    MySqlRecoveryCanonicalBytes.writeText(output, value.identity.sourceIdentity)
    poseValue(output, value.pose)

  private def uniquenessValue(output: DataOutputStream, value: ObservedUnboundUniquenessReceipt): Unit =
    if value == null then throw new SQLDataException("observed-unbound request identity is missing")
    uuidValue(output, value.bindingUuid)
    observed(output, value.observation)
