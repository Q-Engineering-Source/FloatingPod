package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.PlayerEntitlement
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryChunk, PhysicalRecoveryCutoverState, PhysicalRecoveryLocator, PhysicalRecoveryLocatorState, UnboundIntentOrigin}

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.CharBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.security.MessageDigest
import java.util.{HexFormat, UUID}

/** 同一受控 SQL 事务中读取的完整业务与物理恢复图，不代表外部世界清点证明。 */
final case class MySqlFullRepositorySnapshot(
    pods: Vector[MySqlStoredRowData],
    entitlements: Vector[PlayerEntitlement],
    recovery: MySqlRecoveryGraph,
    bootstrapIntents: Vector[MySqlBootstrapIntentData]
):
  require(pods != null && pods.forall(_ != null), "snapshot pod rows are required")
  require(entitlements != null && entitlements.forall(_ != null), "snapshot entitlement rows are required")
  require(recovery != null, "snapshot recovery graph is required")
  require(bootstrapIntents != null && bootstrapIntents.forall(_ != null), "snapshot bootstrap intent rows are required")

  def validate(): Unit = MySqlFullRepositorySnapshot.validate(this)
  lazy val canonicalBytes: Vector[Byte] = MySqlFullRepositorySnapshot.encode(this)
  lazy val canonicalSha256: Vector[Byte] = MessageDigest.getInstance("SHA-256").digest(canonicalBytes.toArray).toVector
  lazy val canonicalSha256Hex: String = HexFormat.of().formatHex(canonicalSha256.toArray)

  /** 摘要不代替比较；精确重放逐字节比较完整规范化图。 */
  def exactlyEquals(other: MySqlFullRepositorySnapshot): Boolean =
    other != null && java.util.Arrays.equals(canonicalBytes.toArray, other.canonicalBytes.toArray)

object MySqlFullRepositorySnapshot:
  private val Domain = "floatingpod.mysql.full-repository-snapshot/v1"

  def empty: MySqlFullRepositorySnapshot =
    MySqlFullRepositorySnapshot(Vector.empty, Vector.empty, MySqlRecoveryGraph(None, Vector.empty, Vector.empty, Vector.empty), Vector.empty)

  private[mysql] def validate(snapshot: MySqlFullRepositorySnapshot): Unit =
    unique(snapshot.pods.map(_.uuidBytes), "floating_pod_state.uuid")
    unique(snapshot.entitlements.map(_.playerUuid), "player_entitlement.player_uuid")
    snapshot.pods.foreach(validatePod)
    snapshot.recovery.validateGraph()
    MySqlBootstrapIntentRows.validateGraph(snapshot.bootstrapIntents)

    val pods = snapshot.pods.map(row => MySqlStoredRows.decodeUuid(row.uuidBytes, "uuid") -> row).toMap
    val locators = snapshot.recovery.locators.map(row => row.podUuid -> row).toMap
    val binding = snapshot.recovery.binding
    if binding.isEmpty && snapshot.bootstrapIntents.nonEmpty then invalid("bootstrap intent committed without its same-transaction binding")
    snapshot.bootstrapIntents.foreach { intent =>
      if !binding.exists(_.bindingUuid == intent.bindingUuid) then invalid("bootstrap binding identity")
      intent.stage match
        case MySqlBootstrapIntentStage.Prepared =>
          if !binding.exists(_.cutoverState == PhysicalRecoveryCutoverState.LegacyUncertified) then invalid("PREPARED requires the actual LEGACY_UNCERTIFIED binding")
        case MySqlBootstrapIntentStage.Complete =>
          if !binding.exists(_.cutoverState == PhysicalRecoveryCutoverState.Certified) then invalid("COMPLETE requires the actual CERTIFIED binding")
    }
    snapshot.recovery.locators.foreach(validateLocator(_, pods))
    snapshot.recovery.intents.foreach(validateIntent(_, binding, pods, locators))

  private def validatePod(row: MySqlStoredRowData): Unit =
    if row.uuidBytes.size != 16 || row.ownerUuidBytes.exists(_.size != 16) then invalid("pod UUID width")
    if row.stateVersion < 0L || row.worldName.trim.isEmpty || row.lastUpdated < 0L then invalid("pod scalar domain")
    if !java.lang.Double.isFinite(row.posX) || !java.lang.Double.isFinite(row.posY) || !java.lang.Double.isFinite(row.posZ) ||
      !java.lang.Double.isFinite(row.yaw) || !java.lang.Double.isFinite(row.pitch) then invalid("pod pose is not finite")
    if row.isActiveFlag != 0 && row.isActiveFlag != 1 then invalid("pod is_active value")
    val materialized = MySqlStoredRows.materializeRawRow(row)
    if row.isActiveFlag == 1 && row.ownerUuidBytes.isEmpty && materialized.operatingMode.databaseValue != "WORLD_IDLE" then
      invalid("active unclaimed pod must use WorldIdle")
    // Active NULL-dimension legacy rows remain representable here; public materialization rejects them.

  private def validateLocator(locator: PhysicalRecoveryLocator, pods: Map[UUID, MySqlStoredRowData]): Unit =
    val needsTicket = Set(
      PhysicalRecoveryLocatorState.CreatePrepared,
      PhysicalRecoveryLocatorState.UnboundAdoptionPrepared,
      PhysicalRecoveryLocatorState.DebugItemCreatePrepared,
      PhysicalRecoveryLocatorState.RowActive,
      PhysicalRecoveryLocatorState.RowInactivePending,
      PhysicalRecoveryLocatorState.RowInactiveVerified
    ).contains(locator.state)
    if needsTicket then
      val dimension = locator.targetDimensionId.getOrElse(invalid("ticket locator dimension"))
      if locator.chunks.size != 25 || locator.chunks.toSet.size != 25 ||
        locator.chunks.exists(_.dimensionId != dimension) || !exactSquare(locator.chunks) then invalid("ticket locator 5x5 chunk inventory")

    locator.state match
      case PhysicalRecoveryLocatorState.CreatePrepared =>
        val row = pods.getOrElse(locator.podUuid, invalid("CREATE_PREPARED requires a domain row"))
        if row.isActiveFlag != 0 || !locator.domainVersion.contains(row.stateVersion) then invalid("CREATE_PREPARED domain version/activity")
      case PhysicalRecoveryLocatorState.RowActive =>
        val row = pods.getOrElse(locator.podUuid, invalid("ROW_ACTIVE requires a domain row"))
        val dim = locator.targetDimensionId.getOrElse(invalid("ROW_ACTIVE dimension"))
        if row.isActiveFlag != 1 || !locator.domainVersion.contains(row.stateVersion) || !row.dimensionId.contains(dim) ||
          locator.chunks.toSet != chunksAt(dim, row.posX, row.posZ) then invalid("ROW_ACTIVE domain/footprint mismatch")
      case PhysicalRecoveryLocatorState.RowInactivePending | PhysicalRecoveryLocatorState.RowInactiveVerified =>
        val row = pods.getOrElse(locator.podUuid, invalid("inactive locator requires a domain row"))
        val dim = locator.targetDimensionId.getOrElse(invalid("inactive locator dimension"))
        if row.isActiveFlag != 0 || !locator.domainVersion.contains(row.stateVersion) || !row.dimensionId.contains(dim) ||
          locator.chunks.toSet != chunksAt(dim, row.posX, row.posZ) then invalid("inactive locator domain/footprint mismatch")
      case _ => ()

  private def validateIntent(
      intent: MySqlRecoveryIntentData,
      binding: Option[PhysicalRecoveryBinding],
      pods: Map[UUID, MySqlStoredRowData],
      locators: Map[UUID, PhysicalRecoveryLocator]
  ): Unit =
    if !binding.exists(_.bindingUuid == intent.bindingUuid) then invalid("unbound intent binding FK")
    val row = pods.getOrElse(intent.podUuid, invalid("unbound intent domain row"))
    val locator = locators.getOrElse(intent.podUuid, invalid("unbound intent locator row"))
    val adoption = intent.origin == UnboundIntentOrigin.ObservedArtifactAdoption
    val prepared = if adoption then PhysicalRecoveryLocatorState.UnboundAdoptionPrepared else PhysicalRecoveryLocatorState.DebugItemCreatePrepared
    intent.phase match
      case MySqlRecoveryIntentPhase.Prepared =>
        if locator.state != prepared || locator.attemptUuid != intent.attemptUuid ||
          !locator.domainVersion.contains(intent.preparedDomainVersion) || !locator.targetDimensionId.contains(intent.targetDimensionId) then
          invalid("PREPARED intent/locator identity")
        if row.isActiveFlag != 0 || row.stateVersion != intent.preparedDomainVersion || row.ownerUuidBytes.nonEmpty ||
          row.operatingModeToken != "WORLD_IDLE" || !row.dimensionId.contains(intent.targetDimensionId) ||
          row.worldName != intent.targetWorldName ||
          !poseEquals(row, intent.targetPosX, intent.targetPosY, intent.targetPosZ, intent.targetYaw, intent.targetPitch) then
          invalid("PREPARED intent/domain target")
        val expected = chunksAt(intent.targetDimensionId, intent.targetPosX, intent.targetPosZ)
        if intent.targetChunkX != chunkCoordinate(intent.targetPosX) || intent.targetChunkZ != chunkCoordinate(intent.targetPosZ) ||
          locator.chunks.toSet != expected then invalid("PREPARED intent exact chunk footprint")
      case MySqlRecoveryIntentPhase.Committed =>
        val committedVersion = intent.committedDomainVersion.getOrElse(invalid("COMMITTED version"))
        if committedVersion != intent.preparedDomainVersion + 1L then invalid("COMMITTED version must follow PREPARED exactly once")
        if locator.attemptUuid != intent.attemptUuid || !locator.domainVersion.contains(row.stateVersion) then
          invalid("COMMITTED intent and current locator identity/version mismatch")
        row.isActiveFlag match
          case 1 if locator.state != PhysicalRecoveryLocatorState.RowActive =>
            invalid("active domain row must retain ROW_ACTIVE locator after unbound commit")
          case 0 if locator.state != PhysicalRecoveryLocatorState.RowInactivePending &&
              locator.state != PhysicalRecoveryLocatorState.RowInactiveVerified =>
            invalid("inactive domain row must retain its inactive locator after unbound commit")
          case 0 | 1 => ()
          case _ => invalid("COMMITTED domain active flag")
        if row.stateVersion < committedVersion then invalid("current domain version predates committed intent")
        if row.stateVersion == committedVersion then
          val dim = intent.committedDimensionId.getOrElse(invalid("COMMITTED dimension"))
          val world = intent.committedWorldName.getOrElse(invalid("COMMITTED world"))
          if row.isActiveFlag != 1 || row.ownerUuidBytes.nonEmpty || row.operatingModeToken != "WORLD_IDLE" ||
            !row.dimensionId.contains(dim) || dim != intent.targetDimensionId || row.worldName != world ||
            !poseEquals(row,
              intent.committedPosX.getOrElse(invalid("COMMITTED pos_x")),
              intent.committedPosY.getOrElse(invalid("COMMITTED pos_y")),
              intent.committedPosZ.getOrElse(invalid("COMMITTED pos_z")),
              intent.committedYaw.getOrElse(invalid("COMMITTED yaw")),
              intent.committedPitch.getOrElse(invalid("COMMITTED pitch"))) then
            invalid("initial COMMITTED intent must match exact active unclaimed WorldIdle domain state")
    val expectedReason = intent.phase match
      case MySqlRecoveryIntentPhase.Prepared => if adoption then "unbound_adoption_prepared" else "debug_item_create_prepared"
      case MySqlRecoveryIntentPhase.Committed => locator.state match
        case PhysicalRecoveryLocatorState.RowActive => "row_active"
        case PhysicalRecoveryLocatorState.RowInactivePending => "row_inactive_pending"
        case PhysicalRecoveryLocatorState.RowInactiveVerified => "fresh_boot_absence_verified"
        case _ => invalid("committed intent locator state")
    if locator.reasonCode != expectedReason then invalid("intent/locator reason")

  private def poseEquals(row: MySqlStoredRowData, x: Double, y: Double, z: Double, yaw: Double, pitch: Double): Boolean =
    sameDouble(row.posX, x) && sameDouble(row.posY, y) && sameDouble(row.posZ, z) &&
      sameDouble(row.yaw, yaw) && sameDouble(row.pitch, pitch)

  private def chunksAt(dimension: Int, x: Double, z: Double): Set[PhysicalRecoveryChunk] =
    val cx = chunkCoordinate(x)
    val cz = chunkCoordinate(z)
    (for x0 <- cx - 2 to cx + 2; z0 <- cz - 2 to cz + 2 yield PhysicalRecoveryChunk(dimension, x0, z0)).toSet

  private def chunkCoordinate(value: Double): Int =
    val floor = Math.floor(value)
    if !java.lang.Double.isFinite(floor) || floor < Int.MinValue.toDouble || floor > Int.MaxValue.toDouble then
      invalid("pose chunk coordinate range")
    floor.toInt >> 4

  private def exactSquare(chunks: List[PhysicalRecoveryChunk]): Boolean =
    val xs = chunks.map(_.chunkX).distinct.sorted
    val zs = chunks.map(_.chunkZ).distinct.sorted
    xs.size == 5 && zs.size == 5 &&
      xs.sliding(2).forall(pair => pair.size < 2 || pair(1).toLong == pair(0).toLong + 1L) &&
      zs.sliding(2).forall(pair => pair.size < 2 || pair(1).toLong == pair(0).toLong + 1L)

  private def unique[A](values: Vector[A], name: String): Unit =
    if values.distinct.size != values.size then invalid("duplicate " + name)
  private def invalid(message: String): Nothing =
    throw new IllegalStateException("MySQL full repository snapshot violates " + message)
  private def sameDouble(a: Double, b: Double): Boolean =
    java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)

  private def encode(snapshot: MySqlFullRepositorySnapshot): Vector[Byte] =
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    try
      text(out, Domain)
      out.writeInt(snapshot.pods.size)
      snapshot.pods.sortBy(row => uuidKey(row.uuidBytes)).foreach(writePod(out, _))
      out.writeInt(snapshot.entitlements.size)
      snapshot.entitlements.sortBy(row => uuidKey(MySqlStoredRows.encodeUuid(row.playerUuid))).foreach { e =>
        uuid(out, e.playerUuid); out.writeInt(e.registrationLimit); out.writeInt(e.simultaneousActiveLimit)
      }
      option(out, snapshot.recovery.binding) { b =>
        uuid(out, b.bindingUuid); text(out, b.cutoverState.databaseValue); out.writeInt(b.formatVersion)
      }
      out.writeInt(snapshot.recovery.locators.size)
      snapshot.recovery.locators.sortBy(l => uuidKey(MySqlStoredRows.encodeUuid(l.podUuid))).foreach { l =>
        uuid(out, l.podUuid); uuid(out, l.attemptUuid); text(out, l.state.databaseValue); text(out, l.reasonCode)
        option(out, l.domainVersion)(out.writeLong); option(out, l.targetDimensionId)(out.writeInt)
        out.writeInt(l.chunks.size)
        l.chunks.sortBy(c => (c.dimensionId, c.chunkX, c.chunkZ)).foreach(writeChunk(out, _))
      }
      out.writeInt(snapshot.recovery.chunks.size)
      snapshot.recovery.chunks.sortBy(c => (uuidKey(MySqlStoredRows.encodeUuid(c.podUuid)), c.chunk.dimensionId, c.chunk.chunkX, c.chunk.chunkZ)).foreach { c =>
        uuid(out, c.podUuid); writeChunk(out, c.chunk)
      }
      out.writeInt(snapshot.recovery.intents.size)
      snapshot.recovery.intents.sortBy(i => uuidKey(MySqlStoredRows.encodeUuid(i.podUuid))).foreach(writeIntent(out, _))
      out.writeInt(snapshot.bootstrapIntents.size)
      snapshot.bootstrapIntents.sortBy(_.singletonFixed1).foreach(writeBootstrap(out, _))
      out.flush()
      bytes.toByteArray.toVector
    finally out.close()

  private def writePod(out: DataOutputStream, r: MySqlStoredRowData): Unit =
    data(out, r.uuidBytes.toArray); option(out, r.ownerUuidBytes)(b => data(out, b.toArray))
    out.writeLong(r.stateVersion); text(out, r.worldName)
    dbl(out, r.posX); dbl(out, r.posY); dbl(out, r.posZ); dbl(out, r.yaw); dbl(out, r.pitch)
    out.writeInt(r.isActiveFlag); out.writeLong(r.lastUpdated); option(out, r.dimensionId)(out.writeInt); text(out, r.operatingModeToken)

  private def writeIntent(out: DataOutputStream, i: MySqlRecoveryIntentData): Unit =
    uuid(out, i.podUuid); uuid(out, i.attemptUuid); uuid(out, i.bindingUuid)
    text(out, i.origin.toString); text(out, i.phase.toString)
    option(out, i.sourceKind)(v => text(out, v.toString)); option(out, i.sourceIdentity)(text(out, _))
    option(out, i.sourceIdentitySha256)(v => data(out, v.toArray)); option(out, i.catalogGeneration)(uuid(out, _))
    option(out, i.createOperationUuid)(uuid(out, _)); out.writeLong(i.preparedDomainVersion); text(out, i.targetWorldName)
    dbl(out, i.targetPosX); dbl(out, i.targetPosY); dbl(out, i.targetPosZ); dbl(out, i.targetYaw); dbl(out, i.targetPitch)
    out.writeInt(i.targetDimensionId); out.writeInt(i.targetChunkX); out.writeInt(i.targetChunkZ)
    option(out, i.committedDomainVersion)(out.writeLong); option(out, i.committedAt)(out.writeLong)
    option(out, i.committedWorldName)(text(out, _)); option(out, i.committedPosX)(dbl(out, _))
    option(out, i.committedPosY)(dbl(out, _)); option(out, i.committedPosZ)(dbl(out, _))
    option(out, i.committedYaw)(dbl(out, _)); option(out, i.committedPitch)(dbl(out, _))
    option(out, i.committedDimensionId)(out.writeInt)

  private def writeBootstrap(out: DataOutputStream, b: MySqlBootstrapIntentData): Unit =
    out.writeInt(b.singletonFixed1); uuid(out, b.bootstrapOperationUuid); uuid(out, b.prepareWriteUuid)
    uuid(out, b.certifyWriteUuid); uuid(out, b.bindingUuid); out.writeInt(b.bindingFormatVersion)
    uuid(out, b.provisioningUuid); text(out, b.sourceWorldIdentity); data(out, b.provisionReceiptSha256.toArray)
    text(out, b.stage.toString); option(out, b.markerAckSha256)(v => data(out, v.toArray))

  private def writeChunk(out: DataOutputStream, c: PhysicalRecoveryChunk): Unit =
    out.writeInt(c.dimensionId); out.writeInt(c.chunkX); out.writeInt(c.chunkZ)
  private def uuid(out: DataOutputStream, id: UUID): Unit =
    out.writeLong(id.getMostSignificantBits); out.writeLong(id.getLeastSignificantBits)
  private def dbl(out: DataOutputStream, value: Double): Unit = out.writeLong(java.lang.Double.doubleToRawLongBits(value))
  private def option[A](out: DataOutputStream, v: Option[A])(write: A => Unit): Unit =
    v match
      case None => out.writeByte(0)
      case Some(value) => out.writeByte(1); write(value)
  private def text(out: DataOutputStream, value: String): Unit =
    val encoded = StandardCharsets.UTF_8.newEncoder()
      .onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
      .encode(CharBuffer.wrap(value))
    val bytes = new Array[Byte](encoded.remaining()); encoded.get(bytes); data(out, bytes)
  private def data(out: DataOutputStream, bytes: Array[Byte]): Unit =
    out.writeInt(bytes.length); out.write(bytes)
  private def uuidKey(bytes: Vector[Byte]): String = HexFormat.of().formatHex(bytes.toArray)
