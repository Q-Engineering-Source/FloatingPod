package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PlayerEntitlement}
import com.wi.floatingpod.storage.*
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.sql.SQLDataException
import java.util.UUID

/** 一次完整关系观察的纯查询视图；不签发 absence、写入或物理资格。 */
private[mysql] final class MySqlFormat3QueryProjection private (
    private val snapshot: MySqlFormat3QueryProjection.SnapshotSource, private val retained: MySqlFormat3ReadBudget,
    private val comparisonOriginalRoot: MySqlFormat3ReadBudget,
    private val comparisonRoot: MySqlFormat3ReadBudget,
    private val continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof]):
  import MySqlFormat3QueryProjection.*
  retained.reserveRetainedMemory(1024L,"F3 query projection structure")
  private val bindingRows = snapshot.relation("physical_recovery_binding").rows
  val binding: Option[PhysicalRecoveryBinding] = bindingRows match
    case Vector() => None
    case Vector(row) =>
      if integer(row,"physical_recovery_binding","singleton_fixed1") != 1L ||
        integer(row,"physical_recovery_binding","format_version") != 2L ||
        text(row,"physical_recovery_binding","cutover_state") != "CERTIFIED" then invalid("certified physical2 binding")
      Some(PhysicalRecoveryBinding(uuid(row,"physical_recovery_binding","binding_uuid"),PhysicalRecoveryCutoverState.Certified,2))
    case _ => invalid("unique binding singleton")
  private def requireBinding(row: Vector[MySqlFormat3Cell], table: String): Unit =
    if !binding.exists(_.bindingUuid == uuid(row,table,"binding_uuid")) then invalid(table + " binding")

  private val pods = snapshot.relation("floating_pod_state").rows.map { row =>
    requireBinding(row,"floating_pod_state")
    charge(512L,"pod DTO sharing original captured text")
    val data = MySqlStoredRowData(bytes(row,"floating_pod_state","uuid"),
      optional(row,"floating_pod_state","owner_uuid")(binaryVector),integer(row,"floating_pod_state","state_version"),
      text(row,"floating_pod_state","world_name"),floating(row,"floating_pod_state","pos_x"),
      floating(row,"floating_pod_state","pos_y"),floating(row,"floating_pod_state","pos_z"),
      floating(row,"floating_pod_state","yaw"),floating(row,"floating_pod_state","pitch"),
      int(row,"floating_pod_state","is_active"),integer(row,"floating_pod_state","last_updated"),
      optional(row,"floating_pod_state","dimension_id")(intCell),text(row,"floating_pod_state","operating_mode"))
    val raw = MySqlStoredRows.materializeRawRow(data)
    if raw.stateVersion < 0L || raw.lastUpdated < 0L || raw.worldName.trim.isEmpty ||
      !Vector(raw.posX,raw.posY,raw.posZ).forall(java.lang.Double.isFinite) ||
      (raw.isActive && raw.ownerUuid.isEmpty && raw.operatingMode.databaseValue != "WORLD_IDLE") then invalid("pod scalar domain")
    raw
  }
  charge(Math.addExact(256L,Math.multiplyExact(pods.size.toLong,96L)),"current pod index")
  private val podIndex = pods.map(row => row.uuid -> row).toMap
  if podIndex.size != pods.size then invalid("duplicate current pod identity")
  private val entitlements = snapshot.relation("player_entitlement").rows.map { row =>
    requireBinding(row,"player_entitlement")
    val registration = int(row,"player_entitlement","registration_limit")
    val simultaneous = int(row,"player_entitlement","simultaneous_active_limit")
    if registration < 0 || simultaneous < 0 then invalid("entitlement negative limit")
    charge(128L,"entitlement DTO")
    PlayerEntitlement(uuid(row,"player_entitlement","player_uuid"),registration,simultaneous)
  }
  charge(Math.addExact(256L,Math.multiplyExact(entitlements.size.toLong,96L)),"entitlement index")
  private val entitlementIndex = entitlements.map(row => row.playerUuid -> row).toMap
  if entitlementIndex.size != entitlements.size then invalid("duplicate entitlement identity")
  private val footprints = snapshot.relation("physical_recovery_footprint").rows
  private val chunks = snapshot.relation("physical_recovery_footprint_chunk").rows
  private val intents = snapshot.relation("physical_recovery_intent").rows
  private val locatorRows = snapshot.relation("physical_recovery_locator").rows

  private type FootprintKey = (UUID,UUID,UUID)
  private def footprintKey(row: Vector[MySqlFormat3Cell],table: String): FootprintKey =
    charge(64L,"footprint key tuple")
    (uuid(row,table,"pod_uuid"),uuid(row,table,"attempt_uuid"),uuid(row,table,"footprint_uuid"))
  charge(Math.addExact(512L,Math.multiplyExact(footprints.size.toLong,128L)),"footprint index")
  private val footprintIndex = footprints.map { row =>
    requireBinding(row,"physical_recovery_footprint")
    footprintKey(row,"physical_recovery_footprint") -> row
  }.toMap
  if footprintIndex.size != footprints.size then invalid("duplicate footprint graph key")
  charge(Math.addExact(512L,Math.multiplyExact(chunks.size.toLong,128L)),"chunk group index")
  private val chunkIndex = chunks.groupBy { row =>
    requireBinding(row,"physical_recovery_footprint_chunk")
    val key=footprintKey(row,"physical_recovery_footprint_chunk")
    if !footprintIndex.contains(key) then invalid("orphan footprint chunk")
    key
  }
  private def footprintFor(pod: UUID, attempt: UUID, footprint: UUID): Vector[MySqlFormat3Cell] =
    footprintIndex.getOrElse((pod,attempt,footprint),invalid("exact footprint graph key"))
  private def decodeFootprintChunks(key: FootprintKey,row: Vector[MySqlFormat3Cell]): List[PhysicalRecoveryChunk] =
    val table = "physical_recovery_footprint"
    val dim = int(row,table,"target_dimension_id")
    val selected = chunkIndex.getOrElse(key,Vector.empty)
    charge(Math.addExact(128L,Math.multiplyExact(selected.size.toLong,128L)),"footprint chunk DTO/list")
    val actual = selected.map { c =>
      PhysicalRecoveryChunk(int(c,"physical_recovery_footprint_chunk","dimension_id"),
        int(c,"physical_recovery_footprint_chunk","chunk_x"),int(c,"physical_recovery_footprint_chunk","chunk_z"))
    }.toList
    temporary(Math.addExact(4096L,Math.multiplyExact(actual.size.toLong,128L)),"footprint sets") {
      val expected = MySqlRepositoryTransitionRules.chunksAt(dim,floating(row,table,"target_pos_x"),floating(row,table,"target_pos_z"))
      val seal = text(row,table,"seal_state")
      if !Set("DRAFT","SEALED").contains(seal) ||
        (seal == "SEALED" && (actual.size != 25 || actual.toSet != expected)) ||
        (seal == "DRAFT" && (actual.size > 25 || actual.toSet.size != actual.size || !actual.toSet.subsetOf(expected))) ||
        int(row,table,"center_chunk_x") != (Math.floor(floating(row,table,"target_pos_x")).toInt >> 4) ||
        int(row,table,"center_chunk_z") != (Math.floor(floating(row,table,"target_pos_z")).toInt >> 4) then invalid("exact footprint inventory")
    }
    actual
  charge(Math.addExact(512L,Math.multiplyExact(footprints.size.toLong,96L)),"decoded footprint index")
  private val decodedChunks = footprintIndex.map { (key,row) => key -> decodeFootprintChunks(key,row) }
  private def footprintChunks(row: Vector[MySqlFormat3Cell], current: Boolean = false): List[PhysicalRecoveryChunk] =
    if current && text(row,"physical_recovery_footprint","seal_state") != "SEALED" then invalid("current footprint must be SEALED")
    decodedChunks(footprintKey(row,"physical_recovery_footprint"))

  private def requireUniqueNullableKey(rows: Vector[Vector[MySqlFormat3Cell]], table: String, names: Vector[String]): Unit =
    temporary(Math.addExact(512L,Math.multiplyExact(rows.size.toLong,Math.addExact(192L,Math.multiplyExact(names.size.toLong,32L)))),"nullable unique-key validation") {
      val present = rows.flatMap { row =>
        val key = names.map(name => cell(row,table,name))
        if key.contains(MySqlFormat3Cell.SqlNull) then None else Some(key)
      }
      if present.distinct.size != present.size then invalid(table + " unique key " + names.mkString(","))
    }
  requireUniqueNullableKey(intents,"physical_recovery_intent",Vector("binding_uuid","create_operation_uuid"))
  requireUniqueNullableKey(intents,"physical_recovery_intent",Vector("binding_uuid","source_kind","catalog_generation","source_identity_sha256"))
  requireUniqueNullableKey(intents,"physical_recovery_intent",Vector("binding_uuid","source_completion_uuid"))
  requireUniqueNullableKey(locatorRows,"physical_recovery_locator",Vector("binding_uuid","current_attempt_uuid"))

  charge(Math.addExact(512L,Math.multiplyExact(snapshot.relation("physical_source_unit").rows.size.toLong,128L)),"source unit reference index")
  private val sourceUnitIndex = snapshot.relation("physical_source_unit").rows.map { row =>
    requireBinding(row,"physical_source_unit")
    uuid(row,"physical_source_unit","unit_uuid") -> row
  }.toMap
  if sourceUnitIndex.size != snapshot.relation("physical_source_unit").rows.size then invalid("duplicate source unit reference")
  charge(Math.addExact(512L,Math.multiplyExact(snapshot.relation("physical_source_forest").rows.size.toLong,192L)),"source forest reference index")
  private val sourceForestIndex = snapshot.relation("physical_source_forest").rows.map { row =>
    requireBinding(row,"physical_source_forest")
    val unit = uuid(row,"physical_source_forest","unit_uuid")
    if !sourceUnitIndex.contains(unit) then invalid("source forest unit reference")
    charge(64L,"source forest reference key")
    (unit,uuid(row,"physical_source_forest","forest_uuid")) -> row
  }.toMap
  if sourceForestIndex.size != snapshot.relation("physical_source_forest").rows.size then invalid("duplicate source forest reference")

  // Validate every history edge rather than taking a latest row or dropping orphan records.
  charge(Math.addExact(256L,Math.multiplyExact(intents.size.toLong,128L)),"intent history index")
  private val intentIndex = intents.map { row =>
    charge(64L,"intent key tuple")
    (uuid(row,"physical_recovery_intent","pod_uuid"),uuid(row,"physical_recovery_intent","attempt_uuid")) -> row
  }.toMap
  if intentIndex.size != intents.size then invalid("duplicate intent history key")
  intents.foreach { row =>
    requireBinding(row,"physical_recovery_intent")
    footprintFor(uuid(row,"physical_recovery_intent","pod_uuid"),uuid(row,"physical_recovery_intent","attempt_uuid"),
      uuid(row,"physical_recovery_intent","target_footprint_uuid"))
    validateUnboundTopology(row)
    optional(row,"physical_recovery_intent","source_unit_uuid")(uuidCell).foreach { unit =>
      val forest = uuid(row,"physical_recovery_intent","source_forest_uuid")
      val source = sourceForestIndex.getOrElse((unit,forest),invalid("intent exact source forest reference"))
      if text(source,"physical_source_forest","forest_state") != "SEALED" then invalid("intent source forest must be SEALED")
    }
  }
  private val locators = locatorRows.map { row =>
    val table = "physical_recovery_locator"; requireBinding(row,table)
    val pod = uuid(row,table,"pod_uuid"); val attempt = uuid(row,table,"current_attempt_uuid")
    val footprint = footprintFor(pod,attempt,uuid(row,table,"current_footprint_uuid"))
    val currentIntent = intentIndex.getOrElse((pod,attempt),invalid("current locator intent"))
    if uuid(currentIntent,"physical_recovery_intent","target_footprint_uuid") != uuid(row,table,"current_footprint_uuid") then
      invalid("current intent/locator footprint")
    val state = PhysicalRecoveryLocatorState.fromDatabase(text(row,table,"locator_state"))
    val dim = int(row,table,"target_dimension_id")
    val version = optional(row,table,"domain_version")(longCell)
    if int(footprint,"physical_recovery_footprint","target_dimension_id") != dim then invalid("current locator footprint dimension")
    charge(256L,"locator DTO")
    val result = PhysicalRecoveryLocator(pod,attempt,state,reason(text(row,table,"reason_code")),version,Some(dim),footprintChunks(footprint,current=true))
    val currentPod = podIndex.get(pod)
    if Set("DEBUG_ITEM_CREATE","UNBOUND_ADOPTION").contains(text(currentIntent,"physical_recovery_intent","origin")) &&
      text(currentIntent,"physical_recovery_intent","phase") == "PREPARED" then
      val actual = currentPod.getOrElse(invalid("prepared unbound domain"))
      val expectedState = if text(currentIntent,"physical_recovery_intent","origin") == "DEBUG_ITEM_CREATE" then
        PhysicalRecoveryLocatorState.DebugItemCreatePrepared else PhysicalRecoveryLocatorState.UnboundAdoptionPrepared
      if state != expectedState || actual.isActive || actual.ownerUuid.nonEmpty || actual.operatingMode.databaseValue != "WORLD_IDLE" ||
        actual.stateVersion != integer(currentIntent,"physical_recovery_intent","expected_domain_version") ||
        version != Some(actual.stateVersion) || actual.dimensionId != Some(dim) ||
        actual.worldName != text(footprint,"physical_recovery_footprint","target_world_name") ||
        java.lang.Double.doubleToRawLongBits(actual.posX) != java.lang.Double.doubleToRawLongBits(floating(footprint,"physical_recovery_footprint","target_pos_x")) ||
        java.lang.Double.doubleToRawLongBits(actual.posY) != java.lang.Double.doubleToRawLongBits(floating(footprint,"physical_recovery_footprint","target_pos_y")) ||
        java.lang.Double.doubleToRawLongBits(actual.posZ) != java.lang.Double.doubleToRawLongBits(floating(footprint,"physical_recovery_footprint","target_pos_z")) ||
        java.lang.Float.floatToRawIntBits(actual.yaw) != java.lang.Float.floatToRawIntBits(exactFloat(floating(footprint,"physical_recovery_footprint","target_yaw"))) ||
        java.lang.Float.floatToRawIntBits(actual.pitch) != java.lang.Float.floatToRawIntBits(exactFloat(floating(footprint,"physical_recovery_footprint","target_pitch"))) then
          invalid("prepared unbound current domain/intent/locator target")
    state match
      case PhysicalRecoveryLocatorState.RowActive | PhysicalRecoveryLocatorState.RowInactivePending | PhysicalRecoveryLocatorState.RowInactiveVerified =>
        val actual = currentPod.getOrElse(invalid("current locator domain"))
        if version != Some(actual.stateVersion) || actual.dimensionId != Some(dim) ||
          actual.isActive != (state == PhysicalRecoveryLocatorState.RowActive) then invalid("current locator domain identity")
        if comparisonOriginalRoot == null then
          requireSourceCurrentChunks(snapshot,retained,result.chunks,dim,actual.posX,actual.posZ)
        else
          snapshot match
            case LegacyFull20(original)=>requireComparisonCurrentChunks(original,comparisonOriginalRoot,comparisonRoot,continuation,retained,result.chunks,dim,actual.posX,actual.posZ)
            case DeploymentReceiptFull26(original,inputs,root)=>
              requireDeploymentComparisonAllocation(original,inputs,root,comparisonOriginalRoot,comparisonRoot,retained)
              compareCurrentChunks(retained,result.chunks,dim,actual.posX,actual.posZ)
            case DecodedDbFirstFull27(original,inputs,root)=>
              requireDecodedDbFirstComparison(original,inputs,root)
              require((retained eq original.allocationBudget) && (comparisonOriginalRoot eq root) && (comparisonRoot eq root),
                "exact decoded comparison allocation required")
              compareCurrentChunks(retained,result.chunks,dim,actual.posX,actual.posZ)
            case DbFirstFull27(original,inputs,root)=>
              requireDbFirstComparisonAllocation(original,inputs,root,comparisonOriginalRoot,comparisonRoot,retained)
              compareCurrentChunks(retained,result.chunks,dim,actual.posX,actual.posZ)
      case _ => ()
    result
  }

  charge(Math.addExact(256L,Math.multiplyExact(locators.size.toLong,96L)),"current locator index")
  private val locatorIndex = locators.map(row => row.podUuid -> row).toMap
  if locatorIndex.size != locators.size then invalid("duplicate current locator identity")

  private def validateUnboundTopology(row: Vector[MySqlFormat3Cell]): Unit =
    val table = "physical_recovery_intent"
    val phase = text(row,table,"phase")
    if !Set("PREPARED","ACTIVATED","CLEANUP_PENDING","CLEANUP_VERIFIED").contains(phase) then invalid("intent phase")
    val committedNames = Vector("activation_operation_uuid","committed_domain_version","committed_at","committed_world_name",
      "committed_pos_x","committed_pos_y","committed_pos_z","committed_yaw","committed_pitch","committed_dimension_id")
    if phase == "ACTIVATED" then
      if committedNames.exists(name => cell(row,table,name) == MySqlFormat3Cell.SqlNull) ||
        integer(row,table,"committed_domain_version") != Math.addExact(integer(row,table,"expected_domain_version"),1L) ||
        integer(row,table,"committed_at") < 0L then invalid("activated intent committed topology")
      Vector("committed_pos_x","committed_pos_y","committed_pos_z").foreach(name => floating(row,table,name))
      exactFloat(floating(row,table,"committed_yaw")); exactFloat(floating(row,table,"committed_pitch"))
    else if committedNames.exists(name => cell(row,table,name) != MySqlFormat3Cell.SqlNull) then invalid("unactivated intent committed NULL topology")
    Vector(("predecessor_attempt_uuid","predecessor_footprint_uuid"),("source_unit_uuid","source_forest_uuid"),
      ("fresh_absence_proof_bytes","fresh_absence_proof_sha256"),("cleanup_proof_bytes","cleanup_proof_sha256")).foreach { (left,right) =>
        if (cell(row,table,left) == MySqlFormat3Cell.SqlNull) != (cell(row,table,right) == MySqlFormat3Cell.SqlNull) then invalid("intent NULL pair " + left)
    }
    val origin = text(row,table,"origin")
    def present(name: String): Boolean = cell(row,table,name) != MySqlFormat3Cell.SqlNull
    if integer(row,table,"expected_domain_version") < 0L then invalid("intent expected version")
    val rearm = Set("REARM_AFTER_RECALL","REARM_AFTER_ABORT_CLEANUP","REARM_AFTER_FRESH_ABSENCE").contains(origin)
    val completedRearm = Set("REARM_AFTER_RECALL","REARM_AFTER_ABORT_CLEANUP").contains(origin)
    if present("predecessor_attempt_uuid") != rearm || present("source_completion_uuid") != completedRearm ||
      present("fresh_absence_proof_bytes") != (origin == "REARM_AFTER_FRESH_ABSENCE") ||
      (origin == "OWNED_RELEASE" && !present("expected_owner_uuid")) ||
      (Set("DEBUG_ITEM_CREATE","UNBOUND_ADOPTION").contains(origin) && present("expected_owner_uuid")) then invalid("intent origin authority NULL topology")
    if (phase == "CLEANUP_VERIFIED" && (!present("cleanup_operation_uuid") || !present("cleanup_proof_bytes"))) ||
      (phase != "CLEANUP_VERIFIED" && present("cleanup_proof_bytes")) ||
      (!Set("CLEANUP_PENDING","CLEANUP_VERIFIED").contains(phase) && present("cleanup_operation_uuid")) then invalid("intent cleanup phase topology")
    Vector(("fresh_absence_proof_bytes","fresh_absence_proof_sha256"),("cleanup_proof_bytes","cleanup_proof_sha256")).foreach { (bytesName,digestName) =>
      cell(row,table,bytesName) match
        case MySqlFormat3Cell.SqlNull => ()
        case MySqlFormat3Cell.Binary(payload) =>
          temporary(768L,"history proof digest") {
            val digest=MessageDigest.getInstance("SHA-256")
            val stream=new java.io.DataOutputStream(new java.security.DigestOutputStream(java.io.OutputStream.nullOutputStream(),digest))
            payload.writeTo(stream)
            cell(row,table,digestName) match
              case MySqlFormat3Cell.Binary(hash) if hash.equalsBytes(digest.digest()) => ()
              case _ => invalid("history proof digest")
          }
        case _ => invalid("history proof binary type")
    }
    text(row,table,"origin") match
      case "DEBUG_ITEM_CREATE" =>
        if Vector("source_kind","source_identity","source_identity_sha256","catalog_generation")
          .exists(name => cell(row,table,name) != MySqlFormat3Cell.SqlNull) || cell(row,table,"create_operation_uuid") == MySqlFormat3Cell.SqlNull then invalid("debug-create source NULL topology")
        uuid(row,table,"create_operation_uuid")
        if integer(row,table,"expected_domain_version") != 0L then invalid("unbound initial version")
      case "UNBOUND_ADOPTION" =>
        if cell(row,table,"create_operation_uuid") != MySqlFormat3Cell.SqlNull || integer(row,table,"expected_domain_version") != 0L then
          invalid("observed-adoption create operation/version")
        sourceKind(text(row,table,"source_kind"))
        uuid(row,table,"catalog_generation")
        val sourceUnit = optional(row,table,"source_unit_uuid")(uuidCell)
        val sourceForest = optional(row,table,"source_forest_uuid")(uuidCell)
        if sourceUnit.isDefined != sourceForest.isDefined then invalid("source unit/forest NULL pair")
        val identity = text(row,table,"source_identity")
        if identity.trim.isEmpty then invalid("observed source identity")
        temporary(Math.addExact(768L,MySqlFormat3Utf8.byteLength(identity)),"source identity digest") {
          val digest = MessageDigest.getInstance("SHA-256").digest(identity.getBytes(java.nio.charset.StandardCharsets.UTF_8))
          cell(row,table,"source_identity_sha256") match
            case MySqlFormat3Cell.Binary(value) if value.equalsBytes(digest) => ()
            case _ => invalid("observed source digest")
        }
      case "OWNED_RELEASE" | "REARM_AFTER_RECALL" | "REARM_AFTER_ABORT_CLEANUP" | "REARM_AFTER_FRESH_ABSENCE" =>
        if Vector("source_kind","source_identity","source_identity_sha256","catalog_generation","create_operation_uuid")
          .exists(name => cell(row,table,name) != MySqlFormat3Cell.SqlNull) then invalid("non-unbound source topology")
      case _ => invalid("intent origin")

  def findByUuid(id: UUID): Option[FloatingPodState] =
    charge(320L,"optional domain state"); pod(id).map(_.toPublicState)
  def findByOwner(id: UUID): List[FloatingPodState] =
    require(id != null,"owner UUID required")
    temporary(Math.addExact(128L,Math.multiplyExact(pods.size.toLong,64L)),"owner filter") { states(pods.filter(_.ownerUuid.contains(id))) }
  def findAllActive(): List[FloatingPodState] =
    temporary(Math.addExact(128L,Math.multiplyExact(pods.size.toLong,64L)),"active filter") { states(pods.filter(_.isActive)) }
  def findAll(): List[FloatingPodState] = states(pods)
  def findAllRecoverable(): List[FloatingPodState] = states(pods)
  def findDesiredActiveRows(): List[StoredPodRow] =
    temporary(Math.addExact(128L,Math.multiplyExact(pods.size.toLong,64L)),"desired-active filter") { outputRows(pods.filter(_.isActive)) }
  def findStoredByUuid(id: UUID): Option[StoredPodRow] = pod(id)
  def findByPlayerUuid(id: UUID): Option[PlayerEntitlement] =
    require(id != null,"player UUID required"); charge(64L,"optional entitlement"); entitlementIndex.get(id)
  def findPhysicalRecoveryBinding(): Option[PhysicalRecoveryBinding] = binding
  def findPhysicalRecoveryLocator(id: UUID): Option[PhysicalRecoveryLocator] =
    require(id != null,"pod UUID required"); charge(64L,"optional locator"); locatorIndex.get(id)
  def findAllPhysicalRecoveryLocators(): List[PhysicalRecoveryLocator] =
    charge(128L + locators.size.toLong * 64L,"locator list"); locators.toList
  def findUnboundPreparation(id: UUID): Option[UnboundPreparationReceipt] =
    require(id != null,"pod UUID required")
    val current = findPhysicalRecoveryLocator(id)
    current.flatMap { locator =>
      val selected = intentIndex.get((id,locator.attemptUuid))
      selected.filter(row => Set("DEBUG_ITEM_CREATE","UNBOUND_ADOPTION").contains(text(row,"physical_recovery_intent","origin"))).map { row =>
        val table = "physical_recovery_intent"
        val footprint = footprintFor(id,locator.attemptUuid,uuid(row,table,"target_footprint_uuid"))
        val ft = "physical_recovery_footprint"
        val origin = text(row,table,"origin") match
          case "DEBUG_ITEM_CREATE" => UnboundIntentOrigin.DebugItemCreate
          case "UNBOUND_ADOPTION" => UnboundIntentOrigin.ObservedArtifactAdoption
        val identity = if origin == UnboundIntentOrigin.DebugItemCreate then None else
          Some(ObservedUnboundIdentity(sourceKind(text(row,table,"source_kind")),
            uuid(row,table,"catalog_generation"),text(row,table,"source_identity")))
        charge(512L,"unbound receipt sharing original captured text")
        UnboundPreparationReceipt(binding.getOrElse(invalid("unbound binding")),id,locator.attemptUuid,origin,
          FloatingPodPose(text(footprint,ft,"target_world_name"),floating(footprint,ft,"target_pos_x"),
            floating(footprint,ft,"target_pos_y"),floating(footprint,ft,"target_pos_z"),
            exactFloat(floating(footprint,ft,"target_yaw")),exactFloat(floating(footprint,ft,"target_pitch")),
            int(footprint,ft,"target_dimension_id")),identity,optional(row,table,"create_operation_uuid")(uuidCell),
          integer(row,table,"expected_domain_version"))
      }
    }
  private def pod(id: UUID): Option[StoredPodRow] =
    require(id != null,"pod UUID required"); charge(64L,"optional pod"); podIndex.get(id)
  private def states(rows: Vector[StoredPodRow]): List[FloatingPodState] =
    charge(128L + rows.size.toLong * 256L,"state result list"); rows.map(_.toPublicState).toList
  private def outputRows(rows: Vector[StoredPodRow]): List[StoredPodRow] =
    charge(128L + rows.size.toLong * 64L,"stored result list"); rows.toList
  private def charge(bytes: Long, label: String): Unit = retained.reserveRetainedMemory(bytes,"F3 query " + label)
  private def binaryVector(value: MySqlFormat3Cell): Vector[Byte] = value match
    case MySqlFormat3Cell.Binary(binary) =>
      val copied = binary.copyBytes(retained,"query UUID")
      charge(128L + copied.length.toLong * 16L,"UUID vector")
      copied.toVector
    case _ => invalid("binary UUID cell")
  private def bytes(row: Vector[MySqlFormat3Cell], table: String, name: String): Vector[Byte] = binaryVector(cell(row,table,name))
  private def uuidCell(value: MySqlFormat3Cell): UUID = value match
    case MySqlFormat3Cell.Binary(binary) if binary.length == 16 =>
      charge(64L,"UUID object")
      val bytes=binary.copyBytes(retained,"query UUID temporary bytes")
      var primary: Throwable = null
      try
        val buffer=ByteBuffer.wrap(bytes)
        new UUID(buffer.getLong(),buffer.getLong())
      catch
        case cause: Throwable => primary=cause; throw cause
      finally
        java.util.Arrays.fill(bytes,0.toByte)
        try retained.releaseRetainedMemory(Math.addExact(bytes.length.toLong,64L),"query UUID temporary bytes")
        catch
          case cleanup: Throwable => if primary==null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
    case _ => invalid("UUID width/type")
  private def temporary[A](bytes: Long,label: String)(body: => A): A =
    retained.reserveTemporaryMemory(bytes,"query " + label)
    var primary: Throwable = null
    try body
    catch
      case cause: Throwable => primary=cause; throw cause
    finally
      try retained.releaseTemporaryMemory(bytes,"query " + label)
      catch
        case cleanup: Throwable => if primary==null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
  private def uuid(row: Vector[MySqlFormat3Cell], table: String, name: String): UUID = uuidCell(cell(row,table,name))

private[mysql] object MySqlFormat3QueryProjection:
  private sealed trait SnapshotSource:
    def relation(table:String):MySqlFormat3RelationSnapshot
  private final case class LegacyFull20(image:MySqlFormat3FullSnapshot) extends SnapshotSource:
    def relation(table:String):MySqlFormat3RelationSnapshot=image.relation(table)
  private final case class DeploymentReceiptFull26(image:MySqlScriptSchemaSnapshot,
      inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget) extends SnapshotSource:
    def relation(table:String):MySqlFormat3RelationSnapshot=image.relation(table)
  private final case class DbFirstFull27(image:MySqlScriptSchemaSnapshot,
      inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget) extends SnapshotSource:
    def relation(table:String):MySqlFormat3RelationSnapshot=image.relation(table)
  /** 仅在原comparison词法作用域内消费，不可用于正常query构造。 */
  private final case class DecodedDbFirstFull27(image:MySqlScriptSchemaSnapshot,
      inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget) extends SnapshotSource:
    def relation(table:String):MySqlFormat3RelationSnapshot=image.relation(table)
  private[mysql] def requireDecodedDbFirstComparison(image:MySqlScriptSchemaSnapshot,
      inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget):Unit =
    val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    require(image != null && inputs != null && root != null && !root.isClosed && !root.isScopedReadBudget &&
      (image.inputs eq inputs) && (inputs.sourceLimitProfile eq root.profile) && image.schema==inputs.schema &&
      inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27 && (image.allocationBudget eq child) &&
      !child.isClosed && child.isCanonicalComparisonChildOf(root) && child.sharesAllocationLedger(root),
      "exact active decoded Full27 comparison image/root required")
    root.requireScriptDataInputs(inputs)
    child.requireScriptDataInputs(inputs)
  private[mysql] def validateDecodedDbFirstComparison(snapshot:MySqlScriptSchemaSnapshot,
      inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget):Unit =
    requireDecodedDbFirstComparison(snapshot,inputs,root)
    val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    inputs.validateRelationsForComparison(snapshot.relations,snapshot.maxAllowedPacketBytes,root,child)
    val manifest=uniqueRow(snapshot.relation("fp_schema_manifest").rows,"decoded schema6 manifest singleton")
    if text(manifest,"fp_schema_manifest","bootstrap_state")!="READY" ||
        integer(manifest,"fp_schema_manifest","format_version")!=6L then invalid("decoded comparison requires READY schema6")
    new MySqlFormat3QueryProjection(DecodedDbFirstFull27(snapshot,inputs,root),child,root,root,None)
    ()
  private[mysql] def requireDbFirstSource(image:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget):Unit =
    require(image!=null && inputs!=null && root!=null && !root.isClosed && !root.isScopedReadBudget &&
      (image.inputs eq inputs) && (inputs.sourceLimitProfile eq root.profile) &&
      inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27 && image.schema==inputs.schema &&
      image.allocationBudget!=null && !image.allocationBudget.isClosed &&
      image.allocationBudget.isSnapshotReadChildOf(root) && image.allocationBudget.sharesAllocationLedger(root),
      "original complete Full27 snapshot-read child/root/exact inputs required")
    root.requireScriptDataInputs(inputs)
  private[mysql] def requireDeploymentSource(image:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget):Unit =
    require(image!=null && inputs!=null && root!=null && !root.isClosed && !root.isScopedReadBudget &&
      (image.inputs eq inputs) && (inputs.sourceLimitProfile eq root.profile) &&
      inputs.dataLayout==MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      image.schema==inputs.schema &&
      image.allocationBudget!=null && !image.allocationBudget.isClosed &&
      ((image.allocationBudget eq root) || image.allocationBudget.isCanonicalChildOf(root)) &&
      image.allocationBudget.sharesAllocationLedger(root),"original complete Full26 query image/root/exact inputs required")
    root.requireScriptDataInputs(inputs)
  private def requireSourceCurrentChunks(source:SnapshotSource,retained:MySqlFormat3ReadBudget,
      chunks:List[PhysicalRecoveryChunk],dimension:Int,posX:Double,posZ:Double):Unit = source match
    case LegacyFull20(image)=>requireCurrentChunks(image,retained,chunks,dimension,posX,posZ)
    case DeploymentReceiptFull26(image,inputs,root)=>
      requireDeploymentSource(image,inputs,root)
      require(retained eq root,"original Full26 query allocation root required")
      compareCurrentChunks(retained,chunks,dimension,posX,posZ)
    case DecodedDbFirstFull27(_,_,_)=>invalid("decoded comparison cannot become current query source")
    case DbFirstFull27(image,inputs,root)=>
      requireDbFirstSource(image,inputs,root)
      requireDbFirstDataAllocation(root,retained)
      compareCurrentChunks(retained,chunks,dimension,posX,posZ)
  private def requireDeploymentComparisonAllocation(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      originalRoot:MySqlFormat3ReadBudget,sourceRoot:MySqlFormat3ReadBudget,comparisonRoot:MySqlFormat3ReadBudget,
      child:MySqlFormat3ReadBudget):Unit =
    requireDeploymentSource(snapshot,inputs,originalRoot)
    require((sourceRoot eq originalRoot) && (comparisonRoot eq originalRoot) && child!=null && !child.isClosed &&
      (child.profile eq originalRoot.profile) && child.isCanonicalComparisonChildOf(originalRoot) &&
      (MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(originalRoot) eq child),
      "Full26 comparison requires original same-root image and actual lexical comparison child")
    child.requireScriptDataInputs(inputs)
  private def requireDbFirstComparisonAllocation(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      originalRoot:MySqlFormat3ReadBudget,sourceRoot:MySqlFormat3ReadBudget,comparisonRoot:MySqlFormat3ReadBudget,
      child:MySqlFormat3ReadBudget):Unit =
    requireDbFirstSource(snapshot,inputs,originalRoot)
    require((sourceRoot eq originalRoot) && (comparisonRoot eq originalRoot) && child!=null && !child.isClosed &&
      (child.profile eq originalRoot.profile) && child.isCanonicalComparisonChildOf(originalRoot) &&
      (MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(originalRoot) eq child),
      "Full27 comparison requires original same-root image and actual lexical comparison child")
    child.requireScriptDataInputs(inputs)
  private[mysql] def validateDbFirstForComparison(snapshot:MySqlScriptSchemaSnapshot,
      inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget):Unit =
    requireDbFirstSource(snapshot,inputs,root)
    val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    requireDbFirstComparisonAllocation(snapshot,inputs,root,root,root,child)
    inputs.validateRelationsForComparison(snapshot.relations,snapshot.maxAllowedPacketBytes,root,child)
    val manifest=uniqueRow(snapshot.relation("fp_schema_manifest").rows,"schema6 comparison manifest singleton")
    if text(manifest,"fp_schema_manifest","bootstrap_state")!="READY" ||
        integer(manifest,"fp_schema_manifest","format_version")!=6L then invalid("schema6 comparison requires READY schema6")
    new MySqlFormat3QueryProjection(DbFirstFull27(snapshot,inputs,root),child,root,root,None)
    ()
  private[mysql] def validateDeploymentReceiptForComparison(snapshot:MySqlScriptSchemaSnapshot,
      inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget):Unit =
    requireDeploymentSource(snapshot,inputs,root)
    val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    requireDeploymentComparisonAllocation(snapshot,inputs,root,root,root,child)
    inputs.validateRelationsForComparison(snapshot.relations,snapshot.maxAllowedPacketBytes,root,child)
    val manifest=uniqueRow(snapshot.relation("fp_schema_manifest").rows,"schema5 comparison manifest singleton")
    if text(manifest,"fp_schema_manifest","bootstrap_state")!="READY" ||
        integer(manifest,"fp_schema_manifest","format_version")!=5L then invalid("schema5 comparison requires READY schema5")
    new MySqlFormat3QueryProjection(DeploymentReceiptFull26(snapshot,inputs,root),child,root,root,None)
    ()
  /** 唯一 current chunk 比较入口；仅校验，不签发 locator/absence，也不释放 raw/DTO。 */
  private[mysql] def requireCurrentChunks(snapshot: MySqlFormat3FullSnapshot, retained: MySqlFormat3ReadBudget,
      chunks: List[PhysicalRecoveryChunk], dimension: Int, posX: Double, posZ: Double): Unit =
    require(snapshot != null && retained != null && chunks != null && !retained.isClosed &&
      (snapshot.allocationBudget eq retained) && (snapshot.limitProfile eq retained.profile),"actual original query root required")
    compareCurrentChunks(retained,chunks,dimension,posX,posZ)

  private def requireComparisonAllocation(snapshot: MySqlFormat3FullSnapshot, sourceRoot: MySqlFormat3ReadBudget,
      comparisonRoot: MySqlFormat3ReadBudget, continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof],
      actualChild: MySqlFormat3ReadBudget): Unit =
    MySqlFencedTransaction.requireControlledDebugSource(snapshot,sourceRoot,comparisonRoot,continuation)
    require(actualChild != null && !actualChild.isClosed &&
      (actualChild.profile eq comparisonRoot.profile) && actualChild.isCanonicalComparisonChildOf(comparisonRoot) &&
      actualChild.sharesAllocationLedger(comparisonRoot) &&
      (MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(comparisonRoot) eq actualChild),
      "comparison requires the exact active canonical comparison child")

  private def requireComparisonCurrentChunks(snapshot: MySqlFormat3FullSnapshot, sourceRoot: MySqlFormat3ReadBudget,
      comparisonRoot: MySqlFormat3ReadBudget, continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof],
      actualChild: MySqlFormat3ReadBudget, chunks: List[PhysicalRecoveryChunk],
      dimension: Int, posX: Double, posZ: Double): Unit =
    requireComparisonAllocation(snapshot,sourceRoot,comparisonRoot,continuation,actualChild)
    require(chunks != null,"actual comparison chunks required")
    compareCurrentChunks(actualChild,chunks,dimension,posX,posZ)

  private def compareCurrentChunks(retained: MySqlFormat3ReadBudget, chunks: List[PhysicalRecoveryChunk],
      dimension: Int, posX: Double, posZ: Double): Unit =
    val scratch=Math.addExact(4096L,Math.multiplyExact(chunks.size.toLong,128L))
    retained.reserveTemporaryMemory(scratch,"query current locator domain chunk comparison")
    var primary: Throwable = null
    try
      if chunks.size != 25 || chunks.toSet != MySqlRepositoryTransitionRules.chunksAt(dimension,posX,posZ) then
        invalid("current locator domain identity")
    catch
      case cause: Throwable => primary=cause; throw cause
    finally
      try retained.releaseTemporaryMemory(scratch,"query current locator domain chunk comparison")
      catch
        case cleanup: Throwable => if primary==null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
  def from(snapshot: MySqlFormat3FullSnapshot, retained: MySqlFormat3ReadBudget): MySqlFormat3QueryProjection =
    createProjection(snapshot,retained,null,null,None)

  /** 完整比较只返回Unit；临时projection/DTO在实际child中计费，不逃逸或签发资格。 */
  private[mysql] def validateForComparison(snapshot: MySqlFormat3FullSnapshot,
      originalRoot: MySqlFormat3ReadBudget): Unit =
    validateForComparison(snapshot,originalRoot,originalRoot,None)

  private[mysql] def validateForComparison(snapshot: MySqlFormat3FullSnapshot,
      sourceRoot: MySqlFormat3ReadBudget, comparisonRoot: MySqlFormat3ReadBudget,
      continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof]): Unit =
    MySqlFencedTransaction.requireControlledDebugSource(snapshot,sourceRoot,comparisonRoot,continuation)
    val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(comparisonRoot)
    requireComparisonAllocation(snapshot,sourceRoot,comparisonRoot,continuation,child)
    createProjection(snapshot,child,sourceRoot,comparisonRoot,continuation)
    ()

  private def createProjection(snapshot: MySqlFormat3FullSnapshot, retained: MySqlFormat3ReadBudget,
      comparisonOriginalRoot: MySqlFormat3ReadBudget,comparisonRoot: MySqlFormat3ReadBudget,
      continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof]): MySqlFormat3QueryProjection =
    require(snapshot != null && retained != null && !retained.isClosed &&
      (snapshot.limitProfile eq retained.profile) && snapshot.allocationBudget.sharesAllocationLedger(retained),"exact open query allocation root required")
    // 先用不分配集合的循环计算完整图校验 scratch；map/zip/PK iterator 均在原 root 保留之后。
    var scratch = 8192L
    var relationIndex = 0
    while relationIndex < snapshot.relations.size do
      val relation = snapshot.relations(relationIndex)
      scratch = Math.addExact(scratch,2048L)
      var rowIndex = 0
      while rowIndex < relation.rows.size do
        scratch = Math.addExact(scratch,Math.addExact(512L,Math.multiplyExact(relation.rows(rowIndex).size.toLong,128L)))
        rowIndex += 1
      relationIndex += 1
    retained.reserveTemporaryMemory(scratch,"query full20 validation scratch")
    var validationFailure: Throwable = null
    try MySqlFormat3SnapshotValidation.validateRelations(snapshot.relations)
    catch
      case cause: Throwable => validationFailure=cause; throw cause
    finally
      try retained.releaseTemporaryMemory(scratch,"query full20 validation scratch")
      catch
        case cleanup: Throwable => if validationFailure==null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(validationFailure,cleanup)
    val manifest = uniqueRow(snapshot.relation("fp_schema_manifest").rows,"manifest singleton")
    if text(manifest,"fp_schema_manifest","bootstrap_state") != "READY" ||
      integer(manifest,"fp_schema_manifest","format_version") != 3L then invalid("normal query requires READY schema3")
    new MySqlFormat3QueryProjection(LegacyFull20(snapshot),retained,comparisonOriginalRoot,comparisonRoot,continuation)
  /** 原worker中的完整DATA解码；不是admission，也不把历史recall payload当成当前活动状态。 */
  private[mysql] def fromDbFirstData(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget):MySqlFormat3QueryProjection =
    fromDbFirstData(snapshot,inputs,root,root)
  private[mysql] def requireDbFirstDataAllocation(root:MySqlFormat3ReadBudget,
      dataBudget:MySqlFormat3ReadBudget):Unit =
    require(root!=null && dataBudget!=null && !root.isClosed && !dataBudget.isClosed &&
      ((dataBudget eq root) || (dataBudget.isCanonicalDataChildOf(root) &&
        dataBudget.sharesAllocationLedger(root) && (dataBudget.profile eq root.profile))),
      "original root or its live direct DATA allocation required")
  private[mysql] def fromDbFirstData(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget,dataBudget:MySqlFormat3ReadBudget):MySqlFormat3QueryProjection =
    requireDbFirstSource(snapshot,inputs,root)
    requireDbFirstDataAllocation(root,dataBudget)
    inputs.validateRelations(snapshot.relations,snapshot.maxAllowedPacketBytes,dataBudget)
    val manifest=uniqueRow(snapshot.relation("fp_schema_manifest").rows,"schema6 DATA manifest singleton")
    if text(manifest,"fp_schema_manifest","bootstrap_state")!="READY" ||
        integer(manifest,"fp_schema_manifest","format_version")!=6L then invalid("schema6 DATA requires READY schema6")
    new MySqlFormat3QueryProjection(DbFirstFull27(snapshot,inputs,root),dataBudget,null,null,None)
  def fromDeploymentReceipt(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget,expected:MySqlSchemaAuditExpectation,actual:MySqlBusinessAdmission):MySqlFormat3QueryProjection =
    requireDeploymentSource(snapshot,inputs,root)
    requireAdmittedDeploymentReceiptSnapshot(snapshot,inputs,root,expected,actual)
    inputs.validateRelations(snapshot.relations,snapshot.maxAllowedPacketBytes,root)
    new MySqlFormat3QueryProjection(DeploymentReceiptFull26(snapshot,inputs,root),root,null,null,None)
  private[mysql] def fromRestrictedDeploymentReceipt(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget,expected:MySqlSchemaAuditExpectation,
      proof:MySqlDeploymentReceiptSession.RestrictedRecall5):MySqlFormat3QueryProjection =
    requireDeploymentSource(snapshot,inputs,root)
    if proof==null then invalid("original restricted schema5 Session proof required")
    proof.requireBootSource(snapshot,inputs,root,expected)
    MySqlDeploymentReceiptReadOwner.requireManifestData(snapshot,expected,root)
    inputs.validateRelations(snapshot.relations,snapshot.maxAllowedPacketBytes,root)
    new MySqlFormat3QueryProjection(DeploymentReceiptFull26(snapshot,inputs,root),root,null,null,None)
  private[mysql] def requireAdmittedDeploymentReceiptSnapshot(snapshot:MySqlScriptSchemaSnapshot,
      inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget,expected:MySqlSchemaAuditExpectation,
      actual:MySqlBusinessAdmission):Unit =
    requireDeploymentSource(snapshot,inputs,root)
    require(expected!=null && actual!=null && expected.profile==MySqlSchemaAuditProfile.DeploymentReceipt1 &&
      expected.expectedPhysicalFormat==MySqlExpectedPhysicalFormat.Format2,"original schema5 expectation/admission required")
    if snapshot.schema!=expected.schema || snapshot.connection.serverUuid!=expected.serverUuid ||
        snapshot.connection.catalog!=expected.schema then invalid("schema5 original server/catalog/schema")
    MySqlDeploymentReceiptReadOwner.requireManifestData(snapshot,expected,root)
    val binding=uniqueRow(snapshot.relation("physical_recovery_binding").rows,"schema5 binding singleton")
    cell(binding,"physical_recovery_binding","binding_uuid") match
      case MySqlFormat3Cell.Binary(value) if value.equalsUuid(actual.bindingUuid)=>()
      case _=>invalid("schema5 actual binding")
    if text(binding,"physical_recovery_binding","cutover_state")!="CERTIFIED" ||
        integer(binding,"physical_recovery_binding","format_version")!=2L then invalid("schema5 certified physical2 binding")
    if actual.bindingUuid!=expected.bindingUuid || snapshot.observationScope.bootUuid!=actual.bootUuid ||
        snapshot.observationScope.bootGeneration!=actual.token.bootGeneration ||
        snapshot.observationScope.connectionGeneration!=actual.token.connectionGeneration then invalid("schema5 actual query generation")
    val writer=uniqueRow(snapshot.relation("fp_runtime_writer").rows,"schema5 writer singleton")
    def matches(name:String,id:UUID):Boolean=cell(writer,"fp_runtime_writer",name) match
      case MySqlFormat3Cell.Binary(value)=>value.equalsUuid(id)
      case _=>false
    if !matches("binding_uuid",actual.bindingUuid) || !matches("owner_boot_uuid",actual.bootUuid) ||
        integer(writer,"fp_runtime_writer","writer_epoch")!=actual.writerEpoch ||
        cell(writer,"fp_runtime_writer","lease_until")!=MySqlFormat3Cell.DateTime(actual.token.leaseUntil) then
      invalid("schema5 same current writer epoch/boot/fixed lease")
  /** 独立批准输入与实际 admission；纯 DTO caller 不能以观测自签资格。 */
  def requireAdmittedSnapshot(snapshot: MySqlFormat3FullSnapshot, expected: MySqlSchemaAuditExpectation,
      actual: MySqlBusinessAdmission): Unit =
    require(snapshot != null && expected != null && actual != null,"actual query admission inputs required")
    val row = uniqueRow(snapshot.relation("fp_schema_manifest").rows,"manifest singleton")
    def digest(hex: String): Vector[Byte] = java.util.HexFormat.of().parseHex(hex).toVector
    val (importState,operation,manifestHash,snapshotHash,receiptHash) = expected.expectedImport match
      case MySqlExpectedNoImport => ("NONE",None,None,None,None)
      case value: MySqlExpectedCommittedImport =>
        ("COMMITTED",Some(value.operationUuid),Some(digest(value.manifestSha256)),Some(digest(value.snapshotSha256)),Some(digest(value.receiptSha256)))
    val locked = MySqlLockedManifest(1,"MYSQL",3,digest(expected.ddlSha256),expected.provisioningUuid,"READY",
      expected.serverUuid,expected.bindingUuid,manifestHash,importState,operation,snapshotHash,receiptHash,Some(digest(expected.objects.canonicalSha256)))
    MySqlFreshFormat3Graph.requireManifestMatches(snapshot,locked,expected.format3Inputs.getOrElse(invalid("approved format3 inputs")))
    MySqlFreshFormat3Graph.requireBindingMatches(snapshot,Some(PhysicalRecoveryBinding(actual.bindingUuid,PhysicalRecoveryCutoverState.Certified,2)))
    if actual.bindingUuid != expected.bindingUuid || snapshot.observationScope.bootUuid != actual.bootUuid ||
      snapshot.observationScope.bootGeneration != actual.token.bootGeneration ||
      snapshot.observationScope.connectionGeneration != actual.token.connectionGeneration then invalid("actual query generation")
    val writer = uniqueRow(snapshot.relation("fp_runtime_writer").rows,"writer singleton")
    def matchesUuid(name: String,id: UUID): Boolean = cell(writer,"fp_runtime_writer",name) match
      case MySqlFormat3Cell.Binary(value) => value.equalsUuid(id)
      case _ => false
    if !matchesUuid("binding_uuid",actual.bindingUuid) || !matchesUuid("owner_boot_uuid",actual.bootUuid) ||
      integer(writer,"fp_runtime_writer","writer_epoch") != actual.writerEpoch ||
      cell(writer,"fp_runtime_writer","lease_until") != MySqlFormat3Cell.DateTime(actual.token.leaseUntil) then invalid("same writer epoch/boot/fixed lease")

  private def invalid(reason: String): Nothing = throw new SQLDataException("MySQL format3 query rejected: " + reason)
  private def cell(row: Vector[MySqlFormat3Cell], table: String, name: String): MySqlFormat3Cell =
    MySqlModeOperationEffectCodec.value(row,table,name)
  private def longCell(value: MySqlFormat3Cell): Long = value match
    case MySqlFormat3Cell.SignedInteger(v) => v
    case _ => invalid("integer cell")
  private def intCell(value: MySqlFormat3Cell): Int =
    val v = longCell(value); if v < Int.MinValue.toLong || v > Int.MaxValue.toLong then invalid("integer range"); v.toInt
  private def integer(row: Vector[MySqlFormat3Cell], table: String, name: String): Long = longCell(cell(row,table,name))
  private def int(row: Vector[MySqlFormat3Cell], table: String, name: String): Int = intCell(cell(row,table,name))
  private def text(row: Vector[MySqlFormat3Cell], table: String, name: String): String = cell(row,table,name) match
    case MySqlFormat3Cell.Text(value) => value
    case _ => invalid("text cell")
  private def floating(row: Vector[MySqlFormat3Cell], table: String, name: String): Double = cell(row,table,name) match
    case MySqlFormat3Cell.Floating(bits) =>
      val value = java.lang.Double.longBitsToDouble(bits); if !java.lang.Double.isFinite(value) then invalid("finite DOUBLE"); value
    case _ => invalid("DOUBLE cell")
  private def exactFloat(value: Double): Float =
    val f = value.toFloat; if !java.lang.Float.isFinite(f) || f.toDouble != value then invalid("exact domain FLOAT"); f
  private def optional[A](row: Vector[MySqlFormat3Cell], table: String, name: String)(decode: MySqlFormat3Cell => A): Option[A] =
    cell(row,table,name) match
      case MySqlFormat3Cell.SqlNull => None
      case value => Some(decode(value))
  private def uniqueRow[A](rows: Vector[A], label: String): A =
    if rows.size != 1 then invalid(label + " cardinality"); rows.head
  private def atMostOne[A](rows: Vector[A], label: String): Option[A] =
    if rows.size > 1 then invalid(label + " cardinality"); rows.headOption
  private def reason(value: String): String = value match
    case "CREATE_PREPARED" => PhysicalRecoveryLocatorReason.CreatePrepared
    case "UNBOUND_ADOPTION_PREPARED" => PhysicalRecoveryLocatorReason.UnboundAdoptionPrepared
    case "DEBUG_ITEM_CREATE_PREPARED" => PhysicalRecoveryLocatorReason.DebugItemCreatePrepared
    case "ROW_ACTIVE" => PhysicalRecoveryLocatorReason.RowActive
    case "ROW_INACTIVE_PENDING" => PhysicalRecoveryLocatorReason.RowInactivePending
    case "FRESH_BOOT_ABSENCE_VERIFIED" => PhysicalRecoveryLocatorReason.FreshBootAbsenceVerified
    case other => other
  private def sourceKind(value: String): ObservedUnboundSourceKind = value match
    case "FRESH" => ObservedUnboundSourceKind.FreshArtifact
    case "LEGACY_UNREGISTERED" => ObservedUnboundSourceKind.LegacyUnregistered
    case _ => invalid("unbound source kind")
