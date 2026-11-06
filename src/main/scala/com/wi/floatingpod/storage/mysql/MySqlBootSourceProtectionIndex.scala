package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.server.NormalRecallRecoverySourceCustody
import com.wi.floatingpod.server.source.{SourceForestAllocationBudget, SourceForestCarrierSummary, SourceForestCodec, SourceForestMembershipCodec}
import java.io.IOException
import java.sql.SQLDataException
import java.util.UUID
import scala.collection.mutable

/** 原 full20 的保存保护 DATA，不证明实际世界覆盖、queue freshness 或保存许可。 */
private[floatingpod] final class MySqlBootSourceProtectionIndex private (
    private val exactProfile: SourceLimitProfile,
    val carriers: Vector[MySqlBootSourceProtectionIndex.Carrier],
    private val protectedChunks: Map[MySqlBootSourceProtectionIndex.Chunk, Vector[String]],
    private val unknownChunks: Map[MySqlBootSourceProtectionIndex.Chunk, Vector[String]],
    val unresolved: Vector[MySqlBootSourceProtectionIndex.Unresolved]
):
  import MySqlBootSourceProtectionIndex.*
  def classify(dimension: Int, x: Int, z: Int): Classification =
    val key = Chunk(dimension, x, z)
    protectedChunks.get(key) match
      case Some(reasons) => Protected(reasons)
      case None =>
        val local = unknownChunks.getOrElse(key, Vector.empty)
        if local.nonEmpty || unresolved.nonEmpty then Unknown(local, unresolved) else Unprotected

  /** Copies only this real Boot index's immutable DATA into the exact original custody account. */
  private[floatingpod] def retainForContinuation(exactProfile: SourceLimitProfile,
      exactAccount: SourceForestAllocationBudget.Account,
      exactOwner: NormalRecallRecoverySourceCustody): RetainedData =
    MySqlBootSourceProtectionIndex.retainForContinuation(
      this, exactProfile, exactAccount, exactOwner)

private[floatingpod] object MySqlBootSourceProtectionIndex:
  final case class Chunk(dimension: Int, x: Int, z: Int)
  final case class Carrier(binding: UUID, unit: UUID, forest: UUID, worldIdentity: String,
      saveIdentity: String, loaderIdentity: String, regionIdentity: String, chunk: Chunk, generation: Long)
  final case class Unresolved(code: String, table: String, cause: Option[Throwable] = None)
  sealed trait Classification
  final case class Protected(reasons: Vector[String]) extends Classification
  final case class Unknown(localReasons: Vector[String], global: Vector[Unresolved]) extends Classification
  case object Unprotected extends Classification

  /**
   * Closeable immutable copy; it deliberately holds no SQL snapshot, SQL root, or source Index reference.
   * Returned Carrier/Classification/container members are borrowed from this holder. Consumers must keep
   * this holder open until their real completion ACK; retaining a returned value past close needs its own
   * reservation in the receiving holder and is never covered by this holder's later refund.
   */
  private[floatingpod] final class RetainedData private[MySqlBootSourceProtectionIndex] (
      initialProfile: SourceLimitProfile,
      initialAccount: SourceForestAllocationBudget.Account,
      initialOwner: NormalRecallRecoverySourceCustody,
      initialCharge: Long,
      initialCarriers: Vector[Carrier],
      initialProtectedChunks: Map[Chunk, Vector[String]],
      initialUnknownChunks: Map[Chunk, Vector[String]],
      initialUnresolved: Vector[Unresolved]
  ) extends AutoCloseable:
    private var profile: SourceLimitProfile = initialProfile
    private var account: SourceForestAllocationBudget.Account = initialAccount
    private var owner: NormalRecallRecoverySourceCustody = initialOwner
    private var charge = initialCharge
    private var retainedCarriers: Vector[Carrier] = initialCarriers
    private var retainedProtectedChunks: Map[Chunk, Vector[String]] = initialProtectedChunks
    private var retainedUnknownChunks: Map[Chunk, Vector[String]] = initialUnknownChunks
    private var retainedUnresolved: Vector[Unresolved] = initialUnresolved
    private var closed = false
    private var cleanupFailure: Throwable = null

    private def requireOpen(): Unit = synchronized {
      if cleanupFailure != null then throw cleanupFailure
      if closed then throw new IllegalStateException("retained source protection DATA is closed")
      if profile == null || account == null || owner == null then
        throw new IllegalStateException("retained source protection DATA lost its original allocation binding")
      owner.requireAllocationResponsibility()
    }

    def classify(dimension: Int, x: Int, z: Int): Classification = synchronized {
      requireOpen()
      retainedProtectedChunks.get(Chunk(dimension, x, z)) match
        case Some(reasons) => Protected(reasons)
        case None =>
          val local = retainedUnknownChunks.getOrElse(Chunk(dimension, x, z), Vector.empty)
          if local.nonEmpty || retainedUnresolved.nonEmpty then Unknown(local, retainedUnresolved)
          else Unprotected
    }

    def carrierCount: Int = synchronized { requireOpen(); retainedCarriers.size }

    def carrierAt(index: Int): Carrier = synchronized {
      requireOpen()
      retainedCarriers(index)
    }

    /** Carrier membership is reference identity, not case-class value equality. */
    private[floatingpod] def containsExactCarrier(candidate: Carrier): Boolean = synchronized {
      requireOpen()
      candidate != null && retainedCarriers.exists(_ eq candidate)
    }

    def foreachProtectedChunk(consume: (Chunk, Vector[String]) => Unit): Unit =
      foreachEntry(() => retainedProtectedChunks.iterator, consume)

    def foreachUnknownChunk(consume: (Chunk, Vector[String]) => Unit): Unit =
      foreachEntry(() => retainedUnknownChunks.iterator, consume)

    def foreachUnresolved(consume: Unresolved => Unit): Unit =
      require(consume != null, "source protection DATA consumer required")
      var index = 0
      var continue = true
      while continue do
        val next = synchronized {
          requireOpen()
          if index < retainedUnresolved.size then Some(retainedUnresolved(index)) else None
        }
        next match
          case Some(value) => consume(value); index = Math.addExact(index, 1)
          case None => continue = false

    private def foreachEntry(underlying: () => Iterator[(Chunk, Vector[String])],
        consume: (Chunk, Vector[String]) => Unit): Unit =
      require(consume != null, "source protection DATA consumer required")
      val entries = synchronized { requireOpen(); underlying() }
      var continue = true
      while continue do
        val next = synchronized {
          requireOpen()
          if entries.hasNext then Some(entries.next()) else None
        }
        next match
          case Some((chunk, reasons)) => consume(chunk, reasons)
          case None => continue = false

    override def close(): Unit = synchronized {
      if cleanupFailure != null then throw cleanupFailure
      if closed then return
      val exactAccount = account
      val exactProfile = profile
      val exactOwner = owner
      if exactAccount == null || exactProfile == null || exactOwner == null then
        throw new IllegalStateException("retained source protection DATA lost its original allocation binding")
      // Validate the original Main-owned custody/account before dropping anything; a rejected refund
      // must leave this holder intact and charged for its real owner to settle later.
      exactOwner.requireAllocationOwner()
      exactOwner.requireAllocationResponsibility()
      require(exactAccount.exactProfile eq exactProfile,
        "retained source protection DATA allocation profile changed")
      val exactCharge = charge
      // Drop all DATA references before returning the corresponding SourceAccount reservation.
      profile = null
      account = null
      owner = null
      charge = 0L
      retainedCarriers = null
      retainedProtectedChunks = null
      retainedUnknownChunks = null
      retainedUnresolved = null
      try
        exactAccount.releaseTemporary(exactCharge, "retained Boot source protection DATA closed")
        closed = true
      catch
        case cause: Throwable =>
          cleanupFailure = cause
          throw cause
    }

  private def copiedStringBytes(value: String): Long =
    Math.addExact(64L, Math.multiplyExact(value.length.toLong, 4L))

  private def copiedUuid(value: UUID): UUID = new UUID(value.getMostSignificantBits, value.getLeastSignificantBits)

  private def copyVector[A](values: Vector[A])(copy: A => A): Vector[A] =
    val result = Vector.newBuilder[A]
    result.sizeHint(values.size)
    values.foreach(value => result += copy(value))
    result.result()

  private def copyReasons(source: Map[Chunk, Vector[String]]): Map[Chunk, Vector[String]] =
    val result = Map.newBuilder[Chunk, Vector[String]]
    source.iterator.foreach { case (chunk, reasons) =>
      val copiedChunk = Chunk(chunk.dimension, chunk.x, chunk.z)
      val copiedReasons = copyVector(reasons)(identity)
      result += copiedChunk -> copiedReasons
    }
    result.result()

  private def retainedCopyCharge(carriers: Vector[Carrier], protectedChunks: Map[Chunk, Vector[String]],
      unknownChunks: Map[Chunk, Vector[String]], unresolved: Vector[Unresolved]): Long =
    var total = 1024L // retained holder, four top-level collections, map builders and release bookkeeping
    carriers.foreach { carrier =>
      total = Math.addExact(total, 320L) // carrier, three UUID values and chunk/location structure
      total = Math.addExact(total, copiedStringBytes(carrier.worldIdentity))
      total = Math.addExact(total, copiedStringBytes(carrier.saveIdentity))
      total = Math.addExact(total, copiedStringBytes(carrier.loaderIdentity))
      total = Math.addExact(total, copiedStringBytes(carrier.regionIdentity))
    }
    def addMap(values: Map[Chunk, Vector[String]]): Unit =
      values.foreach { case (_, reasons) =>
        total = Math.addExact(total, 256L) // map node, cloned chunk key and reasons-vector holder
        reasons.foreach(reason => total = Math.addExact(total, Math.addExact(32L, copiedStringBytes(reason))))
      }
    addMap(protectedChunks)
    addMap(unknownChunks)
    unresolved.foreach { value =>
      total = Math.addExact(total, 160L) // unresolved entry and optional cause holder
      total = Math.addExact(total, copiedStringBytes(value.code))
      total = Math.addExact(total, copiedStringBytes(value.table))
    }
    total

  private def copyCarrier(value: Carrier): Carrier =
    Carrier(copiedUuid(value.binding), copiedUuid(value.unit), copiedUuid(value.forest),
      value.worldIdentity, value.saveIdentity, value.loaderIdentity, value.regionIdentity,
      Chunk(value.chunk.dimension, value.chunk.x, value.chunk.z), value.generation)

  private def copyUnresolved(value: Unresolved): Unresolved =
    Unresolved(value.code, value.table, value.cause.map(identity))

  private def retainForContinuation(index: MySqlBootSourceProtectionIndex,
      exactProfile: SourceLimitProfile, exactAccount: SourceForestAllocationBudget.Account,
      exactOwner: NormalRecallRecoverySourceCustody): RetainedData =
    require(index != null && exactProfile != null && exactAccount != null && exactOwner != null,
      "original Boot protection index/profile/SourceAccount/custody required")
    require((index.exactProfile eq exactProfile) && (exactAccount.exactProfile eq exactProfile),
      "exact original Boot/source allocation profile required")
    exactOwner.requireAllocationOwner()
    val fee = retainedCopyCharge(index.carriers, index.protectedChunks, index.unknownChunks, index.unresolved)
    exactAccount.reserveTemporary(fee, "retained Boot source protection DATA BEFORE copy")
    var copiedCarriers: Vector[Carrier] = null
    var copiedProtectedChunks: Map[Chunk, Vector[String]] = null
    var copiedUnknownChunks: Map[Chunk, Vector[String]] = null
    var copiedUnresolved: Vector[Unresolved] = null
    try
      copiedCarriers = copyVector(index.carriers)(copyCarrier)
      copiedProtectedChunks = copyReasons(index.protectedChunks)
      copiedUnknownChunks = copyReasons(index.unknownChunks)
      copiedUnresolved = copyVector(index.unresolved)(copyUnresolved)
      new RetainedData(exactProfile, exactAccount, exactOwner, fee, copiedCarriers,
        copiedProtectedChunks, copiedUnknownChunks, copiedUnresolved)
    catch
      case cause: Throwable =>
        copiedCarriers = null
        copiedProtectedChunks = null
        copiedUnknownChunks = null
        copiedUnresolved = null
        try exactAccount.releaseTemporary(fee, "failed retained Boot source protection DATA copy")
        catch case cleanup: Throwable => if cleanup ne cause then cause.addSuppressed(cleanup)
        throw cause

  private type Row = Vector[MySqlFormat3Cell]
  private sealed trait CompleteSource:
    def relations:Vector[MySqlFormat3RelationSnapshot]
    def relation(table:String):MySqlFormat3RelationSnapshot
    def workspace(root:MySqlFormat3ReadBudget):MySqlFormat3ReadBudget
    def requireGraph(root:MySqlFormat3ReadBudget):Unit
  private final case class LegacyFull20(image:MySqlFormat3FullSnapshot) extends CompleteSource:
    def relations:Vector[MySqlFormat3RelationSnapshot]=image.relations
    def relation(table:String):MySqlFormat3RelationSnapshot=image.relation(table)
    def workspace(root:MySqlFormat3ReadBudget):MySqlFormat3ReadBudget=root.newCanonicalScope()
    def requireGraph(root:MySqlFormat3ReadBudget):Unit=MySqlControlledDebugRelations.requireValidFullGraph(image,root)
  private final case class DeploymentReceiptFull26(image:MySqlScriptSchemaSnapshot,
      inputs:MySqlScriptSchemaInputs) extends CompleteSource:
    def relations:Vector[MySqlFormat3RelationSnapshot]=image.relations
    def relation(table:String):MySqlFormat3RelationSnapshot=image.relation(table)
    def workspace(root:MySqlFormat3ReadBudget):MySqlFormat3ReadBudget=root.newCanonicalDataScope()
    def requireGraph(root:MySqlFormat3ReadBudget):Unit=MySqlControlledDebugRelations.requireValidDeploymentReceiptGraph(image,inputs,root)
  private final case class DbFirstFull27(image:MySqlScriptSchemaSnapshot,
      inputs:MySqlScriptSchemaInputs) extends CompleteSource:
    def relations:Vector[MySqlFormat3RelationSnapshot]=image.relations
    def relation(table:String):MySqlFormat3RelationSnapshot=image.relation(table)
    def workspace(root:MySqlFormat3ReadBudget):MySqlFormat3ReadBudget=root.newCanonicalDataScope()
    def requireGraph(root:MySqlFormat3ReadBudget):Unit=MySqlControlledDebugRelations.requireValidDbFirstGraph(image,inputs,root)
  private[mysql] def fromDbFirst(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget):MySqlBootSourceProtectionIndex =
    fromDbFirst(snapshot,inputs,root,root)
  private[mysql] def fromDbFirst(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget,dataBudget:MySqlFormat3ReadBudget):MySqlBootSourceProtectionIndex =
    MySqlFormat3QueryProjection.requireDbFirstSource(snapshot,inputs,root)
    MySqlFormat3QueryProjection.requireDbFirstDataAllocation(root,dataBudget)
    build(DbFirstFull27(snapshot,inputs),root,dataBudget)
  private[mysql] def from(snapshot: MySqlFormat3FullSnapshot, root: MySqlFormat3ReadBudget): MySqlBootSourceProtectionIndex =
    require(snapshot != null && root != null && !root.isClosed &&
      (snapshot.limitProfile eq root.profile) && snapshot.allocationBudget.sharesAllocationLedger(root),
      "source protection requires the original open full20 allocation root")
    build(LegacyFull20(snapshot),root)
  private[mysql] def fromDeploymentReceipt(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget):MySqlBootSourceProtectionIndex =
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,inputs,root)
    build(DeploymentReceiptFull26(snapshot,inputs),root)
  /** 计划派生的保护 DATA 只记入 Fenced 为该计划签发的确切子账户，不接受调用方传入的子账户。 */
  private[mysql] def fromDeploymentReceipt(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      exactRoot:MySqlFormat3ReadBudget,
      exactAllocation:MySqlFencedTransaction.SourceReservePlanAllocation,
      exactParent:MySqlFencedTransaction.RestrictedSourceParent5,
      exactLedger:MySqlFormat3AllocationLedger,
      exactIntent:MySqlOwnedIoAdapter.RestrictedSourceReserveIntent):MySqlBootSourceProtectionIndex =
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,inputs,exactRoot)
    require(exactAllocation != null && exactParent != null && exactLedger != null && exactIntent != null,
      "exact original source RESERVE plan allocation lineage required")
    exactAllocation.withPlanDataBudget(exactParent,exactRoot,exactLedger,exactIntent) { planDataBudget =>
      build(DeploymentReceiptFull26(snapshot,inputs),exactRoot,planDataBudget)
    }
  private def build(snapshot:CompleteSource,root:MySqlFormat3ReadBudget):MySqlBootSourceProtectionIndex =
    build(snapshot,root,root)
  private def build(snapshot:CompleteSource,root:MySqlFormat3ReadBudget,
      dataBudget:MySqlFormat3ReadBudget):MySqlBootSourceProtectionIndex =
    require(snapshot != null && root != null && dataBudget != null && !root.isClosed && !dataBudget.isClosed,
      "source protection requires the original open graph root and live DATA allocation")
    if dataBudget ne root then
      require((dataBudget.profile eq root.profile) && dataBudget.sharesAllocationLedger(root) &&
        dataBudget.isCanonicalChildOf(root),
        "plan-derived source protection requires a direct canonical DATA child of the original root")
    // Reserve the complete row/index working set before allocating collections. Binary copies use the child below.
    val rowCount = snapshot.relations.foldLeft(0L)((n,r) => Math.addExact(n,r.rows.size.toLong))
    val scratch = Math.addExact(16384L, Math.multiplyExact(rowCount, 4096L))
    dataBudget.reserveTemporaryMemory(scratch, "Boot protection complete graph/index working set")
    var child: MySqlFormat3ReadBudget = null
    var primary: Throwable = null
    var retained = 0L
    def keep(bytes: Long): Unit =
      val nextRetained = Math.addExact(retained, bytes)
      dataBudget.reserveRetainedMemory(bytes, "Boot immutable source protection DATA")
      retained = nextRetained
    try
      // 临时复制工作区词法归属到确切的计划账户；source graph 权威仍由 root 校验，
      // 不复用 snapshot 图像的 retained 账户作为此计划账户。
      child = snapshot.workspace(dataBudget)
      val work = child
      val protectedChunks = mutable.HashMap.empty[Chunk, Vector[String]]
      val unknownChunks = mutable.HashMap.empty[Chunk, Vector[String]]
      val unresolved = mutable.ArrayBuffer.empty[Unresolved]
      val carriers = mutable.ArrayBuffer.empty[Carrier]
      val carrierKeys = mutable.ArrayBuffer.empty[(Vector[Byte], Carrier)]
      def rows(table: String): Vector[Row] = snapshot.relation(table).rows
      def c(row: Row, table: String, name: String): MySqlFormat3Cell = MySqlModeOperationEffectCodec.value(row,table,name)
      def text(row: Row, table: String, name: String): String = c(row,table,name) match
        case MySqlFormat3Cell.Text(v) => v
        case _ => throw new SQLDataException(s"Boot protection requires $table.$name text")
      def number(row: Row, table: String, name: String): Long = c(row,table,name) match
        case MySqlFormat3Cell.SignedInteger(v) => v
        case _ => throw new SQLDataException(s"Boot protection requires $table.$name integer")
      def int(row: Row, table: String, name: String): Int = Math.toIntExact(number(row,table,name))
      def binary(row: Row, table: String, name: String): MySqlFormat3Binary = c(row,table,name) match
        case MySqlFormat3Cell.Binary(v) => v
        case _ => throw new SQLDataException(s"Boot protection requires $table.$name binary")
      def uuid(row: Row, table: String, name: String): UUID =
        val b = binary(row,table,name)
        if b.length != 16 then throw new SQLDataException("Boot protection UUID width")
        var hi = 0L; var lo = 0L; var n = 0
        while n < 8 do
          hi = (hi << 8) | (b.byteAt(n).toLong & 255L)
          lo = (lo << 8) | (b.byteAt(n+8).toLong & 255L)
          n += 1
        new UUID(hi,lo)
      def bytes(row: Row, table: String, name: String): Vector[Byte] =
        val b = binary(row,table,name)
        work.reserveTemporaryMemory(Math.addExact(128L,Math.multiplyExact(b.length.toLong,32L)), "protection full binary key vector")
        b.copyBytes(work,"protection original binary copy").toVector
      def carrierKey(save: String, loader: String, region: String, x: Int, z: Int): MySqlRecoveryResourceKey =
        MySqlRecoveryCanonicalBytes.withAllocationBudget(work) {
          MySqlRecoveryResourceKey.carrier(save,loader,region,x,z,root.profile)
        }
      def unknown(code: String, table: String, cause: Option[Throwable] = None): Unit =
        keep(512L); unresolved += Unresolved(code,table,cause)
      def add(target: mutable.HashMap[Chunk,Vector[String]], chunk: Chunk, reason: String): Unit =
        keep(384L); target.update(chunk,target.getOrElse(chunk,Vector.empty) :+ reason)
      def protect(chunk: Chunk, reason: String): Unit = add(protectedChunks,chunk,reason)
      def localUnknown(chunk: Chunk, reason: String): Unit = add(unknownChunks,chunk,reason)
      // Use the actual full-table FK/envelope validator, not fixture-generated relationships.
      snapshot.requireGraph(root)

      val operations = rows("fp_recovery_operation")
      def operation(id: MySqlFormat3Cell): Option[Row] =
        val found = operations.filter(r => c(r,"fp_recovery_operation","operation_uuid") == id)
        if found.size == 1 then Some(found.head) else None
      def committed(id: MySqlFormat3Cell): Boolean = operation(id).exists(r => text(r,"fp_recovery_operation","operation_state") == "COMMITTED")
      def proof(row: Row, table: String, field: String, operationField: String): Boolean =
        val shaField = field.stripSuffix("_bytes")+"_sha256"
        if c(row,table,field) == MySqlFormat3Cell.SqlNull || c(row,table,shaField) == MySqlFormat3Cell.SqlNull then false
        else
          val payload = binary(row,table,field).copyBytes(work,"release proof DATA copy")
          work.reserveTemporaryMemory(4096L,"release proof digest work")
          val digest = java.security.MessageDigest.getInstance("SHA-256").digest(payload).toVector
          digest == bytes(row,table,shaField) && committed(c(row,table,operationField))

      val units = rows("physical_source_unit")
      val forests = rows("physical_source_forest")
      units.foreach { row =>
        val t = "physical_source_unit"
        val observation = binary(row,t,"source_observation_bytes").copyBytes(work,"unit original source observation")
        if java.security.MessageDigest.getInstance("SHA-256").digest(observation).toVector != bytes(row,t,"source_observation_sha256") then
          throw new SQLDataException("SQL source observation digest differs")
      }
      val adapter = new SourceForestAllocationBudget:
        override def exactProfile() = root.profile
        override def reserveTemporary(n: Long, context: String): Unit =
          try
            if root.isClosed || work.isClosed || !work.isCanonicalChildOf(root) then throw new SQLDataException("source protection budget retired")
            work.reserveTemporaryMemory(n,context)
          catch case cause: java.sql.SQLException => throw new IOException("source protection allocation failed",cause)
        override def releaseTemporary(n: Long, context: String): Unit =
          try work.releaseTemporaryMemory(n,context)
          catch case cause: java.sql.SQLException => throw new IOException("source protection allocation cleanup failed",cause)
      forests.foreach { row =>
        val t = "physical_source_forest"
        val binding = uuid(row,t,"binding_uuid"); val unit = uuid(row,t,"unit_uuid"); val forest = uuid(row,t,"forest_uuid")
        if !Set("DRAFT","SEALED").contains(text(row,t,"forest_state")) then throw new SQLDataException("SQL source forest state unsupported")
        val matching = units.filter(r => uuid(r,"physical_source_unit","binding_uuid") == binding && uuid(r,"physical_source_unit","unit_uuid") == unit)
        if matching.size != 1 then unknown("ORPHAN_SOURCE_FOREST",t)
        if number(row,t,"carrier_count") == 0L then unknown("SOURCE_FOREST_WITHOUT_LOCATABLE_CARRIERS",t)
        val expected = new SourceForestMembershipCodec.ForestExpectation(binding,unit,forest,number(row,t,"source_epoch"),
          uuid(row,t,"created_operation_uuid"),text(row,t,"forest_codec"),number(row,t,"member_count"),number(row,t,"root_count"),number(row,t,"carrier_count"),number(row,t,"retained_bytes"))
        SourceForestCarrierSummary.consumeVerified(binary(row,t,"forest_bytes").copyBytes(work,"complete forest copy"),
          binary(row,t,"forest_sha256").copyBytes(work,"forest SHA copy"),expected,root.profile,adapter, summary =>
            val id = summary.identity()
            val strings = Vector(id.worldIdentity(),id.saveIdentity(),id.loaderProfile(),summary.regionIdentity())
            keep(Math.addExact(1024L,strings.foldLeft(0L)((n,s) => Math.addExact(n,Math.multiplyExact(s.length.toLong,4L)))))
            val carrier = Carrier(binding,unit,forest,id.worldIdentity(),id.saveIdentity(),id.loaderProfile(),summary.regionIdentity(),
              Chunk(summary.dimension(),summary.chunkX(),summary.chunkZ()),summary.saveGeneration())
            carriers += carrier
            if matching.size == 1 then
              val u = matching.head; val ut = "physical_source_unit"
              if text(u,ut,"world_identity") != id.worldIdentity() || text(u,ut,"save_identity") != id.saveIdentity() ||
                  text(u,ut,"loader_identity") != id.loaderProfile() || uuid(u,ut,"catalog_generation") != id.catalogGeneration() ||
                  number(u,ut,"read_generation") != id.readGeneration() ||
                  bytes(u,ut,"source_observation_sha256") != summary.sourceObservationSha().copy().toVector then throw new SQLDataException("SQL unit/forest identity differs")
            val key = carrierKey(carrier.saveIdentity,carrier.loaderIdentity,carrier.regionIdentity,carrier.chunk.x,carrier.chunk.z)
            val encoded = key.copyCanonicalBytes(work,"complete derived carrier key")
            work.reserveTemporaryMemory(Math.addExact(256L,Math.multiplyExact(encoded.length.toLong,32L)),"build-only complete carrier key index")
            carrierKeys += ((encoded.toVector,carrier))
            // Existing source history is not an interpreted release proof. Keep its exact chunk local;
            // other chunks can still be Unprotected, and OPEN obligations below take Protected precedence.
            localUnknown(carrier.chunk,"SOURCE_CARRIER_RELEASE_NOT_ESTABLISHED")
            if Set(SourceForestCodec.CarrierDisposition.PENDING,SourceForestCodec.CarrierDisposition.UNKNOWN).contains(summary.disposition()) || summary.unresolvedMembers() then
              localUnknown(carrier.chunk,"UNRESOLVED_SOURCE_FOREST")
        )
      }
      def locate(key: Vector[Byte], unit: Option[UUID] = None, forest: Option[UUID] = None): Vector[Carrier] =
        carrierKeys.iterator.collect { case (bytes,carrier) if bytes == key && unit.forall(_ == carrier.unit) && forest.forall(_ == carrier.forest) => carrier }.toVector
      def affect(found: Vector[Carrier], reason: String, uncertain: Boolean = false): Unit =
        if found.isEmpty then unknown("UNLOCATED_"+reason,"source")
        else found.foreach(carrier => if uncertain then localUnknown(carrier.chunk,reason) else protect(carrier.chunk,reason))
      // Validate every source write key using explicit SQL components before joining complete forest carriers.
      rows("physical_source_write").foreach { row =>
        val t = "physical_source_write"
        val derived = carrierKey(text(row,t,"save_identity"),text(row,t,"loader_identity"),text(row,t,"region_identity"),int(row,t,"slot_x"),int(row,t,"slot_z"))
        val key = bytes(row,t,"carrier_key")
        if derived.copyCanonicalBytes(work,"write canonical key").toVector != key || derived.keyHashSha256 != bytes(row,t,"carrier_key_hash") then
          unknown("SOURCE_WRITE_KEY_MISMATCH",t)
        val state = text(row,t,"write_state")
        val found = locate(key)
        if Set("PREPARED","FAILED_NEEDS_RECONCILIATION").contains(state) then affect(found,"SOURCE_WRITE_"+state)
        else if Set("DURABLE","NO_PUT","SUPERSEDED").contains(state) then
          // No typed result/undo receipt parser exists here. Do not infer released protection from state/hash alone.
          affect(found,"UNINTERPRETED_WRITE_RESULT",true)
        else unknown("UNKNOWN_SOURCE_WRITE_STATE",t)
      }
      rows("physical_source_obligation").foreach { row =>
        val t = "physical_source_obligation"
        val key = bytes(row,t,"carrier_key")
        val found = locate(key,Some(uuid(row,t,"unit_uuid")),Some(uuid(row,t,"forest_uuid")))
        val actualHash = java.security.MessageDigest.getInstance("SHA-256").digest(key.toArray).toVector
        if actualHash != bytes(row,t,"carrier_key_hash") then unknown("SOURCE_OBLIGATION_KEY_HASH",t)
        text(row,t,"obligation_state") match
          case "OPEN" => affect(found,"SOURCE_OBLIGATION_"+text(row,t,"purpose"))
          case "DISCHARGED" =>
            val validEnvelope = proof(row,t,"discharge_proof_bytes","discharged_operation_uuid")
            affect(found,if validEnvelope then "UNINTERPRETED_RELEASE_PROOF" else "INVALID_RELEASE_PROOF",true)
          case _ => unknown("UNKNOWN_SOURCE_OBLIGATION_STATE",t)
      }
      units.foreach { row =>
        val t = "physical_source_unit"
        val found = carriers.filter(_.unit == uuid(row,t,"unit_uuid")).toVector
        text(row,t,"unit_state") match
          case "DRAFT" => affect(found,"DRAFT_SOURCE_UNIT",true)
          case "SEALED" =>
            if c(row,t,"current_forest_uuid") == MySqlFormat3Cell.SqlNull || c(row,t,"current_epoch") == MySqlFormat3Cell.SqlNull || found.isEmpty then
              unknown("UNLOCATED_SEALED_SOURCE_UNIT",t)
          case _ => unknown("UNKNOWN_SOURCE_UNIT_STATE",t)
      }
      rows("physical_source_transfer").foreach { row =>
        val t = "physical_source_transfer"
        val found = carriers.filter(_.unit == uuid(row,t,"unit_uuid")).toVector
        affect(found,if text(row,t,"phase") == "COMPLETED" then "UNINTERPRETED_TRANSFER_COMPLETION" else "SOURCE_TRANSFER_PENDING",true)
      }
      val footprints = rows("physical_recovery_footprint")
      def footprintChunks(row: Row, table: String): Vector[Chunk] =
        val names = Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")
        val matches = footprints.filter(f => names.forall(n => c(f,"physical_recovery_footprint",n) == c(row,table,n)))
        if matches.size != 1 then Vector.empty
        else rows("physical_recovery_footprint_chunk").filter(r => names.forall(n => c(r,"physical_recovery_footprint_chunk",n) == c(row,table,n))).map { r =>
          Chunk(int(r,"physical_recovery_footprint_chunk","dimension_id"),int(r,"physical_recovery_footprint_chunk","chunk_x"),int(r,"physical_recovery_footprint_chunk","chunk_z"))
        }
      rows("physical_recovery_obligation").foreach { row =>
        val t = "physical_recovery_obligation"; val found = footprintChunks(row,t)
        if found.isEmpty then unknown("UNLOCATED_RECOVERY_OBLIGATION",t)
        else text(row,t,"obligation_state") match
          case "PENDING" => found.foreach(protect(_,"PENDING_RECOVERY_OBLIGATION"))
          case "VERIFIED" =>
            val validEnvelope = proof(row,t,"verification_proof_bytes","verified_operation_uuid")
            found.foreach(localUnknown(_,if validEnvelope then "UNINTERPRETED_RELEASE_PROOF" else "INVALID_RELEASE_PROOF"))
          case _ => unknown("UNKNOWN_RECOVERY_OBLIGATION_STATE",t)
      }
      rows("fp_recovery_reservation").foreach { row =>
        val t = "fp_recovery_reservation"
        val fullKey = bytes(row,t,"resource_key")
        if java.security.MessageDigest.getInstance("SHA-256").digest(fullKey.toArray).toVector != bytes(row,t,"resource_key_hash") then
          unknown("RESERVATION_FULL_KEY_HASH_MISMATCH",t)
        if text(row,t,"reservation_state") == "HELD" then
          text(row,t,"resource_kind") match
            case "CARRIER" => affect(locate(bytes(row,t,"resource_key")),"HELD_CARRIER_RESERVATION")
            case "SOURCE_UNIT" => affect(carriers.filter(_.unit == uuid(row,t,"resource_unit_uuid")).toVector,"HELD_SOURCE_UNIT_RESERVATION")
            case "POD" =>
              val found = rows("physical_recovery_footprint_chunk").filter(r => c(r,"physical_recovery_footprint_chunk","pod_uuid") == c(row,t,"resource_pod_uuid"))
              if found.isEmpty then unknown("UNLOCATED_HELD_POD_RESERVATION",t)
              else found.foreach(r => protect(Chunk(int(r,"physical_recovery_footprint_chunk","dimension_id"),int(r,"physical_recovery_footprint_chunk","chunk_x"),int(r,"physical_recovery_footprint_chunk","chunk_z")),"HELD_POD_RESERVATION"))
            case _ => unknown("UNKNOWN_RESERVATION_KIND",t)
        else if text(row,t,"reservation_state") == "RELEASED" then
          text(row,t,"resource_kind") match
            case "CARRIER" => affect(locate(fullKey),"UNINTERPRETED_RESERVATION_RELEASE",true)
            case "SOURCE_UNIT" => affect(carriers.filter(_.unit == uuid(row,t,"resource_unit_uuid")).toVector,"UNINTERPRETED_RESERVATION_RELEASE",true)
            case "POD" =>
              val matching = rows("physical_recovery_footprint_chunk").filter(r => c(r,"physical_recovery_footprint_chunk","pod_uuid") == c(row,t,"resource_pod_uuid"))
              if matching.isEmpty then unknown("UNLOCATED_RESERVATION_RELEASE",t)
              else matching.foreach(r => localUnknown(Chunk(int(r,"physical_recovery_footprint_chunk","dimension_id"),int(r,"physical_recovery_footprint_chunk","chunk_x"),int(r,"physical_recovery_footprint_chunk","chunk_z")),"UNINTERPRETED_RESERVATION_RELEASE"))
            case _ => unknown("UNKNOWN_RESERVATION_KIND",t)
        else unknown("UNKNOWN_RESERVATION_STATE",t)
      }
      operations.foreach { row =>
        val t = "fp_recovery_operation"
        if text(row,t,"operation_state") == "RESERVED" then
          // The raw operation scope/preimage is not an interpreted affected-carrier closure.
          // Even a located reservation cannot prove that it covers all effects of this operation.
          unknown("RESERVED_OPERATION_SCOPE_UNINTERPRETED",t)
      }
      // Prepared intent/transfer history remains protection even without a current locator.
      rows("physical_recovery_intent").foreach { row =>
        val t = "physical_recovery_intent"
        if text(row,t,"phase") == "PREPARED" then
          val names = Vector("binding_uuid","pod_uuid","attempt_uuid")
          val found = rows("physical_recovery_footprint_chunk").filter(r => names.forall(n => c(r,"physical_recovery_footprint_chunk",n) == c(row,t,n)) && c(r,"physical_recovery_footprint_chunk","footprint_uuid") == c(row,t,"target_footprint_uuid"))
          if found.isEmpty then unknown("UNLOCATED_PREPARED_INTENT",t)
          else found.foreach(r => protect(Chunk(int(r,"physical_recovery_footprint_chunk","dimension_id"),int(r,"physical_recovery_footprint_chunk","chunk_x"),int(r,"physical_recovery_footprint_chunk","chunk_z")),"PREPARED_INTENT"))
      }
      if rows("physical_footprint_transfer").nonEmpty then unknown("FOOTPRINT_TRANSFER_PROOF_UNINTERPRETED","physical_footprint_transfer")
      keep(Math.addExact(1024L,Math.multiplyExact(rowCount,64L)))
      new MySqlBootSourceProtectionIndex(root.profile,carriers.toVector,protectedChunks.toMap,unknownChunks.toMap,unresolved.toVector)
    catch
      case cause: Throwable =>
        primary = cause
        if retained != 0L then
          try dataBudget.releaseRetainedMemory(retained,"failed Boot source protection cleanup")
          catch case cleanup: Throwable => if cleanup ne cause then cause.addSuppressed(cleanup)
        throw cause
    finally
      val currentChild = child
      try MySqlFormat3BudgetCleanup.run(primary,Vector(
          () => if currentChild != null then currentChild.closeScopedReadBudget(),
          () => dataBudget.releaseTemporaryMemory(scratch,"Boot protection complete graph/index working set")))
      catch
        case cleanup: Throwable =>
          if primary == null && retained != 0L then
            try dataBudget.releaseRetainedMemory(retained,"unpublished protection cleanup failure")
            catch case secondary: Throwable => if secondary ne cleanup then cleanup.addSuppressed(secondary)
          throw cleanup
