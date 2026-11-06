package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.IoOperationTag
import java.time.LocalDateTime
import java.util.{Arrays, UUID}

/** 对外只读的 SQL 值视图；连接、JDBC、budget 与资格 token 均不离开 owned session。 */
enum MySqlOwnedFormat3Value:
  case SqlNull
  case Text(value: String)
  case Binary(value: MySqlOwnedFormat3Binary)
  case SignedInteger(value: Long)
  case Floating(rawBits: Long)
  case DateTime(value: LocalDateTime)

/** 二进制字段采用防御性副本，调用者不能修改快照中保留的 SQL 值。 */
final class MySqlOwnedFormat3Binary private (private val content: Array[Byte]):
  def length: Int = content.length
  def bytes: Array[Byte] = content.clone()
  /** 固定UUID查找不分配防御副本；仅同一owned数据边界使用。 */
  private[mysql] def equalsUuid(value: UUID): Boolean =
    if value == null || content.length != 16 then false
    else
      var index = 0
      while index < 16 do
        val bits = if index < 8 then value.getMostSignificantBits else value.getLeastSignificantBits
        val shift = (7 - index % 8) * 8
        if content(index) != (bits >>> shift).toByte then return false
        index += 1
      true
  override def equals(other: Any): Boolean = other match
    case value: MySqlOwnedFormat3Binary => Arrays.equals(content, value.content)
    case _ => false
  override def hashCode(): Int = Arrays.hashCode(content)
  override def toString: String = "MySqlOwnedFormat3Binary[" + length + " bytes]"

private[mysql] object MySqlOwnedFormat3Binary:
  def takeOwned(bytes: Array[Byte]): MySqlOwnedFormat3Binary =
    require(bytes != null, "owned binary bytes are required")
    new MySqlOwnedFormat3Binary(bytes)

final case class MySqlOwnedFormat3Row(cells: Vector[MySqlOwnedFormat3Value]):
  require(cells != null && cells.forall(_ != null), "owned format3 row cells are required")

final case class MySqlOwnedFormat3Relation(table: String, rows: Vector[MySqlOwnedFormat3Row]):
  require(table != null && table.nonEmpty && rows != null && rows.forall(_ != null),
    "owned format3 relation is incomplete")

/**
 * 完整二十关系只读视图。构造只在 MySQL 同一 owned worker 的真实 full20 reader 后发生；
 * 此数据投影本身不授予 Ready、world custody、覆盖范围或数据库写权。
 */
final class MySqlOwnedFormat3Snapshot private[mysql] (
    val schema: String,
    val serverUuid: UUID,
    val serverVersion: String,
    val bootUuid: UUID,
    val bootGeneration: Long,
    val connectionGeneration: Long,
    val maxAllowedPacketBytes: Long,
    val limitProfile: com.wi.floatingpod.config.SourceLimitProfile,
    initialRelations: Vector[MySqlOwnedFormat3Relation],
    val canonicalSha256Hex: String,
    val canonicalByteLength: Long,
    private var bootData: Option[MySqlBootMainReadData] = None,
    private var originalBootRoot: Option[MySqlFormat3ReadBudget] = None,
    private var originalBootSource: Option[MySqlFormat3FullSnapshot] = None,
    private val originalBootSubmission: Option[(IoOperationTag, Long)] = None,
    private val completeFamily: MySqlOwnedFormat3Snapshot.CompleteFamily = MySqlOwnedFormat3Snapshot.CompleteFamily.LegacyFull20,
    private var originalDeploymentBootSource: Option[MySqlScriptSchemaSnapshot] = None
):
  private var retainedRelations = initialRelations
  private var bootInputDropped = false
  def relations: Vector[MySqlOwnedFormat3Relation] = synchronized {
    require(!bootInputDropped, "original Boot input has been dropped")
    retainedRelations
  }
  require(schema != null && serverUuid != null && serverVersion != null && bootUuid != null &&
    bootGeneration > 0L && connectionGeneration > 0L && maxAllowedPacketBytes > 0L &&
    limitProfile != null && relations != null &&
    canonicalSha256Hex != null && MySqlSchemaAuditor.Sha256Hex.matches(canonicalSha256Hex) &&
    canonicalByteLength > 0L, "owned format3 full snapshot is incomplete")
  requireCompleteFamily()
  /** 仅核原 worker 已封闭的完整 DATA family；不发行数据库/物理/业务资格。 */
  private[floatingpod] def requireCompleteFamily(): Unit = synchronized {
    require(!bootInputDropped,"original complete input has been dropped")
    val expected = completeFamily match
      case MySqlOwnedFormat3Snapshot.CompleteFamily.LegacyFull20 => MySqlFormat3RelationShapes.canonicalOrder
      case MySqlOwnedFormat3Snapshot.CompleteFamily.DeploymentReceiptFull26 => MySqlDeploymentReceiptRelationShapes.canonicalOrder
      case MySqlOwnedFormat3Snapshot.CompleteFamily.DbFirstFull27 => MySqlDbFirstSnapshotSql.canonicalOrder
    require(retainedRelations.map(_.table) == expected.map(_.table),"exact closed complete projection family required")
    completeFamily match
      case MySqlOwnedFormat3Snapshot.CompleteFamily.DeploymentReceiptFull26 | MySqlOwnedFormat3Snapshot.CompleteFamily.DbFirstFull27 =>
        retainedRelations.zip(expected).foreach { (relation,shape) =>
          require(relation.rows.forall(_.cells.size == shape.columns.size),"complete Script family row widths required")
        }
      case MySqlOwnedFormat3Snapshot.CompleteFamily.LegacyFull20 => ()
  }
  private[floatingpod] def requireBootReadData: MySqlBootMainReadData = synchronized {
    require(!bootInputDropped, "original Boot input has been dropped")
    require(originalBootRoot.nonEmpty && (originalBootSource.nonEmpty || originalDeploymentBootSource.nonEmpty),
      "actual worker-issued Boot retained root/source required")
    bootData.getOrElse(throw new IllegalStateException("snapshot has no worker-decoded Boot data"))
  }
  private[floatingpod] def requireOriginalBootEpoch(actualBootTag: IoOperationTag): Long =
    requireBootReadData
    originalBootSubmission match
      case Some((tag, epoch)) if actualBootTag != null && (tag eq actualBootTag) &&
          tag.bootId == bootUuid && tag.bootGeneration == bootGeneration && tag.connectionGeneration == connectionGeneration => epoch
      case _ => throw new IllegalArgumentException("original worker-captured Boot tag/epoch required")
  /**
   * 仅 session 在原 Main drop 已确认后消费；返回原 root，不在仍持有 source 的调用栈中关闭它。
   * 本方法不发行 Main closure、SQL/native 释放资格或新 Boot 数据。
   */
  private[mysql] def detachOriginalBootInputForExit(source: MySqlFormat3FullSnapshot,
      actualBootTag: IoOperationTag): MySqlFormat3ReadBudget = synchronized {
    requireOriginalBootEpoch(actualBootTag)
    require(source != null && originalBootSource.exists(_ eq source), "exact original Boot source required for drop")
    val root = originalBootRoot.get
    require(!root.isClosed, "original Boot root closed before its input drop")
    bootInputDropped = true
    retainedRelations = Vector.empty
    bootData = None
    originalBootSource = None
    originalBootRoot = None
    root
  }
  private[mysql] def detachOriginalBootInputForExit(source: MySqlScriptSchemaSnapshot,
      actualBootTag: IoOperationTag): MySqlFormat3ReadBudget = synchronized {
    requireOriginalBootEpoch(actualBootTag)
    require(completeFamily == MySqlOwnedFormat3Snapshot.CompleteFamily.DeploymentReceiptFull26 &&
      source != null && originalDeploymentBootSource.exists(_ eq source),"exact original Full26 Boot source required for drop")
    val root = originalBootRoot.get
    require(!root.isClosed,"original Full26 Boot root closed before input drop")
    bootInputDropped = true
    retainedRelations = Vector.empty
    bootData = None
    originalDeploymentBootSource = None
    originalBootRoot = None
    root
  }
  def rowCount: Long = relations.foldLeft(0L)((count, relation) => Math.addExact(count, relation.rows.size.toLong))
  def relation(table: String): MySqlOwnedFormat3Relation =
    if table == null then throw new IllegalArgumentException("relation table is required")
    relations.find(_.table == table).getOrElse(
      throw new IllegalArgumentException("table is outside the complete format3 snapshot"))
  override def toString: String = synchronized {
    if bootInputDropped then "MySqlOwnedFormat3Snapshot[dropped Boot input, sha256=" + canonicalSha256Hex + "]"
    else "MySqlOwnedFormat3Snapshot[relations=" + relations.size + ", rows=" + rowCount +
      ", sha256=" + canonicalSha256Hex + "]"
  }

private[mysql] object MySqlOwnedFormat3Snapshot:
  private[mysql] enum CompleteFamily:
    case LegacyFull20,DeploymentReceiptFull26,DbFirstFull27

  def from(snapshot: MySqlFormat3FullSnapshot): MySqlOwnedFormat3Snapshot =
    if snapshot == null then throw new IllegalArgumentException("actual format3 full snapshot is required")
    from(snapshot, snapshot.allocationBudget)

  def from(snapshot: MySqlFormat3FullSnapshot, budget: MySqlFormat3ReadBudget): MySqlOwnedFormat3Snapshot =
    if snapshot == null || budget == null || snapshot.allocationBudget == null || budget.isClosed ||
      (budget.profile ne snapshot.limitProfile) || !snapshot.allocationBudget.sharesAllocationLedger(budget) then
      throw new IllegalArgumentException("actual full20 projection requires the exact open shared allocation ledger")
    val fingerprint = snapshot.canonicalSha256Hex
    val projected = projectRelations(snapshot.relations,budget)
    new MySqlOwnedFormat3Snapshot(snapshot.schema, snapshot.connection.serverUuid, snapshot.connection.serverVersion,
      snapshot.observationScope.bootUuid, snapshot.observationScope.bootGeneration,
      snapshot.observationScope.connectionGeneration, snapshot.maxAllowedPacketBytes, snapshot.limitProfile,
      projected, fingerprint, snapshot.canonicalByteLength)

  def fromDeploymentReceipt(snapshot: MySqlScriptSchemaSnapshot,inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget): MySqlOwnedFormat3Snapshot =
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,inputs,root)
    val fingerprint = snapshot.canonicalSha256Hex
    val projected = projectRelations(snapshot.relations,root)
    new MySqlOwnedFormat3Snapshot(snapshot.schema,snapshot.connection.serverUuid,snapshot.connection.serverVersion,
      snapshot.observationScope.bootUuid,snapshot.observationScope.bootGeneration,
      snapshot.observationScope.connectionGeneration,snapshot.maxAllowedPacketBytes,inputs.sourceLimitProfile,
      projected,fingerprint,snapshot.canonicalByteLength,completeFamily=CompleteFamily.DeploymentReceiptFull26)

  /** 完整Full27 DATA副本，仍不授予Main claim/恢复/Ready；由原Session责任持有。 */
  private[mysql] def fromDbFirst(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget):MySqlOwnedFormat3Snapshot =
    fromDbFirst(snapshot,inputs,root,root)
  private[mysql] def fromDbFirst(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget,dataBudget:MySqlFormat3ReadBudget):MySqlOwnedFormat3Snapshot =
    MySqlFormat3QueryProjection.requireDbFirstDataAllocation(root,dataBudget)
    require(snapshot!=null && inputs!=null && root!=null && !root.isClosed &&
      inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27 && (snapshot.inputs eq inputs) &&
      snapshot.allocationBudget.isSnapshotReadChildOf(root) && !snapshot.allocationBudget.isClosed &&
      (root.profile eq inputs.sourceLimitProfile),"original live Full27 image child and exact root required")
    root.requireScriptDataInputs(inputs)
    val fingerprint=snapshot.canonicalSha256Hex
    // 来源reader可先退休；与source digest同量的保守费用随Main投影保留。
    dataBudget.reserveRetainedMemory(640L,"Full27 Main fingerprint metadata and shared digest lifetime")
    val projected=projectRelations(snapshot.relations,dataBudget,retainSharedText=true)
    new MySqlOwnedFormat3Snapshot(snapshot.schema,snapshot.connection.serverUuid,snapshot.connection.serverVersion,
      snapshot.observationScope.bootUuid,snapshot.observationScope.bootGeneration,snapshot.observationScope.connectionGeneration,
      snapshot.maxAllowedPacketBytes,inputs.sourceLimitProfile,projected,fingerprint,snapshot.canonicalByteLength,
      completeFamily=CompleteFamily.DbFirstFull27)

  private def projectRelations(relations: Vector[MySqlFormat3RelationSnapshot],
      budget: MySqlFormat3ReadBudget, retainSharedText:Boolean=false): Vector[MySqlOwnedFormat3Relation] =
    val relationOverhead = Math.multiplyExact(relations.size.toLong, 128L)
    budget.reserveRetainedMemory(relationOverhead, "owned format3 public relation projection")
    relations.map { relation =>
      val rows = relation.rows.map { row =>
        val rowBytes = Math.addExact(128L, Math.multiplyExact(row.size.toLong, 48L))
        budget.reserveRetainedMemory(rowBytes, relation.table + " public row projection")
        val cells = row.map {
          case MySqlFormat3Cell.SqlNull => MySqlOwnedFormat3Value.SqlNull
          case MySqlFormat3Cell.Text(value) =>
            // Full27代次可先于来源reader退休；共享String的存活费用必须随新代保留。
            if retainSharedText then budget.reserveRetainedMemory(
              Math.addExact(64L,Math.multiplyExact(value.length.toLong,2L)),relation.table+" shared Main text lifetime")
            MySqlOwnedFormat3Value.Text(value)
          case MySqlFormat3Cell.Binary(value) =>
            MySqlOwnedFormat3Value.Binary(MySqlOwnedFormat3Binary.takeOwned(
              value.copyBytes(budget, relation.table + " public binary projection")))
          case MySqlFormat3Cell.SignedInteger(value) => MySqlOwnedFormat3Value.SignedInteger(value)
          case MySqlFormat3Cell.Floating(rawBits) => MySqlOwnedFormat3Value.Floating(rawBits)
          case MySqlFormat3Cell.DateTime(value) => MySqlOwnedFormat3Value.DateTime(value)
        }
        MySqlOwnedFormat3Row(cells)
      }
      MySqlOwnedFormat3Relation(relation.table, rows)
    }

  def fromAcceptedDeploymentReceiptBoot(snapshot: MySqlScriptSchemaSnapshot,inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget,expectation: MySqlSchemaAuditExpectation,admission: MySqlBusinessAdmission,
      tag: IoOperationTag,epoch: Long): MySqlOwnedFormat3Snapshot =
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,inputs,root)
    require(tag != null && epoch >= 0L && tag.bootId == snapshot.observationScope.bootUuid &&
      tag.bootGeneration == snapshot.observationScope.bootGeneration &&
      tag.connectionGeneration == snapshot.observationScope.connectionGeneration,"original Full26 Boot capture identity required")
    root.reserveRetainedMemory(128L,"original immutable Full26 Boot submission stamp")
    val data = MySqlBootMainReadData.fromDeploymentReceipt(snapshot,inputs,root,expectation,admission)
    val full = fromDeploymentReceipt(snapshot,inputs,root)
    root.reserveRetainedMemory(256L,"Full26 Boot retained source holder")
    new MySqlOwnedFormat3Snapshot(full.schema,full.serverUuid,full.serverVersion,full.bootUuid,
      full.bootGeneration,full.connectionGeneration,full.maxAllowedPacketBytes,full.limitProfile,
      full.relations,full.canonicalSha256Hex,full.canonicalByteLength,Some(data),Some(root),None,Some((tag,epoch)),
      CompleteFamily.DeploymentReceiptFull26,Some(snapshot))

  private[mysql] def fromRestrictedDeploymentReceiptBoot(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget,expectation:MySqlSchemaAuditExpectation,
      proof:MySqlDeploymentReceiptSession.RestrictedRecall5,tag:IoOperationTag,epoch:Long):MySqlOwnedFormat3Snapshot =
    proof.requireBootSource(snapshot,inputs,root,expectation)
    require(tag!=null && epoch>=0L && tag.bootId==snapshot.observationScope.bootUuid &&
      tag.bootGeneration==snapshot.observationScope.bootGeneration && tag.connectionGeneration==snapshot.observationScope.connectionGeneration,
      "original restricted Full26 Boot tag/epoch required")
    root.reserveRetainedMemory(384L,"original restricted Full26 Boot stamp/source holder BEFORE allocation")
    val data=MySqlBootMainReadData.fromRestrictedDeploymentReceipt(snapshot,inputs,root,expectation,proof)
    val full=fromDeploymentReceipt(snapshot,inputs,root)
    new MySqlOwnedFormat3Snapshot(full.schema,full.serverUuid,full.serverVersion,full.bootUuid,full.bootGeneration,
      full.connectionGeneration,full.maxAllowedPacketBytes,full.limitProfile,full.relations,full.canonicalSha256Hex,
      full.canonicalByteLength,Some(data),Some(root),None,Some((tag,epoch)),CompleteFamily.DeploymentReceiptFull26,Some(snapshot))

  /** 旧纯 DATA factory 不含 lease epoch，不能被 claimed consumer 采用。 */
  def fromForBoot(snapshot: MySqlFormat3FullSnapshot): MySqlOwnedFormat3Snapshot = buildBootData(snapshot, None)

  /** actual session 仅使用 private Work 的原 tag/epoch；不从 Main 当前值重签。 */
  def fromAcceptedBoot(snapshot: MySqlFormat3FullSnapshot, tag: IoOperationTag, epoch: Long): MySqlOwnedFormat3Snapshot =
    require(snapshot != null && tag != null && epoch >= 0L && tag.bootId == snapshot.observationScope.bootUuid &&
      tag.bootGeneration == snapshot.observationScope.bootGeneration &&
      tag.connectionGeneration == snapshot.observationScope.connectionGeneration, "original Boot capture identity required")
    val budget = snapshot.allocationBudget
    budget.reserveRetainedMemory(128L, "original immutable Boot submission stamp")
    buildBootData(snapshot, Some((tag, epoch)))

  private def buildBootData(snapshot: MySqlFormat3FullSnapshot,
      originalSubmission: Option[(IoOperationTag, Long)]): MySqlOwnedFormat3Snapshot =
    require(snapshot != null, "actual Boot full20 snapshot required")
    val budget = snapshot.allocationBudget
    val data = MySqlBootMainReadData.from(snapshot, budget)
    val full = from(snapshot, budget)
    budget.reserveRetainedMemory(256L, "Boot retained source holder")
    new MySqlOwnedFormat3Snapshot(full.schema, full.serverUuid, full.serverVersion, full.bootUuid,
      full.bootGeneration, full.connectionGeneration, full.maxAllowedPacketBytes, full.limitProfile,
      full.relations, full.canonicalSha256Hex, full.canonicalByteLength, Some(data), Some(budget), Some(snapshot), originalSubmission)
