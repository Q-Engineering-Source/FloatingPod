package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.source.{SourceForestAllocationBudget, SourceForestMembershipCodec}
import com.wi.floatingpod.config.SourceLimitProfile
import java.io.IOException
import java.sql.SQLDataException
import java.util.UUID
import MySqlModeOperationEffectCodec.limitProfile

/** 完整锁定观测中的纯DATA碰撞检查；不发行absence或Ready。 */
private[mysql] object MySqlFormat3SourceForestMembershipScan:
  /** 当前 Full27 全量只读入口；不复用 Full26 producer/source 资格，也不按目标或 transfer 筛选。 */
  private[mysql] def consumeDbFirst(snapshot: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, binding: UUID, holder: MySqlDbFirstForestRead): Unit =
    if snapshot == null || inputs == null || root == null || holder == null || root.isClosed || root.isScopedReadBudget ||
      inputs.dataLayout != MySqlScriptSchemaDataLayout.DbFirstFull27 || (snapshot.inputs ne inputs) ||
      (snapshot.limitProfile ne root.profile) || snapshot.allocationBudget.isClosed ||
      !snapshot.allocationBudget.sharesAllocationLedger(root) ||
      ((snapshot.allocationBudget ne root) && !snapshot.allocationBudget.isSnapshotReadChildOf(root)) then
      throw new SQLDataException("Full27 forest DATA requires exact current complete image/root/inputs")
    if snapshot.relations.size != inputs.canonicalOrder.size then
      throw new SQLDataException("Full27 forest DATA requires all current relations")
    var relationIndex=0
    while relationIndex < snapshot.relations.size do
      if snapshot.relations(relationIndex).table != inputs.canonicalOrder(relationIndex).table then
        throw new SQLDataException("Full27 forest DATA relation order differs from current layout")
      relationIndex+=1
    root.requireScriptDataInputs(inputs)
    holder.requireOwner(root,inputs,binding)
    def uniqueBinding(table: String,column: String): Unit =
      val relation=snapshot.relation(table)
      val index=inputs.forTable(table).columns.indexWhere(_.name == column)
      if relation.rows.size != 1 || index < 0 then throw new SQLDataException("Full27 forest authority binding row absent or ambiguous: " + table)
      relation.rows.head(index) match
        case MySqlFormat3Cell.Binary(value) if value != null && value.length == 16 && value.equalsUuid(binding) => ()
        case _ => throw new SQLDataException("Full27 forest authority binding differs: " + table)
    uniqueBinding("fp_schema_manifest","expected_binding_uuid")
    uniqueBinding("physical_recovery_binding","binding_uuid")
    val relation=snapshot.relation("physical_source_forest")
    var index=0
    while index < relation.rows.size do
      if root.isClosed || snapshot.allocationBudget.isClosed || (snapshot.inputs ne inputs) ||
        (snapshot.limitProfile ne root.profile) || !snapshot.allocationBudget.sharesAllocationLedger(root) ||
        ((snapshot.allocationBudget ne root) && !snapshot.allocationBudget.isSnapshotReadChildOf(root)) then
        throw new SQLDataException("Full27 forest row lost its exact current image/root/inputs")
      root.requireScriptDataInputs(inputs)
      holder.requireOwner(root,inputs,binding)
      holder.consumeRow(relation.rows(index))
      index+=1

  /** 冷读原前像的纯DATA扫描，不把comparison image提升为当前query source。 */
  private[mysql] def containsDbFirstComparison(snapshot:MySqlScriptSchemaSnapshot,
      root:MySqlFormat3ReadBudget,binding:UUID,proposedPod:UUID):Boolean =
    val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    require(snapshot != null && binding != null && proposedPod != null && !root.isClosed && !root.isScopedReadBudget &&
      (snapshot.allocationBudget eq child) && !child.isClosed && child.isCanonicalComparisonChildOf(root) &&
      child.sharesAllocationLedger(root) && snapshot.inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27,
      "original decoded Full27 comparison DATA required")
    root.requireScriptDataInputs(snapshot.inputs)
    snapshot.relation("physical_source_forest").rows.foreach { row=>
      if !binary(row,0).equalsUuid(binding) then throw new SQLDataException("original forest binding differs")
    }
    scanForestRows(snapshot.relation("physical_source_forest"),root,proposedPod,null,null,snapshot.inputs)

  def contains(snapshot: MySqlFormat3FullSnapshot, originalRoot: MySqlFormat3ReadBudget, proposedPod: UUID): Boolean =
    scan(snapshot,originalRoot,proposedPod,null,null)

  def containsDeploymentReceipt(snapshot: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget, proposedPod: UUID): Boolean =
    scanDeploymentReceipt(snapshot,inputs,originalRoot,proposedPod,null,null)

  private[mysql] def containsInDeploymentReceiptForest(snapshot: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs, originalRoot: MySqlFormat3ReadBudget,
      proposedPod: UUID, exactUnit: UUID, exactForest: UUID): Boolean =
    if exactUnit == null || exactForest == null then throw new SQLDataException("exact source unit/forest identity required")
    scanDeploymentReceipt(snapshot,inputs,originalRoot,proposedPod,exactUnit,exactForest)

  private def scanDeploymentReceipt(snapshot: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, proposedPod: UUID, exactUnit: UUID, exactForest: UUID): Boolean =
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,inputs,root)
    if proposedPod == null then throw new SQLDataException("original proposed POD identity required")
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      MySqlFormat3QueryProjection.validateDeploymentReceiptForComparison(snapshot,inputs,root)
    }
    scanForestRows(snapshot.relation("physical_source_forest"),root,proposedPod,exactUnit,exactForest,inputs)

  /** 旧 prepare 图只凭实际 runner continuation 在当前 commit root 下重验并计费。 */
  private[mysql] def containsControlled(snapshot: MySqlFormat3FullSnapshot, currentRoot: MySqlFormat3ReadBudget,
      proposedPod: UUID, sourceRoot: MySqlFormat3ReadBudget,
      continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof]): Boolean =
    scan(snapshot,currentRoot,proposedPod,null,null,sourceRoot,continuation)

  /** 精确 source obligation 关联；仍验证全部森林，布尔不是 absence 或业务权限。 */
  private[mysql] def containsInForest(snapshot: MySqlFormat3FullSnapshot, originalRoot: MySqlFormat3ReadBudget,
      proposedPod: UUID, exactUnit: UUID, exactForest: UUID): Boolean =
    if exactUnit==null || exactForest==null then throw new SQLDataException("exact source unit/forest identity required")
    scan(snapshot,originalRoot,proposedPod,exactUnit,exactForest)

  private def scan(snapshot: MySqlFormat3FullSnapshot, originalRoot: MySqlFormat3ReadBudget,
      proposedPod: UUID, exactUnit: UUID, exactForest: UUID,
      sourceRoot: MySqlFormat3ReadBudget = null,
      continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof] = None): Boolean =
    val snapshotRoot = if sourceRoot == null then originalRoot else sourceRoot
    if sourceRoot != null then
      MySqlFencedTransaction.requireControlledDebugSource(snapshot,sourceRoot,originalRoot,continuation)
    if snapshot == null || originalRoot == null || proposedPod == null || originalRoot.isClosed || originalRoot.isScopedReadBudget ||
        (snapshot.limitProfile ne originalRoot.profile) || snapshot.allocationBudget.isClosed ||
        !snapshot.allocationBudget.sharesAllocationLedger(originalRoot) ||
        ((snapshot.allocationBudget ne snapshotRoot) && !snapshot.allocationBudget.isSnapshotReadChildOf(snapshotRoot)) then
      throw new SQLDataException("source forest scan requires the exact open snapshot/profile/allocation root")
    // 容器、closure和验证scratch先在原ledger子scope计费；不把该reserve当新容量。
    originalRoot.reserveTemporaryMemory(256L, "source forest validation scope and cleanup closure")
    var validation: MySqlFormat3ReadBudget = null
    var primary: Throwable = null
    try
      validation = originalRoot.newCanonicalScope()
      validation.reserveTemporaryMemory(validationWorkspace(snapshot), "full20 row/column/primary-key validation scratch")
      MySqlFormat3SnapshotValidation.validateRelations(snapshot.relations)
    catch
      case failure: Throwable => primary = failure; throw failure
    finally
      val owned = validation
      validation = null
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => if owned != null then owned.closeScopedReadBudget(),
        () => originalRoot.releaseTemporaryMemory(256L, "source forest validation scope and cleanup closure")))
    scanForestRows(snapshot.relation("physical_source_forest"),originalRoot,proposedPod,exactUnit,exactForest)

  private def scanForestRows(relation: MySqlFormat3RelationSnapshot, originalRoot: MySqlFormat3ReadBudget,
      proposedPod: UUID, exactUnit: UUID, exactForest: UUID,
      deploymentInputs: MySqlScriptSchemaInputs = null): Boolean =
    var found = false
    var ordinal = 0
    while ordinal < relation.rows.size do
      // 即使已找到碰撞，也完整验证后续历史/无关行。
      val row=relation.rows(ordinal)
      val rowContains=scanRow(row,originalRoot,proposedPod,deploymentInputs)
      val selected=exactUnit==null || (binary(row,1).equalsUuid(exactUnit) && binary(row,2).equalsUuid(exactForest))
      found = (rowContains && selected) || found
      ordinal += 1
    found

  private def validationWorkspace(snapshot: MySqlFormat3FullSnapshot): Long =
    var bytes = 16384L
    var relationIndex = 0
    while relationIndex < snapshot.relations.size do
      val relation = snapshot.relations(relationIndex)
      var rowIndex = 0
      while rowIndex < relation.rows.size do
        bytes = Math.addExact(bytes, Math.addExact(1024L, Math.multiplyExact(relation.rows(rowIndex).size.toLong, 256L)))
        rowIndex += 1
      relationIndex += 1
    bytes

  private def scanRow(row: Vector[MySqlFormat3Cell], root: MySqlFormat3ReadBudget, proposed: UUID,
      deploymentInputs: MySqlScriptSchemaInputs): Boolean =
    root.reserveTemporaryMemory(1024L, "forest row child/adapter/expectation/functions/cleanup representations")
    var child: MySqlFormat3ReadBudget = null
    var forest: Array[Byte] = null
    var forestSha: Array[Byte] = null
    var index: Array[Byte] = null
    var indexSha: Array[Byte] = null
    var expected: SourceForestMembershipCodec.ForestExpectation = null
    var adapter: SourceForestAllocationBudget = null
    var primary: Throwable = null
    try
      child = if deploymentInputs == null then root.newCanonicalScope()
      else
        root.requireScriptDataInputs(deploymentInputs)
        root.newCanonicalDataScope()
      val actualChild = child
      adapter = new SourceForestAllocationBudget:
        override def exactProfile(): SourceLimitProfile = root.profile
        override def reserveTemporary(bytes: Long, context: String): Unit =
          try
            if root.isClosed || actualChild.isClosed || !actualChild.isCanonicalChildOf(root) || !actualChild.sharesAllocationLedger(root) then
              throw new SQLDataException("source forest adapter lost exact open root")
            actualChild.reserveTemporaryMemory(bytes, context)
          catch
            case failure: java.sql.SQLException => throw new IOException("source forest work reservation failed", failure)
        override def releaseTemporary(bytes: Long, context: String): Unit =
          try actualChild.releaseTemporaryMemory(bytes, context)
          catch
            case failure: java.sql.SQLException => throw new IOException("source forest work release failed", failure)
      if row.size != 15 then throw new SQLDataException("source forest complete row width differs")
      val state = text(row, 4)
      if state != "DRAFT" && state != "SEALED" then throw new SQLDataException("source forest state is unsupported")
      val codec = text(row, 5)
      if codec != "FP_SOURCE_FOREST_BE_V2" then throw new SQLDataException("source forest codec is unsupported")
      expected = new SourceForestMembershipCodec.ForestExpectation(uuid(row, 0, child), uuid(row, 1, child), uuid(row, 2, child),
        integer(row, 3), uuid(row, 12, child), codec, integer(row, 8), integer(row, 9), integer(row, 10), integer(row, 11))
      forest = binary(row, 6).copyBytes(child, "complete forest SQL payload copy")
      forestSha = binary(row, 7).copyBytes(child, "complete forest SQL SHA copy")
      // NULL pair is retained DATA but cannot support a negative collision result.
      index = binary(row, 13).copyBytes(child, "complete membership SQL payload copy")
      indexSha = binary(row, 14).copyBytes(child, "complete membership SQL SHA copy")
      SourceForestMembershipCodec.containsVerified(forest, forestSha, index, indexSha, root.profile, adapter, proposed, expected)
    catch
      case failure: Throwable => primary = failure; throw failure
    finally
      forest = null;forestSha = null;index = null;indexSha = null;expected = null;adapter = null
      val owned = child
      child = null
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => if owned != null then owned.closeScopedReadBudget(),
        () => root.releaseTemporaryMemory(1024L, "forest row child/adapter/expectation/functions/cleanup representations")))

  private def binary(row: Vector[MySqlFormat3Cell], index: Int): MySqlFormat3Binary = row(index) match
    case MySqlFormat3Cell.Binary(value) if value != null => value
    case _ => throw new SQLDataException("source forest mandatory binary payload is absent or mistyped")
  private def integer(row: Vector[MySqlFormat3Cell], index: Int): Long = row(index) match
    case MySqlFormat3Cell.SignedInteger(value) => value
    case _ => throw new SQLDataException("source forest scalar is mistyped")
  private def text(row: Vector[MySqlFormat3Cell], index: Int): String = row(index) match
    case MySqlFormat3Cell.Text(value) if value != null => value
    case _ => throw new SQLDataException("source forest text is mistyped")
  private def uuid(row: Vector[MySqlFormat3Cell], index: Int, budget: MySqlFormat3ReadBudget): UUID =
    val value = binary(row, index)
    if value.length != 16 then throw new SQLDataException("source forest UUID is not exact 16-byte DATA")
    budget.reserveTemporaryMemory(32L, "source forest SQL expectation UUID")
    var most = 0L;var least = 0L;var offset = 0
    while offset < 8 do
      most = (most << 8) | (value.byteAt(offset).toLong & 255L)
      least = (least << 8) | (value.byteAt(offset + 8).toLong & 255L)
      offset += 1
    new UUID(most, least)
