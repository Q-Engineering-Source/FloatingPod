package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import scala.collection.mutable

/** 固定 DDL6/DDL7 共有 foundation 的真实62 FK 图；不接收 caller 提供的 SQL/table/key。 */
private[mysql] object MySqlControlledDebugRelations:
  private final case class ForeignKey(table: String, columns: Vector[String], target: String, targetColumns: Vector[String])
  private val ForeignKeys = Vector(
    ForeignKey("fp_recovery_operation",Vector("binding_uuid"),"physical_recovery_binding",Vector("binding_uuid")),
    ForeignKey("fp_recovery_operation",Vector("binding_uuid","reserve_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("fp_recovery_operation",Vector("binding_uuid","previous_step_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("fp_recovery_operation",Vector("binding_uuid","settled_by_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("floating_pod_state",Vector("binding_uuid"),"physical_recovery_binding",Vector("binding_uuid")),
    ForeignKey("floating_pod_state",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("player_entitlement",Vector("binding_uuid"),"physical_recovery_binding",Vector("binding_uuid")),
    ForeignKey("player_entitlement",Vector("binding_uuid","created_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("fp_recovery_reservation",Vector("binding_uuid","operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("fp_recovery_reservation",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_recovery_footprint",Vector("pod_uuid"),"floating_pod_state",Vector("uuid")),
    ForeignKey("physical_recovery_footprint",Vector("binding_uuid"),"physical_recovery_binding",Vector("binding_uuid")),
    ForeignKey("physical_recovery_footprint",Vector("binding_uuid","created_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_recovery_footprint_chunk",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")),
    ForeignKey("physical_recovery_intent",Vector("binding_uuid","pod_uuid","attempt_uuid","target_footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")),
    ForeignKey("physical_recovery_intent",Vector("binding_uuid","pod_uuid","predecessor_attempt_uuid","predecessor_footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")),
    ForeignKey("physical_recovery_intent",Vector("binding_uuid","prepare_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_recovery_intent",Vector("binding_uuid","activation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_recovery_intent",Vector("binding_uuid","cleanup_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_recovery_intent",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_recovery_locator",Vector("binding_uuid","pod_uuid","current_attempt_uuid","current_footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")),
    ForeignKey("physical_recovery_locator",Vector("binding_uuid","pod_uuid","current_attempt_uuid"),"physical_recovery_intent",Vector("binding_uuid","pod_uuid","attempt_uuid")),
    ForeignKey("physical_recovery_locator",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_recovery_obligation",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")),
    ForeignKey("physical_recovery_obligation",Vector("binding_uuid","created_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_recovery_obligation",Vector("binding_uuid","attempt_retirement_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_recovery_obligation",Vector("binding_uuid","verified_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_recovery_obligation",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("fp_lifecycle_completion",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")),
    ForeignKey("fp_lifecycle_completion",Vector("binding_uuid","completion_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("fp_lifecycle_completion",Vector("binding_uuid","consumed_by_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("fp_lifecycle_completion",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_footprint_transfer",Vector("binding_uuid","pod_uuid","attempt_uuid","from_footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")),
    ForeignKey("physical_footprint_transfer",Vector("binding_uuid","pod_uuid","attempt_uuid","to_footprint_uuid"),"physical_recovery_footprint",Vector("binding_uuid","pod_uuid","attempt_uuid","footprint_uuid")),
    ForeignKey("physical_footprint_transfer",Vector("binding_uuid","prepare_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_footprint_transfer",Vector("binding_uuid","commit_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_footprint_transfer",Vector("binding_uuid","cleanup_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_footprint_transfer",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_source_unit",Vector("binding_uuid"),"physical_recovery_binding",Vector("binding_uuid")),
    ForeignKey("physical_source_unit",Vector("binding_uuid","created_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_source_unit",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_source_forest",Vector("binding_uuid","unit_uuid"),"physical_source_unit",Vector("binding_uuid","unit_uuid")),
    ForeignKey("physical_source_forest",Vector("binding_uuid","created_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_source_transfer",Vector("binding_uuid","unit_uuid","old_forest_uuid","old_epoch"),"physical_source_forest",Vector("binding_uuid","unit_uuid","forest_uuid","source_epoch")),
    ForeignKey("physical_source_transfer",Vector("binding_uuid","unit_uuid","target_forest_uuid","target_epoch"),"physical_source_forest",Vector("binding_uuid","unit_uuid","forest_uuid","source_epoch")),
    ForeignKey("physical_source_transfer",Vector("binding_uuid","prepare_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_source_transfer",Vector("binding_uuid","cutover_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_source_transfer",Vector("binding_uuid","retire_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_source_transfer",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_source_write",Vector("binding_uuid","resource_kind","carrier_key_hash"),"fp_recovery_reservation",Vector("binding_uuid","resource_kind","resource_key_hash")),
    ForeignKey("physical_source_write",Vector("binding_uuid","prepared_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_source_write",Vector("binding_uuid","resolved_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_source_write",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_source_obligation",Vector("binding_uuid","unit_uuid","forest_uuid"),"physical_source_forest",Vector("binding_uuid","unit_uuid","forest_uuid")),
    ForeignKey("physical_source_obligation",Vector("binding_uuid","resource_kind","carrier_key_hash"),"fp_recovery_reservation",Vector("binding_uuid","resource_kind","resource_key_hash")),
    ForeignKey("physical_source_obligation",Vector("binding_uuid","write_uuid"),"physical_source_write",Vector("binding_uuid","write_uuid")),
    ForeignKey("physical_source_obligation",Vector("binding_uuid","created_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_source_obligation",Vector("binding_uuid","discharged_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_source_obligation",Vector("binding_uuid","last_mutation_operation_uuid"),"fp_recovery_operation",Vector("binding_uuid","operation_uuid")),
    ForeignKey("physical_source_unit",Vector("binding_uuid","unit_uuid","current_forest_uuid","current_epoch"),"physical_source_forest",Vector("binding_uuid","unit_uuid","forest_uuid","source_epoch")),
    ForeignKey("physical_recovery_intent",Vector("binding_uuid","source_completion_uuid"),"fp_lifecycle_completion",Vector("binding_uuid","receipt_uuid")),
    ForeignKey("physical_recovery_intent",Vector("binding_uuid","source_unit_uuid","source_forest_uuid"),"physical_source_forest",Vector("binding_uuid","unit_uuid","forest_uuid"))
  )

  /** 完整图验证仅 Unit 逃逸；临时 projection/索引债由同 root comparison child 关闭。 */
  def requireValidFullGraph(snapshot: MySqlFormat3FullSnapshot, root: MySqlFormat3ReadBudget,
      sourceRoot: MySqlFormat3ReadBudget = null,
      continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof] = None): Unit =
    requireGraph(snapshot,null,null,root,if sourceRoot==null then root else sourceRoot,continuation)

  /** schema5 保留完整26关系；这里只验证 DATA，不发行 operation 或 mutation 资格。 */
  def requireValidDeploymentReceiptGraph(snapshot: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget): Unit =
    MySqlFormat3QueryProjection.requireDeploymentSource(snapshot,inputs,root)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val child = MySqlRecoveryCanonicalBytes.allocationBudgetFor(root.profile)
      MySqlFormat3QueryProjection.validateDeploymentReceiptForComparison(snapshot,inputs,root)
      // 从冻结 schema definition 读取全部外键，包含四个 script 关系；schema5 新关系无外键。
      MySqlScriptSchemaProfile.tables.foreach { table =>
        table.foreignKeys.foreach { key =>
          requireForeignKey(snapshot.relation,
            ForeignKey(table.name,key.columns,key.target,key.targetColumns),child,null)
        }
      }
      val rows = snapshot.relation("fp_recovery_operation").rows
      var index = 0
      while index < rows.size do
        MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(rows(index),root.profile,child,Some(inputs))
        index += 1
    }

  /** Full27的完整FK及原操作envelope验证；临时图仅在原root的comparison child内存活。 */
  def requireValidDbFirstGraph(snapshot:MySqlScriptSchemaSnapshot,
      inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget):Unit =
    MySqlFormat3QueryProjection.requireDbFirstSource(snapshot,inputs,root)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetFor(root.profile)
      MySqlFormat3QueryProjection.validateDbFirstForComparison(snapshot,inputs,root)
      requireDbFirstGraphRows(snapshot,inputs,root,child)
    }

  /** 仅验证原decoded前像的完整业务图，不发行query source或运行资格。 */
  def requireValidDbFirstComparisonGraph(snapshot:MySqlScriptSchemaSnapshot,
      inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget):Unit =
    MySqlFormat3QueryProjection.validateDecodedDbFirstComparison(snapshot,inputs,root)
    val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    requireDbFirstGraphRows(snapshot,inputs,root,child)

  private def requireDbFirstGraphRows(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget,child:MySqlFormat3ReadBudget):Unit =
    MySqlScriptSchemaProfile.tables.foreach { table =>
      table.foreignKeys.foreach { key =>
        requireForeignKey(snapshot.relation,
          ForeignKey(table.name,key.columns,key.target,key.targetColumns),child,null)
      }
    }
    MySqlDbFirstSchemaDefinition.snapshotForeignKeys.foreach { key =>
      requireForeignKey(snapshot.relation,
        ForeignKey(MySqlDbFirstSchemaDefinition.SnapshotTable,key.columns,key.target,key.targetColumns),child,null)
    }
    val rows=snapshot.relation("fp_recovery_operation").rows
    var index=0
    while index<rows.size do
      MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(rows(index),root.profile,child,Some(inputs))
      index+=1

  def requireControlledDeploymentReceiptClosure(snapshot: MySqlScriptSchemaSnapshot,
      request: MySqlControlledDebugPrepareRequest,plan: MySqlOwnedRepositoryOperationPlan,
      root: MySqlFormat3ReadBudget): Unit =
    if request == null || plan == null then fail("original request and immutable plan required")
    requireValidDeploymentReceiptGraph(snapshot,snapshot.inputs,root)
    if MySqlFormat3SourceForestMembershipScan.containsDeploymentReceipt(snapshot,snapshot.inputs,root,request.podUuid) then
      fail("proposed controlled POD already occurs in an actual source forest member")
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val child = MySqlRecoveryCanonicalBytes.allocationBudgetFor(root.profile)
      MySqlFormat3RelationShapes.canonicalOrder.foreach { shape =>
        val relation = snapshot.relation(shape.table)
        if MySqlControlledDebugSelection.isSharedHeader(relation.table) && relation.rows.size != 1 then
          fail("controlled closure requires each fixed actual singleton")
        relation.rows.foreach(row => MySqlControlledDebugSelection.requireNoForeignIdentity(relation.table,row,request,plan))
      }
      ForeignKeys.foreach(key => requireForeignKey(snapshot.relation,key,child,request))
    }

  def requireControlledClosure(snapshot: MySqlFormat3FullSnapshot,request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan,root: MySqlFormat3ReadBudget,
      sourceRoot: MySqlFormat3ReadBudget = null,
      continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof] = None): Unit =
    if request==null || plan==null then fail("original request and immutable plan required")
    requireGraph(snapshot,request,plan,root,if sourceRoot==null then root else sourceRoot,continuation)

  private def requireGraph(snapshot: MySqlFormat3FullSnapshot,request: MySqlControlledDebugPrepareRequest,
      plan: MySqlOwnedRepositoryOperationPlan,root: MySqlFormat3ReadBudget,
      sourceRoot: MySqlFormat3ReadBudget,
      continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof]): Unit =
    MySqlFencedTransaction.requireControlledDebugSource(snapshot,sourceRoot,root,continuation)
    if snapshot == null || root == null || root.isClosed || root.isScopedReadBudget ||
      (snapshot.limitProfile ne root.profile) || !root.sharesAllocationLedger(snapshot.allocationBudget) then
      fail("exact open original root and full snapshot required")
    // 扫描全部 source forest 原文及完整重推 membership；Boolean 仍仅 DATA。
    // 保持在 comparison child 之外，让 scanner 使用同一 original root 的 lexical 子债。
    if request!=null && MySqlFormat3SourceForestMembershipScan.containsControlled(snapshot,root,request.podUuid,sourceRoot,continuation) then
      fail("proposed controlled POD already occurs in an actual source forest member")
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      val child = MySqlRecoveryCanonicalBytes.allocationBudgetFor(root.profile)
      // 复用真实全图 domain/current-history validator，结果绝不交给 mutation route。
      MySqlFormat3QueryProjection.validateForComparison(snapshot,sourceRoot,root,continuation)
      if request!=null then
        var relationIndex=0
        while relationIndex<snapshot.relations.size do
          val relation=snapshot.relations(relationIndex)
          if MySqlControlledDebugSelection.isSharedHeader(relation.table) && relation.rows.size!=1 then
            fail("controlled closure requires each fixed actual singleton")
          var rowIndex=0
          while rowIndex<relation.rows.size do
            MySqlControlledDebugSelection.requireNoForeignIdentity(relation.table,relation.rows(rowIndex),request,plan)
            rowIndex+=1
          relationIndex+=1
      requireForeignKeys(snapshot,child,request)
      val rows = snapshot.relation("fp_recovery_operation").rows
      var index = 0
      while index < rows.size do
        MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(rows(index),root.profile,child)
        index += 1
    }

  private def requireForeignKeys(snapshot: MySqlFormat3FullSnapshot,budget: MySqlFormat3ReadBudget,
      request: MySqlControlledDebugPrepareRequest): Unit =
    var keyIndex = 0
    while keyIndex < ForeignKeys.size do
      val key = ForeignKeys(keyIndex)
      requireForeignKey(snapshot.relation,key,budget,request)
      keyIndex += 1

  private def requireForeignKey(relation: String => MySqlFormat3RelationSnapshot,key: ForeignKey,
      budget: MySqlFormat3ReadBudget,request: MySqlControlledDebugPrepareRequest): Unit =
      val rows = relation(key.table).rows
      val targets = relation(key.target).rows
      val scratch = Math.addExact(4096L,Math.multiplyExact(targets.size.toLong,
        Math.addExact(512L,Math.multiplyExact(key.targetColumns.size.toLong,128L))))
      budget.reserveTemporaryMemory(scratch,"controlled fixed FK index and current key scratch")
      var index: mutable.HashMap[Vector[MySqlFormat3Cell],(Int,Boolean)] = null
      var currentKey: Vector[MySqlFormat3Cell] = null
      var primary: Throwable = null
      try
        index = mutable.HashMap.empty
        var targetIndex = 0
        while targetIndex < targets.size do
          currentKey = key.targetColumns.map(name => value(targets(targetIndex),key.target,name))
          val previousCount=index.get(currentKey).fold(0)(_._1)
          val selected=request!=null && MySqlControlledDebugSelection.includes(key.target,targets(targetIndex),request)
          index.update(currentKey,(Math.addExact(previousCount,1),selected))
          currentKey = null
          targetIndex += 1
        var rowIndex = 0
        while rowIndex < rows.size do
          val row = rows(rowIndex)
          var nullable = false
          var fieldIndex = 0
          while fieldIndex < key.columns.size do
            if value(row,key.table,key.columns(fieldIndex)) == MySqlFormat3Cell.SqlNull then nullable = true
            fieldIndex += 1
          if !nullable then
            currentKey = key.columns.map(name => value(row,key.table,name))
            index.get(currentKey) match
              case Some((1,parentSelected)) =>
                if request!=null && !MySqlControlledDebugSelection.isSharedHeader(key.target) &&
                  MySqlControlledDebugSelection.includes(key.table,row,request)!=parentSelected then
                  fail("unsupported foreign or source history crosses the controlled closure")
              case _ => fail(s"fixed complete foreign-key reference ${key.table}(${key.columns.mkString(",")}) -> ${key.target}(${key.targetColumns.mkString(",")}) does not have one actual target")
            currentKey = null
          rowIndex += 1
      catch
        case cause: Throwable => primary=cause;throw cause
      finally
        currentKey=null;index=null
        try budget.releaseTemporaryMemory(scratch,"controlled fixed FK index and current key release")
        catch case cleanup: Throwable =>
          if primary==null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)

  private def value(row: Vector[MySqlFormat3Cell],table: String,column: String): MySqlFormat3Cell =
    val shape = MySqlDbFirstRelationShapes.canonicalOrder.find(_.table == table)
      .getOrElse(fail("foreign-key table is outside the fixed complete layout"))
    val index = shape.columns.indexWhere(_.name == column)
    if index < 0 || index >= row.size then fail("foreign-key column is absent from complete row")
    row(index)
  private def fail(reason: String): Nothing = throw new SQLDataException("controlled full20 relation graph: " + reason)
