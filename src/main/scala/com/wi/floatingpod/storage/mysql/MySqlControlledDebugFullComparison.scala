package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException

/**
 * 完整 raw 读回比较，不依赖 closure、descriptor 或摘要决定相等。
 * expected 必须是原 owned materialization 保存的完整目标；本 DATA seam 不发行 ACK。
 */
private[mysql] object MySqlControlledDebugFullComparison:
  def requireExactDeploymentReceiptReadback(expected: MySqlScriptSchemaSnapshot,
      observed: MySqlScriptSchemaSnapshot,originalRoot: MySqlFormat3ReadBudget): Unit =
    if expected == null || observed == null then fail("complete original Full26 readback required")
    MySqlFormat3QueryProjection.requireDeploymentSource(expected,expected.inputs,originalRoot)
    MySqlFormat3QueryProjection.requireDeploymentSource(observed,expected.inputs,originalRoot)
    if expected.schema != observed.schema || expected.connection != observed.connection ||
        expected.observationScope != observed.observationScope || expected.maxAllowedPacketBytes != observed.maxAllowedPacketBytes then
      fail("complete Full26 observation identity differs")
    MySqlControlledDebugRelations.requireValidDeploymentReceiptGraph(expected,expected.inputs,originalRoot)
    MySqlControlledDebugRelations.requireValidDeploymentReceiptGraph(observed,expected.inputs,originalRoot)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(originalRoot) {
      compareRelations(expected.relations,observed.relations,expected.inputs.canonicalOrder)
    }

  def requireExactReadback(expected: MySqlFormat3FullSnapshot, observed: MySqlFormat3FullSnapshot,
      originalRoot: MySqlFormat3ReadBudget,sourceRoot: MySqlFormat3ReadBudget = null,
      continuation: Option[MySqlFencedTransaction.ControlledDebugContinuationProof] = None): Unit =
    requireInputs(expected, observed, originalRoot)
    MySqlControlledDebugRelations.requireValidFullGraph(expected,originalRoot,sourceRoot,continuation)
    MySqlControlledDebugRelations.requireValidFullGraph(observed,originalRoot,sourceRoot,continuation)
    MySqlRecoveryCanonicalBytes.withComparisonBudget(originalRoot) {
      compareRelations(expected.relations,observed.relations,MySqlFormat3RelationShapes.canonicalOrder)
    }

  private def compareRelations(expected: Vector[MySqlFormat3RelationSnapshot],
      observed: Vector[MySqlFormat3RelationSnapshot],shapes: Vector[MySqlFormat3RelationShape]): Unit =
      if expected.size != shapes.size || observed.size != shapes.size then fail("complete relation count differs")
      var relationIndex = 0
      while relationIndex < shapes.size do
        val left = expected(relationIndex)
        val right = observed(relationIndex)
        val shape = shapes(relationIndex)
        if left.table != shape.table || right.table != shape.table || left.rows.size != right.rows.size then
          fail("complete relation identity or row count differs")
        var rowIndex = 0
        while rowIndex < left.rows.size do
          requireExactRow(left.rows(rowIndex), right.rows(rowIndex), shape, rowIndex)
          rowIndex += 1
        relationIndex += 1

  /** 也用于原 prepare ACK 的 committed reserve row；不取 SHA 替代内容。 */
  def requireExactOperationRow(expected: Vector[MySqlFormat3Cell], observed: Vector[MySqlFormat3Cell],
      originalRoot: MySqlFormat3ReadBudget): Unit =
    if expected == null || observed == null || originalRoot == null || originalRoot.isClosed then
      fail("original open root and both complete operation rows required")
    MySqlRecoveryCanonicalBytes.withComparisonBudget(originalRoot) {
      val child = MySqlRecoveryCanonicalBytes.allocationBudgetFor(originalRoot.profile)
      MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(expected, originalRoot.profile, child)
      MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(observed, originalRoot.profile, child)
      requireExactRow(expected, observed, MySqlFormat3RelationShapes.forTable("fp_recovery_operation"), 0)
    }

  private def requireInputs(expected: MySqlFormat3FullSnapshot, observed: MySqlFormat3FullSnapshot,
      root: MySqlFormat3ReadBudget): Unit =
    if expected == null || observed == null || root == null || root.isClosed || root.isScopedReadBudget ||
      (root.profile ne expected.limitProfile) || (root.profile ne observed.limitProfile) ||
      !root.sharesAllocationLedger(expected.allocationBudget) || !root.sharesAllocationLedger(observed.allocationBudget) then
      fail("same original workflow root and exact profile required")
    if expected.schema != observed.schema || expected.connection != observed.connection ||
      expected.observationScope != observed.observationScope ||
      expected.maxAllowedPacketBytes != observed.maxAllowedPacketBytes ||
      expected.relations.size != MySqlFormat3RelationShapes.ExpectedRelationCount ||
      observed.relations.size != MySqlFormat3RelationShapes.ExpectedRelationCount then
      fail("complete snapshot observation identity differs")

  private def requireExactRow(expected: Vector[MySqlFormat3Cell], observed: Vector[MySqlFormat3Cell],
      shape: MySqlFormat3RelationShape, rowIndex: Int): Unit =
    val width = shape.columns.size
    if expected == null || observed == null || expected.size != width || observed.size != width then
      fail("complete row width differs")
    var index = 0
    while index < width do
      // Binary.equals compares every original byte; Floating compares raw bits, NULL remains distinct.
      if expected(index) == null || observed(index) == null || expected(index) != observed(index) then
        // 只报告冻结结构坐标，不把原始业务单元内容写入异常。
        fail(s"complete raw cell differs: ${shape.table} row=$rowIndex column=${shape.columns(index).name}")
      index += 1

  private def fail(reason: String): Nothing = throw new SQLDataException("controlled full20 readback: " + reason)
