package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException

/** 首启独立静态门：预期来自本次发布配方与版本化 transport 校准，不从实际对象反推。 */
private[mysql] object MySqlManagedStaticValidation:
  def validate(rendered: MySqlDbFirstSchemaProfile.Rendered, sets: Vector[MySqlAuditResultSet],
      root: MySqlFormat3ReadBudget, routineSource: MySqlAuditResultSet): MySqlApprovedObjectManifest =
    MySqlManifestConfigurationIdentity.requireRoot(root)
    if rendered == null || sets == null || sets.map(_.id) != MySqlAuditCoverage.StaticResultSetIds then
      reject("managed static validation requires the exact compiled render and ten actual sets")
    val workspace = Math.addExact(16L * 1024L * 1024L,
      Math.multiplyExact(MySqlDeploymentReceiptSchemaProfile.sourceSqlLength(), 256L))
    MySqlManifestConfigurationIdentity.temporary(root, workspace) {
      val plan = MySqlManagedSchemaPlan.build(rendered)
      val recipe = MySqlManagedStaticRecipe.fromPlan(plan)
      val projection = MySqlManagedTableProjection.project(recipe, rendered)
      MySqlManagedJdbcMetadata.validate(sets, root)
      projection.sets.foreach { expected =>
        val actual = sets.find(_.id == expected.id).getOrElse(reject("managed catalog projection set missing"))
        compareRows(expected.id, expected.rows, actual.rows, actual.columns.size, root)
      }
      MySqlManagedRoutineValidation.validate(recipe, sets, root, routineSource)
      // 原 manifest 构造器继续做同一 profile 的shape、self-description、完整计数及重复行校验。
      // 返回值只有静态 DATA；schema READY、Session、writer 与业务 Ready 仍由各自原门负责。
      MySqlApprovedObjectManifest.forDbFirst1(sets)
    }

  /** 同计划未完成时逐表重查。空集仅表示未观察到该表，不授权重发任何SQL。 */
  def verifyTable(rendered: MySqlDbFirstSchemaProfile.Rendered, table: String,
      sets: Vector[MySqlAuditResultSet], root: MySqlFormat3ReadBudget): Boolean =
    val plan = MySqlManagedSchemaPlan.build(rendered)
    verifyTableAt(rendered, table, sets, root, plan.deferredConstraintSteps.map(_.ordinal).toSet)

  /** 只接受原 journal 已授权阶段的封闭 SOURCE 约束集合；最终静态门仍固定为全部约束。 */
  def verifyTableAt(rendered: MySqlDbFirstSchemaProfile.Rendered, table: String,
      sets: Vector[MySqlAuditResultSet], root: MySqlFormat3ReadBudget,
      appliedConstraintOrdinals: Set[Int]): Boolean =
    MySqlManifestConfigurationIdentity.requireRoot(root)
    if rendered == null || table == null || !rendered.tables.exists(_.name == table) ||
        sets == null || sets.map(_.id) != MySqlAuditCoverage.StaticResultSetIds then
      reject("managed table inspection requires an exact original recipe table and actual capture")
    MySqlManagedJdbcMetadata.validate(sets, root)
    val tableSet = sets.head
    if !tableSet.rows.exists(row => row.cells.lift(1).contains(MySqlAuditText(table))) then return false
    val workspace = Math.addExact(16L * 1024L * 1024L,
      Math.multiplyExact(MySqlDeploymentReceiptSchemaProfile.sourceSqlLength(), 256L))
    MySqlManifestConfigurationIdentity.temporary(root, workspace) {
      val recipe = MySqlManagedStaticRecipe.fromPlanAt(MySqlManagedSchemaPlan.build(rendered), appliedConstraintOrdinals)
      val projection = MySqlManagedTableProjection.projectAt(recipe, rendered, appliedConstraintOrdinals)
      val tableColumns = Map("RS02_TABLES" -> 1, "RS03_COLUMNS" -> 1, "RS04_STATISTICS" -> 1,
        "RS05_TABLE_CONSTRAINTS" -> 4, "RS06_KEY_COLUMN_USAGE" -> 5, "RS07_REFERENTIAL_CONSTRAINTS" -> 9)
      projection.sets.foreach { expected =>
        val index = tableColumns(expected.id)
        val expectedRows = expected.rows.filter(_.lift(index).contains(
          MySqlManagedTableProjection.ExpectedField.Exact(MySqlAuditText(table))))
        val actual = sets.find(_.id == expected.id).getOrElse(reject("managed table inspection set missing"))
        val actualRows = actual.rows.filter(_.cells.lift(index).contains(MySqlAuditText(table)))
        compareRows(expected.id, expectedRows, actualRows, actual.columns.size, root)
      }
      val checks = sets.find(_.id == "RS08_CHECK_CONSTRAINTS")
        .getOrElse(reject("managed table CHECK inspection set missing"))
      MySqlManagedRoutineValidation.validateTableChecks(recipe, table, checks, root)
      true
    }

  private def compareRows(id: String, expected: Vector[Vector[MySqlManagedTableProjection.ExpectedField]],
      actual: Vector[MySqlAuditRow], columns: Int, root: MySqlFormat3ReadBudget): Unit =
    if expected.size != actual.size then reject("managed static row count differs from release recipe: " + id)
    expected.zip(actual).zipWithIndex.foreach { case ((fields, row), rowNumber) =>
      if fields.size != columns || row.cells.size != columns then reject("managed static row framing differs: " + id)
      fields.zip(row.cells).zipWithIndex.foreach { case ((field, cell), column) =>
        val equal = field match
          case MySqlManagedTableProjection.ExpectedField.Exact(value) => cell == value
          case MySqlManagedTableProjection.ExpectedField.GeneratedExpression(tokens) => cell match
            case MySqlAuditText(value) =>
              MySqlManifestConfigurationIdentity.temporary(root, Math.addExact(4096L, Math.multiplyExact(128L, value.length.toLong))) {
                MySqlManagedCheckExpression.equivalent(tokens, value)
              }
            case _ => false
        if !equal then reject(s"managed static field differs from release recipe: $id row=$rowNumber column=$column")
      }
    }

  private def reject(message: String): Nothing = throw new SQLDataException(message)
