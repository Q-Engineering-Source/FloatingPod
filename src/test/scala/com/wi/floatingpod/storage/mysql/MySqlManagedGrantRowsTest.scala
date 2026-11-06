package com.wi.floatingpod.storage.mysql

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import com.google.gson.JsonParser
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.*

class MySqlManagedGrantRowsTest:
  private val profile = DeploymentReceiptStaticTestData.defaultProfile()
  private val rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(profile)
  private val plan = MySqlManagedSchemaPlan.build(rendered)
  private val expected = MySqlDbFirstSchemaProfile.expectedPrivilegeRows(rendered)

  @Test def everyCompiledNonRoutineGrantMapsAllItsRows(): Unit =
    val unmapped = plan.steps.filter(_.kind == MySqlManagedSchemaPlan.Kind.PrivilegeGrant)
      .filterNot(_.sql.startsWith("GRANT EXECUTE ON PROCEDURE "))
      .filter(step => MySqlManagedGrantRows.forStep(step.sql,expected).forall(_.isEmpty))
    assertEquals(Vector.empty[String],unmapped.map(_.sql),unmapped.map(_.sql).mkString("\n"))

  @Test def everyCompiledColumnGrantSelectsEachColumnAndTableGrantEachPrivilege(): Unit =
    plan.steps.filter(_.kind == MySqlManagedSchemaPlan.Kind.PrivilegeGrant)
      .filterNot(_.sql.startsWith("GRANT EXECUTE ON PROCEDURE ")).foreach { step =>
        val mapped = MySqlManagedGrantRows.forStep(step.sql,expected)
        val head = step.sql.stripPrefix("GRANT ").take(step.sql.indexOf(" ON ")-6)
        if head.startsWith("UPDATE (") then
          val columns = head.stripPrefix("UPDATE (").stripSuffix(")").split(",").toSet
          assertEquals(columns,mapped(1).map(_(4)).toSet,step.sql)
          assertTrue(mapped.zipWithIndex.filter(_._2 != 1).forall(_._1.isEmpty),step.sql)
        else
          val privileges = head.split(",").map(_.trim).toSet
          val got = mapped.zipWithIndex.flatMap { (rows,index) =>
            val column = Vector(4,5,3,2)(index)
            rows.map(_(column))
          }.toSet
          assertEquals(privileges,got,step.sql)
      }

  @Test def columnAndTableScopesAndUnrelatedIdentitiesNeverSubstituteForEachOther(): Unit =
    val columnStep = plan.steps.find(s => s.kind == MySqlManagedSchemaPlan.Kind.PrivilegeGrant && s.sql.startsWith("GRANT UPDATE (")).get
    val mapped = MySqlManagedGrantRows.forStep(columnStep.sql,expected)
    assertTrue(mapped(1).size > 1)
    val row = mapped(1).head
    val sourceColumns = columnStep.sql.substring(columnStep.sql.indexOf('(')+1,columnStep.sql.indexOf(')'))
    val narrowed = MySqlManagedGrantRows.forStep(columnStep.sql.replace(sourceColumns,row(4)),expected)
    assertEquals(Vector(row),narrowed(1))
    assertEquals(1,narrowed.map(_.size).sum)
    val tableSql = columnStep.sql.replace("UPDATE ("+sourceColumns+")","UPDATE")
    assertTrue(MySqlManagedGrantRows.forStep(tableSql,expected)(1).isEmpty)
    for altered <- Vector(columnStep.sql.replace(row(3),"not_the_table"),
        columnStep.sql.replace(row(2),"not_the_schema"),columnStep.sql.replace(row(0),"'not_the_user'@'127.0.0.1'"),
        columnStep.sql.replace(sourceColumns,"not_the_column")) do
      assertNotEquals(columnStep.sql,altered)
      assertTrue(MySqlManagedGrantRows.forStep(altered,expected).forall(_.isEmpty),altered)

  private def actualFinalSecurity(): Vector[MySqlAuditResultSet] =
    val reader = new InputStreamReader(getClass.getResourceAsStream("/mysql/rbss-final-security-20261105.json"), StandardCharsets.UTF_8)
    try
      JsonParser.parseReader(reader).getAsJsonObject.getAsJsonArray("sets").asScala.toVector.map { item =>
        val set = item.getAsJsonObject
        val columns = set.getAsJsonArray("columns").asScala.toVector.map { value =>
          val c = value.getAsJsonObject
          MySqlAuditColumn(c.get("label").getAsString,c.get("jdbcType").getAsInt,c.get("typeName").getAsString,
            c.get("precision").getAsInt,c.get("scale").getAsInt,c.get("nullable").getAsInt,c.get("signed").getAsBoolean)
        }
        val rows = set.getAsJsonArray("rows").asScala.toVector.map { row =>
          MySqlAuditRow(row.getAsJsonArray.asScala.toVector.map { cell =>
            val c = cell.getAsJsonObject
            assertEquals("text",c.get("kind").getAsString)
            MySqlAuditText(c.get("value").getAsString)
          })
        }
        MySqlAuditResultSet(set.get("id").getAsString,columns,rows)
      }
    finally reader.close()

  @Test def realFinalSecurityCanonicalOrderPreservesAllDataAndRejectsChangedMetadataOrMultiplicity(): Unit =
    val actual = actualFinalSecurity()
    assertEquals(Vector(282,179,5,7),actual.map(_.rows.size))
    val canonical = actual.map(s => s.copy(rows = s.rows.sortBy(_.cells.map {
      case MySqlAuditText(value) => value
      case _ => throw new AssertionError("text required")
    }.mkString("\u0000"))))
    assertNotEquals(actual,canonical,"actual RS17/RS18 order differs from semantic canonical order")
    assertTrue(MySqlManagedGrantRows.sameSecurityData(canonical,actual))
    assertTrue(MySqlManagedGrantRows.sameSecurityData(canonical,actual.map(s => s.copy(rows=s.rows.reverse))))
    def changed(index: Int, set: MySqlAuditResultSet): Vector[MySqlAuditResultSet] = actual.updated(index,set)
    val first = actual.head
    assertFalse(MySqlManagedGrantRows.sameSecurityData(canonical,actual.reverse))
    assertFalse(MySqlManagedGrantRows.sameSecurityData(canonical,changed(0,first.copy(id="wrong"))))
    assertFalse(MySqlManagedGrantRows.sameSecurityData(canonical,changed(0,first.copy(columns=first.columns.updated(0,first.columns.head.copy(precision=1))))))
    assertFalse(MySqlManagedGrantRows.sameSecurityData(canonical,changed(0,first.copy(rows=first.rows.tail))))
    assertFalse(MySqlManagedGrantRows.sameSecurityData(canonical,changed(0,first.copy(rows=first.rows :+ first.rows.head))))
    assertFalse(MySqlManagedGrantRows.sameSecurityData(canonical,changed(0,first.copy(rows=first.rows.updated(1,first.rows.head)))))
    val changedRow=first.rows.head.copy(cells=first.rows.head.cells.updated(0,MySqlAuditText("different-account")))
    assertFalse(MySqlManagedGrantRows.sameSecurityData(canonical,changed(0,first.copy(rows=first.rows.updated(0,changedRow)))))
