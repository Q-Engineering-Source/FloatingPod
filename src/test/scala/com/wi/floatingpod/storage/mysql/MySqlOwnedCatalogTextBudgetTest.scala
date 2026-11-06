package com.wi.floatingpod.storage.mysql

import java.io.StringReader
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 合成的发布配方文本仅用于实际 reader 计费回归，不作数据库观测或对象批准。 */
class MySqlOwnedCatalogTextBudgetTest:
  @Test def full27CatalogTextFitsTheActualManagedCaptureBudget(): Unit =
    val profile = MySqlManagedReleasePolicy.profile("floatingpodrbssv1",
      UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d"), "127.0.0.1")
    val rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(profile)
    val recipe = MySqlManagedStaticRecipe.fromPlan(MySqlManagedSchemaPlan.build(rendered))
    val projection = MySqlManagedTableProjection.project(recipe, rendered)
    val root = MySqlFormat3ReadBudget.rootOnLedger(profile.sourceLimitProfile,
      profile.sourceLimitProfile.maxSqlPacketBytes(), new MySqlFormat3AllocationLedger(profile.sourceLimitProfile.maxRetainedBytes()))
    try
      val allocation = new MySqlSchemaAuditor.AuditAllocation(root):
        override protected def requireOwner(): Unit = assertFalse(root.isClosed)
        override protected def requireObservationOwner(value: UnverifiedMySqlSchemaObservation): Unit =
          throw new UnsupportedOperationException("text decode does not issue observations")
      var count = 0
      projection.sets.foreach(_.rows.foreach(_.foreach {
        case MySqlManagedTableProjection.ExpectedField.Exact(MySqlAuditText(text)) =>
          assertEquals(text, MySqlSchemaAuditor.readBoundedTextOwned(new StringReader(text), allocation, "compiled catalog text"))
          count += 1
        case _ => ()
      }))
      assertTrue(count > 1000)
      assertTrue(root.accountedMemoryBytes < profile.sourceLimitProfile.maxRetainedBytes())
    finally root.closeRootReadBudget()
