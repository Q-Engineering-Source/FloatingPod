package com.wi.floatingpod.storage.mysql

import java.nio.file.Path
import java.sql.{SQLDataException, SQLNonTransientConnectionException}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class MySqlManagedReadBudgetsTest:
  @TempDir var directory: Path = _
  private val profile = DeploymentReceiptStaticTestData.defaultProfile().sourceLimitProfile

  @Test def installationCatalogDataCanBeCapturedBeforeDeploymentBinding(): Unit =
    val budgets = new MySqlManagedReadBudgets(profile, profile.maxSqlPacketBytes())
    try
      val child = budgets.dataRoot.newCanonicalDataScope()
      try
        assertTrue(child.isCanonicalDataChildOf(budgets.dataRoot))
        child.reserveRetainedMemory(1024L, "actual pre-bind catalog DATA")
      finally child.closeScopedReadBudget()
      assertEquals(0L, budgets.dataRoot.accountedMemoryBytes)
      assertThrows(classOf[SQLNonTransientConnectionException], () => budgets.assemblyRoot.newCanonicalDataScope())
    finally budgets.close()

  @Test def dataAndAssemblyShareOneAggregateCapAndReleaseTheirOwnCharges(): Unit =
    val budgets = new MySqlManagedReadBudgets(profile, profile.maxSqlPacketBytes())
    try
      assertTrue(budgets.dataRoot.sharesAllocationLedger(budgets.assemblyRoot))
      budgets.dataRoot.reserveRetainedMemory(profile.maxRetainedBytes() - 32L, "installation retained payload")
      assertThrows(classOf[SQLDataException], () =>
        budgets.assemblyRoot.reserveRetainedMemory(64L, "assembly cannot bypass installation charges"))
      budgets.dataRoot.releaseRetainedMemory(profile.maxRetainedBytes() - 32L, "payload detached")
      budgets.assemblyRoot.reserveRetainedMemory(64L, "assembly within shared cap")
    finally budgets.close()
    assertTrue(budgets.dataRoot.isClosed)
    assertTrue(budgets.assemblyRoot.isClosed)
    budgets.close()

  @Test def realStaticAssemblyBindsOnlyItsOwnRootWhileDataScopeIsRetained(): Unit =
    val fixture = MySqlDbFirstHistoricalTestFixture.fixture(directory, DeploymentReceiptStaticTestData.defaultProfile())
    val budgets = new MySqlManagedReadBudgets(fixture.profile.sourceLimitProfile, fixture.profile.sourceLimitProfile.maxSqlPacketBytes())
    var child: MySqlFormat3ReadBudget = null
    var owner: MySqlRuntimeInputs.AssemblyOwner = null
    try
      child = budgets.dataRoot.newCanonicalDataScope()
      child.reserveRetainedMemory(1024L, "catalog retained across normal assembly")
      owner = MySqlRuntimeInputs.beginV2(budgets.assemblyRoot, Thread.currentThread())
      val graph = MySqlRuntimeInputs.assembleDbFirstStatic(fixture.metadata, owner, DeploymentReceiptStaticTestData.policy)
      assertNotNull(graph.configurationSha256)
      assertFalse(budgets.dataRoot.isClosed)
      assertTrue(child.isCanonicalDataChildOf(budgets.dataRoot))
    finally
      try
        if owner != null then owner.close()
      finally
        try
          if child != null then child.closeScopedReadBudget()
        finally budgets.close()
