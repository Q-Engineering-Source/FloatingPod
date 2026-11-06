package com.wi.floatingpod.storage.mysql

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.sql.{SQLDataException, SQLNonTransientConnectionException}

/** 真reader使用的budget issuance seam；输入是DATA观察，不伪JDBC/Fresh actor admission。 */
final class MySqlOriginalActorImageBudgetTest:
  @Test def ordinaryRootRejectsForeignWorkerAndCannotAcquireRetainedHandoff(): Unit =
    val profile = MySqlFull20CodecFixture.profile()
    val root = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    val failure = new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val worker = new Thread(() =>
      try root.issueOwnedSnapshotImage(MySqlFull20CodecFixture.schema,
        MySqlFull20CodecFixture.observation, MySqlFull20CodecFixture.scope, 16777216L)
      catch case cause: Throwable => failure.set(cause)
      (), "foreign-ordinary-image-worker")
    worker.start(); worker.join(3000L)
    assertFalse(worker.isAlive)
    assertTrue(failure.get().isInstanceOf[SQLNonTransientConnectionException])
    assertThrows(classOf[SQLNonTransientConnectionException], () => root.bindRetainedImageWorker(new Object()))
    val image = root.issueOwnedSnapshotImage(MySqlFull20CodecFixture.schema,
      MySqlFull20CodecFixture.observation, MySqlFull20CodecFixture.scope, 16777216L)
    image.closeScopedReadBudget()
    root.closeRootReadBudget()

  @Test def oneOriginalObservationIssuesDifferentImagesOnSameLedgerWithoutReset(): Unit =
    val profile = MySqlFull20CodecFixture.profile()
    val root = new MySqlFormat3ReadBudget(profile,profile.maxSqlPacketBytes())
    val first = root.issueOwnedSnapshotImage(MySqlFull20CodecFixture.schema,
      MySqlFull20CodecFixture.observation,MySqlFull20CodecFixture.scope,16777216L)
    first.reserveRetainedMemory(4096L,"first actual image retained typed rows")
    val originalCharge = root.accountedMemoryBytes
    val second = root.issueOwnedSnapshotImage(MySqlFull20CodecFixture.schema,
      MySqlFull20CodecFixture.observation,MySqlFull20CodecFixture.scope,16777216L)
    assertNotSame(first,second)
    assertTrue(first.isSnapshotReadChildOf(root))
    assertTrue(second.isSnapshotReadChildOf(root))
    assertTrue(first.sharesAllocationLedger(root))
    assertTrue(second.sharesAllocationLedger(root))
    assertTrue(root.accountedMemoryBytes > originalCharge)
    assertFalse(first.isClosed)
    assertFalse(root.isClosed)

  @Test def changedActualSessionPacketRetainsOriginalImagesAndStickyCause(): Unit =
    val profile = MySqlFull20CodecFixture.profile()
    val root = new MySqlFormat3ReadBudget(profile,profile.maxSqlPacketBytes())
    val first = root.issueOwnedSnapshotImage(MySqlFull20CodecFixture.schema,
      MySqlFull20CodecFixture.observation,MySqlFull20CodecFixture.scope,16777216L)
    val charge = root.accountedMemoryBytes
    val cause = assertThrows(classOf[SQLNonTransientConnectionException], () => root.issueOwnedSnapshotImage(
      MySqlFull20CodecFixture.schema,MySqlFull20CodecFixture.observation,MySqlFull20CodecFixture.scope,8000000L))
    val later = assertThrows(classOf[SQLNonTransientConnectionException], () => root.issueOwnedSnapshotImage(
      MySqlFull20CodecFixture.schema,MySqlFull20CodecFixture.observation,MySqlFull20CodecFixture.scope,16777216L))
    assertSame(cause,later.getCause)
    assertTrue(cause.getStackTrace.nonEmpty)
    assertEquals(charge,root.accountedMemoryBytes)
    assertFalse(first.isClosed)
    assertFalse(root.isClosed)

  @Test def inspectionGenerationCannotReuseOrdinaryImageIssuer(): Unit =
    val profile=MySqlFull20CodecFixture.profile()
    val root=new MySqlFormat3ReadBudget(profile,profile.maxSqlPacketBytes())
    val original=MySqlFull20CodecFixture.scope
    val first=root.issueOwnedSnapshotImage(MySqlFull20CodecFixture.schema,
      MySqlFull20CodecFixture.observation,original,16777216L)
    val charge=root.accountedMemoryBytes
    val inspection=original.copy(connectionGeneration=Math.incrementExact(original.connectionGeneration))
    val cause=assertThrows(classOf[SQLNonTransientConnectionException],()=>root.issueOwnedSnapshotImage(
      MySqlFull20CodecFixture.schema,MySqlFull20CodecFixture.observation,inspection,16777216L))
    assertEquals("original actor session/image observation changed",cause.getMessage)
    val later=assertThrows(classOf[SQLNonTransientConnectionException],()=>root.issueOwnedSnapshotImage(
      MySqlFull20CodecFixture.schema,MySqlFull20CodecFixture.observation,original,16777216L))
    assertSame(cause,later.getCause)
    assertEquals(charge,root.accountedMemoryBytes)
    assertFalse(first.isClosed)
    first.closeScopedReadBudget()
    root.closeRootReadBudget()

  @Test def secondImageCapacityFailureNeverRetiresOriginalRootOrFirstImage(): Unit =
    val profile = MySqlFull20CodecFixture.profile()
    val root = new MySqlFormat3ReadBudget(profile,profile.maxSqlPacketBytes())
    val first = root.issueOwnedSnapshotImage(MySqlFull20CodecFixture.schema,
      MySqlFull20CodecFixture.observation,MySqlFull20CodecFixture.scope,16777216L)
    root.reserveTemporaryMemory(root.availableMemoryBytes - 1L,"actual test-owned live budget competitor")
    val charge = root.accountedMemoryBytes
    val cause = assertThrows(classOf[SQLDataException], () => root.issueOwnedSnapshotImage(
      MySqlFull20CodecFixture.schema,MySqlFull20CodecFixture.observation,MySqlFull20CodecFixture.scope,16777216L))
    val later = assertThrows(classOf[SQLNonTransientConnectionException], () => root.issueOwnedSnapshotImage(
      MySqlFull20CodecFixture.schema,MySqlFull20CodecFixture.observation,MySqlFull20CodecFixture.scope,16777216L))
    assertSame(cause,later.getCause)
    assertEquals(charge,root.accountedMemoryBytes)
    assertFalse(first.isClosed)
    assertFalse(root.isClosed)
