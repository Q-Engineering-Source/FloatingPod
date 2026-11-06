package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.source.{SourceForestAllocationBudget, SourceForestCodec, SourceForestMembershipCodec, SourceForestSharedBudgetFixtures}
import java.security.MessageDigest
import java.util.UUID
import java.sql.SQLDataException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

final class MySqlFormat3SourceForestMembershipScanTest:
  private val profile = SourceForestSharedBudgetFixtures.profile()
  private def id(value: Long): UUID = new UUID(0L, value)
  private def binary(bytes: Array[Byte]): MySqlFormat3Cell = MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes))
  private def uuid(value: UUID): MySqlFormat3Cell =
    val bytes = java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()
    binary(bytes)
  private def row(identity: Long, other: Boolean = false, retired: Boolean = false): Vector[MySqlFormat3Cell] =
    val forest = SourceForestSharedBudgetFixtures.forest(identity, other, false, retired)
    val encoded = SourceForestCodec.encode(forest, profile)
    val index = SourceForestMembershipCodec.derive(forest, profile)
    val own = forest.identity()
    Vector(uuid(own.bindingId()), uuid(own.unitId()), uuid(own.forestId()), MySqlFormat3Cell.SignedInteger(own.sourceEpoch()),
      MySqlFormat3Cell.Text("SEALED"), MySqlFormat3Cell.Text(SourceForestCodec.DOMAIN), binary(encoded.bytes().copy()),
      binary(encoded.sha256().copy()), MySqlFormat3Cell.SignedInteger(encoded.memberCount()), MySqlFormat3Cell.SignedInteger(encoded.rootCount()),
      MySqlFormat3Cell.SignedInteger(encoded.carrierCount()), MySqlFormat3Cell.SignedInteger(encoded.retainedBytes()), uuid(own.createdOperationId()),
      binary(index.bytes().copy()), binary(index.sha256().copy()))
  private def snapshot(root: MySqlFormat3ReadBudget, rows: Vector[Vector[MySqlFormat3Cell]]): MySqlFormat3FullSnapshot =
    val relations = MySqlFormat3RelationShapes.canonicalOrder.map(shape =>
      MySqlFormat3RelationSnapshot(shape.table, if shape.table == "physical_source_forest" then rows else Vector.empty))
    val observation = MySqlFormat3ConnectionObservation(id(300), "9.7.2", "fp_forest_test", "root@localhost", "InnoDB", 1, 1L,
      "REPEATABLE-READ", "REPEATABLE-READ", "STRICT_TRANS_TABLES", "+00:00", "TLSv1.3", "TLS_AES_256_GCM_SHA384")
    val result = new MySqlFormat3FullSnapshot("fp_forest_test", observation, MySqlFormat3ObservationScope(id(301), 1L, 1L),
      profile.maxSqlPacketBytes(), profile, relations, root)
    root.recordMaterializedSnapshot(relations)
    result
  private def withRoot(body: MySqlFormat3ReadBudget => Unit): Unit =
    val root = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    try body(root)
    finally root.closeRootReadBudget()

  @Test def completeNegativeAndRetiredFinalAliasUseTheOriginalLedger(): Unit = withRoot { root =>
    val image = snapshot(root, Vector(row(1, other = true), row(2, retired = true)))
    val baseline = root.accountedMemoryBytes
    assertTrue(MySqlFormat3SourceForestMembershipScan.contains(image, root, id(1)))
    assertEquals(baseline, root.accountedMemoryBytes)
    assertFalse(MySqlFormat3SourceForestMembershipScan.contains(image, root, id(9999)))
    assertEquals(baseline, root.accountedMemoryBytes)
  }
  @Test def positiveAndNegativeDoNotHideFinalMalformedHistory(): Unit = withRoot { root =>
    val broken = row(2).updated(7, binary(Array.fill[Byte](32)(0)))
    val image = snapshot(root, Vector(row(1), broken))
    val baseline = root.accountedMemoryBytes
    assertThrows(classOf[IllegalArgumentException], () => MySqlFormat3SourceForestMembershipScan.contains(image, root, id(1)))
    assertEquals(baseline, root.accountedMemoryBytes)
    assertThrows(classOf[IllegalArgumentException], () => MySqlFormat3SourceForestMembershipScan.contains(image, root, id(9999)))
    assertEquals(baseline, root.accountedMemoryBytes)
  }
  @Test def deletedAliasWithRecomputedShaCannotProduceNegative(): Unit = withRoot { root =>
    val original = row(1)
    val bytes = original(13) match
      case MySqlFormat3Cell.Binary(value) => value.copyBytes(root, "test index mutation")
      case _ => throw new AssertionError("fixture binary required")
    val reduced = java.util.Arrays.copyOf(bytes, bytes.length - 16)
    java.nio.ByteBuffer.wrap(reduced).putLong(66, 2L)
    val changed = original.updated(13, binary(reduced)).updated(14, binary(MessageDigest.getInstance("SHA-256").digest(reduced)))
    val image = snapshot(root, Vector(changed))
    val baseline = root.accountedMemoryBytes
    val failure = assertThrows(classOf[IllegalArgumentException], () => MySqlFormat3SourceForestMembershipScan.contains(image, root, id(3)))
    assertTrue(failure.getMessage.contains("union"))
    assertEquals(baseline, root.accountedMemoryBytes)
  }
  @Test def draftNullMembershipCannotSupportAbsence(): Unit = withRoot { root =>
    val unknown = row(1).updated(4, MySqlFormat3Cell.Text("DRAFT")).updated(13, MySqlFormat3Cell.SqlNull).updated(14, MySqlFormat3Cell.SqlNull)
    val image = snapshot(root, Vector(unknown))
    val baseline = root.accountedMemoryBytes
    assertThrows(classOf[SQLDataException], () => MySqlFormat3SourceForestMembershipScan.contains(image, root, id(9999)))
    assertEquals(baseline, root.accountedMemoryBytes)
  }
  @Test def originalRawNearCapRejectsLocallyValidGraphWithoutLosingResponsibility(): Unit = withRoot { root =>
    val image = snapshot(root, Vector(row(1)))
    root.reserveRetainedMemory(root.availableMemoryBytes - 4096L, "actual RAW responsibility near cap")
    val baseline = root.accountedMemoryBytes
    assertThrows(classOf[SQLDataException], () => MySqlFormat3SourceForestMembershipScan.contains(image, root, id(9999)))
    assertEquals(baseline, root.accountedMemoryBytes)
  }
  @Test def foreignAndClosedAllocationRootsCannotBeAccepted(): Unit = withRoot { root =>
    val image = snapshot(root, Vector(row(1)))
    withRoot { foreign => assertThrows(classOf[SQLDataException], () => MySqlFormat3SourceForestMembershipScan.contains(image, foreign, id(9999))) }
    val child = root.newCanonicalScope()
    try assertThrows(classOf[SQLDataException], () => MySqlFormat3SourceForestMembershipScan.contains(image, child, id(9999)))
    finally child.closeScopedReadBudget()
    root.closeRootReadBudget()
    assertThrows(classOf[SQLDataException], () => MySqlFormat3SourceForestMembershipScan.contains(image, root, id(9999)))
  }

  @Test def unresolvedClassificationAndWrongSqlScalarsNeverProduceAbsence(): Unit = withRoot { root =>
    val original = row(1)
    val unresolved = SourceForestCodec.encode(SourceForestSharedBudgetFixtures.forest(1L, false, true, false), profile)
    val unknown = original.updated(6, binary(unresolved.bytes().copy())).updated(7, binary(unresolved.sha256().copy()))
      .updated(11, MySqlFormat3Cell.SignedInteger(unresolved.retainedBytes()))
    val candidates = Vector(unknown, original.updated(3, MySqlFormat3Cell.SignedInteger(2L)),
      original.updated(8, MySqlFormat3Cell.SignedInteger(99L)), original.updated(9, MySqlFormat3Cell.SignedInteger(99L)),
      original.updated(10, MySqlFormat3Cell.SignedInteger(99L)), original.updated(12, uuid(id(999L))))
    candidates.foreach { candidate =>
      val image = snapshot(root, Vector(candidate))
      val baseline = root.accountedMemoryBytes
      assertThrows(classOf[IllegalArgumentException], () => MySqlFormat3SourceForestMembershipScan.contains(image, root, id(9999)))
      assertEquals(baseline, root.accountedMemoryBytes)
    }
  }
  @Test def everyAllocationPhaseRestoresTheActualLedgerAndPreservesCleanupFailure(): Unit = withRoot { root =>
    val forest = SourceForestSharedBudgetFixtures.forest(1L, false, false, false)
    val encoded = SourceForestCodec.encode(forest, profile)
    val index = SourceForestMembershipCodec.derive(forest, profile)
    val own = forest.identity()
    val expected = new SourceForestMembershipCodec.ForestExpectation(own.bindingId(), own.unitId(), own.forestId(), own.sourceEpoch(),
      own.createdOperationId(), SourceForestCodec.DOMAIN, encoded.memberCount(), encoded.rootCount(), encoded.carrierCount(), encoded.retainedBytes())
    val phases = Vector("forest owned decode input", "forest Reader decoded representation", "forest topology collection and path workspace",
      "NBT semantic allocation", "forest canonical buffer and defensive copies", "membership parse clone defensive result and hash framing",
      "membership TreeSet identities and framing objects", "membership canonical array defensive copies and result list")
    phases.foreach { phase =>
      val child = root.newCanonicalScope()
      val expectedFailure = new java.io.IOException("injected reservation boundary " + phase)
      val cleanupFailure = new java.io.IOException("injected cleanup boundary")
      var injected = false
      var cleanupInjected = false
      val baseline = root.accountedMemoryBytes
      val adapter = new SourceForestAllocationBudget:
        override def exactProfile(): com.wi.floatingpod.config.SourceLimitProfile = profile
        override def reserveTemporary(bytes: Long, context: String): Unit =
          if context == phase && !injected then
            injected = true
            throw expectedFailure
          child.reserveTemporaryMemory(bytes, context)
        override def releaseTemporary(bytes: Long, context: String): Unit =
          child.releaseTemporaryMemory(bytes, context)
          if injected && !cleanupInjected then
            cleanupInjected = true
            throw cleanupFailure
      var primary: Throwable = null
      var observed: Throwable = null
      try
        val failure = assertThrows(classOf[java.io.UncheckedIOException], () => SourceForestMembershipCodec.containsVerified(
          encoded.bytes().copy(), encoded.sha256().copy(), index.bytes().copy(), index.sha256().copy(), profile, adapter, id(9999), expected))
        observed = failure
        assertTrue(injected, phase)
        assertSame(expectedFailure, failure.getCause, phase)
        assertTrue(failure.getSuppressed.exists(_.getCause eq cleanupFailure), phase)
        assertFalse(child.isClosed, phase)
        if phase == "forest topology collection and path workspace" then
          assertTrue(root.accountedMemoryBytes > baseline, "unsettled nested cleanup must retain the caller's allocation responsibility")
      catch case cause: Throwable =>
        primary = cause
        throw cause
      finally
        try child.closeScopedReadBudget()
        catch case cleanup: Throwable =>
          if primary != null then MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
          else if observed != null then
            MySqlOwnedSessionLifecycle.suppress(observed, cleanup)
            throw observed
          else throw cleanup
      assertTrue(child.isClosed, phase)
      assertEquals(baseline, root.accountedMemoryBytes, phase)
    }
  }

  @Test def byteEquivalentForeignProfileOnSameActualLedgerIsRejected(): Unit =
    val ledger = new MySqlFormat3AllocationLedger(profile.maxRetainedBytes())
    val root = MySqlFormat3ReadBudget.rootOnLedger(profile, profile.maxSqlPacketBytes(), ledger)
    val copiedProfile = com.wi.floatingpod.config.SourceLimitProfile.decode(profile.canonicalBytes())
    val other = MySqlFormat3ReadBudget.rootOnLedger(copiedProfile, copiedProfile.maxSqlPacketBytes(), ledger)
    try
      val image = snapshot(root, Vector(row(1)))
      assertTrue(other.sharesAllocationLedger(root))
      val baseline = root.accountedMemoryBytes
      assertThrows(classOf[SQLDataException], () => MySqlFormat3SourceForestMembershipScan.contains(image, other, id(9999)))
      assertEquals(baseline, root.accountedMemoryBytes)
    finally
      other.closeRootReadBudget()
      root.closeRootReadBudget()

  @Test def exactForestSelectionCannotCrossCorrelateAnUnrelatedObligation(): Unit = withRoot { root =>
    val image=snapshot(root,Vector(row(1,other=true),row(2)))
    val baseline=root.accountedMemoryBytes
    assertTrue(MySqlFormat3SourceForestMembershipScan.containsInForest(image,root,id(1),id(1002),id(1003)))
    assertFalse(MySqlFormat3SourceForestMembershipScan.containsInForest(image,root,id(1),id(1999),id(1003)))
    assertFalse(MySqlFormat3SourceForestMembershipScan.containsInForest(image,root,id(1),id(1002),id(1999)))
    assertEquals(baseline,root.accountedMemoryBytes)
  }

  @Test def exactForestNegativeStillValidatesUnrelatedMalformedHistory(): Unit = withRoot { root =>
    val malformed=row(2).updated(14,binary(new Array[Byte](32)))
    val image=snapshot(root,Vector(row(1),malformed))
    val baseline=root.accountedMemoryBytes
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlFormat3SourceForestMembershipScan.containsInForest(image,root,id(9999),id(1999),id(1999)))
    assertEquals(baseline,root.accountedMemoryBytes)
  }

  @Test def exactForestSelectionRejectsForeignSiblingSnapshotAllocation(): Unit =
    val ledger=new MySqlFormat3AllocationLedger(profile.maxRetainedBytes())
    val original=MySqlFormat3ReadBudget.rootOnLedger(profile,profile.maxSqlPacketBytes(),ledger)
    val foreign=MySqlFormat3ReadBudget.rootOnLedger(profile,profile.maxSqlPacketBytes(),ledger)
    try
      val image=snapshot(original,Vector(row(1)))
      assertThrows(classOf[SQLDataException], () =>
        MySqlFormat3SourceForestMembershipScan.containsInForest(image,foreign,id(1),id(1002),id(1003)))
    finally
      foreign.closeRootReadBudget()
      original.closeRootReadBudget()
