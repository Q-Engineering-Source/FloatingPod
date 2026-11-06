package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.sql.{SQLDataException, SQLNonTransientConnectionException}
import java.time.LocalDateTime
import java.util.{Arrays, UUID}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotSame, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

final class MySqlModeOperationEffectCodecTest:
  private val OperationTable = "fp_recovery_operation"
  private val WriterTable = "fp_runtime_writer"

  @Test def full27RawPreservesOldRowsAndRoutesOnlyItsOwnNewStep(): Unit =
    val fixtureInputs = MySqlScriptSchemaDataTestFixture.Full26Inputs
    val configured = MySqlDeploymentReceiptSchemaProfile.inputs(fixtureInputs.schema,
      DeploymentReceiptStaticTestData.defaultProfile().accounts, fixtureInputs.sourceLimitProfile,
      fixtureInputs.budget, fixtureInputs.deploymentBudget.get, 8L, Vector.empty)
    val rendered = MySqlDbFirstSchemaProfile.render(configured)
    val inputs = MySqlScriptSchemaInputs.createDbFirstFull27(rendered)
    val foreignInputs = MySqlScriptSchemaInputs.createDbFirstFull27(rendered)
    val profile = inputs.sourceLimitProfile
    def rootFor(i: MySqlScriptSchemaInputs): MySqlFormat3ReadBudget =
      i.newAllocationRoot(i.budget.packetBytes, new MySqlFormat3AllocationLedger(profile.maxRetainedBytes()))
    val root = rootFor(inputs)
    val foreignRoot = rootFor(inputs)
    val closedRoot = rootFor(inputs)
    closedRoot.closeRootReadBudget()
    val oldRows = MySqlScriptSchemaDataTestFixture.full26OrdinaryRelations(fixtureInputs)
    val manifestShape = inputs.forTable("fp_schema_manifest")
    val relations = oldRows.map { relation =>
      if relation.table != "fp_schema_manifest" then relation
      else relation.copy(rows = relation.rows.map(row => row
        .updated(manifestShape.columns.indexWhere(_.name == "format_version"), MySqlFormat3Cell.SignedInteger(6L))
        .updated(manifestShape.columns.indexWhere(_.name == "ddl_sha256"), MySqlFormat3Cell.Binary(
          MySqlFormat3Binary.takeOwned(java.util.HexFormat.of().parseHex(inputs.ddlSha256))))))
    } :+ MySqlFormat3RelationSnapshot(MySqlDbFirstSchemaDefinition.SnapshotTable, Vector.empty)
    val image = MySqlScriptSchemaSnapshot.captureMaterialized(inputs.schema,
      MySqlScriptSchemaDataTestFixture.Connection, MySqlScriptSchemaDataTestFixture.Scope,
      inputs.budget.packetBytes, inputs, relations, root)
    val original = image.relation(OperationTable).rows.find(row =>
      MySqlModeOperationEffectCodec.value(row, OperationTable, "step") == MySqlFormat3Cell.Text("DEPLOYMENT_FINALIZE")).get
    val tag = IoOperationTag(MySqlScriptSchemaDataTestFixture.Scope.bootUuid, 1L, 1L, id(9002))
    val registry = new MySqlModeProjectionBudgetRegistry(Thread.currentThread(), tag.bootId, tag.bootGeneration, tag.connectionGeneration)
    registry.retainRoot(tag, root)
    var primary: Throwable = null
    var retained: MySqlRecoveryOperationRawEnvelope = null
    try
      assertThrows(classOf[IllegalStateException], () => MySqlRecoveryOperationRawEnvelopeCodec.encode(original, profile, inputs))
      def verifyRows(): Unit = MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        assertSame(inputs, MySqlRecoveryOperationRawEnvelopeCodec.inputsForImage(image, root).get)
        assertThrows(classOf[IllegalArgumentException], () => MySqlRecoveryOperationRawEnvelopeCodec.inputsForImage(image, foreignRoot))
        assertThrows(classOf[IllegalArgumentException], () => MySqlRecoveryOperationRawEnvelopeCodec.inputsForImage(image, closedRoot))
        assertThrows(classOf[SQLNonTransientConnectionException], () =>
          MySqlRecoveryOperationRawEnvelopeCodec.encode(original, profile, foreignInputs))
        val stepIndex = MySqlFormat3RelationShapes.forTable(OperationTable).columns.indexWhere(_.name == "step")
        val newRow = original.updated(stepIndex, MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallStep))
        Vector(original, newRow).foreach { row =>
          MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, profile, root, inputs)
          val encoded = MySqlRecoveryOperationRawEnvelopeCodec.encode(row, profile, inputs)
          if row eq original then retained = encoded
          val bytes = encoded.canonical.copyBytes(root, "Full27 RAW exact roundtrip")
          val decoded = MySqlRecoveryOperationRawEnvelopeCodec.decode(bytes, encoded.sha256, profile, inputs)
          assertEquals(row, decoded.row)
          row.zip(decoded.row).foreach { (before, after) => before match
            case MySqlFormat3Cell.Binary(value) => assertEquals(MySqlFormat3Cell.Binary(value), after)
            case _ => ()
          }
          assertTrue(MySqlRecoveryOperationRawEnvelopeCodec.matchesCanonical(row, profile, encoded.canonical, inputs))
          assertTrue(decoded.exactlyMatches(row, profile, Some(inputs)))
          val historical = MySqlRecoveryOperationRawEnvelopeCodec.encodeHistorical(decoded.row, profile, Vector(decoded.row), inputs)
          assertTrue(historical.canonical.exactlyEquals(encoded.canonical))
          val outputBytes = new ByteArrayOutputStream()
          val output = new DataOutputStream(outputBytes)
          MySqlRecoveryOperationRawEnvelopeCodec.writeRow(output, row, MySqlFormat3RelationShapes.forTable(OperationTable))
          output.flush()
          assertTrue(Arrays.equals(bytes, outputBytes.toByteArray))
          MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
            assertTrue(encoded.exactlyMatches(row, profile))
            assertThrows(classOf[IllegalStateException], () =>
              MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, profile, foreignRoot, inputs))
          }
        }
      }
      verifyRows()
      MySqlRecoveryCanonicalBytes.withAllocationBudget(foreignRoot) {
        assertThrows(classOf[IllegalArgumentException], () => retained.exactlyMatches(original, profile))
      }
      val legacy = MySqlScriptSchemaDataTestFixture.root(fixtureInputs)
      try MySqlRecoveryCanonicalBytes.withAllocationBudget(legacy) {
        val oldEncoded = MySqlRecoveryOperationRawEnvelopeCodec.encode(original, profile, fixtureInputs)
        val expectedBytes = new ByteArrayOutputStream()
        val output = new DataOutputStream(expectedBytes)
        MySqlRecoveryOperationRawEnvelopeCodec.writeRow(output, original, MySqlFormat3RelationShapes.forTable(OperationTable))
        output.flush()
        assertTrue(oldEncoded.canonical.borrowedBinary.equalsBytes(expectedBytes.toByteArray))
        assertTrue(retained.canonical.exactlyEquals(oldEncoded.canonical))
        val newRow = original.updated(MySqlFormat3RelationShapes.forTable(OperationTable).columns.indexWhere(_.name == "step"),
          MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallStep))
        assertThrows(classOf[SQLDataException], () => MySqlRecoveryOperationRawEnvelopeCodec.encode(newRow, profile, fixtureInputs))
        val newBytes = new ByteArrayOutputStream()
        val newOutput = new DataOutputStream(newBytes)
        MySqlRecoveryOperationRawEnvelopeCodec.writeRow(newOutput, newRow, MySqlFormat3RelationShapes.forTable(OperationTable))
        newOutput.flush()
        val rawBytes = newBytes.toByteArray
        assertThrows(classOf[SQLDataException], () => MySqlRecoveryOperationRawEnvelopeCodec.decode(rawBytes,
          MessageDigest.getInstance("SHA-256").digest(rawBytes).toVector, profile, fixtureInputs))
      }
      finally legacy.closeRootReadBudget()
      val first = root.newCanonicalScope()
      val sibling = root.newCanonicalScope()
      try MySqlRecoveryCanonicalBytes.withAllocationBudget(first) {
        assertThrows(classOf[IllegalStateException], () =>
          MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(original, profile, sibling, inputs))
      }
      finally MySqlFormat3BudgetCleanup.run(null, Vector(() => first.closeScopedReadBudget(), () => sibling.closeScopedReadBudget()))
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      retained = null
      MySqlFormat3BudgetCleanup.run(primary, Vector(() => registry.release(tag), () => foreignRoot.closeRootReadBudget()))

  @Test def full26RawEntriesKeepOneExplicitFamilyAndTheOriginalWire(): Unit =
    val inputs = MySqlScriptSchemaDataTestFixture.Full26Inputs
    val profile = inputs.sourceLimitProfile
    val root = MySqlScriptSchemaDataTestFixture.root(inputs)
    val rows = MySqlScriptSchemaDataTestFixture.full26OrdinaryRelations(inputs)
      .find(_.table == OperationTable).get.rows
    val register = rows.find(row => MySqlModeOperationEffectCodec.value(row, OperationTable, "step") ==
      MySqlFormat3Cell.Text("DEPLOYMENT_REGISTER")).get
    val observation = MySqlScriptSchemaDataTestFixture.Scope
    val tag = IoOperationTag(observation.bootUuid, observation.bootGeneration, observation.connectionGeneration, id(9001))
    val registry = new MySqlModeProjectionBudgetRegistry(Thread.currentThread(), tag.bootId,
      tag.bootGeneration, tag.connectionGeneration)
    registry.retainRoot(tag, root)
    var primary: Throwable = null
    try
      assertThrows(classOf[IllegalStateException], () => MySqlRecoveryOperationRawEnvelopeCodec.encode(register, profile, inputs))
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        val update = rows.find(row => MySqlModeOperationEffectCodec.value(row, OperationTable, "step") ==
          MySqlFormat3Cell.Text("DEPLOYMENT_FINALIZE")).get
        // CLEANUP 仅覆盖 RAW 单行契约；不从此派生完整 graph 或 authority 资格。
        val cleanup = update.updated(MySqlFormat3RelationShapes.forTable(OperationTable).columns.indexWhere(_.name == "step"),
          MySqlFormat3Cell.Text("DEPLOYMENT_CLEANUP"))
        (rows :+ cleanup).foreach { row =>
          MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, profile, root, inputs)
          val before = root.accountedMemoryBytes
          val encoded = MySqlRecoveryOperationRawEnvelopeCodec.encode(row, profile, inputs)
          assertEquals(before + encoded.length.toLong + 640L + 48L, root.accountedMemoryBytes,
            "Full26 envelope must account for its retained inputs metadata")
          encoded.requireInputs(Some(inputs))
          val bytes = encoded.canonical.copyBytes(root, "Full26 RAW direct roundtrip")
          val decoded = MySqlRecoveryOperationRawEnvelopeCodec.decode(bytes, encoded.sha256, profile, inputs)
          assertSame(inputs, decoded.deploymentInputs.get)
          assertEquals(row, decoded.row)
          assertTrue(MySqlRecoveryOperationRawEnvelopeCodec.matchesCanonical(row, profile, encoded.canonical, inputs))
          assertTrue(decoded.exactlyMatches(row, profile))
          val historical = MySqlRecoveryOperationRawEnvelopeCodec.encodeHistorical(decoded.row, profile, Vector(decoded.row), inputs)
          assertTrue(historical.canonical.exactlyEquals(encoded.canonical))
          assertSame(decoded.row, historical.row)
          // 直接调用原 writer，避免只用两个新入口互相证明 framing。
          val original = new ByteArrayOutputStream()
          val output = new DataOutputStream(original)
          MySqlRecoveryOperationRawEnvelopeCodec.writeRow(output, row, MySqlFormat3RelationShapes.forTable(OperationTable))
          output.flush()
          assertTrue(Arrays.equals(bytes, original.toByteArray))
          val step = MySqlModeOperationEffectCodec.value(row, OperationTable, "step")
          if step == MySqlFormat3Cell.Text("DEPLOYMENT_REGISTER") then
            assertThrows(classOf[SQLDataException], () => MySqlRecoveryOperationRawEnvelopeCodec.encode(row, profile))
            assertThrows(classOf[SQLDataException], () => MySqlRecoveryOperationRawEnvelopeCodec.decode(bytes, encoded.sha256, profile))
        }
      }
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      // Unit construction refs leave scope before registry exact drop retires borrows and closes the root.
      MySqlFormat3BudgetCleanup.run(primary, Vector(() => registry.release(tag)))

  @Test def full26RawRejectsWrongInputsNullDigestAndCrossFamilyReuse(): Unit =
    val inputs = MySqlScriptSchemaDataTestFixture.Full26Inputs
    val profile = inputs.sourceLimitProfile
    val root = MySqlScriptSchemaDataTestFixture.root(inputs)
    val foreignInputs = MySqlScriptSchemaInputs.createDeploymentReceiptFull26(inputs.schema, inputs.ddlSha256,
      profile, inputs.budget, inputs.deploymentBudget.get)
    val foreignRoot = MySqlScriptSchemaDataTestFixture.root(foreignInputs)
    val legacyImage = snapshot(profile, Vector.empty, id(10))
    val legacyRoot = legacyImage.allocationBudget
    val rows = MySqlScriptSchemaDataTestFixture.full26OrdinaryRelations(inputs)
      .find(_.table == OperationTable).get.rows
    val row = rows.find(row => MySqlModeOperationEffectCodec.value(row, OperationTable, "step") ==
      MySqlFormat3Cell.Text("DEPLOYMENT_REGISTER")).get
    def changed(name: String, value: MySqlFormat3Cell): Vector[MySqlFormat3Cell] =
      row.updated(MySqlFormat3RelationShapes.forTable(OperationTable).columns.indexWhere(_.name == name), value)
    var encoded: MySqlRecoveryOperationRawEnvelope = null
    var cache: MySqlModeRawEnvelopeConstruction = null
    try
      def actualImage(exact: MySqlScriptSchemaInputs, owner: MySqlFormat3ReadBudget): MySqlScriptSchemaSnapshot =
        MySqlScriptSchemaSnapshot.captureMaterialized(exact.schema, MySqlScriptSchemaDataTestFixture.Connection,
          MySqlScriptSchemaDataTestFixture.Scope, exact.budget.packetBytes, exact,
          MySqlScriptSchemaDataTestFixture.full26OrdinaryRelations(exact), owner)
      val actual = actualImage(inputs, root)
      val foreignActual = actualImage(foreignInputs, foreignRoot)
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        encoded = MySqlRecoveryOperationRawEnvelopeCodec.encode(row, profile, inputs)
        cache = new MySqlModeRawEnvelopeConstruction(root, 1)
        cache.initialize()
        cache.retain(encoded)
        assertSame(encoded, cache.findSameRow(row,
          MySqlRecoveryOperationRawEnvelopeCodec.inputsForImage(actual, root)).get)
        val legacyInputs = MySqlRecoveryOperationRawEnvelopeCodec.inputsForImage(legacyImage, legacyRoot)
        val foreignImageInputs = MySqlRecoveryOperationRawEnvelopeCodec.inputsForImage(foreignActual, foreignRoot)
        assertThrows(classOf[IllegalArgumentException], () => cache.findSameRow(row, legacyInputs))
        assertThrows(classOf[IllegalArgumentException], () => cache.findSameRow(row, foreignImageInputs))
        assertThrows(classOf[IllegalArgumentException], () => encoded.exactlyMatches(row, profile, legacyInputs))
        assertThrows(classOf[IllegalArgumentException], () => encoded.exactlyMatches(row, profile, foreignImageInputs))
        assertThrows(classOf[IllegalArgumentException], () => encoded.exactlyMatches(row, profile, None))
        assertThrows(classOf[IllegalArgumentException], () => encoded.exactlyMatches(row, profile, Some(foreignInputs)))
        assertThrows(classOf[SQLNonTransientConnectionException], () => MySqlRecoveryOperationRawEnvelopeCodec.encode(row, profile, foreignInputs))
        assertThrows(classOf[IllegalArgumentException], () => MySqlRecoveryOperationRawEnvelopeCodec.encode(row, profile, null: MySqlScriptSchemaInputs))
        assertThrows(classOf[SQLDataException], () => MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(
          changed("preimage_bytes", MySqlFormat3Cell.SqlNull), profile, root, inputs))
        assertThrows(classOf[SQLDataException], () => MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(
          changed("preimage_sha256", MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](32)(7)))), profile, root, inputs))
        assertThrows(classOf[SQLDataException], () => MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(
          changed("preimage_bytes", MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array[Byte](1)))), profile, root, inputs))
        assertThrows(classOf[SQLDataException], () => MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(
          changed("expected_domain_version", MySqlFormat3Cell.SignedInteger(1L)), profile, root, inputs))
        // REGISTER 的同一 empty+SHA(empty) pair 不可通过其他 schema5 step。
        assertThrows(classOf[SQLDataException], () => MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(
          changed("step", MySqlFormat3Cell.Text("DEPLOYMENT_START")), profile, root, inputs))
        assertThrows(classOf[SQLDataException], () => MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(
          changed("step", MySqlFormat3Cell.Text("DEPLOYMENT_UNKNOWN")), profile, root, inputs))
        assertThrows(classOf[SQLDataException], () => MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(
          changed("previous_step_operation_uuid", MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(new Array[Byte](16)))), profile, root, inputs))
      }
      MySqlRecoveryCanonicalBytes.withAllocationBudget(legacyRoot) {
        assertThrows(classOf[SQLNonTransientConnectionException], () => encoded.exactlyMatches(row, profile))
      }
      MySqlRecoveryCanonicalBytes.withAllocationBudget(foreignRoot) {
        assertThrows(classOf[SQLNonTransientConnectionException], () => encoded.exactlyMatches(row, profile))
      }
    finally
      MySqlFormat3BudgetCleanup.run(null, Vector(
        () => if cache != null then cache.dropReferences(),
        () => root.closeRootReadBudget(), () => foreignRoot.closeRootReadBudget(), () => legacyRoot.closeRootReadBudget()))

  @Test
  def rawOperationEnvelopeRoundTripsBytesAndRejectsTamperedOrMismatchedFields(): Unit =
    val profile = testProfile()
    val fixture = identities(profile)
    val row = operationRow(
      operationUuid = fixture.executeUuid,
      bindingUuid = fixture.binding.bindingUuid,
      podUuid = fixture.podUuid,
      attemptUuid = fixture.attemptUuid,
      footprintUuid = id(41),
      step = "DOMAIN_MUTATION",
      state = "COMMITTED"
    )
    val encoded = MySqlRecoveryOperationRawEnvelopeCodec.encode(row, profile)
    val budget = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    val bytes = encoded.canonical.copyBytes(budget, "mode O raw roundtrip test")
    try
      val decoded = MySqlRecoveryOperationRawEnvelopeCodec.decode(bytes, encoded.sha256, profile)
      assertEquals(encoded.operationUuid, decoded.operationUuid)
      assertTrue(encoded.exactlyMatches(decoded.row, profile))

      val tampered = bytes.clone()
      try
        tampered(tampered.length - 1) = (tampered(tampered.length - 1) ^ 1).toByte
        assertThrows(classOf[SQLDataException], () => {
          MySqlRecoveryOperationRawEnvelopeCodec.decode(tampered, encoded.sha256, profile)
          ()
        })
      finally Arrays.fill(tampered, 0.toByte)

      val brokenHashPair = put(row, OperationTable, "target_bytes",
        bytesCell("changed-target".getBytes(java.nio.charset.StandardCharsets.US_ASCII)))
      assertThrows(classOf[SQLDataException], () => {
        MySqlRecoveryOperationRawEnvelopeCodec.encode(brokenHashPair, profile)
        ()
      })
    finally
      Arrays.fill(bytes, 0.toByte)
      budget.releaseRetainedMemory(Math.addExact(encoded.length.toLong, 64L), "mode O raw roundtrip test release")

  @Test
  def executeProjectionRetainsHistoricalClosureOBytesButExcludesUnrelatedPodRows(): Unit =
    val fixture = identities(testProfile())
    val reserve = operationRow(fixture.reserveUuid, fixture.binding.bindingUuid, fixture.podUuid, fixture.attemptUuid,
      id(41), "RESERVE", "COMMITTED")
    val historical = operationRow(id(31), fixture.binding.bindingUuid, fixture.podUuid, fixture.attemptUuid,
      id(41), "DOMAIN_MUTATION", "COMMITTED")
    val current = operationRow(fixture.executeUuid, fixture.binding.bindingUuid, fixture.podUuid, fixture.attemptUuid,
      id(41), "DOMAIN_MUTATION", "RESERVED")
    val outside = operationRow(id(32), fixture.binding.bindingUuid, id(51), id(52),
      id(53), "RECALL_CHECKPOINT", "COMMITTED", None)
    val original = snapshot(fixture.profile, Vector(reserve, current, historical, outside), fixture.binding.bindingUuid)
    val originalProjection = project(original, fixture, MySqlOperationEffectImage.Before)

    val changedHistory = withDiagnosticIdentity(historical,
      "opaque historical O bytes".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    val historySnapshot = snapshot(fixture.profile, Vector(reserve, current, changedHistory, outside), fixture.binding.bindingUuid)
    val historyProjection = project(historySnapshot, fixture, MySqlOperationEffectImage.Before)
    assertFalse(originalProjection.exactlyEquals(historyProjection),
      "a historical O row in this pod/attempt closure must remain raw in the projection")

    val missingHistory = snapshot(fixture.profile, Vector(reserve, current, outside), fixture.binding.bindingUuid)
    assertFalse(originalProjection.exactlyEquals(project(missingHistory, fixture, MySqlOperationEffectImage.Before)),
      "a missing historical O in the fixed closure changes its exact projection")

    val addedHistory = operationRow(id(30), fixture.binding.bindingUuid, fixture.podUuid, fixture.attemptUuid,
      id(41), "DOMAIN_MUTATION", "COMMITTED")
    val extraHistory = snapshot(fixture.profile, Vector(reserve, current, addedHistory, historical, outside),
      fixture.binding.bindingUuid)
    assertFalse(originalProjection.exactlyEquals(project(extraHistory, fixture, MySqlOperationEffectImage.Before)),
      "an added historical O in the fixed closure changes its exact projection")

    val changedOutside = withOpaqueTarget(outside, "other-pod-history".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    val outsideSnapshot = snapshot(fixture.profile, Vector(reserve, current, historical, changedOutside), fixture.binding.bindingUuid)
    val outsideProjection = project(outsideSnapshot, fixture, MySqlOperationEffectImage.Before)
    assertTrue(originalProjection.exactlyEquals(outsideProjection),
      "operation rows outside the fixed pod closure are guarded by full20 exact comparison")
    assertFalse(original.exactlyEquals(outsideSnapshot),
      "the independent complete snapshot still observes an outside-closure O mutation")

  @Test
  def reserveTargetAndExecuteBeforeMaskOnlyTheirOwnDeclaredCycles(): Unit =
    val fixture = identities(testProfile())
    val reserve = operationRow(fixture.reserveUuid, fixture.binding.bindingUuid, fixture.podUuid, fixture.attemptUuid,
      id(41), "RESERVE", "COMMITTED")
    val executeReserved = operationRow(fixture.executeUuid, fixture.binding.bindingUuid, fixture.podUuid,
      fixture.attemptUuid, id(41), "DOMAIN_MUTATION", "RESERVED")
    val historical = operationRow(id(31), fixture.binding.bindingUuid, fixture.podUuid, fixture.attemptUuid,
      id(41), "DOMAIN_MUTATION", "COMMITTED")
    val baseline = snapshot(fixture.profile, Vector(reserve, executeReserved, historical), fixture.binding.bindingUuid)
    val reserveTarget = project(baseline, fixture, MySqlOperationEffectImage.Target, MySqlOperationEffectPhase.Reserve)
    val executeBefore = project(baseline, fixture, MySqlOperationEffectImage.Before, MySqlOperationEffectPhase.Execute)

    val reserveMarkers = Vector(
      "reserve context" -> withOpaqueContext(reserve, "reserve-context-change".getBytes(java.nio.charset.StandardCharsets.US_ASCII)),
      "reserve target" -> withOpaqueTarget(reserve, "reserve-target-change".getBytes(java.nio.charset.StandardCharsets.US_ASCII)),
      "reserve fingerprint" -> put(reserve, OperationTable, "operation_fingerprint", bytesCell(bytes(32, 4))),
      "reserve witness" -> withCommitWitness(reserve, "reserve-witness-change".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    )
    reserveMarkers.foreach { case (label, changedReserve) =>
      val changed = snapshot(fixture.profile, Vector(changedReserve, executeReserved, historical), fixture.binding.bindingUuid)
      assertTrue(reserveTarget.exactlyEquals(project(changed, fixture, MySqlOperationEffectImage.Target,
        MySqlOperationEffectPhase.Reserve)), label)
      assertFalse(baseline.exactlyEquals(changed), label + " remains different in full20")
    }

    val reservePreimageChanged = withOpaquePreimage(reserve,
      "reserve preimage remains raw".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    val reservePreimageSnapshot = snapshot(fixture.profile,
      Vector(reservePreimageChanged, executeReserved, historical), fixture.binding.bindingUuid)
    assertFalse(reserveTarget.exactlyEquals(project(reservePreimageSnapshot, fixture,
      MySqlOperationEffectImage.Target, MySqlOperationEffectPhase.Reserve)),
      "reserveO preimage is retained raw")

    val changedReserveResult = put(reserve, OperationTable, "result_at",
      MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 8, 18, 20, 0, 123457000)))
    val resultSnapshot = snapshot(fixture.profile, Vector(changedReserveResult, executeReserved, historical),
      fixture.binding.bindingUuid)
    assertFalse(reserveTarget.exactlyEquals(project(resultSnapshot, fixture,
      MySqlOperationEffectImage.Target, MySqlOperationEffectPhase.Reserve)),
      "the fixed reserve terminal result time remains raw")

    val changedPlannedAt = LocalDateTime.of(2026, 10, 8, 17, 45, 0, 123456000)
    val reserveWithNewPlanTime = put(reserve, OperationTable, "planned_at", MySqlFormat3Cell.DateTime(changedPlannedAt))
    val executeWithSamePlanTime = put(executeReserved, OperationTable, "planned_at", MySqlFormat3Cell.DateTime(changedPlannedAt))
    val planTimeSnapshot = snapshot(fixture.profile, Vector(reserveWithNewPlanTime, executeWithSamePlanTime, historical),
      fixture.binding.bindingUuid)
    assertFalse(reserveTarget.exactlyEquals(project(planTimeSnapshot, fixture,
      MySqlOperationEffectImage.Target, MySqlOperationEffectPhase.Reserve)),
      "the shared fixed planning time remains raw even when both O rows agree")

    val executeMarkers = Vector(
      "execute context" -> withOpaqueContext(executeReserved,
        "execute-context-change".getBytes(java.nio.charset.StandardCharsets.US_ASCII)),
      "execute target" -> withOpaqueTarget(executeReserved,
        "execute-target-change".getBytes(java.nio.charset.StandardCharsets.US_ASCII)),
      "execute fingerprint" -> put(executeReserved, OperationTable, "operation_fingerprint", bytesCell(bytes(32, 5))),
      "execute preimage" -> withOpaquePreimage(executeReserved,
        "execute-preimage-change".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    )
    executeMarkers.foreach { case (label, changedExecute) =>
      val changed = snapshot(fixture.profile, Vector(reserve, changedExecute, historical), fixture.binding.bindingUuid)
      assertTrue(executeBefore.exactlyEquals(project(changed, fixture, MySqlOperationEffectImage.Before,
        MySqlOperationEffectPhase.Execute)), label)
      assertFalse(baseline.exactlyEquals(changed), label + " remains different in full20")
    }

    val executeRequestChanged = putBytesPair(executeReserved, "request_identity_bytes", "request_identity_sha256",
      "request remains raw".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    val requestSnapshot = snapshot(fixture.profile, Vector(reserve, executeRequestChanged, historical), fixture.binding.bindingUuid)
    assertFalse(executeBefore.exactlyEquals(project(requestSnapshot, fixture,
      MySqlOperationEffectImage.Before, MySqlOperationEffectPhase.Execute)),
      "execute request identity is never an excluded self-reference")
    assertEquals(MySqlFormat3Cell.SqlNull,
      cell(executeReserved, OperationTable, "commit_witness_bytes"))
    assertEquals(MySqlFormat3Cell.SqlNull,
      cell(executeReserved, OperationTable, "commit_witness_sha256"))
    assertEquals(MySqlFormat3Cell.SqlNull, cell(executeReserved, OperationTable, "result_at"))
    assertEquals(MySqlFormat3Cell.SqlNull,
      cell(executeReserved, OperationTable, "settled_by_operation_uuid"))

  @Test
  def reservationResourceKeyRevisionAndPresenceRemainRawInTheProjection(): Unit =
    val fixture = identities(testProfile())
    val reserve = operationRow(fixture.reserveUuid, fixture.binding.bindingUuid, fixture.podUuid, fixture.attemptUuid,
      id(41), "RESERVE", "COMMITTED")
    val execute = operationRow(fixture.executeUuid, fixture.binding.bindingUuid, fixture.podUuid, fixture.attemptUuid,
      id(41), "DOMAIN_MUTATION", "RESERVED")
    val reservation = reservationRow(fixture.profile, fixture.binding.bindingUuid, fixture.podUuid,
      fixture.plan.workflowUuid, fixture.executeUuid, fixture.reserveUuid, 0L, "HELD")
    val baseline = snapshot(fixture.profile, Vector(reserve, execute), fixture.binding.bindingUuid,
      reservationRows = Vector(reservation))
    val projection = project(baseline, fixture, MySqlOperationEffectImage.Target,
      MySqlOperationEffectPhase.Reserve)

    val changedRevision = put(reservation, "fp_recovery_reservation", "reservation_revision",
      MySqlFormat3Cell.SignedInteger(1L))
    val revisionSnapshot = snapshot(fixture.profile, Vector(reserve, execute), fixture.binding.bindingUuid,
      reservationRows = Vector(changedRevision))
    assertFalse(projection.exactlyEquals(project(revisionSnapshot, fixture, MySqlOperationEffectImage.Target,
      MySqlOperationEffectPhase.Reserve)), "reservation revision is not a self-reference")

    val changedKey = put(reservation, "fp_recovery_reservation", "resource_key",
      bytesCell("different complete resource key bytes".getBytes(java.nio.charset.StandardCharsets.US_ASCII)))
    val keySnapshot = snapshot(fixture.profile, Vector(reserve, execute), fixture.binding.bindingUuid,
      reservationRows = Vector(changedKey))
    assertFalse(projection.exactlyEquals(project(keySnapshot, fixture, MySqlOperationEffectImage.Target,
      MySqlOperationEffectPhase.Reserve)), "the complete resource key bytes are included raw")

    val missingReservation = snapshot(fixture.profile, Vector(reserve, execute), fixture.binding.bindingUuid)
    assertFalse(projection.exactlyEquals(project(missingReservation, fixture, MySqlOperationEffectImage.Target,
      MySqlOperationEffectPhase.Reserve)), "missing closure R is visible in the projected image")

  @Test
  def executeProjectionMasksOnlyDeclaredSelfReferencesAndWriterFingerprint(): Unit =
    val fixture = identities(testProfile())
    val reserve = operationRow(fixture.reserveUuid, fixture.binding.bindingUuid, fixture.podUuid, fixture.attemptUuid,
      id(41), "RESERVE", "COMMITTED")
    val historical = operationRow(id(31), fixture.binding.bindingUuid, fixture.podUuid, fixture.attemptUuid,
      id(41), "DOMAIN_MUTATION", "COMMITTED")
    val current = operationRow(fixture.executeUuid, fixture.binding.bindingUuid, fixture.podUuid, fixture.attemptUuid,
      id(41), "DOMAIN_MUTATION", "COMMITTED")
    val base = snapshot(fixture.profile, Vector(reserve, current, historical), fixture.binding.bindingUuid)
    val targetProjection = project(base, fixture, MySqlOperationEffectImage.Target)

    val changedCurrentContext = withOpaqueContext(current,
      "different context placeholder".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    val contextChanged = snapshot(fixture.profile, Vector(reserve, changedCurrentContext, historical), fixture.binding.bindingUuid)
    assertTrue(targetProjection.exactlyEquals(project(contextChanged, fixture, MySqlOperationEffectImage.Target)),
      "the execute self-reference fields are typed markers in its projection")
    assertFalse(base.exactlyEquals(contextChanged),
      "projection markers never assert raw O bytes are already equal")

    val markerVariants = Vector(
      "target bytes" -> withOpaqueTarget(current, "different target".getBytes(java.nio.charset.StandardCharsets.US_ASCII)),
      "preimage bytes" -> withOpaquePreimage(current, "different preimage".getBytes(java.nio.charset.StandardCharsets.US_ASCII)),
      "commit witness" -> withCommitWitness(current, "different planned witness".getBytes(java.nio.charset.StandardCharsets.US_ASCII)),
      "operation fingerprint" -> put(current, OperationTable, "operation_fingerprint", bytesCell(bytes(32, 3)))
    )
    markerVariants.foreach { case (label, changedRow) =>
      val changed = snapshot(fixture.profile, Vector(reserve, changedRow, historical), fixture.binding.bindingUuid)
      assertTrue(targetProjection.exactlyEquals(project(changed, fixture, MySqlOperationEffectImage.Target)), label)
      assertFalse(base.exactlyEquals(changed), label + " remains different in the raw full20 snapshot")
    }

    val changedRequest = putBytesPair(current, "request_identity_bytes", "request_identity_sha256",
      "different request".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    val requestChanged = snapshot(fixture.profile, Vector(reserve, changedRequest, historical), fixture.binding.bindingUuid)
    assertFalse(targetProjection.exactlyEquals(project(requestChanged, fixture, MySqlOperationEffectImage.Target)),
      "request identity stays inside the projection")
    val changedScope = putBytesPair(current, "scope_bytes", "scope_sha256",
      "different scope".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    val scopeChanged = snapshot(fixture.profile, Vector(reserve, changedScope, historical), fixture.binding.bindingUuid)
    assertFalse(targetProjection.exactlyEquals(project(scopeChanged, fixture, MySqlOperationEffectImage.Target)),
      "scope stays inside the projection")
    val changedDiagnostic = withDiagnosticIdentity(current,
      "diagnostic is original state".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    val diagnosticChanged = snapshot(fixture.profile, Vector(reserve, changedDiagnostic, historical), fixture.binding.bindingUuid)
    assertFalse(targetProjection.exactlyEquals(project(diagnosticChanged, fixture, MySqlOperationEffectImage.Target)),
      "diagnostic identity stays inside the projection")

    val changedResultTime = put(current, OperationTable, "result_at",
      MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 8, 18, 5, 6, 123457000)))
    val timeChanged = snapshot(fixture.profile, Vector(reserve, changedResultTime, historical), fixture.binding.bindingUuid)
    assertFalse(targetProjection.exactlyEquals(project(timeChanged, fixture, MySqlOperationEffectImage.Target)),
      "result_at is fixed raw state, not an excluded self-reference")
    val changedPlannedTime = put(current, OperationTable, "planned_at",
      MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 8, 17, 30, 0, 123456000)))
    val plannedTimeChanged = snapshot(fixture.profile, Vector(reserve, changedPlannedTime, historical), fixture.binding.bindingUuid)
    assertFalse(targetProjection.exactlyEquals(project(plannedTimeChanged, fixture, MySqlOperationEffectImage.Target)),
      "planned_at remains the fixed raw operation time")

    val baseWriter = snapshot(fixture.profile, Vector(reserve, current, historical), fixture.binding.bindingUuid,
      writerOperation = fixture.reserveUuid, writerFingerprint = bytes(32, 1))
    val newWriterFingerprint = snapshot(fixture.profile, Vector(reserve, current, historical), fixture.binding.bindingUuid,
      writerOperation = fixture.reserveUuid, writerFingerprint = bytes(32, 2))
    assertTrue(project(baseWriter, fixture, MySqlOperationEffectImage.Target)
      .exactlyEquals(project(newWriterFingerprint, fixture, MySqlOperationEffectImage.Target)),
      "only the target W fingerprint is a derived marker")
    val newWriterOperation = snapshot(fixture.profile, Vector(reserve, current, historical), fixture.binding.bindingUuid,
      writerOperation = fixture.executeUuid, writerFingerprint = bytes(32, 1))
    assertFalse(project(baseWriter, fixture, MySqlOperationEffectImage.Target)
      .exactlyEquals(project(newWriterOperation, fixture, MySqlOperationEffectImage.Target)),
      "the W operation UUID remains raw and fixed in the projection")

    val originalWriter = writerRow(fixture.binding.bindingUuid, fixture.reserveUuid, bytes(32, 1))
    val changedLease = put(originalWriter, WriterTable, "lease_until",
      MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 8, 18, 30, 0, 123456000)))
    val leaseSnapshot = snapshot(fixture.profile, Vector(reserve, current, historical), fixture.binding.bindingUuid,
      writerRowOverride = changedLease)
    assertFalse(project(baseWriter, fixture, MySqlOperationEffectImage.Target)
      .exactlyEquals(project(leaseSnapshot, fixture, MySqlOperationEffectImage.Target)),
      "the fixed W lease remains raw in the target projection")

  @Test
  def resultAndWitnessNullTopologyRemainDistinctRawOperationStates(): Unit =
    val profile = testProfile()
    val committed = operationRow(id(61), id(62), id(63), id(64), id(65), "DOMAIN_MUTATION", "COMMITTED")
    val reserved = operationRow(id(66), id(62), id(63), id(64), id(65), "DOMAIN_MUTATION", "RESERVED")
    val committedEnvelope = MySqlRecoveryOperationRawEnvelopeCodec.encode(committed, profile)
    val reservedEnvelope = MySqlRecoveryOperationRawEnvelopeCodec.encode(reserved, profile)
    assertTrue(committedEnvelope.length > reservedEnvelope.length)
    assertFalse(committedEnvelope.sha256 == reservedEnvelope.sha256,
      "COMMITTED witness/result bytes and RESERVED SQL NULL cells remain distinct raw O states")

  @Test
  def buildWorkflowAcceptsTheExactTwoOperationReservationAndModeHistory(): Unit =
    val images = modeWorkflowImages()
    val effects = MySqlModeOperationEffectCodec.buildWorkflow(
      images.fixture.plan, images.fixture.operation, images.fixture.context, 0L,
      images.before, images.reserveTarget, images.executeTarget)

    assertEquals(Vector(MySqlOperationEffectPhase.Reserve, MySqlOperationEffectPhase.Execute),
      effects.map(_.phase))
    val expectedOperationIds = Vector(images.fixture.reserveUuid, images.fixture.executeUuid, id(71))
    assertEquals(expectedOperationIds, effects.head.rawOperationEnvelopes.map(_.operationUuid))
    assertEquals(expectedOperationIds, effects(1).preimageRawOperationEnvelopes.map(_.operationUuid))
    assertEquals(expectedOperationIds, effects(1).rawOperationEnvelopes.map(_.operationUuid))
    effects.head.verifyPreimage(images.before)
    effects.head.verifyReadback(images.reserveTarget)
    effects(1).verifyPreimage(images.reserveTarget)
    effects(1).verifyReadback(images.executeTarget)

  @Test
  def buildWorkflowRejectsMissingExecuteOperationAndMissingReservationRows(): Unit =
    val images = modeWorkflowImages()
    val missingExecute = relationRows(images.reserveTarget, OperationTable)
      .filter(row => MySqlModeOperationEffectCodec.rowOperationUuid(row) != images.fixture.executeUuid)
    val missingExecuteSnapshot = replaceRelation(images.reserveTarget, OperationTable, missingExecute)
    assertThrows(classOf[SQLDataException], () => {
      MySqlModeOperationEffectCodec.buildWorkflow(images.fixture.plan, images.fixture.operation,
        images.fixture.context, 0L, images.before, missingExecuteSnapshot, images.executeTarget)
      ()
    })

    val missingReservationSnapshot = replaceRelation(images.executeTarget, "fp_recovery_reservation", Vector.empty)
    assertThrows(classOf[SQLDataException], () => {
      MySqlModeOperationEffectCodec.buildWorkflow(images.fixture.plan, images.fixture.operation,
        images.fixture.context, 0L, images.before, images.reserveTarget, missingReservationSnapshot)
      ()
    })

  @Test
  def fixedPlanningTimestampMaterializesBothModePhasesAcrossAllTwentyRelations(): Unit =
    val images = modeWorkflowImages()
    val result = MySqlModeWorkflowMaterializer.materialize(images.fixture.plan, images.fixture.operation,
      images.fixture.context, workflowTime, images.before, 0L)

    result.verify()
    assertEquals(workflowTime, result.plannedAt)
    assertEquals(20, result.preimage.relations.size)
    assertEquals(20, result.reserveTarget.relations.size)
    assertEquals(20, result.executeTarget.relations.size)
    assertEquals(Vector(MySqlOperationEffectPhase.Reserve, MySqlOperationEffectPhase.Execute),
      result.effects.map(_.phase))

    val allowedChangedRelations = Set(
      "fp_runtime_writer", "fp_recovery_operation", "fp_recovery_reservation",
      "floating_pod_state", "physical_recovery_locator"
    )
    images.before.relations.filterNot(relation => allowedChangedRelations.contains(relation.table)).foreach { original =>
      assertEquals(original.rows, result.executeTarget.relation(original.table).rows,
        "mode planning preserves the complete unrelated history in " + original.table)
    }

    val originalDomain = images.before.relation("floating_pod_state").rows.head
    val targetDomain = result.executeTarget.relation("floating_pod_state").rows.head
    assertEquals(cell(originalDomain, "floating_pod_state", "owner_uuid"),
      cell(targetDomain, "floating_pod_state", "owner_uuid"))
    assertEquals(cell(originalDomain, "floating_pod_state", "is_active"),
      cell(targetDomain, "floating_pod_state", "is_active"))
    assertEquals(cell(originalDomain, "floating_pod_state", "dimension_id"),
      cell(targetDomain, "floating_pod_state", "dimension_id"))
    assertEquals(MySqlFormat3Cell.SignedInteger(1L), cell(targetDomain, "floating_pod_state", "state_version"))
    assertEquals(MySqlFormat3Cell.Text(PodOperatingMode.WorldIdle.databaseValue),
      cell(targetDomain, "floating_pod_state", "operating_mode"))
    assertEquals(MySqlFormat3Cell.SignedInteger(workflowTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli),
      cell(targetDomain, "floating_pod_state", "last_updated"))

    val operationRows = result.executeTarget.relation(OperationTable).rows
    val reserve = operationRows.find(MySqlModeOperationEffectCodec.rowOperationUuid(_) == images.fixture.reserveUuid)
      .getOrElse(throw new AssertionError("reserve O was not materialized"))
    val execute = operationRows.find(MySqlModeOperationEffectCodec.rowOperationUuid(_) == images.fixture.executeUuid)
      .getOrElse(throw new AssertionError("execute O was not materialized"))
    assertEquals(result.effects.head.preimage.binaryCell, cell(reserve, OperationTable, "preimage_bytes"),
      "reserve O retains the exact reserve phase preimage before its target was projected")
    assertEquals(result.effects.head.preimage.sha256Cell, cell(reserve, OperationTable, "preimage_sha256"),
      "reserve O retains the SHA-256 of that exact preimage")
    assertEquals(uuidCell(id(41)), cell(reserve, OperationTable, "footprint_uuid"),
      "new operation rows retain the locked footprint as a typed 16-byte UUID cell")
    assertEquals(MySqlFormat3Cell.DateTime(workflowTime), cell(reserve, OperationTable, "planned_at"))
    assertEquals(MySqlFormat3Cell.DateTime(workflowTime), cell(reserve, OperationTable, "result_at"))
    assertTrue(cell(reserve, OperationTable, "commit_witness_bytes").isInstanceOf[MySqlFormat3Cell.Binary])
    assertEquals(MySqlFormat3Cell.DateTime(workflowTime), cell(execute, OperationTable, "planned_at"))
    assertEquals(MySqlFormat3Cell.DateTime(workflowTime), cell(execute, OperationTable, "result_at"))
    assertTrue(cell(execute, OperationTable, "commit_witness_bytes").isInstanceOf[MySqlFormat3Cell.Binary])
    val reservationRows = result.executeTarget.relation("fp_recovery_reservation").rows
    assertEquals(1, reservationRows.size, "the exact POD reservation remains in full20 history")
    assertEquals(MySqlFormat3Cell.Text("RELEASED"), cell(reservationRows.head,
      "fp_recovery_reservation", "reservation_state"))
    assertEquals(MySqlFormat3Cell.SignedInteger(1L), cell(reservationRows.head,
      "fp_recovery_reservation", "reservation_revision"))
    assertEquals(uuidCell(images.fixture.executeUuid), cell(reservationRows.head,
      "fp_recovery_reservation", "operation_uuid"))

    val reserved = result.reserveTarget.relation(OperationTable).rows
      .find(MySqlModeOperationEffectCodec.rowOperationUuid(_) == images.fixture.executeUuid)
      .getOrElse(throw new AssertionError("reserved execute O was not materialized"))
    assertEquals(MySqlFormat3Cell.Text("RESERVED"), cell(reserved, OperationTable, "operation_state"))
    assertEquals(MySqlFormat3Cell.SqlNull, cell(reserved, OperationTable, "result_at"))
    assertEquals(MySqlFormat3Cell.SqlNull, cell(reserved, OperationTable, "commit_witness_bytes"))
    assertEquals(MySqlFormat3Cell.SqlNull, cell(reserved, OperationTable, "commit_witness_sha256"))

  @Test
  def modeMaterializationIsDeterministicAndRejectsAChangedWitnessEvenWithItsNewDigest(): Unit =
    val firstImages = modeWorkflowImages()
    val first = MySqlModeWorkflowMaterializer.materialize(firstImages.fixture.plan, firstImages.fixture.operation,
      firstImages.fixture.context, workflowTime, firstImages.before, 0L)
    val replayImages = modeWorkflowImages()
    val replay = MySqlModeWorkflowMaterializer.materialize(replayImages.fixture.plan, replayImages.fixture.operation,
      replayImages.fixture.context, workflowTime, replayImages.before, 0L)
    assertTrue(first.executeTarget.exactlyEquals(replay.executeTarget),
      "the fixed Main identities, full20 preimage and explicit planning timestamp produce one immutable plan")

    val laterImages = modeWorkflowImages()
    val laterTime = workflowTime.plusNanos(1000)
    val later = MySqlModeWorkflowMaterializer.materialize(laterImages.fixture.plan, laterImages.fixture.operation,
      laterImages.fixture.context, laterTime, laterImages.before, 0L)
    assertFalse(first.executeTarget.exactlyEquals(later.executeTarget),
      "changing the fixed planning timestamp changes the exact operation materialization")

    val execute = first.executeTarget.relation(OperationTable).rows
      .find(MySqlModeOperationEffectCodec.rowOperationUuid(_) == firstImages.fixture.executeUuid)
      .getOrElse(throw new AssertionError("execute O was not materialized"))
    val changedExecute = withCommitWitness(execute, "different semantic witness".getBytes(StandardCharsets.US_ASCII))
    val tampered = replaceRelation(first.executeTarget, OperationTable,
      first.executeTarget.relation(OperationTable).rows.map(row =>
        if MySqlModeOperationEffectCodec.rowOperationUuid(row) == firstImages.fixture.executeUuid then changedExecute else row))
    assertThrows(classOf[SQLDataException], () => {
      first.effects(1).verifyReadback(tampered)
      ()
    })

  /** 完整实际codec/materializer逻辑使用typed DATA fixture；不签SQL、clock或admission。 */
  @Test def trackedDriftMaterializesFullTwentyAndKeepsOwnerModeAttemptAndHistory(): Unit =
    val images = modeWorkflowImages()
    val (plan, operation, context) = trackedDriftInputs(images)
    val root = images.before.allocationBudget
    val beforeCharge = root.accountedMemoryBytes
    println(s"STAGEC_DRIFT_ACTUAL_BUDGET before=$beforeCharge available=${root.availableMemoryBytes}")
    val result =
      try
        MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
          MySqlDriftWorkflowMaterializer.materialize(plan, operation, context, workflowTime, images.before, 0L)
        }
      finally println(s"STAGEC_DRIFT_ACTUAL_BUDGET after=${root.accountedMemoryBytes} available=${root.availableMemoryBytes}")
    result.verify()
    assertEquals(20, result.preimage.relations.size)
    assertEquals(20, result.reserveTarget.relations.size)
    assertEquals(20, result.executeTarget.relations.size)
    val original = images.before.relation("floating_pod_state").rows.head
    val durable = result.executeTarget.relation("floating_pod_state").rows.head
    for column <- Vector("owner_uuid", "operating_mode", "is_active", "dimension_id", "binding_uuid") do
      assertEquals(cell(original, "floating_pod_state", column), cell(durable, "floating_pod_state", column))
    assertEquals(MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(2D)), cell(durable, "floating_pod_state", "pos_x"))
    assertEquals(MySqlFormat3Cell.SignedInteger(1L), cell(durable, "floating_pod_state", "state_version"))
    val allowed = Set("floating_pod_state", "physical_recovery_locator", "fp_runtime_writer",
      "fp_recovery_operation", "fp_recovery_reservation")
    images.before.relations.filterNot(r => allowed.contains(r.table)).foreach(r =>
      assertEquals(r.rows, result.executeTarget.relation(r.table).rows))
    val originalHistory = images.before.relation(OperationTable).rows.find(row =>
      MySqlModeOperationEffectCodec.rowOperationUuid(row) == id(71)).get
    val targetHistory = result.executeTarget.relation(OperationTable).rows.find(row =>
      MySqlModeOperationEffectCodec.rowOperationUuid(row) == id(71)).get
    assertEquals(originalHistory, targetHistory)
    assertSame(originalHistory, targetHistory)
    images.before.relations.filterNot(r => allowed.contains(r.table)).foreach(r =>
      assertSame(r, result.reserveTarget.relation(r.table))
      assertSame(r, result.executeTarget.relation(r.table)))
    for column <- Vector("owner_uuid", "is_active", "dimension_id", "binding_uuid") do
      assertSame(cell(original, "floating_pod_state", column), cell(durable, "floating_pod_state", column))
    assertTrue(result.reserveTarget.allocationBudget eq root)
    assertTrue(result.executeTarget.allocationBudget eq root)
    assertTrue(root.accountedMemoryBytes > beforeCharge)
    assertFalse(root.isClosed)
    val fixedCharge = root.accountedMemoryBytes
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      result.verify()
      result.verify()
      result.effects.foreach(effect => effect.requireOperationContext(effect.exactOperationContext))
    }
    assertEquals(fixedCharge, root.accountedMemoryBytes)
    result.effects.head.rawOperationEnvelopes.foreach { envelope =>
      val same = result.effects(1).preimageRawOperationEnvelopes.find(_.operationUuid == envelope.operationUuid).get
      assertSame(envelope, same)
    }
    val originalEnvelope = result.effects.head.preimageRawOperationEnvelopes.find(_.operationUuid == id(71)).get
    assertSame(originalEnvelope, result.effects(1).rawOperationEnvelopes.find(_.operationUuid == id(71)).get)
    val plannedExecute = result.effects.head.rawOperationEnvelopes.find(_.operationUuid == plan.executeOperationUuid).get
    val committedExecute = result.effects(1).rawOperationEnvelopes.find(_.operationUuid == plan.executeOperationUuid).get
    assertNotSame(plannedExecute, committedExecute)
    assertFalse(plannedExecute.canonical.exactlyEquals(committedExecute.canonical))
    assertEquals(MySqlFormat3Cell.Text("RELEASED"), cell(result.executeTarget.relation("fp_recovery_reservation").rows.head,
      "fp_recovery_reservation", "reservation_state"))
    assertTrue(result.preimage.allocationBudget eq images.before.allocationBudget)

  @Test def trackedDriftRejectsRealFullGraphCriticalBaseBeforeMaterialization(): Unit =
    val images = modeWorkflowImages()
    val (_, operation, _) = trackedDriftInputs(images)
    assertEquals(MySqlDriftPreflight.Execute, MySqlDriftOperationEffectCodec.preflight(images.before, operation))
    val original = images.before.relation("floating_pod_state").rows.head
    for fieldValue <- Vector(
        "state_version" -> MySqlFormat3Cell.SignedInteger(1L),
        "last_updated" -> MySqlFormat3Cell.SignedInteger(101L),
        "pos_x" -> MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(-0.0D))) do
      val changed = replaceRelation(images.before, "floating_pod_state",
        Vector(put(original, "floating_pod_state", fieldValue._1, fieldValue._2)))
      assertTrue(MySqlDriftOperationEffectCodec.preflight(changed, operation)
        .isInstanceOf[MySqlDriftPreflight.Rejected])
    assertEquals(original, images.before.relation("floating_pod_state").rows.head)

  @Test def trackedDriftNearCapRejectsBeforeAnyMaterializedHistoryOrDefaultRowAllocation(): Unit =
    val images = modeWorkflowImages()
    val (plan, operation, context) = trackedDriftInputs(images)
    val root = images.before.allocationBudget
    val beforeRelations = images.before.relations
    // 真原root被其他同时reachable责任占用，只留下1byte；直接调用生产materializer入口。
    val held = root.availableMemoryBytes - 1L
    assertTrue(held > 0L)
    root.reserveRetainedMemory(held, "actual other retained root obligation")
    val charged = root.accountedMemoryBytes
    val original = assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        MySqlDriftWorkflowMaterializer.materialize(plan, operation, context, workflowTime, images.before, 0L)
      })
    assertTrue(original.getMessage.contains("drift materializer structures before allocation"))
    assertSame(beforeRelations, images.before.relations)
    assertEquals(charged, root.accountedMemoryBytes)
    assertFalse(root.isClosed)
    assertEquals(20, images.before.relations.size)

  /** 真production child-scope逻辑；纯DATA预算fixture，不代表JDBC/admission。 */
  @Test def trackedDriftComparisonScopeReleasesOnlyScratchAndPreservesOriginalFailure(): Unit =
    val images = modeWorkflowImages()
    val root = images.before.allocationBudget
    val charged = root.accountedMemoryBytes
    val originalRelations = images.before.relations
    val original = new IllegalStateException("actual scratch verification failure")
    val originalStack = original.getStackTrace
    val observed = assertThrows(classOf[IllegalStateException], () =>
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        val temporary = MySqlRecoveryCanonicalBytes.encode("FP_DRIFT_SCRATCH_TEST_BE_V1", images.before.limitProfile) { output =>
          output.writeInt(71)
          output.writeLong(991L)
        }
        assertTrue(temporary.length > 0)
        throw original
      })
    assertSame(original, observed)
    assertTrue(originalStack.sameElements(observed.getStackTrace))
    assertEquals(charged, root.accountedMemoryBytes)
    assertFalse(root.isClosed)
    assertSame(originalRelations, images.before.relations)
    val foreign = new MySqlFormat3ReadBudget(images.before.limitProfile, images.before.limitProfile.maxSqlPacketBytes())
    try
      val foreignCharge = foreign.accountedMemoryBytes
      assertThrows(classOf[IllegalStateException], () =>
        MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
          MySqlRecoveryCanonicalBytes.withComparisonBudget(foreign) { () }
        })
      assertEquals(foreignCharge, foreign.accountedMemoryBytes)
      assertEquals(charged, root.accountedMemoryBytes)
    finally foreign.closeRootReadBudget()

  @Test def exactRawEnvelopeUsesOneFinalPayloadWithinActualRemainingRootCapacity(): Unit =
    val images = modeWorkflowImages()
    val root = images.before.allocationBudget
    val row = images.before.relation(OperationTable).rows.head
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val first = MySqlRecoveryOperationRawEnvelopeCodec.encode(row, images.before.limitProfile)
      val charged = root.accountedMemoryBytes
      // 原 first/raw graph 都仍 reachable 并全额计费；只留下一个最终 payload + SHA 预算。
      val needed = first.length.toLong + 640L + 1024L
      val held = root.availableMemoryBytes - needed
      assertTrue(held > 0L)
      root.reserveRetainedMemory(held, "other exact-root live obligations")
      try
        val before = root.accountedMemoryBytes
        val second = MySqlRecoveryOperationRawEnvelopeCodec.encode(row, images.before.limitProfile)
        assertTrue(first.canonical.exactlyEquals(second.canonical))
        assertSame(row, second.row)
        assertEquals(before + first.length.toLong + 640L, root.accountedMemoryBytes)
        assertEquals(1024L, root.availableMemoryBytes)
        val full = root.accountedMemoryBytes
        assertThrows(classOf[SQLDataException], () => {
          MySqlRecoveryOperationRawEnvelopeCodec.encode(row, images.before.limitProfile)
          ()
        })
        assertEquals(full, root.accountedMemoryBytes)
      finally root.releaseRetainedMemory(held, "other exact-root obligation released")
      assertTrue(root.accountedMemoryBytes > charged)
    }

  @Test def exactCanonicalWriterIsSinglePassAndRejectsUnderOverrunWithoutLosingCause(): Unit =
    val images = modeWorkflowImages()
    val root = images.before.allocationBudget
    val profile = images.before.limitProfile
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val original = new IllegalStateException("exact writer original failure")
      val stack = original.getStackTrace
      val before = root.accountedMemoryBytes
      var calls = 0
      val failure = assertThrows(classOf[IllegalStateException], () =>
        MySqlRecoveryCanonicalBytes.encodeExact("FP_EXACT_RAW_TEST_BE_V1", profile, 8L) { output =>
          calls += 1
          output.writeInt(71)
          throw original
        })
      assertSame(original, failure)
      assertTrue(stack.sameElements(failure.getStackTrace))
      assertEquals(1, calls)
      assertEquals(before, root.accountedMemoryBytes)
      assertThrows(classOf[SQLDataException], () =>
        MySqlRecoveryCanonicalBytes.encodeExact("FP_EXACT_RAW_TEST_BE_V1", profile, 8L)(_.writeInt(71)))
      assertEquals(before, root.accountedMemoryBytes)
      assertThrows(classOf[SQLDataException], () =>
        MySqlRecoveryCanonicalBytes.encodeExact("FP_EXACT_RAW_TEST_BE_V1", profile, 3L)(_.writeInt(71)))
      assertEquals(before, root.accountedMemoryBytes)
      var rejectedCalls = 0
      assertThrows(classOf[SQLDataException], () =>
        MySqlRecoveryCanonicalBytes.encodeExact("FP_EXACT_RAW_TEST_BE_V1", profile,
          profile.maxSqlPacketBytes() + 1L) { _ => rejectedCalls += 1 })
      assertEquals(0, rejectedCalls)
      assertEquals(before, root.accountedMemoryBytes)
      val exact = MySqlRecoveryCanonicalBytes.encodeExact("FP_EXACT_RAW_TEST_BE_V1", profile, 12L) { output =>
        output.writeInt(71)
        output.writeLong(991L)
      }
      val reference = MySqlRecoveryCanonicalBytes.encode("FP_EXACT_RAW_TEST_BE_V1", profile) { output =>
        output.writeInt(71)
        output.writeLong(991L)
      }
      assertTrue(exact.exactlyEquals(reference))
    }

  @Test def rawComparisonDoesNotRetainBooleanOnlyCanonicals(): Unit =
    val images = modeWorkflowImages()
    val root = images.before.allocationBudget
    val profile = images.before.limitProfile
    val row = images.before.relation(OperationTable).rows.head
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val envelope = MySqlRecoveryOperationRawEnvelopeCodec.encode(row, profile)
      // 同长度/同encoding的不同完整bytes；不只验证SHA或输入row equality。
      val different = MySqlRecoveryCanonicalBytes.encodeExact(envelope.canonical.encoding, profile,
        envelope.length.toLong) { output =>
        var index = 0
        while index < envelope.length do
          output.writeByte(0)
          index += 1
      }
      val held = root.availableMemoryBytes - 2048L
      assertTrue(held > 0L)
      root.reserveRetainedMemory(held, "actual live projections during raw comparison")
      try
        val before = root.accountedMemoryBytes
        var index = 0
        while index < 3 do
          assertTrue(envelope.exactlyMatches(row, profile))
          assertFalse(MySqlRecoveryOperationRawEnvelopeCodec.matchesCanonical(row, profile, different))
          assertEquals(before, root.accountedMemoryBytes)
          index += 1
        assertSame(row, envelope.row)
        assertEquals(2048L, root.availableMemoryBytes)
      finally root.releaseRetainedMemory(held, "raw comparison other projection release")
    }

  @Test def rawUuidAndDigestValidationDoesNotAccumulateDeadCopies(): Unit =
    val images = modeWorkflowImages()
    val root = images.before.allocationBudget
    val profile = images.before.limitProfile
    val row = images.before.relation(OperationTable).rows.head
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      var index = 0
      while index < 3 do
        val before = root.accountedMemoryBytes
        val envelope = MySqlRecoveryOperationRawEnvelopeCodec.encode(row, profile)
        val sameUuid = cell(row, OperationTable, "operation_uuid") match
          case MySqlFormat3Cell.Binary(value) => value.equalsUuid(envelope.operationUuid)
          case _ => false
        assertTrue(sameUuid)
        // 只允许实际仍reachable的 finalpayload+digest debt；UUID/SHA验证副本不能永久记账。
        assertEquals(before + envelope.length.toLong + 640L, root.accountedMemoryBytes)
        index += 1
      val broken = put(row, OperationTable, "target_sha256", bytesCell(new Array[Byte](32)))
      val before = root.accountedMemoryBytes
      val observed = assertThrows(classOf[SQLDataException], () => {
        MySqlRecoveryOperationRawEnvelopeCodec.encode(broken, profile)
        ()
      })
      assertTrue(observed.getStackTrace.nonEmpty)
      assertEquals(before, root.accountedMemoryBytes)
      assertFalse(root.isClosed)
    }

  @Test def driftTailOperationUuidReadsDoNotRetainDiscardedCopies(): Unit =
    val images = modeWorkflowImages()
    val root = images.before.allocationBudget
    val row = images.before.relation(OperationTable).rows.head
    val exact = id(71)
    val signed = new UUID(Long.MinValue + 17L, -33L)
    val signedRow = put(row, OperationTable, "operation_uuid", uuidCell(signed))
    val invalidRow = put(row, OperationTable, "operation_uuid", bytesCell(new Array[Byte](15)))
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val before = root.accountedMemoryBytes
      var index = 0
      while index < 20 do
        assertEquals(exact, MySqlDriftOperationEffectCodec.rowOperationUuid(row))
        assertEquals(signed, MySqlDriftOperationEffectCodec.rowOperationUuid(signedRow))
        assertEquals(before, root.accountedMemoryBytes)
        index += 1
      val failure = assertThrows(classOf[SQLDataException], () => {
        MySqlDriftOperationEffectCodec.rowOperationUuid(invalidRow)
        ()
      })
      assertTrue(failure.getStackTrace.nonEmpty)
      assertEquals(before, root.accountedMemoryBytes)
      assertFalse(root.isClosed)
      assertSame(row, images.before.relation(OperationTable).rows.head)
    }

  @Test def rawRowValidationDoesNotAllocateHistoricalCanonicalAtLowRemainingCapacity(): Unit =
    val images = modeWorkflowImages()
    val root = images.before.allocationBudget
    val profile = images.before.limitProfile
    val row = images.before.relation(OperationTable).rows.head
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val heldEnvelope = MySqlRecoveryOperationRawEnvelopeCodec.encode(row, profile)
      assertTrue(heldEnvelope.length.toLong + 640L > 2048L)
      val held = root.availableMemoryBytes - 2048L
      assertTrue(held > 0L)
      root.reserveRetainedMemory(held, "actual historical projection still retained")
      try
        val before = root.accountedMemoryBytes
        MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, profile, root)
        assertEquals(before, root.accountedMemoryBytes)
        assertSame(row, heldEnvelope.row)
        assertThrows(classOf[SQLDataException], () => {
          MySqlRecoveryOperationRawEnvelopeCodec.encode(row, profile)
          ()
        })
        assertEquals(before, root.accountedMemoryBytes)
        MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, profile, root)
        assertEquals(before, root.accountedMemoryBytes)
      finally root.releaseRetainedMemory(held, "other historical projection release")
    }

  @Test def rawRowValidationPreservesStrictDataFailuresAndActualBudgetScope(): Unit =
    val images = modeWorkflowImages()
    val root = images.before.allocationBudget
    val profile = images.before.limitProfile
    val row = images.before.relation(OperationTable).rows.head
    val badRows = Vector(
      row.dropRight(1),
      put(row, OperationTable, "operation_uuid", MySqlFormat3Cell.SqlNull),
      put(row, OperationTable, "operation_uuid", bytesCell(new Array[Byte](15))),
      put(row, OperationTable, "target_sha256", bytesCell(new Array[Byte](32))),
      put(row, OperationTable, "commit_witness_bytes", MySqlFormat3Cell.SqlNull),
      put(row, OperationTable, "operation_state", MySqlFormat3Cell.Text("RESERVED")))
    val foreign = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    val closed = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    closed.closeRootReadBudget()
    val equivalentProfile = SourceLimitProfile.decode(profile.canonicalBytes())
    val child = root.newCanonicalScope()
    val sibling = root.newCanonicalScope()
    try
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        val before = root.accountedMemoryBytes
        badRows.foreach { invalid =>
          val failure = assertThrows(classOf[SQLDataException], () =>
            MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(invalid, profile, root))
          assertTrue(failure.getStackTrace.nonEmpty)
          assertTrue(failure.getCause == null, "the original responsible data exception is not wrapped")
          assertEquals(before, root.accountedMemoryBytes)
        }
        assertThrows(classOf[IllegalArgumentException], () =>
          MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, equivalentProfile, root))
        assertThrows(classOf[IllegalArgumentException], () =>
          MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, profile, closed))
        assertThrows(classOf[IllegalStateException], () =>
          MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, profile, foreign))
        MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, profile, child)
        MySqlRecoveryCanonicalBytes.withAllocationBudget(child) {
          assertThrows(classOf[IllegalStateException], () =>
            MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, profile, sibling))
          MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, profile, child)
        }
        assertEquals(before, root.accountedMemoryBytes)
        val held = root.availableMemoryBytes - 1L
        root.reserveRetainedMemory(held, "actual other retained responsibility")
        try
          val charged = root.accountedMemoryBytes
          val failure = assertThrows(classOf[SQLDataException], () =>
            MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, profile, root))
          assertTrue(failure.getStackTrace.nonEmpty)
          assertTrue(failure.getCause == null)
          assertEquals(charged, root.accountedMemoryBytes)
        finally root.releaseRetainedMemory(held, "other validation responsibility release")
        MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, profile, root)
        assertEquals(before, root.accountedMemoryBytes)
      }
    finally
      sibling.closeScopedReadBudget()
      child.closeScopedReadBudget()
      foreign.closeRootReadBudget()

  private def trackedDriftInputs(images: ModeWorkflowImages):
      (MySqlOwnedRepositoryOperationPlan, MySqlRepositoryOperation, MySqlRepositoryOperationContextIdentity) =
    val f = images.fixture
    val base = FloatingPodState(f.podUuid, Some(id(12)), 0L, "world", 0D, 64D, 0D,
      0F, 0F, true, 100L, Some(7), PodOperatingMode.Normal)
    val operation = MySqlRepositoryOperation.CommitTrackedActiveDrift(f.binding, base,
      base.copy(posX = 2D, yaw = 12F), f.attemptUuid)
    val plan = MySqlOwnedRepositoryOperationPlan.single(f.plan.tag, f.plan.workflowUuid,
      f.plan.reserveOperationUuid, operation, f.profile)
    val old = f.context
    val context = new MySqlRepositoryOperationContextIdentity(old.tag, old.workflowUuid, old.reserveOperationUuid,
      old.barrierGenerationUuid, old.actualConnectionId, old.barrierNamespace, old.bindingUuid, old.writerEpoch,
      old.fixedLeaseUntil, old.serverUuid, old.serverVersion, old.schema, old.effectiveUser, old.provisioningUuid,
      old.schemaFormat, old.physicalFormat, old.ddlSha256, old.approvedObjectManifestSha256,
      old.configurationSha256, old.sourceProvenanceKind, old.sourceProvenanceSha256,
      old.limitProfileSha256, "RUN", plan.requestIdentitySha256, f.profile)
    (plan, operation, context)

  private[mysql] final case class ModeWorkflowImages(
      fixture: Fixture,
      before: MySqlFormat3FullSnapshot,
      reserveTarget: MySqlFormat3FullSnapshot,
      executeTarget: MySqlFormat3FullSnapshot
  )

  private[mysql] def modeWorkflowImages(profile: SourceLimitProfile = testProfile(), bindingUuid: UUID = id(10)): ModeWorkflowImages =
    val fixture = identities(profile, bindingUuid)
    val emptySlotWriter = modeWriterRow(fixture, None, None)
    val originalDomain = modeDomainRow(fixture, 0L, PodOperatingMode.Normal, id(71))
    val originalLocator = modeLocatorRow(fixture, 0L, 0L, id(71))
    val footprint = modeFootprintRow(fixture, id(71))
    val historyOperation = modeOperationRow(fixture, id(71), "RESERVE", "COMMITTED", None)
    val chunks = modeChunkRows(fixture)
    val originalBefore = snapshot(profile, Vector.empty, fixture.binding.bindingUuid,
      relationOverrides = Map(
        "fp_schema_manifest" -> Vector(modeManifestRow(fixture)),
        "physical_recovery_binding" -> Vector(modeBindingRow(fixture)),
        WriterTable -> Vector(emptySlotWriter),
        "floating_pod_state" -> Vector(originalDomain),
        "physical_recovery_locator" -> Vector(originalLocator),
        "physical_recovery_footprint" -> Vector(footprint),
        "physical_recovery_footprint_chunk" -> chunks,
        OperationTable -> Vector(historyOperation),
        "fp_recovery_reservation" -> Vector.empty
      ))
    // 真实canonical输入路径提供不可变storage owner；原fixture分配继续计费，不能仅补标签。
    val root = originalBefore.allocationBudget
    val payload = MySqlFormat3SnapshotCodec.encode(originalBefore, root)
    val decoded = MySqlFormat3SnapshotCodec.decode(payload, profile, root)
    root.reserveRetainedMemory(8192L, "test decoded producer DATA context and wrappers BEFORE allocation")
    val before = new MySqlFormat3FullSnapshot(decoded.schema, decoded.connection, decoded.observationScope,
      decoded.maxAllowedPacketBytes, profile, decoded.relations, root)
    assertTrue(before.exactlyEquals(originalBefore))
    val scope = modeScope(fixture)
    val requestBytes = canonicalBytes(fixture.plan.exactRequestIdentity.canonical)
    try
      val reservePreimage = MySqlModeOperationEffectCodec.projectImage(before, fixture.plan,
        fixture.operation, fixture.context, 0L, MySqlOperationEffectPhase.Reserve, MySqlOperationEffectImage.Before)
      val reserveDraft = withPreimage(withRequestAndScope(
        modeOperationRow(fixture, fixture.reserveUuid, "RESERVE", "COMMITTED", None), requestBytes, scope),
        reservePreimage)
      val executeReservedDraft = withRequestAndScope(
        modeOperationRow(fixture, fixture.executeUuid, "DOMAIN_MUTATION", "RESERVED", Some(fixture.reserveUuid)),
        requestBytes, scope)
      val heldReservation = reservationRow(profile, fixture.binding.bindingUuid, fixture.podUuid,
        fixture.plan.workflowUuid, fixture.executeUuid, fixture.reserveUuid, 0L, "HELD")
      val reserveScaffold = snapshot(profile, Vector(reserveDraft, executeReservedDraft), fixture.binding.bindingUuid,
        writerRowOverride = modeWriterRow(fixture, Some(fixture.reserveUuid), Some(bytes(32, 31))),
        reservationRows = Vector(heldReservation),
        relationOverrides = Map(
          "fp_schema_manifest" -> Vector(modeManifestRow(fixture)),
          "physical_recovery_binding" -> Vector(modeBindingRow(fixture)),
          WriterTable -> Vector(modeWriterRow(fixture, Some(fixture.reserveUuid), Some(bytes(32, 31)))),
          "floating_pod_state" -> Vector(originalDomain),
          "physical_recovery_locator" -> Vector(originalLocator),
          "physical_recovery_footprint" -> Vector(footprint),
          "physical_recovery_footprint_chunk" -> chunks,
          OperationTable -> Vector(reserveDraft, executeReservedDraft, historyOperation),
          "fp_recovery_reservation" -> Vector(heldReservation)
        ))
      val reserveTargetImage = MySqlModeOperationEffectCodec.projectImage(reserveScaffold, fixture.plan,
        fixture.operation, fixture.context, 0L, MySqlOperationEffectPhase.Reserve, MySqlOperationEffectImage.Target)
      val reserveFingerprint = modeFingerprint(fixture, MySqlOperationEffectPhase.Reserve,
        reservePreimage, reserveTargetImage, scope)
      val reserveContext = modeOperationContextBytes(fixture, fixture.reserveUuid, reserveFingerprint.sha256)
      val reserveFinal = withPhaseValues(reserveDraft, requestBytes, scope, reservePreimage,
        reserveTargetImage, reserveContext, reserveFingerprint.sha256)
      val reserveWriterReceipt = replaceRelation(reserveScaffold, WriterTable,
        Vector(modeWriterRow(fixture, Some(fixture.reserveUuid), Some(reserveFingerprint.sha256.toArray))))
      val reserveTargetSkeleton = replaceRelation(reserveWriterReceipt, OperationTable,
        Vector(reserveFinal, executeReservedDraft, historyOperation))

      val changedDomain = modeDomainRow(fixture, 1L, PodOperatingMode.WorldIdle, fixture.executeUuid)
      val changedLocator = modeLocatorRow(fixture, 1L, 1L, fixture.executeUuid)
      val releasedReservation = reservationRow(profile, fixture.binding.bindingUuid, fixture.podUuid,
        fixture.plan.workflowUuid, fixture.executeUuid, fixture.executeUuid, 1L, "RELEASED")
      val executePreimage = MySqlModeOperationEffectCodec.projectImage(reserveTargetSkeleton, fixture.plan,
        fixture.operation, fixture.context, 0L, MySqlOperationEffectPhase.Execute, MySqlOperationEffectImage.Before)
      val executeReservedWithPreimageDraft = withPreimage(withRequestAndScope(
        modeOperationRow(fixture, fixture.executeUuid, "DOMAIN_MUTATION", "RESERVED", Some(fixture.reserveUuid)),
        requestBytes, scope), executePreimage)

      val executeCommittedDraft = withPreimage(withRequestAndScope(
        modeOperationRow(fixture, fixture.executeUuid, "DOMAIN_MUTATION", "COMMITTED", Some(fixture.reserveUuid)),
        requestBytes, scope), executePreimage)
      val executeScaffold = snapshot(profile, Vector(reserveFinal, executeCommittedDraft), fixture.binding.bindingUuid,
        writerRowOverride = modeWriterRow(fixture, Some(fixture.executeUuid), Some(bytes(32, 32))),
        reservationRows = Vector(releasedReservation),
        relationOverrides = Map(
          "fp_schema_manifest" -> Vector(modeManifestRow(fixture)),
          "physical_recovery_binding" -> Vector(modeBindingRow(fixture)),
          WriterTable -> Vector(modeWriterRow(fixture, Some(fixture.executeUuid), Some(bytes(32, 32)))),
          "floating_pod_state" -> Vector(changedDomain),
          "physical_recovery_locator" -> Vector(changedLocator),
          "physical_recovery_footprint" -> Vector(footprint),
          "physical_recovery_footprint_chunk" -> chunks,
          OperationTable -> Vector(reserveFinal, executeCommittedDraft, historyOperation),
          "fp_recovery_reservation" -> Vector(releasedReservation)
        ))
      val executeTargetImage = MySqlModeOperationEffectCodec.projectImage(executeScaffold, fixture.plan,
        fixture.operation, fixture.context, 0L, MySqlOperationEffectPhase.Execute, MySqlOperationEffectImage.Target)
      val executeFingerprint = modeFingerprint(fixture, MySqlOperationEffectPhase.Execute,
        executePreimage, executeTargetImage, scope)
      val executeContext = modeOperationContextBytes(fixture, fixture.executeUuid, executeFingerprint.sha256)
      val executeReserved = withPhaseValues(executeReservedWithPreimageDraft, requestBytes, scope, executePreimage,
        executeTargetImage, executeContext, executeFingerprint.sha256)
      val finalReserveTarget = replaceRelation(reserveTargetSkeleton, OperationTable,
        Vector(reserveFinal, executeReserved, historyOperation))
      val executeWriterReceipt = replaceRelation(executeScaffold, WriterTable,
        Vector(modeWriterRow(fixture, Some(fixture.executeUuid), Some(executeFingerprint.sha256.toArray))))
      val executeCommitted = withPhaseValues(executeCommittedDraft, requestBytes, scope, executePreimage,
        executeTargetImage, executeContext, executeFingerprint.sha256)
      val finalExecuteTarget = replaceRelation(executeWriterReceipt, OperationTable,
        Vector(reserveFinal, executeCommitted, historyOperation))
      ModeWorkflowImages(fixture, before, finalReserveTarget, finalExecuteTarget)
    finally Arrays.fill(requestBytes, 0.toByte)

  private def modeManifestRow(fixture: Fixture): Vector[MySqlFormat3Cell] =
    val table = "fp_schema_manifest"
    val profileBytes = fixture.profile.canonicalBytes()
    val sourceBytes = "mode-workflow-source-proof".getBytes(StandardCharsets.US_ASCII)
    val ddl = java.util.HexFormat.of().parseHex(fixture.context.ddlSha256)
    val guard = java.util.HexFormat.of().parseHex(fixture.context.approvedObjectManifestSha256)
    val config = fixture.context.configurationSha256.toArray
    try
      var row = defaultRow(MySqlFormat3RelationShapes.forTable(table))
      row = put(row, table, "singleton_fixed1", MySqlFormat3Cell.SignedInteger(1L))
      row = put(row, table, "backend", MySqlFormat3Cell.Text("MYSQL"))
      row = put(row, table, "format_version", MySqlFormat3Cell.SignedInteger(3L))
      row = put(row, table, "ddl_sha256", bytesCell(ddl))
      row = put(row, table, "provisioning_uuid", uuidCell(fixture.context.provisioningUuid))
      row = put(row, table, "bootstrap_state", MySqlFormat3Cell.Text("READY"))
      row = put(row, table, "expected_server_uuid", uuidCell(fixture.context.serverUuid))
      row = put(row, table, "expected_binding_uuid", uuidCell(fixture.binding.bindingUuid))
      row = put(row, table, "import_manifest_sha256", MySqlFormat3Cell.SqlNull)
      row = put(row, table, "import_state", MySqlFormat3Cell.Text("NONE"))
      row = put(row, table, "import_operation_uuid", MySqlFormat3Cell.SqlNull)
      row = put(row, table, "import_snapshot_sha256", MySqlFormat3Cell.SqlNull)
      row = put(row, table, "import_receipt_sha256", MySqlFormat3Cell.SqlNull)
      row = put(row, table, "guard_manifest_sha256", bytesCell(guard))
      row = put(row, table, "configuration_sha256", bytesCell(config))
      row = put(row, table, "source_provenance_kind", MySqlFormat3Cell.Text(fixture.context.sourceProvenanceKind))
      row = putBytesPair(row, table, "source_provenance_bytes", "source_provenance_sha256", sourceBytes)
      row = put(row, table, "limit_profile_encoding", MySqlFormat3Cell.Text(SourceLimitProfile.ENCODING))
      row = put(row, table, "limit_profile_bytes", bytesCell(profileBytes))
      row = put(row, table, "limit_profile_sha256", bytesCell(fixture.profile.sha256()))
      Vector(
        "max_region_bytes" -> fixture.profile.maxRegionBytes(),
        "max_chunk_bytes" -> fixture.profile.maxChunkBytes(),
        "max_decompressed_bytes" -> fixture.profile.maxDecompressedBytes(),
        "max_nbt_depth" -> fixture.profile.maxNbtDepth(),
        "max_members" -> fixture.profile.maxMembers(),
        "max_retained_bytes" -> fixture.profile.maxRetainedBytes(),
        "max_sql_packet_bytes" -> fixture.profile.maxSqlPacketBytes(),
        "max_native_artifact_bytes" -> fixture.profile.maxNativeArtifactBytes()
      ).foreach { case (name, value) => row = put(row, table, name, MySqlFormat3Cell.SignedInteger(value)) }
      row
    finally
      Arrays.fill(profileBytes, 0.toByte)
      Arrays.fill(sourceBytes, 0.toByte)
      Arrays.fill(ddl, 0.toByte)
      Arrays.fill(guard, 0.toByte)
      Arrays.fill(config, 0.toByte)

  private def modeBindingRow(fixture: Fixture): Vector[MySqlFormat3Cell] =
    val table = "physical_recovery_binding"
    var row = defaultRow(MySqlFormat3RelationShapes.forTable(table))
    row = put(row, table, "singleton_fixed1", MySqlFormat3Cell.SignedInteger(1L))
    row = put(row, table, "binding_uuid", uuidCell(fixture.binding.bindingUuid))
    row = put(row, table, "cutover_state", MySqlFormat3Cell.Text("CERTIFIED"))
    put(row, table, "format_version", MySqlFormat3Cell.SignedInteger(2L))

  private def modeWriterRow(fixture: Fixture, operationUuid: Option[UUID],
      fingerprint: Option[Array[Byte]]): Vector[MySqlFormat3Cell] =
    val table = WriterTable
    var row = defaultRow(MySqlFormat3RelationShapes.forTable(table))
    row = put(row, table, "singleton_fixed1", MySqlFormat3Cell.SignedInteger(1L))
    row = put(row, table, "binding_uuid", uuidCell(fixture.binding.bindingUuid))
    row = put(row, table, "writer_epoch", MySqlFormat3Cell.SignedInteger(fixture.context.writerEpoch))
    row = put(row, table, "owner_boot_uuid", uuidCell(fixture.context.tag.bootId))
    row = put(row, table, "lease_until", MySqlFormat3Cell.DateTime(fixture.context.fixedLeaseUntil))
    row = put(row, table, "last_commit_operation_uuid", operationUuid.map(uuidCell).getOrElse(MySqlFormat3Cell.SqlNull))
    row = put(row, table, "last_commit_fingerprint", fingerprint.map(bytesCell).getOrElse(MySqlFormat3Cell.SqlNull))
    row

  private def modeDomainRow(fixture: Fixture, version: Long, mode: PodOperatingMode,
      lastMutation: UUID): Vector[MySqlFormat3Cell] =
    val table = "floating_pod_state"
    val pose = fixture.operation.asInstanceOf[MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned].actualPose
    var row = defaultRow(MySqlFormat3RelationShapes.forTable(table))
    row = put(row, table, "uuid", uuidCell(fixture.podUuid))
    row = put(row, table, "owner_uuid", uuidCell(modeOwner(fixture)))
    row = put(row, table, "state_version", MySqlFormat3Cell.SignedInteger(version))
    row = put(row, table, "world_name", MySqlFormat3Cell.Text(pose.worldName))
    row = put(row, table, "pos_x", MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.posX)))
    row = put(row, table, "pos_y", MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.posY)))
    row = put(row, table, "pos_z", MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.posZ)))
    row = put(row, table, "yaw", MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.yaw.toDouble)))
    row = put(row, table, "pitch", MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.pitch.toDouble)))
    row = put(row, table, "is_active", MySqlFormat3Cell.SignedInteger(1L))
    row = put(row, table, "last_updated", MySqlFormat3Cell.SignedInteger(100L + version))
    row = put(row, table, "dimension_id", MySqlFormat3Cell.SignedInteger(pose.dimensionId.toLong))
    row = put(row, table, "operating_mode", MySqlFormat3Cell.Text(mode.databaseValue))
    row = put(row, table, "binding_uuid", uuidCell(fixture.binding.bindingUuid))
    put(row, table, "last_mutation_operation_uuid", uuidCell(lastMutation))

  private def modeLocatorRow(fixture: Fixture, version: Long, revision: Long,
      lastMutation: UUID): Vector[MySqlFormat3Cell] =
    val table = "physical_recovery_locator"
    var row = defaultRow(MySqlFormat3RelationShapes.forTable(table))
    row = put(row, table, "binding_uuid", uuidCell(fixture.binding.bindingUuid))
    row = put(row, table, "pod_uuid", uuidCell(fixture.podUuid))
    row = put(row, table, "current_attempt_uuid", uuidCell(fixture.attemptUuid))
    row = put(row, table, "current_footprint_uuid", uuidCell(id(41)))
    row = put(row, table, "locator_state", MySqlFormat3Cell.Text("ROW_ACTIVE"))
    row = put(row, table, "reason_code", MySqlFormat3Cell.Text("ROW_ACTIVE"))
    row = put(row, table, "domain_version", MySqlFormat3Cell.SignedInteger(version))
    row = put(row, table, "target_dimension_id", MySqlFormat3Cell.SignedInteger(7L))
    row = put(row, table, "locator_revision", MySqlFormat3Cell.SignedInteger(revision))
    put(row, table, "last_mutation_operation_uuid", uuidCell(lastMutation))

  private def modeFootprintRow(fixture: Fixture, createdOperation: UUID): Vector[MySqlFormat3Cell] =
    val table = "physical_recovery_footprint"
    val pose = fixture.operation.asInstanceOf[MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned].actualPose
    var row = defaultRow(MySqlFormat3RelationShapes.forTable(table))
    row = put(row, table, "binding_uuid", uuidCell(fixture.binding.bindingUuid))
    row = put(row, table, "pod_uuid", uuidCell(fixture.podUuid))
    row = put(row, table, "attempt_uuid", uuidCell(fixture.attemptUuid))
    row = put(row, table, "footprint_uuid", uuidCell(id(41)))
    row = put(row, table, "origin_version", MySqlFormat3Cell.SignedInteger(0L))
    row = put(row, table, "target_world_name", MySqlFormat3Cell.Text(pose.worldName))
    row = put(row, table, "target_pos_x", MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.posX)))
    row = put(row, table, "target_pos_y", MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.posY)))
    row = put(row, table, "target_pos_z", MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.posZ)))
    row = put(row, table, "target_yaw", MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.yaw.toDouble)))
    row = put(row, table, "target_pitch", MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(pose.pitch.toDouble)))
    row = put(row, table, "target_dimension_id", MySqlFormat3Cell.SignedInteger(pose.dimensionId.toLong))
    row = put(row, table, "center_chunk_x", MySqlFormat3Cell.SignedInteger(0L))
    row = put(row, table, "center_chunk_z", MySqlFormat3Cell.SignedInteger(0L))
    row = put(row, table, "purpose", MySqlFormat3Cell.Text("MIGRATED_UNVERIFIED"))
    row = put(row, table, "seal_state", MySqlFormat3Cell.Text("SEALED"))
    put(row, table, "created_operation_uuid", uuidCell(createdOperation))

  private def modeChunkRows(fixture: Fixture): Vector[Vector[MySqlFormat3Cell]] =
    val table = "physical_recovery_footprint_chunk"
    (for x <- -2 to 2; z <- -2 to 2 yield {
      var row = defaultRow(MySqlFormat3RelationShapes.forTable(table))
      row = put(row, table, "binding_uuid", uuidCell(fixture.binding.bindingUuid))
      row = put(row, table, "pod_uuid", uuidCell(fixture.podUuid))
      row = put(row, table, "attempt_uuid", uuidCell(fixture.attemptUuid))
      row = put(row, table, "footprint_uuid", uuidCell(id(41)))
      row = put(row, table, "dimension_id", MySqlFormat3Cell.SignedInteger(7L))
      row = put(row, table, "chunk_x", MySqlFormat3Cell.SignedInteger(x.toLong))
      put(row, table, "chunk_z", MySqlFormat3Cell.SignedInteger(z.toLong))
    }).toVector

  private def modeOperationRow(fixture: Fixture, operationUuid: UUID, step: String,
      state: String, reserveParent: Option[UUID]): Vector[MySqlFormat3Cell] =
    var row = operationRow(operationUuid, fixture.binding.bindingUuid, fixture.podUuid, fixture.attemptUuid,
      id(41), step, state, reserveParent)
    row = put(row, OperationTable, "boot_generation", MySqlFormat3Cell.SignedInteger(fixture.context.tag.bootGeneration))
    row = put(row, OperationTable, "connection_generation_uuid", uuidCell(fixture.context.barrierGenerationUuid))
    row = put(row, OperationTable, "connection_id", MySqlFormat3Cell.SignedInteger(fixture.context.actualConnectionId))
    row = put(row, OperationTable, "fence_epoch", MySqlFormat3Cell.SignedInteger(fixture.context.writerEpoch))
    row = put(row, OperationTable, "expected_domain_version", MySqlFormat3Cell.SignedInteger(0L))
    row = put(row, OperationTable, "expected_locator_revision", MySqlFormat3Cell.SignedInteger(0L))
    row = put(row, OperationTable, "expected_source_revision", MySqlFormat3Cell.SqlNull)
    row = put(row, OperationTable, "planned_at", MySqlFormat3Cell.DateTime(workflowTime))
    row = put(row, OperationTable, "fixed_lease_until", MySqlFormat3Cell.DateTime(fixture.context.fixedLeaseUntil))
    put(row, OperationTable, "result_at",
      if state == "COMMITTED" then MySqlFormat3Cell.DateTime(workflowTime) else MySqlFormat3Cell.SqlNull)

  private def modeOwner(fixture: Fixture): UUID = fixture.operation match
    case MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(_, _, owner, _, _, _, _) => owner
    case _ => throw new IllegalArgumentException("mode workflow fixture must retain the exact owned operation")

  private val workflowTime = LocalDateTime.of(2026, 10, 8, 17, 45, 0, 123456000)

  private def modeScope(fixture: Fixture): MySqlRecoveryCanonicalBytes =
    MySqlRecoveryOperationScope.encode(fixture.context, Vector(MySqlRecoveryResourceRevision(
      MySqlRecoveryResourceKey.pod(fixture.podUuid, fixture.profile), 0L, Some(fixture.attemptUuid))))

  private def withRequestAndScope(row: Vector[MySqlFormat3Cell], request: Array[Byte],
      scope: MySqlRecoveryCanonicalBytes): Vector[MySqlFormat3Cell] =
    putBytesPair(putBytesPair(row, OperationTable, "request_identity_bytes", "request_identity_sha256", request),
      OperationTable, "scope_bytes", "scope_sha256", canonicalBytes(scope))

  private def withPreimage(row: Vector[MySqlFormat3Cell],
      preimage: MySqlRecoveryCanonicalBytes): Vector[MySqlFormat3Cell] =
    val raw = canonicalBytes(preimage)
    try putBytesPair(row, OperationTable, "preimage_bytes", "preimage_sha256", raw)
    finally Arrays.fill(raw, 0.toByte)

  private def withPhaseValues(row: Vector[MySqlFormat3Cell], request: Array[Byte],
      scope: MySqlRecoveryCanonicalBytes, preimage: MySqlRecoveryCanonicalBytes,
      target: MySqlRecoveryCanonicalBytes, context: MySqlRecoveryCanonicalBytes,
      fingerprint: Vector[Byte]): Vector[MySqlFormat3Cell] =
    var next = withRequestAndScope(row, request, scope)
    next = putBytesPair(next, OperationTable, "original_context_bytes", "original_context_sha256", canonicalBytes(context))
    next = putBytesPair(next, OperationTable, "preimage_bytes", "preimage_sha256", canonicalBytes(preimage))
    next = putBytesPair(next, OperationTable, "target_bytes", "target_sha256", canonicalBytes(target))
    put(next, OperationTable, "operation_fingerprint", bytesCell(fingerprint.toArray))

  private def modeFingerprint(fixture: Fixture, phase: MySqlOperationEffectPhase,
      preimage: MySqlRecoveryCanonicalBytes, target: MySqlRecoveryCanonicalBytes,
      scope: MySqlRecoveryCanonicalBytes): MySqlRecoveryCanonicalBytes =
    MySqlRecoveryCanonicalBytes.encode("FP_MYSQL_MODE_EFFECT_FINGERPRINT_BE_V1", fixture.profile) { output =>
      MySqlRecoveryCanonicalBytes.writeText(output, "com.wi.floatingpod.mysql-operation-effect-fingerprint/v1")
      output.writeByte(1)
      val phaseToken = phase match
        case MySqlOperationEffectPhase.Reserve => "RESERVE"
        case MySqlOperationEffectPhase.Execute => "EXECUTE"
      MySqlRecoveryCanonicalBytes.writeText(output, phaseToken)
      MySqlRecoveryCanonicalBytes.writeUuid(output, fixture.binding.bindingUuid)
      MySqlRecoveryCanonicalBytes.writeUuid(output, fixture.podUuid)
      MySqlRecoveryCanonicalBytes.writeUuid(output, fixture.attemptUuid)
      MySqlRecoveryCanonicalBytes.writeUuid(output, fixture.plan.workflowUuid)
      MySqlRecoveryCanonicalBytes.writeUuid(output, fixture.plan.reserveOperationUuid)
      MySqlRecoveryCanonicalBytes.writeUuid(output, fixture.plan.executeOperationUuid)
      output.writeInt(fixture.plan.exactRequestIdentity.canonical.length)
      fixture.plan.exactRequestIdentity.canonical.writeTo(output)
      output.writeInt(scope.length)
      scope.writeTo(output)
      output.writeInt(preimage.length)
      preimage.writeTo(output)
      output.writeInt(target.length)
      target.writeTo(output)
    }

  private def modeOperationContextBytes(fixture: Fixture, operationUuid: UUID,
      fingerprint: Vector[Byte]): MySqlRecoveryCanonicalBytes =
    MySqlRecoveryCanonicalBytes.encode("FP_RECOVERY_OPERATION_BE_V1", fixture.profile) { output =>
      val domain = "com.wi.floatingpod.mysql-operation-context/v1".getBytes(StandardCharsets.US_ASCII)
      writeLengthFramed(output, domain)
      MySqlRecoveryCanonicalBytes.writeUuid(output, fixture.context.tag.bootId)
      output.writeLong(fixture.context.writerEpoch)
      MySqlRecoveryCanonicalBytes.writeUuid(output, fixture.binding.bindingUuid)
      MySqlRecoveryCanonicalBytes.writeUuid(output, operationUuid)
      output.writeInt(1)
      writeLengthFramed(output, ("pod/" + fixture.podUuid.toString).getBytes(StandardCharsets.US_ASCII))
      output.writeLong(0L)
      writeLengthFramed(output, fingerprint.toArray)
    }

  private def writeLengthFramed(output: DataOutputStream, value: Array[Byte]): Unit =
    output.writeInt(value.length)
    output.write(value)

  private def canonicalBytes(value: MySqlRecoveryCanonicalBytes): Array[Byte] =
    val result = new ByteArrayOutputStream(value.length)
    val output = new DataOutputStream(result)
    value.writeTo(output)
    output.flush()
    result.toByteArray

  private def replaceRelation(snapshot: MySqlFormat3FullSnapshot, table: String,
      rows: Vector[Vector[MySqlFormat3Cell]]): MySqlFormat3FullSnapshot =
    val relations = snapshot.relations.map(relation =>
      if relation.table == table then MySqlFormat3RelationSnapshot(table, rows) else relation)
    makeSnapshot(snapshot.schema, snapshot.connection, snapshot.observationScope,
      snapshot.maxAllowedPacketBytes, snapshot.limitProfile, relations)

  private def relationRows(snapshot: MySqlFormat3FullSnapshot, table: String): Vector[Vector[MySqlFormat3Cell]] =
    snapshot.relation(table).rows

  private def makeSnapshot(schema: String, connection: MySqlFormat3ConnectionObservation,
      scope: MySqlFormat3ObservationScope, maxPacket: Long, profile: SourceLimitProfile,
      relations: Vector[MySqlFormat3RelationSnapshot]): MySqlFormat3FullSnapshot =
    val budget = new MySqlFormat3ReadBudget(profile, maxPacket)
    budget.initialize(schema, connection, scope, maxPacket)
    budget.recordMaterializedSnapshot(relations)
    new MySqlFormat3FullSnapshot(schema, connection, scope, maxPacket, profile, relations, budget)

  private[mysql] final case class Fixture(
      profile: SourceLimitProfile,
      binding: PhysicalRecoveryBinding,
      podUuid: UUID,
      attemptUuid: UUID,
      reserveUuid: UUID,
      executeUuid: UUID,
      plan: MySqlOwnedRepositoryOperationPlan,
      operation: MySqlRepositoryOperation,
      context: MySqlRepositoryOperationContextIdentity
  )

  private def identities(profile: SourceLimitProfile, bindingUuid: UUID = id(10)): Fixture =
    val podUuid = id(11)
    val ownerUuid = id(12)
    val attemptUuid = id(13)
    val workflowUuid = id(14)
    val reserveUuid = id(15)
    val executeUuid = id(16)
    val bootUuid = id(17)
    val serverUuid = id(18)
    val barrierUuid = id(19)
    val provisioningUuid = id(20)
    val binding = PhysicalRecoveryBinding(bindingUuid, PhysicalRecoveryCutoverState.Certified, 2)
    val pose = FloatingPodPose("world", 0.0d, 64.0d, 0.0d, 0.0f, 0.0f, 7)
    val operation = MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(
      binding, podUuid, ownerUuid, 0L, attemptUuid, PodOperatingMode.WorldIdle, pose)
    val tag = IoOperationTag(bootUuid, 5L, 9L, executeUuid)
    val plan = MySqlOwnedRepositoryOperationPlan.single(tag, workflowUuid, reserveUuid, operation, profile)
    val context = new MySqlRepositoryOperationContextIdentity(
      tag, workflowUuid, reserveUuid, barrierUuid, 123L, "fpb1:" + ("A" * 43),
      bindingUuid, 3L, LocalDateTime.of(2026, 10, 8, 18, 0, 0, 123456000),
      serverUuid, "9.7.2", "fp_mode_codec", "fp_mode_test@127.0.0.1", provisioningUuid,
      3, 2, "a" * 64, "b" * 64, Vector.fill(32)(1.toByte),
      "FRESH_PLAN", MessageDigest.getInstance("SHA-256")
        .digest("mode-workflow-source-proof".getBytes(StandardCharsets.US_ASCII)).toVector,
      profile.sha256().toVector, "RUN", plan.requestIdentitySha256, profile)
    Fixture(profile, binding, podUuid, attemptUuid, reserveUuid, executeUuid, plan, operation, context)

  private def project(snapshot: MySqlFormat3FullSnapshot, fixture: Fixture,
      image: MySqlOperationEffectImage,
      phase: MySqlOperationEffectPhase = MySqlOperationEffectPhase.Execute): MySqlRecoveryCanonicalBytes =
    MySqlModeOperationEffectCodec.projectImage(snapshot, fixture.plan, fixture.operation,
      fixture.context, 0L, phase, image)

  private def snapshot(profile: SourceLimitProfile, operationRows: Vector[Vector[MySqlFormat3Cell]],
      bindingUuid: UUID, writerOperation: UUID = id(70), writerFingerprint: Array[Byte] = bytes(32, 7),
      reservationRows: Vector[Vector[MySqlFormat3Cell]] = Vector.empty,
      writerRowOverride: Vector[MySqlFormat3Cell] = null,
      relationOverrides: Map[String, Vector[Vector[MySqlFormat3Cell]]] = Map.empty): MySqlFormat3FullSnapshot =
    val relations = MySqlFormat3RelationShapes.canonicalOrder.map { shape =>
      val rows = relationOverrides.get(shape.table).getOrElse {
        shape.table match
          case OperationTable => operationRows
          case WriterTable => Vector(Option(writerRowOverride).getOrElse(writerRow(bindingUuid, writerOperation, writerFingerprint)))
          case "fp_recovery_reservation" => reservationRows
          case _ => Vector.empty
      }
      MySqlFormat3RelationSnapshot(shape.table, rows)
    }
    val schema = "fp_mode_codec"
    val connection = MySqlFormat3ConnectionObservation(
      id(18), "9.7.2", schema, "fp_mode_test@127.0.0.1", "InnoDB",
      1, 1L, "READ-COMMITTED", "REPEATABLE-READ", "STRICT_ALL_TABLES", "+00:00",
      "TLSv1.3", "TLS_AES_256_GCM_SHA384")
    val scope = MySqlFormat3ObservationScope(id(17), 5L, 9L)
    val budget = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    budget.initialize(schema, connection, scope, 16_777_216L)
    budget.recordMaterializedSnapshot(relations)
    new MySqlFormat3FullSnapshot(schema, connection, scope, Math.max(16_777_216L, profile.maxSqlPacketBytes()), profile, relations, budget)

  private def writerRow(bindingUuid: UUID, operationUuid: UUID, fingerprint: Array[Byte]): Vector[MySqlFormat3Cell] =
    val shape = MySqlFormat3RelationShapes.forTable(WriterTable)
    var row = defaultRow(shape)
    row = put(row, WriterTable, "singleton_fixed1", MySqlFormat3Cell.SignedInteger(1L))
    row = put(row, WriterTable, "binding_uuid", uuidCell(bindingUuid))
    row = put(row, WriterTable, "last_commit_operation_uuid", uuidCell(operationUuid))
    put(row, WriterTable, "last_commit_fingerprint", bytesCell(fingerprint))

  private def reservationRow(profile: SourceLimitProfile, bindingUuid: UUID, podUuid: UUID,
      workflowUuid: UUID, reservationOperationUuid: UUID, mutationOperationUuid: UUID,
      revision: Long, state: String): Vector[MySqlFormat3Cell] =
    val resource = MySqlRecoveryResourceKey.pod(podUuid, profile)
    val budget = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    val canonicalKey = resource.copyCanonicalBytes(budget, "mode projection reservation key test")
    try
      val table = "fp_recovery_reservation"
      var row = defaultRow(MySqlFormat3RelationShapes.forTable(table))
      row = put(row, table, "binding_uuid", uuidCell(bindingUuid))
      row = put(row, table, "resource_kind", MySqlFormat3Cell.Text("POD"))
      row = put(row, table, "resource_key_hash", bytesCell(resource.keyHashSha256.toArray))
      row = put(row, table, "resource_key", bytesCell(canonicalKey))
      row = put(row, table, "reservation_revision", MySqlFormat3Cell.SignedInteger(revision))
      row = put(row, table, "workflow_uuid", uuidCell(workflowUuid))
      row = put(row, table, "operation_uuid", uuidCell(reservationOperationUuid))
      row = put(row, table, "reservation_state", MySqlFormat3Cell.Text(state))
      row = put(row, table, "last_mutation_operation_uuid", uuidCell(mutationOperationUuid))
      row = put(row, table, "resource_pod_uuid", uuidCell(podUuid))
      put(row, table, "resource_unit_uuid", MySqlFormat3Cell.SqlNull)
    finally
      Arrays.fill(canonicalKey, 0.toByte)
      budget.releaseRetainedMemory(Math.addExact(resource.canonicalLength.toLong, 64L),
        "mode projection reservation key test release")

  private def operationRow(operationUuid: UUID, bindingUuid: UUID, podUuid: UUID, attemptUuid: UUID,
      footprintUuid: UUID, step: String, state: String,
      reserveParentUuid: Option[UUID] = Some(id(15))): Vector[MySqlFormat3Cell] =
    var row = defaultRow(MySqlFormat3RelationShapes.forTable(OperationTable))
    row = put(row, OperationTable, "operation_uuid", uuidCell(operationUuid))
    row = put(row, OperationTable, "binding_uuid", uuidCell(bindingUuid))
    row = put(row, OperationTable, "workflow_uuid", uuidCell(id(14)))
    row = put(row, OperationTable, "pod_uuid", uuidCell(podUuid))
    row = put(row, OperationTable, "attempt_uuid", uuidCell(attemptUuid))
    row = put(row, OperationTable, "footprint_uuid", uuidCell(footprintUuid))
    row = put(row, OperationTable, "boot_uuid", uuidCell(id(17)))
    row = put(row, OperationTable, "step", MySqlFormat3Cell.Text(step))
    row = put(row, OperationTable, "operation_state", MySqlFormat3Cell.Text(state))
    if step == "RESERVE" then
      row = put(row, OperationTable, "reserve_operation_uuid", MySqlFormat3Cell.SqlNull)
      row = put(row, OperationTable, "previous_step_operation_uuid", MySqlFormat3Cell.SqlNull)
    else
      row = put(row, OperationTable, "reserve_operation_uuid", reserveParentUuid.map(uuidCell).getOrElse(MySqlFormat3Cell.SqlNull))
      row = put(row, OperationTable, "previous_step_operation_uuid", reserveParentUuid.map(uuidCell).getOrElse(MySqlFormat3Cell.SqlNull))
    row = put(row, OperationTable, "context_encoding", MySqlFormat3Cell.Text("FP_RECOVERY_OPERATION_BE_V1"))
    row = putBytesPair(row, "request_identity_bytes", "request_identity_sha256",
      "request".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    row = putBytesPair(row, "original_context_bytes", "original_context_sha256",
      "context".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    row = putBytesPair(row, "scope_bytes", "scope_sha256", "scope".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    row = putBytesPair(row, "preimage_bytes", "preimage_sha256", "preimage".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    row = putBytesPair(row, "target_bytes", "target_sha256", "target".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
    row = put(row, OperationTable, "operation_fingerprint", bytesCell(bytes(32, 9)))
    row = put(row, OperationTable, "original_diagnostic_identity", MySqlFormat3Cell.SqlNull)
    row = put(row, OperationTable, "original_diagnostic_sha256", MySqlFormat3Cell.SqlNull)
    state match
      case "RESERVED" =>
        row = put(row, OperationTable, "result_at", MySqlFormat3Cell.SqlNull)
        row = put(row, OperationTable, "settled_by_operation_uuid", MySqlFormat3Cell.SqlNull)
        row = put(row, OperationTable, "commit_witness_bytes", MySqlFormat3Cell.SqlNull)
        put(row, OperationTable, "commit_witness_sha256", MySqlFormat3Cell.SqlNull)
      case "COMMITTED" =>
        row = put(row, OperationTable, "result_at",
          MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 8, 18, 10, 0, 123456000)))
        row = put(row, OperationTable, "settled_by_operation_uuid", MySqlFormat3Cell.SqlNull)
        putBytesPair(row, "commit_witness_bytes", "commit_witness_sha256",
          "predetermined witness".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
      case _ => throw new IllegalArgumentException("test operation state")

  private def withDiagnosticIdentity(row: Vector[MySqlFormat3Cell], payload: Array[Byte]): Vector[MySqlFormat3Cell] =
    putBytesPair(row, "original_diagnostic_identity", "original_diagnostic_sha256", payload)

  private def withOpaqueTarget(row: Vector[MySqlFormat3Cell], payload: Array[Byte]): Vector[MySqlFormat3Cell] =
    putBytesPair(row, "target_bytes", "target_sha256", payload)

  private def withOpaqueContext(row: Vector[MySqlFormat3Cell], payload: Array[Byte]): Vector[MySqlFormat3Cell] =
    putBytesPair(row, "original_context_bytes", "original_context_sha256", payload)

  private def withOpaquePreimage(row: Vector[MySqlFormat3Cell], payload: Array[Byte]): Vector[MySqlFormat3Cell] =
    putBytesPair(row, "preimage_bytes", "preimage_sha256", payload)

  private def withCommitWitness(row: Vector[MySqlFormat3Cell], payload: Array[Byte]): Vector[MySqlFormat3Cell] =
    putBytesPair(row, "commit_witness_bytes", "commit_witness_sha256", payload)

  private def putBytesPair(row: Vector[MySqlFormat3Cell], bytesName: String, shaName: String,
      payload: Array[Byte]): Vector[MySqlFormat3Cell] =
    val raw = payload.clone()
    val digest = MessageDigest.getInstance("SHA-256").digest(raw)
    var next = put(row, OperationTable, bytesName, bytesCell(raw))
    next = put(next, OperationTable, shaName, bytesCell(digest))
    Arrays.fill(raw, 0.toByte)
    Arrays.fill(digest, 0.toByte)
    next

  private def putBytesPair(row: Vector[MySqlFormat3Cell], table: String, bytesName: String,
      shaName: String, payload: Array[Byte]): Vector[MySqlFormat3Cell] =
    val raw = payload.clone()
    val digest = MessageDigest.getInstance("SHA-256").digest(raw)
    var next = put(row, table, bytesName, bytesCell(raw))
    next = put(next, table, shaName, bytesCell(digest))
    Arrays.fill(raw, 0.toByte)
    Arrays.fill(digest, 0.toByte)
    next

  private def defaultRow(shape: MySqlFormat3RelationShape): Vector[MySqlFormat3Cell] =
    shape.columns.map { column =>
      if column.nullable then MySqlFormat3Cell.SqlNull
      else column.kind match
        case MySqlFormat3SqlKind.Text => MySqlFormat3Cell.Text("x")
        case MySqlFormat3SqlKind.Binary =>
          val length =
            if column.name.endsWith("_uuid") || column.name == "uuid" then 16
            else if column.name.endsWith("_sha256") || column.name.endsWith("_fingerprint") then 32
            else 1
          MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(Array.fill[Byte](length)(0.toByte)))
        case MySqlFormat3SqlKind.Integer => MySqlFormat3Cell.SignedInteger(1L)
        case MySqlFormat3SqlKind.Floating => MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(1.25d))
        case MySqlFormat3SqlKind.DateTime => MySqlFormat3Cell.DateTime(LocalDateTime.of(2026, 10, 8, 17, 0, 0, 123456000))
    }

  private def put(row: Vector[MySqlFormat3Cell], table: String, name: String,
      value: MySqlFormat3Cell): Vector[MySqlFormat3Cell] =
    val index = MySqlFormat3RelationShapes.forTable(table).columns.indexWhere(_.name == name)
    if index < 0 then throw new IllegalArgumentException("missing fixed test column " + table + "." + name)
    row.updated(index, value)

  private def cell(row: Vector[MySqlFormat3Cell], table: String, name: String): MySqlFormat3Cell =
    val index = MySqlFormat3RelationShapes.forTable(table).columns.indexWhere(_.name == name)
    if index < 0 then throw new IllegalArgumentException("missing fixed test column " + table + "." + name)
    row(index)

  private def bytesCell(value: Array[Byte]): MySqlFormat3Cell =
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(value.clone()))

  private def uuidCell(value: UUID): MySqlFormat3Cell =
    val raw = ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()
    MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(raw))

  private def bytes(size: Int, fill: Int): Array[Byte] = Array.fill[Byte](size)(fill.toByte)

  private def testProfile(): SourceLimitProfile =
    SourceLimitProfile.of(1_000_000L, 1_000_000L, 2_000_000L, 64L, 100_000L,
      8_000_000L, 4_000_000L, 2_000_000L)

  private def id(value: Int): UUID =
    UUID.fromString(f"00000000-0000-0000-0000-$value%012d")
