package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.time.LocalDateTime
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

/** 真materializer/原registry DATA责任，不发行SQL/native/Boot资格。 */
final class MySqlModeSharedBudgetResponsibilityTest:
  private val fixedAt = LocalDateTime.of(2026, 10, 8, 17, 0, 0, 123456000)

  private def retained(plan: MySqlOwnedRepositoryOperationPlan, operation: MySqlRepositoryOperation,
      context: MySqlRepositoryOperationContextIdentity, root: MySqlFormat3ReadBudget):
      (MySqlModeOriginalWitnessDraft, MySqlModeProjectionBudgetRegistry) =
    root.reserveRetainedMemory(512L, "test real mode draft holder")
    val draft = new MySqlModeOriginalWitnessDraft(plan, operation, context, root)
    val registry = new MySqlModeProjectionBudgetRegistry(Thread.currentThread(), draft.tag.bootId,
      draft.tag.bootGeneration, draft.tag.connectionGeneration)
    registry.retainRoot(draft.tag, root)
    registry.retainWitnessDraft(draft.tag, draft)
    (draft, registry)

  @Test def actualSharedRowsAndRawReferencesKeepFullHistoryWithoutSecondPayloadAllocation(): Unit =
    val images = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
    val (draft, registry) = retained(images.fixture.plan, images.fixture.operation, images.fixture.context, images.before.allocationBudget)
    val f = images.fixture
    val root = draft.originalRoot
    assertEquals(8000000L, root.profile.maxRetainedBytes())
    var result = MySqlModeWorkflowMaterializer.materialize(f.plan, f.operation, f.context,
      fixedAt, images.before, 0L, draft)
    result.verify()
    assertSame(root, result.reserveTarget.allocationBudget)
    assertSame(root, result.executeTarget.allocationBudget)
    // 所有非改变的关系共享原immutable rows，而不是同值新数组冒共享。
    images.before.relations.filter(r => !Set("fp_recovery_operation", "fp_recovery_reservation",
      "fp_runtime_writer", "floating_pod_state", "physical_recovery_locator").contains(r.table)).foreach { relation =>
      assertSame(relation.rows, result.reserveTarget.relation(relation.table).rows)
      assertSame(relation.rows, result.executeTarget.relation(relation.table).rows)
    }
    val reserve = result.effects.head.rawOperationEnvelopes
    val executeBefore = result.effects(1).preimageRawOperationEnvelopes
    reserve.foreach { original =>
      val reused = executeBefore.find(_.operationUuid == original.operationUuid).get
      assertSame(original.row, reused.row)
      assertSame(original, reused)
    }
    val held = draft.retainedRawConstruction
    assertNotNull(held)
    assertTrue(held.retainedCount > 0)
    assertTrue(root.accountedMemoryBytes <= 8000000L)
    result = null
    registry.release(draft.tag)
    assertEquals(0, held.retainedCount, "exactdrop clears actual RAW references before checked root close")
    assertEquals(0, draft.partialRowReferenceCount)
    assertEquals(0, draft.partialCellReferenceCount)
    assertTrue(draft.isDropped)
    assertTrue(root.isClosed)

  @Test def actualPartialRawFailureKeepsExactDraftRootCauseAndCreditsZero(): Unit =
    val measuredImages = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
    val (measured, _) = retained(measuredImages.fixture.plan, measuredImages.fixture.operation, measuredImages.fixture.context, measuredImages.before.allocationBudget)
    val mf = measuredImages.fixture
    val measuredStart = measured.originalRoot.accountedMemoryBytes
    MySqlModeWorkflowMaterializer.materialize(mf.plan, mf.operation, mf.context,
      fixedAt, measuredImages.before, 0L, measured)
    val firstRawIncrement = Math.subtractExact(measured.retainedRawConstruction.firstRawSuccessfulReservePeakBytes, measuredStart)
    assertTrue(firstRawIncrement > 0L)

    val images = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
    val (draft, registry) = retained(images.fixture.plan, images.fixture.operation, images.fixture.context, images.before.allocationBudget)
    val f = images.fixture
    val root = draft.originalRoot
    assertEquals(measuredStart, root.accountedMemoryBytes)
    // 用真实生产getter的第一RAW保留费用，不复制预算算法或注入throwing callback。
    root.reserveRetainedMemory(Math.subtractExact(root.availableMemoryBytes,
      Math.addExact(firstRawIncrement, 1L)), "existing same-root obligation leaves first retained RAW only")
    val cause = assertThrows(classOf[SQLDataException], () =>
      MySqlModeWorkflowMaterializer.materialize(f.plan, f.operation, f.context,
        fixedAt, images.before, 0L, draft))
    val stack = cause.getStackTrace
    assertTrue(draft.retainedProducerRawCount > 0, "real RAW returned and retained before subsequent allocation failed")
    assertSame(draft, registry.retainedWitnessDraft(draft.tag).get)
    assertSame(root, draft.originalRoot)
    assertSame(cause, draft.retainedFailure.get)
    assertTrue(stack.sameElements(cause.getStackTrace))
    val charged = root.accountedMemoryBytes
    val rejected = assertThrows(classOf[IllegalStateException], () => registry.release(draft.tag))
    assertSame(cause, rejected.getCause)
    assertEquals(charged, root.accountedMemoryBytes)
    assertFalse(root.isClosed)
    assertFalse(draft.isDropped)

  @Test def oneRemainingByteRejectsProductionEntryBeforeAllocatingNewPayloadAndRetainsOriginalCause(): Unit =
    val images = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
    val (draft, registry) = retained(images.fixture.plan, images.fixture.operation, images.fixture.context, images.before.allocationBudget)
    val root = draft.originalRoot
    val f = images.fixture
    root.reserveRetainedMemory(root.availableMemoryBytes - 1L, "existing nearcapacity root obligation")
    val charged = root.accountedMemoryBytes
    val cause = assertThrows(classOf[SQLDataException], () =>
      MySqlModeWorkflowMaterializer.materialize(f.plan, f.operation, f.context,
        fixedAt, images.before, 0L, draft))
    assertSame(cause, draft.retainedFailure.get)
    assertSame(draft, registry.retainedWitnessDraft(draft.tag).get)
    assertEquals(charged, root.accountedMemoryBytes)
    assertEquals(0, draft.retainedProducerRawCount)
    assertFalse(root.isClosed)

  @Test def returnedUuidCellAndPartialRowSurviveActualPutReserveFailure(): Unit =
    val measuredImages = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
    val (measured, _) = retained(measuredImages.fixture.plan, measuredImages.fixture.operation,
      measuredImages.fixture.context, measuredImages.before.allocationBudget)
    val mf = measuredImages.fixture
    val measuredStart = measured.originalRoot.accountedMemoryBytes
    MySqlModeWorkflowMaterializer.materialize(mf.plan, mf.operation, mf.context,
      fixedAt, measuredImages.before, 0L, measured)
    val firstUuidIncrement = Math.subtractExact(measured.firstUuidCellAccountedBytes, measuredStart)
    assertTrue(firstUuidIncrement > 0L)

    val images = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
    val (draft, registry) = retained(images.fixture.plan, images.fixture.operation,
      images.fixture.context, images.before.allocationBudget)
    val f = images.fixture
    val root = draft.originalRoot
    assertEquals(measuredStart, root.accountedMemoryBytes)
    root.reserveRetainedMemory(Math.subtractExact(root.availableMemoryBytes,
      Math.addExact(firstUuidIncrement, 1L)), "existing obligation permits first actual UUID cell but not put vector")
    val cause = assertThrows(classOf[SQLDataException], () =>
      MySqlModeWorkflowMaterializer.materialize(f.plan, f.operation, f.context,
        fixedAt, images.before, 0L, draft))
    val stack = cause.getStackTrace
    assertTrue(stack.exists(_.getMethodName == "put"), "actual put reserve must be the failure boundary")
    assertSame(root, draft.originalRoot)
    assertSame(draft, registry.retainedWitnessDraft(draft.tag).get)
    assertSame(cause, draft.retainedFailure.get)
    assertTrue(draft.partialRowReferenceCount > 0)
    assertNotNull(draft.partialRowReferenceAt(0))
    assertTrue(draft.partialCellReferenceCount > 0)
    draft.firstUuidCellReference match
      case MySqlFormat3Cell.Binary(bytes) =>
        assertTrue(bytes.equalsUuid(MySqlModeOperationEffectCodec.modeRequest(f.operation).binding.bindingUuid))
      case _ => throw new AssertionError("actual produced UUID typed cell must remain strongly retained")
    val charged = root.accountedMemoryBytes
    val rejected = assertThrows(classOf[IllegalStateException], () => registry.release(draft.tag))
    assertSame(cause, rejected.getCause)
    assertTrue(stack.sameElements(cause.getStackTrace))
    assertEquals(charged, root.accountedMemoryBytes)
    assertFalse(root.isClosed)
    assertFalse(draft.isDropped)
