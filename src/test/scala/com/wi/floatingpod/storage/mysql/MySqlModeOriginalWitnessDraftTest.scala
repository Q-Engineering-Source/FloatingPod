package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertNotSame, assertNull, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

/** 真实 registry/draft DATA 生命周期；不自称 actualSQL planning issuer 验收。 */
final class MySqlModeOriginalWitnessDraftTest:
  private def registry(draft: MySqlModeOriginalWitnessDraft): MySqlModeProjectionBudgetRegistry =
    new MySqlModeProjectionBudgetRegistry(Thread.currentThread(), draft.tag.bootId,
      draft.tag.bootGeneration, draft.tag.connectionGeneration)

  @Test def exactEntryRetainsItsDraftAndNormalDropPrecedesRootClose(): Unit =
    val first = ModeWitnessDataFixtures.draft()
    val registry = this.registry(first)
    registry.retainRoot(first.tag, first.originalRoot)
    registry.retainWitnessDraft(first.tag, first)
    assertSame(first, registry.retainedWitnessDraft(first.tag).get)
    registry.release(first.tag)
    assertTrue(first.isDropped)
    assertTrue(first.originalRoot.isClosed)
    assertEquals(0, registry.pendingCount)

  @Test def duplicateAndForeignRootDraftAreRejectedWithoutReplacingOriginal(): Unit =
    val first = ModeWitnessDataFixtures.draft()
    val other = ModeWitnessDataFixtures.draft()
    val registry = this.registry(first)
    registry.retainRoot(first.tag, first.originalRoot)
    registry.retainWitnessDraft(first.tag, first)
    assertThrows(classOf[IllegalArgumentException], () => registry.retainWitnessDraft(first.tag, first))
    assertThrows(classOf[IllegalArgumentException], () => registry.retainWitnessDraft(first.tag, other))
    assertSame(first, registry.retainedWitnessDraft(first.tag).get)
    assertFalse(first.originalRoot.isClosed)

  @Test def failedDraftPreservesPrimaryStackAndHasNoOrdinaryReleaseQualification(): Unit =
    val draft = ModeWitnessDataFixtures.draft()
    val registry = this.registry(draft)
    registry.retainRoot(draft.tag, draft.originalRoot)
    registry.retainWitnessDraft(draft.tag, draft)
    val original = new SQLDataException("controlled DATA failure")
    val stack = original.getStackTrace
    draft.recordFailure(original)
    val rejection = assertThrows(classOf[IllegalStateException], () => registry.release(draft.tag))
    assertSame(original, rejection.getCause)
    assertSame(original, draft.retainedFailure.get)
    assertTrue(stack.sameElements(original.getStackTrace))
    assertFalse(draft.isDropped)
    assertFalse(draft.originalRoot.isClosed)
    assertEquals(1, registry.pendingCount)

  @Test def oneRemainingByteRejectsBeforeCanonicalImageAllocationAndRetainsDraftCause(): Unit =
    val draft = ModeWitnessDataFixtures.draft()
    val root = draft.originalRoot
    val source = ModeWitnessDataFixtures.source(root)
    root.reserveRetainedMemory(root.availableMemoryBytes - 1L, "actual nearcapacity DATA obligation")
    val before = root.accountedMemoryBytes
    val cause = assertThrows(classOf[SQLDataException], () => MySqlModeWitnessCodec.encode(source, draft))
    assertSame(cause, draft.retainedFailure.get)
    assertEquals(before, root.accountedMemoryBytes)
    assertFalse(root.isClosed)
    assertThrows(classOf[IllegalStateException], () => draft.requireDroppable())

  @Test def actualProductionReleaseActionSeamPreservesFailureWithoutSecondAttempt(): Unit =
    // 同production helper；throwing action只证明责任逻辑，非实际budget/nativeclose故障验收。
    val attempt = new MySqlModeProjectionReleaseAttempt(Thread.currentThread())
    val original = new IllegalStateException("controlled checked-close action failure")
    val secondary = new IllegalArgumentException("already-associated cleanup cause")
    original.addSuppressed(secondary)
    val stack = original.getStackTrace
    var dropped = false
    var childCalls = 0
    var rootCalls = 0
    val primary = assertThrows(classOf[IllegalStateException], () => attempt.run(
      () => { dropped = true },
      () => { childCalls += 1; throw original },
      () => { rootCalls += 1 }))
    assertSame(original, primary)
    assertSame(primary, attempt.retainedFailure.get)
    assertSame(secondary, primary.getSuppressed.head)
    assertTrue(stack.sameElements(primary.getStackTrace))
    assertTrue(dropped)
    assertTrue(attempt.hasAttempted)
    assertEquals(1, childCalls)
    assertEquals(0, rootCalls)
    val repeated = assertThrows(classOf[IllegalStateException], () => attempt.run(
      () => { dropped = false }, () => { childCalls += 1 }, () => { rootCalls += 1 }))
    assertSame(primary, repeated.getCause)
    assertTrue(dropped)
    assertEquals(1, childCalls)
    assertEquals(0, rootCalls)

  @Test def multipleOriginalTagsKeepIndependentRegistryEntriesWithoutSingleDraftGate(): Unit =
    val first = ModeWitnessDataFixtures.draft()
    val other = ModeWitnessDataFixtures.draft()
    val c = other.originalContext
    val tag = c.tag.copy(operationId = java.util.UUID.fromString("6db8758d-520f-46f5-bcf4-6e9b3d599e40"))
    val workflow = java.util.UUID.fromString("6db8758d-520f-46f5-bcf4-6e9b3d599e41")
    val reserve = java.util.UUID.fromString("6db8758d-520f-46f5-bcf4-6e9b3d599e42")
    val plan = MySqlOwnedRepositoryOperationPlan.single(tag, workflow, reserve, other.originalOperation, other.originalRoot.profile)
    val context = new MySqlRepositoryOperationContextIdentity(tag, workflow, reserve, c.barrierGenerationUuid,
      c.actualConnectionId, c.barrierNamespace, c.bindingUuid, c.writerEpoch, c.fixedLeaseUntil,
      c.serverUuid, c.serverVersion, c.schema, c.effectiveUser, c.provisioningUuid, c.schemaFormat,
      c.physicalFormat, c.ddlSha256, c.approvedObjectManifestSha256, c.configurationSha256,
      c.sourceProvenanceKind, c.sourceProvenanceSha256, c.limitProfileSha256, c.requestKind,
      plan.requestIdentitySha256, other.originalRoot.profile)
    val second = new MySqlModeOriginalWitnessDraft(plan, other.originalOperation, context, other.originalRoot)
    val registry = this.registry(first)
    registry.retainRoot(first.tag, first.originalRoot)
    registry.retainWitnessDraft(first.tag, first)
    registry.retainRoot(second.tag, second.originalRoot)
    registry.retainWitnessDraft(second.tag, second)
    assertEquals(2, registry.pendingCount)
    assertSame(first, registry.retainedWitnessDraft(first.tag).get)
    assertSame(second, registry.retainedWitnessDraft(second.tag).get)
    registry.release(first.tag)
    assertEquals(1, registry.pendingCount)
    assertFalse(second.originalRoot.isClosed)

  @Test def foreignThreadCannotRegisterOrDropOriginalDraft(): Unit =
    val draft = ModeWitnessDataFixtures.draft()
    val registry = this.registry(draft)
    registry.retainRoot(draft.tag, draft.originalRoot)
    registry.retainWitnessDraft(draft.tag, draft)
    val result = new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val thread = new Thread(() =>
      try registry.release(draft.tag)
      catch case cause: Throwable => result.set(cause)
    )
    thread.start()
    thread.join(5000L)
    assertFalse(thread.isAlive)
    assertTrue(result.get().isInstanceOf[IllegalArgumentException])
    assertSame(draft, registry.retainedWitnessDraft(draft.tag).get)
    assertFalse(draft.originalRoot.isClosed)

  @Test def actualProducerRetainsCompletedReserveAndPartialExecuteOnBudgetFailure(): Unit =
    // 两次真实materializer固定输入。第一次只取得真实accounted阶段费用，非镜像预算oracle。
    val measuredImages = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
    val measuredFixture = measuredImages.fixture
    val measuredRoot = measuredImages.before.allocationBudget
    measuredRoot.reserveRetainedMemory(512L, "test actual producer draft instance")
    val measured = new MySqlModeOriginalWitnessDraft(measuredFixture.plan, measuredFixture.operation,
      measuredFixture.context, measuredRoot)
    val measuredRegistry = this.registry(measured)
    measuredRegistry.retainRoot(measured.tag, measuredRoot)
    measuredRegistry.retainWitnessDraft(measured.tag, measured)
    val measuredStart = measuredRoot.accountedMemoryBytes
    val fixedAt = java.time.LocalDateTime.of(2026, 10, 8, 17, 0, 0, 123456000)
    MySqlRecoveryCanonicalBytes.withAllocationBudget(measuredRoot) {
      MySqlModeWorkflowMaterializer.materialize(measuredFixture.plan, measuredFixture.operation,
        measuredFixture.context, fixedAt, measuredImages.before, 0L, measured)
    }
    val scaffoldIncrement = Math.subtractExact(measured.executeScaffoldSuccessfulReservePeakBytes, measuredStart)
    assertTrue(scaffoldIncrement > 0L)
    assertTrue(measured.reserveCompletedAccountedBytes > measuredStart)
    assertTrue(measured.executeScaffoldAccountedBytes > measured.reserveCompletedAccountedBytes)

    val images = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
    val f = images.fixture
    val root = images.before.allocationBudget
    root.reserveRetainedMemory(512L, "test actual producer draft instance")
    val draft = new MySqlModeOriginalWitnessDraft(f.plan, f.operation, f.context, root)
    val registry = this.registry(draft)
    registry.retainRoot(draft.tag, root)
    registry.retainWitnessDraft(draft.tag, draft)
    assertEquals(measuredStart, root.accountedMemoryBytes, "same actual fixture/root initial charge")
    val remaining = Math.addExact(scaffoldIncrement, 1L)
    root.reserveRetainedMemory(Math.subtractExact(root.availableMemoryBytes, remaining),
      "existing same-root obligation leaves room for actual Reserve/Execute scaffold only")
    val cause = assertThrows(classOf[SQLDataException], () =>
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        MySqlModeWorkflowMaterializer.materialize(f.plan, f.operation, f.context,
          fixedAt, images.before, 0L, draft)
      })
    val stack = cause.getStackTrace
    assertSame(cause, draft.retainedFailure.get)
    assertSame(draft, registry.retainedWitnessDraft(draft.tag).get)
    assertSame(root, draft.originalRoot)
    assertTrue(draft.producerSnapshot(MySqlModeWitnessProducerStage.ReserveTargetBase) != null)
    assertTrue(draft.producerCanonical(MySqlModeWitnessProducerStage.ReserveWitness) != null)
    assertTrue(draft.producerPhase(MySqlModeWitnessProducerStage.ReservePhase) != null)
    assertTrue(draft.producerSnapshot(MySqlModeWitnessProducerStage.ExecuteScaffold) != null)
    assertEquals(MySqlModeWitnessProducerStage.ExecuteScaffold, draft.producerStage,
      "actual failure must occur in Execute canonical allocation after completed Reserve/scaffold")
    val chargeAfterFailure = root.accountedMemoryBytes
    val rejection = assertThrows(classOf[IllegalStateException], () => registry.release(draft.tag))
    assertSame(cause, rejection.getCause)
    assertTrue(stack.sameElements(cause.getStackTrace))
    assertEquals(chargeAfterFailure, root.accountedMemoryBytes, "failed ordinary release must credit zero bytes")
    assertFalse(root.isClosed)
    assertFalse(draft.isDropped)
    assertEquals(1, registry.pendingCount)

  @Test def actualProducerNormalDropClearsPartialStageReferencesBeforeRootClose(): Unit =
    val images = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
    val f = images.fixture
    val root = images.before.allocationBudget
    root.reserveRetainedMemory(512L, "test actual producer draft instance")
    val draft = new MySqlModeOriginalWitnessDraft(f.plan, f.operation, f.context, root)
    val registry = this.registry(draft)
    registry.retainRoot(draft.tag, root)
    registry.retainWitnessDraft(draft.tag, draft)
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      MySqlModeWorkflowMaterializer.materialize(f.plan, f.operation, f.context,
        java.time.LocalDateTime.of(2026, 10, 8, 17, 0, 0, 123456000), images.before, 0L, draft)
    }
    assertTrue(draft.producerSnapshot(MySqlModeWitnessProducerStage.ExecuteTarget) != null)
    assertTrue(draft.producerCanonical(MySqlModeWitnessProducerStage.ExecuteWitness) != null)
    assertEquals(MySqlModeWitnessProducerStage.Materialization, draft.producerStage)
    assertTrue(draft.retainedProducerReferenceCount > 0)
    registry.release(draft.tag)
    assertTrue(draft.isDropped)
    assertEquals(0, draft.retainedProducerReferenceCount)
    assertTrue(root.isClosed)
    assertEquals(0, registry.pendingCount)

  @Test def nestedSnapshotIsRetainedBeforeHelperDigestBudgetFailure(): Unit =
    val measuredImages = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
    val mf = measuredImages.fixture
    val mr = measuredImages.before.allocationBudget
    mr.reserveRetainedMemory(512L, "test actual producer draft instance")
    val measured = new MySqlModeOriginalWitnessDraft(mf.plan, mf.operation, mf.context, mr)
    val measuredRegistry = this.registry(measured)
    measuredRegistry.retainRoot(measured.tag, mr)
    measuredRegistry.retainWitnessDraft(measured.tag, measured)
    val start = mr.accountedMemoryBytes
    val fixedAt = java.time.LocalDateTime.of(2026, 10, 8, 17, 0, 0, 123456000)
    MySqlRecoveryCanonicalBytes.withAllocationBudget(mr) {
      MySqlModeWorkflowMaterializer.materialize(mf.plan, mf.operation, mf.context,
        fixedAt, measuredImages.before, 0L, measured)
    }
    val prefix = Math.subtractExact(measured.reserveScaffoldWriterBeforeDigestAccountedBytes, start)
    assertTrue(prefix > 0L)

    val images = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
    val f = images.fixture
    val root = images.before.allocationBudget
    root.reserveRetainedMemory(512L, "test actual producer draft instance")
    val draft = new MySqlModeOriginalWitnessDraft(f.plan, f.operation, f.context, root)
    val registry = this.registry(draft)
    registry.retainRoot(draft.tag, root)
    registry.retainWitnessDraft(draft.tag, draft)
    assertEquals(start, root.accountedMemoryBytes)
    root.reserveRetainedMemory(Math.subtractExact(root.availableMemoryBytes, Math.addExact(prefix, 1L)),
      "existing original-root obligation leaves one byte at nested snapshot digest")
    val cause = assertThrows(classOf[SQLDataException], () =>
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        MySqlModeWorkflowMaterializer.materialize(f.plan, f.operation, f.context,
          fixedAt, images.before, 0L, draft)
      })
    val stack = cause.getStackTrace
    assertTrue(cause.getMessage.contains("format3 SHA-256 workspace"))
    val held = draft.producerSnapshot(MySqlModeWitnessProducerStage.ReserveScaffoldWriterBeforeDigest)
    assertNotNull(held)
    assertNotSame(images.before, held)
    assertSame(root, held.allocationBudget)
    assertNull(draft.producerSnapshot(MySqlModeWitnessProducerStage.ReserveScaffoldWriter),
      "the nested helper must fail before its caller can register a returned image")
    assertEquals(MySqlModeWitnessProducerStage.ReserveScaffoldWriterBeforeDigest, draft.producerStage)
    assertSame(draft, registry.retainedWitnessDraft(draft.tag).get)
    assertSame(cause, draft.retainedFailure.get)
    val charge = root.accountedMemoryBytes
    val rejection = assertThrows(classOf[IllegalStateException], () => registry.release(draft.tag))
    assertSame(cause, rejection.getCause)
    assertTrue(stack.sameElements(cause.getStackTrace))
    assertEquals(charge, root.accountedMemoryBytes)
    assertFalse(root.isClosed)
    assertFalse(draft.isDropped)
    assertEquals(1, registry.pendingCount)
