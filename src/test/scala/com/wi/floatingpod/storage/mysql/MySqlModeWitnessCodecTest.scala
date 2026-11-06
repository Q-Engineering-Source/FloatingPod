package com.wi.floatingpod.server {
  /** 复用原 pure B1 DATA fixture，不发行 native/private SQL plan。 */
  private[floatingpod] object ModeWitnessSourceFixtureBridge {
    def record(): B1ResumeRecord = B1TwoSlotFixture.record(0L, new Array[Byte](32),
      B1ResumeRecord.Kind.GENESIS, B1TwoSlotFixture.genesis(B1TwoSlotFixture.pureOwner()))
  }
}

package com.wi.floatingpod.storage.mysql {
  import com.wi.floatingpod.server.{B1ResumeRecordCanonical, ModeWitnessSourceFixtureBridge}
  import java.io.{ByteArrayOutputStream, DataOutputStream}
  import java.security.MessageDigest
  import java.sql.SQLDataException
  import java.time.LocalDateTime
  import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
  import org.junit.jupiter.api.Test

  private[mysql] object ModeWitnessDataFixtures {
    def draft(): MySqlModeOriginalWitnessDraft = {
      val images = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
      draftFrom(images)
    }
    def compatibilityDraft(): MySqlModeOriginalWitnessDraft = {
      // 既有HistoricalBusinessReceiptTest.data profile；仅旧编码兼容，不替代8MB容量验收。
      val profile = com.wi.floatingpod.config.SourceLimitProfile.of(536870912L,16777216L,134217728L,
        64L,256L,536870912L,67108864L,536870912L)
      draftFrom(new MySqlModeOperationEffectCodecTest().modeWorkflowImages(profile))
    }
    private def draftFrom(images: MySqlModeOperationEffectCodecTest#ModeWorkflowImages): MySqlModeOriginalWitnessDraft = {
      val f = images.fixture
      val root = images.before.allocationBudget
      MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        root.reserveRetainedMemory(512L, "test DATA draft holder")
        val result = new MySqlModeOriginalWitnessDraft(f.plan, f.operation, f.context, root)
        val value = MySqlModeWorkflowMaterializer.materialize(f.plan, f.operation, f.context,
          LocalDateTime.of(2026, 10, 8, 17, 0, 0, 123456000), images.before, 0L, result)
        value.originalLocatorRevision = 0L
        result
      }
    }
    def source(root: MySqlFormat3ReadBudget): MySqlModeWitnessCodec.SourceDescriptor = {
      val record = ModeWitnessSourceFixtureBridge.record()
      val canonical = B1ResumeRecordCanonical.encode(record)
      val receipt = record.originalReceipt()
      val predecessor = MySqlFormat3Binary.copyOf(canonical, root, "pure source predecessor DATA")
      val original = MySqlFormat3Binary.copyOf(receipt, root, "pure source receipt DATA")
      MySqlModeWitnessCodec.SourceDescriptor(4, record.sequence(), 1L, Vector.fill(16)(1.toByte), predecessor,
        MessageDigest.getInstance("SHA-256").digest(canonical).toVector,
        record.sourceAnchorSha256().toVector, original)
    }
  }

  final class MySqlModeWitnessCodecTest {
    @Test def frozenOriginalV1StillDecodesAfterVersionedRepair(): Unit = {
      val profile = com.wi.floatingpod.config.SourceLimitProfile.of(536870912L,16777216L,134217728L,
        64L,256L,536870912L,67108864L,536870912L)
      val root = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
      val directory = java.nio.file.Path.of("D:/WI - Dev Workspace/FloatingPod-test/_evidence/FP-REPAIR-20261006/MAINLINE-EXCEPTION-BOUNDARY-20261021-v1/normal-stop-handoff/normal-recall-r1/crossboot-A/mode-frozen-v1-compatibility")
      val bytes = java.nio.file.Files.readAllBytes(directory.resolve("artifact.bin"))
      root.reserveRetainedMemory(bytes.length.toLong + 64L, "frozen V1 test input")
      val decoded = MySqlModeWitnessCodec.decode(MySqlModeWitnessCodec.capture(bytes, root), MySqlModeWitnessCodec.newDecodeDraft(root))
      decoded.effects.zipWithIndex.foreach { (effect, index) =>
        assertEquals(MySqlModeProjectionVersion.V1, MySqlModeOperationEffectCodec.projectionVersion(effect.preimage))
        assertTrue(effect.preimage.borrowedBinary.equalsBytes(java.nio.file.Files.readAllBytes(directory.resolve(s"phase$index-before.bin"))))
        assertTrue(effect.target.borrowedBinary.equalsBytes(java.nio.file.Files.readAllBytes(directory.resolve(s"phase$index-target.bin"))))
      }
    }
    @Test def wholeThreeImagesAndOrderedTwoPhasesStrictlyRoundTrip(): Unit = {
      val draft = ModeWitnessDataFixtures.compatibilityDraft()
      val root = draft.originalRoot
      val encoded = MySqlModeWitnessCodec.encode(ModeWitnessDataFixtures.source(root), draft)
      val coldRoot = new MySqlFormat3ReadBudget(root.profile, root.profile.maxSqlPacketBytes())
      val originalInput = encoded.bytes.copyBytes(coldRoot, "actual cold input retained before capture")
      val captured = MySqlModeWitnessCodec.capture(originalInput, coldRoot)
      val cold = MySqlModeWitnessCodec.newDecodeDraft(coldRoot)
      assertEquals(0, coldRoot.retainedCanonicalDataChildCount)
      val decoded = MySqlModeWitnessCodec.decode(captured, cold)
      assertEquals(3, decoded.images.size)
      assertEquals(20, decoded.images.head.relations.size)
      assertTrue(decoded.images(0).exactlyEquals(draft.value.preimage))
      assertTrue(decoded.images(1).exactlyEquals(draft.value.reserveTarget))
      assertTrue(decoded.images(2).exactlyEquals(draft.value.executeTarget))
      assertEquals(Vector(MySqlOperationEffectPhase.Reserve, MySqlOperationEffectPhase.Execute), decoded.effects.map(_.phase))
      val reserve = decoded.effects(0).rawOperationEnvelopes
      val executeBefore = decoded.effects(1).preimageRawOperationEnvelopes
      assertEquals(reserve.map(_.operationUuid), executeBefore.map(_.operationUuid))
      reserve.zip(executeBefore).foreach { (left, right) =>
        assertEquals(left.row, right.row)
        assertTrue(left.canonical.exactlyEquals(right.canonical))
      }
      assertFalse(root.isClosed)
      assertEquals(3, cold.decodedImageCount)
      assertEquals(3, coldRoot.retainedCanonicalDataChildCount)
      val accounted = coldRoot.accountedMemoryBytes
      assertThrows(classOf[IllegalArgumentException], () => coldRoot.closeRootReadBudget())
      assertFalse(coldRoot.isClosed)
      assertEquals(3, coldRoot.retainedCanonicalDataChildCount)
      assertEquals(accounted, coldRoot.accountedMemoryBytes)
    }

    @Test def actualCompleteHistoryEnvelopeExceedsReceiptSizeWithoutPadding(): Unit = {
      val draft = ModeWitnessDataFixtures.compatibilityDraft()
      val encoded = MySqlModeWitnessCodec.encode(ModeWitnessDataFixtures.source(draft.originalRoot), draft)
      // full20 schema/actual typed historical O rows + 3 images/2 phases，而非填充任意bytes。
      assertTrue(encoded.bytes.length > 65536, "actual complete envelope length=" + encoded.bytes.length)
      val decoded = MySqlModeWitnessCodec.decode(encoded, MySqlModeWitnessCodec.newDecodeDraft(draft.originalRoot))
      assertEquals(draft.value.preimage.relation("fp_recovery_operation").rows,
        decoded.images.head.relation("fp_recovery_operation").rows)
    }

    @Test def actualDateTimeWriterIsThirteenBytesAndRejectsSubMicrosecond(): Unit = {
      val stream = new ByteArrayOutputStream()
      val out = new DataOutputStream(stream)
      val timestamp = LocalDateTime.of(2026, 10, 8, 17, 3, 4, 123456000)
      MySqlModeWitnessCodec.writeDateTime(out, timestamp)
      assertEquals(13, stream.size())
      val expected = new ByteArrayOutputStream()
      val writer = new DataOutputStream(expected)
      writer.writeInt(2026); writer.writeByte(10); writer.writeByte(8); writer.writeByte(17)
      writer.writeByte(3); writer.writeByte(4); writer.writeInt(123456)
      assertTrue(stream.toByteArray.sameElements(expected.toByteArray))
      assertThrows(classOf[SQLDataException], () => MySqlModeWitnessCodec.writeDateTime(out, timestamp.plusNanos(1L)))
    }

    @Test def trailingBytesRejectAndRetainCompletedTypedImagesAndOriginalCause(): Unit = {
      val draft = ModeWitnessDataFixtures.compatibilityDraft()
      val root = draft.originalRoot
      val good = MySqlModeWitnessCodec.encode(ModeWitnessDataFixtures.source(root), draft)
      val bytes = good.bytes.copyBytes(root, "test trailing DATA")
      val bad = MySqlModeWitnessCodec.capture(java.util.Arrays.copyOf(bytes, bytes.length + 1), root)
      val cold = MySqlModeWitnessCodec.newDecodeDraft(root)
      val cause = assertThrows(classOf[SQLDataException], () => MySqlModeWitnessCodec.decode(bad, cold))
      assertSame(cause, cold.retainedFailure.get)
      assertEquals(3, cold.decodedImageCount)
      assertTrue(cold.retainedBlobCount > 0)
      assertFalse(root.isClosed)
    }

    @Test def truncatedPayloadFailsExplicitlyWithoutClosingOriginalRoot(): Unit = {
      val draft = ModeWitnessDataFixtures.compatibilityDraft()
      val root = draft.originalRoot
      val good = MySqlModeWitnessCodec.encode(ModeWitnessDataFixtures.source(root), draft)
      val bytes = good.bytes.copyBytes(root, "test malformed DATA")
      val bad = MySqlModeWitnessCodec.capture(java.util.Arrays.copyOf(bytes, bytes.length / 2), root)
      val cold = MySqlModeWitnessCodec.newDecodeDraft(root)
      val cause = assertThrows(classOf[Throwable], () => MySqlModeWitnessCodec.decode(bad, cold))
      assertSame(cause, cold.retainedFailure.get)
      assertFalse(root.isClosed)
    }

    @Test def profileBytesTamperIsNotAcceptedByDataDecoder(): Unit = {
      val draft = ModeWitnessDataFixtures.compatibilityDraft()
      val root = draft.originalRoot
      val good = MySqlModeWitnessCodec.encode(ModeWitnessDataFixtures.source(root), draft)
      val bytes = good.bytes.copyBytes(root, "profile tamper DATA")
      val profileStart = 4 + MySqlModeWitnessCodec.Domain.getBytes(java.nio.charset.StandardCharsets.UTF_8).length + 4
      bytes(profileStart + 10) = (bytes(profileStart + 10) ^ 1).toByte
      val bad = MySqlModeWitnessCodec.capture(bytes, root)
      val cold = MySqlModeWitnessCodec.newDecodeDraft(root)
      val cause = assertThrows(classOf[SQLDataException], () => MySqlModeWitnessCodec.decode(bad, cold))
      assertTrue(cause.getMessage.contains("profile"))
      assertSame(cause, cold.retainedFailure.get)
      assertEquals(0, cold.decodedImageCount)
    }

    @Test def foreignOriginalRootCannotBorrowPayload(): Unit = {
      val first = ModeWitnessDataFixtures.compatibilityDraft()
      val encoded = MySqlModeWitnessCodec.encode(ModeWitnessDataFixtures.source(first.originalRoot), first)
      val second = ModeWitnessDataFixtures.compatibilityDraft()
      assertThrows(classOf[IllegalArgumentException], () => MySqlModeWitnessCodec.decode(encoded,
        MySqlModeWitnessCodec.newDecodeDraft(second.originalRoot)))
      assertFalse(first.originalRoot.isClosed)
    }
    @Test def closedOriginalRootRejectsBeforeDecoding(): Unit = {
      val draft = ModeWitnessDataFixtures.compatibilityDraft()
      val root = draft.originalRoot
      val original = MySqlModeWitnessCodec.encode(ModeWitnessDataFixtures.source(root), draft)
      val closedRoot = new MySqlFormat3ReadBudget(root.profile, root.profile.maxSqlPacketBytes())
      val bytes = original.bytes.copyBytes(root, "closed root test original canonical copy")
      val encoded = MySqlModeWitnessCodec.capture(bytes, closedRoot)
      val cold = MySqlModeWitnessCodec.newDecodeDraft(closedRoot)
      closedRoot.closeRootReadBudget()
      assertFalse(root.isClosed)
      assertThrows(classOf[IllegalArgumentException], () => MySqlModeWitnessCodec.decode(encoded, cold))
      assertEquals(0, cold.decodedImageCount)
    }

    @Test def semanticBlobMustMatchActualTargetOperationWitness(): Unit = {
      val draft = ModeWitnessDataFixtures.compatibilityDraft()
      val root = draft.originalRoot
      val encoded = MySqlModeWitnessCodec.encode(ModeWitnessDataFixtures.source(root), draft)
      val bytes = encoded.bytes.copyBytes(root, "test semantic DATA mutation")
      val needle = draft.value.originalExecuteWitness.copyBytes(root, "test exact semantic witness")
      var offset = bytes.length - needle.length
      var matched = false
      while offset >= 0 && !matched do {
        var index = 0
        while index < needle.length && bytes(offset + index) == needle(index) do index += 1
        matched = index == needle.length
        if !matched then offset -= 1
      }
      assertTrue(matched, "actual writer must embed the complete original Execute witness")
      bytes(offset + needle.length - 1) = (bytes(offset + needle.length - 1) ^ 1).toByte
      val bad = MySqlModeWitnessCodec.capture(bytes, root)
      val cold = MySqlModeWitnessCodec.newDecodeDraft(root)
      val cause = assertThrows(classOf[SQLDataException], () => MySqlModeWitnessCodec.decode(bad, cold))
      assertTrue(cause.getMessage.contains("full target O"))
      assertSame(cause, cold.retainedFailure.get)
      assertEquals(3, cold.decodedImageCount)
      assertFalse(root.isClosed)
    }
    @Test def actualEnvelopeStreamsOriginalThreeImagesAndKeepsOneFinalBinary(): Unit = {
      val draft = ModeWitnessDataFixtures.compatibilityDraft()
      val root = draft.originalRoot
      val m = draft.value
      val reserve = draft.producerPhase(MySqlModeWitnessProducerStage.ReservePhase)
      val execute = draft.producerPhase(MySqlModeWitnessProducerStage.ExecutePhase)
      val accounted = root.accountedMemoryBytes
      assertSame(reserve, MySqlModeOperationEffectCodec.requireRetainedCanonicalPhase(m.exactPlan, m.exactOperation,
        draft.originalContext, m.originalLocatorRevision, MySqlOperationEffectPhase.Reserve,
        m.preimage, m.reserveTarget, reserve, root))
      assertSame(execute, MySqlModeOperationEffectCodec.requireRetainedCanonicalPhase(m.exactPlan, m.exactOperation,
        draft.originalContext, m.originalLocatorRevision, MySqlOperationEffectPhase.Execute,
        m.reserveTarget, m.executeTarget, execute, root))
      assertEquals(accounted, root.accountedMemoryBytes)
      def rejectChangedPhase(changed: MySqlModePhaseCanonical): Unit = {
        assertThrows(classOf[SQLDataException], () => MySqlModeOperationEffectCodec.requireRetainedCanonicalPhase(
          m.exactPlan, m.exactOperation, draft.originalContext, m.originalLocatorRevision,
          MySqlOperationEffectPhase.Reserve, m.preimage, m.reserveTarget, changed, root))
        assertEquals(accounted, root.accountedMemoryBytes)
        assertFalse(root.isClosed)
      }
      rejectChangedPhase(reserve.copy(preimage = execute.preimage))
      rejectChangedPhase(reserve.copy(target = reserve.preimage))
      rejectChangedPhase(reserve.copy(scope = draft.originalContext.canonical))
      rejectChangedPhase(reserve.copy(fingerprint = execute.fingerprint))
      rejectChangedPhase(reserve.copy(operationContext = execute.operationContext))
      rejectChangedPhase(reserve.copy(operationContextBytes = execute.operationContextBytes))
      val encoded = MySqlModeWitnessCodec.encode(ModeWitnessDataFixtures.source(root), draft)
      assertSame(m.preimage, encoded.images(0))
      assertSame(m.reserveTarget, encoded.images(1))
      assertSame(m.executeTarget, encoded.images(2))
      assertSame(reserve, draft.producerPhase(MySqlModeWitnessProducerStage.ReservePhase))
      assertSame(execute, draft.producerPhase(MySqlModeWitnessProducerStage.ExecutePhase))
      assertSame(encoded.bytes, draft.retainedEncodingBinary)
      assertTrue(encoded.bytes.length > 65536)
      assertTrue(root.successfulReserveHighWaterBytes <= root.profile.maxRetainedBytes())
      assertFalse(root.isClosed)
    }

    @Test def actualSuccessfulReserveHighWaterKeepsReleasedTemporaryPeak(): Unit = {
      val images = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
      val root = images.before.allocationBudget
      val before = root.accountedMemoryBytes
      val oldPeak = root.successfulReserveHighWaterBytes
      val temporary = Math.addExact(Math.subtractExact(oldPeak, before), 1024L)
      root.reserveTemporaryMemory(temporary, "test actual temporary peak")
      val peak = root.successfulReserveHighWaterBytes
      assertEquals(Math.addExact(before, temporary), peak)
      root.releaseTemporaryMemory(temporary, "test exact temporary drop")
      assertEquals(before, root.accountedMemoryBytes)
      assertEquals(peak, root.successfulReserveHighWaterBytes)
      assertFalse(root.isClosed)
    }
  }
}
