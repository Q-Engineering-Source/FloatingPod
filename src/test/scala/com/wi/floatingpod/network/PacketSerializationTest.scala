package com.wi.floatingpod.network

import io.netty.buffer.{ByteBuf, Unpooled}

import net.minecraft.util.math.BlockPos
import net.minecraftforge.fml.common.network.simpleimpl.IMessage

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import java.util.UUID

/** 验证协议值在 Forge ByteBuf 边界上的首个可互通 tracer。 */
class PacketSerializationTest:
  @Test
  def releaseRequestRoundTripsUuidPackedPositionAndExpectedVersion(): Unit =
    val expectedUuid = UUID.fromString("00000000-0000-0000-0000-000000000123")
    val expectedPosition = new BlockPos(12, 64, -8)
    val encoded = Unpooled.buffer()

    new C2SReleaseRequest(expectedUuid, expectedPosition, 7L).toBytes(encoded)

    val decoded = new C2SReleaseRequest()
    decoded.fromBytes(encoded)

    assertEquals(expectedUuid, decoded.podUuid)
    assertEquals(expectedPosition, decoded.targetPos)
    assertEquals(7L, decoded.expectedOwnerVersion)

  @Test
  def recallRequestRoundTripsUuidAndExpectedVersion(): Unit =
    val expectedUuid = UUID.fromString("00000000-0000-0000-0000-000000000124")
    val encoded = Unpooled.buffer()

    new C2SRecallRequest(expectedUuid, 8L).toBytes(encoded)

    val decoded = new C2SRecallRequest()
    decoded.fromBytes(encoded)

    assertEquals(expectedUuid, decoded.podUuid)
    assertEquals(8L, decoded.expectedStateVersion)

  @Test
  def stateSyncRoundTripsContinuousCoordinatesRotationAndVersion(): Unit =
    val expectedUuid = UUID.fromString("00000000-0000-0000-0000-000000000125")
    val encoded = Unpooled.buffer()

    new S2CStateSync(expectedUuid, 12.5D, 64.25D, -8.75D, 90.0F, -15.0F, 9L).toBytes(encoded)

    val decoded = new S2CStateSync()
    decoded.fromBytes(encoded)

    assertEquals(expectedUuid, decoded.podUuid)
    assertEquals(12.5D, decoded.x)
    assertEquals(64.25D, decoded.y)
    assertEquals(-8.75D, decoded.z)
    assertEquals(90.0F, decoded.yaw)
    assertEquals(-15.0F, decoded.pitch)
    assertEquals(9L, decoded.stateVersion)

  @Test
  def releaseResultRoundTripsBothSuccessAndBoundedFailureShapes(): Unit =
    val expectedUuid = UUID.fromString("00000000-0000-0000-0000-000000000126")
    val successBuffer = Unpooled.buffer()
    val failureBuffer = Unpooled.buffer()

    new S2CReleaseResult(true, expectedUuid, Some(10L), None).toBytes(successBuffer)
    new S2CReleaseResult(false, expectedUuid, None, Some(ReleaseFailureReason.CapacityReached)).toBytes(failureBuffer)

    val decodedSuccess = new S2CReleaseResult()
    decodedSuccess.fromBytes(successBuffer)
    val decodedFailure = new S2CReleaseResult()
    decodedFailure.fromBytes(failureBuffer)

    assertEquals(Some(10L), decodedSuccess.newStateVersion)
    assertEquals(None, decodedSuccess.reason)
    assertEquals(None, decodedFailure.newStateVersion)
    assertEquals(Some(ReleaseFailureReason.CapacityReached), decodedFailure.reason)

  @Test
  def recallResultRoundTripsBothSuccessAndBoundedFailureShapes(): Unit =
    val expectedUuid = UUID.fromString("00000000-0000-0000-0000-000000000127")
    val successBuffer = Unpooled.buffer()
    val failureBuffer = Unpooled.buffer()

    new S2CRecallResult(true, expectedUuid, Some(11L), None).toBytes(successBuffer)
    new S2CRecallResult(false, expectedUuid, None, Some(RecallFailureReason.NotActive)).toBytes(failureBuffer)

    val decodedSuccess = new S2CRecallResult()
    decodedSuccess.fromBytes(successBuffer)
    val decodedFailure = new S2CRecallResult()
    decodedFailure.fromBytes(failureBuffer)

    assertEquals(Some(11L), decodedSuccess.newStateVersion)
    assertEquals(None, decodedSuccess.reason)
    assertEquals(None, decodedFailure.newStateVersion)
    assertEquals(Some(RecallFailureReason.NotActive), decodedFailure.reason)

  @Test
  def decodersRejectProtocolMismatchesTruncationAndTrailingPayload(): Unit =
    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000128")
    val mismatch = Unpooled.buffer().writeByte(2)
    val truncated = Unpooled.buffer().writeByte(1).writeLong(uuid.getMostSignificantBits)
    val trailing = Unpooled.buffer()
    new C2SRecallRequest(uuid, 12L).toBytes(trailing)
    trailing.writeByte(0)

    assertThrows(classOf[RuntimeException], () => new C2SReleaseRequest().fromBytes(mismatch))
    assertThrows(classOf[RuntimeException], () => new C2SReleaseRequest().fromBytes(truncated))
    assertThrows(classOf[RuntimeException], () => new C2SRecallRequest().fromBytes(trailing))

  @Test
  def resultCodecsRejectInconsistentOptionalShapesAndUnknownReasons(): Unit =
    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000129")
    val malformed = Unpooled.buffer()
    malformed.writeByte(1)
    malformed.writeBoolean(false)
    malformed.writeLong(uuid.getMostSignificantBits)
    malformed.writeLong(uuid.getLeastSignificantBits)
    malformed.writeBoolean(false)
    malformed.writeBoolean(true)
    malformed.writeByte(99)

    assertThrows(classOf[IllegalArgumentException], () => new S2CReleaseResult(true, uuid, None, None).toBytes(Unpooled.buffer()))
    assertThrows(classOf[IllegalArgumentException], () => new S2CReleaseResult(true, uuid, Some(3L), Some(ReleaseFailureReason.NotOwner)).toBytes(Unpooled.buffer()))
    assertThrows(classOf[IllegalArgumentException], () => new S2CRecallResult(false, uuid, Some(3L), Some(RecallFailureReason.NotActive)).toBytes(Unpooled.buffer()))
    assertThrows(classOf[RuntimeException], () => new S2CReleaseResult().fromBytes(malformed))

  @Test
  def resultCodecsRejectNonCanonicalWireBooleans(): Unit =
    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000131")
    val releaseSuccess = Unpooled.buffer()
    releaseSuccess.writeByte(PacketHandler.PROTOCOL_VERSION)
    releaseSuccess.writeByte(2)
    releaseSuccess.writeLong(uuid.getMostSignificantBits)
    releaseSuccess.writeLong(uuid.getLeastSignificantBits)
    releaseSuccess.writeByte(1)
    releaseSuccess.writeLong(3L)
    releaseSuccess.writeByte(0)
    val recallReasonPresence = Unpooled.buffer()
    recallReasonPresence.writeByte(PacketHandler.PROTOCOL_VERSION)
    recallReasonPresence.writeByte(0)
    recallReasonPresence.writeLong(uuid.getMostSignificantBits)
    recallReasonPresence.writeLong(uuid.getLeastSignificantBits)
    recallReasonPresence.writeByte(0)
    recallReasonPresence.writeByte(2)
    recallReasonPresence.writeByte(RecallFailureReason.NotActive.wireId)

    assertThrows(classOf[IllegalArgumentException], () => new S2CReleaseResult().fromBytes(releaseSuccess))
    assertThrows(classOf[IllegalArgumentException], () => new S2CRecallResult().fromBytes(recallReasonPresence))

  @Test
  def everyPacketFamilyRejectsMalformedTruncatedAndTrailingPayloads(): Unit =
    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000132")
    val decoders = List[ByteBuf => Unit](
      input => new C2SReleaseRequest().fromBytes(input),
      input => new C2SRecallRequest().fromBytes(input),
      input => new S2CStateSync().fromBytes(input),
      input => new S2CReleaseResult().fromBytes(input),
      input => new S2CRecallResult().fromBytes(input)
    )
    val validPayloads = List(
      encoded(new C2SReleaseRequest(uuid, new BlockPos(1, 64, 1), 1L)),
      encoded(new C2SRecallRequest(uuid, 1L)),
      encoded(new S2CStateSync(uuid, 1.0D, 64.0D, 1.0D, 0.0F, 0.0F, 1L)),
      encoded(S2CReleaseResult.succeeded(uuid, 1L)),
      encoded(S2CRecallResult.succeeded(uuid, 1L))
    )

    decoders.foreach(decoder => {
      assertThrows(classOf[RuntimeException], () => decoder(Unpooled.buffer().writeByte(2)))
      assertThrows(classOf[RuntimeException], () => decoder(Unpooled.buffer().writeByte(PacketHandler.PROTOCOL_VERSION)))
    })
    decoders.zip(validPayloads).foreach { case (decoder, payload) =>
      payload.writeByte(0)
      assertThrows(classOf[RuntimeException], () => decoder(payload))
    }

  @Test
  def everyPacketFamilyRejectsDecodedNegativeVersionsAndUnknownResultReasons(): Unit =
    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000133")
    val releaseRequest = Unpooled.buffer().writeByte(PacketHandler.PROTOCOL_VERSION).writeLong(uuid.getMostSignificantBits).writeLong(uuid.getLeastSignificantBits).writeLong(new BlockPos(1, 64, 1).toLong).writeLong(-1L)
    val recallRequest = Unpooled.buffer().writeByte(PacketHandler.PROTOCOL_VERSION).writeLong(uuid.getMostSignificantBits).writeLong(uuid.getLeastSignificantBits).writeLong(-1L)
    val sync = Unpooled.buffer().writeByte(PacketHandler.PROTOCOL_VERSION).writeLong(uuid.getMostSignificantBits).writeLong(uuid.getLeastSignificantBits).writeDouble(1.0D).writeDouble(64.0D).writeDouble(1.0D).writeFloat(0.0F).writeFloat(0.0F).writeLong(-1L)
    val releaseResult = successfulResultWithVersion(uuid, -1L)
    val recallResult = successfulResultWithVersion(uuid, -1L)
    val releaseUnknown = failedResultWithReason(uuid, 99)
    val recallUnknown = failedResultWithReason(uuid, 99)

    assertThrows(classOf[IllegalArgumentException], () => new C2SReleaseRequest().fromBytes(releaseRequest))
    assertThrows(classOf[IllegalArgumentException], () => new C2SRecallRequest().fromBytes(recallRequest))
    assertThrows(classOf[IllegalArgumentException], () => new S2CStateSync().fromBytes(sync))
    assertThrows(classOf[IllegalArgumentException], () => new S2CReleaseResult().fromBytes(releaseResult))
    assertThrows(classOf[IllegalArgumentException], () => new S2CRecallResult().fromBytes(recallResult))
    assertThrows(classOf[IllegalArgumentException], () => new S2CReleaseResult().fromBytes(releaseUnknown))
    assertThrows(classOf[IllegalArgumentException], () => new S2CRecallResult().fromBytes(recallUnknown))

  @Test
  def stateSyncRejectsNonFiniteCoordinates(): Unit =
    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000130")

    assertThrows(classOf[IllegalArgumentException], () =>
      new S2CStateSync(uuid, Double.NaN, 64.0D, 0.0D, 0.0F, 0.0F, 1L).toBytes(Unpooled.buffer())
    )

  private def encoded(message: IMessage): ByteBuf =
    val output = Unpooled.buffer()
    message.toBytes(output)
    output

  private def successfulResultWithVersion(uuid: UUID, version: Long): ByteBuf =
    Unpooled.buffer().writeByte(PacketHandler.PROTOCOL_VERSION).writeByte(1).writeLong(uuid.getMostSignificantBits).writeLong(uuid.getLeastSignificantBits).writeByte(1).writeLong(version).writeByte(0)

  private def failedResultWithReason(uuid: UUID, reason: Int): ByteBuf =
    Unpooled.buffer().writeByte(PacketHandler.PROTOCOL_VERSION).writeByte(0).writeLong(uuid.getMostSignificantBits).writeLong(uuid.getLeastSignificantBits).writeByte(0).writeByte(1).writeByte(reason)
