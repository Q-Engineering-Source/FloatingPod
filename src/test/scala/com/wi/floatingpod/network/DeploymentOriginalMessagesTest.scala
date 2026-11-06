package com.wi.floatingpod.network

import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.*
import io.netty.buffer.{ByteBuf, Unpooled}
import net.minecraftforge.fml.common.network.simpleimpl.IMessage
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

import java.util.{Optional, UUID}

/** 真实 ByteBuf 与原 codec 的消息边界测试，不实例化世界、玩家或数据库。 */
class DeploymentOriginalMessagesTest:
  private val operation = new UUID(1L, 2L)
  private def query = new QueryOriginalRequest(operation)
  private def missing = new QueryOriginalResult(operation, LookupStatus.NOT_FOUND, Optional.empty[MutationReceipt]())
  private def receipt = new MutationReceipt(operation, new UUID(3L, 4L), new UUID(5L, 6L), 7L,
    MutationKind.DEPLOY, new ExpectedCommandVersions(new UUID(8L, 9L), 10L, new UUID(11L, 12L), 13L, 14L, 15L, 16L),
    OperationPhase.QUEUED, ReceiptReason.NONE, ActualEffectStatus.NOT_STARTED,
    CriticalCommitStatus.NOT_ATTEMPTED, ReadinessStatus.NOT_EVALUATED, CleanupStatus.NOT_REQUIRED)
  private def found = new QueryOriginalResult(operation, LookupStatus.FOUND, Optional.of(receipt))

  @Test def queryRoundTripsExactCodecBytes(): Unit =
    val original = query
    val expected = DeploymentCommandWireCodec.encodeRequest(original)
    assertEquals(22, expected.length)
    withBuffer(Unpooled.buffer()) { output =>
      new C2SDeploymentOriginalQuery(original).toBytes(output)
      assertArrayEquals(expected, readableCopy(output))
      val decoded = new C2SDeploymentOriginalQuery()
      decoded.fromBytes(output)
      assertEquals(0, output.readableBytes())
      assertEquals(original, decoded.request)
      assertArrayEquals(expected, encoded(decoded))
    }

  @Test def notFoundRoundTripsExactCodecBytes(): Unit =
    resultRoundTrip(missing, 23)

  @Test def foundRoundTripsExactOriginalReceiptBytes(): Unit =
    resultRoundTrip(found, 143)

  @Test def invalidLengthsAndTrailingBytesRejectBeforeReading(): Unit =
    for length <- List(0, 1, 21, 23, 134, 144) do
      withBuffer(Unpooled.wrappedBuffer(new Array[Byte](length))) { input =>
        val before = input.readerIndex()
        assertThrows(classOf[IllegalArgumentException], () => new C2SDeploymentOriginalQuery().fromBytes(input))
        assertEquals(before, input.readerIndex())
      }
    for length <- List(0, 1, 22, 24, 142, 144) do
      withBuffer(Unpooled.wrappedBuffer(new Array[Byte](length))) { input =>
        val before = input.readerIndex()
        assertThrows(classOf[IllegalArgumentException], () => new S2CDeploymentOriginalResult().fromBytes(input))
        assertEquals(before, input.readerIndex())
      }
    rejectQuery(DeploymentCommandWireCodec.encodeRequest(query) ++ Array[Byte](0))
    rejectResult(DeploymentCommandWireCodec.encodeResponse(found) ++ Array[Byte](0))
    // 尾随数据恰好补成另一个允许长度时，仍须由原 codec 拒绝。
    rejectResult(DeploymentCommandWireCodec.encodeResponse(missing) ++ new Array[Byte](120))

  @Test def otherCommandKindsCannotEnterQueryMessages(): Unit =
    val original = receipt
    val deploy = new DeployRequest(operation, original.podUuid(), original.moduleInstanceId(),
      original.moduleGeneration(), original.expected())
    val retract = new RetractRequest(operation, original.podUuid(), original.moduleInstanceId(),
      original.moduleGeneration(), original.expected())
    for request <- List(deploy, retract) do
      val bytes = DeploymentCommandWireCodec.encodeRequest(request)
      rejectQuery(bytes)
      rejectQuery(bytes.take(22))
    val failure = new ProtocolFailure(Optional.of(new UntrustedCorrelation(operation)), StructuralCode.BAD_MAGIC)
    val notice = new CommandNotice(operation, NoticeReason.AUTHORIZATION_DENIED)
    for response <- List(original, notice, failure) do
      rejectResult(DeploymentCommandWireCodec.encodeResponse(response))
    rejectResult(DeploymentCommandWireCodec.encodeResponse(original) ++ Array[Byte](0))

  @Test def malformedFramesRetainCodecStageAndCode(): Unit =
    val badMagic = DeploymentCommandWireCodec.encodeRequest(query)
    badMagic(0) = 0
    val queryFailure = rejectQuery(badMagic)
    assertTrue(queryFailure.getMessage.contains("PREFIX/BAD_MAGIC"))
    val badVersion = DeploymentCommandWireCodec.encodeRequest(query)
    badVersion(4) = 99
    rejectQuery(badVersion)
    val badKind = DeploymentCommandWireCodec.encodeRequest(query)
    badKind(5) = 99
    rejectQuery(badKind)
    val badStatus = DeploymentCommandWireCodec.encodeResponse(missing)
    badStatus(22) = 99
    val resultFailure = rejectResult(badStatus)
    assertTrue(resultFailure.getMessage.contains("RESPONSE_BODY/UNKNOWN_KIND_OR_CODE"))
    val badReceipt = DeploymentCommandWireCodec.encodeResponse(found)
    badReceipt(142) = 99
    rejectResult(badReceipt)

  @Test def failedDecodeNeverPublishesPartialOrDefaultValues(): Unit =
    val originalQuery = query
    val originalResult = found
    val queryMessage = new C2SDeploymentOriginalQuery(originalQuery)
    val resultMessage = new S2CDeploymentOriginalResult(originalResult)
    val badQuery = DeploymentCommandWireCodec.encodeRequest(query)
    badQuery(0) = 0
    val badResult = DeploymentCommandWireCodec.encodeResponse(missing)
    badResult(22) = 99
    withBuffer(Unpooled.wrappedBuffer(badQuery)) { input =>
      assertThrows(classOf[IllegalArgumentException], () => queryMessage.fromBytes(input))
    }
    withBuffer(Unpooled.wrappedBuffer(badResult)) { input =>
      assertThrows(classOf[IllegalArgumentException], () => resultMessage.fromBytes(input))
    }
    assertSame(originalQuery, queryMessage.request)
    assertSame(originalResult, resultMessage.result)
    val emptyQuery = new C2SDeploymentOriginalQuery()
    val emptyResult = new S2CDeploymentOriginalResult()
    withBuffer(Unpooled.wrappedBuffer(badQuery)) { input =>
      assertThrows(classOf[IllegalArgumentException], () => emptyQuery.fromBytes(input))
    }
    withBuffer(Unpooled.wrappedBuffer(badResult)) { input =>
      assertThrows(classOf[IllegalArgumentException], () => emptyResult.fromBytes(input))
    }
    assertThrows(classOf[IllegalStateException], () => emptyQuery.request)
    assertThrows(classOf[IllegalStateException], () => emptyResult.result)

  @Test def uninitializedMessagesRejectEncodingWithoutWriting(): Unit =
    withBuffer(Unpooled.buffer()) { output =>
      assertThrows(classOf[IllegalStateException], () => new C2SDeploymentOriginalQuery().toBytes(output))
      assertThrows(classOf[IllegalStateException], () => new S2CDeploymentOriginalResult().toBytes(output))
      assertEquals(0, output.writerIndex())
    }
    assertThrows(classOf[IllegalArgumentException], () => new C2SDeploymentOriginalQuery(null))
    assertThrows(classOf[IllegalArgumentException], () => new S2CDeploymentOriginalResult(null))

  private def resultRoundTrip(original: QueryOriginalResult, length: Int): Unit =
    val expected = DeploymentCommandWireCodec.encodeResponse(original)
    assertEquals(length, expected.length)
    withBuffer(Unpooled.buffer()) { output =>
      new S2CDeploymentOriginalResult(original).toBytes(output)
      assertArrayEquals(expected, readableCopy(output))
      val decoded = new S2CDeploymentOriginalResult()
      decoded.fromBytes(output)
      assertEquals(0, output.readableBytes())
      assertEquals(original, decoded.result)
      assertArrayEquals(expected, encoded(decoded))
    }

  private def rejectQuery(bytes: Array[Byte]): IllegalArgumentException =
    withBuffer(Unpooled.wrappedBuffer(bytes)) { input =>
      assertThrows(classOf[IllegalArgumentException], () => new C2SDeploymentOriginalQuery().fromBytes(input))
    }

  private def rejectResult(bytes: Array[Byte]): IllegalArgumentException =
    withBuffer(Unpooled.wrappedBuffer(bytes)) { input =>
      assertThrows(classOf[IllegalArgumentException], () => new S2CDeploymentOriginalResult().fromBytes(input))
    }

  private def readableCopy(input: ByteBuf): Array[Byte] =
    val bytes = new Array[Byte](input.readableBytes())
    input.getBytes(input.readerIndex(), bytes)
    bytes

  private def encoded(message: IMessage): Array[Byte] =
    withBuffer(Unpooled.buffer()) { output =>
      message.toBytes(output)
      readableCopy(output)
    }

  private def withBuffer[A](buffer: ByteBuf)(body: ByteBuf => A): A =
    try body(buffer)
    finally buffer.release()

object DeploymentOriginalMessagesTest:
  /** 仅运行本切片新增用例，后续放行后与 JUnit 使用同一组真实行为。 */
  def main(args: Array[String]): Unit =
    val tests = new DeploymentOriginalMessagesTest()
    tests.queryRoundTripsExactCodecBytes()
    tests.notFoundRoundTripsExactCodecBytes()
    tests.foundRoundTripsExactOriginalReceiptBytes()
    tests.invalidLengthsAndTrailingBytesRejectBeforeReading()
    tests.otherCommandKindsCannotEnterQueryMessages()
    tests.malformedFramesRetainCodecStageAndCode()
    tests.failedDecodeNeverPublishesPartialOrDefaultValues()
    tests.uninitializedMessagesRejectEncodingWithoutWriting()
    println("DeploymentOriginalMessagesTest: PASS (8 directed message behavior cases)")
