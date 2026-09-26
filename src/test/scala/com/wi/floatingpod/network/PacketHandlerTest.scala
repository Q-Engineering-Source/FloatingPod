package net.minecraftforge.fml.common.network.simpleimpl {
  import net.minecraftforge.fml.relauncher.Side

  /** 仅供 M2 回归测试传递非空、可识别的 MessageContext。 */
  final class ControlledMessageContext extends MessageContext(null, Side.SERVER)
}

package com.wi.floatingpod.network {

import com.wi.floatingpod.FloatingPod

import io.netty.buffer.Unpooled

import net.minecraft.util.math.BlockPos
import net.minecraftforge.fml.common.network.simpleimpl.{ControlledMessageContext, IMessage, IMessageHandler, MessageContext}
import net.minecraftforge.fml.relauncher.Side

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.UUID
import scala.collection.mutable

/** 验证公共网络注册表的首个可观察 tracer。 */
class PacketHandlerTest:
  @Test
  def registersTheFiveStableMessagesWithTheirReceiveSides(): Unit =
    val registrar = new RecordingPacketRegistrar()

    new PacketHandler(registrar).registerOnce()

    assertEquals(
      List(
        (classOf[C2SReleaseRequest], 0, Side.SERVER),
        (classOf[C2SRecallRequest], 1, Side.SERVER),
        (classOf[S2CStateSync], 2, Side.CLIENT),
        (classOf[S2CReleaseResult], 3, Side.CLIENT),
        (classOf[S2CRecallResult], 4, Side.CLIENT)
      ),
      registrar.registrations.map(entry => (entry.messageType, entry.discriminator, entry.receivingSide))
    )

  @Test
  def registrationIsIdempotentAndExposesTheStableChannelAndProtocol(): Unit =
    val registrar = new RecordingPacketRegistrar()
    val handler = new PacketHandler(registrar)

    handler.registerOnce()
    handler.registerOnce()

    assertEquals("floatingpod", PacketHandler.CHANNEL_NAME)
    assertEquals(1, PacketHandler.PROTOCOL_VERSION)
    assertEquals(5, registrar.registrations.size)
    assertEquals(5, registrar.registrations.map(_.discriminator).distinct.size)

  @Test
  def connectionGateAcceptsOnlyTheExactVersionOnBothRemoteSides(): Unit =
    val exact = java.util.Map.of(FloatingPod.MOD_ID, FloatingPod.VERSION)
    val missing = java.util.Map.of[String, String]()
    val mismatch = java.util.Map.of(FloatingPod.MOD_ID, "other")

    List(Side.CLIENT, Side.SERVER).foreach { side =>
      assertTrue(FloatingPod.acceptsRemoteVersions(exact, side))
      assertFalse(FloatingPod.acceptsRemoteVersions(missing, side))
      assertFalse(FloatingPod.acceptsRemoteVersions(mismatch, side))
    }

  @Test
  def serverEndpointRunsOnlyAfterTheSchedulerExecutesAndDefaultsFailClosed(): Unit =
    val scheduler = new RecordingScheduler()
    val endpoint = new RecordingServerEndpoint()
    val request = new C2SReleaseRequest(UUID.randomUUID(), new BlockPos(0, 64, 0), 0L)

    new C2SReleaseHandler(scheduler, endpoint).onMessage(request, null)
    assertEquals(0, endpoint.releaseCalls)
    scheduler.runNext()
    assertEquals(1, endpoint.releaseCalls)

    new C2SRecallHandler(scheduler, UnavailableServerPacketEndpoint).onMessage(new C2SRecallRequest(UUID.randomUUID(), 0L), null)
    assertThrows(classOf[IllegalStateException], () => scheduler.runNext())

  @Test
  def registeredC2SHandlerResolvesTheRuntimeInstalledAfterRegistration(): Unit =
    ServerPacketRuntime.uninstall()
    val registrar = new RecordingPacketRegistrar()
    val scheduler = new RecordingScheduler()
    new PacketHandler(registrar, scheduler).registerOnce()
    val handler = registrar.registrations.find(_.discriminator == 0).get.handler
      .asInstanceOf[IMessageHandler[C2SReleaseRequest, IMessage]]
    val request = new C2SReleaseRequest(UUID.randomUUID(), new BlockPos(0, 64, 0), 0L)

    handler.onMessage(request, null)
    assertThrows(classOf[IllegalStateException], () => scheduler.runNext())

    val endpoint = new RecordingServerEndpoint()
    ServerPacketRuntime.install(endpoint)
    try
      handler.onMessage(request, null)
      scheduler.runNext()
      assertEquals(1, endpoint.releaseCalls)
    finally ServerPacketRuntime.uninstall()

  @Test
  def c2sEndpointsReceiveTheExactNonNullContextOnlyAfterScheduledExecution(): Unit =
    val scheduler = new RecordingScheduler()
    val endpoint = new RecordingServerEndpoint()
    val context = new ControlledMessageContext()
    val release = new C2SReleaseRequest(UUID.randomUUID(), new BlockPos(0, 64, 0), 0L)
    val recall = new C2SRecallRequest(UUID.randomUUID(), 0L)

    new C2SReleaseHandler(scheduler, endpoint).onMessage(release, context)
    new C2SRecallHandler(scheduler, endpoint).onMessage(recall, context)
    assertEquals(Nil, endpoint.releaseContexts.toList)
    assertEquals(Nil, endpoint.recallContexts.toList)
    scheduler.runAll()
    assertEquals(List(context), endpoint.releaseContexts.toList)
    assertEquals(List(context), endpoint.recallContexts.toList)

  @Test
  def encodedPayloadHeaderUsesPacketHandlerProtocolVersionAsItsOnlyAuthority(): Unit =
    val payload = Unpooled.buffer()

    new C2SReleaseRequest(UUID.randomUUID(), new BlockPos(0, 64, 0), 0L).toBytes(payload)

    assertEquals(PacketHandler.PROTOCOL_VERSION, payload.getUnsignedByte(payload.readerIndex()))

  @Test
  def clientSinkIsNotUpdatedBeforeItsScheduledTaskAndReceivesAllS2CMessages(): Unit =
    val scheduler = new RecordingScheduler()
    val sink = new RecordingClientSink()
    val uuid = UUID.randomUUID()
    val sync = new S2CStateSync(uuid, 0.0D, 64.0D, 0.0D, 0.0F, 0.0F, 1L)
    val release = S2CReleaseResult.succeeded(uuid, 2L)
    val recall = S2CRecallResult.succeeded(uuid, 3L)

    new S2CStateSyncHandler(scheduler, sink).onMessage(sync, null)
    new S2CReleaseResultHandler(scheduler, sink).onMessage(release, null)
    new S2CRecallResultHandler(scheduler, sink).onMessage(recall, null)
    assertEquals(0, sink.received.size)
    scheduler.runAll()
    assertEquals(List(sync, release, recall), sink.received.toList)

final class RecordingPacketRegistrar extends PacketRegistrar:
  val registrations = mutable.ListBuffer.empty[PacketRegistration]
  override def register(entry: PacketRegistration): Unit = registrations += entry

private final class RecordingScheduler extends MainThreadScheduler:
  private val work = mutable.Queue.empty[Runnable]
  override def schedule(context: MessageContext, runnable: Runnable): Unit = work.enqueue(runnable)
  def runNext(): Unit = work.dequeue().run()
  def runAll(): Unit = while work.nonEmpty do runNext()

private final class RecordingServerEndpoint extends ServerPacketEndpoint:
  var releaseCalls = 0
  val releaseContexts = mutable.ListBuffer.empty[MessageContext]
  val recallContexts = mutable.ListBuffer.empty[MessageContext]
  override def receiveRelease(message: C2SReleaseRequest, context: MessageContext): Unit =
    releaseCalls += 1
    releaseContexts += context
  override def receiveRecall(message: C2SRecallRequest, context: MessageContext): Unit = recallContexts += context

private final class RecordingClientSink extends ClientPacketSink:
  val received = mutable.ListBuffer.empty[IMessage]
  override def receiveStateSync(message: S2CStateSync): Unit = received += message
  override def receiveReleaseResult(message: S2CReleaseResult): Unit = received += message
  override def receiveRecallResult(message: S2CRecallResult): Unit = received += message
}
