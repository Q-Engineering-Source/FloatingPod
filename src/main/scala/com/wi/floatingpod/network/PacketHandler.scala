package com.wi.floatingpod.network

import io.netty.buffer.ByteBuf

import net.minecraft.entity.player.EntityPlayerMP
import net.minecraftforge.fml.common.network.NetworkRegistry
import net.minecraftforge.fml.common.network.simpleimpl.{IMessage, IMessageHandler, MessageContext, SimpleNetworkWrapper}
import net.minecraftforge.fml.relauncher.Side

/** 描述一个固定 discriminator、接收端与 handler 的公共消息注册项。 */
final case class PacketRegistration(
    messageType: Class[? <: IMessage],
    discriminator: Int,
    receivingSide: Side,
    handler: IMessageHandler[?, ? <: IMessage]
)

/** 隔离 Forge SimpleNetworkWrapper，使注册表可在普通单元测试中观察。 */
trait PacketRegistrar:
  def register(entry: PacketRegistration): Unit

/** 把公共注册项适配到真实 Forge channel。 */
final class ForgePacketRegistrar(channel: SimpleNetworkWrapper) extends PacketRegistrar:
  require(channel != null, "channel must not be null")
  override def register(entry: PacketRegistration): Unit =
    channel.registerMessage(
      entry.handler.asInstanceOf[IMessageHandler[IMessage, IMessage]],
      entry.messageType.asInstanceOf[Class[IMessage]],
      entry.discriminator,
      entry.receivingSide
    )

/** 注册固定公共协议表；每个实例最多写入 registrar 一次。 */
final class PacketHandler(registrar: PacketRegistrar, scheduler: MainThreadScheduler = ForgeMainThreadScheduler):
  require(registrar != null, "registrar must not be null")
  require(scheduler != null, "scheduler must not be null")
  private var registered = false

  /** 幂等注册当前五个消息；一旦成功注册，后续调用不重复占用 discriminator。 */
  def registerOnce(): Unit = synchronized {
    if !registered then
      PacketHandler.registrations(scheduler).foreach(registrar.register)
      registered = true
  }

object PacketHandler:
  /** 当前公共 payload 的固定协议版本。 */
  final val PROTOCOL_VERSION: Int = 1
  /** Forge channel 的稳定名称。 */
  final val CHANNEL_NAME: String = "floatingpod"

  private lazy val productionChannel = NetworkRegistry.INSTANCE.newSimpleChannel(CHANNEL_NAME)

  private lazy val productionHandler = new PacketHandler(new ForgePacketRegistrar(productionChannel))

  private[network] def registrations(scheduler: MainThreadScheduler = ForgeMainThreadScheduler): List[PacketRegistration] = List(
    PacketRegistration(classOf[C2SReleaseRequest], 0, Side.SERVER, new C2SReleaseHandler(scheduler, ServerPacketRuntime)),
    PacketRegistration(classOf[C2SRecallRequest], 1, Side.SERVER, new C2SRecallHandler(scheduler, ServerPacketRuntime)),
    PacketRegistration(classOf[S2CStateSync], 2, Side.CLIENT, new S2CStateSyncHandler()),
    PacketRegistration(classOf[S2CReleaseResult], 3, Side.CLIENT, new S2CReleaseResultHandler()),
    PacketRegistration(classOf[S2CRecallResult], 4, Side.CLIENT, new S2CRecallResultHandler())
  )

  /** 在 Mod 预初始化时注册一次真实 channel。 */
  def initialize(): Unit = productionHandler.registerOnce()

  /** 服务器 endpoint 的最小 channel 发送入口；不暴露 channel 或注册表可变状态。 */
  private[network] def sendTo(message: IMessage, player: EntityPlayerMP): Unit =
    require(message != null, "message must not be null")
    require(player != null, "player must not be null")
    productionChannel.sendTo(message, player)
