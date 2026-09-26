package com.wi.floatingpod.server

import java.lang.Double as JavaDouble

/** 不依赖 Minecraft Entity 或 World 的权威位置坐标，便于在 handler 边界前纯粹验证。 */
final case class AuthoritativePosition(x: Double, y: Double, z: Double)

/** 请求释放的位置；实际 BlockPos 可在 production adapter 层转为这个值类型。 */
final case class ReleasePosition(x: Double, y: Double, z: Double)

/** 纯位置验证的有限结果，避免以自由文字承载拒绝原因。 */
enum PositionValidation:
  case Accepted, NonFiniteInput, OutsideReach, OutsideVerticalBounds, Unsupported, Obstructed

/** Release 的位置和环境判定边界；调用者必须提供服务端权威的玩家与世界资料。 */
trait ReleasePositionPolicy:
  def validate(
      player: AuthoritativePosition,
      target: ReleasePosition,
      maximumReach: Double,
      hasSolidSupport: Boolean,
      hasTwoBlockHeadroom: Boolean
  ): PositionValidation

/** 判定由服务端提供的当前玩家位置、距离限制与方块环境是否允许释放。 */
final class PositionValidator extends ReleasePositionPolicy:
  /**
   * 验证有限坐标与 reach、世界垂直边界、三维距离、实心支撑及完整两格净空。
   * `maximumReach` 由服务端当前权威状态输入，没有内建固定距离。
   */
  def validate(
      player: AuthoritativePosition,
      target: ReleasePosition,
      maximumReach: Double,
      hasSolidSupport: Boolean,
      hasTwoBlockHeadroom: Boolean
  ): PositionValidation =
    require(player != null, "player must not be null")
    require(target != null, "target must not be null")
    if !isFinite(player.x) || !isFinite(player.y) || !isFinite(player.z) ||
        !isFinite(target.x) || !isFinite(target.y) || !isFinite(target.z) || !isFinite(maximumReach) || maximumReach <= 0.0D then
      PositionValidation.NonFiniteInput
    else if target.y <= 0.0D || target.y >= 256.0D then
      PositionValidation.OutsideVerticalBounds
    else if squaredDistance(player, target) > maximumReach * maximumReach then
      PositionValidation.OutsideReach
    else if !hasSolidSupport then
      PositionValidation.Unsupported
    else if !hasTwoBlockHeadroom then
      PositionValidation.Obstructed
    else PositionValidation.Accepted

  private def isFinite(value: Double): Boolean = JavaDouble.isFinite(value)

  private def squaredDistance(player: AuthoritativePosition, target: ReleasePosition): Double =
    val deltaX = target.x - player.x
    val deltaY = target.y - player.y
    val deltaZ = target.z - player.z
    deltaX * deltaX + deltaY * deltaY + deltaZ * deltaZ
