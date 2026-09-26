package com.wi.floatingpod.storage

import com.wi.floatingpod.domain.FloatingPodState

import java.util.UUID
import scala.util.Try

/**
 * 定义领域层访问恢复权威存储的最小接口，使领域逻辑不依赖具体 JDBC backend。
 */
trait FloatingPodRepository:
  /**
   * 写入新状态并拒绝重复 UUID，防止恢复记录被静默覆盖。
   *
   * @param state 需要持久化的初始状态。
   * @return 成功时返回已写入状态，失败时保留异常原因。
   */
  def insert(state: FloatingPodState): Try[FloatingPodState]

  /**
   * 以状态版本执行乐观锁更新，避免旧写入覆盖已提交的新状态。
   *
   * @param state 调用方基于当前版本构造的下一状态。
   * @return 成功时返回时间戳和版本均已更新的状态。
   */
  def update(state: FloatingPodState): Try[FloatingPodState]

  /**
   * 按唯一标识读取恢复状态。
   *
   * @param uuid 目标辅助机标识。
   * @return 找到时返回状态，未找到时返回 `None`。
   */
  def findByUuid(uuid: UUID): Try[Option[FloatingPodState]]

  /**
   * 按所有者读取全部状态，供领域层执行所有权校验。
   *
   * @param ownerUuid 所有者标识。
   * @return 与该所有者匹配的状态列表。
   */
  def findByOwner(ownerUuid: UUID): Try[List[FloatingPodState]]

  /**
   * 读取所有活动状态，供恢复活动实体前的服务端权威检查使用。
   *
   * @return 当前标记为活动的全部状态。
   */
  def findAllActive(): Try[List[FloatingPodState]]

  /**
   * 读取全部持久化状态，供管理员显式迁移包导出使用。
   *
   * @return 按稳定 UUID 顺序返回的所有状态。
   */
  def findAll(): Try[List[FloatingPodState]]

  /**
   * 删除指定恢复状态，调用方据返回值区分不存在和实际删除。
   *
   * @param uuid 目标辅助机标识。
   * @return `true` 表示删除了一条记录。
   */
  def delete(uuid: UUID): Try[Boolean]

  /**
   * 执行单连接事务，使导入和关键状态变更可作为一个原子边界提交。
   *
   * @param fn 需要在事务中执行的代码块。
   * @return 成功时返回代码块结果，失败时已回滚并保留异常。
   */
  def transaction[T](fn: => T): Try[T]

  /**
   * 显式释放 backend 连接，避免服务器关闭后继续持有恢复权威资源。
   */
  def close(): Unit
