package com.wi.floatingpod.storage.mysql

/** 仅内部 full20 reader 的固定首读 seam；不包含 JDBC connection、Main callback 或 admission 发行。 */
private[mysql] trait MySqlFormat3QuerySnapshotSource:
  def readWithinQueryWorkflow(root: MySqlFormat3ReadBudget): MySqlFormat3FullSnapshot

/** 全 vector 先分类，纯查询只消费同一已捕获 full20；不执行 mutation 或任意函数。 */
private[mysql] object MySqlFormat3ReadDispatcher:
  def isRead(operation: MySqlRepositoryOperation): Boolean = operation match
    case MySqlRepositoryOperation.FindByUuid(uuid) => true
    case MySqlRepositoryOperation.FindByOwner(ownerUuid) => true
    case MySqlRepositoryOperation.FindAllActive => true
    case MySqlRepositoryOperation.FindAll => true
    case MySqlRepositoryOperation.FindByPlayerUuid(playerUuid) => true
    case MySqlRepositoryOperation.FindDesiredActiveRows => true
    case MySqlRepositoryOperation.FindStoredByUuid(uuid) => true
    case MySqlRepositoryOperation.FindAllRecoverable => true
    case MySqlRepositoryOperation.FindPhysicalRecoveryBinding => true
    case MySqlRepositoryOperation.FindPhysicalRecoveryLocator(podUuid) => true
    case MySqlRepositoryOperation.FindAllPhysicalRecoveryLocators => true
    case MySqlRepositoryOperation.FindUnboundPreparation(podUuid) => true
    case _ => false
  def requireReads(operations: Vector[MySqlRepositoryOperation]): Unit =
    require(operations != null && operations.nonEmpty && operations.forall(isRead),"nonempty fixed format3 read vector required")
  def capture(operations: Vector[MySqlRepositoryOperation], source: MySqlFormat3QuerySnapshotSource,
      root: MySqlFormat3ReadBudget): MySqlFormat3FullSnapshot =
    requireReads(operations)
    require(source != null && root != null && !root.isClosed,"actual fixed-query source and original root required")
    val snapshot = source.readWithinQueryWorkflow(root)
    require(snapshot != null && (snapshot.allocationBudget eq root),"fixed query capture must retain exact original root")
    snapshot
  def execute(operations: Vector[MySqlRepositoryOperation], projection: MySqlFormat3QueryProjection): Vector[MySqlRepositoryReply] =
    requireReads(operations)
    require(projection != null,"same actual full20 query projection required")
    operations.map { operation => operation match
      case MySqlRepositoryOperation.FindByUuid(uuid) => MySqlRepositoryReply.OptionalState(projection.findByUuid(uuid))
      case MySqlRepositoryOperation.FindByOwner(ownerUuid) => MySqlRepositoryReply.States(projection.findByOwner(ownerUuid))
      case MySqlRepositoryOperation.FindAllActive => MySqlRepositoryReply.States(projection.findAllActive())
      case MySqlRepositoryOperation.FindAll => MySqlRepositoryReply.States(projection.findAll())
      case MySqlRepositoryOperation.FindByPlayerUuid(playerUuid) => MySqlRepositoryReply.Entitlement(projection.findByPlayerUuid(playerUuid))
      case MySqlRepositoryOperation.FindDesiredActiveRows => MySqlRepositoryReply.StoredRows(projection.findDesiredActiveRows())
      case MySqlRepositoryOperation.FindStoredByUuid(uuid) => MySqlRepositoryReply.OptionalStoredRow(projection.findStoredByUuid(uuid))
      case MySqlRepositoryOperation.FindAllRecoverable => MySqlRepositoryReply.States(projection.findAllRecoverable())
      case MySqlRepositoryOperation.FindPhysicalRecoveryBinding => MySqlRepositoryReply.OptionalBinding(projection.findPhysicalRecoveryBinding())
      case MySqlRepositoryOperation.FindPhysicalRecoveryLocator(podUuid) => MySqlRepositoryReply.OptionalLocator(projection.findPhysicalRecoveryLocator(podUuid))
      case MySqlRepositoryOperation.FindAllPhysicalRecoveryLocators => MySqlRepositoryReply.Locators(projection.findAllPhysicalRecoveryLocators())
      case MySqlRepositoryOperation.FindUnboundPreparation(podUuid) => MySqlRepositoryReply.OptionalPreparation(projection.findUnboundPreparation(podUuid))
      case _ => throw new IllegalArgumentException("mutation is not a format3 query")
    }
  /** 只接受 Session 刚在 captured projection 上建立的 active lexical view；不给 Main callback。 */
  def executeOwned(operations: Vector[MySqlRepositoryOperation], view: MySqlOwnedRepositoryView): Vector[MySqlRepositoryReply] =
    requireReads(operations)
    require(view != null,"actual session-owned query view required")
    operations.map(operation => MySqlImmutableCommandDispatcher.execute(operation,view).get)
