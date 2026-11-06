package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodState, PlayerEntitlement}
import com.wi.floatingpod.storage.*
import java.sql.SQLDataException
import java.util.UUID

/** worker 已完整解码的数据，不发行 Main lease、SQL admission、absence 或 Ready。 */
private[floatingpod] final class MySqlBootMainReadData private[mysql] (
    private val pods: Map[UUID, StoredPodRow],
    val recoveryRows: Vector[StoredPodRow],
    private val entitlements: Map[UUID, PlayerEntitlement],
    val binding: Option[PhysicalRecoveryBinding],
    val locators: Vector[PhysicalRecoveryLocator],
    private val receipts: Map[UUID, UnboundPreparationReceipt],
    val desiredActiveRows: Vector[StoredPodRow],
    val sourceProtection: MySqlBootSourceProtectionIndex,
    val activeAttemptFootprints: Map[UUID, (UUID, UUID)],
    private[mysql] val recallRequests:Option[MySqlDbFirstRecallRequestIndex],
    private val preparedInitials:Map[UUID,MySqlDbFirstInitialPreparationDataView]
):
  def stored(id: UUID): Option[StoredPodRow] =
    require(id != null, "pod UUID required"); pods.get(id)
  def state(id: UUID): Option[FloatingPodState] = stored(id).map(_.toPublicState)
  def entitlement(id: UUID): Option[PlayerEntitlement] =
    require(id != null, "player UUID required"); entitlements.get(id)
  def receipt(id: UUID): Option[UnboundPreparationReceipt] =
    require(id != null, "pod UUID required"); receipts.get(id)
  def preparedInitial(create:UUID):Option[MySqlDbFirstInitialPreparationDataView] =
    require(create!=null,"original create UUID required");preparedInitials.get(create)
  /** 原Full27已独立计费的完整typed metadata；phase只分类，不证明Main恢复收口。 */
  def recallRequestCensus:Vector[MySqlDbFirstRecallRequestRecord] =
    recallRequests.getOrElse(throw new IllegalStateException("schema6 complete durable request census missing")).census
  def activeCount: Int = desiredActiveRows.size
  def activeCountFor(id: UUID): Int =
    require(id != null, "player UUID required"); desiredActiveRows.count(_.ownerUuid.contains(id))

private[mysql] object MySqlBootMainReadData:
  private sealed trait CompleteSource:
    def relation(table:String):MySqlFormat3RelationSnapshot
    def columnIndex(table:String,column:String):Int
    def protection(root:MySqlFormat3ReadBudget,dataBudget:MySqlFormat3ReadBudget):MySqlBootSourceProtectionIndex
  private final case class LegacyFull20(snapshot:MySqlFormat3FullSnapshot) extends CompleteSource:
    def relation(table:String):MySqlFormat3RelationSnapshot=snapshot.relation(table)
    def columnIndex(table:String,column:String):Int=MySqlFormat3RelationShapes.forTable(table).columns.indexWhere(_.name==column)
    def protection(root:MySqlFormat3ReadBudget,dataBudget:MySqlFormat3ReadBudget):MySqlBootSourceProtectionIndex=MySqlBootSourceProtectionIndex.from(snapshot,root)
  private final case class DeploymentReceiptFull26(snapshot:MySqlScriptSchemaSnapshot,
      inputs:MySqlScriptSchemaInputs) extends CompleteSource:
    def relation(table:String):MySqlFormat3RelationSnapshot=snapshot.relation(table)
    def columnIndex(table:String,column:String):Int=inputs.forTable(table).columns.indexWhere(_.name==column)
    def protection(root:MySqlFormat3ReadBudget,dataBudget:MySqlFormat3ReadBudget):MySqlBootSourceProtectionIndex=MySqlBootSourceProtectionIndex.fromDeploymentReceipt(snapshot,inputs,root)
  private final case class DbFirstFull27(snapshot:MySqlScriptSchemaSnapshot,
      inputs:MySqlScriptSchemaInputs) extends CompleteSource:
    def relation(table:String):MySqlFormat3RelationSnapshot=snapshot.relation(table)
    def columnIndex(table:String,column:String):Int=inputs.forTable(table).columns.indexWhere(_.name==column)
    def protection(root:MySqlFormat3ReadBudget,dataBudget:MySqlFormat3ReadBudget):MySqlBootSourceProtectionIndex=MySqlBootSourceProtectionIndex.fromDbFirst(snapshot,inputs,root,dataBudget)
  private[mysql] def fromDbFirst(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget):MySqlBootMainReadData =
    fromDbFirst(snapshot,inputs,root,root)
  private[mysql] def fromDbFirst(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget,dataBudget:MySqlFormat3ReadBudget):MySqlBootMainReadData =
    val query=MySqlFormat3QueryProjection.fromDbFirstData(snapshot,inputs,root,dataBudget)
    val requests=MySqlDbFirstRecallRequestIndex.from(snapshot,inputs,root,dataBudget)
    val prepared=MySqlDbFirstInitialPreparationDataView.currentPreparedInitials(snapshot,root,dataBudget,query)
    materialize(DbFirstFull27(snapshot,inputs),query,dataBudget,root,Some(requests),prepared)
  def from(snapshot: MySqlFormat3FullSnapshot, budget: MySqlFormat3ReadBudget): MySqlBootMainReadData =
    require(snapshot != null && budget != null && !budget.isClosed &&
      (snapshot.limitProfile eq budget.profile) && snapshot.allocationBudget.sharesAllocationLedger(budget),
      "Boot DATA requires the actual open full20 retained root")
    val q = MySqlFormat3QueryProjection.from(snapshot, budget)
    materialize(LegacyFull20(snapshot),q,budget)
  def fromDeploymentReceipt(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget,expected:MySqlSchemaAuditExpectation,admission:MySqlBusinessAdmission):MySqlBootMainReadData =
    val query=MySqlFormat3QueryProjection.fromDeploymentReceipt(snapshot,inputs,root,expected,admission)
    materialize(DeploymentReceiptFull26(snapshot,inputs),query,root)
  private[mysql] def fromRestrictedDeploymentReceipt(snapshot:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget,expected:MySqlSchemaAuditExpectation,
      proof:MySqlDeploymentReceiptSession.RestrictedRecall5):MySqlBootMainReadData =
    val query=MySqlFormat3QueryProjection.fromRestrictedDeploymentReceipt(snapshot,inputs,root,expected,proof)
    materialize(DeploymentReceiptFull26(snapshot,inputs),query,root)
  private def materialize(snapshot:CompleteSource,q:MySqlFormat3QueryProjection,
      budget:MySqlFormat3ReadBudget):MySqlBootMainReadData =
    materialize(snapshot,q,budget,budget)
  private def materialize(snapshot:CompleteSource,q:MySqlFormat3QueryProjection,
      budget:MySqlFormat3ReadBudget,sourceRoot:MySqlFormat3ReadBudget,
      recallRequests:Option[MySqlDbFirstRecallRequestIndex]=None,
      preparedInitials:Map[UUID,MySqlDbFirstInitialPreparationDataView]=Map.empty):MySqlBootMainReadData =
    def identities(table: String, column: String): Vector[UUID] =
      val rows = snapshot.relation(table).rows
      budget.reserveRetainedMemory(Math.addExact(128L, Math.multiplyExact(rows.size.toLong, 96L)), table + " Boot identity vector")
      val index = snapshot.columnIndex(table,column)
      require(index >= 0, "actual relation UUID column required")
      rows.map { row =>
        row(index) match
          case MySqlFormat3Cell.Binary(value) if value.length == 16 =>
            var high = 0L; var low = 0L
            var n = 0
            while n < 8 do
              high = (high << 8) | (value.byteAt(n).toLong & 255L)
              low = (low << 8) | (value.byteAt(n + 8).toLong & 255L)
              n += 1
            new UUID(high, low)
          case _ => throw new SQLDataException("Boot DATA requires exact binary UUID")
      }
    val podIds = identities("floating_pod_state", "uuid")
    val playerIds = identities("player_entitlement", "player_uuid")
    budget.reserveRetainedMemory(Math.addExact(1024L, Math.multiplyExact(Math.addExact(podIds.size.toLong, playerIds.size.toLong), 320L)),
      "Boot immutable rows and lookup structures")
    val rows = podIds.map(id => q.findStoredByUuid(id).getOrElse(throw new SQLDataException("full20 pod index mismatch")))
    val entitlements = playerIds.map(id => id -> q.findByPlayerUuid(id).getOrElse(throw new SQLDataException("full20 entitlement index mismatch"))).toMap
    budget.reserveRetainedMemory(Math.addExact(256L,
      Math.multiplyExact(snapshot.relation("physical_recovery_locator").rows.size.toLong, 160L)), "Boot locator and receipt indices")
    val locators = q.findAllPhysicalRecoveryLocators().toVector
    val receipts = locators.flatMap(locator => q.findUnboundPreparation(locator.podUuid).map(locator.podUuid -> _)).toMap
    val sourceProtection = snapshot.protection(sourceRoot,budget)
    val activeRows = snapshot.relation("physical_recovery_locator").rows.filter(row =>
      MySqlModeOperationEffectCodec.value(row, "physical_recovery_locator", "locator_state") == MySqlFormat3Cell.Text("ROW_ACTIVE"))
    budget.reserveRetainedMemory(Math.addExact(128L, Math.multiplyExact(activeRows.size.toLong, 256L)),
      "Boot original active attempt and footprint identities")
    def locatorUuid(row: Vector[MySqlFormat3Cell], column: String): UUID =
      MySqlModeOperationEffectCodec.value(row, "physical_recovery_locator", column) match
        case MySqlFormat3Cell.Binary(bytes) if bytes.length == 16 =>
          var high = 0L; var low = 0L
          (0 until 8).foreach { index =>
            high = (high << 8) | (bytes.byteAt(index).toLong & 255L)
            low = (low << 8) | (bytes.byteAt(index + 8).toLong & 255L)
          }
          new UUID(high, low)
        case _ => throw new SQLDataException("active locator requires complete original attempt/footprint UUID")
    val activeAttemptFootprints = activeRows.map(row => locatorUuid(row, "pod_uuid") ->
      (locatorUuid(row, "current_attempt_uuid"), locatorUuid(row, "current_footprint_uuid"))).toMap
    if activeAttemptFootprints.size != activeRows.size then throw new SQLDataException("duplicate active Boot locator identity")
    new MySqlBootMainReadData(rows.map(row => row.uuid -> row).toMap, rows, entitlements,
      q.findPhysicalRecoveryBinding(), locators, receipts, q.findDesiredActiveRows().toVector, sourceProtection, activeAttemptFootprints,recallRequests,preparedInitials)
