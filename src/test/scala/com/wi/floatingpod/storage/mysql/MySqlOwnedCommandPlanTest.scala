package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 真command/plan纯匹配；不注入Session/JDBC成功，不授DB/Ready权限。 */
class MySqlOwnedCommandPlanTest:
  import MySqlPrepareModeGraphFixture.*
  private val limits = SourceLimitProfile.of(1048576L, 1048576L, 1048576L, 64L, 65536L, 1048576L, 1048576L, 1048576L)
  private def single(operation: MySqlRepositoryOperation): MySqlOwnedRepositoryOperationPlan =
    MySqlOwnedRepositoryOperationPlan.single(tag, id(801), id(802), operation, limits)

  @Test def allFiveCommandsUseTheirMandatoryOriginalPlanAndActualProfile(): Unit =
    val prepare = prepared().request
    val change = mode().request
    val receipt = prepared().receipt
    val commit = MySqlOwnedIoCommand.controlledCommitOperation(receipt, receipt.targetPose, 256)
    val run = MySqlRepositoryOperation.FindAll
    val operations = Vector(run, MySqlRepositoryOperation.FindByUuid(id(803)))
    val transaction = MySqlOwnedRepositoryOperationPlan.transaction(tag, id(801), id(802), operations, limits)
    val cases = Vector(
      (MySqlOwnedIoCommand.Run(run, single(run)), single(run)),
      (MySqlOwnedIoCommand.Transaction(operations, transaction), transaction),
      (MySqlOwnedIoCommand.PrepareControlledUnbound(prepare, single(prepare.repositoryOperation)), single(prepare.repositoryOperation)),
      (MySqlOwnedIoCommand.CommitControlledUnbound(receipt, receipt.targetPose, 256, single(commit)), single(commit)),
      (MySqlOwnedIoCommand.CommitOwnedMode(change, single(change.repositoryOperation)), single(change.repositoryOperation)))
    cases.foreach { (command, expected) =>
      assertEquals(expected, command.requireRepositoryPlan(tag, limits))
    }

  @Test def wrongTagProfileAndOriginalRequestAreExplicitlyRejected(): Unit =
    val original = MySqlRepositoryOperation.FindByUuid(id(810))
    val plan = single(original)
    val command = MySqlOwnedIoCommand.Run(original, plan)
    assertSame(plan, command.requireRepositoryPlan(tag, limits))
    assertThrows(classOf[IllegalArgumentException], () => command.requireRepositoryPlan(tag.copy(operationId = id(811)), limits))
    val otherLimits = SourceLimitProfile.of(1048576L, 1048576L, 1048576L, 64L, 65536L, 1048577L, 1048576L, 1048576L)
    assertThrows(classOf[IllegalArgumentException], () => command.requireRepositoryPlan(tag, otherLimits))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlOwnedIoCommand.Run(MySqlRepositoryOperation.FindByUuid(id(812)), plan).requireRepositoryPlan(tag, limits))
    assertThrows(classOf[IllegalArgumentException], () => MySqlOwnedIoCommand.Run(original, null).requireRepositoryPlan(tag, limits))

  @Test def transactionOrderCannotUseSingleOrPermutedPlan(): Unit =
    val operations = Vector(MySqlRepositoryOperation.FindByUuid(id(820)), MySqlRepositoryOperation.FindByUuid(id(821)))
    val plan = MySqlOwnedRepositoryOperationPlan.transaction(tag, id(801), id(802), operations, limits)
    assertSame(plan, MySqlOwnedIoCommand.Transaction(operations, plan).requireRepositoryPlan(tag, limits))
    assertThrows(classOf[IllegalArgumentException], () => MySqlOwnedIoCommand.Transaction(operations.reverse, plan).requireRepositoryPlan(tag, limits))
    assertThrows(classOf[IllegalArgumentException], () => MySqlOwnedIoCommand.Transaction(operations, single(operations.head)).requireRepositoryPlan(tag, limits))

  @Test def projectionsKeepActualReceiptAndDomainCreateUuidDistinctFromExecuteUuid(): Unit =
    val data = prepared()
    val prepare = data.request.repositoryOperation
    assertEquals(MySqlRepositoryOperation.PrepareUnboundCreate(data.request.expectedBinding, data.request.proposedPodUuid,
      data.request.proposedAttemptUuid, data.request.targetPose, data.request.createOperationUuid), prepare)
    val receipt = data.receipt.copy(podUuid = id(830)) // DATA projection only; not a DB-issued successful graph.
    val commit = MySqlOwnedIoCommand.controlledCommitOperation(receipt, receipt.targetPose, 256)
    assertEquals(MySqlRepositoryOperation.CommitUnboundCreate(receipt.binding, id(830), receipt.attemptUuid,
      receipt.preparedVersion, receipt.targetPose, receipt.createOperationUuid.get, 256), commit)
    assertNotEquals(tag.operationId, receipt.createOperationUuid.get)
    val plan = single(commit)
    assertSame(plan, MySqlOwnedIoCommand.CommitControlledUnbound(receipt, receipt.targetPose, 256, plan).requireRepositoryPlan(tag, limits))
