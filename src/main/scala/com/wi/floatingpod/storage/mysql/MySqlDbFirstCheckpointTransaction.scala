package com.wi.floatingpod.storage.mysql

import java.time.LocalDateTime
import java.util.UUID

/** 原Entry登记的checkpoint专用执行身份；共用六表事务机械逻辑，不共用确认或实体清理权限。 */
private[mysql] final class MySqlDbFirstCheckpointTransaction private[mysql] (
    reservationInput:MySqlDbFirstCaptureReservation,requestInput:MySqlDbFirstCheckpointRequest,rootInput:MySqlFormat3ReadBudget)
  extends MySqlDbFirstSnapshotTransaction[MySqlDbFirstCheckpointRequest,MySqlDbFirstSession.DbFirstCheckpointObservation](reservationInput,requestInput,rootInput):
  protected def requireExecution(issuer:MySqlOwnedRetainedPlanIssuer,session:MySqlDbFirstSession,inputs:MySqlScriptSchemaInputs):Unit =
    issuer.requireExecutingDbFirstCheckpoint(session,this,inputs)
  protected def requireCoordinator(coordinator:MySqlWriterFenceCoordinator,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit =
    coordinator.requireDbFirstCheckpoint(this,admission,verified,scope)
  protected def auditQualification(coordinator:MySqlWriterFenceCoordinator,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,audit:MySqlSchemaAuditor.DbFirstAudit):VerifiedSchemaQualification =
    coordinator.auditDbFirstCheckpoint(this,admission,verified,scope,audit)
  protected def materializeOriginal(issuer:MySqlOwnedRetainedPlanIssuer,before:MySqlScriptSchemaSnapshot,
      connectionId:Long,barrier:UUID,plannedAt:LocalDateTime,resultAt:LocalDateTime):MySqlDbFirstRecallMaterialization =
    issuer.materializeExecutingDbFirstCheckpoint(this,before,connectionId,barrier,plannedAt,resultAt)
  protected def publishOriginal(session:MySqlDbFirstSession):Unit = session.publishDbFirstCheckpoint(this)
  protected def failOriginal(issuer:MySqlOwnedRetainedPlanIssuer,cause:Throwable):Unit = issuer.failDbFirstCheckpoint(this,cause)
  protected def newObservation(canonicalSha256:String):MySqlDbFirstSession.DbFirstCheckpointObservation =
    MySqlDbFirstSession.DbFirstCheckpointObservation(this.request.tag,this.request.podUuid,commitAttempted,acknowledged,true,canonicalSha256)

private[mysql] object MySqlDbFirstCheckpointTransaction:
  val HolderBytes=MySqlDbFirstSnapshotTransaction.HolderBytes
  type Progress=MySqlDbFirstSnapshotTransaction.Progress
