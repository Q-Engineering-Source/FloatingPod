package com.wi.floatingpod.storage.mysql

import java.time.LocalDateTime
import java.util.UUID

/** 原Entry登记的recall专用执行身份；共用六表事务机械逻辑，不共用确认或实体清理权限。 */
private[mysql] final class MySqlDbFirstRecallTransaction private[mysql] (
    reservationInput:MySqlDbFirstCaptureReservation,requestInput:MySqlDbFirstRecallRequest,rootInput:MySqlFormat3ReadBudget)
  extends MySqlDbFirstSnapshotTransaction[MySqlDbFirstRecallRequest,MySqlDbFirstSession.DbFirstRecallObservation](reservationInput,requestInput,rootInput):
  protected def requireExecution(issuer:MySqlOwnedRetainedPlanIssuer,session:MySqlDbFirstSession,inputs:MySqlScriptSchemaInputs):Unit =
    issuer.requireExecutingDbFirstRecall(session,this,inputs)
  protected def requireCoordinator(coordinator:MySqlWriterFenceCoordinator,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope):Unit =
    coordinator.requireDbFirstRecall(this,admission,verified,scope)
  protected def auditQualification(coordinator:MySqlWriterFenceCoordinator,admission:MySqlBusinessAdmission,
      verified:VerifiedMySqlConnection,scope:MySqlAuditBootScope,audit:MySqlSchemaAuditor.DbFirstAudit):VerifiedSchemaQualification =
    coordinator.auditDbFirstRecall(this,admission,verified,scope,audit)
  protected def materializeOriginal(issuer:MySqlOwnedRetainedPlanIssuer,before:MySqlScriptSchemaSnapshot,
      connectionId:Long,barrier:UUID,plannedAt:LocalDateTime,resultAt:LocalDateTime):MySqlDbFirstRecallMaterialization =
    issuer.materializeExecutingDbFirstRecall(this,before,connectionId,barrier,plannedAt,resultAt)
  protected def publishOriginal(session:MySqlDbFirstSession):Unit = session.publishDbFirstRecall(this)
  protected def failOriginal(issuer:MySqlOwnedRetainedPlanIssuer,cause:Throwable):Unit = issuer.failDbFirstRecall(this,cause)
  protected def newObservation(canonicalSha256:String):MySqlDbFirstSession.DbFirstRecallObservation =
    MySqlDbFirstSession.DbFirstRecallObservation(this.request.tag,this.request.podUuid,commitAttempted,acknowledged,true,canonicalSha256)

private[mysql] object MySqlDbFirstRecallTransaction:
  val HolderBytes=MySqlDbFirstSnapshotTransaction.HolderBytes
  type Progress=MySqlDbFirstSnapshotTransaction.Progress
