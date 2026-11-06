package com.wi.floatingpod.storage.mysql

/** Recovering只保留原worker责任；它不携带business admission。 */
type MySqlProductionBootReply = MySqlOwnedIoReply.DeploymentReceiptRecallRecoveringBooted | MySqlOwnedIoReply.FreshBooted | MySqlOwnedIoReply.OrdinaryCertifiedBooted | MySqlOwnedIoReply.RecallRecoveringBooted | MySqlOwnedIoReply.DeploymentReceiptBooted

object MySqlProductionBootReply:
  type OrdinaryApplicationReply = MySqlOwnedIoReply.OrdinaryCertifiedBooted | MySqlOwnedIoReply.DeploymentReceiptBooted
  enum DeploymentReceiptPhase:
    case FreshCertifiedPreparation,OrdinaryAdmitted
  /** Fresh 认证准备结果和 Ordinary 业务准入分型；C 不能将前者发布为 Ready。 */
  private[floatingpod] sealed trait DeploymentReceiptBacking:
    def phase:DeploymentReceiptPhase
    def identity: MySqlOwnedIoIdentity
    def bindingId: java.util.UUID
    def writerEpoch: Long
    def repositoryEpochAtSubmission: Long
    def actualSourceLimitProfile: com.wi.floatingpod.config.SourceLimitProfile
    def completeSnapshot: MySqlOwnedFormat3Snapshot
    def databaseAdmission: MySqlOwnedDatabaseAdmission
    def normalRecallSourceFacts:Option[MySqlNormalRecallSourceIdentity]
    private[mysql] def retireOriginalPublication(original:MySqlDeploymentReceiptSession.Publication5):Unit

  private final class CompletedDeploymentReceipt(original:MySqlDeploymentReceiptSession.Publication5)
      extends DeploymentReceiptBacking:
    private val worker=Thread.currentThread()
    val identity:MySqlOwnedIoIdentity=original.identity
    val bindingId:java.util.UUID=original.binding
    val writerEpoch:Long=original.epoch
    val repositoryEpochAtSubmission:Long=original.repositoryEpochAtSubmission
    private var profile:com.wi.floatingpod.config.SourceLimitProfile=original.profile
    val phase:DeploymentReceiptPhase=original.phase
    private var snapshot=original.immutableSnapshot
    private var recallSource=original.normalRecallSourceFacts
    private var admission=MySqlOwnedDatabaseAdmission(identity.bootId,identity.bootGeneration,identity.connectionGeneration,bindingId,writerEpoch)
    private var retired=false
    private def requireLive():Unit=if retired then throw new IllegalStateException("original schema5 publication DATA retired")
    def actualSourceLimitProfile:com.wi.floatingpod.config.SourceLimitProfile=synchronized {requireLive();profile}
    def completeSnapshot:MySqlOwnedFormat3Snapshot=synchronized {requireLive();snapshot}
    def databaseAdmission:MySqlOwnedDatabaseAdmission=synchronized {requireLive();admission}
    def normalRecallSourceFacts:Option[MySqlNormalRecallSourceIdentity]=synchronized {requireLive();recallSource}
    private[mysql] def retireOriginalPublication(actual:MySqlDeploymentReceiptSession.Publication5):Unit=synchronized {
      require(Thread.currentThread() eq worker,"original schema5 publication worker required")
      requireLive()
      if actual==null || (actual.immutableSnapshot ne snapshot) || actual.repositoryEpochAtSubmission!=repositoryEpochAtSubmission then
        throw new IllegalStateException("exact original schema5 immutable publication required for retirement")
      snapshot=null;admission=null;profile=null;recallSource=None;retired=true
    }
  private[mysql] def issueDeploymentReceipt(original:MySqlDeploymentReceiptSession.Publication5):DeploymentReceiptBacking =
    require(original!=null,"original Session-issued complete publication proof required")
    original.requireCurrent()
    new CompletedDeploymentReceipt(original)

  /** 只有原 Session 的 restricted HANDOFF issuer 可构造；全部字段只是 Main 读输入。 */
  private[floatingpod] final class RestrictedRecallBacking private[MySqlProductionBootReply] (
      val recovery:MySqlOwnedIoAdapter.RecallRecoveryLink,val completeSnapshot:MySqlOwnedFormat3Snapshot,
      val originalSourcePins:com.wi.floatingpod.bootstrap.ProductionBootstrapControl.OriginalSourcePins,
      val repositoryEpochAtSubmission:Long):
    val identity:MySqlOwnedIoIdentity=recovery.identity
    val actualSourceLimitProfile:com.wi.floatingpod.config.SourceLimitProfile=completeSnapshot.limitProfile
  private[mysql] def issueRestrictedRecall(original:MySqlDeploymentReceiptSession.RestrictedPublication5):RestrictedRecallBacking =
    original.requireCurrent()
    new RestrictedRecallBacking(original.recovery,original.snapshot,original.originalSourcePins,original.repositoryEpochAtSubmission)

  def databaseAdmission(value: MySqlProductionBootReply): MySqlOwnedDatabaseAdmission = value match
    case _:MySqlOwnedIoReply.DeploymentReceiptRecallRecoveringBooted =>
      throw new IllegalStateException("restricted schema5 Recall has no business admission")
    case value: MySqlOwnedIoReply.DeploymentReceiptBooted => value.owned.databaseAdmission
    case fresh: MySqlOwnedIoReply.FreshBooted => fresh.databaseAdmission
    case ordinary: MySqlOwnedIoReply.OrdinaryCertifiedBooted => ordinary.owned.databaseAdmission
    case _: MySqlOwnedIoReply.RecallRecoveringBooted =>
      throw new IllegalStateException("restricted Recall recovery has no business admission")

  def fullTwentyRelationSnapshot(value: MySqlProductionBootReply): MySqlOwnedFormat3Snapshot = value match
    case value:MySqlOwnedIoReply.DeploymentReceiptRecallRecoveringBooted => value.owned.completeSnapshot
    case value: MySqlOwnedIoReply.DeploymentReceiptBooted => value.owned.completeSnapshot
    case fresh: MySqlOwnedIoReply.FreshBooted => fresh.fullTwentyRelationSnapshot
    case ordinary: MySqlOwnedIoReply.OrdinaryCertifiedBooted => ordinary.owned.fullTwentyRelationSnapshot
    case recovering: MySqlOwnedIoReply.RecallRecoveringBooted => recovering.fullTwentyRelationSnapshot

  def actualSourceLimitProfile(value: MySqlProductionBootReply): com.wi.floatingpod.config.SourceLimitProfile = value match
    case value:MySqlOwnedIoReply.DeploymentReceiptRecallRecoveringBooted => value.owned.actualSourceLimitProfile
    case value: MySqlOwnedIoReply.DeploymentReceiptBooted => value.owned.actualSourceLimitProfile
    case fresh: MySqlOwnedIoReply.FreshBooted => fresh.actualSourceLimitProfile
    case ordinary: MySqlOwnedIoReply.OrdinaryCertifiedBooted => ordinary.owned.actualSourceLimitProfile
    case recovering: MySqlOwnedIoReply.RecallRecoveringBooted => recovering.actualSourceLimitProfile

  def ordinaryFacts(value: MySqlProductionBootReply): Option[MySqlOrdinaryFacts] = value match
    case _:MySqlOwnedIoReply.DeploymentReceiptRecallRecoveringBooted =>
      throw new IllegalStateException("restricted schema5 source pins are not legacy ordinary facts")
    case _: MySqlOwnedIoReply.DeploymentReceiptBooted =>
      throw new IllegalStateException("schema5 cannot use legacy ordinaryFacts; package C consumer required")
    case _: MySqlOwnedIoReply.FreshBooted => None
    case ordinary: MySqlOwnedIoReply.OrdinaryCertifiedBooted => Some(ordinary.owned.ordinaryFacts)
    case recovering: MySqlOwnedIoReply.RecallRecoveringBooted => Some(recovering.ordinaryFacts)

  /** 纯DATA身份比较；调用方仍须各自原mailbox/opaque link及原Boot epoch。 */
  private[floatingpod] def sessionIdentity(value: MySqlProductionBootReply):
      (java.util.UUID, Long, Long, java.util.UUID, Long) = value match
    case value:MySqlOwnedIoReply.DeploymentReceiptRecallRecoveringBooted =>
      val link=value.owned.recovery
      (link.identity.bootId,link.identity.bootGeneration,link.identity.connectionGeneration,link.bindingId,link.writerEpoch)
    case value: MySqlOwnedIoReply.DeploymentReceiptBooted =>
      val b = value.owned
      (b.identity.bootId, b.identity.bootGeneration, b.identity.connectionGeneration, b.bindingId, b.writerEpoch)
    case recovering: MySqlOwnedIoReply.RecallRecoveringBooted =>
      val link = recovering.recovery
      (link.identity.bootId, link.identity.bootGeneration, link.identity.connectionGeneration, link.bindingId, link.writerEpoch)
    case _ =>
      val a = databaseAdmission(value)
      (a.bootId, a.bootGeneration, a.connectionGeneration, a.bindingId, a.writerEpoch)
