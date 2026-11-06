package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{WindowsB1TwoSlotStore, WindowsMarkerIo, WindowsWorldProvisioner}

/** 原Session单槽持有的Ordinary native权限；不由Fresh权限、Main回执或队列空单独签发。 */
final class MySqlCertifiedOrdinaryNativeExitPermit private[mysql] (
    private var session: MySqlOwnedDatabaseSession,
    private var actor: MySqlCertifiedRecoveryInspectionActor,
    private var holder: MySqlCertifiedRestoreNativeHolder,
    private var root: MySqlFormat3ReadBudget,
    private var inspection: WindowsWorldProvisioner.ResumeInspection,
    private var world: WindowsWorldProvisioner.ProvisionedWorld,
    private var installed: WindowsB1TwoSlotStore.InstalledResumeRecordScope,
    private var acknowledgement: WindowsB1TwoSlotStore.DurableHandoffNativeAck,
    private var io: WindowsMarkerIo):
  private var phase = 0
  private def requireLiveIdentity(): Unit =
    if session == null || phase == 3 then throw new IllegalStateException("original Ordinary native identity was retired")

  private[mysql] def requirePrepared(actualSession: MySqlOwnedDatabaseSession): Unit =
    if (actualSession ne session) || phase != 0 then throw new IllegalStateException("exact original prepared Ordinary native permit required")
    requireLiveIdentity()
    session.requireCertifiedOrdinaryNativeExitPermit(this)

  private[mysql] def sqlSettlementReturned(actualSession: MySqlOwnedDatabaseSession): Unit =
    requirePrepared(actualSession)
    session.requireOrdinarySqlSettlement(this)
    phase=1

  private[mysql] def requireHolder(actual: MySqlCertifiedRestoreNativeHolder): Unit =
    requireLiveIdentity()
    session.requireCertifiedOrdinaryNativeExitPermit(this)
    if (actual ne holder) || phase != 1 || root.isClosed then throw new IllegalStateException("exact SQL-settled Ordinary native owner required")
    actor.requireNativeExitPermit(this,holder,root)

  def requireInstalledIdentity(actualInspection: WindowsWorldProvisioner.ResumeInspection,
      actualWorld: WindowsWorldProvisioner.ProvisionedWorld,
      actualInstalled: WindowsB1TwoSlotStore.InstalledResumeRecordScope,
      actualAcknowledgement: WindowsB1TwoSlotStore.DurableHandoffNativeAck,
      actualIo: WindowsMarkerIo): Unit =
    requireLiveIdentity()
    session.requireCertifiedOrdinaryNativeExitPermit(this)
    if phase != 1 || (actualInspection ne inspection) || (actualWorld ne world) ||
        (actualInstalled ne installed) || (actualAcknowledgement ne acknowledgement) || (actualIo ne io) || root.isClosed then
      throw new IllegalStateException("exact installed Ordinary inspection/world/ACK/IO required")
    actor.requireNativeExitPermit(this,holder,root)

  private[mysql] def nativeCloseReturned(actual: MySqlCertifiedRestoreNativeHolder): Unit =
    requireHolder(actual)
    phase=2

  private[mysql] def requireNativeClosed(actualSession: MySqlOwnedDatabaseSession): Unit =
    requireLiveIdentity()
    session.requireCertifiedOrdinaryNativeExitPermit(this)
    if (actualSession ne session) || phase != 2 then throw new IllegalStateException("actual Ordinary native close required")

  private[mysql] def originalActorForHolder(actual: MySqlCertifiedRestoreNativeHolder): MySqlCertifiedRecoveryInspectionActor =
    requireHolder(actual)
    actor

  def requireClosedInstalledIdentity(actual: WindowsB1TwoSlotStore.InstalledResumeRecordScope): Unit =
    requireLiveIdentity()
    session.requireCertifiedOrdinaryNativeExitPermit(this)
    if phase != 2 || (actual ne installed) then throw new IllegalStateException("actual closed original installed identity required")

  /** 所有native身份消费者与实际root关闭后才撤九条原强边；失败保留原槽。 */
  private[mysql] def detachAfterRootClose(actualSession: MySqlOwnedDatabaseSession): Unit =
    requireNativeClosed(actualSession)
    if root == null || !root.isClosed then throw new IllegalStateException("actual original Ordinary root close required")
    session=null;actor=null;holder=null;root=null;inspection=null;world=null
    installed=null;acknowledgement=null;io=null
    phase=3

  private[mysql] def requireDetachedShell(): Unit =
    if phase != 3 || session != null || actor != null || holder != null || root != null || inspection != null ||
        world != null || installed != null || acknowledgement != null || io != null then
      throw new IllegalStateException("original Ordinary native permit still retains identity responsibility")
