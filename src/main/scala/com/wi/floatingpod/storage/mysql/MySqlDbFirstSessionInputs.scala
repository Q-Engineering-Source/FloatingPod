package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.MySqlRuntimeMetadata
import java.nio.file.Path
import java.time.Duration
import java.util.UUID

/** 原schema6静态图的受控输入借用；不授予SQL、native、恢复或Ready资格。 */
private[mysql] final class MySqlDbFirstSessionInputs private (
    private val owner: MySqlRuntimeInputs.AssemblyOwner,
    private val root: MySqlFormat3ReadBudget,
    private val identity: MySqlOwnedIoIdentity,
    private var settingsValue: MySqlConnectionSettings,
    private var expectationValue: MySqlSchemaAuditExpectation,
    private var full27Value: MySqlScriptSchemaInputs,
    private var bindingValue: MySqlManifestConfigurationIdentity.DbFirstBinding,
    private var auditValue: MySqlDbFirstSchemaAuditInputs,
    private var lease: MySqlRuntimeInputs.ConsumerLease,
    private var retainedFee: Long
) extends AutoCloseable:
  private var closed=false
  private var closing=false
  private var closeFailure: Throwable=null
  private var liveReaders=0
  private var serviceAttempted=false
  private var serviceOwner:MySqlDbFirstSession=null
  private var serviceIdentity:MySqlOwnedIoIdentity=null
  private var serviceSource:MySqlDbFirstSession.RetiredRecallWriterRecovery=null
  private var serviceBindingFee=0L
  private var serviceAdapter:MySqlOwnedIoAdapter=null
  private var finalCloseOwner:MySqlDbFirstSession=null
  def requireOpen(): Unit =
    if closed || closing then throw new IllegalStateException("schema6 inputs detached or cleanup incomplete",closeFailure)
    owner.requireDbFirstSessionInputs(this,root)
    bindingValue.requireLease(lease)
    root.requireScriptDataInputs(full27Value)
  def requireForSession(exactRoot: MySqlFormat3ReadBudget, actualIoIdentity: MySqlOwnedIoIdentity): Unit =
    requireOpen()
    if root ne exactRoot then
      throw new IllegalArgumentException("exact schema6 root and boot/connection IO identity required")
    if identity ne actualIoIdentity then
      if serviceOwner==null || (serviceIdentity ne actualIoIdentity) || serviceSource==null && serviceAdapter==null then
        throw new IllegalArgumentException("exact schema6 original or privately transferred service identity required")
      if serviceAdapter!=null then serviceAdapter.requireRegisteredRecoveredService(serviceOwner,this,root,actualIoIdentity)
      else serviceSource.requireServiceInputBinding(serviceOwner,this,root,actualIoIdentity)
  /** 只返回本inputs中已实际登记的服务owner；原身份继续走原快照issuer。 */
  private[mysql] def serviceOwnerForSnapshot(exactRoot:MySqlFormat3ReadBudget,
      actualIdentity:MySqlOwnedIoIdentity):MySqlDbFirstSession =
    requireForSession(exactRoot,actualIdentity)
    if actualIdentity eq identity then null else serviceOwner
  /** 仅实际Recovery已登记的新Session可增加精确服务身份；原历史身份/配置/root绑定不变。 */
  private[mysql] def bindServiceOwner(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      actual:MySqlDbFirstSession,exactRoot:MySqlFormat3ReadBudget,next:MySqlOwnedIoIdentity):Unit =
    requireOpen()
    require(source!=null && actual!=null && next!=null && !serviceAttempted && serviceOwner==null &&
      (root eq exactRoot),"actual one-shot service input transfer required")
    source.requireServiceInputBinding(actual,this,exactRoot,next)
    serviceAttempted=true
    root.reserveRetainedMemory(256L,"schema6 exact service input binding slots BEFORE registration")
    serviceBindingFee=256L
    serviceOwner=actual;serviceIdentity=next;serviceSource=source
  private[mysql] def bindRegisteredServiceRuntime(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      actual:MySqlDbFirstSession,adapter:MySqlOwnedIoAdapter,exactRoot:MySqlFormat3ReadBudget,
      next:MySqlOwnedIoIdentity):Unit =
    requireOpen()
    require(source!=null && (serviceSource eq source) && serviceAdapter==null && (serviceOwner eq actual) &&
      (finalCloseOwner eq actual) && (root eq exactRoot) && (serviceIdentity eq next) && serviceBindingFee==256L,
      "actual original service input/final-close binding required before runtime proof transfer")
    adapter.requireRegisteredRecoveredService(actual,this,exactRoot,next)
    serviceAdapter=adapter;serviceSource=null

  private[mysql] def detachClosedServiceOwner(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      actual:MySqlDbFirstSession,exactRoot:MySqlFormat3ReadBudget,next:MySqlOwnedIoIdentity):Unit =
    requireOpen()
    require(source!=null && ((serviceSource eq source) || serviceAdapter!=null) && actual!=null && (serviceOwner eq actual) &&
      (root eq exactRoot) && (serviceIdentity eq next),
      "actual closed service Session and exact binding required")
    if serviceAdapter!=null then serviceAdapter.requireRegisteredRecoveredService(actual,this,exactRoot,next)
    source.requireServiceInputBinding(actual,this,exactRoot,next)
    actual.requireServiceResourcesBeforeInputClose(source)
    serviceOwner=null;serviceIdentity=null;serviceSource=null;serviceAdapter=null
    root.releaseRetainedMemory(serviceBindingFee,"closed service input binding detached")
    serviceBindingFee=0L
  /** 仅撤未转入SQL/最终close责任的空service壳绑定；不构造JDBC关闭事实。 */
  private[mysql] def detachStoppedEmptyServiceOwner(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      actual:MySqlDbFirstSession,exactRoot:MySqlFormat3ReadBudget,next:MySqlOwnedIoIdentity,
      bindingReturned:Boolean):Unit =
    requireOpen()
    require(source!=null && actual!=null && (root eq exactRoot) && ((serviceIdentity eq next)==bindingReturned) &&
      finalCloseOwner==null && serviceAdapter==null,
      "actual stopped empty service binding context required")
    source.requireStoppedServiceShell(actual,closingAllowed=true)
    if bindingReturned then
      require((serviceSource eq source) && (serviceOwner eq actual) && serviceBindingFee==256L,
        "actual returned service input binding required for empty-shell detach")
      serviceOwner=null;serviceIdentity=null;serviceSource=null
      root.releaseRetainedMemory(serviceBindingFee,"stopped empty service input binding detached")
      serviceBindingFee=0L
    else
      require(serviceOwner==null && serviceIdentity==null && serviceSource==null && serviceBindingFee==0L,
        "failed unpublished service input binding must retain no actual owner")
  private[mysql] def detachRegisteredServiceOwner(actual:MySqlDbFirstSession,adapter:MySqlOwnedIoAdapter,
      exactRoot:MySqlFormat3ReadBudget,next:MySqlOwnedIoIdentity):Unit =
    requireOpen()
    require(actual!=null && adapter!=null && (serviceOwner eq actual) && (serviceAdapter eq adapter) &&
      serviceSource==null && (root eq exactRoot) && (serviceIdentity eq next) && (finalCloseOwner eq actual),
      "actual owned service closing input binding required")
    adapter.requireRegisteredRecoveredService(actual,this,exactRoot,next)
    actual.requireOwnedServiceResourcesBeforeInputs(adapter)
    serviceOwner=null;serviceIdentity=null;serviceAdapter=null
    root.releaseRetainedMemory(serviceBindingFee,"actual owned service input runtime binding detached")
    serviceBindingFee=0L

  /** 旧读图与borrow实际退休后才转最终close责任；身份binding仍由同一Recovery保管。 */
  private[mysql] def transferFinalCloseOwner(source:MySqlDbFirstSession.RetiredRecallWriterRecovery,
      previous:MySqlDbFirstSession,actual:MySqlDbFirstSession,next:MySqlOwnedIoIdentity):Unit =
    requireOpen()
    require(source!=null && (serviceSource eq source) && (serviceOwner eq actual) &&
      (serviceIdentity eq next) && finalCloseOwner==null && liveReaders==1,
      "actual sole new service borrow and untransferred final-close responsibility required")
    source.requireInputCloseTransfer(previous,actual,this,root,next)
    finalCloseOwner=actual

  private[mysql] def requireFinalCloseOwner(actual:MySqlDbFirstSession):Unit =
    require(actual!=null && (finalCloseOwner eq actual),"actual privately transferred inputs close owner required")

  private[mysql] def closeFromTransferredSession(actual:MySqlDbFirstSession):Unit =
    requireFinalCloseOwner(actual)
    actual.requireOwnedInputClose(this)
    closeInputs(true)

  /** None仅表示K6未提供可信历史来源，不表示历史检查完成。 */
  def historicalInputs: Option[MySqlDbFirstHistoryInputs] = { requireOpen();owner.historicalInputs }
  def settings: MySqlConnectionSettings = { requireOpen();settingsValue }
  def auditExpectation: MySqlSchemaAuditExpectation = { requireOpen();expectationValue }
  def full27Inputs: MySqlScriptSchemaInputs = { requireOpen();full27Value }
  def ioIdentity: MySqlOwnedIoIdentity = { requireOpen();identity }
  def configurationSha256: String = { requireOpen();bindingValue.hex }
  def sourceLimitProfile = { requireOpen();full27Value.sourceLimitProfile }
  /** 仅原K明确绑定的托管既有库可进入首次物理绑定；不得从数据库观察推导此权限。 */
  private[mysql] def managedFirstBindingAllowed: Boolean =
    requireOpen()
    val semantics=bindingValue.semantics
    semantics.family == MySqlDeploymentReceiptRuntimeContract.DbFirstFamily &&
      semantics.rendered.managedExistingDatabase && semantics.text("sourceProvenanceKind") == "MANAGED_INSTALL"
  /** 首次绑定配额来自同一受控K/profile；外部schema6没有可借用的安装配额。 */
  private[mysql] def firstBindingQuotaMaximumRecords: Long =
    requireOpen()
    if !managedFirstBindingAllowed then
      throw new IllegalStateException("schema6 first-binding quota requires exact managed-install provenance")
    val maximum=bindingValue.semantics.profile.quotaMaximumRecords
    require(maximum > 0L,"managed first-binding quota must be positive")
    maximum
  /** 仅延长原输入/security借用；不授予SQL或Ready资格。 */
  private[mysql] def borrowForReader(): AutoCloseable =
    requireOpen()
    val next=Math.addExact(liveReaders,1)
    root.reserveRetainedMemory(64L,"schema6 reader borrow holder BEFORE allocation")
    try
      val token=new ReaderBorrow()
      liveReaders=next
      token
    catch case cause: Throwable =>
      try root.releaseRetainedMemory(64L,"unpublished schema6 reader borrow holder")
      catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      throw cause
  private final class ReaderBorrow extends AutoCloseable:
    private var open=true
    override def close(): Unit =
      // 包括重复close在内，均先核原worker；失败不得消耗token或改变计数。
      owner.requireDbFirstAssemblyRoot(root)
      if open then
        requireOpen()
        if liveReaders <= 0 then throw new IllegalStateException("schema6 reader borrow count lost")
        root.releaseRetainedMemory(64L,"schema6 reader borrow holder detached")
        liveReaders-=1
        open=false
  private[mysql] def cleanupObservation: MySqlDeploymentReceiptSessionInputs.CleanupObservation =
    MySqlDeploymentReceiptSessionInputs.CleanupObservation(closed,auditValue != null,lease != null,retainedFee,Option(closeFailure))
  override def close(): Unit = closeInputs(false)
  private[mysql] def closeUnpublished(): Unit = closeInputs(false)
  private def closeInputs(transferred:Boolean): Unit =
    if closeFailure != null then throw closeFailure
    if closed then return
    owner.requireDbFirstAssemblyRoot(root)
    if finalCloseOwner!=null && !transferred then
      throw new IllegalStateException("schema6 inputs final close belongs to actual transferred service")
    if serviceOwner!=null || serviceBindingFee!=0L then
      throw new IllegalStateException("schema6 inputs retain actual service identity/cleanup responsibility")
    if liveReaders != 0 then throw new IllegalStateException("schema6 inputs still have live reader borrows")
    closing=true
    def settle(action: => Unit): Boolean =
      try { action;true }
      catch case failure: Throwable =>
        if closeFailure == null then closeFailure=failure else MySqlOwnedSessionLifecycle.suppress(closeFailure,failure)
        false
    if auditValue != null && settle(auditValue.close()) then auditValue=null
    if lease != null && settle(lease.close()) then lease=null
    if auditValue == null && lease == null then
      settingsValue=null;expectationValue=null;full27Value=null;bindingValue=null;finalCloseOwner=null
      if retainedFee != 0L && settle(root.releaseRetainedMemory(retainedFee,"schema6 Session references detached; root binding retained")) then retainedFee=0L
    closed=auditValue == null && lease == null && retainedFee == 0L
    if closeFailure != null then throw closeFailure
  override def toString: String = "MySqlDbFirstSessionInputs[redacted; no admission]"

private[mysql] object MySqlDbFirstSessionInputs:
  private[mysql] def create(owner: MySqlRuntimeInputs.AssemblyOwner,root: MySqlFormat3ReadBudget,
      identity: MySqlOwnedIoIdentity,metadata: MySqlRuntimeMetadata.DbFirstStatic,
      binding: MySqlManifestConfigurationIdentity.DbFirstBinding,lease: MySqlRuntimeInputs.ConsumerLease,
      audit: MySqlDbFirstSchemaAuditInputs,expectation: MySqlSchemaAuditExpectation,
      retainedFee: Long,full27: MySqlScriptSchemaInputs): MySqlDbFirstSessionInputs =
    binding.requireLease(lease)
    val m=metadata.deploymentSemantics().canonicalFields()
    def text(key: String): String=m.get(key)
    val baseSettings=MySqlConnectionSettings(text("host"),text("port").toInt,text("schema"),
      UUID.fromString(text("expectedServerUuid")),text("expectedEffectiveUser"),
      MySqlFileSources.credentials(Thread.currentThread(),text("username"),
        new MySqlRuntimeMetadata.FileReference(Path.of(text("credentialSourceRef").substring(5)))),
      Path.of(text("pkcs12TrustStorePath")),
      MySqlFileSources.trustSecret(Thread.currentThread(),
        new MySqlRuntimeMetadata.FileReference(Path.of(text("trustSecretSourceRef").substring(5)))),
      Duration.ofMillis(text("connectTimeoutMillis").toLong),Duration.ofMillis(text("socketTimeoutMillis").toLong),
      MySqlDurabilityExpectation(text("innodbFlushLogAtTrxCommit").toInt,text("syncBinlog").toLong))
    val semantics = binding.semantics
    val settings = if semantics.family == MySqlDeploymentReceiptRuntimeContract.DbFirstFamily &&
      semantics.rendered.managedExistingDatabase && semantics.text("sourceProvenanceKind") == "MANAGED_INSTALL" then
      baseSettings.withUtf8mb4Results
    else baseSettings
    require((root.profile eq binding.semantics.profile.sourceLimitProfile) && (full27.sourceLimitProfile eq root.profile),
      "schema6 inputs require exact original K/root profile")
    require(full27.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27 && expectation.profile == MySqlSchemaAuditProfile.DbFirst1,
      "schema6 Full27 and actual security expectation required")
    new MySqlDbFirstSessionInputs(owner,root,identity,settings,expectation,full27,binding,audit,lease,retainedFee)
