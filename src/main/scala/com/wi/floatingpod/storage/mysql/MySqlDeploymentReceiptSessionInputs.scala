package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.MySqlRuntimeMetadata
import java.nio.file.Path
import java.time.Duration
import java.util.UUID

/** 同一静态装配的受控借用；不是 SQL、native、恢复或 Ready 资格。 */
private[mysql] final class MySqlDeploymentReceiptSessionInputs private (
    private val owner: MySqlRuntimeInputs.AssemblyOwner,
    private val root: MySqlFormat3ReadBudget,
    private val identity: MySqlOwnedIoIdentity,
    private val requestedMode: MySqlDeploymentReceiptSessionInputs.ModeRequest,
    private var graphValue: MySqlRuntimeInputs.DeploymentReceiptStaticGraph,
    private var settingsValue: MySqlConnectionSettings,
    private var expectationValue: MySqlSchemaAuditExpectation,
    private var full26Value: MySqlScriptSchemaInputs,
    private var bindingValue: MySqlManifestConfigurationIdentity.DeploymentReceiptBinding,
    private var auditValue: MySqlDeploymentReceiptSchemaAuditInputs,
    private var lease: MySqlRuntimeInputs.ConsumerLease,
    private val activeLimitValue: Int,
    private var retainedFee: Long
) extends AutoCloseable:
  private var closed = false
  private var closing = false
  private var closeFailure: Throwable = null
  private def requireOpen(): Unit =
    if closed || closing then throw new IllegalStateException("schema5 Session inputs detached or cleanup incomplete", closeFailure)
    owner.requireDeploymentSessionInputs(this, root)
    bindingValue.requireLease(lease)
  def requireForSession(exactRoot: MySqlFormat3ReadBudget, actualIoIdentity: MySqlOwnedIoIdentity,
      exactMode: MySqlDeploymentReceiptSessionInputs.ModeRequest): Unit =
    requireOpen()
    if (root ne exactRoot) || (identity ne actualIoIdentity) || requestedMode != exactMode then
      throw new IllegalArgumentException("exact schema5 Session root/IO identity/mode required")
  def settings: MySqlConnectionSettings = { requireOpen(); settingsValue }
  def activeLimit: Int = { requireOpen(); activeLimitValue }
  def auditExpectation: MySqlSchemaAuditExpectation = { requireOpen(); expectationValue }
  def full26Inputs: MySqlScriptSchemaInputs = { requireOpen(); full26Value }
  def sourceLimitProfile = { requireOpen(); full26Value.sourceLimitProfile }
  def configurationSha256: String = { requireOpen(); bindingValue.hex }
  def requireCompleteConfigurationPin(controlled: MySqlFreshControlledMetadata): Unit =
    requireOpen()
    require(controlled != null,"controlled complete-A pin required")
    MySqlManifestConfigurationIdentity.temporary(root,bindingValue.semantics.codecWorkspace) {
      val actual=com.wi.floatingpod.server.WindowsB1SourceAnchorStore.AnchorConfiguration.ofDeploymentReceiptMetadata(
        graphValue.nativeMetadata(lease,root))
      require(actual.configurationSha256().equalsIgnoreCase(controlled.approvedConfigurationSha256),
        "complete configuration A differs from independent control pin")
    }
  private[mysql] def cleanupObservation: MySqlDeploymentReceiptSessionInputs.CleanupObservation =
    MySqlDeploymentReceiptSessionInputs.CleanupObservation(closed,auditValue != null,lease != null,retainedFee,Option(closeFailure))
  def beginFreshNative(worldPath: Path,
      plan: com.wi.floatingpod.server.WindowsWorldProvisionReceiptDataCodec.PlanData,
      boot: com.wi.floatingpod.server.WindowsWorldProvisioner.BootIdentity,
      controlled: MySqlFreshControlledMetadata): MySqlDeploymentReceiptFreshNativeOwner =
    requireOpen()
    require(requestedMode == MySqlDeploymentReceiptSessionInputs.Fresh, "Fresh schema5 mode required")
    require(boot != null && boot.bootId() == identity.bootId && boot.generation() == identity.bootGeneration,
      "native boot differs from exact Session IO identity")
    requirePlanData(plan)
    MySqlDeploymentReceiptFreshNativeOwner.begin(root, graphValue, worldPath, plan, boot, controlled)
  def requirePlanData(plan: com.wi.floatingpod.server.WindowsWorldProvisionReceiptDataCodec.PlanData): Unit =
    requireOpen(); bindingValue.semantics.recipe.requirePlanData(plan)
  def beginOrdinaryNative(original: com.wi.floatingpod.bootstrap.ProductionBootstrapControl.OriginalSourcePins,
      boot: com.wi.floatingpod.server.WindowsWorldProvisioner.BootIdentity,
      controlled: MySqlFreshControlledMetadata): MySqlDeploymentReceiptFreshNativeOwner =
    requireOpen()
    require(requestedMode == MySqlDeploymentReceiptSessionInputs.Ordinary,"Ordinary schema5 mode required")
    require(original != null && boot != null && boot.bootId() == identity.bootId && boot.generation() == identity.bootGeneration,
      "Ordinary native current boot differs from exact Session IO identity")
    requirePlanData(original.plan())
    requireCompleteConfigurationPin(controlled)
    require(original.configurationSha256().equalsIgnoreCase(controlled.approvedConfigurationSha256),"Ordinary original complete A differs from current control")
    MySqlDeploymentReceiptFreshNativeOwner.beginOrdinary(root,graphValue,original,boot,controlled)
  def recipeBytes: Array[Byte] =
    requireOpen()
    MySqlManifestConfigurationIdentity.temporary(root, bindingValue.semantics.codecWorkspace) {
      bindingValue.semantics.recipe.canonicalBytes
    }
  private[mysql] def borrowForPublication():MySqlRuntimeInputs.ConsumerLease =
    requireOpen()
    owner.requireDeploymentAssemblyRoot(root)
    root.requireScriptDataInputs(full26Value)
    graphValue.borrowForPublication(root)
  override def close(): Unit =
    closeInputs(false)
  private[mysql] def closeUnpublished(): Unit = closeInputs(true)
  private def closeInputs(unpublished: Boolean): Unit =
    if closeFailure != null then throw closeFailure
    if closed then return
    // 清理仍须原owner/thread/root，但root失效不能阻止结算各实际资源槽。
    owner.requireDeploymentAssemblyRoot(root)
    closing=true
    def settle(action: => Unit): Boolean =
      try { action;true }
      catch case failure: Throwable =>
        if closeFailure == null then closeFailure = failure
        else MySqlOwnedSessionLifecycle.suppress(closeFailure,failure)
        false
    // auditor 的使用必须已经结束；它的借用与 Session 借用分别结算。
    if auditValue != null && settle(auditValue.close()) then auditValue=null
    if lease != null && settle(lease.close()) then lease=null
    if auditValue == null && lease == null then
      settingsValue=null;expectationValue=null;full26Value=null;bindingValue=null;graphValue=null
      if retainedFee != 0L && settle(root.releaseRetainedMemory(retainedFee,"schema5 Session inputs references dropped; root binding retained")) then retainedFee=0L
    closed = auditValue == null && lease == null && retainedFee == 0L
    if closeFailure != null then throw closeFailure
  override def toString: String = "MySqlDeploymentReceiptSessionInputs[redacted; no admission]"

private[mysql] object MySqlDeploymentReceiptSessionInputs:
  private[mysql] final case class CleanupObservation(closed: Boolean,auditPending: Boolean,
      leasePending: Boolean,retainedFee: Long,failure: Option[Throwable])
  sealed trait ModeRequest
  case object Fresh extends ModeRequest
  case object Ordinary extends ModeRequest

  private[mysql] def create(owner: MySqlRuntimeInputs.AssemblyOwner, root: MySqlFormat3ReadBudget,
      identity: MySqlOwnedIoIdentity, mode: ModeRequest, metadata: MySqlRuntimeMetadata.DeploymentReceiptStatic,
      graph: MySqlRuntimeInputs.DeploymentReceiptStaticGraph,
      baseline: MySqlApprovedStaticBaseline, binding: MySqlManifestConfigurationIdentity.DeploymentReceiptBinding,
      policy: MySqlRuntimeAuditPolicy, lease: MySqlRuntimeInputs.ConsumerLease,
      audit: MySqlDeploymentReceiptSchemaAuditInputs, retainedFee: Long,
      full26: MySqlScriptSchemaInputs): MySqlDeploymentReceiptSessionInputs =
    binding.requireLease(lease)
    val m = metadata.deploymentSemantics().canonicalFields()
    def text(key: String): String = m.get(key)
    val settings = MySqlConnectionSettings(text("host"), text("port").toInt, text("schema"),
      UUID.fromString(text("expectedServerUuid")), text("expectedEffectiveUser"),
      MySqlFileSources.credentials(Thread.currentThread(), text("username"),
        new MySqlRuntimeMetadata.FileReference(Path.of(text("credentialSourceRef").substring(5)))),
      Path.of(text("pkcs12TrustStorePath")),
      MySqlFileSources.trustSecret(Thread.currentThread(),
        new MySqlRuntimeMetadata.FileReference(Path.of(text("trustSecretSourceRef").substring(5)))),
      Duration.ofMillis(text("connectTimeoutMillis").toLong), Duration.ofMillis(text("socketTimeoutMillis").toLong),
      MySqlDurabilityExpectation(text("innodbFlushLogAtTrxCommit").toInt, text("syncBinlog").toLong))
    val id = baseline.identity
    val expectation = MySqlSchemaAuditExpectation.forDeploymentReceipt1(id.serverUuid, id.serverVersion, id.schema,
      audit.effectiveUser, audit.callerRoles, id.provisioningUuid, id.bindingUuid, id.ddlSha256, id.auditDefiner,
      audit.auditDefinerRoles, policy.expectedImport, baseline.objects, audit)
    val p = binding.semantics.profile
    require(root.profile eq p.sourceLimitProfile, "schema5 Session root requires actual K profile reference")
    require(full26 != null && (full26.sourceLimitProfile eq root.profile), "original attempt Full26 inputs required")
    new MySqlDeploymentReceiptSessionInputs(owner, root, identity, mode, graph, settings, expectation, full26, binding,
      audit, lease, text("activeLimit").toInt, retainedFee)
