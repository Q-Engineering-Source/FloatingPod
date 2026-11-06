package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.MySqlRuntimeMetadata
import java.nio.file.{Files, LinkOption, StandardOpenOption}
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.sql.SQLDataException
import java.util.{HexFormat, UUID}

/** 原schema6 owner持有的显式schema5历史DATA；不提供native、Session或SQL资格。 */
private[mysql] final class MySqlDbFirstHistoryInputs private (
    private val owner: MySqlRuntimeInputs.AssemblyOwner,
    private val root: MySqlFormat3ReadBudget,
    private val source: MySqlDbFirstRuntimeContract.HistorySource
):
  private var baseline: MySqlApprovedBaselineLoader.OwnedBaseline=null
  private var contract: MySqlDeploymentReceiptRuntimeContract.Loaded=null
  private var metadata: MySqlRuntimeMetadata.DeploymentReceiptStatic=null
  private var value: MySqlScriptSchemaInputs=null
  private var metadataFee=0L
  private var inputFee=0L
  private var holderFee=MySqlDbFirstHistoryInputs.HolderFee
  private var liveBorrows=0
  private var attempted=false
  private var closed=false
  private var cleanupFailure: Throwable=null
  private var failure: Throwable=null
  def requireOpen(): Unit =
    owner.requireHistoryOwner(this,root,false)
    if closed || value == null || failure != null then throw new IllegalStateException("historical DATA unavailable",failure)
  def inputs: MySqlScriptSchemaInputs = { requireOpen();value }
  def firstFailure: Option[Throwable] = { owner.requireHistoryCleanup(this,root);Option(failure) }
  /** 借用绑定原owner/root/inputs，不能由相同值的新wrapper冒充。 */
  def borrow(): Borrow =
    requireOpen()
    val next=Math.addExact(liveBorrows,1)
    root.reserveRetainedMemory(64L,"historical borrow wrapper BEFORE allocation")
    var lease: MySqlRuntimeInputs.ConsumerLease=null
    try
      lease=owner.newHistoryLease()
      val result=new Borrow(lease)
      liveBorrows=next
      result
    catch case cause: Throwable =>
      if lease != null then
        try lease.close() catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      try root.releaseRetainedMemory(64L,"unpublished historical borrow wrapper")
      catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
      throw cause
  final class Borrow private[MySqlDbFirstHistoryInputs] (private val lease: MySqlRuntimeInputs.ConsumerLease)
      extends MySqlDbFirstHistoryInputs.PayloadBorrow:
    private var open=true
    private var wrapperFee=64L
    def requireInputs(exactRoot: MySqlFormat3ReadBudget, actual: MySqlScriptSchemaInputs): Unit =
      requireOpen()
      if !open || (exactRoot ne root) || (actual ne value) then throw new IllegalStateException("exact historical DATA borrow required")
      lease.requireStatic(owner)
    override def close(): Unit =
      owner.requireHistoryCleanup(MySqlDbFirstHistoryInputs.this,root)
      if open then
        lease.requireStatic(owner)
        if wrapperFee != 0L then
          root.releaseRetainedMemory(wrapperFee,"historical borrow wrapper detached")
          wrapperFee=0L
        lease.close()
        liveBorrows-=1
        open=false
  private[mysql] def requireBaseline(actual: MySqlApprovedStaticBaseline): Unit =
    owner.requireHistoryOwner(this,root,true)
    if baseline == null || (baseline.baselineFor(root) ne actual) then throw new IllegalStateException("exact historical B slot required")
  private[mysql] def adoptContract(actual: MySqlDeploymentReceiptRuntimeContract.Loaded): Unit =
    owner.requireHistoryOwner(this,root,true)
    if contract != null || actual == null || (actual.semantics.owner ne owner) ||
        actual.semantics.family != MySqlDeploymentReceiptRuntimeContract.DeploymentFamily then
      throw new IllegalStateException("exact vacant historical K slot required")
    contract=actual
  private[mysql] def load(current: MySqlDeploymentReceiptRuntimeContract.Semantics): Unit =
    owner.requireHistoryOwner(this,root,true)
    if attempted then throw new IllegalStateException("historical input load is single-use")
    attempted=true
    try
      val before=Files.readAttributes(source.path,classOf[BasicFileAttributes],LinkOption.NOFOLLOW_LINKS)
      val n=before.size()
      if before.isSymbolicLink || !before.isRegularFile || n < 0 || n > MySqlRuntimeMetadata.MAX_METADATA_BYTES then
        throw new SQLDataException("bounded nofollow historical metadata required")
      MySqlManifestConfigurationIdentity.temporary(root,Math.addExact(65536L,Math.multiplyExact(n,12L))) {
        val raw=new Array[Byte](Math.toIntExact(n))
        val stream=Files.newInputStream(source.path,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)
        var primary: Throwable=null
        try
          var offset=0
          while offset < raw.length do
            val count=stream.read(raw,offset,raw.length-offset)
            if count < 0 then throw new SQLDataException("historical metadata truncated")
            offset+=count
          if stream.read() != -1 then throw new SQLDataException("historical metadata grew beyond precharge")
        catch
          case cause: Throwable =>
            primary=cause
            throw cause
        finally
          try stream.close() catch case cleanup: Throwable =>
            if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
        val after=Files.readAttributes(source.path,classOf[BasicFileAttributes],LinkOption.NOFOLLOW_LINKS)
        if after.size() != n || before.fileKey() != after.fileKey() || before.lastModifiedTime() != after.lastModifiedTime() ||
            HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(raw)) != source.sha256 then
          throw new SQLDataException("historical metadata changed or raw SHA pin differs")
        val fee=Math.addExact(4096L,Math.multiplyExact(n,48L))
        root.reserveRetainedMemory(fee,"historical metadata BEFORE decode")
        metadataFee=fee
        metadata=MySqlRuntimeMetadata.fromDeploymentReceiptBytes(raw)
      }
      val m=metadata.deploymentSemantics().canonicalFields()
      Vector("expectedServerUuid" -> "serverUuid","expectedBindingUuid" -> "bindingUuid","schema" -> "schema").foreach { (mk,ck) =>
        if m.get(mk) != current.text(ck) then throw new SQLDataException("historical/current identity differs: " + mk)
      }
      val pin=metadata.baseline()
      val id=MySqlApprovedBaselineIdentity(pin.format(),pin.schemaFormat(),pin.coverage(),UUID.fromString(m.get("expectedServerUuid")),
        pin.serverVersion(),m.get("schema"),pin.provisioningUuid(),UUID.fromString(m.get("expectedBindingUuid")),
        pin.ddlSha256(),pin.auditDefiner(),pin.sourceCaptureSha256())
      baseline=MySqlApprovedBaselineLoader.loadOwned(pin.absolutePath(),pin.fileSha256(),id,root)
      MySqlDeploymentReceiptRuntimeContract.loadHistory(metadata,baseline.baselineFor(root),owner)
      val k=contract.semantics
      Vector("expectedServerUuid" -> "serverUuid","expectedBindingUuid" -> "bindingUuid",
        "expectedProvisioningUuid" -> "provisioningUuid","expectedServerVersion" -> "serverVersion",
        "schema" -> "schema","expectedAuditDefiner" -> "auditDefiner","expectedEffectiveUser" -> "effectiveUser").foreach { (mk,ck) =>
        if m.get(mk) != k.text(ck) then throw new SQLDataException("historical metadata/K identity differs: " + mk)
      }
      if !m.get("approvedDdlSha256").equalsIgnoreCase(k.rendered.sha256) then throw new SQLDataException("historical DDL differs")
      val p=k.profile
      root.reserveRetainedMemory(2048L,"historical full26 DATA inputs BEFORE allocation")
      inputFee=2048L
      value=MySqlScriptSchemaInputs.createDeploymentReceiptFull26(p.schema,k.rendered.sha256,root.profile,p.publicationBudget,p.deploymentBudget)
    catch
      case cause: Throwable =>
        failure=cause
        throw cause
  private[mysql] def closeOwned(): Unit =
    owner.requireHistoryRetirement(this,root)
    if cleanupFailure != null then throw cleanupFailure
    if closed then return
    if liveBorrows != 0 then throw new IllegalStateException("historical DATA still has live consumer borrows")
    value=null;metadata=null
    def settle(action: => Unit): Boolean =
      try { action;true }
      catch case cause: Throwable =>
        if cleanupFailure == null then cleanupFailure=cause else MySqlOwnedSessionLifecycle.suppress(cleanupFailure,cause)
        if failure != null then MySqlOwnedSessionLifecycle.suppress(failure,cause)
        false
    if contract != null && settle(contract.close()) then contract=null
    if baseline != null && settle(baseline.close()) then baseline=null
    if metadataFee != 0L && settle(root.releaseRetainedMemory(metadataFee,"historical metadata detached")) then metadataFee=0L
    if inputFee != 0L && settle(root.releaseRetainedMemory(inputFee,"historical DATA inputs detached")) then inputFee=0L
    if cleanupFailure != null then throw cleanupFailure
    if holderFee != 0L && settle(root.releaseRetainedMemory(holderFee,"historical holder detached")) then holderFee=0L
    if cleanupFailure != null then throw cleanupFailure
    closed=true

private[mysql] object MySqlDbFirstHistoryInputs:
  sealed trait PayloadBorrow extends AutoCloseable:
    def requireInputs(exactRoot: MySqlFormat3ReadBudget, actual: MySqlScriptSchemaInputs): Unit
  val HolderFee=1024L
  private[mysql] def allocate(owner: MySqlRuntimeInputs.AssemblyOwner, root: MySqlFormat3ReadBudget,
      source: MySqlDbFirstRuntimeContract.HistorySource): MySqlDbFirstHistoryInputs =
    new MySqlDbFirstHistoryInputs(owner,root,source)
