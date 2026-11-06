package com.wi.floatingpod.storage.mysql

import java.io.{DataOutputStream, OutputStream}
import java.security.{DigestOutputStream, MessageDigest}
import java.sql.SQLDataException
import java.util.{Arrays, HexFormat}

/** DATA identities do not issue admission. Constructors deliberately distinguish C from A. */
private[mysql] object MySqlManifestConfigurationIdentity:
  val Encoding = "FP_MYSQL_FORMAT3_MANIFEST_CONFIGURATION_BE_V2"
  val RecipeFormat = "FP_MYSQL_FORMAT3_CONFIGURATION_RECIPE_V2"
  val ContractFormat = "FP_MYSQL_FORMAT3_RUNTIME_CONTRACT_V2"
  val EncodingV3 = "FP_MYSQL_FORMAT3_MANIFEST_CONFIGURATION_BE_V3"
  val RecipeFormatV3 = "FP_MYSQL_FORMAT3_CONFIGURATION_RECIPE_V3"
  val ContractFormatV3 = "FP_MYSQL_FORMAT3_RUNTIME_CONTRACT_V3"

  /** Semantic C owner; not independent file approval or runtime admission. */
  final class ManifestSha256 private[MySqlManifestConfigurationIdentity] (
      private var digestOwner: CandidateDigest, private val root: MySqlFormat3ReadBudget
  ) extends AutoCloseable:
    private var closed=false
    def hex: String = synchronized {
      if closed || root.isClosed then throw new IllegalStateException("manifest semantic digest owner is closed")
      digestOwner.hex
    }
    override def close(): Unit = synchronized {
      if !closed then
        // The inner digest is still held if its release fails; do not manufacture a drop ACK.
        digestOwner.close()
        digestOwner=null
        closed=true
        root.releaseRetainedMemory(96L,"manifest semantic digest wrapper dropped")
    }

  /** No Structure/proposal/hex/caller-boolean conversion. C is semantic DATA, never file approval. */
  private[mysql] def fromValidatedSemantics(metadata: com.wi.floatingpod.config.MySqlRuntimeMetadata.DeploymentSemantics,
      contract: MySqlFormat3RuntimeContractLoader.ContractSemantics, policy: MySqlRuntimeAuditPolicy,
      root: MySqlFormat3ReadBudget): ManifestSha256 =
    requireRoot(root)
    if metadata == null || contract == null || policy == null then throw new SQLDataException("validated semantic inputs required")
    contract.requireOwner(root)
    val f=contract.fields
    var workspace=65536L
    workspace=Math.addExact(workspace,Math.multiplyExact(f.sourceBytes.length.toLong,16L))
    val iterator=metadata.canonicalFields().entrySet().iterator()
    while iterator.hasNext do
      val entry=iterator.next()
      workspace=Math.addExact(workspace,256L+8L*(entry.getKey.length.toLong+entry.getValue.length.toLong))
    var issued: ManifestSha256=null
    try temporary(root,workspace) {
      import scala.jdk.CollectionConverters.*
      val m=metadata.canonicalFields().asScala.toMap
      def equal(key: String,value: String): Unit =
        if m(key) != value then throw new SQLDataException("metadata/contract semantic identity differs: "+key)
      val v3=f.callerPrivileges != null
      val encoding=if v3 then EncodingV3 else Encoding
      equal("format3ManifestConfigurationEncoding",encoding)
      if f.selector != encoding then throw new SQLDataException("metadata/contract configuration version differs")
      equal("expectedServerUuid",f.server.toString);equal("expectedBindingUuid",f.binding.toString)
      equal("expectedProvisioningUuid",f.provisioning.toString);equal("schema",f.schema)
      equal("expectedServerVersion",f.version);equal("approvedDdlSha256",f.ddl.toUpperCase(java.util.Locale.ROOT))
      equal("expectedAuditDefiner",f.definer);equal("expectedEffectiveUser",f.effectiveUser)
      if policy.callerRoles != f.callerRoles || policy.auditDefinerRoles != f.definerRoles ||
        policy.maxReceiptAge != f.maxAge || policy.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 ||
        policy.expectedImport != MySqlExpectedNoImport || f.profile != root.profile then
        throw new SQLDataException("semantic policy/profile differs from exact root inputs")
      val hex=HexFormat.of().withUpperCase()
      val fields=Map("format"->(if v3 then ContractFormatV3 else ContractFormat),"format3ManifestConfigurationEncoding"->encoding,
        "coverage"->MySqlAuditCoverage.Format3,"serverUuid"->f.server.toString,"serverVersion"->f.version,
        "schema"->f.schema,"effectiveUser"->f.effectiveUser,"connectedUser"->f.connectedUser,"callerRoles"->f.callerRoles,
        "provisioningUuid"->f.provisioning.toString,"bindingUuid"->f.binding.toString,"ddlSha256"->f.ddl.toUpperCase(java.util.Locale.ROOT),
        "auditDefiner"->f.definer,"auditDefinerRoles"->f.definerRoles,"importState"->"NONE","sourceProvenanceKind"->f.sourceKind,
        "sourceProvenanceHex"->hex.formatHex(f.sourceBytes),"sourceProvenanceSha256"->hex.formatHex(f.sourceSha),
        "limitProfileEncoding"->com.wi.floatingpod.config.SourceLimitProfile.ENCODING,
        "limitProfileHex"->hex.formatHex(f.profile.canonicalBytes()),"limitProfileSha256"->hex.formatHex(f.profile.sha256()))
      val value=digest(root,out=>MySqlCurrentConfigurationProjection.writeCanonical(out,m,fields,f.maxAge.toNanos,f.callerGrants,f.privileges,f.callerPrivileges),encoding)
      var wrapperReserved=false
      var published=false
      var primary: Throwable=null
      try
        // Reuse the real full privilege/security validator with the actual computed C, never dummy bytes.
        new MySqlFormat3AuditInputs(f.server,f.version,f.schema,f.effectiveUser,f.connectedUser,f.callerRoles,
          f.provisioning,f.binding,f.ddl,f.definer,f.callerGrants,f.privileges,MySqlAuditBinary(java.util.HexFormat.of().parseHex(value.hex)),
          f.sourceKind,MySqlAuditBinary(f.sourceBytes),MySqlAuditBinary(f.sourceSha),f.profile,f.callerPrivileges,if v3 then root else null)
        root.reserveRetainedMemory(96L,"manifest semantic wrapper BEFORE allocation")
        wrapperReserved=true
        val result=new ManifestSha256(value,root)
        issued=result
        published=true
        result
      catch
        case failure: Throwable => primary=failure;throw failure
      finally
        if !published then
          try value.close()
          catch case cleanup: Throwable =>
            if primary == null then primary=cleanup else if cleanup ne primary then primary.addSuppressed(cleanup)
          if wrapperReserved then
            try root.releaseRetainedMemory(96L,"unpublished manifest wrapper")
            catch case cleanup: Throwable =>
              if primary == null then throw cleanup else if cleanup ne primary then primary.addSuppressed(cleanup)
    }
    catch
      case failure: Throwable =>
        if issued != null then
          try issued.close()
          catch case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
        throw failure

  /** 完整语义 C 与实际 K/owner 同寿命；不能由调用方摘要或 renderer H 构造。 */
  private[mysql] sealed trait StaticBinding:
    def requireAlive(): Unit
    private[mysql] def requireLease(lease: MySqlRuntimeInputs.ConsumerLease): Unit
    private[mysql] def semantics: MySqlDeploymentReceiptRuntimeContract.Semantics
    def hex: String
    private[mysql] def detach(actual: MySqlRuntimeInputs.AssemblyOwner): Unit

  final class DeploymentReceiptBinding private[MySqlManifestConfigurationIdentity] (
      private var value: MySqlDeploymentReceiptRuntimeContract.Semantics,
      private val digest: ManifestSha256, private val owner: MySqlRuntimeInputs.AssemblyOwner
  ) extends StaticBinding:
    def requireAlive(): Unit =
      owner.requireAssemblingOrReadable()
      if value == null then throw new IllegalStateException("schema5 semantic binding detached")
      value.requireAlive()
    private[mysql] def requireLease(lease: MySqlRuntimeInputs.ConsumerLease): Unit =
      requireAlive(); lease.requireStatic(owner)
    private[mysql] def semantics: MySqlDeploymentReceiptRuntimeContract.Semantics = { requireAlive(); value }
    def hex: String = { requireAlive(); digest.hex }
    private[mysql] def detach(actual: MySqlRuntimeInputs.AssemblyOwner): Unit =
      if actual ne owner then throw new IllegalArgumentException("exact schema5 binding owner required")
      value = null

  final class DbFirstBinding private[MySqlManifestConfigurationIdentity] (
      private var value: MySqlDeploymentReceiptRuntimeContract.Semantics,
      private val digest: ManifestSha256, private val owner: MySqlRuntimeInputs.AssemblyOwner
  ) extends StaticBinding:
    def requireAlive(): Unit =
      owner.requireAssemblingOrReadable()
      if value == null then throw new IllegalStateException("schema6 semantic binding detached")
      value.requireAlive()
    private[mysql] def requireLease(lease: MySqlRuntimeInputs.ConsumerLease): Unit =
      requireAlive(); lease.requireStatic(owner)
    private[mysql] def semantics: MySqlDeploymentReceiptRuntimeContract.Semantics = { requireAlive(); value }
    def hex: String = { requireAlive(); digest.hex }
    private[mysql] def detach(actual: MySqlRuntimeInputs.AssemblyOwner): Unit =
      if actual ne owner then throw new IllegalArgumentException("exact schema6 binding owner required")
      value = null

  private[mysql] def fromDeploymentReceiptSemantics(
      metadata: com.wi.floatingpod.config.MySqlRuntimeMetadata.DeploymentSemantics,
      contract: MySqlDeploymentReceiptRuntimeContract.Semantics, policy: MySqlRuntimeAuditPolicy,
      owner: MySqlRuntimeInputs.AssemblyOwner): (ManifestSha256, DeploymentReceiptBinding) =
    val (digest, binding) = fromStaticSemantics(metadata, contract, policy, owner,
      MySqlDeploymentReceiptRuntimeContract.DeploymentFamily)
    binding match
      case value: DeploymentReceiptBinding => (digest, value)
      case _ => throw new IllegalStateException("schema5 configuration binding invariant failed")

  private[mysql] def fromDbFirstSemantics(
      metadata: com.wi.floatingpod.config.MySqlRuntimeMetadata.DeploymentSemantics,
      contract: MySqlDeploymentReceiptRuntimeContract.Semantics, policy: MySqlRuntimeAuditPolicy,
      owner: MySqlRuntimeInputs.AssemblyOwner): (ManifestSha256, DbFirstBinding) =
    val (digest, binding) = fromStaticSemantics(metadata, contract, policy, owner,
      MySqlDeploymentReceiptRuntimeContract.DbFirstFamily)
    binding match
      case value: DbFirstBinding => (digest, value)
      case _ => throw new IllegalStateException("schema6 configuration binding invariant failed")

  private def fromStaticSemantics(
      metadata: com.wi.floatingpod.config.MySqlRuntimeMetadata.DeploymentSemantics,
      contract: MySqlDeploymentReceiptRuntimeContract.Semantics, policy: MySqlRuntimeAuditPolicy,
      owner: MySqlRuntimeInputs.AssemblyOwner,
      family: MySqlDeploymentReceiptRuntimeContract.Family): (ManifestSha256, StaticBinding) =
    require(contract.family == family, "configuration/K family differs")
    val root = owner.staticAssemblyRoot
    requireRoot(root); contract.requireAlive()
    if family.schemaFormat == 5 then owner.requireStaticContract(contract)
    else owner.requireDbFirstContract(contract)
    if contract.owner ne owner then throw new SQLDataException("exact schema5 K assembly owner required")
    if (contract.root ne root) || policy == null then throw new SQLDataException("exact schema5 root/policy required")
    import scala.jdk.CollectionConverters.*
    val m = metadata.canonicalFields().asScala.toMap
    def equal(key: String, value: String): Unit =
      if m.get(key) != Some(value) then throw new SQLDataException("schema5 metadata/K differs: " + key)
    equal(family.manifestKey, family.encoding)
    equal("approvedSchemaFormat", family.schemaFormat.toString); equal("approvedCoverage", family.coverage)
    Vector("expectedServerUuid" -> "serverUuid", "expectedBindingUuid" -> "bindingUuid",
      "expectedProvisioningUuid" -> "provisioningUuid", "expectedServerVersion" -> "serverVersion",
      "schema" -> "schema", "expectedAuditDefiner" -> "auditDefiner", "expectedEffectiveUser" -> "effectiveUser").foreach {
      (mk, ck) => equal(mk, contract.text(ck))
    }
    equal("approvedDdlSha256", contract.rendered.sha256.toUpperCase(java.util.Locale.ROOT))
    if policy.callerRoles != contract.text("callerRoles") || policy.auditDefinerRoles != contract.text("auditDefinerRoles") ||
      policy.maxReceiptAge.toNanos != contract.long("maxReceiptAgeNanos") || policy.expectedImport != MySqlExpectedNoImport ||
      policy.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 || (contract.profile.sourceLimitProfile ne root.profile) then
      throw new SQLDataException("schema5 policy/profile differs from approved K")
    var workspace = contract.codecWorkspace
    m.foreach { (k, v) => workspace = Math.addExact(workspace, 256L + 16L * (k.length.toLong + v.length.toLong)) }
    temporary(root, workspace) {
      val candidate = digest(root, out =>
        if family.schemaFormat == 5 then MySqlCurrentConfigurationProjection.writeDeploymentReceiptCanonical(out, m, contract, policy)
        else MySqlCurrentConfigurationProjection.writeDbFirstCanonical(out, m, contract, policy),
        family.encoding)
      var wrapper: ManifestSha256 = null
      var reserved = false
      var primary: Throwable = null
      try
        root.reserveRetainedMemory(96L, "schema5 semantic C wrapper BEFORE allocation")
        reserved = true
        wrapper = new ManifestSha256(candidate, root)
        val binding: StaticBinding = if family.schemaFormat == 5 then new DeploymentReceiptBinding(contract, wrapper, owner)
          else new DbFirstBinding(contract, wrapper, owner)
        owner.adoptStaticIdentity(wrapper)
        (wrapper, binding)
      catch
        case failure: Throwable =>
          primary = failure
          try
            if wrapper != null then wrapper.close()
            else
              candidate.close()
              if reserved then root.releaseRetainedMemory(96L, "unpublished schema5 C wrapper")
          catch case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
          throw failure
    }

  /** Complete A is never accepted where a manifest C is required. */
  final class AnchorSha256 private (private val value: Array[Byte]):
    def hex: String = HexFormat.of().withUpperCase().formatHex(value)
  object AnchorSha256:
    def fromCompleteAnchorHex(value: String): AnchorSha256 =
      if value == null || !value.matches("[0-9a-fA-F]{64}") then
        throw new IllegalArgumentException("complete anchor SHA-256 must contain exactly 32 bytes")
      new AnchorSha256(HexFormat.of().parseHex(value))

  /** Structural and current-V1 runtime outputs are intentionally not ManifestSha256. */
  final class CandidateDigest private[MySqlManifestConfigurationIdentity] (
      private val value: Array[Byte], val canonicalLength: Int,
      private val root: MySqlFormat3ReadBudget
  ) extends AutoCloseable:
    private var closed = false
    def hex: String = synchronized {
      if closed || root.isClosed then throw new IllegalStateException("candidate digest owner is closed")
      HexFormat.of().withUpperCase().formatHex(value)
    }
    override def close(): Unit = synchronized {
      if !closed then
        Arrays.fill(value, 0.toByte)
        root.releaseRetainedMemory(128L, "configuration candidate digest dropped")
        closed = true
    }

  private[mysql] def requireRoot(root: MySqlFormat3ReadBudget): Unit =
    if root == null || root.isClosed || root.isScopedReadBudget then
      throw new IllegalArgumentException("exact open workflow root required; no private replacement ledger")

  private[mysql] def temporary[A](root: MySqlFormat3ReadBudget, amount: Long)(body: => A): A =
    requireRoot(root)
    root.reserveTemporaryMemory(amount, "configuration codec workspace BEFORE allocation")
    var primary: Throwable = null
    try body
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      try root.releaseTemporaryMemory(amount, "configuration codec workspace dropped")
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)

  private final class CountSink extends OutputStream:
    var length = 0L
    override def write(value: Int): Unit = length = Math.addExact(length, 1L)
    override def write(value: Array[Byte], offset: Int, count: Int): Unit =
      if value == null || offset < 0 || count < 0 || offset > value.length - count then
        throw new IndexOutOfBoundsException("invalid configuration write slice")
      length = Math.addExact(length, count.toLong)

  /** Count first, then stream the exact accepted preimage; never apply SQL/native packet caps. */
  private[mysql] def digest(root: MySqlFormat3ReadBudget,
      write: OutputStream => Unit, encoding: String = Encoding): CandidateDigest = temporary(root, 4096L) {
    val counter = new CountSink
    write(counter)
    if counter.length > Int.MaxValue then throw new SQLDataException("configuration preimage length exceeds BE32")
    val sha = MessageDigest.getInstance("SHA-256")
    val output = new DataOutputStream(new DigestOutputStream(OutputStream.nullOutputStream(), sha))
    if encoding != Encoding && encoding != EncodingV3 && encoding != MySqlDeploymentReceiptRuntimeContract.Encoding &&
        encoding != MySqlDbFirstRuntimeContract.Encoding then throw new SQLDataException("configuration digest domain is unsupported")
    val tag = encoding.getBytes(java.nio.charset.StandardCharsets.US_ASCII)
    output.writeInt(tag.length); output.write(tag); output.writeInt(counter.length.toInt)
    var actual = 0L
    val exact = new OutputStream:
      override def write(value: Int): Unit =
        actual = Math.addExact(actual, 1L)
        if actual > counter.length then throw new SQLDataException("configuration changed between count and write")
        output.write(value)
    write(exact)
    if actual != counter.length then throw new SQLDataException("configuration count/write lengths differ")
    output.flush()
    root.reserveRetainedMemory(128L, "configuration candidate digest BEFORE allocation")
    try new CandidateDigest(sha.digest(), actual.toInt, root)
    catch
      case cause: Throwable =>
        try root.releaseRetainedMemory(128L, "failed candidate digest allocation")
        catch case cleanup: Throwable => if cleanup ne cause then cause.addSuppressed(cleanup)
        throw cause
  }
