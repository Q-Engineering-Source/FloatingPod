package com.wi.floatingpod.storage.mysql

import com.google.gson.Strictness
import com.google.gson.stream.{JsonReader, JsonToken}
import com.wi.floatingpod.config.{MySqlRuntimeMetadata, SourceLimitProfile}
import com.wi.floatingpod.bootstrap.ProductionOrdinarySourceApproval
import com.wi.floatingpod.server.WindowsWorldProvisioner
import java.io.{ByteArrayOutputStream, IOException, StringReader}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.nio.file.{Files, LinkOption, Path, StandardOpenOption}
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.sql.SQLNonTransientConnectionException
import java.time.Duration
import java.util.{HexFormat, Locale, UUID}
import scala.collection.mutable

/** 独立格式3合同；不包含自身摘要或 B1 配置摘要，避免自指。 */
final class MySqlFormat3RuntimeContract private[mysql] (
    val identity: MySqlApprovedBaselineIdentity,
    val approvedBaselineSha256: String,
    val effectiveUser: String,
    val connectedUser: String,
    val callerRoles: String,
    val callerGrants: Vector[String],
    val auditDefinerRoles: String,
    val expectedPhysicalFormat: MySqlExpectedPhysicalFormat,
    val expectedImport: MySqlExpectedImportState,
    val maxReceiptAge: Duration,
    val sourceProvenanceKind: String,
    private val provenanceValue: Array[Byte],
    private val provenanceDigest: Array[Byte],
    private val privilegeResultSets: Vector[MySqlAuditResultSet],
    val limitProfile: SourceLimitProfile,
    private[mysql] val callerPrivileges: MySqlRuntimeCallerPrivileges.Value,
    private[mysql] val semanticRoot: MySqlFormat3ReadBudget
):
  private[mysql] def this(identity: MySqlApprovedBaselineIdentity, approvedBaselineSha256: String,
      effectiveUser: String, connectedUser: String, callerRoles: String, callerGrants: Vector[String],
      auditDefinerRoles: String, expectedPhysicalFormat: MySqlExpectedPhysicalFormat,
      expectedImport: MySqlExpectedImportState, maxReceiptAge: Duration, sourceProvenanceKind: String,
      provenanceValue: Array[Byte], provenanceDigest: Array[Byte],
      privilegeResultSets: Vector[MySqlAuditResultSet], limitProfile: SourceLimitProfile) =
    this(identity,approvedBaselineSha256,effectiveUser,connectedUser,callerRoles,callerGrants,
      auditDefinerRoles,expectedPhysicalFormat,expectedImport,maxReceiptAge,sourceProvenanceKind,
      provenanceValue,provenanceDigest,privilegeResultSets,limitProfile,null,null)
  private val provenance = provenanceValue.clone()
  private val provenanceSha256Value = provenanceDigest.clone()
  require(identity != null && approvedBaselineSha256 != null && MySqlSchemaAuditor.Sha256Hex.matches(approvedBaselineSha256),
    "format3 contract requires the exact approved static baseline identity and file SHA")
  require(effectiveUser != null && connectedUser != null && callerRoles != null && callerGrants != null &&
    auditDefinerRoles != null, "format3 contract caller identity and grants are required")
  require(expectedPhysicalFormat == MySqlExpectedPhysicalFormat.Format2 && expectedImport == MySqlExpectedNoImport,
    "formal format3 runtime requires the explicit physical2/no-import contract")
  require(maxReceiptAge != null && !maxReceiptAge.isZero && !maxReceiptAge.isNegative &&
    maxReceiptAge.toNanos <= MySqlSchemaAuditor.MaxReceiptAgeNanos, "format3 receipt age must be positive and bounded")
  require(sourceProvenanceKind != null && provenance.nonEmpty && provenanceSha256Value.length == 32 &&
    privilegeResultSets != null && limitProfile != null, "format3 provenance, privileges and LimitProfile are required")

  def sourceProvenanceBytes: Array[Byte] = provenance.clone()
  def sourceProvenanceSha256: Array[Byte] = provenanceSha256Value.clone()

  /** 只有该真实 opaque issuer 路径当前可消费；其它来源类型不能由 metadata 自行证明。 */
  def requireFreshPlan(plan: WindowsWorldProvisioner.Plan): Unit =
    if plan == null || sourceProvenanceKind != "FRESH_PLAN" then reject("format3 source provenance has no admitted FreshPlan issuer")
    val actual = MySqlFormat3FreshPlanProvenance.canonicalBytes(plan)
    if !MessageDigest.isEqual(provenance, actual) then reject("actual FreshPlan differs from the independently pinned source recipe")

  /** Checks the preserved birth recipe as historical DATA; it does not issue or select a Fresh boot path. */
  def requireOriginalCreation(original: ProductionOrdinarySourceApproval.ApprovedOriginalCreation): Unit =
    if original == null || sourceProvenanceKind != "FRESH_PLAN" then
      reject("format3 source provenance does not contain the approved original creation recipe")
    val plan = original.plan()
    if plan == null || !MessageDigest.isEqual(provenance, MySqlFormat3FreshPlanProvenance.canonicalBytes(plan)) ||
      !MessageDigest.isEqual(provenanceSha256Value, MessageDigest.getInstance("SHA-256").digest(provenance)) ||
      !approvedBaselineSha256.equalsIgnoreCase(original.baselineSha256()) ||
      identity.serverUuid != plan.mysqlServerUuid() || identity.bindingUuid != plan.bindingUuid() ||
      identity.schema != plan.schema() || identity.provisioningUuid != plan.provisioningUuid() ||
      !identity.ddlSha256.equalsIgnoreCase(plan.ddlSha256()) then
      reject("approved original birth recipe differs from the pinned Format3 contract")

  def requirePolicy(policy: MySqlRuntimeAuditPolicy): Unit =
    if policy == null || policy.callerRoles != callerRoles || policy.auditDefinerRoles != auditDefinerRoles ||
      policy.expectedPhysicalFormat != expectedPhysicalFormat || policy.expectedImport != expectedImport ||
      policy.maxReceiptAge != maxReceiptAge then
      reject("runtime audit policy differs from the independent format3 contract")

  private[mysql] def auditInputs(configurationSha256: Array[Byte]): MySqlFormat3AuditInputs =
    if configurationSha256 == null || configurationSha256.length != 32 then reject("canonical B1 configuration SHA-256 is required")
    MySqlFormat3AuditInputs(identity.serverUuid, identity.serverVersion, identity.schema, effectiveUser, connectedUser,
      callerRoles, identity.provisioningUuid, identity.bindingUuid, identity.ddlSha256, identity.auditDefiner,
      callerGrants, privilegeResultSets, MySqlAuditBinary(configurationSha256), sourceProvenanceKind,
      MySqlAuditBinary(provenance), MySqlAuditBinary(provenanceSha256Value), limitProfile, callerPrivileges, semanticRoot)

  override def toString: String = "MySqlFormat3RuntimeContract[redacted; static contract only]"
  private def reject(reason: String): Nothing = throw new SQLNonTransientConnectionException(reason)

/** 静态 FreshPlan 配方与运行时 native receipt 分开；此字节不含 B1 配置摘要或未来 receipt。 */
private[mysql] object MySqlFormat3FreshPlanProvenance:
  val Encoding = "FP_MYSQL_FORMAT3_FRESH_PLAN_V1"
  def canonicalBytes(plan: WindowsWorldProvisioner.Plan): Array[Byte] =
    if plan == null then throw new IllegalArgumentException("actual FreshPlan is required")
    Vector(Encoding,
      "creationOperationUuid=" + plan.creationOperationUuid(),
      "bootstrapOperationUuid=" + plan.bootstrapOperationUuid(),
      "prepareWriteUuid=" + plan.prepareWriteUuid(),
      "certifyWriteUuid=" + plan.certifyWriteUuid(),
      "bindingUuid=" + plan.bindingUuid(),
      "physicalFormat=" + plan.physicalFormat(),
      "mysqlServerUuid=" + plan.mysqlServerUuid(),
      "schema=" + plan.schema(),
      "schemaFormat=" + plan.schemaFormat(),
      "ddlSha256=" + plan.ddlSha256().toLowerCase(Locale.ROOT),
      "provisioningUuid=" + plan.provisioningUuid()
    ).mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8)

/** 按 metadata 的绝对路径和原始文件 SHA 读取；不从 pack 或 DB 观测反推批准值。 */
object MySqlFormat3RuntimeContractLoader:
  val Format = "FP_MYSQL_FORMAT3_RUNTIME_CONTRACT_V1"
  val MaxContractBytes = 16 * 1024 * 1024
  private val MaxDepth = 10
  private val RootFields = Set("format", "schemaFormat", "coverage", "approvedBaselineSha256", "serverUuid",
    "serverVersion", "schema", "effectiveUser", "connectedUser", "callerRoles", "provisioningUuid", "bindingUuid",
    "ddlSha256", "auditDefiner", "sourceCaptureSha256", "auditDefinerRoles", "callerGrants", "physicalFormat",
    "importState", "maxReceiptAgeNanos", "sourceProvenanceKind", "sourceProvenanceHex", "sourceProvenanceSha256",
    "limitProfileEncoding", "limitProfileHex", "limitProfileSha256", "definerPrivileges")
  private val PrivilegeFields = Set("id", "columns", "rows")
  private val ColumnFields = Set("label", "jdbcType", "typeName", "precision", "scale", "nullable", "signed")
  private val CellFields = Set("tag", "value", "hex", "timestamp")
  private val SignedInteger = "-?(0|[1-9][0-9]*)".r

  def load(metadata: MySqlRuntimeMetadata, baseline: MySqlApprovedStaticBaseline): MySqlFormat3RuntimeContract =
    if baseline == null then reject("approved format3 static baseline is required")
    loadExpected(metadata, baseline.identity, baseline.fileSha256)

  /** 包内边界仅核合同 pin 与 identity；生产调用必须使用实际 static baseline loader 的返回值。 */
  private[mysql] def loadExpected(metadata: MySqlRuntimeMetadata, expected: MySqlApprovedBaselineIdentity,
      expectedBaselineSha256: String): MySqlFormat3RuntimeContract =
    if metadata == null || expected == null then reject("format3 metadata and approved identity are required")
    if metadata.baseline().schemaFormat() != MySqlApprovedBaselineLoader.Format3SchemaFormat ||
      metadata.baseline().coverage() != MySqlAuditCoverage.Format3 then
      reject("formal runtime contract requires schema format 3 and exact format3 coverage")
    val reference = metadata.format3RuntimeContract().orElseThrow(
      () => new SQLNonTransientConnectionException("format3 runtime contract path and raw SHA are missing"))
    val bytes = readBoundedFile(reference.absolutePath())
    if !digest(bytes).equalsIgnoreCase(reference.fileSha256()) then
      reject("format3 runtime contract raw file SHA-256 differs from the metadata pin")
    val parsed = new StrictParser(decodeStrictUtf8(bytes)).parse()
    if parsed.identity != expected || !parsed.approvedBaselineSha256.equalsIgnoreCase(expectedBaselineSha256) ||
      !expectedBaselineSha256.equalsIgnoreCase(metadata.baseline().fileSha256()) then
      reject("format3 runtime contract is not bound to the loaded approved static baseline")
    val id = parsed.identity
    val pins = metadata.baseline()
    if id.serverUuid != metadata.expectedServerUuid() || id.bindingUuid != metadata.expectedBindingUuid() ||
      id.schema != metadata.schema() || id.serverVersion != pins.serverVersion() ||
      id.provisioningUuid != pins.provisioningUuid() || !id.ddlSha256.equalsIgnoreCase(pins.ddlSha256()) ||
      id.auditDefiner != pins.auditDefiner() || !id.sourceCaptureSha256.equalsIgnoreCase(pins.sourceCaptureSha256()) ||
      parsed.effectiveUser != metadata.expectedEffectiveUser() then
      reject("format3 runtime contract identities differ from explicit deployment metadata")
    parsed

  /** A full independently pinned V2 file, not legacy runtime inputs and not Ready. */
  private[mysql] final class LoadedV2 private[MySqlFormat3RuntimeContractLoader] (
      private var semanticOwner: ContractSemantics,
      val identity: MySqlApprovedBaselineIdentity,
      val fileSha256: String,
      val approvedBaselineSha256: String,
      private val root: MySqlFormat3ReadBudget
  ) extends AutoCloseable:
    private var closed=false
    private[mysql] def semanticFor(expectedRoot: MySqlFormat3ReadBudget): ContractSemantics =
      if closed then reject("loaded V2 contract is closed")
      semanticOwner.requireOwner(expectedRoot)
      semanticOwner
    /** 调用方预付额外 normal form/审计克隆费用；K 原图仍由此处持有。 */
    private[mysql] def runtimeGraphFee(expectedRoot: MySqlFormat3ReadBudget): Long =
      val f=semanticFor(expectedRoot).fields
      // 原4096加64覆盖新增 normal form/audit 两组引用槽及对齐；全K路径在new前预收。
      // 4096 覆盖 normal form、审计输入（含固定 manifest 标签 Vector）、expectation/Some、
      // settings/endpoint/timeouts/durability、两个文件 source wrapper、origin 和已发布的 inputs holder。
      // Contract 克隆 provenance+SHA；AuditBinary 克隆 provenance+SHA+C；另保留账户 grantee。
      Math.addExact(4160L,Math.addExact(3L*f.sourceBytes.length.toLong,8L*f.definer.length.toLong))
    private[mysql] def runtimeWorkspaceFee(expectedRoot: MySqlFormat3ReadBudget): Long =
      val f=semanticFor(expectedRoot).fields
      // 固定的无 parser 校验工作区，加行去重 Set/Vector 节点、UTF8/hex 字符串、
      // 临时 Set/Vector 节点、provenance 比较/编码以及完整 A writer 工作区。
      var fee=Math.addExact(65536L,16L*f.sourceBytes.length.toLong)
      def text(value: String): Unit = fee=Math.addExact(fee,256L+16L*value.length.toLong)
      f.callerGrants.foreach(text)
      f.privileges.foreach { set =>
        text(set.id)
        set.columns.foreach(c=>{text(c.label);text(c.typeName)})
        set.rows.foreach { row =>
          fee=Math.addExact(fee,256L+32L*row.cells.size.toLong)
          row.cells.foreach {
            case MySqlAuditText(value) => text(value)
            case binary: MySqlAuditBinary => fee=Math.addExact(fee,256L+16L*binary.length.toLong)
            case _ => fee=Math.addExact(fee,256L)
          }
        }
      }
      fee
    /** 此桥接仅消费真实已加载的 K，不接受 projection 或 legacy metadata 转换。 */
    private[mysql] def runtimeContractFor(expectedRoot: MySqlFormat3ReadBudget): MySqlFormat3RuntimeContract =
      val f=semanticFor(expectedRoot).fields
      new MySqlFormat3RuntimeContract(identity,approvedBaselineSha256,f.effectiveUser,f.connectedUser,f.callerRoles,f.callerGrants,
        f.definerRoles,MySqlExpectedPhysicalFormat.Format2,MySqlExpectedNoImport,f.maxAge,f.sourceKind,
        f.sourceBytes,f.sourceSha,f.privileges,f.profile,f.callerPrivileges,if f.callerPrivileges == null then null else expectedRoot)
    def requirePolicy(policy: MySqlRuntimeAuditPolicy): Unit =
      val f=semanticFor(root).fields
      if policy == null || policy.callerRoles != f.callerRoles || policy.auditDefinerRoles != f.definerRoles ||
        policy.expectedPhysicalFormat != MySqlExpectedPhysicalFormat.Format2 || policy.expectedImport != MySqlExpectedNoImport ||
        policy.maxReceiptAge != f.maxAge then reject("runtime audit policy differs from loaded V2 contract")
    override def close(): Unit =
      if !closed then
        semanticOwner.close()
        semanticOwner=null
        closed=true
        root.releaseRetainedMemory(96L,"loaded V2 wrapper dropped")
    override def toString: String = "LoadedV2Contract[redacted; no runtime admission]"

  /** Only the actual static baseline loader result can supply B approval to this entry. */
  private[mysql] def loadV2(metadata: MySqlRuntimeMetadata.FullV2, baseline: MySqlApprovedStaticBaseline,
      root: MySqlFormat3ReadBudget): LoadedV2 =
    if root == null || root.isClosed || root.isScopedReadBudget then reject("exact open V2 file workflow root required")
    if metadata == null || baseline == null then reject("full V2 metadata and independently loaded static baseline required")
    val v3=metadata.format3ManifestConfigurationEncoding() == MySqlRuntimeMetadata.MANIFEST_V3
    if !v3 && metadata.format3ManifestConfigurationEncoding() != MySqlRuntimeMetadata.MANIFEST_V2 then reject("full metadata selector differs")
    val pins=metadata.baseline()
    val expected=baseline.identity
    if pins.schemaFormat() != 3 || pins.coverage() != MySqlAuditCoverage.Format3 ||
      !pins.fileSha256().equalsIgnoreCase(baseline.fileSha256) || pins.format() != expected.format ||
      pins.schemaFormat() != expected.schemaFormat || pins.coverage() != expected.coverage ||
      metadata.expectedServerUuid() != expected.serverUuid || metadata.expectedBindingUuid() != expected.bindingUuid ||
      metadata.schema() != expected.schema || pins.serverVersion() != expected.serverVersion ||
      pins.provisioningUuid() != expected.provisioningUuid || !pins.ddlSha256().equalsIgnoreCase(expected.ddlSha256) ||
      pins.auditDefiner() != expected.auditDefiner || !pins.sourceCaptureSha256().equalsIgnoreCase(expected.sourceCaptureSha256) then
      reject("full V2 metadata differs from independently loaded B identity/pins")
    val reference=metadata.format3RuntimeContract()
    var result: LoadedV2=null
    try withV2Temporary(root,65536L) {
      val path=reference.absolutePath()
      val before=Files.readAttributes(path,classOf[BasicFileAttributes],LinkOption.NOFOLLOW_LINKS)
      if before.isSymbolicLink || !before.isRegularFile || before.size() > MaxContractBytes then reject("V2 K must be a bounded regular no-link file")
      val size=before.size()
      withV2Temporary(root,Math.addExact(65536L,Math.multiplyExact(size,8L))) {
        val raw=new Array[Byte](Math.toIntExact(Math.addExact(size,1L)))
        var exact: Array[Byte]=null
        try
          val channel=FileChannel.open(path,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)
          var channelPrimary: Throwable=null
          try
            if channel.size() != size then reject("V2 K size changed before read")
            val buffer=ByteBuffer.wrap(raw)
            var ended=false
            while buffer.hasRemaining && !ended do
              if channel.read(buffer) < 0 then ended=true
            if buffer.position().toLong != size || channel.size() != size then reject("V2 K size changed during read")
          catch
            case failure: Throwable => channelPrimary=failure;throw failure
          finally
            try channel.close()
            catch case cleanup: Throwable =>
              if channelPrimary == null then throw cleanup else if cleanup ne channelPrimary then channelPrimary.addSuppressed(cleanup)
          val after=Files.readAttributes(path,classOf[BasicFileAttributes],LinkOption.NOFOLLOW_LINKS)
          if after.isSymbolicLink || !after.isRegularFile || after.size() != size ||
            after.lastModifiedTime() != before.lastModifiedTime() || !java.util.Objects.equals(before.fileKey(),after.fileKey()) then
            reject("V2 K file identity changed during read")
          exact=java.util.Arrays.copyOf(raw,size.toInt)
          val actualSha=digest(exact)
          if !actualSha.equalsIgnoreCase(reference.fileSha256()) then reject("V2 K raw SHA differs from explicit metadata pin")
          val text=decodeStrictUtf8(exact)
          val fee=Math.addExact(65536L,Math.multiplyExact(size,64L))
          root.reserveRetainedMemory(fee,"full V2 typed graph BEFORE allocation")
          var semantic: ContractSemantics=null
          var graphOwned=true
          var wrapper=false
          var primary: Throwable=null
          try
            val parsed=new StrictParser(text,false,true,v3).parseFullV2()
            if !parsed.legacyIdentity.contains(expected) ||
              !parsed.filePins.exists((b,r)=>b.equalsIgnoreCase(baseline.fileSha256) && r.equalsIgnoreCase(pins.sourceCaptureSha256())) ||
              parsed.semantic.effectiveUser != metadata.expectedEffectiveUser() || parsed.semantic.profile != root.profile then
              reject("V2 K full identity/B/R/policy profile differs from independently loaded inputs")
            semantic=new ContractSemantics(parsed.semantic,root,fee)
            graphOwned=false
            root.reserveRetainedMemory(96L,"loaded V2 wrapper BEFORE allocation")
            wrapper=true
            result=new LoadedV2(semantic,expected,actualSha,baseline.fileSha256,root)
            result
          catch
            case failure: Throwable => primary=failure;throw failure
          finally
            if result == null then
              var cleanupFailure=primary
              def cleanup(action: => Unit): Unit =
                try action
                catch case failure: Throwable =>
                  if cleanupFailure == null then cleanupFailure=failure else if failure ne cleanupFailure then cleanupFailure.addSuppressed(failure)
              if semantic != null then cleanup(semantic.close())
              else if graphOwned then cleanup(root.releaseRetainedMemory(fee,"failed full V2 typed graph"))
              if wrapper then cleanup(root.releaseRetainedMemory(96L,"unpublished V2 wrapper"))
              if primary == null && cleanupFailure != null then throw cleanupFailure
        finally
          java.util.Arrays.fill(raw,0.toByte)
          if exact != null then java.util.Arrays.fill(exact,0.toByte)
      }
    }
    catch
      case failure: Throwable =>
        if result != null then
          try result.close()
          catch case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
        throw failure

  private def withV2Temporary[A](root: MySqlFormat3ReadBudget, amount: Long)(body: => A): A =
    root.reserveTemporaryMemory(amount,"V2 file workspace BEFORE allocation")
    var primary: Throwable=null
    try body
    catch
      case failure: Throwable => primary=failure;throw failure
    finally
      try root.releaseTemporaryMemory(amount,"V2 file workspace dropped")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else if cleanup ne primary then primary.addSuppressed(cleanup)

  /** Pin-free validated contract DATA; its paid graph cannot outlive its original root owner. */
  private[mysql] final class ContractSemantics private[MySqlFormat3RuntimeContractLoader] (
      private var retained: SemanticFields, private val root: MySqlFormat3ReadBudget, private val fee: Long
  ) extends AutoCloseable:
    private var closed = false
    private[mysql] def fields: SemanticFields =
      if closed || (root != null && root.isClosed) then reject("contract semantic owner is closed")
      retained
    private[mysql] def requireOwner(expected: MySqlFormat3ReadBudget): Unit =
      if closed || root == null || (root ne expected) || root.isClosed then reject("contract semantic root differs")
    override def close(): Unit =
      if !closed then
        closed = true
        retained = null
        if root != null then root.releaseRetainedMemory(fee,"contract semantic graph dropped")

  private[mysql] final case class SemanticFields private[MySqlFormat3RuntimeContractLoader] (
      server: UUID, version: String, schema: String, provisioning: UUID, binding: UUID, ddl: String, definer: String,
      effectiveUser: String, connectedUser: String, callerRoles: String, callerGrants: Vector[String], definerRoles: String,
      maxAge: Duration, sourceKind: String, sourceBytes: Array[Byte], sourceSha: Array[Byte],
      privileges: Vector[MySqlAuditResultSet], profile: SourceLimitProfile,
      selector: String, callerPrivileges: MySqlRuntimeCallerPrivileges.Value
  )
  private final case class Parsed(semantic: SemanticFields, filePins: Option[(String,String)], legacyIdentity: Option[MySqlApprovedBaselineIdentity]):
    def contract: MySqlFormat3RuntimeContract =
      val (baselineSha,sourcePin) = filePins.getOrElse(reject("full contract pins are absent"))
      val f=semantic
      val identity=legacyIdentity.getOrElse(reject("full contract identity is absent"))
      new MySqlFormat3RuntimeContract(identity,baselineSha,f.effectiveUser,f.connectedUser,f.callerRoles,f.callerGrants,
        f.definerRoles,MySqlExpectedPhysicalFormat.Format2,MySqlExpectedNoImport,f.maxAge,f.sourceKind,
        f.sourceBytes,f.sourceSha,f.privileges,f.profile)

  /** Separate V2 projection entry. Legacy load/StrictParser never accepts this shape. */
  private[mysql] def parseManifestV2Projection(text: String, root: MySqlFormat3ReadBudget): ContractSemantics =
    parseManifestProjection(text,root,false)

  private[mysql] def parseManifestV3Projection(text: String, root: MySqlFormat3ReadBudget): ContractSemantics =
    parseManifestProjection(text,root,true)

  private def parseManifestProjection(text: String, root: MySqlFormat3ReadBudget, v3: Boolean): ContractSemantics =
    if root == null || root.isClosed || root.isScopedReadBudget then reject("exact open semantic root required")
    val size=MySqlFormat3Utf8.byteLength(text)
    if size > MaxContractBytes then reject("semantic contract exceeds original input bound")
    // Per source UTF-8 unit: decoder/String copies, tree/hash entries, Vector builder paths,
    // scalar wrappers and typed cell/column objects. Even a one-unit scalar pays 64B;
    // object/cell syntax provides further units. Fixed 64KiB pays readers/schema/closures.
    val fee=Math.addExact(65536L,Math.multiplyExact(size,64L))
    root.reserveRetainedMemory(fee,"contract semantic graph BEFORE allocation")
    var success=false
    var primary: Throwable=null
    try
      val f=new StrictParser(text,true,false,v3).parseProjection()
      val result=new ContractSemantics(f,root,fee)
      success=true
      result
    catch
      case failure: Throwable => primary=failure;throw failure
    finally
      if !success then
        try root.releaseRetainedMemory(fee,"failed contract semantic parse")
        catch case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)

  private final class StrictParser(text: String, projection: Boolean = false, fullV2: Boolean = false, v3: Boolean = false):
    private val v2 = projection || fullV2
    private val expectedFields =
      val base=if projection then (RootFields -- Set("approvedBaselineSha256","sourceCaptureSha256")) + "format3ManifestConfigurationEncoding" else if fullV2 then RootFields + "format3ManifestConfigurationEncoding" else RootFields
      if v3 then (base - "callerGrants") + "callerPrivileges" else base
    private val reader = new JsonReader(new StringReader(text))
    private var depth = 0
    private var totalRows = 0
    private var totalCells = 0
    private var payloadBytes = 0L

    def parse(): MySqlFormat3RuntimeContract = parseWith(_.contract)

    def parseProjection(): SemanticFields = parseWith(_.semantic)

    def parseFullV2(): Parsed = parseWith(identity)

    private def parseWith[A](select: Parsed => A): A =
      reader.setStrictness(Strictness.STRICT)
      var primary: Throwable = null
      try
        val value = select(readRoot())
        if reader.peek() != JsonToken.END_DOCUMENT then reject("format3 runtime contract has trailing JSON content")
        value
      catch
        case failure: SQLNonTransientConnectionException => primary = failure; throw failure
        case failure: IOException =>
          val contextual = new SQLNonTransientConnectionException("format3 runtime contract JSON is malformed or truncated", failure)
          primary = contextual
          throw contextual
        case failure: IllegalArgumentException =>
          val contextual = new SQLNonTransientConnectionException("format3 runtime contract contains an invalid typed value", failure)
          primary = contextual
          throw contextual
        case failure: Throwable => primary = failure; throw failure
      finally
        if primary == null then
          try reader.close()
          catch case failure: IOException => throw new SQLNonTransientConnectionException("format3 contract reader close failed", failure)
        else
          try reader.close()
          catch case cleanup: Throwable => addSuppressed(primary, cleanup)

    private def readRoot(): Parsed =
      beginObject()
      val seen = mutable.HashSet.empty[String]
      var format: String = null
      var selector: String = null
      var schemaFormat = Int.MinValue
      var coverage: String = null
      var baselineSha: String = null
      var server: UUID = null
      var version: String = null
      var schema: String = null
      var effectiveUser: String = null
      var connectedUser: String = null
      var callerRoles: String = null
      var provisioning: UUID = null
      var binding: UUID = null
      var ddlSha: String = null
      var definer: String = null
      var sourceCaptureSha: String = null
      var definerRoles: String = null
      var callerGrants: Vector[String] = if v3 then Vector.empty else null
      var callerPrivileges: MySqlRuntimeCallerPrivileges.Value = null
      var physical = Int.MinValue
      var importState: String = null
      var maxAgeNanos = Long.MinValue
      var sourceKind: String = null
      var sourceHex: String = null
      var sourceShaHex: String = null
      var profileEncoding: String = null
      var profileHex: String = null
      var profileShaHex: String = null
      var privileges: Vector[MySqlAuditResultSet] = null
      while reader.peek() != JsonToken.END_OBJECT do
        fieldName(seen, expectedFields, "format3 runtime contract") match
          case "format" => format = text("format")
          case "format3ManifestConfigurationEncoding" => selector = text("manifest configuration selector",64)
          case "schemaFormat" => schemaFormat = integer("schemaFormat")
          case "coverage" => coverage = text("coverage")
          case "approvedBaselineSha256" => baselineSha = shaText("approved baseline SHA-256")
          case "serverUuid" => server = uuid("server UUID")
          case "serverVersion" => version = text("server version")
          case "schema" => schema = text("schema")
          case "effectiveUser" => effectiveUser = text("effective user")
          case "connectedUser" => connectedUser = text("connected user")
          case "callerRoles" => callerRoles = text("caller roles")
          case "provisioningUuid" => provisioning = uuid("provisioning UUID")
          case "bindingUuid" => binding = uuid("binding UUID")
          case "ddlSha256" => ddlSha = shaText("DDL SHA-256")
          case "auditDefiner" => definer = text("audit definer")
          case "sourceCaptureSha256" => sourceCaptureSha = shaText("source capture SHA-256")
          case "auditDefinerRoles" => definerRoles = text("audit definer roles")
          case "callerGrants" => callerGrants = readGrants()
          case "callerPrivileges" => callerPrivileges = MySqlRuntimeCallerPrivileges.read(reader)
          case "physicalFormat" => physical = integer("physical format")
          case "importState" => importState = text("import state")
          case "maxReceiptAgeNanos" => maxAgeNanos = long("max receipt age")
          case "sourceProvenanceKind" => sourceKind = text("source provenance kind")
          case "sourceProvenanceHex" => sourceHex = hexText("source provenance", 1_048_576)
          case "sourceProvenanceSha256" => sourceShaHex = shaText("source provenance SHA-256")
          case "limitProfileEncoding" => profileEncoding = text("limit profile encoding", 64)
          case "limitProfileHex" => profileHex = hexText("limit profile", 512)
          case "limitProfileSha256" => profileShaHex = shaText("LimitProfile SHA-256")
          case "definerPrivileges" => privileges = readPrivileges()
          case _ => reject("format3 runtime contract contains an unsupported field")
      endObject()
      requireFields(seen, expectedFields, "format3 runtime contract")
      if v2 then
        if format != (if v3 then "FP_MYSQL_FORMAT3_RUNTIME_CONTRACT_V3" else "FP_MYSQL_FORMAT3_RUNTIME_CONTRACT_V2") ||
          selector != (if v3 then MySqlRuntimeMetadata.MANIFEST_V3 else MySqlRuntimeMetadata.MANIFEST_V2) then
          reject("V2 contract requires exact format and actual selector")
      else if format != Format then reject("format3 runtime contract format is unknown")
      if schemaFormat != MySqlApprovedBaselineLoader.Format3SchemaFormat || coverage != MySqlAuditCoverage.Format3 then
        reject("format3 runtime contract schema/coverage pair is invalid")
      if physical != 2 || importState != "NONE" then reject("formal format3 contract requires physical2 and import state NONE")
      if maxAgeNanos <= 0L || maxAgeNanos > MySqlSchemaAuditor.MaxReceiptAgeNanos then reject("format3 receipt age is outside its bound")
      if profileEncoding != SourceLimitProfile.ENCODING then reject("format3 LimitProfile encoding is unsupported")
      if !Set("FRESH_PLAN", "FORMAT3_RESTORE", "FORMAT2_IMPORT", "SQLITE_IMPORT").contains(sourceKind) then
        reject("format3 source provenance kind is unknown")
      val sourceBytes = parseUpperHex(sourceHex, "source provenance", 1_048_576)
      val sourceSha = parseUpperHex(sourceShaHex, "source provenance SHA-256", 32)
      if sourceBytes.isEmpty || !MessageDigest.isEqual(MessageDigest.getInstance("SHA-256").digest(sourceBytes), sourceSha) then
        reject("format3 source provenance bytes and SHA-256 disagree")
      val profileBytes = parseUpperHex(profileHex, "LimitProfile", 512)
      val profileSha = parseUpperHex(profileShaHex, "LimitProfile SHA-256", 32)
      val profile =
        try SourceLimitProfile.decode(profileBytes, profileSha)
        catch case failure: IllegalArgumentException => reject("format3 LimitProfile bytes/SHA are invalid", failure)
      // V1 retains its exact real validator, validation order and original cause boundary.
      val legacyIdentity = if !projection then
        try Some(MySqlApprovedBaselineIdentity(MySqlApprovedBaselineLoader.ApprovedFormat, schemaFormat, coverage, server,
          version, schema, provisioning, binding, ddlSha, definer, sourceCaptureSha))
        catch case failure: IllegalArgumentException => reject("format3 contract static identity is invalid", failure)
      else
        if server == null || provisioning == null || binding == null ||
          version != MySqlConnectionSource.SupportedServerVersion || schema == null ||
          !MySqlSchemaAuditor.SchemaIdentifier.matches(schema) || !MySqlSchemaAuditor.Sha256Hex.matches(ddlSha) ||
          definer == null || definer.isEmpty then reject("format3 contract static identity is invalid")
        None
      if v3 && (callerPrivileges == null || effectiveUser != "fp_f3_runtime@localhost" || callerRoles != "NONE" || definerRoles != "NONE" || schema != MySqlRuntimeCallerPrivileges.Schema) then reject("V3 exact runtime principal/roles/schema differs")
      if privileges == null || privileges.map(_.id) != MySqlAuditCoverage.Format3PrivilegeResultSetIds then
        reject("format3 contract requires the exact ordered four privilege resultsets")
      val semantic=SemanticFields(server,version,schema,provisioning,binding,ddlSha,definer,effectiveUser,connectedUser,
        callerRoles,callerGrants,definerRoles,Duration.ofNanos(maxAgeNanos),sourceKind,sourceBytes,sourceSha,privileges,profile,selector,callerPrivileges)
      Parsed(semantic,if projection then None else Some((baselineSha,sourceCaptureSha)),legacyIdentity)

    private def readGrants(): Vector[String] =
      beginArray()
      val seen = mutable.HashSet.empty[String]
      val out = Vector.newBuilder[String]
      var count = 0
      while reader.peek() != JsonToken.END_ARRAY do
        if count >= 2048 then reject("format3 caller grants exceed the row bound")
        val value = text("caller grant")
        if !seen.add(value) then reject("format3 caller grants contain a duplicate")
        out += value
        count += 1
      endArray()
      val values = out.result()
      if values.isEmpty then reject("format3 caller grants must be explicit")
      values

    private def readPrivileges(): Vector[MySqlAuditResultSet] =
      beginArray()
      val out = Vector.newBuilder[MySqlAuditResultSet]
      var count = 0
      while reader.peek() != JsonToken.END_ARRAY do
        if count >= 4 then reject("format3 contract has too many privilege resultsets")
        out += readPrivilegeSet()
        count += 1
      endArray()
      out.result()

    private def readPrivilegeSet(): MySqlAuditResultSet =
      beginObject()
      val seen = mutable.HashSet.empty[String]
      var id: String = null
      var columns: Vector[MySqlAuditColumn] = null
      var rows: Vector[MySqlAuditRow] = null
      while reader.peek() != JsonToken.END_OBJECT do
        fieldName(seen, PrivilegeFields, "definer privilege set") match
          case "id" => id = text("privilege resultset id")
          case "columns" => columns = readColumns()
          case "rows" => rows = readRows()
          case _ => reject("definer privilege resultset contains an unsupported field")
      endObject()
      requireFields(seen, PrivilegeFields, "definer privilege resultset")
      if columns == null || rows == null || columns.isEmpty || rows.exists(_.cells.size != columns.size) then
        reject("definer privilege resultset has incomplete rows")
      MySqlAuditResultSet(id, columns, rows)

    private def readColumns(): Vector[MySqlAuditColumn] =
      beginArray()
      val out = Vector.newBuilder[MySqlAuditColumn]
      var count = 0
      while reader.peek() != JsonToken.END_ARRAY do
        if count >= MySqlSchemaAuditor.MaxColumnsPerResultSet then reject("definer privilege column count exceeds its bound")
        out += readColumn()
        count += 1
      endArray()
      out.result()

    private def readColumn(): MySqlAuditColumn =
      beginObject()
      val seen = mutable.HashSet.empty[String]
      var label: String = null
      var jdbcType = Int.MinValue
      var typeName: String = null
      var precision = Int.MinValue
      var scale = Int.MinValue
      var nullable = Int.MinValue
      var signed: java.lang.Boolean = null
      while reader.peek() != JsonToken.END_OBJECT do
        fieldName(seen, ColumnFields, "privilege column") match
          case "label" => label = text("column label", 256)
          case "jdbcType" => jdbcType = integer("column JDBC type")
          case "typeName" => typeName = text("column type name", 256)
          case "precision" => precision = integer("column precision")
          case "scale" => scale = integer("column scale")
          case "nullable" => nullable = integer("column nullable")
          case "signed" => signed = boolean("column signed")
          case _ => reject("privilege column has an unsupported field")
      endObject()
      requireFields(seen, ColumnFields, "privilege column")
      if nullable < 0 || nullable > 2 then reject("privilege column nullability is invalid")
      MySqlAuditColumn(label, jdbcType, typeName, precision, scale, nullable, signed.booleanValue())

    private def readRows(): Vector[MySqlAuditRow] =
      beginArray()
      val out = Vector.newBuilder[MySqlAuditRow]
      while reader.peek() != JsonToken.END_ARRAY do
        totalRows = Math.incrementExact(totalRows)
        if totalRows > 20_000 then reject("format3 privilege rows exceed the bound")
        out += readRow()
      endArray()
      out.result()

    private def readRow(): MySqlAuditRow =
      beginArray()
      val out = Vector.newBuilder[MySqlAuditCell]
      while reader.peek() != JsonToken.END_ARRAY do
        totalCells = Math.incrementExact(totalCells)
        if totalCells > 200_000 then reject("format3 privilege cells exceed the bound")
        out += readCell()
      endArray()
      MySqlAuditRow(out.result())

    private def readCell(): MySqlAuditCell =
      beginObject()
      val seen = mutable.HashSet.empty[String]
      var tag: String = null
      var value: String = null
      var hex: String = null
      var timestamp: String = null
      while reader.peek() != JsonToken.END_OBJECT do
        fieldName(seen, CellFields, "privilege cell") match
          case "tag" => tag = text("cell tag", 32)
          case "value" => value = text("cell text")
          case "hex" => hex = hexText("cell binary", 1_048_576)
          case "timestamp" => timestamp = text("cell timestamp", 128)
          case _ => reject("privilege cell has an unsupported field")
      endObject()
      tag match
        case "SQL_NULL" => requireFields(seen, Set("tag"), "SQL NULL cell"); MySqlAuditNull
        case "TEXT" => requireFields(seen, Set("tag", "value"), "text cell"); MySqlAuditText(value)
        case "BINARY" => requireFields(seen, Set("tag", "hex"), "binary cell"); MySqlAuditBinary(parseUpperHex(hex, "binary cell", 1_048_576))
        case "TIMESTAMP" =>
          requireFields(seen, Set("tag", "timestamp"), "timestamp cell")
          try MySqlAuditTimestamp(java.time.LocalDateTime.parse(timestamp))
          catch case failure: java.time.format.DateTimeParseException => reject("format3 timestamp cell is malformed", failure)
        case _ => reject("format3 privilege cell tag is unknown")

    private def beginObject(): Unit = { reader.beginObject(); enterDepth() }
    private def endObject(): Unit = { reader.endObject(); leaveDepth() }
    private def beginArray(): Unit = { reader.beginArray(); enterDepth() }
    private def endArray(): Unit = { reader.endArray(); leaveDepth() }
    private def enterDepth(): Unit = { depth += 1; if depth > MaxDepth then reject("format3 contract exceeds its JSON depth bound") }
    private def leaveDepth(): Unit = { depth -= 1; if depth < 0 then reject("format3 contract parser depth became invalid") }

    private def fieldName(seen: mutable.Set[String], allowed: Set[String], context: String): String =
      if reader.peek() != JsonToken.NAME then reject(context + " field name is missing")
      val name = reader.nextName()
      if !allowed.contains(name) then reject(context + " contains an unsupported field")
      if !seen.add(name) then reject(context + " contains a duplicate field")
      name

    private def requireFields(seen: scala.collection.Set[String], expected: Set[String], context: String): Unit =
      if seen != expected then reject(context + " is missing required fields")

    private def text(context: String, maxBytes: Int = MySqlSchemaAuditor.MaxCellBytes): String =
      if reader.peek() != JsonToken.STRING then reject(context + " must be a JSON string")
      val value = reader.nextString()
      val bytes =
        try StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT).encode(java.nio.CharBuffer.wrap(value)).remaining()
        catch case failure: java.nio.charset.CharacterCodingException => reject(context + " is not valid Unicode", failure)
      if bytes > maxBytes then reject(context + " exceeds the text byte bound")
      payloadBytes = Math.addExact(payloadBytes, bytes.toLong)
      if payloadBytes > MaxContractBytes.toLong then reject("format3 decoded payload exceeds the byte bound")
      value

    private def hexText(context: String, maxBytes: Int): String =
      val value = text(context, Math.multiplyExact(maxBytes, 2))
      if value.length % 2 != 0 || !value.forall(isUpperHex) then reject(context + " must use even-length uppercase hexadecimal")
      if value.length / 2 > maxBytes then reject(context + " exceeds its decoded byte bound")
      value

    private def integer(context: String): Int =
      try java.lang.Integer.parseInt(integerText(context))
      catch case failure: NumberFormatException => reject(context + " exceeds the 32-bit integer range", failure)
    private def long(context: String): Long =
      try java.lang.Long.parseLong(integerText(context))
      catch case failure: NumberFormatException => reject(context + " exceeds the 64-bit integer range", failure)
    private def integerText(context: String): String =
      if reader.peek() != JsonToken.NUMBER then reject(context + " must be an integer JSON number")
      val value = reader.nextString()
      if value.length > 20 || !SignedInteger.matches(value) then reject(context + " is not an exact integer")
      value
    private def uuid(context: String): UUID =
      val value = text(context, 36)
      try
        val parsed = UUID.fromString(value)
        if parsed.toString != (if v2 then value else value.toLowerCase(Locale.ROOT)) then reject(context + " is not a canonical UUID")
        parsed
      catch case failure: IllegalArgumentException => reject(context + " is malformed", failure)
    private def shaText(context: String): String =
      val value = text(context, 64)
      if !MySqlSchemaAuditor.Sha256Hex.matches(value) then reject(context + " must be a 64-character SHA-256")
      if v2 && value != value.toUpperCase(Locale.ROOT) then reject(context + " must be canonical uppercase SHA-256")
      value
    private def boolean(context: String): Boolean =
      if reader.peek() != JsonToken.BOOLEAN then reject(context + " must be a JSON boolean")
      reader.nextBoolean()

  private def readBoundedFile(path: Path): Array[Byte] =
    val attributes =
      try Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
      catch case failure: IOException => reject("format3 runtime contract file metadata could not be read", failure)
    if attributes.isSymbolicLink || !attributes.isRegularFile then reject("format3 runtime contract must be a regular non-link file")
    if attributes.size() > MaxContractBytes.toLong then reject("format3 runtime contract exceeds the file byte bound")
    val channel =
      try FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)
      catch case failure: IOException => reject("format3 runtime contract file could not be opened", failure)
    var primary: Throwable = null
    try
      val output = new ByteArrayOutputStream(Math.max(32L, Math.min(attributes.size(), MaxContractBytes.toLong)).toInt)
      val buffer = ByteBuffer.allocate(8192)
      var total = 0L
      var count = channel.read(buffer)
      while count >= 0 do
        if count > 0 then
          total = Math.addExact(total, count.toLong)
          if total > MaxContractBytes.toLong then reject("format3 runtime contract exceeds the file byte bound")
          output.write(buffer.array(), 0, count)
          buffer.clear()
        count = channel.read(buffer)
      output.toByteArray
    catch
      case failure: Throwable => primary = failure; throw failure
    finally
      if primary == null then
        try channel.close()
        catch case failure: IOException => throw new SQLNonTransientConnectionException("format3 contract channel close failed", failure)
      else
        try channel.close()
        catch case cleanup: Throwable => addSuppressed(primary, cleanup)

  private def decodeStrictUtf8(bytes: Array[Byte]): String =
    if bytes.length >= 3 && bytes(0) == 0xef.toByte && bytes(1) == 0xbb.toByte && bytes(2) == 0xbf.toByte then
      reject("format3 runtime contract must not contain a UTF-8 BOM")
    try StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString
    catch case failure: java.nio.charset.CharacterCodingException => reject("format3 runtime contract is not strict UTF-8", failure)

  private def parseUpperHex(value: String, context: String, maxBytes: Int): Array[Byte] =
    if value == null || value.length % 2 != 0 || value.length.toLong > maxBytes.toLong * 2L || !value.forall(isUpperHex) then
      reject(context + " is malformed or exceeds its byte bound")
    try HexFormat.of().parseHex(value)
    catch case failure: IllegalArgumentException => reject(context + " is malformed hexadecimal", failure)

  private def isUpperHex(value: Char): Boolean = value >= '0' && value <= '9' || value >= 'A' && value <= 'F'
  private def digest(bytes: Array[Byte]): String =
    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)).toUpperCase(Locale.ROOT)
  private def addSuppressed(primary: Throwable, secondary: Throwable): Unit =
    if primary ne secondary then primary.addSuppressed(secondary)
  private def reject(reason: String): Nothing = throw new SQLNonTransientConnectionException(reason)
  private def reject(reason: String, cause: Throwable): Nothing = throw new SQLNonTransientConnectionException(reason, cause)
