package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.MySqlRuntimeMetadata
import com.wi.floatingpod.bootstrap.ProductionOrdinarySourceApproval
import com.wi.floatingpod.server.{WindowsB1SourceAnchorStore, WindowsWorldProvisioner}
import java.sql.SQLNonTransientConnectionException
import java.time.Duration
import java.util.{HexFormat, UUID}

/** 调用方冻结的审计合同；不从 pack 或数据库观测推导批准值。 */
final case class MySqlRuntimeAuditPolicy(
    callerRoles: String,
    auditDefinerRoles: String,
    expectedImport: MySqlExpectedImportState,
    expectedPhysicalFormat: MySqlExpectedPhysicalFormat,
    maxReceiptAge: Duration
):
  require(callerRoles != null && callerRoles.nonEmpty, "caller roles must be explicit")
  require(auditDefinerRoles != null && auditDefinerRoles.nonEmpty, "audit definer roles must be explicit")
  require(expectedImport != null && expectedPhysicalFormat != null, "import and physical format must be explicit")
  require(maxReceiptAge != null && !maxReceiptAge.isZero && !maxReceiptAge.isNegative,
    "receipt freshness must be positive")
  require(maxReceiptAge.toNanos <= MySqlSchemaAuditor.MaxReceiptAgeNanos, "receipt freshness exceeds bounded policy")
  override def toString: String = "MySqlRuntimeAuditPolicy[redacted]"

/** 只装配静态输入；不表示 connection、qualification、fence 或 Ready。 */
final class MySqlRuntimeInputs private (
    legacyMetadata: MySqlRuntimeMetadata,
    ioThread: Thread,
    val bootId: UUID,
    val bootGeneration: Long,
    val activeLimit: Int,
    connectionSettings: MySqlConnectionSettings,
    baseline: MySqlApprovedStaticBaseline,
    contract: MySqlFormat3RuntimeContract,
    expectation: MySqlSchemaAuditExpectation,
    policy: MySqlRuntimeAuditPolicy,
    private val v2Owner: MySqlRuntimeInputs.AssemblyOwner = null,
    fullMetadata: MySqlRuntimeMetadata.FullV2 = null
):
  require(contract != null && expectation.profile == MySqlSchemaAuditProfile.Format3,
    "formal owned runtime inputs require exact format3 contract and expectation")
  private var origin: MySqlRuntimeInputs.Origin =
    if v2Owner == null then MySqlRuntimeInputs.LegacyOrigin(legacyMetadata)
    else MySqlRuntimeInputs.V2Origin(fullMetadata)
  private var retainedThread = ioThread
  private var retainedSettings = connectionSettings
  private var retainedBaseline = baseline
  private var retainedContract = contract
  private var retainedExpectation = expectation
  private var retainedPolicy = policy
  private def requireLive(): Unit =
    if v2Owner != null then v2Owner.requireReadable()
  def originalMetadata: MySqlRuntimeMetadata =
    requireLive()
    origin match
      case MySqlRuntimeInputs.LegacyOrigin(value) => value
      case _ => throw new IllegalStateException("V2 inputs have no legacy metadata origin")
  private[mysql] def originalMetadataV2: MySqlRuntimeMetadata.FullV2 =
    requireLive()
    origin match
      case MySqlRuntimeInputs.V2Origin(value) => value
      case _ => throw new IllegalStateException("inputs have no open V2 metadata origin")
  def ownedIoThread: Thread = { requireLive(); retainedThread }
  def settings: MySqlConnectionSettings = { requireLive(); retainedSettings }
  def approvedBaseline: MySqlApprovedStaticBaseline = { requireLive(); retainedBaseline }
  def format3Contract: MySqlFormat3RuntimeContract = { requireLive(); retainedContract }
  def auditExpectation: MySqlSchemaAuditExpectation = { requireLive(); retainedExpectation }
  def auditPolicy: MySqlRuntimeAuditPolicy = { requireLive(); retainedPolicy }
  private[mysql] def dropV2References(owner: MySqlRuntimeInputs.AssemblyOwner): Unit =
    if v2Owner == null || (v2Owner ne owner) then throw new IllegalStateException("exact V2 assembly owner required")
    origin=MySqlRuntimeInputs.ClosedOrigin
    retainedThread=null;retainedSettings=null;retainedBaseline=null;retainedContract=null
    retainedExpectation=null;retainedPolicy=null
  private[mysql] def hasV2Origin: Boolean = v2Owner != null
  private[mysql] def assemblyOwnerV2: MySqlRuntimeInputs.AssemblyOwner =
    requireLive()
    if v2Owner == null then throw new IllegalStateException("explicit V2 inputs required")
    v2Owner
  def newAuditor(): MySqlSchemaAuditor =
    require(Thread.currentThread() eq ownedIoThread, "runtime auditor construction requires exact owned IO thread")
    new MySqlSchemaAuditor(auditExpectation, auditPolicy.maxReceiptAge)
  override def toString: String = "MySqlRuntimeInputs[redacted; static inputs only]"

object MySqlRuntimeInputs:
  val ProductionActiveHardCap = 256

  private sealed trait Origin
  private final case class LegacyOrigin(value: MySqlRuntimeMetadata) extends Origin
  private final case class V2Origin(value: MySqlRuntimeMetadata.FullV2) extends Origin
  private case object ClosedOrigin extends Origin

  /** 调用方在首次 B/K IO 前保留此所有者；它不会关闭调用方的 root。 */
  private[mysql] final class AssemblyOwner private[MySqlRuntimeInputs] (
      private[MySqlRuntimeInputs] val root: MySqlFormat3ReadBudget,
      private[MySqlRuntimeInputs] val thread: Thread
  ) extends AutoCloseable:
    private[MySqlRuntimeInputs] var b: MySqlApprovedBaselineLoader.OwnedBaseline = null
    private[MySqlRuntimeInputs] var k: MySqlFormat3RuntimeContractLoader.LoadedV2 = null
    private[MySqlRuntimeInputs] var k5: MySqlDeploymentReceiptRuntimeContract.Loaded = null
    private[MySqlRuntimeInputs] var k6: MySqlDeploymentReceiptRuntimeContract.Loaded = null
    private var history: MySqlDbFirstHistoryInputs = null
    private var historyAttempted=false
    private[mysql] def historicalInputs: Option[MySqlDbFirstHistoryInputs] =
      requireReadable(); Option(history)
    private[mysql] def requireHistoryOwner(value: MySqlDbFirstHistoryInputs, exactRoot: MySqlFormat3ReadBudget, assembling: Boolean): Unit =
      requireThread()
      val expectedState = if assembling then 1 else 2
      if (exactRoot ne root) || (history ne value) || root.isClosed || state != expectedState then
        throw new IllegalStateException("exact live historical DATA owner/root slot required")
    private[mysql] def requireHistoryCleanup(value: MySqlDbFirstHistoryInputs, exactRoot: MySqlFormat3ReadBudget): Unit =
      requireThread()
      if (exactRoot ne root) || (history ne value) || root.isClosed then
        throw new IllegalStateException("exact historical cleanup custody required")
    private[mysql] def requireHistoryRetirement(value: MySqlDbFirstHistoryInputs, exactRoot: MySqlFormat3ReadBudget): Unit =
      requireHistoryCleanup(value,exactRoot)
      if state != 4 || borrows != 0 then throw new IllegalStateException("historical DATA retires only with its unborrowed closing owner")
    private[mysql] def requireHistoricalBaseline(value: MySqlApprovedStaticBaseline): Unit =
      requireAssemblingOrReadable()
      if history == null || state != 1 then throw new IllegalStateException("historical baseline slot required")
      history.requireBaseline(value)
    private[mysql] def adoptHistoricalContract(value: MySqlDeploymentReceiptRuntimeContract.Loaded): Unit =
      requireAssemblingOrReadable()
      if history == null || state != 1 then throw new IllegalStateException("historical K slot required")
      history.adoptContract(value)
    private[MySqlRuntimeInputs] def loadHistory(): Unit =
      if historyAttempted || state != 1 || k6 == null then throw new IllegalStateException("single historical assembly attempt required")
      historyAttempted=true
      k6.semantics.historicalSource.foreach { source =>
        root.reserveRetainedMemory(MySqlDbFirstHistoryInputs.HolderFee,"historical holder BEFORE allocation")
        try history=MySqlDbFirstHistoryInputs.allocate(this,root,source)
        catch case cause: Throwable =>
          try root.releaseRetainedMemory(MySqlDbFirstHistoryInputs.HolderFee,"unpublished historical holder")
          catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,cleanup)
          throw cause
        history.load(k6.semantics)
      }
    private[MySqlRuntimeInputs] var static6: DbFirstStaticGraph = null
    private[MySqlRuntimeInputs] var static5: DeploymentReceiptStaticGraph = null
    private[MySqlRuntimeInputs] var c: MySqlManifestConfigurationIdentity.ManifestSha256 = null
    private[MySqlRuntimeInputs] var inputs: MySqlRuntimeInputs = null
    private[MySqlRuntimeInputs] var graphFee = 0L
    private[MySqlRuntimeInputs] var state = 0 // 新建、装配中、可读、失败、关闭中、已关闭
    private[MySqlRuntimeInputs] var primary: Throwable = null
    private var cleanupFailure: Throwable = null
    private var graphAttempted=false
    private var cAttempted=false
    private var kAttempted=false
    private var bAttempted=false
    private var holderAttempted=false
    private[MySqlRuntimeInputs] var borrows=0
    private[MySqlRuntimeInputs] def requireThread(): Unit =
      if Thread.currentThread() ne thread then throw new IllegalStateException("exact assembly IO owner thread required")
    private[MySqlRuntimeInputs] def start(): Unit =
      requireThread()
      if state != 0 then throw new IllegalStateException("assembly owner is single-use")
      MySqlManifestConfigurationIdentity.requireRoot(root)
      state=1
    private[mysql] def requireAssemblingOrReadable(): Unit =
      requireThread()
      if (state != 1 && state != 2) || root.isClosed then throw new IllegalStateException("assembly owner no longer owns schema5 DATA", primary)
    private[mysql] def adoptStaticContract(value: MySqlDeploymentReceiptRuntimeContract.Loaded): Unit =
      requireAssemblingOrReadable()
      if state != 1 || k5 != null || k6 != null || value == null || (value.semantics.owner ne this) ||
          value.semantics.family != MySqlDeploymentReceiptRuntimeContract.DeploymentFamily then
        throw new IllegalStateException("exact unoccupied schema5 K owner slot required")
      k5 = value
    private[mysql] def adoptDbFirstContract(value: MySqlDeploymentReceiptRuntimeContract.Loaded): Unit =
      requireAssemblingOrReadable()
      if state != 1 || k6 != null || k5 != null || value == null || (value.semantics.owner ne this) ||
          value.semantics.family != MySqlDeploymentReceiptRuntimeContract.DbFirstFamily then
        throw new IllegalStateException("exact unoccupied schema6 K owner slot required")
      k6 = value
    private[mysql] def requireDbFirstContract(value: MySqlDeploymentReceiptRuntimeContract.Semantics): Unit =
      requireAssemblingOrReadable()
      if state != 1 || k6 == null || (k6.semantics ne value) then
        throw new IllegalStateException("schema6 C requires actual assembling K slot")
    private[mysql] def adoptStaticIdentity(value: MySqlManifestConfigurationIdentity.ManifestSha256): Unit =
      requireAssemblingOrReadable()
      if state != 1 || c != null || value == null then throw new IllegalStateException("exact unoccupied schema5 C owner slot required")
      c = value
    private[mysql] def requireStaticBaseline(value: MySqlApprovedStaticBaseline): Unit =
      requireAssemblingOrReadable()
      if state != 1 || b == null || (b.baselineFor(root) ne value) then
        throw new IllegalStateException("schema5 K requires actual assembling baseline slot")
    private[mysql] def requireStaticContract(value: MySqlDeploymentReceiptRuntimeContract.Semantics): Unit =
      requireAssemblingOrReadable()
      if state != 1 || k5 == null || (k5.semantics ne value) then
        throw new IllegalStateException("schema5 C requires actual assembling K slot")
    private[mysql] def staticAssemblyRoot: MySqlFormat3ReadBudget =
      requireAssemblingOrReadable(); root
    private[mysql] def requireReadable(): Unit =
      requireThread()
      if state != 2 || root.isClosed then throw new IllegalStateException("V2 assembly owner is not readable",primary)
    def failure: Option[Throwable] = Option(primary)
    def hasUnsettledResources: Boolean = state != 5
    def failedCleanup: Option[Throwable] = Option(cleanupFailure)
    /** 消费方持有或使用返回的不可变图引用期间，必须保留此借用凭据。 */
    def borrow(): AutoCloseable = newLease()
    private[mysql] def newHistoryLease(): ConsumerLease = newLease()
    private[MySqlRuntimeInputs] def newLease(): ConsumerLease =
      requireReadable()
      root.reserveRetainedMemory(96L,"V2 consumer borrow BEFORE allocation")
      try
        val lease=new ConsumerLease(this)
        borrows+=1
        lease
      catch
        case failure: Throwable =>
          try root.releaseRetainedMemory(96L,"unpublished V2 consumer borrow")
          catch case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
          throw failure
    private[mysql] def borrowForSession(ledger: MySqlFormat3AllocationLedger): ConsumerLease =
      requireReadable();root.requireLedgerIdentity(ledger)
      newLease()
    private[mysql] def requireSessionInputs(value: MySqlRuntimeInputs, ledger: MySqlFormat3AllocationLedger): Unit =
      requireReadable();root.requireLedgerIdentity(ledger)
      if !sessionGraphReserved then throw new IllegalStateException("Session graph must be prepaid before allocation/adoption")
      if inputs ne value then throw new IllegalArgumentException("exact assembled V2 inputs required")
    private[MySqlRuntimeInputs] var deploymentSession: MySqlDeploymentReceiptSessionInputs = null
    private[MySqlRuntimeInputs] var deploymentAttempt: SessionInputAssemblyAttempt = null
    private[MySqlRuntimeInputs] var deploymentAttemptFee = 0L
    private[MySqlRuntimeInputs] var deploymentInputStarted=false
    private[mysql] def requireDeploymentAssemblyRoot(exactRoot: MySqlFormat3ReadBudget): Unit =
      requireThread()
      if root ne exactRoot then throw new IllegalArgumentException("original schema5 assembly root required")
    private[mysql] def deploymentInputObservation: Option[SessionInputAssemblyObservation] =
      requireThread()
      Option(deploymentAttempt).map(_.observation)
    private[mysql] def requireDeploymentRootBinding(exactRoot: MySqlFormat3ReadBudget,
        graph: DeploymentReceiptStaticGraph, lease: ConsumerLease, full26: MySqlScriptSchemaInputs): Unit =
      requireReadable()
      if (root ne exactRoot) || (static5 ne graph) || deploymentAttempt == null ||
          (deploymentAttempt.sessionLease ne lease) || (deploymentAttempt.full26 ne full26) then
        throw new IllegalArgumentException("original schema5 assembly attempt/graph/lease/inputs required")
      lease.requireStatic(this)
      val p = k5.semantics.profile
      val id = b.baselineFor(root).identity
      if !(p.sourceLimitProfile eq root.profile) || !(full26.sourceLimitProfile eq root.profile) ||
          full26.dataLayout != MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 || full26.schema != p.schema ||
          !full26.ddlSha256.equalsIgnoreCase(id.ddlSha256) || full26.budget != p.publicationBudget ||
          !full26.deploymentBudget.contains(p.deploymentBudget) then
        throw new IllegalArgumentException("actual K/profile/schema/DDL/budgets must bind exact Full26 inputs")
    private[mysql] def borrowForDeploymentSession(graph: DeploymentReceiptStaticGraph,
        exactRoot: MySqlFormat3ReadBudget): ConsumerLease =
      requireReadable()
      if (static5 ne graph) || (root ne exactRoot) then
        throw new IllegalArgumentException("exact schema5 assembly graph/root required")
      if deploymentSession != null || deploymentAttempt == null then throw new IllegalStateException("registered schema5 input attempt required")
      newLease()
    private[mysql] def requireDeploymentSessionInputs(value: MySqlDeploymentReceiptSessionInputs,
        exactRoot: MySqlFormat3ReadBudget): Unit =
      requireReadable()
      if (root ne exactRoot) || (deploymentSession ne value) then
        throw new IllegalArgumentException("exact assembled schema5 session inputs/root required")
    private[MySqlRuntimeInputs] var dbFirstSession: MySqlDbFirstSessionInputs = null
    private[MySqlRuntimeInputs] var dbFirstAttempt: DbFirstInputAssemblyAttempt = null
    private[MySqlRuntimeInputs] var dbFirstAttemptFee = 0L
    private[MySqlRuntimeInputs] var dbFirstInputStarted = false
    private[mysql] def requireDbFirstAssemblyRoot(exactRoot: MySqlFormat3ReadBudget): Unit =
      requireThread()
      if root ne exactRoot then throw new IllegalArgumentException("original schema6 assembly root required")
    private[mysql] def dbFirstInputObservation: Option[SessionInputAssemblyObservation] =
      requireThread()
      Option(dbFirstAttempt).map(_.observation)
    private[mysql] def requireDbFirstRootBinding(exactRoot: MySqlFormat3ReadBudget,
        graph: DbFirstStaticGraph, lease: ConsumerLease, full27: MySqlScriptSchemaInputs): Unit =
      requireReadable()
      if lease == null || full27 == null || (root ne exactRoot) || (static6 ne graph) || dbFirstAttempt == null ||
          (dbFirstAttempt.sessionLease ne lease) || (dbFirstAttempt.full27 ne full27) then
        throw new IllegalArgumentException("original schema6 assembly attempt/graph/lease/inputs required")
      lease.requireStatic(this)
      val p = k6.semantics.profile
      val id = b.baselineFor(root).identity
      if !(p.sourceLimitProfile eq root.profile) || !(full27.sourceLimitProfile eq root.profile) ||
          full27.dataLayout != MySqlScriptSchemaDataLayout.DbFirstFull27 || full27.schema != p.schema ||
          !full27.ddlSha256.equalsIgnoreCase(id.ddlSha256) || full27.budget != p.publicationBudget ||
          !full27.deploymentBudget.contains(p.deploymentBudget) then
        throw new IllegalArgumentException("actual schema6 K/profile/schema/DDL/budgets must bind exact Full27 inputs")
    private[mysql] def requireDbFirstSessionInputs(value: MySqlDbFirstSessionInputs,
        exactRoot: MySqlFormat3ReadBudget): Unit =
      requireReadable()
      if (root ne exactRoot) || (dbFirstSession ne value) then
        throw new IllegalArgumentException("exact assembled schema6 inputs/root required")
    private var sessionGraphReserved=false
    private[mysql] def reserveSessionGraph(): Unit =
      requireReadable()
      if sessionGraphReserved then throw new IllegalStateException("V2 Session graph reservation is single-use")
      // 固定初始图：Session/lifecycle/Option槽4096，actor/两个空ArrayBuffer2048，native holder固定槽2048。
      // 不含后续SQL/native/快照动态图；它们仍须通过原实际账本工作流预收。
      MySqlManifestConfigurationIdentity.temporary(root,65536L) {
        val metadata=inputs.originalMetadataV2
        // 4096字符覆盖固定键/版本marker、UUID/十进制数及B/R/K摘要；变量路径/字段逐项计数。
        var chars=4096L
        val entries=metadata.deploymentSemantics().canonicalFields().entrySet().iterator()
        while entries.hasNext do
          val entry=entries.next()
          chars=Math.addExact(chars,Math.addExact(2L,entry.getKey.length.toLong+entry.getValue.length.toLong))
        chars=Math.addExact(chars,metadata.baseline().absolutePath().toString.length.toLong)
        chars=Math.addExact(chars,metadata.format3RuntimeContract().absolutePath().toString.length.toLong)
        // UTF8最多3字节/UTF16单元：config与Data.configuration各一份，VerifiedAnchorInput另持完整outer canonical。
        // 后者采用codec实际65536B硬界；三个副本均保留到native的location/source/sourceAck真实drop。
        val canonicalGraph=Math.addExact(1024L+65536L,Math.multiplyExact(chars,6L))
        val total=Math.addExact(8192L,canonicalGraph)
        val next=Math.addExact(graphFee,total)
        // raw native读回不能按已批准C计费：未知长key在Properties.put拒绝前已完成loadConvert。
        val rawN=65536L
        var lineCapacity=1024L
        while lineCapacity<=rawN do lineCapacity=Math.multiplyExact(lineCapacity,2L)
        // raw+clone各N；decoder/decoded各2N；旧新line数组3L；builder增长/inflate 6N；
        // key/value及压缩尝试3N；typed的strip4N、两套PathAndRoot结果8N；normalize char/compress暂存3N；
        // UNC host/share substring暂存2N、扩展前缀substring/concat旧新4N，file引用substring2N、file前缀2N。
        val parserPeak=Math.addExact(65600L,Math.addExact(Math.multiplyExact(rawN,40L),Math.multiplyExact(lineCapacity,3L)))
        // fromBytesV2返回后clone/parser作用域结束；实际长known值仍能进入configurationBytesV2。
        // raw N+已解析键值2N+返回PathAndRoot8N/file前缀2N+escape旧新结果8N=21N；Q覆盖固定键与值最多两倍escape。
        // parser返回后strip及Path解析暂存已结束；若Path复用substring，其引用仍由返回PathAndRoot8N包络。
        // WindowsPath.root/path双字段均计入；UNC root不假定为固定盘符。
        // builder旧新6Q、最终String/压缩尝试3Q、UTF8最坏输出及trim副本6Q，另留固定map/对象workspace。
        val rawCanonicalChars=Math.addExact(4096L,Math.multiplyExact(rawN,2L))
        val canonicalPeak=Math.addExact(65536L,Math.addExact(Math.multiplyExact(rawN,21L),Math.multiplyExact(rawCanonicalChars,15L)))
        // 两个raw阶段取峰值；批准typed/canonical的16C独立保留，不冒称heap或整JVM容量证明。
        approvedConfigurationWorkspace=Math.multiplyExact(chars,16L)
        configurationWorkspace=Math.addExact(Math.max(parserPeak,canonicalPeak),approvedConfigurationWorkspace)
        root.reserveRetainedMemory(total,"V2 Session fixed graph and retained complete-A BEFORE allocation")
        graphFee=next;sessionGraphReserved=true
      }
    private var configurationWorkspace=0L
    private var approvedConfigurationWorkspace=0L
    private[mysql] def withApprovedConfigurationWorkspace[A](body: => A): A =
      requireReadable()
      if !sessionGraphReserved then throw new IllegalStateException("Session/configuration reservation required before use")
      // 纯批准FullV2→A不重读raw；只有实际native Store读回路径使用下方独立raw峰。
      MySqlManifestConfigurationIdentity.temporary(root,approvedConfigurationWorkspace)(body)
    private[mysql] def withConfigurationWorkspace[A](body: => A): A =
      requireReadable()
      if !sessionGraphReserved then throw new IllegalStateException("Session/configuration reservation required before use")
      MySqlManifestConfigurationIdentity.temporary(root,configurationWorkspace)(body)
    private[MySqlRuntimeInputs] def failed(failure: Throwable): Unit =
      if primary == null then primary=failure else if primary ne failure then primary.addSuppressed(failure)
      state=3
      try close()
      catch case cleanup: Throwable => if cleanup ne primary then primary.addSuppressed(cleanup)
    override def close(): Unit =
      requireThread()
      if state == 5 then return
      if deploymentAttempt != null then deploymentAttempt.closeUnpublished()
      if dbFirstAttempt != null then dbFirstAttempt.closeUnpublished()
      if borrows != 0 then throw new IllegalStateException("V2 consumers still borrow the assembly graph")
      if cleanupFailure != null then throw new IllegalStateException("V2 cleanup has unsettled failed slots; no automatic retry",cleanupFailure)
      state=4
      // 先清空已发布 holder 的图引用，再结算其保留费用。
      if inputs != null then inputs.dropV2References(this)
      if static5 != null then static5.detach(this)
      if static6 != null then static6.detach(this)
      def settle(action: => Unit): Boolean =
        try { action; true }
        catch
          case failure: Throwable =>
            if cleanupFailure == null then cleanupFailure=failure
            else if cleanupFailure ne failure then cleanupFailure.addSuppressed(failure)
            if primary == null then primary=failure
            else if primary ne failure then primary.addSuppressed(failure)
            false
      if history != null && settle(history.closeOwned()) then history=null
      if cleanupFailure != null then
        state=3
        throw new IllegalStateException("historical assembly cleanup incomplete",cleanupFailure)
      if !graphAttempted then
        graphAttempted=true
        if graphFee != 0L && settle(root.releaseRetainedMemory(graphFee,"V2 assembled graph references dropped")) then graphFee=0L
      if !cAttempted then
        cAttempted=true
        if c != null && settle(c.close()) then c=null
      if !kAttempted then
        kAttempted=true
        if k != null && settle(k.close()) then k=null
        if k5 != null && settle(k5.close()) then k5=null
        if k6 != null && settle(k6.close()) then k6=null
      if !bAttempted then
        bAttempted=true
        if b != null && settle(b.close()) then b=null
      if cleanupFailure != null then
        state=3
        throw new IllegalStateException("V2 assembly cleanup incomplete",cleanupFailure)
      inputs=null;static5=null;static6=null;deploymentSession=null
      deploymentAttempt=null
      dbFirstSession=null;dbFirstAttempt=null
      if dbFirstAttemptFee != 0L then
        if settle(root.releaseRetainedMemory(dbFirstAttemptFee,"schema6 input attempt holder detached")) then dbFirstAttemptFee=0L
        else
          state=3
          throw new IllegalStateException("schema6 input attempt cleanup incomplete",cleanupFailure)
      if deploymentAttemptFee != 0L && settle(root.releaseRetainedMemory(deploymentAttemptFee,"schema5 input attempt holder detached")) then
        deploymentAttemptFee=0L
      if !holderAttempted then
        holderAttempted=true
        if !settle(root.releaseRetainedMemory(512L,"V2 assembly holder dropped")) then
          state=3
          throw new IllegalStateException("V2 assembly holder cleanup incomplete",cleanupFailure)
      state=5

  /** 封闭 schema5 STATIC 图；没有向旧 RuntimeInputs、Session 或 native qualification 的转换。 */
  private[mysql] final class DeploymentReceiptStaticGraph private[MySqlRuntimeInputs] (
      private val owner: AssemblyOwner,
      private var metadataValue: MySqlRuntimeMetadata.DeploymentReceiptStatic,
      private var baselineValue: MySqlApprovedStaticBaseline,
      private var bindingValue: MySqlManifestConfigurationIdentity.DeploymentReceiptBinding,
      private var policyValue: MySqlRuntimeAuditPolicy
  ):
    def approvedBaselineIdentity: MySqlApprovedBaselineIdentity = { owner.requireReadable(); baselineValue.identity }
    def openAuditInputs(): MySqlDeploymentReceiptSchemaAuditInputs =
      owner.requireReadable()
      val lease = owner.newLease()
      try MySqlManifestConfigurationIdentity.temporary(owner.root, bindingValue.semantics.codecWorkspace) {
        new MySqlDeploymentReceiptSchemaAuditInputs(bindingValue, lease)
      }
      catch
        case failure: Throwable =>
          try lease.close()
          catch case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
          throw failure
    def auditPolicy: MySqlRuntimeAuditPolicy = { owner.requireReadable(); policyValue }
    def configurationSha256: String = { owner.requireReadable(); bindingValue.hex }
    def borrow(): AutoCloseable = owner.borrow()
    private[MySqlRuntimeInputs] def sessionOwner: AssemblyOwner = { owner.requireReadable(); owner }
    private[MySqlRuntimeInputs] def sessionMetadata: MySqlRuntimeMetadata.DeploymentReceiptStatic =
      owner.requireReadable(); metadataValue
    private[MySqlRuntimeInputs] def sessionBinding: MySqlManifestConfigurationIdentity.DeploymentReceiptBinding =
      owner.requireReadable(); bindingValue
    private[MySqlRuntimeInputs] def sessionBaseline: MySqlApprovedStaticBaseline =
      owner.requireReadable(); baselineValue
    private[mysql] def borrowForNative(actualRoot: MySqlFormat3ReadBudget): ConsumerLease =
      owner.requireReadable()
      if actualRoot ne owner.root then throw new IllegalArgumentException("schema5 native caller requires exact original root")
      owner.newLease()
    private[mysql] def borrowForPublication(actualRoot:MySqlFormat3ReadBudget):ConsumerLease =
      owner.requireReadable()
      owner.requireDeploymentAssemblyRoot(actualRoot)
      bindingValue.requireAlive()
      owner.newLease()
    private[mysql] def nativeMetadata(lease: ConsumerLease, actualRoot: MySqlFormat3ReadBudget): MySqlRuntimeMetadata.DeploymentReceiptStatic =
      if lease == null || (actualRoot ne owner.root) then throw new IllegalArgumentException("exact schema5 native consumer/root required")
      lease.requireStatic(owner); bindingValue.requireAlive()
      metadataValue
    private[mysql] def requireNativePlan(lease: ConsumerLease, actualRoot: MySqlFormat3ReadBudget,
        plan: com.wi.floatingpod.server.WindowsWorldProvisionReceiptDataCodec.PlanData): Unit =
      nativeMetadata(lease,actualRoot)
      bindingValue.semantics.recipe.requirePlanData(plan)
    private[MySqlRuntimeInputs] def detach(actual: AssemblyOwner): Unit =
      if actual ne owner then throw new IllegalArgumentException("exact schema5 graph owner required")
      if bindingValue != null then bindingValue.detach(owner)
      metadataValue=null;baselineValue=null;bindingValue=null;policyValue=null
    override def toString: String = "DeploymentReceiptStaticGraph[DATA; no admission]"

  private[mysql] def assembleSessionInputs(graph: DeploymentReceiptStaticGraph,
      exactRoot: MySqlFormat3ReadBudget, actualIoIdentity: MySqlOwnedIoIdentity,
      modeRequest: MySqlDeploymentReceiptSessionInputs.ModeRequest): MySqlDeploymentReceiptSessionInputs =
    require(graph != null && actualIoIdentity != null && modeRequest != null, "actual schema5 graph, IO identity and mode required")
    val owner = graph.sessionOwner
    owner.requireReadable()
    require(owner.root eq exactRoot, "exact schema5 assembly root required")
    if owner.deploymentInputStarted || owner.deploymentSession != null then
      throw new IllegalStateException("schema5 session-input assembly is one-shot, including failure")
    // 从原 8192B 固定结构费中拆出实际 attempt 槽和 root 持有的 Full26 对象；不重复计费。
    owner.deploymentInputStarted=true
    try
      exactRoot.reserveRetainedMemory(SessionInputAssemblyAttempt.HolderBytes,"schema5 input attempt BEFORE allocation")
      owner.deploymentAttemptFee=SessionInputAssemblyAttempt.HolderBytes
      val attempt = new SessionInputAssemblyAttempt(owner,graph,exactRoot,actualIoIdentity,modeRequest)
      owner.deploymentAttempt=attempt // 获取首个 lease/audit/inputs 之前先可达
      attempt.assemble()
    catch case failure: Throwable =>
      if owner.primary == null then owner.primary=failure else MySqlOwnedIoAdapter.suppressExitCleanup(owner.primary,failure)
      throw failure

  private object SessionInputAssemblyAttempt:
    // 对象头/标量余量 + 下列14个直接资源、identity与failure引用槽。
    val HolderBytes = 64L + 8L * 14L
    // inputs(7)、独立publication/deployment预算(9/2)、两个Some与Bound各1槽；
    // 另持独立schema/DDL字符串，不能跟随K/Session提前退费或借用K已经退费的对象。
    def bindingBytes(schema: String, ddl: String): Long =
      Math.addExact(64L * 6L + 8L * (7L + 9L + 2L + 1L + 1L + 1L),
        Math.addExact(64L + 2L * schema.length,64L + 2L * ddl.length))

  private[mysql] final case class SessionInputAssemblyObservation(
      stage: String, published: Boolean, inputsPending: Boolean, sessionLeasePending: Boolean,
      auditLeasePending: Boolean, auditPending: Boolean, temporaryFee: Long,
      inputFee: Long, bindingFee: Long, failure: Option[Throwable], cleanupFailure: Option[Throwable])

  private final class SessionInputAssemblyAttempt(owner: AssemblyOwner, graph: DeploymentReceiptStaticGraph,
      root: MySqlFormat3ReadBudget, identity: MySqlOwnedIoIdentity,
      mode: MySqlDeploymentReceiptSessionInputs.ModeRequest):
    var sessionLease: ConsumerLease = null
    private var auditLease: ConsumerLease = null
    private var audit: MySqlDeploymentReceiptSchemaAuditInputs = null
    var full26: MySqlScriptSchemaInputs = null
    private var result: MySqlDeploymentReceiptSessionInputs = null
    private var inputFee=0L
    private var bindingFee=0L
    private var temporaryFee=0L
    private var bound=false
    private var published=false
    private var cleanupAttempted=false
    private var primary: Throwable=null
    private var cleanupFailure: Throwable=null
    private var stage="registered"
    def observation: SessionInputAssemblyObservation = SessionInputAssemblyObservation(stage,published,result != null,
      sessionLease != null,auditLease != null,audit != null,temporaryFee,inputFee,bindingFee,
      Option(primary),Option(cleanupFailure))
    def assemble(): MySqlDeploymentReceiptSessionInputs =
      try
        stage="session-lease"
        sessionLease=owner.borrowForDeploymentSession(graph,root)
        var total=8192L-SessionInputAssemblyAttempt.HolderBytes
        val fields=graph.sessionMetadata.deploymentSemantics().canonicalFields().values().iterator()
        while fields.hasNext do total=Math.addExact(total,Math.multiplyExact(fields.next().length.toLong,16L))
        root.reserveRetainedMemory(total,"schema5 settings/input/root-binding structures BEFORE allocation")
        bindingFee=SessionInputAssemblyAttempt.bindingBytes(graph.sessionBinding.semantics.profile.schema,graph.sessionBaseline.identity.ddlSha256)
        inputFee=total-bindingFee
        root.reserveTemporaryMemory(graph.sessionBinding.semantics.codecWorkspace,"schema5 input assembly workspace")
        temporaryFee=graph.sessionBinding.semantics.codecWorkspace
        stage="audit-lease"
        auditLease=owner.newLease()
        stage="audit"
        audit=new MySqlDeploymentReceiptSchemaAuditInputs(graph.sessionBinding,auditLease)
        val p=graph.sessionBinding.semantics.profile
        val id=graph.sessionBaseline.identity
        stage="full26-inputs"
        full26=MySqlScriptSchemaInputs.createDeploymentReceiptFull26(new String(p.schema.toCharArray),
          new String(id.ddlSha256.toLowerCase(java.util.Locale.ROOT).toCharArray),
          p.sourceLimitProfile,p.publicationBudget.copy(),p.deploymentBudget.copy())
        stage="session-inputs"
        result=MySqlDeploymentReceiptSessionInputs.create(owner,root,identity,mode,graph.sessionMetadata,graph,
          graph.sessionBaseline,graph.sessionBinding,graph.auditPolicy,sessionLease,audit,inputFee,full26)
        stage="root-bind"
        root.bindDeploymentAssembly(owner,graph,sessionLease,full26)
        bound=true // BindingBytes 的计费随root，而非SessionInputs退休。
        stage="workspace-release"
        root.releaseTemporaryMemory(temporaryFee,"schema5 input assembly workspace detached")
        temporaryFee=0L
        stage="publication"
        owner.deploymentSession=result
        published=true
        stage="published"
        sessionLease=null;auditLease=null;audit=null;full26=null;inputFee=0L;bindingFee=0L
        result
      catch
        case failure: Throwable =>
          primary=failure
          if owner.primary == null then owner.primary=failure else MySqlOwnedSessionLifecycle.suppress(owner.primary,failure)
          closeUnpublished()
          throw failure
    def closeUnpublished(): Unit =
      if published then return
      if cleanupAttempted then
        if cleanupFailure != null then throw cleanupFailure
        return
      cleanupAttempted=true
      def settle(action: => Unit): Boolean =
        try { action;true }
        catch case failure: Throwable =>
          if cleanupFailure == null then cleanupFailure=failure else MySqlOwnedSessionLifecycle.suppress(cleanupFailure,failure)
          if primary != null then MySqlOwnedSessionLifecycle.suppress(primary,failure)
          false
      if result != null then
        if settle(result.closeUnpublished()) then
          result=null;sessionLease=null;auditLease=null;audit=null;inputFee=0L
      else
        if audit != null then
          if settle(audit.close()) then {audit=null;auditLease=null}
        else if auditLease != null && settle(auditLease.close()) then auditLease=null
        if sessionLease != null && settle(sessionLease.close()) then sessionLease=null
        if audit == null && auditLease == null && sessionLease == null && inputFee != 0L &&
            settle(root.releaseRetainedMemory(inputFee,"unpublished schema5 input references detached")) then inputFee=0L
      if !bound && full26 != null then full26=null
      if !bound && bindingFee != 0L && settle(root.releaseRetainedMemory(bindingFee,"unbound Full26 input detached")) then bindingFee=0L
      if temporaryFee != 0L && settle(root.releaseTemporaryMemory(temporaryFee,"failed schema5 input assembly workspace")) then temporaryFee=0L
      if cleanupFailure != null && primary == null then throw cleanupFailure

  private[mysql] def assembleDeploymentReceiptStatic(metadata: MySqlRuntimeMetadata.DeploymentReceiptStatic,
      owner: AssemblyOwner, policy: MySqlRuntimeAuditPolicy): DeploymentReceiptStaticGraph =
    require(owner != null, "caller-retained assembly owner required")
    owner.start()
    val root = owner.root
    try MySqlManifestConfigurationIdentity.temporary(root, 65536L) {
      require(metadata != null && policy != null, "actual schema5 metadata and policy required")
      val m = metadata.deploymentSemantics().canonicalFields()
      var fee = 4096L
      val entries = m.entrySet().iterator()
      while entries.hasNext do
        val e = entries.next()
        fee = Math.addExact(fee, 256L + 16L * (e.getKey.length.toLong + e.getValue.length.toLong))
      root.reserveRetainedMemory(fee, "schema5 metadata/binding/audit/static holder BEFORE capture")
      owner.graphFee = fee
      val pin = metadata.baseline()
      val id = MySqlApprovedBaselineIdentity(pin.format(), pin.schemaFormat(), pin.coverage(),
        UUID.fromString(m.get("expectedServerUuid")), pin.serverVersion(), m.get("schema"), pin.provisioningUuid(),
        UUID.fromString(m.get("expectedBindingUuid")), pin.ddlSha256(), pin.auditDefiner(), pin.sourceCaptureSha256())
      owner.b = MySqlApprovedBaselineLoader.loadOwned(pin.absolutePath(), pin.fileSha256(), id, root)
      val baseline = owner.b.baselineFor(root)
      owner.k5 = MySqlDeploymentReceiptRuntimeContract.load(metadata, baseline, owner)
      val semantic = owner.k5.semantics
      val (digest, binding) = MySqlManifestConfigurationIdentity.fromDeploymentReceiptSemantics(
        metadata.deploymentSemantics(), semantic, policy, owner)
      owner.c = digest
      owner.static5 = new DeploymentReceiptStaticGraph(owner, metadata, baseline, binding, policy)
      owner.state = 2
      owner.static5
    }
    catch
      case failure: Throwable => owner.failed(failure); throw failure

  /** schema6独立静态图：发行受控security/Session输入借用，不发行SQL/native Plan。 */
  private[mysql] final class DbFirstStaticGraph private[MySqlRuntimeInputs] (
      private val owner: AssemblyOwner,
      private var metadataValue: MySqlRuntimeMetadata.DbFirstStatic,
      private var baselineValue: MySqlApprovedStaticBaseline,
      private var bindingValue: MySqlManifestConfigurationIdentity.DbFirstBinding,
      private var policyValue: MySqlRuntimeAuditPolicy):
    def approvedBaselineIdentity: MySqlApprovedBaselineIdentity = { owner.requireReadable(); baselineValue.identity }
    def configurationSha256: String = { owner.requireReadable(); bindingValue.hex }
    def auditPolicy: MySqlRuntimeAuditPolicy = { owner.requireReadable(); policyValue }
    def borrow(): AutoCloseable = owner.borrow()
    def openAuditInputs(): MySqlDbFirstSchemaAuditInputs =
      owner.requireReadable()
      val lease = owner.newLease()
      var prepaid = false
      try
        owner.root.reserveRetainedMemory(512L, "schema6 audit consumer/one expectation BEFORE allocation")
        prepaid = true
        MySqlManifestConfigurationIdentity.temporary(owner.root, bindingValue.semantics.codecWorkspace) {
          new MySqlDbFirstSchemaAuditInputs(bindingValue, lease)
        }
      catch
        case cause: Throwable =>
          if prepaid then
            try owner.root.releaseRetainedMemory(512L, "unpublished schema6 consumer")
            catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
          try lease.close()
          catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(cause, cleanup)
          throw cause
    def auditExpectation(inputs: MySqlDbFirstSchemaAuditInputs): MySqlSchemaAuditExpectation =
      owner.requireReadable()
      inputs.expectationFor(bindingValue, baselineValue, policyValue)
    private[MySqlRuntimeInputs] def sessionOwner: AssemblyOwner = { owner.requireReadable(); owner }
    private[MySqlRuntimeInputs] def sessionMetadata: MySqlRuntimeMetadata.DbFirstStatic =
      owner.requireReadable(); metadataValue
    private[MySqlRuntimeInputs] def sessionBinding: MySqlManifestConfigurationIdentity.DbFirstBinding =
      owner.requireReadable(); bindingValue
    private[MySqlRuntimeInputs] def sessionBaseline: MySqlApprovedStaticBaseline =
      owner.requireReadable(); baselineValue
    private[MySqlRuntimeInputs] def detach(actual: AssemblyOwner): Unit =
      if actual ne owner then throw new IllegalArgumentException("exact schema6 graph owner required")
      if bindingValue != null then bindingValue.detach(owner)
      metadataValue=null;baselineValue=null;bindingValue=null;policyValue=null

  private[mysql] def assembleDbFirstSessionInputs(graph: DbFirstStaticGraph,
      exactRoot: MySqlFormat3ReadBudget, actualIoIdentity: MySqlOwnedIoIdentity): MySqlDbFirstSessionInputs =
    require(graph != null && actualIoIdentity != null, "actual schema6 graph and IO identity required")
    val owner = graph.sessionOwner
    owner.requireDbFirstAssemblyRoot(exactRoot)
    if owner.dbFirstInputStarted then throw new IllegalStateException("schema6 input assembly is one-shot, including failure")
    owner.dbFirstInputStarted=true
    try
      exactRoot.reserveRetainedMemory(256L,"schema6 attempt fixed slots BEFORE allocation")
      owner.dbFirstAttemptFee=256L
      val attempt = new DbFirstInputAssemblyAttempt(owner,graph,exactRoot,actualIoIdentity)
      owner.dbFirstAttempt=attempt
      attempt.assemble()
    catch case failure: Throwable =>
      if owner.primary == null then owner.primary=failure else MySqlOwnedSessionLifecycle.suppress(owner.primary,failure)
      throw failure

  private final class DbFirstInputAssemblyAttempt(owner: AssemblyOwner, graph: DbFirstStaticGraph,
      root: MySqlFormat3ReadBudget, identity: MySqlOwnedIoIdentity):
    var sessionLease: ConsumerLease = null
    private var auditLease: ConsumerLease = null
    private var audit: MySqlDbFirstSchemaAuditInputs = null
    var full27: MySqlScriptSchemaInputs = null
    private var result: MySqlDbFirstSessionInputs = null
    private var inputFee=0L
    private var bindingFee=0L
    private var auditFee=0L
    private var temporaryFee=0L
    private var bound=false
    private var published=false
    private var cleanupAttempted=false
    private var primary: Throwable=null
    private var cleanupFailure: Throwable=null
    private var stage="registered"
    def observation: SessionInputAssemblyObservation = SessionInputAssemblyObservation(stage,published,result != null,
      sessionLease != null,auditLease != null,audit != null,temporaryFee,inputFee,bindingFee,
      Option(primary),Option(cleanupFailure))
    def assemble(): MySqlDbFirstSessionInputs =
      try
        stage="session-lease"
        sessionLease=owner.newLease()
        var total=8192L
        val fields=graph.sessionMetadata.deploymentSemantics().canonicalFields().values().iterator()
        while fields.hasNext do total=Math.addExact(total,Math.multiplyExact(fields.next().length.toLong,16L))
        root.reserveRetainedMemory(total,"schema6 settings/audit/input/root-binding BEFORE allocation")
        bindingFee=SessionInputAssemblyAttempt.bindingBytes(graph.sessionBinding.semantics.profile.schema,graph.sessionBaseline.identity.ddlSha256)
        auditFee=512L
        inputFee=total-bindingFee-auditFee
        root.reserveTemporaryMemory(graph.sessionBinding.semantics.codecWorkspace,"schema6 input assembly workspace")
        temporaryFee=graph.sessionBinding.semantics.codecWorkspace
        stage="audit-lease"
        auditLease=owner.newLease()
        stage="audit"
        audit=new MySqlDbFirstSchemaAuditInputs(graph.sessionBinding,auditLease)
        auditFee=0L // 实际 consumer 从此承担该512B直到自身close。
        val p=graph.sessionBinding.semantics.profile
        val id=graph.sessionBaseline.identity
        stage="full27-inputs"
        val copied=MySqlDeploymentReceiptSchemaProfile.inputs(new String(p.schema.toCharArray),p.accounts,
          p.sourceLimitProfile,p.publicationBudget.copy(),p.deploymentBudget.copy(),p.quotaMaximumRecords,
          p.trustedAdministrativeProxyRows)
        val rendered = if graph.sessionBinding.semantics.text("sourceProvenanceKind") == "MANAGED_INSTALL" then
          MySqlDbFirstSchemaProfile.renderExistingDatabase(copied)
        else MySqlDbFirstSchemaProfile.render(copied)
        require(rendered.sha256.equalsIgnoreCase(id.ddlSha256) &&
          rendered.sha256.equalsIgnoreCase(graph.sessionBinding.semantics.rendered.sha256),"schema6 actual renderer differs from pinned K/B")
        // Root仅保留Full27的schema/budgets/DDL；临时renderer不会外逃。
        full27=MySqlScriptSchemaInputs.createDbFirstFull27(rendered)
        stage="session-inputs"
        result=MySqlDbFirstSessionInputs.create(owner,root,identity,graph.sessionMetadata,graph.sessionBinding,
          sessionLease,audit,graph.auditExpectation(audit),inputFee,full27)
        stage="root-bind"
        root.bindDbFirstAssembly(owner,graph,sessionLease,full27)
        bound=true
        stage="workspace-release"
        root.releaseTemporaryMemory(temporaryFee,"schema6 input assembly workspace detached")
        temporaryFee=0L
        owner.dbFirstSession=result
        published=true;stage="published"
        sessionLease=null;auditLease=null;audit=null;full27=null;inputFee=0L;bindingFee=0L
        result
      catch case failure: Throwable =>
        primary=failure
        if owner.primary == null then owner.primary=failure else MySqlOwnedSessionLifecycle.suppress(owner.primary,failure)
        closeUnpublished()
        throw failure
    def closeUnpublished(): Unit =
      if published then return
      if cleanupAttempted then
        if cleanupFailure != null then throw cleanupFailure
        return
      cleanupAttempted=true
      def settle(action: => Unit): Boolean =
        try { action;true }
        catch case failure: Throwable =>
          if cleanupFailure == null then cleanupFailure=failure else MySqlOwnedSessionLifecycle.suppress(cleanupFailure,failure)
          if primary != null then MySqlOwnedSessionLifecycle.suppress(primary,failure)
          false
      if result != null then
        if settle(result.closeUnpublished()) then
          result=null;sessionLease=null;auditLease=null;audit=null;inputFee=0L
      else
        if audit != null then
          if settle(audit.close()) then {audit=null;auditLease=null}
        else if auditLease != null && settle(auditLease.close()) then auditLease=null
        if sessionLease != null && settle(sessionLease.close()) then sessionLease=null
        if audit == null && auditLease == null && sessionLease == null && inputFee != 0L &&
            settle(root.releaseRetainedMemory(inputFee,"unpublished schema6 input references detached")) then inputFee=0L
      if audit == null && auditFee != 0L && settle(root.releaseRetainedMemory(auditFee,"unconstructed schema6 audit consumer")) then auditFee=0L
      if !bound then full27=null
      if !bound && bindingFee != 0L && settle(root.releaseRetainedMemory(bindingFee,"unbound Full27 inputs detached")) then bindingFee=0L
      if temporaryFee != 0L && settle(root.releaseTemporaryMemory(temporaryFee,"failed schema6 assembly workspace")) then temporaryFee=0L
      if cleanupFailure != null && primary == null then throw cleanupFailure

  private[mysql] def assembleDbFirstStatic(metadata: MySqlRuntimeMetadata.DbFirstStatic,
      owner: AssemblyOwner, policy: MySqlRuntimeAuditPolicy): DbFirstStaticGraph =
    require(owner != null, "caller-retained schema6 owner required")
    owner.start()
    val root = owner.root
    try MySqlManifestConfigurationIdentity.temporary(root, 65536L) {
      require(metadata != null && policy != null, "actual schema6 metadata and policy required")
      val m = metadata.deploymentSemantics().canonicalFields()
      var fee = 4096L
      val entries = m.entrySet().iterator()
      while entries.hasNext do
        val e = entries.next()
        fee = Math.addExact(fee, 256L + 16L * (e.getKey.length.toLong + e.getValue.length.toLong))
      root.reserveRetainedMemory(fee, "schema6 metadata/binding/audit/static holder BEFORE capture")
      owner.graphFee = fee
      val pin = metadata.baseline()
      val id = MySqlApprovedBaselineIdentity(pin.format(), pin.schemaFormat(), pin.coverage(),
        UUID.fromString(m.get("expectedServerUuid")), pin.serverVersion(), m.get("schema"), pin.provisioningUuid(),
        UUID.fromString(m.get("expectedBindingUuid")), pin.ddlSha256(), pin.auditDefiner(), pin.sourceCaptureSha256())
      owner.b = MySqlApprovedBaselineLoader.loadOwned(pin.absolutePath(), pin.fileSha256(), id, root)
      val baseline = owner.b.baselineFor(root)
      owner.k6 = MySqlDbFirstRuntimeContract.load(metadata, baseline, owner)
      owner.loadHistory()
      val (digest, binding) = MySqlManifestConfigurationIdentity.fromDbFirstSemantics(
        metadata.deploymentSemantics(), owner.k6.semantics, policy, owner)
      owner.c = digest
      owner.static6 = new DbFirstStaticGraph(owner, metadata, baseline, binding, policy)
      owner.state = 2
      owner.static6
    }
    catch
      case cause: Throwable => owner.failed(cause); throw cause

  /** 实际调用方先持有 lease，再将同一 lease 交给新 Session；构造失败仍由调用方保留。 */
  private[mysql] final class ConsumerLease private[MySqlRuntimeInputs] (private val owner: AssemblyOwner) extends AutoCloseable:
    private var open=true
    private[mysql] def requireStatic(actual: AssemblyOwner): Unit =
      if !open || (actual ne owner) then throw new IllegalStateException("exact live schema5 consumer lease required")
      owner.requireReadable()
    private[mysql] def requireFor(inputs: MySqlRuntimeInputs, ledger: MySqlFormat3AllocationLedger): Unit =
      if !open then throw new IllegalStateException("V2 consumer lease is closed")
      owner.requireSessionInputs(inputs,ledger)
    override def close(): Unit =
      owner.requireThread()
      if open then
        owner.root.releaseRetainedMemory(96L,"V2 consumer borrow dropped")
        open=false;owner.borrows-=1

  private[mysql] def beginV2(root: MySqlFormat3ReadBudget, actualOwnedIoThread: Thread): AssemblyOwner =
    require(actualOwnedIoThread != null && (Thread.currentThread() eq actualOwnedIoThread),"exact V2 IO owner thread required")
    MySqlManifestConfigurationIdentity.requireRoot(root)
    root.reserveRetainedMemory(512L,"V2 caller-retained assembly holder BEFORE allocation")
    try
      val owner=new AssemblyOwner(root,actualOwnedIoThread)
      root.registerDeploymentAssemblyOwner(owner)
      owner
    catch
      case failure: Throwable =>
        try root.releaseRetainedMemory(512L,"unpublished V2 assembly holder")
        catch case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
        throw failure

  private[mysql] def assembleV2(metadata: MySqlRuntimeMetadata.FullV2, owner: AssemblyOwner,
      bootId: UUID, bootGeneration: Long, auditPolicy: MySqlRuntimeAuditPolicy,
      freshPlan: WindowsWorldProvisioner.Plan): MySqlRuntimeInputs =
    assembleOwnedV2(metadata,owner,bootId,bootGeneration,auditPolicy,freshPlan,null,false)

  private[mysql] def assembleCertifiedOrdinaryV2(metadata: MySqlRuntimeMetadata.FullV2, owner: AssemblyOwner,
      bootId: UUID, bootGeneration: Long, auditPolicy: MySqlRuntimeAuditPolicy,
      original: ProductionOrdinarySourceApproval.ApprovedOriginalCreation): MySqlRuntimeInputs =
    assembleOwnedV2(metadata,owner,bootId,bootGeneration,auditPolicy,null,original,true)

  private def assembleOwnedV2(metadata: MySqlRuntimeMetadata.FullV2, owner: AssemblyOwner,
      bootId: UUID, bootGeneration: Long, auditPolicy: MySqlRuntimeAuditPolicy,
      freshPlan: WindowsWorldProvisioner.Plan,
      original: ProductionOrdinarySourceApproval.ApprovedOriginalCreation, ordinary: Boolean): MySqlRuntimeInputs =
    require(owner != null,"caller-retained V2 assembly owner required")
    owner.start()
    val root=owner.root
    try MySqlManifestConfigurationIdentity.temporary(root,65536L) {
      // 固定 census/身份工作区先付费，再迭代 canonical map 并分配身份对象。
      require(metadata != null && bootId != null && bootGeneration > 0L && auditPolicy != null,
        "typed metadata, boot identity, positive generation and policy required")
      require(metadata.activeLimit() >= 1 && metadata.activeLimit() <= ProductionActiveHardCap,"runtime active limit exceeds production hard cap")
      require(!ordinary || original != null,"approved original creation is required")
      val pins=metadata.baseline()
      require(pins.schemaFormat() == MySqlApprovedBaselineLoader.Format3SchemaFormat && pins.coverage() == MySqlAuditCoverage.Format3,
        "V2 assembly requires exact format3 metadata")
      // 承接已创建 FullV2 seed 的存活保留责任；不倒收早先 bootstrap 的分配费用。
      var seedFee=2048L
      val entries=metadata.deploymentSemantics().canonicalFields().entrySet().iterator()
      while entries.hasNext do
        val entry=entries.next()
        seedFee=Math.addExact(seedFee,256L+8L*(entry.getKey.length.toLong+entry.getValue.length.toLong))
      root.reserveRetainedMemory(seedFee,"FullV2 seed live retention BEFORE owner capture")
      owner.graphFee=seedFee
      val identity=MySqlApprovedBaselineIdentity(pins.format(),pins.schemaFormat(),pins.coverage(),metadata.expectedServerUuid(),
        pins.serverVersion(),metadata.schema(),pins.provisioningUuid(),metadata.expectedBindingUuid(),pins.ddlSha256(),pins.auditDefiner(),pins.sourceCaptureSha256())
      owner.b=MySqlApprovedBaselineLoader.loadOwned(pins.absolutePath(),pins.fileSha256(),identity,root)
      val baseline=owner.b.baselineFor(root)
      owner.k=MySqlFormat3RuntimeContractLoader.loadV2(metadata,baseline,root)
      owner.k.requirePolicy(auditPolicy)
      owner.c=MySqlManifestConfigurationIdentity.fromValidatedSemantics(metadata.deploymentSemantics(),owner.k.semanticFor(root),auditPolicy,root)
      val graph=owner.k.runtimeGraphFee(root)
      root.reserveRetainedMemory(graph,"V2 normal form/audit/settings/holder graph BEFORE allocation")
      owner.graphFee=Math.addExact(owner.graphFee,graph)
      MySqlManifestConfigurationIdentity.temporary(root,Math.addExact(seedFee,owner.k.runtimeWorkspaceFee(root))) {
        val contract=owner.k.runtimeContractFor(root)
        if !ordinary then contract.requireFreshPlan(freshPlan)
        else
          val configuration=WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(metadata)
          require(configuration.configurationSha256().equalsIgnoreCase(original.configurationSha256()),"complete current A differs from approved original source")
          require(metadata.format3RuntimeContract().fileSha256().equalsIgnoreCase(original.runtimeContractSha256()),"K raw pin differs from approved original source")
          require(baseline.fileSha256.equalsIgnoreCase(original.baselineSha256()),"B raw pin differs from approved original source")
          contract.requireOriginalCreation(original)
        val audit=contract.auditInputs(HexFormat.of().parseHex(owner.c.hex))
        if metadata.format3ManifestConfigurationEncoding() == MySqlRuntimeMetadata.MANIFEST_V3 &&
          ((audit.privilegeRoot ne root) || audit.callerPrivileges == null) then
          throw new IllegalStateException("V3 audit lost its original paid privilege owner")
        audit.requireIdentity(identity.serverUuid,identity.serverVersion,identity.schema,contract.effectiveUser,contract.callerRoles,
          identity.provisioningUuid,identity.bindingUuid,identity.ddlSha256,identity.auditDefiner)
        val expectation=MySqlSchemaAuditExpectation.forFormat3(identity.serverUuid,identity.serverVersion,identity.schema,contract.effectiveUser,
          contract.callerRoles,identity.provisioningUuid,identity.bindingUuid,identity.ddlSha256,identity.auditDefiner,contract.auditDefinerRoles,
          contract.expectedImport,baseline.objects,audit)
        val settings=MySqlConnectionSettings(metadata.host(),metadata.port(),metadata.schema(),metadata.expectedServerUuid(),metadata.expectedEffectiveUser(),
          MySqlFileSources.credentials(owner.thread,metadata.username(),metadata.credentialSourceRef()),metadata.pkcs12TrustStorePath(),
          MySqlFileSources.trustSecret(owner.thread,metadata.trustSecretSourceRef()),metadata.connectTimeout(),metadata.socketTimeout(),
          MySqlDurabilityExpectation(metadata.innodbFlushLogAtTrxCommit(),metadata.syncBinlog()))
        owner.inputs=new MySqlRuntimeInputs(null,owner.thread,bootId,bootGeneration,metadata.activeLimit(),settings,baseline,contract,expectation,auditPolicy,owner,metadata)
        owner.state=2
        owner.inputs
      }
    }
    catch
      case failure: Throwable =>
        owner.failed(failure)
        throw failure

  /** 必须由真实 IO owner 调用；secret Sources 在此只构造，不 acquire。 */
  def assemble(
      metadata: MySqlRuntimeMetadata,
      actualOwnedIoThread: Thread,
      bootId: UUID,
      bootGeneration: Long,
      auditPolicy: MySqlRuntimeAuditPolicy,
      freshPlan: Option[WindowsWorldProvisioner.Plan] = None
  ): MySqlRuntimeInputs =
    require(actualOwnedIoThread != null && (Thread.currentThread() eq actualOwnedIoThread),
      "runtime assembly requires exact owned IO thread")
    require(metadata != null && bootId != null && bootGeneration > 0L && auditPolicy != null,
      "runtime metadata, boot identity, positive generation and audit policy are required")
    require(metadata.activeLimit() >= 1 && metadata.activeLimit() <= ProductionActiveHardCap,
      "runtime active limit exceeds production hard cap")
    val pins = metadata.baseline()
    if pins.schemaFormat() == MySqlScriptSchemaProfile.SchemaFormat then
      throw new SQLNonTransientConnectionException("schema4 runtime Ready is not supported; independent read-only inspection only")
    require(pins.schemaFormat() == MySqlApprovedBaselineLoader.Format3SchemaFormat &&
      pins.coverage() == MySqlAuditCoverage.Format3,
      "formal owned runtime requires exact schema format 3; format 1/2 remain offline compatibility only")
    val identity = MySqlApprovedBaselineIdentity(
      pins.format(), pins.schemaFormat(), pins.coverage(), metadata.expectedServerUuid(),
      pins.serverVersion(), metadata.schema(), pins.provisioningUuid(), metadata.expectedBindingUuid(),
      pins.ddlSha256(), pins.auditDefiner(), pins.sourceCaptureSha256())
    val baseline = MySqlApprovedBaselineLoader.load(pins.absolutePath(), pins.fileSha256(), identity)
    val contract = MySqlFormat3RuntimeContractLoader.load(metadata, baseline)
    contract.requirePolicy(auditPolicy)
    val plan = freshPlan.getOrElse(throw new SQLNonTransientConnectionException(
      "format3 source provenance requires an actual issuer; this runtime path currently admits only the FreshPlan issuer"))
    contract.requireFreshPlan(plan)
    val configuration = WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(metadata)
    val configurationSha256 = HexFormat.of().parseHex(configuration.configurationSha256())
    val format3Inputs = contract.auditInputs(configurationSha256)
    format3Inputs.requireIdentity(identity.serverUuid, identity.serverVersion, identity.schema,
      contract.effectiveUser, contract.callerRoles, identity.provisioningUuid, identity.bindingUuid,
      identity.ddlSha256, identity.auditDefiner)
    val expectation = MySqlSchemaAuditExpectation.forFormat3(
      identity.serverUuid, identity.serverVersion, identity.schema, contract.effectiveUser,
      contract.callerRoles, identity.provisioningUuid, identity.bindingUuid, identity.ddlSha256,
      identity.auditDefiner, contract.auditDefinerRoles, contract.expectedImport, baseline.objects, format3Inputs)
    val settings = MySqlConnectionSettings(
      metadata.host(), metadata.port(), metadata.schema(), metadata.expectedServerUuid(),
      metadata.expectedEffectiveUser(),
      MySqlFileSources.credentials(actualOwnedIoThread, metadata.username(), metadata.credentialSourceRef()),
      metadata.pkcs12TrustStorePath(),
      MySqlFileSources.trustSecret(actualOwnedIoThread, metadata.trustSecretSourceRef()),
      metadata.connectTimeout(), metadata.socketTimeout(),
      MySqlDurabilityExpectation(metadata.innodbFlushLogAtTrxCommit(), metadata.syncBinlog()))
    new MySqlRuntimeInputs(metadata, actualOwnedIoThread, bootId, bootGeneration, metadata.activeLimit(),
      settings, baseline, contract, expectation, auditPolicy)

  /** Explicit ordinary path: preserve and compare the original birth recipe without selecting FreshPlan admission. */
  def assembleCertifiedOrdinary(
      metadata: MySqlRuntimeMetadata,
      actualOwnedIoThread: Thread,
      bootId: UUID,
      bootGeneration: Long,
      auditPolicy: MySqlRuntimeAuditPolicy,
      original: ProductionOrdinarySourceApproval.ApprovedOriginalCreation
  ): MySqlRuntimeInputs =
    require(actualOwnedIoThread != null && (Thread.currentThread() eq actualOwnedIoThread),
      "ordinary runtime assembly requires exact owned IO thread")
    require(metadata != null && bootId != null && bootGeneration > 0L && auditPolicy != null && original != null,
      "approved original source, current boot identity and policy are required")
    require(metadata.activeLimit() >= 1 && metadata.activeLimit() <= ProductionActiveHardCap,
      "runtime active limit exceeds production hard cap")
    val pins = metadata.baseline()
    if pins.schemaFormat() == MySqlScriptSchemaProfile.SchemaFormat then
      throw new SQLNonTransientConnectionException("schema4 runtime Ready is not supported; independent read-only inspection only")
    require(pins.schemaFormat() == MySqlApprovedBaselineLoader.Format3SchemaFormat &&
      pins.coverage() == MySqlAuditCoverage.Format3,
      "ordinary Certified runtime requires exact schema format 3 and format3 coverage")
    val identity = MySqlApprovedBaselineIdentity(
      pins.format(), pins.schemaFormat(), pins.coverage(), metadata.expectedServerUuid(),
      pins.serverVersion(), metadata.schema(), pins.provisioningUuid(), metadata.expectedBindingUuid(),
      pins.ddlSha256(), pins.auditDefiner(), pins.sourceCaptureSha256())
    val baseline = MySqlApprovedBaselineLoader.load(pins.absolutePath(), pins.fileSha256(), identity)
    val contract = MySqlFormat3RuntimeContractLoader.load(metadata, baseline)
    contract.requirePolicy(auditPolicy)
    val configuration = WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(metadata)
    val configurationSha256 = HexFormat.of().parseHex(configuration.configurationSha256())
    require(configuration.configurationSha256().equalsIgnoreCase(original.configurationSha256()),
      "canonical current metadata configuration differs from approved original source")
    val runtimeContractPin = metadata.format3RuntimeContract().orElseThrow(
      () => new SQLNonTransientConnectionException("ordinary Certified Format3 runtime-contract pin is missing"))
    require(runtimeContractPin.fileSha256().equalsIgnoreCase(original.runtimeContractSha256()),
      "Format3 runtime-contract raw pin differs from approved original source")
    require(baseline.fileSha256.equalsIgnoreCase(original.baselineSha256()),
      "loaded Format3 baseline raw pin differs from approved original source")
    contract.requireOriginalCreation(original)
    val format3Inputs = contract.auditInputs(configurationSha256)
    format3Inputs.requireIdentity(identity.serverUuid, identity.serverVersion, identity.schema,
      contract.effectiveUser, contract.callerRoles, identity.provisioningUuid, identity.bindingUuid,
      identity.ddlSha256, identity.auditDefiner)
    val expectation = MySqlSchemaAuditExpectation.forFormat3(
      identity.serverUuid, identity.serverVersion, identity.schema, contract.effectiveUser,
      contract.callerRoles, identity.provisioningUuid, identity.bindingUuid, identity.ddlSha256,
      identity.auditDefiner, contract.auditDefinerRoles, contract.expectedImport, baseline.objects, format3Inputs)
    val settings = MySqlConnectionSettings(
      metadata.host(), metadata.port(), metadata.schema(), metadata.expectedServerUuid(),
      metadata.expectedEffectiveUser(),
      MySqlFileSources.credentials(actualOwnedIoThread, metadata.username(), metadata.credentialSourceRef()),
      metadata.pkcs12TrustStorePath(),
      MySqlFileSources.trustSecret(actualOwnedIoThread, metadata.trustSecretSourceRef()),
      metadata.connectTimeout(), metadata.socketTimeout(),
      MySqlDurabilityExpectation(metadata.innodbFlushLogAtTrxCommit(), metadata.syncBinlog()))
    new MySqlRuntimeInputs(metadata, actualOwnedIoThread, bootId, bootGeneration, metadata.activeLimit(),
      settings, baseline, contract, expectation, auditPolicy)
