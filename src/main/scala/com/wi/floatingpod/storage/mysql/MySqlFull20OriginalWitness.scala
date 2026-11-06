package com.wi.floatingpod.storage.mysql

import java.io.{DataOutputStream, OutputStream}
import java.nio.charset.StandardCharsets
import com.wi.floatingpod.storage.mysql.MySqlFreshCertifyCapabilities.PlannedFreshCertify
import com.wi.floatingpod.storage.mysql.MySqlFreshPrepareCapabilities.PlannedFreshPrepare
import com.wi.floatingpod.storage.mysql.MySqlCertifiedOrdinaryCapabilities.PlannedCertifiedHandoff
import com.wi.floatingpod.server.{B1ResumeRecord, WindowsB1Full20WitnessStore}

/** 原 actor 的完整 DATA；保留 private-issued plan 和原根，不签 native ACK/SQL/Ready。 */
private[mysql] final class MySqlFull20OriginalWitness private (
    val originalPlan: MySqlFull20OriginalWitness.OriginalPlan,
    val originalRoot: MySqlFormat3ReadBudget,
    val beforePayload: MySqlFormat3CanonicalPayload,
    val targetPayload: MySqlFormat3CanonicalPayload,
    val payload: MySqlFormat3Binary,
    private val originalIntentBytes: Array[Byte],
    private val originalProfileBytes: Array[Byte]
):
  private val owner = Thread.currentThread()
  private var nativeTransfer: MySqlFull20NativeTransfer = null
  private[mysql] def transferForNative(): MySqlFull20NativeTransfer =
    require(Thread.currentThread() eq owner, "exact witness actor thread required")
    require(nativeTransfer == null && !originalRoot.isClosed, "native witness transport is one-shot")
    originalRoot.reserveRetainedMemory(192L, "native witness transfer holder")
    nativeTransfer = new MySqlFull20NativeTransfer(this, owner)
    nativeTransfer.prepare()
    nativeTransfer
  override def toString: String = "Full20OriginalWitness[complete DATA; no admission]"

private[mysql] object MySqlFull20OriginalWitness:
  private val Domain = "FP_B1_FULL20_ORIGINAL_WITNESS_BE_V1".getBytes(StandardCharsets.US_ASCII)

  sealed trait OriginalPlan:
    def before: MySqlB1CommonGraph
    def target: MySqlB1CommonGraph
    def record: B1ResumeRecord
    def byteLength: Int
    def copyBytes(): Array[Byte]
  private final case class Certify(value: PlannedFreshCertify) extends OriginalPlan:
    def before = value.beforeGraph()
    def target = value.targetGraph()
    def record = value.intentRecord()
    def byteLength = value.intentCanonicalByteLength()
    def copyBytes() = value.intentCanonicalBytes()
  private final case class Prepare(value: PlannedFreshPrepare) extends OriginalPlan:
    def before = value.beforeGraph()
    def target = value.targetGraph()
    def record = value.genesisRecord()
    def byteLength = value.genesisCanonicalByteLength()
    def copyBytes() = value.genesisCanonicalBytes()
  private final case class CertifiedHandoff(value: PlannedCertifiedHandoff) extends OriginalPlan:
    def before = value.beforeB1Graph
    def target = value.targetB1Graph
    def record = value.targetTransition
    def byteLength = value.canonicalBytes.length
    def copyBytes() = value.canonicalBytes.clone()

  /** 注册在 actual actor 字段后才能 materialize；失败部分强引用不会由局部返回值遗失。 */
  final class Draft private[MySqlFull20OriginalWitness] (
      val plan: OriginalPlan, val root: MySqlFormat3ReadBudget):
    private[MySqlFull20OriginalWitness] var beforePayload: MySqlFormat3CanonicalPayload = null
    private[MySqlFull20OriginalWitness] var targetPayload: MySqlFormat3CanonicalPayload = null
    private[MySqlFull20OriginalWitness] var intent: Array[Byte] = null
    private[MySqlFull20OriginalWitness] var profile: Array[Byte] = null
    private[MySqlFull20OriginalWitness] var completed: MySqlFull20OriginalWitness = null
    private[MySqlFull20OriginalWitness] var attempted = false

  def draft(plan: PlannedFreshCertify, root: MySqlFormat3ReadBudget): Draft =
    require(plan != null && root != null && !root.isClosed, "exact original actor plan/root required")
    root.reserveRetainedMemory(192L, "retained original witness draft")
    new Draft(Certify(plan), root)

  def draft(plan: PlannedFreshPrepare, root: MySqlFormat3ReadBudget): Draft =
    require(plan != null && root != null && !root.isClosed, "exact original actor prepare/root required")
    root.reserveRetainedMemory(192L, "retained original prepare witness draft")
    new Draft(Prepare(plan), root)

  def draft(plan: PlannedCertifiedHandoff, root: MySqlFormat3ReadBudget): Draft =
    require(plan != null && root != null && !root.isClosed && (plan.source.root eq root),
      "exact original actor HANDOFF plan/root required")
    root.reserveRetainedMemory(192L, "retained original Certified HANDOFF witness draft")
    new Draft(CertifiedHandoff(plan), root)

  /** 只能由实际 actor 在 requireCertifyPlan 后调用；这里不从 DATA 自行发行资格。 */
  def materialize(draft: Draft): MySqlFull20OriginalWitness =
    require(draft != null && !draft.attempted, "original witness draft is one-shot")
    draft.attempted = true
    val plan = draft.plan
    val root = draft.root
    require(plan != null && root != null && !root.isClosed, "exact live original witness plan/root required")
    val before = plan.before.format3Snapshot.getOrElse(
      throw new IllegalStateException("original before full20 payload is missing"))
    val target = plan.target.format3Snapshot.getOrElse(
      throw new IllegalStateException("original target full20 payload is missing"))
    require((before.limitProfile eq root.profile) && (target.limitProfile eq root.profile) &&
      before.allocationBudget.sharesAllocationLedger(root) && target.allocationBudget.sharesAllocationLedger(root),
      "original before/target must remain on the same actual actor root/profile")
    // 两个 canonical arrays 与输入 typed history 同时保留，不能在 native publish 前释放 child。
    draft.beforePayload = MySqlFormat3SnapshotCodec.encode(before, root)
    draft.targetPayload = MySqlFormat3SnapshotCodec.encode(target, root)
    val beforePayload = draft.beforePayload
    val targetPayload = draft.targetPayload
    val intentLength = plan.byteLength
    root.reserveRetainedMemory(Math.addExact(intentLength.toLong, 64L), "original witness V4 context defensive bytes")
    draft.intent = plan.copyBytes()
    val intent = draft.intent
    root.reserveRetainedMemory(161L, "original witness canonical profile defensive bytes")
    draft.profile = root.profile.canonicalBytes()
    val profile = draft.profile
    require(profile.length == 97 && intent.length == intentLength, "fixed original framing changed")
    root.reserveRetainedMemory(256L, "original witness final holder")
    val binary = MySqlFull20WitnessCodec.encodeParts(profile, intent, beforePayload, targetPayload, root)
    draft.completed = new MySqlFull20OriginalWitness(plan, root, beforePayload, targetPayload,
      binary, intent, profile)
    draft.completed

/** 由原 holder 一次创建的 worker-only传输；不是 SQL/native 权威，原根保留所有活引用。 */
final class MySqlFull20NativeTransfer private[mysql] (
    private var witness: MySqlFull20OriginalWitness, private val owner: Thread):
  private var transport: Array[Byte] = null
  private var workspaceReserved = false
  private var workspaceReturned = false
  private var ack: WindowsB1Full20WitnessStore.NativeArtifactDurability = null
  private var coldBytes: Array[Byte] = null
  private var coldStarted = false
  private var coldReturned = false
  private var coldComparison: MySqlFull20WitnessData = null
  private var coldDecodeDraft: MySqlFull20WitnessCodec.DecodeDraft = null
  private var v5Reserved = false
  def reserveV5Publication(): Unit =
    requireOpen(); require(!v5Reserved, "V5 native record holder is one-shot")
    witness.originalRoot.reserveTemporaryMemory(10L * 65536L + 200704L,
      "V5 fixed record encode/decode/native/readback simultaneously live bounded workspaces")
    v5Reserved = true
  private[mysql] def completeColdComparison(): MySqlFull20WitnessData =
    requireOpen(); require(coldReturned && coldBytes != null && coldComparison == null, "actual cold native read required")
    coldDecodeDraft = MySqlFull20WitnessCodec.draft(coldBytes, witness.originalRoot.profile, witness.originalRoot)
    coldComparison = MySqlFull20WitnessCodec.decode(coldDecodeDraft)
    require(coldComparison.exactlyMatches(witness), "complete cold witness differs from original before/target/context")
    coldComparison
  private var referenceReadInProgress = false
  private var referenceReadWorkspace = 0L
  private var referenceReadLength = 0L
  def beginReferenceRevalidation(): Unit = beginReferenceRevalidation(witness.payload.length.toLong)
  def beginReferenceRevalidation(actualPayloadLength: Long): Unit =
    requireOpen(); requireCompleteColdComparison()
    require(!referenceReadInProgress && actualPayloadLength > 0L && actualPayloadLength <= artifactLimit(),
      "one exact bounded native reference read cannot overlap")
    val workspace = Math.addExact(Math.multiplyExact(actualPayloadLength, 3L), 200704L)
    witness.originalRoot.reserveTemporaryMemory(workspace, "native reference full read and arena before record mutation")
    referenceReadWorkspace = workspace
    referenceReadLength = actualPayloadLength
    referenceReadInProgress = true
  def referenceRevalidationReturned(): Unit = referenceRevalidationReturned(referenceReadLength)
  def referenceRevalidationReturned(actualPayloadLength: Long): Unit =
    requireOpen()
    val expected = Math.addExact(Math.multiplyExact(actualPayloadLength, 3L), 200704L)
    require(referenceReadInProgress && referenceReadWorkspace == expected,
      "actual exact-length reference revalidation workspace required")
    witness.originalRoot.releaseTemporaryMemory(referenceReadWorkspace, "actual reference handle/arena closed and local bytes dropped")
    referenceReadWorkspace = 0L
    referenceReadLength = 0L
    referenceReadInProgress = false
  private var v5ColdDecodeReserved = false
  private var v5CanonicalCopyIssued = false
  private var v5HistoryRetirementWorkspace = 0L
  private var v5HistoryRetirementsRemaining = 0
  private val v5HistoryRetirementBytesPerRecord = 6L * 260L + 8192L
  /** Freeze both private IDDATA records and their bounded IO workspace before the first CREATE_NEW. */
  def reserveV5HistoryRetirements(recordCount: Int, encodedLength: Int): Unit =
    requireOpen()
    require(v5Reserved && recordCount == 2 && encodedLength == 260 &&
      v5HistoryRetirementWorkspace == 0L && v5HistoryRetirementsRemaining == 0,
      "exact two-record canonical V5 history IDDATA workset required")
    val workspace = Math.multiplyExact(v5HistoryRetirementBytesPerRecord, recordCount.toLong)
    witness.originalRoot.reserveTemporaryMemory(workspace,
      "two V5 history IDDATA encode/create/readback/reopen workspaces before first mutation")
    try witness.originalRoot.reserveRetainedMemory(Math.multiplyExact(encodedLength.toLong + 768L, recordCount.toLong),
      "two V5 history identity and receipt holders retained under the original actor ledger")
    catch
      case cause: Throwable =>
        witness.originalRoot.releaseTemporaryMemory(workspace,
          "two-record V5 history reservation failed before native mutation")
        throw cause
    v5HistoryRetirementWorkspace = workspace
    v5HistoryRetirementsRemaining = recordCount

  /** Return one fixed workspace only after same-held close and history reopen/check/close ACKs. */
  def v5HistoryRetirementReturned(): Unit =
    requireOpen()
    require(v5HistoryRetirementWorkspace >= v5HistoryRetirementBytesPerRecord &&
      v5HistoryRetirementsRemaining > 0, "exact retained V5 history workset required")
    witness.originalRoot.releaseTemporaryMemory(v5HistoryRetirementBytesPerRecord,
      "one V5 history rename held-handle and checked reopen lifecycle completed")
    v5HistoryRetirementWorkspace = Math.subtractExact(v5HistoryRetirementWorkspace, v5HistoryRetirementBytesPerRecord)
    v5HistoryRetirementsRemaining = Math.subtractExact(v5HistoryRetirementsRemaining, 1)

  def copyV5CanonicalBytes(value: Array[Byte]): Array[Byte] =
    requireOpen()
    require(v5Reserved && !v5CanonicalCopyIssued && value != null && value.nonEmpty && value.length <= 65536,
      "one complete bounded V5 native DATA canonical copy required")
    witness.originalRoot.reserveRetainedMemory(Math.addExact(value.length.toLong,64L),
      "actual V5 native DATA canonical bytes retained before copy")
    v5CanonicalCopyIssued = true
    value.clone()

  def reserveV5ColdDecode(length: Int): Unit =
    requireOpen(); require(v5Reserved && !v5ColdDecodeReserved && length > 0 && length <= 65536, "exact V5 cold decode once required")
    witness.originalRoot.reserveRetainedMemory(Math.addExact(Math.multiplyExact(length.toLong, 8L), 16384L),
      "V5 cold DATA record arrays/strings/reference holders")
    v5ColdDecodeReserved = true
  def requireCompleteColdComparison(): Unit =
    requireOpen(); require(coldComparison != null && coldComparison.exactlyMatches(witness),
      "actual complete cold comparison is required; DATA flag cannot self-approve")
  private val workspaceBytes = Math.addExact(Math.multiplyExact(witness.payload.length.toLong, 3L), 200704L)
  private var storeReserved = false
  def reserveNativeStore(): Unit =
    requireOpen(); require(!storeReserved, "native witness store is one-shot")
    witness.originalRoot.reserveRetainedMemory(1032L, "原native store/pending/inventory与consumer phase新增字段在new前预付")
    storeReserved = true
  def requireSameOriginalRoot(other: MySqlFull20NativeTransfer): Unit =
    requireOpen()
    require(other != null, "actual original witness transfer required")
    other.requireOpen()
    require((witness.originalRoot eq other.witness.originalRoot) && (owner eq other.owner),
      "all record artifact references must retain the exact same actor root/thread")
  def requireSameOriginalRoot(accounting: MySqlFull20ColdReadAccounting): Unit =
    requireOpen()
    require(accounting != null, "actual original source accounting required")
    accounting.requireOpen()
    require(witness.originalRoot eq accounting.originalRoot,
      "historical artifact and ordinary transfer must share the exact original actor root")
  def requireOpen(): Unit =
    if witness == null then throw new IllegalStateException("original witness transfer consumers ended")
    require((Thread.currentThread() eq owner) && !witness.originalRoot.isClosed,
      "exact retained witness actor root/thread required")
  private[mysql] def prepare(): Unit =
    requireOpen()
    witness.originalRoot.reserveTemporaryMemory(workspaceBytes, "native arena/two readbacks/strict header simultaneous workspace")
    workspaceReserved = true
    transport = witness.payload.copyBytes(witness.originalRoot, "native original immutable transport")
  def ownedPayload(): Array[Byte] =
    requireOpen(); require(transport != null && workspaceReserved, "fully reserved native transfer required")
    transport // 仅 exact worker store 消费；不向 Main reply 暴露。
  def profileSha256(): Array[Byte] =
    requireOpen(); witness.originalRoot.profile.sha256()
  def beginColdRead(): Unit =
    requireOpen(); require(workspaceReturned && !coldStarted, "cold native read is one-shot after actual publication")
    witness.originalRoot.reserveTemporaryMemory(workspaceBytes, "cold native readbacks/arena simultaneous workspace")
    coldStarted = true
  def retainColdBytes(bytes: Array[Byte]): Unit =
    requireOpen(); require(coldStarted && coldBytes == null && bytes != null, "exact one-shot cold bytes required")
    coldBytes = bytes
  def coldNativeReturned(): Unit =
    requireOpen(); require(coldStarted && coldBytes != null && !coldReturned, "actual cold native read/checked close required")
    witness.originalRoot.releaseTemporaryMemory(workspaceBytes - (coldBytes.length.toLong + 128L),
      "cold arena/comparison dropped; actual cold payload stays charged")
    coldReturned = true
  def artifactLimit(): Long =
    requireOpen(); witness.originalRoot.profile.maxNativeArtifactBytes()
  def originalRecord(): B1ResumeRecord =
    requireOpen(); witness.originalPlan.record
  def nativeReturned(value: WindowsB1Full20WitnessStore.NativeArtifactDurability): Unit =
    requireOpen()
    require(value != null && value.originalTransfer() == this && !workspaceReturned, "exact native ACK/once workspace return required")
    ack = value // ACK 自身完整 readback bytes 继续 retained，不能因 native-return 清零根。
    // 原预留中只退实际已关闭 arena/已丢弃比较 buffers；ACK readback及其 holder仍在原根计费。
    val stillHeld = Math.addExact(witness.payload.length.toLong, 128L)
    witness.originalRoot.releaseTemporaryMemory(Math.subtractExact(workspaceBytes, stillHeld),
      "actual native arenas closed and temporary comparison refs dropped; ACK debt retained")
    workspaceReturned = true
  private[mysql] def originalWitness: MySqlFull20OriginalWitness = witness

  def requireOriginalTerminalIdentity(actor: MySqlCertifiedRecoveryInspectionActor,
      state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): Unit =
    if (Thread.currentThread() ne owner) || witness == null || (witness.originalRoot ne state.originalRoot) then
      throw new IllegalStateException("original live transfer/root terminal identity required")
    actor.requireOriginalTerminalTransfer(this,state)
    if !workspaceReturned || referenceReadInProgress || referenceReadWorkspace != 0L ||
        (coldStarted && !coldReturned) then
      throw new IllegalStateException("original transfer native/readback window remains active")

  def requireOriginalTerminalAcknowledgement(actor: MySqlCertifiedRecoveryInspectionActor,
      state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement,
      actual: WindowsB1Full20WitnessStore.NativeArtifactDurability): Unit =
    requireOriginalTerminalIdentity(actor,state)
    if actual == null || (ack ne actual) then throw new IllegalStateException("exact originally returned Full20 ACK required")

  private[mysql] def detachOrdinaryTerminal(actor: MySqlCertifiedRecoveryInspectionActor,
      state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): Unit =
    requireOriginalTerminalIdentity(actor,state)
    transport=null;ack=null;coldBytes=null;coldComparison=null;coldDecodeDraft=null;witness=null
