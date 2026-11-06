package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.B1ResumeRecordCanonical
import com.wi.floatingpod.server.WindowsB1TwoSlotStore
import java.util.Arrays

/** Java原生cold reader消费的窄预算能力；仅实际恢复actor能构造，无ACK/SQL/Ready权。 */
final class MySqlFull20ColdReadAccounting private[mysql] (
    private var actor: MySqlCertifiedRecoveryInspectionActor,
    private[mysql] var originalRoot: MySqlFormat3ReadBudget,
    private var parent: Option[MySqlFull20ColdReadAccounting]
):
  private var attempted = false
  private var nativeReturned = false
  private var v5DecodeAttempted = false
  private var witnessDecodeAttempted = false
  private var expectedLength = 0L
  private var scratch = 0L
  private var v4Reads = 0
  private var selectedCopyReserved = false
  private[mysql] var selectedSourceRecord: com.wi.floatingpod.server.B1ResumeRecord = null
  private[mysql] var retainedBytes: Array[Byte] = null
  private[mysql] var witnessDraft: MySqlFull20WitnessCodec.DecodeDraft = null
  private[mysql] var witness: MySqlFull20WitnessData = null
  private[mysql] var matchedAttempt: com.wi.floatingpod.server.B1ResumeRecord.AttemptAssertions = null
  private[mysql] var failure: Throwable = null

  def requireOpen(): Unit =
    if actor == null then throw new IllegalStateException("original accounting consumers ended")
    actor.requireAccounting(this, originalRoot)
    if failure != null then throw failure

  def requireRegisteredLookupParent(): Unit =
    requireOpen()
    require(parent.isEmpty, "exact registered lookup parent accounting is required")
    actor.requireRegisteredLookupParent(this, originalRoot)

  def profileSha256(): String =
    requireOpen()
    originalRoot.profile.sha256Hex()

  def issueVerifiedV5ArtifactSource(reference: com.wi.floatingpod.server.B1Full20WitnessReference,
      attempt: com.wi.floatingpod.server.B1ResumeRecord.AttemptAssertions,
      identity: com.wi.floatingpod.server.WindowsMarkerIo.FileIdentity): MySqlCertifiedRecoveryInspectionActor.VerifiedV5ArtifactSource =
    requireOpen()
    require(parent.isEmpty && reference != null && attempt != null && identity != null,
      "actual registered parent/accounting V5 reference issuer required")
    actor.issueVerifiedV5ArtifactSource(reference, attempt, identity)

  def artifactLimit(): Long =
    requireOpen()
    originalRoot.profile.maxNativeArtifactBytes()

  /** 固定最多六个真实OPEN：两V5slots+四个被引用artifact；父本身不执行读。 */
  def newReadAccounting(): MySqlFull20ColdReadAccounting =
    requireOpen()
    require(parent.isEmpty && !attempted, "only the registered lookup parent issues fixed read slots")
    actor.issueReadAccounting(this)

  private var v5History: com.wi.floatingpod.server.WindowsB1V5HistoricalLookup = null
  private var v5HistoryLimit = 0L
  private var v5HistoryReads = 0L
  private var v5HistoryContainers = 0L
  private var v5HistoryAttempts = 0L
  private var v5HistoryReserved = false
  private var v5HistoryPass = false
  private var v5HistoryScratch = 0L

  def beginV5HistoryPass(lookup: com.wi.floatingpod.server.WindowsB1V5HistoricalLookup, worldLength: Int): Unit = guarded {
    require(parent.isEmpty && v5HistoryReserved && !v5HistoryPass && lookup != null &&
      (v5History == null || (v5History eq lookup)), "one original lexical V5 history source pass required")
    lookup.requireAccounting(this)
    v5History = lookup
    v5HistoryScratch = MySqlFull20ColdReadAccounting.reserveInventoryWorkspace(originalRoot, worldLength)
    v5HistoryPass = true
  }

  def requireV5HistoryPass(lookup: com.wi.floatingpod.server.WindowsB1V5HistoricalLookup): Unit =
    requireV5History(lookup)
    require(v5HistoryPass, "prepaid actual V5 historical native source pass required")

  def finishV5HistoryPass(lookup: com.wi.floatingpod.server.WindowsB1V5HistoricalLookup): Unit = guarded {
    requireV5HistoryPass(lookup)
    originalRoot.releaseTemporaryMemory(v5HistoryScratch, "historical source pass native scopes and handles returned")
    v5HistoryScratch = 0L
    v5HistoryPass = false
  }

  /** Separate actual history lane; the existing six cold readers remain unchanged. */
  def reserveV5HistoryHolder(containers: Long): Unit = guarded {
    require(parent.isEmpty && !v5HistoryReserved && containers > 0L && containers <= originalRoot.profile.maxMembers(),
      "bounded original V5 history container count required once")
    // At most 3 files/container + 4 unique artifacts/container, each read twice
    // (initial and consumption). Fixed V4 copies and registry precede construction.
    v5HistoryLimit = MySqlFull20ColdReadAccounting.reserveV5HistoryWorkspace(originalRoot, containers)
    v5HistoryContainers = containers
    v5HistoryReserved = true
  }

  def reserveV5HistoryAttempt(lookup: com.wi.floatingpod.server.WindowsB1V5HistoricalLookup): Unit = guarded {
    requireV5HistoryPass(lookup)
    val next = Math.incrementExact(v5HistoryAttempts)
    MySqlFull20ColdReadAccounting.requireV5HistoryMembers(originalRoot, v5HistoryContainers, next)
    originalRoot.reserveRetainedMemory(1024L, "V5 unique historical artifact/attempt index before insertion")
    v5HistoryAttempts = next
  }

  def reserveV5HistoryResult(lookup: com.wi.floatingpod.server.WindowsB1V5HistoricalLookup, count: Int): Unit = guarded {
    requireV5History(lookup)
    require(!v5HistoryPass && count > 0 && count.toLong == v5HistoryContainers,
      "complete original V5 history result count required")
    originalRoot.reserveRetainedMemory(Math.addExact(512L, Math.multiplyExact(count.toLong, 32L)),
      "immutable consumed V5 history result before allocation")
  }

  def newV5HistoryReadAccounting(lookup: com.wi.floatingpod.server.WindowsB1V5HistoricalLookup): MySqlFull20ColdReadAccounting = guarded {
    require(parent.isEmpty && v5HistoryReserved && v5HistoryPass && lookup != null && v5HistoryReads < v5HistoryLimit,
      "actual bounded original V5 history read required")
    lookup.requireAccounting(this)
    require(v5History == null || (v5History eq lookup), "V5 historical issuer differs")
    v5History = lookup
    v5HistoryReads = Math.incrementExact(v5HistoryReads)
    // OPEN/path/metadata occur before payload length is known. Keep this charge
    // with the retained read; beforeRead separately accounts complete arrays.
    originalRoot.reserveRetainedMemory(204800L, "V5 history read wrapper and bounded pre-OPEN native workspace")
    actor.issueV5HistoryReadAccounting(this, lookup)
  }

  private[mysql] def requireV5History(lookup: com.wi.floatingpod.server.WindowsB1V5HistoricalLookup): Unit =
    requireOpen()
    require(parent.isEmpty && (v5History eq lookup) && v5HistoryReserved, "registered original V5 history required")
    lookup.requireAccounting(this)

  def failV5History(lookup: com.wi.floatingpod.server.WindowsB1V5HistoricalLookup, cause: Throwable): Unit =
    require(parent.isEmpty && (v5History eq lookup) && cause != null, "original historical failure required")
    failure = cause
    actor.recordFailure(cause)

  /** V4复读事实和codec副本仍由原cold scope持有；没有提前credit。 */
  def beforeV4Read(length: Int): Unit = guarded {
    require(parent.isEmpty && length > 0 && length <= 65536 && v4Reads < 10,
      "registered cold parent accepts at most ten exact bounded V4 reads")
    originalRoot.reserveRetainedMemory(Math.addExact(Math.multiplyExact(length.toLong, 12L), 200704L),
      "V4 first/second arrays, codec facts and native workspace before allocation")
    v4Reads += 1
  }

  def beforeSelectedV4Copy(length: Int): Unit = guarded {
    require(parent.isEmpty && !selectedCopyReserved && length > 0 && length <= 65536,
      "selected actual V4 canonical copy required once")
    originalRoot.reserveRetainedMemory(Math.addExact(length.toLong, 64L),
      "selected predecessor full canonical bytes before copy")
    selectedCopyReserved = true
  }

  def beforeRead(length: Long): Unit = guarded {
    require(parent.nonEmpty && !attempted && length > 0L && length <= Int.MaxValue.toLong &&
      length <= artifactLimit(), "exact bounded native read length required once")
    // firstArray保留；secondArray和native L+1+int count为临时。固定余量来源见API合同。
    originalRoot.reserveRetainedMemory(Math.addExact(length, 64L), "cold first payload retained before native allocation")
    scratch = Math.addExact(Math.multiplyExact(length, 2L), 200704L)
    originalRoot.reserveTemporaryMemory(scratch, "cold second read/native/path/metadata workspace before allocation")
    expectedLength = length
    attempted = true
  }

  def retainReadBytes(bytes: Array[Byte]): Unit = guarded {
    require(attempted && retainedBytes == null && bytes != null && bytes.length.toLong == expectedLength,
      "exact native first array must be retained once")
    retainedBytes = bytes // 无clone；真正native holder与actor同原预算保有此对象。
  }

  /** 只能在second已drop且Arena+exacthandle checked-close都成功后调用；失败不credit。 */
  def nativeReadClosed(): Unit = guarded {
    require(attempted && retainedBytes != null && !nativeReturned, "actual completed native read cleanup required once")
    originalRoot.releaseTemporaryMemory(scratch, "cold native/second workspace actually dropped")
    nativeReturned = true
  }

  def beforeV5Decode(length: Int): Unit = guarded {
    require(nativeReturned && retainedBytes != null && retainedBytes.length == length && !v5DecodeAttempted &&
      length > 0 && length <= 65536, "exact retained V5 record decode required once")
    originalRoot.reserveRetainedMemory(Math.addExact(Math.multiplyExact(length.toLong, 12L), 32768L),
      "retained V5 decoder/reencoder/attempt/reference structures before allocation")
    v5DecodeAttempted = true
  }

  def decodeWitness(bytes: Array[Byte], expectedAttemptCanonical: Array[Byte], selectedV4Canonical: Array[Byte]): Unit = guarded {
    require(nativeReturned && (bytes eq retainedBytes) && !witnessDecodeAttempted &&
      expectedAttemptCanonical != null && expectedAttemptCanonical.length > 0 && expectedAttemptCanonical.length <= 65536,
      "exact retained witness and selected-record full attempt framing required once")
    require(selectedV4Canonical != null && selectedV4Canonical.length > 0 && selectedV4Canonical.length <= 65536,
      "actual selected predecessor complete canonical bytes required")
    witnessDecodeAttempted = true
    originalRoot.reserveRetainedMemory(Math.addExact(Math.multiplyExact(selectedV4Canonical.length.toLong, 8L), 16384L),
      "selected original source DATA codec structures before allocation")
    selectedSourceRecord = B1ResumeRecordCanonical.decodeData(selectedV4Canonical)
    witnessDraft = MySqlFull20WitnessCodec.draft(bytes, originalRoot.profile, originalRoot)
    witness = MySqlFull20WitnessCodec.decode(witnessDraft)
    require(witnessDraft.beforeChild != null && witnessDraft.targetChild != null &&
      !witnessDraft.beforeChild.isClosed && !witnessDraft.targetChild.isClosed,
      "cold witness must retain its actual successful DATA child owners")
    originalRoot.reserveTemporaryMemory(262144L, "original source hash/receipt defensive comparison copies before allocation")
    var sourcePrimary: Throwable = null
    try
      require(Arrays.equals(witness.originalRecord.sourceAnchorSha256(), selectedSourceRecord.sourceAnchorSha256()) &&
        Arrays.equals(witness.originalRecord.originalReceipt(), selectedSourceRecord.originalReceipt()) &&
        witness.originalRecord.barrierNamespace() == selectedSourceRecord.barrierNamespace(),
        "cold original source anchor/receipt/namespace differs from selected predecessor")
    catch
      case cause: Throwable =>
        sourcePrimary = cause
        throw cause
    finally
      try originalRoot.releaseTemporaryMemory(262144L, "source comparison defensive copies no longer referenced")
      catch
        case cleanup: Throwable =>
          if sourcePrimary != null && (cleanup ne sourcePrimary) then sourcePrimary.addSuppressed(cleanup)
          else if sourcePrimary == null then throw cleanup
    val assertions = witness.originalRecord.assertions()
    originalRoot.reserveRetainedMemory(512L, "four-role original attempt selection before allocation")
    originalRoot.reserveTemporaryMemory(65536L, "one full canonical attempt encoding before allocation")
    var actual: Array[Byte] = null
    var primary: Throwable = null
    var found = false
    try
      val candidates = Array(assertions.originalPrepareWitness(), assertions.pendingAttempt(),
        if assertions.latestSettlement() == null then null else assertions.latestSettlement().settledAttempt(),
        assertions.acknowledgedCertifyWitness())
      var index = 0
      while index < candidates.length && !found do
        val candidate = candidates(index)
        if candidate != null then
          actual = B1ResumeRecordCanonical.encodeAttemptData(candidate)
          if Arrays.equals(actual, expectedAttemptCanonical) then
            matchedAttempt = candidate
            found = true
          Arrays.fill(actual, 0.toByte)
          actual = null
        index += 1
      require(found, "cold original complete B1 record lacks the exact selected historical attempt")
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      if actual != null then Arrays.fill(actual, 0.toByte)
      actual = null
      try originalRoot.releaseTemporaryMemory(65536L, "original attempt scratch dropped")
      catch
        case cleanup: Throwable =>
          if primary != null && (cleanup ne primary) then primary.addSuppressed(cleanup)
          else if primary == null then throw cleanup
  }

  private def guarded[A](body: => A): A =
    requireOpen()
    try body
    catch
      case cause: Throwable =>
        failure = cause
        try actor.recordFailure(cause)
        catch
          case secondary: Throwable =>
            if secondary ne cause then cause.addSuppressed(secondary)
        throw cause
  private var inventoryOwner: Object = null
  private var inventoryGeneration = 0L
  private var inventoryActive = false
  private var inventoryScratch = 0L
  private var historicalPermit: WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit = null
  private var historicalCapacity = 0
  private var historicalScratch = 0L
  private var historicalActive = false
  private var historicalFailure: Throwable = null
  private var historicalAttemptScratchActive = false
  private var historicalReadChildrenIssued = 0

  /** METHOD4 fixed formula；path length只能由同lookup的私有source读出，无caller scratchBytes。 */
  def beginInventoryPass(lookup: Object, capturedWorldLength: Int): Long =
    requireOpen()
    require(parent.isEmpty && lookup != null && !inventoryActive && capturedWorldLength >= 49,
      "registered parent and one lexical inventory pass required")
    require(inventoryOwner == null || (inventoryOwner eq lookup), "exact original inventory lookup required")
    val temporary = MySqlFull20ColdReadAccounting.reserveInventoryWorkspace(originalRoot, capturedWorldLength)
    inventoryOwner = lookup
    inventoryGeneration = Math.addExact(inventoryGeneration, 1L)
    inventoryScratch = temporary
    inventoryActive = true
    inventoryGeneration

  def requireInventoryPass(lookup: Object, generation: Long): Unit =
    requireOpen()
    require(parent.isEmpty && inventoryActive && (inventoryOwner eq lookup) && generation == inventoryGeneration,
      "exact live lexical inventory generation required")

  /** native/guard调用已返回、所有pass-owned临时引用实际drop后才调用；不释放原read/facts。 */
  def finishInventoryPass(lookup: Object, generation: Long): Unit =
    requireInventoryPass(lookup, generation)
    originalRoot.releaseTemporaryMemory(inventoryScratch, "complete inventory scratch dropped after checked cleanup")
    inventoryScratch = 0L
    inventoryActive = false

  def failInventoryPass(lookup: Object, generation: Long, primary: Throwable): Unit =
    requireInventoryPass(lookup, generation)
    require(primary != null, "actual inventory failure required")
    failure = primary
    actor.recordFailure(primary)
    // 保active generation及完整temporary款；不开放下一pass，不把checked-close等同所有refs消失。

  /** Actual V4 attempts are DATA already retained by the source; reserve wrappers before retaining references. */
  def reserveHistoricalExpected(attemptCount: Int): Unit = guarded {
    require(parent.isEmpty && !historicalActive && attemptCount > 0 && attemptCount <= 4,
      "registered parent and bounded actual historical witness set required")
    originalRoot.reserveRetainedMemory(Math.addExact(512L, Math.multiplyExact(attemptCount.toLong, 256L)),
      "historical expected-attempt registry before retaining source assertions")
  }

  /** One extra bounded reader belongs only to the actual V4 locator permit; it does not consume one of six V5 cold slots. */
  def newHistoricalReadAccounting(permit: WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit): MySqlFull20ColdReadAccounting =
    requireOpen()
    try
      require(parent.isEmpty && permit != null && (historicalPermit eq permit) && historicalActive &&
        historicalFailure == null && historicalReadChildrenIssued < permit.expectedAttemptCount(),
        "actual historical V4 permit and bounded child reader slot required")
      val child = actor.issueHistoricalReadAccounting(this, permit)
      historicalReadChildrenIssued = Math.addExact(historicalReadChildrenIssued, 1)
      child
    catch
      case cause: Throwable =>
        failure = cause
        actor.recordFailure(cause)
        throw cause

  def decodeHistoricalWitness(bytes: Array[Byte], expectedAttempt: com.wi.floatingpod.server.B1ResumeRecord.AttemptAssertions,
      selectedV4Canonical: Array[Byte]): Unit =
    requireOpen()
    require(parent.nonEmpty && expectedAttempt != null, "actual V4 historical child and exact attempt required")
    originalRoot.reserveTemporaryMemory(65536L, "one complete expected historical attempt encoding before allocation")
    var expected: Array[Byte] = null
    var primary: Throwable = null
    try
      expected = B1ResumeRecordCanonical.encodeAttemptData(expectedAttempt)
      decodeWitness(bytes, expected, selectedV4Canonical)
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      if expected != null then Arrays.fill(expected, 0.toByte)
      expected = null
      try originalRoot.releaseTemporaryMemory(65536L, "expected historical attempt encoding dropped")
      catch
        case cleanup: Throwable =>
          if primary != null && (cleanup ne primary) then primary.addSuppressed(cleanup)
          else if primary == null then throw cleanup

  def decodedHistoricalWitness(): MySqlFull20WitnessData =
    requireOpen()
    require(parent.nonEmpty && witnessDecodeAttempted && witness != null, "actual decoded historical Full20 DATA required")
    witness
  def matchedWitnessAttempt(): com.wi.floatingpod.server.B1ResumeRecord.AttemptAssertions =
    requireOpen()
    require(witnessDecodeAttempted && witness != null && matchedAttempt != null,
      "exact complete B1 attempt whose canonical bytes matched this artifact is required")
    matchedAttempt
  def retainedHistoricalBytes(): Array[Byte] =
    requireOpen()
    require(parent.nonEmpty && nativeReturned && retainedBytes != null && witnessDecodeAttempted && witness != null,
      "actual historical bytes must remain retained after native read and strict decode")
    retainedBytes

  /** Canonical attempt bytes are transient and may include the complete existing 64 KiB record bound. */
  def reserveHistoricalAttemptCompare(): Unit = guarded {
    require(parent.isEmpty && !historicalAttemptScratchActive, "one exact historical attempt comparison at a time")
    originalRoot.reserveTemporaryMemory(131072L, "two complete canonical attempt encodings before allocation")
    historicalAttemptScratchActive = true
  }

  def finishHistoricalAttemptCompare(): Unit = guarded {
    require(parent.isEmpty && historicalAttemptScratchActive, "actual historical attempt comparison cleanup required")
    originalRoot.releaseTemporaryMemory(131072L, "actual historical attempt encodings dropped")
    historicalAttemptScratchActive = false
  }

  /** A later exact same-source reference read is prepaid on the original child/root, not a new cold slot. */
  private var v5PreinstallPassActive = false
  private var v5PreinstallPassScratch = 0L
  private var v5PreinstallContainersRead = 0
  private var v5PreinstallContainerScratch = 0L
  private var v5PreinstallContainerLength = 0L
  /** A separately paid, bounded two-slot source rebarrier; this does not create cold reader slots. */
  def beginV5PreinstallPass(capturedWorldLength: Int): Unit = guarded {
    require(parent.isEmpty && !v5PreinstallPassActive && capturedWorldLength >= 49,
      "registered actor root and one exact V5 preinstall source pass required")
    val scratch = MySqlFull20ColdReadAccounting.reserveInventoryWorkspace(originalRoot,capturedWorldLength)
    v5PreinstallPassScratch = scratch
    v5PreinstallPassActive = true
    v5PreinstallContainersRead = 0
  }
  def copyV5PreinstallBytes(value: Array[Byte]): Array[Byte] = guarded {
    require(parent.isEmpty && v5PreinstallPassActive && value != null && value.nonEmpty && value.length <= 65536,
      "Actual bounded V5 source bytes required for a charged defensive copy")
    originalRoot.reserveRetainedMemory(Math.addExact(value.length.toLong,64L),
      "V5 source canonical bytes copied into the actor capability before allocation")
    value.clone()
  }
  def copyCompletedV5SourceBytes(value: Array[Byte]): Array[Byte] = guarded[Array[Byte]] {
    require(parent.isEmpty && v5PreinstallPassCompleted && !v5PreinstallPassActive &&
      v5PreinstallPassScratch == 0L && v5PreinstallContainerScratch == 0L &&
      value != null && value.nonEmpty && value.length <= 65536,
      "completed bounded V5 source pass required for a charged defensive copy")
    originalRoot.reserveRetainedMemory(Math.addExact(value.length.toLong, 64L),
      "completed V5 source canonical bytes copied into the actor capability before allocation")
    value.clone()
  }
  def beforeV5PreinstallContainerRead(length: Long): Long = guarded {
    require(parent.isEmpty && v5PreinstallPassActive && v5PreinstallContainerScratch == 0L &&
      v5PreinstallContainersRead < 2 && length > 0L && length <= 65536L && length <= artifactLimit(),
      "exact fixed V5 container and bounded native workspace required before payload allocation")
    originalRoot.reserveRetainedMemory(Math.addExact(Math.multiplyExact(length,12L),32768L),
      "actual preinstall V5 complete container and retained decode facts before read")
    val scratch = Math.addExact(Math.multiplyExact(length,2L),200704L)
    originalRoot.reserveTemporaryMemory(scratch,
      "preinstall V5 second container read and native/path workspace before allocation")
    v5PreinstallContainerLength = length
    v5PreinstallContainerScratch = scratch
    scratch
  }
  def completeV5PreinstallContainerRead(length: Long,scratch: Long): Unit = guarded {
    require(parent.isEmpty && v5PreinstallPassActive && v5PreinstallContainerScratch == scratch &&
      scratch > 0L && v5PreinstallContainerLength == length,
      "exact V5 source payload handle and second read must be closed before temporary credit")
    originalRoot.releaseTemporaryMemory(scratch,"preinstall V5 second read/native workspace actually dropped")
    v5PreinstallContainerScratch = 0L
    v5PreinstallContainerLength = 0L
    v5PreinstallContainersRead += 1
  }
  private var v5PreinstallPassCompleted = false
  private var v5ArtifactReferenceScratch = 0L
  private var v5ArtifactReferenceLength = 0L

  /** One actual installed-custody complete-artifact rebarrier, without another cold-reader slot. */
  def beginV5ArtifactReferenceRead(length: Long): Long = guarded {
    require(parent.isEmpty && v5PreinstallPassCompleted && !v5PreinstallPassActive &&
      v5ArtifactReferenceScratch == 0L && length > 0L && length <= artifactLimit() && length <= Int.MaxValue.toLong,
      "exact settled V5 source artifact and completed preinstall pass required before actual installed rebarrier")
    val scratch = Math.addExact(Math.multiplyExact(length,3L),200704L)
    originalRoot.reserveTemporaryMemory(scratch,
      "three complete installed-custody full20 comparisons and native workspace before OPEN")
    v5ArtifactReferenceLength = length
    v5ArtifactReferenceScratch = scratch
    scratch
  }

  def finishV5ArtifactReferenceRead(length: Long,scratch: Long): Unit = guarded {
    require(parent.isEmpty && v5ArtifactReferenceScratch == scratch && scratch > 0L &&
      v5ArtifactReferenceLength == length, "actual installed full20 read/handle cleanup required before temporary credit")
    originalRoot.releaseTemporaryMemory(scratch,
      "installed source full20 comparison arrays/handle/local arena actually dropped")
    v5ArtifactReferenceScratch = 0L
    v5ArtifactReferenceLength = 0L
  }

  def failV5ArtifactReferenceRead(length: Long,scratch: Long,primary: Throwable): Unit = guarded {
    require(parent.isEmpty && primary != null && v5ArtifactReferenceScratch == scratch && scratch > 0L &&
      v5ArtifactReferenceLength == length, "actual failed installed full20 source read responsibility required")
    failure = primary
    actor.recordFailure(primary)
    // Keep original temporary credit and complete source bytes held by the actor after uncertain close/read failure.
  }

  def finishV5PreinstallPass(): Unit = guarded {
    require(parent.isEmpty && v5PreinstallPassActive && v5PreinstallContainerScratch == 0L &&
      v5PreinstallContainersRead <= 2,
      "complete fixed V5 source pass and all native handles/temporary payloads dropped")
    originalRoot.releaseTemporaryMemory(v5PreinstallPassScratch,"V5 preinstall source pass scratch actually dropped")
    v5PreinstallPassScratch = 0L
    v5PreinstallPassActive = false
    v5PreinstallPassCompleted = true
  }
  def failV5PreinstallPass(primary: Throwable): Unit =
    require(primary != null,"actual V5 preinstall source failure required")
    failure = primary
    actor.recordFailure(primary)
    // Keep the active pass and all fixed/read scratch reserved with the same source failure.

  private var historicalReferenceScratch = 0L
  private var historicalReferenceActive = false
  def beginHistoricalReferenceRead(length: Int): Unit = guarded {
    require(parent.nonEmpty && nativeReturned && witnessDecodeAttempted && retainedBytes != null &&
      retainedBytes.length == length && length > 0 && length.toLong <= artifactLimit() && !historicalReferenceActive,
      "exact decoded historical artifact and one non-overlapping reference read are required")
    historicalReferenceScratch = Math.addExact(Math.multiplyExact(length.toLong, 2L), 200704L)
    originalRoot.reserveTemporaryMemory(historicalReferenceScratch,
      "same-source historical artifact first/second full read and native workspace before OPEN")
    historicalReferenceActive = true
  }
  def finishHistoricalReferenceRead(): Unit = guarded {
    require(parent.nonEmpty && historicalReferenceActive && historicalReferenceScratch > 0L,
      "actual successful historical reference handle cleanup required")
    originalRoot.releaseTemporaryMemory(historicalReferenceScratch,
      "historical reference read arrays/arena dropped after checked close")
    historicalReferenceScratch = 0L
    historicalReferenceActive = false
  }
  def failHistoricalReferenceRead(primary: Throwable): Unit =
    if primary != null then
      if failure == null then failure = primary
      else if failure ne primary then failure.addSuppressed(primary)
      actor.recordFailure(primary)

  /** Prepay both bounded enumeration passes before directory identity OPEN or FindFirstFileW. */
  def beginHistoricalDiscovery(permit: WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit,
      requiredAttempts: Int, capturedDirectoryLength: Int): Int = guarded {
    require(parent.isEmpty && permit != null && requiredAttempts > 0 && requiredAttempts <= 4 &&
      capturedDirectoryLength >= 3 && !historicalActive && historicalPermit == null,
      "one actual V4 historical discovery permit and finite attempt set required")
    historicalPermit = permit
    historicalActive = true
    val retainedAvailable = originalRoot.availableMemoryBytes
    val perCandidateRetained = 2L * (512L + 64L + 8L)
    val fixedRetained = Math.addExact(8192L, Math.multiplyExact(capturedDirectoryLength.toLong + 8L, 4L))
    val metadataCapacity = if retainedAvailable <= fixedRetained then 0L
      else (retainedAvailable - fixedRetained) / perCandidateRetained
    val profileCapacity = Math.min(64L, originalRoot.profile.maxMembers())
    val capacity = Math.toIntExact(Math.min(profileCapacity, metadataCapacity))
    require(capacity >= requiredAttempts,
      "actual retained-memory budget cannot prepay every required historical candidate")
    val names = Math.addExact(fixedRetained,
      Math.multiplyExact(capacity.toLong, perCandidateRetained))
    originalRoot.reserveRetainedMemory(names,
      "both bounded historical candidate name/metadata arrays before enumeration")
    historicalCapacity = capacity
    historicalScratch = Math.multiplyExact(2L,
      Math.addExact(2048L, Math.multiplyExact(capturedDirectoryLength.toLong + 8L, 2L)))
    originalRoot.reserveTemporaryMemory(historicalScratch,
      "both bounded FindHandle/data/Arena/native pattern/path workspaces before first OPEN")
    capacity
  }

  def requireHistoricalDiscovery(permit: WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit): Unit =
    requireOpen()
    require(parent.isEmpty && permit != null && (historicalPermit eq permit) && historicalActive &&
      historicalFailure == null && historicalCapacity > 0,
      "exact live V4 historical discovery accounting is required")

  def finishHistoricalDiscovery(permit: WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit): Unit = guarded {
    require(parent.isEmpty && (historicalPermit eq permit) && historicalActive && historicalFailure == null &&
      historicalScratch > 0L, "exact successful two-pass historical discovery required")
    originalRoot.releaseTemporaryMemory(historicalScratch,
      "both native search sequences and their path/data arenas actually closed")
    historicalScratch = 0L
    historicalActive = false
  }

  def failHistoricalDiscovery(permit: WindowsB1TwoSlotStore.Full20HistoricalDiscoveryPermit,
      primary: Throwable): Unit =
    require(parent.isEmpty && (historicalPermit eq permit) && primary != null,
      "exact original historical discovery failure required")
    if historicalFailure == null then historicalFailure = primary
    else if historicalFailure ne primary then historicalFailure.addSuppressed(primary)
    failure = primary
    actor.recordFailure(primary)

  /** 只核原登记，不返回DATA、不重新取得普通读取资格。 */
  def requireOriginalTerminalIdentity(actual: MySqlCertifiedRecoveryInspectionActor,
      state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): Unit =
    if actor == null || (actor ne actual) then throw new IllegalStateException("original terminal accounting actor required")
    actor.requireTerminalAccounting(this,state)
    if failure != null then throw failure
    if inventoryActive || historicalActive || historicalAttemptScratchActive || v5HistoryPass ||
        v5PreinstallPassActive || v5PreinstallContainerScratch != 0L || v5ArtifactReferenceScratch != 0L ||
        historicalReferenceActive || (attempted && !nativeReturned) then
      throw new IllegalStateException("actual accounting native/read/compare window remains active")
    if historicalFailure != null then throw historicalFailure

  private[mysql] def endOrdinaryPayload(actual: MySqlCertifiedRecoveryInspectionActor,
      state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): Unit =
    requireOriginalTerminalIdentity(actual,state)
    selectedSourceRecord=null;retainedBytes=null;witnessDraft=null;witness=null;matchedAttempt=null
    v5History=null;inventoryOwner=null;historicalPermit=null

  private[mysql] def detachOrdinaryTerminal(actual: MySqlCertifiedRecoveryInspectionActor,
      state: MySqlCertifiedRecoveryInspectionActor.OrdinaryBorrowRetirement): Unit =
    requireOriginalTerminalIdentity(actual,state)
    // scratch的旧数值不是活窗口；nativeReturned才证明原read临时费已经正常返回。
    selectedSourceRecord=null;retainedBytes=null;witnessDraft=null;witness=null;matchedAttempt=null
    v5History=null;inventoryOwner=null;historicalPermit=null
    parent=None;originalRoot=null;actor=null

private[mysql] object MySqlFull20ColdReadAccounting:
  private[mysql] def requireV5HistoryMembers(root: MySqlFormat3ReadBudget, containers: Long, attempts: Long): Unit =
    require(root != null && !root.isClosed && !root.isScopedReadBudget && containers > 0L && attempts >= 0L &&
      Math.addExact(containers, attempts) <= root.profile.maxMembers(), "combined V5 container/attempt member budget exceeded")

  private[mysql] def reserveV5HistoryWorkspace(root: MySqlFormat3ReadBudget, containers: Long): Long =
    requireV5HistoryMembers(root, containers, 0L)
    val reads = Math.addExact(Math.multiplyExact(containers, 14L), 16L)
    root.reserveRetainedMemory(Math.addExact(1114112L, Math.multiplyExact(reads, 1024L)),
      "V5 history holder, V4 defensive copies and bounded read registry before construction")
    reads

  /** 生产入口共用的DATA计费seam；本函数不构造actor/accounting/inspection/ACK。 */
  private[mysql] def reserveInventoryWorkspace(root: MySqlFormat3ReadBudget, capturedWorldLength: Int): Long =
    require(root != null && !root.isClosed && !root.isScopedReadBudget && capturedWorldLength >= 49,
      "original open workflow root and captured native path bound required")
    val components = Math.addExact(1L, (capturedWorldLength.toLong - 49L) / 2L)
    val reservation = Math.addExact(4032346L, Math.addExact(Math.multiplyExact(96L, components), 256L))
    // draft与ACK是真正两compound；caller可保已退役DATA，不以retire声称这两个对象不可达。
    root.reserveRetainedMemory(512L, "inventory draft and generation DATA acknowledgment before allocation")
    val temporary = Math.subtractExact(reservation, 512L)
    root.reserveTemporaryMemory(temporary, "METHOD4 complete borrowed guard/stream inventory scratch before OPEN")
    temporary
