package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.DbFirstBootRecallRequestCustody
import com.wi.floatingpod.domain.RecallRefusalReason
import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 测原生产Main gate，不把metadata fixture当作SQL或恢复完成资格。 */
class DbFirstBootRecallRequestCustodyTest:
  private def record(phase:MySqlDbFirstRecallRequestPhase):MySqlDbFirstRecallRequestRecord =
    def tag()=IoOperationTag(UUID.randomUUID(),1L,1L,UUID.randomUUID())
    new MySqlDbFirstRecallRequestRecord(tag(),tag(),UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),7L,
      Some(UUID.randomUUID()),"a"*64,phase)

  @Test def everyPersistedPhaseRemainsUnresolvedWithoutActualBootLifecycleOwner():Unit =
    for phase<-MySqlDbFirstRecallRequestPhase.values do
      val original=record(phase);val records=Vector(original)
      val custody=new DbFirstBootRecallRequestCustody(Thread.currentThread(),records)
      assertSame(records,custody.originalRequests);assertSame(original,custody.originalRequests.head)
      assertEquals(1,custody.unresolvedCount)
      assertThrows(classOf[IllegalArgumentException],()=>custody.requireSettled())
      assertEquals(phase,custody.originalRequests.head.phase)

  @Test def completeCensusRetainsAllOriginalRequestsIncludingUncorrelatedLegacy():Unit =
    val original=record(MySqlDbFirstRecallRequestPhase.JournalCommitted)
    val legacy=new MySqlDbFirstRecallRequestRecord(original.journalTag,original.originalTag,original.bindingUuid,
      original.actorUuid,original.podUuid,original.expectedStateVersion,None,original.journalRequestSha256,original.phase)
    val records=MySqlDbFirstRecallRequestPhase.values.toVector.map(record):+legacy
    val custody=new DbFirstBootRecallRequestCustody(Thread.currentThread(),records)
    assertEquals(6,custody.unresolvedCount);assertSame(legacy,custody.originalRequests.last)
    assertTrue(custody.originalRequests.last.clientRequestUuid.isEmpty)
    assertThrows(classOf[IllegalArgumentException],()=>custody.requireSettled())

  @Test def emptyCompleteCensusAllowsGateButMissingCensusDoesNot():Unit =
    val custody=new DbFirstBootRecallRequestCustody(Thread.currentThread(),Vector.empty)
    assertEquals(0,custody.unresolvedCount);custody.requireSettled()
    assertThrows(classOf[IllegalArgumentException],()=>new DbFirstBootRecallRequestCustody(Thread.currentThread(),null))
    assertThrows(classOf[IllegalArgumentException],()=>new DbFirstBootRecallRequestCustody(Thread.currentThread(),Vector(null)))

  @Test def foreignThreadCannotObserveOrBypassOriginalMainGate():Unit =
    val custody=new DbFirstBootRecallRequestCustody(Thread.currentThread(),Vector(record(MySqlDbFirstRecallRequestPhase.RecallCommitted)))
    val cause=new AtomicReference[Throwable]()
    val thread=new Thread(()=>{
      try custody.requireSettled()
      catch case actual:Throwable=>cause.set(actual)
    })
    thread.start();thread.join();assertInstanceOf(classOf[IllegalArgumentException],cause.get())
    assertEquals("requirement failed: live original Boot request Main required",cause.get().getMessage)
    assertEquals(1,custody.unresolvedCount)

  private def reasoned(binding:UUID,client:Option[UUID]=Some(UUID.randomUUID())):MySqlDbFirstRecallRequestRecord =
    val original=record(MySqlDbFirstRecallRequestPhase.RefusedCommitted)
    new MySqlDbFirstRecallRequestRecord(original.journalTag,original.originalTag,binding,original.actorUuid,
      original.podUuid,original.expectedStateVersion,client,original.journalRequestSha256,original.phase,Some(RecallRefusalReason.Busy))

  @Test def completeReasonedRefusalCohortMayEnterActualRestoreButDoesNotGrantReady():Unit =
    val binding=UUID.randomUUID();val records=Vector(reasoned(binding),reasoned(binding,None))
    val custody=new DbFirstBootRecallRequestCustody(Thread.currentThread(),records)
    assertTrue(custody.mayRestoreResults(binding));assertEquals(2,custody.unresolvedCount)
    assertThrows(classOf[IllegalArgumentException],()=>custody.requireSettled())
    assertSame(records,custody.originalRequests)
    assertFalse(custody.mayRestoreResults(UUID.randomUUID()))

  @Test def pendingPhaseLegacyReasonOrForeignBindingCannotEnterTerminalResultRestoration():Unit =
    val binding=UUID.randomUUID();val accepted=reasoned(binding)
    for phase<-MySqlDbFirstRecallRequestPhase.values do
      val other=record(phase) // V1/refused phase alone has no detailed reason.
      val sameBinding=new MySqlDbFirstRecallRequestRecord(other.journalTag,other.originalTag,binding,other.actorUuid,
        other.podUuid,other.expectedStateVersion,other.clientRequestUuid,other.journalRequestSha256,phase,None)
      val custody=new DbFirstBootRecallRequestCustody(Thread.currentThread(),Vector(accepted,sameBinding))
      assertEquals(phase==MySqlDbFirstRecallRequestPhase.RecallCommitted,custody.mayRestoreResults(binding))
      assertEquals(2,custody.unresolvedCount)
      assertThrows(classOf[IllegalArgumentException],()=>custody.requireSettled())
    val foreign=new DbFirstBootRecallRequestCustody(Thread.currentThread(),Vector(accepted,reasoned(UUID.randomUUID())))
    assertFalse(foreign.mayRestoreResults(binding))

  @Test def restorationSelectionRequiresActualBindingEvenForEmptyCensus():Unit =
    val custody=new DbFirstBootRecallRequestCustody(Thread.currentThread(),Vector.empty)
    assertThrows(classOf[IllegalArgumentException],()=>custody.mayRestoreResults(null))
    assertThrows(classOf[IllegalArgumentException],()=>custody.mayRestoreResults(new UUID(0L,0L)))
    assertTrue(custody.mayRestoreResults(UUID.randomUUID()));custody.requireSettled()

  @Test def ordinaryJavaMetadataPassCannotSettleWithoutActualIssuer():Unit =
    val binding=UUID.randomUUID();val records=Vector(reasoned(binding))
    val custody=new DbFirstBootRecallRequestCustody(Thread.currentThread(),records)
    com.wi.floatingpod.server.BootRefusalJavaCaller.assertUnissuedPassRejected(custody,records,binding)
    assertEquals(1,custody.unresolvedCount)
    assertThrows(classOf[IllegalArgumentException],()=>custody.requireSettled())
    assertSame(records,custody.originalRequests)

  private def committed(binding:UUID,client:Option[UUID]=Some(UUID.randomUUID())):MySqlDbFirstRecallRequestRecord =
    val original=record(MySqlDbFirstRecallRequestPhase.RecallCommitted)
    new MySqlDbFirstRecallRequestRecord(original.journalTag,original.originalTag,binding,original.actorUuid,
      original.podUuid,original.expectedStateVersion,client,original.journalRequestSha256,original.phase,None)

  @Test def committedAndReasonedRefusalMayShareActualRestoreWithoutSettlingFromMetadata():Unit =
    val binding=UUID.randomUUID()
    for records<-Vector(Vector(committed(binding)),Vector(committed(binding,None),reasoned(binding))) do
      val custody=new DbFirstBootRecallRequestCustody(Thread.currentThread(),records)
      assertTrue(custody.mayRestoreResults(binding))
      assertEquals(records.size,custody.unresolvedCount)
      assertThrows(classOf[IllegalArgumentException],()=>custody.requireSettled())
      com.wi.floatingpod.server.BootRefusalJavaCaller.assertUnissuedPassRejected(custody,records,binding)
      assertEquals(records.size,custody.unresolvedCount)
      assertSame(records,custody.originalRequests)

  @Test def committedCohortCannotHidePendingForeignOrMalformedReasonMetadata():Unit =
    val binding=UUID.randomUUID();val accepted=committed(binding)
    val malformed=new MySqlDbFirstRecallRequestRecord(accepted.journalTag,accepted.originalTag,binding,accepted.actorUuid,
      accepted.podUuid,accepted.expectedStateVersion,accepted.clientRequestUuid,accepted.journalRequestSha256,
      accepted.phase,Some(RecallRefusalReason.Busy))
    val legacy=record(MySqlDbFirstRecallRequestPhase.RefusedCommitted)
    for other<-Vector(record(MySqlDbFirstRecallRequestPhase.JournalCommitted),
        record(MySqlDbFirstRecallRequestPhase.OriginalCReserved),record(MySqlDbFirstRecallRequestPhase.OriginalCCancelled),
        committed(UUID.randomUUID()),malformed,legacy) do
      val custody=new DbFirstBootRecallRequestCustody(Thread.currentThread(),Vector(accepted,other))
      assertFalse(custody.mayRestoreResults(binding));assertEquals(2,custody.unresolvedCount)
      assertThrows(classOf[IllegalArgumentException],()=>custody.requireSettled())

  @Test def completeTerminalAndMultipleJournalCohortSelectsPodRestoreWithoutSettlingAnyOriginal():Unit =
    val binding=UUID.randomUUID()
    def journal():MySqlDbFirstRecallRequestRecord =
      val source=record(MySqlDbFirstRecallRequestPhase.JournalCommitted)
      new MySqlDbFirstRecallRequestRecord(source.journalTag,source.originalTag,binding,source.actorUuid,
        source.podUuid,source.expectedStateVersion,source.clientRequestUuid,source.journalRequestSha256,source.phase)
    val records=Vector(committed(binding),reasoned(binding),journal(),journal())
    val custody=new DbFirstBootRecallRequestCustody(Thread.currentThread(),records)
    assertTrue(custody.selectsPodRestoration(binding))
    assertFalse(custody.mayRestoreResults(binding),"J-only never enters old terminal pass")
    assertEquals(4,custody.unresolvedCount);assertSame(records,custody.originalRequests)
    assertThrows(classOf[IllegalArgumentException],()=>custody.requireSettled())
    com.wi.floatingpod.server.BootRefusalJavaCaller.assertUnissuedPassRejected(custody,records,binding)
    assertEquals(4,custody.unresolvedCount,"selection or forged result pass cannot settle J-only")

  @Test def podRestorationSelectionRejectsReservedCancelledMissingReasonMalformedJournalAndForeignBinding():Unit =
    val binding=UUID.randomUUID();val terminal=committed(binding)
    def sameBinding(phase:MySqlDbFirstRecallRequestPhase,reason:Option[RecallRefusalReason]=None)=
      new MySqlDbFirstRecallRequestRecord(terminal.journalTag,terminal.originalTag,binding,terminal.actorUuid,
        terminal.podUuid,terminal.expectedStateVersion,terminal.clientRequestUuid,terminal.journalRequestSha256,phase,reason)
    for other<-Vector(sameBinding(MySqlDbFirstRecallRequestPhase.OriginalCReserved),
        sameBinding(MySqlDbFirstRecallRequestPhase.OriginalCCancelled),
        sameBinding(MySqlDbFirstRecallRequestPhase.RefusedCommitted),
        sameBinding(MySqlDbFirstRecallRequestPhase.JournalCommitted,Some(RecallRefusalReason.Busy)),
        sameBinding(MySqlDbFirstRecallRequestPhase.RecallCommitted,Some(RecallRefusalReason.Busy)),
        committed(UUID.randomUUID())) do
      val records=Vector(terminal,other);val custody=new DbFirstBootRecallRequestCustody(Thread.currentThread(),records)
      assertFalse(custody.selectsPodRestoration(binding));assertEquals(2,custody.unresolvedCount)
      assertSame(records,custody.originalRequests)
      assertThrows(classOf[IllegalArgumentException],()=>custody.requireSettled())
    val empty=new DbFirstBootRecallRequestCustody(Thread.currentThread(),Vector.empty)
    assertThrows(classOf[IllegalArgumentException],()=>empty.selectsPodRestoration(null))
    assertThrows(classOf[IllegalArgumentException],()=>empty.selectsPodRestoration(new UUID(0L,0L)))
    assertTrue(empty.selectsPodRestoration(binding));empty.requireSettled()

  @Test def independentBootCensusChargeSurvivesOrdinaryDataChildRetirementUntilWorkerRetiresHolder():Unit =
    val source=MainBootProjectionFixture.snapshot()
    val root=source.allocationBudget
    val baseline=root.accountedMemoryBytes
    val records=Vector(record(MySqlDbFirstRecallRequestPhase.RecallCommitted))
    val child=root.newCanonicalDataScope()
    val withChild=root.accountedMemoryBytes
    assertTrue(withChild>baseline)
    child.reserveRetainedMemory(4096L,"old Main DATA metadata fixture")
    val holder=MySqlDbFirstBootRecallCensus.retain(root,Thread.currentThread(),records)
    val retained=MySqlDbFirstBootRecallCensus.retainedBytes(records.size)
    assertEquals(withChild+4096L+retained,root.accountedMemoryBytes)
    child.closeScopedReadBudget() // 与实际superseded backing相同的退款原语。
    assertEquals(baseline+retained,root.accountedMemoryBytes)
    assertEquals(1,records.size)
    assertThrows(classOf[IllegalArgumentException],()=>holder.claim(null))
    assertThrows(classOf[IllegalArgumentException],()=>holder.detach(null))
    assertEquals(baseline+retained,root.accountedMemoryBytes)
    holder.retireOnWorker()
    assertEquals(baseline,root.accountedMemoryBytes)
    assertThrows(classOf[IllegalArgumentException],()=>holder.retireOnWorker())
    root.closeRootReadBudget()

  @Test def independentBootCensusRejectsWrongWorkerAndDoesNotRefundOnForeignRetirement():Unit =
    val source=MainBootProjectionFixture.snapshot();val root=source.allocationBudget
    val records=Vector(record(MySqlDbFirstRecallRequestPhase.JournalCommitted))
    assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstBootRecallCensus.retain(root,new Thread(),records))
    val holder=MySqlDbFirstBootRecallCensus.retain(root,Thread.currentThread(),records)
    val charged=root.accountedMemoryBytes
    val result=new AtomicReference[Throwable]()
    val foreign=new Thread(()=>
      try holder.retireOnWorker()
      catch case cause:Throwable=>result.set(cause))
    foreign.start();foreign.join()
    assertInstanceOf(classOf[IllegalArgumentException],result.get())
    assertEquals(charged,root.accountedMemoryBytes)
    holder.retireOnWorker();root.closeRootReadBudget()

  @Test def successorResultComparisonPreservesOriginalIdentityAndDoesNotSettleFromPhase():Unit =
    val original=record(MySqlDbFirstRecallRequestPhase.JournalCommitted)
    val reason=Some(RecallRefusalReason.Busy)
    def result(journal:IoOperationTag=original.journalTag,operation:IoOperationTag=original.originalTag,
        binding:UUID=original.bindingUuid,actor:UUID=original.actorUuid,pod:UUID=original.podUuid,
        version:Long=original.expectedStateVersion,client:Option[UUID]=original.clientRequestUuid,
        digest:String=original.journalRequestSha256,
        phase:MySqlDbFirstRecallRequestPhase=MySqlDbFirstRecallRequestPhase.RefusedCommitted,
        why:Option[RecallRefusalReason]=reason)=
      new MySqlDbFirstRecallRequestRecord(journal,operation,binding,actor,pod,version,client,digest,phase,why)
    val current=result()
    assertTrue(DbFirstBootRecallRequestCustody.matchesResult(original,current,current.phase,reason))
    for altered<-Vector(result(journal=original.originalTag),result(operation=original.journalTag),
        result(binding=UUID.randomUUID()),result(actor=UUID.randomUUID()),result(pod=UUID.randomUUID()),
        result(version=original.expectedStateVersion+1),result(client=None),result(digest="b"*64),
        result(phase=MySqlDbFirstRecallRequestPhase.JournalCommitted),result(why=None),
        result(why=Some(RecallRefusalReason.VersionMismatch))) do
      assertFalse(DbFirstBootRecallRequestCustody.matchesResult(original,altered,current.phase,reason))
    val custody=new DbFirstBootRecallRequestCustody(Thread.currentThread(),Vector(original))
    com.wi.floatingpod.server.BootRefusalJavaCaller.assertUnissuedSettlementRejected(custody,original,RecallRefusalReason.Busy)
    assertEquals(1,custody.unresolvedCount)
    assertSame(original,custody.originalRequests.head)
    assertEquals(MySqlDbFirstRecallRequestPhase.JournalCommitted,original.phase)
    assertThrows(classOf[IllegalArgumentException],()=>custody.requireSettled())

  @Test def initialTerminalOutcomesKeepCommittedReasonEmptyAndCannotSettleMixedCohortFromMetadata():Unit =
    val binding=UUID.randomUUID()
    val accepted=committed(binding,None);val refused=reasoned(binding)
    val source=record(MySqlDbFirstRecallRequestPhase.JournalCommitted)
    val journal=new MySqlDbFirstRecallRequestRecord(source.journalTag,source.originalTag,binding,source.actorUuid,
      source.podUuid,source.expectedStateVersion,source.clientRequestUuid,source.journalRequestSha256,source.phase)
    val records=Vector(accepted,refused,journal)
    val custody=new DbFirstBootRecallRequestCustody(Thread.currentThread(),records)
    assertEquals(Some((MySqlDbFirstRecallRequestPhase.RecallCommitted,None)),
      DbFirstBootRecallRequestCustody.terminalResult(accepted))
    assertEquals(Some((MySqlDbFirstRecallRequestPhase.RefusedCommitted,Some(RecallRefusalReason.Busy))),
      DbFirstBootRecallRequestCustody.terminalResult(refused))
    assertTrue(DbFirstBootRecallRequestCustody.terminalResult(journal).isEmpty)
    assertTrue(custody.selectsPodRestoration(binding));assertFalse(custody.mayRestoreResults(binding))
    for original<-records do
      com.wi.floatingpod.server.BootRefusalJavaCaller.assertUnissuedRestoredTerminalRejected(custody,original)
      com.wi.floatingpod.server.BootRefusalJavaCaller.assertUnissuedCommittedRejected(custody,original)
    assertEquals(3,custody.unresolvedCount);assertSame(records,custody.originalRequests)
    assertEquals(MySqlDbFirstRecallRequestPhase.JournalCommitted,journal.phase)
    assertTrue(accepted.refusalReason.isEmpty)
    assertThrows(classOf[IllegalArgumentException],()=>custody.requireSettled())

  @Test def initialTerminalClassificationRejectsEveryPendingPhaseAndMalformedTerminalReason():Unit =
    val source=committed(UUID.randomUUID())
    for (phase,why)<-Vector(
        (MySqlDbFirstRecallRequestPhase.JournalCommitted,None),
        (MySqlDbFirstRecallRequestPhase.OriginalCReserved,None),
        (MySqlDbFirstRecallRequestPhase.OriginalCCancelled,None),
        (MySqlDbFirstRecallRequestPhase.RefusedCommitted,None),
        (MySqlDbFirstRecallRequestPhase.RecallCommitted,Some(RecallRefusalReason.Busy))) do
      val original=new MySqlDbFirstRecallRequestRecord(source.journalTag,source.originalTag,source.bindingUuid,
        source.actorUuid,source.podUuid,source.expectedStateVersion,source.clientRequestUuid,source.journalRequestSha256,phase,why)
      assertTrue(DbFirstBootRecallRequestCustody.terminalResult(original).isEmpty)
      val custody=new DbFirstBootRecallRequestCustody(Thread.currentThread(),Vector(original))
      com.wi.floatingpod.server.BootRefusalJavaCaller.assertUnissuedRestoredTerminalRejected(custody,original)
      assertEquals(1,custody.unresolvedCount)
    assertThrows(classOf[IllegalArgumentException],()=>DbFirstBootRecallRequestCustody.terminalResult(null))
