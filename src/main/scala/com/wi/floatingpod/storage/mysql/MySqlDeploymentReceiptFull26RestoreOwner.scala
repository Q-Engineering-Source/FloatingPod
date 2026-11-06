package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{B1Full26WitnessReference,B1ResumeRecord,B1ResumeRecordCanonical,WindowsB1V6WitnessRecordStore}
import java.sql.SQLDataException
import java.util.Arrays
import scala.jdk.CollectionConverters.*

/** cold body 的实际 owner；所有历史引用都读回并解码，不从摘要合成图或执行资格。 */
private[mysql] final class MySqlDeploymentReceiptFull26RestoreOwner private (
    private val actor:MySqlDeploymentReceiptBootstrapActor,
    private val inputs:MySqlScriptSchemaInputs,
    private val root:MySqlFormat3ReadBudget
):
  private val thread=Thread.currentThread()
  private var attempted=false
  private var primary:Throwable=null
  private var bodies:List[(B1Full26WitnessReference,MySqlFull26WitnessCodec.Owner)]=Nil
  private var inventory:WindowsB1V6WitnessRecordStore.CurrentInventory=null
  private var pendingInventory:WindowsB1V6WitnessRecordStore.CurrentInventory=null
  private var currentRecord:com.wi.floatingpod.server.B1ResumeRecordV6=null
  private var retirementAttempted=false
  private var continuationProof:MySqlDeploymentReceiptFull26RestoreOwner.OriginalOperationContinuation5=null
  private var continuationHistory=Vector.empty[MySqlNormalRecallRecovery.ReceiptHistoryData]
  private var continuationFee=0L
  private var continuationBefore:MySqlScriptSchemaSnapshot=null
  private var continuationAck:WindowsB1V6WitnessRecordStore.DurableV6Ack=null
  private var continuationHistoryComplete=false
  private def requireOwner():Unit =
    actor.requireCurrent()
    root.requireScriptDataInputs(inputs)
    if Thread.currentThread() ne thread then throw new IllegalStateException("exact Full26 restore worker required")
    if primary!=null then throw primary
  private def requireCurrentImage(image:MySqlScriptSchemaSnapshot):Unit =
    if image==null || (image.inputs ne inputs) || image.allocationBudget.isClosed ||
        (!(image.allocationBudget eq root) && !image.allocationBudget.isCanonicalChildOf(root)) ||
        (image.allocationBudget.profile ne root.profile) then
      throw new SQLDataException("actual current Full26 image must belong to the original Session root/inputs/profile")
    root.requireInitializedObservation(image.schema,image.connection,image.observationScope,image.maxAllowedPacketBytes)

  /** 只从同一实际V6完整历史与已校验body发行；调用方不能从DATA assertions自行构造。 */
  private[mysql] def historicalCertifyProof(record:B1ResumeRecord,
      attempt:B1ResumeRecord.AttemptAssertions):MySqlDeploymentReceiptFull26RestoreOwner.HistoricalCertifyProof5 =
    requireOwner()
    if inventory==null || record==null || attempt==null || attempt.phase()!=B1ResumeRecord.Phase.CERTIFY then
      throw new SQLDataException("actual retained inventory and original CERTIFY attempt required")
    inventory.requireRegisteredCurrent()
    val records=inventory.records()
    val temporary=records.asScala.foldLeft(Math.addExact(256L,Math.multiplyExact(records.size().toLong,128L))) { (fee,ack)=>
      Math.addExact(fee,Math.multiplyExact(ack.retainedCanonicalByteLength().toLong,4L))
    }
    root.reserveTemporaryMemory(temporary,"original complete historical CERTIFY proof workspace before indexing")
    var failure:Throwable=null
    try issueHistoricalProof(record,attempt,records)
    catch
      case cause:Throwable=>failure=cause;throw cause
    finally
      try root.releaseTemporaryMemory(temporary,"original historical CERTIFY proof workspace dropped")
      catch
        case cleanup:Throwable=>
          if failure==null then throw cleanup
          else MySqlOwnedIoAdapter.suppressExitCleanup(failure,cleanup)

  private def issueHistoricalProof(record:B1ResumeRecord,attempt:B1ResumeRecord.AttemptAssertions,
      records:java.util.List[WindowsB1V6WitnessRecordStore.DurableV6Ack]):MySqlDeploymentReceiptFull26RestoreOwner.HistoricalCertifyProof5 =
    val history=records.asScala.filter(_.record().transition().sequence()<record.sequence())
    def confirmed(phase:B1ResumeRecord.Phase)=history.filter { ack=>
      val settlement=ack.record().transition().assertions().latestSettlement()
      settlement!=null && settlement.kind()==B1ResumeRecord.SettlementKind.CONFIRMED && settlement.settledAttempt().phase()==phase
    }
    val prepares=confirmed(B1ResumeRecord.Phase.PREPARE).map(_.record().references().settled()).filter(_!=null)
      .groupBy(_.attemptId()).values.map(_.head).toVector
    if prepares.size!=1 then throw new SQLDataException("historical CERTIFY needs the unique actually committed PREPARE")
    val handoff=confirmed(B1ResumeRecord.Phase.HANDOFF).lastOption.getOrElse(
      throw new SQLDataException("historical CERTIFY needs a preceding confirmed HANDOFF"))
    val handoffReference=handoff.record().references().settled()
    def retained(reference:B1Full26WitnessReference):MySqlFull26WitnessCodec.Owner =
      bodies.find(_._1.exactlyEquals(reference)).map(_._2).getOrElse(
        throw new SQLDataException("historical CERTIFY proof body is not retained by the original restore owner"))
    val prepareBody=retained(prepares.head)
    val handoffBody=retained(handoffReference)
    root.reserveRetainedMemory(256L,"original historical CERTIFY proof before allocation")
    historicalProofFee=Math.addExact(historicalProofFee,256L)
    val proof=new MySqlDeploymentReceiptFull26RestoreOwner.HistoricalCertifyProof5(this,record,attempt,prepares.head,
      prepareBody,handoffReference,handoffBody,root)
    historicalProofs=proof::historicalProofs
    proof

  private var historicalProofs:List[MySqlDeploymentReceiptFull26RestoreOwner.HistoricalCertifyProof5]=Nil
  private var historicalProofFee=0L
  private def requireProof(proof:MySqlDeploymentReceiptFull26RestoreOwner.HistoricalCertifyProof5):Unit =
    requireOwner()
    if !historicalProofs.exists(_ eq proof) then throw new IllegalStateException("original registered historical CERTIFY proof required")
    inventory.requireRegisteredCurrent()

  /** 只由原actor持有的完整native/V6/body/SQL历史联合发行；DATA相等本身不能取得此对象。 */
  private[mysql] def originalOperationContinuation(previous:WindowsB1V6WitnessRecordStore.DurableV6Ack,
      before:MySqlScriptSchemaSnapshot,writer:MySqlBusinessWriterRow,held:HeldSessionBarrier,
      native:MySqlDeploymentReceiptFreshNativeOwner):MySqlDeploymentReceiptFull26RestoreOwner.OriginalOperationContinuation5 =
    requireOwner();requireCurrentImage(before)
    if continuationProof!=null || previous==null || writer==null || held==null || native==null ||
        inventory==null || (previous.record() ne currentRecord) then
      throw new SQLDataException("original operation continuation requires the exact one-shot current restore inventory")
    inventory.requireRegisteredCurrent();previous.requireCurrent()
    val records=inventory.records()
    if records.size().toLong>root.profile.maxMembers() then throw new SQLDataException("original V6 history exceeds complete member budget")
    val workspace=records.asScala.foldLeft(Math.addExact(262144L,Math.multiplyExact(records.size().toLong,512L))) { (fee,ack)=>
      Math.addExact(fee,Math.multiplyExact(ack.retainedCanonicalByteLength().toLong,8L))
    }
    root.reserveTemporaryMemory(workspace,"original Full26 operation history/native defensive workspace before copies")
    try native.withSource { source=>
      source.validateLive()
      val original=currentRecord.transition();val state=original.assertions()
      val oldOwner=source.previousOwner()
      val retirement=source.previousOwnerRetirement()
      if state.pendingAttempt()!=null || state.acknowledgedCertifyWitness()==null || retirement==null ||
          !state.currentOwner().equals(oldOwner) || original.barrierNamespace()!=held.namespace.lockName() ||
          source.namespace()!=held.namespace.lockName() ||
          !Arrays.equals(original.originalReceipt(),source.originalReceipt()) ||
          !Arrays.equals(original.sourceAnchorSha256(),java.util.HexFormat.of().parseHex(source.anchorSha256())) ||
          writer.bindingUuid!=state.currentWriter().binding() || writer.writerEpoch!=state.currentWriter().epoch() ||
          writer.ownerBootUuid.exists(_!=oldOwner.boot()) || writer.leaseUntil.isAfter(writer.databaseNow) then
        throw new SQLDataException("advanced receipt lost actual original native source/retirement/epoch/expired W")
      root.reserveRetainedMemory(640L,"original one-shot Full26 operation continuation/history holder")
      continuationFee=640L
      continuationBefore=before;continuationAck=previous
      continuationProof=new MySqlDeploymentReceiptFull26RestoreOwner.OriginalOperationContinuation5(this,actor,
        previous,before,writer,source,root)
      val certifyReference=currentRecord.references().acknowledgedCertify()
      if certifyReference==null then throw new SQLDataException("advanced receipt lacks original complete CERTIFY reference")
      val certifyRecord=inventory.records().asScala.filter { ack=>
        val ref=ack.record().references().acknowledgedCertify()
        ref!=null && ref.exactlyEquals(certifyReference)
      }.sortBy(_.record().transition().sequence()).headOption.getOrElse(
        throw new SQLDataException("original complete CERTIFY V6 lineage missing"))
      val certSequence=certifyRecord.record().transition().sequence()
      val edges=inventory.records().asScala.filter { ack=>
        val settlement=ack.record().transition().assertions().latestSettlement()
        ack.record().transition().sequence()>certSequence && settlement!=null &&
          settlement.kind()==B1ResumeRecord.SettlementKind.CONFIRMED &&
          settlement.settledAttempt().phase()==B1ResumeRecord.Phase.HANDOFF
      }.groupBy(_.record().transition().assertions().latestSettlement().settledAttempt().attemptId())
        .values.map(_.minBy(_.record().transition().sequence())).toVector.sortBy(_.record().transition().sequence()).reverse
      var cursor=before
      var expectedNativeOwner=oldOwner
      val seen=scala.collection.mutable.HashSet.empty[(java.util.UUID,java.util.UUID)]
      def segment(anchor:MySqlScriptSchemaSnapshot):Unit =
        val history=MySqlNormalRecallRecovery.verifyCompleteReceiptHistoryData(before,cursor,anchor,root)
        history.newestFirst.foreach { receipt=>
          if !seen.add((receipt.context.bindingUuid,receipt.context.operationUuid)) then
            throw new SQLDataException("advanced Full26 history repeats an original operation")
          actor.requireHistoricalReceiptScope(receipt,held)
          receipt.recall.foreach { recall=>
            if recall.phase.header.source.sourceIdentity!=source.sourceWorldIdentity() ||
                recall.phase.header.source.worldIdentity!=source.sourceWorldIdentity() then
              throw new SQLDataException("advanced recall history changed actual original native world")
          }
        }
        root.reserveRetainedMemory(Math.addExact(256L,Math.multiplyExact(continuationHistory.size.toLong+1L,64L)),
          "original Full26 continuation history holder references")
        continuationHistory=continuationHistory :+ history
      def retained(reference:B1Full26WitnessReference):MySqlFull26WitnessCodec.Owner =
        bodies.find(_._1.exactlyEquals(reference)).map(_._2).getOrElse(
          throw new SQLDataException("advanced original V6 complete body is not retained"))
      edges.foreach { edge=>
        val ref=edge.record().references().settled()
        if ref==null then throw new SQLDataException("confirmed original HANDOFF has no complete body")
        retained(ref).withData { (originalEdge,edgeBefore,edgeTarget,_,_)=>
          val edgeState=originalEdge.assertions()
          if !edgeState.currentOwner().equals(expectedNativeOwner) || edgeState.previousOwner()==null ||
              edgeState.retirement()==null || originalEdge.barrierNamespace()!=original.barrierNamespace() ||
              !Arrays.equals(originalEdge.originalReceipt(),original.originalReceipt()) ||
              !Arrays.equals(originalEdge.sourceAnchorSha256(),original.sourceAnchorSha256()) then
            throw new SQLDataException("original native owners/source/retirement do not join across Full26 epoch edges")
          segment(edgeTarget);cursor=edgeBefore
          expectedNativeOwner=edgeState.previousOwner()
        }
      }
      retained(certifyReference).withData { (originalCertify,_,certified,_,_)=>
        if !originalCertify.assertions().currentOwner().equals(expectedNativeOwner) ||
            originalCertify.barrierNamespace()!=original.barrierNamespace() ||
            !Arrays.equals(originalCertify.originalReceipt(),original.originalReceipt()) ||
            !Arrays.equals(originalCertify.sourceAnchorSha256(),original.sourceAnchorSha256()) then
          throw new SQLDataException("original complete CERTIFY source/native owner does not join Full26 history")
        segment(certified)
      }
      val latest=MySqlNormalRecallRecovery.verifyLatestAnyReceiptData(before,writer,oldOwner.boot(),writer.writerEpoch,root)
      actor.requireHistoricalReceiptScope(latest,held)
      if !seen.contains((latest.context.bindingUuid,latest.context.operationUuid)) then
        throw new SQLDataException("actual latest W operation is absent from original complete V6 receipt history")
      continuationProof.complete(new B1ResumeRecord.ReceiptSlotAssertions(latest.context.bootUuid,
        latest.context.writerEpoch,latest.context.bindingUuid,latest.context.operationUuid,latest.receiptFingerprint.toArray))
      source.validateLive();inventory.requireRegisteredCurrent()
      continuationHistoryComplete=true
      continuationProof
    }
    catch
      case cause:Throwable=>
        primary=cause
        throw cause
    finally
      try root.releaseTemporaryMemory(workspace,"original Full26 operation history workspace dropped")
      catch case cleanup:Throwable=>
        if primary==null then { primary=cleanup;throw cleanup }
        else MySqlOwnedIoAdapter.suppressExitCleanup(primary,cleanup)

  /** 词法借原已验证 history；新 ACK 必须已由真实 advanceInventory 接入同一 store。 */
  private[mysql] def consumeContinuationHistory[A](actual:MySqlDeploymentReceiptBootstrapActor,
      current:WindowsB1V6WitnessRecordStore.DurableV6Ack,originalBefore:MySqlScriptSchemaSnapshot)
      (consume:Vector[MySqlNormalRecallRecovery.ReceiptHistoryData]=>A):A =
    requireOwner();requireCurrentImage(originalBefore)
    if (actual ne actor) || !continuationHistoryComplete || continuationProof==null ||
        (originalBefore ne continuationBefore) || continuationHistory.isEmpty || inventory==null ||
        current==null || (inventory.current() ne current) || (current.record() ne currentRecord) ||
        !inventory.records().asScala.exists(_ eq continuationAck) then
      throw new SQLDataException("original completed Full26 history/current advanced inventory required")
    inventory.requireRegisteredCurrent();current.requireCurrent()
    consume(continuationHistory)

  private def requireContinuation(proof:MySqlDeploymentReceiptFull26RestoreOwner.OriginalOperationContinuation5):Unit =
    requireOwner()
    if continuationProof ne proof then throw new SQLDataException("original registered Full26 operation continuation required")
    inventory.requireRegisteredCurrent()

  def restoreInventory(actual:WindowsB1V6WitnessRecordStore.CurrentInventory,
      native:MySqlDeploymentReceiptFreshNativeOwner):Unit =
    requireOwner()
    if attempted || actual==null || native==null then throw new IllegalStateException("original cold inventory restore is one-shot")
    attempted=true;inventory=actual
    consumeInventory(actual,native)

  /** 新ACK已真实发布后，只接同store历史中的原record引用；原partial bodies仍由此owner保留。 */
  private[mysql] def advanceInventory(actual:WindowsB1V6WitnessRecordStore.CurrentInventory,
      native:MySqlDeploymentReceiptFreshNativeOwner):Unit =
    requireOwner()
    if !attempted || currentRecord==null || actual==null || native==null || pendingInventory!=null then
      throw new IllegalStateException("original completed restore and next actual inventory required")
    pendingInventory=actual
    try
      actual.requireCurrent()
      val next=actual.current().record()
      if next.transition().sequence()<=currentRecord.transition().sequence() ||
          !actual.records().asScala.exists(ack=>ack.record() eq currentRecord) then
        throw new SQLDataException("next inventory does not extend the exact original retained V6 history")
      inventory=actual
      consumeInventory(actual,native)
      pendingInventory=null
    catch
      case cause:Throwable=>primary=cause;throw cause

  private def consumeInventory(actual:WindowsB1V6WitnessRecordStore.CurrentInventory,
      native:MySqlDeploymentReceiptFreshNativeOwner):Unit =
    try
      actual.requireCurrent()
      currentRecord=actual.current().record()
      actual.records().asScala.foreach { durable=>
        val temporary=Math.addExact(Math.multiplyExact(12L,durable.retainedCanonicalByteLength().toLong),32768L)
        root.reserveTemporaryMemory(temporary,"original V6 body assertions/semantic defensive workspace before copies")
        var failure:Throwable=null
        try
          restoreRecord(durable,native)
        catch
          case cause:Throwable=>failure=cause;throw cause
        finally
          try root.releaseTemporaryMemory(temporary,"original V6 body assertions workspace dropped")
          catch
            case cleanup:Throwable=>
              if failure==null then throw cleanup
              else MySqlOwnedIoAdapter.suppressExitCleanup(failure,cleanup)
      }
      actual.requireCurrent()
    catch
      case failure:Throwable=>
        primary=failure
        throw failure

  private def restoreRecord(durable:WindowsB1V6WitnessRecordStore.DurableV6Ack,
      native:MySqlDeploymentReceiptFreshNativeOwner):Unit =
        val record=durable.record()
        record.allReferences().asScala.foreach { reference=>
          bodies.find(_._1.exactlyEquals(reference)) match
            case Some((_,body))=>body.requireReference(reference)
            case None=>
              val body=MySqlFull26WitnessCodec.begin(inputs,root)
              // 原 partial owner 在任何 native read/body decode 前即挂入，不丢失失败槽。
              bodies=(reference,body)::bodies
              val expectedAttempt=Vector(Option(record.transition().assertions().pendingAttempt()),
                Option(record.transition().assertions().latestSettlement()).map(_.settledAttempt()),
                Option(record.transition().assertions().acknowledgedCertifyWitness())).flatten.find(_.attemptId()==reference.attemptId())
              val proof=expectedAttempt.filter { a=>
                val writer=a.beforeWriter();val slot=writer.lastSlot()
                a.phase()==B1ResumeRecord.Phase.CERTIFY && slot!=null &&
                  (slot.boot()!=writer.ownerBoot() || slot.writerEpoch()!=writer.epoch())
              }.map(a=>historicalCertifyProof(record.transition(),a))
              native.withArtifact(reference) { bytes=>body.decodeWithHistoricalCertify(bytes,proof) }
              body.requireReference(reference)
              body.withBootstrapSemanticData { (original,before,target,_,_)=>
                MySqlDeploymentReceiptFull26RestoreOwner.requireBootstrapBody(original,before,target,inputs,root)
              }
          val expected=Vector(Option(record.transition().assertions().originalPrepareWitness()),
            Option(record.transition().assertions().pendingAttempt()),
            Option(record.transition().assertions().latestSettlement()).map(_.settledAttempt()),
            Option(record.transition().assertions().acknowledgedCertifyWitness())).flatten
            .find(_.attemptId()==reference.attemptId()).getOrElse(throw new SQLDataException("retained Full26 reference lacks original attempt"))
          val body=bodies.find(_._1.exactlyEquals(reference)).get._2
          body.withData { (original,_,_,_,_)=>
            val assertion=original.assertions().pendingAttempt()
            if !MySqlDeploymentReceiptFull26RestoreOwner.sameAttempt(assertion,expected,original,record.transition()) then
              throw new SQLDataException("cold complete Full26 body attempt/source differs from retained V6 assertions")
          }
        }

  /** 只有 actual pending 需要 before/after 判定；两者皆非时保留 unresolved，禁止安装或提交。 */
  def requireResolvedCurrent(image:MySqlScriptSchemaSnapshot):Unit =
    currentPendingPosition(image)
    ()

  def currentPendingPosition(image:MySqlScriptSchemaSnapshot):Option[MySqlDeploymentReceiptFull26RestoreOwner.Position] =
    requireOwner()
    requireCurrentImage(image)
    if inventory==null then throw new IllegalStateException("complete original cold inventory required")
    inventory.requireRegisteredCurrent()
    val record=currentRecord
    Option(record.references().pending()).map { reference=>
      val body=bodies.find(_._1.exactlyEquals(reference)).getOrElse(throw new SQLDataException("pending original Full26 body missing"))._2
      var result:MySqlDeploymentReceiptFull26RestoreOwner.Position=null
      body.withData { (_,before,target,_,_)=>
        MySqlDeploymentReceiptFull26RestoreOwner.classify(image,before,target) match
          case MySqlDeploymentReceiptFull26RestoreOwner.Position.Unresolved=>
            throw new SQLDataException("schema5 original pending operation remains unresolved; complete current Full26 is neither before nor target")
          case actual=>result=actual
      }
      result
    }

  private[mysql] def requireCancelledPrepareBefore(image:MySqlScriptSchemaSnapshot):Unit =
    requireOwner()
    requireCurrentImage(image)
    if inventory==null || currentRecord==null then throw new IllegalStateException("original complete cancelled history required")
    inventory.requireRegisteredCurrent()
    val state=currentRecord.transition().assertions()
    val settlement=state.latestSettlement()
    val reference=currentRecord.references().settled()
    if currentRecord.transition().kind()!=B1ResumeRecord.Kind.ATTEMPT_CANCELLED || state.pendingAttempt()!=null ||
        settlement==null || settlement.kind()!=B1ResumeRecord.SettlementKind.CANCELLED ||
        settlement.settledAttempt().phase()!=B1ResumeRecord.Phase.PREPARE || reference==null then
      throw new SQLDataException("successor PREPARE requires the exact cancelled original PREPARE")
    val body=bodies.find(_._1.exactlyEquals(reference)).map(_._2).getOrElse(
      throw new SQLDataException("cancelled PREPARE original body is not retained"))
    body.withData { (_,before,target,_,_)=>
      if MySqlDeploymentReceiptFull26RestoreOwner.classify(image,before,target)!=MySqlDeploymentReceiptFull26RestoreOwner.Position.Before then
        throw new SQLDataException("successor PREPARE requires the exact locked original before, never target or unresolved")
    }

  private[mysql] def retireForTerminal(intent:MySqlDeploymentReceiptSession.TerminalIntent5):Unit =
    actor.requireTerminalHolder(intent)
    if retirementAttempted || primary!=null then throw new IllegalStateException("original Full26 restore retirement is one-shot",primary)
    retirementAttempted=true
    if continuationProof!=null then continuationProof.requireConsumedForRetirement()
    continuationHistory=Vector.empty
    var failure:Throwable=null
    bodies=bodies.filter { (_,body)=>
      try { body.close();false }
      catch case cause:Throwable=>
        if failure==null then failure=cause else MySqlOwnedIoAdapter.suppressExitCleanup(failure,cause)
        true
    }
    if failure!=null then
      primary=failure;throw failure
    root.releaseRetainedMemory(historicalProofFee,"original historical CERTIFY proofs retired")
    historicalProofFee=0L;historicalProofs=Nil;inventory=null;pendingInventory=null;currentRecord=null
    try
      root.releaseRetainedMemory(continuationFee,"original Full26 operation continuation retired")
      continuationFee=0L;continuationProof=null
    catch
      case cause:Throwable=>
        primary=cause
        throw cause
    root.releaseRetainedMemory(512L,"original Full26 restore holder retired")

private[mysql] object MySqlDeploymentReceiptFull26RestoreOwner:
  final class OriginalOperationContinuation5 private[MySqlDeploymentReceiptFull26RestoreOwner] (
      owner:MySqlDeploymentReceiptFull26RestoreOwner,actor:MySqlDeploymentReceiptBootstrapActor,
      original:WindowsB1V6WitnessRecordStore.DurableV6Ack,before:MySqlScriptSchemaSnapshot,
      writer:MySqlBusinessWriterRow,source:com.wi.floatingpod.server.WindowsWorldProvisioner.DeploymentReceiptIoScope,
      root:MySqlFormat3ReadBudget):
    private var bound:Option[(WindowsB1V6WitnessRecordStore.DurableV6Ack,
        MySqlScriptSchemaSnapshot,MySqlBusinessWriterRow,
        com.wi.floatingpod.server.WindowsWorldProvisioner.DeploymentReceiptIoScope)]=
      Some((original,before,writer,source))
    private var slot:Option[B1ResumeRecord.ReceiptSlotAssertions]=None
    private var consumed=false
    private[MySqlDeploymentReceiptFull26RestoreOwner] def requireConsumedForRetirement():Unit =
      if !consumed || bound.nonEmpty || slot.nonEmpty then
        throw new SQLDataException("unconsumed original Full26 operation continuation blocks retirement")
    private[MySqlDeploymentReceiptFull26RestoreOwner] def complete(actual:B1ResumeRecord.ReceiptSlotAssertions):Unit =
      owner.requireContinuation(this)
      if slot.nonEmpty || consumed || actual==null then throw new SQLDataException("original Full26 continuation cannot be completed twice")
      slot=Some(actual)
    private[mysql] def consume(actualActor:MySqlDeploymentReceiptBootstrapActor,
        actualOriginal:WindowsB1V6WitnessRecordStore.DurableV6Ack,actualBefore:MySqlScriptSchemaSnapshot,
        actualWriter:MySqlBusinessWriterRow,
        actualSource:com.wi.floatingpod.server.WindowsWorldProvisioner.DeploymentReceiptIoScope,
        actualRoot:MySqlFormat3ReadBudget):B1ResumeRecord.ReceiptSlotAssertions =
      owner.requireContinuation(this)
      val (ack,image,w,native)=bound.getOrElse(throw new SQLDataException("original Full26 continuation was consumed"))
      if consumed || slot.isEmpty || (actualActor ne actor) || (actualOriginal ne ack) ||
          (actualBefore ne image) || (actualWriter ne w) || (actualSource ne native) || (actualRoot ne root) then
        throw new SQLDataException("Full26 continuation differs from original actor/source/before/W/root")
      actualSource.validateLive();actualOriginal.requireCurrent()
      val result=slot.get
      if !MySqlOrdinaryHandoffGraph.originalSlotMatches(actualWriter,Some(result)) then
        throw new SQLDataException("Full26 original operation proof differs from actual W receipt")
      consumed=true;bound=None;slot=None
      result

  final class HistoricalCertifyProof5 private[MySqlDeploymentReceiptFull26RestoreOwner] (
      owner:MySqlDeploymentReceiptFull26RestoreOwner,original:B1ResumeRecord,expected:B1ResumeRecord.AttemptAssertions,
      prepareReference:B1Full26WitnessReference,prepare:MySqlFull26WitnessCodec.Owner,
      handoffReference:B1Full26WitnessReference,handoff:MySqlFull26WitnessCodec.Owner,
      root:MySqlFormat3ReadBudget):
    private[mysql] def requireBefore(record:B1ResumeRecord,before:MySqlScriptSchemaSnapshot,
        actualRoot:MySqlFormat3ReadBudget):Unit =
      owner.requireProof(this)
      if (actualRoot ne root) || record.assertions().pendingAttempt().phase()!=B1ResumeRecord.Phase.CERTIFY ||
          !sameAttempt(record.assertions().pendingAttempt(),expected,record,original) then
        throw new SQLDataException("historical CERTIFY proof differs from the original attempt/root/source")
      prepare.requireReference(prepareReference);handoff.requireReference(handoffReference)
      prepare.withData { (prepared,_,preparedTarget,_,_)=>
        val committed=prepared.assertions().pendingAttempt()
        if committed.phase()!=B1ResumeRecord.Phase.PREPARE then throw new SQLDataException("historical committed PREPARE body required")
        handoff.withData { (transferred,_,transferredTarget,_,_)=>
          val settled=transferred.assertions().pendingAttempt()
          val a=committed.targetWriter().lastSlot();val b=record.assertions().pendingAttempt().beforeWriter().lastSlot()
          if settled.phase()!=B1ResumeRecord.Phase.HANDOFF || !sameData(before,transferredTarget) ||
              a==null || b==null || a.boot()!=b.boot() || a.writerEpoch()!=b.writerEpoch() ||
              a.binding()!=b.binding() || a.operation()!=b.operation() || !Arrays.equals(a.fingerprint(),b.fingerprint()) ||
              !Arrays.equals(prepared.originalReceipt(),record.originalReceipt()) ||
              !Arrays.equals(transferred.originalReceipt(),record.originalReceipt()) then
            throw new SQLDataException("historical CERTIFY before lacks committed PREPARE receipt and settled HANDOFF image continuity")
        }
      }

  enum Position:
    case Before,After,Unresolved
  private[mysql] def requireBootstrapBody(record:B1ResumeRecord,before:MySqlScriptSchemaSnapshot,
      target:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget):Unit =
    root.requireScriptDataInputs(inputs)
    val attempt=record.assertions().pendingAttempt()
    if attempt==null || before==null || target==null || (before.inputs ne inputs) || (target.inputs ne inputs) ||
        before.schema!=target.schema || before.connection!=target.connection ||
        before.observationScope!=target.observationScope || before.maxAllowedPacketBytes!=target.maxAllowedPacketBytes then
      throw new SQLDataException("original bootstrap body requires one exact complete Full26 observation")
    attempt.phase() match
      case B1ResumeRecord.Phase.PREPARE=>requirePrepareBody(record,before,target,inputs,root)
      case B1ResumeRecord.Phase.HANDOFF=>requireHandoffBody(record,before,target,root)
      case B1ResumeRecord.Phase.CERTIFY=>requireCertifyBody(record,before,target,inputs,root)

  private def requireCertifyBody(record:B1ResumeRecord,before:MySqlScriptSchemaSnapshot,
      target:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget):Unit =
    val attempt=record.assertions().pendingAttempt()
    val marker=record.assertions().marker()
    if marker==null then throw new SQLDataException("original CERTIFY body lacks its complete marker assertions")
    val receipt=com.wi.floatingpod.server.WindowsWorldProvisionReceiptDataCodec.decode(record.originalReceipt())
    val writer=attempt.targetWriter()
    val targetWriter=MySqlFreshFormat3WriterTarget(writer.binding(),writer.epoch(),Option(writer.ownerBoot()),
      writer.leaseUntil(),attempt.logicalWriteUuid())
    val receiptSha=java.security.MessageDigest.getInstance("SHA-256").digest(record.originalReceipt()).toVector
    val markerSha=java.security.MessageDigest.getInstance("SHA-256").digest(marker.fullMarkerBytes()).toVector
    MySqlDeploymentReceiptBootstrapGraph.requireCertifyTransition(before,target,inputs,root,receipt.plan(),
      receipt.sourceIdentity(),receiptSha,markerSha,attempt.beforeWriter(),targetWriter)
    val context=MySqlDeploymentReceiptBootstrapGraph.contextForTransition("CERTIFY",before,target,receipt.plan(),
      receipt.sourceIdentity(),receiptSha,targetWriter)
    if !sameContext(attempt.context(),MySqlFreshPrepareRules.operation(context)) then
      throw new SQLDataException("original CERTIFY body effect/context differs from complete marker-bound transition")

  /** HANDOFF 的历史 DATA 使用原完整 W assertions；不补造 databaseNow 或新的 logical receipt。 */
  private def requireHandoffBody(record:B1ResumeRecord,before:MySqlScriptSchemaSnapshot,
      target:MySqlScriptSchemaSnapshot,root:MySqlFormat3ReadBudget):Unit =
    val attempt=record.assertions().pendingAttempt()
    val previous=attempt.beforeWriter();val next=attempt.targetWriter()
    // 正常退出可清空 W owner；这里只核原 body DATA，实际旧进程退出仍由原 native inventory/source 验证。
    if previous==null || next==null ||
        next.binding()!=previous.binding() || next.epoch()!=Math.addExact(previous.epoch(),1L) ||
        next.ownerBoot()!=attempt.context().boot() || next.ownerBoot()==null then
      throw new SQLDataException("original HANDOFF body is not the exact next owned W transition")
    val writerShape=MySqlDeploymentReceiptRelationShapes.forTable("fp_runtime_writer")
    val changed=Set("writer_epoch","owner_boot_uuid","lease_until")
    before.relations.zip(target.relations).foreach { (a,b)=>
      if a.table!=b.table then throw new SQLDataException("original HANDOFF changed complete relation order")
      if a.table!="fp_runtime_writer" then
        if a!=b then throw new SQLDataException("original HANDOFF changed a non-W Full26 relation")
      else
        if a.rows.size!=1 || b.rows.size!=1 then throw new SQLDataException("original HANDOFF requires singleton W")
        writerShape.columns.zipWithIndex.foreach { (column,index)=>
          if !changed.contains(column.name) && a.rows.head(index)!=b.rows.head(index) then
            throw new SQLDataException("original HANDOFF changed a preserved W column")
        }
    }
    if before.relations.size!=target.relations.size then throw new SQLDataException("original HANDOFF changed Full26 relation count")
    val effect=MySqlOrdinaryHandoffGraph.effectFingerprint(previous,next,attempt.attemptId(),attempt.logicalWriteUuid(),root)
    val context=MySqlOperationContext(attempt.context().boot(),next.epoch(),next.binding(),attempt.logicalWriteUuid(),
      Vector(MySqlExpectedEffect("certified-ordinary/writer",previous.epoch(),effect)))
    if !sameContext(attempt.context(),MySqlFreshPrepareRules.operation(context)) then
      throw new SQLDataException("original HANDOFF body effect/context differs from the complete preserved-slot W transition")

  /** 历史 PREPARE 用原 assertions 重建完整 composite，不捏造当时数据库时钟。 */
  private[mysql] def requirePrepareBody(record:B1ResumeRecord,before:MySqlScriptSchemaSnapshot,
      target:MySqlScriptSchemaSnapshot,inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget):Unit =
    val attempt=record.assertions().pendingAttempt()
    if attempt==null then throw new SQLDataException("original body must retain its complete pending attempt")
    if attempt.phase()==B1ResumeRecord.Phase.PREPARE then
      val receipt=com.wi.floatingpod.server.WindowsWorldProvisionReceiptDataCodec.decode(record.originalReceipt())
      val plan=receipt.plan()
      val writer=attempt.targetWriter()
      val targetWriter=MySqlFreshFormat3WriterTarget(writer.binding(),writer.epoch(),Option(writer.ownerBoot()),
        writer.leaseUntil(),attempt.logicalWriteUuid())
      val receiptSha=java.security.MessageDigest.getInstance("SHA-256").digest(record.originalReceipt()).toVector
      MySqlDeploymentReceiptFreshPrepareComposition.requirePrepareTransition(before,target,inputs,root,plan,
        receipt.sourceIdentity(),receiptSha,attempt.beforeWriter(),targetWriter)
      val context=MySqlDeploymentReceiptFreshPrepareComposition.contextForPrepare(before,target,inputs,root,plan,
        receipt.sourceIdentity(),receiptSha,targetWriter)
      if !sameContext(attempt.context(),MySqlFreshPrepareRules.operation(context)) then
        throw new SQLDataException("original PREPARE body effect/context differs from complete composite transition")

  private def sameContext(left:B1ResumeRecord.OperationAssertions,right:B1ResumeRecord.OperationAssertions):Boolean =
    left!=null && right!=null && left.boot()==right.boot() && left.writerEpoch()==right.writerEpoch() &&
      left.binding()==right.binding() && left.operation()==right.operation() &&
      Arrays.equals(left.originalOperationFingerprint(),right.originalOperationFingerprint()) &&
      left.effects().size()==right.effects().size() && left.effects().asScala.zip(right.effects().asScala).forall { (a,b)=>
        a.targetIdentity()==b.targetIdentity() && a.expectedVersion()==b.expectedVersion() &&
          Arrays.equals(a.desiredFingerprint(),b.desiredFingerprint())
      }
  def sameData(left:MySqlScriptSchemaSnapshot,right:MySqlScriptSchemaSnapshot):Boolean =
    left!=null && right!=null && left.schema==right.schema && left.connection.serverUuid==right.connection.serverUuid &&
      left.connection.catalog==right.connection.catalog && (left.inputs eq right.inputs) && left.relations==right.relations
  def classify(current:MySqlScriptSchemaSnapshot,before:MySqlScriptSchemaSnapshot,target:MySqlScriptSchemaSnapshot):Position =
    require(current!=null && before!=null && target!=null,"three complete actual Full26 images required")
    if sameData(current,before) then Position.Before
    else if sameData(current,target) then Position.After
    else Position.Unresolved
  private def sameAttempt(actual:B1ResumeRecord.AttemptAssertions,expected:B1ResumeRecord.AttemptAssertions,
      body:B1ResumeRecord,retained:B1ResumeRecord):Boolean =
    // Java records contain arrays; compare the canonical complete assertion encoding, never record.equals.
    if actual==null || expected==null || !Arrays.equals(body.originalReceipt(),retained.originalReceipt()) ||
        !Arrays.equals(body.sourceAnchorSha256(),retained.sourceAnchorSha256()) || body.barrierNamespace()!=retained.barrierNamespace() then false
    else
      def normalized(attempt:B1ResumeRecord.AttemptAssertions):Array[Byte] =
        val state=body.assertions()
        val replaced=new B1ResumeRecord.FullStateAssertions(state.currentOwner(),state.previousOwner(),state.retirement(),
          state.originalPrepareWitness(),state.currentWriter(),state.targetWriter(),attempt,state.latestSettlement(),state.marker(),state.acknowledgedCertifyWitness())
        B1ResumeRecordCanonical.encodeSchema5Data(new B1ResumeRecord(body.sequence(),body.previousRecordSha256(),body.kind(),
          body.sourceAnchorSha256(),body.originalReceipt(),body.barrierNamespace(),replaced))
      Arrays.equals(normalized(actual),normalized(expected))
  def allocate(actor:MySqlDeploymentReceiptBootstrapActor,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget):MySqlDeploymentReceiptFull26RestoreOwner =
    require(actor!=null && inputs!=null && root!=null,"original actor/inputs/root required")
    actor.requireCurrent();root.requireScriptDataInputs(inputs)
    root.reserveRetainedMemory(512L,"registered Full26 restore owner BEFORE allocation")
    new MySqlDeploymentReceiptFull26RestoreOwner(actor,inputs,root)
