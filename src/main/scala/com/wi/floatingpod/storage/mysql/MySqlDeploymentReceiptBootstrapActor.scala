package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{B1ResumeRecord,B1ResumeRecordCanonical,B1ResumeRecordV6,WindowsB1Full26WitnessStore,WindowsB1V6WitnessRecordStore,WindowsCanonicalMarkerPublicationBarrier,WindowsMarkerIo,WindowsWorldProvisionReceiptDataCodec}
import java.security.MessageDigest
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.{Arrays,HexFormat,UUID}

/** 原 Session 唯一 actor；私有 plan 的状态只随 actual durable/commit/readback 推进。 */
private[mysql] final class MySqlDeploymentReceiptBootstrapActor private (private val session:MySqlDeploymentReceiptSession):
  import MySqlDeploymentReceiptBootstrapActor.*
  private var plans:List[Planned5]=Nil
  private var pendingGraphs:List[MySqlFormat3ReadBudget]=Nil
  private var restore:MySqlDeploymentReceiptFull26RestoreOwner=null
  private var completed:Completion5=null
  private var restrictedHandoffAck:WindowsB1V6WitnessRecordStore.DurableV6Ack=null
  private var restrictedHandoffPlan:Planned5=null
  private var attempted=false
  private var retirementAttempted=false
  private var retirementFailure:Throwable=null
  private var retiredPlanFee=0L
  private var successorPermit:SuccessorPrepareInstallPermit5=null
  private var successorPermitFee=0L
  private var bootstrapReceipt:Array[Byte]=null
  private var bootstrapReceiptSource:com.wi.floatingpod.server.WindowsWorldProvisioner.DeploymentReceiptIoScope=null
  private var bootstrapReceiptFee=0L
  private[mysql] def terminalRetirementComplete:Boolean =
    retirementAttempted && retirementFailure==null && retiredPlanFee==0L && successorPermitFee==0L && successorPermit==null &&
      bootstrapReceipt==null && bootstrapReceiptSource==null && bootstrapReceiptFee==0L &&
      plans.isEmpty && pendingGraphs.isEmpty && restore==null && completed==null
  private[mysql] def completionForTerminal(actual:MySqlDeploymentReceiptSession):Completion5 =
    session.requireTerminalPreparationActor(this)
    if (actual ne session) || completed==null then
      throw new IllegalStateException("actual schema5 completed keeper/adoption required before terminal preparation")
    completed.requireTerminalData(this)
    completed
  private[mysql] def requireTerminalHolder(intent:MySqlDeploymentReceiptSession.TerminalIntent5):Unit =
    session.requireTerminalHolder(this,intent)
  private[mysql] def retireForTerminal(intent:MySqlDeploymentReceiptSession.TerminalIntent5,
      root:MySqlFormat3ReadBudget):Unit =
    session.requireTerminalRoot(this,intent,root)
    if retirementAttempted then throw new IllegalStateException("original schema5 actor retirement is one-shot",retirementFailure)
    retirementAttempted=true
    def settle(action: =>Unit):Boolean =
      try { action;true }
      catch case cause:Throwable=>
        if retirementFailure==null then retirementFailure=cause else MySqlOwnedIoAdapter.suppressExitCleanup(retirementFailure,cause)
        false
    completed=null
    bootstrapReceipt=null;bootstrapReceiptSource=null
    if bootstrapReceiptFee!=0L then settle {
      root.releaseRetainedMemory(bootstrapReceiptFee,"original actor receipt bytes retired");bootstrapReceiptFee=0L
    }
    successorPermit=null
    if successorPermitFee!=0L then settle {
      root.releaseRetainedMemory(successorPermitFee,"original successor installation permit retired");successorPermitFee=0L
    }
    val originalCount=plans.size
    plans=plans.filter { plan=>
      val closed=plan.body==null || settle { plan.body.close();plan.body=null }
      plan.before=null;plan.target=null;plan.checked=null;plan.artifact=null;plan.durable=null
      !closed
    }
    if originalCount!=plans.size then
      retiredPlanFee=Math.addExact(retiredPlanFee,512L*(originalCount-plans.size))
      settle { root.releaseRetainedMemory(retiredPlanFee,"original schema5 terminal plan holders retired");retiredPlanFee=0L }
    if restore!=null then settle { restore.retireForTerminal(intent);restore=null }
    pendingGraphs=pendingGraphs.filter(budget=> !settle(budget.closeScopedReadBudget()))
    if retirementFailure!=null then throw retirementFailure
  private[mysql] def requireCurrent():Unit=session.requireActor(this)
  private[mysql] def requireHistoricalReceiptScope(data:MySqlNormalRecallRecovery.ReceiptData,
      held:HeldSessionBarrier):Unit =session.requireHistoricalReceiptScope(this,data,held)
  private def retainBootstrapReceipt(source:com.wi.floatingpod.server.WindowsWorldProvisioner.DeploymentReceiptIoScope,
      root:MySqlFormat3ReadBudget):Array[Byte] =
    if bootstrapReceiptSource!=null then
      if (bootstrapReceiptSource ne source) || bootstrapReceipt==null then
        throw new IllegalStateException("original actor receipt partial holder is unavailable")
    else
      val fee=Math.addExact(source.originalReceiptLength().toLong,64L)
      root.reserveRetainedMemory(fee,"original actor receipt defensive bytes BEFORE allocation")
      bootstrapReceiptFee=fee;bootstrapReceiptSource=source
      bootstrapReceipt=source.originalReceipt()
    bootstrapReceipt
  private[mysql] def requirePlan(plan:Planned5):Unit =
    requireCurrent()
    if plan==null || (plan.actor ne this) || !plans.exists(_ eq plan) || plan.state!=PlanState.Durable then
      throw new IllegalStateException("exact unexecuted original schema5 durable plan required")
  private[mysql] def requireDurable(plan:Planned5,ack:WindowsB1V6WitnessRecordStore.DurableV6Ack,held:HeldSessionBarrier):Unit =
    requirePlan(plan)
    if (plan.durable ne ack) || held.actualConnectionId!=plan.attempt.connectionId() ||
        held.connectionGenerationId!=plan.attempt.connectionGeneration() || held.namespace.lockName()!=plan.attempt.namespace() then
      throw new IllegalStateException("schema5 original plan/ACK/session barrier changed")
    ack.requireRegisteredCurrent()
    if plan.confirmation==null then throw new IllegalStateException("original complete confirmation capacity must be reserved before DML")
    plan.confirmation.requirePrepared(plan,ack)
    if plan.checked==null || (plan.checked.plan ne plan) || (plan.checked.ack ne ack) then
      throw new IllegalStateException("original external durable rebarrier required before SQL lock")
    plan.body.requireReference(plan.checked.reference)
  private[mysql] def rebarrierBeforeWrite(plan:Planned5,ack:WindowsB1V6WitnessRecordStore.DurableV6Ack):Unit =
    requirePlan(plan)
    if plan.confirmation==null then throw new IllegalStateException("original complete confirmation reservation required before SQL gate")
    plan.confirmation.requirePrepared(plan,ack)
    if (plan.durable ne ack) || plan.checked!=null then throw new IllegalStateException("original durable write verification is one-shot")
    ack.requireCurrent()
    val reference=plan.artifact.reference()
    plan.body.requireReference(reference);plan.artifact.rebarrier()
    if !Arrays.equals(B1ResumeRecordCanonical.encodeSchema5Data(ack.record().transition()),
        B1ResumeRecordCanonical.encodeSchema5Data(plan.record)) then throw new SQLDataException("actual durable transition differs from original plan")
    plan.checked=new DurableCheck5(plan,ack,reference)
  private[mysql] def commitAcknowledged(plan:Planned5):Unit =
    requirePlan(plan);plan.state=PlanState.Committed
  private[mysql] def readbackConfirmed(plan:Planned5):Unit =
    requireCurrent()
    if plan==null || (plan.actor ne this) || plan.state!=PlanState.Committed then throw new IllegalStateException("actual commit acknowledgement required")
    plan.state=PlanState.Readback

  private[mysql] def continueFreshPrepare():Unit =
    requireCurrent()
    if attempted then throw new IllegalStateException("schema5 actor bootstrap is one-shot")
    attempted=true
    val (receipt,anchorSha,sourceOwner,world)=session.withFreshNativePreflight(this) { (_,root,native)=>native.withSource { source=>
      (retainBootstrapReceipt(source,root),source.anchorSha256(),source.currentOwner(),source.sourceWorldIdentity())
    }}
    session.withFreshPlanning(this) { (request,before,writer,inputs,root,scope,held,native)=>
        val receiptHash=hash(receipt).toVector
        val targetWriter=MySqlFreshFormat3WriterTarget(request.plan.bindingUuid(),Math.addExact(writer.writerEpoch,1L),
          Some(scope.bootUuid),deadline(writer.databaseNow),request.plan.prepareWriteUuid())
        val target=MySqlDeploymentReceiptFreshPrepareComposition.prepareTarget(before,inputs,root,request.plan,world,receiptHash,writer,targetWriter)
        pendingGraphs=target.allocationBudget::pendingGraphs
        val context=MySqlDeploymentReceiptFreshPrepareComposition.contextForPrepare(before,target,inputs,root,request.plan,world,receiptHash,targetWriter)
        val attempt=makeAttempt(B1ResumeRecord.Phase.PREPARE,UUID.randomUUID(),before,target,writer,targetWriter,context,inputs,root,scope,held,None)
        val state=new B1ResumeRecord.FullStateAssertions(sourceOwner,null,null,attempt,attempt.beforeWriter(),attempt.targetWriter(),attempt,null,null,null)
        val record=new B1ResumeRecord(0L,new Array[Byte](32),B1ResumeRecord.Kind.GENESIS,HexFormat.of().parseHex(anchorSha),receipt,held.namespace.lockName(),state)
        val plan=register(record,attempt,before,target,context,inputs,root)
        session.settlePlanningRead(this)
        durable(plan,None,native)
        reserveConfirmation(plan,native) {
          val prepareSettlement=new B1ResumeRecord.SettlementAssertions(B1ResumeRecord.SettlementKind.CONFIRMED,attempt,
            "MYSQL_ORIGINAL_COMPOSITE_PREPARE_COMMIT_IDLE_FULL26_READBACK_V1",B1ResumeRecordCanonical.encodeSchema5Data(record),new Array[Byte](32))
          val confirmationState=new B1ResumeRecord.FullStateAssertions(state.currentOwner(),null,null,attempt,attempt.targetWriter(),attempt.targetWriter(),null,prepareSettlement,null,null)
          next(plan.durable,B1ResumeRecord.Kind.PREPARED_CONFIRMED,confirmationState,
            new B1ResumeRecordV6.References(plan.artifact.reference(),null,plan.artifact.reference(),null))
        }
        val prepared=session.executePlan(this,plan,plan.durable)
        val confirmed=publishConfirmation(plan,native)
        val marker=native.publishPreparedMarker();native.rebarrierMarker(marker)
        session.withCommittedImage(this,prepared) { (actual,preparedWriter)=>
          val certifyWriter=MySqlFreshFormat3WriterTarget(request.plan.bindingUuid(),preparedWriter.writerEpoch,Some(scope.bootUuid),
            preparedWriter.leaseUntil,request.plan.certifyWriteUuid())
          val certified=MySqlDeploymentReceiptBootstrapGraph.certifyTarget(actual,inputs,root,request.plan,world,receiptHash,
            HexFormat.of().parseHex(marker.markerSha256()).toVector,preparedWriter,certifyWriter)
          pendingGraphs=certified.allocationBudget::pendingGraphs
          val certifyContext=MySqlDeploymentReceiptBootstrapGraph.contextForTransition("CERTIFY",actual,certified,request.plan,world,receiptHash,certifyWriter)
          val certifyAttempt=makeAttempt(B1ResumeRecord.Phase.CERTIFY,UUID.randomUUID(),actual,certified,preparedWriter,certifyWriter,
            certifyContext,inputs,root,scope,held,Some(attempt.targetWriter().lastSlot()))
          val markerAssertions=markerData(marker,sourceOwner.generation())
          val certifyState=new B1ResumeRecord.FullStateAssertions(state.currentOwner(),null,null,attempt,certifyAttempt.beforeWriter(),
            certifyAttempt.targetWriter(),certifyAttempt,null,markerAssertions,null)
          val certifyPlan=register(nextRecord(confirmed,B1ResumeRecord.Kind.NATIVE_ACK_CERTIFY_INTENT,certifyState),certifyAttempt,actual,certified,certifyContext,inputs,root)
          durable(certifyPlan,Some(confirmed),native);native.rebarrierMarker(marker)
          reserveConfirmation(certifyPlan,native) {
            val completeState=new B1ResumeRecord.FullStateAssertions(state.currentOwner(),null,null,attempt,certifyAttempt.targetWriter(),
              certifyAttempt.targetWriter(),null,null,markerAssertions,certifyAttempt)
            next(certifyPlan.durable,B1ResumeRecord.Kind.CERTIFIED,completeState,
              new B1ResumeRecordV6.References(plan.artifact.reference(),null,null,certifyPlan.artifact.reference()))
          }
          val finalImage=session.executePlan(this,certifyPlan,certifyPlan.durable)
          val completeAck=publishConfirmation(certifyPlan,native)
          native.rebarrierMarker(marker);completeAck.requireCurrent()
          completed=new Completion5(this,finalImage,certifyPlan,completeAck,Some(marker),CompletionKind5.FreshPreparation,None)
          session.withCommittedImage(this,finalImage) { (_,writer)=>
            val post=session.auditFreshPreparation(this,writer)
            completed.admission=Some(session.adoptOrdinaryCompletion(this,completed,post))
          }
        }
    }

  private[mysql] def continueOrdinary(qualification:VerifiedSchemaQualification):Unit =
    requireCurrent()
    if attempted then throw new IllegalStateException("schema5 actor bootstrap is one-shot")
    attempted=true
    val inventory=session.withOrdinaryNativePreflight(this) { (inputs,root,native)=>
      val actual=native.discoverCurrent()
      if actual.kind()==WindowsB1V6WitnessRecordStore.CurrentInventory.Kind.RECORDED then
        restore=MySqlDeploymentReceiptFull26RestoreOwner.allocate(this,inputs,root)
        restore.restoreInventory(actual,native)
      actual
    }
    if inventory.kind()==WindowsB1V6WitnessRecordStore.CurrentInventory.Kind.PRE_GENESIS then
      continuePreGenesis(qualification,inventory)
    else
      val currentAck=inventory.current()
      session.withOrdinaryPlanning(this) { (pins,current,before,writer,inputs,root,scope,held,native)=>
        val position=restore.currentPendingPosition(before)
        // 锁内只比较原 inventory/body；回到 idle 后才发布 settlement/读 marker 或 native owner。
        session.settlePlanningRead(this)
        val previous=position.map(value=>settleColdPending(currentAck,value,native)).getOrElse(currentAck)
        if position.nonEmpty then
          val settledInventory=native.retainCurrentInventory(previous)
          restore.advanceInventory(settledInventory,native)
        val recall=MySqlNormalRecallRecovery.discover(before,writer.bindingUuid,Some(root))
        val container=previous.record();val original=container.transition();val old=original.assertions()
        if old.pendingAttempt()!=null ||
            (old.originalPrepareWitness()==null)!=(container.references().originalPrepare()==null) then
          throw new SQLDataException("schema5 Ordinary requires settled original PREPARE history")
        if recall.nonEmpty && (old.acknowledgedCertifyWitness()==null || old.marker()==null) then
          throw new SQLDataException("restricted schema5 Recall requires its original certified source marker/witness")
        if old.acknowledgedCertifyWitness()!=null && old.marker()==null then
          throw new SQLDataException("schema5 acknowledged CERTIFY lacks its original marker")
        if old.acknowledgedCertifyWitness()==null && (writer.ownerBootUuid.isEmpty ||
            !writer.lastOperationUuid.contains(pins.plan().prepareWriteUuid())) then
          continueSuccessorPrepare(previous,before,writer,pins.plan(),qualification,inputs,root,scope,held,native,inventory)
        else
          if old.originalPrepareWitness()==null then
            throw new SQLDataException("schema5 HANDOFF/CERTIFY requires its original confirmed PREPARE witness/reference")
          if old.marker()!=null then native.rebarrierOriginalMarker(old.marker())
          native.withSource { source=>
            if writer.ownerBootUuid.nonEmpty && writer.leaseUntil.isAfter(writer.databaseNow) then throw new SQLDataException("schema5 occupied original writer lease has not expired")
            val originalSlot=Option(old.currentWriter().lastSlot())
            val slot=if recall.isEmpty && MySqlOrdinaryHandoffGraph.originalSlotMatches(writer,originalSlot) then originalSlot
              else
                val proof=restore.originalOperationContinuation(previous,before,writer,held,native)
                Some(proof.consume(this,previous,before,writer,source,root))
            val targetWriter=writer.copy(writerEpoch=Math.addExact(writer.writerEpoch,1L),ownerBootUuid=Some(scope.bootUuid),leaseUntil=deadline(writer.databaseNow))
            val target=handoffTarget(before,targetWriter,root)
            val effect=MySqlOrdinaryHandoffGraph.effectFingerprint(writer,targetWriter,current.handoffAttempt(),current.handoffOperation(),root)
            val context=MySqlOperationContext(scope.bootUuid,targetWriter.writerEpoch,writer.bindingUuid,current.handoffOperation(),
              Vector(MySqlExpectedEffect("certified-ordinary/writer",writer.writerEpoch,effect)))
            val beforeAssertion=new B1ResumeRecord.WriterAssertions(writer.bindingUuid,writer.writerEpoch,writer.ownerBootUuid.orNull,writer.leaseUntil,slot.orNull)
            val targetAssertion=new B1ResumeRecord.WriterAssertions(targetWriter.bindingUuid,targetWriter.writerEpoch,targetWriter.ownerBootUuid.orNull,targetWriter.leaseUntil,slot.orNull)
            val attempt=new B1ResumeRecord.AttemptAssertions(current.handoffAttempt(),B1ResumeRecord.Phase.HANDOFF,current.handoffOperation(),
              scope.bootGeneration,held.connectionGenerationId,held.actualConnectionId,held.namespace.lockName(),MySqlFreshPrepareRules.operation(context),
              beforeAssertion,targetAssertion,MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(before,inputs,root).toResumeAssertions,
              MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(target,inputs,root).toResumeAssertions)
            val state=new B1ResumeRecord.FullStateAssertions(source.currentOwner(),source.previousOwner(),source.previousOwnerRetirement(),
              old.originalPrepareWitness(),beforeAssertion,targetAssertion,attempt,old.latestSettlement(),old.marker(),old.acknowledgedCertifyWitness())
            val plan=register(nextRecord(previous,B1ResumeRecord.Kind.HANDOFF_PREPARED,state),attempt,before,target,context,inputs,root)
            durable(plan,Some(previous),native)
            native.installOrdinary(plan.durable)
            reserveConfirmation(plan,native) {
              val settlement=new B1ResumeRecord.SettlementAssertions(B1ResumeRecord.SettlementKind.CONFIRMED,attempt,
                "MYSQL_SAME_CID_NULL_SAFE_OCCUPIED_W_CAS_COMMIT_IDLE_FULL26_READBACK_V1",
                B1ResumeRecordCanonical.encodeSchema5Data(plan.record),new Array[Byte](32))
              val settledState=new B1ResumeRecord.FullStateAssertions(state.currentOwner(),state.previousOwner(),state.retirement(),
                state.originalPrepareWitness(),targetAssertion,targetAssertion,null,settlement,state.marker(),state.acknowledgedCertifyWitness())
              val refs=new B1ResumeRecordV6.References(container.references().originalPrepare(),null,plan.artifact.reference(),container.references().acknowledgedCertify())
              next(plan.durable,B1ResumeRecord.Kind.DB_ACQUIRED,settledState,refs)
            }
            val after=session.executePlan(this,plan,plan.durable)
            session.withCommittedImage(this,after) { (_,actualWriter)=>
              val settled=publishConfirmation(plan,native)
              if recall.nonEmpty then
                val postAudit=session.auditOrdinaryCompletion(this,qualification,actualWriter)
                native.rebarrierOriginalMarker(old.marker())
                restore.advanceInventory(native.retainCurrentInventory(settled),native)
                restore.consumeContinuationHistory(this,settled,before) { history=>
                  val decisions=recall.map(i=>MySqlNormalRecallRecovery.classifyCheckpointData(before,history,i,root))
                  restrictedHandoffAck=settled;restrictedHandoffPlan=plan
                  session.retainRestrictedRecall(this,settled,after,actualWriter,recall,plan,postAudit,decisions)
                }
              else if old.acknowledgedCertifyWitness()==null then
                restore.advanceInventory(native.retainCurrentInventory(settled),native)
                finishRecoveredCertify(settled,after,actualWriter,pins.plan(),qualification,inputs,root,scope,held,native)
              else
                val postAudit=session.auditOrdinaryCompletion(this,qualification,actualWriter)
                native.rebarrierOriginalMarker(old.marker())
                completed=new Completion5(this,after,plan,settled,None,CompletionKind5.OrdinaryHandoff,None)
                val admission=session.adoptOrdinaryCompletion(this,completed,postAudit)
                completed.admission=Some(admission)
            }
          }
      }

  /** 只校验原真实 HANDOFF 计划和仍 current 的确认；不发行普通 completion。 */
  private[mysql] def requireRestrictedRecallHandoff(plan:Planned5,
      ack:WindowsB1V6WitnessRecordStore.DurableV6Ack):Unit =
    requireCurrent()
    if plan==null || (plan.actor ne this) || !plans.exists(_ eq plan) ||
        (restrictedHandoffPlan ne plan) || (restrictedHandoffAck ne ack) ||
        plan.phase!=B1ResumeRecord.Phase.HANDOFF || plan.state!=PlanState.Readback || plan.confirmation==null then
      throw new SQLDataException("restricted Recall requires original HANDOFF commit/readback/confirmation")
    ack.requireCurrent()

  private def continuePreGenesis(qualification:VerifiedSchemaQualification,
      inventory:WindowsB1V6WitnessRecordStore.CurrentInventory):Unit =
    session.requirePreGenesisQualification(this)
    val (receipt,anchor,owner,previousOwner,retirement,sourceIdentity)=session.withOrdinaryNativePreflight(this) { (_,root,native)=>
      native.withSource { source=>
        (retainBootstrapReceipt(source,root),source.anchorSha256(),source.currentOwner(),source.previousOwner(),
          source.previousOwnerRetirement(),source.sourceWorldIdentity())
      }
    }
    session.withOrdinaryPlanning(this) { (pins,_,before,writer,inputs,root,scope,held,native)=>
      val targetWriter=MySqlFreshFormat3WriterTarget(pins.plan().bindingUuid(),Math.addExact(writer.writerEpoch,1L),Some(scope.bootUuid),
        deadline(writer.databaseNow),pins.plan().prepareWriteUuid())
      val receiptHash=hash(receipt).toVector
      val target=MySqlDeploymentReceiptFreshPrepareComposition.prepareTarget(before,inputs,root,pins.plan(),sourceIdentity,
        receiptHash,writer,targetWriter)
      pendingGraphs=target.allocationBudget::pendingGraphs
      val context=MySqlDeploymentReceiptFreshPrepareComposition.contextForPrepare(before,target,inputs,root,pins.plan(),
        sourceIdentity,receiptHash,targetWriter)
      val attempt=makeAttempt(B1ResumeRecord.Phase.PREPARE,UUID.randomUUID(),before,target,writer,targetWriter,context,
        inputs,root,scope,held,None)
      val state=new B1ResumeRecord.FullStateAssertions(owner,previousOwner,retirement,attempt,attempt.beforeWriter(),attempt.targetWriter(),
        attempt,null,null,null)
      val record=new B1ResumeRecord(0L,new Array[Byte](32),B1ResumeRecord.Kind.GENESIS,HexFormat.of().parseHex(anchor),receipt,held.namespace.lockName(),state)
      val plan=register(record,attempt,before,target,context,inputs,root)
      session.settlePlanningRead(this)
      durable(plan,None,native)
      native.withSource { source=>
        root.reserveRetainedMemory(256L,"original pre-genesis PREPARE installation permit BEFORE allocation")
        successorPermitFee=256L
        successorPermit=new SuccessorPrepareInstallPermit5(this,source,inventory,PrepareInstallOrigin5.PreGenesis,before,held,plan)
      }
      native.installSuccessorPrepare(successorPermit)
      reserveConfirmation(plan,native) {
        val settlement=new B1ResumeRecord.SettlementAssertions(B1ResumeRecord.SettlementKind.CONFIRMED,attempt,
          "MYSQL_PRE_GENESIS_COMPOSITE_PREPARE_COMMIT_IDLE_FULL26_READBACK_V1",B1ResumeRecordCanonical.encodeSchema5Data(record),new Array[Byte](32))
        val confirmedState=new B1ResumeRecord.FullStateAssertions(owner,previousOwner,retirement,attempt,attempt.targetWriter(),
          attempt.targetWriter(),null,settlement,null,null)
        val refs=new B1ResumeRecordV6.References(plan.artifact.reference(),null,plan.artifact.reference(),null)
        next(plan.durable,B1ResumeRecord.Kind.PREPARED_CONFIRMED,confirmedState,refs)
      }
      val prepared=session.executePlan(this,plan,plan.durable)
      session.withCommittedImage(this,prepared) { (_,actualWriter)=>
        val confirmed=publishConfirmation(plan,native)
        finishRecoveredCertify(confirmed,prepared,actualWriter,pins.plan(),qualification,inputs,root,scope,held,native)
      }
    }

  private def continueSuccessorPrepare(previous:WindowsB1V6WitnessRecordStore.DurableV6Ack,
      before:MySqlScriptSchemaSnapshot,writer:MySqlBusinessWriterRow,
      originalPlan:WindowsWorldProvisionReceiptDataCodec.PlanData,qualification:VerifiedSchemaQualification,
      inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget,scope:MySqlAuditBootScope,
      held:HeldSessionBarrier,native:MySqlDeploymentReceiptFreshNativeOwner,
      originalInventory:WindowsB1V6WitnessRecordStore.CurrentInventory):Unit =
    restore.requireCancelledPrepareBefore(before)
    session.requireSuccessorInstallBefore(this,before,held)
    val old=previous.record().transition().assertions()
    val (receipt,sourceIdentity)=native.withSource(source=>(retainBootstrapReceipt(source,root),source.sourceWorldIdentity()))
    val targetWriter=MySqlFreshFormat3WriterTarget(originalPlan.bindingUuid(),Math.addExact(writer.writerEpoch,1L),
      Some(scope.bootUuid),deadline(writer.databaseNow),originalPlan.prepareWriteUuid())
    val target=MySqlDeploymentReceiptFreshPrepareComposition.prepareTarget(before,inputs,root,originalPlan,sourceIdentity,
      hash(receipt).toVector,writer,targetWriter)
    pendingGraphs=target.allocationBudget::pendingGraphs
    val context=MySqlDeploymentReceiptFreshPrepareComposition.contextForPrepare(before,target,inputs,root,originalPlan,
      sourceIdentity,hash(receipt).toVector,targetWriter)
    val attempt=makeAttempt(B1ResumeRecord.Phase.PREPARE,UUID.randomUUID(),before,target,writer,targetWriter,context,
      inputs,root,scope,held,None)
    native.withSource { source=>
      val state=new B1ResumeRecord.FullStateAssertions(source.currentOwner(),source.previousOwner(),source.previousOwnerRetirement(),
        old.originalPrepareWitness(),attempt.beforeWriter(),attempt.targetWriter(),attempt,old.latestSettlement(),null,null)
      val plan=register(nextRecord(previous,B1ResumeRecord.Kind.PREPARE_SUCCESSOR_INTENT,state),attempt,before,target,context,inputs,root)
      durable(plan,Some(previous),native)
      if successorPermit!=null || successorPermitFee!=0L then throw new IllegalStateException("original successor installation permit is one-shot")
      root.reserveRetainedMemory(256L,"original successor PREPARE installation permit BEFORE allocation")
      successorPermitFee=256L
      successorPermit=new SuccessorPrepareInstallPermit5(this,source,originalInventory,PrepareInstallOrigin5.Cancelled(previous),before,held,plan)
      native.installSuccessorPrepare(successorPermit)
      reserveConfirmation(plan,native) {
        val settlement=new B1ResumeRecord.SettlementAssertions(B1ResumeRecord.SettlementKind.CONFIRMED,attempt,
          "MYSQL_SUCCESSOR_COMPOSITE_PREPARE_COMMIT_IDLE_FULL26_READBACK_V1",B1ResumeRecordCanonical.encodeSchema5Data(plan.record),new Array[Byte](32))
        val first=state.originalPrepareWitness()==null
        val original=if first then attempt else state.originalPrepareWitness()
        val confirmedState=new B1ResumeRecord.FullStateAssertions(state.currentOwner(),state.previousOwner(),state.retirement(),original,
          attempt.targetWriter(),attempt.targetWriter(),null,settlement,null,null)
        val refs=new B1ResumeRecordV6.References(if first then plan.artifact.reference() else previous.record().references().originalPrepare(),
          null,plan.artifact.reference(),null)
        next(plan.durable,B1ResumeRecord.Kind.PREPARED_CONFIRMED,confirmedState,refs)
      }
      val prepared=session.executePlan(this,plan,plan.durable)
      session.withCommittedImage(this,prepared) { (_,actualWriter)=>
        val confirmed=publishConfirmation(plan,native)
        finishRecoveredCertify(confirmed,prepared,actualWriter,originalPlan,qualification,inputs,root,scope,held,native)
      }
    }

  /** PREPARED恢复必须先完成真实HANDOFF；新PREPARE已安装当前custody时直接续当前CERTIFY。 */
  private def finishRecoveredCertify(previous:WindowsB1V6WitnessRecordStore.DurableV6Ack,
      before:MySqlScriptSchemaSnapshot,writer:MySqlBusinessWriterRow,
      originalPlan:com.wi.floatingpod.server.WindowsWorldProvisionReceiptDataCodec.PlanData,
      qualification:VerifiedSchemaQualification,inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget,
      scope:MySqlAuditBootScope,held:HeldSessionBarrier,native:MySqlDeploymentReceiptFreshNativeOwner):Unit =
    val old=previous.record().transition().assertions()
    val (receipt,sourceIdentity)=native.withSource(source=>(retainBootstrapReceipt(source,root),source.sourceWorldIdentity()))
    val marker=native.publishPreparedMarker();native.rebarrierMarker(marker)
    val targetWriter=MySqlFreshFormat3WriterTarget(writer.bindingUuid,writer.writerEpoch,writer.ownerBootUuid,
      writer.leaseUntil,originalPlan.certifyWriteUuid())
    val receiptHash=hash(receipt).toVector
    val target=MySqlDeploymentReceiptBootstrapGraph.certifyTarget(before,inputs,root,originalPlan,sourceIdentity,
      receiptHash,HexFormat.of().parseHex(marker.markerSha256()).toVector,writer,targetWriter)
    pendingGraphs=target.allocationBudget::pendingGraphs
    val context=MySqlDeploymentReceiptBootstrapGraph.contextForTransition("CERTIFY",before,target,originalPlan,
      sourceIdentity,receiptHash,targetWriter)
    val attempt=makeAttempt(B1ResumeRecord.Phase.CERTIFY,UUID.randomUUID(),before,target,writer,targetWriter,context,
      inputs,root,scope,held,Option(old.currentWriter().lastSlot()))
    val markerAssertions=markerData(marker,scope.bootGeneration)
    val state=new B1ResumeRecord.FullStateAssertions(old.currentOwner(),old.previousOwner(),old.retirement(),old.originalPrepareWitness(),
      attempt.beforeWriter(),attempt.targetWriter(),attempt,old.latestSettlement(),markerAssertions,null)
    val plan=register(nextRecord(previous,B1ResumeRecord.Kind.NATIVE_ACK_CERTIFY_INTENT,state),attempt,before,target,context,inputs,root)
    durable(plan,Some(previous),native);native.rebarrierMarker(marker)
    reserveConfirmation(plan,native) {
      val settlement=new B1ResumeRecord.SettlementAssertions(B1ResumeRecord.SettlementKind.CONFIRMED,attempt,
        "MYSQL_RECOVERED_CERTIFY_COMMIT_IDLE_FULL26_READBACK_V1",B1ResumeRecordCanonical.encodeSchema5Data(plan.record),new Array[Byte](32))
      val complete=new B1ResumeRecord.FullStateAssertions(state.currentOwner(),state.previousOwner(),state.retirement(),
        state.originalPrepareWitness(),attempt.targetWriter(),attempt.targetWriter(),null,settlement,markerAssertions,attempt)
      val refs=new B1ResumeRecordV6.References(previous.record().references().originalPrepare(),null,plan.artifact.reference(),plan.artifact.reference())
      next(plan.durable,B1ResumeRecord.Kind.CERTIFIED,complete,refs)
    }
    val after=session.executePlan(this,plan,plan.durable)
    session.withCommittedImage(this,after) { (_,actualWriter)=>
      val acknowledged=publishConfirmation(plan,native)
      native.rebarrierMarker(marker);acknowledged.requireCurrent()
      val post=session.auditOrdinaryCompletion(this,qualification,actualWriter)
      completed=new Completion5(this,after,plan,acknowledged,Some(marker),CompletionKind5.RecoveredCertify,None)
      completed.admission=Some(session.adoptOrdinaryCompletion(this,completed,post))
    }

  /** 只有同 owner 完整 native body/SQL 比较才能结算原 pending；不重提原 DML。 */
  private def settleColdPending(previous:WindowsB1V6WitnessRecordStore.DurableV6Ack,
      position:MySqlDeploymentReceiptFull26RestoreOwner.Position,native:MySqlDeploymentReceiptFreshNativeOwner):WindowsB1V6WitnessRecordStore.DurableV6Ack =
    val container=previous.record();val original=container.transition();val old=original.assertions()
    val pending=old.pendingAttempt()
    if pending==null || position==MySqlDeploymentReceiptFull26RestoreOwner.Position.Unresolved then
      throw new SQLDataException("actual resolved original pending Full26 comparison required")
    val confirmed=position==MySqlDeploymentReceiptFull26RestoreOwner.Position.After
    val writer=if confirmed then pending.targetWriter() else pending.beforeWriter()
    val settlement=new B1ResumeRecord.SettlementAssertions(
      if confirmed then B1ResumeRecord.SettlementKind.CONFIRMED else B1ResumeRecord.SettlementKind.CANCELLED,pending,
      "MYSQL_ORIGINAL_FULL26_COLD_BODY_LOCKED_EXACT_BEFORE_OR_TARGET_V1",B1ResumeRecordCanonical.encodeSchema5Data(original),
      hash(B1ResumeRecordCanonical.encodeSchema5Data(original)))
    val certify=if confirmed && pending.phase()==B1ResumeRecord.Phase.CERTIFY then pending else old.acknowledgedCertifyWitness()
    val firstPrepare=confirmed && pending.phase()==B1ResumeRecord.Phase.PREPARE && old.originalPrepareWitness()==null
    val originalPrepare=if firstPrepare then pending else old.originalPrepareWitness()
    val state=new B1ResumeRecord.FullStateAssertions(old.currentOwner(),old.previousOwner(),old.retirement(),originalPrepare,
      writer,writer,null,settlement,old.marker(),certify)
    val kind=if !confirmed then B1ResumeRecord.Kind.ATTEMPT_CANCELLED
      else pending.phase() match
        case B1ResumeRecord.Phase.PREPARE=>B1ResumeRecord.Kind.PREPARED_CONFIRMED
        case B1ResumeRecord.Phase.CERTIFY=>B1ResumeRecord.Kind.CERTIFIED
        case B1ResumeRecord.Phase.HANDOFF=>B1ResumeRecord.Kind.DB_ACQUIRED
    val refs=new B1ResumeRecordV6.References(if firstPrepare then container.references().pending() else container.references().originalPrepare(),null,container.references().pending(),
      if confirmed && pending.phase()==B1ResumeRecord.Phase.CERTIFY then container.references().pending() else container.references().acknowledgedCertify())
    native.publishNext(previous,next(previous,kind,state,refs),None)

  private def handoffTarget(before:MySqlScriptSchemaSnapshot,writer:MySqlBusinessWriterRow,root:MySqlFormat3ReadBudget):MySqlScriptSchemaSnapshot =
    val child=root.newCanonicalDataScope()
    pendingGraphs=child::pendingGraphs
    child.bindCanonicalDataPacketLimit(before.maxAllowedPacketBytes)
    child.reserveRetainedMemory(8192L,"Full26 HANDOFF relation holders BEFORE allocation")
    MySqlRecoveryCanonicalBytes.withAllocationBudget(child) {
      val relations=before.relations.map { relation=>
        val shape=before.inputs.forTable(relation.table)
        val rows=relation.rows.map { row=>
          child.reserveRetainedMemory(256L+64L*row.size,"Full26 HANDOFF row/cell holders BEFORE allocation")
          row.zipWithIndex.map { (cell,index)=>
          val name=shape.columns(index).name
          if relation.table=="fp_runtime_writer" && name=="writer_epoch" then MySqlFormat3Cell.SignedInteger(writer.writerEpoch)
          else if relation.table=="fp_runtime_writer" && name=="owner_boot_uuid" then
            val id=writer.ownerBootUuid.get
            child.reserveRetainedMemory(80L,"Full26 HANDOFF owned boot BEFORE allocation")
            val bytes=java.nio.ByteBuffer.allocate(16).putLong(id.getMostSignificantBits).putLong(id.getLeastSignificantBits).array()
            MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes,child))
          else if relation.table=="fp_runtime_writer" && name=="lease_until" then MySqlFormat3Cell.DateTime(writer.leaseUntil)
          else cell match
            case MySqlFormat3Cell.Binary(value)=>MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(value.copyBytes(child,"Full26 HANDOFF owned unchanged bytes"),child))
            case MySqlFormat3Cell.Text(value)=>
              child.reserveRetainedMemory(64L+4L*value.length,"Full26 HANDOFF unchanged text BEFORE allocation")
              MySqlFormat3Cell.Text(new String(value.toCharArray))
            case other=>other
        }}
        MySqlFormat3RelationSnapshot(relation.table,rows)
      }
      MySqlScriptSchemaSnapshot.captureMaterialized(before.schema,before.connection,before.observationScope,before.maxAllowedPacketBytes,before.inputs,relations,child)
    }

  private[mysql] def completionFor(actual:MySqlDeploymentReceiptSession):Completion5 =
    requireCurrent()
    if (actual ne session) || completed==null then throw new IllegalStateException("original completed schema5 Session required")
    completed.requireFor(this);completed

  private def register(record:B1ResumeRecord,attempt:B1ResumeRecord.AttemptAssertions,before:MySqlScriptSchemaSnapshot,
      target:MySqlScriptSchemaSnapshot,context:MySqlOperationContext,inputs:MySqlScriptSchemaInputs,root:MySqlFormat3ReadBudget):Planned5 =
    root.reserveRetainedMemory(512L,"registered original schema5 plan BEFORE body allocation")
    val plan=new Planned5(this,record,attempt,before,target,context);plans=plan::plans
    plan.body=MySqlFull26WitnessCodec.begin(inputs,root)
    val slot=attempt.beforeWriter().lastSlot()
    if attempt.phase()==B1ResumeRecord.Phase.CERTIFY && slot!=null &&
        (slot.boot()!=attempt.beforeWriter().ownerBoot() || slot.writerEpoch()!=attempt.beforeWriter().epoch()) then
      if restore==null then throw new SQLDataException("historical CERTIFY encoder requires original restore/history owner")
      plan.body.encodeWithHistoricalCertify(record,before,target,restore.historicalCertifyProof(record,attempt))
    else plan.body.encode(record,before,target)
    plan
  private def durable(plan:Planned5,previous:Option[WindowsB1V6WitnessRecordStore.DurableV6Ack],native:MySqlDeploymentReceiptFreshNativeOwner):Unit =
    if plan.state!=PlanState.Planned then throw new IllegalStateException("original schema5 plan publication is one-shot")
    plan.artifact=native.publishBody(plan.record,plan.body)
    val old=previous.map(_.record().references())
    val refs=new B1ResumeRecordV6.References(old.map(_.originalPrepare()).getOrElse(plan.artifact.reference()),plan.artifact.reference(),
      old.map(_.settled()).orNull,old.map(_.acknowledgedCertify()).orNull)
    val container=new B1ResumeRecordV6(plan.record,previous.map(a=>hash(B1ResumeRecordV6.encodeData(a.record()))).getOrElse(new Array[Byte](32)),refs)
    plan.durable=previous match
      case None=>native.publishGenesis(container,plan.artifact)
      case Some(value)=>native.publishNext(value,container,Some(plan.artifact))
    plan.state=PlanState.Durable
  private def reserveConfirmation(plan:Planned5,native:MySqlDeploymentReceiptFreshNativeOwner)(record: =>B1ResumeRecordV6):Unit =
    requirePlan(plan)
    if plan.confirmation!=null then throw new IllegalStateException("original plan confirmation is single-use")
    plan.confirmation=native.reserveConfirmation(plan,plan.durable)(record)
  private def publishConfirmation(plan:Planned5,native:MySqlDeploymentReceiptFreshNativeOwner):WindowsB1V6WitnessRecordStore.DurableV6Ack =
    requireCurrent()
    if plan.state!=PlanState.Readback || plan.confirmation==null then throw new IllegalStateException("actual commit/readback and original confirmation required")
    val ack=native.publishConfirmation(plan,plan.durable,plan.confirmation)
    plan.confirmation=null
    ack
  private def makeAttempt(phase:B1ResumeRecord.Phase,id:UUID,before:MySqlScriptSchemaSnapshot,target:MySqlScriptSchemaSnapshot,
      writer:MySqlBusinessWriterRow,targetWriter:MySqlFreshFormat3WriterTarget,context:MySqlOperationContext,inputs:MySqlScriptSchemaInputs,
      root:MySqlFormat3ReadBudget,scope:MySqlAuditBootScope,held:HeldSessionBarrier,retainedSlot:Option[B1ResumeRecord.ReceiptSlotAssertions]):B1ResumeRecord.AttemptAssertions =
    val beforeAssertion=new B1ResumeRecord.WriterAssertions(writer.bindingUuid,writer.writerEpoch,writer.ownerBootUuid.orNull,writer.leaseUntil,retainedSlot.orNull)
    val targetSlot=new B1ResumeRecord.ReceiptSlotAssertions(context.bootUuid,context.writerEpoch,context.bindingUuid,context.operationUuid,context.canonicalFingerprintSha256.toArray)
    val targetAssertion=new B1ResumeRecord.WriterAssertions(targetWriter.bindingUuid,targetWriter.writerEpoch,targetWriter.ownerBootUuid.orNull,targetWriter.leaseUntil,targetSlot)
    new B1ResumeRecord.AttemptAssertions(id,phase,context.operationUuid,scope.bootGeneration,held.connectionGenerationId,held.actualConnectionId,
      held.namespace.lockName(),MySqlFreshPrepareRules.operation(context),beforeAssertion,targetAssertion,
      MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(before,inputs,root).toResumeAssertions,
      MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(target,inputs,root).toResumeAssertions)
  private def deadline(now:LocalDateTime):LocalDateTime=now.plusNanos(session.actualLeaseDuration.toNanos)
  private def nextRecord(previous:WindowsB1V6WitnessRecordStore.DurableV6Ack,kind:B1ResumeRecord.Kind,state:B1ResumeRecord.FullStateAssertions):B1ResumeRecord =
    val old=previous.record().transition()
    new B1ResumeRecord(Math.addExact(old.sequence(),1L),hash(B1ResumeRecordCanonical.encodeSchema5Data(old)),kind,old.sourceAnchorSha256(),old.originalReceipt(),old.barrierNamespace(),state)
  private def next(previous:WindowsB1V6WitnessRecordStore.DurableV6Ack,kind:B1ResumeRecord.Kind,state:B1ResumeRecord.FullStateAssertions,
      refs:B1ResumeRecordV6.References):B1ResumeRecordV6 =
    new B1ResumeRecordV6(nextRecord(previous,kind,state),hash(B1ResumeRecordV6.encodeData(previous.record())),refs)
  private def markerData(marker:WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck,generation:Long):B1ResumeRecord.MarkerAssertions =
    new B1ResumeRecord.MarkerAssertions(WindowsMarkerIo.markerBytes(marker.bindingUuid(),marker.physicalFormat()),marker.fileIdentity().versionedIdentity(),
      marker.parentIdentity().versionedIdentity(),marker.durabilityProtocol(),generation,HexFormat.of().parseHex(marker.markerSha256()))
  private def hash(bytes:Array[Byte]):Array[Byte]=MessageDigest.getInstance("SHA-256").digest(bytes)

private[floatingpod] object MySqlDeploymentReceiptBootstrapActor:
  private enum PrepareInstallOrigin5:
    case PreGenesis
    case Cancelled(ack:WindowsB1V6WitnessRecordStore.DurableV6Ack)
  final class SuccessorPrepareInstallPermit5 private[MySqlDeploymentReceiptBootstrapActor] (
      actor:MySqlDeploymentReceiptBootstrapActor,
      source:com.wi.floatingpod.server.WindowsWorldProvisioner.DeploymentReceiptIoScope,
      originalInventory:WindowsB1V6WitnessRecordStore.CurrentInventory,
      origin:PrepareInstallOrigin5,
      before:MySqlScriptSchemaSnapshot,held:HeldSessionBarrier,plan:Planned5):
    private var consumed=false
    def requireForInstallation(actual:com.wi.floatingpod.server.WindowsWorldProvisioner.DeploymentReceiptIoScope):Unit =
      actor.requirePlan(plan)
      if consumed || (actor.successorPermit ne this) || (actual ne source) ||
          plan.phase!=B1ResumeRecord.Phase.PREPARE || (plan.before ne before) ||
          !source.currentOwner().equals(plan.record.assertions().currentOwner()) then
        throw new IllegalStateException("exact original unconsumed successor PREPARE installation permit required")
      origin match
        case PrepareInstallOrigin5.PreGenesis=>
          if plan.record.kind()!=B1ResumeRecord.Kind.GENESIS || plan.record.sequence()!=0L then
            throw new IllegalStateException("pre-genesis installation requires its original first GENESIS")
        case PrepareInstallOrigin5.Cancelled(cancelled)=>
          val original=cancelled.record().transition()
          if plan.record.kind()!=B1ResumeRecord.Kind.PREPARE_SUCCESSOR_INTENT || original.kind()!=B1ResumeRecord.Kind.ATTEMPT_CANCELLED ||
              original.assertions().latestSettlement().settledAttempt().phase()!=B1ResumeRecord.Phase.PREPARE then
            throw new IllegalStateException("successor installation requires the original cancelled PREPARE")
      actor.session.requireSuccessorInstallBefore(actor,before,held)
      originalInventory.requireRetiredOriginalExecutorsForInstallation(source)
      plan.durable.requireCurrent();plan.body.requireReference(plan.artifact.reference())
      consumed=true

  private enum CompletionKind5:
    case FreshPreparation,OrdinaryHandoff,RecoveredCertify
  private final class DurableCheck5(val plan:Planned5,val ack:WindowsB1V6WitnessRecordStore.DurableV6Ack,
      val reference:com.wi.floatingpod.server.B1Full26WitnessReference)
  private enum PlanState:
    case Planned,Durable,Committed,Readback
  final class Planned5 private[MySqlDeploymentReceiptBootstrapActor] (
      private[MySqlDeploymentReceiptBootstrapActor] val actor:MySqlDeploymentReceiptBootstrapActor,val record:B1ResumeRecord,
      val attempt:B1ResumeRecord.AttemptAssertions,var before:MySqlScriptSchemaSnapshot,var target:MySqlScriptSchemaSnapshot,val context:MySqlOperationContext
  ):
    def phase:B1ResumeRecord.Phase=attempt.phase()
    private[MySqlDeploymentReceiptBootstrapActor] var body:MySqlFull26WitnessCodec.Owner=null
    private[MySqlDeploymentReceiptBootstrapActor] var artifact:WindowsB1Full26WitnessStore.DurableFull26Ack=null
    private[MySqlDeploymentReceiptBootstrapActor] var durable:WindowsB1V6WitnessRecordStore.DurableV6Ack=null
    private[MySqlDeploymentReceiptBootstrapActor] var state=PlanState.Planned
    private[MySqlDeploymentReceiptBootstrapActor] var checked:DurableCheck5=null
    private[MySqlDeploymentReceiptBootstrapActor] var confirmation:WindowsB1V6WitnessRecordStore.ConfirmationReservation=null
  final class Completion5 private[MySqlDeploymentReceiptBootstrapActor] (
      private val actor:MySqlDeploymentReceiptBootstrapActor,private[mysql] val image:MySqlScriptSchemaSnapshot,private val plan:Planned5,
      private val durable:WindowsB1V6WitnessRecordStore.DurableV6Ack,private val marker:Option[WindowsCanonicalMarkerPublicationBarrier.MarkerPublicationAck],
      private val kind:CompletionKind5,
      private[MySqlDeploymentReceiptBootstrapActor] var admission:Option[MySqlBusinessAdmission]
  ):
    private[mysql] def requireTerminalData(actual:MySqlDeploymentReceiptBootstrapActor):Unit =
      actor.session.requireTerminalPreparationActor(actual)
      if (actual ne actor) || (actual.completed ne this) || plan.state!=PlanState.Readback || admission.isEmpty then
        throw new IllegalStateException("actual original schema5 settled completion/adoption required")
    private[mysql] def admittedForSession(actual:MySqlDeploymentReceiptSession):MySqlBusinessAdmission =
      if actual ne actor.session then throw new IllegalStateException("original schema5 completion Session required")
      requireFor(actor)
      admission.getOrElse(throw new IllegalStateException("schema5 completion has no actual business adoption"))
    private[mysql] def writerEpoch:Long=plan.attempt.targetWriter().epoch()
    private[mysql] def requireFor(actual:MySqlDeploymentReceiptBootstrapActor):Unit =
      actual.requireCurrent()
      if (actual ne actor) || (actual.completed ne this) || plan.state!=PlanState.Readback then throw new IllegalStateException("original complete schema5 proof required")
      durable.requireCurrent()
      marker.foreach(value=>actual.session.withNative(actual)(_.rebarrierMarker(value)))
    private[mysql] def isFreshPreparation:Boolean=kind==CompletionKind5.FreshPreparation
    private[mysql] def requireBootstrapAdoption(actual:MySqlDeploymentReceiptBootstrapActor):Unit =
      requireFor(actual)
      kind match
        case CompletionKind5.OrdinaryHandoff=>
          if marker.nonEmpty || plan.phase!=B1ResumeRecord.Phase.HANDOFF then throw new IllegalStateException("actual complete Ordinary HANDOFF required")
        case CompletionKind5.FreshPreparation | CompletionKind5.RecoveredCertify=>
          if marker.isEmpty || plan.phase!=B1ResumeRecord.Phase.CERTIFY then throw new IllegalStateException("actual completed CERTIFY/marker required")
  private[mysql] def allocate(session:MySqlDeploymentReceiptSession):MySqlDeploymentReceiptBootstrapActor =
    require(session!=null,"original schema5 Session partial holder required")
    new MySqlDeploymentReceiptBootstrapActor(session)
