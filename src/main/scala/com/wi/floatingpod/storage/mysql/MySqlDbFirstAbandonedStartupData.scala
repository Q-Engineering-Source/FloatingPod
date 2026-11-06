package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID

/** 仅诊断首次绑定后 Acquire/Renew 的数据差异，不构成退役许可或 writer token。 */
private[mysql] object MySqlDbFirstAbandonedStartupData:
  private val Writer = "fp_runtime_writer"
  private val Changed = Set("writer_epoch", "owner_boot_uuid", "lease_until")

  /** 严格原P链的只读诊断身份；不是writer token、SQL许可或activation确认。 */
  final case class CommittedPreparation(create:UUID, preparation:UUID, pod:UUID, attempt:UUID,
      footprint:UUID, epoch:Long)

  final case class CommittedActive(create:UUID,preparation:UUID,activation:UUID,pod:UUID,attempt:UUID,
      footprint:UUID,epoch:Long,lastOperation:UUID,lastKind:String,steps:Long,chainSha256:String,version:Long)

  /** 严格新 PREPARATION→单一 ACTIVATION 停服链；完整图只在原始比较作用域内借用。 */
  final case class CommittedCreationActivation(create:UUID,preparation:UUID,activation:UUID,pod:UUID,
      attempt:UUID,footprint:UUID,epoch:Long,lastOperation:UUID,lastKind:String,steps:Long,
      chainSha256:String,version:Long,preservedActivePods:Vector[(UUID,Long)])

  /** 实际末slot仅用于选择严格诊断；本判断不授予通过、写入或恢复权限。 */
  def hasSnapshotTail(image:MySqlScriptSchemaSnapshot,now:LocalDateTime):Boolean =
    val writer=MySqlDbFirstBootstrapProof.writer(image,now)
    writer.lastOperationUuid.exists { operation=>
      val selected=image.relation("fp_recovery_operation").rows.filter(row=>
        MySqlDbFirstInitialActivationMaterializer.uuidEquals(
          MySqlDbFirstInitialActivationMaterializer.value(row,"fp_recovery_operation","operation_uuid",image.inputs),operation))
      selected.size==1 && Set(MySqlDbFirstSchemaDefinition.InitialActivationStep,MySqlDbFirstSchemaDefinition.CheckpointStep)
        .exists(kind=>MySqlDbFirstInitialActivationMaterializer.value(selected.head,"fp_recovery_operation","step",image.inputs)==MySqlFormat3Cell.Text(kind))
    }

  /** 仅同一原Pod的activation/checkpoint完整链；不结算旧Anvil失败，不签发退休许可。 */
  def inspectCommittedActive(released:MySqlScriptSchemaSnapshot,current:MySqlScriptSchemaSnapshot,
      originalBoot:UUID,auditedOwner:Option[UUID],auditedEpoch:Long,databaseNow:LocalDateTime,
      root:MySqlFormat3ReadBudget,bootstrapReader:Option[MySqlDbFirstBootstrapSnapshotJdbc],
      nativeSource:Option[MySqlDbFirstBootstrapActor.NativePreparationSource]):CommittedActive =
    def check(ok:Boolean,reason:String):Unit =
      if !ok then throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_ACTIVE_CHAIN_"+reason)
    val actual=MySqlDbFirstBootstrapProof.writer(current,databaseNow)
    check(actual.ownerBootUuid==auditedOwner && actual.ownerBootUuid.contains(originalBoot) &&
      actual.writerEpoch==auditedEpoch && actual.leaseUntil.isBefore(databaseNow),"CURRENT_WRITER")
    val inputs=current.inputs
    def value(row:Vector[MySqlFormat3Cell],table:String,name:String)=
      MySqlDbFirstInitialActivationMaterializer.value(row,table,name,inputs)
    def matches(cell:MySqlFormat3Cell,id:UUID)=MySqlDbFirstInitialActivationMaterializer.uuidEquals(cell,id)
    def uuid(cell:MySqlFormat3Cell):UUID = cell match
      case MySqlFormat3Cell.Binary(bytes) if bytes.length==16=>
        var most=0L;var least=0L;var i=0
        while i<8 do
          most=(most<<8)|(bytes.byteAt(i).toLong&255L)
          least=(least<<8)|(bytes.byteAt(i+8).toLong&255L);i+=1
        new UUID(most,least)
      case _=>throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_ACTIVE_CHAIN_UUID")
    val operations=current.relation("fp_recovery_operation").rows
    val workspace=Math.addExact(16384L,Math.multiplyExact(operations.size.toLong,256L))
    root.reserveTemporaryMemory(workspace,"strict active chain selection workspace BEFORE allocation")
    try
      val activations=operations.filter(row=>
        value(row,"fp_recovery_operation","step")==MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.InitialActivationStep) &&
          matches(value(row,"fp_recovery_operation","boot_uuid"),originalBoot))
      check(activations.size==1,"UNIQUE_ACTIVATION")
      root.reserveRetainedMemory(1024L,"strict active-chain immutable identities BEFORE allocation")
      val activation=uuid(value(activations.head,"fp_recovery_operation","operation_uuid"))
      var result:CommittedActive=null
      MySqlDbFirstInitialActivationPersistedRead.withCommittedIdentity(current,activation,root,
        bootstrapReader,nativeSource) { (before,_,request,facts)=>
        val tip=MySqlDbFirstBootstrapProof.writer(released,databaseNow)
        val start=inspect(released,before,originalBoot,auditedOwner,auditedEpoch,databaseNow)
        check(start.writerEpoch==Math.addExact(tip.writerEpoch,1L) && facts.writerEpoch==start.writerEpoch &&
          request.tag.bootId==originalBoot && request.tag.operationId==activation &&
          request.ownerUuid.isEmpty && request.capturedStateVersion==0L,"ORIGINAL_ACTIVATION")
        val intents=released.relation("physical_recovery_intent").rows.filter(row=>
          matches(value(row,"physical_recovery_intent","pod_uuid"),request.podUuid) &&
            matches(value(row,"physical_recovery_intent","create_operation_uuid"),request.creationOperationUuid) &&
            matches(value(row,"physical_recovery_intent","attempt_uuid"),request.attemptUuid))
        check(intents.size==1,"ORIGINAL_PREPARED_INTENT")
        val intent=intents.head
        check(value(intent,"physical_recovery_intent","phase")==MySqlFormat3Cell.Text("PREPARED") &&
          value(intent,"physical_recovery_intent","origin")==MySqlFormat3Cell.Text("DEBUG_ITEM_CREATE"),"PREPARED_ORIGIN")
        val preparation=uuid(value(intent,"physical_recovery_intent","prepare_operation_uuid"))
        val footprint=uuid(value(intent,"physical_recovery_intent","target_footprint_uuid"))
        check(tip.lastOperationUuid.contains(preparation),"PREDECESSOR_PREPARATION_SLOT")
        val chain=MySqlDbFirstRecallPersistedRead.verifyStopSnapshotReceiptSegment(current,before,root,
          request.podUuid,activation,bootstrapReader,nativeSource)
        val domains=current.relation("floating_pod_state").rows.filter(row=>matches(value(row,"floating_pod_state","uuid"),request.podUuid))
        check(domains.size==1,"CURRENT_DOMAIN")
        val domain=domains.head
        val version=value(domain,"floating_pod_state","state_version") match
          case MySqlFormat3Cell.SignedInteger(v) if v>=1L=>v
          case _=>throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_ACTIVE_CHAIN_VERSION")
        check(value(domain,"floating_pod_state","owner_uuid")==MySqlFormat3Cell.SqlNull &&
          value(domain,"floating_pod_state","is_active")==MySqlFormat3Cell.SignedInteger(1) &&
          value(domain,"floating_pod_state","operating_mode")==MySqlFormat3Cell.Text("WORLD_IDLE"),"CURRENT_ACTIVE_STATE")
        val last=actual.lastOperationUuid.getOrElse(throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_ACTIVE_CHAIN_SLOT"))
        val tail=operations.filter(row=>matches(value(row,"fp_recovery_operation","operation_uuid"),last))
        check(tail.size==1,"UNIQUE_TAIL")
        val lastKind=value(tail.head,"fp_recovery_operation","step") match
          case MySqlFormat3Cell.Text(kind)=>kind
          case _=>throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_ACTIVE_CHAIN_KIND")
        result=CommittedActive(request.creationOperationUuid,preparation,activation,request.podUuid,request.attemptUuid,
          footprint,actual.writerEpoch,last,lastKind,chain.steps,chain.sha256,version)
      }
      check(result!=null,"NO_RESULT")
      result
    finally root.releaseTemporaryMemory(workspace,"strict active-chain selection workspace retired")

  /**
   * 只接受 base→一次 Acquire→新 P→同一 Pod 的单一 A→current；P/A 完整图只在同一原始预算作用域内借用。
   * 这是一项受限诊断/请求校验，不会产生 retirement 或 SQL 执行资格。
   */
  def inspectCommittedCreationActivation(released:MySqlScriptSchemaSnapshot,current:MySqlScriptSchemaSnapshot,
      originalBoot:UUID,auditedOwner:Option[UUID],auditedEpoch:Long,databaseNow:LocalDateTime,
      root:MySqlFormat3ReadBudget,bootstrapReader:Option[MySqlDbFirstBootstrapSnapshotJdbc]=None,
      nativeSource:Option[MySqlDbFirstBootstrapActor.NativePreparationSource]=None):CommittedCreationActivation =
    def check(ok:Boolean,reason:String):Unit =
      if !ok then throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_CREATION_ACTIVATION_"+reason)
    require(bootstrapReader.isEmpty || nativeSource.isEmpty,"one actual creation/activation comparison source required")
    require(released!=null && current!=null && root!=null && !root.isClosed && !root.isScopedReadBudget &&
      (released.inputs eq current.inputs) && current.inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27 &&
      current.allocationBudget.sharesAllocationLedger(root) && !current.allocationBudget.isClosed &&
      ((current.allocationBudget eq root) || current.allocationBudget.isSnapshotReadChildOf(root) ||
        bootstrapReader.nonEmpty || nativeSource.nonEmpty),
      "actual Full27 creation/activation images and open original root required")
    bootstrapReader.foreach(_.requirePreparationComparisonSource(current,root))
    nativeSource.foreach(_.requireComparisonSource(current,root))
    root.requireScriptDataInputs(current.inputs)
    val actual=MySqlDbFirstBootstrapProof.writer(current,databaseNow)
    check(actual.ownerBootUuid==auditedOwner && actual.ownerBootUuid.contains(originalBoot) &&
      actual.writerEpoch==auditedEpoch && actual.leaseUntil.isBefore(databaseNow),"CURRENT_WRITER")
    val tailOperation=actual.lastOperationUuid.getOrElse(
      throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_CREATION_ACTIVATION_CURRENT_TAIL_MISSING"))
    val inputs=current.inputs
    val O="fp_recovery_operation"
    val D="floating_pod_state"
    def value(row:Vector[MySqlFormat3Cell],table:String,name:String)=
      MySqlDbFirstInitialActivationMaterializer.value(row,table,name,inputs)
    def matches(cell:MySqlFormat3Cell,id:UUID)=MySqlDbFirstInitialActivationMaterializer.uuidEquals(cell,id)
    def uuid(cell:MySqlFormat3Cell,reason:String):UUID=cell match
      case MySqlFormat3Cell.Binary(bytes) if bytes.length==16=>
        var most=0L;var least=0L;var i=0
        while i<8 do
          most=(most<<8)|(bytes.byteAt(i).toLong&255L)
          least=(least<<8)|(bytes.byteAt(i+8).toLong&255L);i+=1
        new UUID(most,least)
      case _=>throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_CREATION_ACTIVATION_"+reason)
    val operations=current.relation(O).rows
    val domainRows=released.relation(D).rows
    val graphRows=current.relations.iterator.map(_.rows.size.toLong).sum+domainRows.size.toLong
    val workspace=Math.addExact(16384L,Math.addExact(Math.multiplyExact(operations.size.toLong,256L),
      Math.multiplyExact(graphRows,128L)))
    root.reserveTemporaryMemory(workspace,"exact new preparation/activation inventory BEFORE allocation")
    try
      val activations=operations.filter(row=>
        value(row,O,"step")==MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.InitialActivationStep) &&
          matches(value(row,O,"boot_uuid"),originalBoot))
      check(activations.size==1,"UNIQUE_ACTIVATION_FOR_FAILED_BOOT")
      val activation=uuid(value(activations.head,O,"operation_uuid"),"ACTIVATION_UUID")
      check(activation==tailOperation,"CURRENT_TAIL_IS_NOT_ACTIVATION")
      var result:CommittedCreationActivation=null
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        MySqlDbFirstInitialActivationPersistedRead.withCommittedIdentity(current,activation,root,
          bootstrapReader,nativeSource) { (activationBefore,_,activationRequest,activationFacts)=>
          check(activationRequest.tag.bootId==originalBoot && activationRequest.tag.operationId==activation &&
            activationFacts.writerEpoch==auditedEpoch && activationRequest.capturedStateVersion==0L &&
            activationRequest.ownerUuid.isEmpty,"ACTIVATION_IDENTITY")
          val intentRows=activationBefore.relation("physical_recovery_intent").rows.filter(row=>
            matches(value(row,"physical_recovery_intent","binding_uuid"),activationRequest.bindingUuid) &&
              matches(value(row,"physical_recovery_intent","pod_uuid"),activationRequest.podUuid) &&
              matches(value(row,"physical_recovery_intent","attempt_uuid"),activationRequest.attemptUuid))
          check(intentRows.size==1,"UNIQUE_PREPARED_INTENT")
          val intent=intentRows.head
          check(value(intent,"physical_recovery_intent","phase")==MySqlFormat3Cell.Text("PREPARED") &&
            value(intent,"physical_recovery_intent","origin")==MySqlFormat3Cell.Text("DEBUG_ITEM_CREATE") &&
            matches(value(intent,"physical_recovery_intent","create_operation_uuid"),activationRequest.creationOperationUuid),
            "PREPARED_INTENT_IDENTITY")
          val preparation=uuid(value(intent,"physical_recovery_intent","prepare_operation_uuid"),"PREPARATION_UUID")
          val footprint=uuid(value(intent,"physical_recovery_intent","target_footprint_uuid"),"FOOTPRINT_UUID")
          val create=activationRequest.creationOperationUuid
          val pod=activationRequest.podUuid
          check(preparation!=activation && create!=preparation && create!=activation,"ALIASED_OPERATION_IDENTITIES")
          val preparationRows=operations.filter(row=>matches(value(row,O,"operation_uuid"),preparation))
          check(preparationRows.size==1,"UNIQUE_PREPARATION_RAW")
          val prepRaw=preparationRows.head
          check(value(prepRaw,O,"step")==MySqlFormat3Cell.Text("REARM") &&
            value(prepRaw,O,"operation_state")==MySqlFormat3Cell.Text("COMMITTED") &&
            matches(value(prepRaw,O,"boot_uuid"),originalBoot),"PREPARATION_RAW_STATE")

          val consumePreparation:(MySqlScriptSchemaSnapshot,MySqlScriptSchemaSnapshot,
            MySqlDbFirstInitialPreparationData,MySqlDbFirstInitialPreparationFacts)=>Unit =
            (preparationBefore,preparationTarget,preparationData,preparationFacts)=>
              val request=preparationData.request
              check(preparationData.tag.bootId==originalBoot && preparationData.tag.operationId==preparation &&
                preparationFacts.writerEpoch==auditedEpoch && preparationData.tag.bootGeneration==activationRequest.tag.bootGeneration &&
                request.expectedBinding.bindingUuid==activationRequest.bindingUuid &&
                request.createOperationUuid==create && request.podUuid==pod &&
                request.attemptUuid==activationRequest.attemptUuid && request.footprintUuid==footprint,
                "PREPARATION_CROSS_BINDING")
              val footprintMatches=value(intent,"physical_recovery_intent","target_footprint_uuid") match
                case MySqlFormat3Cell.Binary(bytes)=>bytes.equalsUuid(request.footprintUuid)
                case _=>false
              check(footprintMatches,"PREPARATION_FOOTPRINT")
              val initial=inspect(released,preparationBefore,originalBoot,auditedOwner,auditedEpoch,databaseNow)
              val baseWriter=MySqlDbFirstBootstrapProof.writer(released,databaseNow)
              val acquired=MySqlDbFirstBootstrapProof.writer(preparationBefore,databaseNow)
              check(acquired.bindingUuid==baseWriter.bindingUuid && acquired.ownerBootUuid.contains(originalBoot) &&
                acquired.writerEpoch==Math.addExact(baseWriter.writerEpoch,1L) &&
                acquired.lastOperationUuid==baseWriter.lastOperationUuid &&
                acquired.lastFingerprint==baseWriter.lastFingerprint && initial.writerEpoch==auditedEpoch,
                "BASE_TO_PREPARATION_BEFORE_NOT_SINGLE_ACQUIRE")
              check(MySqlDbFirstRecallMaterializer.sameReceiptWithLeaseContinuation(
                preparationTarget.relations,activationBefore.relations,inputs),
                "PREPARATION_TO_ACTIVATION_BEFORE_NOT_ADJACENT")
              val segment=MySqlDbFirstRecallPersistedRead.verifyStopSnapshotReceiptSegment(current,activationBefore,root,
                pod,activation,bootstrapReader,nativeSource)
              check(segment.steps==1L && segment.sha256.length==64,"ACTIVATION_SEGMENT_NOT_SINGLE_STEP")
              val stateRows=current.relation(D).rows.filter(row=>matches(value(row,D,"uuid"),pod))
              check(stateRows.size==1,"CURRENT_NEW_DOMAIN_NOT_UNIQUE")
              val state=stateRows.head
              val version=value(state,D,"state_version") match
                case MySqlFormat3Cell.SignedInteger(v) if v==1L=>v
                case _=>throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_CREATION_ACTIVATION_NEW_ACTIVE_VERSION")
              check(matches(value(state,D,"binding_uuid"),activationRequest.bindingUuid) &&
                value(state,D,"owner_uuid")==MySqlFormat3Cell.SqlNull &&
                value(state,D,"is_active")==MySqlFormat3Cell.SignedInteger(1) &&
                value(state,D,"operating_mode")==MySqlFormat3Cell.Text("WORLD_IDLE"),"CURRENT_NEW_DOMAIN_STATE")
              val previousActiveCount=domainRows.count(row=>
                value(row,D,"is_active")==MySqlFormat3Cell.SignedInteger(1))
              root.reserveRetainedMemory(512L+128L*previousActiveCount.toLong,
                "preserved active Pod identities and combined chain metadata BEFORE retention")
              val previousActive=domainRows.filter(row=>
                value(row,D,"is_active")==MySqlFormat3Cell.SignedInteger(1)).map { row=>
                val oldPod=uuid(value(row,D,"uuid"),"PRESERVED_ACTIVE_UUID")
                val oldVersion=value(row,D,"state_version") match
                  case MySqlFormat3Cell.SignedInteger(v) if v>=1L=>v
                  case _=>throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_CREATION_ACTIVATION_PRESERVED_ACTIVE_VERSION")
                check(oldPod!=pod,"NEW_POD_ALIASES_PRESERVED_ACTIVE_POD")
                val retained=current.relation(D).rows.filter(candidate=>matches(value(candidate,D,"uuid"),oldPod))
                check(retained.size==1 && retained.head==row,"PRESERVED_ACTIVE_DOMAIN_CHANGED")
                oldPod->oldVersion
              }
              check(previousActive.nonEmpty,"NO_PRESERVED_ACTIVE_POD")
              val pFingerprint=value(prepRaw,O,"operation_fingerprint") match
                case MySqlFormat3Cell.Binary(bytes) if bytes.length==32=>bytes
                case _=>throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_CREATION_ACTIVATION_PREPARATION_FINGERPRINT")
              val chain=java.security.MessageDigest.getInstance("SHA-256")
              chain.update("FP_DB_FIRST_NEW_PREPARATION_ACTIVATION_V1".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
              def updateUuid(id:UUID):Unit =
                var shift=56
                while shift>=0 do
                  chain.update((id.getMostSignificantBits>>>shift).toByte);shift-=8
                shift=56
                while shift>=0 do
                  chain.update((id.getLeastSignificantBits>>>shift).toByte);shift-=8
              updateUuid(preparation)
              var index=0
              while index<pFingerprint.length do
                chain.update(pFingerprint.byteAt(index));index+=1
              var hex=0
              while hex<segment.sha256.length do
                val high=Character.digit(segment.sha256.charAt(hex),16)
                val low=Character.digit(segment.sha256.charAt(hex+1),16)
                check(high>=0 && low>=0,"ACTIVATION_SEGMENT_DIGEST")
                chain.update(((high<<4)|low).toByte)
                hex+=2
              updateUuid(activation)
              updateUuid(tailOperation)
              val chainHex=java.util.HexFormat.of().formatHex(chain.digest())
              result=CommittedCreationActivation(create,preparation,activation,pod,activationRequest.attemptUuid,
                footprint,actual.writerEpoch,tailOperation,MySqlDbFirstSchemaDefinition.InitialActivationStep,
                2L,chainHex,version,previousActive)
          bootstrapReader match
            case Some(reader)=>MySqlDbFirstInitialPreparationPersistedRead.withCommittedBootstrapIdentity(
              reader,current,preparation,root)(consumePreparation)
            case None=>nativeSource match
              case Some(source)=>MySqlDbFirstInitialPreparationPersistedRead.withCommittedNativeIdentity(
                source,current,preparation,root)(consumePreparation)
              case None=>MySqlDbFirstInitialPreparationPersistedRead.withCommittedIdentity(
                current,preparation,root)(consumePreparation)
        }
      }
      check(result!=null,"NO_RESULT")
      result
    catch
      case diagnostic:MySqlDbFirstAbandonedStartupInspection=>throw diagnostic
      case cause:SQLDataException=>
        throw new MySqlDbFirstAbandonedStartupInspection("UNSUPPORTED_NOT_EXACT_CREATION_ACTIVATION_CHAIN",cause)
    finally root.releaseTemporaryMemory(workspace,"exact new preparation/activation inventory retired")

  def inspectCommittedPreparation(released:MySqlScriptSchemaSnapshot,current:MySqlScriptSchemaSnapshot,
      originalBoot:UUID,auditedOwner:Option[UUID],auditedEpoch:Long,databaseNow:LocalDateTime,
      root:MySqlFormat3ReadBudget,
      bootstrapReader:Option[MySqlDbFirstBootstrapSnapshotJdbc]=None,
      nativeSource:Option[MySqlDbFirstBootstrapActor.NativePreparationSource]=None):CommittedPreparation =
    require(bootstrapReader.isEmpty || nativeSource.isEmpty,"one original committed preparation source required")
    def check(condition:Boolean,reason:String):Unit =
      if !condition then throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_PREPARATION_"+reason)
    val actual=MySqlDbFirstBootstrapProof.writer(current,databaseNow)
    val operation=actual.lastOperationUuid.getOrElse(
      throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_PREPARATION_SLOT_MISSING"))
    var result:CommittedPreparation=null
    val compare:(MySqlScriptSchemaSnapshot,MySqlScriptSchemaSnapshot,
      MySqlDbFirstInitialPreparationData,MySqlDbFirstInitialPreparationFacts)=>Unit = (before,target,data,facts)=>
      // 该精确事故只跨一次Acquire；不同于历史unchanged-business端点诊断允许多次正常启动。
      val tail=MySqlDbFirstBootstrapProof.writer(released,databaseNow)
      val initial=inspect(released,before,originalBoot,auditedOwner,auditedEpoch,databaseNow)
      check(initial.writerEpoch==Math.addExact(tail.writerEpoch,1L),"ACQUIRE_EPOCH")
      check(data.tag.bootId==originalBoot && data.tag.operationId==operation &&
        facts.writerEpoch==initial.writerEpoch,"ORIGINAL_IDENTITY")
      check(actual.ownerBootUuid==auditedOwner && actual.ownerBootUuid.contains(originalBoot) &&
        actual.writerEpoch==auditedEpoch,"AUDIT_CHANGED")
      check(actual.leaseUntil.isBefore(databaseNow),"LEASE_NOT_EXPIRED")
      val expected=MySqlDbFirstBootstrapProof.writer(target,databaseNow)
      check(!actual.leaseUntil.isBefore(expected.leaseUntil),"LEASE_REGRESSED")
      check(actual.copy(leaseUntil=expected.leaseUntil)==expected,"WRITER_CHANGED")
      check(current.inputs.canonicalOrder.filterNot(_.table==Writer).forall(shape=>
        current.relation(shape.table)==target.relation(shape.table)),"NON_WRITER_DATA")
      root.reserveRetainedMemory(512L,"verified committed preparation diagnostic metadata BEFORE retention")
      val request=data.request
      result=CommittedPreparation(request.createOperationUuid,operation,request.podUuid,
        request.attemptUuid,request.footprintUuid,actual.writerEpoch)
    bootstrapReader match
      case Some(reader)=>MySqlDbFirstInitialPreparationPersistedRead.withCommittedBootstrapIdentity(
        reader,current,operation,root)(compare)
      case None=>nativeSource match
        case Some(source)=>MySqlDbFirstInitialPreparationPersistedRead.withCommittedNativeIdentity(
          source,current,operation,root)(compare)
        case None=>MySqlDbFirstInitialPreparationPersistedRead.withCommittedIdentity(current,operation,root)(compare)
    require(result!=null,"verified preparation comparison did not return metadata")
    result

  def inspect(released: MySqlScriptSchemaSnapshot, current: MySqlScriptSchemaSnapshot,
      originalBoot: UUID, auditedOwner: Option[UUID], auditedEpoch: Long,
      databaseNow: LocalDateTime): MySqlBusinessWriterRow =
    def check(condition: Boolean, reason: String): Unit =
      if !condition then throw new MySqlDbFirstAbandonedStartupInspection("REJECTED_" + reason)
    check(released != null && current != null && originalBoot != null && databaseNow != null, "INPUT")
    check((released.inputs eq current.inputs) && released.schema == current.schema &&
      released.connection.serverUuid == current.connection.serverUuid, "DATABASE_IDENTITY")
    val tables = current.inputs.canonicalOrder.map(_.table)
    check(tables.size == 27 && released.relations.map(_.table) == tables &&
      current.relations.map(_.table) == tables, "FULL27_LAYOUT")
    tables.filterNot(_ == Writer).foreach { table =>
      check(released.relation(table) == current.relation(table), "NON_WRITER_DATA")
    }
    val before = MySqlDbFirstBootstrapProof.writer(released, databaseNow)
    val actual = MySqlDbFirstBootstrapProof.writer(current, databaseNow)
    check(before.ownerBootUuid.isEmpty && before.writerEpoch < Long.MaxValue, "RELEASE_TAIL")
    // 历史 RELEASE 后可已有多次正常 Acquire/Release；比较的是当前端点 DATA，不证明中间事件历史。
    // 当前 epoch 仍须与本次 audit 精确相同；不降 epoch、不重 Acquire，后续仅原完整 before/target CAS。
    check(actual.bindingUuid == before.bindingUuid && actual.ownerBootUuid.contains(originalBoot) &&
      actual.writerEpoch > before.writerEpoch, "ACQUIRE_IDENTITY")
    check(actual.ownerBootUuid == auditedOwner && actual.writerEpoch == auditedEpoch, "AUDIT_CHANGED")
    check(actual.leaseUntil.isBefore(databaseNow), "LEASE_NOT_EXPIRED")
    check(actual.leaseUntil.isAfter(before.leaseUntil), "LEASE_NOT_AFTER_RELEASE_TAIL")
    check(actual.lastOperationUuid == before.lastOperationUuid &&
      actual.lastFingerprint == before.lastFingerprint, "COMMIT_SLOT_CHANGED")
    val columns = current.inputs.forTable(Writer).columns
    val oldRow = released.relation(Writer).rows
    val newRow = current.relation(Writer).rows
    check(oldRow.size == 1 && newRow.size == 1, "WRITER_ROW_COUNT")
    columns.indices.filterNot(i => Changed(columns(i).name)).foreach { i =>
      check(oldRow.head(i) == newRow.head(i), "WRITER_COLUMN_CHANGED")
    }
    actual

  def retirementTarget(before: MySqlScriptSchemaSnapshot, now: LocalDateTime,
      root: MySqlFormat3ReadBudget): MySqlScriptSchemaSnapshot =
    val writer = MySqlDbFirstBootstrapProof.writer(before, now)
    require(writer.ownerBootUuid.nonEmpty && writer.leaseUntil.isBefore(now), "exact expired startup writer required")
    MySqlDbFirstBootstrapWriterGraph.buildTarget(before, before.inputs, root,
      writer.copy(ownerBootUuid = None, leaseUntil = now.minusNanos(1000L)))

  def requireRetirementTarget(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      now: LocalDateTime): Unit =
    require((before.inputs eq target.inputs) && before.schema == target.schema &&
      before.connection == target.connection && before.observationScope == target.observationScope &&
      before.maxAllowedPacketBytes == target.maxAllowedPacketBytes &&
      before.relations.map(_.table) == target.relations.map(_.table), "exact retirement Full27 headers required")
    val old = MySqlDbFirstBootstrapProof.writer(before, now)
    val result = MySqlDbFirstBootstrapProof.writer(target, now)
    require(old.ownerBootUuid.nonEmpty && old.leaseUntil.isBefore(now) &&
      result == old.copy(ownerBootUuid = None, leaseUntil = now.minusNanos(1000L)),
      "retirement only clears exact expired owner and lease, retaining epoch and commit slot")
    before.relations.foreach { relation =>
      val next = target.relation(relation.table)
      if relation.table != Writer then require(relation == next, "retirement cannot change non-W DATA")
      else
        before.inputs.forTable(Writer).columns.zipWithIndex.foreach { (column, i) =>
          if column.name != "owner_boot_uuid" && column.name != "lease_until" then
            require(relation.rows.head(i) == next.rows.head(i), "retirement cannot change other W columns")
        }
    }

/** 只允许固定原因及显式公共身份进入诊断，绝不打印 SQL/连接配置或任意异常 message。 */
private[floatingpod] final class MySqlDbFirstAbandonedStartupInspection private[mysql](
    val safeSummary: String,cause:Throwable)
    extends SQLDataException("Abandoned startup writer inspection stopped before mutation",cause):
  private[mysql] def this(safeSummary:String)=this(safeSummary,null)
