package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.entity.{DbFirstCaptureAllocation, DbFirstPodSnapshot, DbFirstPodSnapshotTest}

import com.wi.floatingpod.domain.{RecallRefusalReason,RecallRequestNamespace}
import com.wi.floatingpod.server.{DbFirstBootRecallRequestCustody,MainDbFirstReadAdmission}
import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.server.io.{IoOperationTag, IoSubmission}
import java.io.{ByteArrayInputStream, DataInputStream, IOException, InputStream}
import java.sql.{SQLDataException, SQLNonTransientConnectionException}
import java.util.UUID
import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.function.Executable

/** 同一生产core的真实actor/root测试；None issuer只表示DATA内存seam，无Session/Boot/SQL资格。 */
final class MySqlRetainedPlanReservationCoreTest:
  private def id(value: Long): UUID = new UUID(0L, value)

  /** 完整27表目标的显式synthetic正例预算；共享默认值和source limits保持原样。 */
  private def dbFirstMaterializationInputs(snapshotBytes:Long=131072L): MySqlScriptSchemaInputs =
    val base=DeploymentReceiptStaticTestData.defaultProfile()
    val profile=MySqlDeploymentReceiptSchemaProfile.inputs(base.schema,base.accounts,base.sourceLimitProfile,
      base.publicationBudget.copy(snapshotBytes=snapshotBytes),base.deploymentBudget,base.quotaMaximumRecords,
      base.trustedAdministrativeProxyRows)
    MySqlScriptSchemaInputs.createDbFirstFull27(MySqlDbFirstSchemaProfile.render(profile))

  private def assertDbFirstMaterializationSizes(result: MySqlDbFirstRecallMaterialization): Unit =
    val cap=result.target.inputs.budget.snapshotBytes
    assertEquals(131072L,cap)
    assertTrue(result.scope.length.toLong<=cap)
    assertTrue(result.fingerprint.length.toLong<=cap)
    assertTrue(result.context.length.toLong<=cap)
    assertTrue(result.preimage.length.toLong<=cap)
    assertTrue(result.projectedTarget.length.toLong<=cap)
    assertTrue(result.witness.length<=65536,"compact witness must still fit the original 64KiB field cap")
    val targetLength=result.target.canonicalByteLength
    assertTrue(targetLength>65536L,"complete target must demonstrate why the original fixture cannot fit")
    assertTrue(targetLength<=cap)
    println(s"DB-first synthetic materialization sizes: scope=${result.scope.length}, preimage=${result.preimage.length}, targetProjection=${result.projectedTarget.length}, fingerprint=${result.fingerprint.length}, context=${result.context.length}, witness=${result.witness.length}, completeTarget=$targetLength, snapshotCap=$cap")

  @Test def emptyDataCoreCannotChangeGenerationWithoutActualServiceRecoveryOwner():Unit =
    val f=new Fixture(1)
    try
      f.onWorker {
        val before=f.shared.accountedBytes
        val original=f.identity
        assertThrows(classOf[IllegalArgumentException],()=>
          f.core.transferDbFirstServiceIdentity(null,null,null,original,original.copy(connectionGeneration=2L)))
        assertEquals(before,f.shared.accountedBytes)
        assertTrue(f.core.observation.plans.isEmpty)
      }
      val plan=f.reserve(997)
      assertEquals(1L,plan.tag.connectionGeneration,"denied transfer must preserve original identity")
      f.core.submitted(plan,IoSubmission.RejectedCapacity)
      f.core.cancelExplicitlyRejected(plan.originalRetainedHandle.get)
      assertEquals(0L,f.shared.accountedBytes)
      assertTrue(f.core.observation.plans.isEmpty)
    finally f.close()

  @Test def originalRecallWriterLeaseWindowRequiresExactWriterAndActualTimeWithoutExtension():Unit =
    val inputs=dbFirstMaterializationInputs()
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var source:MySqlScriptSchemaSnapshot=null
    var primary:Throwable=null
    try
      source=dbFirstBefore(f,inputs)
      f.onWorker {
        val lease=java.time.LocalDateTime.of(2026,10,29,13,0)
        val before=f.shared.accountedBytes
        def duration(binding:UUID=id(503),boot:UUID=id(1),epoch:Long=3L,
            original:java.time.LocalDateTime=lease,at:java.time.LocalDateTime=lease.minusSeconds(10)) =
          MySqlDbFirstSnapshotJdbc.observedRecallLeaseNanos(source,binding,boot,epoch,original,at)
        assertEquals(java.time.Duration.ofSeconds(10).toNanos,duration())
        assertEquals(MySqlSchemaAuditor.MaxReceiptAgeNanos,duration(at=lease.minusHours(1)))
        assertEquals(java.time.Duration.ofSeconds(10).toNanos,duration(original=lease.minusSeconds(1)),
          "same original epoch may retain a renewed lease, but the original floor is never extended")
        assertThrows(classOf[SQLDataException],()=>duration(binding=id(999)))
        assertThrows(classOf[SQLDataException],()=>duration(boot=id(999)))
        assertThrows(classOf[SQLDataException],()=>duration(epoch=4L))
        assertThrows(classOf[SQLDataException],()=>duration(original=lease.plusSeconds(1)))
        assertThrows(classOf[SQLDataException],()=>duration(at=lease.minusSeconds(10).plusNanos(1)))
        assertThrows(classOf[java.sql.SQLException],()=>duration(at=lease))
        assertThrows(classOf[java.sql.SQLException],()=>duration(at=lease.plusSeconds(1)))
        val window=MySqlAdmissionWindow(100L,duration())
        assertTrue(window.remainingAt(100L)>0L)
        assertEquals(0L,window.remainingAt(99L),"backwards monotonic observation cannot extend a lease")
        assertEquals(0L,window.remainingAt(100L+duration()),"exact expiry cannot be current")
        assertEquals(before,f.shared.accountedBytes,"scalar lease checks cannot change original DATA ownership")
        assertTrue(f.core.observation.plans.isEmpty,"DATA window is not an Entry, confirmation or writer admission")
      }
    catch case cause:Throwable=>
      primary=cause
      throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(
      ()=>if source!=null then source.allocationBudget.closeRootReadBudget(),()=>f.close()))

  @Test def failedRecallEntryCannotSettleFromCallerScalarOrPrecommitFailure():Unit =
    val inputs=dbFirstMaterializationInputs()
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    try
      val reservation=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(978)))
      val captured=f.core.captureDbFirstSnapshot(reservation)(a=>
        DbFirstPodSnapshotTest.captureForCore(reservation.tag.operationId,a,100))
      val request=f.core.dbFirstRecallRequest(reservation,captured)
      val counterfeit=MySqlDbFirstSession.FailedRecallReadback(reservation.tag,request.podUuid,"0"*64,0L)
      val unregisteredDrop=MySqlDbFirstRecallDropReceipt.issue(f.core,reservation.handle)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRetainedDbFirstDrop(unregisteredDrop,null))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.handoffRetainedDbFirstMainDrop(unregisteredDrop,null))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRetainedDbFirstReleaseSubmission(unregisteredDrop,null))
      assertFalse(unregisteredDrop.retainedEntryRetired)
      assertThrows(classOf[IllegalArgumentException],()=>unregisteredDrop.markRetainedEntryRetired())
      val ordinaryConfirmation=MySqlDbFirstRecallConfirmation.issue(f.core,reservation.handle,request)
      assertFalse(ordinaryConfirmation.isRetained)
      assertThrows(classOf[IllegalArgumentException],()=>ordinaryConfirmation.detachRetainedQualification(null))
      assertFalse(ordinaryConfirmation.isRetained,"detaching a capability cannot change its mode")
      assertTrue(f.core.failedDbFirstRecallSqlReadback(reservation).isEmpty)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.retainedDbFirstRecallFailure(reservation,null))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.claimRetainedDbFirstRecallConfirmation(reservation,null))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordRetainedDbFirstMainDrop(null,null,null))
      assertThrows(classOf[IllegalArgumentException],()=>
        MySqlDbFirstRecallDropReceipt.issueRetained(f.core,reservation.handle,null,null,null))
      f.core.requireDbFirstRecallSubmission(reservation)
      val cause=new IOException("original precommit failure remains unresolved")
      f.onWorker {
        val runner=f.core.beginDbFirstRecall(reservation)
        val before=f.shared.accountedBytes
        assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRetainedDbFirstDrop(null,null))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRetainedDbFirstDrop(unregisteredDrop,null))
        assertThrows(classOf[IllegalArgumentException],()=>
          f.core.settleFailedDbFirstRecallReadback(runner,counterfeit))
        assertThrows(classOf[IllegalArgumentException],()=>
          f.core.recordFailedDbFirstRecallQualification(null,runner,null))
        f.core.failDbFirstRecall(runner,cause)
        assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRetainedDbFirstDrop(unregisteredDrop,null))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.beginRetainedDbFirstDrop(unregisteredDrop,null))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.detachRetainedDbFirstPayload(unregisteredDrop,null))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.retireRetainedDbFirstDrop(unregisteredDrop))
        assertThrows(classOf[IllegalArgumentException],()=>runner.retireFailedEntryPayload(counterfeit,cause))
        assertSame(request,runner.request,"rejected retirement cannot detach original failed request")
        assertFalse(unregisteredDrop.retainedEntryRetired)
        assertThrows(classOf[IllegalArgumentException],()=>
          f.core.settleFailedDbFirstRecallReadback(runner,counterfeit))
        assertThrows(classOf[IllegalArgumentException],()=>
          f.core.recordFailedDbFirstRecallQualification(null,runner,null))
        assertThrows(classOf[IllegalArgumentException],()=>
          runner.requireFailedRecallSqlSettlement(counterfeit,cause))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.failedDbFirstRecallSqlReadback(reservation))
        assertEquals(before,f.shared.accountedBytes,"rejected scalar must not allocate or refund original Entry")
        assertFalse(runner.resourcesRetired)
        assertFalse(runner.failedSqlResourcesRetired)
      }
      assertTrue(f.core.failedDbFirstRecallSqlReadback(reservation).isEmpty)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.retainedDbFirstRecallFailure(reservation,null))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.claimRetainedDbFirstRecallConfirmation(reservation,null))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordRetainedDbFirstMainDrop(null,null,null))
      assertThrows(classOf[IllegalArgumentException],()=>
        MySqlDbFirstRecallDropReceipt.issueRetained(f.core,reservation.handle,null,null,null))
      assertSame(cause,f.core.observation.plans.find(_.tag==reservation.tag).get.cause.get)
      val counterfeitReservation=new MySqlDbFirstCaptureReservation(f.core,reservation.handle,f.limits)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.failedDbFirstRecallSqlReadback(counterfeitReservation))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.claimDbFirstRecallConfirmation(reservation,null))
    finally f.close()

  @Test def dbFirstWorkerMaterializesSingleOriginalOAndCompleteFull27WithoutRetainingVerificationScratch(): Unit =
    for shutdown<-Vector(false,true) do
      val inputs=dbFirstMaterializationInputs()
      val f=new Fixture(1,dbFirstInputs=Some(inputs))
      var source: MySqlScriptSchemaSnapshot=null
      var primary: Throwable=null
      try
        val reservation=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(980)))
        val captured=f.core.captureDbFirstSnapshot(reservation) { a=>
          if shutdown then DbFirstPodSnapshotTest.captureShutdownForCore(reservation.tag.operationId,a,100)
          else DbFirstPodSnapshotTest.captureForCore(reservation.tag.operationId,a,100)
        }
        val request=f.core.dbFirstRecallRequest(reservation,captured)
        source=dbFirstBefore(f,inputs)
        val planned=java.time.LocalDateTime.of(2026,10,29,12,0)
        var result: MySqlDbFirstRecallMaterialization=null
        assertThrows(classOf[IllegalArgumentException],()=>f.core.materializeDbFirstRecall(reservation,request,source,91L,id(981),planned,planned.plusSeconds(1)))
        val counterfeit=new MySqlDbFirstCaptureReservation(f.core,reservation.handle,f.limits)
        assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure(
          f.core.materializeDbFirstRecall(counterfeit,request,source,91L,id(981),planned,planned.plusSeconds(1))))
        f.core.requireDbFirstRecallSubmission(reservation)
        f.onWorker {
          val runner=f.core.beginDbFirstRecall(reservation)
          result=f.core.materializeExecutingDbFirstRecall(runner,source,91L,id(981),planned,planned.plusSeconds(1))
          val root=f.core.validationBudget(reservation.handle)
          assertDbFirstMaterializationSizes(result)
          assertDbFirstWitnessCommitmentsAndOriginalTargetGuard(result,inputs,root)
          val retained=f.shared.accountedBytes
          (0 until 3).foreach(_=>result.verify())
          assertEquals(retained,f.shared.accountedBytes,"comparison child must refund every recomputation")
          assertEquals(27,result.target.relations.size)
          assertEquals(0L,number(result.heldReservation,"fp_recovery_reservation","reservation_revision",inputs))
          assertEquals(1L,number(result.target.relation("fp_recovery_reservation").rows.head,"fp_recovery_reservation","reservation_revision",inputs))
          assertEquals(MySqlFormat3Cell.Text("HELD"),cell(result.heldReservation,"fp_recovery_reservation","reservation_state",inputs))
          assertEquals(MySqlFormat3Cell.Text("RESERVED"),cell(result.reservedOperation,"fp_recovery_operation","operation_state",inputs))
          assertEquals(MySqlFormat3Cell.SqlNull,cell(result.reservedOperation,"fp_recovery_operation","reserve_operation_uuid",inputs))
          val newO=result.target.relation("fp_recovery_operation").rows.find(r=>cell(r,"fp_recovery_operation","operation_uuid",inputs) match
            case MySqlFormat3Cell.Binary(bytes)=>bytes.equalsUuid(request.tag.operationId)
            case _=>false).get
          assertEquals(cell(newO,"fp_recovery_operation","operation_uuid",inputs),cell(newO,"fp_recovery_operation","workflow_uuid",inputs))
          assertEquals(MySqlFormat3Cell.Text("COMMITTED"),cell(newO,"fp_recovery_operation","operation_state",inputs))
          assertEquals(MySqlFormat3Cell.Text("RELEASED"),cell(result.target.relation("fp_recovery_reservation").rows.head,"fp_recovery_reservation","reservation_state",inputs))
          val snapshot=result.target.relation(MySqlDbFirstSchemaDefinition.SnapshotTable).rows.head
          cell(snapshot,MySqlDbFirstSchemaDefinition.SnapshotTable,"nbt_bytes",inputs) match
            case MySqlFormat3Cell.Binary(bytes)=>
              assertEquals(captured.byteLength(),bytes.length)
              (0 until bytes.length).foreach(i=>assertEquals(captured.byteAt(i),bytes.byteAt(i)))
            case _=>fail("complete original NBT missing")
          val changed=Set("fp_recovery_operation","fp_recovery_reservation","floating_pod_state","physical_recovery_locator","fp_runtime_writer",MySqlDbFirstSchemaDefinition.SnapshotTable)
          source.relations.filterNot(r=>changed.contains(r.table)).foreach(r=>assertEquals(r,result.target.relation(r.table)))
          assertEquals(source.relation("fp_recovery_operation").rows.head,result.target.relation("fp_recovery_operation").rows.head,
            "historical RAW bytes/scalars must remain identical")
          val domain=result.target.relation("floating_pod_state")
          val tampered=domain.rows.head.updated(inputs.forTable(domain.table).columns.indexWhere(_.name=="pos_x"),MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(123D)))
          val rows=result.target.relations.map(r=>if r.table==domain.table then r.copy(rows=Vector(tampered)) else r)
          val bad=MySqlScriptSchemaSnapshot.captureMaterialized(result.target.schema,result.target.connection,result.target.observationScope,
            result.target.maxAllowedPacketBytes,inputs,rows,root)
          assertThrows(classOf[SQLDataException],()=>result.verifyTarget(bad))
          val beforeProjection=root.accountedMemoryBytes
          MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
            val changedProjection=MySqlDbFirstRecallCanonical.projection(request,result.before,rows,true,root)
            assertNotEquals(result.projectedTarget.sha256,changedProjection.sha256)
          }
          assertEquals(beforeProjection,root.accountedMemoryBytes)
        }
        assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure(
          f.core.materializeDbFirstRecall(reservation,request,source,91L,id(981),planned,planned.plusSeconds(1))))
        assertTrue(f.core.observation.plans.head.cause.isEmpty,"replay guard must not poison original materialization")
      catch
        case cause: Throwable =>
          primary=cause
          throw cause
      finally MySqlFormat3BudgetCleanup.run(primary,Vector(
        () => if source!=null then source.allocationBudget.closeRootReadBudget(),
        () => f.close()))

  @Test def dbFirstOriginalReadbackRequiresCompleteTargetAndKeepsBeforeOrChangedWriterUnknown():Unit =
    val inputs=dbFirstMaterializationInputs()
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var source:MySqlScriptSchemaSnapshot=null
    var primary:Throwable=null
    try
      val reservation=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(997)))
      val captured=f.core.captureDbFirstSnapshot(reservation)(a=>DbFirstPodSnapshotTest.captureForCore(reservation.tag.operationId,a,100))
      val request=f.core.dbFirstRecallRequest(reservation,captured)
      source=dbFirstBefore(f,inputs)
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      f.onWorker {
        val result=f.core.materializeDbFirstRecall(reservation,request,source,91L,id(998),at,at.plusSeconds(1))
        result.verify()
        val root=f.core.validationBudget(reservation.handle)
        val cold=inputs.newAllocationRoot(inputs.budget.packetBytes,f.shared)
        try
          cold.initialize(inputs.schema,result.target.connection,MySqlFormat3ObservationScope(id(999),2L,2L),inputs.budget.packetBytes)
          val baseline=f.shared.accountedBytes
          def observe(rows:Vector[MySqlFormat3RelationSnapshot],connection:MySqlFormat3ConnectionObservation=result.target.connection):MySqlDbFirstSession.DbFirstFailedRecallImage =
            val child=cold.newScopedReadBudget(inputs.budget.packetBytes)
            try
              val scope=MySqlFormat3ObservationScope(id(999),2L,2L)
              child.initialize(inputs.schema,connection,scope,inputs.budget.packetBytes)
              val image=MySqlScriptSchemaSnapshot.decoded(inputs.schema,connection,scope,inputs.budget.packetBytes,
                inputs,copyPersistedRows(rows,child),child)
              result.compareOriginalObservation(image,cold)
            finally child.closeScopedReadBudget()
          import MySqlDbFirstSession.DbFirstFailedRecallImage.*
          assertEquals(CommittedTargetObserved,observe(result.target.relations))
          assertEquals(CommittedTargetObserved,observe(result.target.relations,
            result.target.connection.copy(tlsCipher=result.target.connection.tlsCipher+"-new-observation")))
          assertEquals(BeforeObservedStillUnknown,observe(result.before.relations))
          def writerChanged(column:String,value:MySqlFormat3Cell):Vector[MySqlFormat3RelationSnapshot] =
            result.target.relations.map { relation=>
              if relation.table!="fp_runtime_writer" then relation
              else relation.copy(rows=relation.rows.map(_.updated(inputs.forTable(relation.table).columns.indexWhere(_.name==column),value)))
            }
          assertEquals(CommittedTargetWithRenewedLeaseObserved,observe(writerChanged("lease_until",MySqlFormat3Cell.DateTime(result.facts.leaseUntil.plusSeconds(1)))))
          assertEquals(DifferentObservedStillUnknown,observe(writerChanged("lease_until",MySqlFormat3Cell.DateTime(result.facts.leaseUntil.minusSeconds(1)))))
          val renewed=writerChanged("lease_until",MySqlFormat3Cell.DateTime(result.facts.leaseUntil.plusSeconds(1)))
          def afterRenewalChange(table:String,column:String,value:MySqlFormat3Cell):Vector[MySqlFormat3RelationSnapshot] =
            renewed.map { relation=>
              if relation.table!=table then relation
              else relation.copy(rows=relation.rows.map(_.updated(inputs.forTable(table).columns.indexWhere(_.name==column),value)))
            }
          assertEquals(DifferentObservedStillUnknown,observe(afterRenewalChange("fp_runtime_writer","owner_boot_uuid",MySqlFormat3Cell.SqlNull)))
          assertEquals(DifferentObservedStillUnknown,observe(afterRenewalChange("fp_runtime_writer","owner_boot_uuid",
            cell(result.before.relation("fp_recovery_operation").rows.head,"fp_recovery_operation","operation_uuid",inputs))))
          assertEquals(DifferentObservedStillUnknown,observe(afterRenewalChange("fp_runtime_writer","writer_epoch",
            MySqlFormat3Cell.SignedInteger(result.facts.writerEpoch+1L))))
          assertEquals(DifferentObservedStillUnknown,observe(afterRenewalChange("fp_runtime_writer","last_commit_fingerprint",
            cell(result.before.relation("fp_recovery_operation").rows.head,"fp_recovery_operation","operation_fingerprint",inputs))))
          assertEquals(DifferentObservedStillUnknown,observe(afterRenewalChange("floating_pod_state","pos_y",
            MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(65D)))))
          assertEquals(DifferentObservedStillUnknown,observe(writerChanged("last_commit_operation_uuid",
            cell(result.before.relation("fp_recovery_operation").rows.head,"fp_recovery_operation","operation_uuid",inputs))))
          assertThrows(classOf[SQLDataException],()=>observe(result.target.relations,
            result.target.connection.copy(serverUuid=id(1000))))
          assertEquals(baseline,f.shared.accountedBytes,"all actual comparison/observation children must retire")
          val foreign=inputs.newAllocationRoot(inputs.budget.packetBytes,new MySqlFormat3AllocationLedger(root.profile.maxRetainedBytes()))
          try assertThrows(classOf[IllegalArgumentException],()=>result.compareOriginalObservation(result.target,foreign))
          finally foreign.closeRootReadBudget()
          assertTrue(f.core.observation.plans.head.cause.isEmpty,"DATA comparison cannot resolve or poison original Entry")
          assertFalse(f.core.observation.plans.head.accepted,"DATA target observation is not execution or confirmation admission")
        finally cold.closeRootReadBudget()
      }
    catch case cause:Throwable=>
      primary=cause
      throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(
      ()=>if source!=null then source.allocationBudget.closeRootReadBudget(),()=>f.close()))

  @Test def dbFirstSnapshotReceiptSegmentReplaysOtherPodsCommitAndRejectsUnexplainedCursor():Unit =
    val inputs=dbFirstMaterializationInputs(snapshotBytes=524288L) // 显式两提交Full27 DATA fixture预算，不改生产配置。
    val f=new Fixture(2,dbFirstInputs=Some(inputs))
    var source:MySqlScriptSchemaSnapshot=null
    var primary:Throwable=null
    try
      val one=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(1101)))
      val captured=f.core.captureDbFirstSnapshot(one)(a=>DbFirstPodSnapshotTest.captureForCore(one.tag.operationId,a,100))
      val original=f.core.dbFirstRecallRequest(one,captured)
      val two=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(1102)),DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT)
      val saved=f.core.captureDbFirstSnapshot(two)(a=>DbFirstPodSnapshotTest.captureCheckpointForCore(
        two.tag.operationId,a,id(602),id(503),id(604),id(501),"Normal"))
      val checkpoint=f.core.dbFirstCheckpointRequest(two,saved)
      source=dbFirstBefore(f,inputs,checkpoint=true,twoPods=true)
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      f.onWorker {
        val anchor=f.core.materializeDbFirstRecall(one,original,source,91L,id(1103),at,at.plusSeconds(1))
        val newest=f.core.materializeDbFirstCheckpoint(two,checkpoint,anchor.target,91L,id(1103),at.plusSeconds(2),at.plusSeconds(3))
        anchor.verify();newest.verify()
        val cold=inputs.newAllocationRoot(inputs.budget.packetBytes,f.shared)
        try
          val scope=MySqlFormat3ObservationScope(id(1104),2L,2L)
          cold.initialize(inputs.schema,newest.target.connection,scope,inputs.budget.packetBytes)
          val baseline=f.shared.accountedBytes
          def inspect(rows:Vector[MySqlFormat3RelationSnapshot]):Long =
            val child=cold.newScopedReadBudget(inputs.budget.packetBytes)
            try
              child.initialize(inputs.schema,newest.target.connection,scope,inputs.budget.packetBytes)
              val image=MySqlScriptSchemaSnapshot.decoded(inputs.schema,newest.target.connection,scope,inputs.budget.packetBytes,
                inputs,copyPersistedRows(rows,child),child)
              anchor.verifyObservedSnapshotHistory(image,cold)
            finally child.closeScopedReadBudget()
          def changed(table:String,column:String,value:MySqlFormat3Cell):Vector[MySqlFormat3RelationSnapshot] =
            newest.target.relations.map { relation=>
              if relation.table!=table then relation
              else relation.copy(rows=relation.rows.map(_.updated(inputs.forTable(table).columns.indexWhere(_.name==column),value)))
            }
          assertEquals(0L,inspect(anchor.target.relations))
          (0 until 2).foreach(_=>assertEquals(1L,inspect(newest.target.relations)))
          assertEquals(1L,inspect(changed("fp_runtime_writer","lease_until",MySqlFormat3Cell.DateTime(newest.facts.leaseUntil.plusSeconds(1)))))
          assertThrows(classOf[SQLDataException],()=>inspect(changed("fp_runtime_writer","lease_until",MySqlFormat3Cell.DateTime(newest.facts.leaseUntil.minusSeconds(1)))))
          assertThrows(classOf[SQLDataException],()=>inspect(changed("fp_runtime_writer","owner_boot_uuid",MySqlFormat3Cell.SqlNull)))
          assertThrows(classOf[SQLDataException],()=>inspect(changed("floating_pod_state","pos_y",MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(65D)))))
          assertThrows(classOf[SQLDataException],()=>inspect(changed("fp_runtime_writer","last_commit_operation_uuid",
            cell(anchor.before.relation("fp_recovery_operation").rows.head,"fp_recovery_operation","operation_uuid",inputs))))
          assertEquals(baseline,f.shared.accountedBytes,"all decoded receipt/cursor/visited DATA must retire on success and rejection")
          assertTrue(f.core.observation.plans.forall(_.cause.isEmpty))
          assertTrue(f.core.observation.plans.forall(!_.accepted),"verified DATA history cannot issue execution/confirmation")
        finally cold.closeRootReadBudget()
      }
    catch case cause:Throwable=>
      primary=cause
      throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(
      ()=>if source!=null then source.allocationBudget.closeRootReadBudget(),()=>f.close()))

  /** 直接检查wire；篡改拒绝仅证明原target身份门，不冒充persisted commitment decoder。 */

  private def assertDbFirstWitnessCommitmentsAndOriginalTargetGuard(result: MySqlDbFirstRecallMaterialization,
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget): Unit =
    assertTrue(result.witness.length<=inputs.budget.snapshotBytes,"witness must fit original explicit snapshot byte budget")
    val bytes=result.witness.borrowedBinary.copyBytes(root,"test witness framing inspection")
    val in=new DataInputStream(new ByteArrayInputStream(bytes))
    def token(): String = new String(in.readNBytes(in.readInt()),java.nio.charset.StandardCharsets.US_ASCII)
    assertEquals(MySqlDbFirstRecallCanonical.Witness,token())
    assertEquals(1,in.readUnsignedByte())
    assertArrayEquals(result.request.identity.canonicalBytes.toArray,in.readNBytes(in.readInt()))
    var roleOffset=0
    var lengthOffset=0
    var hashOffset=0
    Vector("SCOPE"->result.scope,"PREIMAGE"->result.preimage,"TARGET"->result.projectedTarget,
      "FINGERPRINT"->result.fingerprint,"CONTEXT"->result.context).zipWithIndex.foreach { case ((role,canonical),index) =>
      if index==0 then roleOffset=bytes.length-in.available()+4
      assertEquals(role,token())
      assertEquals(canonical.encoding,token())
      if index==0 then lengthOffset=bytes.length-in.available()
      assertEquals(canonical.length,in.readInt())
      if index==0 then hashOffset=bytes.length-in.available()
      assertArrayEquals(canonical.sha256.toArray,in.readNBytes(32))
    }
    assertEquals(-1,in.read(),"fixed five commitments must consume the complete witness")
    val shape=inputs.forTable("fp_recovery_operation")
    val witnessIndex=shape.columns.indexWhere(_.name=="commit_witness_bytes")
    val hashIndex=shape.columns.indexWhere(_.name=="commit_witness_sha256")
    Vector("role"->roleOffset,"length"->lengthOffset,"hash"->hashOffset).foreach { (label,offset) =>
      root.reserveRetainedMemory(bytes.length.toLong+64L,"test mutated witness BEFORE allocation")
      val mutated=bytes.clone()
      mutated(offset)=(mutated(offset)^1).toByte
      root.reserveRetainedMemory(96L,"test mutated witness digest BEFORE allocation")
      val digest=java.security.MessageDigest.getInstance("SHA-256").digest(mutated)
      val relations=result.target.relations.map { relation =>
        if relation.table!=shape.table then relation
        else relation.copy(rows=relation.rows.map { row =>
          cell(row,shape.table,"operation_uuid",inputs) match
            case MySqlFormat3Cell.Binary(uuid) if uuid.equalsUuid(result.request.tag.operationId) =>
              row.updated(witnessIndex,MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(mutated,root)))
                .updated(hashIndex,MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(digest,root)))
            case _ => row
        })
      }
      val candidate=MySqlScriptSchemaSnapshot.captureMaterialized(result.target.schema,result.target.connection,
        result.target.observationScope,result.target.maxAllowedPacketBytes,inputs,relations,root)
      val failure=assertThrows(classOf[SQLDataException],()=>result.verifyTarget(candidate),label+" mutation")
      assertEquals("DB-first complete target differs from original materialization",failure.getMessage,
        label+" mutation rejected by original target identity gate, not a persisted semantic decoder")
    }

  @Test def dbFirstReleasedReservationAdvancesBothHeldAndReleasedRevisions(): Unit =
    val inputs=dbFirstMaterializationInputs()
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var source: MySqlScriptSchemaSnapshot=null
    var primary: Throwable=null
    try
      val reservation=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(985)))
      val captured=f.core.captureDbFirstSnapshot(reservation)(a=>DbFirstPodSnapshotTest.captureForCore(reservation.tag.operationId,a,100))
      val request=f.core.dbFirstRecallRequest(reservation,captured)
      source=dbFirstBefore(f,inputs,"released")
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      f.onWorker {
        val result=f.core.materializeDbFirstRecall(reservation,request,source,91L,id(986),at,at.plusSeconds(1))
        assertDbFirstMaterializationSizes(result)
        assertEquals(8L,number(result.heldReservation,"fp_recovery_reservation","reservation_revision",inputs))
        assertEquals(9L,number(result.target.relation("fp_recovery_reservation").rows.head,"fp_recovery_reservation","reservation_revision",inputs))
        assertTrue(result.witness.length<=inputs.budget.snapshotBytes)
        assertEquals(8L,result.facts.heldReservationRevision)
        assertEquals(9L,result.facts.releasedReservationRevision)
      }
    catch
      case cause: Throwable =>
        primary=cause
        throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(
      () => if source!=null then source.allocationBudget.closeRootReadBudget(),
      () => f.close()))

  @Test def dbFirst64KiBWholeSnapshotFailureRetainsOriginalCaptureOperationAndCause(): Unit =
    val inputs=MySqlScriptSchemaInputs.createDbFirstFull27(MySqlDbFirstSchemaProfile.render(DeploymentReceiptStaticTestData.defaultProfile()))
    assertEquals(65536L,inputs.budget.snapshotBytes)
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var source: MySqlScriptSchemaSnapshot=null
    var primary: Throwable=null
    try
      val tag=IoOperationTag(id(1),1L,1L,id(987))
      val reservation=f.core.reserveDbFirstCapture(tag)
      val captured=f.core.captureDbFirstSnapshot(reservation)(a=>DbFirstPodSnapshotTest.captureForCore(tag.operationId,a,100))
      val request=f.core.dbFirstRecallRequest(reservation,captured)
      val root=f.core.validationBudget(reservation.handle)
      source=dbFirstBefore(f,inputs)
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      val failure=f.onWorkerFailure(f.core.materializeDbFirstRecall(reservation,request,source,91L,id(988),at,at.plusSeconds(1)))
      assertInstanceOf(classOf[SQLDataException],failure)
      assertEquals("ScriptPublication1 canonical snapshot exceeds explicit snapshot/retained budget",failure.getMessage)
      val state=f.core.observation.plans.head
      assertEquals(tag,state.tag)
      assertSame(failure,state.cause.get)
      assertFalse(state.accepted)
      assertFalse(state.workerObserved)
      assertFalse(state.dropped)
      assertFalse(root.isClosed)
      assertTrue(f.shared.accountedBytes>0L)
      assertEquals(tag,request.tag)
      assertEquals(request.nbtByteLength,captured.byteLength())
      assertEquals(request.nbtSha256,captured.sha256().toVector)
      var recaptured=false
      assertThrows(classOf[IllegalArgumentException],()=>f.core.captureDbFirstSnapshot(reservation) { _ =>
        recaptured=true
        captured
      })
      assertFalse(recaptured,"capacity failure must not recapture the original operation")
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(tag))
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure(
        f.core.materializeDbFirstRecall(reservation,request,source,91L,id(988),at,at.plusSeconds(1))))
      assertSame(failure,f.core.observation.plans.head.cause.get,"replay rejection must preserve the original cause")
      f.core.stop()
      assertInstanceOf(classOf[IllegalStateException],f.onWorkerFailure(f.core.requireStoppedEmptyForTerminal()))
      assertFalse(root.isClosed,"shutdown cannot retire a live captured entry after materialization failure")
    catch
      case cause: Throwable =>
        primary=cause
        throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(
      () => if source!=null then source.allocationBudget.closeRootReadBudget(),
      () => f.close()))

  @Test def dbFirstMaterializationRejectsWorldDimensionAndRevisionOverflowWithOriginalResponsibility(): Unit =
    Vector("world","dimension","revision").foreach { fault =>
      val inputs=MySqlScriptSchemaInputs.createDbFirstFull27(MySqlDbFirstSchemaProfile.render(DeploymentReceiptStaticTestData.defaultProfile()))
      val f=new Fixture(1,dbFirstInputs=Some(inputs))
      var source: MySqlScriptSchemaSnapshot=null
      var primary: Throwable=null
      try
        val reservation=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(990)))
        val captured=f.core.captureDbFirstSnapshot(reservation)(a=>DbFirstPodSnapshotTest.captureForCore(reservation.tag.operationId,a,100))
        val request=f.core.dbFirstRecallRequest(reservation,captured)
        source=dbFirstBefore(f,inputs,fault)
        val root=f.core.validationBudget(reservation.handle)
        val at=java.time.LocalDateTime.of(2026,10,29,12,0)
        val failure=f.onWorkerFailure(f.core.materializeDbFirstRecall(reservation,request,source,91L,id(991),at,at.plusSeconds(1)))
        if fault=="revision" then assertInstanceOf(classOf[ArithmeticException],failure)
        else assertInstanceOf(classOf[SQLDataException],failure)
        assertSame(failure,f.core.observation.plans.head.cause.get)
        assertFalse(root.isClosed)
        assertTrue(f.shared.accountedBytes>0L)
        f.core.stop()
        assertInstanceOf(classOf[IllegalStateException],f.onWorkerFailure(f.core.requireStoppedEmptyForTerminal()))
      catch
        case cause: Throwable =>
          primary=cause
          throw cause
      finally MySqlFormat3BudgetCleanup.run(primary,Vector(
        () => if source!=null then source.allocationBudget.closeRootReadBudget(),
        () => f.close()))
    }

  private def cell(row: Vector[MySqlFormat3Cell],table: String,name: String,inputs: MySqlScriptSchemaInputs): MySqlFormat3Cell =
    row(inputs.forTable(table).columns.indexWhere(_.name==name))
  private def number(row: Vector[MySqlFormat3Cell],table: String,name: String,inputs: MySqlScriptSchemaInputs): Long =
    cell(row,table,name,inputs) match
      case MySqlFormat3Cell.SignedInteger(n)=>n
      case _=>throw new AssertionError("integer fixture cell required")

  @Test def comparisonDecodedFull27UsesRealNonemptyDeploymentCodecAndRejectsMissingOrForeignOwner(): Unit =
    val base=DeploymentReceiptStaticTestData.defaultProfile()
    val sourceProfile=MySqlScriptSchemaDataTestFixture.SourceProfile
    val profile=MySqlDeploymentReceiptSchemaProfile.inputs(MySqlScriptSchemaDataTestFixture.Schema,base.accounts,sourceProfile,
      MySqlScriptSchemaDataTestFixture.Inputs.budget,MySqlScriptSchemaDataTestFixture.Full26Inputs.deploymentBudget.get,
      base.quotaMaximumRecords,base.trustedAdministrativeProxyRows)
    val inputs=MySqlScriptSchemaInputs.createDbFirstFull27(MySqlDbFirstSchemaProfile.render(profile))
    val root=inputs.newAllocationRoot(inputs.budget.packetBytes,new MySqlFormat3AllocationLedger(sourceProfile.maxRetainedBytes()))
    var primary: Throwable=null
    try
      val manifest=inputs.forTable("fp_schema_manifest")
      val rows=MySqlScriptSchemaDataTestFixture.full26OrdinaryRelations(MySqlScriptSchemaDataTestFixture.Full26Inputs).map { r=>
        if r.table!="fp_schema_manifest" then r
        else r.copy(rows=r.rows.map(row=>row.updated(manifest.columns.indexWhere(_.name=="format_version"),MySqlFormat3Cell.SignedInteger(6L))
          .updated(manifest.columns.indexWhere(_.name=="ddl_sha256"),MySqlScriptSchemaDataTestFixture.binary(java.util.HexFormat.of().parseHex(inputs.ddlSha256)))))
      } :+ MySqlFormat3RelationSnapshot(MySqlDbFirstSchemaDefinition.SnapshotTable,Vector.empty)
      assertTrue(rows.find(_.table==MySqlDeploymentReceiptSchemaDefinition.OperationTable).get.rows.nonEmpty)
      val owned=copyPersistedRows(rows,root)
      val connection=MySqlScriptSchemaDataTestFixture.Connection
      val observation=MySqlScriptSchemaDataTestFixture.Scope
      val image=MySqlScriptSchemaSnapshot.captureMaterialized(inputs.schema,connection,observation,inputs.budget.packetBytes,inputs,owned,root)
      val baseline=root.accountedMemoryBytes
      MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
        val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
        child.reserveRetainedMemory(1024L,"test decoded DATA representation BEFORE allocation")
        val decoded=MySqlScriptSchemaSnapshot.decodedForComparison(image.schema,image.connection,image.observationScope,
          image.maxAllowedPacketBytes,inputs,image.relations,root,child)
        assertEquals(image.relations,decoded.relations)
        assertThrows(classOf[IllegalArgumentException],()=>MySqlScriptSchemaSnapshot.decodedForComparison(image.schema,image.connection,
          image.observationScope,image.maxAllowedPacketBytes,inputs,image.relations,null,child))
        val sibling=root.newCanonicalScope()
        var childPrimary: Throwable=null
        try assertThrows(classOf[IllegalArgumentException],()=>MySqlScriptSchemaSnapshot.decodedForComparison(image.schema,image.connection,
          image.observationScope,image.maxAllowedPacketBytes,inputs,image.relations,root,sibling))
        catch
          case cause: Throwable=>
            childPrimary=cause
            throw cause
        finally MySqlFormat3BudgetCleanup.run(childPrimary,Vector(() => sibling.closeScopedReadBudget()))
      }
      assertEquals(baseline,root.accountedMemoryBytes,"nonempty deployment decoder scopes must retire without debt")
    catch
      case cause: Throwable=>
        primary=cause
        throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(() => root.closeRootReadBudget()))

  private def copyPersistedRows(rows: Vector[MySqlFormat3RelationSnapshot], owner: MySqlFormat3ReadBudget): Vector[MySqlFormat3RelationSnapshot] =
    owner.reserveRetainedMemory(4096L+rows.map(_.rows.size.toLong).sum*512L,"test cold persisted row containers BEFORE allocation")
    rows.map(r=>r.copy(rows=r.rows.map { row=>
      owner.reserveRetainedMemory(256L+96L*row.size,"test cold persisted cells BEFORE allocation")
      row.map {
        case MySqlFormat3Cell.Binary(b)=>MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(b.copyBytes(owner,"test cold persisted binary"),owner))
        case c=>c
      }
    }))

  private def withPersistedDbFirstImage(laterHistory: Boolean=false,queryGraph:Boolean=false,initial:Boolean=false,
      snapshotBytes:Long=131072L,checkpoint:Boolean=false,activeBaseline:Boolean=false)(
      body: (MySqlScriptSchemaSnapshot,MySqlFormat3ReadBudget,UUID)=>Unit): Unit =
    val inputs=dbFirstMaterializationInputs(snapshotBytes)
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var source: MySqlScriptSchemaSnapshot=null
    var primary: Throwable=null
    try
      val reservation=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(989)),
        if initial then DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION else if checkpoint then DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT else DbFirstPodSnapshot.Purpose.RECALL)
      val captured=f.core.captureDbFirstSnapshot(reservation)(a=>
        if initial then DbFirstPodSnapshotTest.captureInitialForCore(reservation.tag.operationId,a)
        else if checkpoint then DbFirstPodSnapshotTest.captureCheckpointForCore(reservation.tag.operationId,a,id(502),id(503),id(504),id(501),"Normal")
        else DbFirstPodSnapshotTest.captureForCore(reservation.tag.operationId,a,100))
      val initialRequest=if initial then f.core.dbFirstInitialActivationRequest(reservation,captured) else null
      val recallRequest=if initial || checkpoint then null else f.core.dbFirstRecallRequest(reservation,captured)
      val checkpointRequest=if checkpoint then f.core.dbFirstCheckpointRequest(reservation,captured) else null
      source=if initial then initialBefore(f,inputs) else dbFirstBefore(f,inputs,if laterHistory then "history-reserved" else "",checkpoint=checkpoint)
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      f.onWorker {
        val target=if activeBaseline then source else if initial then
          f.core.materializeDbFirstInitialActivation(reservation,initialRequest,source,91L,id(990),at,at.plusSeconds(1),256).target
        else if checkpoint then f.core.materializeDbFirstCheckpoint(reservation,checkpointRequest,source,91L,id(990),at,at.plusSeconds(1)).target
        else f.core.materializeDbFirstRecall(reservation,recallRequest,source,91L,id(990),at,at.plusSeconds(1)).target
        val cold=inputs.newAllocationRoot(inputs.budget.packetBytes,f.shared)
        var imageOwner: MySqlFormat3ReadBudget=null
        var coldPrimary: Throwable=null
        try
          val connection=source.connection.copy(serverVersion=source.connection.serverVersion+"-later-observation")
          val observation=MySqlFormat3ObservationScope(id(991),2L,2L)
          cold.initialize(inputs.schema,connection,observation,inputs.budget.packetBytes)
          // 真root发行的SnapshotRead child；本纯测不执行JDBC或伪造reader holder。
          imageOwner=cold.newScopedReadBudget(inputs.budget.packetBytes)
          imageOwner.initialize(inputs.schema,connection,observation,inputs.budget.packetBytes)
          var rows=copyPersistedRows(target.relations,imageOwner)
          if queryGraph then
            def u(n:Long)=MainBootProjectionFixture.binary(java.nio.ByteBuffer.allocate(16).putLong(0L).putLong(n).array())
            def i(n:Long)=MySqlFormat3Cell.SignedInteger(n)
            def t(s:String)=MySqlFormat3Cell.Text(s)
            def d(n:Double)=MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(n))
            def row(table:String,values:(String,MySqlFormat3Cell)*)=MySqlScriptSchemaDataTestFixture.row(table,values*)
            val footprint=row("physical_recovery_footprint","binding_uuid"->u(503),"pod_uuid"->u(502),
              "attempt_uuid"->u(504),"footprint_uuid"->u(505),"origin_version"->i(6),"target_world_name"->t("world"),
              "target_pos_x"->d(0),"target_pos_y"->d(0),"target_pos_z"->d(0),"target_yaw"->d(0),"target_pitch"->d(0),
              "target_dimension_id"->i(0),"center_chunk_x"->i(0),"center_chunk_z"->i(0),"seal_state"->t("SEALED"),
              "created_operation_uuid"->u(700))
            val chunks=(for x <- -2 to 2; z <- -2 to 2 yield row("physical_recovery_footprint_chunk",
              "binding_uuid"->u(503),"pod_uuid"->u(502),"attempt_uuid"->u(504),"footprint_uuid"->u(505),
              "dimension_id"->i(0),"chunk_x"->i(x),"chunk_z"->i(z))).toVector
            val intent=row("physical_recovery_intent","binding_uuid"->u(503),"pod_uuid"->u(502),"attempt_uuid"->u(504),
              "origin"->t("OWNED_RELEASE"),"phase"->t("ACTIVATED"),"target_footprint_uuid"->u(505),
              "prepare_operation_uuid"->u(700),"last_mutation_operation_uuid"->u(700),
              "expected_domain_version"->i(6),"expected_owner_uuid"->u(501),"activation_operation_uuid"->u(700),
              "committed_domain_version"->i(7),"committed_at"->i(1),"committed_world_name"->t("world"),
              "committed_pos_x"->d(0),"committed_pos_y"->d(0),"committed_pos_z"->d(0),"committed_yaw"->d(0),
              "committed_pitch"->d(0),"committed_dimension_id"->i(0))
            val graph=Map("physical_recovery_footprint"->Vector(footprint),"physical_recovery_footprint_chunk"->chunks,
              "physical_recovery_intent"->Vector(intent))
            rows=copyPersistedRows(rows.map { r=>
              val bindingIndex=inputs.forTable(r.table).columns.indexWhere(_.name=="binding_uuid")
              val actualRows=graph.getOrElse(r.table,r.rows)
              r.copy(rows=if bindingIndex<0 then actualRows else actualRows.map(_.updated(bindingIndex,u(503))))
            },imageOwner)
          if laterHistory then
            val shape=inputs.forTable("fp_recovery_operation")
            def put(r: Vector[MySqlFormat3Cell],n: String,c: MySqlFormat3Cell)=r.updated(shape.columns.indexWhere(_.name==n),c)
            val original=rows.find(_.table==shape.table).get.rows.find(r=>cell(r,shape.table,"operation_uuid",inputs) match
              case MySqlFormat3Cell.Binary(b)=>b.equalsUuid(id(700))
              case _=>false).get
            rows=rows.map(r=>if r.table!=shape.table then r else r.copy(rows=r.rows.map { row=>
              cell(row,shape.table,"operation_uuid",inputs) match
                case MySqlFormat3Cell.Binary(b) if b.equalsUuid(id(777))=>
                  put(put(put(put(row,"operation_state",MySqlFormat3Cell.Text("COMMITTED")),"result_at",cell(original,shape.table,"result_at",inputs)),
                    "commit_witness_bytes",cell(original,shape.table,"commit_witness_bytes",inputs)),
                    "commit_witness_sha256",cell(original,shape.table,"commit_witness_sha256",inputs))
                case _=>row
            }))
          val image=MySqlScriptSchemaSnapshot.decoded(inputs.schema,connection,observation,inputs.budget.packetBytes,inputs,rows,imageOwner)
          if activeBaseline then MySqlControlledDebugRelations.requireValidDbFirstGraph(image,inputs,cold)
          body(image,cold,reservation.tag.operationId)
        catch
          case cause: Throwable=>
            coldPrimary=cause
            throw cause
        finally MySqlFormat3BudgetCleanup.run(coldPrimary,Vector(
          () => if imageOwner!=null then imageOwner.closeScopedReadBudget(),() => cold.closeRootReadBudget()))
      }
    catch
      case cause: Throwable=>
        primary=cause
        throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(
      () => if source!=null then source.allocationBudget.closeRootReadBudget(),() => f.close()))

  @Test def full27MainDataProjectionKeepsSnapshotRelationAndRejectsForeignRoot():Unit =
    withPersistedDbFirstImage() { (image,root,operation)=>
      val projected=MySqlOwnedFormat3Snapshot.fromDbFirst(image,image.inputs,root)
      projected.requireCompleteFamily()
      assertEquals(image.relations.map(_.table),projected.relations.map(_.table))
      assertEquals(27,projected.relations.size)
      val snapshot=projected.relations.find(_.table==MySqlDbFirstSchemaDefinition.SnapshotTable).get
      assertEquals(1,snapshot.rows.size)
      assertEquals(image.canonicalSha256Hex,projected.canonicalSha256Hex)
      assertThrows(classOf[IllegalArgumentException],()=>projected.requireBootReadData)
      val foreign=image.inputs.newAllocationRoot(image.inputs.budget.packetBytes,new MySqlFormat3AllocationLedger(root.profile.maxRetainedBytes()))
      try assertThrows(classOf[IllegalArgumentException],()=>MySqlOwnedFormat3Snapshot.fromDbFirst(image,image.inputs,foreign))
      finally foreign.closeRootReadBudget()
    }

  @Test def full27TypedQueryReadsCommittedDomainWithoutPromotingHistoricalPayload():Unit =
    withPersistedDbFirstImage(queryGraph=true) { (image,root,operation)=>
      val query=MySqlFormat3QueryProjection.fromDbFirstData(image,image.inputs,root)
      val domain=query.findStoredByUuid(new UUID(0L,502L)).get
      assertFalse(domain.isActive)
      assertTrue(query.findDesiredActiveRows().isEmpty)
      assertEquals(1,query.findAllPhysicalRecoveryLocators().size)
      val foreign=image.inputs.newAllocationRoot(image.inputs.budget.packetBytes,new MySqlFormat3AllocationLedger(root.profile.maxRetainedBytes()))
      try assertThrows(classOf[IllegalArgumentException],()=>MySqlFormat3QueryProjection.fromDbFirstData(image,image.inputs,foreign))
      finally foreign.closeRootReadBudget()
    }

  @Test def full27ImmutableMainDataIncludesCurrentRowsAndSourceProtection():Unit =
    withPersistedDbFirstImage(queryGraph=true) { (image,root,operation)=>
      val before=root.accountedMemoryBytes
      MySqlControlledDebugRelations.requireValidDbFirstGraph(image,image.inputs,root)
      assertEquals(before,root.accountedMemoryBytes,"temporary complete graph must retire its comparison child")
      val data=MySqlBootMainReadData.fromDbFirst(image,image.inputs,root)
      val domain=data.stored(id(502)).get
      assertEquals(8L,domain.stateVersion)
      assertFalse(domain.isActive)
      assertEquals(0,data.activeCount)
      assertEquals(1,data.locators.size)
      assertNotNull(data.sourceProtection)
      assertEquals(1,data.recoveryRows.size)
    }

  @Test def full27MainGenerationsReleaseOnlyTheirOwnDataAllocation():Unit =
    withPersistedDbFirstImage(queryGraph=true) { (image,root,operation)=>
      // 原source的lazy digest仍由活着的SnapshotRead child持有，不属于待退款Main DATA。
      assertEquals(64,image.canonicalSha256Hex.length)
      val before=root.accountedMemoryBytes
      val first=root.newCanonicalDataScope()
      val second=root.newCanonicalDataScope()
      var firstProjection=MySqlOwnedFormat3Snapshot.fromDbFirst(image,image.inputs,root,first)
      var firstData=MySqlBootMainReadData.fromDbFirst(image,image.inputs,root,first)
      var secondProjection=MySqlOwnedFormat3Snapshot.fromDbFirst(image,image.inputs,root,second)
      var secondData=MySqlBootMainReadData.fromDbFirst(image,image.inputs,root,second)
      assertEquals(firstProjection.canonicalSha256Hex,secondProjection.canonicalSha256Hex)
      assertEquals(firstData.stored(id(502)),secondData.stored(id(502)))
      val both=root.accountedMemoryBytes
      firstProjection=null;firstData=null
      first.closeScopedReadBudget()
      assertTrue(root.accountedMemoryBytes<both,"retiring first DATA must actually refund its child")
      assertTrue(root.accountedMemoryBytes>before,"second DATA responsibility must remain charged")
      assertEquals(8L,secondData.stored(id(502)).get.stateVersion)
      assertEquals(27,secondProjection.relations.size)
      assertNotNull(secondData.sourceProtection)
      secondProjection=null;secondData=null
      second.closeScopedReadBudget()
      assertEquals(before,root.accountedMemoryBytes,"both dropped DATA generations must fully refund")
    }

  @Test def full27UnpublishedMainDataKeepsIndependentLifetimeAfterObservationChildRetires():Unit =
    withPersistedDbFirstImage(queryGraph=true) { (image,root,operation)=>
      assertEquals(64,image.canonicalSha256Hex.length) // 原source lazy fingerprint先计入仍存活的source基线。
      val before=root.accountedMemoryBytes
      val observed=root.newScopedReadBudget(image.maxAllowedPacketBytes)
      val data=root.newCanonicalDataScope()
      var primary:Throwable=null
      try
        observed.initialize(image.schema,image.connection,image.observationScope,image.maxAllowedPacketBytes)
        val copied=MySqlScriptSchemaSnapshot.decoded(image.schema,image.connection,image.observationScope,
          image.maxAllowedPacketBytes,image.inputs,copyPersistedRows(image.relations,observed),observed)
        val projection=MySqlOwnedFormat3Snapshot.fromDbFirst(copied,image.inputs,root,data)
        val domain=MySqlBootMainReadData.fromDbFirst(copied,image.inputs,root,data)
        assertEquals(image.canonicalSha256Hex,projection.canonicalSha256Hex)
        observed.closeScopedReadBudget()
        assertTrue(observed.isClosed)
        assertTrue(root.accountedMemoryBytes>before,"independent unpublished DATA must stay funded after observation retirement")
        assertEquals(27,projection.relations.size)
        assertEquals(8L,domain.stored(id(502)).get.stateVersion)
        val binaries=projection.relations.flatMap(_.rows).flatMap(_.cells).collect {
          case MySqlOwnedFormat3Value.Binary(value)=>value
        }
        assertTrue(binaries.nonEmpty)
        binaries.foreach { value=>
          val expected=value.bytes
          val altered=value.bytes
          if altered.nonEmpty then altered(0)=(altered(0)^1).toByte
          assertArrayEquals(expected,value.bytes,"owned binary must remain independent and immutable")
        }
      catch case cause:Throwable=>
        primary=cause
        throw cause
      finally MySqlFormat3BudgetCleanup.run(primary,Vector(
        ()=>if !observed.isClosed then observed.closeScopedReadBudget(),()=>data.closeScopedReadBudget()))
      assertEquals(before,root.accountedMemoryBytes,"unpublished DATA refund must not release other source responsibility")
    }

  @Test def full27MainDataRejectsForeignOrClosedAllocation():Unit =
    withPersistedDbFirstImage(queryGraph=true) { (image,root,operation)=>
      val foreign=image.inputs.newAllocationRoot(image.inputs.budget.packetBytes,
        new MySqlFormat3AllocationLedger(root.profile.maxRetainedBytes()))
      try
        val child=foreign.newCanonicalDataScope()
        try assertThrows(classOf[IllegalArgumentException],()=>
          MySqlBootMainReadData.fromDbFirst(image,image.inputs,root,child))
        finally child.closeScopedReadBudget()
      finally foreign.closeRootReadBudget()
      assertThrows(classOf[IllegalArgumentException],()=>
        MySqlBootMainReadData.fromDbFirst(image,image.inputs,root,image.allocationBudget))
      val closed=root.newCanonicalDataScope()
      closed.closeScopedReadBudget()
      assertThrows(classOf[IllegalArgumentException],()=>
        MySqlOwnedFormat3Snapshot.fromDbFirst(image,image.inputs,root,closed))
    }

  @Test def full27SnapshotRejectsEachMissingReferencedTarget():Unit =
    withPersistedDbFirstImage(queryGraph=true) { (image,root,operation)=>
      Vector("physical_recovery_binding","floating_pod_state","fp_recovery_operation").foreach { missing=>
        val child=root.newScopedReadBudget(image.maxAllowedPacketBytes)
        var primary:Throwable=null
        try
          child.initialize(image.schema,image.connection,image.observationScope,image.maxAllowedPacketBytes)
          val changed=copyPersistedRows(image.relations.map(r=>if r.table==missing then r.copy(rows=Vector.empty) else r),child)
          assertThrows(classOf[SQLDataException],()=>
            val invalid=MySqlScriptSchemaSnapshot.decoded(image.schema,image.connection,image.observationScope,
              image.maxAllowedPacketBytes,image.inputs,changed,child)
            MySqlControlledDebugRelations.requireValidDbFirstGraph(invalid,image.inputs,root),missing)
        catch case cause:Throwable=>
          primary=cause
          throw cause
        finally MySqlFormat3BudgetCleanup.run(primary,Vector(()=>child.closeScopedReadBudget()))
      }
    }

  @Test def persistedDbFirstRecallRebuildsOriginalHeaderOnActualSnapshotChildAndSessionHolder(): Unit =
    withPersistedDbFirstImage() { (image,root,operation)=>
      assertTrue(image.allocationBudget.isSnapshotReadChildOf(root))
      image.canonicalByteLength
      val baseline=root.accountedMemoryBytes
      (0 until 3).foreach(_=>MySqlDbFirstRecallPersistedRead.verifyCommitted(image,operation,root))
      assertEquals(baseline,root.accountedMemoryBytes,"all persisted decoder/recompute child allocations must retire")
      root.reserveRetainedMemory(MySqlDbFirstSession.HistoricalRead.HolderBytes,"test actual historical holder BEFORE allocation")
      val read=new MySqlDbFirstSession.HistoricalRead(root,image.inputs)
      var primary: Throwable=null
      try
        read.acquire(None)
        read.validate(image)
        assertEquals(1L,read.dbFirstRecallSummary.verifiedCommitted)
        assertEquals(0L,read.dbFirstRecallSummary.unresolvedReserved)
        assertEquals(0L,read.dbFirstRecallSummary.pendingCancelled)
      catch
        case cause: Throwable=>
          primary=cause
          throw cause
      finally MySqlFormat3BudgetCleanup.run(primary,Vector(() => read.close()))
      assertEquals(baseline,root.accountedMemoryBytes)
      val foreign=image.inputs.newAllocationRoot(image.maxAllowedPacketBytes,new MySqlFormat3AllocationLedger(root.profile.maxRetainedBytes()))
      try assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstRecallPersistedRead.verifyCommitted(image,operation,foreign))
      finally foreign.closeRootReadBudget()
    }

  @Test def persistedDbFirstRecallPreservesOriginalReservedNullsWhenInventoryLaterCommits(): Unit =
    withPersistedDbFirstImage(true) { (image,root,operation)=>
      MySqlDbFirstRecallPersistedRead.verifyCommitted(image,operation,root)
    }

  @Test def persistedDbFirstInitialActivationRebuildsOriginalHeaderOnActualSnapshotChildAndSessionHolder(): Unit =
    withPersistedDbFirstImage(initial=true) { (image,root,operation)=>
      assertTrue(image.allocationBudget.isSnapshotReadChildOf(root))
      image.canonicalByteLength
      val baseline=root.accountedMemoryBytes
      (0 until 3).foreach(_=>MySqlDbFirstInitialActivationPersistedRead.verifyCommitted(image,operation,root))
      assertEquals(baseline,root.accountedMemoryBytes,"all persisted decoder/recompute child allocations must retire")
      root.reserveRetainedMemory(MySqlDbFirstSession.HistoricalRead.HolderBytes,"test actual historical holder BEFORE allocation")
      val read=new MySqlDbFirstSession.HistoricalRead(root,image.inputs)
      var primary: Throwable=null
      try
        read.acquire(None)
        read.validate(image)
        assertEquals(1L,read.dbFirstInitialActivationSummary.verifiedCommitted)
        assertEquals(0L,read.dbFirstInitialPreparationSummary.verifiedCommitted)
        assertEquals(0L,read.dbFirstRecallSummary.verifiedCommitted)
        assertEquals(0L,read.dbFirstInitialActivationSummary.unresolvedReserved)
        assertEquals(0L,read.dbFirstInitialActivationSummary.pendingCancelled)
      catch
        case cause: Throwable=>
          primary=cause
          throw cause
      finally MySqlFormat3BudgetCleanup.run(primary,Vector(() => read.close()))
      assertEquals(baseline,root.accountedMemoryBytes)
      val foreign=image.inputs.newAllocationRoot(image.maxAllowedPacketBytes,new MySqlFormat3AllocationLedger(root.profile.maxRetainedBytes()))
      try assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstInitialActivationPersistedRead.verifyCommitted(image,operation,foreign))
      finally foreign.closeRootReadBudget()
    }

  @Test def persistedDbFirstInitialActivationRejectsValidOuterRawWithChangedScopeEffectAndMissingHistory(): Unit =
    withPersistedDbFirstImage(initial=true) { (image,root,operation)=>
      val inputs=image.inputs
      val shape=inputs.forTable("fp_recovery_operation")
      def own(row:Vector[MySqlFormat3Cell],id:UUID)=cell(row,shape.table,"operation_uuid",inputs) match
        case MySqlFormat3Cell.Binary(b)=>b.equalsUuid(id)
        case _=>false
      val row=image.relation(shape.table).rows.find(own(_,operation)).get
      def rejected(candidate: MySqlScriptSchemaSnapshot,label: String): SQLDataException =
        val baseline=root.accountedMemoryBytes
        val failure=assertThrows(classOf[SQLDataException],()=>MySqlDbFirstInitialActivationPersistedRead.verifyCommitted(candidate,operation,root),label)
        assertEquals(baseline,root.accountedMemoryBytes,label+" must retire all failed verification workspace")
        failure
      def historicalReadFailure(candidate: MySqlScriptSchemaSnapshot): Unit =
        val baseline=root.accountedMemoryBytes
        root.reserveRetainedMemory(MySqlDbFirstSession.HistoricalRead.HolderBytes,"test failing historical holder BEFORE allocation")
        val read=new MySqlDbFirstSession.HistoricalRead(root,inputs)
        var primary: Throwable=null
        var suppressedBeforeClose=Vector.empty[Throwable]
        val propagated=assertThrows(classOf[SQLDataException],()=>{
          try
            read.acquire(None)
            read.validate(candidate)
          catch
            case cause: Throwable=>
              primary=cause
              suppressedBeforeClose=cause.getSuppressed.toVector
              assertThrows(classOf[IllegalStateException],()=>read.result)
              assertThrows(classOf[IllegalStateException],()=>read.dbFirstRecallSummary)
              throw cause
          finally MySqlFormat3BudgetCleanup.run(primary,Vector(() => read.close()))
        })
        assertSame(primary,propagated,"holder cleanup must preserve the original validation exception")
        assertEquals(suppressedBeforeClose,propagated.getSuppressed.toVector,"successful cleanup must preserve existing suppressed causes")
        assertTrue(propagated.getStackTrace.nonEmpty)
        assertEquals(baseline,root.accountedMemoryBytes,"failed actual HistoricalRead must retire its child and holder fees")
      def changedBlob(label:String,field:String,change:Array[Byte]=>Unit,expected:String="DB-first persisted DATA:"):Unit =
        val bytes=cell(row,shape.table,field+"_bytes",inputs).asInstanceOf[MySqlFormat3Cell.Binary].value.copyBytes(root,"test persisted mutation")
        change(bytes)
        root.reserveRetainedMemory(96L,"test persisted mutation digest BEFORE allocation")
        val digest=java.security.MessageDigest.getInstance("SHA-256").digest(bytes)
        val modified=row.updated(shape.columns.indexWhere(_.name==field+"_bytes"),MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes,root)))
          .updated(shape.columns.indexWhere(_.name==field+"_sha256"),MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(digest,root)))
        MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
          val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
          MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(modified,root.profile,child,Some(inputs))
        }
        val relations=image.relations.map(r=>if r.table==shape.table then r.copy(rows=r.rows.map(x=>if own(x,operation) then modified else x)) else r)
        val candidate=MySqlScriptSchemaSnapshot.captureMaterialized(image.schema,image.connection,image.observationScope,image.maxAllowedPacketBytes,inputs,relations,root)
        val failure=rejected(candidate,label)
        assertTrue(failure.getMessage.contains(expected),label+" must reach its persisted DATA boundary: "+failure.getMessage)
        if label=="witness commitment with valid outer digest" then
          assertEquals("DB-first persisted DATA: independent complete local transition differs",failure.getMessage)
        if label=="old ScopeV1" then historicalReadFailure(candidate)
      def find(bytes:Array[Byte],needle:Array[Byte],until:Int=Int.MaxValue):Int =
        val found=(0 to Math.min(bytes.length-needle.length,until-needle.length)).find(i=>needle.indices.forall(j=>bytes(i+j)==needle(j)))
        assertTrue(found.nonEmpty,"fixture wire token must exist")
        found.get
      def ascii(text:String)=text.getBytes(java.nio.charset.StandardCharsets.US_ASCII)
      def length(bytes:Array[Byte],offset:Int)=java.nio.ByteBuffer.wrap(bytes,offset,4).getInt()
      def markerOffsets(bytes:Array[Byte]):(Int,Int,Int) =
        var offset=find(bytes,ascii("CURRENT"))+"CURRENT".length
        offset+=4+length(bytes,offset) // table
        assertEquals(1,length(bytes,offset));offset+=4
        assertEquals(3,bytes(offset).toInt);offset+=1 // W singleton_fixed1 typed integer
        val pk=offset+7
        offset+=8
        val column=offset+4
        offset+=4+length(bytes,offset)
        (pk,column,offset+4)
      def descriptorOffsets(bytes:Array[Byte]):(Int,Int) =
        var offset=find(bytes,ascii("HISTORICAL"))+"HISTORICAL".length
        offset+=4+length(bytes,offset) // column
        offset+=4+length(bytes,offset) // declaration
        (offset,offset+8)
      changedBlob("old ScopeV1","scope",bytes=>bytes(4+MySqlDbFirstInitialActivationCanonical.Scope.length-1)='9'.toByte)
      changedBlob("original before digest","scope",bytes=>{
        val start=4+MySqlDbFirstInitialActivationCanonical.Scope.length+1+16+8+8+5*16+8+8+4+14+4*8+2*14+4
        bytes(start)=(bytes(start)^1).toByte
      })
      changedBlob("closed effect domain","target",bytes=>bytes(4)=(bytes(4)^1).toByte)
      changedBlob("original connection header","scope",bytes=>{
        val offset=find(bytes,ascii(MySqlScriptSchemaDataTestFixture.Connection.serverVersion))
        bytes(offset)='8'.toByte
      },"reconstructed complete before SHA differs")
      Vector("CURRENT PK"->0,"CURRENT column"->1,"CURRENT declaration"->2).foreach { (label,which)=>
        changedBlob(label,"target",bytes=>{
          val offsets=markerOffsets(bytes)
          val offset=if which==0 then offsets._1 else if which==1 then offsets._2 else offsets._3
          bytes(offset)=(bytes(offset)^1).toByte
        })
      }
      changedBlob("descriptor length","preimage",bytes=>{
        val offset=descriptorOffsets(bytes)._1+7;bytes(offset)=(bytes(offset)^1).toByte
      },"historical RAW descriptor length/SHA differs")
      changedBlob("descriptor SHA","preimage",bytes=>{
        val offset=descriptorOffsets(bytes)._2;bytes(offset)=(bytes(offset)^1).toByte
      },"historical RAW descriptor length/SHA differs")
      changedBlob("descriptor source PK","preimage",bytes=>{
        val until=find(bytes,ascii("HISTORICAL"))
        val uuid=MySqlScriptSchemaDataTestFixture.uuidBytes(id(700))
        val prefix=MySqlScriptSchemaDataTestFixture.uuidBytes(id(503)) ++ Array[Byte](2,0,0,0,16) ++ uuid
        val start=find(bytes,prefix,until)
        val offset=start+prefix.length-1
        bytes(offset)=(bytes(offset)^1).toByte
      },"missing original RAW by full PK")
      val nbt=cell(image.relation(MySqlDbFirstSchemaDefinition.SnapshotTable).rows.head,
        MySqlDbFirstSchemaDefinition.SnapshotTable,"nbt_bytes",inputs).asInstanceOf[MySqlFormat3Cell.Binary].value
        .copyBytes(root,"test actual persisted NBT mutation location")
      changedBlob("target NBT with valid outer digest","target",bytes=>{
        val offset=find(bytes,nbt)+nbt.length-2;bytes(offset)=(bytes(offset)^1).toByte
      },"snapshot NBT SHA-256 differs")
      changedBlob("witness commitment with valid outer digest","commit_witness",bytes=>bytes(bytes.length-1)=(bytes.last^1).toByte,
        "independent complete local transition differs")
      val old=image.relation(shape.table).rows.find(own(_,id(700))).get
      val oldRequest=cell(old,shape.table,"request_identity_bytes",inputs).asInstanceOf[MySqlFormat3Cell.Binary].value
        .copyBytes(root,"test changed original RAW request")
      oldRequest(oldRequest.length-1)=(oldRequest.last^1).toByte
      root.reserveRetainedMemory(96L,"test changed original RAW digest BEFORE allocation")
      val oldHash=java.security.MessageDigest.getInstance("SHA-256").digest(oldRequest)
      val changedOld=old.updated(shape.columns.indexWhere(_.name=="request_identity_bytes"),MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(oldRequest,root)))
        .updated(shape.columns.indexWhere(_.name=="request_identity_sha256"),MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(oldHash,root)))
      val changedRows=image.relations.map(r=>if r.table==shape.table then r.copy(rows=r.rows.map(x=>if own(x,id(700)) then changedOld else x)) else r)
      val changed=MySqlScriptSchemaSnapshot.captureMaterialized(image.schema,image.connection,image.observationScope,image.maxAllowedPacketBytes,inputs,changedRows,root)
      val changedFailure=rejected(changed,"changed original RAW")
      assertTrue(changedFailure.getMessage.contains("historical RAW descriptor length/SHA differs"))
      val missing=image.relations.map(r=>if r.table==shape.table then r.copy(rows=r.rows.filterNot(own(_,id(700)))) else r)
      // 初建快照的实际Full27关系校验已要求原prepare O；不能绕过该边界制造无效image。
      val failure=assertThrows(classOf[SQLDataException],()=>MySqlScriptSchemaSnapshot.captureMaterialized(
        image.schema,image.connection,image.observationScope,image.maxAllowedPacketBytes,inputs,missing,root))
      assertTrue(failure.getMessage.contains("initial snapshot lacks original preparation commit"))
    }

  @Test def persistedDbFirstRecallRejectsValidOuterRawWithChangedScopeEffectAndMissingHistory(): Unit =
    rejectCorruptSnapshotTransition(false)

  @Test def persistedActiveCheckpointRejectsValidOuterRawWithChangedScopeEffectAndMissingHistory(): Unit =
    rejectCorruptSnapshotTransition(true)

  private def rejectCorruptSnapshotTransition(checkpoint:Boolean): Unit =
    val scopeDomain=if checkpoint then "FP_DB_FIRST_ACTIVE_CHECKPOINT_SCOPE_BE_V2" else MySqlDbFirstRecallCanonical.Scope
    withPersistedDbFirstImage(checkpoint=checkpoint) { (image,root,operation)=>
      val inputs=image.inputs
      val shape=inputs.forTable("fp_recovery_operation")
      def own(row:Vector[MySqlFormat3Cell],id:UUID)=cell(row,shape.table,"operation_uuid",inputs) match
        case MySqlFormat3Cell.Binary(b)=>b.equalsUuid(id)
        case _=>false
      val row=image.relation(shape.table).rows.find(own(_,operation)).get
      def rejected(candidate: MySqlScriptSchemaSnapshot,label: String): SQLDataException =
        val baseline=root.accountedMemoryBytes
        val failure=assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallPersistedRead.verifyCommitted(candidate,operation,root),label)
        assertEquals(baseline,root.accountedMemoryBytes,label+" must retire all failed verification workspace")
        failure
      def historicalReadFailure(candidate: MySqlScriptSchemaSnapshot): Unit =
        val baseline=root.accountedMemoryBytes
        root.reserveRetainedMemory(MySqlDbFirstSession.HistoricalRead.HolderBytes,"test failing historical holder BEFORE allocation")
        val read=new MySqlDbFirstSession.HistoricalRead(root,inputs)
        var primary: Throwable=null
        var suppressedBeforeClose=Vector.empty[Throwable]
        val propagated=assertThrows(classOf[SQLDataException],()=>{
          try
            read.acquire(None)
            read.validate(candidate)
          catch
            case cause: Throwable=>
              primary=cause
              suppressedBeforeClose=cause.getSuppressed.toVector
              assertThrows(classOf[IllegalStateException],()=>read.result)
              assertThrows(classOf[IllegalStateException],()=>read.dbFirstRecallSummary)
              throw cause
          finally MySqlFormat3BudgetCleanup.run(primary,Vector(() => read.close()))
        })
        assertSame(primary,propagated,"holder cleanup must preserve the original validation exception")
        assertEquals(suppressedBeforeClose,propagated.getSuppressed.toVector,"successful cleanup must preserve existing suppressed causes")
        assertTrue(propagated.getStackTrace.nonEmpty)
        assertEquals(baseline,root.accountedMemoryBytes,"failed actual HistoricalRead must retire its child and holder fees")
      def changedBlob(label:String,field:String,change:Array[Byte]=>Unit,expected:String="DB-first persisted DATA:"):Unit =
        val bytes=cell(row,shape.table,field+"_bytes",inputs).asInstanceOf[MySqlFormat3Cell.Binary].value.copyBytes(root,"test persisted mutation")
        change(bytes)
        root.reserveRetainedMemory(96L,"test persisted mutation digest BEFORE allocation")
        val digest=java.security.MessageDigest.getInstance("SHA-256").digest(bytes)
        val modified=row.updated(shape.columns.indexWhere(_.name==field+"_bytes"),MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes,root)))
          .updated(shape.columns.indexWhere(_.name==field+"_sha256"),MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(digest,root)))
        MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
          val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
          MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(modified,root.profile,child,Some(inputs))
        }
        val relations=image.relations.map(r=>if r.table==shape.table then r.copy(rows=r.rows.map(x=>if own(x,operation) then modified else x)) else r)
        val candidate=MySqlScriptSchemaSnapshot.captureMaterialized(image.schema,image.connection,image.observationScope,image.maxAllowedPacketBytes,inputs,relations,root)
        val failure=rejected(candidate,label)
        assertTrue(failure.getMessage.contains(expected),label+" must reach its persisted DATA boundary: "+failure.getMessage)
        if label=="witness commitment with valid outer digest" then
          assertEquals("DB-first persisted DATA: independent complete local transition differs",failure.getMessage)
        if label=="old ScopeV1" then historicalReadFailure(candidate)
      def find(bytes:Array[Byte],needle:Array[Byte],until:Int=Int.MaxValue):Int =
        val found=(0 to Math.min(bytes.length-needle.length,until-needle.length)).find(i=>needle.indices.forall(j=>bytes(i+j)==needle(j)))
        assertTrue(found.nonEmpty,"fixture wire token must exist")
        found.get
      def ascii(text:String)=text.getBytes(java.nio.charset.StandardCharsets.US_ASCII)
      def length(bytes:Array[Byte],offset:Int)=java.nio.ByteBuffer.wrap(bytes,offset,4).getInt()
      def markerOffsets(bytes:Array[Byte]):(Int,Int,Int) =
        var offset=find(bytes,ascii("CURRENT"))+"CURRENT".length
        offset+=4+length(bytes,offset) // table
        assertEquals(1,length(bytes,offset));offset+=4
        assertEquals(3,bytes(offset).toInt);offset+=1 // W singleton_fixed1 typed integer
        val pk=offset+7
        offset+=8
        val column=offset+4
        offset+=4+length(bytes,offset)
        (pk,column,offset+4)
      def descriptorOffsets(bytes:Array[Byte]):(Int,Int) =
        var offset=find(bytes,ascii("HISTORICAL"))+"HISTORICAL".length
        offset+=4+length(bytes,offset) // column
        offset+=4+length(bytes,offset) // declaration
        (offset,offset+8)
      changedBlob("old ScopeV1","scope",bytes=>bytes(4+scopeDomain.length-1)='1'.toByte)
      changedBlob("original before digest","scope",bytes=>{
        val start=4+scopeDomain.length+1+16+8+8+5*16+8+8+14+4*8+2*14+4
        bytes(start)=(bytes(start)^1).toByte
      })
      changedBlob("closed effect domain","target",bytes=>bytes(4)=(bytes(4)^1).toByte)
      changedBlob("original connection header","scope",bytes=>{
        val offset=find(bytes,ascii(MySqlScriptSchemaDataTestFixture.Connection.serverVersion))
        bytes(offset)='8'.toByte
      },"reconstructed complete before SHA differs")
      Vector("CURRENT PK"->0,"CURRENT column"->1,"CURRENT declaration"->2).foreach { (label,which)=>
        changedBlob(label,"target",bytes=>{
          val offsets=markerOffsets(bytes)
          val offset=if which==0 then offsets._1 else if which==1 then offsets._2 else offsets._3
          bytes(offset)=(bytes(offset)^1).toByte
        })
      }
      changedBlob("descriptor length","preimage",bytes=>{
        val offset=descriptorOffsets(bytes)._1+7;bytes(offset)=(bytes(offset)^1).toByte
      },"historical RAW descriptor length/SHA differs")
      changedBlob("descriptor SHA","preimage",bytes=>{
        val offset=descriptorOffsets(bytes)._2;bytes(offset)=(bytes(offset)^1).toByte
      },"historical RAW descriptor length/SHA differs")
      changedBlob("descriptor source PK","preimage",bytes=>{
        val until=find(bytes,ascii("HISTORICAL"))
        val uuid=MySqlScriptSchemaDataTestFixture.uuidBytes(id(700))
        val prefix=MySqlScriptSchemaDataTestFixture.uuidBytes(id(503)) ++ Array[Byte](2,0,0,0,16) ++ uuid
        val start=find(bytes,prefix,until)
        val offset=start+prefix.length-1
        bytes(offset)=(bytes(offset)^1).toByte
      },"missing original RAW by full PK")
      val nbt=cell(image.relation(MySqlDbFirstSchemaDefinition.SnapshotTable).rows.head,
        MySqlDbFirstSchemaDefinition.SnapshotTable,"nbt_bytes",inputs).asInstanceOf[MySqlFormat3Cell.Binary].value
        .copyBytes(root,"test actual persisted NBT mutation location")
      changedBlob("target NBT with valid outer digest","target",bytes=>{
        val offset=find(bytes,nbt)+nbt.length-2;bytes(offset)=(bytes(offset)^1).toByte
      },"snapshot NBT SHA-256 differs")
      changedBlob("witness commitment with valid outer digest","commit_witness",bytes=>bytes(bytes.length-1)=(bytes.last^1).toByte,
        "independent complete local transition differs")
      val old=image.relation(shape.table).rows.find(own(_,id(700))).get
      val oldRequest=cell(old,shape.table,"request_identity_bytes",inputs).asInstanceOf[MySqlFormat3Cell.Binary].value
        .copyBytes(root,"test changed original RAW request")
      oldRequest(oldRequest.length-1)=(oldRequest.last^1).toByte
      root.reserveRetainedMemory(96L,"test changed original RAW digest BEFORE allocation")
      val oldHash=java.security.MessageDigest.getInstance("SHA-256").digest(oldRequest)
      val changedOld=old.updated(shape.columns.indexWhere(_.name=="request_identity_bytes"),MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(oldRequest,root)))
        .updated(shape.columns.indexWhere(_.name=="request_identity_sha256"),MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(oldHash,root)))
      val changedRows=image.relations.map(r=>if r.table==shape.table then r.copy(rows=r.rows.map(x=>if own(x,id(700)) then changedOld else x)) else r)
      val changed=MySqlScriptSchemaSnapshot.captureMaterialized(image.schema,image.connection,image.observationScope,image.maxAllowedPacketBytes,inputs,changedRows,root)
      val changedFailure=rejected(changed,"changed original RAW")
      assertTrue(changedFailure.getMessage.contains("historical RAW descriptor length/SHA differs"))
      val missing=image.relations.map(r=>if r.table==shape.table then r.copy(rows=r.rows.filterNot(own(_,id(700)))) else r)
      val candidate=MySqlScriptSchemaSnapshot.captureMaterialized(image.schema,image.connection,image.observationScope,image.maxAllowedPacketBytes,inputs,missing,root)
      val failure=rejected(candidate,"missing original RAW")
      assertTrue(failure.getMessage.contains("missing original RAW by full PK"))
    }

  /** 显式Full27 DATA fixture；旧O的generic context用真encoder生成，不声称SQL/held或历史业务witness资格。 */
  private def dbFirstBefore(f: Fixture,inputs: MySqlScriptSchemaInputs,
      fault: String="",checkpoint: Boolean=false,ownerless: Boolean=false,twoPods:Boolean=false,
      physicalGraph:Boolean=false): MySqlScriptSchemaSnapshot =
    import MySqlScriptSchemaDataTestFixture.{binary,uuidBytes}
    def u(n: Long)=binary(uuidBytes(id(n)))
    def i(n: Long)=MySqlFormat3Cell.SignedInteger(n)
    def t(s: String)=MySqlFormat3Cell.Text(s)
    val root=inputs.newAllocationRoot(inputs.budget.packetBytes,f.shared)
    def change(row: Vector[MySqlFormat3Cell],table: String,values: (String,MySqlFormat3Cell)*): Vector[MySqlFormat3Cell] =
      values.foldLeft(row)((r,p)=>r.updated(inputs.forTable(table).columns.indexWhere(_.name==p._1),p._2))
    val oldInputs=MySqlScriptSchemaDataTestFixture.Full26Inputs
    var rows=MySqlScriptSchemaDataTestFixture.full26GenesisRelations(oldInputs,true).map { relation =>
      val mapped=relation.rows.map { r=>relation.table match
        case "fp_schema_manifest"=>change(r,relation.table,"format_version"->i(6),"expected_binding_uuid"->u(503),
          "ddl_sha256"->binary(java.util.HexFormat.of().parseHex(inputs.ddlSha256)),
          "limit_profile_bytes"->binary(inputs.sourceLimitProfile.canonicalBytes()),"limit_profile_sha256"->binary(inputs.sourceLimitProfile.sha256()),
          "max_region_bytes"->i(f.limits.maxRegionBytes()),"max_chunk_bytes"->i(f.limits.maxChunkBytes()),
          "max_decompressed_bytes"->i(f.limits.maxDecompressedBytes()),"max_nbt_depth"->i(f.limits.maxNbtDepth()),
          "max_members"->i(f.limits.maxMembers()),"max_retained_bytes"->i(f.limits.maxRetainedBytes()),
          "max_sql_packet_bytes"->i(f.limits.maxSqlPacketBytes()),"max_native_artifact_bytes"->i(f.limits.maxNativeArtifactBytes()))
        case "physical_recovery_binding"=>change(r,relation.table,"binding_uuid"->u(503),"cutover_state"->t("CERTIFIED"))
        case "fp_runtime_writer"=>change(r,relation.table,"binding_uuid"->u(503),"writer_epoch"->i(3),"owner_boot_uuid"->u(1),
          "lease_until"->MySqlFormat3Cell.DateTime(java.time.LocalDateTime.of(2026,10,29,13,0)),
          "last_commit_operation_uuid"->MySqlFormat3Cell.SqlNull,"last_commit_fingerprint"->MySqlFormat3Cell.SqlNull)
        case _=>r
      }
      relation.copy(rows=mapped)
    } :+ MySqlFormat3RelationSnapshot(MySqlDbFirstSchemaDefinition.SnapshotTable,Vector.empty)
    def fixtureRow(table: String,values: (String,MySqlFormat3Cell)*)=
      MySqlScriptSchemaDataTestFixture.row(table,values*)
    val domain=fixtureRow("floating_pod_state","uuid"->u(502),"owner_uuid"->u(501),"binding_uuid"->u(503),
      "state_version"->i(7),"is_active"->i(1),"world_name"->t(if fault=="world" then "other-world" else "world"),
      "dimension_id"->i(if fault=="dimension" then 1 else 0),"operating_mode"->t(if ownerless then "WORLD_IDLE" else "NORMAL"),
      "pos_x"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(if checkpoint then 1D else 0D)),
      "pos_y"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(if checkpoint then 64D else 0D)),
      "pos_z"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(if checkpoint then 2D else 0D)),
      "yaw"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(if checkpoint then 3D else 0D)),
      "pitch"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(if checkpoint then 4D else 0D)),
      "last_mutation_operation_uuid"->u(700))
    val actualDomain=if ownerless then change(domain,"floating_pod_state","owner_uuid"->MySqlFormat3Cell.SqlNull) else domain
    val locator=fixtureRow("physical_recovery_locator","binding_uuid"->u(503),"pod_uuid"->u(502),"current_attempt_uuid"->u(504),
      "current_footprint_uuid"->u(505),"locator_state"->t("ROW_ACTIVE"),"reason_code"->t("ROW_ACTIVE"),
      "domain_version"->i(7),"target_dimension_id"->i(0),"locator_revision"->i(9),"last_mutation_operation_uuid"->u(700))
    val old=MainBootProjectionFixture.withCommittedOperationReferences(Map("floating_pod_state"->Vector(domain)),Set(id(700)))("fp_recovery_operation").head
    val oldContext=MySqlOperationContext(id(1),3L,id(503),id(700),Vector(MySqlExpectedEffect("fixture-original",6L,Vector.fill(32)(7.toByte))))
    val encoded=MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      MySqlRecoveryCanonicalBytes.encode("FP_RECOVERY_OPERATION_BE_V1",root.profile)(out=>MySqlModeOperationEffectCodec.writeOperationContext(out,oldContext))
    }
    val historical=change(old,"fp_recovery_operation","binding_uuid"->u(503),"boot_uuid"->u(1),"boot_generation"->i(1),
      "connection_generation_uuid"->u(701),"connection_id"->i(90),"fence_epoch"->i(3),
      "pod_uuid"->u(502),"attempt_uuid"->u(504),"footprint_uuid"->u(505),"expected_domain_version"->i(6),"expected_locator_revision"->i(8),
      "planned_at"->MySqlFormat3Cell.DateTime(java.time.LocalDateTime.of(2026,10,29,10,0)),
      "result_at"->MySqlFormat3Cell.DateTime(java.time.LocalDateTime.of(2026,10,29,10,1)),
      "fixed_lease_until"->MySqlFormat3Cell.DateTime(java.time.LocalDateTime.of(2026,10,29,11,0)),
      "original_context_bytes"->encoded.binaryCell,"original_context_sha256"->binary(encoded.sha256.toArray),
      "operation_fingerprint"->binary(oldContext.canonicalFingerprintSha256.toArray))
    val writer=change(rows.find(_.table=="fp_runtime_writer").get.rows.head,"fp_runtime_writer",
      "last_commit_operation_uuid"->u(700),"last_commit_fingerprint"->binary(oldContext.canonicalFingerprintSha256.toArray))
    val replaced=Map("floating_pod_state"->Vector(actualDomain),"physical_recovery_locator"->Vector(locator),
      "fp_recovery_operation"->Vector(historical),"fp_runtime_writer"->Vector(writer))
    rows=rows.map(r=>r.copy(rows=replaced.getOrElse(r.table,r.rows)))
    if fault=="revision" || fault=="released" then
      val key=MySqlRecoveryCanonicalBytes.withAllocationBudget(root){MySqlRecoveryResourceKey.pod(id(502),root.profile)}
      val bytes=key.copyCanonicalBytes(root,"fixture original POD key")
      val reservation=fixtureRow("fp_recovery_reservation","binding_uuid"->u(503),"resource_kind"->t("POD"),
        "resource_key"->binary(bytes),"resource_key_hash"->binary(key.keyHashSha256.toArray),
        "resource_pod_uuid"->u(502),"reservation_revision"->i(if fault=="revision" then Long.MaxValue-1L else 7L),"reservation_state"->t("RELEASED"),
        "workflow_uuid"->u(700),"operation_uuid"->u(700),"last_mutation_operation_uuid"->u(700))
      rows=rows.map(r=>if r.table=="fp_recovery_reservation" then r.copy(rows=Vector(reservation)) else r)
    if fault=="history-reserved" then
      val context=MySqlOperationContext(id(1),3L,id(503),id(777),Vector(MySqlExpectedEffect("fixture-original",6L,Vector.fill(32)(7.toByte))))
      val canonical=MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        MySqlModeOperationEffectCodec.operationContextCanonicalBytes(context,root.profile)
      }
      val pending=change(historical,"fp_recovery_operation","operation_uuid"->u(777),"workflow_uuid"->u(777),
        "operation_state"->t("RESERVED"),"result_at"->MySqlFormat3Cell.SqlNull,
        "commit_witness_bytes"->MySqlFormat3Cell.SqlNull,"commit_witness_sha256"->MySqlFormat3Cell.SqlNull,
        "original_context_bytes"->canonical.binaryCell,"original_context_sha256"->binary(canonical.sha256.toArray),
        "operation_fingerprint"->binary(context.canonicalFingerprintSha256.toArray))
      rows=rows.map(r=>if r.table=="fp_recovery_operation" then r.copy(rows=(r.rows :+ pending)
        .sortWith((a,b)=>MySqlControlledDebugDelta.compareKey(a,b,inputs.forTable(r.table))<0)) else r)
    if twoPods then
      rows=rows.map { relation=>
        val extra=relation.table match
          case "floating_pod_state"=>Some(change(relation.rows.head,relation.table,"uuid"->u(602)))
          case "physical_recovery_locator"=>Some(change(relation.rows.head,relation.table,"pod_uuid"->u(602),"current_attempt_uuid"->u(604)))
          case _=>None
        extra.fold(relation)(row=>relation.copy(rows=relation.rows :+ row))
      }
    if physicalGraph then
      def d(n:Double)=MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(n))
      val footprint=fixtureRow("physical_recovery_footprint","binding_uuid"->u(503),"pod_uuid"->u(502),
        "attempt_uuid"->u(504),"footprint_uuid"->u(505),"origin_version"->i(6),"target_world_name"->t("world"),
        "target_pos_x"->d(0),"target_pos_y"->d(0),"target_pos_z"->d(0),"target_yaw"->d(0),"target_pitch"->d(0),
        "target_dimension_id"->i(0),"center_chunk_x"->i(0),"center_chunk_z"->i(0),"seal_state"->t("SEALED"),
        "created_operation_uuid"->u(700))
      val chunks=(for x <- -2 to 2; z <- -2 to 2 yield fixtureRow("physical_recovery_footprint_chunk",
        "binding_uuid"->u(503),"pod_uuid"->u(502),"attempt_uuid"->u(504),"footprint_uuid"->u(505),
        "dimension_id"->i(0),"chunk_x"->i(x),"chunk_z"->i(z))).toVector
      val intent=fixtureRow("physical_recovery_intent","binding_uuid"->u(503),"pod_uuid"->u(502),"attempt_uuid"->u(504),
        "origin"->t("OWNED_RELEASE"),"phase"->t("ACTIVATED"),"target_footprint_uuid"->u(505),
        "prepare_operation_uuid"->u(700),"last_mutation_operation_uuid"->u(700),
        "expected_domain_version"->i(6),"expected_owner_uuid"->u(501),"activation_operation_uuid"->u(700),
        "committed_domain_version"->i(7),"committed_at"->i(1),"committed_world_name"->t("world"),
        "committed_pos_x"->d(0),"committed_pos_y"->d(0),"committed_pos_z"->d(0),"committed_yaw"->d(0),
        "committed_pitch"->d(0),"committed_dimension_id"->i(0))
      val graph=Map("physical_recovery_footprint"->Vector(footprint),"physical_recovery_footprint_chunk"->chunks,
        "physical_recovery_intent"->Vector(intent))
      rows=rows.map { relation=>
        val bindingIndex=inputs.forTable(relation.table).columns.indexWhere(_.name=="binding_uuid")
        val actual=graph.getOrElse(relation.table,relation.rows)
        relation.copy(rows=if bindingIndex<0 then actual else actual.map(_.updated(bindingIndex,u(503))))
      }
    val owned=rows.map(r=>r.copy(rows=r.rows.map(_.map {
      case MySqlFormat3Cell.Binary(bytes)=>MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes.copyBytes(root,"Full27 fixture actual binary custody"),root))
      case c=>c
    })))
    MySqlScriptSchemaSnapshot.captureMaterialized(inputs.schema,MySqlScriptSchemaDataTestFixture.Connection.copy(catalog=inputs.schema),
      MySqlFormat3ObservationScope(id(1),1L,1L),inputs.budget.packetBytes,inputs,owned,root)

  @Test def recallJournalMaterializesExactOAndWWhilePreservingOther25Tables():Unit =
    val inputs=dbFirstMaterializationInputs()
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var before:MySqlScriptSchemaSnapshot=null
    try
      before=dbFirstBefore(f,inputs)
      val root=before.allocationBudget
      val j=IoOperationTag(id(1),1L,1L,id(1501))
      val c=j.copy(operationId=id(1502))
      val request=MySqlDbFirstRecallJournalRequest.fresh(j,c,id(503),id(501),id(502),7L,Some(id(1503)),root)
      val planned=java.time.LocalDateTime.of(2026,10,29,12,0)
      val facts=MySqlDbFirstRecallJournalFacts(91L,id(1504),3L,planned.plusHours(1),planned,planned.plusSeconds(1))
      val draft=MySqlDbFirstRecallJournalMaterializer.allocate(request,facts,before,root)
      val result=MySqlDbFirstRecallJournalMaterializer.materialize(draft)
      assertSame(result,draft.result)
      assertTrue(draft.attempted);assertNull(draft.failure)
      val o=result.target.relation("fp_recovery_operation")
      def field(row:Vector[MySqlFormat3Cell],table:String,name:String)=
        MySqlDbFirstRecallMaterializer.value(row,table,name,inputs)
      val own=o.rows.find(r=>MySqlDbFirstRecallMaterializer.uuidEquals(field(r,o.table,"operation_uuid"),j.operationId)).get
      assertEquals(MySqlFormat3Cell.Text("DB_FIRST_RECALL_REQUEST"),field(own,o.table,"step"))
      assertTrue(MySqlDbFirstRecallMaterializer.uuidEquals(field(own,o.table,"workflow_uuid"),c.operationId))
      assertEquals(MySqlFormat3Cell.Text("COMMITTED"),field(own,o.table,"operation_state"))
      assertEquals(MySqlFormat3Cell.Text("RESERVED"),field(result.reservedOperation,o.table,"operation_state"))
      for name<-Vector("result_at","commit_witness_bytes","commit_witness_sha256","settled_by_operation_uuid") do
        assertEquals(MySqlFormat3Cell.SqlNull,field(result.reservedOperation,o.table,name))
      assertFalse(o.rows.exists(r=>MySqlDbFirstRecallMaterializer.uuidEquals(field(r,o.table,"operation_uuid"),c.operationId)),
        "journal does not fabricate original business or refusal outcome")
      assertEquals(before.relation(o.table).rows,o.rows.filterNot(_ eq own))
      before.relations.filterNot(r=>r.table==o.table || r.table=="fp_runtime_writer").foreach { r=>
        assertEquals(r,result.target.relation(r.table),"journal may not change "+r.table)
      }
      val oldW=before.relation("fp_runtime_writer").rows.head
      val newW=result.target.relation("fp_runtime_writer").rows.head
      for column<-inputs.forTable("fp_runtime_writer").columns if !Set("last_commit_operation_uuid","last_commit_fingerprint").contains(column.name) do
        assertEquals(field(oldW,"fp_runtime_writer",column.name),field(newW,"fp_runtime_writer",column.name))
      val retained=root.accountedMemoryBytes
      result.verify();result.verify()
      assertEquals(retained,root.accountedMemoryBytes,"complete comparison scopes actually retire")
      def tamper(table:String,column:String,cell:MySqlFormat3Cell,ownOnly:Boolean=false,
          extra:Vector[(String,MySqlFormat3Cell)]=Vector.empty,captureRejected:Boolean=false):Unit =
        val relations=result.target.relations.map { r=>
          if r.table!=table then r else r.copy(rows=r.rows.map { row=>
            if ownOnly && !(row eq own) then row else
              ((column,cell) +: extra).foldLeft(row)((current,pair)=>
                current.updated(inputs.forTable(table).columns.indexWhere(_.name==pair._1),pair._2))
          })
        }
        def capture()=MySqlScriptSchemaSnapshot.captureMaterialized(result.target.schema,result.target.connection,
          result.target.observationScope,result.target.maxAllowedPacketBytes,inputs,relations,root)
        if captureRejected then assertThrows(classOf[SQLDataException],()=>{capture();()},table+"."+column+" must fail the earlier Full27 boundary")
        else
          val changed=capture()
          val failure=assertThrows(classOf[SQLDataException],()=>result.verifyTarget(changed),table+"."+column)
          if extra.nonEmpty then assertEquals("journal independent complete O/W transition differs",failure.getMessage,
            "a valid generic context must reach the independent full transition comparison")
      tamper("floating_pod_state","pos_x",MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(123D)))
      tamper("fp_runtime_writer","last_commit_fingerprint",MySqlScriptSchemaDataTestFixture.binary(Array.fill(32)(8.toByte)))
      tamper(o.table,"original_context_bytes",MySqlScriptSchemaDataTestFixture.binary(Array(1.toByte)),true,captureRejected=true)
      val differentContext=MySqlOperationContext(j.bootId,facts.writerEpoch,request.bindingUuid,j.operationId,
        Vector(MySqlExpectedEffect("DB_FIRST_RECALL_REQUEST:"+c.operationId,7L,Vector.fill(32)(8.toByte))))
      val validContext=MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        MySqlModeOperationEffectCodec.operationContextCanonicalBytes(differentContext,root.profile)
      }
      tamper(o.table,"original_context_bytes",validContext.binaryCell,true,
        extra=Vector("original_context_sha256"->MySqlScriptSchemaDataTestFixture.binary(validContext.sha256.toArray)))
      tamper(o.table,"target_sha256",MySqlScriptSchemaDataTestFixture.binary(Array.fill(32)(8.toByte)),true,captureRejected=true)
      tamper(o.table,"workflow_uuid",MySqlScriptSchemaDataTestFixture.binary(MySqlScriptSchemaDataTestFixture.uuidBytes(j.operationId)),true)
      assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstRecallJournalMaterializer.materialize(draft))
      assertNull(draft.failure,"replay guard does not poison existing completed DATA")
    finally
      if before!=null then before.allocationBudget.closeRootReadBudget()
      f.close()

  @Test def recallJournalRejectsWrongBeforeIdentityAndRetainsExactFailedDraftWithoutRetry():Unit =
    val inputs=dbFirstMaterializationInputs()
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var before:MySqlScriptSchemaSnapshot=null
    try
      before=dbFirstBefore(f,inputs)
      val root=before.allocationBudget
      val j=IoOperationTag(id(1),1L,1L,id(1511))
      val planned=java.time.LocalDateTime.of(2026,10,29,12,0)
      val facts=MySqlDbFirstRecallJournalFacts(91L,id(1514),3L,planned.plusHours(1),planned,planned.plusSeconds(1))
      def request(actor:UUID=id(501),binding:UUID=id(503),pod:UUID=id(502),version:Long=7L,c:UUID=id(1512))=
        MySqlDbFirstRecallJournalRequest.fresh(j,j.copy(operationId=c),binding,actor,pod,version,Some(id(1513)),root)
      val cases=Vector(request(actor=id(9)),request(binding=id(9)),request(pod=id(9)),request(version=6L),request(c=id(700)))
      cases.foreach { value=>
        val draft=MySqlDbFirstRecallJournalMaterializer.allocate(value,facts,before,root)
        val problem=assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallJournalMaterializer.materialize(draft))
        assertSame(problem,draft.failure)
        assertSame(before,draft.before)
        assertTrue(draft.attempted);assertNull(draft.result)
        assertFalse(root.isClosed)
        assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstRecallJournalMaterializer.materialize(draft))
        assertSame(problem,draft.failure)
      }
      val wrongEpoch=MySqlDbFirstRecallJournalMaterializer.allocate(request(),facts.copy(writerEpoch=4L),before,root)
      assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallJournalMaterializer.materialize(wrongEpoch))
      val wrongLease=MySqlDbFirstRecallJournalMaterializer.allocate(request(),facts.copy(leaseUntil=planned.plusHours(2)),before,root)
      assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallJournalMaterializer.materialize(wrongLease))
    finally
      if before!=null then before.allocationBudget.closeRootReadBudget()
      f.close()

  @Test def recallJournalConstructionCapacityFailureKeepsOriginalRootAndCause():Unit =
    val inputs=dbFirstMaterializationInputs()
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var before:MySqlScriptSchemaSnapshot=null
    try
      before=dbFirstBefore(f,inputs)
      val root=before.allocationBudget
      val j=IoOperationTag(id(1),1L,1L,id(1521))
      val request=MySqlDbFirstRecallJournalRequest.fresh(j,j.copy(operationId=id(1522)),id(503),id(501),id(502),7L,None,root)
      val planned=java.time.LocalDateTime.of(2026,10,29,12,0)
      val facts=MySqlDbFirstRecallJournalFacts(91L,id(1524),3L,planned.plusHours(1),planned,planned.plusSeconds(1))
      val draft=MySqlDbFirstRecallJournalMaterializer.allocate(request,facts,before,root)
      val pressure=root.availableMemoryBytes-32768L
      root.reserveRetainedMemory(pressure,"journal construction pressure")
      val retained=root.accountedMemoryBytes
      val failure=assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallJournalMaterializer.materialize(draft))
      assertSame(failure,draft.failure)
      assertSame(root,draft.root);assertSame(before,draft.before);assertSame(request,draft.request)
      assertTrue(draft.attempted);assertNull(draft.result);assertFalse(root.isClosed)
      assertEquals(retained,root.accountedMemoryBytes,"failed first container charge did not refund accepted inputs")
      root.releaseRetainedMemory(pressure,"journal construction pressure removed")
      assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstRecallJournalMaterializer.materialize(draft))
    finally
      if before!=null then before.allocationBudget.closeRootReadBudget()
      f.close()

  @Test def recallJournalPreflightCannotTrustRequestBytesWhoseCarrierNamesAnotherResponsibility():Unit =
    val inputs=dbFirstMaterializationInputs()
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var before:MySqlScriptSchemaSnapshot=null
    try
      before=dbFirstBefore(f,inputs,twoPods=true)
      val root=before.allocationBudget
      val j=IoOperationTag(id(1),1L,1L,id(1531))
      val c=j.copy(operationId=id(1532))
      val request=MySqlDbFirstRecallJournalRequest.fresh(j,c,id(503),id(501),id(502),7L,Some(id(1533)),root)
      val planned=java.time.LocalDateTime.of(2026,10,29,12,0)
      val facts=MySqlDbFirstRecallJournalFacts(91L,id(1534),3L,planned.plusHours(1),planned,planned.plusSeconds(1))
      val result=MySqlDbFirstRecallJournalMaterializer.materialize(
        MySqlDbFirstRecallJournalMaterializer.allocate(request,facts,before,root))
      val nextJ=j.copy(operationId=id(1541))
      val next=MySqlDbFirstRecallJournalRequest.fresh(nextJ,nextJ.copy(operationId=id(1542)),
        id(503),id(501),id(502),7L,Some(id(1543)),root)
      assertThrows(classOf[SQLDataException],()=>
        MySqlDbFirstRecallJournalMaterializer.preflight(next,facts,result.target,root),"actual unresolved same pod must remain held")
      val unrelated=MySqlDbFirstRecallJournalRequest.fresh(nextJ,nextJ.copy(operationId=id(1542)),
        id(503),id(501),id(602),7L,Some(id(1543)),root)
      val permitted:Executable=()=>MySqlDbFirstRecallJournalMaterializer.preflight(unrelated,facts,result.target,root)
      assertDoesNotThrow(permitted,
        "an unresolved journal for another pod cannot block this pod")
      def rejects(other:MySqlDbFirstRecallJournalRequest,label:String):Unit =
        val rows=result.target.relations.map { relation=>
          if relation.table!="fp_recovery_operation" then relation else relation.copy(rows=relation.rows.map { row=>
            if !MySqlDbFirstRecallMaterializer.uuidEquals(MySqlDbFirstRecallMaterializer.value(row,relation.table,"operation_uuid",inputs),j.operationId) then row
            else row.updated(inputs.forTable(relation.table).columns.indexWhere(_.name=="request_identity_bytes"),other.canonical.binaryCell)
              .updated(inputs.forTable(relation.table).columns.indexWhere(_.name=="request_identity_sha256"),
                MySqlScriptSchemaDataTestFixture.binary(other.canonical.sha256.toArray))
          })
        }
        val corrupted=MySqlScriptSchemaSnapshot.captureMaterialized(result.target.schema,result.target.connection,
          result.target.observationScope,result.target.maxAllowedPacketBytes,inputs,rows,root)
        assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallJournalMaterializer.preflight(next,facts,corrupted,root),label)
      def encoded(jt:IoOperationTag=j,ct:IoOperationTag=c,binding:UUID=id(503),pod:UUID=id(502),version:Long=7L)=
        MySqlDbFirstRecallJournalRequest.fresh(jt,ct,binding,id(501),pod,version,Some(id(1533)),root)
      rejects(encoded(binding=id(999)),"binding cannot differ from actual carrier O")
      rejects(encoded(jt=j.copy(operationId=id(1551))),"J cannot differ from actual carrier O")
      rejects(encoded(ct=c.copy(operationId=id(1552))),"C cannot differ from actual carrier O")
      rejects(encoded(pod=id(1553)),"pod cannot differ from actual carrier O")
      rejects(encoded(jt=j.copy(bootId=id(1554)),ct=c.copy(bootId=id(1554))),"boot cannot differ from actual carrier O")
      rejects(encoded(jt=j.copy(bootGeneration=2L),ct=c.copy(bootGeneration=2L)),"boot generation cannot differ from actual carrier O")
      rejects(encoded(jt=j.copy(connectionGeneration=2L),ct=c.copy(connectionGeneration=2L)),"numeric connection generation cannot differ from actual scope")
      rejects(encoded(version=8L),"version cannot differ from actual carrier O")
    finally
      if before!=null then before.allocationBudget.closeRootReadBudget()
      f.close()

  /** 丢弃构造端root后仅从实际SnapshotRead child验证，不把原draft当持久证据。 */
  private def withPersistedRecallJournalImage(body:(MySqlScriptSchemaSnapshot,MySqlScriptSchemaSnapshot,
      MySqlFormat3ReadBudget,UUID)=>Unit):Unit =
    val inputs=dbFirstMaterializationInputs()
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var source:MySqlScriptSchemaSnapshot=null
    var cold:MySqlFormat3ReadBudget=null
    var child:MySqlFormat3ReadBudget=null
    var primary:Throwable=null
    try
      source=dbFirstBefore(f,inputs,physicalGraph=true)
      val j=IoOperationTag(id(1),1L,1L,id(1561))
      val request=MySqlDbFirstRecallJournalRequest.fresh(j,j.copy(operationId=id(1562)),id(503),id(501),id(502),7L,Some(id(1563)),source.allocationBudget)
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      val facts=MySqlDbFirstRecallJournalFacts(91L,id(1564),3L,at.plusHours(1),at,at.plusSeconds(1))
      val target=MySqlDbFirstRecallJournalMaterializer.materialize(
        MySqlDbFirstRecallJournalMaterializer.allocate(request,facts,source,source.allocationBudget)).target
      cold=inputs.newAllocationRoot(inputs.budget.packetBytes,f.shared)
      val later=MySqlFormat3ObservationScope(id(1599),9L,11L)
      cold.initialize(inputs.schema,source.connection,later,source.maxAllowedPacketBytes)
      child=cold.newScopedReadBudget(inputs.budget.packetBytes)
      child.initialize(inputs.schema,source.connection,later,source.maxAllowedPacketBytes)
      val image=MySqlScriptSchemaSnapshot.decoded(inputs.schema,source.connection,later,source.maxAllowedPacketBytes,
        inputs,copyPersistedRows(target.relations,child),child)
      val anchor=MySqlScriptSchemaSnapshot.decoded(inputs.schema,source.connection,later,source.maxAllowedPacketBytes,
        inputs,copyPersistedRows(source.relations,child),child)
      source.allocationBudget.closeRootReadBudget()
      assertTrue(source.allocationBudget.isClosed)
      body(image,anchor,cold,j.operationId)
    catch case cause:Throwable=>
      primary=cause
      throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(
      ()=>if child!=null then child.closeScopedReadBudget(),()=>if cold!=null then cold.closeRootReadBudget(),
      ()=>if source!=null && !source.allocationBudget.isClosed then source.allocationBudget.closeRootReadBudget(),()=>f.close()))

  /** 仅DATA编解码/增量验证；不通过此fixture授予J提交确认、freeze或C执行许可。 */
  private def withJournalBoundRecall(client:Option[UUID],snapshotBytes:Long=131072L,crossScope:Boolean=false)(
      body:(MySqlScriptSchemaSnapshot,MySqlScriptSchemaSnapshot,DbFirstPodSnapshot,
        MySqlFormat3ReadBudget,MySqlDbFirstRecallJournalRequest)=>Unit):Unit =
    val inputs=dbFirstMaterializationInputs(snapshotBytes)
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var before:MySqlScriptSchemaSnapshot=null
    var dataRoot:MySqlFormat3ReadBudget=null
    var primary:Throwable=null
    try
      before=dbFirstBefore(f,inputs,physicalGraph=true)
      val j=IoOperationTag(id(1),1L,1L,id(1901));val c=j.copy(operationId=id(1902))
      val request=MySqlDbFirstRecallJournalRequest.fresh(j,c,id(503),id(501),id(502),7L,client,before.allocationBudget)
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      val target=MySqlDbFirstRecallJournalMaterializer.materialize(MySqlDbFirstRecallJournalMaterializer.allocate(
        request,MySqlDbFirstRecallJournalFacts(91L,id(1904),3L,at.plusHours(1),at,at.plusSeconds(1)),before,before.allocationBudget)).target
      val reservation=f.core.reserveDbFirstCapture(c)
      val captured=f.core.captureDbFirstSnapshot(reservation)(a=>DbFirstPodSnapshotTest.captureForCore(c.operationId,a,100))
      // DATA验证不冒充retained C执行：capture root保持原线程守卫，另在同账本计费纯物化root。
      dataRoot=inputs.newAllocationRoot(inputs.budget.packetBytes,f.shared)
      val current=if !crossScope then target else
        val rows=copyPersistedRows(target.relations,dataRoot).map { relation=>
          if relation.table!="fp_runtime_writer" then relation else relation.copy(rows=relation.rows.map { row=>
            Vector("owner_boot_uuid"->MySqlScriptSchemaDataTestFixture.binary(MySqlScriptSchemaDataTestFixture.uuidBytes(id(1910))),
              "writer_epoch"->MySqlFormat3Cell.SignedInteger(4L),"lease_until"->MySqlFormat3Cell.DateTime(at.plusHours(2))).foldLeft(row) { (r,pair)=>
              r.updated(inputs.forTable(relation.table).columns.indexWhere(_.name==pair._1),pair._2)
            }
          })
        }
        MySqlScriptSchemaSnapshot.captureMaterialized(inputs.schema,target.connection,
          MySqlFormat3ObservationScope(id(1910),9L,11L),target.maxAllowedPacketBytes,inputs,rows,dataRoot)
      body(before,current,captured,dataRoot,request)
    catch case cause:Throwable=>
      primary=cause
      throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(
      ()=>if dataRoot!=null && !dataRoot.isClosed then dataRoot.closeRootReadBudget(),
      ()=>if before!=null && !before.allocationBudget.isClosed then before.allocationBudget.closeRootReadBudget(),()=>f.close()))

  @Test def journalBoundRecallV2ColdReconstructsExactJAndOriginalCForBothClientOptions():Unit =
    Vector(None,Some(id(1903))).foreach { client=>
      withJournalBoundRecall(client,524288L) { (anchor,source,captured,root,journal)=>
        var backing:MySqlRepositoryRequestIdentity=null
        val request=MySqlDbFirstRecallRequest.capture(journal.originalTag,captured,root,
          Some(MySqlDbFirstRecallJournalLink.from(journal)))(b=>backing=b)
        val at=java.time.LocalDateTime.of(2026,10,29,12,0)
        val target=MySqlDbFirstRecallMaterializer.materialize(request,captured,backing,source,91L,id(1905),
          at.plusSeconds(2),at.plusSeconds(3),new MySqlDbFirstRecallDraft(root)).target
        assertTrue(target.canonicalByteLength>131072L)
        assertTrue(target.canonicalByteLength<=524288L)
        println(s"J-bound C V2 synthetic complete target=${target.canonicalByteLength}, explicit snapshot cap=524288, client=$client")
        val row=target.relation("fp_recovery_operation").rows.find(r=>MySqlDbFirstRecallMaterializer.uuidEquals(
          cell(r,"fp_recovery_operation","operation_uuid",source.inputs),journal.originalTag.operationId)).get
        assertTrue(MySqlDbFirstRecallMaterializer.uuidEquals(cell(row,"fp_recovery_operation","previous_step_operation_uuid",source.inputs),journal.journalTag.operationId))
        val cold=source.inputs.newAllocationRoot(source.maxAllowedPacketBytes,
          new MySqlFormat3AllocationLedger(root.profile.maxRetainedBytes()))
        val later=MySqlFormat3ObservationScope(id(1999),9L,11L)
        cold.initialize(source.schema,source.connection,later,source.maxAllowedPacketBytes)
        val child=cold.newScopedReadBudget(source.maxAllowedPacketBytes)
        child.initialize(source.schema,source.connection,later,source.maxAllowedPacketBytes)
        try
          val image=MySqlScriptSchemaSnapshot.decoded(source.schema,source.connection,later,source.maxAllowedPacketBytes,
            source.inputs,copyPersistedRows(target.relations,child),child)
          val original=MySqlScriptSchemaSnapshot.decoded(source.schema,source.connection,later,source.maxAllowedPacketBytes,
            source.inputs,copyPersistedRows(anchor.relations,child),child)
          source.allocationBudget.closeRootReadBudget()
          root.closeRootReadBudget()
          val baseline=cold.accountedMemoryBytes
          (0 until 3).foreach { _=>
            MySqlDbFirstRecallPersistedRead.verifyCommitted(image,journal.originalTag.operationId,cold)
            assertEquals(2L,MySqlDbFirstRecallPersistedRead.verifySnapshotReceiptSegment(image,original,cold))
          }
          assertEquals(baseline,cold.accountedMemoryBytes,"nested J/C cold comparisons retire their actual scopes")
          cold.reserveRetainedMemory(MySqlDbFirstSession.HistoricalRead.HolderBytes,"V2 history holder BEFORE allocation")
          val history=new MySqlDbFirstSession.HistoricalRead(cold,source.inputs)
          try
            history.acquire(None);history.validate(image)
            assertEquals(1L,history.dbFirstRecallSummary.verifiedCommitted)
          finally history.close()
          assertEquals(baseline,cold.accountedMemoryBytes)
        finally
          child.closeScopedReadBudget();cold.closeRootReadBudget()
      }
    }

  @Test def journalBoundRecallDoesNotBypassActualSmallSnapshotBudget():Unit =
    withJournalBoundRecall(Some(id(1903))) { (_,source,captured,root,journal)=>
      var backing:MySqlRepositoryRequestIdentity=null
      val request=MySqlDbFirstRecallRequest.capture(journal.originalTag,captured,root,
        Some(MySqlDbFirstRecallJournalLink.from(journal)))(b=>backing=b)
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      val failure=assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallMaterializer.materialize(
        request,captured,backing,source,91L,id(1905),at.plusSeconds(2),at.plusSeconds(3),new MySqlDbFirstRecallDraft(root)))
      assertEquals("ScriptPublication1 canonical snapshot exceeds explicit snapshot/retained budget",failure.getMessage)
      assertFalse(root.isClosed,"rejected DATA remains accounted until actual root cleanup")
    }

  @Test def journalBoundRecallRejectsMissingOriginalAndMismatchedClientOrDigestWithoutV1Bypass():Unit =
    withJournalBoundRecall(Some(id(1903))) { (anchor,source,captured,root,journal)=>
      val link=MySqlDbFirstRecallJournalLink.from(journal)
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      def rejects(candidate:Option[MySqlDbFirstRecallJournalLink],before:MySqlScriptSchemaSnapshot=source):Unit =
        var backing:MySqlRepositoryRequestIdentity=null
        val request=MySqlDbFirstRecallRequest.capture(journal.originalTag,captured,root,candidate)(b=>backing=b)
        assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallMaterializer.materialize(request,captured,backing,
          before,91L,id(1905),at.plusSeconds(2),at.plusSeconds(3),new MySqlDbFirstRecallDraft(root)))
      rejects(Some(link),anchor)
      rejects(Some(link.copy(journalTag=link.journalTag.copy(operationId=id(1911)))))
      rejects(Some(link.copy(clientRequestUuid=None)))
      rejects(Some(link.copy(clientRequestUuid=Some(id(1912)))))
      rejects(Some(link.copy(journalRequestSha256=Vector.fill(32)(0.toByte))))
      rejects(None)
      assertThrows(classOf[IllegalArgumentException],()=>link.copy(actorUuid=id(1913)).requireFor(journal.originalTag,Some(id(501))))
      assertThrows(classOf[IllegalArgumentException],()=>link.copy(journalTag=link.journalTag.copy(connectionGeneration=2L))
        .requireFor(journal.originalTag,Some(id(501))))
    }

  @Test def journalBoundRecallV2HasExplicitFooterAndLeavesHistoricalV1BytesUnchanged():Unit =
    withJournalBoundRecall(Some(id(1903))) { (_,_,captured,root,journal)=>
      def capture(link:Option[MySqlDbFirstRecallJournalLink])=
        MySqlDbFirstRecallRequest.capture(journal.originalTag,captured,root,link)(_=>())
      val v1=capture(None);val v2=capture(Some(MySqlDbFirstRecallJournalLink.from(journal)))
      val old=v1.identity.canonicalBytes.toArray
      val modern=v2.identity.canonicalBytes.toArray
      val reader=new DataInputStream(new ByteArrayInputStream(old))
      val outer=new Array[Byte](reader.readInt());reader.readFully(outer)
      assertEquals("FP_MYSQL_REPOSITORY_REQUEST_BE_V1",new String(outer,java.nio.charset.StandardCharsets.UTF_8))
      assertEquals(1,reader.readUnsignedByte());assertEquals(1,reader.readInt())
      val kindOffset=old.length-reader.available()
      val kindLength=reader.readInt()
      val kind=new Array[Byte](kindLength);reader.readFully(kind)
      assertEquals("DB_FIRST_RECALL_V1",new String(kind,java.nio.charset.StandardCharsets.UTF_8))
      val expected=old.clone()
      expected(kindOffset+4+kindLength-1)='2'.toByte
      assertArrayEquals(expected,modern.take(old.length),"only explicit V2 discriminator changes the historical prefix")
      assertEquals(117,modern.length-old.length)
      val footer=new DataInputStream(new ByteArrayInputStream(modern,old.length,117))
      assertEquals(journal.journalTag.bootId,new UUID(footer.readLong(),footer.readLong()))
      assertEquals(journal.journalTag.bootGeneration,footer.readLong())
      assertEquals(journal.journalTag.connectionGeneration,footer.readLong())
      assertEquals(journal.journalTag.operationId,new UUID(footer.readLong(),footer.readLong()))
      assertEquals(journal.actorUuid,new UUID(footer.readLong(),footer.readLong()))
      assertTrue(footer.readBoolean());assertEquals(journal.clientRequestUuid.get,new UUID(footer.readLong(),footer.readLong()))
      assertEquals(32,footer.readInt())
      val digest=new Array[Byte](32);footer.readFully(digest)
      assertArrayEquals(journal.canonical.sha256.toArray,digest);assertEquals(0,footer.available())
      assertEquals(v1.identity,capture(None).identity,"same original V1 capture remains byte-identical")
      assertNotEquals(v1.identity.sha256,v2.identity.sha256)
    }

  @Test def persistedRecallJournalReconstructsColdOriginalGraphAndReceiptCursor():Unit =
    withPersistedRecallJournalImage { (image,anchor,root,operation)=>
      assertTrue(image.allocationBudget.isSnapshotReadChildOf(root))
      val baseline=root.accountedMemoryBytes
      (0 until 3).foreach { _=>
        MySqlDbFirstRecallJournalPersistedRead.verifyCommitted(image,operation,root)
        assertEquals(1L,MySqlDbFirstRecallPersistedRead.verifySnapshotReceiptSegment(image,anchor,root))
      }
      assertEquals(baseline,root.accountedMemoryBytes,"cold journal/cursor comparison scopes retire all memory")
      root.reserveRetainedMemory(MySqlDbFirstSession.HistoricalRead.HolderBytes,"actual journal historical holder BEFORE allocation")
      val read=new MySqlDbFirstSession.HistoricalRead(root,image.inputs)
      try
        read.acquire(None)
        read.validate(image)
        assertEquals(0L,read.dbFirstRecallSummary.verifiedCommitted,"J is not an original recall C outcome")
      finally read.close()
      assertEquals(baseline,root.accountedMemoryBytes)
      val foreign=image.inputs.newAllocationRoot(image.maxAllowedPacketBytes,new MySqlFormat3AllocationLedger(root.profile.maxRetainedBytes()))
      try assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstRecallJournalPersistedRead.verifyCommitted(image,operation,foreign))
      finally foreign.closeRootReadBudget()
    }

  @Test def persistedRecallJournalRejectsValidOuterDigestsWithChangedScopeMarkersOrWitness():Unit =
    withPersistedRecallJournalImage { (image,anchor,root,operation)=>
      val table="fp_recovery_operation"
      val shape=image.inputs.forTable(table)
      val own=image.relation(table).rows.find(row=>MySqlDbFirstRecallMaterializer.uuidEquals(
        MySqlDbFirstRecallMaterializer.value(row,table,"operation_uuid",image.inputs),operation)).get
      def blob(name:String)=MySqlDbFirstRecallMaterializer.value(own,table,name,image.inputs)
        .asInstanceOf[MySqlFormat3Cell.Binary].value
      def changed(name:String,mutate:Array[Byte]=>Unit):Unit =
        val bytes=blob(name+"_bytes").copyBytes(root,"test journal persisted mutation")
        mutate(bytes)
        val hash=java.security.MessageDigest.getInstance("SHA-256").digest(bytes)
        val row=own.updated(shape.columns.indexWhere(_.name==name+"_bytes"),MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes,root)))
          .updated(shape.columns.indexWhere(_.name==name+"_sha256"),MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(hash,root)))
        val rows=image.relations.map(r=>if r.table!=table then r else r.copy(rows=r.rows.map(old=>if old eq own then row else old)))
        val candidate=MySqlScriptSchemaSnapshot.captureMaterialized(image.schema,image.connection,image.observationScope,
          image.maxAllowedPacketBytes,image.inputs,rows,root)
        assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallJournalPersistedRead.verifyCommitted(candidate,operation,root),name)
      changed("scope",bytes=>bytes(4+MySqlDbFirstRecallJournalCanonical.Scope.length) = 9.toByte)
      changed("scope",bytes=>{
        val numericGeneration=4+MySqlDbFirstRecallJournalCanonical.Scope.length+1+16+8
        bytes(numericGeneration+7)=2.toByte
      })
      changed("target",bytes=>bytes(4)=(bytes(4)^1).toByte)
      changed("preimage",bytes=>bytes(bytes.length-1)=(bytes.last^1).toByte)
      changed("commit_witness",bytes=>bytes(bytes.length-1)=(bytes.last^1).toByte)
      MySqlDbFirstRecallJournalPersistedRead.verifyCommitted(image,operation,root)
      assertEquals(1L,MySqlDbFirstRecallPersistedRead.verifySnapshotReceiptSegment(image,anchor,root))
    }

  @Test def persistedRecallJournalRejectsSelfConsistentCanonicalWithUnauthorisedDomainDelta():Unit =
    withPersistedRecallJournalImage { (image,anchor,root,operation)=>
      var forged:MySqlScriptSchemaSnapshot=null
      MySqlDbFirstRecallJournalPersistedRead.withCommittedTransition(image,operation,root) { (before,target)=>
        val child=MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
        val table="fp_recovery_operation"
        val raw=target.relation(table).rows.find(row=>MySqlDbFirstRecallMaterializer.uuidEquals(
          MySqlDbFirstRecallMaterializer.value(row,table,"operation_uuid",image.inputs),operation)).get
        val request=MySqlDbFirstRecallJournalRequest.decode(MySqlDbFirstRecallMaterializer.value(raw,table,"request_identity_bytes",image.inputs)
          .asInstanceOf[MySqlFormat3Cell.Binary].value,child)
        val at=java.time.LocalDateTime.of(2026,10,29,12,0)
        val facts=MySqlDbFirstRecallJournalFacts(91L,id(1564),3L,at.plusHours(1),at,at.plusSeconds(1))
        def altered(rows:Vector[MySqlFormat3RelationSnapshot])=rows.map { relation=>
          if relation.table!="floating_pod_state" then relation else relation.copy(rows=relation.rows.map(row=>
            row.updated(image.inputs.forTable(relation.table).columns.indexWhere(_.name=="pos_x"),
              MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(123D)))))
        }
        val c=MySqlDbFirstRecallJournalCanonical
        val scope=c.scope(request,facts,before,root)
        val preimage=c.projection(request,before,before.relations,false,root)
        val projected=c.projection(request,before,altered(target.relations),true,root)
        val fingerprint=c.fingerprint(request,scope,preimage,projected,root)
        val context=c.context(request,facts,fingerprint,root)
        val witness=c.witness(request,scope,preimage,projected,fingerprint,context,root)
        val generic=c.operationContext(request,facts,fingerprint,root).canonicalFingerprintSha256
        val own=MySqlDbFirstRecallJournalMaterializer.operation(request,facts,before,scope,
          Some((context,preimage,projected,witness,generic)),child)
        val rows=altered(MySqlDbFirstRecallJournalMaterializer.transition(before,request,own,
          MySqlDbFirstRecallJournalMaterializer.binary(generic,child),child))
        forged=MySqlScriptSchemaSnapshot.captureMaterialized(image.schema,image.connection,image.observationScope,
          image.maxAllowedPacketBytes,image.inputs,copyPersistedRows(rows,root),root)
      }
      // 所有frame、SHA、context及见证均自洽；必须由独立业务增量门禁拒绝，而非靠外层字节损坏。
      MySqlDbFirstRecallJournalPersistedRead.requireCommittedReconstruction(forged,operation,root)
      val failure=assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallJournalPersistedRead.verifyCommitted(forged,operation,root))
      assertEquals("recall journal: persisted independent complete O/W transition differs",failure.getMessage)
      assertThrows(classOf[SQLDataException],()=>{MySqlDbFirstRecallPersistedRead.verifySnapshotReceiptSegment(forged,anchor,root);()})
      root.reserveRetainedMemory(MySqlDbFirstSession.HistoricalRead.HolderBytes,"rejected journal history holder BEFORE allocation")
      val history=new MySqlDbFirstSession.HistoricalRead(root,image.inputs)
      try
        history.acquire(None)
        assertThrows(classOf[SQLDataException],()=>history.validate(forged))
        assertThrows(classOf[IllegalStateException],()=>{history.dbFirstRecallSummary;()})
      finally history.close()
    }

  @Test def journalEntryReservesBothJAndOriginalCWithoutCaptureOrGenericDrop():Unit =
    val inputs=dbFirstMaterializationInputs()
    val f=new Fixture(4,dbFirstInputs=Some(inputs))
    try
      val j=IoOperationTag(id(1),1L,1L,id(1801));val c=j.copy(operationId=id(1802))
      val reservation=f.core.reserveDbFirstRecallJournal(j,c,id(503),id(501),id(502),7L,Some(id(1803)))
      val root=f.core.validationBudget(reservation.handle)
      assertSame(root,reservation.request.canonical.borrowedBinary.storageBudget)
      assertEquals(c,reservation.originalTag);assertEquals(1,f.core.retainedRoots)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(j))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(c))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstInitialPreparation(c,freshPreparationRequest(),1))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstRecallJournal(j.copy(operationId=id(1804)),c,id(503),id(501),id(502),7L,None))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordMainDrop(reservation.handle))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.cancelExplicitlyRejected(reservation.handle))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.beginDbFirstRecallJournal(reservation))
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure { f.core.beginDbFirstRecallJournal(reservation);() })
      f.core.requireDbFirstRecallJournalSubmission(reservation)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireDbFirstRecallJournalSubmission(reservation))
      var work:MySqlDbFirstRecallJournalWork=null
      f.onWorker {
        val forged=new MySqlDbFirstRecallJournalWork(reservation,root)
        assertThrows(classOf[IllegalArgumentException],()=>f.core.requireExecutingDbFirstRecallJournal(forged))
        work=f.core.beginDbFirstRecallJournal(reservation)
        f.core.requireExecutingDbFirstRecallJournal(work)
        assertSame(root,work.root);assertSame(reservation,work.reservation)
        assertThrows(classOf[IllegalArgumentException],()=>f.core.beginDbFirstRecallJournal(reservation))
      }
      f.core.submittedDbFirstRecallJournal(reservation,IoSubmission.Accepted)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordRejectedJournalDrop(reservation))
      assertFalse(root.isClosed)
      assertTrue(f.core.observation.plans.head.workerObserved)
    finally f.close()

  @Test def journalCaptureTransferRequiresActualSqlConfirmationAndMainReadAckBeforeAllocation():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(1921));val c=j.copy(operationId=id(1922))
      val reservation=f.core.reserveDbFirstRecallJournal(j,c,id(503),id(501),id(502),7L,Some(id(1923)))
      val original=f.core.validationBudget(reservation.handle)
      val baseline=f.shared.accountedBytes
      val forged=MySqlDbFirstRecallJournalConfirmation.issue(f.core,reservation.handle,reservation.request,"0"*64)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.prepareJournalCaptureTransfer(reservation,forged))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.prepareJournalCaptureTransfer(reservation,null))
      assertEquals(baseline,f.shared.accountedBytes,"invalid confirmation cannot create unregistered successor root")
      assertEquals(1,f.core.retainedRoots);assertFalse(original.isClosed)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(c))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.cancelExplicitlyRejected(reservation.handle))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordMainDrop(reservation.handle))
      assertTrue(f.core.observation.plans.head.cause.isEmpty,"invalid external metadata cannot poison original J")
    finally f.close()

  @Test def forgedJournalCaptureTransferCannotRetirePublishActivateOrClearOriginalCClaim():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(1931));val c=j.copy(operationId=id(1932))
      val reservation=f.core.reserveDbFirstRecallJournal(j,c,id(503),id(501),id(502),7L,None)
      val original=f.core.validationBudget(reservation.handle)
      val handle=new MySqlRetainedPlanHandle(null,99L,c,f.core)
      val capture=new MySqlDbFirstCaptureReservation(f.core,handle,f.limits)
      val forged=new MySqlDbFirstRecallJournalCaptureTransfer(f.core,reservation.handle,capture,id(503),id(501),id(502),7L)
      val baseline=f.shared.accountedBytes
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireJournalCaptureTransferSubmission(forged))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.submittedJournalCaptureTransfer(forged,IoSubmission.Accepted))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.submissionUnknownJournalCaptureTransfer(forged,new IOException("foreign UNKNOWN")))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireJournalCaptureTransferRetired(forged))
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure(f.core.classifyJournalCaptureTransferFailure(forged,new IOException("foreign failure"))))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.retireJournalCaptureTransfer(forged))
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure(f.core.retireJournalCaptureTransfer(forged)))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.publishJournalCaptureTransfer(forged))
      assertThrows(classOf[IllegalStateException],()=>{f.core.activateJournalCaptureTransfer(forged);()})
      assertThrows(classOf[IllegalArgumentException],()=>f.core.failJournalCaptureTransfer(forged,new IOException("not registered")))
      assertThrows(classOf[IllegalStateException],()=>f.core.requireDbFirstCaptureReservation(capture))
      assertEquals(baseline,f.shared.accountedBytes);assertEquals(1,f.core.retainedRoots)
      assertFalse(original.isClosed)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(c))
      assertTrue(f.core.observation.failure.isEmpty);assertTrue(f.core.observation.plans.head.cause.isEmpty)
    finally f.close()

  @Test def journalExplicitRejectionRequiresExactDedicatedDropBeforeRootRetirement():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(1811))
      val reservation=f.core.reserveDbFirstRecallJournal(j,j.copy(operationId=id(1812)),id(503),id(501),id(502),7L,None)
      val root=f.core.validationBudget(reservation.handle)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordRejectedJournalDrop(reservation))
      f.core.requireDbFirstRecallJournalSubmission(reservation)
      f.core.submittedDbFirstRecallJournal(reservation,IoSubmission.RejectedCapacity)
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure { f.core.beginDbFirstRecallJournal(reservation);() })
      val receipt=f.core.recordRejectedJournalDrop(reservation)
      assertFalse(root.isClosed,"Main detach alone is not root retirement")
      assertThrows(classOf[IllegalArgumentException],()=>f.core.cancelRejectedJournal(new MySqlDbFirstRejectedJournalReceipt(f.core,reservation.handle)))
      f.core.cancelRejectedJournal(receipt)
      assertTrue(root.isClosed);assertEquals(0,f.core.retainedRoots);assertEquals(0L,f.shared.accountedBytes)
      assertThrows(classOf[IllegalStateException],()=>f.core.cancelRejectedJournal(receipt))
    finally f.close()

  @Test def journalUnknownSubmissionRetainsOriginalIdsCauseAndRootWithoutRetry():Unit =
    val f=new Fixture(2,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(1821));val c=j.copy(operationId=id(1822))
      val reservation=f.core.reserveDbFirstRecallJournal(j,c,id(503),id(501),id(502),7L,Some(id(1823)))
      val root=f.core.validationBudget(reservation.handle)
      f.core.requireDbFirstRecallJournalSubmission(reservation)
      val cause=new IOException("actual journal submission outcome unknown")
      f.core.submissionUnknownDbFirstRecallJournal(reservation,cause)
      assertSame(cause,f.onWorkerFailure { f.core.beginDbFirstRecallJournal(reservation);() })
      assertSame(cause,assertThrows(classOf[IOException],()=>f.core.recordRejectedJournalDrop(reservation)))
      assertSame(cause,assertThrows(classOf[IOException],()=>f.core.reserveDbFirstCapture(c)))
      assertSame(cause,f.core.observation.plans.head.cause.get)
      assertSame(cause,f.core.observation.failure.get)
      assertEquals(j,reservation.tag);assertEquals(c,reservation.originalTag)
      assertFalse(root.isClosed);assertEquals(1,f.core.retainedRoots)
      assertFalse(f.core.observation.plans.head.workerObserved)
    finally f.close()

  @Test def journalTransactionRegistersOriginalWorkBeforeIoAndCannotBeReplacedOrRetried():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(1851))
      val reservation=f.core.reserveDbFirstRecallJournal(j,j.copy(operationId=id(1852)),id(503),id(501),id(502),7L,None)
      f.core.requireDbFirstRecallJournalSubmission(reservation)
      var work:MySqlDbFirstRecallJournalWork=null
      var runner:MySqlDbFirstRecallJournalTransaction=null
      f.onWorker { work=f.core.beginDbFirstRecallJournal(reservation) }
      assertThrows(classOf[IllegalArgumentException],()=>work.beginTransaction())
      f.onWorker {
        assertThrows(classOf[IllegalStateException],()=>{work.retainedTransaction;()})
        val before=work.root.accountedMemoryBytes
        runner=work.beginTransaction()
        assertEquals(before+MySqlDbFirstRecallJournalTransaction.HolderBytes+MySqlDbFirstRecallJournalConfirmation.HolderBytes,work.root.accountedMemoryBytes)
        assertSame(runner,work.retainedTransaction);assertSame(work,runner.work)
        runner.requireRegisteredWorker()
        val forged=new MySqlDbFirstRecallJournalTransaction(work)
        assertThrows(classOf[IllegalArgumentException],()=>work.requireTransaction(forged))
        assertThrows(classOf[IllegalArgumentException],()=>forged.requireRegisteredWorker())
        assertThrows(classOf[IllegalArgumentException],()=>work.beginTransaction())
        assertFalse(runner.commitAttempted);assertFalse(runner.acknowledged)
        assertEquals(MySqlDbFirstSession.DbFirstSnapshotOutcome.NotAttempted,runner.outcome)
        assertFalse(runner.readersPending);assertFalse(runner.gatePending)
        assertFalse(runner.auditPending);assertFalse(runner.historyPending);assertFalse(runner.forestPending)
        assertTrue(runner.firstFailure.isEmpty)
      }
      assertThrows(classOf[IllegalArgumentException],()=>runner.requireRegisteredWorker())
      f.core.submittedDbFirstRecallJournal(reservation,IoSubmission.Accepted)
      assertFalse(work.root.isClosed);assertEquals(1,f.core.retainedRoots)
      assertTrue(f.core.observation.plans.head.cause.isEmpty)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordRejectedJournalDrop(reservation))
    finally f.close()

  @Test def journalSqlConfirmationAndReaderRetirementRejectDataWithoutActualReadback():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(1861));val c=j.copy(operationId=id(1862))
      val reservation=f.core.reserveDbFirstRecallJournal(j,c,id(503),id(501),id(502),7L,Some(id(1863)))
      val observation=MySqlDbFirstSession.DbFirstRecallJournalObservation(j,c,id(502),true,true,true,"0"*64)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.claimDbFirstRecallJournalConfirmation(reservation,observation))
      f.core.requireDbFirstRecallJournalSubmission(reservation)
      var work:MySqlDbFirstRecallJournalWork=null
      var runner:MySqlDbFirstRecallJournalTransaction=null
      f.onWorker {
        work=f.core.beginDbFirstRecallJournal(reservation);runner=work.beginTransaction()
        assertThrows(classOf[IllegalArgumentException],()=>runner.confirmedMainReadSource(observation))
        assertThrows(classOf[IllegalArgumentException],()=>runner.retireConfirmedResources())
        assertThrows(classOf[IllegalArgumentException],()=>work.retirePreflightSource(runner))
        val capture=new MySqlDbFirstCaptureReservation(f.core,
          new MySqlRetainedPlanHandle(null,98L,reservation.originalTag,f.core),f.limits)
        val forgedTransfer=new MySqlDbFirstRecallJournalCaptureTransfer(f.core,reservation.handle,capture,id(503),id(501),id(502),7L)
        assertThrows(classOf[IllegalArgumentException],()=>runner.retireEntryPayload(forgedTransfer))
        assertThrows(classOf[IllegalArgumentException],()=>work.retireEntryPayload(runner,forgedTransfer))
        assertFalse(runner.payloadRetired,"complete materialized DATA is not original durable-J retirement permission")
        assertThrows(classOf[IllegalArgumentException],()=>runner.requireConfirmedObservation(observation))
        assertFalse(runner.resourcesRetired);assertTrue(runner.firstFailure.isEmpty)
      }
      f.core.submittedDbFirstRecallJournal(reservation,IoSubmission.Accepted)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.claimDbFirstRecallJournalConfirmation(reservation,observation))
      val forged=MySqlDbFirstRecallJournalConfirmation.issue(f.core,reservation.handle,reservation.request,"0"*64)
      assertThrows(classOf[IllegalArgumentException],()=>forged.requireOriginal(reservation))
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure { f.core.beginJournalReadRelease(forged) })
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireJournalReadReleased(forged,reservation))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.acknowledgeJournalReadReleased(forged,reservation))
      assertFalse(work.root.isClosed);assertEquals(1,f.core.retainedRoots)
      assertTrue(f.core.observation.plans.head.cause.isEmpty,"failed proof guards do not rewrite actual original outcome")
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(c))
    finally f.close()

  @Test def journalUsesRealSingleAttemptCommitBoundaryWithoutTreatingAckAsReadback():Unit =
    import MySqlDbFirstSession.DbFirstSnapshotOutcome.*
    def fresh()=new MySqlDbFirstSnapshotTransaction.Progress(Thread.currentThread())
    val before=fresh()
    assertThrows(classOf[IllegalArgumentException],()=>before.attemptCommit())
    before.begin();before.fail()
    assertFalse(before.commitAttempted);assertEquals(FailedBeforeCommit,before.outcome)
    assertThrows(classOf[IllegalArgumentException],()=>before.attemptCommit())
    val unknown=fresh();unknown.begin();unknown.attemptCommit();unknown.fail()
    assertTrue(unknown.commitAttempted);assertFalse(unknown.acknowledged)
    assertEquals(CommitUnknown,unknown.outcome);unknown.requireFailedCommitObservation()
    assertThrows(classOf[IllegalArgumentException],()=>unknown.attemptCommit())
    assertThrows(classOf[IllegalArgumentException],()=>unknown.acknowledge())
    val ack=fresh();ack.begin();ack.attemptCommit();ack.acknowledge();ack.fail()
    assertEquals(AcknowledgedPending,ack.outcome);ack.requireFailedCommitObservation()
    assertThrows(classOf[IllegalArgumentException],()=>ack.verifyReadback())
    val read=fresh();read.begin();read.attemptCommit();read.acknowledge();read.verifyReadback()
    assertEquals(ReadbackVerifiedPending,read.outcome)
    assertThrows(classOf[IllegalArgumentException],()=>read.attemptCommit())
    assertThrows(classOf[IllegalArgumentException],()=>read.verifyReadback())
    assertThrows(classOf[IllegalArgumentException],()=>read.requireFailedCommitObservation())

  @Test def journalWorkCopiesOriginalSnapshotIntoEntryAndRetainsFailedAttemptCause():Unit =
    val inputs=dbFirstMaterializationInputs()
    val f=new Fixture(2,dbFirstInputs=Some(inputs))
    var source:MySqlScriptSchemaSnapshot=null
    var child:MySqlFormat3ReadBudget=null
    try
      source=dbFirstBefore(f,inputs,physicalGraph=true)
      child=source.allocationBudget.newScopedReadBudget(source.maxAllowedPacketBytes)
      child.initialize(source.schema,source.connection,source.observationScope,source.maxAllowedPacketBytes)
      val snapshot=MySqlScriptSchemaSnapshot.decoded(source.schema,source.connection,source.observationScope,
        source.maxAllowedPacketBytes,inputs,copyPersistedRows(source.relations,child),child)
      val j=IoOperationTag(id(1),1L,1L,id(1831))
      val reservation=f.core.reserveDbFirstRecallJournal(j,j.copy(operationId=id(1832)),id(503),id(501),id(502),7L,None)
      f.core.requireDbFirstRecallJournalSubmission(reservation)
      val bad=f.core.reserveDbFirstRecallJournal(j.copy(operationId=id(1841)),j.copy(operationId=id(1842)),id(503),id(501),id(502),8L,None)
      f.core.requireDbFirstRecallJournalSubmission(bad)
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      val facts=MySqlDbFirstRecallJournalFacts(91L,id(1834),3L,at.plusHours(1),at,at.plusSeconds(1))
      f.onWorker {
        val work=f.core.beginDbFirstRecallJournal(reservation)
        val runner=work.beginTransaction()
        runner.requireRegisteredWorker()
        val result=work.materialize(snapshot,source.allocationBudget,facts)
        assertSame(result,work.result);assertSame(work.root,result.before.allocationBudget)
        assertSame(work.draft.result,result);assertSame(snapshot,work.source)
        assertNotSame(snapshot,result.before);assertTrue(snapshot.exactlyEquals(result.before))
        assertThrows(classOf[IllegalArgumentException],()=>runner.retireConfirmedResources())
        assertThrows(classOf[IllegalArgumentException],()=>work.retirePreflightSource(runner))
        assertSame(snapshot,work.source);assertSame(source.allocationBudget,work.sourceRoot)
        assertSame(result,work.result);assertFalse(runner.resourcesRetired)
        assertThrows(classOf[IllegalArgumentException],()=>work.materialize(snapshot,source.allocationBudget,facts))
        assertTrue(f.core.observation.plans.find(_.tag==j).get.cause.isEmpty,"replay guard cannot poison completed original work")
        val failed=f.core.beginDbFirstRecallJournal(bad)
        val cause=assertThrows(classOf[SQLDataException],()=>failed.materialize(snapshot,source.allocationBudget,facts))
        assertSame(cause,failed.draft.failure);assertSame(snapshot,failed.source)
        assertSame(failed.root,failed.before.allocationBudget);assertTrue(failed.materializationAttempted)
        assertSame(cause,assertThrows(classOf[SQLDataException],()=>f.core.requireExecutingDbFirstRecallJournal(failed)))
        assertSame(cause,assertThrows(classOf[SQLDataException],()=>failed.materialize(snapshot,source.allocationBudget,facts)))
        assertFalse(failed.root.isClosed)
      }
      f.core.submittedDbFirstRecallJournal(reservation,IoSubmission.Accepted)
      f.core.submittedDbFirstRecallJournal(bad,IoSubmission.Accepted)
      assertEquals(2,f.core.retainedRoots)
      assertTrue(f.core.observation.failure.isEmpty,"local DATA rejection does not itself classify shared SQL health")
    finally
      if child!=null then child.closeScopedReadBudget()
      if source!=null then source.allocationBudget.closeRootReadBudget()
      f.close()

  private def freshPreparationRequest(): MySqlControlledDebugPrepareRequest =
    MySqlControlledDebugPrepareRequest(
      com.wi.floatingpod.storage.PhysicalRecoveryBinding(id(503),com.wi.floatingpod.storage.PhysicalRecoveryCutoverState.Certified,2),
      id(800),id(801),id(802),com.wi.floatingpod.domain.FloatingPodPose("world",1,64,2,3,4,0),id(803))

  @Test def initialPublicationRetainsPreparationAndCaptureAtTheSameTime():Unit =
    val f=new Fixture(2,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val preparation=f.core.reserveDbFirstInitialPreparation(
        IoOperationTag(id(1),1L,1L,id(1290)),freshPreparationRequest(),1)
      val original=f.core.validationBudget(preparation.handle)
      val capture=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(1291)),
        DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION)
      assertEquals(2,f.core.retainedRoots)
      assertFalse(original.isClosed,"initial capture must not retire its original preparation")
      f.core.requireDbFirstCaptureReservation(capture)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(
        IoOperationTag(id(1),1L,1L,id(1292))))
      assertEquals(2,f.core.retainedRoots,"capacity refusal must not allocate a third Entry")
    finally f.close()

  @Test def initialPreparationOwnsDistinctSharedEntryBeforeWorkerAndRejectsGenericDrop(): Unit =
    val f=new Fixture(2,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val request=freshPreparationRequest()
      val tag=IoOperationTag(id(1),1L,1L,id(1200))
      val reservation=f.core.reserveDbFirstInitialPreparation(tag,request,17)
      assertSame(request,reservation.request)
      assertEquals(17,reservation.activeLimit)
      val root=f.core.validationBudget(reservation.handle)
      assertTrue(root.accountedMemoryBytes>1024L,"original request backing is charged to shared Entry")
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(tag))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.beginDbFirstInitialPreparation(reservation))
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure { f.core.beginDbFirstInitialPreparation(reservation); () })
      f.core.requireDbFirstInitialPreparationSubmission(reservation)
      var work:MySqlDbFirstInitialPreparationWork=null
      f.onWorker {
        work=f.core.beginDbFirstInitialPreparation(reservation)
        assertSame(root,work.root)
        assertSame(reservation,work.reservation)
        assertEquals("RUN",work.requestBacking.requestKind)
        f.core.requireExecutingDbFirstInitialPreparation(work)
        assertThrows(classOf[IllegalArgumentException],()=>f.core.beginDbFirstInitialPreparation(reservation))
      }
      // worker可先于Main收到submit结果；两者仍须落到同一原Entry。
      f.core.submittedDbFirstInitialPreparation(reservation,IoSubmission.Accepted)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordMainDrop(reservation.handle))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.cancelExplicitlyRejected(reservation.handle))
      val cause=new SQLDataException("original preparation DATA failure")
      f.onWorker {
        f.core.failDbFirstInitialPreparation(work,cause)
        assertSame(cause,assertThrows(classOf[SQLDataException],()=>f.core.requireExecutingDbFirstInitialPreparation(work)))
      }
      assertTrue(f.core.observation.failure.isEmpty,"local DATA failure alone does not classify shared SQL state")
      assertFalse(root.isClosed)
      f.core.reserveDbFirstInitialPreparation(tag.copy(operationId=id(1201)),request.copy(podUuid=id(810)),17)
      assertEquals(2,f.core.retainedRoots)
    finally f.close()

  @Test def explicitlyRejectedPreparationDropsExactMainPayloadAndRestoresCapacity():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val reservation=f.core.reserveDbFirstInitialPreparation(IoOperationTag(id(1),1L,1L,id(1208)),freshPreparationRequest(),1)
      val root=f.core.validationBudget(reservation.handle)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordRejectedPreparationDrop(reservation))
      f.core.requireDbFirstInitialPreparationSubmission(reservation)
      f.core.submittedDbFirstInitialPreparation(reservation,IoSubmission.RejectedCapacity)
      val receipt=f.core.recordRejectedPreparationDrop(reservation)
      assertFalse(root.isClosed,"recording Main detach is not allocation retirement")
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordRejectedPreparationDrop(reservation))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.cancelRejectedPreparation(
        new MySqlDbFirstRejectedPreparationReceipt(f.core,reservation.handle)))
      assertEquals(1,f.core.retainedRoots)
      f.core.cancelRejectedPreparation(receipt)
      assertTrue(root.isClosed)
      assertEquals(0,f.core.retainedRoots)
      assertThrows(classOf[IllegalStateException],()=>f.core.cancelRejectedPreparation(receipt))
      assertTrue(f.core.observation.failure.isEmpty)
      val next=f.core.reserveDbFirstInitialPreparation(IoOperationTag(id(1),1L,1L,id(1209)),freshPreparationRequest(),1)
      f.core.requireDbFirstInitialPreparationSubmission(next)
      f.core.submittedDbFirstInitialPreparation(next,IoSubmission.Accepted)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordRejectedPreparationDrop(next))
      assertFalse(f.core.validationBudget(next.handle).isClosed)
    finally f.close()

  @Test def initialPreparationRejectedAndUnknownCannotReplayOrBorrowGenericCancellation(): Unit =
    val f=new Fixture(2,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val request=freshPreparationRequest()
      val rejected=f.core.reserveDbFirstInitialPreparation(IoOperationTag(id(1),1L,1L,id(1210)),request,1)
      f.core.requireDbFirstInitialPreparationSubmission(rejected)
      f.core.submittedDbFirstInitialPreparation(rejected,IoSubmission.RejectedCapacity)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.cancelExplicitlyRejected(rejected.handle))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireDbFirstInitialPreparationSubmission(rejected))
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure { f.core.beginDbFirstInitialPreparation(rejected); () })
      val uncertain=f.core.reserveDbFirstInitialPreparation(IoOperationTag(id(1),1L,1L,id(1211)),request,1)
      val root=f.core.validationBudget(uncertain.handle)
      f.core.requireDbFirstInitialPreparationSubmission(uncertain)
      val cause=new IllegalStateException("actual queue acceptance unknown")
      f.core.submissionUnknownDbFirstInitialPreparation(uncertain,cause)
      assertSame(cause,f.core.observation.failure.get)
      assertSame(cause,assertThrows(classOf[IllegalStateException],()=>f.core.recordRejectedPreparationDrop(uncertain)))
      assertSame(cause,f.onWorkerFailure { f.core.beginDbFirstInitialPreparation(uncertain); () })
      assertFalse(root.isClosed)
      assertEquals(2,f.core.retainedRoots)
    finally f.close()

  @Test def initialPreparationWorkerRetainsOriginalPreflightSourceAndFault(): Unit =
    verifyPreparationWorker(524288L,false)

  @Test def initialPreparationActivationProofRequiresAndPreservesActiveOldPod(): Unit =
    verifyPreparationWorker(524288L,false,activeBaseline=true)

  @Test def initialPreparationCompleteTargetBudgetFailureRetainsOriginalResponsibility(): Unit =
    verifyPreparationWorker(131072L,true)

  private def verifyPreparationWorker(snapshotBytes:Long,expectBudgetFailure:Boolean,activeBaseline:Boolean=false):Unit =
    withPersistedDbFirstImage(queryGraph=true,snapshotBytes=snapshotBytes,activeBaseline=activeBaseline) { (observed,sourceRoot,existingOperation) =>
      val f=new Fixture(4,dbFirstInputs=Some(observed.inputs))
      var preparedTarget:MySqlScriptSchemaSnapshot=null
      var preparedBefore:MySqlScriptSchemaSnapshot=null
      var preparedView:MySqlDbFirstInitialPreparationDataView=null
      try
        val activeLimit=if activeBaseline then 2 else 1
        val good=f.core.reserveDbFirstInitialPreparation(IoOperationTag(id(1),1L,1L,id(1220)),freshPreparationRequest(),activeLimit)
        val bad=f.core.reserveDbFirstInitialPreparation(IoOperationTag(id(1),1L,1L,id(1221)),
          freshPreparationRequest().copy(podUuid=id(502)),activeLimit)
        val malformed=f.core.reserveDbFirstInitialPreparation(IoOperationTag(id(1),1L,1L,id(1222)),
          freshPreparationRequest().copy(podUuid=id(810)),activeLimit)
        Vector(good,bad,malformed).foreach { reservation =>
          f.core.requireDbFirstInitialPreparationSubmission(reservation)
          f.core.submittedDbFirstInitialPreparation(reservation,IoSubmission.Accepted)
          f.onWorker {
            val work=f.core.beginDbFirstInitialPreparation(reservation)
            val currentScope=MySqlFormat3ObservationScope(reservation.tag.bootId,reservation.tag.bootGeneration,reservation.tag.connectionGeneration)
            work.root.initialize(observed.schema,observed.connection,currentScope,observed.maxAllowedPacketBytes)
            val actualSourceRoot=if reservation eq good then
              observed.inputs.newAllocationRoot(observed.maxAllowedPacketBytes,f.shared)
            else work.root
            if actualSourceRoot ne work.root then
              actualSourceRoot.initialize(observed.schema,observed.connection,currentScope,observed.maxAllowedPacketBytes)
            val child=actualSourceRoot.newScopedReadBudget(observed.maxAllowedPacketBytes)
            child.initialize(observed.schema,observed.connection,currentScope,observed.maxAllowedPacketBytes)
            var sourceFailure:Throwable=null
            try
              val image=MySqlScriptSchemaSnapshot.decoded(observed.schema,observed.connection,currentScope,
                observed.maxAllowedPacketBytes,observed.inputs,copyPersistedRows(observed.relations,child),child)
              if reservation eq good then
                val runner=work.beginTransaction()
                assertSame(work,runner.work)
                work.requireTransaction(runner)
                assertFalse(runner.resourcesRetired)
                assertThrows(classOf[IllegalArgumentException],()=>runner.retireConfirmedResources())
                assertThrows(classOf[IllegalArgumentException],()=>runner.confirmedMainReadSource(null))
                assertThrows(classOf[IllegalArgumentException],()=>runner.requireRejectedPrecommitCleanup(
                  new MySqlDbFirstPreparationRejected(MySqlDbFirstPreparationRejected.Reason.ActiveCapacity,"caller DATA")))
                val forgedRejection=new MySqlDbFirstPreparationRejected(MySqlDbFirstPreparationRejected.Reason.ActiveCapacity,"caller DATA")
                assertThrows(classOf[IllegalArgumentException],()=>runner.retireRejectedResources(forgedRejection))
                assertThrows(classOf[IllegalArgumentException],()=>work.retireRejectedPreflightSource(runner,forgedRejection))
                assertFalse(runner.rejectedResourcesRetired)
                assertTrue(runner.firstFailure.isEmpty)
                assertThrows(classOf[IllegalArgumentException],()=>work.retirePreflightSource(runner))
                assertFalse(runner.resourcesRetired)
                assertThrows(classOf[IllegalArgumentException],()=>work.beginTransaction())
                assertThrows(classOf[IllegalArgumentException],()=>work.requireTransaction(new MySqlDbFirstInitialPreparationTransaction(work)))
                assertFalse(actualSourceRoot eq work.root)
                assertTrue(actualSourceRoot.sharesAllocationLedger(work.root))
                work.preflight(image,actualSourceRoot)
                assertSame(image,work.validatedDataSource)
                assertThrows(classOf[IllegalArgumentException],()=>work.preflight(image))
                assertSame(image,work.validatedDataSource)
                val at=java.time.LocalDateTime.of(2026,10,29,12,0)
                val draft=work.constructDomainRows(at)
                assertTrue(draft.before.exactlyEquals(image))
                val constructed=draft.domainConstruction
                assertEquals(27,constructed.size)
                val changed=Set("floating_pod_state","physical_recovery_intent","physical_recovery_locator",
                  "physical_recovery_footprint","physical_recovery_footprint_chunk")
                image.relations.foreach { old =>
                  val current=constructed.find(_.table==old.table).get
                  assertTrue(old.rows.forall(current.rows.contains),"all original rows retained: "+old.table)
                  if !changed.contains(old.table) then assertEquals(old.rows,current.rows)
                }
                def rows(table:String)=constructed.find(_.table==table).get.rows.filter { row =>
                  val name=if table=="floating_pod_state" then "uuid" else "pod_uuid"
                  cell(row,table,name,image.inputs).asInstanceOf[MySqlFormat3Cell.Binary].value.equalsUuid(id(800))
                }
                def field(table:String,name:String)=cell(rows(table).head,table,name,image.inputs)
                assertEquals(MySqlFormat3Cell.SignedInteger(0),field("floating_pod_state","is_active"))
                assertEquals(MySqlFormat3Cell.SignedInteger(0),field("floating_pod_state","state_version"))
                assertEquals(MySqlFormat3Cell.SqlNull,field("floating_pod_state","owner_uuid"))
                assertEquals(MySqlFormat3Cell.Text("WORLD_IDLE"),field("floating_pod_state","operating_mode"))
                assertEquals(MySqlFormat3Cell.Text("PREPARED"),field("physical_recovery_intent","phase"))
                assertTrue(field("physical_recovery_intent","create_operation_uuid").asInstanceOf[MySqlFormat3Cell.Binary].value.equalsUuid(id(803)))
                assertEquals(MySqlFormat3Cell.Text("DEBUG_ITEM_CREATE_PREPARED"),field("physical_recovery_locator","locator_state"))
                assertEquals(MySqlFormat3Cell.Text("CREATE_TARGET"),field("physical_recovery_footprint","purpose"))
                assertEquals(MySqlFormat3Cell.Text("SEALED"),field("physical_recovery_footprint","seal_state"))
                assertEquals(MySqlFormat3Cell.SignedInteger(0),field("physical_recovery_footprint","origin_version"))
                Vector("floating_pod_state","physical_recovery_intent","physical_recovery_locator").foreach { table =>
                  assertTrue(field(table,"last_mutation_operation_uuid").asInstanceOf[MySqlFormat3Cell.Binary].value.equalsUuid(good.tag.operationId))
                }
                val coordinates=rows("physical_recovery_footprint_chunk").map(r=>
                  (cell(r,"physical_recovery_footprint_chunk","chunk_x",image.inputs),cell(r,"physical_recovery_footprint_chunk","chunk_z",image.inputs))).toSet
                assertEquals((for x <- -2 to 2; z <- -2 to 2 yield
                  (MySqlFormat3Cell.SignedInteger(x.toLong),MySqlFormat3Cell.SignedInteger(z.toLong))).toSet,coordinates)
                assertThrows(classOf[IllegalArgumentException],()=>work.constructDomainRows(at))
                val data=MySqlDbFirstInitialPreparationData(good.tag,good.request,good.activeLimit,work.requestBacking)
                val facts=MySqlDbFirstInitialPreparationFacts(91L,id(1230),3L,at.plusHours(1),at,at.plusSeconds(1))
                val canonical=MySqlDbFirstInitialPreparationCanonical
                val scope=canonical.scope(data,facts,draft.before,work.root)
                val preimage=canonical.projection(data,draft.before,draft.before.relations,false,work.root)
                assertTrue(preimage.length>0)
                val differentActiveLimit=if good.activeLimit==1 then 2 else 1
                assertFalse(scope.exactlyEquals(canonical.scope(data.copy(activeLimit=differentActiveLimit),facts,draft.before,work.root)))
                assertFalse(scope.exactlyEquals(canonical.scope(data,facts.copy(connectionId=92L),draft.before,work.root)))
                assertThrows(classOf[IllegalArgumentException],()=>canonical.scope(data,facts.copy(writerEpoch=4L),draft.before,work.root))
                assertThrows(classOf[IllegalArgumentException],()=>canonical.scope(data.copy(request=data.request.copy(podUuid=id(999))),facts,draft.before,work.root))
                assertThrows(classOf[IllegalArgumentException],()=>canonical.scope(data.copy(tag=data.tag.copy(bootGeneration=2L)),facts,draft.before,work.root))
                assertThrows(classOf[IllegalArgumentException],()=>canonical.projection(data,draft.before,constructed,true,work.root))
                assertThrows(classOf[IllegalArgumentException],()=>canonical.projection(data,draft.before,constructed.dropRight(1),false,work.root))
                if expectBudgetFailure then
                  val cause=assertThrows(classOf[SQLDataException],()=>work.constructTransition(facts))
                  assertTrue(cause.getMessage.contains("canonical snapshot exceeds explicit snapshot/retained budget"))
                  assertSame(cause,assertThrows(classOf[SQLDataException],()=>work.constructTransition(facts)))
                  assertFalse(work.root.isClosed)
                else
                  val transition=work.constructTransition(facts)
                  val target=transition.target
                  preparedTarget=target
                  preparedBefore=draft.before
                  Vector("floating_pod_state"->"uuid","physical_recovery_intent"->"pod_uuid",
                    "physical_recovery_footprint"->"pod_uuid","physical_recovery_locator"->"pod_uuid").foreach { (table,column)=>
                    val selected=MySqlDbFirstInitialPreparationTransaction.targetRow(transition,table)
                    assertTrue(cell(selected,table,column,image.inputs).asInstanceOf[MySqlFormat3Cell.Binary].value.equalsUuid(id(800)))
                  }
                  assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstInitialPreparationTransaction.targetRow(transition,"fp_pod_snapshot"))
                  val intentShape=image.inputs.forTable("physical_recovery_intent")
                  val writeColumns=MySqlDbFirstInitialPreparationTransaction.writeColumns(intentShape)
                  val generated=intentShape.columns.indexWhere(_.name=="source_identity_sha256")
                  assertTrue(generated>=0)
                  assertEquals(intentShape.columns.indices.filterNot(_==generated).toVector,writeColumns)
                  val insert=MySqlDbFirstInitialPreparationTransaction.insertSql(image.schema,intentShape)
                  assertEquals(writeColumns.size,insert.count(_=='?'))
                  val sqlColumns=insert.substring(insert.indexOf('(')+1,insert.indexOf(')')).split(",").toVector
                  assertEquals(writeColumns.map(i=>"`"+intentShape.columns(i).name+"`"),sqlColumns)
                  assertFalse(sqlColumns.contains("`source_identity_sha256`"))
                  assertEquals(intentShape.columns.size,MySqlDbFirstInitialPreparationTransaction.targetRow(transition,"physical_recovery_intent").size,
                    "full readback target retains the generated column")
                  if activeBaseline then assertTrue(target.canonicalByteLength>0L && target.canonicalByteLength<=snapshotBytes)
                  else assertTrue(target.canonicalByteLength>131072L && target.canonicalByteLength<=snapshotBytes)
                  println("PREPARATION_DATA_BYTES="+target.canonicalByteLength+", SNAPSHOT_LIMIT="+snapshotBytes)
                  val graphOwner=work.root.newScopedReadBudget(target.maxAllowedPacketBytes)
                  graphOwner.initialize(target.schema,target.connection,target.observationScope,target.maxAllowedPacketBytes)
                  try
                    val graph=MySqlScriptSchemaSnapshot.decoded(target.schema,target.connection,target.observationScope,
                      target.maxAllowedPacketBytes,target.inputs,copyPersistedRows(target.relations,graphOwner),graphOwner)
                    MySqlControlledDebugRelations.requireValidDbFirstGraph(graph,target.inputs,work.root)
                    graph.canonicalSha256
                    val receiptBaseline=f.shared.accountedBytes
                    assertEquals(1L,MySqlDbFirstRecallPersistedRead.verifySnapshotReceiptSegment(graph,draft.before,work.root))
                    assertEquals(receiptBaseline,f.shared.accountedBytes,"preparation receipt proof must refund complete decoded DATA")
                    val now=at.plusDays(1)
                    val originalWriter=MySqlDbFirstBootstrapProof.writer(draft.before,now)
                    val prior=MySqlDbFirstBootstrapWriterGraph.buildTarget(draft.before,target.inputs,work.root,
                      originalWriter.copy(writerEpoch=originalWriter.writerEpoch-1L,ownerBootUuid=None,
                        leaseUntil=at.minusDays(1)))
                    try
                      val inspected=MySqlDbFirstAbandonedStartupData.inspectCommittedPreparation(prior,graph,
                        good.tag.bootId,Some(good.tag.bootId),facts.writerEpoch,now,work.root)
                      assertEquals(good.request.createOperationUuid,inspected.create)
                      assertEquals(good.tag.operationId,inspected.preparation)
                      assertNotEquals(inspected.create,inspected.preparation)
                      assertEquals(good.request.podUuid,inspected.pod)
                      assertEquals(good.request.attemptUuid,inspected.attempt)
                      assertEquals(good.request.footprintUuid,inspected.footprint)
                      assertThrows(classOf[MySqlDbFirstAbandonedStartupInspection],()=>
                        MySqlDbFirstAbandonedStartupData.inspect(prior,graph,good.tag.bootId,
                          Some(good.tag.bootId),facts.writerEpoch,now))
                      assertThrows(classOf[MySqlDbFirstAbandonedStartupInspection],()=>
                        MySqlDbFirstAbandonedStartupData.inspectCommittedPreparation(prior,graph,id(9999),
                          Some(id(9999)),facts.writerEpoch,now,work.root))
                      assertThrows(classOf[MySqlDbFirstAbandonedStartupInspection],()=>
                        MySqlDbFirstAbandonedStartupData.inspectCommittedPreparation(prior,graph,good.tag.bootId,
                          Some(good.tag.bootId),facts.writerEpoch+1L,now,work.root))
                      assertThrows(classOf[MySqlDbFirstAbandonedStartupInspection],()=>
                        MySqlDbFirstAbandonedStartupData.inspectCommittedPreparation(prior,graph,good.tag.bootId,
                          Some(good.tag.bootId),facts.writerEpoch,at,work.root))
                    finally prior.allocationBudget.closeScopedReadBudget()
                    val dataBudget=work.root.newCanonicalDataScope()
                    try
                      val view=MySqlDbFirstInitialPreparationDataView.read(good.request,graph,work.root,dataBudget,work.root)
                      preparedView=view
                      assertEquals(good.request.podUuid,view.state.uuid)
                      assertFalse(view.state.isActive)
                      assertEquals(0L,view.state.stateVersion)
                      assertEquals(good.request.createOperationUuid,view.receipt.createOperationUuid.get)
                      assertEquals(good.request.targetPose,view.receipt.targetPose)
                      val query=MySqlFormat3QueryProjection.fromDbFirstData(graph,graph.inputs,work.root,dataBudget)
                      val index=MySqlDbFirstInitialPreparationDataView.currentPreparedInitials(graph,work.root,dataBudget,query)
                      assertEquals(view,index(good.request.createOperationUuid))
                      assertFalse(index.contains(good.tag.operationId),"P execution identity cannot replace original create identity")
                      assertFalse(index.contains(id(9999)))
                      assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstInitialPreparationDataView.read(
                        good.request.copy(attemptUuid=id(9999)),graph,work.root,dataBudget,work.root))
                    finally dataBudget.closeScopedReadBudget()
                  finally graphOwner.closeScopedReadBudget()
                  assertEquals(27,target.relations.size)
                  assertEquals(image.relation("fp_pod_snapshot").rows,target.relation("fp_pod_snapshot").rows)
                  assertTrue(image.relation("fp_recovery_operation").rows.forall(target.relation("fp_recovery_operation").rows.contains))
                  val operation=target.relation("fp_recovery_operation").rows.find(r=>
                    cell(r,"fp_recovery_operation","operation_uuid",image.inputs).asInstanceOf[MySqlFormat3Cell.Binary].value.equalsUuid(good.tag.operationId)).get
                  assertEquals(MySqlFormat3Cell.Text("REARM"),cell(operation,"fp_recovery_operation","step",image.inputs))
                  assertEquals(MySqlFormat3Cell.Text("COMMITTED"),cell(operation,"fp_recovery_operation","operation_state",image.inputs))
                  assertEquals(MySqlFormat3Cell.Text("RESERVED"),cell(transition.reservedOperation,"fp_recovery_operation","operation_state",image.inputs))
                  assertEquals(MySqlFormat3Cell.SqlNull,cell(transition.reservedOperation,"fp_recovery_operation","commit_witness_bytes",image.inputs))
                  assertEquals(MySqlFormat3Cell.Text("HELD"),cell(transition.heldReservation,"fp_recovery_reservation","reservation_state",image.inputs))
                  assertEquals(MySqlFormat3Cell.SignedInteger(0),cell(transition.heldReservation,"fp_recovery_reservation","reservation_revision",image.inputs))
                  val released=target.relation("fp_recovery_reservation").rows.find(r=>
                    cell(r,"fp_recovery_reservation","resource_pod_uuid",image.inputs).asInstanceOf[MySqlFormat3Cell.Binary].value.equalsUuid(id(800))).get
                  assertEquals(MySqlFormat3Cell.Text("RELEASED"),cell(released,"fp_recovery_reservation","reservation_state",image.inputs))
                  assertEquals(MySqlFormat3Cell.SignedInteger(1),cell(released,"fp_recovery_reservation","reservation_revision",image.inputs))
                  val accounted=work.root.accountedMemoryBytes
                  (0 until 3).foreach(_=>transition.verifyTarget(target))
                  assertEquals(accounted,work.root.accountedMemoryBytes,"independent canonical comparisons refund all child debt")
                  def reconstruct(rows:Vector[MySqlFormat3RelationSnapshot],business:Boolean=true):Unit =
                    val cold=work.root.newScopedReadBudget(target.maxAllowedPacketBytes)
                    val later=MySqlFormat3ObservationScope(id(1999),9L,9L)
                    cold.initialize(target.schema,target.connection,later,target.maxAllowedPacketBytes)
                    try
                      val observed=MySqlScriptSchemaSnapshot.decoded(target.schema,target.connection,later,
                        target.maxAllowedPacketBytes,target.inputs,copyPersistedRows(rows,cold),cold)
                      if business then
                        work.root.reserveRetainedMemory(MySqlDbFirstSession.HistoricalRead.HolderBytes,"actual preparation history holder BEFORE allocation")
                        val history=new MySqlDbFirstSession.HistoricalRead(work.root,image.inputs)
                        var failure:Throwable=null
                        try
                          history.acquire(None)
                          history.validate(observed)
                          assertEquals(1L,history.dbFirstInitialPreparationSummary.verifiedCommitted)
                          assertEquals(0L,history.dbFirstInitialPreparationSummary.unresolvedReserved)
                          assertEquals(0L,history.dbFirstInitialPreparationSummary.pendingCancelled)
                        catch case cause:Throwable=>
                          failure=cause
                          assertThrows(classOf[IllegalStateException],()=>history.dbFirstInitialPreparationSummary)
                          throw cause
                        finally MySqlFormat3BudgetCleanup.run(failure,Vector(()=>history.close()))
                      else MySqlDbFirstInitialPreparationPersistedRead.requireCommittedReconstruction(observed,good.tag.operationId,work.root)
                    finally cold.closeScopedReadBudget()
                  (0 until 3).foreach(_=>reconstruct(target.relations))
                  assertEquals(accounted,work.root.accountedMemoryBytes,"cold reconstruction refunds decode/canonical children")
                  def replaceOperation(changed:Vector[MySqlFormat3Cell])=target.relations.map { relation=>
                    if relation.table!="fp_recovery_operation" then relation
                    else MySqlFormat3RelationSnapshot(relation.table,relation.rows.map(r=>if r==operation then changed else r))
                  }
                  val missingHistory=target.relations.map { relation=>
                    if relation.table!="fp_recovery_operation" then relation
                    else MySqlFormat3RelationSnapshot(relation.table,Vector(operation))
                  }
                  assertThrows(classOf[SQLDataException],()=>reconstruct(missingHistory))
                  MySqlRecoveryCanonicalBytes.withAllocationBudget(work.root) {
                    // 攻击样本重算所有外层摘要/指纹/见证，旧重建一致性门通过；业务重算必须拒绝。
                    val mutated=target.relations.map { relation=>
                      if relation.table!="floating_pod_state" then relation
                      else MySqlFormat3RelationSnapshot(relation.table,relation.rows.map { row=>
                        if cell(row,relation.table,"uuid",image.inputs).asInstanceOf[MySqlFormat3Cell.Binary].value.equalsUuid(id(800)) then
                          MySqlControlledDebugRows.update(row,relation.table,Map("pos_x"->MySqlControlledDebugRows.floating(99.0)),work.root)
                        else row
                      })
                    }
                    def consistent(source:MySqlScriptSchemaSnapshot,changed:Vector[MySqlFormat3RelationSnapshot]):Vector[MySqlFormat3RelationSnapshot] =
                      val originalScope=canonical.scope(data,facts,source,work.root)
                      val originalPreimage=canonical.projection(data,source,source.relations,false,work.root)
                      // scope在target中不是derived marker，先更新该原O字段再编码target。
                      def hash(bytes:Vector[Byte]):MySqlFormat3Cell =
                        work.root.reserveRetainedMemory(64L+bytes.size,"semantic tamper digest BEFORE allocation")
                        MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes.toArray,work.root))
                      val scopedOperation=MySqlControlledDebugRows.update(operation,"fp_recovery_operation",Map(
                        "scope_bytes"->originalScope.binaryCell,"scope_sha256"->hash(originalScope.sha256)),work.root)
                      val scopedRows=changed.map { relation=>
                        if relation.table!="fp_recovery_operation" then relation
                        else MySqlFormat3RelationSnapshot(relation.table,relation.rows.map(r=>if r==operation then scopedOperation else r))
                      }
                      val wrongTarget=canonical.projection(data,source,scopedRows,true,work.root)
                      val wrongFingerprint=canonical.fingerprint(data,originalScope,originalPreimage,wrongTarget,work.root)
                      val wrongContext=canonical.context(data,facts,wrongFingerprint,work.root)
                      val wrongWitness=canonical.witness(data,originalScope,originalPreimage,wrongTarget,wrongFingerprint,wrongContext,work.root)
                      var wrongOperation=scopedOperation
                      Vector("preimage"->originalPreimage,"target"->wrongTarget,"original_context"->wrongContext,"commit_witness"->wrongWitness).foreach { (prefix,bytes)=>
                        wrongOperation=MySqlControlledDebugRows.update(wrongOperation,"fp_recovery_operation",Map(
                          (prefix+"_bytes")->bytes.binaryCell,(prefix+"_sha256")->hash(bytes.sha256)),work.root)
                      }
                      val wrongGeneric=hash(canonical.operationContext(data,facts,wrongFingerprint,work.root).canonicalFingerprintSha256)
                      wrongOperation=MySqlControlledDebugRows.update(wrongOperation,"fp_recovery_operation",Map("operation_fingerprint"->wrongGeneric),work.root)
                      scopedRows.map { relation=>
                        if relation.table=="fp_recovery_operation" then
                          MySqlFormat3RelationSnapshot(relation.table,relation.rows.map(r=>if r==scopedOperation then wrongOperation else r))
                        else if relation.table=="fp_runtime_writer" then
                          MySqlFormat3RelationSnapshot(relation.table,relation.rows.map(r=>MySqlControlledDebugRows.update(r,relation.table,
                            Map("last_commit_fingerprint"->wrongGeneric),work.root)))
                        else relation
                      }
                    val consistentWrong=consistent(draft.before,mutated)
                    reconstruct(consistentWrong,false)
                    val semanticFailure=assertThrows(classOf[SQLDataException],()=>reconstruct(consistentWrong))
                    assertTrue(semanticFailure.getMessage.contains("independent preparation business transition differs"))
                    val oldFootprint=draft.before.relation("physical_recovery_footprint").rows.head
                    val orphan=MySqlControlledDebugRows.update(oldFootprint,"physical_recovery_footprint",Map(
                      "created_operation_uuid"->MySqlControlledDebugRows.uuid(id(2999))),work.root)
                    def orphanRows(rows:Vector[MySqlFormat3RelationSnapshot])=rows.map { relation=>
                      if relation.table!="physical_recovery_footprint" then relation
                      else MySqlFormat3RelationSnapshot(relation.table,relation.rows.map(r=>if r==oldFootprint then orphan else r))
                    }
                    val orphanBefore=MySqlScriptSchemaSnapshot.captureMaterialized(draft.before.schema,draft.before.connection,
                      draft.before.observationScope,draft.before.maxAllowedPacketBytes,draft.before.inputs,orphanRows(draft.before.relations),work.root)
                    val consistentOrphan=consistent(orphanBefore,orphanRows(target.relations))
                    val debtBeforeOrphanCheck=work.root.accountedMemoryBytes
                    reconstruct(consistentOrphan,false)
                    val orphanFailure=assertThrows(classOf[SQLDataException],()=>reconstruct(consistentOrphan))
                    assertTrue(orphanFailure.getMessage.contains("foreign-key"),orphanFailure.getMessage)
                    assertEquals(debtBeforeOrphanCheck,work.root.accountedMemoryBytes,"failed cold graph comparison refunds all child debt")
                    Vector("preimage_bytes","target_bytes").foreach { column=>
                      val original=cell(operation,"fp_recovery_operation",column,image.inputs).asInstanceOf[MySqlFormat3Cell.Binary].value
                      work.root.reserveRetainedMemory(192L+original.length,"tampered persisted effect frame BEFORE allocation")
                      val bytes=new Array[Byte](original.length+1)
                      (0 until original.length).foreach(i=>bytes(i)=original.byteAt(i))
                      val digest=java.security.MessageDigest.getInstance("SHA-256").digest(bytes)
                      val changed=MySqlControlledDebugRows.update(operation,"fp_recovery_operation",Map(
                        column->MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes,work.root)),
                        (column.stripSuffix("_bytes")+"_sha256")->MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(digest,work.root))),work.root)
                      MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(changed,work.root.profile,work.root,Some(image.inputs))
                      assertThrows(classOf[SQLDataException],()=>reconstruct(replaceOperation(changed)))
                    }
                    def rejectValidOuter(changed:Vector[MySqlFormat3Cell]):Unit =
                      MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(changed,work.root.profile,work.root,Some(image.inputs))
                      assertThrows(classOf[SQLDataException],()=>
                        MySqlDbFirstInitialPreparationPersistedRead.requireCommittedIdentity(changed,image.inputs,work.root))
                    rejectValidOuter(MySqlControlledDebugRows.update(operation,"fp_recovery_operation",
                      Map("boot_generation"->MySqlFormat3Cell.SignedInteger(2)),work.root))
                    rejectValidOuter(MySqlControlledDebugRows.update(operation,"fp_recovery_operation",
                      Map("footprint_uuid"->MySqlControlledDebugRows.uuid(id(999))),work.root))
                    Vector("request_identity_bytes","scope_bytes").foreach { column =>
                      val original=cell(operation,"fp_recovery_operation",column,image.inputs).asInstanceOf[MySqlFormat3Cell.Binary].value
                      work.root.reserveRetainedMemory(192L+original.length,"tampered complete frame and digest BEFORE allocation")
                      val bytes=new Array[Byte](original.length+1)
                      (0 until original.length).foreach(i=>bytes(i)=original.byteAt(i))
                      val digest=java.security.MessageDigest.getInstance("SHA-256").digest(bytes)
                      val hashColumn=column.stripSuffix("_bytes")+"_sha256"
                      rejectValidOuter(MySqlControlledDebugRows.update(operation,"fp_recovery_operation",Map(
                        column->MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes,work.root)),
                        hashColumn->MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(digest,work.root))),work.root))
                    }
                  }
                  assertThrows(classOf[IllegalArgumentException],()=>work.constructTransition(facts))
                  val fakeTransition=new MySqlDbFirstInitialPreparationTransition(work,draft,facts)
                  assertThrows(classOf[IllegalArgumentException],()=>fakeTransition.target)
                if !expectBudgetFailure then
                  val foreign=new MySqlDbFirstInitialPreparationDraft(good,work.root,at)
                  assertThrows(classOf[IllegalArgumentException],()=>foreign.domainConstruction)
              else if reservation eq malformed then
                work.preflight(image)
                val invalid=java.time.LocalDateTime.of(2026,11,2,12,0).withNano(1)
                val cause=assertThrows(classOf[IllegalArgumentException],()=>work.constructDomainRows(invalid))
                assertSame(cause,assertThrows(classOf[IllegalArgumentException],()=>work.constructDomainRows(invalid.withNano(0))))
                assertSame(cause,assertThrows(classOf[IllegalArgumentException],()=>work.validatedDataSource))
                assertFalse(work.root.isClosed)
              else
                val runner=work.beginTransaction()
                val cause=assertThrows(classOf[MySqlDbFirstPreparationRejected],()=>work.preflight(image))
                assertTrue(cause.getSuppressed.isEmpty)
                assertSame(image,work.rejectedDataSource(runner,cause))
                val forged=new MySqlDbFirstPreparationRejected(cause.reason,"same reason is not original cause")
                assertThrows(classOf[IllegalArgumentException],()=>work.rejectedDataSource(runner,forged))
                assertThrows(classOf[IllegalArgumentException],()=>work.rejectedDataSource(new MySqlDbFirstInitialPreparationTransaction(work),cause))
                assertThrows(classOf[IllegalArgumentException],()=>runner.rejectedMainReadSource(cause))
                assertThrows(classOf[IllegalArgumentException],()=>runner.retireRejectedResources(cause))
                assertThrows(classOf[IllegalArgumentException],()=>runner.requireRejectedResourcesRetired(cause))
                assertFalse(runner.rejectedResourcesRetired)
                assertSame(cause,assertThrows(classOf[SQLDataException],()=>work.validatedDataSource))
                assertSame(cause,assertThrows(classOf[SQLDataException],()=>work.preflight(image)))
                assertFalse(work.root.isClosed)
            catch case cause:Throwable=>
              sourceFailure=cause
              throw cause
            finally
              MySqlFormat3BudgetCleanup.run(sourceFailure,Vector(
                ()=>child.closeScopedReadBudget(),
                ()=>if actualSourceRoot ne work.root then
                  val debt=work.root.accountedMemoryBytes
                  actualSourceRoot.closeRootReadBudget()
                  assertTrue(actualSourceRoot.isClosed)
                  assertFalse(work.root.isClosed)
                  assertTrue(work.root.accountedMemoryBytes<debt,"independent source root debt refunded; original Entry payload remains")
                  if !expectBudgetFailure then
                    assertSame(work.root,preparedTarget.allocationBudget,"preparation complete target must be independently owned")
                    assertEquals(27,preparedTarget.relations.size)
                    var bytes=0L
                    preparedTarget.relations.foreach(_.rows.foreach(_.foreach {
                      case MySqlFormat3Cell.Binary(value)=>
                        var i=0
                        while i<value.length do
                          value.byteAt(i) // 来源root关闭后实际读取每个保留binary，不借用lazy hash作为证明。
                          bytes+=1L;i+=1
                      case _=>()
                    }))
                    assertTrue(bytes>0L,"retained preparation payload remains readable after source root closes")
              ))
          }
        }
        if !expectBudgetFailure then
          val forged=MySqlDbFirstSession.DbFirstInitialPreparationObservation(good.tag,good.request.podUuid,true,true,true,
            preparedTarget.canonicalSha256Hex)
          assertThrows(classOf[IllegalArgumentException],()=>f.core.claimDbFirstInitialPreparationConfirmation(good,forged))
          val rejected=MySqlDbFirstSession.DbFirstInitialPreparationRejection(good.tag,good.request.podUuid,
            new MySqlDbFirstPreparationRejected(MySqlDbFirstPreparationRejected.Reason.ActiveCapacity,"caller DATA"),"0"*64)
          assertThrows(classOf[IllegalArgumentException],()=>f.core.claimPreparationRejection(good,rejected))
          val fakeRejected=MySqlDbFirstPreparationRejectionConfirmation.issue(f.core,good.handle,rejected)
          assertSame(rejected.cause,fakeRejected.cause)
          assertThrows(classOf[IllegalArgumentException],()=>fakeRejected.requireOriginal(good))
          assertThrows(classOf[IllegalArgumentException],()=>f.core.detachRejectedPreparation(fakeRejected))
          assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRejectedPreparationReadReleased(fakeRejected,good))
          f.onWorker {
            assertThrows(classOf[IllegalArgumentException],()=>f.core.beginRejectedPreparationReadRelease(fakeRejected))
            assertThrows(classOf[IllegalArgumentException],()=>f.core.finishRejectedPreparationReadRelease(fakeRejected))
            assertThrows(classOf[IllegalArgumentException],()=>f.core.retireRejectedPreparation(fakeRejected))
          }
          // 仅验证真实router的DATA/精确ACK和slot寿命；未登记confirmation仍被core拒绝。
          val rejectedRouter=new MySqlTrackedDriftCompletionRouter(MySqlOwnedIoIdentity(good.tag.bootId,
            good.tag.bootGeneration,good.tag.connectionGeneration),1)
          assertTrue(rejectedRouter.begin(good.tag,MySqlCompletionChannel.Ordinary,true))
          rejectedRouter.submitted(good.tag,IoSubmission.Accepted)
          rejectedRouter.routeBatch(Vector(com.wi.floatingpod.server.io.IoCompletion[MySqlOwnedIoReply](good.tag,
            scala.util.Success(MySqlOwnedIoReply.DbFirstInitialPreparationRejected(rejected)))))
          rejectedRouter.take(MySqlCompletionChannel.Ordinary,1)
          rejectedRouter.beginRelease(good.tag,MySqlCompletionChannel.Ordinary)
          rejectedRouter.submitted(good.tag,IoSubmission.RejectedCapacity)
          assertEquals(1,rejectedRouter.retainedCount)
          rejectedRouter.beginRelease(good.tag,MySqlCompletionChannel.Ordinary)
          rejectedRouter.submitted(good.tag,IoSubmission.Accepted)
          val rejectedAck=com.wi.floatingpod.server.io.IoCompletion[MySqlOwnedIoReply](good.tag,
            scala.util.Success(MySqlOwnedIoReply.RejectedPreparationMainReadReleased(fakeRejected)))
          rejectedRouter.routeBatch(Vector(rejectedAck))
          assertSame(rejectedAck,rejectedRouter.take(MySqlCompletionChannel.Ordinary,1).head)
          assertThrows(classOf[IllegalArgumentException],()=>rejectedRouter.requireRejectedPreparationReadAck(fakeRejected,rejectedAck.copy()))
          assertThrows(classOf[IllegalArgumentException],()=>rejectedRouter.beginRelease(good.tag,MySqlCompletionChannel.Ordinary))
          rejectedRouter.finishRejectedPreparationReadAck(fakeRejected,rejectedAck)
          assertEquals(1,rejectedRouter.retainedCount,"read ACK does not retire original rejected Entry")
          assertThrows(classOf[IllegalArgumentException],()=>rejectedRouter.finishRejectedPreparationReadAck(fakeRejected,rejectedAck))
          rejectedRouter.beginRelease(good.tag,MySqlCompletionChannel.Ordinary)
          rejectedRouter.submitted(good.tag,IoSubmission.Accepted)
          val finalRejectedAck=com.wi.floatingpod.server.io.IoCompletion[MySqlOwnedIoReply](good.tag,
            scala.util.Success(MySqlOwnedIoReply.RejectedPreparationRetired(fakeRejected)))
          assertThrows(classOf[IllegalArgumentException],()=>rejectedRouter.finishRejectedPreparationRetirement(fakeRejected,finalRejectedAck))
          rejectedRouter.routeBatch(Vector(finalRejectedAck))
          assertSame(finalRejectedAck,rejectedRouter.take(MySqlCompletionChannel.Ordinary,1).head)
          assertThrows(classOf[IllegalArgumentException],()=>rejectedRouter.finishRejectedPreparationRetirement(fakeRejected,finalRejectedAck.copy()))
          rejectedRouter.finishRejectedPreparationRetirement(fakeRejected,finalRejectedAck)
          assertEquals(0,rejectedRouter.retainedCount)
          assertTrue(rejectedRouter.failure.isEmpty)
          rejectedRouter.requireTerminalEmpty()
          assertThrows(classOf[IllegalStateException],()=>rejectedRouter.finishRejectedPreparationRetirement(fakeRejected,finalRejectedAck))
          val unregistered=MySqlDbFirstInitialPreparationConfirmation.issue(f.core,good.handle,good.request)
          assertThrows(classOf[IllegalArgumentException],()=>f.core.requirePreparationReadReleased(unregistered,good))
          assertThrows(classOf[IllegalArgumentException],()=>f.core.recordDbFirstPreparationFinal(unregistered))
          val unregisteredFinal=MySqlDbFirstPreparationFinalReceipt.issue(f.core,good.handle)
          assertEquals(good.tag,unregisteredFinal.tag)
          assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstPreparationLocalRetirementException.fromCore(
            f.core,unregisteredFinal,new IllegalStateException("caller has no local retirement qualification")))
          // 仅router内存策略：此未登记DATA不能通过上方core资格门，不制造SQL成功。
          val router=new MySqlTrackedDriftCompletionRouter(MySqlOwnedIoIdentity(good.tag.bootId,
            good.tag.bootGeneration,good.tag.connectionGeneration),1)
          assertTrue(router.begin(good.tag,MySqlCompletionChannel.Ordinary,true))
          router.submitted(good.tag,IoSubmission.Accepted)
          router.routeBatch(Vector(com.wi.floatingpod.server.io.IoCompletion[MySqlOwnedIoReply](good.tag,
            scala.util.Success(MySqlOwnedIoReply.Transaction(Vector.empty)))))
          router.take(MySqlCompletionChannel.Ordinary,1)
          router.beginRelease(good.tag,MySqlCompletionChannel.Ordinary)
          router.submitted(good.tag,IoSubmission.Accepted)
          val ack=com.wi.floatingpod.server.io.IoCompletion[MySqlOwnedIoReply](good.tag,
            scala.util.Success(MySqlOwnedIoReply.PreparationMainReadReleased(unregistered)))
          router.routeBatch(Vector(ack));assertSame(ack,router.take(MySqlCompletionChannel.Ordinary,1).head)
          assertThrows(classOf[IllegalArgumentException],()=>router.requirePreparationReadAck(unregistered,ack.copy()))
          assertThrows(classOf[IllegalArgumentException],()=>router.beginRelease(good.tag,MySqlCompletionChannel.Ordinary))
          router.finishPreparationReadAck(unregistered,ack)
          assertEquals(1,router.retainedCount,"read generation ACK must retain original business slot")
          assertTrue(router.failure.isEmpty)
          assertThrows(classOf[IllegalArgumentException],()=>router.finishPreparationReadAck(unregistered,ack))
          router.beginRelease(good.tag,MySqlCompletionChannel.Ordinary)

          assertThrows(classOf[IllegalArgumentException],()=>unregistered.requireOriginal(good))
          val activation=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(1231)),DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION)
          val activationBudget=f.core.validationBudget(activation.handle)
          val beforeMetadata=activationBudget.accountedMemoryBytes
          assertSame(preparedView,f.core.retainRecoveredInitialData(activation,preparedView))
          assertTrue(activationBudget.accountedMemoryBytes>=beforeMetadata+4096L)
          assertThrows(classOf[IllegalArgumentException],()=>f.core.retainRecoveredInitialData(activation,preparedView))
          f.core.requireDbFirstCaptureReservation(activation)
          val capture=f.core.captureDbFirstSnapshot(activation)(a=>
            DbFirstPodSnapshotTest.captureInitialForCore(activation.tag.operationId,a,id(803),id(800),id(503),id(801)))
          f.core.requireDbFirstInitialActivationSubmission(activation)
          f.core.submittedDbFirstInitialActivation(activation,IoSubmission.Accepted)
          f.onWorker {
            val runner=f.core.beginDbFirstInitialActivation(activation,activeLimit)
            val at=java.time.LocalDateTime.of(2026,10,29,12,1)
            val result=f.core.materializeExecutingDbFirstInitialActivation(runner,preparedTarget,91L,id(1232),at,at.plusSeconds(1),activeLimit)
            result.verify()
            val activationRoot=f.core.validationBudget(activation.handle)
            result.target.canonicalSha256
            val mixedBaseline=f.shared.accountedBytes
            (0 until 2).foreach(_=>assertEquals(2L,MySqlDbFirstRecallPersistedRead.verifySnapshotReceiptSegment(
              result.target,preparedBefore,activationRoot)))
            assertEquals(mixedBaseline,f.shared.accountedBytes,"mixed initial/preparation cursor must stay live until outer scope retires")
            val stopChain=MySqlDbFirstRecallPersistedRead.verifyStopSnapshotReceiptSegment(result.target,
              preparedTarget,activationRoot,result.request.podUuid,result.request.tag.operationId,None,None)
            assertEquals(1L,stopChain.steps)
            assertEquals(64,stopChain.sha256.length)
            assertThrows(classOf[java.sql.SQLDataException],()=>
              MySqlDbFirstRecallPersistedRead.verifyStopSnapshotReceiptSegment(result.target,
                preparedTarget,activationRoot,id(9999),result.request.tag.operationId,None,None))
            assertThrows(classOf[java.sql.SQLDataException],()=>
              MySqlDbFirstRecallPersistedRead.verifyStopSnapshotReceiptSegment(result.target,
                preparedTarget,activationRoot,result.request.podUuid,id(9999),None,None))
            val domain=result.target.relation("floating_pod_state").rows.find(r=>
              cell(r,"floating_pod_state","uuid",observed.inputs).asInstanceOf[MySqlFormat3Cell.Binary].value.equalsUuid(id(800))).get
            assertEquals(MySqlFormat3Cell.SignedInteger(1),cell(domain,"floating_pod_state","is_active",observed.inputs))
            assertEquals(MySqlFormat3Cell.SignedInteger(1),cell(domain,"floating_pod_state","state_version",observed.inputs))
            assertEquals(preparedTarget.relation("fp_pod_snapshot").rows.size+1,result.target.relation("fp_pod_snapshot").rows.size)
            assertEquals(capture.byteLength(),result.request.nbtByteLength)
            val currentOwner=activationRoot.newScopedReadBudget(result.target.maxAllowedPacketBytes)
            var currentData:MySqlFormat3ReadBudget=null
            try
              currentOwner.initialize(result.target.schema,result.target.connection,result.target.observationScope,result.target.maxAllowedPacketBytes)
              val current=MySqlScriptSchemaSnapshot.decoded(result.target.schema,result.target.connection,result.target.observationScope,
                result.target.maxAllowedPacketBytes,result.target.inputs,copyPersistedRows(result.target.relations,currentOwner),currentOwner)
              val proofNow=java.time.LocalDateTime.of(2026,10,30,12,0)
              val acquired=MySqlDbFirstBootstrapProof.writer(preparedBefore,proofNow)
              val released=MySqlDbFirstBootstrapWriterGraph.buildTarget(preparedBefore,result.target.inputs,activationRoot,
                acquired.copy(writerEpoch=acquired.writerEpoch-1L,ownerBootUuid=None,
                  leaseUntil=java.time.LocalDateTime.of(2026,10,29,11,59)))
              try
                if activeBaseline then
                  val creation=MySqlDbFirstAbandonedStartupData.inspectCommittedCreationActivation(released,current,
                    activation.tag.bootId,Some(activation.tag.bootId),acquired.writerEpoch,proofNow,activationRoot)
                  assertEquals(good.request.createOperationUuid,creation.create)
                  assertEquals(good.tag.operationId,creation.preparation)
                  assertEquals(result.request.tag.operationId,creation.activation)
                  assertEquals(good.request.podUuid,creation.pod)
                  assertEquals(good.request.attemptUuid,creation.attempt)
                  assertEquals(good.request.footprintUuid,creation.footprint)
                  assertEquals(acquired.writerEpoch,creation.epoch)
                  assertEquals(creation.activation,creation.lastOperation)
                  assertEquals(MySqlDbFirstSchemaDefinition.InitialActivationStep,creation.lastKind)
                  assertEquals(2L,creation.steps,"the new chain counts only its exact committed P and single A")
                  assertEquals(1L,creation.version)
                  assertEquals(Vector(id(502)->7L),creation.preservedActivePods)
                  assertNotEquals(stopChain.sha256,creation.chainSha256,"the combined digest includes committed P as well as A")
                  val wrongEpoch=assertThrows(classOf[MySqlDbFirstAbandonedStartupInspection],()=>
                    MySqlDbFirstAbandonedStartupData.inspectCommittedCreationActivation(released,current,
                      activation.tag.bootId,Some(activation.tag.bootId),acquired.writerEpoch+1L,proofNow,activationRoot))
                  assertEquals("REJECTED_CREATION_ACTIVATION_CURRENT_WRITER",wrongEpoch.safeSummary)
                else
                  val noPreservedActive=assertThrows(classOf[MySqlDbFirstAbandonedStartupInspection],()=>
                    MySqlDbFirstAbandonedStartupData.inspectCommittedCreationActivation(released,current,
                      activation.tag.bootId,Some(activation.tag.bootId),acquired.writerEpoch,proofNow,activationRoot))
                  assertEquals("REJECTED_CREATION_ACTIVATION_NO_PRESERVED_ACTIVE_POD",noPreservedActive.safeSummary)
              finally released.allocationBudget.closeScopedReadBudget()
              currentData=activationRoot.newCanonicalDataScope()
              val query=MySqlFormat3QueryProjection.fromDbFirstData(current,current.inputs,activationRoot,currentData)
              assertTrue(MySqlDbFirstInitialPreparationDataView.currentPreparedInitials(current,activationRoot,currentData,query).isEmpty,
                "activated Pod must no longer be offered as resumable PREPARED DATA")
            finally
              if currentData!=null then currentData.closeScopedReadBudget()
              currentOwner.closeScopedReadBudget()
            println("PREPARE_THEN_INITIAL_DATA_BYTES="+result.target.canonicalByteLength)
          }
        assertEquals(if expectBudgetFailure then 3 else 4,f.core.retainedRoots)
        assertTrue(f.core.observation.failure.isEmpty)
      finally f.close()
    }

  @Test def initialPreparationPreflightUsesFull27AndRejectsExistingIdentitiesAndCapacity(): Unit =
    withPersistedDbFirstImage(queryGraph=true) { (observed,root,existingOperation) =>
      val inputs=observed.inputs
      val originalDigest=observed.canonicalSha256Hex
      val originalSnapshots=observed.relation(MySqlDbFirstSchemaDefinition.SnapshotTable).rows
      assertFalse(originalSnapshots.isEmpty,"exercise an actual retained snapshot relation")
      val request=MySqlControlledDebugPrepareRequest(
        com.wi.floatingpod.storage.PhysicalRecoveryBinding(id(503),com.wi.floatingpod.storage.PhysicalRecoveryCutoverState.Certified,2),
        id(800),id(801),id(802),com.wi.floatingpod.domain.FloatingPodPose("world",1,64,2,3,4,0),id(803))
      val cases=Vector((request,id(804),1,true),(request,id(804),256,true),
        (request,id(804),0,false),(request,id(804),257,false),
        (request.copy(podUuid=id(502)),id(804),1,false),
        (request.copy(attemptUuid=id(504)),id(804),1,false),
        (request.copy(footprintUuid=id(505)),id(804),1,false),
        (request,existingOperation,1,false))
      cases.foreach { (candidate,operation,limit,accepted) =>
        val baseline=root.accountedMemoryBytes
        root.reserveRetainedMemory(MySqlDbFirstForestRead.HolderBytes,"preparation preflight original forest holder")
        val holder=new MySqlDbFirstForestRead(root,inputs,id(503))
        try
          if accepted then
            MySqlDbFirstInitialPreparationPreflight.requireFresh(observed,candidate,operation,limit,root,holder)
            assertEquals(0L,holder.result.verifiedForests)
          else
            val failure=assertThrows(classOf[SQLDataException],()=>
              MySqlDbFirstInitialPreparationPreflight.requireFresh(observed,candidate,operation,limit,root,holder))
            if limit>=1 && limit<=256 then
              assertEquals(MySqlDbFirstPreparationRejected.Reason.ExistingIdentity,
                assertInstanceOf(classOf[MySqlDbFirstPreparationRejected],failure).reason)
            else assertFalse(failure.isInstanceOf[MySqlDbFirstPreparationRejected],"invalid configuration is not a request rejection")
        finally holder.close()
        assertEquals(baseline,root.accountedMemoryBytes,"preflight must refund comparison debt and keep original DATA")
        assertEquals(27,observed.relations.size)
        assertEquals(originalSnapshots,observed.relation(MySqlDbFirstSchemaDefinition.SnapshotTable).rows)
        assertEquals(originalDigest,observed.canonicalSha256Hex)
      }
    }

  private def initialBefore(f: Fixture,inputs: MySqlScriptSchemaInputs,fault: String="", activeSibling: Boolean=false, historicalIntent:Boolean=false): MySqlScriptSchemaSnapshot =
    val base=dbFirstBefore(f,inputs)
    val root=base.allocationBudget
    val pose=com.wi.floatingpod.domain.FloatingPodPose("world",1,64,2,3,4,0)
    val at=java.time.LocalDateTime.of(2026,10,29,10,1)
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      def change(row:Vector[MySqlFormat3Cell],table:String,fields:(String,MySqlFormat3Cell)*)=
        fields.foldLeft(row)((r,p)=>r.updated(inputs.forTable(table).columns.indexWhere(_.name==p._1),p._2))
      val domain=MySqlControlledDebugRows.preparedDomain(id(503),id(502),pose,id(700),at,root)
      val locator=MySqlControlledDebugRows.preparedLocator(id(503),id(502),id(504),id(505),0,id(700),root)
      val intent=MySqlControlledDebugRows.preparedIntent(id(503),id(502),id(504),id(505),id(700),
        id(if fault=="creation" then 506 else 505),root)
      val originalFootprint=MySqlControlledDebugRows.preparedFootprint(id(503),id(502),id(504),id(505),pose,id(700),root)
      val footprint=fault match
        case "footprint-version"=>change(originalFootprint,"physical_recovery_footprint","origin_version"->MySqlFormat3Cell.SignedInteger(1))
        case "footprint-center"=>change(originalFootprint,"physical_recovery_footprint","center_chunk_x"->MySqlFormat3Cell.SignedInteger(1))
        case "footprint-operation"=>change(originalFootprint,"physical_recovery_footprint","created_operation_uuid"->
          MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(MySqlScriptSchemaDataTestFixture.uuidBytes(id(701)),root)))
        case _=>originalFootprint
      val allChunks=MySqlControlledDebugRows.preparedChunks(id(503),id(502),id(504),id(505),pose,root)
      val chunks=if fault=="chunks" then allChunks.dropRight(1) else allChunks
      val operation=change(base.relation("fp_recovery_operation").rows.head,"fp_recovery_operation",
        "step"->MySqlFormat3Cell.Text("REARM"),"expected_domain_version"->MySqlFormat3Cell.SignedInteger(0),
        "expected_locator_revision"->MySqlFormat3Cell.SignedInteger(0))
      val siblingRows=if activeSibling then
        val siblingPrepared=MySqlControlledDebugRows.preparedDomain(id(503),id(600),pose,id(710),at,root)
        val sibling=MySqlControlledDebugRows.activatedDomain(siblingPrepared,id(710),at.plusSeconds(1),root)
        // 明确的历史DATA样本；用真context encoder，仍不声称原SQL业务witness验收。
        val context=MySqlOperationContext(id(1),3L,id(503),id(710),
          Vector(MySqlExpectedEffect("fixture-existing-active",0L,Vector.fill(32)(7.toByte))))
        val encoded=MySqlModeOperationEffectCodec.operationContextCanonicalBytes(context,root.profile)
        def binary(value:Array[Byte])=MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(value,root))
        val historical=change(base.relation("fp_recovery_operation").rows.head,"fp_recovery_operation",
          "operation_uuid"->MySqlControlledDebugRows.uuid(id(710)),"workflow_uuid"->MySqlControlledDebugRows.uuid(id(710)),
          "pod_uuid"->MySqlControlledDebugRows.uuid(id(600)),"attempt_uuid"->MySqlFormat3Cell.SqlNull,
          "footprint_uuid"->MySqlFormat3Cell.SqlNull,"step"->MySqlFormat3Cell.Text("DOMAIN_MUTATION"),
          "expected_domain_version"->MySqlFormat3Cell.SignedInteger(0),"expected_locator_revision"->MySqlFormat3Cell.SqlNull,
          "original_context_bytes"->encoded.binaryCell,"original_context_sha256"->binary(encoded.sha256.toArray),
          "operation_fingerprint"->binary(context.canonicalFingerprintSha256.toArray))
        (Vector(sibling),Vector(historical))
      else (Vector.empty[Vector[MySqlFormat3Cell]],Vector.empty[Vector[MySqlFormat3Cell]])
      // 仅DATA主键选择回归；完整保留另一attempt，不声称其历史业务witness/SQL已验收。
      val otherIntent=if historicalIntent then Vector(MySqlControlledDebugRows.preparedIntent(id(503),id(502),id(506),id(507),id(700),id(508),root)) else Vector.empty
      val otherFootprint=if historicalIntent then Vector(MySqlControlledDebugRows.preparedFootprint(id(503),id(502),id(506),id(507),pose,id(700),root)) else Vector.empty
      val otherChunks=if historicalIntent then MySqlControlledDebugRows.preparedChunks(id(503),id(502),id(506),id(507),pose,root) else Vector.empty
      val replacement=Map("floating_pod_state"->(Vector(domain)++siblingRows._1),"physical_recovery_locator"->Vector(locator),
        "physical_recovery_intent"->(Vector(intent)++otherIntent),"physical_recovery_footprint"->(Vector(footprint)++otherFootprint),
        "physical_recovery_footprint_chunk"->(chunks++otherChunks),"fp_recovery_operation"->(Vector(operation)++siblingRows._2))
      val rows=base.relations.map(r=>r.copy(rows=replacement.getOrElse(r.table,r.rows)))
      MySqlScriptSchemaSnapshot.captureMaterialized(base.schema,base.connection,base.observationScope,
        base.maxAllowedPacketBytes,inputs,rows,root)
    }

  @Test def initialWorkerMaterializesFullPayloadAndActiveDomainIntentLocatorUnderOriginalEntry(): Unit =
    val inputs=dbFirstMaterializationInputs()
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var source:MySqlScriptSchemaSnapshot=null
    try
      val reservation=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(1100)),DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION)
      val snapshot=f.core.captureDbFirstSnapshot(reservation)(a=>DbFirstPodSnapshotTest.captureInitialForCore(reservation.tag.operationId,a))
      val request=f.core.dbFirstInitialActivationRequest(reservation,snapshot)
      source=initialBefore(f,inputs,historicalIntent=true)
      val planned=java.time.LocalDateTime.of(2026,10,29,12,0)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.materializeDbFirstInitialActivation(reservation,request,source,91L,id(1101),planned,planned.plusSeconds(1),256))
      f.core.requireDbFirstInitialActivationSubmission(reservation)
      f.onWorker {
        val runner=f.core.beginDbFirstInitialActivation(reservation,256)
        assertSame(request,runner.request)
        val result=f.core.materializeExecutingDbFirstInitialActivation(runner,source,91L,id(1101),planned,planned.plusSeconds(1),256)
        val selectedIntent=MySqlDbFirstInitialActivationTransaction.targetRow(result,"physical_recovery_intent")
        assertEquals(MySqlFormat3Cell.Text("ACTIVATED"),cell(selectedIntent,"physical_recovery_intent","phase",inputs))
        assertTrue(cell(selectedIntent,"physical_recovery_intent","attempt_uuid",inputs).asInstanceOf[MySqlFormat3Cell.Binary].value.equalsUuid(id(504)))
        val historical=source.relation("physical_recovery_intent").rows.last
        assertEquals(historical,result.target.relation("physical_recovery_intent").rows.last,"other attempt must remain byte-identical")
        def value(t:String,c:String)=result.target.relation(t).rows.head(inputs.forTable(t).columns.indexWhere(_.name==c))
        assertEquals(MySqlFormat3Cell.SignedInteger(1),value("floating_pod_state","is_active"))
        assertEquals(MySqlFormat3Cell.SignedInteger(1),value("floating_pod_state","state_version"))
        assertEquals(MySqlFormat3Cell.Text("WORLD_IDLE"),value("floating_pod_state","operating_mode"))
        assertEquals(MySqlFormat3Cell.Text("ACTIVATED"),value("physical_recovery_intent","phase"))
        assertEquals(MySqlFormat3Cell.Text("ROW_ACTIVE"),value("physical_recovery_locator","locator_state"))
        assertEquals(MySqlFormat3Cell.Text("RELEASED"),value("fp_recovery_reservation","reservation_state"))
        assertEquals(27,result.target.relations.size)
        assertEquals(256,result.facts.activeLimit)
        val materializationRoot=f.core.validationBudget(reservation.handle)
        result.target.canonicalSha256
        val receiptBaseline=f.shared.accountedBytes
        assertEquals(1L,MySqlDbFirstRecallPersistedRead.verifySnapshotReceiptSegment(result.target,source,materializationRoot))
        assertEquals(receiptBaseline,f.shared.accountedBytes,"initial receipt decoder/cursor must retire in shared comparison scope")
        MySqlRecoveryCanonicalBytes.withComparisonBudget(materializationRoot) {
          val differentLimit=MySqlDbFirstInitialActivationCanonical.scope(request,result.facts.copy(activeLimit=1),result.before,materializationRoot)
          assertFalse(result.scope.exactlyEquals(differentLimit),"scope must bind actual configured active capacity")
        }
        val bytes=value("fp_pod_snapshot","nbt_bytes") match
          case MySqlFormat3Cell.Binary(actual)=>actual
          case _=>throw new AssertionError("snapshot payload must be binary")
        assertEquals(snapshot.byteLength(),bytes.length)
        (0 until bytes.length).foreach(i=>assertEquals(snapshot.byteAt(i),bytes.byteAt(i)))
        result.verify()
        assertThrows(classOf[IllegalArgumentException],()=>f.core.materializeDbFirstInitialActivation(reservation,request,source,91L,id(1101),planned,planned.plusSeconds(1),256))
      }
    finally
      if source!=null then source.allocationBudget.closeRootReadBudget()
      f.close()

  @Test def initialMaterializationRejectsWrongCreationAndIncompletePhysicalTarget(): Unit =
    Vector("creation","chunks","footprint-version","footprint-center","footprint-operation").foreach { fault=>
      val inputs=dbFirstMaterializationInputs();val f=new Fixture(1,dbFirstInputs=Some(inputs))
      var source:MySqlScriptSchemaSnapshot=null
      try
        val reservation=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(1102)),DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION)
        val snapshot=f.core.captureDbFirstSnapshot(reservation)(a=>DbFirstPodSnapshotTest.captureInitialForCore(reservation.tag.operationId,a))
        val request=f.core.dbFirstInitialActivationRequest(reservation,snapshot)
        source=initialBefore(f,inputs,fault)
        val planned=java.time.LocalDateTime.of(2026,10,29,12,0)
        assertInstanceOf(classOf[SQLDataException],f.onWorkerFailure(
          f.core.materializeDbFirstInitialActivation(reservation,request,source,91L,id(1103),planned,planned.plusSeconds(1),256)))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.captureDbFirstSnapshot(reservation)(_=>snapshot))
      finally
        if source!=null then source.allocationBudget.closeRootReadBudget()
        f.close()
    }

  @Test def initialConfiguredCapacityRejectsInvalidOrFullAndAcceptsRemainingSlot(): Unit =
    Vector((0,false,false),(257,false,false),(1,true,false),(2,true,true),(1,false,true)).foreach { (limit,occupied,accepted)=>
      val inputs=dbFirstMaterializationInputs();val f=new Fixture(2,dbFirstInputs=Some(inputs))
      var source:MySqlScriptSchemaSnapshot=null
      try
        val reservation=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(1110)),DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION)
        val snapshot=f.core.captureDbFirstSnapshot(reservation)(a=>DbFirstPodSnapshotTest.captureInitialForCore(reservation.tag.operationId,a))
        val request=f.core.dbFirstInitialActivationRequest(reservation,snapshot)
        source=initialBefore(f,inputs,activeSibling=occupied)
        val originalRoot=f.core.validationBudget(reservation.handle)
        val planned=java.time.LocalDateTime.of(2026,10,29,12,0)
        if accepted then f.onWorker {
          val result=f.core.materializeDbFirstInitialActivation(reservation,request,source,91L,id(1111),planned,planned.plusSeconds(1),limit)
          val activeIndex=inputs.forTable("floating_pod_state").columns.indexWhere(_.name=="is_active")
          assertEquals(limit,result.target.relation("floating_pod_state").rows.count(_(activeIndex)==MySqlFormat3Cell.SignedInteger(1)))
          assertEquals(limit,result.facts.activeLimit)
          if occupied then
            assertEquals(source.relation("floating_pod_state").rows(1),result.target.relation("floating_pod_state").rows(1),
              "activation must preserve the existing active sibling exactly")
          result.verify()
        }
        else
          val failure=f.onWorkerFailure(f.core.materializeDbFirstInitialActivation(reservation,request,source,91L,id(1111),planned,planned.plusSeconds(1),limit))
          if occupied then
            assertInstanceOf(classOf[SQLDataException],failure)
            assertTrue(failure.getMessage.contains("configured active capacity"))
          else
            assertInstanceOf(classOf[IllegalArgumentException],failure)
            assertTrue(failure.getMessage.contains("configured active limit"))
          assertFalse(originalRoot.isClosed)
          assertThrows(classOf[IllegalArgumentException],()=>f.core.captureDbFirstSnapshot(reservation)(_=>snapshot))
          assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure(
            f.core.materializeDbFirstInitialActivation(reservation,request,source,91L,id(1111),planned,planned.plusSeconds(1),2)))
          val healthy=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(1112)))
          f.core.requireDbFirstCaptureReservation(healthy)
          assertEquals(2,f.core.retainedRoots,"local rejection retains original Entry while allowing another core admission")
      finally
        if source!=null then source.allocationBudget.closeRootReadBudget()
        f.close()
    }

  @Test def full27RetainedCoreKeepsExactFamilyAndCannotIssueOldPlansOrMainImages(): Unit =
    val configuration=DeploymentReceiptStaticTestData.defaultProfile()
    val inputs=MySqlScriptSchemaInputs.createDbFirstFull27(MySqlDbFirstSchemaProfile.render(configuration))
    val foreign=MySqlScriptSchemaInputs.createDbFirstFull27(MySqlDbFirstSchemaProfile.render(configuration))
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    try
      assertThrows(classOf[IllegalArgumentException],()=>f.reserve(971))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveTransaction(
        IoOperationTag(id(1),1L,1L,id(972)),id(973),id(974),Vector(MySqlRepositoryOperation.FindAll)))
      assertEquals(0,f.core.retainedRoots)
      val before=f.shared.accountedBytes
      val reservation=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(975)))
      val root=f.core.validationBudget(reservation.handle)
      root.requireScriptDataInputs(inputs)
      root.requireLedgerIdentity(f.shared)
      assertThrows(classOf[SQLNonTransientConnectionException],()=>root.requireScriptDataInputs(foreign))
      assertThrows(classOf[IllegalArgumentException],()=>root.requireLedgerIdentity(
        new MySqlFormat3AllocationLedger(f.limits.maxRetainedBytes())))
      assertThrows(classOf[SQLNonTransientConnectionException],()=>root.initialize(inputs.schema,
        MySqlScriptSchemaDataTestFixture.Connection,
        MySqlFormat3ObservationScope(f.identity.bootId,f.identity.bootGeneration,f.identity.connectionGeneration),
        inputs.budget.packetBytes))
      assertInstanceOf(classOf[SQLNonTransientConnectionException],f.onWorkerFailure(root.bindRetainedImageWorker(new Object())))
      val captured=f.core.captureDbFirstSnapshot(reservation)(allocation=>
        DbFirstPodSnapshotTest.captureForCore(reservation.tag.operationId,allocation,100))
      assertSame(f.core.dbFirstRecallRequest(reservation,captured),f.core.dbFirstRecallRequest(reservation,captured))
      val retained=f.shared.accountedBytes
      assertTrue(retained>before)
      f.core.stop()
      assertInstanceOf(classOf[IllegalStateException],f.onWorkerFailure(f.core.requireStoppedEmptyForTerminal()))
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure(f.core.retireSettledMemoryGraph()))
      assertFalse(root.isClosed)
      assertEquals(retained,f.shared.accountedBytes,"live capture cannot be cleared by terminal owner retirement")
      assertEquals(1,f.core.retainedRoots)
    finally f.close()

  @Test def full27RetainedFamilyRejectsFull26AndFull26FamilyRejectsFull27(): Unit =
    val inputs=MySqlScriptSchemaInputs.createDbFirstFull27(
      MySqlDbFirstSchemaProfile.render(DeploymentReceiptStaticTestData.defaultProfile()))
    val f=new Fixture(1)
    try
      assertThrows(classOf[IllegalArgumentException],()=>new MySqlRetainedPlanReservationCore(
        f.identity,f.worker,Thread.currentThread(),inputs.sourceLimitProfile,f.shared,1,None,Some(inputs)))
      val old=MySqlScriptSchemaDataTestFixture.Full26Inputs
      assertThrows(classOf[IllegalArgumentException],()=>new MySqlRetainedPlanReservationCore(
        f.identity,f.worker,Thread.currentThread(),old.sourceLimitProfile,f.shared,1,None,
        dbFirstInputsInput=Some(old)))
      assertThrows(classOf[IllegalArgumentException],()=>MySqlFormat3ReadBudget.retainedDbFirstRootOnLedger(
        old,old.budget.packetBytes,f.shared,new Object(),f.worker))
      assertThrows(classOf[IllegalArgumentException],()=>MySqlFormat3ReadBudget.retainedDeploymentReceiptRootOnLedger(
        inputs,inputs.budget.packetBytes,f.shared,new Object(),f.worker))
      assertEquals(0L,f.shared.accountedBytes)
    finally f.close()

  @Test def dbFirstCapturedRequestIsOneOriginalIdentityAndSensitiveToActualNbtAndPose(): Unit =
    val f=new Fixture(3)
    val foreign=new Fixture(1)
    try
      val tag=IoOperationTag(id(1),1L,1L,id(950))
      val reservation=f.core.reserveDbFirstCapture(tag)
      val captured=f.core.captureDbFirstSnapshot(reservation)(allocation=>
        DbFirstPodSnapshotTest.captureForCore(tag.operationId,allocation,100))
      val request=f.core.dbFirstRecallRequest(reservation,captured)
      assertSame(request,f.core.dbFirstRecallRequest(reservation,captured))
      assertEquals(tag,request.tag)
      assertEquals(captured.bindingUuid(),request.bindingUuid)
      assertEquals(captured.podUuid(),request.podUuid)
      assertEquals(Option(captured.ownerUuid()),request.ownerUuid)
      assertEquals(captured.attemptUuid(),request.attemptUuid)
      assertEquals(captured.stateVersion(),request.capturedStateVersion)
      assertEquals(captured.formatVersion(),request.snapshotFormatVersion)
      assertEquals(captured.byteLength(),request.nbtByteLength)
      assertEquals(captured.sha256().toVector,request.nbtSha256)
      assertEquals(f.limits.canonicalBytes().toVector,request.profileBytes)
      assertEquals(f.limits.sha256().toVector,request.profileSha256)
      assertEquals(captured.pose(),request.pose)
      val counterfeit=new MySqlDbFirstCaptureReservation(f.core,reservation.handle,f.limits)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.dbFirstRecallRequest(counterfeit,captured))
      assertThrows(classOf[IllegalArgumentException],()=>foreign.core.dbFirstRecallRequest(reservation,captured))
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure(f.core.dbFirstRecallRequest(reservation,captured)))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.captureDbFirstSnapshot(reservation)(_=>captured))
      val matching=foreign.core.reserveDbFirstCapture(tag)
      val different=foreign.core.captureDbFirstSnapshot(matching)(allocation=>
        DbFirstPodSnapshotTest.captureForCore(tag.operationId,allocation,100,1.toByte,
          new DbFirstPodSnapshot.Pose("world",0,1,64,2,3,4)))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.dbFirstRecallRequest(reservation,different))
      val changed=foreign.core.dbFirstRecallRequest(matching,different)
      assertEquals(request.nbtByteLength,changed.nbtByteLength)
      assertNotEquals(request.nbtSha256,changed.nbtSha256)
      assertNotEquals(request.identity.sha256,changed.identity.sha256)
      val root=f.core.validationBudget(reservation.handle)
      def identity(pose: DbFirstPodSnapshot.Pose, originalTag: IoOperationTag=tag) =
        MySqlRepositoryRequestIdentity.dbFirstRecall(originalTag,request.bindingUuid,request.podUuid,request.ownerUuid,
          request.attemptUuid,request.capturedStateVersion,request.snapshotFormatVersion,request.nbtByteLength,
          request.nbtSha256,request.profileBytes,request.profileSha256,pose,root)
      val encoded=identity(request.pose)
      val expectedBytes=Vector.tabulate(encoded.canonicalLength)(encoded.canonical.borrowedBinary.byteAt)
      assertEquals(expectedBytes,request.identity.canonicalBytes)
      assertEquals(encoded.sha256,request.identity.sha256)
      assertEquals(encoded.canonicalLength,request.identity.canonicalLength)
      assertEquals(encoded.requestKind,request.identity.requestKind)
      assertEquals(request.identity.sha256,java.security.MessageDigest.getInstance("SHA-256")
        .digest(request.identity.canonicalBytes.toArray).toVector)
      assertNotEquals(request.identity.sha256,identity(new DbFirstPodSnapshot.Pose("world",0,-0.0,64,2,3,4)).sha256)
      assertNotEquals(request.identity.sha256,identity(request.pose,tag.copy(connectionGeneration=2L)).sha256)
      val detached=request.identity
      root.closeRootReadBudget()
      assertEquals(expectedBytes,detached.canonicalBytes)
      assertEquals(encoded.sha256,detached.sha256)
      assertEquals(expectedBytes.size,detached.canonicalLength)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.dbFirstRecallRequest(reservation,captured))
    finally
      f.close()
      foreign.close()

  @Test def dbFirstRequestAllocationFailureRetainsOriginalSnapshotAndRejectsRemint(): Unit =
    Vector(2048L,8000L).foreach { available =>
      val f=new Fixture(1)
      try
        val tag=IoOperationTag(id(1),1L,1L,id(951))
        val reservation=f.core.reserveDbFirstCapture(tag)
        val root=f.core.validationBudget(reservation.handle)
        val before=f.shared.accountedBytes
        var captured: DbFirstPodSnapshot=null
        var pressure=0L
        val failure=assertThrows(classOf[SQLDataException],()=>f.core.captureDbFirstSnapshot(reservation) { allocation =>
          captured=DbFirstPodSnapshotTest.captureForCore(tag.operationId,allocation,100)
          pressure=root.availableMemoryBytes-available
          root.reserveRetainedMemory(pressure,"test concurrent retained responsibility before request allocation")
          captured
        })
        assertNotNull(captured)
        assertFalse(root.isClosed)
        assertSame(failure,f.core.observation.plans.find(_.tag == tag).get.cause.get)
        root.releaseRetainedMemory(pressure,"test exact concurrent retained responsibility detached")
        if available == 2048L then assertEquals(before+captured.retainedBytes(),f.shared.accountedBytes)
        else
          assertTrue(failure.getMessage.contains("DB-first detached complete canonical vector"))
          assertTrue(f.shared.accountedBytes > before+captured.retainedBytes())
        assertThrows(classOf[IllegalArgumentException],()=>f.core.dbFirstRecallRequest(reservation,captured))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.captureDbFirstSnapshot(reservation)(_=>captured))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(tag))
      finally f.close()
    }

  @Test def initialRequestBindsOriginalCreationAndCannotUseRecallEncoding(): Unit =
    val inputs=dbFirstMaterializationInputs()
    val first=new Fixture(1,dbFirstInputs=Some(inputs))
    val second=new Fixture(1,dbFirstInputs=Some(inputs))
    try
      val tag=IoOperationTag(id(1),1L,1L,id(999))
      val one=first.core.reserveDbFirstCapture(tag,DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION)
      val two=second.core.reserveDbFirstCapture(tag,DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION)
      val a=first.core.captureDbFirstSnapshot(one)(x=>DbFirstPodSnapshotTest.captureInitialForCore(tag.operationId,x,id(1001)))
      val b=second.core.captureDbFirstSnapshot(two)(x=>DbFirstPodSnapshotTest.captureInitialForCore(tag.operationId,x,id(1002)))
      val request=first.core.dbFirstInitialActivationRequest(one,a)
      val changed=second.core.dbFirstInitialActivationRequest(two,b)
      assertEquals(request.nbtSha256,changed.nbtSha256)
      assertNotEquals(request.creationOperationUuid,changed.creationOperationUuid)
      assertNotEquals(request.identity.canonicalBytes,changed.identity.canonicalBytes)
      assertNotEquals(request.identity.sha256,changed.identity.sha256)
      assertThrows(classOf[IllegalArgumentException],()=>first.core.dbFirstInitialActivationRequest(one,b))
      assertThrows(classOf[IllegalArgumentException],()=>second.core.dbFirstInitialActivationRequest(one,a))
      assertInstanceOf(classOf[IllegalArgumentException],first.onWorkerFailure(first.core.dbFirstInitialActivationRequest(one,a)))
      val root=first.core.validationBudget(one.handle)
      val recall=MySqlRepositoryRequestIdentity.dbFirstRecall(tag,request.bindingUuid,request.podUuid,None,
        request.attemptUuid,request.capturedStateVersion,request.snapshotFormatVersion,request.nbtByteLength,
        request.nbtSha256,request.profileBytes,request.profileSha256,request.pose,root)
      assertNotEquals(recall.sha256,request.identity.sha256)
      root.closeRootReadBudget()
      assertEquals(id(1001),request.creationOperationUuid)
      assertThrows(classOf[IllegalArgumentException],()=>first.core.dbFirstInitialActivationRequest(one,a))
    finally
      first.close();second.close()

  @Test def initialCaptureUsesOriginalSchema6LedgerWithoutRecallAuthorityOrRemint(): Unit =
    val f=new Fixture(3,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    val legacy=new Fixture(1)
    try
      val tag=IoOperationTag(id(1),1L,1L,id(996))
      val legacyBefore=legacy.shared.accountedBytes
      assertThrows(classOf[IllegalArgumentException],()=>legacy.core.reserveDbFirstCapture(tag,DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION))
      assertEquals(legacyBefore,legacy.shared.accountedBytes)
      val reservation=f.core.reserveDbFirstCapture(tag,DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION)
      val before=f.shared.accountedBytes
      val snapshot=f.core.captureDbFirstSnapshot(reservation)(a=>DbFirstPodSnapshotTest.captureInitialForCore(tag.operationId,a))
      assertTrue(f.shared.accountedBytes>before+snapshot.retainedBytes(), "complete request/backing ownership must also be charged")
      val charged=f.shared.accountedBytes
      val request=f.core.dbFirstInitialActivationRequest(reservation,snapshot)
      assertSame(request,f.core.dbFirstInitialActivationRequest(reservation,snapshot))
      assertEquals(charged,f.shared.accountedBytes,"request reads cannot allocate or remint")
      assertEquals(snapshot.initialCreationOperationUuid(),request.creationOperationUuid)
      assertEquals(snapshot.sha256().toVector,request.nbtSha256)
      assertEquals(snapshot.pose(),request.pose)
      assertEquals(snapshot.attemptUuid(),request.attemptUuid)
      assertEquals(0L,request.capturedStateVersion)
      assertTrue(request.ownerUuid.isEmpty)
      assertEquals(java.security.MessageDigest.getInstance("SHA-256").digest(request.identity.canonicalBytes.toArray).toVector,
        request.identity.sha256)
      assertEquals(DbFirstPodSnapshot.Purpose.INITIAL_ACTIVATION,reservation.purpose)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.dbFirstRecallRequest(reservation,snapshot))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireDbFirstRecallSubmission(reservation))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.captureDbFirstSnapshot(reservation)(_=>snapshot))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(tag))
      val wrong=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(997)))
      val beforeWrong=f.shared.accountedBytes
      assertThrows(classOf[IllegalArgumentException],()=>f.core.captureDbFirstSnapshot(wrong)(a=>
        DbFirstPodSnapshotTest.captureInitialForCore(wrong.tag.operationId,a)))
      assertEquals(beforeWrong,f.shared.accountedBytes)
      val healthy=f.core.reserveDbFirstCapture(IoOperationTag(id(1),1L,1L,id(998)))
      val recalled=f.core.captureDbFirstSnapshot(healthy)(a=>DbFirstPodSnapshotTest.captureForCore(healthy.tag.operationId,a,100))
      assertNotNull(f.core.dbFirstRecallRequest(healthy,recalled))
    finally
      f.close();legacy.close()

  @Test def dbFirstCaptureUsesOriginalSharedLedgerAndFailureDoesNotFreezeHealthyEntry(): Unit =
    val f = new Fixture(5)
    val foreign = new Fixture(1)
    try
      def tag(n: Long): IoOperationTag = IoOperationTag(id(1), 1L, 1L, id(n))
      val first = f.core.reserveDbFirstCapture(tag(900))
      val second = f.core.reserveDbFirstCapture(tag(901))
      f.core.requireDbFirstCaptureReservation(first)
      var held: DbFirstCaptureAllocation = null
      val snapshot = f.core.captureDbFirstSnapshot(first) { allocation =>
        held = allocation
        DbFirstPodSnapshotTest.captureForCore(first.tag.operationId, allocation, 40000)
      }
      assertEquals(2, f.core.retainedRoots)
      assertTrue(snapshot.byteLength() > 40000)
      assertSame(f.limits, snapshot.limitProfile())
      val beforeSecond = f.shared.accountedBytes
      val exceeded = assertThrows(classOf[java.io.IOException], () => f.core.captureDbFirstSnapshot(second) { allocation =>
        DbFirstPodSnapshotTest.captureForCore(second.tag.operationId, allocation, 60000)
      })
      assertInstanceOf(classOf[java.sql.SQLDataException], exceeded.getCause)
      assertEquals(beforeSecond, f.shared.accountedBytes)
      assertTrue(f.core.observation.failure.isEmpty)
      assertTrue(f.core.observation.plans.find(_.tag == second.tag).get.cause.nonEmpty)
      assertSame(exceeded, f.core.observation.plans.find(_.tag == second.tag).get.cause.get)
      assertEquals(0.toByte, snapshot.byteAt(snapshot.byteLength() - 1))
      assertThrows(classOf[IllegalArgumentException], () => held.reserveTemporary(1L, "expired capture"))
      assertThrows(classOf[IllegalArgumentException], () => f.core.captureDbFirstSnapshot(first)(_ => snapshot))
      assertThrows(classOf[IllegalArgumentException], () => f.core.reserveDbFirstCapture(first.tag))
      assertThrows(classOf[IllegalArgumentException], () => foreign.core.requireDbFirstCaptureReservation(first))
      assertInstanceOf(classOf[IllegalArgumentException], f.onWorkerFailure(f.core.requireDbFirstCaptureReservation(first)))
      val failed = f.core.reserveDbFirstCapture(tag(902))
      val beforeFailure = f.shared.accountedBytes
      val cause = new java.io.IOException("actual capture callback failure")
      val observed = assertThrows(classOf[java.io.IOException], () => f.core.captureDbFirstSnapshot(failed) { allocation =>
        allocation.reserveTemporary(1024L, "actual callback workspace")
        allocation.reserveRetained(64L, "untransferred callback metadata")
        throw cause
      })
      assertSame(cause, observed)
      assertSame(cause, f.core.observation.plans.find(_.tag == failed.tag).get.cause.get)
      assertEquals(beforeFailure, f.shared.accountedBytes)
      assertThrows(classOf[IllegalArgumentException], () => f.core.requireDbFirstCaptureReservation(first))
      assertThrows(classOf[IllegalArgumentException], () => f.core.requireDbFirstCaptureReservation(second))
      val third = f.core.reserveDbFirstCapture(tag(903))
      val thirdSnapshot = f.core.captureDbFirstSnapshot(third)(allocation =>
        DbFirstPodSnapshotTest.captureForCore(third.tag.operationId, allocation, 100))
      assertTrue(thirdSnapshot.byteLength() > 100)
      val escaping = f.core.reserveDbFirstCapture(tag(904))
      val beforeEscaping = f.shared.accountedBytes
      var escaped: DbFirstPodSnapshot = null
      assertSame(cause, assertThrows(classOf[java.io.IOException], () => f.core.captureDbFirstSnapshot(escaping) { allocation =>
        escaped = DbFirstPodSnapshotTest.captureForCore(escaping.tag.operationId, allocation, 100)
        throw cause
      }))
      assertEquals(beforeEscaping + escaped.retainedBytes(), f.shared.accountedBytes)
      val unused = foreign.core.reserveDbFirstCapture(tag(910))
      foreign.core.stop()
      assertThrows(classOf[IllegalArgumentException], () => foreign.core.requireDbFirstCaptureReservation(unused))
      f.core.stop()
      assertThrows(classOf[IllegalStateException], () => f.onWorker(f.core.requireStoppedEmptyForTerminal()))
    finally
      f.close(); foreign.close()

  @Test def full26RegistryRejectsDifferentPlanChildEvenOnSameLedger():Unit =
    val inputs=MySqlScriptSchemaDataTestFixture.Full26Inputs
    val f=new Fixture(2,Some(inputs))
    var registry:MySqlModeProjectionBudgetRegistry=null
    try
      val first=f.reserve(810)
      val second=f.reserve(811)
      val firstRoot=f.core.validationBudget(first.originalRetainedHandle.get)
      val secondRoot=f.core.validationBudget(second.originalRetainedHandle.get)
      f.onWorker {
        f.core.beginExecution(first);f.core.beginExecution(second)
        val child=secondRoot.issueDeploymentReceiptSnapshotImage(inputs,inputs.schema,
          MySqlScriptSchemaDataTestFixture.Connection,
          MySqlFormat3ObservationScope(f.identity.bootId,f.identity.bootGeneration,f.identity.connectionGeneration),inputs.budget.packetBytes)
        registry=new MySqlModeProjectionBudgetRegistry(Thread.currentThread(),f.identity.bootId,
          f.identity.bootGeneration,f.identity.connectionGeneration)
        registry.retainRoot(first.tag,firstRoot)
        val retained=f.shared.accountedBytes
        assertThrows(classOf[IllegalArgumentException],()=>registry.retainScoped(first.tag,child))
        assertFalse(child.isClosed)
        assertFalse(secondRoot.isClosed)
        assertEquals(retained,f.shared.accountedBytes)
        assertThrows(classOf[IllegalStateException],()=>secondRoot.closeRootReadBudget())
        child.closeScopedReadBudget()
      }
      f.core.submitted(first,IoSubmission.Accepted);f.core.submitted(second,IoSubmission.Accepted)
      val firstDrop=f.core.recordMainDrop(first.originalRetainedHandle.get)
      val secondDrop=f.core.recordMainDrop(second.originalRetainedHandle.get)
      f.onWorker {
        f.core.dropWorkerPlanReferences(firstDrop);registry.release(first.tag);f.core.releaseDropped(firstDrop)
        f.core.dropWorkerPlanReferences(secondDrop);f.core.releaseDropped(secondDrop)
      }
      assertEquals(0L,f.shared.accountedBytes)
    finally f.close()

  @Test def originalProjectionDropFailureStopsChildAndRootCloseAndPreservesCause():Unit =
    val attempt=new MySqlModeProjectionReleaseAttempt(Thread.currentThread())
    val primary=new IllegalStateException("actual original drop failure")
    val stack=primary.getStackTrace
    var childClosed=false
    var rootClosed=false
    val actual=assertThrows(classOf[IllegalStateException],()=>attempt.run(
      ()=>throw primary,()=>{childClosed=true},()=>{rootClosed=true}))
    assertSame(primary,actual)
    assertSame(primary,attempt.retainedFailure.get)
    assertTrue(stack.sameElements(actual.getStackTrace))
    assertFalse(childClosed);assertFalse(rootClosed)
    val repeated=assertThrows(classOf[IllegalStateException],()=>attempt.run(()=>(),()=>(),()=>()))
    assertSame(primary,repeated.getCause)

  @Test def full26ProjectionRegistryOwnsTransferredChildUntilOriginalPlanDrop():Unit =
    val inputs=MySqlScriptSchemaDataTestFixture.Full26Inputs
    val f=new Fixture(1,Some(inputs))
    var registry:MySqlModeProjectionBudgetRegistry=null
    var child:MySqlFormat3ReadBudget=null
    try
      val plan=f.reserve(10)
      val handle=plan.originalRetainedHandle.get
      val root=f.core.validationBudget(handle)
      val observation=MySqlFormat3ObservationScope(f.identity.bootId,f.identity.bootGeneration,f.identity.connectionGeneration)
      f.onWorker {
        f.core.beginExecution(plan)
        child=root.issueDeploymentReceiptSnapshotImage(inputs,inputs.schema,
          MySqlScriptSchemaDataTestFixture.Connection,observation,inputs.budget.packetBytes)
        MySqlScriptSchemaSnapshot.captureMaterialized(inputs.schema,MySqlScriptSchemaDataTestFixture.Connection,
          observation,inputs.budget.packetBytes,inputs,
          MySqlScriptSchemaDataTestFixture.full26GenesisRelations(inputs,catalogPresent=true),child)
        registry=new MySqlModeProjectionBudgetRegistry(Thread.currentThread(),f.identity.bootId,
          f.identity.bootGeneration,f.identity.connectionGeneration)
        registry.retainRoot(plan.tag,root)
        registry.retainScoped(plan.tag,child)
        val retained=f.shared.accountedBytes
        assertThrows(classOf[IllegalStateException],()=>root.closeRootReadBudget())
        assertEquals(retained,f.shared.accountedBytes)
        assertFalse(child.isClosed)
      }
      f.core.submitted(plan,IoSubmission.Accepted)
      val receipt=f.core.recordMainDrop(handle)
      f.onWorker {
        f.core.dropWorkerPlanReferences(receipt)
        registry.release(plan.tag)
        assertTrue(child.isClosed)
        assertTrue(root.isClosed)
        f.core.releaseDropped(receipt)
      }
      assertEquals(0L,f.shared.accountedBytes)
      assertEquals(0,f.core.retainedRoots)
    finally f.close()

  @Test def full26ProjectionReleaseKeepsFailureAndFeesWhenOriginalChildWasNotTransferred():Unit =
    val inputs=MySqlScriptSchemaDataTestFixture.Full26Inputs
    val f=new Fixture(1,Some(inputs))
    try
      val plan=f.reserve(10)
      val root=f.core.validationBudget(plan.originalRetainedHandle.get)
      f.onWorker {
        f.core.beginExecution(plan)
        val child=root.issueDeploymentReceiptSnapshotImage(inputs,inputs.schema,
          MySqlScriptSchemaDataTestFixture.Connection,
          MySqlFormat3ObservationScope(f.identity.bootId,f.identity.bootGeneration,f.identity.connectionGeneration),inputs.budget.packetBytes)
        val registry=new MySqlModeProjectionBudgetRegistry(Thread.currentThread(),f.identity.bootId,
          f.identity.bootGeneration,f.identity.connectionGeneration)
        registry.retainRoot(plan.tag,root)
        val retained=f.shared.accountedBytes
        val cause=assertThrows(classOf[IllegalStateException],()=>registry.release(plan.tag))
        assertSame(cause,registry.retainedReleaseFailure(plan.tag).get)
        assertFalse(root.isClosed)
        assertFalse(child.isClosed)
        assertEquals(retained,f.shared.accountedBytes)
        val repeated=assertThrows(classOf[IllegalStateException],()=>registry.release(plan.tag))
        assertSame(cause,repeated.getCause)
        assertTrue(registry.contains(plan.tag))
      }
    finally f.close()

  @Test def full26CoreCapturesExactInputsBeforeMainReserveAndHandsImagesToOriginalWorker():Unit =
    val inputs=MySqlScriptSchemaDataTestFixture.Full26Inputs
    val f=new Fixture(1,Some(inputs))
    try
      val plan=f.reserve(10)
      val handle=plan.originalRetainedHandle.get
      val root=f.core.validationBudget(handle)
      root.requireScriptDataInputs(inputs)
      val scope=MySqlFormat3ObservationScope(f.identity.bootId,f.identity.bootGeneration,f.identity.connectionGeneration)
      def image()=root.issueDeploymentReceiptSnapshotImage(inputs,inputs.schema,
        MySqlScriptSchemaDataTestFixture.Connection,scope,inputs.budget.packetBytes)
      assertThrows(classOf[SQLNonTransientConnectionException],()=>image())
      f.onWorker {
        assertSame(root,f.core.beginExecution(plan))
        root.bindInitialMeasuredPacketLimit(inputs.budget.packetBytes)
        root.initialize(inputs.schema,MySqlScriptSchemaDataTestFixture.Connection,scope,inputs.budget.packetBytes)
        val child=image()
        try
          val snapshot=MySqlScriptSchemaSnapshot.captureMaterialized(inputs.schema,
            MySqlScriptSchemaDataTestFixture.Connection,scope,inputs.budget.packetBytes,inputs,
            MySqlScriptSchemaDataTestFixture.full26GenesisRelations(inputs,catalogPresent=true),child)
          assertSame(inputs,snapshot.inputs)
          assertEquals(26,snapshot.relations.size)
          assertTrue(child.isSnapshotReadChildOf(root))
          assertTrue(child.sharesAllocationLedger(root))
          assertThrows(classOf[IllegalStateException],()=>root.closeRootReadBudget())
          assertFalse(root.isClosed)
        finally child.closeScopedReadBudget()
        assertThrows(classOf[IllegalArgumentException],()=>f.core.beginExecution(plan))
      }
      f.core.submitted(plan,IoSubmission.Accepted)
      assertThrows(classOf[SQLNonTransientConnectionException],()=>image())
      val drop=f.core.recordMainDrop(handle)
      f.onWorker { f.core.releaseDropped(drop) }
      assertTrue(root.isClosed)
      assertEquals(0L,f.shared.accountedBytes)
      assertEquals(0,f.core.retainedRoots)
    finally f.close()

  @Test def freshSealRequiresOriginalMainAndClosesOnlyTheUnusedIssuer(): Unit =
    val f = new Fixture(1)
    try
      assertTrue(f.onWorkerFailure { f.core.sealUnusedForFreshCompletion() }.isInstanceOf[IllegalArgumentException])
      assertTrue(f.onWorkerFailure { f.core.requireFreshCompletionSeal() }.isInstanceOf[IllegalArgumentException])
      f.core.sealUnusedForFreshCompletion()
      f.onWorker { f.core.requireFreshCompletionSeal() }
      assertThrows(classOf[IllegalArgumentException], () => f.core.requireFreshCompletionSeal())
      assertThrows(classOf[IllegalArgumentException], () => f.core.sealUnusedForFreshCompletion())
      assertThrows(classOf[IllegalArgumentException], () => f.reserve(10))
      assertEquals(0L, f.shared.accountedBytes)
    finally f.close()

  @Test def freshSealRejectsUnsubmittedPlanAndDoesNotEraseItsRootOrHistory(): Unit =
    val f = new Fixture(1)
    try
      var plan = Option(f.reserve(10))
      val handle = plan.get.originalRetainedHandle.get
      val root = f.core.validationBudget(handle)
      val bytes = f.shared.accountedBytes
      assertThrows(classOf[IllegalArgumentException], () => f.core.sealUnusedForFreshCompletion())
      assertFalse(root.isClosed)
      assertEquals(bytes, f.shared.accountedBytes)
      assertEquals(1, f.core.retainedRoots)
      f.core.submitted(plan.get, IoSubmission.RejectedQuiescing)
      plan = None
      f.core.cancelExplicitlyRejected(handle)
      assertTrue(root.isClosed)
      assertEquals(0, f.core.retainedRoots)
      // Empty current entries cannot erase the fact that this issuer produced a business plan.
      assertThrows(classOf[IllegalArgumentException], () => f.core.sealUnusedForFreshCompletion())
      assertTrue(f.onWorkerFailure { f.core.requireFreshCompletionSeal() }.isInstanceOf[IllegalArgumentException])
    finally f.close()

  @Test def freshSealRetainsOriginalFailureAndPlainStopCannotForgeASeal(): Unit =
    val f = new Fixture(1)
    val stopped = new Fixture(1)
    try
      val cause = new IllegalStateException("original retained failure")
      f.core.freeze(cause)
      assertSame(cause, assertThrows(classOf[IllegalStateException], () => f.core.sealUnusedForFreshCompletion()))
      assertSame(cause, f.onWorkerFailure { f.core.requireFreshCompletionSeal() })
      stopped.core.stop()
      assertTrue(stopped.onWorkerFailure { stopped.core.requireFreshCompletionSeal() }.isInstanceOf[IllegalArgumentException])
      assertThrows(classOf[IllegalArgumentException], () => stopped.core.sealUnusedForFreshCompletion())
    finally
      try f.close()
      finally stopped.close()

  @Test def originalWorkerHandoffIssuesSuccessiveImagesAndSettlesThroughActualDropAck(): Unit =
    val f = new Fixture(1)
    try
      val plan = f.reserve(10)
      val handle = plan.originalRetainedHandle.get
      val root = f.core.validationBudget(handle)
      def image() = root.issueOwnedSnapshotImage(MySqlFull20CodecFixture.schema,
        MySqlFull20CodecFixture.observation, MySqlFull20CodecFixture.scope, 65536L)
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => image())
      f.onWorker {
        // Actual worker may enter before Main observes the Accepted submit return.
        assertSame(root, f.core.beginExecution(plan))
        root.bindInitialMeasuredPacketLimit(65536L)
        root.initialize(MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation,
          MySqlFull20CodecFixture.scope, 65536L)
        val first = image()
        val second = image()
        assertNotSame(first, second)
        assertTrue(first.isSnapshotReadChildOf(root))
        assertTrue(second.sharesAllocationLedger(root))
        first.closeScopedReadBudget()
        second.closeScopedReadBudget()
        assertThrows(classOf[IllegalArgumentException], () => f.core.beginExecution(plan))
      }
      f.core.submitted(plan, IoSubmission.Accepted)
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => image())
      val foreignThreadFailure = new AtomicReference[Throwable]()
      val foreignThread = new Thread(() =>
        try image()
        catch case cause: Throwable => foreignThreadFailure.set(cause)
        (), "foreign-image-owner")
      foreignThread.start(); foreignThread.join(3000L)
      assertFalse(foreignThread.isAlive)
      assertTrue(foreignThreadFailure.get().isInstanceOf[java.sql.SQLNonTransientConnectionException])
      val receipt = f.core.recordMainDrop(handle)
      f.onWorker { f.core.releaseDropped(receipt) }
      assertTrue(root.isClosed)
      assertEquals(0L, f.shared.accountedBytes)
      assertEquals(0, f.core.retainedRoots)
    finally f.close()

  @Test def handoffRejectsForeignInitializedAndClosedOriginalRoots(): Unit =
    val f = new Fixture(3)
    val other = new Fixture(1)
    try
      val foreign = other.reserve(40)
      assertTrue(f.onWorkerFailure { f.core.beginExecution(foreign); () }.isInstanceOf[IllegalArgumentException])
      val initialized = f.reserve(10)
      val root = f.core.validationBudget(initialized.originalRetainedHandle.get)
      root.initialize(MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation,
        MySqlFull20CodecFixture.scope, 65536L)
      assertTrue(f.onWorkerFailure { f.core.beginExecution(initialized); () }
        .isInstanceOf[java.sql.SQLNonTransientConnectionException])
      assertFalse(f.core.observation.plans.find(_.tag == initialized.tag).get.workerObserved)
      val closed = f.reserve(20)
      f.core.validationBudget(closed.originalRetainedHandle.get).closeRootReadBudget()
      assertTrue(f.onWorkerFailure { f.core.beginExecution(closed); () }.isInstanceOf[IllegalStateException])
      assertFalse(f.core.observation.plans.find(_.tag == closed.tag).get.workerObserved)
      root.closeRootReadBudget()
      other.core.validationBudget(foreign.originalRetainedHandle.get).closeRootReadBudget()
    finally
      try f.close()
      finally other.close()

  @Test def explicitRejectedDropCancelsOnceAndFullCapacityCanReserveAgain(): Unit =
    val f = new Fixture(1)
    var primary: Throwable = null
    try
      var plan = Option(f.reserve(10))
      val handle = plan.get.originalRetainedHandle.get
      assertTrue(f.shared.accountedBytes > 0L)
      assertThrows(classOf[IllegalArgumentException], () => f.reserve(20))
      f.core.submitted(plan.get, IoSubmission.RejectedCapacity)
      plan = None // 测试真实drop自有canonical plan引用；不是GC或SQL ACK。
      f.core.cancelExplicitlyRejected(handle)
      assertEquals(0L, f.shared.accountedBytes)
      assertEquals(0, f.core.retainedRoots)
      assertThrows(classOf[IllegalStateException], () => f.core.cancelExplicitlyRejected(handle))
      var next = Option(f.reserve(20))
      val nextHandle = next.get.originalRetainedHandle.get
      f.core.submitted(next.get, IoSubmission.RejectedQuiescing)
      next = None
      f.core.cancelExplicitlyRejected(nextHandle)
      assertEquals(0L, f.shared.accountedBytes)
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally closePreserving(f, primary)

  @Test def unsubmittedForeignWrongThreadAndAcceptedCannotUseRejectedCancel(): Unit =
    val f = new Fixture(2)
    val other = new Fixture(1)
    var primary: Throwable = null
    try
      val plan = f.reserve(10)
      val handle = plan.originalRetainedHandle.get
      assertThrows(classOf[IllegalArgumentException], () => f.core.cancelExplicitlyRejected(handle))
      assertThrows(classOf[IllegalArgumentException], () => other.core.cancelExplicitlyRejected(handle))
      f.core.submitted(plan, IoSubmission.RejectedCapacity)
      val wrongThread = f.onWorkerFailure { f.core.cancelExplicitlyRejected(handle) }
      assertTrue(wrongThread.isInstanceOf[IllegalArgumentException])
      f.core.submitted(plan, IoSubmission.Accepted)
      f.onWorker { f.core.beginExecution(plan); () }
      assertThrows(classOf[IllegalArgumentException], () => f.core.cancelExplicitlyRejected(handle))
      assertTrue(f.shared.accountedBytes > 0L)
      assertEquals(1, f.core.retainedRoots)
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally
      var cleanupPrimary = primary
      try closePreserving(f, cleanupPrimary)
      catch
        case cause: Throwable =>
          cleanupPrimary = cause
          throw cause
      finally closePreserving(other, cleanupPrimary)

  @Test def actualRootCloseFailureFreezesOriginalResponsibilityAndPreservesCauseIdentity(): Unit =
    val f = new Fixture(1)
    var primary: Throwable = null
    try
      var plan = Option(f.reserve(10))
      val handle = plan.get.originalRetainedHandle.get
      val root = f.core.validationBudget(handle)
      val observation = MySqlFormat3ConnectionObservation(id(501), "9.7.2", "fp_test", "test@127.0.0.1",
        "InnoDB", 1, 1L, "READ-COMMITTED", "REPEATABLE-READ", "STRICT_ALL_TABLES", "+00:00", "TLSv1.3", "TLS_AES_256_GCM_SHA384")
      root.initialize("fp_test", observation, MySqlFormat3ObservationScope(id(1), 1L, 1L), 65536L)
      root.beginRow(1, "controlled_row", 1L)
      f.core.submitted(plan.get, IoSubmission.RejectedCapacity)
      plan = None
      val original = assertThrows(classOf[IllegalStateException], () => f.core.cancelExplicitlyRejected(handle))
      val stack = original.getStackTrace
      assertSame(original, assertThrows(classOf[IllegalStateException], () => f.core.cancelExplicitlyRejected(handle)))
      assertTrue(stack.sameElements(original.getStackTrace))
      assertEquals(1, f.core.retainedRoots)
      assertFalse(root.isClosed)
      assertTrue(f.shared.accountedBytes > 0L)
      f.core.stop()
      assertSame(original, f.onWorkerFailure { f.core.retireSettledMemoryGraph() })
      root.endRow("controlled_row", 1L) // 测试资源关闭；core frozen责任不被自动清除。
      root.closeRootReadBudget()
      assertEquals(1, f.core.retainedRoots)
      assertSame(original, assertThrows(classOf[IllegalStateException], () => f.reserve(20)))
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally closePreserving(f, primary)

  @Test def workerUnknownPreservesOriginalPlanRootAndRefusesMainDropOrNewReserve(): Unit =
    val f = new Fixture(2)
    var primary: Throwable = null
    try
      val plan = f.reserve(10)
      val original = new IllegalStateException("controlled worker uncertainty")
      f.core.submitted(plan, IoSubmission.Accepted)
      f.onWorker {
        f.core.beginExecution(plan)
        f.core.markUnknown(plan, original)
      }
      assertThrows(classOf[IllegalArgumentException], () => f.core.recordMainDrop(plan.originalRetainedHandle.get))
      assertSame(original, assertThrows(classOf[IllegalStateException], () => f.reserve(20)))
      assertEquals(1, f.core.retainedRoots)
      assertTrue(f.shared.accountedBytes > 0L)
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally closePreserving(f, primary)

  @Test def acceptedDropReceiptSettlesOnlyExactWorkerOnceAndOldMainLosesRightsAfterTransfer(): Unit =
    val f = new Fixture(1)
    var primary: Throwable = null
    try
      var plan = Option(f.reserve(10))
      val handle = plan.get.originalRetainedHandle.get
      f.core.submitted(plan.get, IoSubmission.Accepted)
      f.onWorker { f.core.beginExecution(plan.get); () }
      plan = None
      val receipt = f.core.recordMainDrop(handle)
      assertThrows(classOf[IllegalArgumentException], () => f.core.releaseDropped(receipt))
      f.onWorker { f.core.releaseDropped(receipt) }
      assertEquals(0L, f.shared.accountedBytes)
      assertEquals(0, f.core.retainedRoots)
      assertTrue(f.onWorkerFailure { f.core.releaseDropped(receipt) }.isInstanceOf[IllegalStateException])
      f.core.suspendMain()
      assertThrows(classOf[IllegalArgumentException], () => f.reserve(20))
      val result = new AtomicReference[Throwable]()
      val target = new Thread(() =>
        try
          f.core.claimMain(Thread.currentThread())
          var next = Option(f.reserve(20))
          val nextHandle = next.get.originalRetainedHandle.get
          f.core.submitted(next.get, IoSubmission.RejectedCapacity)
          next = None
          f.core.cancelExplicitlyRejected(nextHandle)
        catch case cause: Throwable => result.set(cause)
        (), "retained-plan-target-main")
      target.start()
      try
        target.join(4000L)
        assertFalse(target.isAlive, "target actor did not terminate")
        if result.get() != null then throw result.get()
        assertEquals(0, f.core.retainedRoots)
        assertThrows(classOf[IllegalArgumentException], () => f.reserve(30))
      finally
        if target.isAlive then target.interrupt()
        target.join(4000L)
        assertFalse(target.isAlive, "target actor cleanup failed")
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally closePreserving(f, primary)

  @Test def encodeFailureRetainsActualTagCauseAndRootSlotWithoutFakePlanOrCancel(): Unit =
    val limits = SourceLimitProfile.of(1024L, 1024L, 1024L, 8L, 1L, 128L, 1024L, 1024L)
    val shared = new MySqlFormat3AllocationLedger(limits.maxRetainedBytes())
    val neverStartedWorker = new Thread(() => (), "unstarted-pure-budget-worker")
    val core = new MySqlRetainedPlanReservationCore(MySqlOwnedIoIdentity(id(1), 1L, 1L),
      neverStartedWorker, Thread.currentThread(), limits, shared, 1, None)
    val tag = IoOperationTag(id(1), 1L, 1L, id(10))
    val original = assertThrows(classOf[java.sql.SQLDataException], () =>
      core.reserveSingle(tag, id(110), id(210), MySqlRepositoryOperation.FindAll))
    val observation = core.observation
    assertEquals(1, observation.plans.size)
    assertEquals(tag, observation.plans.head.tag)
    assertSame(original, observation.plans.head.cause.get)
    assertFalse(observation.plans.head.accepted)
    assertFalse(observation.plans.head.workerObserved)
    assertThrows(classOf[IllegalArgumentException], () =>
      core.reserveSingle(tag, id(111), id(211), MySqlRepositoryOperation.FindAll))
    assertSame(original, core.observation.plans.head.cause.get)

  @Test def unsettledStopOriginalCausePreventsLifecycleReleaseWhileSettledLifecycleRemainsEligible(): Unit =
    val f = new Fixture(1)
    var primary: Throwable = null
    try
      val plan = f.reserve(10)
      f.core.submitted(plan, IoSubmission.Accepted)
      f.onWorker { f.core.beginExecution(plan); () }
      val original = new IllegalStateException("actual retained Main stop responsibility")
      val stack = original.getStackTrace
      f.core.freeze(original)
      f.onWorker {
        val lifecycle = new MySqlOwnedSessionLifecycle(Thread.currentThread())
        lifecycle.beginOpen()
        lifecycle.admit() // 纯actor状态，不是JDBC/SQL admission资格。
        f.core.observation.failure.foreach(lifecycle.markFailed)
        lifecycle.beginClose(None)
        assertFalse(lifecycle.releaseEligible)
        assertSame(original, lifecycle.failure.get)
        assertTrue(stack.sameElements(lifecycle.failure.get.getStackTrace))
        lifecycle.finishClose()
        val settled = new MySqlOwnedSessionLifecycle(Thread.currentThread())
        settled.beginOpen()
        settled.admit()
        settled.beginClose(None)
        assertTrue(settled.releaseEligible)
        settled.finishClose()
      }
      assertEquals(1, f.core.retainedRoots)
      assertTrue(f.shared.accountedBytes > 0L)
      assertSame(original, f.core.observation.failure.get)
    catch
      case cause: Throwable =>
        primary = cause
        throw cause
    finally closePreserving(f, primary)

  @Test def settledEverIssuedMemoryRetirementRejectsEveryOldAliasEntry(): Unit =
    val f = new Fixture(1)
    try
      val plan = f.reserve(10)
      val handle = plan.originalRetainedHandle.get
      val root = f.core.validationBudget(handle)
      f.core.submitted(plan, IoSubmission.Accepted)
      f.onWorker { f.core.beginExecution(plan); () }
      val receipt = f.core.recordMainDrop(handle)
      f.onWorker {
        f.core.dropWorkerPlanReferences(receipt)
        f.core.releaseDropped(receipt)
      }
      assertTrue(root.isClosed)
      assertEquals(0L, f.shared.accountedBytes)
      assertTrue(f.onWorkerFailure { f.core.retireSettledMemoryGraph() }.isInstanceOf[IllegalArgumentException])
      f.core.stop()
      assertThrows(classOf[IllegalArgumentException], () => f.core.retireSettledMemoryGraph())
      f.onWorker { f.core.retireSettledMemoryGraph() }
      assertSame(f.core, handle.reservationCore)
      assertSame(handle, receipt.handle)
      val cause = new IllegalStateException("must not replace retirement")
      // 真实旧别名逐入口失能；null参数故意证明退休检查先于参数及旧handle解引用。
      val calls = Vector[() => Unit](
        () => { f.core.reserveSingle(null, null, null, null); () },
        () => { f.core.reserveTransaction(null, null, null, null); () },
        () => f.core.requireSingle(plan, null),
        () => f.core.requireTransaction(plan, null),
        () => f.core.submitted(plan, null),
        () => { f.core.beginExecution(plan); () },
        () => f.core.markUnknown(plan, cause),
        () => { f.core.recordMainDrop(handle); () },
        () => f.core.cancelExplicitlyRejected(handle),
        () => f.core.requireDropReceipt(receipt),
        () => f.core.requireReleaseKind(receipt, MySqlRetainedPlanReleaseKind.Ordinary),
        () => f.core.dropWorkerPlanReferences(receipt),
        () => f.core.markReleaseFailed(receipt, cause),
        () => f.core.releaseDropped(receipt),
        () => f.core.suspendMain(),
        () => f.core.claimMain(Thread.currentThread()),
        () => f.core.stop(),
        () => f.core.sealUnusedForFreshCompletion(),
        () => f.core.requireFreshCompletionSeal(),
        () => f.core.freeze(cause),
        () => { f.core.rootForOriginal(plan); () },
        () => { f.core.validationBudget(handle); () },
        () => { f.core.retainedRoots; () },
        () => { f.core.accountedBytes; () },
        () => { f.core.observation; () },
        () => f.core.retireSettledMemoryGraph())
      calls.foreach { call =>
        val rejected = assertThrows(classOf[IllegalStateException], () => call())
        assertEquals("retained core memory graph was retired", rejected.getMessage)
        val workerRejected = f.onWorkerFailure { call() }
        assertEquals("retained core memory graph was retired", workerRejected.getMessage)
      }
      assertEquals(0L, f.shared.accountedBytes)
    finally f.close()

  @Test def memoryRetirementPreservesPendingRootAndOriginalFrozenCause(): Unit =
    val pending = new Fixture(1)
    val frozen = new Fixture(1)
    try
      val plan = pending.reserve(10)
      val root = pending.core.validationBudget(plan.originalRetainedHandle.get)
      val bytes = pending.shared.accountedBytes
      pending.core.stop()
      assertTrue(pending.onWorkerFailure { pending.core.retireSettledMemoryGraph() }.isInstanceOf[IllegalArgumentException])
      assertFalse(root.isClosed)
      assertEquals(bytes, pending.shared.accountedBytes)
      assertEquals(1, pending.core.retainedRoots)
      val cause = new IllegalStateException("original memory retirement failure")
      val stack = cause.getStackTrace
      frozen.core.freeze(cause)
      frozen.core.stop()
      assertSame(cause, frozen.onWorkerFailure { frozen.core.retireSettledMemoryGraph() })
      assertSame(cause, frozen.core.observation.failure.get)
      assertTrue(stack.sameElements(cause.getStackTrace))
    finally
      try pending.close()
      finally frozen.close()

  @Test def originalFreshSealCanRetireOnlyItsSettledMemoryGraph(): Unit =
    val f = new Fixture(1)
    try
      f.core.sealUnusedForFreshCompletion()
      f.onWorker {
        f.core.requireFreshCompletionSeal()
        f.core.retireSettledMemoryGraph()
      }
      assertEquals(0L, f.shared.accountedBytes)
      assertThrows(classOf[IllegalStateException], () => f.core.observation)
    finally f.close()

  private def closePreserving(f: Fixture, primary: Throwable): Unit =
    try f.close()
    catch
      case cleanup: Throwable =>
        if primary == null then throw cleanup
        else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  @Test def deploymentOriginalSingleRootUsesSharedLedgerAndActualDropAck(): Unit =
    val f = new Fixture(1)
    try
      val operation = MySqlRepositoryOperation.FindOriginalDeploymentOperation(id(71), id(72))
      val plan = f.core.reserveSingle(IoOperationTag(id(1), 1L, 1L, id(10)), id(110), id(210), operation)
      f.core.requireSingle(plan, operation)
      val handle = plan.originalRetainedHandle.get
      val root = f.core.validationBudget(handle)
      f.onWorker {
        assertSame(root, f.core.beginExecution(plan))
        root.initializeDeploymentOriginalRead(MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation,
          MySqlFull20CodecFixture.scope, 65536L)
        root.requireLedgerIdentity(f.shared)
        root.beginRow(31, "fp_deployment_operation", 0L)
        assertArrayEquals(Array[Byte](1, 2, 3), MySqlFormat3CellStreamDecoder.readBinary(
          new ByteArrayInputStream(Array[Byte](1, 2, 3)), Some(3), root, "deployment binary"))
        root.endRow("fp_deployment_operation", 0L)
      }
      val receipt = f.core.recordMainDrop(handle)
      f.onWorker { f.core.releaseDropped(receipt) }
      assertTrue(root.isClosed)
      assertEquals(0L, f.shared.accountedBytes)
    finally f.close()

  @Test def deploymentOriginalTransactionsRejectBeforeSerialSlotAndRoot(): Unit =
    val f = new Fixture(1)
    try
      val find = MySqlRepositoryOperation.FindOriginalDeploymentOperation(id(71), id(72))
      val tag = IoOperationTag(id(1), 1L, 1L, id(10))
      assertThrows(classOf[IllegalArgumentException], () => f.core.reserveTransaction(tag, id(110), id(210), Vector(find)))
      assertThrows(classOf[IllegalArgumentException], () => f.core.reserveTransaction(tag, id(110), id(210), Vector(MySqlRepositoryOperation.FindAll, find)))
      assertEquals(0L, f.shared.accountedBytes)
      assertEquals(0, f.core.retainedRoots)
      val plan = f.core.reserveSingle(tag, id(110), id(210), find)
      val handle = plan.originalRetainedHandle.get
      assertEquals(1L, handle.serial)
      f.core.submitted(plan, IoSubmission.RejectedQuiescing)
      f.core.cancelExplicitlyRejected(handle)
      assertEquals(0L, f.shared.accountedBytes)
    finally f.close()

  @Test def deploymentOriginalInitializationRequiresWorkerAndRetainsFirstReserveFailure(): Unit =
    val f = new Fixture(1)
    try
      val plan = f.reserveDeployment(10)
      val handle = plan.originalRetainedHandle.get
      val root = f.core.validationBudget(handle)
      assertThrows(classOf[SQLNonTransientConnectionException], () => root.initializeDeploymentOriginalRead(
        MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation, MySqlFull20CodecFixture.scope, 65536L))
      f.onWorker {
        f.core.beginExecution(plan)
        assertThrows(classOf[SQLDataException], () => root.initializeDeploymentOriginalRead(
          "wrong_schema", MySqlFull20CodecFixture.observation, MySqlFull20CodecFixture.scope, 65536L))
        val occupied = root.availableMemoryBytes
        root.reserveRetainedMemory(occupied, "test actual shared capacity")
        val first = assertThrows(classOf[SQLDataException], () => root.initializeDeploymentOriginalRead(
          MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation, MySqlFull20CodecFixture.scope, 65536L))
        root.releaseRetainedMemory(occupied, "test actual shared capacity returned")
        assertSame(first, assertThrows(classOf[SQLDataException], () => root.initializeDeploymentOriginalRead(
          MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation, MySqlFull20CodecFixture.scope, 65536L)))
        assertFalse(root.isInitialized)
      }
      val receipt = f.core.recordMainDrop(handle)
      f.onWorker { f.core.releaseDropped(receipt) }
      assertEquals(0L, f.shared.accountedBytes)
    finally f.close()

  @Test def deploymentOriginalRowAttemptCannotReplayAfterSuccessOrReserveFailure(): Unit =
    val f = new Fixture(2)
    try
      val first = f.reserveDeployment(10)
      val second = f.reserveDeployment(20)
      f.onWorker {
        val root = f.core.beginExecution(first)
        root.initializeDeploymentOriginalRead(MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation,
          MySqlFull20CodecFixture.scope, 65536L)
        assertThrows(classOf[SQLNonTransientConnectionException], () => root.initializeDeploymentOriginalRead(
          MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation, MySqlFull20CodecFixture.scope, 65536L))
        assertThrows(classOf[SQLDataException], () => root.beginRow(30, "fp_deployment_operation", 0L))
        root.beginRow(31, "fp_deployment_operation", 0L)
        root.endRow("fp_deployment_operation", 0L)
        assertThrows(classOf[SQLDataException], () => root.beginRow(31, "fp_deployment_operation", 0L))
        val failed = f.core.beginExecution(second)
        failed.initializeDeploymentOriginalRead(MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation,
          MySqlFull20CodecFixture.scope, 65536L)
        val occupied = failed.availableMemoryBytes
        failed.reserveRetainedMemory(occupied, "test row capacity")
        val cause = assertThrows(classOf[SQLDataException], () => failed.beginRow(31, "fp_deployment_operation", 0L))
        failed.releaseRetainedMemory(occupied, "test row capacity returned")
        assertSame(cause, assertThrows(classOf[SQLDataException], () => failed.beginRow(31, "fp_deployment_operation", 0L)))
      }
      val firstReceipt = f.core.recordMainDrop(first.originalRetainedHandle.get)
      val secondReceipt = f.core.recordMainDrop(second.originalRetainedHandle.get)
      f.onWorker { f.core.releaseDropped(firstReceipt); f.core.releaseDropped(secondReceipt) }
      assertEquals(0L, f.shared.accountedBytes)
    finally f.close()

  @Test def deploymentOriginalJdbcClaimRejectsReplayBeforeFurtherBootstrapCalls(): Unit =
    val f = new Fixture(1)
    try
      val plan = f.reserveDeployment(10)
      f.onWorker {
        val root = f.core.beginExecution(plan)
        var bootstrapCalls = 0
        def bootstrap(): Unit =
          root.claimDeploymentOriginalJdbcAttempt()
          bootstrapCalls += 1
        bootstrap()
        assertEquals(1, bootstrapCalls)
        assertThrows(classOf[SQLNonTransientConnectionException], () => bootstrap())
        assertEquals(1, bootstrapCalls)
      }
      val receipt = f.core.recordMainDrop(plan.originalRetainedHandle.get)
      f.onWorker { f.core.releaseDropped(receipt) }
      assertEquals(0L, f.shared.accountedBytes)
    finally f.close()

  @Test def deploymentOriginalJdbcClaimRejectsInitializedAndInitializationAttemptedRoots(): Unit =
    val f = new Fixture(2)
    try
      val first = f.reserveDeployment(10)
      val second = f.reserveDeployment(20)
      f.onWorker {
        val initialized = f.core.beginExecution(first)
        initialized.initializeDeploymentOriginalRead(MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation,
          MySqlFull20CodecFixture.scope, 65536L)
        assertThrows(classOf[SQLNonTransientConnectionException], () => initialized.claimDeploymentOriginalJdbcAttempt())
        val attempted = f.core.beginExecution(second)
        val occupied = attempted.availableMemoryBytes
        attempted.reserveRetainedMemory(occupied, "test initialization attempt capacity")
        val primary = assertThrows(classOf[SQLDataException], () => attempted.initializeDeploymentOriginalRead(
          MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation, MySqlFull20CodecFixture.scope, 65536L))
        attempted.releaseRetainedMemory(occupied, "test initialization attempt capacity returned")
        assertSame(primary, assertThrows(classOf[SQLDataException], () => attempted.claimDeploymentOriginalJdbcAttempt()))
      }
      val firstReceipt = f.core.recordMainDrop(first.originalRetainedHandle.get)
      val secondReceipt = f.core.recordMainDrop(second.originalRetainedHandle.get)
      f.onWorker { f.core.releaseDropped(firstReceipt); f.core.releaseDropped(secondReceipt) }
      assertEquals(0L, f.shared.accountedBytes)
    finally f.close()

  @Test def deploymentOriginalJdbcBootstrapFailureRetainsFirstCauseAndBlocksFurtherCalls(): Unit =
    val f = new Fixture(1)
    try
      val plan = f.reserveDeployment(10)
      f.onWorker {
        val root = f.core.beginExecution(plan)
        val primary = new IOException("original JDBC bootstrap failure")
        val stack = primary.getStackTrace
        var bootstrapCalls = 0
        def bootstrap(): Unit =
          root.claimDeploymentOriginalJdbcAttempt()
          bootstrapCalls += 1
          throw primary
        assertSame(primary, assertThrows(classOf[IOException], () => bootstrap()))
        root.recordDeploymentOriginalJdbcFailure(primary)
        assertSame(primary, assertThrows(classOf[IOException], () => bootstrap()))
        assertEquals(1, bootstrapCalls)
        root.recordDeploymentOriginalJdbcFailure(new IOException("later JDBC bootstrap failure"))
        assertSame(primary, assertThrows(classOf[IOException], () => root.requireDeploymentOriginalReadLayout()))
        assertTrue(stack.sameElements(primary.getStackTrace))
      }
      val receipt = f.core.recordMainDrop(plan.originalRetainedHandle.get)
      f.onWorker { f.core.releaseDropped(receipt) }
      assertEquals(0L, f.shared.accountedBytes)
    finally f.close()

  @Test def deploymentOriginalRejectsSnapshotDataAndBorrowButAllowsComparison(): Unit =
    val f = new Fixture(1)
    try
      val plan = f.reserveDeployment(10)
      val handle = plan.originalRetainedHandle.get
      val root = f.core.validationBudget(handle)
      f.onWorker {
        f.core.beginExecution(plan)
        root.initializeDeploymentOriginalRead(MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation,
          MySqlFull20CodecFixture.scope, 65536L)
        val before = f.shared.accountedBytes
        assertThrows(classOf[SQLNonTransientConnectionException], () => root.initialize(
          MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation, MySqlFull20CodecFixture.scope, 65536L))
        assertThrows(classOf[SQLNonTransientConnectionException], () => root.issueOwnedSnapshotImage(
          MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation, MySqlFull20CodecFixture.scope, 65536L))
        assertThrows(classOf[SQLNonTransientConnectionException], () => root.issueScriptSchemaSnapshotImage(
          null, MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation, MySqlFull20CodecFixture.scope, 65536L))
        assertThrows(classOf[SQLNonTransientConnectionException], () => root.recordMaterializedSnapshot(Vector.empty))
        assertThrows(classOf[SQLNonTransientConnectionException], () => root.newScopedReadBudget(65536L))
        assertThrows(classOf[SQLNonTransientConnectionException], () => root.newCanonicalDataScope())
        assertThrows(classOf[SQLNonTransientConnectionException], () => root.beginRawBorrow())
        assertThrows(classOf[SQLNonTransientConnectionException], () => root.beginDecodeBorrow(null))
        assertThrows(classOf[SQLNonTransientConnectionException], () => root.registerBorrow(null, null))
        val ordinary = MySqlFormat3ReadBudget.rootOnLedger(f.limits, 65536L, f.shared)
        val originalBytes = MySqlFormat3Binary.takeOwned(Array[Byte](1), root)
        assertThrows(classOf[SQLNonTransientConnectionException], () => ordinary.registerBorrow(originalBytes, null))
        ordinary.closeRootReadBudget()
        val comparison = root.newCanonicalScope()
        assertTrue(comparison.isCanonicalComparisonChildOf(root))
        assertTrue(comparison.sharesAllocationLedger(root))
        assertThrows(classOf[SQLNonTransientConnectionException], () => comparison.beginRawBorrow())
        assertThrows(classOf[SQLNonTransientConnectionException], () => comparison.newCanonicalDataScope())
        assertThrows(classOf[SQLNonTransientConnectionException], () => comparison.beginRow(31, "fp_deployment_operation", 0L))
        comparison.closeScopedReadBudget()
        assertEquals(before, f.shared.accountedBytes)
      }
      val receipt = f.core.recordMainDrop(handle)
      f.onWorker { f.core.releaseDropped(receipt) }
      assertEquals(0L, f.shared.accountedBytes)
    finally f.close()

  @Test def deploymentOriginalStreamPreservesReadAndCloseCausesAndReturnsScratch(): Unit =
    val f = new Fixture(1)
    try
      val plan = f.reserveDeployment(10)
      val handle = plan.originalRetainedHandle.get
      val readFailure = new IOException("original deployment stream read")
      val closeFailure = new IOException("original deployment stream close")
      f.onWorker {
        val root = f.core.beginExecution(plan)
        root.initializeDeploymentOriginalRead(MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation,
          MySqlFull20CodecFixture.scope, 65536L)
        root.beginRow(31, "fp_deployment_operation", 0L)
        val before = f.shared.accountedBytes
        val stream = new InputStream:
          override def read(): Int = throw readFailure
          override def read(bytes: Array[Byte], offset: Int, length: Int): Int = throw readFailure
          override def close(): Unit = throw closeFailure
        val actual = assertThrows(classOf[IOException], () =>
          MySqlFormat3CellStreamDecoder.readBinary(stream, Some(32), root, "deployment failing stream"))
        assertSame(readFailure, actual)
        assertTrue(actual.getSuppressed.exists(_ eq closeFailure))
        assertEquals(before, f.shared.accountedBytes)
        root.endRow("fp_deployment_operation", 0L)
      }
      val receipt = f.core.recordMainDrop(handle)
      f.onWorker { f.core.releaseDropped(receipt) }
      assertEquals(0L, f.shared.accountedBytes)
    finally f.close()

  @Test def preparationCommitProgressKeepsUnknownAndAcknowledgedFailureDistinct():Unit =
    import MySqlDbFirstSession.DbFirstInitialPreparationOutcome.*
    def fresh()=new MySqlDbFirstInitialPreparationTransaction.Progress(Thread.currentThread())
    val before=fresh();before.begin();before.fail()
    assertEquals(FailedBeforeCommit,before.outcome)
    assertThrows(classOf[IllegalArgumentException],()=>before.attemptCommit())
    val unknown=fresh();unknown.begin();unknown.attemptCommit();unknown.fail()
    assertEquals(CommitUnknown,unknown.outcome)
    assertTrue(unknown.commitAttempted);assertFalse(unknown.acknowledged)
    assertThrows(classOf[IllegalArgumentException],()=>unknown.attemptCommit())
    assertThrows(classOf[IllegalArgumentException],()=>unknown.acknowledge())
    val acknowledged=fresh();acknowledged.begin();acknowledged.attemptCommit();acknowledged.acknowledge();acknowledged.fail()
    assertEquals(AcknowledgedPending,acknowledged.outcome)
    assertThrows(classOf[IllegalArgumentException],()=>acknowledged.verifyReadback())
    val verified=fresh();verified.begin();verified.attemptCommit();verified.acknowledge();verified.verifyReadback()
    assertEquals(ReadbackVerifiedPending,verified.outcome)
    assertThrows(classOf[IllegalArgumentException],()=>verified.begin())

  @Test def preparationRollbackProgressCannotConvertCommitUnknownOrAckIntoLocalCleanup():Unit =
    val before=new MySqlDbFirstInitialPreparationTransaction.Progress(Thread.currentThread())
    assertThrows(classOf[IllegalArgumentException],()=>before.recordPrecommitRollbackSettled())
    before.begin()
    assertThrows(classOf[IllegalArgumentException],()=>before.recordPrecommitRollbackSettled())
    before.fail();before.recordPrecommitRollbackSettled()
    assertTrue(before.precommitRollbackSettled)
    assertThrows(classOf[IllegalArgumentException],()=>before.recordPrecommitRollbackSettled())
    for ack <- Vector(false,true) do
      val attempted=new MySqlDbFirstInitialPreparationTransaction.Progress(Thread.currentThread())
      attempted.begin();attempted.attemptCommit()
      if ack then attempted.acknowledge()
      attempted.fail()
      assertThrows(classOf[IllegalArgumentException],()=>attempted.recordPrecommitRollbackSettled())
      assertFalse(attempted.precommitRollbackSettled)

  @Test def activeCheckpointUsesOriginalSharedCaptureOwnerAndCannotBecomeRecallOrInitialPublication(): Unit =
    val inputs=MySqlScriptSchemaInputs.createDbFirstFull27(MySqlDbFirstSchemaProfile.render(DeploymentReceiptStaticTestData.defaultProfile()))
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var primary: Throwable=null
    try
      val tag=IoOperationTag(id(1),1L,1L,id(990))
      val reservation=f.core.reserveDbFirstCapture(tag,DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT)
      val captured=f.core.captureDbFirstSnapshot(reservation)(a=>
        DbFirstPodSnapshotTest.captureCheckpointForCore(tag.operationId,a,id(992),id(993),id(994)))
      val request=f.core.dbFirstCheckpointRequest(reservation,captured)
      assertSame(request,f.core.dbFirstCheckpointRequest(reservation,captured))
      assertEquals(DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT,captured.purpose())
      assertEquals(None,request.ownerUuid); assertEquals(7L,request.capturedStateVersion)
      assertEquals(captured.byteLength(),request.nbtByteLength)
      assertEquals(captured.sha256().toVector,request.nbtSha256)
      assertTrue(f.shared.accountedBytes>captured.retainedBytes())
      val root=f.core.validationBudget(reservation.handle)
      val recall=MySqlRepositoryRequestIdentity.dbFirstRecall(tag,request.bindingUuid,request.podUuid,
        request.ownerUuid,request.attemptUuid,request.capturedStateVersion,request.snapshotFormatVersion,
        request.nbtByteLength,request.nbtSha256,request.profileBytes,request.profileSha256,request.pose,root)
      def requestPayload(bytes:Vector[Byte],marker:String):Array[Byte] =
        val input=new java.io.DataInputStream(new java.io.ByteArrayInputStream(bytes.toArray))
        def token():String =
          val n=input.readInt();assertTrue(n>0 && n<100)
          val value=new Array[Byte](n);input.readFully(value)
          new String(value,java.nio.charset.StandardCharsets.US_ASCII)
        assertEquals("FP_MYSQL_REPOSITORY_REQUEST_BE_V1",token())
        assertEquals(1,input.readUnsignedByte());assertEquals(1,input.readInt())
        assertEquals(marker,token());input.readAllBytes()
      val checkpointBody=requestPayload(request.identity.canonicalBytes,"DB_FIRST_ACTIVE_CHECKPOINT_V1")
      val recallBytes=Vector.tabulate(recall.canonicalLength)(recall.canonical.borrowedBinary.byteAt)
      assertArrayEquals(checkpointBody,requestPayload(recallBytes,"DB_FIRST_RECALL_V1"))
      assertNotEquals(request.identity.sha256,recall.sha256,"checkpoint must never alias inactive recall wire identity")
      assertThrows(classOf[IllegalArgumentException],()=>f.core.dbFirstRecallRequest(reservation,captured))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.dbFirstInitialActivationRequest(reservation,captured))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireDbFirstRecallSubmission(reservation))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireDbFirstInitialActivationSubmission(reservation))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordMainDrop(reservation.handle))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.cancelExplicitlyRejected(reservation.handle))
      val counterfeit=new MySqlDbFirstCaptureReservation(f.core,reservation.handle,f.limits,DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.dbFirstCheckpointRequest(counterfeit,captured))
      assertTrue(f.core.observation.failure.isEmpty)
      assertTrue(f.core.observation.plans.head.cause.isEmpty)
    catch
      case cause: Throwable => primary=cause;throw cause
    finally closePreserving(f,primary)

  @Test def activeCheckpointCannotAcquireLegacyCaptureRoot(): Unit =
    val f=new Fixture(1)
    try
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(
        IoOperationTag(id(1),1L,1L,id(991)),DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT))
      assertEquals(0L,f.shared.accountedBytes)
      assertTrue(f.core.observation.plans.isEmpty)
    finally f.close()

  @Test def activeCheckpointFull27KeepsActiveBusinessStateAndReplaysWithoutLeakingComparisonAllocation(): Unit =
    Vector(false->false,true->false,false->true,true->true).foreach { (ownerless,drift) =>
      val inputs=dbFirstMaterializationInputs()
      val f=new Fixture(1,dbFirstInputs=Some(inputs))
      var source: MySqlScriptSchemaSnapshot=null
      var primary: Throwable=null
      try
        val tag=IoOperationTag(id(1),1L,1L,id(995))
        val reservation=f.core.reserveDbFirstCapture(tag,DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT)
        val captured=f.core.captureDbFirstSnapshot(reservation)(a=>DbFirstPodSnapshotTest.captureCheckpointForCore(
          tag.operationId,a,id(502),id(503),id(504),if ownerless then null else id(501),if ownerless then "WorldIdle" else "Normal"))
        val request=f.core.dbFirstCheckpointRequest(reservation,captured)
        source=dbFirstBefore(f,inputs,checkpoint= !drift,ownerless=ownerless)
        val at=java.time.LocalDateTime.of(2026,10,29,12,0)
        assertThrows(classOf[IllegalArgumentException],()=>f.core.materializeDbFirstCheckpoint(reservation,request,source,91L,id(996),at,at.plusSeconds(1)))
        f.onWorker {
          val m=f.core.materializeDbFirstCheckpoint(reservation,request,source,91L,id(996),at,at.plusSeconds(1))
          val root=f.core.validationBudget(reservation.handle)
          assertSame(request,m.request)
          val before=m.before.relation("floating_pod_state").rows.head
          val after=m.target.relation("floating_pod_state").rows.head
          inputs.forTable("floating_pod_state").columns.zipWithIndex.foreach { (column,index)=>
            val changed=Set("state_version","last_updated","last_mutation_operation_uuid") ++
              (if drift then Set("pos_x","pos_y","pos_z","yaw","pitch") else Set.empty[String])
            if !changed.contains(column.name) then
              assertEquals(before(index),after(index),column.name)
          }
          assertEquals(MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(1D)),cell(after,"floating_pod_state","pos_x",inputs))
          assertEquals(MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(64D)),cell(after,"floating_pod_state","pos_y",inputs))
          assertEquals(MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(2D)),cell(after,"floating_pod_state","pos_z",inputs))
          assertEquals(8L,number(after,"floating_pod_state","state_version",inputs))
          assertEquals(MySqlFormat3Cell.Text("ROW_ACTIVE"),cell(m.target.relation("physical_recovery_locator").rows.head,"physical_recovery_locator","locator_state",inputs))
          assertEquals(m.before.relation("physical_recovery_footprint"),m.target.relation("physical_recovery_footprint"))
          val operation=m.target.relation("fp_recovery_operation").rows.find(r=>MySqlDbFirstRecallMaterializer.uuidEquals(cell(r,"fp_recovery_operation","operation_uuid",inputs),tag.operationId)).get
          assertEquals(MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.CheckpointStep),cell(operation,"fp_recovery_operation","step",inputs))
          m.target.canonicalSha256
          val held=root.accountedMemoryBytes
          (0 until 2).foreach { _=>
            m.verify()
            MySqlDbFirstRecallPersistedRead.verifyCommitted(m.target,tag.operationId,root)
          }
          assertEquals(held,root.accountedMemoryBytes)
          root.reserveRetainedMemory(MySqlDbFirstSession.HistoricalRead.HolderBytes,"checkpoint test actual history holder")
          val history=new MySqlDbFirstSession.HistoricalRead(root,inputs)
          try
            history.acquire(None);history.validate(m.target)
            assertEquals(0L,history.dbFirstRecallSummary.verifiedCommitted)
            assertEquals(MySqlDbFirstSession.DbFirstCheckpointSummary(1L,0L,0L),history.dbFirstCheckpointSummary)
          finally history.close()
          assertEquals(held,root.accountedMemoryBytes)
          assertThrows(classOf[IllegalArgumentException],()=>f.core.materializeDbFirstCheckpoint(reservation,request,source,91L,id(996),at,at.plusSeconds(1)))
          assertFalse(f.core.observation.plans.head.accepted)
          assertFalse(f.core.observation.plans.head.workerObserved)
          assertFalse(root.isClosed)
        }
      catch
        case cause:Throwable=>
          primary=cause
          throw cause
      finally MySqlFormat3BudgetCleanup.run(primary,Vector(
        ()=>if source!=null then source.allocationBudget.closeRootReadBudget(),()=>f.close()))
    }

  @Test def activeCheckpointCrossChunkPoseFailsAndRetainsOriginalEntryCauseAndRoot(): Unit =
    val inputs=dbFirstMaterializationInputs()
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var source: MySqlScriptSchemaSnapshot=null
    var primary: Throwable=null
    try
      val tag=IoOperationTag(id(1),1L,1L,id(997))
      val reservation=f.core.reserveDbFirstCapture(tag,DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT)
      val captured=f.core.captureDbFirstSnapshot(reservation)(a=>DbFirstPodSnapshotTest.captureCheckpointForCoreAt(
        tag.operationId,a,id(502),id(503),id(504),id(501),"Normal",16D))
      val request=f.core.dbFirstCheckpointRequest(reservation,captured)
      val retainedRoot=f.core.validationBudget(reservation.handle)
      source=dbFirstBefore(f,inputs)
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      val failure=f.onWorkerFailure(f.core.materializeDbFirstCheckpoint(reservation,request,source,91L,id(998),at,at.plusSeconds(1)))
      assertInstanceOf(classOf[SQLDataException],failure)
      assertTrue(failure.getMessage.contains("cannot migrate the original center chunk/footprint"))
      assertSame(failure,f.core.observation.plans.head.cause.get)
      assertFalse(retainedRoot.isClosed)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(tag,DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT))
    catch
      case cause:Throwable=>
        primary=cause
        throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(
      ()=>if source!=null then source.allocationBudget.closeRootReadBudget(),()=>f.close()))

  @Test def checkpointRegisteredWorkerKeepsDistinctIdentityAndRequiresRealReadbackForConfirmation():Unit =
    val inputs=dbFirstMaterializationInputs()
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var source:MySqlScriptSchemaSnapshot=null
    var primary:Throwable=null
    try
      val tag=IoOperationTag(id(1),1L,1L,id(999))
      val reservation=f.core.reserveDbFirstCapture(tag,DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT)
      val captured=f.core.captureDbFirstSnapshot(reservation)(a=>DbFirstPodSnapshotTest.captureCheckpointForCore(
        tag.operationId,a,id(502),id(503),id(504),id(501),"Normal"))
      val request=f.core.dbFirstCheckpointRequest(reservation,captured)
      val root=f.core.validationBudget(reservation.handle)
      source=dbFirstBefore(f,inputs,checkpoint=true)
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure(f.core.beginDbFirstCheckpoint(reservation)))
      f.core.requireDbFirstCheckpointSubmission(reservation)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireDbFirstCheckpointSubmission(reservation))
      f.core.submittedDbFirstCheckpoint(reservation,IoSubmission.Accepted)
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      var runner:MySqlDbFirstCheckpointTransaction=null
      f.onWorker {
        runner=f.core.beginDbFirstCheckpoint(reservation)
        assertSame(request,runner.request)
        f.core.requireExecutingDbFirstCheckpoint(runner)
        val m=f.core.materializeExecutingDbFirstCheckpoint(runner,source,91L,id(1000),at,at.plusSeconds(1))
        assertSame(request,m.request)
        assertEquals(1L,number(m.target.relation("floating_pod_state").rows.head,"floating_pod_state","is_active",inputs))
        assertFalse(runner.commitAttempted)
        assertFalse(runner.acknowledged)
        assertFalse(runner.resourcesRetired)
        val forged=MySqlDbFirstSession.DbFirstCheckpointObservation(tag,id(502),true,true,true,m.target.canonicalSha256Hex)
        assertThrows(classOf[IllegalArgumentException],()=>runner.requireConfirmedObservation(forged))
        assertThrows(classOf[IllegalArgumentException],()=>runner.confirmedMainReadSource(forged))
        assertThrows(classOf[IllegalArgumentException],()=>runner.retireConfirmedResources())
        assertThrows(classOf[IllegalArgumentException],()=>f.core.beginDbFirstCheckpoint(reservation))
      }
      val forged=MySqlDbFirstSession.DbFirstCheckpointObservation(tag,id(502),true,true,true,"a"*64)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.claimDbFirstCheckpointConfirmation(reservation,forged))
      val counterfeit=MySqlDbFirstCheckpointConfirmation.issue(f.core,reservation.handle,request)
      assertThrows(classOf[IllegalArgumentException],()=>counterfeit.requireOriginal(reservation))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireCheckpointReadReleased(counterfeit,reservation))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.acknowledgeCheckpointReadReleased(counterfeit,reservation))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordDbFirstCheckpointFinal(counterfeit))
      val forgedFinal=MySqlDbFirstCheckpointFinalReceipt.issue(f.core,reservation.handle)
      f.onWorker {
        assertThrows(classOf[IllegalArgumentException],()=>f.core.beginCheckpointReadRelease(counterfeit))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.finishCheckpointReadRelease(counterfeit))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.requireDbFirstCheckpointFinal(forgedFinal))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.retireDbFirstCheckpointFinal(forgedFinal))
        assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstCheckpointLocalRetirementException.fromCore(
          f.core,forgedFinal,new IllegalStateException("not a registered local retirement")))
      }
      assertFalse(root.isClosed)
      assertFalse(f.core.observation.plans.head.dropped)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.claimDbFirstRecallConfirmation(reservation,
        MySqlDbFirstSession.DbFirstRecallObservation(tag,id(502),true,true,true,"a"*64)))
      val failure=new IllegalStateException("checkpoint worker failed before actual SQL")
      f.onWorker {
        f.core.failDbFirstCheckpoint(runner,failure)
        assertSame(failure,assertThrows(classOf[IllegalStateException],()=>f.core.requireExecutingDbFirstCheckpoint(runner)))
      }
      assertSame(failure,f.core.observation.plans.head.cause.get)
      assertTrue(f.core.observation.plans.head.workerObserved)
      assertFalse(root.isClosed)
      assertSame(request,runner.request)
    catch
      case cause:Throwable=>
        primary=cause
        throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(
      ()=>if source!=null then source.allocationBudget.closeRootReadBudget(),()=>f.close()))

  @Test def checkpointRejectedAndUnknownSubmissionsRetainOriginalCaptureAndCannotExecute():Unit =
    Vector(false,true).foreach { unknown=>
      val inputs=dbFirstMaterializationInputs()
      val f=new Fixture(1,dbFirstInputs=Some(inputs))
      try
        val tag=IoOperationTag(id(1),1L,1L,id(1001))
        val reservation=f.core.reserveDbFirstCapture(tag,DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT)
        val captured=f.core.captureDbFirstSnapshot(reservation)(a=>DbFirstPodSnapshotTest.captureCheckpointForCore(
          tag.operationId,a,id(502),id(503),id(504)))
        f.core.dbFirstCheckpointRequest(reservation,captured)
        val root=f.core.validationBudget(reservation.handle)
        f.core.requireDbFirstCheckpointSubmission(reservation)
        if unknown then
          val cause=new IllegalStateException("checkpoint submission unknown")
          f.core.submissionUnknownDbFirstCheckpoint(reservation,cause)
          assertSame(cause,f.onWorkerFailure(f.core.beginDbFirstCheckpoint(reservation)))
          assertSame(cause,f.core.observation.failure.get)
        else
          f.core.submittedDbFirstCheckpoint(reservation,IoSubmission.RejectedCapacity)
          assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure(f.core.beginDbFirstCheckpoint(reservation)))
        assertFalse(root.isClosed)
        assertFalse(f.core.observation.plans.head.workerObserved)
        assertFalse(f.core.observation.plans.head.dropped)
        val remint=assertThrows(classOf[RuntimeException],()=>f.core.reserveDbFirstCapture(tag,DbFirstPodSnapshot.Purpose.ACTIVE_CHECKPOINT))
        if unknown then assertSame(f.core.observation.failure.get,remint)
        else assertInstanceOf(classOf[IllegalArgumentException],remint)
      finally f.close()
    }

  /** 两笔O/W-only DATA的显式synthetic预算；不授予not-applied或SQL资格，不改生产容量。 */
  private def withRefusalBefore(client:Option[UUID]=Some(id(2703)),crossBoot:Boolean=false,snapshotBytes:Long=524288L,
      reason:Option[RecallRefusalReason]=None)(
      body:(MySqlDbFirstRecallRefusalRequest,MySqlDbFirstRecallRefusalFacts,MySqlScriptSchemaSnapshot,
        MySqlScriptSchemaSnapshot,MySqlFormat3ReadBudget)=>Unit):Unit =
    val inputs=dbFirstMaterializationInputs(snapshotBytes);val f=new Fixture(1,dbFirstInputs=Some(inputs))
    var source:MySqlScriptSchemaSnapshot=null;var dataRoot:MySqlFormat3ReadBudget=null;var primary:Throwable=null
    try
      source=dbFirstBefore(f,inputs,physicalGraph=true)
      val j=IoOperationTag(id(1),1L,1L,id(2701));val c=j.copy(operationId=id(2702))
      val journal=MySqlDbFirstRecallJournalRequest.fresh(j,c,id(503),id(501),id(502),7L,client,source.allocationBudget)
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      val admitted=MySqlDbFirstRecallJournalMaterializer.materialize(MySqlDbFirstRecallJournalMaterializer.allocate(
        journal,MySqlDbFirstRecallJournalFacts(91L,id(2704),3L,at.plusHours(1),at,at.plusSeconds(1)),source,source.allocationBudget)).target
      dataRoot=inputs.newAllocationRoot(inputs.budget.packetBytes,f.shared)
      val tag=if crossBoot then IoOperationTag(id(2709),9L,11L,c.operationId) else c
      val lease=if crossBoot then at.plusHours(2) else at.plusHours(1)
      val rows=copyPersistedRows(admitted.relations,dataRoot).map { relation=>
        if !crossBoot || relation.table!="fp_runtime_writer" then relation else relation.copy(rows=relation.rows.map { row=>
          Vector("owner_boot_uuid"->MySqlScriptSchemaDataTestFixture.binary(MySqlScriptSchemaDataTestFixture.uuidBytes(tag.bootId)),
            "writer_epoch"->MySqlFormat3Cell.SignedInteger(4L),"lease_until"->MySqlFormat3Cell.DateTime(lease)).foldLeft(row) { (r,pair)=>
            r.updated(inputs.forTable(relation.table).columns.indexWhere(_.name==pair._1),pair._2)
          }
        })
      }
      val current=MySqlScriptSchemaSnapshot.captureMaterialized(inputs.schema,source.connection,
        MySqlFormat3ObservationScope(tag.bootId,tag.bootGeneration,tag.connectionGeneration),source.maxAllowedPacketBytes,inputs,rows,dataRoot)
      val request=reason match
        case Some(value)=>MySqlDbFirstRecallRefusalRequest.notAppliedWithReason(tag,journal,value,dataRoot)
        case None=>MySqlDbFirstRecallRefusalRequest.notApplied(tag,journal,dataRoot)
      val facts=MySqlDbFirstRecallRefusalFacts(92L,id(2705),if crossBoot then 4L else 3L,lease,at.plusSeconds(2),at.plusSeconds(3))
      body(request,facts,current,source,dataRoot)
    catch case cause:Throwable=>
      primary=cause
      throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(
      ()=>if dataRoot!=null && !dataRoot.isClosed then dataRoot.closeRootReadBudget(),
      ()=>if source!=null && !source.allocationBudget.isClosed then source.allocationBudget.closeRootReadBudget(),()=>f.close()))

  @Test def refusalMaterializesOnlyOriginalCReceiptAndWAfterExactJournalForBothScopes():Unit =
    for client<-Vector(None,Some(id(2703)));crossBoot<-Vector(false,true) do
      withRefusalBefore(client,crossBoot) { (request,facts,before,_,root)=>
        val draft=MySqlDbFirstRecallRefusalMaterializer.allocate(request,facts,before,root)
        val result=MySqlDbFirstRecallRefusalMaterializer.materialize(draft)
        val inputs=before.inputs;val o="fp_recovery_operation"
        def value(row:Vector[MySqlFormat3Cell],name:String)=MySqlDbFirstRecallMaterializer.value(row,o,name,inputs)
        val own=result.target.relation(o).rows.find(r=>MySqlDbFirstRecallMaterializer.uuidEquals(value(r,"operation_uuid"),request.originalTag.operationId)).get
        assertEquals(MySqlFormat3Cell.Text("DB_FIRST_RECALL_REFUSED"),value(own,"step"))
        assertEquals(MySqlFormat3Cell.Text("COMMITTED"),value(own,"operation_state"))
        assertTrue(MySqlDbFirstRecallMaterializer.uuidEquals(value(own,"workflow_uuid"),request.originalTag.operationId))
        assertTrue(MySqlDbFirstRecallMaterializer.uuidEquals(value(own,"previous_step_operation_uuid"),request.journalTag.operationId))
        assertTrue(MySqlDbFirstRecallMaterializer.uuidEquals(value(own,"boot_uuid"),request.tag.bootId))
        assertEquals(MySqlFormat3Cell.SqlNull,value(own,"settled_by_operation_uuid"))
        assertEquals(before.relation(o).rows,result.target.relation(o).rows.filterNot(_ eq own))
        before.relations.filterNot(r=>Set(o,"fp_runtime_writer").contains(r.table)).foreach(r=>assertEquals(r,result.target.relation(r.table)))
        assertEquals(MySqlFormat3Cell.Text("RESERVED"),value(result.reservedOperation,"operation_state"))
        assertEquals(MySqlFormat3Cell.SqlNull,value(result.reservedOperation,"result_at"))
        assertEquals(MySqlFormat3Cell.SqlNull,value(result.reservedOperation,"commit_witness_bytes"))
        val baseline=root.accountedMemoryBytes
        result.verify();result.verify()
        assertEquals(baseline,root.accountedMemoryBytes,"all complete verification scopes retire")
        assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstRecallRefusalMaterializer.materialize(draft))
      }

  @Test def refusalIndependentFullTransitionRejectsDomainWriterAndOriginalJournalTampering():Unit =
    withRefusalBefore() { (request,facts,before,_,root)=>
      val result=MySqlDbFirstRecallRefusalMaterializer.materialize(MySqlDbFirstRecallRefusalMaterializer.allocate(request,facts,before,root))
      def reject(table:String,column:String,cell:MySqlFormat3Cell):Unit =
        val rows=result.target.relations.map { relation=>
          if relation.table!=table then relation else relation.copy(rows=relation.rows.map { row=>
            if table=="fp_recovery_operation" && !MySqlDbFirstRecallMaterializer.uuidEquals(
                MySqlDbFirstRecallMaterializer.value(row,table,"operation_uuid",before.inputs),request.originalTag.operationId) then row
            else row.updated(before.inputs.forTable(table).columns.indexWhere(_.name==column),cell)
          })
        }
        assertThrows(classOf[SQLDataException],()=>{
          val changed=MySqlScriptSchemaSnapshot.captureMaterialized(before.schema,before.connection,before.observationScope,
            before.maxAllowedPacketBytes,before.inputs,rows,root)
          result.verifyTarget(changed)
        },table+"."+column)
      reject("floating_pod_state","pos_x",MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(444D)))
      reject("fp_runtime_writer","last_commit_operation_uuid",MySqlScriptSchemaDataTestFixture.binary(MySqlScriptSchemaDataTestFixture.uuidBytes(request.journalTag.operationId)))
      reject("fp_recovery_operation","previous_step_operation_uuid",MySqlScriptSchemaDataTestFixture.binary(MySqlScriptSchemaDataTestFixture.uuidBytes(id(700))))
      reject("fp_recovery_operation","step",MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallStep))
      reject("fp_recovery_operation","expected_domain_version",MySqlFormat3Cell.SignedInteger(8L))
    }

  @Test def refusalPreflightRejectsAnyExistingOriginalCOrMissingOrMismatchedJournal():Unit =
    withRefusalBefore() { (request,facts,before,source,root)=>
      assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRefusalMaterializer.preflight(request,facts,source,source.allocationBudget))
      val result=MySqlDbFirstRecallRefusalMaterializer.materialize(MySqlDbFirstRecallRefusalMaterializer.allocate(request,facts,before,root))
      assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRefusalMaterializer.preflight(request,facts,result.target,root))
      val o="fp_recovery_operation";val inputs=before.inputs
      val own=result.target.relation(o).rows.find(r=>MySqlDbFirstRecallMaterializer.uuidEquals(MySqlDbFirstRecallMaterializer.value(r,o,"operation_uuid",inputs),request.tag.operationId)).get
      val rows=before.relations.map(r=>if r.table!=o then r else r.copy(rows=(r.rows :+ result.reservedOperation)
        .sortWith((a,b)=>MySqlControlledDebugDelta.compareKey(a,b,inputs.forTable(o))<0)))
      val reserved=MySqlScriptSchemaSnapshot.captureMaterialized(before.schema,before.connection,before.observationScope,before.maxAllowedPacketBytes,inputs,rows,root)
      assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRefusalMaterializer.preflight(request,facts,reserved,root))
      val other=MySqlDbFirstRecallJournalRequest.fresh(request.journalTag,request.originalTag,request.bindingUuid,id(999),request.podUuid,
        request.expectedStateVersion,request.clientRequestUuid,root)
      val wrong=MySqlDbFirstRecallRefusalRequest.notApplied(request.tag,other,root)
      assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRefusalMaterializer.preflight(wrong,facts,before,root))
      assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRefusalMaterializer.preflight(request,facts.copy(writerEpoch=8L),before,root))
    }

  @Test def refusalColdGraphAndReceiptCursorSurviveActualConstructionRootRetirement():Unit =
    for crossBoot<-Vector(false,true) do withRefusalBefore(crossBoot=crossBoot) { (request,facts,before,source,root)=>
      val result=MySqlDbFirstRecallRefusalMaterializer.materialize(MySqlDbFirstRecallRefusalMaterializer.allocate(request,facts,before,root))
      val inputs=before.inputs;val cold=inputs.newAllocationRoot(inputs.budget.packetBytes,new MySqlFormat3AllocationLedger(root.profile.maxRetainedBytes()))
      val later=MySqlFormat3ObservationScope(id(2799),19L,21L)
      cold.initialize(inputs.schema,before.connection,later,before.maxAllowedPacketBytes)
      val child=cold.newScopedReadBudget(inputs.budget.packetBytes)
      child.initialize(inputs.schema,before.connection,later,before.maxAllowedPacketBytes)
      try
        val image=MySqlScriptSchemaSnapshot.decoded(inputs.schema,before.connection,later,before.maxAllowedPacketBytes,inputs,copyPersistedRows(result.target.relations,child),child)
        val anchor=MySqlScriptSchemaSnapshot.decoded(inputs.schema,before.connection,later,before.maxAllowedPacketBytes,inputs,copyPersistedRows(before.relations,child),child)
        val genesis=if !crossBoot then Some(MySqlScriptSchemaSnapshot.decoded(inputs.schema,before.connection,later,before.maxAllowedPacketBytes,inputs,copyPersistedRows(source.relations,child),child)) else None
        val operation=request.originalTag.operationId
        root.closeRootReadBudget();source.allocationBudget.closeRootReadBudget()
        val baseline=cold.accountedMemoryBytes
        MySqlDbFirstRecallRefusalPersistedRead.verifyCommitted(image,operation,cold)
        assertEquals(1L,MySqlDbFirstRecallPersistedRead.verifySnapshotReceiptSegment(image,anchor,cold))
        genesis.foreach(g=>assertEquals(2L,MySqlDbFirstRecallPersistedRead.verifySnapshotReceiptSegment(image,g,cold)))
        assertEquals(baseline,cold.accountedMemoryBytes,"cold reconstruction and full receipt cursor refund only their comparison children")
      finally
        child.closeScopedReadBudget();cold.closeRootReadBudget()
    }

  @Test def refusalFailedConstructionKeepsOriginalDraftCauseAndForbidsRetry():Unit =
    withRefusalBefore() { (request,facts,before,_,root)=>
      val draft=MySqlDbFirstRecallRefusalMaterializer.allocate(request,facts,before,root)
      val pressure=root.availableMemoryBytes-256L
      root.reserveRetainedMemory(pressure,"refusal construction pressure")
      val cause=assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRefusalMaterializer.materialize(draft))
      assertSame(cause,draft.failure);assertTrue(draft.attempted);assertNull(draft.result);assertFalse(root.isClosed)
      root.releaseRetainedMemory(pressure,"refusal pressure removed")
      assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstRecallRefusalMaterializer.materialize(draft))
    }

  @Test def refusalCompleteTwoReceiptDataFitsOriginalSmallSnapshotCapacityWithoutIncreasingIt():Unit =
    withRefusalBefore(snapshotBytes=131072L) { (request,facts,before,_,root)=>
      val draft=MySqlDbFirstRecallRefusalMaterializer.allocate(request,facts,before,root)
      val result=MySqlDbFirstRecallRefusalMaterializer.materialize(draft)
      val sizes=Vector("scope"->draft.scope.length,"preimage"->draft.preimage.length,"target"->draft.projectedTarget.length,
        "fingerprint"->draft.fingerprint.length,"context"->draft.context.length,"witness"->draft.witness.length)
      assertEquals(131072L,before.inputs.budget.snapshotBytes)
      sizes.foreach((role,length)=>assertTrue(length>0 && length.toLong<=before.inputs.budget.snapshotBytes,role))
      assertNull(draft.failure);assertSame(result,draft.result);result.verify()
      println("refusal two-receipt DATA fixture at unchanged128KiB: "+sizes.mkString(","))
    }

  private def withRefusalHistoricalRead(root:MySqlFormat3ReadBudget,inputs:MySqlScriptSchemaInputs)(
      body:MySqlDbFirstSession.HistoricalRead=>Unit):Unit =
    root.reserveRetainedMemory(MySqlDbFirstSession.HistoricalRead.HolderBytes,"actual refusal historical holder BEFORE allocation")
    val read=new MySqlDbFirstSession.HistoricalRead(root,inputs)
    try
      read.acquire(None);body(read)
    finally read.close()

  @Test def actualHistoricalOwnerStrictlyAcceptsRefusalWithoutCountingSuccessfulRecall():Unit =
    withRefusalBefore(crossBoot=true) { (request,facts,before,_,root)=>
      val draft=MySqlDbFirstRecallRefusalMaterializer.allocate(request,facts,before,root)
      val result=MySqlDbFirstRecallRefusalMaterializer.materialize(draft)
      val domain=MySqlDbFirstRecallRefusalCanonical.Scope.getBytes(java.nio.charset.StandardCharsets.US_ASCII)
      val expected=java.nio.ByteBuffer.allocate(5+domain.length+128).putInt(domain.length).put(domain).put(1.toByte)
      def uuid(value:UUID):Unit={expected.putLong(value.getMostSignificantBits);expected.putLong(value.getLeastSignificantBits);()}
      uuid(request.tag.bootId);expected.putLong(request.tag.bootGeneration);expected.putLong(request.tag.connectionGeneration)
      uuid(request.tag.operationId);uuid(request.bindingUuid);uuid(request.podUuid);uuid(request.journalTag.operationId)
      uuid(facts.barrierGenerationUuid);expected.putLong(facts.connectionId);expected.putLong(facts.writerEpoch)
      assertEquals(expected.capacity(),expected.position())
      assertArrayEquals(expected.array(),draft.scope.copyBytes(root,"independent current refusal SQL scope prefix").take(expected.capacity()))
      val baseline=root.accountedMemoryBytes
      withRefusalHistoricalRead(root,before.inputs) { read=>
        read.validate(result.target)
        assertEquals(0L,read.dbFirstRecallSummary.verifiedCommitted,"refusal is not a successful original recall")
      }
      assertEquals(baseline,root.accountedMemoryBytes,"actual HistoricalRead holder and comparison children retire")
      val reserved=before.relations.map { relation=>
        if relation.table!="fp_recovery_operation" then relation else relation.copy(rows=(relation.rows :+ result.reservedOperation)
          .sortWith((a,b)=>MySqlControlledDebugDelta.compareKey(a,b,before.inputs.forTable(relation.table))<0))
      }
      val partial=MySqlScriptSchemaSnapshot.captureMaterialized(before.schema,before.connection,before.observationScope,
        before.maxAllowedPacketBytes,before.inputs,reserved,root)
      withRefusalHistoricalRead(root,before.inputs) { read=>
        assertThrows(classOf[SQLDataException],()=>read.validate(partial))
        assertThrows(classOf[IllegalStateException],()=>read.dbFirstRecallSummary)
      }
    }

  @Test def actualHistoricalOwnerRejectsSelfConsistentRefusalThatIllegallyChangesDomain():Unit =
    withRefusalBefore() { (request,facts,before,_,root)=>
      val draft=MySqlDbFirstRecallRefusalMaterializer.allocate(request,facts,before,root)
      val result=MySqlDbFirstRecallRefusalMaterializer.materialize(draft)
      def changeDomain(relations:Vector[MySqlFormat3RelationSnapshot])=relations.map { relation=>
        if relation.table!="floating_pod_state" then relation else relation.copy(rows=relation.rows.map(row=>
          row.updated(before.inputs.forTable(relation.table).columns.indexWhere(_.name=="pos_x"),
            MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(444D)))))
      }
      val c=MySqlDbFirstRecallRefusalCanonical
      val projected=c.projection(request,before,changeDomain(result.target.relations),true,root)
      val fingerprint=c.fingerprint(request,draft.scope,draft.preimage,projected,root)
      val context=c.context(request,facts,fingerprint,root)
      val witness=c.witness(request,draft.scope,draft.preimage,projected,fingerprint,context,root)
      val generic=c.operationContext(request,facts,fingerprint,root).canonicalFingerprintSha256
      val own=MySqlDbFirstRecallRefusalMaterializer.operation(request,facts,before,draft.scope,
        Some((context,draft.preimage,projected,witness,generic)),root)
      val rows=changeDomain(MySqlDbFirstRecallRefusalMaterializer.transition(before,request,own,
        MySqlDbFirstRecallRefusalMaterializer.binary(generic,root),root))
      val forged=MySqlScriptSchemaSnapshot.captureMaterialized(before.schema,before.connection,before.observationScope,
        before.maxAllowedPacketBytes,before.inputs,rows,root)
      // 外层SHA/完整规范帧确实自洽；实际owner必须进一步执行独立O/W业务重算。
      MySqlDbFirstRecallRefusalPersistedRead.requireCommittedReconstruction(forged,request.tag.operationId,root)
      withRefusalHistoricalRead(root,before.inputs) { read=>
        assertThrows(classOf[SQLDataException],()=>read.validate(forged))
        assertThrows(classOf[IllegalStateException],()=>read.dbFirstRecallSummary)
      }
    }

  @Test def actualPreCaptureExclusionHoldsOriginalIdentityAndCauseWithoutIssuingSqlProof():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(2801));val c=j.copy(operationId=id(2802))
      val reservation=f.core.reserveDbFirstRecallJournal(j,c,id(503),id(501),id(502),7L,Some(id(2803)))
      val root=f.core.validationBudget(reservation.handle);val baseline=f.shared.accountedBytes
      val originalCause=new IOException("original pre-capture failure")
      val excluded=f.core.excludeRecallBeforeCapture(reservation,originalCause)
      assertSame(originalCause,excluded.cause);assertEquals(j,excluded.journalTag);assertEquals(c,excluded.originalTag)
      assertEquals(baseline+MySqlDbFirstRecallPreCaptureExclusion.HolderBytes,f.shared.accountedBytes)
      f.core.requireRecallExcludedBeforeCapture(excluded)
      f.onWorker { f.core.requireRecallExcludedBeforeCapture(excluded) }
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure {
        f.core.requireConfirmedRecallExcludedBeforeCapture(excluded)
      })
      assertThrows(classOf[IllegalArgumentException],()=>f.core.excludeRecallBeforeCapture(reservation,new IOException("replacement")))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(c))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordMainDrop(reservation.handle))
      assertEquals(1,f.core.retainedRoots);assertFalse(root.isClosed)
      assertTrue(f.core.observation.failure.isEmpty);assertTrue(f.core.observation.plans.head.cause.isEmpty,
        "original C refusal cause must not poison still-running J SQL")
      // 封路不伪装J已确认，也不重试J；原次J仍可进入worker并持有其原责任。
      f.core.requireDbFirstRecallJournalSubmission(reservation)
      f.onWorker { f.core.beginDbFirstRecallJournal(reservation);() }
      f.core.submittedDbFirstRecallJournal(reservation,IoSubmission.Accepted)
      f.core.requireRecallExcludedBeforeCapture(excluded)
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure {
        f.core.requireConfirmedRecallExcludedBeforeCapture(excluded)
      })
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordRejectedJournalDrop(reservation))
      assertEquals(1,f.core.retainedRoots);assertFalse(root.isClosed)
    finally f.close()

  @Test def rebuiltPreCaptureExclusionAndWrongActorCannotBorrowRegisteredSeal():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(2811));val c=j.copy(operationId=id(2812))
      val reservation=f.core.reserveDbFirstRecallJournal(j,c,id(503),id(501),id(502),7L,None)
      val cause=new IOException("actual original cause")
      assertThrows(classOf[IllegalArgumentException],()=>f.core.excludeRecallBeforeCapture(reservation,null))
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure {
        f.core.excludeRecallBeforeCapture(reservation,cause);()
      })
      val excluded=f.core.excludeRecallBeforeCapture(reservation,cause)
      val baseline=f.shared.accountedBytes
      val forged=new MySqlDbFirstRecallPreCaptureExclusion(f.core,reservation,cause)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRecallExcludedBeforeCapture(forged))
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure {
        f.core.requireConfirmedRecallExcludedBeforeCapture(forged)
      })
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRecallExcludedBeforeCapture(null))
      val unregistered=new MySqlDbFirstRecallJournalReservation(f.core,reservation.handle,reservation.request)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.excludeRecallBeforeCapture(unregistered,cause))
      f.core.requireRecallExcludedBeforeCapture(excluded)
      assertEquals(baseline,f.shared.accountedBytes);assertEquals(1,f.core.retainedRoots)
    finally f.close()

  @Test def preCaptureExclusionAllocationFailureKeepsSealAndOriginalSlotWithoutRetry():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(2821));val c=j.copy(operationId=id(2822))
      val reservation=f.core.reserveDbFirstRecallJournal(j,c,id(503),id(501),id(502),7L,None)
      val root=f.core.validationBudget(reservation.handle)
      val pressure=root.availableMemoryBytes-256L
      root.reserveRetainedMemory(pressure,"test concurrent responsibility before exclusion holder")
      val failure=assertThrows(classOf[SQLDataException],()=>
        f.core.excludeRecallBeforeCapture(reservation,new IOException("original C cannot proceed")))
      root.releaseRetainedMemory(pressure,"test concurrent responsibility detached")
      val baseline=f.shared.accountedBytes
      assertSame(failure,assertThrows(classOf[SQLDataException],()=>
        f.core.excludeRecallBeforeCapture(reservation,new IOException("must not retry"))))
      val forged=MySqlDbFirstRecallJournalConfirmation.issue(f.core,reservation.handle,reservation.request,"0"*64)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.prepareJournalCaptureTransfer(reservation,forged))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(c))
      assertSame(failure,f.core.observation.plans.head.cause.get)
      assertEquals(baseline,f.shared.accountedBytes);assertEquals(1,f.core.retainedRoots);assertFalse(root.isClosed)
    finally f.close()

  @Test def journalSubmissionUnknownPreservesPreCaptureExclusionCauseAndOriginalClaim():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(2831));val c=j.copy(operationId=id(2832))
      val reservation=f.core.reserveDbFirstRecallJournal(j,c,id(503),id(501),id(502),7L,None)
      val root=f.core.validationBudget(reservation.handle)
      val originalCause=new IOException("original C admission failure")
      val excluded=f.core.excludeRecallBeforeCapture(reservation,originalCause)
      f.core.requireDbFirstRecallJournalSubmission(reservation)
      val unknown=new IOException("original J submission UNKNOWN")
      f.core.submissionUnknownDbFirstRecallJournal(reservation,unknown)
      assertSame(originalCause,excluded.cause)
      assertSame(unknown,assertThrows(classOf[IOException],()=>f.core.requireRecallExcludedBeforeCapture(excluded)))
      assertSame(unknown,assertThrows(classOf[IOException],()=>f.core.requireDbFirstRecallJournalSubmission(reservation)))
      assertSame(unknown,f.core.observation.failure.get)
      assertEquals(1,f.core.retainedRoots);assertFalse(root.isClosed)
      assertThrows(classOf[IOException],()=>f.core.reserveDbFirstCapture(c))
      assertThrows(classOf[IOException],()=>f.core.recordRejectedJournalDrop(reservation))
    finally f.close()

  @Test def actualExclusionCannotSubmitOrStartRefusalWithoutDurableJournalAndMainAck():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(2841));val c=j.copy(operationId=id(2842))
      val reservation=f.core.reserveDbFirstRecallJournal(j,c,id(503),id(501),id(502),7L,None)
      val root=f.core.validationBudget(reservation.handle)
      val excluded=f.core.excludeRecallBeforeCapture(reservation,new IOException("original C failure"))
      val baseline=f.shared.accountedBytes
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRecallRefusalSubmission(excluded))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.submittedRecallRefusal(excluded,IoSubmission.Accepted))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.submissionUnknownRecallRefusal(excluded,new IOException("not submitted")))
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure { f.core.beginRecallRefusal(excluded);() })
      f.core.requireDbFirstRecallJournalSubmission(reservation)
      f.onWorker { f.core.beginDbFirstRecallJournal(reservation);() }
      f.core.submittedDbFirstRecallJournal(reservation,IoSubmission.Accepted)
      val afterJournalStart=f.shared.accountedBytes
      assertTrue(afterJournalStart>baseline)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRecallRefusalSubmission(excluded))
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure { f.core.beginRecallRefusal(excluded);() })
      assertEquals(afterJournalStart,f.shared.accountedBytes)
      assertTrue(f.core.observation.failure.isEmpty);assertTrue(f.core.observation.plans.head.cause.isEmpty)
      assertEquals(1,f.core.retainedRoots);assertFalse(root.isClosed)
    finally f.close()

  @Test def forgedRefusalWorkerCannotMaterializePoisonOrClaimOriginalEntry():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(2851));val c=j.copy(operationId=id(2852))
      val reservation=f.core.reserveDbFirstRecallJournal(j,c,id(503),id(501),id(502),7L,Some(id(2853)))
      val root=f.core.validationBudget(reservation.handle)
      val excluded=f.core.excludeRecallBeforeCapture(reservation,new IOException("actual original cause"))
      val forged=new MySqlDbFirstRecallRefusalWork(excluded,root)
      val baseline=f.shared.accountedBytes
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure { f.core.requireExecutingRecallRefusal(forged) })
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure { forged.materialize(null,root,null);() })
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure { f.core.failRecallRefusal(forged,new IOException("foreign failure")) })
      assertFalse(forged.materializationAttempted);assertNull(forged.request);assertNull(forged.draft)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.submittedRecallRefusal(excluded,IoSubmission.RejectedCapacity))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(c))
      assertEquals(baseline,f.shared.accountedBytes);assertEquals(1,f.core.retainedRoots)
      assertTrue(f.core.observation.failure.isEmpty);assertTrue(f.core.observation.plans.head.cause.isEmpty)
    finally f.close()

  @Test def unregisteredRefusalRunnerCannotStartBorrowOrRetireOriginalResponsibility():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(2861));val c=j.copy(operationId=id(2862))
      val reservation=f.core.reserveDbFirstRecallJournal(j,c,id(503),id(501),id(502),7L,None)
      val root=f.core.validationBudget(reservation.handle)
      val excluded=f.core.excludeRecallBeforeCapture(reservation,new IOException("original C cannot proceed"))
      val forgedWork=new MySqlDbFirstRecallRefusalWork(excluded,root)
      val baseline=f.shared.accountedBytes
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure { forgedWork.beginTransaction();() })
      f.onWorker {
        val forgedRunner=new MySqlDbFirstRecallRefusalTransaction(forgedWork)
        assertThrows(classOf[IllegalArgumentException],()=>forgedRunner.requireRegisteredWorker())
        assertThrows(classOf[IllegalArgumentException],()=>forgedWork.requireTransaction(forgedRunner))
        assertThrows(classOf[IllegalArgumentException],()=>forgedRunner.retireConfirmedResources())
        assertThrows(classOf[IllegalArgumentException],()=>forgedRunner.confirmedMainReadSource(null))
        assertThrows(classOf[IllegalArgumentException],()=>forgedRunner.requireConfirmedObservation(
          MySqlDbFirstSession.DbFirstRecallRefusalObservation(c,j,id(502),true,true,true,"0"*64)))
        assertFalse(forgedRunner.commitAttempted);assertFalse(forgedRunner.acknowledged)
        assertFalse(forgedRunner.resourcesRetired);assertFalse(forgedRunner.readersPending)
      }
      assertEquals(baseline,f.shared.accountedBytes);assertEquals(1,f.core.retainedRoots);assertFalse(root.isClosed)
      assertTrue(f.core.observation.failure.isEmpty);assertTrue(f.core.observation.plans.head.cause.isEmpty)
    finally f.close()

  @Test def refusalReadAckRequiresActualSqlRetiredRunnerAndRegisteredConfirmation():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(2871));val c=j.copy(operationId=id(2872))
      val reservation=f.core.reserveDbFirstRecallJournal(j,c,id(503),id(501),id(502),7L,None)
      val root=f.core.validationBudget(reservation.handle)
      val exclusion=f.core.excludeRecallBeforeCapture(reservation,new IOException("original C failure"))
      val data=MySqlDbFirstRecallRefusalRequest.notApplied(c,reservation.request,root)
      val forged=MySqlDbFirstRecallRefusalConfirmation.issue(f.core,reservation.handle,data,"0"*64)
      assertEquals(j,forged.tag);assertEquals(c,forged.originalTag)
      val baseline=f.shared.accountedBytes
      val observation=MySqlDbFirstSession.DbFirstRecallRefusalObservation(c,j,id(502),true,true,true,"0"*64)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.claimRecallRefusalConfirmation(exclusion,observation))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRecallRefusalConfirmation(forged,exclusion))
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure { f.core.beginRefusalReadRelease(forged) })
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure { f.core.finishRefusalReadRelease(forged) })
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRefusalReadReleased(forged,exclusion))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.acknowledgeRefusalReadReleased(forged,exclusion))
      assertEquals(baseline,f.shared.accountedBytes);assertEquals(1,f.core.retainedRoots);assertFalse(root.isClosed)
      assertTrue(f.core.observation.failure.isEmpty);assertTrue(f.core.observation.plans.head.cause.isEmpty)
    finally f.close()

  @Test def refusalFinalCannotBeMintedOrRetiredFromEqualDataWithoutActualReadAck():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(2881));val c=j.copy(operationId=id(2882))
      val reservation=f.core.reserveDbFirstRecallJournal(j,c,id(503),id(501),id(502),7L,None)
      val root=f.core.validationBudget(reservation.handle)
      val exclusion=f.core.excludeRecallBeforeCapture(reservation,new IOException("original C failure"))
      val data=MySqlDbFirstRecallRefusalRequest.notApplied(c,reservation.request,root)
      val confirmation=MySqlDbFirstRecallRefusalConfirmation.issue(f.core,reservation.handle,data,"0"*64)
      val forged=MySqlDbFirstRecallRefusalFinalReceipt.issue(confirmation)
      assertEquals(j,forged.tag);assertEquals(c,forged.originalTag)
      assertEquals(confirmation.bindingUuid,forged.bindingUuid)
      assertEquals(confirmation.clientRequestUuid,forged.clientRequestUuid)
      val baseline=f.shared.accountedBytes
      assertThrows(classOf[IllegalArgumentException],()=>f.core.recordRefusalFinal(confirmation,exclusion))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRefusalFinalSubmission(forged))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.submittedRefusalFinal(forged,IoSubmission.Accepted))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.submissionUnknownRefusalFinal(forged,new IOException("foreign attempt")))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRefusalFinalRetired(forged))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.acknowledgeRefusalFinal(forged))
      f.onWorker {
        val jw=new MySqlDbFirstRecallJournalWork(reservation,root)
        val rw=new MySqlDbFirstRecallRefusalWork(exclusion,root)
        val jt=new MySqlDbFirstRecallJournalTransaction(jw)
        val rt=new MySqlDbFirstRecallRefusalTransaction(rw)
        assertThrows(classOf[IllegalArgumentException],()=>f.core.retireRefusalFinal(forged))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.failRefusalFinal(forged,new IOException("foreign final failure")))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRefusalFinalLocalRetirementFailure(forged,new IOException("foreign local failure")))
        assertThrows(classOf[IllegalArgumentException],()=>jt.retireRefusalFinalPayload(forged))
        assertThrows(classOf[IllegalArgumentException],()=>rt.retireRefusalFinalPayload(forged))
        assertThrows(classOf[IllegalArgumentException],()=>jw.retireRefusalFinalPayload(jt,forged))
        assertThrows(classOf[IllegalArgumentException],()=>rw.retireRefusalFinalPayload(rt,forged))
        assertSame(reservation,jw.reservation);assertSame(exclusion,rw.exclusion)
        assertSame(reservation,jt.reservation);assertSame(exclusion,rt.work.exclusion)
        assertEquals(reservation.tag,rt.work.exclusion.routeTag)
        assertFalse(jt.payloadRetired);assertFalse(rt.payloadRetired)
      }
      assertEquals(baseline,f.shared.accountedBytes);assertEquals(1,f.core.retainedRoots);assertFalse(root.isClosed)
      assertTrue(f.core.observation.failure.isEmpty);assertTrue(f.core.observation.plans.head.cause.isEmpty)
    finally f.close()

  private def withRecallIndexImage(source:MySqlScriptSchemaSnapshot)(
      body:(MySqlScriptSchemaSnapshot,MySqlFormat3ReadBudget,MySqlFormat3ReadBudget)=>Unit):Unit =
    val root=source.inputs.newAllocationRoot(source.maxAllowedPacketBytes,new MySqlFormat3AllocationLedger(source.inputs.sourceLimitProfile.maxRetainedBytes()))
    root.initialize(source.schema,source.connection,source.observationScope,source.maxAllowedPacketBytes)
    val child=root.newScopedReadBudget(source.maxAllowedPacketBytes)
    child.initialize(source.schema,source.connection,source.observationScope,source.maxAllowedPacketBytes)
    val data=root.newCanonicalDataScope()
    var primary:Throwable=null
    try
      val image=MySqlScriptSchemaSnapshot.decoded(source.schema,source.connection,source.observationScope,
        source.maxAllowedPacketBytes,source.inputs,copyPersistedRows(source.relations,child),child)
      body(image,root,data)
    catch case cause:Throwable=>
      primary=cause
      throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(()=>data.closeScopedReadBudget(),()=>child.closeScopedReadBudget(),()=>root.closeRootReadBudget()))

  @Test def durableRecallIndexKeepsAuthenticatedNamespaceAndOriginalCBeforeAnyRemint():Unit =
    for client<-Vector(None,Some(id(2703))) do
      withRefusalBefore(client=client) { (request,_,before,_,_)=>
        withRecallIndexImage(before) { (image,root,data)=>
          val index=MySqlDbFirstRecallRequestIndex.from(image,image.inputs,root,data)
          val entry=index.original(request.bindingUuid,request.actorUuid,request.originalTag.operationId).get
          assertEquals(request.originalTag,entry.originalTag);assertEquals(request.journalTag,entry.journalTag)
          assertEquals(request.podUuid,entry.podUuid);assertEquals(7L,entry.expectedStateVersion)
          assertEquals(client,entry.clientRequestUuid);assertEquals(MySqlDbFirstRecallRequestPhase.JournalCommitted,entry.phase)
          assertEquals(Vector(entry),index.census)
          val boot=new DbFirstBootRecallRequestCustody(Thread.currentThread(),index.census)
          assertSame(index.census,boot.originalRequests);assertEquals(1,boot.unresolvedCount)
          assertThrows(classOf[IllegalArgumentException],()=>boot.requireSettled())
          client.foreach { id=>
            assertSame(entry,index.client(request.bindingUuid,request.actorUuid,id).get)
            assertTrue(index.client(request.bindingUuid,new UUID(1L,1L),id).isEmpty)
            assertTrue(index.client(new UUID(1L,2L),request.actorUuid,id).isEmpty)
          }
          assertTrue(index.original(request.bindingUuid,request.actorUuid,request.journalTag.operationId).isEmpty)
          assertThrows(classOf[IllegalArgumentException],()=>index.client(request.bindingUuid,request.actorUuid,new UUID(0L,0L)))
        }
      }

  @Test def currentRecallLookupRejectsForeignNamespaceWithoutPoisoningActualReadAdmission():Unit =
    withRefusalBefore(client=Some(id(2703))) { (request,_,before,_,_)=>
      withRecallIndexImage(before) { (image,root,data)=>
        val index=MySqlDbFirstRecallRequestIndex.from(image,image.inputs,root,data)
        val actual=RecallRequestNamespace(request.bindingUuid,request.actorUuid)
        val reads=new MainDbFirstReadAdmission(Thread.currentThread())
        for foreign<-Vector(actual.copy(bindingUuid=id(2801)),actual.copy(actorUuid=id(2802))) do
          assertEquals(MySqlDbFirstRecallRequestLookup.NamespaceMismatch(actual),
            reads.withRead(MySqlDbFirstRecallRequestLookup.client(actual,foreign,id(2703),index)))
          assertEquals(MySqlDbFirstRecallRequestLookup.NamespaceMismatch(actual),
            reads.withRead(MySqlDbFirstRecallRequestLookup.original(actual,foreign,request.originalTag.operationId,index)))
        val entry=index.original(request.bindingUuid,request.actorUuid,request.originalTag.operationId).get
        assertEquals(MySqlDbFirstRecallRequestLookup.Found(actual,entry),
          reads.withRead(MySqlDbFirstRecallRequestLookup.client(actual,actual,id(2703),index)))
        reads.sealNewRequestsForPreStop()
        assertTrue(reads.preStopReadFailureAfterScopesEnded.isEmpty)
        reads.requirePreStopReadDrainSettled()
        assertThrows(classOf[MainDbFirstReadAdmission.Sealed],
          ()=>reads.withRead(MySqlDbFirstRecallRequestLookup.client(actual,actual,id(2703),index)))
      }
    }

  @Test def currentRecallLookupReturnsExactOriginalCAndReadOnlyMissingRatherThanJournalAlias():Unit =
    withRefusalBefore(client=Some(id(2703))) { (request,_,before,_,_)=>
      withRecallIndexImage(before) { (image,root,data)=>
        val index=MySqlDbFirstRecallRequestIndex.from(image,image.inputs,root,data)
        val actual=RecallRequestNamespace(request.bindingUuid,request.actorUuid)
        val entry=index.original(request.bindingUuid,request.actorUuid,request.originalTag.operationId).get
        assertEquals(MySqlDbFirstRecallRequestLookup.Found(actual,entry),
          MySqlDbFirstRecallRequestLookup.original(actual,actual,request.originalTag.operationId,index))
        assertEquals(MySqlDbFirstRecallRequestLookup.Found(actual,entry),
          MySqlDbFirstRecallRequestLookup.client(actual,actual,id(2703),index))
        assertEquals(MySqlDbFirstRecallRequestLookup.NotFound(actual),
          MySqlDbFirstRecallRequestLookup.original(actual,actual,request.journalTag.operationId,index))
        assertEquals(MySqlDbFirstRecallRequestLookup.NotFound(actual),
          MySqlDbFirstRecallRequestLookup.client(actual,actual,id(2899),index))
        val other=actual.copy(actorUuid=id(2802))
        assertEquals(MySqlDbFirstRecallRequestLookup.NotFound(other),
          MySqlDbFirstRecallRequestLookup.original(other,other,request.originalTag.operationId,index))
        assertSame(entry,index.client(request.bindingUuid,request.actorUuid,id(2703)).get)
        assertFalse(root.isClosed);assertFalse(data.isClosed)
      }
    }

  @Test def recallNamespaceAndLookupRejectUnusableIdentifiers():Unit =
    val zero=new UUID(0L,0L)
    assertThrows(classOf[IllegalArgumentException],()=>RecallRequestNamespace(zero,id(1)))
    assertThrows(classOf[IllegalArgumentException],()=>RecallRequestNamespace(id(1),zero))
    assertThrows(classOf[IllegalArgumentException],()=>RecallRequestNamespace(null,id(1)))
    assertThrows(classOf[IllegalArgumentException],()=>RecallRequestNamespace(id(1),null))
    withRefusalBefore(client=Some(id(2703))) { (request,_,before,_,_)=>
      withRecallIndexImage(before) { (image,root,data)=>
        val index=MySqlDbFirstRecallRequestIndex.from(image,image.inputs,root,data)
        val actual=RecallRequestNamespace(request.bindingUuid,request.actorUuid)
        assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstRecallRequestLookup.client(actual,actual,zero,index))
        assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstRecallRequestLookup.original(actual,actual,null,index))
        assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstRecallRequestLookup.client(actual,null,id(2703),index))
        assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstRecallRequestLookup.original(null,actual,request.originalTag.operationId,index))
      }
    }

  @Test def durableRefusedIndexPreservesOriginalCrossBootCAndOutlivesOldRawSources():Unit =
    withRefusalBefore(crossBoot=true) { (request,facts,before,source,oldRoot)=>
      val target=MySqlDbFirstRecallRefusalMaterializer.materialize(MySqlDbFirstRecallRefusalMaterializer.allocate(request,facts,before,oldRoot)).target
      withRecallIndexImage(target) { (image,root,data)=>
        val index=MySqlDbFirstRecallRequestIndex.from(image,image.inputs,root,data)
        oldRoot.closeRootReadBudget();source.allocationBudget.closeRootReadBudget()
        val entry=index.client(id(503),id(501),id(2703)).get
        assertEquals(MySqlDbFirstRecallRequestPhase.RefusedCommitted,entry.phase)
        assertSame(entry,index.census.head)
        val boot=new DbFirstBootRecallRequestCustody(Thread.currentThread(),index.census)
        assertThrows(classOf[IllegalArgumentException],()=>boot.requireSettled())
        assertEquals(request.originalTag,entry.originalTag);assertNotEquals(request.tag.bootId,entry.originalTag.bootId)
        assertSame(entry,index.original(id(503),id(501),request.originalTag.operationId).get)
        assertFalse(data.isClosed);assertFalse(root.isClosed)
      }
    }

  @Test def durableRecallIndexDistinguishesCommittedAndActuallyReservedOriginalC():Unit =
    for reserved<-Vector(false,true) do
      withJournalBoundRecall(Some(id(1903)),524288L) { (_,source,captured,root,journal)=>
        var backing:MySqlRepositoryRequestIdentity=null
        val request=MySqlDbFirstRecallRequest.capture(journal.originalTag,captured,root,Some(MySqlDbFirstRecallJournalLink.from(journal)))(b=>backing=b)
        val at=java.time.LocalDateTime.of(2026,10,29,12,0)
        val result=MySqlDbFirstRecallMaterializer.materialize(request,captured,backing,source,91L,id(1905),
          at.plusSeconds(2),at.plusSeconds(3),new MySqlDbFirstRecallDraft(root))
        val image=if !reserved then result.target else
          val rows=source.relations.map { r=>
            if r.table=="fp_recovery_operation" then r.copy(rows=r.rows:+result.reservedOperation)
            else if r.table=="physical_recovery_reservation" then r.copy(rows=Vector(result.heldReservation)) else r
          }
          MySqlScriptSchemaSnapshot.captureMaterialized(source.schema,source.connection,source.observationScope,
            source.maxAllowedPacketBytes,source.inputs,rows,root)
        withRecallIndexImage(image) { (actual,current,data)=>
          val assembled=MySqlBootMainReadData.fromDbFirst(actual,actual.inputs,current,data)
          val entry=assembled.recallRequests.get.client(journal.bindingUuid,journal.actorUuid,id(1903)).get
          assertEquals(if reserved then MySqlDbFirstRecallRequestPhase.OriginalCReserved else MySqlDbFirstRecallRequestPhase.RecallCommitted,entry.phase)
          assertTrue(entry.refusalReason.isEmpty)
          assertEquals(journal.originalTag,entry.originalTag)
          assertSame(entry,assembled.recallRequestCensus.head)
          assertSame(assembled.recallRequests.get.census,assembled.recallRequestCensus)
          val boot=new DbFirstBootRecallRequestCustody(Thread.currentThread(),assembled.recallRequestCensus)
          assertEquals(1,boot.unresolvedCount)
          assertEquals(!reserved,boot.mayRestoreResults(journal.bindingUuid))
          assertThrows(classOf[IllegalArgumentException],()=>boot.requireSettled())
        }
      }

  @Test def durableRecallIndexRejectsDuplicateClientNamespaceInsteadOfOverwritingOriginal():Unit =
    withRefusalBefore() { (request,_,before,source,root)=>
      val j=IoOperationTag(id(1),1L,1L,id(2891));val c=j.copy(operationId=id(2892))
      val second=MySqlDbFirstRecallJournalRequest.fresh(j,c,request.bindingUuid,request.actorUuid,request.podUuid,
        request.expectedStateVersion,request.clientRequestUuid,source.allocationBudget)
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      val target=MySqlDbFirstRecallJournalMaterializer.materialize(MySqlDbFirstRecallJournalMaterializer.allocate(second,
        MySqlDbFirstRecallJournalFacts(91L,id(2893),3L,at.plusHours(1),at,at.plusSeconds(1)),source,source.allocationBudget)).target
      val extra=target.relation("fp_recovery_operation").rows.find(r=>MySqlDbFirstRecallMaterializer.uuidEquals(
        cell(r,"fp_recovery_operation","operation_uuid",before.inputs),j.operationId)).get
      val independent=copyPersistedRows(Vector(target.relation("fp_recovery_operation").copy(rows=Vector(extra))),root).head.rows.head
      val rows=before.relations.map(r=>if r.table=="fp_recovery_operation" then r.copy(rows=r.rows:+independent) else r)
      val malformed=MySqlScriptSchemaSnapshot.captureMaterialized(before.schema,before.connection,before.observationScope,
        before.maxAllowedPacketBytes,before.inputs,rows,root)
      withRecallIndexImage(malformed) { (actual,current,data)=>
        val cause=assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRequestIndex.from(actual,actual.inputs,current,data))
        assertTrue(cause.getMessage.contains("duplicate authenticated client request namespace"))
        assertFalse(current.isClosed)
      }
    }

  @Test def durableRecallIndexRejectsTamperedCommittedJournalCarrier():Unit =
    withRefusalBefore() { (_,_,before,_,root)=>
      val shape=before.inputs.forTable("fp_recovery_operation")
      val rows=before.relations.map { r=>
        if r.table!="fp_recovery_operation" then r else r.copy(rows=r.rows.map(row=>row.updated(
          shape.columns.indexWhere(_.name=="workflow_uuid"),MySqlScriptSchemaDataTestFixture.binary(MySqlScriptSchemaDataTestFixture.uuidBytes(id(2899))))))
      }
      val malformed=MySqlScriptSchemaSnapshot.captureMaterialized(before.schema,before.connection,before.observationScope,
        before.maxAllowedPacketBytes,before.inputs,rows,root)
      withRecallIndexImage(malformed) { (actual,current,data)=>
        assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRequestIndex.from(actual,actual.inputs,current,data))
        assertFalse(current.isClosed)
      }
    }

  @Test def durableRecallIndexBudgetFailureDoesNotEscapePartialIndexOrCloseOriginalRead():Unit =
    withRefusalBefore() { (_,_,before,_,_)=>
      withRecallIndexImage(before) { (actual,root,data)=>
        val pressure=data.availableMemoryBytes-512L
        data.reserveRetainedMemory(pressure,"request metadata allocation pressure")
        val baseline=root.accountedMemoryBytes
        assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRequestIndex.from(actual,actual.inputs,root,data))
        assertEquals(baseline,root.accountedMemoryBytes)
        assertFalse(root.isClosed);assertFalse(actual.allocationBudget.isClosed)
        data.releaseRetainedMemory(pressure,"request metadata pressure removed")
      }
    }

  private final class Fixture(capacity: Int,deploymentInputs:Option[MySqlScriptSchemaInputs]=None,
      dbFirstInputs:Option[MySqlScriptSchemaInputs]=None):
    val limits = dbFirstInputs.orElse(deploymentInputs).map(_.sourceLimitProfile).getOrElse(
      SourceLimitProfile.of(1048576L, 65536L, 1048576L, 32L, 256L, 100000L, 65536L, 65536L))
    val shared = new MySqlFormat3AllocationLedger(limits.maxRetainedBytes())
    val identity = MySqlOwnedIoIdentity(id(1), 1L, 1L)
    private val actions = new ArrayBlockingQueue[Runnable](1)
    private val poison = new Runnable { override def run(): Unit = () }
    val worker = new Thread(() =>
      var running = true
      while running do
        val action = actions.take()
        if action eq poison then running = false else action.run()
      (), "retained-plan-core-worker")
    val core = new MySqlRetainedPlanReservationCore(identity, worker, Thread.currentThread(), limits, shared, capacity, None,deploymentInputs,dbFirstInputs)
    worker.start()
    def reserve(n: Long): MySqlOwnedRepositoryOperationPlan =
      core.reserveSingle(IoOperationTag(id(1), 1L, 1L, id(n)), id(n + 100), id(n + 200), MySqlRepositoryOperation.FindAll)
    def reserveDeployment(n: Long): MySqlOwnedRepositoryOperationPlan =
      core.reserveSingle(IoOperationTag(id(1), 1L, 1L, id(n)), id(n + 100), id(n + 200),
        MySqlRepositoryOperation.FindOriginalDeploymentOperation(id(71), id(72)))
    private def dispatch(action: => Unit): Option[Throwable] =
      val done = new CountDownLatch(1)
      val observed = new AtomicReference[Throwable]()
      val task = new Runnable:
        override def run(): Unit =
          try action
          catch case cause: Throwable => observed.set(cause)
          finally done.countDown()
      assertTrue(actions.offer(task, 3L, TimeUnit.SECONDS), "worker action slot unavailable")
      assertTrue(done.await(3L, TimeUnit.SECONDS), "worker action timed out; not passed")
      Option(observed.get())
    def onWorker(action: => Unit): Unit = dispatch(action).foreach(cause => throw cause)
    def onWorkerFailure(action: => Unit): Throwable = dispatch(action).getOrElse(throw new AssertionError("expected actual worker failure"))
    def close(): Unit =
      var primary: Throwable = null
      try
        assertTrue(actions.offer(poison, 3L, TimeUnit.SECONDS), "worker shutdown signal not accepted")
        worker.join(4000L)
        assertFalse(worker.isAlive, "actual core actor did not stop within deadline")
      catch
        case cause: Throwable =>
          primary = cause
          throw cause
      finally
        try
          if worker.isAlive then worker.interrupt()
          worker.join(4000L)
          assertFalse(worker.isAlive, "actual core actor cleanup leaked")
        catch
          case cleanup: Throwable =>
            if primary == null then throw cleanup
            else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  @Test def typedRefusalV2ReasonUsesIndependentExactBytesAndLeavesLegacyV1ExplicitlyUnknown():Unit =
    for client<-Vector(None,Some(id(2703)));reason<-RecallRefusalReason.values do
      withRefusalBefore(client=client,reason=Some(reason)) { (request,_,_,_,root)=>
        assertEquals(Some(reason),request.refusalReason)
        val buffer=new java.io.ByteArrayOutputStream();val out=new java.io.DataOutputStream(buffer)
        val domain="FP_DB_FIRST_RECALL_REFUSED_REQUEST_BE_V2".getBytes(java.nio.charset.StandardCharsets.US_ASCII)
        def uuid(value:UUID):Unit={out.writeLong(value.getMostSignificantBits);out.writeLong(value.getLeastSignificantBits)}
        def tag(value:IoOperationTag):Unit={uuid(value.bootId);out.writeLong(value.bootGeneration);out.writeLong(value.connectionGeneration);uuid(value.operationId)}
        out.writeInt(domain.length);out.write(domain);out.writeByte(2);out.writeByte(1)
        tag(request.tag);tag(request.originalTag);tag(request.journalTag)
        uuid(request.bindingUuid);uuid(request.actorUuid);uuid(request.podUuid);out.writeLong(request.expectedStateVersion)
        out.writeByte(if client.isEmpty then 0 else 1);client.foreach(uuid)
        out.writeInt(32);request.journalRequestSha256.foreach(b=>out.writeByte(b.toInt))
        out.writeByte(reason match
          case RecallRefusalReason.NotOwner=>1
          case RecallRefusalReason.VersionMismatch=>2
          case RecallRefusalReason.NotActive=>3
          case RecallRefusalReason.WrongDimension=>4
          case RecallRefusalReason.UnknownPod=>5
          case RecallRefusalReason.Busy=>6)
        val profile=root.profile.canonicalBytes();val hash=root.profile.sha256()
        out.writeInt(profile.length);out.write(profile);out.writeInt(hash.length);out.write(hash)
        assertArrayEquals(buffer.toByteArray,request.canonical.copyBytes(root,"V2 independent expected bytes"))
        val decoded=MySqlDbFirstRecallRefusalRequest.decode(request.canonical.borrowedBinary,root)
        assertEquals(Some(reason),decoded.refusalReason);assertTrue(decoded.canonical.exactlyEquals(request.canonical))
      }
    withRefusalBefore() { (legacy,_,_,_,root)=>
      assertTrue(legacy.refusalReason.isEmpty)
      assertTrue(MySqlDbFirstRecallRefusalRequest.decode(legacy.canonical.borrowedBinary,root).refusalReason.isEmpty)
    }

  @Test def typedRefusalV2RejectsUnknownReasonVersionAndNoncanonicalLengthWithoutFallback():Unit =
    withRefusalBefore(reason=Some(RecallRefusalReason.Busy)) { (request,_,_,_,root)=>
      val bytes=request.canonical.copyBytes(root,"V2 tamper source")
      val prefix=4+"FP_DB_FIRST_RECALL_REFUSED_REQUEST_BE_V2".length+2
      val reasonOffset=prefix+237+16
      def reject(changed:Array[Byte]):Unit =
        val input=MySqlFormat3Binary.copyOf(changed,root,"V2 tampered input")
        assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRefusalRequest.decode(input,root))
      for bad<-Vector(0,7,255) do
        val changed=bytes.clone();changed(reasonOffset)=bad.toByte;reject(changed)
      val version=bytes.clone();version(4+MySqlDbFirstRecallRefusalRequest.ReasonDomain.length)=1;reject(version)
      val domain=bytes.clone();domain(4+MySqlDbFirstRecallRefusalRequest.ReasonDomain.length-1)='3'.toByte;reject(domain)
      reject(bytes.dropRight(1));reject(bytes:+0.toByte)
      val legacyDomain=bytes.clone();legacyDomain(4+MySqlDbFirstRecallRefusalRequest.ReasonDomain.length-1)='1'.toByte
      legacyDomain(4+MySqlDbFirstRecallRefusalRequest.ReasonDomain.length)=1;reject(legacyDomain)
    }

  @Test def typedRefusalV2StrictHistoryAndColdIndexRecoverExactReasonRatherThanPhaseOrCauseText():Unit =
    for reason<-RecallRefusalReason.values do withRefusalBefore(crossBoot=true,reason=Some(reason)) { (request,facts,before,source,root)=>
      val target=MySqlDbFirstRecallRefusalMaterializer.materialize(MySqlDbFirstRecallRefusalMaterializer.allocate(request,facts,before,root)).target
      withRecallIndexImage(target) { (image,current,data)=>
        val index=MySqlDbFirstRecallRequestIndex.from(image,image.inputs,current,data)
        val entry=index.original(request.bindingUuid,request.actorUuid,request.originalTag.operationId).get
        root.closeRootReadBudget();source.allocationBudget.closeRootReadBudget()
        assertEquals(MySqlDbFirstRecallRequestPhase.RefusedCommitted,entry.phase)
        assertEquals(Some(reason),entry.refusalReason);assertSame(entry,index.census.head)
        assertFalse(data.isClosed);assertFalse(current.isClosed)
      }
    }

  @Test def registeredPreCaptureExclusionCarriesTypedReasonWithoutParsingOriginalCause():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val j=IoOperationTag(id(1),1L,1L,id(2901));val c=j.copy(operationId=id(2902))
      val reservation=f.core.reserveDbFirstRecallJournal(j,c,id(503),id(501),id(502),7L,None)
      val cause=new IOException("arbitrary private message says NotOwner but is not reason DATA")
      assertThrows(classOf[IllegalArgumentException],()=>f.core.excludeRecallBeforeCapture(reservation,cause,null))
      val excluded=f.core.excludeRecallBeforeCapture(reservation,cause,Some(RecallRefusalReason.Busy))
      assertSame(cause,excluded.cause);assertEquals(Some(RecallRefusalReason.Busy),excluded.refusalReason)
      f.core.requireRecallExcludedBeforeCapture(excluded)
      assertThrows(classOf[IllegalArgumentException],()=>f.core.excludeRecallBeforeCapture(reservation,cause,Some(RecallRefusalReason.NotOwner)))
      assertEquals(Some(RecallRefusalReason.Busy),excluded.refusalReason)
    finally f.close()

  @Test def refusalProtocolVersionNeverDependsOnBusinessStateVersion():Unit =
    for stateVersion<-Vector(1L,2L,7L);client<-Vector(None,Some(id(2703)));reason<-Vector(None,Some(RecallRefusalReason.Busy)) do
      withRefusalBefore(client=client) { (legacy,_,_,_,root)=>
        val journal=MySqlDbFirstRecallJournalRequest.fresh(legacy.journalTag,legacy.originalTag,legacy.bindingUuid,
          legacy.actorUuid,legacy.podUuid,stateVersion,client,root)
        val request=reason match
          case None=>MySqlDbFirstRecallRefusalRequest.notApplied(legacy.tag,journal,root)
          case Some(value)=>MySqlDbFirstRecallRefusalRequest.notAppliedWithReason(legacy.tag,journal,value,root)
        val decoded=MySqlDbFirstRecallRefusalRequest.decode(request.canonical.borrowedBinary,root)
        assertEquals(stateVersion,decoded.expectedStateVersion);assertEquals(reason,decoded.refusalReason)
        assertEquals(client,decoded.clientRequestUuid);assertTrue(decoded.canonical.exactlyEquals(request.canonical))
        assertEquals(if reason.isEmpty then 1 else 2,request.canonical.borrowedBinary.byteAt(4+MySqlDbFirstRecallRefusalRequest.Domain.length).toInt)
      }

  @Test def bootTakeoverStrictDataKeepsExactJAndRejectsPresentCOrChangedOriginalIdentity():Unit =
    for client<-Vector(None,Some(id(1903))) do withJournalBoundRecall(client,524288L,crossScope=true) { (_,source,captured,root,journal)=>
      val execution=IoOperationTag(source.observationScope.bootUuid,source.observationScope.bootGeneration,
        source.observationScope.connectionGeneration,journal.originalTag.operationId)
      val control=execution.copy(operationId=id(1990))
      def record(actor:UUID=journal.actorUuid,version:Long=journal.expectedStateVersion,digest:String=journal.canonical.hexSha256,
          phase:MySqlDbFirstRecallRequestPhase=MySqlDbFirstRecallRequestPhase.JournalCommitted)=
        new MySqlDbFirstRecallRequestRecord(journal.journalTag,journal.originalTag,journal.bindingUuid,actor,journal.podUuid,
          version,journal.clientRequestUuid,digest,phase)
      val intent=new MySqlDbFirstBootRecallTakeoverIntent(control,execution,record())
      val link=MySqlDbFirstBootRecallTakeoverData.strictLink(source,root,root,intent)
      assertEquals(MySqlDbFirstRecallJournalLink.forRecovery(journal,execution),link)
      assertEquals(journal.originalTag.operationId,execution.operationId)
      assertFalse(MySqlBootReadEpoch.mayChangeRepository(MySqlOwnedIoCommand.TakeoverBootRecall(intent)))
      for wrong<-Vector(record(actor=id(1991)),record(version=8L),record(digest="0"*64),
          record(phase=MySqlDbFirstRecallRequestPhase.OriginalCReserved)) do
        assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstBootRecallTakeoverData.strictLink(source,root,root,
          new MySqlDbFirstBootRecallTakeoverIntent(control,execution,wrong)))
      assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstBootRecallTakeoverData.strictLink(source,root,root,
        new MySqlDbFirstBootRecallTakeoverIntent(control,execution.copy(operationId=id(1992)),record())))
      var backing:MySqlRepositoryRequestIdentity=null
      val request=MySqlDbFirstRecallRequest.capture(execution,captured,root,Some(link))(b=>backing=b)
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      val committed=MySqlDbFirstRecallMaterializer.materialize(request,captured,backing,source,92L,id(1911),
        at.plusSeconds(2),at.plusSeconds(3),new MySqlDbFirstRecallDraft(root)).target
      assertThrows(classOf[java.sql.SQLDataException],()=>MySqlDbFirstBootRecallTakeoverData.strictLink(committed,root,root,intent))
    }

  @Test def bootRefusalStrictFreshDataSurvivesSourceRetirementAndStrictFullResultRecovery():Unit =
    for client<-Vector(None,Some(id(2703)));reason<-RecallRefusalReason.values do
      withRefusalBefore(client,crossBoot=true,reason=Some(reason)) { (old,facts,before,source,root)=>
        val original=new MySqlDbFirstRecallRequestRecord(old.journalTag,old.originalTag,old.bindingUuid,
          old.actorUuid,old.podUuid,old.expectedStateVersion,old.clientRequestUuid,
          java.util.HexFormat.of().withUpperCase().formatHex(old.journalRequestSha256.toArray),MySqlDbFirstRecallRequestPhase.JournalCommitted)
        val intent=new MySqlDbFirstBootRecallTakeoverIntent(old.tag.copy(operationId=id(2990)),old.tag,original)
        val request=MySqlDbFirstBootRecallTakeoverData.strictRefusalRequest(before,root,root,intent,reason)
        assertEquals(old.originalTag,request.originalTag);assertEquals(old.journalTag,request.journalTag)
        assertEquals(old.tag,request.tag);assertEquals(old.journalRequestSha256,request.journalRequestSha256)
        assertEquals(client,request.clientRequestUuid);assertEquals(Some(reason),request.refusalReason)
        assertTrue(old.canonical.exactlyEquals(request.canonical),"recovered metadata encoding preserves existing V2 bytes")
        assertThrows(classOf[IllegalArgumentException],()=>
          MySqlDbFirstBootRecallTakeoverData.strictRefusalRequest(before,root,root,intent,null))
        assertThrows(classOf[IllegalArgumentException],()=>
          MySqlDbFirstBootRecallTakeoverData.strictRefusalRequest(before,root,root,
            new MySqlDbFirstBootRecallTakeoverIntent(intent.controlTag,old.tag.copy(operationId=id(2991)),original),reason))
        val target=MySqlDbFirstRecallRefusalMaterializer.materialize(
          MySqlDbFirstRecallRefusalMaterializer.allocate(request,facts,before,root)).target
        assertThrows(classOf[java.sql.SQLDataException],()=>
          MySqlDbFirstBootRecallTakeoverData.strictRefusalRequest(target,root,root,intent,reason))
        withRecallIndexImage(target) { (image,current,data)=>
          val index=MySqlDbFirstRecallRequestIndex.from(image,image.inputs,current,data)
          val entry=index.original(request.bindingUuid,request.actorUuid,request.originalTag.operationId).get
          root.closeRootReadBudget();source.allocationBudget.closeRootReadBudget()
          assertEquals(MySqlDbFirstRecallRequestPhase.RefusedCommitted,entry.phase)
          assertEquals(Some(reason),entry.refusalReason);assertEquals(client,entry.clientRequestUuid)
          assertEquals(original.originalTag,entry.originalTag);assertEquals(original.journalTag,entry.journalTag)
          assertEquals(original.journalRequestSha256,entry.journalRequestSha256)
          assertFalse(data.isClosed);assertFalse(current.isClosed)
        }
      }

  @Test def bootMainDecisionMetadataOrMissingActualIssuerCannotAuthorizeOriginalEntry():Unit =
    val f=new Fixture(1,dbFirstInputs=Some(dbFirstMaterializationInputs()))
    try
      val execution=IoOperationTag(id(1),1L,1L,id(2993))
      val reservation=f.core.reserveDbFirstCapture(execution)
      val old=execution.copy(bootId=id(2994))
      val original=new MySqlDbFirstRecallRequestRecord(old.copy(operationId=id(2995)),old,id(503),id(501),id(502),
        7L,None,"A"*64,MySqlDbFirstRecallRequestPhase.JournalCommitted)
      val intent=new MySqlDbFirstBootRecallTakeoverIntent(execution.copy(operationId=id(2996)),execution,original)
      for reason<-Vector(None,Some(RecallRefusalReason.Busy)) do
        val forged=new MySqlDbFirstBootRecallMainDecision(f.core,intent,reason,reservation.handle)
        assertThrows(classOf[IllegalArgumentException],()=>f.core.requireBootRecallMainDecision(forged))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.activateBootRecallCapture(null,forged,reservation))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.requireRecallRefusalSubmission(forged))
        assertThrows(classOf[IllegalArgumentException],()=>f.core.finishBootRecallMainDecision(null,intent,reservation,reason))
      assertThrows(classOf[IllegalArgumentException],()=>f.core.beginBootRecallMainDecision(null,intent,reservation))
      f.core.requireDbFirstCaptureReservation(reservation) // rejected forgery never changes an unrelated real capture.
      assertThrows(classOf[IllegalArgumentException],()=>f.core.requireStoppedEmptyForTerminal())
    finally f.close()

  @Test def unqualifiedBootWorkCannotRegisterInactiveReservationOrPretendTerminalEmpty():Unit =
    val inputs=dbFirstMaterializationInputs()
    val f=new Fixture(1,dbFirstInputs=Some(inputs))
    try
      val execution=IoOperationTag(id(1),1L,1L,id(1994))
      val old=execution.copy(bootId=id(1995),operationId=id(1996))
      val record=new MySqlDbFirstRecallRequestRecord(old,old.copy(operationId=execution.operationId),id(503),id(501),id(502),
        7L,None,"a"*64,MySqlDbFirstRecallRequestPhase.JournalCommitted)
      val intent=new MySqlDbFirstBootRecallTakeoverIntent(execution.copy(operationId=id(1997)),execution,record)
      var work:MySqlDbFirstBootRecallTakeoverWork=null
      f.onWorker { work=f.core.beginBootRecallTakeover(intent) }
      // 等值C metadata/伪造handle不能把尚未完成的真实Boot Work兑换成退休资格。
      val forgedHandle=new MySqlRetainedPlanHandle(null,Long.MaxValue,execution,f.core)
      val forgedDrop=MySqlDbFirstRecallDropReceipt.issue(f.core,forgedHandle)
      f.onWorker {
        val before=f.shared.accountedBytes
        assertThrows(classOf[IllegalArgumentException],()=>f.core.requireBootRecallFinalPayload(forgedDrop,work))
        assertThrows(classOf[IllegalArgumentException],()=>work.retireRecallMetadata(forgedDrop))
        assertFalse(work.isMetadataRetired);assertFalse(work.root.isClosed)
        assertEquals(before,f.shared.accountedBytes)
      }
      assertInstanceOf(classOf[IllegalArgumentException],f.onWorkerFailure { f.core.finishBootRecallTakeover(work);() })
      assertThrows(classOf[IllegalArgumentException],()=>f.core.reserveDbFirstCapture(execution))
      f.core.stop()
      assertInstanceOf(classOf[IllegalStateException],f.onWorkerFailure(f.core.requireStoppedEmptyForTerminal()))
    finally f.close()

  @Test def recoveredRecallV3ColdHistoryAndIndexKeepOriginalRequestWithCurrentExecutionScope():Unit =
    for client<-Vector(None,Some(id(1903))) do withJournalBoundRecall(client,524288L,crossScope=true) { (anchor,source,captured,root,journal)=>
      val execution=IoOperationTag(source.observationScope.bootUuid,source.observationScope.bootGeneration,
        source.observationScope.connectionGeneration,journal.originalTag.operationId)
      val link=MySqlDbFirstRecallJournalLink.forRecovery(journal,execution)
      assertEquals(Some(journal.originalTag),link.recoveredOriginal)
      var backing:MySqlRepositoryRequestIdentity=null
      val request=MySqlDbFirstRecallRequest.capture(execution,captured,root,Some(link))(b=>backing=b)
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      val result=MySqlDbFirstRecallMaterializer.materialize(request,captured,backing,source,92L,id(1911),
        at.plusSeconds(2),at.plusSeconds(3),new MySqlDbFirstRecallDraft(root))
      result.verify()
      assertEquals(execution,request.tag)
      assertEquals(journal.originalTag.operationId,request.tag.operationId)
      assertNotEquals(journal.originalTag.bootId,request.tag.bootId)
      assertEquals(MySqlDbFirstRecallCanonical.RecoveryScope,MySqlDbFirstRecallCanonical.domain(request,MySqlDbFirstRecallCanonical.Scope))
      withRecallIndexImage(result.target) { (image,current,data)=>
        val assembled=MySqlBootMainReadData.fromDbFirst(image,image.inputs,current,data)
        val original=assembled.recallRequestCensus.head
        assertEquals(journal.originalTag,original.originalTag)
        assertEquals(journal.journalTag,original.journalTag)
        assertEquals(client,original.clientRequestUuid)
        assertEquals(MySqlDbFirstRecallRequestPhase.RecallCommitted,original.phase)
      }
      val cold=source.inputs.newAllocationRoot(source.maxAllowedPacketBytes,new MySqlFormat3AllocationLedger(root.profile.maxRetainedBytes()))
      val later=MySqlFormat3ObservationScope(id(1999),13L,17L)
      cold.initialize(source.schema,source.connection,later,source.maxAllowedPacketBytes)
      val child=cold.newScopedReadBudget(source.maxAllowedPacketBytes)
      child.initialize(source.schema,source.connection,later,source.maxAllowedPacketBytes)
      try
        val image=MySqlScriptSchemaSnapshot.decoded(source.schema,source.connection,later,source.maxAllowedPacketBytes,
          source.inputs,copyPersistedRows(result.target.relations,child),child)
        val prior=MySqlScriptSchemaSnapshot.decoded(source.schema,source.connection,later,source.maxAllowedPacketBytes,
          source.inputs,copyPersistedRows(source.relations,child),child)
        anchor.allocationBudget.closeRootReadBudget();root.closeRootReadBudget()
        val baseline=cold.accountedMemoryBytes
        for i<-0 until 2 do
          MySqlDbFirstRecallPersistedRead.verifyCommitted(image,execution.operationId,cold)
          assertEquals(1L,MySqlDbFirstRecallPersistedRead.verifySnapshotReceiptSegment(image,prior,cold))
        assertEquals(baseline,cold.accountedMemoryBytes)
        cold.reserveRetainedMemory(MySqlDbFirstSession.HistoricalRead.HolderBytes,"V3 historical holder BEFORE allocation")
        val history=new MySqlDbFirstSession.HistoricalRead(cold,source.inputs)
        try
          history.acquire(None);history.validate(image)
          assertEquals(1L,history.dbFirstRecallSummary.verifiedCommitted)
        finally history.close()
        assertEquals(baseline,cold.accountedMemoryBytes)
      finally
        child.closeScopedReadBudget();cold.closeRootReadBudget()
    }

  @Test def recoveredRecallV3RejectsRemintOriginalIdentityLossAndV2ScopeReinterpretation():Unit =
    withJournalBoundRecall(Some(id(1903)),524288L,crossScope=true) { (_,source,captured,root,journal)=>
      val execution=IoOperationTag(source.observationScope.bootUuid,9L,11L,journal.originalTag.operationId)
      assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstRecallJournalLink.forRecovery(journal,execution.copy(operationId=id(1919))))
      assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstRecallJournalLink.forRecovery(journal,journal.originalTag))
      val v2=MySqlDbFirstRecallJournalLink.from(journal)
      assertThrows(classOf[IllegalArgumentException],()=>MySqlDbFirstRecallRequest.capture(execution,captured,root,Some(v2))(_=>()))
      val link=MySqlDbFirstRecallJournalLink.forRecovery(journal,execution)
      for wrong<-Vector(link.copy(recoveredOriginal=None),link.copy(recoveredOriginal=Some(execution)),
          link.copy(journalTag=link.journalTag.copy(operationId=id(1920))),link.copy(clientRequestUuid=None),
          link.copy(journalRequestSha256=Vector.fill(32)(0.toByte))) do
        assertThrows(classOf[IllegalArgumentException],()=>wrong.requireJournalIdentity(journal,execution))
      // 当前writer/observation仍是硬边界，不能拿完整原J/C identity让另一execution通过。
      var backing:MySqlRepositoryRequestIdentity=null
      val request=MySqlDbFirstRecallRequest.capture(execution,captured,root,Some(link))(b=>backing=b)
      val wrong=new MySqlDbFirstRecallRequest(execution.copy(bootId=id(1921)),request.bindingUuid,request.podUuid,request.ownerUuid,
        request.attemptUuid,request.capturedStateVersion,request.snapshotFormatVersion,request.nbtByteLength,request.nbtSha256,
        request.profileBytes,request.profileSha256,request.pose,request.identity,Some(link))
      val at=java.time.LocalDateTime.of(2026,10,29,12,0)
      assertThrows(classOf[java.sql.SQLDataException],()=>MySqlDbFirstRecallMaterializer.materialize(wrong,captured,backing,
        source,92L,id(1911),at.plusSeconds(2),at.plusSeconds(3),new MySqlDbFirstRecallDraft(root)))
    }
