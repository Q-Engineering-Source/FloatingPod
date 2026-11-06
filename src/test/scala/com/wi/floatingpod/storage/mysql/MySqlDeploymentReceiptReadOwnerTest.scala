package com.wi.floatingpod.storage.mysql

import java.sql.{ResultSetMetaData, SQLDataException, Types}
import javax.sql.rowset.RowSetMetaDataImpl
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

final class MySqlDeploymentReceiptReadOwnerTest:
  @TempDir var temporary: Path = _
  private def metadata(table: String): RowSetMetaDataImpl =
    val shape = MySqlDeploymentReceiptRelationShapes.forTable(table)
    val value = new RowSetMetaDataImpl()
    value.setColumnCount(shape.columns.size)
    shape.columns.zipWithIndex.foreach { (column, offset) =>
      val i = offset + 1
      value.setColumnLabel(i, column.name)
      value.setColumnType(i, if column.sqlDeclaration.startsWith("MEDIUMBLOB") then Types.LONGVARBINARY else column.jdbcType)
      value.setNullable(i, if column.nullable then ResultSetMetaData.columnNullable else ResultSetMetaData.columnNoNulls)
      if column.kind == MySqlFormat3SqlKind.DateTime then value.setScale(i, 6)
    }
    value

  @Test def acceptsAllFrozen26MetadataAndMeasuresActualSql(): Unit =
    val shapes = MySqlDeploymentReceiptRelationShapes.canonicalOrder
    assertEquals(26, shapes.size)
    assertEquals(467, shapes.map(_.columns.size).sum)
    shapes.foreach { shape =>
      MySqlDeploymentReceiptReadOwner.validateMetadata(metadata(shape.table), shape.table)
      val sql = MySqlDeploymentReceiptReadOwner.select("fp_receipt_current", shape.table)
      assertEquals(MySqlFormat3FullSnapshotSql.utf8Length(sql), MySqlDeploymentReceiptReadOwner.selectLength("fp_receipt_current", shape))
    }

  @Test def rejectsWrongDeploymentSmallintAndMissingColumn(): Unit =
    val shape = MySqlDeploymentReceiptRelationShapes.operation
    val wrong = metadata(shape.table)
    val index = shape.columns.indexWhere(_.sqlDeclaration.startsWith("SMALLINT")) + 1
    assertTrue(index > 0)
    wrong.setColumnType(index, Types.INTEGER)
    assertThrows(classOf[SQLDataException], () => MySqlDeploymentReceiptReadOwner.validateMetadata(wrong, shape.table))
    val missing = metadata(shape.table)
    missing.setColumnCount(shape.columns.size - 1)
    assertThrows(classOf[SQLDataException], () => MySqlDeploymentReceiptReadOwner.validateMetadata(missing, shape.table))

  @Test def rejectsWrongNullableAndDateTimePrecision(): Unit =
    val shape = MySqlDeploymentReceiptRelationShapes.canonicalOrder.find(_.columns.exists(_.kind == MySqlFormat3SqlKind.DateTime)).get
    val i = shape.columns.indexWhere(_.kind == MySqlFormat3SqlKind.DateTime) + 1
    val wrong = metadata(shape.table)
    wrong.setScale(i, 3)
    assertThrows(classOf[SQLDataException], () => MySqlDeploymentReceiptReadOwner.validateMetadata(wrong, shape.table))
    val nullable = metadata(MySqlDeploymentReceiptRelationShapes.quota.table)
    nullable.setNullable(1, ResultSetMetaData.columnNullable)
    assertThrows(classOf[SQLDataException], () => MySqlDeploymentReceiptReadOwner.validateMetadata(nullable, MySqlDeploymentReceiptRelationShapes.quota.table))

  @Test def fixedRootLockRejectsArbitrarySqlIdentifiers(): Unit =
    assertEquals("SELECT `singleton_fixed1` FROM `fp_receipt_current`.`fp_schema_manifest` ORDER BY `singleton_fixed1` FOR UPDATE",
      MySqlDeploymentReceiptReadOwner.rootLockSql("fp_receipt_current", "fp_schema_manifest"))
    assertThrows(classOf[IllegalArgumentException], () => MySqlDeploymentReceiptReadOwner.rootLockSql("fp_receipt_current", "deployment_operation"))
    assertThrows(classOf[IllegalArgumentException], () => MySqlDeploymentReceiptReadOwner.select("x`;DROP", "fp_schema_manifest"))
    assertThrows(classOf[IllegalArgumentException], () => MySqlDeploymentReceiptReadOwner.select("fp_receipt_current", "caller_table"))

  @Test def rollbackAndRestoreUncertaintyCannotBecomeSettledOrPermitRestoration(): Unit =
    val rollback=new MySqlDeploymentReceiptReadOwner.ReadSettlement
    val cause=new java.sql.SQLException("original rollback failure")
    rollback.failed(cause)
    assertTrue(rollback.isUncertain);assertFalse(rollback.isSettled)
    assertSame(cause,rollback.failure.get)
    assertThrows(classOf[IllegalStateException],()=>rollback.requireRestorable())
    assertThrows(classOf[IllegalStateException],()=>rollback.rollbackReturned())
    val restore=new MySqlDeploymentReceiptReadOwner.ReadSettlement
    restore.rollbackReturned();restore.requireRestorable()
    val secondary=new java.sql.SQLException("actual restoration failure")
    restore.failed(secondary)
    assertSame(secondary,restore.failure.get);assertFalse(restore.isSettled)
    assertThrows(classOf[IllegalStateException],()=>restore.restorationReturned())

  @Test def assemblyFull26ReadChildFeedsActualCompositionAndWitnessWithoutCopyingRoot(): Unit =
    import MySqlScriptSchemaDataTestFixture.*
    import com.wi.floatingpod.server.{B1ResumeRecord,WindowsWorldProvisionReceiptDataCodec,WindowsWorldProvisioner}
    import com.wi.floatingpod.server.B1ResumeRecord.*
    import java.util.{UUID,List as JList,HexFormat}
    import java.time.LocalDateTime
    val f=DeploymentReceiptStaticTestData.fixture(temporary.resolve("actual-root-body-chain").toAbsolutePath)
    val profile=f.profile.sourceLimitProfile
    val ledger=new MySqlFormat3AllocationLedger(profile.maxRetainedBytes())
    val root=MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(profile,profile.maxSqlPacketBytes(),ledger)
    val assembly=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val boot=UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c7c1")
    var inputs: MySqlDeploymentReceiptSessionInputs=null
    var image: MySqlFormat3ReadBudget=null
    var target: MySqlScriptSchemaSnapshot=null
    var body: MySqlFull26WitnessCodec.Owner=null
    try
      val graph=MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata,assembly,DeploymentReceiptStaticTestData.policy)
      inputs=MySqlRuntimeInputs.assembleSessionInputs(graph,root,MySqlOwnedIoIdentity(boot,1L,1L),MySqlDeploymentReceiptSessionInputs.Fresh)
      val i=inputs.full26Inputs
      root.requireScriptDataInputs(i)
      val recipe=inputs.recipeBytes
      val fields=new String(recipe,java.nio.charset.StandardCharsets.UTF_8).split("\n").drop(1).map { line =>
        val n=line.indexOf('=');line.substring(0,n)->line.substring(n+1)
      }.toMap
      def id(name: String)=UUID.fromString(fields(name))
      val plan=new WindowsWorldProvisionReceiptDataCodec.PlanData(id("creationOperationUuid"),id("bootstrapOperationUuid"),
        id("prepareWriteUuid"),id("certifyWriteUuid"),id("bindingUuid"),2,id("mysqlServerUuid"),i.schema,5,i.ddlSha256,id("provisioningUuid"))
      inputs.requirePlanData(plan)
      val now=LocalDateTime.of(2026,10,23,12,0)
      val connection=Connection.copy(serverUuid=plan.mysqlServerUuid(),catalog=i.schema)
      val scope=Scope.copy(bootUuid=boot)
      image=root.issueDeploymentReceiptSnapshotImage(i,i.schema,connection,scope,i.budget.packetBytes)
      var rows=full26GenesisRelations(i,catalogPresent=false).map { relation =>
        val shape=i.forTable(relation.table)
        if Set("physical_recovery_binding","binding_bootstrap_intent").contains(relation.table) then relation.copy(rows=Vector.empty)
        else relation.copy(rows=relation.rows.map { row => shape.columns.zip(row).map { (column,cell) =>
          if Set("binding_uuid","expected_binding_uuid","authority_binding_uuid").contains(column.name) then binary(uuidBytes(plan.bindingUuid()))
          else cell
        }})
      }
      def update(table: String,name: String,value: MySqlFormat3Cell): Unit =
        val n=i.forTable(table).columns.indexWhere(_.name==name)
        rows=rows.map(r=>if r.table==table then r.copy(rows=r.rows.map(_.updated(n,value))) else r)
      update("fp_schema_manifest","expected_server_uuid",binary(uuidBytes(plan.mysqlServerUuid())))
      update("fp_schema_manifest","provisioning_uuid",binary(uuidBytes(plan.provisioningUuid())))
      update("fp_schema_manifest","configuration_sha256",binary(HexFormat.of().parseHex(inputs.configurationSha256)))
      update("fp_schema_manifest","source_provenance_bytes",binary(recipe))
      update("fp_schema_manifest","source_provenance_sha256",binary(digest(recipe)))
      update("fp_schema_manifest","limit_profile_bytes",binary(profile.canonicalBytes()))
      update("fp_schema_manifest","limit_profile_sha256",binary(profile.sha256()))
      Vector("max_region_bytes"->profile.maxRegionBytes(),"max_chunk_bytes"->profile.maxChunkBytes(),
        "max_decompressed_bytes"->profile.maxDecompressedBytes(),"max_nbt_depth"->profile.maxNbtDepth(),
        "max_members"->profile.maxMembers(),"max_retained_bytes"->profile.maxRetainedBytes(),
        "max_sql_packet_bytes"->profile.maxSqlPacketBytes(),"max_native_artifact_bytes"->profile.maxNativeArtifactBytes())
        .foreach((name,value)=>update("fp_schema_manifest",name,MySqlFormat3Cell.SignedInteger(value)))
      update("fp_runtime_writer","lease_until",MySqlFormat3Cell.DateTime(now))
      val before=MySqlScriptSchemaSnapshot.captureMaterialized(i.schema,connection,scope,i.budget.packetBytes,i,rows,image)
      val receipt=WindowsWorldProvisionReceiptDataCodec.encode(new WindowsWorldProvisionReceiptDataCodec.ReceiptData(plan,
        "FP_WIN32_FILE_ID_V1;volume=0000000000000001;id=01010101010101010101010101010101",
        "FP_WIN32_FILE_ID_V1;volume=0000000000000001;id=02020202020202020202020202020202",1234L,133800000000000123L,
        new WindowsWorldProvisioner.BootIdentity(boot,1L)))
      val sourceIdentity=WindowsWorldProvisionReceiptDataCodec.decode(receipt).sourceIdentity()
      val seed=MySqlBusinessWriterRow(plan.bindingUuid(),0L,None,now,None,None,now)
      val targetWriter=MySqlFreshFormat3WriterTarget(plan.bindingUuid(),1L,Some(boot),now.plusMinutes(1),plan.prepareWriteUuid())
      target=MySqlDeploymentReceiptFreshPrepareComposition.prepareTarget(before,i,root,plan,sourceIdentity,digest(receipt).toVector,seed,targetWriter)
      MySqlDeploymentReceiptFreshPrepareComposition.requirePrepareTransition(before,target,i,root,plan,sourceIdentity,digest(receipt).toVector,seed,targetWriter)
      val immutable=MySqlOwnedFormat3Snapshot.fromDeploymentReceipt(target,i,root)
      immutable.requireCompleteFamily()
      assertEquals(i.canonicalOrder.map(_.table),immutable.relations.map(_.table))
      assertEquals(26,immutable.relations.size)
      assertEquals(target.rowCount,immutable.rowCount)
      i.canonicalOrder.drop(20).foreach { shape =>
        assertEquals(target.relation(shape.table).rows.size,immutable.relation(shape.table).rows.size)
      }
      assertThrows(classOf[IllegalArgumentException],()=>immutable.requireBootReadData)
      // 相同ledger也不能把另一family/root当成这个已绑定的完整26原来源。
      val foreignRoot=MySqlFormat3ReadBudget.rootOnLedger(profile,profile.maxSqlPacketBytes(),ledger)
      try
        assertThrows(classOf[IllegalArgumentException],()=>MySqlOwnedFormat3Snapshot.fromDeploymentReceipt(target,i,foreignRoot))
      finally foreignRoot.closeRootReadBudget()
      // 生产ReadOwner在实际rollback/restore后保留同一image；此处只测非权限状态与原DATA链。
      val settled=new MySqlDeploymentReceiptReadOwner.ReadSettlement
      settled.rollbackReturned();settled.restorationReturned()
      assertTrue(settled.isSettled);assertFalse(image.isClosed)
      val bg=MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(before,i,root)
      val tg=MySqlB1CommonGraph.captureDeploymentReceiptFull26Data(target,i,root)
      def bytes(cell: MySqlFormat3Cell): Array[Byte]=cell match
        case MySqlFormat3Cell.Binary(value)=>Array.tabulate(value.length)(value.byteAt)
        case _=>throw new AssertionError("actual binary required")
      val fingerprint=bytes(target.relation("fp_runtime_writer").rows.head(i.forTable("fp_runtime_writer").columns.indexWhere(_.name=="last_commit_fingerprint")))
      val namespace=MySqlSessionBarrierNamespace.forIdentity(plan.mysqlServerUuid(),i.schema,plan.provisioningUuid(),plan.bindingUuid()).lockName()
      val beforeWriter=new WriterAssertions(plan.bindingUuid(),0L,null,now,null)
      val afterWriter=new WriterAssertions(plan.bindingUuid(),1L,boot,targetWriter.leaseUntil,
        new ReceiptSlotAssertions(boot,1L,plan.bindingUuid(),plan.prepareWriteUuid(),fingerprint))
      // 历史HANDOFF必须复用同一编码；原slot跨boot/epoch保留，不补造历史数据库时间。
      val handoffBoot=UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c721")
      val handoffAttempt=UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c722")
      val handoffOperation=UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c723")
      val liveBefore=MySqlBusinessWriterRow(plan.bindingUuid(),1L,Some(boot),targetWriter.leaseUntil,
        Some(plan.prepareWriteUuid()),Some(fingerprint.toVector),now)
      val liveAfter=liveBefore.copy(writerEpoch=2L,ownerBootUuid=Some(handoffBoot),leaseUntil=now.plusMinutes(2))
      val historicalAfter=new WriterAssertions(plan.bindingUuid(),2L,handoffBoot,liveAfter.leaseUntil,afterWriter.lastSlot())
      val liveEffect=MySqlOrdinaryHandoffGraph.effectFingerprint(liveBefore,liveAfter,handoffAttempt,handoffOperation,root)
      val historicalEffect=MySqlOrdinaryHandoffGraph.effectFingerprint(afterWriter,historicalAfter,handoffAttempt,handoffOperation,root)
      assertEquals(liveEffect,historicalEffect)
      assertEquals(liveEffect,MySqlOrdinaryHandoffGraph.effectFingerprint(liveBefore.copy(databaseNow=now.plusDays(1)),
        liveAfter.copy(databaseNow=now.plusDays(1)),handoffAttempt,handoffOperation,root))
      assertNotEquals(historicalEffect,MySqlOrdinaryHandoffGraph.effectFingerprint(afterWriter,historicalAfter,
        handoffAttempt,plan.certifyWriteUuid(),root))
      val context=MySqlDeploymentReceiptFreshPrepareComposition.contextForPrepare(before,target,i,root,plan,
        sourceIdentity,digest(receipt).toVector,targetWriter)
      assertArrayEquals(context.canonicalFingerprintSha256.toArray,fingerprint)
      val operation=MySqlFreshPrepareRules.operation(context)
      val attempt=new AttemptAssertions(UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c711"),Phase.PREPARE,plan.prepareWriteUuid(),1L,
        UUID.fromString("18a0b51d-4ff4-4a71-a27f-d67f48d3c713"),123L,namespace,operation,beforeWriter,afterWriter,bg.toResumeAssertions,tg.toResumeAssertions)
      val state=new FullStateAssertions(new NativeOwnerAssertions(1234L,133800000000000123L,boot,1L),null,null,null,beforeWriter,afterWriter,attempt,null,null,null)
      val record=new B1ResumeRecord(0L,new Array[Byte](32),Kind.GENESIS,HexFormat.of().parseHex(inputs.configurationSha256),receipt,namespace,state)
      bg.storageBudget.get.closeScopedReadBudget();tg.storageBudget.get.closeScopedReadBudget()
      body=MySqlFull26WitnessCodec.begin(i,root)
      body.encode(record,before,target)
      body.withBootstrapSemanticData { (original,actualBefore,actualTarget,_,_)=>
        MySqlDeploymentReceiptFull26RestoreOwner.requireBootstrapBody(original,actualBefore,actualTarget,i,root)
      }
      body.withData { (_,actualBefore,actualTarget,_,_) =>
        assertTrue(actualBefore.exactlyEquals(before));assertTrue(actualTarget.exactlyEquals(target))
      }
      assertTrue(before.allocationBudget eq image)
      assertThrows(classOf[IllegalStateException],()=>root.closeRootReadBudget())
    finally
      if body != null then body.close()
      if target != null then target.allocationBudget.closeScopedReadBudget()
      if image != null then image.closeScopedReadBudget()
      if inputs != null then inputs.close()
      assembly.close();root.closeRootReadBudget()
