package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.entity.DbFirstPodSnapshot
import com.wi.floatingpod.server.io.IoOperationTag
import java.io.{ByteArrayInputStream,DataInputStream}
import java.nio.ByteBuffer
import java.util.UUID
import com.wi.floatingpod.domain.RecallRefusalReason
import java.nio.charset.StandardCharsets
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class MySqlDbFirstSchemaProfileTest:
  private def inputs: MySqlDeploymentReceiptSchemaProfile.Inputs =
    import MySqlDeploymentReceiptSchemaProfile.*
    def account(name: String) = Account(name, "127.0.0.1")
    MySqlDeploymentReceiptSchemaProfile.inputs("fp_db_first_test",
      Accounts(account("fp_s5_guard"), account("fp_s5_audit"), account("fp_s5_runtime"),
        account("fp_s5_provisioner"), account("fp_s5_importer"), account("fp_s5_quota")),
      SourceLimitProfile.of(1000000, 65536, 2000000, 64, 100000, 134217728, 1000000, 2000000),
      MySqlScriptSchemaProfile.Budget(16384, 16384, 16384, 4096, 65536, 142, 20000, 2000000, 1000000),
      MySqlDeploymentReceiptSchemaDefinition.Budget(8192, 8192), 4096, Vector.empty)

  private def isAuditRoutine(statement: String): Boolean =
    statement.matches("(?s)^CREATE DEFINER=.*\\s+PROCEDURE\\s+`[^`]+`\\.`[^`]+`\\(.*$")

  @Test def generatedMetadataMatchesEveryActualDdlStatementWithoutChangingSchema5(): Unit =
    val explicit = inputs
    val original = MySqlDeploymentReceiptSchemaProfile.render(explicit)
    val actual = MySqlDbFirstSchemaProfile.render(explicit)
    val statements = MySqlScriptSchemaProfile.statements(actual.sql).toOption.get
    assertEquals(27, actual.tables.size)
    assertEquals(480, actual.tables.map(_.columns.size).sum)
    assertEquals(81, actual.guards.size)
    assertEquals(MySqlDbFirstSchemaProfile.TableNames, actual.tables.map(_.name))
    assertEquals(MySqlDbFirstSchemaProfile.GuardNames, actual.guards.map(_.name))
    val tablePattern = "(?s)CREATE TABLE `?([A-Za-z0-9_]+)`? \\(.*".r
    val tableStatements = statements.collect { case sql @ tablePattern(name) => name -> sql }.toMap
    actual.tables.foreach { table =>
      assertEquals(MySqlScriptSchemaProfile.sha256(tableStatements(table.name).getBytes(StandardCharsets.UTF_8)), table.statementSha256)
    }
    val guardPattern = "(?s)CREATE DEFINER=.*? TRIGGER ([A-Za-z0-9_]+) BEFORE (INSERT|UPDATE|DELETE) ON `?([A-Za-z0-9_]+)`? .*".r
    val guardStatements = statements.collect { case sql @ guardPattern(name, _, _) => name -> sql }.toMap
    actual.guards.foreach { guard =>
      assertEquals(MySqlScriptSchemaProfile.sha256(guardStatements(guard.name).getBytes(StandardCharsets.UTF_8)), guard.statementSha256)
    }
    assertEquals(original.sql, MySqlDeploymentReceiptSchemaProfile.render(explicit).sql)
    assertNotEquals(original.sha256, actual.sha256)
    assertEquals(26, original.tables.size)
    assertEquals(78, original.guards.size)
    assertFalse(MySqlDeploymentReceiptSchemaDefinition.Schema5RecoverySteps.contains(MySqlDbFirstSchemaDefinition.RecallStep))
    assertEquals(MySqlDeploymentReceiptSchemaDefinition.Schema5RecoverySteps,
      MySqlDbFirstSchemaDefinition.RecoverySteps.dropRight(5))
    assertEquals(Vector("DB_FIRST_RECALL","DB_FIRST_INITIAL_ACTIVATION","DB_FIRST_ACTIVE_CHECKPOINT","DB_FIRST_RECALL_REQUEST","DB_FIRST_RECALL_REFUSED"),
      MySqlDbFirstSchemaDefinition.RecoverySteps.takeRight(5))

  @Test def existingDatabaseRecipeMovesWholeAuditAndGrantBlockBeforeSeedsAndGuards(): Unit =
    val explicit = inputs
    val legacy = MySqlDbFirstSchemaProfile.render(explicit)
    val managed = MySqlDbFirstSchemaProfile.renderExistingDatabase(explicit)
    val legacyStatements = MySqlScriptSchemaProfile.statements(legacy.sql).toOption.get
    val managedStatements = MySqlScriptSchemaProfile.statements(managed.sql).toOption.get
    val seedAnchor = "-- Two control seeds only; no fake B1/domain/operation/source history or READY seal."
    val triggerAnchor = "-- SOURCE v5：沿用原72个守卫并增加6个部署/配额守卫。"
    val routineAnchor = "-- SOURCE v5：准确覆盖26张表、467列、78个守卫与相同的18个结果集。"
    val legacySeed = legacy.sql.indexOf(seedAnchor)
    val legacyTrigger = legacy.sql.indexOf(triggerAnchor)
    val legacyRoutine = legacy.sql.indexOf(routineAnchor)
    val managedSeed = managed.sql.indexOf(seedAnchor)
    val managedTrigger = managed.sql.indexOf(triggerAnchor)
    val managedRoutine = managed.sql.indexOf(routineAnchor)
    assertTrue(legacySeed >= 0 && legacySeed < legacyTrigger && legacyTrigger < legacyRoutine,
      "legacy creation renderer retains its source order")
    assertTrue(managedRoutine >= 0 && managedRoutine < managedSeed && managedSeed < managedTrigger,
      "managed routine/grants precede seed and all guards remain after seed")

    val rawRoutineAndGrantBlock = legacy.sql.substring(legacyRoutine)
    assertEquals(rawRoutineAndGrantBlock,
      managed.sql.substring(managedRoutine, managedRoutine + rawRoutineAndGrantBlock.length),
      "the complete routine, RS markers, delimiters, and all grants are moved byte-for-byte")
    assertEquals(5, legacyStatements.count(statement => statement.startsWith("SET SESSION sql_mode") || statement.startsWith("SET NAMES ")))
    assertEquals(7, managedStatements.count(statement => statement.startsWith("SET SESSION sql_mode") || statement.startsWith("SET NAMES ")))
    assertEquals(2, managedStatements.count(_ == s"USE `${explicit.schema}`"))
    assertFalse(managedStatements.exists(_.startsWith("CREATE DATABASE ")))
    val legacyTables = legacyStatements.filter(_.startsWith("CREATE TABLE "))
    val managedTables = managedStatements.filter(_.startsWith("CREATE TABLE "))
    val legacyBareTail = ") ENGINE=InnoDB"
    val legacyExplicitCharsetTail = ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"
    val managedTail = ") ENGINE=InnoDB ROW_FORMAT=DYNAMIC DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"
    assertEquals(27, legacyTables.size)
    assertEquals(21, legacyTables.count(_.endsWith(legacyBareTail)))
    assertEquals(6, legacyTables.count(_.endsWith(legacyExplicitCharsetTail)))
    assertEquals(27, legacyTables.count(statement => statement.endsWith(legacyBareTail) || statement.endsWith(legacyExplicitCharsetTail)))
    assertEquals(27, managedTables.size)
    assertTrue(managedTables.forall(_.contains("ROW_FORMAT=DYNAMIC")))
    assertTrue(managedTables.forall(_.endsWith(managedTail)))

    val tableIndexes = managedStatements.indices.filter(index => managedStatements(index).startsWith("CREATE TABLE "))
    val routineIndex = managedStatements.indexWhere(isAuditRoutine)
    val grantIndexes = managedStatements.indices.filter(index => managedStatements(index).startsWith("GRANT "))
    val insertIndexes = managedStatements.indices.filter(index => managedStatements(index).startsWith("INSERT INTO "))
    val quotaIndex = managedStatements.indexWhere(_.startsWith(s"INSERT INTO `${explicit.schema}`.`fp_deployment_quota`"))
    val controlSeedIndexes = managedStatements.indices.filter(index =>
      managedStatements(index).startsWith("INSERT INTO fp_schema_manifest") || managedStatements(index).startsWith("INSERT INTO fp_runtime_writer"))
    val triggerIndexes = managedStatements.indices.filter(index =>
      managedStatements(index).startsWith("CREATE DEFINER=") && managedStatements(index).contains(" TRIGGER "))
    assertEquals(27, tableIndexes.size)
    assertEquals(3, insertIndexes.size)
    assertEquals(2, controlSeedIndexes.size)
    assertEquals(81, triggerIndexes.size)
    assertTrue(tableIndexes.max < routineIndex && routineIndex < grantIndexes.min &&
      grantIndexes.max < insertIndexes.min && insertIndexes.max < triggerIndexes.min)
    assertTrue(quotaIndex >= 0 && quotaIndex < controlSeedIndexes.min,
      "fixed quota configuration executes after grants and before the two control seeds")

  @Test def journalGuardsMatchActualFreshRequestWireWithoutExecutingSql():Unit =
    val explicit=inputs
    val rendered=MySqlDbFirstSchemaProfile.render(explicit)
    val owner=new MySqlFormat3ReadBudget(explicit.sourceLimitProfile,explicit.publicationBudget.packetBytes)
    def id(n:Long)=new java.util.UUID(0L,n)
    val j=com.wi.floatingpod.server.io.IoOperationTag(id(1),11L,13L,id(2))
    val c=j.copy(operationId=id(3))
    try
      val some=MySqlDbFirstRecallJournalRequest.fresh(j,c,id(4),id(5),id(6),17L,Some(id(7)),owner)
        .canonical.copyBytes(owner,"actual Fresh request guard offsets")
      val none=MySqlDbFirstRecallJournalRequest.fresh(j,c,id(4),id(5),id(6),17L,None,owner)
        .canonical.copyBytes(owner,"actual legacy request guard length")
      def uuid(offset:Int,expected:java.util.UUID):Unit =
        val bytes=java.nio.ByteBuffer.wrap(some,offset-1,16)
        assertEquals(expected,new java.util.UUID(bytes.getLong(),bytes.getLong()),s"SQL offset $offset")
      Vector(47->id(1),95->id(1),79->id(2),127->id(3),143->id(4),159->id(5),175->id(6),200->id(7))
        .foreach((offset,expected)=>uuid(offset,expected))
      Vector(63->11L,111->11L,71->13L,119->13L,191->17L).foreach { (offset,expected)=>
        assertEquals(expected,java.nio.ByteBuffer.wrap(some,offset-1,8).getLong(),s"SQL offset $offset")
      }
      assertEquals(1,some(198).toInt);assertEquals(0,none(198).toInt)
      val statements=MySqlScriptSchemaProfile.statements(rendered.sql).toOption.get
      val hex=java.util.HexFormat.of()
      val tail=none.drop(199)
      assertArrayEquals(tail,some.drop(215))
      Vector("fp_s6_04_bi","fp_s6_04_bu").foreach { name=>
        val matches=statements.filter(_.contains(" TRIGGER "+name+" "))
        assertEquals(1,matches.size)
        val sql=matches.head
        assertTrue(sql.contains("NEW.step='DB_FIRST_RECALL_REQUEST'"))
        assertTrue(sql.contains(s"LEFT(NEW.request_identity_bytes,46)=UNHEX('${hex.formatHex(some.take(46))}')"))
        assertTrue(sql.contains(s"RIGHT(NEW.request_identity_bytes,${tail.length})=UNHEX('${hex.formatHex(tail)}')"))
        Vector(none.length,some.length).foreach(n=>assertTrue(sql.contains(s"OCTET_LENGTH(NEW.request_identity_bytes)=$n")))
        Vector(47->"boot_uuid",95->"boot_uuid",79->"operation_uuid",127->"workflow_uuid",143->"binding_uuid",175->"pod_uuid")
          .foreach((offset,column)=>assertTrue(sql.contains(s"SUBSTRING(NEW.request_identity_bytes,$offset,16)=NEW.$column")))
        assertTrue(sql.contains("d.owner_uuid=SUBSTRING(NEW.request_identity_bytes,159,16)"))
        assertTrue(sql.contains("SUBSTRING(NEW.request_identity_bytes,71,8)=SUBSTRING(NEW.request_identity_bytes,119,8)"))
        assertTrue(sql.contains("<=> TRUE"),"SQL NULL must fail the exact journal conjunction")
        assertTrue(sql.contains(if name.endsWith("bi") then "NEW.operation_state='RESERVED'" else
          "OLD.operation_state='RESERVED' AND NEW.operation_state='COMMITTED'"))
      }
    finally owner.closeRootReadBudget()

  @Test def recallV2V3GuardOffsetsMatchActualEncoderWithBothClientOptionsAndUtf8World():Unit =
    val explicit=inputs
    val rendered=MySqlDbFirstSchemaProfile.render(explicit)
    val statements=MySqlScriptSchemaProfile.statements(rendered.sql).toOption.get
    val sql=Vector("fp_s6_04_bi","fp_s6_04_bu").map { name=>
      val selected=statements.filter(_.contains(" TRIGGER "+name+" "))
      assertEquals(1,selected.size);selected.head
    }
    val root=new MySqlFormat3ReadBudget(explicit.sourceLimitProfile,explicit.publicationBudget.packetBytes)
    def id(n:Long)=new UUID(0L,n)
    val j=IoOperationTag(id(1),11L,13L,id(2));val c=j.copy(operationId=id(3))
    val profile=explicit.sourceLimitProfile.canonicalBytes()
    val hex=java.util.HexFormat.of()
    try
      for recovered<-Vector(false,true);client<-Vector(None,Some(id(7)));world<-Vector("world","世界🌍") do
        val journal=MySqlDbFirstRecallJournalRequest.fresh(j,c,id(4),id(5),id(6),17L,client,root)
        val execution=if recovered then IoOperationTag(id(9),19L,23L,c.operationId) else c
        val link=if recovered then MySqlDbFirstRecallJournalLink.forRecovery(journal,execution) else MySqlDbFirstRecallJournalLink.from(journal)
        val pose=new DbFirstPodSnapshot.Pose(world,0,1D,64D,2D,3F,4F)
        val request=MySqlRepositoryRequestIdentity.dbFirstRecall(execution,id(4),id(6),Some(id(5)),id(8),17L,
          DbFirstPodSnapshot.FORMAT_VERSION,9,Vector.fill(32)(1.toByte),profile.toVector,
          explicit.sourceLimitProfile.sha256().toVector,pose,root,Some(link))
          .canonical.copyBytes(root,"actual V2 request SQL wire layout")
        val original=journal.canonical.copyBytes(root,"actual V2 J reference SQL wire layout")
        def uuid(offset:Int,expected:UUID):Unit =
          val value=ByteBuffer.wrap(request,offset-1,16)
          assertEquals(expected,new UUID(value.getLong(),value.getLong()),s"V2 SQL offset $offset")
        Vector(65->execution.bootId,97->id(3),113->id(4),129->id(6),146->id(5),162->id(8))
          .foreach((offset,expected)=>uuid(offset,expected))
        Vector(81->execution.bootGeneration,89->execution.connectionGeneration,178->17L).foreach { (offset,expected)=>
          assertEquals(expected,ByteBuffer.wrap(request,offset-1,8).getLong())
        }
        assertEquals(1,request(144).toInt)
        Vector(186->DbFirstPodSnapshot.FORMAT_VERSION,190->9,194->32,230->profile.length,
          (234+profile.length)->32,(270+profile.length)->world.getBytes(StandardCharsets.UTF_8).length)
          .foreach((offset,expected)=>assertEquals(expected,ByteBuffer.wrap(request,offset-1,4).getInt(),s"V2 SQL offset $offset"))
        assertArrayEquals(profile,request.slice(233,233+profile.length))
        assertArrayEquals(explicit.sourceLimitProfile.sha256(),request.slice(237+profile.length,269+profile.length))
        val footer=(if client.nonEmpty then 117 else 101)+(if recovered then 48 else 0)
        assertEquals(309+profile.length+world.getBytes(StandardCharsets.UTF_8).length+footer,request.length)
        val expectedFooter=original.slice(46,94) ++ original.slice(158,174) ++ original.slice(198,if client.nonEmpty then 215 else 199) ++
          ByteBuffer.allocate(4).putInt(32).array() ++ journal.canonical.sha256.toArray ++
          (if recovered then original.slice(94,142) else Array.emptyByteArray)
        assertArrayEquals(expectedFooter,request.takeRight(footer))
        if recovered then
          assertFalse(java.util.Arrays.equals(original.slice(94,142),request.slice(64,112)))
          assertArrayEquals(original.slice(94,142),request.takeRight(48),"V3 explicit complete original C tag")
        else assertArrayEquals(original.slice(94,142),request.slice(64,112),"V2 whole original C namespace")
        val reader=new DataInputStream(new ByteArrayInputStream(request))
        def token():String=
          val data=new Array[Byte](reader.readInt());reader.readFully(data);new String(data,StandardCharsets.UTF_8)
        assertEquals("FP_MYSQL_REPOSITORY_REQUEST_BE_V1",token())
        assertEquals(1,reader.readUnsignedByte());assertEquals(1,reader.readInt())
        assertEquals(if recovered then "DB_FIRST_RECALL_V3" else "DB_FIRST_RECALL_V2",token())
        assertEquals(request.length-64,reader.available())
        sql.foreach { statement=>
          assertTrue(statement.contains(s"LEFT(NEW.request_identity_bytes,64)=UNHEX('${hex.formatHex(request.take(64))}')"))
          assertTrue(statement.contains(s"CONV(HEX(SUBSTRING(NEW.request_identity_bytes,${270+profile.length},4)),16,10)"))
          assertTrue(statement.contains(s"OCTET_LENGTH(NEW.request_identity_bytes)=${309+profile.length}+"))
          assertTrue(statement.contains("IF(SUBSTRING(j.request_identity_bytes,199,1)=UNHEX('01'),117,101)"))
          val framed=ByteBuffer.allocate(8+profile.length+32).putInt(profile.length).put(profile).putInt(32)
            .put(explicit.sourceLimitProfile.sha256()).array()
          assertTrue(statement.contains(s"SUBSTRING(NEW.request_identity_bytes,230,${framed.length})=UNHEX('${hex.formatHex(framed)}')"))
        }
    finally root.closeRootReadBudget()

  @Test def recallV2SqlStructureBindsOriginalJAndSnapshotAndRetainsLegacyExclusion():Unit =
    val explicit=inputs
    val old=MySqlDeploymentReceiptSchemaProfile.render(explicit)
    val rendered=MySqlDbFirstSchemaProfile.render(explicit)
    val statements=MySqlScriptSchemaProfile.statements(rendered.sql).toOption.get
    val guards=statements.filter(_.contains("DB-first V2/V3 requires exact original J/C request and committed snapshot"))
    assertEquals(2,guards.size,"V2 contract belongs to actual O insert/update guards only")
    Vector("fp_s6_04_bi","fp_s6_04_bu").foreach { name=>
      val statement=guards.find(_.contains(" TRIGGER "+name+" ")).get
      Vector("NEW.workflow_uuid=NEW.operation_uuid AND NEW.previous_step_operation_uuid IS NOT NULL",
        "j.operation_uuid=NEW.previous_step_operation_uuid AND j.workflow_uuid=NEW.operation_uuid",
        "j.step='DB_FIRST_RECALL_REQUEST' AND j.operation_state='COMMITTED'",
        "SUBSTRING(NEW.request_identity_bytes,65,48)=SUBSTRING(j.request_identity_bytes,95,48)",
        "SUBSTRING(NEW.request_identity_bytes,146,16)=SUBSTRING(j.request_identity_bytes,159,16)",
        "UNHEX('00000020'),j.request_identity_sha256,",
        "SUBSTRING(NEW.request_identity_bytes,65,48)<>SUBSTRING(j.request_identity_bytes,95,48)",
        "SUBSTRING(j.request_identity_bytes,95,48),UNHEX(''))",
        "j.expected_domain_version=NEW.expected_domain_version AND j.expected_locator_revision=NEW.expected_locator_revision",
        "s.last_mutation_operation_uuid=NEW.operation_uuid AND s.attempt_uuid=NEW.attempt_uuid",
        "s.nbt_sha256=SUBSTRING(NEW.request_identity_bytes,198,32)",
        "SUBSTRING(NEW.request_identity_bytes,190,4)=UNHEX(LPAD(HEX(OCTET_LENGTH(s.nbt_bytes)),8,'0'))",
        "DB-first V1 cannot bypass original journal responsibility",
        "j.workflow_uuid=NEW.operation_uuid OR (j.pod_uuid=NEW.pod_uuid",
        "c.operation_uuid=j.workflow_uuid AND c.operation_state='COMMITTED'",
        "DB-first recall requires an explicit supported request version").foreach(part=>assertTrue(statement.contains(part),part))
      val bytes=MySqlDbFirstRecallCanonical.JournalScope.getBytes(StandardCharsets.US_ASCII)
      val prefix=ByteBuffer.allocate(5+bytes.length).putInt(bytes.length).put(bytes).put(1.toByte).array()
      assertTrue(statement.contains(s"LEFT(NEW.scope_bytes,${prefix.length})=IF("))
      assertTrue(statement.contains(s"UNHEX('${java.util.HexFormat.of().formatHex(prefix)}')"))
      val recovered=MySqlDbFirstRecallCanonical.RecoveryScope.getBytes(StandardCharsets.US_ASCII)
      val recoveredPrefix=ByteBuffer.allocate(5+recovered.length).putInt(recovered.length).put(recovered).put(1.toByte).array()
      assertTrue(statement.contains(s"UNHEX('${java.util.HexFormat.of().formatHex(recoveredPrefix)}')"))
      assertTrue(statement.contains("<=> TRUE"),"malformed NULL conjunctions must reject")
    }
    assertEquals(old.sql,MySqlDeploymentReceiptSchemaProfile.render(explicit).sql)
    assertFalse(old.sql.contains("DB_FIRST_RECALL_V2"))
    println(s"Explicit schema6 V2 guard identity=${rendered.sha256}, schema5 unchanged=${old.sha256}")

  @Test def initialGuardsUseTheActualPreparedDomainModeEncoding(): Unit =
    val rendered=MySqlDbFirstSchemaProfile.render(inputs)
    val i=MySqlScriptSchemaInputs.createDbFirstFull27(rendered)
    val b=i.newAllocationRoot(i.budget.packetBytes,new MySqlFormat3AllocationLedger(i.sourceLimitProfile.maxRetainedBytes()))
    try
      val domain=MySqlRecoveryCanonicalBytes.withAllocationBudget(b) {
        MySqlControlledDebugRows.preparedDomain(new java.util.UUID(0,1),new java.util.UUID(0,2),
          com.wi.floatingpod.domain.FloatingPodPose("world",1,64,2,3,4,0),new java.util.UUID(0,3),
          java.time.LocalDateTime.of(2026,11,2,12,0),b)
      }
      val mode=domain(i.forTable("floating_pod_state").columns.indexWhere(_.name=="operating_mode")) match
        case MySqlFormat3Cell.Text(value)=>value
        case _=>fail("actual domain mode must be SQL text")
      val statements=MySqlScriptSchemaProfile.statements(rendered.sql).toOption.get
      Vector("fp_s6_27_bi"->"d", "fp_s6_27_bu"->"d", "fp_s6_05_bu"->"OLD",
        "fp_s6_05_bu"->"NEW", "fp_s6_04_bu"->"d").foreach { (name,alias)=>
        val guard=statements.filter(_.contains(" TRIGGER "+name+" "))
        assertEquals(1,guard.size)
        assertTrue(guard.head.contains(alias+".operating_mode='"+mode+"'"),
          name+" must accept the mode token emitted by actual domain producer")
      }
    finally b.closeRootReadBudget()

  @Test def full27IncludesSnapshotWhileOldFull26RejectsIt(): Unit =
    val snapshot = MySqlDbFirstRelationShapes.forTable(MySqlDbFirstSchemaDefinition.SnapshotTable)
    assertEquals(MySqlDeploymentReceiptRelationShapes.canonicalOrder, MySqlDbFirstRelationShapes.canonicalOrder.dropRight(1))
    assertEquals(13, snapshot.columns.size)
    assertEquals(Vector("binding_uuid", "pod_uuid"), snapshot.primaryKey)
    assertThrows(classOf[IllegalArgumentException], () => MySqlDeploymentReceiptRelationShapes.forTable(snapshot.table))
    assertThrows(classOf[IllegalArgumentException], () => MySqlDbFirstRelationShapes.forTable("not_a_relation"))

  @Test def blobLimitUsesExplicitPacketDecompressionAndSqlTypeBounds(): Unit =
    def limits(decompressed: Long, packet: Long) = SourceLimitProfile.of(1, 1, decompressed, 1, 1, Long.MaxValue, packet, 1)
    assertEquals(7L, MySqlDbFirstSchemaDefinition.maximumNbtBytes(limits(7, 20)))
    assertEquals(5L, MySqlDbFirstSchemaDefinition.maximumNbtBytes(limits(20, 5)))
    assertEquals(4294967295L, MySqlDbFirstSchemaDefinition.maximumNbtBytes(limits(Long.MaxValue, Long.MaxValue)))
    assertThrows(classOf[IllegalArgumentException], () => MySqlDbFirstSchemaDefinition.maximumNbtBytes(null))

  @Test def staticAuditRejectsMissingSubstitutedOrDuplicatedSnapshotIdentities(): Unit =
    val profile = MySqlSchemaAuditProfile.DbFirst1
    assertEquals(18, MySqlAuditCoverage.resultSetIds(profile).size)
    assertEquals(18, MySqlSchemaAuditor.labelsFor(profile).size)
    assertTrue(MySqlApprovedBaselineLoader.supportsProfile(6, MySqlDbFirstSchemaDefinition.Coverage))
    assertFalse(MySqlApprovedBaselineLoader.supportsProfile(5, MySqlDbFirstSchemaDefinition.Coverage))
    assertFalse(MySqlApprovedBaselineLoader.supportsProfile(6, MySqlDeploymentReceiptSchemaProfile.Coverage))
    val sets = MySqlAuditCoverage.StaticResultSetIds.zip(MySqlSchemaAuditor.labelsFor(profile).slice(1, 11)).map { (id, labels) =>
      val columns = labels.map(label => MySqlAuditColumn(label, java.sql.Types.VARCHAR, "VARCHAR", 1024, 0, 0, false))
      def row(values: Map[String, String]) = MySqlAuditRow(labels.map(label => MySqlAuditText(values.getOrElse(label, "fixture"))))
      val rows = id match
        case "RS02_TABLES" => MySqlDbFirstRelationShapes.canonicalOrder.map(t => row(Map("TABLE_NAME" -> t.table)))
        case "RS03_COLUMNS" => MySqlDbFirstRelationShapes.canonicalOrder.flatMap(t => t.columns.map(c =>
          row(Map("TABLE_NAME" -> t.table, "COLUMN_NAME" -> c.name))))
        case "RS09_TRIGGERS" => MySqlDbFirstSchemaProfile.GuardNames.map(n => row(Map("TRIGGER_NAME" -> n)))
        case _ => Vector.empty
      MySqlAuditResultSet(id, columns, rows)
    }
    MySqlSchemaAuditor.validateProfileObjectCounts(profile, sets)
    Vector(0, 1, 7).foreach { index =>
      val set = sets(index)
      val missing = sets.updated(index, set.copy(rows = set.rows.dropRight(1)))
      val duplicate = sets.updated(index, set.copy(rows = set.rows.dropRight(1) :+ set.rows.head))
      val extra = sets.updated(index, set.copy(rows = set.rows :+ set.rows.last))
      Vector(missing, duplicate, extra).foreach(bad => assertThrows(classOf[java.sql.SQLNonTransientConnectionException],
        () => MySqlSchemaAuditor.validateProfileObjectCounts(profile, bad)))
    }
    assertNotEquals(MySqlSchemaAuditor.canonicalSha256(profile, sets),
      MySqlSchemaAuditor.canonicalSha256(MySqlSchemaAuditProfile.DeploymentReceipt1, sets))

  @Test def schema6PrivilegesAndConfigurationHaveIndependentIdentity(): Unit =
    val explicit = inputs
    val actual = MySqlDbFirstSchemaProfile.render(explicit)
    val privileges = MySqlDbFirstSchemaProfile.expectedPrivilegeRows(actual)
    val tableRows = privileges(0).filter(_(3) == MySqlDbFirstSchemaDefinition.SnapshotTable)
    val runtime = tableRows.filter(_.head == explicit.accounts.runtime.grantee).map(_(4)).toSet
    assertEquals(Set("SELECT", "INSERT"), runtime)
    assertFalse(tableRows.exists(_(4) == "DELETE"))
    assertFalse(tableRows.exists(_.head == explicit.accounts.importer.grantee))
    val updates = privileges(1).filter(r => r.head == explicit.accounts.runtime.grantee && r(3) == MySqlDbFirstSchemaDefinition.SnapshotTable)
    assertEquals(MySqlDbFirstSchemaDefinition.snapshotColumns.map(_.name).toSet -- Set("binding_uuid", "pod_uuid"),
      updates.map(_(4)).toSet)
    assertNotEquals(explicit.profileIdentitySha256, actual.profileIdentitySha256)
    assertEquals(actual.profileIdentitySha256, MySqlDbFirstSchemaProfile.render(explicit).profileIdentitySha256)
    assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlDeploymentReceiptSchemaProfile.expectedPrivilegeRows(actual.sql, explicit, MySqlDeploymentReceiptSchemaProfile.AuditRoutine))

  @Test def securityValidatorRequiresTheWholeSchema6PrivilegeGraph(): Unit =
    val explicit = inputs
    val actual = MySqlDbFirstSchemaProfile.render(explicit)
    val sets = MySqlDbFirstSchemaProfile.expectedPrivilegeRows(actual).zipWithIndex.map { (rows, i) =>
      val labels = MySqlSchemaAuditor.Format3PrivilegeLabels(i)
      MySqlAuditResultSet(MySqlAuditCoverage.Format3PrivilegeResultSetIds(i), labels.map(label =>
        MySqlAuditColumn(label, java.sql.Types.VARCHAR, "VARCHAR", 512, 0, java.sql.ResultSetMetaData.columnNoNulls, false)),
        rows.map(row => MySqlAuditRow(row.map(MySqlAuditText.apply))))
    }
    MySqlDeploymentReceiptSchemaAuditInputs.validateSecurity(explicit, actual, explicit.schema, sets)
    val table = sets.head
    val snapshotRows = table.rows.filter(_.cells(table.columns.indexWhere(_.label == "TABLE_NAME")) ==
      MySqlAuditText(MySqlDbFirstSchemaDefinition.SnapshotTable))
    assertFalse(snapshotRows.isEmpty)
    val missing = sets.updated(0, table.copy(rows = table.rows.filterNot(snapshotRows.contains)))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlDeploymentReceiptSchemaAuditInputs.validateSecurity(explicit, actual, explicit.schema, missing))
    val wrongGrant = snapshotRows.head.copy(cells = snapshotRows.head.cells.updated(
      table.columns.indexWhere(_.label == "PRIVILEGE_TYPE"), MySqlAuditText("DELETE")))
    val extra = sets.updated(0, table.copy(rows = table.rows :+ wrongGrant))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlDeploymentReceiptSchemaAuditInputs.validateSecurity(explicit, actual, explicit.schema, extra))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlDeploymentReceiptSchemaAuditInputs.validateSecurity(explicit, actual, "other_schema", sets))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlDeploymentReceiptSchemaAuditInputs.validateSecurity(explicit,
        MySqlDeploymentReceiptSchemaProfile.render(explicit), explicit.schema, sets))

  @Test def committedDbFirstPodReleaseRequiresExactSnapshotAndKeepsInheritedReservationGuards(): Unit =
    val explicit=inputs
    val old=MySqlDeploymentReceiptSchemaProfile.render(explicit)
    val actual=MySqlDbFirstSchemaProfile.render(explicit)
    def guard(sql: String, name: String): String =
      val statements=MySqlScriptSchemaProfile.statements(sql).toOption.get
      val matches=statements.filter(_.contains(" TRIGGER "+name+" "))
      assertEquals(1,matches.size)
      matches.head
    val inherited=guard(old.sql,"fp_s5_08_bu").replace("TRIGGER fp_s5_08_bu ","TRIGGER fp_s6_08_bu ")
      .replace("format_version=5","format_version=6").replace("format_version = 5","format_version = 6")
    val updated=guard(actual.sql,"fp_s6_08_bu")
    val release="OLD.reservation_state='HELD' AND NEW.reservation_state='RELEASED' AND NEW.operation_uuid=OLD.operation_uuid AND NEW.workflow_uuid=OLD.workflow_uuid AND o.operation_uuid=OLD.operation_uuid AND o.workflow_uuid=OLD.workflow_uuid AND o.operation_state='COMMITTED' AND "
    val start=updated.indexOf(release)
    val liveWriter=" AND o.boot_uuid=w.owner_boot_uuid"
    val end=updated.indexOf(liveWriter,start)
    assertTrue(start>=0 && end>start)
    // 原分支外侧还有两个右括号；只取这次闭合替换的branch内容。
    val changed=updated.substring(start,end-2)
    assertTrue(changed.startsWith(release+"(o.step='DOMAIN_MUTATION' OR (o.step='REARM'"))
    assertTrue(changed.contains("OR (o.step IN ('DB_FIRST_RECALL','DB_FIRST_INITIAL_ACTIVATION','DB_FIRST_ACTIVE_CHECKPOINT')"))
    val scopeDomain=MySqlDbFirstInitialPreparationCanonical.Scope.getBytes(StandardCharsets.US_ASCII)
    val scopeHeader=java.nio.ByteBuffer.allocate(5+scopeDomain.length).putInt(scopeDomain.length).put(scopeDomain).put(1.toByte).array()
    assertTrue(changed.contains("LEFT(o.scope_bytes,"+scopeHeader.length+")=UNHEX('"+java.util.HexFormat.of().formatHex(scopeHeader)+"')"))
    Vector("o.workflow_uuid=o.operation_uuid", "o.expected_domain_version=0", "o.expected_locator_revision=0",
      "o.commit_witness_bytes IS NOT NULL", "o.commit_witness_sha256 IS NOT NULL", "o.result_at IS NOT NULL",
      "d.is_active=0 AND d.state_version=0", "d.owner_uuid IS NULL AND d.operating_mode='WORLD_IDLE'",
      "i.phase='PREPARED' AND i.origin='DEBUG_ITEM_CREATE'", "i.prepare_operation_uuid=o.operation_uuid AND i.intent_revision=0",
      "i.activation_operation_uuid IS NULL", "l.locator_state='DEBUG_ITEM_CREATE_PREPARED'",
      "l.current_attempt_uuid=o.attempt_uuid AND l.current_footprint_uuid=o.footprint_uuid",
      "f.purpose='CREATE_TARGET' AND f.origin_version=0 AND f.seal_state='SEALED'",
      "NOT EXISTS(SELECT 1 FROM fp_pod_snapshot s WHERE s.binding_uuid=d.binding_uuid AND s.pod_uuid=d.uuid)").foreach { clause=>
      assertTrue(changed.contains(clause),"rendered preparation release contract: "+clause)
    }
    assertTrue(updated.contains("o.operation_state IN ('COMMITTED','CANCELLED')"),"terminal-state guard must still reject RESERVED release")
    Vector("NEW.resource_kind='POD'", "NEW.resource_unit_uuid IS NULL", "NEW.resource_pod_uuid=o.pod_uuid",
      "FROM fp_pod_snapshot s JOIN floating_pod_state d", "d.binding_uuid=s.binding_uuid", "d.uuid=s.pod_uuid",
      "s.binding_uuid=o.binding_uuid", "s.pod_uuid=o.pod_uuid", "s.attempt_uuid=o.attempt_uuid",
      "s.captured_state_version=o.expected_domain_version", "s.committed_state_version=o.expected_domain_version+1",
      "s.last_mutation_operation_uuid=o.operation_uuid", "d.last_mutation_operation_uuid=o.operation_uuid",
      "d.is_active=IF(o.step='DB_FIRST_RECALL',0,1)", "d.state_version=s.committed_state_version", "(d.owner_uuid <=> s.owner_uuid)").foreach { clause =>
      assertTrue(changed.contains(clause),clause)
    }
    val originalBranch=release+"o.step='DOMAIN_MUTATION'"
    assertEquals(inherited,updated.substring(0,start)+originalBranch+updated.substring(end-2),
      "only the exact committed release branch changes; admission, live writer/connection/lease, immutable keys, revision and RESERVED retention remain inherited")
    val metadata=actual.guards.find(_.name=="fp_s6_08_bu").get
    assertEquals(MySqlScriptSchemaProfile.sha256(updated.getBytes(StandardCharsets.UTF_8)),metadata.statementSha256)
    assertNotEquals(MySqlScriptSchemaProfile.sha256(inherited.getBytes(StandardCharsets.UTF_8)),metadata.statementSha256)
    assertEquals(MySqlScriptSchemaProfile.sha256(actual.sql.getBytes(StandardCharsets.UTF_8)),actual.sha256)
    assertEquals(old.sql,MySqlDeploymentReceiptSchemaProfile.render(explicit).sql)
    assertFalse(guard(old.sql,"fp_s5_08_bu").contains("DB_FIRST_RECALL"))
    assertEquals(guard(old.sql,"fp_s5_08_bi").replace("TRIGGER fp_s5_08_bi ","TRIGGER fp_s6_08_bi ")
      .replace("format_version=5","format_version=6").replace("format_version = 5","format_version = 6"),
      guard(actual.sql,"fp_s6_08_bi"))

  @Test def refusalGuardsBindOriginalJournalAndCurrentSettlementWireForBothClientOptions():Unit =
    val explicit=inputs;val rendered=MySqlDbFirstSchemaProfile.render(explicit)
    val old=MySqlDeploymentReceiptSchemaProfile.render(explicit)
    val statements=MySqlScriptSchemaProfile.statements(rendered.sql).toOption.get
    val selected=Vector("fp_s6_04_bi","fp_s6_04_bu").map { name=>
      val matches=statements.filter(_.contains(" TRIGGER "+name+" "))
      assertEquals(1,matches.size);matches.head
    }
    def header(domain:String)=
      val bytes=domain.getBytes(StandardCharsets.US_ASCII)
      ByteBuffer.allocate(5+bytes.length).putInt(bytes.length).put(bytes).put(1.toByte).array()
    val prefix=header("FP_DB_FIRST_RECALL_REFUSED_REQUEST_BE_V1") :+ 1.toByte
    val scope=header("FP_DB_FIRST_RECALL_REFUSED_SCOPE_BE_V1")
    val start=prefix.length;val profile=explicit.sourceLimitProfile.canonicalBytes();val hex=java.util.HexFormat.of()
    val tail=ByteBuffer.allocate(8+profile.length+32).putInt(profile.length).put(profile).putInt(32).put(explicit.sourceLimitProfile.sha256()).array()
    val root=new MySqlFormat3ReadBudget(explicit.sourceLimitProfile,explicit.publicationBudget.packetBytes)
    def id(n:Long)=new UUID(0L,n)
    val j=IoOperationTag(id(1),11L,13L,id(2));val original=j.copy(operationId=id(3))
    val current=IoOperationTag(id(9),21L,23L,original.operationId)
    try
      for client<-Vector(None,Some(id(7))) do
        val journal=MySqlDbFirstRecallJournalRequest.fresh(j,original,id(4),id(5),id(6),17L,client,root)
        val request=MySqlDbFirstRecallRefusalRequest.notApplied(current,journal,root).canonical.copyBytes(root,"actual refusal SQL wire")
        val jb=journal.canonical.copyBytes(root,"actual original J SQL wire")
        val clientLength=if client.nonEmpty then 16 else 0
        assertArrayEquals(prefix,request.take(start))
        assertEquals(283+MySqlDbFirstRecallRefusalRequest.Domain.length+profile.length+clientLength,request.length)
        def uuid(offset:Int):UUID={val b=ByteBuffer.wrap(request,start+offset,16);new UUID(b.getLong(),b.getLong())}
        assertEquals(current.bootId,uuid(0));assertEquals(current.bootGeneration,ByteBuffer.wrap(request,start+16,8).getLong())
        assertEquals(current.connectionGeneration,ByteBuffer.wrap(request,start+24,8).getLong());assertEquals(original.operationId,uuid(32))
        assertArrayEquals(jb.slice(94,142),request.slice(start+48,start+96))
        assertArrayEquals(jb.slice(46,94),request.slice(start+96,start+144))
        assertEquals(id(4),uuid(144));assertEquals(id(5),uuid(160));assertEquals(id(6),uuid(176))
        assertEquals(17L,ByteBuffer.wrap(request,start+192,8).getLong())
        assertEquals(if client.nonEmpty then 1 else 0,request(start+200).toInt)
        client.foreach(value=>assertEquals(value,uuid(201)))
        assertEquals(32,ByteBuffer.wrap(request,start+201+clientLength,4).getInt())
        assertArrayEquals(journal.canonical.sha256.toArray,request.slice(start+205+clientLength,start+237+clientLength))
        assertArrayEquals(tail,request.takeRight(tail.length))
        selected.foreach { sql=>
          assertTrue(sql.contains("NEW.step='DB_FIRST_RECALL_REFUSED'"))
          assertTrue(sql.contains(s"LEFT(NEW.request_identity_bytes,$start)=UNHEX('${hex.formatHex(prefix)}')"))
          assertTrue(sql.contains(s"LEFT(NEW.scope_bytes,${scope.length})=UNHEX('${hex.formatHex(scope)}')"))
          assertTrue(sql.contains(s"OCTET_LENGTH(NEW.request_identity_bytes)=${request.length}"))
          assertTrue(sql.contains(s"RIGHT(NEW.request_identity_bytes,${tail.length})=UNHEX('${hex.formatHex(tail)}')"))
          for (offset,column)<-Vector(0->"boot_uuid",32->"operation_uuid",80->"operation_uuid",128->"previous_step_operation_uuid",144->"binding_uuid",176->"pod_uuid") do
            assertTrue(sql.contains(s"SUBSTRING(NEW.request_identity_bytes,${start+offset+1},16)=NEW.$column"))
          assertTrue(sql.contains(s"SUBSTRING(NEW.request_identity_bytes,${start+49},48)=SUBSTRING(j.request_identity_bytes,95,48)"))
          assertTrue(sql.contains(s"SUBSTRING(NEW.request_identity_bytes,${start+97},48)=SUBSTRING(j.request_identity_bytes,47,48)"))
          assertTrue(sql.contains(s"SUBSTRING(NEW.scope_bytes,${scope.length+1},48)=SUBSTRING(NEW.request_identity_bytes,${start+1},48)"))
          assertTrue(sql.contains(s"SUBSTRING(NEW.request_identity_bytes,${start+205}+IF(SUBSTRING(NEW.request_identity_bytes,${start+201},1)=UNHEX('01'),16,0)+1,32)=j.request_identity_sha256"))
          Vector("NEW.workflow_uuid", "NEW.previous_step_operation_uuid<>NEW.operation_uuid", "j.operation_state='COMMITTED'",
            "j.expected_locator_revision=NEW.expected_locator_revision", "j.request_identity_sha256=UNHEX(SHA2(j.request_identity_bytes,256))",
            "NEW.reserve_operation_uuid IS NULL", "NEW.settled_by_operation_uuid IS NULL", "<=> TRUE").foreach(part=>assertTrue(sql.contains(part),part))
        }
      assertTrue(selected.head.contains("NEW.operation_state='RESERVED'"))
      assertTrue(selected(1).contains("OLD.operation_state='RESERVED' AND NEW.operation_state='COMMITTED'"))
      assertTrue(rendered.tables.find(_.name=="fp_recovery_operation").get.checks.exists(_.contains("'DB_FIRST_RECALL_REFUSED'")))
      rendered.guards.filterNot(_.table=="fp_recovery_operation").foreach { guard=>
        val sql=statements.find(_.contains(" TRIGGER "+guard.name+" ")).get
        assertFalse(sql.contains("'DB_FIRST_RECALL_REFUSED'"),"refusal must not grant domain/snapshot/locator/reservation mutation: "+guard.name)
      }
      assertEquals(old.sql,MySqlDeploymentReceiptSchemaProfile.render(explicit).sql)
      assertFalse(old.sql.contains("DB_FIRST_RECALL_REFUSED"))
      println(s"Explicit schema6 refusal guard identity=${rendered.sha256}, schema5 unchanged=${old.sha256}")
    finally root.closeRootReadBudget()

  @Test def reasonedRefusalV2GuardBindsExactDomainVersionLengthReasonAndUnchangedJournalOffsets():Unit =
    val explicit=inputs;val rendered=MySqlDbFirstSchemaProfile.render(explicit)
    val statements=MySqlScriptSchemaProfile.statements(rendered.sql).toOption.get
    val selected=Vector("fp_s6_04_bi","fp_s6_04_bu").map(name=>statements.find(_.contains(" TRIGGER "+name+" ")).get)
    val domain="FP_DB_FIRST_RECALL_REFUSED_REQUEST_BE_V2".getBytes(StandardCharsets.US_ASCII)
    val prefix=ByteBuffer.allocate(6+domain.length).putInt(domain.length).put(domain).put(2.toByte).put(1.toByte).array()
    val start=prefix.length;val hex=java.util.HexFormat.of()
    val root=new MySqlFormat3ReadBudget(explicit.sourceLimitProfile,explicit.publicationBudget.packetBytes)
    def id(n:Long)=new UUID(0L,n)
    val j=IoOperationTag(id(1),11L,13L,id(2));val c=j.copy(operationId=id(3))
    try
      for client<-Vector(None,Some(id(7)));reason<-RecallRefusalReason.values do
        val journal=MySqlDbFirstRecallJournalRequest.fresh(j,c,id(4),id(5),id(6),17L,client,root)
        val request=MySqlDbFirstRecallRefusalRequest.notAppliedWithReason(c,journal,reason,root).canonical.copyBytes(root,"actual V2 SQL wire")
        val optionLength=if client.isEmpty then 0 else 16
        assertArrayEquals(prefix,request.take(start))
        assertEquals(284+domain.length+root.profile.canonicalBytes().length+optionLength,request.length)
        assertEquals(reason.canonicalId,request(start+237+optionLength).toInt)
        assertArrayEquals(journal.canonical.sha256.toArray,request.slice(start+205+optionLength,start+237+optionLength))
        selected.foreach { sql=>
          assertTrue(sql.contains(s"LEFT(NEW.request_identity_bytes,$start)=UNHEX('${hex.formatHex(prefix)}')"))
          assertTrue(sql.contains(s"OCTET_LENGTH(NEW.request_identity_bytes)=${request.length}"))
          assertTrue(sql.contains(s"SUBSTRING(NEW.request_identity_bytes,${start+237}+IF(SUBSTRING(NEW.request_identity_bytes,${start+201},1)=UNHEX('01'),16,0)+1,1) IN"))
          assertTrue(sql.contains("(UNHEX('01'),UNHEX('02'),UNHEX('03'),UNHEX('04'),UNHEX('05'),UNHEX('06'))"))
        }
      assertFalse(MySqlDeploymentReceiptSchemaProfile.render(explicit).sql.contains(java.util.HexFormat.of().formatHex(prefix)))
      println(s"Explicit schema6 reasoned-refusal V2 guard identity=${rendered.sha256}")
    finally root.closeRootReadBudget()
