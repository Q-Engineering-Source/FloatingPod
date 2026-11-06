package com.wi.floatingpod.storage.mysql

import java.io.{ByteArrayInputStream, StringReader}
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData, SQLException, Types}
import java.time.{Duration, LocalDateTime}
import java.util.{HexFormat, UUID}
import scala.collection.mutable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.Assertions.*
import java.nio.file.Path

/** Typed JDBC proxies exercise only the real bootstrap reader/decoder and its caller-owned transaction boundary. */
class MySqlDbFirstBootstrapSnapshotJdbcTest:
  import DeploymentReceiptStaticTestData.*

  @TempDir var directory: Path = _

  private final case class JdbcCounts(statements: mutable.ArrayBuffer[String],
      var commits: Int = 0, var rollbacks: Int = 0, var autoCommitChanges: Int = 0,
      var isolationChanges: Int = 0, var statementClosures: Int = 0, var resultSetClosures: Int = 0,
      var failStatementCloseForTable: Option[String] = None,
      var failResultSetCloseForTable: Option[String] = None)

  private final class Harness(qualificationState: String, bindingRowState: String):
    val profile = defaultProfile()
    val fixture = MySqlDbFirstHistoricalTestFixture.fixture(directory, profile)
    val root = fixture.root()
    val identity = MySqlOwnedIoIdentity(UUID.fromString("12345678-1234-1234-1234-123456789abc"), 7L, 1L)
    val assembly = MySqlRuntimeInputs.beginV2(root, Thread.currentThread())
    val graph = MySqlRuntimeInputs.assembleDbFirstStatic(fixture.metadata, assembly, policy)
    val inputs = MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph, root, identity)
    val counts = JdbcCounts(mutable.ArrayBuffer.empty[String])
    val manifestValues: Map[String, Any] = Map(
      "singleton_fixed1" -> 1L,
      "backend" -> "MYSQL",
      "format_version" -> 6L,
      "ddl_sha256" -> HexFormat.of().parseHex(inputs.auditExpectation.ddlSha256),
      "provisioning_uuid" -> uuidBytes(inputs.auditExpectation.provisioningUuid),
      "bootstrap_state" -> "READY",
      "expected_server_uuid" -> uuidBytes(inputs.auditExpectation.serverUuid),
      "expected_binding_uuid" -> uuidBytes(inputs.auditExpectation.bindingUuid),
      "import_state" -> "NONE",
      "configuration_sha256" -> HexFormat.of().parseHex(graph.configurationSha256),
      "source_provenance_kind" -> fixture.contract.get("sourceProvenanceKind").getAsString,
      "source_provenance_bytes" -> HexFormat.of().parseHex(fixture.contract.get("sourceProvenanceHex").getAsString),
      "source_provenance_sha256" -> HexFormat.of().parseHex(fixture.contract.get("sourceProvenanceSha256").getAsString),
      "limit_profile_encoding" -> com.wi.floatingpod.config.SourceLimitProfile.ENCODING,
      "limit_profile_bytes" -> fixture.profile.sourceLimitProfile.canonicalBytes(),
      "limit_profile_sha256" -> fixture.profile.sourceLimitProfile.sha256(),
      "max_region_bytes" -> fixture.profile.sourceLimitProfile.maxRegionBytes(),
      "max_chunk_bytes" -> fixture.profile.sourceLimitProfile.maxChunkBytes(),
      "max_decompressed_bytes" -> fixture.profile.sourceLimitProfile.maxDecompressedBytes(),
      "max_nbt_depth" -> fixture.profile.sourceLimitProfile.maxNbtDepth().toLong,
      "max_members" -> fixture.profile.sourceLimitProfile.maxMembers(),
      "max_retained_bytes" -> fixture.profile.sourceLimitProfile.maxRetainedBytes(),
      "max_sql_packet_bytes" -> fixture.profile.sourceLimitProfile.maxSqlPacketBytes(),
      "max_native_artifact_bytes" -> fixture.profile.sourceLimitProfile.maxNativeArtifactBytes())
    val connection = createConnection(counts, inputs.auditExpectation.bindingUuid, bindingRowState, manifestValues)
    val connectionIdentity = new MySqlConnectionIdentity(inputs.auditExpectation.serverUuid,
      inputs.auditExpectation.serverVersion, inputs.auditExpectation.schema, inputs.auditExpectation.effectiveUser,
      "InnoDB", 1, 1L, "REPEATABLE-READ", "STRICT_ALL_TABLES", "+00:00", "TLSv1.3", "TLS_AES_256_GCM_SHA384")
    val verified = new VerifiedMySqlConnection(connection, connectionIdentity)
    val scope = MySqlAuditBootScope.forOwnedIo(verified, Thread.currentThread(), identity.bootId,
      identity.bootGeneration, identity.connectionGeneration)
    val expectation = inputs.auditExpectation
    val physicalQualification: MySqlObservedPhysicalBinding = qualificationState match
      case "NONE" => MySqlNoPhysicalBinding
      case "CERTIFIED" => MySqlCertifiedPhysicalBinding(expectation.bindingUuid, 2)
      case "LEGACY" => MySqlLegacyPhysicalBinding(expectation.bindingUuid, 2)
      case other => throw new IllegalArgumentException("unknown test physical qualification: " + other)
    val qualification = new VerifiedSchemaQualification(expectation.profile, expectation.expectedPhysicalFormat,
      verified, Thread.currentThread(), identity.bootId, identity.bootGeneration, identity.connectionGeneration,
      expectation.serverUuid, expectation.schema, expectation.bindingUuid, physicalQualification,
      expectation.provisioningUuid, expectation.ddlSha256, expectation.objects.canonicalSha256,
      "NONE", None, None, None, None, 0L, None, System.nanoTime(), Duration.ofMinutes(5).toNanos)
    val rollbackOnly = new MySqlRollbackOnlyState
    MySqlConnectionTransactionGate.enter(connection, rollbackOnly).foreach(throw _)
    val gate = MySqlConnectionTransactionGate.currentOwner(connection, rollbackOnly)
    val reader = MySqlDbFirstBootstrapSnapshotJdbc.create(inputs, root, identity)

    def read(): MySqlScriptSchemaSnapshot = reader.readLocked(verified, scope, qualification, gate, rollbackOnly)

    def readCertifiedReadback(): MySqlScriptSchemaSnapshot =
      reader.readCertifiedReadbackLocked(verified, scope, qualification, gate, rollbackOnly)

    def close(): Unit =
      try reader.close()
      finally
        try MySqlConnectionTransactionGate.leave(connection, rollbackOnly)
        finally
          try inputs.close()
          finally
            try assembly.close()
            finally root.closeRootReadBudget()

  @Test def unboundAndLegacyBootstrapReadAllTwentySevenRelationsWithoutOrdinaryAuthority(): Unit =
    Vector(("NONE", "NONE"), ("LEGACY", "LEGACY_UNCERTIFIED")).foreach { (qualification, binding) =>
      val h = new Harness(qualification, binding)
      try
        val image = h.read()
        assertEquals(27, image.relations.size)
        assertEquals(h.inputs.full27Inputs.canonicalOrder.map(_.table), image.relations.map(_.table))
        assertEquals(if binding == "NONE" then 0 else 1, image.relation("physical_recovery_binding").rows.size)
        assertEquals(28, h.counts.statements.size)
        assertEquals(28, h.counts.statementClosures)
        assertEquals(28, h.counts.resultSetClosures)
        assertEquals(0, h.counts.commits)
        assertEquals(0, h.counts.rollbacks)
        assertEquals(0, h.counts.autoCommitChanges)
        assertEquals(0, h.counts.isolationChanges)
        assertTrue(h.rollbackOnly.cause.isEmpty)
        assertFalse(h.root.isInitialized)
      finally h.close()
    }

  @Test def certifiedQualificationIsRejectedBeforeAnySqlAndReaderDoesNotTouchCallerTransaction(): Unit =
    val h = new Harness("CERTIFIED", "CERTIFIED")
    try
      assertTrue(h.counts.statements.isEmpty, "holder creation and registration precede every SQL statement")
      assertFalse(h.root.isInitialized, "bootstrap DATA capture must not issue a snapshot image")
      val failure = assertThrows(classOf[IllegalStateException], () => h.read())
      assertTrue(failure.getMessage.contains("FirstBinding6"))
      assertTrue(h.counts.statements.isEmpty, "CERTIFIED is not a first-binding bootstrap source")
      assertSame(failure, h.rollbackOnly.cause.get)
      assertEquals(0, h.counts.commits)
      assertEquals(0, h.counts.rollbacks)
      assertEquals(0, h.counts.autoCommitChanges)
      assertEquals(0, h.counts.isolationChanges)
      assertFalse(h.root.isInitialized)
    finally h.close()

  @Test def bootstrapReaderLocksManifestThenRejectsCertifiedTableRowAndRetainsPartialDataForCleanup(): Unit =
    val h = new Harness("LEGACY", "CERTIFIED")
    try
      assertTrue(h.counts.statements.isEmpty)
      assertFalse(h.root.isInitialized)
      val failure = assertThrows(classOf[java.sql.SQLDataException], () => h.read())
      assertTrue(failure.getMessage.contains("LEGACY_UNCERTIFIED Format2 binding"))
      assertEquals(3, h.counts.statements.size, "packet, manifest and physical-binding queries stop at the rejected root row")
      assertEquals("SELECT @@SESSION.max_allowed_packet", h.counts.statements.head)
      assertTrue(h.counts.statements(1).endsWith("FOR UPDATE"))
      assertTrue(h.counts.statements(2).endsWith("FOR UPDATE"))
      assertSame(failure, h.rollbackOnly.cause.get)
      assertEquals(0, h.counts.commits)
      assertEquals(0, h.counts.rollbacks)
      assertEquals(0, h.counts.autoCommitChanges)
      assertEquals(0, h.counts.isolationChanges)
      assertFalse(h.root.isInitialized)
      val state = h.reader.cleanupObservation
      assertTrue(state.dataChildOpen)
      assertEquals(2, state.partialRelationCount)
      assertTrue(state.inputBorrowRetained)
      assertNotNull(h.reader.firstFailure.get)
    finally h.close()

  @Test def certifiedReadbackRequiresCertifiedQualificationAndExactCertifiedRoot(): Unit =
    val mismatchedQualification = new Harness("LEGACY", "CERTIFIED")
    try
      val failure = assertThrows(classOf[IllegalStateException], () => mismatchedQualification.readCertifiedReadback())
      assertTrue(failure.getMessage.contains("Certified Format2 qualification"))
      assertTrue(mismatchedQualification.counts.statements.isEmpty,
        "a legacy current qualification cannot enter the CERTIFY/RELEASE readback path")
      assertSame(failure, mismatchedQualification.rollbackOnly.cause.get)
      assertEquals(0, mismatchedQualification.counts.commits)
      assertEquals(0, mismatchedQualification.counts.rollbacks)
      assertEquals(0, mismatchedQualification.counts.autoCommitChanges)
      assertEquals(0, mismatchedQualification.counts.isolationChanges)
    finally mismatchedQualification.close()

    val mismatchedRoot = new Harness("CERTIFIED", "LEGACY_UNCERTIFIED")
    try
      val failure = assertThrows(classOf[java.sql.SQLDataException], () => mismatchedRoot.readCertifiedReadback())
      assertTrue(failure.getMessage.contains("CERTIFIED Format2 binding"))
      assertEquals(3, mismatchedRoot.counts.statements.size,
        "the certified reader must stop at the first locked B row when its state is not CERTIFIED")
      assertSame(failure, mismatchedRoot.rollbackOnly.cause.get)
      assertEquals(0, mismatchedRoot.counts.commits)
      assertEquals(0, mismatchedRoot.counts.rollbacks)
      assertEquals(0, mismatchedRoot.counts.autoCommitChanges)
      assertEquals(0, mismatchedRoot.counts.isolationChanges)
      assertEquals(2, mismatchedRoot.reader.cleanupObservation.partialRelationCount)
      assertTrue(mismatchedRoot.reader.cleanupObservation.dataChildOpen)
    finally mismatchedRoot.close()

  @Test def certifiedReadbackReturnsOnlyCompleteFull27DataAndDoesNotIssueAnImageOrTouchTransaction(): Unit =
    val h = new Harness("CERTIFIED", "CERTIFIED")
    try
      assertThrows(classOf[IllegalArgumentException],()=>h.reader.requirePreparationComparisonSource(null,h.root))
      val snapshot = h.readCertifiedReadback()
      assertSame(snapshot, h.reader.snapshot)
      h.reader.requirePreparationComparisonSource(snapshot,h.root)
      assertThrows(classOf[IllegalArgumentException],()=>
        h.reader.requirePreparationComparisonSource(snapshot,null))
      assertThrows(classOf[IllegalArgumentException],()=>
        MySqlDbFirstInitialPreparationPersistedRead.withCommittedIdentity(snapshot,UUID.randomUUID(),h.root)((_,_,_,_)=>()))
      val missing=assertThrows(classOf[java.sql.SQLDataException],()=>
        MySqlDbFirstInitialPreparationPersistedRead.withCommittedBootstrapIdentity(h.reader,snapshot,
          UUID.randomUUID(),h.root)((_,_,_,_)=>()))
      assertTrue(missing.getMessage.contains("original preparation operation is not unique"),
        "real bootstrap owner must reach identity validation without borrowing ordinary image authority")
      assertThrows(classOf[IllegalArgumentException],()=>
        MySqlDbFirstInitialActivationPersistedRead.withCommittedTransition(snapshot,UUID.randomUUID(),h.root)((_,_)=>()))
      val missingActivation=assertThrows(classOf[java.sql.SQLDataException],()=>
        MySqlDbFirstInitialActivationPersistedRead.withCommittedIdentity(snapshot,UUID.randomUUID(),h.root,
          Some(h.reader),None)((_,_,_,_)=>()))
      assertTrue(missingActivation.getMessage.contains("original operation is not unique"))
      assertEquals(27, snapshot.relations.size)
      assertEquals(28, h.counts.statements.size, "packet plus all 27 locked relation queries")
      assertTrue(h.counts.statements(1).endsWith("FOR UPDATE"))
      assertTrue(h.counts.statements(2).endsWith("FOR UPDATE"))
      assertTrue(h.counts.statements(3).endsWith("FOR UPDATE"))
      assertEquals(MySqlDbFirstSnapshotSql.lockedReadOrder.map(shape =>
        MySqlDbFirstSnapshotSql.select(h.inputs.full27Inputs.schema, shape.table)), h.counts.statements.drop(1).toVector)
      assertFalse(h.root.isInitialized, "Certified readback is DATA only and cannot issue an ordinary image")
      assertTrue(h.rollbackOnly.cause.isEmpty)
      assertEquals(0, h.counts.commits)
      assertEquals(0, h.counts.rollbacks)
      assertEquals(0, h.counts.autoCommitChanges)
      assertEquals(0, h.counts.isolationChanges)
      assertTrue(h.reader.cleanupObservation.dataChildOpen,
        "the caller borrows complete DATA while the reader retains its child until explicit close")
      assertEquals(28, h.counts.statementClosures)
      assertEquals(28, h.counts.resultSetClosures)
      h.reader.close()
      assertThrows(classOf[IllegalStateException],()=>h.reader.requirePreparationComparisonSource(snapshot,h.root))
      assertThrows(classOf[IllegalStateException],()=>
        MySqlDbFirstInitialActivationPersistedRead.withCommittedIdentity(snapshot,UUID.randomUUID(),h.root,
          Some(h.reader),None)((_,_,_,_)=>()))
    finally h.close()

  @Test def relationCloseFailuresKeepTheOriginalCauseAndSuppressedStatementFailureWithPartialData(): Unit =
    val h = new Harness("LEGACY", "LEGACY_UNCERTIFIED")
    h.counts.failResultSetCloseForTable = Some("physical_recovery_binding")
    h.counts.failStatementCloseForTable = Some("physical_recovery_binding")
    try
      val failure = assertThrows(classOf[MySqlFormat3SnapshotReadException], () => h.read())
      assertTrue(failure.getMessage.contains("physical_recovery_binding"))
      assertInstanceOf(classOf[SQLException], failure.getCause)
      assertEquals("synthetic ResultSet close failure for physical_recovery_binding", failure.getCause.getMessage)
      assertEquals(1, failure.getCause.getSuppressed.length)
      assertEquals("synthetic PreparedStatement close failure for physical_recovery_binding",
        failure.getCause.getSuppressed.head.getMessage)
      assertSame(failure, h.rollbackOnly.cause.get)
      assertEquals(3, h.counts.statements.size)
      assertEquals(1, h.reader.cleanupObservation.partialRelationCount,
        "the already-read manifest remains owned after the binding ResultSet close failure")
      assertTrue(h.reader.cleanupObservation.dataChildOpen)
      assertEquals(3, h.counts.statementClosures)
      assertEquals(3, h.counts.resultSetClosures)
      assertEquals(0, h.counts.commits)
      assertEquals(0, h.counts.rollbacks)
      assertEquals(0, h.counts.autoCommitChanges)
      assertEquals(0, h.counts.isolationChanges)
    finally h.close()

  private def createConnection(counts: JdbcCounts, binding: UUID, bindingRowState: String,
      manifestValues: Map[String, Any]): Connection =
    proxy(classOf[Connection], new InvocationHandler:
      override def invoke(instance: Object, method: Method, args: Array[Object]): Object = method.getName match
        case "getAutoCommit" => java.lang.Boolean.FALSE
        case "getTransactionIsolation" => Int.box(Connection.TRANSACTION_REPEATABLE_READ)
        case "isClosed" => java.lang.Boolean.FALSE
        case "prepareStatement" =>
          val sql = args(0).asInstanceOf[String]
          counts.statements += sql
          createStatement(sql, counts, binding, bindingRowState, manifestValues)
        case "commit" => counts.commits += 1; null
        case "rollback" => counts.rollbacks += 1; null
        case "setAutoCommit" => counts.autoCommitChanges += 1; null
        case "setTransactionIsolation" => counts.isolationChanges += 1; null
        case "toString" => "BootstrapReaderTestConnection"
        case "hashCode" => Int.box(System.identityHashCode(instance))
        case "equals" => java.lang.Boolean.valueOf(instance.asInstanceOf[AnyRef] eq args(0))
        case other => defaultValue(method.getReturnType)
    )

  private def createStatement(sql: String, counts: JdbcCounts, binding: UUID,
      bindingRowState: String, manifestValues: Map[String, Any]): PreparedStatement =
    proxy(classOf[PreparedStatement], new InvocationHandler:
      override def invoke(instance: Object, method: Method, args: Array[Object]): Object = method.getName match
        case "executeQuery" => createResultSet(sql, counts, binding, bindingRowState, manifestValues)
        case "close" =>
          counts.statementClosures += 1
          val table = tableFromSql(sql)
          if counts.failStatementCloseForTable.contains(table) then
            throw new SQLException(s"synthetic PreparedStatement close failure for $table")
          null
        case "setFetchSize" => null
        case "toString" => "BootstrapReaderTestStatement"
        case other => defaultValue(method.getReturnType)
    )

  private def createResultSet(sql: String, counts: JdbcCounts, binding: UUID, bindingRowState: String,
      manifestValues: Map[String, Any]): ResultSet =
    val isPacket = sql == "SELECT @@SESSION.max_allowed_packet"
    val table = if isPacket then "" else
      "FROM `[^`]+`\\.`([^`]+)` ORDER BY".r.findFirstMatchIn(sql).map(_.group(1))
        .getOrElse(throw new AssertionError("fixed schema6 table query was not recognized: " + sql))
    val shape = if isPacket then null else MySqlDbFirstRelationShapes.forTable(table)
    val columns = if isPacket then Vector.empty else shape.columns
    val metadata = if isPacket then null else createMetadata(columns)
    val row: Vector[Vector[Any]] =
      if isPacket then Vector(Vector[Any](1_048_576L))
      else if table == "physical_recovery_binding" && bindingRowState == "NONE" then Vector.empty
      else if Set("fp_schema_manifest", "physical_recovery_binding", "fp_runtime_writer",
          MySqlDeploymentReceiptSchemaDefinition.QuotaTable).contains(table) then
        Vector(columns.map(column => valueFor(table, column.name, column.kind, column.maximumLength, column.nullable,
          binding, bindingRowState, manifestValues)))
      else Vector.empty
    var rowIndex = -1
    var lastWasNull = false
    proxy(classOf[ResultSet], new InvocationHandler:
      override def invoke(instance: Object, method: Method, args: Array[Object]): Object = method.getName match
        case "next" => rowIndex += 1; java.lang.Boolean.valueOf(rowIndex < row.size)
        case "wasNull" => java.lang.Boolean.valueOf(lastWasNull)
        case "getMetaData" => metadata
        case "getLong" =>
          val value = row(rowIndex)(args(0).asInstanceOf[Int] - 1)
          lastWasNull = value == null
          Long.box(if value == null then 0L else value.asInstanceOf[Long])
        case "getDouble" =>
          val value = row(rowIndex)(args(0).asInstanceOf[Int] - 1)
          lastWasNull = value == null
          Double.box(if value == null then 0.0 else value.asInstanceOf[Double])
        case "getCharacterStream" =>
          val value = row(rowIndex)(args(0).asInstanceOf[Int] - 1).asInstanceOf[String]
          lastWasNull = value == null
          if value == null then null else new StringReader(value)
        case "getBinaryStream" =>
          val value = row(rowIndex)(args(0).asInstanceOf[Int] - 1).asInstanceOf[Array[Byte]]
          lastWasNull = value == null
          if value == null then null else new ByteArrayInputStream(value)
        case "getObject" =>
          val value = row(rowIndex)(args(0).asInstanceOf[Int] - 1)
          lastWasNull = value == null
          value.asInstanceOf[AnyRef]
        case "close" =>
          counts.resultSetClosures += 1
          if counts.failResultSetCloseForTable.contains(table) then
            throw new SQLException(s"synthetic ResultSet close failure for $table")
          null
        case "toString" => "BootstrapReaderTestResultSet"
        case other => defaultValue(method.getReturnType)
    )

  private def valueFor(table: String, name: String, kind: MySqlFormat3SqlKind,
      maximumLength: Option[Int], nullable: Boolean, binding: UUID, bindingRowState: String,
      manifestValues: Map[String, Any]): Any =
    (table, name) match
      case ("fp_schema_manifest", field) if manifestValues.contains(field) => manifestValues(field)
      case ("fp_schema_manifest", "singleton_fixed1") => 1L
      case ("fp_schema_manifest", "backend") => "MYSQL"
      case ("fp_schema_manifest", "format_version") => 6L
      case ("fp_schema_manifest", "bootstrap_state") => "READY"
      case ("fp_schema_manifest", "import_state") => "NONE"
      case ("physical_recovery_binding", "singleton_fixed1") => 1L
      case ("physical_recovery_binding", "binding_uuid") => uuidBytes(binding)
      case ("physical_recovery_binding", "cutover_state") => bindingRowState
      case ("physical_recovery_binding", "format_version") => 2L
      case ("fp_runtime_writer", "singleton_fixed1") => 1L
      case ("fp_runtime_writer", "binding_uuid") => uuidBytes(binding)
      case (MySqlDeploymentReceiptSchemaDefinition.QuotaTable, "namespace") =>
        HexFormat.of().parseHex(MySqlDeploymentReceiptSchemaDefinition.NamespaceHex)
      case (MySqlDeploymentReceiptSchemaDefinition.QuotaTable, "max_records") => 1L
      case (MySqlDeploymentReceiptSchemaDefinition.QuotaTable, "used_records") => 0L
      case (MySqlDeploymentReceiptSchemaDefinition.QuotaTable, "quota_revision") => 0L
      case _ if nullable => null
      case _ => kind match
        case MySqlFormat3SqlKind.Text => ""
        case MySqlFormat3SqlKind.Binary => Array.ofDim[Byte](maximumLength.getOrElse(0))
        case MySqlFormat3SqlKind.Integer => 0L
        case MySqlFormat3SqlKind.Floating => 0.0
        case MySqlFormat3SqlKind.DateTime => LocalDateTime.of(2026, 1, 1, 0, 0)

  @Test def pinnedConnectorNonDecimalScaleIsZeroWhileDateTimeDdlRemainsAudited(): Unit =
    val table = "fp_runtime_writer"
    val columns = MySqlDbFirstRelationShapes.forTable(table).columns
    MySqlDbFirstSnapshotSql.validateMetadata(createMetadata(columns), table)
    val failure = assertThrows(classOf[MySqlDbFirstMetadataMismatch], () =>
      MySqlDbFirstSnapshotSql.validateMetadata(createMetadata(columns, dateTimeScale = 6), table))
    assertTrue(failure.safeDifference.contains("column=lease_until"))
    assertTrue(failure.safeDifference.contains("scaleActual=6 scaleExpected=0"))

  @Test def metadataMismatchCarriesOnlyFrozenLocationAndNumericDifference(): Unit =
    val columns = MySqlDbFirstRelationShapes.forTable("fp_schema_manifest").columns
    val failure = assertThrows(classOf[MySqlDbFirstMetadataMismatch], () =>
      MySqlDbFirstSnapshotSql.validateMetadata(createMetadata(columns, wrongFirstLabel = true), "fp_schema_manifest"))
    assertTrue(failure.safeDifference.contains("table=fp_schema_manifest column=singleton_fixed1 ordinal=1 labelMatches=false"))
    assertTrue(failure.safeDifference.contains("jdbcActual=-6 jdbcExpected=-6 nullableActual=0 nullableExpected=0"))
    assertFalse(failure.safeDifference.contains("private-driver-label-secret"))
    assertFalse(failure.getMessage.contains("private-driver-label-secret"))

  private def createMetadata(columns: Vector[MySqlFormat3ColumnShape], wrongFirstLabel: Boolean = false, dateTimeScale: Int = 0): ResultSetMetaData =
    proxy(classOf[ResultSetMetaData], new InvocationHandler:
      override def invoke(instance: Object, method: Method, args: Array[Object]): Object = method.getName match
        case "getColumnCount" => Int.box(columns.size)
        case "getColumnLabel" | "getColumnName" =>
          if wrongFirstLabel && args(0).asInstanceOf[Int] == 1 then "private-driver-label-secret"
          else columns(args(0).asInstanceOf[Int] - 1).name
        case "getColumnType" =>
          val column = columns(args(0).asInstanceOf[Int] - 1)
          val jdbc = if column.sqlDeclaration.startsWith("MEDIUMBLOB") then Types.LONGVARBINARY else column.jdbcType
          Int.box(jdbc)
        case "isNullable" =>
          val column = columns(args(0).asInstanceOf[Int] - 1)
          Int.box(if column.nullable then ResultSetMetaData.columnNullable else ResultSetMetaData.columnNoNulls)
        case "getScale" =>
          val column = columns(args(0).asInstanceOf[Int] - 1)
          Int.box(if column.kind == MySqlFormat3SqlKind.DateTime then dateTimeScale else 0)
        case other => defaultValue(method.getReturnType)
    )

  private def uuidBytes(value: UUID): Array[Byte] =
    val buffer = java.nio.ByteBuffer.allocate(16)
    buffer.putLong(value.getMostSignificantBits)
    buffer.putLong(value.getLeastSignificantBits)
    buffer.array()

  private def tableFromSql(sql: String): String =
    "FROM `[^`]+`\\.`([^`]+)` ORDER BY".r.findFirstMatchIn(sql).map(_.group(1)).getOrElse("")

  private def proxy[A](clazz: Class[A], handler: InvocationHandler): A =
    Proxy.newProxyInstance(clazz.getClassLoader, Array[Class[?]](clazz), handler).asInstanceOf[A]

  private def defaultValue(returnType: Class[?]): AnyRef =
    if !returnType.isPrimitive then null
    else if returnType == java.lang.Boolean.TYPE then java.lang.Boolean.FALSE
    else if returnType == java.lang.Integer.TYPE then Int.box(0)
    else if returnType == java.lang.Long.TYPE then Long.box(0L)
    else if returnType == java.lang.Double.TYPE then Double.box(0.0)
    else if returnType == java.lang.Float.TYPE then Float.box(0.0f)
    else if returnType == java.lang.Short.TYPE then Short.box(0.toShort)
    else if returnType == java.lang.Byte.TYPE then Byte.box(0.toByte)
    else if returnType == java.lang.Character.TYPE then java.lang.Character.valueOf(0.toChar)
    else null
