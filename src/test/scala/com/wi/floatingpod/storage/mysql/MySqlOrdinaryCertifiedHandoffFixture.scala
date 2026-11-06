package com.wi.floatingpod.storage.mysql

import com.google.gson.{GsonBuilder, JsonArray, JsonObject}
import com.google.gson.JsonParser
import com.wi.floatingpod.bootstrap.{ProductionBootstrapControl, ProductionOrdinarySourceApproval}
import com.wi.floatingpod.config.{MySqlRuntimeMetadata, SourceLimitProfile}
import com.wi.floatingpod.server.{WindowsB1SourceAnchorStore, WindowsWorldProvisioner}
import com.wi.floatingpod.server.io.IoOperationTag
import java.io.IOException
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData, Statement, Types}
import java.time.{Duration, LocalDateTime}
import java.util.{HexFormat, Locale, UUID}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*

/** Frozen, test-only Format3 shape and business DATA. This fixture issues no source, session or native capability. */
private[mysql] object MySqlOrdinaryCertifiedHandoffFixture:
  private val Resource = "/com/wi/floatingpod/storage/mysql/approved-static-baseline-format3-ordinary-fixture.json"
  private val ResourceSha256 = "34D10D2AEB0C73241C67E5A0D7EFD86752F7F7C43DA16F27765DA4A748BADF74"

  // These three contracts intentionally have different cardinalities and meanings.
  val StaticResultSetIds = Vector(
    "RS02_TABLES", "RS03_COLUMNS", "RS04_STATISTICS", "RS05_TABLE_CONSTRAINTS", "RS06_KEY_COLUMN_USAGE",
    "RS07_REFERENTIAL_CONSTRAINTS", "RS08_CHECK_CONSTRAINTS", "RS09_TRIGGERS", "RS10_AUDITSELF",
    "RS11_AUDITSELF_PARAMETERS")
  val RuntimeResultSetIds = Vector(
    "RS01_HEADER", "RS02_TABLES", "RS03_COLUMNS", "RS04_STATISTICS", "RS05_TABLE_CONSTRAINTS",
    "RS06_KEY_COLUMN_USAGE", "RS07_REFERENTIAL_CONSTRAINTS", "RS08_CHECK_CONSTRAINTS", "RS09_TRIGGERS",
    "RS10_AUDITSELF", "RS11_AUDITSELF_PARAMETERS", "RS12_BINDING", "RS13_WRITER", "RS14_FOOTER",
    "RS15_TABLE_PRIVILEGES", "RS16_COLUMN_PRIVILEGES", "RS17_SCHEMA_PRIVILEGES", "RS18_USER_PRIVILEGES")
  val Full20RelationNames = Vector(
    "fp_schema_manifest", "fp_runtime_writer", "physical_recovery_binding", "fp_recovery_operation",
    "floating_pod_state", "player_entitlement", "binding_bootstrap_intent", "fp_recovery_reservation",
    "physical_recovery_footprint", "physical_recovery_footprint_chunk", "physical_recovery_intent",
    "physical_recovery_locator", "physical_recovery_obligation", "fp_lifecycle_completion",
    "physical_footprint_transfer", "physical_source_unit", "physical_source_forest", "physical_source_transfer",
    "physical_source_write", "physical_source_obligation")

  private val oldLease = LocalDateTime.of(2026, 10, 11, 1, 0, 0, 123456000)

  /** Reads only the pinned independent relation-definition fixture; no production loader/auditor generates this data. */
  private def fixtureBytes(): Array[Byte] =
    val stream = Option(getClass.getResourceAsStream(Resource)).getOrElse(
      throw new IOException("independent Format3 relation-definition fixture is missing"))
    val bytes = try stream.readAllBytes() finally stream.close()
    val actual = HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
    require(actual == ResourceSha256, "independent Format3 relation-definition fixture digest changed")
    bytes

  def relationDefinitions(): JsonObject =
    JsonParser.parseString(new String(fixtureBytes(), StandardCharsets.UTF_8)).getAsJsonObject

  def tableNames(): Vector[String] =
    val names = relationDefinitions().keySet().asScala.filter(Full20RelationNames.toSet).toVector.sorted
    require(names.toSet == Full20RelationNames.toSet && names.size == 20,
      "frozen Format3 relation definitions must cover the exact full20 relation set")
    names

  /** Frozen independent synthetic object contract; synthetic bytes never imply production eligibility. */
  def syntheticContract(): JsonObject =
    val value = relationDefinitions().getAsJsonObject("_ordinarySyntheticContract")
    require(value != null && value.get("attribution").getAsString == "SYNTHETIC_ORDINARY_TEST_ONLY",
      "ordinary object definitions must retain permanent synthetic attribution")
    value

  def syntheticTriggerNames(): Vector[String] =
    tableNames().indices.toVector.flatMap { index =>
      val number = f"${index + 1}%02d"
      Vector(s"fp_f3_${number}_bi", s"fp_f3_${number}_bu", s"fp_f3_${number}_bd")
    }

  def columnDeclarations(): Vector[(String, String, String)] =
    val definitions = relationDefinitions()
    tableNames().flatMap { table =>
      val columns = definitions.getAsJsonObject(table).getAsJsonArray("cols")
      columns.iterator().asScala.toVector.map { raw =>
        val pair = raw.getAsJsonArray
        require(pair.size() == 2, "frozen relation column must retain its name and SQL type declaration")
        (table, pair.get(0).getAsString, pair.get(1).getAsString)
      }
    }

  /** Full20 typed business image with a genuine row and SQL-NULL receipt pair; still DATA, not authority. */
  def full20WithUnownedHistoricalWriter(): MySqlFormat3FullSnapshot =
    import MainBootProjectionFixture.*
    val writer = row("fp_runtime_writer",
      "singleton_fixed1" -> i(1),
      "binding_uuid" -> u(id(2)),
      "writer_epoch" -> i(7),
      "owner_boot_uuid" -> MySqlFormat3Cell.SqlNull,
      "lease_until" -> MySqlFormat3Cell.DateTime(oldLease),
      "last_commit_operation_uuid" -> MySqlFormat3Cell.SqlNull,
      "last_commit_fingerprint" -> MySqlFormat3Cell.SqlNull)
    snapshot(extra = Map(
      "fp_runtime_writer" -> Vector(writer),
      "floating_pod_state" -> Vector(pod(number = 41L, active = 0L, updated = 1_791_683_200_000L))))

  def historicalWriterAssertion(): com.wi.floatingpod.server.B1ResumeRecord.WriterAssertions =
    import MainBootProjectionFixture.id
    new com.wi.floatingpod.server.B1ResumeRecord.WriterAssertions(id(2), 7L, null, oldLease, null)

  /** Independent synthetic DDL bytes: the loader's expected manifest is built from these fixture definitions,
    * never from MySqlApprovedObjectManifest or MySqlSchemaAuditor output.
    */
  def syntheticDdlBytes(): Array[Byte] =
    val definitions = relationDefinitions()
    val contract = syntheticContract()
    val schema = contract.get("schema").getAsString
    val tableDdl = tableNames().map { table =>
      val definition = definitions.getAsJsonObject(table)
      val columns = definition.getAsJsonArray("cols").iterator().asScala.toVector.map { raw =>
        val pair = raw.getAsJsonArray
        "  `" + pair.get(0).getAsString + "` " + pair.get(1).getAsString
      }
      val primary = definition.getAsJsonArray("pk").iterator().asScala.map(_.getAsString).mkString(", ")
      val checks = definition.getAsJsonArray("checks").iterator().asScala.zipWithIndex.map { case (raw, index) =>
        "  CONSTRAINT `synthetic_" + table + "_check_" + f"${index + 1}%02d" + "` CHECK (" + raw.getAsString + ")"
      }.toVector
      val body = columns ++ Vector("  PRIMARY KEY (" + primary + ")") ++ checks
      "CREATE TABLE `" + schema + "`.`" + table + "` (\n" + body.mkString(",\n") + "\n) ENGINE=InnoDB;"
    }
    val triggers = syntheticTriggerNames().map { name =>
      val tableIndex = name.substring("fp_f3_".length, "fp_f3_".length + 2).toInt - 1
      val table = tableNames()(tableIndex)
      val event = name.takeRight(2) match
        case "bi" => "INSERT"
        case "bu" => "UPDATE"
        case "bd" => "DELETE"
      "CREATE TRIGGER `" + schema + "`.`" + name + "` BEFORE " + event + " ON `" + schema + "`.`" + table +
        "` FOR EACH ROW " + contract.get("triggerBody").getAsString
    }
    val procedure = "CREATE DEFINER=`" + contract.get("auditDefiner").getAsString + "` PROCEDURE `" + schema + "`.`" +
      contract.get("auditProcedure").getAsString + "`() SQL SECURITY DEFINER READS SQL DATA " +
      contract.get("auditRoutineBody").getAsString
    (tableDdl ++ triggers ++ Vector(procedure)).mkString("\n").getBytes(StandardCharsets.UTF_8)

  private val StaticLabels = Vector(
    Vector("TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ENGINE", "VERSION", "ROW_FORMAT", "TABLE_COLLATION", "CREATE_OPTIONS", "TABLE_COMMENT"),
    Vector("TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE", "DATETIME_PRECISION", "CHARACTER_SET_NAME", "COLLATION_NAME", "COLUMN_TYPE", "COLUMN_KEY", "EXTRA", "COLUMN_COMMENT", "GENERATION_EXPRESSION"),
    Vector("TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE", "INDEX_SCHEMA", "INDEX_NAME", "SEQ_IN_INDEX", "COLUMN_NAME", "COLLATION", "SUB_PART", "PACKED", "NULLABLE", "INDEX_TYPE", "COMMENT", "INDEX_COMMENT", "IS_VISIBLE", "EXPRESSION"),
    Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_TYPE", "ENFORCED"),
    Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "POSITION_IN_UNIQUE_CONSTRAINT", "REFERENCED_TABLE_SCHEMA", "REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME"),
    Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "UNIQUE_CONSTRAINT_CATALOG", "UNIQUE_CONSTRAINT_SCHEMA", "UNIQUE_CONSTRAINT_NAME", "MATCH_OPTION", "UPDATE_RULE", "DELETE_RULE", "TABLE_NAME", "REFERENCED_TABLE_NAME"),
    Vector("CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "CHECK_CLAUSE", "clause_octets", "clause_sha256"),
    Vector("TRIGGER_SCHEMA", "TRIGGER_NAME", "EVENT_OBJECT_SCHEMA", "EVENT_OBJECT_TABLE", "EVENT_MANIPULATION", "ACTION_TIMING", "ACTION_ORDER", "ACTION_ORIENTATION", "ACTION_STATEMENT", "SQL_MODE", "DEFINER", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION", "statement_octets", "statement_sha256"),
    Vector("ROUTINE_SCHEMA", "ROUTINE_NAME", "ROUTINE_TYPE", "ROUTINE_DEFINITION", "SECURITY_TYPE", "SQL_DATA_ACCESS", "SQL_MODE", "DEFINER", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "DATABASE_COLLATION", "IS_DETERMINISTIC", "DATA_TYPE", "DTD_IDENTIFIER", "ROUTINE_COMMENT", "definition_octets", "definition_sha256"),
    Vector("SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME", "ORDINAL_POSITION", "PARAMETER_MODE", "PARAMETER_NAME", "DATA_TYPE", "DTD_IDENTIFIER", "CHARACTER_SET_NAME", "COLLATION_NAME"))

  private val HeaderLabels = Vector(
    "server_uuid", "server_version", "connection_id", "audit_definer", "connected_caller", "effective_roles",
    "routine_default_schema", "observed_db_time", "audited_schema", "singleton_fixed1", "backend", "format_version",
    "ddl_sha256", "provisioning_uuid", "bootstrap_state", "expected_server_uuid", "expected_binding_uuid", "import_state",
    "import_operation_uuid", "import_manifest_sha256", "import_snapshot_sha256", "import_receipt_sha256", "guard_manifest_sha256",
    "configuration_sha256", "source_provenance_kind", "source_provenance_bytes", "source_provenance_sha256",
    "limit_profile_encoding", "limit_profile_bytes", "limit_profile_sha256", "max_region_bytes", "max_chunk_bytes",
    "max_decompressed_bytes", "max_nbt_depth", "max_members", "max_retained_bytes", "max_sql_packet_bytes",
    "max_native_artifact_bytes")
  private val CallerScopeLabels = Vector("server_uuid", "connection_id", "caller_default_schema", "caller_effective_principal",
    "caller_effective_roles", "connected_user", "caller_server_version")

  private def text(value: String): MySqlAuditCell = MySqlAuditText(value)
  private def sqlNull: MySqlAuditCell = MySqlAuditNull
  private def digest(bytes: Array[Byte]): MySqlAuditCell = MySqlAuditBinary(MessageDigest.getInstance("SHA-256").digest(bytes))
  private def bytesOf(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)
  private def safeDeclType(declaration: String): String = declaration.takeWhile(ch => ch != ' ' && ch != '(').toLowerCase(java.util.Locale.ROOT)
  private def lengthFrom(declaration: String): Option[String] =
    val open = declaration.indexOf('(')
    val close = declaration.indexOf(')', open + 1)
    if open >= 0 && close > open then Some(declaration.substring(open + 1, close)) else None

  private def independentStaticSets(): Vector[MySqlAuditResultSet] =
    val definitions = relationDefinitions()
    val contract = syntheticContract()
    val schema = contract.get("schema").getAsString
    val tables = tableNames()
    val columns = columnDeclarations()
    def rows(values: Vector[Vector[MySqlAuditCell]]): Vector[MySqlAuditRow] = values.map(MySqlAuditRow.apply)
    def jdbcType(setIndex: Int, label: String): Int =
      val integer = Set("ORDINAL_POSITION", "POSITION_IN_UNIQUE_CONSTRAINT", "NON_UNIQUE", "SEQ_IN_INDEX", "ACTION_ORDER")
      val wideInteger = Set("CHARACTER_MAXIMUM_LENGTH", "CHARACTER_OCTET_LENGTH", "NUMERIC_PRECISION", "NUMERIC_SCALE",
        "DATETIME_PRECISION", "clause_octets", "statement_octets", "definition_octets")
      if label.endsWith("sha256") then Types.VARBINARY
      else if integer.contains(label) then Types.INTEGER
      else if wideInteger.contains(label) then Types.BIGINT
      else Types.VARCHAR
    def set(index: Int, values: Vector[Vector[MySqlAuditCell]]): MySqlAuditResultSet =
      val id = StaticResultSetIds(index)
      MySqlAuditResultSet(id, StaticLabels(index).map { label =>
        val kind = jdbcType(index, label)
        val typeName = kind match
          case Types.INTEGER => "INT"
          case Types.BIGINT => "BIGINT"
          case Types.VARBINARY => "VARBINARY"
          case _ => "VARCHAR"
        MySqlAuditColumn(label, kind, typeName, if kind == Types.VARBINARY then 32 else 4096, 0, 1, kind == Types.INTEGER || kind == Types.BIGINT)
      }, rows(values))
    val tableRows = tables.map(name => Vector(text(schema), text(name), text("BASE TABLE"), text("InnoDB"),
      text("10"), text("Dynamic"), text("utf8mb4_0900_bin"), text(""), text("")))
    val primaryByTable = tables.map { table =>
      val values = definitions.getAsJsonObject(table).getAsJsonArray("pk").iterator().asScala.map(_.getAsString).toVector
      table -> values
    }.toMap
    val columnRows = tables.flatMap { table =>
      definitions.getAsJsonObject(table).getAsJsonArray("cols").iterator().asScala.zipWithIndex.map { case (raw, index) =>
        val pair = raw.getAsJsonArray
        val name = pair.get(0).getAsString
        val declaration = pair.get(1).getAsString
        val kind = safeDeclType(declaration)
        val charType = Set("varchar", "char", "text", "longtext").contains(kind)
        val binaryType = Set("varbinary", "binary", "blob", "longblob").contains(kind)
        val max = lengthFrom(declaration)
        Vector(text(schema), text(table), text(name), text((index + 1).toString), sqlNull,
          text(if declaration.endsWith(" NULL") && !declaration.endsWith(" NOT NULL") then "YES" else "NO"), text(kind),
          if charType || binaryType then max.map(text).getOrElse(sqlNull) else sqlNull,
          if charType && max.nonEmpty then max.map(value => text((value.toInt * 4).toString)).getOrElse(sqlNull)
          else if binaryType then max.map(text).getOrElse(sqlNull) else sqlNull,
          sqlNull, sqlNull, if kind == "datetime" then text("6") else sqlNull,
          if charType then text("utf8mb4") else sqlNull,
          if charType then text("utf8mb4_0900_bin") else sqlNull,
          text(declaration.toLowerCase(java.util.Locale.ROOT)), text(if primaryByTable(table).contains(name) then "PRI" else ""),
          text(""), text(""), text(""))
      }
    }
    val statisticRows = tables.flatMap { table => primaryByTable(table).zipWithIndex.map { case (column, index) =>
      Vector(text(schema), text(table), text("0"), text(schema), text("PRIMARY"), text((index + 1).toString),
        text(column), text("A"), sqlNull, sqlNull, text("NO"), text("BTREE"), text(""), text(""), text("YES"), sqlNull)
    }}
    val constraintRows = tables.flatMap { table =>
      if primaryByTable(table).nonEmpty then Vector(Vector(text("def"), text(schema), text("PRIMARY"), text(schema), text(table), text("PRIMARY KEY"), text("YES")))
      else Vector.empty
    }
    val keyRows = tables.flatMap { table => primaryByTable(table).zipWithIndex.map { case (column, index) =>
      Vector(text("def"), text(schema), text("PRIMARY"), text("def"), text(schema), text(table), text(column), text((index + 1).toString),
        sqlNull, sqlNull, sqlNull, sqlNull)
    }}
    val checkRows = tables.flatMap { table =>
      definitions.getAsJsonObject(table).getAsJsonArray("checks").iterator().asScala.zipWithIndex.map { case (raw, index) =>
        val clause = raw.getAsString
        Vector(text("def"), text(schema), text("synthetic_" + table + "_check_" + f"${index + 1}%02d"), text(clause),
          text(bytesOf(clause).length.toString), digest(bytesOf(clause)))
      }
    }
    val triggerRows = syntheticTriggerNames().map { name =>
      val number = name.substring("fp_f3_".length, "fp_f3_".length + 2).toInt - 1
      val table = tables(number)
      val event = name.takeRight(2) match
        case "bi" => "INSERT"
        case "bu" => "UPDATE"
        case "bd" => "DELETE"
      val body = contract.get("triggerBody").getAsString
      Vector(text(schema), text(name), text(schema), text(table), text(event), text("BEFORE"), text("1"), text("ROW"), text(body),
        text("STRICT_ALL_TABLES"), text(contract.get("auditDefiner").getAsString), text("utf8mb4"), text("utf8mb4_0900_bin"),
        text("utf8mb4_0900_bin"), text(bytesOf(body).length.toString), digest(bytesOf(body)))
    }
    val routine = contract.get("auditRoutineBody").getAsString
    val routineRows = Vector(Vector(text(schema), text(contract.get("auditProcedure").getAsString), text("PROCEDURE"), text(routine),
      text("DEFINER"), text("READS SQL DATA"), text("STRICT_ALL_TABLES"), text(contract.get("auditDefiner").getAsString),
      text("utf8mb4"), text("utf8mb4_0900_bin"), text("utf8mb4_0900_bin"), text("NO"), sqlNull, sqlNull, text(""),
      text(bytesOf(routine).length.toString), digest(bytesOf(routine))))
    Vector(set(0, tableRows), set(1, columnRows), set(2, statisticRows), set(3, constraintRows), set(4, keyRows),
      set(5, Vector.empty), set(6, checkRows), set(7, triggerRows), set(8, routineRows), set(9, Vector.empty))

  private def encodeCell(cell: MySqlAuditCell): JsonObject =
    val result = new JsonObject
    cell match
      case MySqlAuditNull => result.addProperty("tag", "SQL_NULL")
      case MySqlAuditText(value) => result.addProperty("tag", "TEXT"); result.addProperty("value", value)
      case binary: MySqlAuditBinary => result.addProperty("tag", "BINARY"); result.addProperty("hex", HexFormat.of().withUpperCase().formatHex(binary.bytes))
      case _ => throw new IllegalArgumentException("synthetic static baseline contains an unsupported cell kind")
    result

  private def encodeResultSet(set: MySqlAuditResultSet): JsonObject =
    val result = new JsonObject
    result.addProperty("id", set.id)
    val columnArray = new JsonArray
    set.columns.foreach { column =>
      val encoded = new JsonObject
      encoded.addProperty("label", column.label); encoded.addProperty("jdbcType", column.jdbcType)
      encoded.addProperty("typeName", column.typeName); encoded.addProperty("precision", column.precision)
      encoded.addProperty("scale", column.scale); encoded.addProperty("nullable", column.nullable); encoded.addProperty("signed", column.signed)
      columnArray.add(encoded)
    }
    result.add("columns", columnArray)
    val rowArray = new JsonArray
    set.rows.foreach { row =>
      val encoded = new JsonArray
      row.cells.foreach(cell => encoded.add(encodeCell(cell)))
      rowArray.add(encoded)
    }
    result.add("rows", rowArray)
    result

  /** Creates an independently specified ten-set Format3 pack, then exercises the real pinned loader. */
  def loadIndependentStaticBaseline(directory: Path): MySqlApprovedStaticBaseline =
    require(directory != null && directory.isAbsolute, "test-owned static pack directory must be absolute")
    Files.createDirectories(directory)
    val definitions = relationDefinitions()
    val contract = syntheticContract()
    val sourceCaptureSha = HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("SHA-256").digest(fixtureBytes()))
    val ddlSha = HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("SHA-256").digest(syntheticDdlBytes()))
    val root = new JsonObject
    root.addProperty("format", MySqlApprovedBaselineLoader.ApprovedFormat)
    root.addProperty("schemaFormat", MySqlApprovedBaselineLoader.Format3SchemaFormat)
    root.addProperty("coverage", MySqlAuditCoverage.Format3)
    root.addProperty("serverUuid", contract.get("serverUuid").getAsString)
    root.addProperty("serverVersion", contract.get("serverVersion").getAsString)
    root.addProperty("schema", contract.get("schema").getAsString)
    root.addProperty("provisioningUuid", contract.get("provisioningUuid").getAsString)
    root.addProperty("bindingUuid", contract.get("bindingUuid").getAsString)
    root.addProperty("ddlSha256", ddlSha)
    root.addProperty("auditDefiner", contract.get("auditDefiner").getAsString)
    root.addProperty("sourceCaptureSha256", sourceCaptureSha)
    val resultArray = new JsonArray
    independentStaticSets().foreach(set => resultArray.add(encodeResultSet(set)))
    root.add("resultSets", resultArray)
    val bytes = new GsonBuilder().disableHtmlEscaping().create().toJson(root).getBytes(StandardCharsets.UTF_8)
    val path = directory.resolve("approved-static-baseline-format3-ordinary-synthetic.json")
    Files.write(path, bytes)
    val identity = MySqlApprovedBaselineIdentity(MySqlApprovedBaselineLoader.ApprovedFormat,
      MySqlApprovedBaselineLoader.Format3SchemaFormat, MySqlAuditCoverage.Format3,
      java.util.UUID.fromString(contract.get("serverUuid").getAsString), contract.get("serverVersion").getAsString,
      contract.get("schema").getAsString, java.util.UUID.fromString(contract.get("provisioningUuid").getAsString),
      java.util.UUID.fromString(contract.get("bindingUuid").getAsString), ddlSha, contract.get("auditDefiner").getAsString, sourceCaptureSha)
    val expectedSha = HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
    val loaded = MySqlApprovedBaselineLoader.load(path, expectedSha, identity)
    require(loaded.identity == identity && loaded.objects.resultSets.size == 10 && loaded.objects.profile == MySqlSchemaAuditProfile.Format3,
      "actual static loader must retain the independent exact-ten Format3 pack identity")
    loaded

  private val SyntheticServer = java.util.UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d")
  private val SyntheticProvisioning = java.util.UUID.fromString("16fb58b8-a867-43b7-8e8e-03554578bf14")
  private val SyntheticBinding = java.util.UUID.fromString("e83a7eb6-c7fd-4094-9751-029bdef5c0cf")
  private val SyntheticSchema = "fp_ordinary_synthetic"
  private val SyntheticUser = "fp_runtime@localhost"
  private val SyntheticDefiner = "fp_audit@localhost"
  private val MaxReceiptAge = Duration.ofMinutes(10)
  private val WriterLease = Duration.ofMinutes(5)
  private val Profile = SourceLimitProfile.of(1_000_000L, 1_000_000L, 2_000_000L, 64L, 256L,
    32_000_000L, 4_000_000L, 2_000_000L)

  private def syntheticPlan: WindowsWorldProvisioner.Plan = new WindowsWorldProvisioner.Plan(
    UUID.fromString("d21a167d-7d47-4c43-a48f-a61d45de2320"),
    UUID.fromString("e7f436ea-0775-4f6c-8139-9166b164cfec"),
    UUID.fromString("02691cb7-c802-4210-8bd5-27ccf7a91f7e"),
    UUID.fromString("9752345c-3082-40b5-b29c-e2bc9ef36fb5"),
    SyntheticBinding, 2, SyntheticServer, SyntheticSchema, 3,
    HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("SHA-256").digest(syntheticDdlBytes())),
    SyntheticProvisioning)

  private def sha(bytes: Array[Byte]): String =
    HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))

  private def hex(bytes: Array[Byte]): String = HexFormat.of().withUpperCase().formatHex(bytes)

  private def privilegeSet(id: String, labels: Vector[String], rows: Vector[Vector[String]]): JsonObject =
    val result = new JsonObject
    result.addProperty("id", id)
    val columns = new JsonArray
    labels.foreach { label =>
      val value = new JsonObject
      value.addProperty("label", label)
      value.addProperty("jdbcType", Types.VARCHAR)
      value.addProperty("typeName", "VARCHAR")
      value.addProperty("precision", 256)
      value.addProperty("scale", 0)
      value.addProperty("nullable", 1)
      value.addProperty("signed", false)
      columns.add(value)
    }
    result.add("columns", columns)
    val encodedRows = new JsonArray
    rows.foreach { values =>
      val row = new JsonArray
      values.foreach { cell =>
        val value = new JsonObject
        value.addProperty("tag", "TEXT")
        value.addProperty("value", cell)
        row.add(value)
      }
      encodedRows.add(row)
    }
    result.add("rows", encodedRows)
    result

  private def format3Contract(baseline: MySqlApprovedStaticBaseline): Array[Byte] =
    val plan = syntheticPlan
    val provenance = MySqlFormat3FreshPlanProvenance.canonicalBytes(plan)
    val root = new JsonObject
    root.addProperty("format", MySqlFormat3RuntimeContractLoader.Format)
    root.addProperty("schemaFormat", 3)
    root.addProperty("coverage", MySqlAuditCoverage.Format3)
    root.addProperty("approvedBaselineSha256", baseline.fileSha256)
    root.addProperty("serverUuid", SyntheticServer.toString)
    root.addProperty("serverVersion", "9.7.2")
    root.addProperty("schema", SyntheticSchema)
    root.addProperty("effectiveUser", SyntheticUser)
    root.addProperty("connectedUser", SyntheticUser)
    root.addProperty("callerRoles", "NONE")
    root.addProperty("provisioningUuid", SyntheticProvisioning.toString)
    root.addProperty("bindingUuid", SyntheticBinding.toString)
    root.addProperty("ddlSha256", baseline.identity.ddlSha256)
    root.addProperty("auditDefiner", SyntheticDefiner)
    root.addProperty("sourceCaptureSha256", baseline.identity.sourceCaptureSha256)
    root.addProperty("auditDefinerRoles", "NONE")
    val grants = new JsonArray
    grants.add("GRANT USAGE ON *.* TO 'fp_runtime'@'localhost'")
    root.add("callerGrants", grants)
    root.addProperty("physicalFormat", 2)
    root.addProperty("importState", "NONE")
    root.addProperty("maxReceiptAgeNanos", MaxReceiptAge.toNanos)
    root.addProperty("sourceProvenanceKind", "FRESH_PLAN")
    root.addProperty("sourceProvenanceHex", hex(provenance))
    root.addProperty("sourceProvenanceSha256", sha(provenance))
    root.addProperty("limitProfileEncoding", SourceLimitProfile.ENCODING)
    root.addProperty("limitProfileHex", hex(Profile.canonicalBytes()))
    root.addProperty("limitProfileSha256", Profile.sha256Hex().toUpperCase(Locale.ROOT))
    val privileges = new JsonArray
    privileges.add(privilegeSet("RS15_TABLE_PRIVILEGES",
      Vector("GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE"),
      Vector(Vector("'fp_audit'@'localhost'", "def", SyntheticSchema, "fp_schema_manifest", "SELECT", "NO"))))
    privileges.add(privilegeSet("RS16_COLUMN_PRIVILEGES",
      Vector("GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE"), Vector.empty))
    privileges.add(privilegeSet("RS17_SCHEMA_PRIVILEGES",
      Vector("GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "PRIVILEGE_TYPE", "IS_GRANTABLE"),
      Vector(Vector("'fp_audit'@'localhost'", "def", SyntheticSchema, "SELECT", "NO"),
        Vector("'fp_audit'@'localhost'", "def", SyntheticSchema, "TRIGGER", "NO"))))
    privileges.add(privilegeSet("RS18_USER_PRIVILEGES",
      Vector("GRANTEE", "TABLE_CATALOG", "PRIVILEGE_TYPE", "IS_GRANTABLE"),
      Vector(Vector("'fp_audit'@'localhost'", "def", "USAGE", "NO"))))
    root.add("definerPrivileges", privileges)
    new GsonBuilder().disableHtmlEscaping().create().toJson(root).getBytes(StandardCharsets.UTF_8)

  /** Writes only explicit synthetic test inputs; Fresh source/approval facts are produced later by the actual actor. */
  def prepareOrdinaryProcessFixture(root: Path): Unit =
    require(root != null && root.isAbsolute && root.getParent != null, "absolute isolated process fixture root required")
    Files.createDirectories(root.resolve("control"))
    val control = root.resolve("control")
    val baseline = loadIndependentStaticBaseline(control.resolve("baseline"))
    val baselinePath = control.resolve("baseline/approved-static-baseline-format3-ordinary-synthetic.json")
    val contractPath = control.resolve("format3-runtime-contract.json")
    val contract = format3Contract(baseline)
    Files.write(contractPath, contract)
    val metadataPath = control.resolve("runtime.properties")
    val prefix = control.toAbsolutePath.normalize().toString.replace('\\', '/')
    val metadata = Vector(
      "backend=mysql", "host=127.0.0.1", "port=33317", s"schema=$SyntheticSchema",
      s"expectedServerUuid=$SyntheticServer", s"expectedBindingUuid=$SyntheticBinding",
      s"expectedEffectiveUser=$SyntheticUser", "username=fp_runtime",
      s"pkcs12TrustStorePath=$prefix/trust.p12", s"credentialSourceRef=file:$prefix/credential.test-only",
      s"trustSecretSourceRef=file:$prefix/trust-secret.test-only", "connectTimeoutMillis=3000", "socketTimeoutMillis=10000",
      "activeLimit=32", "innodbFlushLogAtTrxCommit=1", "syncBinlog=1", s"approvedBaselinePath=${baselinePath.toString.replace('\\', '/')}",
      s"approvedBaselineSha256=${baseline.fileSha256}", s"approvedBaselineFormat=${MySqlApprovedBaselineLoader.ApprovedFormat}",
      "approvedSchemaFormat=3", s"approvedCoverage=${MySqlAuditCoverage.Format3}", "expectedServerVersion=9.7.2",
      s"expectedProvisioningUuid=$SyntheticProvisioning", s"approvedDdlSha256=${baseline.identity.ddlSha256}",
      s"expectedAuditDefiner=$SyntheticDefiner", s"approvedSourceCaptureSha256=${baseline.identity.sourceCaptureSha256}",
      s"format3RuntimeContractPath=${contractPath.toString.replace('\\', '/')}", s"format3RuntimeContractSha256=${sha(contract)}", ""
    ).mkString("\n")
    Files.writeString(metadataPath, metadata, StandardCharsets.UTF_8)
    val loaded = MySqlRuntimeMetadata.load(metadataPath)
    WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(loaded) // reject malformed independent config before spawning native children.
    Files.writeString(control.resolve("control.properties"), "synthetic-control=test-only\n", StandardCharsets.UTF_8)
    ScriptedDatabase.initialize(control.resolve("sql-state.json"))
    verifyIndependentFull20Contract(root, loaded)

  private def metadataAt(root: Path): MySqlRuntimeMetadata =
    MySqlRuntimeMetadata.load(root.resolve("control/runtime.properties"))

  private def policy: MySqlRuntimeAuditPolicy = MySqlRuntimeAuditPolicy("NONE", "NONE",
    MySqlExpectedNoImport, MySqlExpectedPhysicalFormat.Format2, MaxReceiptAge)

  /** Pure JDBC assembly: synthetic committed Certified DATA, actual verifier and runtime session issuer. */
  final class ControlledClose:
    private var armed = false
    private var closeFailure = Option.empty[Throwable]
    private var getterFailure = Option.empty[Throwable]
    private var getterValue = Option.empty[Boolean]
    private var closes = 0
    private var getters = 0
    def arm(close: Option[Throwable], value: Option[Boolean], getter: Option[Throwable]): Unit = synchronized {
      require(!armed && closes == 0, "close faults must be armed once before close")
      closeFailure = close; getterValue = value; getterFailure = getter; armed = true
    }
    def closeAttempt(): Unit = synchronized {
      closes += 1
      if armed then closeFailure.foreach(throw _)
    }
    def isClosed(actual: Boolean): Boolean = synchronized {
      // Verification/readmission getters must stay healthy until actual close was attempted.
      if closes == 0 then actual
      else
        getters += 1
        if armed then getterFailure.foreach(throw _)
        if armed then getterValue.getOrElse(actual) else actual
    }
    def closeCalls: Int = synchronized { closes }
    def getterCallsAfterClose: Int = synchronized { getters }

  def controlledRunnerSource(root: Path, boot: UUID,
      closeControl: ControlledClose = new ControlledClose): (MySqlRuntimeInputs, () => VerifiedMySqlConnection) =
    val metadata = metadataAt(root)
    val database = new ScriptedDatabase(root.resolve("control/sql-state.json"), metadata, syntheticPlan, closeControl)
    database.seedControlledCertifiedData()
    val inputs = MySqlRuntimeInputs.assemble(metadata, Thread.currentThread(), boot, 1L, policy, Some(syntheticPlan))
    (inputs, () => new MySqlConnectionSource(inputs.settings).verifyAndTransfer(database.connection()))

  /** Actual owned worker actor runner. It never opens JDBC in this caller or forges a Verified wrapper. */
  def runOrdinaryProcessChild(mode: String, root: Path): Unit =
    require(Set("fresh", "ordinary-v4", "ordinary-v5").contains(mode) && root != null && root.isAbsolute,
      "known actor child mode and absolute root required")
    val metadata = metadataAt(root)
    mode match
      case "fresh" => runFreshActor(root, metadata)
      case "ordinary-v4" => runOrdinaryActor(root, metadata, currentGeneration = 2L, nativeLabel = "V4")
      case "ordinary-v5" => runOrdinaryActor(root, metadata, currentGeneration = 3L, nativeLabel = "SETTLED_V5")

  private def runFreshActor(root: Path, metadata: MySqlRuntimeMetadata): Unit =
    val plan = syntheticPlan
    val boot = UUID.randomUUID()
    val identity = MySqlOwnedIoIdentity(boot, 1L, 1L)
    val tag = IoOperationTag(boot, 1L, 1L, UUID.randomUUID())
    val request = MySqlFreshBootRequest(root.resolve("world"), plan, new WindowsWorldProvisioner.BootIdentity(boot, 1L),
      MySqlFreshControlledMetadata(root.resolve("control/runtime.properties"),
        WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(metadata).configurationSha256()))
    val database = new ScriptedDatabase(root.resolve("control/sql-state.json"), metadata, plan)
    val source = () =>
      val inputs = MySqlRuntimeInputs.assemble(metadata, Thread.currentThread(), boot, 1L, policy, Some(plan))
      new MySqlConnectionSource(inputs.settings).verifyAndTransfer(database.connection())
    val adapter = MySqlOwnedIoAdapter.withFreshConnectionSourceForAssembly(1, Thread.currentThread(),
      "ordinary-format3-fresh-actor-test", identity, source)
    require(adapter.submit(tag, MySqlOwnedIoCommand.BootFresh(metadata, policy, WriterLease, request)) ==
      com.wi.floatingpod.server.io.IoSubmission.Accepted, "actual Fresh command was not accepted")
    var workerStopped = false
    try
      val facts = awaitActor(adapter, tag) match
        case MySqlOwnedIoReply.FreshBooted(_, _, facts, _) => facts
        case other => throw new IllegalStateException("actual Fresh actor returned a foreign reply: " + other.getClass.getName)
      val close = stopActor(adapter)
      workerStopped = true
      val native = close.freshResponsibility.getOrElse(
        throw new IllegalStateException("Fresh actor close observation omitted actual Fresh responsibility")).native
      require(native.sourceCapture.nonEmpty && native.sourceAnchor.nonEmpty && native.creation.nonEmpty && native.marker.nonEmpty,
        "Fresh source approval requires actual source capture, anchor, creation and marker facts")
      require(close.connectionOpened && close.jdbcClosed.contains(true) && close.failure.isEmpty &&
        close.releaseReturned && native.retainedResponsibility,
        "Fresh actor must close JDBC and retain the actual process-lifetime native source custody before pinning")
      val sqlObservation = database.observation()
      require(sqlObservation.commitCount > 0 && sqlObservation.full20LockedReadCount >= 20 &&
        sqlObservation.sessionLockAcquireCount > 0 && sqlObservation.sessionLockOwnerCheckCount > 0,
        "Fresh actor must commit SQL state, lock-read all twenty typed relations, and verify same-session native custody")
      val approvalPath = root.resolve("control/ordinary-source.properties")
      val approvalBytes = approvalProperties(facts, close, metadata)
      Files.write(approvalPath, approvalBytes)
      val metadataBytes = Files.readAllBytes(root.resolve("control/runtime.properties"))
      val metadataSha = sha(metadataBytes)
      val contractSha = metadata.format3RuntimeContract().orElseThrow().fileSha256()
      val configSha = native.sourceCapture.get.configurationSha256
      val approved = ProductionOrdinarySourceApproval.loadPinned(approvalPath, sha(approvalBytes),
        root.resolve("control/control.properties"), root.resolve("world"), metadata, metadataSha, configSha, contractSha)
      require(approved.plan == facts.creation.request.plan && approved.anchorSha256 == native.sourceAnchor.get.anchorSha256,
        "actual loadPinned result differs from the retained Fresh facts")
      val result = Vector("actor=FRESH_BOOTED", "jdbcClosed=true", "freshNativeSourceAnchor=true", "approvalLoaded=true",
        s"producerBoot=${facts.creation.request.boot.bootId()}", s"approvalSha256=${sha(approvalBytes)}",
        s"sqlCommits=${sqlObservation.commitCount}", s"full20LockedReads=${sqlObservation.full20LockedReadCount}",
        s"sessionLockOwnerChecks=${sqlObservation.sessionLockOwnerCheckCount}",
        s"connectionId=${sqlObservation.connectionId}", "syntheticJdbc=true", "")
      Files.writeString(root.resolve("control/fresh-result.properties"), result.mkString("\n"), StandardCharsets.UTF_8)
    catch
      case cause: Throwable =>
        if !workerStopped then
          try stopActor(adapter)
          catch case cleanup: Throwable => if cleanup ne cause then cause.addSuppressed(cleanup)
        throw cause

  private def runOrdinaryActor(root: Path, metadata: MySqlRuntimeMetadata, currentGeneration: Long, nativeLabel: String): Unit =
    require(currentGeneration > 1L && Set("V4", "SETTLED_V5").contains(nativeLabel),
      "ordinary actor requires its explicit current generation and native source stage")
    require(Files.isRegularFile(root.resolve("control/fresh-exited")),
      "ordinary V4 producer cannot consume Fresh native source until the Fresh producer process exit was observed by its parent")
    val approvalPath = root.resolve("control/ordinary-source.properties")
    val approvalBytes = Files.readAllBytes(approvalPath)
    val configSha = WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(metadata).configurationSha256()
    val runtimeSha = metadata.format3RuntimeContract().orElseThrow().fileSha256().toUpperCase(Locale.ROOT)
    val approved = ProductionOrdinarySourceApproval.loadPinned(approvalPath, sha(approvalBytes),
      root.resolve("control/control.properties"), root.resolve("world"), metadata,
      sha(Files.readAllBytes(root.resolve("control/runtime.properties"))), configSha, runtimeSha)
    if nativeLabel == "SETTLED_V5" then
      require(Files.isRegularFile(root.resolve("control/ordinary-v4-exited")),
        "CurrentV5 source cannot be consumed until the actual V4 producer process exit was observed by its parent")
    val bootId = UUID.randomUUID()
    val submit = UUID.randomUUID()
    val attempt = UUID.randomUUID()
    val handoff = UUID.randomUUID()
    val identity = MySqlOwnedIoIdentity(bootId, currentGeneration, currentGeneration)
    val controlIdentity = ProductionBootstrapControl.OrdinaryIdentity(bootId, currentGeneration, currentGeneration,
      submit, attempt, handoff)
    val boot = new WindowsWorldProvisioner.BootIdentity(bootId, currentGeneration)
    val request = MySqlOrdinaryCertifiedBootRequest(root.resolve("world"), approved, boot, controlIdentity,
      MySqlFreshControlledMetadata(root.resolve("control/runtime.properties"), configSha), runtimeSha)
    val database = new ScriptedDatabase(root.resolve("control/sql-state.json"), metadata, approved.plan())
    val before = database.observation()
    val connectionSource = () =>
      val inputs = MySqlRuntimeInputs.assembleCertifiedOrdinary(metadata, Thread.currentThread(), bootId,
        currentGeneration, policy, approved)
      new MySqlConnectionSource(inputs.settings).verifyAndTransfer(database.connection())
    val adapter = MySqlOwnedIoAdapter.withCertifiedOrdinaryConnectionSourceForAssembly(1, Thread.currentThread(),
      s"ordinary-format3-$nativeLabel-actor-test", identity, connectionSource)
    val tag = IoOperationTag(bootId, currentGeneration, currentGeneration, submit)
    require(adapter.submit(tag, MySqlOwnedIoCommand.BootOrdinaryCertified(metadata, policy, WriterLease, request)) ==
      com.wi.floatingpod.server.io.IoSubmission.Accepted, "actual ordinary Certified command was not accepted")
    var workerStopped = false
    try
      val facts = awaitActor(adapter, tag) match
        case result: MySqlOwnedIoReply.OrdinaryCertifiedBooted =>
          val snapshot = result.owned.fullTwentyRelationSnapshot
          val ordinary = result.owned.ordinaryFacts
          require(snapshot.relations.size == 20 && snapshot.rowCount > 0L,
            "actual ordinary owned actor did not publish a nonempty complete typed Full20 inventory")
          ordinary
        case other => throw new IllegalStateException("actual ordinary actor returned a foreign reply: " + other.getClass.getName)
      // 期望来自独立原loadPinned批准输入，不由被测getter结果反推；真实native/SQL fixture本片不运行。
      require(facts.sourceWorldIdentity == approved.sourceWorldIdentity(),
        "ordinary installed source identity differs from original approval")
      require(facts.guardFileIdentity == approved.guardFileIdentity(),
        "ordinary installed guard identity differs from original approval")
      require(facts.receiptFileIdentity == approved.receiptFileIdentity(),
        "ordinary installed receipt identity differs from original approval")
      require(facts.anchorFileIdentity == approved.anchorFileIdentity(),
        "ordinary installed anchor identity differs from original approval")
      require(facts.actualReceiptSha256 == approved.canonicalReceiptSha256(),
        "ordinary installed receipt SHA differs from original approval")
      require(facts.actualAnchorSha256 == approved.anchorSha256(),
        "ordinary installed anchor SHA differs from original approval")
      val close = stopActor(adapter)
      workerStopped = true
      require(close.connectionOpened && close.jdbcClosed.contains(true) && close.failure.isEmpty && close.releaseReturned,
        "ordinary actor must close its actual JDBC session without discarding native custody")
      require(facts.originalCreation eq approved, "ordinary facts must retain the exact loaded pinned source approval")
      val sequences = facts.recordFacts.map(_.sequence)
      require(sequences.nonEmpty && sequences == sequences.sorted && sequences.distinct == sequences &&
        sequences.size == facts.recordFacts.size && sequences.forall(_ > 0L),
        "ordinary actor must expose its actual monotonic native record history")
      if nativeLabel == "SETTLED_V5" then
        require(facts.recordFacts.size >= 3,
          "distinct CurrentV5 consumer must retain source, handoff and settled native V5 records")
      val state = database.observation()
      require(state.writerEpoch > before.writerEpoch && state.writerUpdateCount > before.writerUpdateCount,
        "ordinary actor must commit an actual scripted JDBC writer fence/CAS after native handoff")
      require(state.commitCount > before.commitCount && state.full20LockedReadCount >= before.full20LockedReadCount + 20 &&
        state.sessionLockAcquireCount > before.sessionLockAcquireCount &&
        state.sessionLockOwnerCheckCount > before.sessionLockOwnerCheckCount,
        "ordinary actor must commit, lock-read all twenty typed relations, and verify same-session native custody")
      val sourceExited = if nativeLabel == "SETTLED_V5" then "true" else "not-applicable"
      val result = Vector(
        s"actor=$nativeLabel-ORDINARY-CERTIFIED-BOOTED",
        s"nativeSource=$nativeLabel",
        "nativeAcknowledgement=DB_ACQUIRED",
        s"recordCount=${facts.recordFacts.size}",
        s"recordSequences=${sequences.mkString(",")}",
        s"writerEpoch=${state.writerEpoch}",
        "writerCasCommitted=true",
        s"sqlCommits=${state.commitCount - before.commitCount}",
        s"full20LockedReads=${state.full20LockedReadCount - before.full20LockedReadCount}",
        s"sessionLockOwnerChecks=${state.sessionLockOwnerCheckCount - before.sessionLockOwnerCheckCount}",
        s"connectionId=${state.connectionId}",
        s"sourceProcessExited=$sourceExited",
        "full20Witness=true",
        s"jdbcClosed=${close.jdbcClosed.contains(true)}",
        "syntheticJdbc=true",
        "")
      Files.writeString(root.resolve(s"control/ordinary-${if nativeLabel == "V4" then "v4" else "v5"}-result.properties"),
        result.mkString("\n"), StandardCharsets.UTF_8)
    catch
      case cause: Throwable =>
        if !workerStopped then
          try stopActor(adapter)
          catch case cleanup: Throwable => if cleanup ne cause then cause.addSuppressed(cleanup)
        throw cause

  private def approvalProperties(facts: MySqlFreshBootFacts, close: MySqlOwnedSessionCloseObservation,
      metadata: MySqlRuntimeMetadata): Array[Byte] =
    val creation = facts.creation
    val native = close.freshResponsibility.getOrElse(throw new IllegalStateException("Fresh native close facts are missing")).native
    val capture = native.sourceCapture.getOrElse(throw new IllegalStateException("actual Fresh source capture facts are missing"))
    val anchor = native.sourceAnchor.getOrElse(throw new IllegalStateException("actual Fresh source anchor facts are missing"))
    val plan = creation.request.plan
    val metadataSha = sha(Files.readAllBytes(creation.request.controlledMetadata.metadataPath))
    val runtimeSha = metadata.format3RuntimeContract().orElseThrow().fileSha256()
    val values = Vector(
      "approvalFormat" -> "FP_ORDINARY_SOURCE_APPROVAL_V1",
      "creationUuid" -> plan.creationOperationUuid().toString,
      "bootstrapUuid" -> plan.bootstrapOperationUuid().toString,
      "prepareUuid" -> plan.prepareWriteUuid().toString,
      "certifyUuid" -> plan.certifyWriteUuid().toString,
      "creationBootUuid" -> creation.request.boot.bootId().toString,
      "creationBootGeneration" -> creation.request.boot.generation().toString,
      "bindingUuid" -> plan.bindingUuid().toString,
      "physicalFormat" -> plan.physicalFormat().toString,
      "serverUuid" -> plan.mysqlServerUuid().toString,
      "schema" -> plan.schema(),
      "schemaFormat" -> plan.schemaFormat().toString,
      "ddlSha256" -> plan.ddlSha256().toUpperCase(Locale.ROOT),
      "provisioningUuid" -> plan.provisioningUuid().toString,
      "canonicalReceiptLength" -> creation.receiptCanonicalBytes.length.toString,
      "canonicalReceiptSha256" -> creation.receiptSha256.toUpperCase(Locale.ROOT),
      "sourceWorldIdentity" -> creation.sourceWorldIdentity,
      "guardFileIdentity" -> creation.guardFileIdentity,
      "receiptFileIdentity" -> creation.receiptFileIdentity,
      "anchorFileIdentity" -> anchor.fileIdentity,
      "worldPath" -> creation.request.requestedWorldPath.toAbsolutePath.normalize().toString.replace('\\', '/'),
      "anchorPath" -> capture.canonicalAnchorPath.toAbsolutePath.normalize().toString.replace('\\', '/'),
      "anchorSha256" -> anchor.anchorSha256.toUpperCase(Locale.ROOT),
      "configurationSha256" -> capture.configurationSha256.toUpperCase(Locale.ROOT),
      "metadataSha256" -> metadataSha,
      "baselineSha256" -> facts.approvedBaselineFileSha256.toUpperCase(Locale.ROOT),
      "runtimeContractSha256" -> runtimeSha.toUpperCase(Locale.ROOT)
    )
    values.map { case (key, value) => s"$key=$value" }.mkString("\n", "\n", "\n").getBytes(StandardCharsets.UTF_8)

  private def awaitActor(adapter: MySqlOwnedIoAdapter, tag: IoOperationTag): MySqlOwnedIoReply =
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(55)
    var result = Option.empty[com.wi.floatingpod.server.io.IoCompletion[MySqlOwnedIoReply]]
    while result.isEmpty && System.nanoTime() < deadline do
      val completed = adapter.drain(1)
      completed.foreach { value =>
        require(value.tag == tag, "actual actor completion tag differs from its accepted command")
        result = Some(value)
      }
      if result.isEmpty then Thread.sleep(5L)
    val completion = result.getOrElse(throw new java.util.concurrent.TimeoutException("owned actor completion exceeded 55 seconds"))
    completion.result match
      case scala.util.Success(value) => value
      case scala.util.Failure(cause) => throw cause

  private def stopActor(adapter: MySqlOwnedIoAdapter): MySqlOwnedSessionCloseObservation =
    adapter.quiesce()
    val failure = new AtomicReference[Throwable]()
    val stopped = new AtomicReference[java.lang.Boolean]()
    val done = new CountDownLatch(1)
    val waiter = new Thread(() =>
      try stopped.set(adapter.awaitExitOffMain(12L, TimeUnit.SECONDS))
      catch case cause: Throwable => failure.set(cause)
      finally done.countDown(), "ordinary-test-worker-exit-observer")
    waiter.start()
    if !done.await(13L, TimeUnit.SECONDS) then throw new java.util.concurrent.TimeoutException("owned worker exit observation timed out")
    waiter.join(1000L)
    require(!waiter.isAlive, "owned worker exit observer did not terminate")
    Option(failure.get()).foreach(throw _)
    require(java.lang.Boolean.TRUE == stopped.get(), "actual owned IO worker did not terminate")
    adapter.checkpoint.resources match
      case Some(MySqlOwnedIoResourceObservation.SessionObserved(value)) => value
      case Some(MySqlOwnedIoResourceObservation.PartialInitializationObserved(value)) => value
      case other => throw new IllegalStateException("actual owned worker exit omitted its registered JDBC/native close observation: " + other)

  /** Observable result of the test-only SQL transaction state; this is never a production admission token. */
  final case class DatabaseObservation(writerEpoch: Long, writerUpdateCount: Int, commitCount: Int,
      bindingState: Option[String], intentStage: Option[String], full20LockedReadCount: Int,
      sessionLockAcquireCount: Int, sessionLockOwnerCheckCount: Int, connectionId: Long)

  private final case class IntentState(bootstrap: String, prepare: String, certify: String, binding: String,
      provisioning: String, sourceIdentity: String, receiptSha256: String, stage: String, acknowledgementSha256: Option[String])
  private final case class DatabaseState(writerEpoch: Long, writerOwner: Option[String], leaseUntil: LocalDateTime,
      lastOperation: Option[String], lastFingerprint: Option[String], bindingState: Option[String],
      intent: Option[IntentState], writerUpdateCount: Int, commitCount: Int,
      controlledRows: Map[String, Vector[Vector[Any]]] = Map.empty)
  private final case class JdbcColumn(label: String, jdbcType: Int, typeName: String, precision: Int = 4096,
      scale: Int = 0, nullable: Int = ResultSetMetaData.columnNullable, signed: Boolean = false)
  private final case class JdbcData(columns: Vector[JdbcColumn], rows: Vector[Vector[Any]])
  private final case class OrdinaryGoldenColumn(name: String, declaration: String, kind: MySqlFormat3SqlKind,
      jdbcType: Int, nullable: Boolean, maximumLength: Option[Int], primary: Boolean):
    def jdbcMetadata: JdbcColumn = JdbcColumn(name, jdbcType, declaration.takeWhile(ch => ch != ' ' && ch != '('),
      maximumLength.getOrElse(0), if kind == MySqlFormat3SqlKind.DateTime then 6 else 0,
      if nullable then ResultSetMetaData.columnNullable else ResultSetMetaData.columnNoNulls,
      kind == MySqlFormat3SqlKind.Integer || kind == MySqlFormat3SqlKind.Floating)
  private final case class OrdinaryGoldenRelation(table: String, columns: Vector[OrdinaryGoldenColumn], primaryKey: Vector[String])
  private val Full20ProjectionPattern = "(?is)^\\s*SELECT\\s+(.+?)\\s+FROM\\s+`[^`]+`\\.`([a-z][a-z0-9_]*)`\\s+ORDER BY\\s+(.+?)\\s+(FOR SHARE|FOR UPDATE)\\s*$".r

  private def fixedIdentifierList(sql: String): Vector[String] =
    sql.split(",", -1).toVector.map { raw =>
      val value = raw.trim
      if value.length < 3 || value.head != '`' || value.last != '`' then
        throw new java.sql.SQLException("scripted MySQL Full20 query contains a non-fixed identifier: " + raw)
      value.substring(1, value.length - 1)
    }

  /** The independently hashed JSON contract is the scripted server's only Full20 metadata oracle. */
  private lazy val OrdinaryGoldenFull20: Vector[OrdinaryGoldenRelation] =
    val definitions = relationDefinitions()
    Full20RelationNames.map { table =>
      val definition = Option(definitions.getAsJsonObject(table)).getOrElse(
        throw new IllegalStateException("independent synthetic relation definition is missing: " + table))
      val primary = definition.getAsJsonArray("pk").iterator().asScala.map(_.getAsString).toVector
      val columns = definition.getAsJsonArray("cols").iterator().asScala.map { raw =>
        val pair = raw.getAsJsonArray
        if pair.size() != 2 then throw new IllegalStateException("independent Full20 column definition must contain name and declaration")
        val name = pair.get(0).getAsString
        val declaration = pair.get(1).getAsString
        val base = declaration.takeWhile(ch => ch != ' ' && ch != '(').toUpperCase(Locale.ROOT)
        val kind = base match
          case "VARCHAR" | "LONGTEXT" => MySqlFormat3SqlKind.Text
          case "VARBINARY" | "LONGBLOB" => MySqlFormat3SqlKind.Binary
          case "TINYINT" | "INT" | "BIGINT" => MySqlFormat3SqlKind.Integer
          case "DOUBLE" => MySqlFormat3SqlKind.Floating
          case "DATETIME" => MySqlFormat3SqlKind.DateTime
          case other => throw new IllegalStateException("independent Full20 fixture has an unsupported SQL type: " + other)
        val jdbcType = (base, kind) match
          case ("LONGTEXT", _) => Types.LONGVARCHAR
          case ("LONGBLOB", _) => Types.LONGVARBINARY
          case ("VARCHAR", _) => Types.VARCHAR
          case ("VARBINARY", _) => Types.VARBINARY
          case ("TINYINT", _) => Types.TINYINT
          case ("INT", _) => Types.INTEGER
          case ("BIGINT", _) => Types.BIGINT
          case ("DOUBLE", _) => Types.DOUBLE
          case ("DATETIME", _) => Types.TIMESTAMP
          case _ => throw new IllegalStateException("independent Full20 fixture has no JDBC type mapping")
        val declaredWidth = lengthFrom(declaration).map(_.toInt)
        val width = if Set("VARCHAR", "VARBINARY", "DATETIME").contains(base) then declaredWidth else None
        val nullable = !declaration.toUpperCase(Locale.ROOT).endsWith(" NOT NULL")
        OrdinaryGoldenColumn(name, declaration, kind, jdbcType, nullable, width, primary.contains(name))
      }.toVector
      OrdinaryGoldenRelation(table, columns, primary)
    }

  /** Compare the independent golden contract with production and exercise the reader's real metadata gate. */
  private def verifyIndependentFull20Contract(root: Path, metadata: MySqlRuntimeMetadata): Unit =
    val golden = OrdinaryGoldenFull20
    val production = MySqlFormat3RelationShapes.canonicalOrder
    val lockedTables = Vector("fp_schema_manifest", "physical_recovery_binding", "fp_runtime_writer",
      "fp_recovery_reservation", "fp_recovery_operation") ++ Full20RelationNames.drop(4).filterNot(_ == "fp_recovery_reservation")
    require(MySqlFormat3RelationShapes.lockedReadOrder.map(_.table) == lockedTables,
      "production Full20 locking must retain independent M/B/W/reservation/operation order")
    require(golden.size == 20 && golden.map(_.columns.size).sum == 368,
      "pinned independent synthetic Full20 golden must define exactly twenty relations and 368 columns")
    require(golden.map(_.table) == Full20RelationNames && production.map(_.table) == golden.map(_.table),
      "production Full20 canonical traversal must match the independent frozen relation order")
    val database = new ScriptedDatabase(root.resolve("control/sql-state.json"), metadata, syntheticPlan)
    golden.zip(production).foreach { case (expected, actual) =>
      require(actual.primaryKey == expected.primaryKey && actual.columns.size == expected.columns.size,
        "production relation key or column count differs from independent golden: " + expected.table)
      expected.columns.zip(actual.columns).foreach { case (goldenColumn, productionColumn) =>
        require(productionColumn.name == goldenColumn.name && productionColumn.sqlDeclaration == goldenColumn.declaration &&
          productionColumn.kind == goldenColumn.kind && productionColumn.jdbcType == goldenColumn.jdbcType &&
          productionColumn.nullable == goldenColumn.nullable && productionColumn.maximumLength == goldenColumn.maximumLength,
          "production relation column differs from independent golden: " + expected.table + "." + goldenColumn.name)
      }
      MySqlFormat3FullSnapshotSql.validateMetadata(database.full20MetadataForContractProbe(expected.table), actual)
    }
    val first = production.head
    val metadataFromGolden = database.full20MetadataForContractProbe(first.table)
    val firstColumn = first.columns.head
    val oneColumnDrift = first.copy(columns = first.columns.updated(0, firstColumn.copy(nullable = !firstColumn.nullable)))
    requireReaderMetadataRejection(metadataFromGolden, oneColumnDrift, "single-column nullability drift")
    val reordered = first.copy(columns = first.columns.updated(0, first.columns(1)).updated(1, first.columns(0)))
    requireReaderMetadataRejection(metadataFromGolden, reordered, "single adjacent-column order drift")

  private def requireReaderMetadataRejection(metadata: ResultSetMetaData, drifted: MySqlFormat3RelationShape,
      drift: String): Unit =
    try
      MySqlFormat3FullSnapshotSql.validateMetadata(metadata, drifted)
      throw new IllegalStateException("actual Full20 reader metadata gate accepted " + drift)
    catch
      case expected: java.sql.SQLException => ()

  /** 仅通过公开 JDBC 接口向真实 actor 提供的严格 MySQL 协议内存脚本；状态只服务于合成测试。 */
  private final class ScriptedDatabase(statePath: Path, metadata: MySqlRuntimeMetadata,
      freshPlan: WindowsWorldProvisioner.Plan, closeControl: ControlledClose = new ControlledClose):
    require(statePath != null && statePath.isAbsolute && metadata != null && freshPlan != null,
      "isolated scripted JDBC state, parsed runtime metadata, and original Fresh plan are required")
    private val baseline = loadBaseline(metadata)
    private val contractJson = JsonParser.parseString(new String(Files.readAllBytes(
      metadata.format3RuntimeContract().orElseThrow().absolutePath()), StandardCharsets.UTF_8)).getAsJsonObject
    private val anchorConfiguration = WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(metadata)
    private val configShaBytes = HexFormat.of().parseHex(anchorConfiguration.configurationSha256())
    private val connectionIdPath = statePath.resolveSibling("connection-id-sequence.txt")
    private val lastConnectionId = new java.util.concurrent.atomic.AtomicLong()
    private val lockOwners = new java.util.concurrent.ConcurrentHashMap[String, java.lang.Long]()
    private val full20LockedReadCount = new java.util.concurrent.atomic.AtomicInteger()
    private val sessionLockAcquireCount = new java.util.concurrent.atomic.AtomicInteger()
    private val sessionLockOwnerCheckCount = new java.util.concurrent.atomic.AtomicInteger()
    // Test SQL rows are independent of materializer images and become visible only on JDBC commit.
    private var committedControlledRows = Map.empty[String, Vector[Vector[Any]]]
    def seedControlledCertifiedData(): Unit =
      val before = readState()
      require(before.commitCount == 0 && before.bindingState.isEmpty && before.writerOwner.isEmpty,
        "controlled synthetic initial DATA cannot replace an executed actor state")
      val history = IntentState(freshPlan.bootstrapOperationUuid().toString, freshPlan.prepareWriteUuid().toString,
        freshPlan.certifyWriteUuid().toString, freshPlan.bindingUuid().toString, freshPlan.provisioningUuid().toString,
        "controlled-jdbc-synthetic-world", "11" * 32, "COMPLETE", Some("22" * 32))
      writeState(before.copy(bindingState = Some("CERTIFIED"), intent = Some(history)))
    private val ControlledTables = Set("fp_recovery_operation", "fp_recovery_reservation", "floating_pod_state",
      "physical_recovery_footprint", "physical_recovery_footprint_chunk", "physical_recovery_intent", "physical_recovery_locator")

    private def sameSqlCell(left: Any, right: Any): Boolean = (left, right) match
      case (a: Array[Byte], b: Array[Byte]) => java.util.Arrays.equals(a, b)
      case (a: Number, b: Number) => BigDecimal(a.toString) == BigDecimal(b.toString)
      case _ => left == right

    private def applyControlledMutation(sql: String, parameters: Map[Int, Any], before: DatabaseState,
        table: String): (DatabaseState, Int) =
      val relation = OrdinaryGoldenFull20.find(_.table == table).get
      val names = relation.columns.map(_.name)
      val rows = before.controlledRows.getOrElse(table, Vector.empty)
      def index(name: String): Int =
        val found = names.indexOf(name)
        if found < 0 then throw new java.sql.SQLException("controlled SQL column outside frozen contract: " + name)
        found
      val insert = "(?s)^INSERT INTO `[^`]+`\\.`[^`]+` \\((.+)\\) VALUES \\((.+)\\)$".r
      val update = "(?s)^UPDATE `[^`]+`\\.`[^`]+` SET (.+) WHERE (.+)$".r
      sql match
        case insert(columns, placeholders) =>
          val actual = columns.split(",").toVector.map(_.stripPrefix("`").stripSuffix("`"))
          if actual != names || placeholders.split(",").toVector != Vector.fill(names.size)("?") || parameters.size != names.size then
            throw new java.sql.SQLException("controlled INSERT differs from frozen complete relation projection")
          val row = names.indices.map(i => safeParam(parameters, i + 1)).toVector
          val keys = relation.primaryKey.map(index)
          if rows.exists(old => keys.forall(i => sameSqlCell(old(i), row(i)))) then
            throw new java.sql.SQLException("controlled INSERT duplicate primary key")
          (before.copy(controlledRows = before.controlledRows.updated(table, rows :+ row)), 1)
        case update(assignments, predicates) =>
          val changes = assignments.split(",").toVector.map { term =>
            if !term.matches("`[a-z][a-z0-9_]*`=\\?") then throw new java.sql.SQLException("uncontracted controlled assignment")
            term.substring(1, term.indexOf('`', 1))
          }
          val guards = predicates.split(" AND ").toVector.map { term =>
            if !term.matches("`[a-z][a-z0-9_]*` <=> \\?") then throw new java.sql.SQLException("uncontracted controlled predicate")
            term.substring(1, term.indexOf('`', 1))
          }
          val expected = table match
            case "floating_pod_state" => Vector("state_version", "is_active", "last_mutation_operation_uuid")
            case "physical_recovery_footprint" => Vector("seal_state", "created_operation_uuid")
            case "physical_recovery_intent" => Vector("intent_revision", "phase", "last_mutation_operation_uuid")
            case "physical_recovery_locator" => Vector("locator_revision", "current_attempt_uuid", "current_footprint_uuid", "last_mutation_operation_uuid")
            case "fp_recovery_operation" => Vector("operation_state", "operation_fingerprint")
            case "fp_recovery_reservation" => Vector("reservation_revision", "reservation_state", "workflow_uuid", "operation_uuid", "last_mutation_operation_uuid")
            case _ => throw new java.sql.SQLException("controlled UPDATE outside fixed DAG")
          if guards != relation.primaryKey ++ expected || changes.isEmpty || changes.distinct != changes ||
              changes.exists(relation.primaryKey.contains) || parameters.size != changes.size + guards.size then
            throw new java.sql.SQLException("controlled UPDATE lost exact primary-key/CAS guard")
          val matches = rows.indices.filter(i => guards.indices.forall(j =>
            sameSqlCell(rows(i)(index(guards(j))), safeParam(parameters, changes.size + j + 1))))
          if matches.size != 1 then throw new java.sql.SQLException("controlled UPDATE did not match one exact SQL preimage")
          var after = rows(matches.head)
          changes.indices.foreach(i => after = after.updated(index(changes(i)), safeParam(parameters, i + 1)))
          (before.copy(controlledRows = before.controlledRows.updated(table, rows.updated(matches.head, after))), 1)
        case _ => throw new java.sql.SQLException("controlled SQL syntax outside fixed DAG")

    private def acquireSessionLock(name: String, id: Long): Int =
      val current = lockOwners.putIfAbsent(name, java.lang.Long.valueOf(id))
      if current == null || current.longValue() == id then 1 else 0

    private def sessionLockOwner(name: String): Option[Long] = Option(lockOwners.get(name)).map(_.longValue())

    private def releaseSessionLocks(id: Long): Unit =
      val entries = lockOwners.entrySet().iterator()
      while entries.hasNext do
        val entry = entries.next()
        if entry.getValue.longValue() == id then lockOwners.remove(entry.getKey, entry.getValue)

    def connection(): Connection =
      val id = allocateConnectionId()
      lastConnectionId.set(id)
      new ConnectionScope(id).connection

    private def allocateConnectionId(): Long = synchronized {
      if !Files.isRegularFile(connectionIdPath) then
        throw new java.sql.SQLException("scripted MySQL connection ID sequence was not initialized by the test fixture")
      val raw = Files.readString(connectionIdPath, StandardCharsets.UTF_8).trim
      val current = try raw.toLong
      catch case cause: NumberFormatException => throw new java.sql.SQLException("scripted MySQL connection ID sequence is malformed", cause)
      if current <= 0L then throw new java.sql.SQLException("scripted MySQL connection ID sequence is not positive")
      val next = Math.addExact(current, 1L)
      val temporary = connectionIdPath.resolveSibling(connectionIdPath.getFileName.toString + ".next")
      Files.writeString(temporary, next.toString, StandardCharsets.UTF_8)
      Files.move(temporary, connectionIdPath, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
        java.nio.file.StandardCopyOption.REPLACE_EXISTING)
      current
    }

    private def jdbcProxy[T](interface: Class[T])(
        dispatch: (Object, Method, Array[Object]) => Object
    ): T =
      val handler = new InvocationHandler:
        override def invoke(proxy: Object, method: Method, arguments: Array[Object]): Object =
          val args = if arguments == null then Array.empty[Object] else arguments
          method.getName match
            case "toString" => s"ScriptedMySqlJdbc(${interface.getSimpleName})"
            case "hashCode" => Integer.valueOf(System.identityHashCode(proxy))
            case "equals" => java.lang.Boolean.valueOf(args.nonEmpty && (proxy eq args(0)))
            case _ => dispatch(proxy, method, args)
      Proxy.newProxyInstance(interface.getClassLoader, Array[Class[?]](interface), handler).asInstanceOf[T]

    private def unsupported(method: Method): Nothing =
      throw new java.sql.SQLFeatureNotSupportedException(
        s"synthetic MySQL JDBC script does not implement ${method.getDeclaringClass.getSimpleName}.${method.getName}")

    private def objectMethod(method: Method, proxy: Object, args: Array[Object]): Option[Object] = method.getName match
      case "unwrap" =>
        val requested = args(0).asInstanceOf[Class[?]]
        if requested.isInstance(proxy) then Some(proxy)
        else throw new java.sql.SQLException("scripted JDBC proxy cannot unwrap to " + requested.getName)
      case "isWrapperFor" => Some(java.lang.Boolean.valueOf(args(0).asInstanceOf[Class[?]].isInstance(proxy)))
      case _ => None

    private final class ConnectionScope(connectionId: Long):
      private var closed = false
      var autoCommit = true
      var isolation = Connection.TRANSACTION_REPEATABLE_READ
      private var transaction = Option.empty[DatabaseState]
      val connection: Connection = jdbcProxy(classOf[Connection]) { (proxy, method, args) =>
        objectMethod(method, proxy, args).getOrElse {
          method.getName match
            case "createStatement" => ensureOpen(); statement(None).statementProxy
            case "prepareStatement" =>
              ensureOpen()
              if args.isEmpty || !args(0).isInstanceOf[String] then unsupported(method)
              prepared(args(0).asInstanceOf[String])
            case "getAutoCommit" => ensureOpen(); java.lang.Boolean.valueOf(autoCommit)
            case "setAutoCommit" =>
              ensureOpen()
              val next = args(0).asInstanceOf[java.lang.Boolean].booleanValue()
              if autoCommit != next then
                if next then commitTransaction()
                autoCommit = next
                if !next then transaction = Some(readState())
              null
            case "getTransactionIsolation" => ensureOpen(); Integer.valueOf(isolation)
            case "setTransactionIsolation" =>
              ensureOpen()
              val next = args(0).asInstanceOf[Number].intValue()
              if next != Connection.TRANSACTION_READ_COMMITTED && next != Connection.TRANSACTION_REPEATABLE_READ then
                throw new java.sql.SQLException("scripted MySQL supports only the actor's READ COMMITTED / REPEATABLE READ isolation levels")
              isolation = next
              null
            case "commit" =>
              ensureOpen()
              if args.nonEmpty then unsupported(method)
              if autoCommit then throw new java.sql.SQLException("scripted JDBC commit requires an active explicit transaction")
              commitTransaction()
              null
            case "rollback" =>
              ensureOpen()
              if args.nonEmpty then unsupported(method)
              if autoCommit then throw new java.sql.SQLException("scripted JDBC rollback requires an active explicit transaction")
              transaction = None
              null
            case "close" =>
              if !closed then
                transaction = None // JDBC close 会回滚仍未提交的测试事务。
                releaseSessionLocks(connectionId)
                closed = true
              closeControl.closeAttempt()
              null
            case "isClosed" => java.lang.Boolean.valueOf(closeControl.isClosed(closed))
            case "isValid" => java.lang.Boolean.valueOf(!closed)
            case "getCatalog" => ensureOpen(); metadata.schema()
            case "setCatalog" => ensureOpen(); if args(0) != metadata.schema() then throw new java.sql.SQLException("scripted schema change rejected"); null
            case "getSchema" => ensureOpen(); metadata.schema()
            case "setSchema" => ensureOpen(); if args(0) != metadata.schema() then throw new java.sql.SQLException("scripted schema change rejected"); null
            case "setReadOnly" => ensureOpen(); if args(0).asInstanceOf[java.lang.Boolean].booleanValue() then unsupported(method); null
            case "isReadOnly" => ensureOpen(); java.lang.Boolean.FALSE
            case "getHoldability" => ensureOpen(); Integer.valueOf(ResultSet.CLOSE_CURSORS_AT_COMMIT)
            case "setHoldability" => ensureOpen(); if args(0).asInstanceOf[Number].intValue() != ResultSet.CLOSE_CURSORS_AT_COMMIT then unsupported(method); null
            case "getWarnings" => ensureOpen(); null
            case "clearWarnings" => ensureOpen(); null
            case "getNetworkTimeout" => ensureOpen(); Integer.valueOf(0)
            case "setNetworkTimeout" => ensureOpen(); if args(1).asInstanceOf[Number].intValue() != 0 then unsupported(method); null
            case "abort" => closeConnection(); null
            case "getMetaData" => unsupported(method)
            case other => unsupported(method)
        }
      }

      def ensureOpen(): Unit = if closed then throw new java.sql.SQLException("scripted MySQL connection is closed")
      private def closeConnection(): Unit =
        transaction = None
        releaseSessionLocks(connectionId)
        closed = true
      def currentState(): DatabaseState =
        ensureOpen()
        if autoCommit then readState()
        else
          if transaction.isEmpty then transaction = Some(readState())
          transaction.get
      private def commitTransaction(): Unit =
        transaction.foreach { current =>
          val committed = current.copy(commitCount = Math.addExact(current.commitCount, 1))
          writeState(committed)
        }
        transaction = None
      def applyMutation(sql: String, parameters: Map[Int, Any]): Int =
        ensureOpen()
        if autoCommit then throw new java.sql.SQLException("scripted actor SQL mutation requires an explicit owned transaction")
        val before = currentState()
        val (after, count) = executeUpdate(sql, parameters, Some(before))
        transaction = Some(after)
        count

      private def statement(preparedSql: Option[String]): StatementScope = new StatementScope(this, preparedSql)

      private def prepared(sql: String): PreparedStatement =
        val value = new StatementScope(this, Some(sql))
        value.preparedStatement

      private final class StatementScope(owner: ConnectionScope, preparedSql: Option[String]):
        private var closedStatement = false
        private var parameters = Map.empty[Int, Any]
        private var result = Option.empty[ResultSet]
        private var callSets = Vector.empty[JdbcData]
        private var callPosition = -1
        private var updateCount = -1
        val statementProxy: Statement = jdbcProxy(classOf[Statement]) { (proxy, method, args) =>
          dispatchStatement(proxy, method, args, preparedMode = false)
        }
        val preparedStatement: PreparedStatement = jdbcProxy(classOf[PreparedStatement]) { (proxy, method, args) =>
          dispatchStatement(proxy, method, args, preparedMode = true)
        }

        private def ensureStatementOpen(): Unit =
          owner.ensureOpen()
          if closedStatement then throw new java.sql.SQLException("scripted MySQL statement is closed")

        private def sqlFrom(args: Array[Object]): String = preparedSql.getOrElse {
          if args.isEmpty || !args(0).isInstanceOf[String] then throw new java.sql.SQLException("SQL text is required")
          args(0).asInstanceOf[String]
        }

        private def copyParameters(): Map[Int, Any] = parameters.map { case (index, value) =>
          index -> (value match
            case bytes: Array[Byte] => bytes.clone()
            case other => other)
        }

        private def currentStatement(preparedMode: Boolean): Statement = if preparedMode then preparedStatement else statementProxy

        private def verifyLockingRead(sql: String): Unit =
          val normalized = sql.trim.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT)
          if normalized.contains(" FOR UPDATE") || normalized.contains(" FOR SHARE") then
            val full20 = Full20ProjectionPattern.findFirstMatchIn(sql).nonEmpty
            if owner.autoCommit || (full20 && owner.isolation != Connection.TRANSACTION_REPEATABLE_READ) then
              throw new java.sql.SQLException("scripted Full20 row lock requires the actual owned REPEATABLE READ transaction")

        private def queryResult(sql: String, preparedMode: Boolean): ResultSet =
          verifyLockingRead(sql)
          val data = query(sql, copyParameters(), connectionId, owner.currentState())
          result = Some(resultSet(data, currentStatement(preparedMode)))
          callSets = Vector.empty
          callPosition = -1
          updateCount = -1
          result.get

        private def executeMutation(sql: String): Int =
          result = None
          callSets = Vector.empty
          callPosition = -1
          updateCount = -1
          owner.applyMutation(sql, copyParameters())

        private def executeAny(sql: String, preparedMode: Boolean): Boolean =
          val normalized = sql.trim.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT)
          if normalized.startsWith("CALL ") then
            val expectedCall = s"CALL `${baseline.identity.schema.toUpperCase(Locale.ROOT)}`.`FP_AUDIT_SCHEMA_FORMAT3`()"
            if normalized != expectedCall then
              throw new java.sql.SQLException("scripted MySQL JDBC rejected an uncontracted stored procedure call: " + sql)
            val state = owner.currentState()
            callSets = auditCall(connectionId, state)
            callPosition = 0
            updateCount = -1
            result = Some(resultSet(callSets.head, currentStatement(preparedMode)))
            true
          else if normalized.startsWith("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED") then
            owner.isolation = Connection.TRANSACTION_READ_COMMITTED
            currentIsolation = "READ-COMMITTED"
            result = None; callSets = Vector.empty; callPosition = -1; updateCount = -1
            false
          else if normalized.startsWith("SET SESSION SQL_MODE = 'STRICT_ALL_TABLES'") then
            currentSqlMode = "STRICT_ALL_TABLES"
            result = None; callSets = Vector.empty; callPosition = -1; updateCount = -1
            false
          else if normalized.startsWith("SET SESSION TIME_ZONE = '+00:00'") then
            currentTimeZone = "+00:00"
            result = None; callSets = Vector.empty; callPosition = -1; updateCount = -1
            false
          else if normalized.startsWith("SELECT ") || normalized.startsWith("SHOW ") then
            verifyLockingRead(sql)
            result = Some(resultSet(query(sql, copyParameters(), connectionId, owner.currentState()), currentStatement(preparedMode)))
            callSets = Vector.empty; callPosition = -1; updateCount = -1
            true
          else throw new java.sql.SQLException("scripted MySQL JDBC rejected an uncontracted Statement.execute: " + sql)

        private def dispatchStatement(proxy: Object, method: Method, args: Array[Object], preparedMode: Boolean): Object =
          objectMethod(method, proxy, args).getOrElse {
            method.getName match
              case "close" => closedStatement = true; result = None; null
              case "isClosed" => java.lang.Boolean.valueOf(closedStatement)
              case "getConnection" => owner.connection
              case "getResultSet" => result.orNull
              case "getUpdateCount" => Integer.valueOf(updateCount)
              case "getLargeUpdateCount" => java.lang.Long.valueOf(updateCount.toLong)
              case "getMoreResults" =>
                if callSets.isEmpty then { result = None; updateCount = -1; java.lang.Boolean.FALSE }
                else if callPosition < callSets.size - 1 then
                  callPosition += 1
                  result = Some(resultSet(callSets(callPosition), proxy.asInstanceOf[Statement]))
                  updateCount = -1
                  java.lang.Boolean.TRUE
                else if callPosition == callSets.size - 1 then
                  callPosition = callSets.size
                  result = None
                  updateCount = 0
                  java.lang.Boolean.FALSE
                else if callPosition == callSets.size && updateCount == 0 then
                  callPosition += 1
                  updateCount = -1
                  java.lang.Boolean.FALSE
                else
                  updateCount = -1
                  java.lang.Boolean.FALSE
              case "executeQuery" => ensureStatementOpen(); resultSetReturn(queryResult(sqlFrom(args), preparedMode))
              case "executeUpdate" => ensureStatementOpen(); Integer.valueOf(executeMutation(sqlFrom(args)))
              case "executeLargeUpdate" => ensureStatementOpen(); java.lang.Long.valueOf(executeMutation(sqlFrom(args)).toLong)
              case "execute" => ensureStatementOpen(); java.lang.Boolean.valueOf(executeAny(sqlFrom(args), preparedMode))
              case "setBytes" | "setString" | "setNString" | "setInt" | "setLong" | "setShort" | "setByte" |
                  "setBoolean" | "setDouble" | "setFloat" | "setBigDecimal" | "setTimestamp" | "setDate" | "setTime" |
                  "setObject" | "setNull" =>
                if !preparedMode || args.length < 2 then unsupported(method)
                val index = args(0).asInstanceOf[Number].intValue()
                if index <= 0 then throw new java.sql.SQLException("JDBC parameter indexes begin at one")
                val value = if method.getName == "setNull" then null else args(1)
                parameters = parameters.updated(index, value match
                  case bytes: Array[Byte] => bytes.clone()
                  case other => other)
                null
              case "clearParameters" => if !preparedMode then unsupported(method); parameters = Map.empty; null
              case "setFetchSize" | "setQueryTimeout" | "setMaxRows" | "setLargeMaxRows" | "setFetchDirection" |
                  "setMaxFieldSize" | "setEscapeProcessing" | "setPoolable" | "closeOnCompletion" => null
              case "getFetchSize" | "getQueryTimeout" | "getMaxRows" | "getLargeMaxRows" | "getFetchDirection" |
                  "getMaxFieldSize" => Integer.valueOf(0)
              case "getResultSetType" => Integer.valueOf(ResultSet.TYPE_FORWARD_ONLY)
              case "getResultSetConcurrency" => Integer.valueOf(ResultSet.CONCUR_READ_ONLY)
              case "getResultSetHoldability" => Integer.valueOf(ResultSet.CLOSE_CURSORS_AT_COMMIT)
              case "getWarnings" => null
              case "clearWarnings" | "cancel" => null
              case "isPoolable" | "isCloseOnCompletion" => java.lang.Boolean.FALSE
              case other => unsupported(method)
          }

        private def resultSetReturn(value: ResultSet): ResultSet = value

      private def resultSet(data: JdbcData, statement: Statement): ResultSet =
        if data == null || data.columns.isEmpty then throw new java.sql.SQLException("scripted query returned a resultset without columns")
        var rowIndex = -1
        var closedResult = false
        var wasNull = false
        var afterLast = false
        def ensureResultOpen(): Unit = if closedResult then throw new java.sql.SQLException("scripted MySQL resultset is closed")
        def columnIndex(raw: AnyRef): Int =
          val index = raw match
            case number: Number => number.intValue()
            case label: String => data.columns.indexWhere(_.label.equalsIgnoreCase(label)) + 1
            case other => throw new java.sql.SQLException("scripted ResultSet column key has unsupported type " + other.getClass.getName)
          if index < 1 || index > data.columns.size then throw new java.sql.SQLException("scripted ResultSet column is outside metadata bounds: " + raw)
          index - 1
        def cell(raw: AnyRef): Any =
          ensureResultOpen()
          if rowIndex < 0 || rowIndex >= data.rows.size then throw new java.sql.SQLException("scripted ResultSet cursor is not on a row")
          val value = data.rows(rowIndex)(columnIndex(raw))
          wasNull = value == null
          value
        def numeric(raw: AnyRef): Number = cell(raw) match
          case null => java.lang.Long.valueOf(0L)
          case value: Number => value
          case value: String =>
            try java.lang.Long.valueOf(value)
            catch case failure: NumberFormatException => throw new java.sql.SQLException("scripted numeric value is malformed", failure)
          case other => throw new java.sql.SQLException("scripted value is not numeric: " + other.getClass.getName)
        val metadataProxy = resultSetMetadata(data)
        jdbcProxy(classOf[ResultSet]) { (proxy, method, args) =>
          objectMethod(method, proxy, args).getOrElse {
            method.getName match
              case "next" => ensureResultOpen(); if rowIndex + 1 < data.rows.size then { rowIndex += 1; java.lang.Boolean.TRUE } else { rowIndex = data.rows.size; afterLast = true; java.lang.Boolean.FALSE }
              case "close" => closedResult = true; null
              case "isClosed" => java.lang.Boolean.valueOf(closedResult)
              case "wasNull" => java.lang.Boolean.valueOf(wasNull)
              case "getMetaData" => ensureResultOpen(); metadataProxy
              case "getStatement" => statement
              case "findColumn" => Integer.valueOf(columnIndex(args(0)) + 1)
              case "getString" | "getNString" => cell(args(0)) match
                case null => null
                case bytes: Array[Byte] => HexFormat.of().withUpperCase().formatHex(bytes)
                case value => value.toString
              case "getBytes" => cell(args(0)) match
                case null => null
                case bytes: Array[Byte] => bytes.clone()
                case value: String => value.getBytes(StandardCharsets.UTF_8)
                case other => throw new java.sql.SQLException("scripted value cannot be read as bytes: " + other.getClass.getName)
              case "getBinaryStream" => cell(args(0)) match
                case null => null
                case bytes: Array[Byte] => new java.io.ByteArrayInputStream(bytes.clone())
                case other => throw new java.sql.SQLException("scripted value cannot be streamed as binary: " + other.getClass.getName)
              case "getCharacterStream" | "getNCharacterStream" => cell(args(0)) match
                case null => null
                case value: String => new java.io.StringReader(value)
                case other => throw new java.sql.SQLException("scripted value cannot be streamed as text: " + other.getClass.getName)
              case "getInt" => Integer.valueOf(numeric(args(0)).intValue())
              case "getLong" => java.lang.Long.valueOf(numeric(args(0)).longValue())
              case "getShort" => java.lang.Short.valueOf(numeric(args(0)).shortValue())
              case "getByte" => java.lang.Byte.valueOf(numeric(args(0)).byteValue())
              case "getDouble" => cell(args(0)) match
                case null => java.lang.Double.valueOf(0d)
                case value: Number => java.lang.Double.valueOf(value.doubleValue())
                case value: String => java.lang.Double.valueOf(value)
                case other => throw new java.sql.SQLException("scripted value cannot be read as double: " + other.getClass.getName)
              case "getFloat" => java.lang.Float.valueOf(numeric(args(0)).floatValue())
              case "getBoolean" => cell(args(0)) match
                case null => java.lang.Boolean.FALSE
                case value: java.lang.Boolean => value
                case number: Number => java.lang.Boolean.valueOf(number.longValue() != 0L)
                case other => throw new java.sql.SQLException("scripted value cannot be read as boolean: " + other.getClass.getName)
              case "getTimestamp" => cell(args(0)) match
                case null => null
                case timestamp: java.sql.Timestamp => timestamp
                case value: LocalDateTime => java.sql.Timestamp.valueOf(value)
                case other => throw new java.sql.SQLException("scripted value cannot be read as timestamp: " + other.getClass.getName)
              case "getObject" =>
                val value = cell(args(0))
                if value == null || args.length == 1 then value.asInstanceOf[Object]
                else convertObject(value, args(1).asInstanceOf[Class[?]])
              case "getBigDecimal" => cell(args(0)) match
                case null => null
                case value: java.math.BigDecimal => value
                case number: Number => java.math.BigDecimal.valueOf(number.doubleValue())
                case value: String => new java.math.BigDecimal(value)
                case other => throw new java.sql.SQLException("scripted value cannot be read as BigDecimal: " + other.getClass.getName)
              case "getRow" => Integer.valueOf(if rowIndex >= 0 && rowIndex < data.rows.size then rowIndex + 1 else 0)
              case "isBeforeFirst" => java.lang.Boolean.valueOf(rowIndex < 0 && data.rows.nonEmpty)
              case "isAfterLast" => java.lang.Boolean.valueOf(afterLast && data.rows.nonEmpty)
              case "isFirst" => java.lang.Boolean.valueOf(rowIndex == 0 && data.rows.nonEmpty)
              case "isLast" => java.lang.Boolean.valueOf(rowIndex == data.rows.size - 1 && data.rows.nonEmpty)
              case "getType" => Integer.valueOf(ResultSet.TYPE_FORWARD_ONLY)
              case "getConcurrency" => Integer.valueOf(ResultSet.CONCUR_READ_ONLY)
              case "getHoldability" => Integer.valueOf(ResultSet.CLOSE_CURSORS_AT_COMMIT)
              case "getFetchDirection" => Integer.valueOf(ResultSet.FETCH_FORWARD)
              case "getFetchSize" => Integer.valueOf(data.rows.size)
              case other => unsupported(method)
          }
        }

      private def convertObject(value: Any, target: Class[?]): Object =
        if target.isInstance(value.asInstanceOf[Object]) then value.asInstanceOf[Object]
        else if target == classOf[LocalDateTime] then value match
          case local: LocalDateTime => local
          case timestamp: java.sql.Timestamp => timestamp.toLocalDateTime
          case date: java.sql.Date => date.toLocalDate.atStartOfDay()
          case _ => throw new java.sql.SQLException("scripted value cannot convert to LocalDateTime")
        else if target == classOf[java.sql.Timestamp] then value match
          case timestamp: java.sql.Timestamp => timestamp
          case local: LocalDateTime => java.sql.Timestamp.valueOf(local)
          case _ => throw new java.sql.SQLException("scripted value cannot convert to Timestamp")
        else throw new java.sql.SQLException("scripted value cannot convert to requested class " + target.getName)

    private def resultSetMetadata(data: JdbcData): ResultSetMetaData = jdbcProxy(classOf[ResultSetMetaData]) { (proxy, method, args) =>
      objectMethod(method, proxy, args).getOrElse {
        method.getName match
          case "getColumnCount" => Integer.valueOf(data.columns.size)
          case "getColumnLabel" | "getColumnName" => data.columns(columnIndex(args(0), data.columns.size)).label
          case "getColumnType" => Integer.valueOf(data.columns(columnIndex(args(0), data.columns.size)).jdbcType)
          case "getColumnTypeName" => data.columns(columnIndex(args(0), data.columns.size)).typeName
          case "getPrecision" | "getColumnDisplaySize" => Integer.valueOf(data.columns(columnIndex(args(0), data.columns.size)).precision)
          case "getScale" => Integer.valueOf(data.columns(columnIndex(args(0), data.columns.size)).scale)
          case "isNullable" => Integer.valueOf(data.columns(columnIndex(args(0), data.columns.size)).nullable)
          case "isSigned" => java.lang.Boolean.valueOf(data.columns(columnIndex(args(0), data.columns.size)).signed)
          case "isAutoIncrement" | "isCurrency" | "isWritable" | "isDefinitelyWritable" => java.lang.Boolean.FALSE
          case "isCaseSensitive" | "isSearchable" | "isReadOnly" => java.lang.Boolean.TRUE
          case "getCatalogName" | "getSchemaName" | "getTableName" => ""
          case "getColumnClassName" =>
            val column = data.columns(columnIndex(args(0), data.columns.size))
            column.jdbcType match
              case Types.VARBINARY | Types.BINARY | Types.LONGVARBINARY | Types.BLOB => classOf[Array[Byte]].getName
              case Types.INTEGER | Types.BIGINT | Types.SMALLINT | Types.TINYINT => classOf[java.lang.Long].getName
              case Types.DOUBLE | Types.FLOAT | Types.REAL | Types.DECIMAL | Types.NUMERIC => classOf[java.lang.Double].getName
              case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE => classOf[LocalDateTime].getName
              case _ => classOf[String].getName
          case other => unsupported(method)
      }
    }

    private def columnIndex(raw: AnyRef, size: Int): Int =
      val index = raw.asInstanceOf[Number].intValue()
      if index < 1 || index > size then throw new java.sql.SQLException("scripted metadata column index is out of range: " + index)
      index - 1

    def observation(): DatabaseObservation =
      val value = readState()
      DatabaseObservation(value.writerEpoch, value.writerUpdateCount, value.commitCount,
        value.bindingState, value.intent.map(_.stage), full20LockedReadCount.get(),
        sessionLockAcquireCount.get(), sessionLockOwnerCheckCount.get(), lastConnectionId.get())

    private def loadBaseline(metadata: MySqlRuntimeMetadata): MySqlApprovedStaticBaseline =
      val pins = metadata.baseline()
      val identity = MySqlApprovedBaselineIdentity(pins.format(), pins.schemaFormat(), pins.coverage(),
        metadata.expectedServerUuid(), pins.serverVersion(), metadata.schema(), pins.provisioningUuid(),
        metadata.expectedBindingUuid(), pins.ddlSha256(), pins.auditDefiner(), pins.sourceCaptureSha256())
      MySqlApprovedBaselineLoader.load(pins.absolutePath(), pins.fileSha256(), identity)

    private def readState(): DatabaseState =
      val root = JsonParser.parseString(Files.readString(statePath, StandardCharsets.UTF_8)).getAsJsonObject
      def optional(name: String): Option[String] = Option(root.get(name)).filterNot(_.isJsonNull).map(_.getAsString)
      val intent = optional("intentBootstrap").map { bootstrap =>
        IntentState(bootstrap, optional("intentPrepare").get, optional("intentCertify").get,
          optional("intentBinding").get, optional("intentProvisioning").get, optional("intentSource").get,
          optional("intentReceipt").get, optional("intentStage").get, optional("intentAck"))
      }
      DatabaseState(root.get("writerEpoch").getAsLong, optional("writerOwner"),
        LocalDateTime.parse(root.get("leaseUntil").getAsString), optional("lastOperation"),
        optional("lastFingerprint"), optional("bindingState"), intent,
        root.get("writerUpdateCount").getAsInt, root.get("commitCount").getAsInt, committedControlledRows)

    private def writeState(value: DatabaseState): Unit =
      val root = new JsonObject
      def optional(name: String, content: Option[String]): Unit = content match
        case Some(text) => root.addProperty(name, text)
        case None => root.add(name, com.google.gson.JsonNull.INSTANCE)
      root.addProperty("writerEpoch", value.writerEpoch)
      optional("writerOwner", value.writerOwner)
      root.addProperty("leaseUntil", value.leaseUntil.toString)
      optional("lastOperation", value.lastOperation)
      optional("lastFingerprint", value.lastFingerprint)
      optional("bindingState", value.bindingState)
      value.intent match
        case Some(intent) =>
          root.addProperty("intentBootstrap", intent.bootstrap)
          root.addProperty("intentPrepare", intent.prepare)
          root.addProperty("intentCertify", intent.certify)
          root.addProperty("intentBinding", intent.binding)
          root.addProperty("intentProvisioning", intent.provisioning)
          root.addProperty("intentSource", intent.sourceIdentity)
          root.addProperty("intentReceipt", intent.receiptSha256)
          root.addProperty("intentStage", intent.stage)
          optional("intentAck", intent.acknowledgementSha256)
        case None =>
          Vector("intentBootstrap", "intentPrepare", "intentCertify", "intentBinding", "intentProvisioning",
            "intentSource", "intentReceipt", "intentStage", "intentAck").foreach(name => optional(name, None))
      root.addProperty("writerUpdateCount", value.writerUpdateCount)
      root.addProperty("commitCount", value.commitCount)
      val next = statePath.resolveSibling(statePath.getFileName.toString + ".next")
      Files.writeString(next, new GsonBuilder().disableHtmlEscaping().create().toJson(root), StandardCharsets.UTF_8)
      Files.move(next, statePath, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
        java.nio.file.StandardCopyOption.REPLACE_EXISTING)
      committedControlledRows = value.controlledRows

    private def requireTransaction(active: Option[DatabaseState]): DatabaseState = active.getOrElse(
      throw new java.sql.SQLException("scripted MySQL mutation escaped the actor-owned JDBC transaction"))

    private def update(active: Option[DatabaseState], next: DatabaseState => DatabaseState): DatabaseState =
      requireTransaction(active) match
        case current => next(current)

    private def bytes(value: String): Array[Byte] = HexFormat.of().parseHex(value)
    private def hexBytes(value: Array[Byte]): String = HexFormat.of().withUpperCase().formatHex(value)
    private def uuidBytes(value: String): Array[Byte] =
      val uuid = UUID.fromString(value)
      java.nio.ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits).putLong(uuid.getLeastSignificantBits).array()
    private def uuidFromBytes(value: Array[Byte]): String =
      if value == null || value.length != 16 then throw new java.sql.SQLException("scripted MySQL UUID binding must be exactly 16 bytes")
      val input = java.nio.ByteBuffer.wrap(value)
      new UUID(input.getLong(), input.getLong()).toString
    private def safeParam(parameters: Map[Int, Any], index: Int): Any = parameters.getOrElse(index,
      throw new java.sql.SQLException(s"scripted MySQL statement omitted required JDBC parameter $index"))
    private def paramBytes(parameters: Map[Int, Any], index: Int): Array[Byte] = safeParam(parameters, index) match
      case value: Array[Byte] => value
      case null => null
      case other => throw new java.sql.SQLException(s"scripted MySQL JDBC parameter $index is not binary: ${other.getClass.getName}")
    private def paramUuid(parameters: Map[Int, Any], index: Int): String = uuidFromBytes(paramBytes(parameters, index))
    private def paramTime(parameters: Map[Int, Any], index: Int): LocalDateTime = safeParam(parameters, index) match
      case value: LocalDateTime => value
      case value: java.sql.Timestamp => value.toLocalDateTime
      case other => throw new java.sql.SQLException(s"scripted MySQL JDBC parameter $index is not DATETIME(6): ${other.getClass.getName}")

    private def valueBytes(value: Option[String], uuid: Boolean = false): Array[Byte] = value.map { raw =>
      if uuid then uuidBytes(raw) else bytes(raw)
    }.orNull

    private def auditCellValue(value: MySqlAuditCell): Any = value match
      case MySqlAuditNull => null
      case MySqlAuditText(text) => text
      case binary: MySqlAuditBinary => binary.bytes
      case MySqlAuditTimestamp(timestamp) => timestamp

    private def auditData(value: MySqlAuditResultSet): JdbcData = JdbcData(
      value.columns.map(column => JdbcColumn(column.label, column.jdbcType, column.typeName, column.precision,
        column.scale, column.nullable, column.signed)),
      value.rows.map(_.cells.map(auditCellValue)))

    private def contractResultSet(value: JsonObject): JdbcData =
      val columns = value.getAsJsonArray("columns").iterator().asScala.toVector.map { raw =>
        val column = raw.getAsJsonObject
        JdbcColumn(column.get("label").getAsString, column.get("jdbcType").getAsInt,
          column.get("typeName").getAsString, column.get("precision").getAsInt, column.get("scale").getAsInt,
          column.get("nullable").getAsInt, column.get("signed").getAsBoolean)
      }
      val rows = value.getAsJsonArray("rows").iterator().asScala.toVector.map { rawRow =>
        rawRow.getAsJsonArray.iterator().asScala.toVector.map { rawCell =>
          val cell = rawCell.getAsJsonObject
          cell.get("tag").getAsString match
            case "SQL_NULL" => null
            case "TEXT" => cell.get("value").getAsString
            case "BINARY" => HexFormat.of().parseHex(cell.get("hex").getAsString)
            case other => throw new java.sql.SQLException("synthetic definer privilege contract has an unknown cell kind: " + other)
        }
      }
      JdbcData(columns, rows)

    private def dynamicAuditColumn(label: String): JdbcColumn =
      val integer = Set("connection_id", "singleton_fixed1", "format_version", "max_region_bytes", "max_chunk_bytes",
        "max_decompressed_bytes", "max_nbt_depth", "max_members", "max_retained_bytes", "max_sql_packet_bytes",
        "max_native_artifact_bytes")
      if label == "observed_db_time" then JdbcColumn(label, Types.TIMESTAMP, "TIMESTAMP", scale = 6)
      else if Set("ddl_sha256", "provisioning_uuid", "expected_server_uuid", "expected_binding_uuid",
        "import_operation_uuid", "import_manifest_sha256", "import_snapshot_sha256", "import_receipt_sha256",
        "guard_manifest_sha256", "configuration_sha256", "source_provenance_bytes", "source_provenance_sha256",
        "limit_profile_bytes", "limit_profile_sha256").contains(label) then
        JdbcColumn(label, Types.VARBINARY, "VARBINARY", precision = 65535)
      else if integer.contains(label) then
        val kind = if Set("connection_id", "max_region_bytes", "max_chunk_bytes", "max_decompressed_bytes",
          "max_retained_bytes", "max_sql_packet_bytes", "max_native_artifact_bytes").contains(label) then Types.BIGINT else Types.INTEGER
        JdbcColumn(label, kind, if kind == Types.BIGINT then "BIGINT" else "INT", signed = true)
      else JdbcColumn(label, Types.VARCHAR, "VARCHAR")

    private def headerValues(connectionId: Long, state: DatabaseState): Vector[Any] =
      val baselineIdentity = baseline.identity
      val runtime = contractJson
      val profileBytes = HexFormat.of().parseHex(runtime.get("limitProfileHex").getAsString)
      val provenanceBytes = HexFormat.of().parseHex(runtime.get("sourceProvenanceHex").getAsString)
      val rawNow = LocalDateTime.now(java.time.ZoneOffset.UTC)
      val now = rawNow.withNano((rawNow.getNano / 1000) * 1000)
      val values: Map[String, Any] = Map(
        "server_uuid" -> baselineIdentity.serverUuid.toString,
        "server_version" -> baselineIdentity.serverVersion,
        "connection_id" -> connectionId,
        "audit_definer" -> baselineIdentity.auditDefiner,
        "connected_caller" -> runtime.get("connectedUser").getAsString,
        "effective_roles" -> runtime.get("auditDefinerRoles").getAsString,
        "routine_default_schema" -> baselineIdentity.schema,
        "observed_db_time" -> now,
        "audited_schema" -> baselineIdentity.schema,
        "singleton_fixed1" -> 1,
        "backend" -> "MYSQL",
        "format_version" -> 3,
        "ddl_sha256" -> HexFormat.of().parseHex(baselineIdentity.ddlSha256),
        "provisioning_uuid" -> uuidBytes(baselineIdentity.provisioningUuid.toString),
        "bootstrap_state" -> "READY",
        "expected_server_uuid" -> uuidBytes(baselineIdentity.serverUuid.toString),
        "expected_binding_uuid" -> uuidBytes(baselineIdentity.bindingUuid.toString),
        "import_state" -> "NONE",
        "import_operation_uuid" -> null,
        "import_manifest_sha256" -> null,
        "import_snapshot_sha256" -> null,
        "import_receipt_sha256" -> null,
        "guard_manifest_sha256" -> bytes(baseline.objects.canonicalSha256),
        "configuration_sha256" -> configShaBytes,
        "source_provenance_kind" -> runtime.get("sourceProvenanceKind").getAsString,
        "source_provenance_bytes" -> provenanceBytes,
        "source_provenance_sha256" -> HexFormat.of().parseHex(runtime.get("sourceProvenanceSha256").getAsString),
        "limit_profile_encoding" -> runtime.get("limitProfileEncoding").getAsString,
        "limit_profile_bytes" -> profileBytes,
        "limit_profile_sha256" -> HexFormat.of().parseHex(runtime.get("limitProfileSha256").getAsString),
        "max_region_bytes" -> Profile.maxRegionBytes(),
        "max_chunk_bytes" -> Profile.maxChunkBytes(),
        "max_decompressed_bytes" -> Profile.maxDecompressedBytes(),
        "max_nbt_depth" -> Profile.maxNbtDepth(),
        "max_members" -> Profile.maxMembers(),
        "max_retained_bytes" -> Profile.maxRetainedBytes(),
        "max_sql_packet_bytes" -> Profile.maxSqlPacketBytes(),
        "max_native_artifact_bytes" -> Profile.maxNativeArtifactBytes())
      HeaderLabels.map(label => values.getOrElse(label,
        throw new java.sql.SQLException("no synthetic MySQL audit header value for " + label)))

    private def auditCall(connectionId: Long, state: DatabaseState): Vector[JdbcData] =
      val headerCells = headerValues(connectionId, state)
      val header = JdbcData(HeaderLabels.map(dynamicAuditColumn), Vector(headerCells))
      val static = independentStaticSets().map(auditData)
      val binding = bindingData(state, audit = true)
      val writer = writerData(state, connectionId, audit = true)
      val privileges = contractJson.getAsJsonArray("definerPrivileges").iterator().asScala.toVector.map(raw => contractResultSet(raw.getAsJsonObject))
      val footer = JdbcData(HeaderLabels.map(dynamicAuditColumn), Vector(headerCells))
      val result = Vector(header) ++ static.take(10) ++ Vector(binding, writer, footer) ++ privileges
      if result.size != 18 then throw new java.sql.SQLException("synthetic SQL CALL does not implement exact Format3 RS01-RS18")
      result

    private def manifestData(state: DatabaseState, audit: Boolean): JdbcData =
      if audit then auditCall(1L, state).head match
        case value => value
      else
        val labels = Vector("singleton_fixed1", "backend", "format_version", "ddl_sha256", "provisioning_uuid", "bootstrap_state",
          "expected_server_uuid", "expected_binding_uuid", "import_manifest_sha256", "import_state", "import_operation_uuid",
          "import_snapshot_sha256", "import_receipt_sha256", "guard_manifest_sha256")
        JdbcData(labels.map(dynamicAuditColumn), Vector(Vector[Any](1, "MYSQL", 3,
          bytes(baseline.identity.ddlSha256), uuidBytes(baseline.identity.provisioningUuid.toString), "READY",
          uuidBytes(baseline.identity.serverUuid.toString), uuidBytes(baseline.identity.bindingUuid.toString), null,
          "NONE", null, null, null, bytes(baseline.objects.canonicalSha256))))

    private def bindingData(state: DatabaseState, audit: Boolean): JdbcData =
      val labels = Vector("singleton_fixed1", "binding_uuid", "cutover_state", "format_version")
      val columns = labels.map { label =>
        if label == "singleton_fixed1" || label == "format_version" then JdbcColumn(label, Types.INTEGER, "INT", signed = true)
        else if label == "binding_uuid" then JdbcColumn(label, Types.VARBINARY, "VARBINARY", precision = 16)
        else JdbcColumn(label, Types.VARCHAR, "VARCHAR")
      }
      val rows = state.bindingState.toVector.map(stage => Vector[Any](1, uuidBytes(freshPlan.bindingUuid().toString), stage, 2))
      JdbcData(columns, rows)

    private def writerData(state: DatabaseState, connectionId: Long, audit: Boolean): JdbcData =
      val labels = if audit then Vector("singleton_fixed1", "binding_uuid", "writer_epoch", "owner_boot_uuid",
        "lease_until", "last_commit_operation_uuid", "last_commit_fingerprint") else
        Vector("singleton_fixed1", "binding_uuid", "writer_epoch", "owner_boot_uuid", "lease_until",
          "last_commit_operation_uuid", "last_commit_fingerprint", "database_now")
      val columns = labels.map { label => label match
        case "singleton_fixed1" => JdbcColumn(label, Types.INTEGER, "INT", signed = true)
        case "writer_epoch" => JdbcColumn(label, Types.BIGINT, "BIGINT", signed = true)
        case "binding_uuid" | "owner_boot_uuid" | "last_commit_operation_uuid" | "last_commit_fingerprint" =>
          JdbcColumn(label, Types.VARBINARY, "VARBINARY", precision = 32)
        case "lease_until" | "database_now" => JdbcColumn(label, Types.TIMESTAMP, "TIMESTAMP", scale = 6)
        case other => JdbcColumn(other, Types.VARCHAR, "VARCHAR")
      }
      val now = nowDatabase()
      val row = Vector[Any](1, uuidBytes(freshPlan.bindingUuid().toString), state.writerEpoch,
        valueBytes(state.writerOwner, uuid = true), state.leaseUntil, valueBytes(state.lastOperation, uuid = true),
        valueBytes(state.lastFingerprint), now)
      JdbcData(columns, Vector(row.take(labels.size)))

    private def nowDatabase(): LocalDateTime =
      val value = LocalDateTime.now(java.time.ZoneOffset.UTC)
      value.withNano((value.getNano / 1000) * 1000)

    private def intentData(state: DatabaseState): JdbcData =
      val labels = Vector("singleton_fixed1", "bootstrap_operation_uuid", "prepare_write_uuid", "certify_write_uuid",
        "binding_uuid", "binding_format_version", "provisioning_uuid", "source_world_identity",
        "provision_receipt_sha256", "stage", "marker_ack_sha256")
      val columns = labels.map { label =>
        if label == "singleton_fixed1" || label == "binding_format_version" then JdbcColumn(label, Types.INTEGER, "INT", signed = true)
        else if Set("bootstrap_operation_uuid", "prepare_write_uuid", "certify_write_uuid", "binding_uuid",
            "provisioning_uuid").contains(label) then JdbcColumn(label, Types.VARBINARY, "VARBINARY", precision = 16)
        else if Set("provision_receipt_sha256", "marker_ack_sha256").contains(label) then
          JdbcColumn(label, Types.VARBINARY, "VARBINARY", precision = 32)
        else JdbcColumn(label, Types.VARCHAR, "VARCHAR")
      }
      val rows = state.intent.toVector.map { value => Vector[Any](1, uuidBytes(value.bootstrap), uuidBytes(value.prepare),
        uuidBytes(value.certify), uuidBytes(value.binding), 2, uuidBytes(value.provisioning), value.sourceIdentity,
        bytes(value.receiptSha256), value.stage, value.acknowledgementSha256.map(bytes).orNull) }
      JdbcData(columns, rows)

    private def full20Data(table: String, state: DatabaseState, connectionId: Long): JdbcData =
      val relation = OrdinaryGoldenFull20.find(_.table == table).getOrElse(
        throw new java.sql.SQLException("unknown relation in independent synthetic Full20 contract: " + table))
      val rows: Vector[Vector[Any]] = table match
        case "fp_schema_manifest" =>
          val header = headerValues(connectionId, state)
          val manifest = HeaderLabels.zip(header).toMap
          Vector(relation.columns.map(column => manifest.getOrElse(column.name,
            throw new java.sql.SQLException("synthetic Full20 manifest lacks exact field " + column.name))))
        case "fp_runtime_writer" => Vector(writerData(state, connectionId, audit = true).rows.head)
        case "physical_recovery_binding" => bindingData(state, audit = true).rows
        case "binding_bootstrap_intent" => intentData(state).rows
        case _ => state.controlledRows.getOrElse(table, Vector.empty)
      val columns = relation.columns.map(_.jdbcMetadata)
      if rows.exists(_.size != columns.size) then throw new java.sql.SQLException("scripted Full20 row differs from the frozen 368-column relation shape: " + table)
      val keyIndexes = relation.primaryKey.map(name => relation.columns.indexWhere(_.name == name))
      def compareCell(left: Any, right: Any): Int = (left, right) match
        case (a: Array[Byte], b: Array[Byte]) => java.util.Arrays.compareUnsigned(a, b)
        case (a: Number, b: Number) => BigDecimal(a.toString).compare(BigDecimal(b.toString))
        case (a: String, b: String) => a.compareTo(b)
        case _ => throw new java.sql.SQLException("scripted Full20 primary-key type is unsupported")
      val ordered = rows.sortWith { (left, right) =>
        var compared = 0
        var i = 0
        while compared == 0 && i < keyIndexes.size do
          val column = keyIndexes(i)
          compared = compareCell(left(column), right(column))
          i += 1
        compared < 0
      }
      JdbcData(columns, ordered)

    def full20MetadataForContractProbe(table: String): ResultSetMetaData =
      resultSetMetadata(full20Data(table, readState(), 1L))

    private def query(sql: String, parameters: Map[Int, Any], connectionId: Long, state: DatabaseState): JdbcData =
      val normalized = sql.trim.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT)
      if normalized.startsWith("SELECT @@GLOBAL.SERVER_UUID") then
        JdbcData(Vector(JdbcColumn("server_uuid", Types.VARCHAR, "VARCHAR"), JdbcColumn("version", Types.VARCHAR, "VARCHAR"),
          JdbcColumn("catalog", Types.VARCHAR, "VARCHAR"), JdbcColumn("effective_user", Types.VARCHAR, "VARCHAR"),
          JdbcColumn("default_storage_engine", Types.VARCHAR, "VARCHAR"), JdbcColumn("innodb_flush_log_at_trx_commit", Types.INTEGER, "INT", signed = true),
          JdbcColumn("sync_binlog", Types.BIGINT, "BIGINT", signed = true)),
          Vector(Vector[Any](baseline.identity.serverUuid.toString, baseline.identity.serverVersion, baseline.identity.schema,
            metadata.expectedEffectiveUser(), "InnoDB", metadata.innodbFlushLogAtTrxCommit(), metadata.syncBinlog())))
      else if normalized.startsWith("SELECT @@SESSION.TRANSACTION_ISOLATION") then
        JdbcData(Vector(JdbcColumn("transaction_isolation", Types.VARCHAR, "VARCHAR"), JdbcColumn("sql_mode", Types.VARCHAR, "VARCHAR"),
          JdbcColumn("time_zone", Types.VARCHAR, "VARCHAR")),
          Vector(Vector[Any](currentIsolation, currentSqlMode, currentTimeZone)))
      else if normalized == "SELECT @@SESSION.MAX_ALLOWED_PACKET" then
        JdbcData(Vector(JdbcColumn("max_allowed_packet", Types.BIGINT, "BIGINT", signed = true)),
          Vector(Vector[Any](Profile.maxSqlPacketBytes())))
      else if normalized == "SELECT CONNECTION_ID()" then
        JdbcData(Vector(JdbcColumn("CONNECTION_ID()", Types.BIGINT, "BIGINT", signed = true)), Vector(Vector[Any](connectionId)))
      else if normalized.startsWith("SELECT GET_LOCK(") then
        val name = MySqlSessionBarrierNamespace.forIdentity(baseline.identity.serverUuid, baseline.identity.schema,
          baseline.identity.provisioningUuid, baseline.identity.bindingUuid).lockName()
        require(parameters.get(1).contains(name) && parameters.get(2).exists(_.isInstanceOf[Number]),
          "session barrier requested an unexpected lock namespace")
        sessionLockAcquireCount.incrementAndGet()
        JdbcData(Vector(JdbcColumn("GET_LOCK", Types.INTEGER, "INT", signed = true)),
          Vector(Vector[Any](acquireSessionLock(name, connectionId))))
      else if normalized.startsWith("SELECT CONNECTION_ID(), IS_USED_LOCK(") then
        val name = MySqlSessionBarrierNamespace.forIdentity(baseline.identity.serverUuid, baseline.identity.schema,
          baseline.identity.provisioningUuid, baseline.identity.bindingUuid).lockName()
        require(parameters.get(1).contains(name),
          "session barrier owner check changed lock namespace")
        sessionLockOwnerCheckCount.incrementAndGet()
        JdbcData(Vector(JdbcColumn("CONNECTION_ID()", Types.BIGINT, "BIGINT", signed = true),
          JdbcColumn("IS_USED_LOCK(?)", Types.BIGINT, "BIGINT", nullable = ResultSetMetaData.columnNullable, signed = true)),
          Vector(Vector[Any](connectionId, sessionLockOwner(name).map(value => java.lang.Long.valueOf(value)).orNull)))
      else if normalized.startsWith("SHOW SESSION STATUS LIKE ") then
        val name = if normalized.contains("'SSL_VERSION'") then "Ssl_version"
          else if normalized.contains("'SSL_CIPHER'") then "Ssl_cipher"
          else throw new java.sql.SQLException("unexpected SHOW SESSION STATUS selector: " + sql)
        JdbcData(Vector(JdbcColumn("Variable_name", Types.VARCHAR, "VARCHAR"), JdbcColumn("Value", Types.VARCHAR, "VARCHAR")),
          Vector(Vector[Any](name, if name == "Ssl_version" then "TLSv1.3" else "TLS_AES_256_GCM_SHA384")))
      else if normalized == "SHOW GRANTS" then
        val grants = contractJson.getAsJsonArray("callerGrants").iterator().asScala.toVector.map(value => Vector[Any](value.getAsString))
        JdbcData(Vector(JdbcColumn("Grants for fp_runtime@localhost", Types.VARCHAR, "VARCHAR")), grants)
      else if normalized.startsWith("SELECT @@SERVER_UUID AS SERVER_UUID,") then
        JdbcData(CallerScopeLabels.map(dynamicAuditColumn),
          Vector(Vector[Any](baseline.identity.serverUuid.toString, connectionId, baseline.identity.schema,
            metadata.expectedEffectiveUser(), contractJson.get("callerRoles").getAsString,
            contractJson.get("connectedUser").getAsString, baseline.identity.serverVersion)))
      else if normalized.startsWith("CALL `" + baseline.identity.schema.toUpperCase(Locale.ROOT) + "`.`FP_AUDIT_SCHEMA_FORMAT3`()") then
        throw new java.sql.SQLException("stored procedure result protocol is available only through Statement.execute(CALL)")
      else if normalized.startsWith("SELECT SINGLETON_FIXED1,BACKEND,FORMAT_VERSION,DDL_SHA256,PROVISIONING_UUID,BOOTSTRAP_STATE,") then
        manifestData(state, audit = false)
      else if normalized.startsWith("SELECT SINGLETON_FIXED1,BINDING_UUID,CUTOVER_STATE,FORMAT_VERSION FROM ") then
        bindingData(state, audit = false)
      else if normalized.startsWith("SELECT SINGLETON_FIXED1,BINDING_UUID,WRITER_EPOCH,OWNER_BOOT_UUID,LEASE_UNTIL,LAST_COMMIT_OPERATION_UUID,") then
        writerData(state, connectionId, audit = false)
      else if normalized.startsWith("SELECT SINGLETON_FIXED1,BOOTSTRAP_OPERATION_UUID,PREPARE_WRITE_UUID,CERTIFY_WRITE_UUID,") then
        intentData(state)
      else
        Full20ProjectionPattern.findFirstMatchIn(sql) match
          case Some(found) if Full20RelationNames.contains(found.group(2)) =>
            val relation = OrdinaryGoldenFull20.find(_.table == found.group(2)).getOrElse(
              throw new java.sql.SQLException("independent synthetic Full20 relation is missing: " + found.group(2)))
            if fixedIdentifierList(found.group(1)) != relation.columns.map(_.name) then
              throw new java.sql.SQLException("actor Full20 SELECT projection/order differs from independent golden: " + relation.table)
            if fixedIdentifierList(found.group(3)) != relation.primaryKey then
              throw new java.sql.SQLException("actor Full20 ORDER BY differs from independent golden: " + relation.table)
            if normalized.contains(" FOR UPDATE") || normalized.contains(" FOR SHARE") then full20LockedReadCount.incrementAndGet()
            full20Data(relation.table, state, connectionId)
          case _ => throw new java.sql.SQLException("scripted MySQL JDBC rejected an uncontracted actor query: " + sql)

    private var currentIsolation = "REPEATABLE-READ"
    private var currentSqlMode = "STRICT_TRANS_TABLES"
    private var currentTimeZone = "+00:00"

    private def executeUpdate(sql: String, parameters: Map[Int, Any], active: Option[DatabaseState]): (DatabaseState, Int) =
      val normalized = sql.trim.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT)
      val before = requireTransaction(active)
      val controlledTable = "(?is)^(?:INSERT INTO|UPDATE) `([^`]+)`\\.`([^`]+)` .*".r
      val controlled = sql.trim match
        case controlledTable(schema, table) if schema == metadata.schema() && ControlledTables.contains(table) => Some(table)
        case _ => None
      if controlled.nonEmpty then applyControlledMutation(sql, parameters, before, controlled.get)
      else if normalized.startsWith("INSERT INTO ") && normalized.contains(".`BINDING_BOOTSTRAP_INTENT`") then
        val exactSql = normalized.contains("(SINGLETON_FIXED1,BOOTSTRAP_OPERATION_UUID,PREPARE_WRITE_UUID,CERTIFY_WRITE_UUID,BINDING_UUID,BINDING_FORMAT_VERSION,") &&
          normalized.contains("PROVISIONING_UUID,SOURCE_WORLD_IDENTITY,PROVISION_RECEIPT_SHA256,STAGE,MARKER_ACK_SHA256) VALUES (1,?,?,?,?,?,?,?,?, 'PREPARED',NULL)")
        if before.intent.nonEmpty then throw new java.sql.SQLException("B1 intent singleton insert conflicted with existing synthetic state")
        if !exactSql || paramBytes(parameters, 4) == null || paramBytes(parameters, 4).length != 16 || safeParam(parameters, 5).asInstanceOf[Number].intValue() != 2 ||
            paramBytes(parameters, 8).length != 32 then
          throw new java.sql.SQLException("B1 PREPARED intent has malformed UUID/receipt bytes")
        val intent = IntentState(paramUuid(parameters, 1), paramUuid(parameters, 2), paramUuid(parameters, 3),
          paramUuid(parameters, 4), paramUuid(parameters, 6), safeParam(parameters, 7).asInstanceOf[String],
          hexBytes(paramBytes(parameters, 8)), "PREPARED", None)
        (before.copy(intent = Some(intent)), 1)
      else if normalized.startsWith("INSERT INTO ") && normalized.contains(".`PHYSICAL_RECOVERY_BINDING`") then
        val exactSql = normalized.contains("(SINGLETON_FIXED1,BINDING_UUID,CUTOVER_STATE,FORMAT_VERSION) VALUES (1,?,'LEGACY_UNCERTIFIED',2)")
        if !exactSql || before.bindingState.nonEmpty || paramUuid(parameters, 1) != freshPlan.bindingUuid().toString then
          throw new java.sql.SQLException("legacy B1 binding insertion violated the singleton/Fresh plan precondition")
        (before.copy(bindingState = Some("LEGACY_UNCERTIFIED")), 1)
      else if normalized.startsWith("UPDATE ") && normalized.contains(".`PHYSICAL_RECOVERY_BINDING`") && normalized.contains("SET CUTOVER_STATE='CERTIFIED'") then
        val exactSql = normalized.contains("AND BINDING_UUID=?") &&
          normalized.contains("AND CUTOVER_STATE='LEGACY_UNCERTIFIED'") && normalized.contains("AND FORMAT_VERSION=2")
        if !exactSql || before.bindingState != Some("LEGACY_UNCERTIFIED") || paramUuid(parameters, 1) != freshPlan.bindingUuid().toString then
          throw new java.sql.SQLException("B1 certification update did not match its exact legacy binding preimage")
        (before.copy(bindingState = Some("CERTIFIED")), 1)
      else if normalized.startsWith("UPDATE ") && normalized.contains(".`BINDING_BOOTSTRAP_INTENT`") then
        val old = before.intent.getOrElse(throw new java.sql.SQLException("B1 completion update has no PREPARED intent"))
        val exactSql = normalized.contains("WHERE SINGLETON_FIXED1=1") && normalized.contains("AND BOOTSTRAP_OPERATION_UUID=?") &&
          normalized.contains("AND PREPARE_WRITE_UUID=?") && normalized.contains("AND CERTIFY_WRITE_UUID=?") &&
          normalized.contains("AND BINDING_UUID=?") && normalized.contains("AND BINDING_FORMAT_VERSION=2") &&
          normalized.contains("AND PROVISIONING_UUID=?") && normalized.contains("AND STAGE='PREPARED'") &&
          normalized.contains("AND MARKER_ACK_SHA256 IS NULL")
        val exact = paramUuid(parameters, 2) == old.bootstrap && paramUuid(parameters, 3) == old.prepare &&
          paramUuid(parameters, 4) == old.certify && paramUuid(parameters, 5) == old.binding &&
          paramUuid(parameters, 6) == old.provisioning && safeParam(parameters, 7).asInstanceOf[String] == old.sourceIdentity &&
          hexBytes(paramBytes(parameters, 8)) == old.receiptSha256 && old.stage == "PREPARED" && old.acknowledgementSha256.isEmpty
        if !exactSql || !exact || paramBytes(parameters, 1) == null || paramBytes(parameters, 1).length != 32 then
          throw new java.sql.SQLException("B1 marker ACK update did not match the exact PREPARED intent")
        (before.copy(intent = Some(old.copy(stage = "COMPLETE", acknowledgementSha256 = Some(hexBytes(paramBytes(parameters, 1)))))), 1)
      else if normalized.startsWith("UPDATE ") && normalized.contains(".`FP_RUNTIME_WRITER`") && normalized.contains("SET WRITER_EPOCH=?,OWNER_BOOT_UUID=?,LEASE_UNTIL=?") then
        if normalized.endsWith("AND OWNER_BOOT_UUID IS NULL AND LEASE_UNTIL=? AND LEASE_UNTIL<=?") && parameters.size == 7 then
          val exact = before.writerOwner.isEmpty && paramUuid(parameters, 4) == freshPlan.bindingUuid().toString &&
            safeParam(parameters, 5).asInstanceOf[Number].longValue() == before.writerEpoch &&
            paramTime(parameters, 6) == before.leaseUntil && !before.leaseUntil.isAfter(paramTime(parameters, 7)) &&
            safeParam(parameters, 1).asInstanceOf[Number].longValue() == Math.addExact(before.writerEpoch, 1L)
          if !exact then throw new java.sql.SQLException("certified writer acquire differs from exact unowned expired preimage")
          (before.copy(writerEpoch = safeParam(parameters, 1).asInstanceOf[Number].longValue(),
            writerOwner = Some(paramUuid(parameters, 2)), leaseUntil = paramTime(parameters, 3),
            writerUpdateCount = before.writerUpdateCount + 1), 1)
        else if normalized.contains("AND LAST_COMMIT_OPERATION_UUID IS NULL") then
          val exactSql = normalized.contains("WHERE SINGLETON_FIXED1=1") && normalized.contains("AND BINDING_UUID=?") &&
            normalized.contains("AND WRITER_EPOCH=?") && normalized.contains("AND OWNER_BOOT_UUID IS NULL") &&
            normalized.contains("AND LEASE_UNTIL=?") && normalized.contains("AND LEASE_UNTIL<=?") &&
            normalized.contains("AND LAST_COMMIT_FINGERPRINT IS NULL")
          val exact = exactSql && before.writerOwner.isEmpty && before.lastOperation.isEmpty && before.lastFingerprint.isEmpty &&
            paramUuid(parameters, 6) == freshPlan.bindingUuid().toString &&
            safeParam(parameters, 7).asInstanceOf[Number].longValue() == before.writerEpoch &&
            paramTime(parameters, 8) == before.leaseUntil && !before.leaseUntil.isAfter(paramTime(parameters, 9))
          if !exact then throw new java.sql.SQLException("Fresh writer claim did not match its exact expired unowned receipt preimage")
          val next = before.copy(writerEpoch = safeParam(parameters, 1).asInstanceOf[Number].longValue(),
            writerOwner = Some(paramUuid(parameters, 2)), leaseUntil = paramTime(parameters, 3),
            lastOperation = Some(paramUuid(parameters, 4)), lastFingerprint = Some(hexBytes(paramBytes(parameters, 5))),
            writerUpdateCount = before.writerUpdateCount + 1)
          (next, 1)
        else
          val ownerBytes = paramBytes(parameters, 6)
          val operationBytes = paramBytes(parameters, 8)
          val fingerprintBytes = paramBytes(parameters, 9)
          val exactSql = normalized.contains("WHERE SINGLETON_FIXED1=1") && normalized.contains("AND BINDING_UUID=?") &&
            normalized.contains("AND WRITER_EPOCH=?") && normalized.contains("AND OWNER_BOOT_UUID <=> ?") &&
            normalized.contains("AND LEASE_UNTIL=?") && normalized.contains("AND LAST_COMMIT_OPERATION_UUID <=> ?") &&
            normalized.contains("AND LAST_COMMIT_FINGERPRINT <=> ?") &&
            normalized.contains("AND (OWNER_BOOT_UUID IS NULL OR LEASE_UNTIL<=UTC_TIMESTAMP(6))")
          val exact = exactSql && paramUuid(parameters, 4) == freshPlan.bindingUuid().toString &&
            safeParam(parameters, 5).asInstanceOf[Number].longValue() == before.writerEpoch &&
            Option(ownerBytes).map(uuidFromBytes) == before.writerOwner && paramTime(parameters, 7) == before.leaseUntil &&
            Option(operationBytes).map(uuidFromBytes) == before.lastOperation &&
            Option(fingerprintBytes).map(hexBytes) == before.lastFingerprint &&
            (before.writerOwner.isEmpty || !before.leaseUntil.isAfter(nowDatabase()))
          if !exact then throw new java.sql.SQLException("ordinary W owner/epoch/receipt CAS did not match its locked preimage")
          val next = before.copy(writerEpoch = safeParam(parameters, 1).asInstanceOf[Number].longValue(),
            writerOwner = Some(paramUuid(parameters, 2)), leaseUntil = paramTime(parameters, 3),
            writerUpdateCount = before.writerUpdateCount + 1)
          (next, 1)
      else if normalized.startsWith("UPDATE ") && normalized.contains(".`FP_RUNTIME_WRITER`") && normalized.contains("SET LAST_COMMIT_OPERATION_UUID=?,LAST_COMMIT_FINGERPRINT=?") then
        val oldOwner = Option(paramBytes(parameters, 5)).map(uuidFromBytes)
        val oldOperation = Option(paramBytes(parameters, 7)).map(uuidFromBytes)
        val oldFingerprint = Option(paramBytes(parameters, 8)).map(hexBytes)
        val exactSql = normalized.contains("WHERE SINGLETON_FIXED1=1") && normalized.contains("AND BINDING_UUID=?") &&
          normalized.contains("AND WRITER_EPOCH=?") && normalized.contains("AND OWNER_BOOT_UUID=?") &&
          normalized.contains("AND LEASE_UNTIL=?") && normalized.contains("AND LAST_COMMIT_OPERATION_UUID <=> ?") &&
          normalized.contains("AND LAST_COMMIT_FINGERPRINT <=> ?")
        val exact = exactSql && paramUuid(parameters, 3) == freshPlan.bindingUuid().toString &&
          safeParam(parameters, 4).asInstanceOf[Number].longValue() == before.writerEpoch &&
          oldOwner == before.writerOwner && paramTime(parameters, 6) == before.leaseUntil &&
          oldOperation == before.lastOperation && oldFingerprint == before.lastFingerprint
        if !exact then throw new java.sql.SQLException("B1 writer receipt CAS did not match its exact previous operation slot")
        (before.copy(lastOperation = Some(paramUuid(parameters, 1)),
          lastFingerprint = Some(hexBytes(paramBytes(parameters, 2))), writerUpdateCount = before.writerUpdateCount + 1), 1)
      else if normalized.startsWith("UPDATE ") && normalized.contains(".`FP_RUNTIME_WRITER`") && normalized.contains("SET OWNER_BOOT_UUID=NULL,LEASE_UNTIL=?") then
        val exactSql = normalized.contains("WHERE SINGLETON_FIXED1=1") && normalized.contains("AND BINDING_UUID=?") &&
          normalized.contains("AND WRITER_EPOCH=?") && normalized.contains("AND OWNER_BOOT_UUID=?") &&
          normalized.contains("AND LEASE_UNTIL=?") && normalized.contains("AND LEASE_UNTIL>?") &&
          normalized.contains("AND LAST_COMMIT_OPERATION_UUID <=> ?") && normalized.contains("AND LAST_COMMIT_FINGERPRINT <=> ?")
        val databaseNow = paramTime(parameters, 6)
        val releasedLease = paramTime(parameters, 1)
        val exact = exactSql && paramUuid(parameters, 2) == freshPlan.bindingUuid().toString &&
          safeParam(parameters, 3).asInstanceOf[Number].longValue() == before.writerEpoch &&
          Option(paramBytes(parameters, 4)).map(uuidFromBytes) == before.writerOwner &&
          paramTime(parameters, 5) == before.leaseUntil && before.leaseUntil.isAfter(databaseNow) &&
          releasedLease.plusNanos(1000L) == databaseNow &&
          Option(paramBytes(parameters, 7)).map(uuidFromBytes) == before.lastOperation &&
          Option(paramBytes(parameters, 8)).map(hexBytes) == before.lastFingerprint
        if !exact then throw new java.sql.SQLException("writer release did not match its live owner/epoch/receipt slot")
        (before.copy(writerOwner = None, leaseUntil = paramTime(parameters, 1),
          writerUpdateCount = before.writerUpdateCount + 1), 1)
      else if normalized.startsWith("UPDATE ") && normalized.contains(".`FP_RUNTIME_WRITER`") && normalized.contains("SET LEASE_UNTIL=?") then
        val exactSql = normalized.contains("AND BINDING_UUID=?") && normalized.contains("AND WRITER_EPOCH=?") &&
          normalized.contains("AND OWNER_BOOT_UUID=?") && normalized.contains("AND LEASE_UNTIL=?") &&
          normalized.contains("AND LEASE_UNTIL>?") && normalized.contains("AND LAST_COMMIT_OPERATION_UUID <=> ?") &&
          normalized.contains("AND LAST_COMMIT_FINGERPRINT <=> ?")
        val exactState = exactSql && paramUuid(parameters, 2) == freshPlan.bindingUuid().toString &&
          safeParam(parameters, 3).asInstanceOf[Number].longValue() == before.writerEpoch &&
          paramUuid(parameters, 4) == before.writerOwner.getOrElse("") && paramTime(parameters, 5) == before.leaseUntil &&
          before.leaseUntil.isAfter(paramTime(parameters, 6)) &&
          Option(paramBytes(parameters, 7)).map(uuidFromBytes) == before.lastOperation &&
          Option(paramBytes(parameters, 8)).map(hexBytes) == before.lastFingerprint &&
          paramTime(parameters, 1).isAfter(before.leaseUntil)
        if !exactState then throw new java.sql.SQLException("writer renew did not match its exact live owner/epoch/receipt slot")
        (before.copy(leaseUntil = paramTime(parameters, 1), writerUpdateCount = before.writerUpdateCount + 1), 1)
      else throw new java.sql.SQLException("scripted MySQL JDBC rejected an uncontracted actor mutation: " + sql)

  private object ScriptedDatabase:
    def initialize(path: Path): Unit =
      require(path != null && path.isAbsolute, "scripted SQL state path must be absolute")
      Files.createDirectories(path.getParent)
      val connectionIdPath = path.resolveSibling("connection-id-sequence.txt")
      if Files.exists(path) || Files.exists(connectionIdPath) then
        throw new IllegalStateException("test-owned scripted SQL state already exists")
      val rawNow = LocalDateTime.now(java.time.ZoneOffset.UTC)
      val now = rawNow.withNano((rawNow.getNano / 1000) * 1000)
      val initial = DatabaseState(7L, None, now.minusHours(2),
        None, None, None, None, 0, 0)
      Files.writeString(connectionIdPath, "700", StandardCharsets.UTF_8)
      val root = new JsonObject
      root.addProperty("writerEpoch", initial.writerEpoch)
      root.add("writerOwner", com.google.gson.JsonNull.INSTANCE)
      root.addProperty("leaseUntil", initial.leaseUntil.toString)
      root.add("lastOperation", com.google.gson.JsonNull.INSTANCE)
      root.add("lastFingerprint", com.google.gson.JsonNull.INSTANCE)
      root.add("bindingState", com.google.gson.JsonNull.INSTANCE)
      Vector("intentBootstrap", "intentPrepare", "intentCertify", "intentBinding", "intentProvisioning",
        "intentSource", "intentReceipt", "intentStage", "intentAck").foreach(name => root.add(name, com.google.gson.JsonNull.INSTANCE))
      root.addProperty("writerUpdateCount", initial.writerUpdateCount)
      root.addProperty("commitCount", initial.commitCount)
      Files.writeString(path, new GsonBuilder().disableHtmlEscaping().create().toJson(root), StandardCharsets.UTF_8)

