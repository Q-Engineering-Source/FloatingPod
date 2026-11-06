package com.wi.floatingpod.storage.mysql

import com.google.gson.{GsonBuilder, JsonArray, JsonObject}
import com.wi.floatingpod.config.{MySqlRuntimeMetadata, SourceLimitProfile}
import com.wi.floatingpod.server.WindowsWorldProvisioner
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.sql.{SQLNonTransientConnectionException, Types}
import java.time.Duration
import java.util.{HexFormat, Locale, UUID}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.Assertions.*

/** 只验证独立静态合同解析；不读取数据库、不签发 qualification 或 Ready。 */
class MySqlFormat3RuntimeContractLoaderTest:
  @TempDir var temporary: Path = _

  private val server = UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d")
  private val provisioning = UUID.fromString("16fb58b8-a867-43b7-8e8e-03554578bf14")
  private val binding = UUID.fromString("e83a7eb6-c7fd-4094-9751-029bdef5c0cf")
  private val sourceCaptureSha = "A" * 64
  private val ddlSha = "B" * 64
  private val baselineSha = "C" * 64
  private val definer = "fp_audit@localhost"
  private val schema = "fp_test_20261006_sec06"
  private val profile = SourceLimitProfile.of(1024, 2048, 4096, 32, 100, 65536, 8192, 16384)
  private val expectedIdentity = MySqlApprovedBaselineIdentity(
    MySqlApprovedBaselineLoader.ApprovedFormat, 3, MySqlAuditCoverage.Format3, server, "9.7.2",
    schema, provisioning, binding, ddlSha, definer, sourceCaptureSha)
  private val plan = new WindowsWorldProvisioner.Plan(
    UUID.fromString("d21a167d-7d47-4c43-a48f-a61d45de2320"),
    UUID.fromString("e7f436ea-0775-4f6c-8139-9166b164cfec"),
    UUID.fromString("02691cb7-c802-4210-8bd5-27ccf7a91f7e"),
    UUID.fromString("9752345c-3082-40b5-b29c-e2bc9ef36fb5"),
    binding, 2, server, schema, 3, ddlSha, provisioning)

  private val gson = new GsonBuilder().disableHtmlEscaping().create()
  private def sha(bytes: Array[Byte]): String =
    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)).toUpperCase(Locale.ROOT)
  private def hex(bytes: Array[Byte]): String = HexFormat.of().formatHex(bytes).toUpperCase(Locale.ROOT)

  private def column(label: String): JsonObject =
    val value = new JsonObject
    value.addProperty("label", label)
    value.addProperty("jdbcType", Types.VARCHAR)
    value.addProperty("typeName", "VARCHAR")
    value.addProperty("precision", 256)
    value.addProperty("scale", 0)
    value.addProperty("nullable", 1)
    value.addProperty("signed", false)
    value

  private def textRow(values: String*): JsonArray =
    val row = new JsonArray
    values.foreach { value =>
      val cell = new JsonObject
      cell.addProperty("tag", "TEXT")
      cell.addProperty("value", value)
      row.add(cell)
    }
    row

  private def privilegeSet(id: String, labels: Vector[String], rows: Vector[Vector[String]]): JsonObject =
    val set = new JsonObject
    set.addProperty("id", id)
    val columns = new JsonArray
    labels.foreach(label => columns.add(column(label)))
    set.add("columns", columns)
    val encodedRows = new JsonArray
    rows.foreach(values => encodedRows.add(textRow(values*)))
    set.add("rows", encodedRows)
    set

  private def contractObject(unknownField: Boolean = false): JsonObject =
    val value = new JsonObject
    value.addProperty("format", MySqlFormat3RuntimeContractLoader.Format)
    value.addProperty("schemaFormat", 3)
    value.addProperty("coverage", MySqlAuditCoverage.Format3)
    value.addProperty("approvedBaselineSha256", baselineSha)
    value.addProperty("serverUuid", server.toString)
    value.addProperty("serverVersion", "9.7.2")
    value.addProperty("schema", schema)
    value.addProperty("effectiveUser", "fp_runtime@localhost")
    value.addProperty("connectedUser", "fp_runtime@localhost")
    value.addProperty("callerRoles", "NONE")
    value.addProperty("provisioningUuid", provisioning.toString)
    value.addProperty("bindingUuid", binding.toString)
    value.addProperty("ddlSha256", ddlSha)
    value.addProperty("auditDefiner", definer)
    value.addProperty("sourceCaptureSha256", sourceCaptureSha)
    value.addProperty("auditDefinerRoles", "NONE")
    val grants = new JsonArray
    grants.add("GRANT USAGE ON *.* TO 'fp_runtime'@'localhost'")
    value.add("callerGrants", grants)
    value.addProperty("physicalFormat", 2)
    value.addProperty("importState", "NONE")
    value.addProperty("maxReceiptAgeNanos", 10000000000L)
    value.addProperty("sourceProvenanceKind", "FRESH_PLAN")
    val provenance = MySqlFormat3FreshPlanProvenance.canonicalBytes(plan)
    value.addProperty("sourceProvenanceHex", hex(provenance))
    value.addProperty("sourceProvenanceSha256", sha(provenance))
    value.addProperty("limitProfileEncoding", SourceLimitProfile.ENCODING)
    value.addProperty("limitProfileHex", hex(profile.canonicalBytes()))
    value.addProperty("limitProfileSha256", profile.sha256Hex().toUpperCase(Locale.ROOT))

    val privileges = new JsonArray
    val tableLabels = Vector("GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE")
    val columnLabels = Vector("GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "PRIVILEGE_TYPE", "IS_GRANTABLE")
    val schemaLabels = Vector("GRANTEE", "TABLE_CATALOG", "TABLE_SCHEMA", "PRIVILEGE_TYPE", "IS_GRANTABLE")
    val userLabels = Vector("GRANTEE", "TABLE_CATALOG", "PRIVILEGE_TYPE", "IS_GRANTABLE")
    privileges.add(privilegeSet("RS15_TABLE_PRIVILEGES", tableLabels,
      Vector(Vector("'fp_audit'@'localhost'", "def", schema, "fp_schema_manifest", "SELECT", "NO"))))
    privileges.add(privilegeSet("RS16_COLUMN_PRIVILEGES", columnLabels, Vector.empty))
    privileges.add(privilegeSet("RS17_SCHEMA_PRIVILEGES", schemaLabels,
      Vector(Vector("'fp_audit'@'localhost'", "def", schema, "SELECT", "NO"),
        Vector("'fp_audit'@'localhost'", "def", schema, "TRIGGER", "NO"))))
    privileges.add(privilegeSet("RS18_USER_PRIVILEGES", userLabels,
      Vector(Vector("'fp_audit'@'localhost'", "def", "USAGE", "NO"))))
    value.add("definerPrivileges", privileges)
    if unknownField then value.addProperty("configurationSha256", "D" * 64)
    value

  private def metadata(contractBytes: Array[Byte], effectiveUser: String = "fp_runtime@localhost"): MySqlRuntimeMetadata =
    val contractPath = temporary.resolve("format3-runtime-contract.json").toAbsolutePath
    Files.write(contractPath, contractBytes)
    val root = temporary.toAbsolutePath.toString.replace('\\', '/')
    val text = Vector(
      "backend=mysql",
      "host=127.0.0.1",
      "port=33317",
      "schema=" + schema,
      "expectedServerUuid=" + server,
      "expectedBindingUuid=" + binding,
      "expectedEffectiveUser=" + effectiveUser,
      "username=fp_runtime",
      "pkcs12TrustStorePath=" + root + "/trust.p12",
      "credentialSourceRef=file:" + root + "/credential.secret",
      "trustSecretSourceRef=file:" + root + "/trust.secret",
      "connectTimeoutMillis=3000",
      "socketTimeoutMillis=10000",
      "activeLimit=32",
      "innodbFlushLogAtTrxCommit=1",
      "syncBinlog=1",
      "approvedBaselinePath=" + root + "/approved-static-baseline.json",
      "approvedBaselineSha256=" + baselineSha,
      "approvedBaselineFormat=" + MySqlApprovedBaselineLoader.ApprovedFormat,
      "approvedSchemaFormat=3",
      "approvedCoverage=" + MySqlAuditCoverage.Format3,
      "expectedServerVersion=9.7.2",
      "expectedProvisioningUuid=" + provisioning,
      "approvedDdlSha256=" + ddlSha,
      "expectedAuditDefiner=" + definer,
      "approvedSourceCaptureSha256=" + sourceCaptureSha,
      "format3RuntimeContractPath=" + contractPath.toString.replace('\\', '/'),
      "format3RuntimeContractSha256=" + sha(contractBytes),
      ""
    ).mkString("\n")
    MySqlRuntimeMetadata.fromBytes(text.getBytes(StandardCharsets.UTF_8))

  private def load(bytes: Array[Byte]): MySqlFormat3RuntimeContract =
    MySqlFormat3RuntimeContractLoader.loadExpected(metadata(bytes), expectedIdentity, baselineSha)

  @Test def independentlyPinnedContractLoadsFreshPlanAndExactTypedInputs(): Unit =
    val bytes = gson.toJson(contractObject()).getBytes(StandardCharsets.UTF_8)
    val loaded = load(bytes)
    assertEquals(expectedIdentity, loaded.identity)
    assertEquals(baselineSha, loaded.approvedBaselineSha256)
    assertEquals("FRESH_PLAN", loaded.sourceProvenanceKind)
    assertEquals(profile, loaded.limitProfile)
    assertTrue(java.util.Arrays.equals(MySqlFormat3FreshPlanProvenance.canonicalBytes(plan), loaded.sourceProvenanceBytes))
    loaded.requireFreshPlan(plan)
    assertThrows(classOf[SQLNonTransientConnectionException], () =>
      loaded.requireFreshPlan(new WindowsWorldProvisioner.Plan(plan.creationOperationUuid(),
        plan.bootstrapOperationUuid(), plan.prepareWriteUuid(), plan.certifyWriteUuid(), binding, 2, server,
        schema, 3, "E" * 64, provisioning)))
    val canonicalConfigurationSha = Array.tabulate[Byte](32)(index => index.toByte)
    val audit = loaded.auditInputs(canonicalConfigurationSha)
    assertTrue(java.util.Arrays.equals(canonicalConfigurationSha, audit.configurationSha256.bytes))
    assertEquals(MySqlAuditCoverage.Format3PrivilegeResultSetIds,
      audit.definerPrivileges.map(_.id))
    loaded.requirePolicy(MySqlRuntimeAuditPolicy("NONE", "NONE", MySqlExpectedNoImport,
      MySqlExpectedPhysicalFormat.Format2, Duration.ofNanos(10000000000L)))
    assertThrows(classOf[SQLNonTransientConnectionException], () =>
      loaded.requirePolicy(MySqlRuntimeAuditPolicy("OTHER", "NONE", MySqlExpectedNoImport,
        MySqlExpectedPhysicalFormat.Format2, Duration.ofNanos(10000000000L))))
    assertFalse(loaded.toString.contains(schema))

  @Test def contractRejectsWrongRawShaIdentityAndUnpermittedSelfReferenceField(): Unit =
    val good = gson.toJson(contractObject()).getBytes(StandardCharsets.UTF_8)
    val metadataWithWrongFileSha = metadata(good)
    val path = temporary.resolve("format3-runtime-contract.json")
    Files.write(path, good)
    // Change content after metadata parsed so the raw SHA pin is exercised by the actual file reader.
    Files.write(path, gson.toJson(contractObject(unknownField = true)).getBytes(StandardCharsets.UTF_8))
    assertThrows(classOf[SQLNonTransientConnectionException], () =>
      MySqlFormat3RuntimeContractLoader.loadExpected(metadataWithWrongFileSha, expectedIdentity, baselineSha))

    val unknown = gson.toJson(contractObject(unknownField = true)).getBytes(StandardCharsets.UTF_8)
    assertThrows(classOf[SQLNonTransientConnectionException], () => load(unknown))

    val otherBinding = UUID.fromString("07d3827c-0370-457d-bf5e-96c8fd8b4def")
    val otherIdentity = MySqlApprovedBaselineIdentity(
      MySqlApprovedBaselineLoader.ApprovedFormat, 3, MySqlAuditCoverage.Format3, server, "9.7.2",
      schema, provisioning, otherBinding, ddlSha, definer, sourceCaptureSha)
    assertThrows(classOf[SQLNonTransientConnectionException], () =>
      MySqlFormat3RuntimeContractLoader.loadExpected(metadata(good), otherIdentity, baselineSha))
    assertThrows(classOf[SQLNonTransientConnectionException], () =>
      MySqlFormat3RuntimeContractLoader.loadExpected(metadata(good, "other_runtime@localhost"),
        expectedIdentity, baselineSha))
    assertThrows(classOf[SQLNonTransientConnectionException], () =>
      MySqlFormat3RuntimeContractLoader.loadExpected(metadata(good), expectedIdentity, "D" * 64))

  @Test def strictParserRejectsDuplicateRootPropertyWithCorrectRawFileSha(): Unit =
    val valid = gson.toJson(contractObject())
    val needle = "\"format\":\"" + MySqlFormat3RuntimeContractLoader.Format + "\""
    val duplicate = valid.replace(needle, needle + ",\"format\":\"" + MySqlFormat3RuntimeContractLoader.Format + "\"")
    assertNotEquals(valid, duplicate)
    assertThrows(classOf[SQLNonTransientConnectionException], () =>
      load(duplicate.getBytes(StandardCharsets.UTF_8)))

  @Test def strictParserRejectsLegacyRawControlCharacterWithItsActualFileHash(): Unit =
    val valid = gson.toJson(contractObject())
    val malformed = valid.replace("fp_runtime@localhost", "fp_runtime@\nlocalhost")
    assertNotEquals(valid, malformed)
    assertThrows(classOf[SQLNonTransientConnectionException], () =>
      load(malformed.getBytes(StandardCharsets.UTF_8)))


  private val v2Profile=SourceLimitProfile.of(1024,2048,4096,32,100,8L*1024*1024,8192,16384)
  private def v2Root(): MySqlFormat3ReadBudget = new MySqlFormat3ReadBudget(v2Profile,v2Profile.maxSqlPacketBytes())
  private def realSyntheticB(): MySqlApprovedStaticBaseline =
    // Existing independent synthetic DATA fixture writes a pack, then uses the real baseline loader.
    MySqlOrdinaryCertifiedHandoffFixture.loadIndependentStaticBaseline(temporary.resolve("v2-baseline").toAbsolutePath)
  private def v2Contract(b: MySqlApprovedStaticBaseline): JsonObject =
    val i=b.identity
    val grantee="'"+i.auditDefiner.split("@",2)(0)+"'@'"+i.auditDefiner.split("@",2)(1)+"'"
    val text=gson.toJson(contractObject()).replace(schema,i.schema).replace("'fp_audit'@'localhost'",grantee).replace(definer,i.auditDefiner)
    val c=com.google.gson.JsonParser.parseString(text).getAsJsonObject
    Map("format"->"FP_MYSQL_FORMAT3_RUNTIME_CONTRACT_V2","format3ManifestConfigurationEncoding"->MySqlRuntimeMetadata.MANIFEST_V2,
      "approvedBaselineSha256"->b.fileSha256.toUpperCase(Locale.ROOT),"serverUuid"->i.serverUuid.toString,"serverVersion"->i.serverVersion,
      "schema"->i.schema,"provisioningUuid"->i.provisioningUuid.toString,"bindingUuid"->i.bindingUuid.toString,
      "ddlSha256"->i.ddlSha256,"auditDefiner"->i.auditDefiner,"sourceCaptureSha256"->i.sourceCaptureSha256,
      "limitProfileHex"->hex(v2Profile.canonicalBytes()),"limitProfileSha256"->hex(v2Profile.sha256())).foreach((k,v)=>c.addProperty(k,v))
    c
  private def v2Metadata(b: MySqlApprovedStaticBaseline,c: JsonObject,edits: Map[String,String]=Map.empty): MySqlRuntimeMetadata.FullV2 =
    val i=b.identity;val path=temporary.resolve("v2-contract.json").toAbsolutePath
    val bytes=gson.toJson(c).getBytes(StandardCharsets.UTF_8);Files.write(path,bytes)
    val fields=Map("backend"->"mysql","host"->"127.0.0.1","port"->"33317","schema"->i.schema,
      "expectedServerUuid"->i.serverUuid.toString,"expectedBindingUuid"->i.bindingUuid.toString,
      "expectedEffectiveUser"->"fp_runtime@localhost","username"->"fp_runtime",
      "pkcs12TrustStorePath"->temporary.resolve("trust.p12").toAbsolutePath.toString,
      "credentialSourceRef"->("file:"+temporary.resolve("credential.secret").toAbsolutePath),
      "trustSecretSourceRef"->("file:"+temporary.resolve("trust.secret").toAbsolutePath),
      "connectTimeoutMillis"->"3000","socketTimeoutMillis"->"10000","activeLimit"->"32","innodbFlushLogAtTrxCommit"->"1","syncBinlog"->"1",
      "approvedBaselinePath"->temporary.resolve("v2-baseline/approved-static-baseline-format3-ordinary-synthetic.json").toAbsolutePath.toString,
      "approvedBaselineSha256"->b.fileSha256,"approvedBaselineFormat"->i.format,"approvedSchemaFormat"->"3","approvedCoverage"->i.coverage,
      "expectedServerVersion"->i.serverVersion,"expectedProvisioningUuid"->i.provisioningUuid.toString,"approvedDdlSha256"->i.ddlSha256,
      "expectedAuditDefiner"->i.auditDefiner,"approvedSourceCaptureSha256"->i.sourceCaptureSha256,
      "format3RuntimeContractPath"->path.toString,"format3RuntimeContractSha256"->sha(bytes),
      "format3ManifestConfigurationEncoding"->MySqlRuntimeMetadata.MANIFEST_V2)++edits
    val properties=new java.util.Properties;fields.foreach((k,v)=>properties.setProperty(k,v))
    val writer=new java.io.StringWriter;properties.store(writer,null)
    val file=temporary.resolve("v2-metadata.properties");Files.writeString(file,writer.toString,StandardCharsets.UTF_8)
    MySqlRuntimeMetadata.loadV2(file)

  @Test def fullV2LoadsActualBAndKWithRootOwnedTypedLifetime(): Unit =
    val b=realSyntheticB();val c=v2Contract(b);val metadata=v2Metadata(b,c);val root=v2Root();val other=v2Root()
    try
      val loaded=MySqlFormat3RuntimeContractLoader.loadV2(metadata,b,root)
      assertEquals(b.identity,loaded.identity);assertEquals(metadata.format3RuntimeContract().fileSha256(),loaded.fileSha256)
      assertTrue(root.accountedMemoryBytes>96L)
      loaded.requirePolicy(MySqlRuntimeAuditPolicy("NONE","NONE",MySqlExpectedNoImport,MySqlExpectedPhysicalFormat.Format2,Duration.ofSeconds(10)))
      assertThrows(classOf[Exception],()=>loaded.semanticFor(other))
      assertThrows(classOf[Exception],()=>loaded.requirePolicy(MySqlRuntimeAuditPolicy("OTHER","NONE",MySqlExpectedNoImport,MySqlExpectedPhysicalFormat.Format2,Duration.ofSeconds(10))))
      loaded.close();loaded.close();assertEquals(0L,root.accountedMemoryBytes)
      assertThrows(classOf[Exception],()=>loaded.semanticFor(root))
    finally other.closeRootReadBudget();root.closeRootReadBudget()
    val retired=v2Root();val owned=MySqlFormat3RuntimeContractLoader.loadV2(metadata,b,retired)
    retired.closeRootReadBudget()
    assertThrows(classOf[Exception],()=>owned.semanticFor(retired))
    assertThrows(classOf[IllegalStateException],()=>owned.close())

  @Test def equalSemanticCDoesNotApproveAnyChangedBRawKOrSourcePin(): Unit =
    val b=realSyntheticB();val c=v2Contract(b);val metadata=v2Metadata(b,c);val root=v2Root()
    val loaded=MySqlFormat3RuntimeContractLoader.loadV2(metadata,b,root)
    val policy=MySqlRuntimeAuditPolicy("NONE","NONE",MySqlExpectedNoImport,MySqlExpectedPhysicalFormat.Format2,Duration.ofSeconds(10))
    def semanticC(m: MySqlRuntimeMetadata.FullV2): String =
      val value=MySqlManifestConfigurationIdentity.fromValidatedSemantics(m.deploymentSemantics(),loaded.semanticFor(root),policy,root)
      try value.hex finally value.close()
    try
      val original=semanticC(metadata)
      Vector("approvedBaselineSha256","approvedSourceCaptureSha256","format3RuntimeContractSha256").foreach { key=>
        val changed=v2Metadata(b,c,Map(key->("E"*64)))
        assertEquals(original,semanticC(changed))
        val before=root.accountedMemoryBytes
        assertThrows(classOf[Exception],()=>MySqlFormat3RuntimeContractLoader.loadV2(changed,b,root))
        assertEquals(before,root.accountedMemoryBytes)
      }
      val bPath=metadata.baseline().absolutePath()
      assertThrows(classOf[Exception],()=>MySqlApprovedBaselineLoader.load(bPath,"E"*64,b.identity))
      assertThrows(classOf[Exception],()=>MySqlApprovedBaselineLoader.load(bPath,b.fileSha256,b.identity.copy(sourceCaptureSha256="E"*64)))
    finally loaded.close();assertEquals(0L,root.accountedMemoryBytes);root.closeRootReadBudget()

  @Test def fullV2RejectsWrongSelectorPinsMalformedBytesAndRootWithoutLeaking(): Unit =
    val b=realSyntheticB();val root=v2Root()
    try
      Vector[JsonObject=>Unit](
        c=>c.addProperty("format",MySqlFormat3RuntimeContractLoader.Format),
        c=>{c.remove("format3ManifestConfigurationEncoding");()},
        c=>c.addProperty("sourceCaptureSha256","E"*64),
        c=>c.addProperty("approvedBaselineSha256","E"*64),
        c=>c.addProperty("serverVersion","9.7.1"),
        c=>c.addProperty("format3ManifestConfigurationEncoding","OLD")).foreach { change=>
        val c=v2Contract(b);change(c);val m=v2Metadata(b,c)
        assertThrows(classOf[Exception],()=>MySqlFormat3RuntimeContractLoader.loadV2(m,b,root))
        assertEquals(0L,root.accountedMemoryBytes)
      }
      val directory=Files.createDirectory(temporary.resolve("not-a-K-file"))
      val directoryMetadata=v2Metadata(b,v2Contract(b),Map("format3RuntimeContractPath"->directory.toString))
      assertThrows(classOf[Exception],()=>MySqlFormat3RuntimeContractLoader.loadV2(directoryMetadata,b,root));assertEquals(0L,root.accountedMemoryBytes)
      val missingPath=temporary.resolve("missing-K.json")
      val missing=v2Metadata(b,v2Contract(b),Map("format3RuntimeContractPath"->missingPath.toString))
      val missingFailure=assertThrows(classOf[java.nio.file.NoSuchFileException],()=>MySqlFormat3RuntimeContractLoader.loadV2(missing,b,root))
      assertTrue(missingFailure.getStackTrace.nonEmpty);assertEquals(0L,root.accountedMemoryBytes)
      val m=v2Metadata(b,v2Contract(b));Files.write(m.format3RuntimeContract().absolutePath(),Array(0xC3.toByte,0x28.toByte))
      assertThrows(classOf[Exception],()=>MySqlFormat3RuntimeContractLoader.loadV2(m,b,root));assertEquals(0L,root.accountedMemoryBytes)
      val scope=root.newCanonicalScope()
      try assertThrows(classOf[Exception],()=>MySqlFormat3RuntimeContractLoader.loadV2(m,b,scope))
      finally scope.closeScopedReadBudget()
      val tinyProfile=SourceLimitProfile.of(1024,2048,4096,32,100,1024,8192,16384)
      val tiny=new MySqlFormat3ReadBudget(tinyProfile,tinyProfile.maxSqlPacketBytes())
      try
        assertThrows(classOf[Exception],()=>MySqlFormat3RuntimeContractLoader.loadV2(m,b,tiny));assertEquals(0L,tiny.accountedMemoryBytes)
      finally tiny.closeRootReadBudget()
      root.closeRootReadBudget();assertThrows(classOf[Exception],()=>MySqlFormat3RuntimeContractLoader.loadV2(m,b,root))
    finally root.closeRootReadBudget()

  @Test def v3ProjectionHasClosedSemanticKeysAndNeverFallsBackToV2(): Unit =
    val budget=CallerPrivilegesV3TestData.root()
    try
      val c=contractObject()
      c.remove("approvedBaselineSha256");c.remove("sourceCaptureSha256");c.remove("callerGrants")
      c.addProperty("format","FP_MYSQL_FORMAT3_RUNTIME_CONTRACT_V3")
      c.addProperty("format3ManifestConfigurationEncoding",MySqlRuntimeMetadata.MANIFEST_V3)
      c.addProperty("schema",MySqlRuntimeCallerPrivileges.Schema)
      c.addProperty("effectiveUser","fp_f3_runtime@localhost");c.addProperty("callerRoles","NONE");c.addProperty("auditDefinerRoles","NONE")
      c.addProperty("limitProfileHex",hex(budget.profile.canonicalBytes()))
      c.addProperty("limitProfileSha256",hex(budget.profile.sha256()))
      c.add("callerPrivileges",CallerPrivilegesV3TestData.json)
      val text=gson.toJson(c)
      val semantic=MySqlFormat3RuntimeContractLoader.parseManifestV3Projection(text,budget)
      assertEquals(MySqlRuntimeMetadata.MANIFEST_V3,semantic.fields.selector)
      assertEquals(58,semantic.fields.callerPrivileges.grants.size)
      assertTrue(semantic.fields.callerGrants.isEmpty)
      semantic.close()
      assertThrows(classOf[SQLNonTransientConnectionException],()=>MySqlFormat3RuntimeContractLoader.parseManifestV2Projection(text,budget))
      c.add("callerGrants",new JsonArray)
      assertThrows(classOf[SQLNonTransientConnectionException],()=>MySqlFormat3RuntimeContractLoader.parseManifestV3Projection(gson.toJson(c),budget))
      assertEquals(0L,budget.accountedMemoryBytes)
    finally budget.closeRootReadBudget()
