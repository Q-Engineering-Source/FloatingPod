package com.wi.floatingpod.storage.mysql

import com.google.gson.{GsonBuilder, JsonArray, JsonElement, JsonObject}
import com.wi.floatingpod.config.{MySqlRuntimeMetadata, SourceLimitProfile}
import com.wi.floatingpod.server.WindowsB1SourceAnchorStore
import java.io.{ByteArrayOutputStream, DataOutputStream, IOException}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Path
import java.security.MessageDigest
import java.sql.Types
import java.time.Duration
import java.util.{HexFormat, UUID}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.Assertions.*

/** Candidate tests; not executed. Synthetic structural DATA is deliberately not admission. */
class MySqlConfigurationIdentityTest:
  @TempDir var directory: Path = _
  private val gson = new GsonBuilder().disableHtmlEscaping().create()
  private val metadataNames = "backend host port schema expectedServerUuid expectedBindingUuid expectedEffectiveUser username pkcs12TrustStorePath credentialSourceRef trustSecretSourceRef connectTimeoutMillis socketTimeoutMillis activeLimit innodbFlushLogAtTrxCommit syncBinlog approvedBaselinePath approvedBaselineFormat approvedSchemaFormat approvedCoverage expectedServerVersion expectedProvisioningUuid approvedDdlSha256 expectedAuditDefiner format3RuntimeContractPath format3ManifestConfigurationEncoding".split(" ").toVector
  private val contractNames = "format format3ManifestConfigurationEncoding schemaFormat coverage serverUuid serverVersion schema effectiveUser connectedUser callerRoles provisioningUuid bindingUuid ddlSha256 auditDefiner auditDefinerRoles callerGrants physicalFormat importState maxReceiptAgeNanos sourceProvenanceKind sourceProvenanceHex sourceProvenanceSha256 limitProfileEncoding limitProfileHex limitProfileSha256 definerPrivileges".split(" ").toVector
  private def profile(retained: Long = 8L * 1024 * 1024): SourceLimitProfile =
    SourceLimitProfile.of(1024, 2048, 4096, 32, 100, retained, 8192, 16384)
  private def root(retained: Long = 8L * 1024 * 1024): MySqlFormat3ReadBudget =
    val p = profile(retained)
    new MySqlFormat3ReadBudget(p, p.maxSqlPacketBytes())
  private def hex(bytes: Array[Byte]): String = HexFormat.of().withUpperCase().formatHex(bytes)

  /** Independent fixture renderer: sorted fixed ASCII keys, Gson primitives, no production writer. */
  private def render(element: JsonElement): String =
    if element.isJsonObject then
      val obj = element.getAsJsonObject
      import scala.jdk.CollectionConverters.*
      obj.keySet().asScala.toVector.sorted.map(k => gson.toJson(k) + ":" + render(obj.get(k))).mkString("{", ",", "}")
    else if element.isJsonArray then
      import scala.jdk.CollectionConverters.*
      element.getAsJsonArray.iterator().asScala.map(render).mkString("[", ",", "]")
    else gson.toJson(element)

  private def fixture(): JsonObject =
    val result = new JsonObject
    result.addProperty("format", MySqlManifestConfigurationIdentity.RecipeFormat)
    val metadata = new JsonObject
    metadataNames.foreach(k => metadata.addProperty(k, "fixture"))
    metadata.addProperty("format3ManifestConfigurationEncoding", MySqlManifestConfigurationIdentity.Encoding)
    result.add("metadata", metadata)
    val contract = new JsonObject
    contractNames.foreach(k => contract.addProperty(k, "fixture"))
    contract.addProperty("format", MySqlManifestConfigurationIdentity.ContractFormat)
    contract.addProperty("format3ManifestConfigurationEncoding", MySqlManifestConfigurationIdentity.Encoding)
    contract.addProperty("schemaFormat", 3); contract.addProperty("physicalFormat", 2)
    contract.addProperty("maxReceiptAgeNanos", 1000000000L)
    Vector("ddlSha256", "sourceProvenanceSha256", "limitProfileSha256").foreach(k => contract.addProperty(k, "A" * 64))
    contract.addProperty("sourceProvenanceHex", "4142")
    contract.addProperty("limitProfileHex", hex(profile().canonicalBytes()))
    contract.addProperty("limitProfileEncoding", SourceLimitProfile.ENCODING)
    val grants = new JsonArray; grants.add("grant-A"); grants.add("grant-B")
    contract.add("callerGrants", grants)
    val sets = new JsonArray
    MySqlAuditCoverage.Format3PrivilegeResultSetIds.foreach { id =>
      val set = new JsonObject; set.addProperty("id", id)
      val columns = new JsonArray
      val column = new JsonObject
      column.addProperty("label", "label"); column.addProperty("jdbcType", Types.VARCHAR)
      column.addProperty("typeName", "VARCHAR"); column.addProperty("precision", 256)
      column.addProperty("scale", 0); column.addProperty("nullable", 1); column.addProperty("signed", false)
      columns.add(column); set.add("columns", columns)
      val rows = new JsonArray; val row = new JsonArray; val cell = new JsonObject
      cell.addProperty("tag", "TEXT"); cell.addProperty("value", "value")
      row.add(cell); rows.add(row); set.add("rows", rows); sets.add(set)
    }
    contract.add("definerPrivileges", sets); result.add("contract", contract)
    result

  private def inspected(value: JsonObject, budget: MySqlFormat3ReadBudget): String =
    val result = MySqlConfigurationRecipeCodec.inspect(render(value).getBytes(UTF_8), budget)
    try result.candidate.hex finally result.close()

  @Test def structuralDigestUsesIndependentExactDomainPreimageAndReturnsAllDebt(): Unit =
    val budget = root()
    try
      val value = fixture()
      val bytes = render(value).getBytes(UTF_8)
      val preimage = new ByteArrayOutputStream
      val output = new DataOutputStream(preimage)
      val tag = "FP_MYSQL_FORMAT3_MANIFEST_CONFIGURATION_BE_V2".getBytes(UTF_8)
      output.writeInt(tag.length); output.write(tag); output.writeInt(bytes.length); output.write(bytes)
      assertEquals(hex(MessageDigest.getInstance("SHA-256").digest(preimage.toByteArray)), inspected(value, budget))
      assertEquals(0L, budget.accountedMemoryBytes)
      assertTrue(budget.successfulReserveHighWaterBytes > 0L)
    finally budget.closeRootReadBudget()

  @Test def everyScalarAndOrderedGrantPrivilegeValueAffectsStructuralDATA(): Unit =
    val budget = root()
    try
      val original = fixture()
      val expected = inspected(original, budget)
      metadataNames.filterNot(_ == "format3ManifestConfigurationEncoding").foreach { name =>
        val changed = original.deepCopy(); changed.getAsJsonObject("metadata").addProperty(name, "changed")
        assertNotEquals(expected, inspected(changed, budget), name)
      }
      contractNames.filterNot(Set("format", "format3ManifestConfigurationEncoding", "callerGrants", "definerPrivileges", "limitProfileEncoding")).foreach { name =>
        val changed = original.deepCopy(); val c = changed.getAsJsonObject("contract")
        if name == "schemaFormat" || name == "physicalFormat" then c.addProperty(name, 4)
        else if name == "maxReceiptAgeNanos" then c.addProperty(name, 2000000000L)
        else if name.endsWith("Sha256") || name.endsWith("Hex") then c.addProperty(name, "B" * c.get(name).getAsString.length)
        else c.addProperty(name, "changed")
        assertNotEquals(expected, inspected(changed, budget), name)
      }
      val changed = original.deepCopy()
      changed.getAsJsonObject("contract").getAsJsonArray("callerGrants").set(0, new com.google.gson.JsonPrimitive("grant-B"))
      assertNotEquals(expected, inspected(changed, budget))
      val nested = original.deepCopy()
      nested.getAsJsonObject("contract").getAsJsonArray("definerPrivileges").get(0).getAsJsonObject
        .getAsJsonArray("rows").get(0).getAsJsonArray.get(0).getAsJsonObject.addProperty("value", "changed")
      assertNotEquals(expected, inspected(nested, budget))
      assertEquals(0L, budget.accountedMemoryBytes)
    finally budget.closeRootReadBudget()

  @Test def oldEncodingClosedKeysNoncanonicalUtf8AndQuotaReject(): Unit =
    val budget = root()
    try
      val original = fixture()
      val old = original.deepCopy(); old.addProperty("format", "FP_MYSQL_FORMAT3_CONFIGURATION_RECIPE_V1")
      assertThrows(classOf[Exception], () => inspected(old, budget))
      Vector("approvedBaselineSha256", "approvedSourceCaptureSha256", "format3RuntimeContractSha256").foreach { name =>
        val extra = original.deepCopy(); extra.getAsJsonObject("metadata").addProperty(name, "A" * 64)
        assertThrows(classOf[Exception], () => inspected(extra, budget))
      }
      val missing = original.deepCopy(); missing.getAsJsonObject("metadata").remove("host")
      assertThrows(classOf[Exception], () => inspected(missing, budget))
      val bytes = render(original)
      Vector(" " + bytes, bytes + "\n", bytes.replace("\"host\":\"fixture\"", "\"host\":\"fixture\",\"host\":\"fixture\""),
        bytes.replace("fixture", "\\u0066ixture")).foreach { invalid =>
        assertThrows(classOf[Exception], () => MySqlConfigurationRecipeCodec.inspect(invalid.getBytes(UTF_8), budget))
      }
      assertThrows(classOf[Exception], () => MySqlConfigurationRecipeCodec.inspect(Array(0xc0.toByte, 0xaf.toByte), budget))
      assertEquals(0L, budget.accountedMemoryBytes)
    finally budget.closeRootReadBudget()
    val tiny = root(1024)
    try
      assertThrows(classOf[Exception], () => inspected(fixture(), tiny))
      assertEquals(0L, tiny.accountedMemoryBytes)
    finally tiny.closeRootReadBudget()

  @Test def unexpectedWriterCauseAndCleanupSuppressedAreNotReplaced(): Unit =
    val budget = root()
    val primary = new IOException("injected actual writer failure")
    try
      val observed = assertThrows(classOf[IOException], () =>
        MySqlManifestConfigurationIdentity.digest(budget, _ => throw primary))
      assertSame(primary, observed)
      assertEquals(0L, budget.accountedMemoryBytes)
    finally budget.closeRootReadBudget()
    val closing = root()
    val failure = new IOException("injected writer failure with root retirement")
    val observed = assertThrows(classOf[IOException], () =>
      MySqlManifestConfigurationIdentity.temporary(closing, 256L) {
        closing.closeRootReadBudget(); throw failure
      })
    assertSame(failure, observed)
    assertTrue(observed.getSuppressed.nonEmpty)

  @Test def canonicalUnicodeAndActualSectionCellBounds(): Unit =
    val output = new ByteArrayOutputStream
    MySqlConfigurationRecipeCodec.quoted(output, "三😀\n\\\"")
    assertEquals("\"三😀\\u000A\\\\\\\"\"", output.toString(UTF_8))
    assertThrows(classOf[Exception], () => MySqlConfigurationRecipeCodec.quoted(output, "\ud800"))
    val budget = root(32L * 1024 * 1024)
    try
      val unicode = fixture()
      unicode.getAsJsonObject("contract").addProperty("connectedUser", "三😀")
      assertNotNull(inspected(unicode, budget))
      val largeMetadata = fixture()
      largeMetadata.getAsJsonObject("metadata").addProperty("host", "a" * 65536)
      assertThrows(classOf[Exception], () => inspected(largeMetadata, budget))
      val largeCell = fixture()
      largeCell.getAsJsonObject("contract").addProperty("connectedUser", "a" * (1048576 + 1))
      assertThrows(classOf[Exception], () => inspected(largeCell, budget))
      assertEquals(0L, budget.accountedMemoryBytes)
    finally budget.closeRootReadBudget()

  /** Real existing pure A encoder: no native IO, file creation or database. */
  @Test def completeAnchorStillCommitsAllThreeDerivedPins(): Unit =
    def metadata(pin: String, sourcePin: String, contractPin: String, active: Int = 32): MySqlRuntimeMetadata =
      val absolute = directory.toAbsolutePath.toString.replace('\\', '/')
      val fields = Map("backend" -> "mysql", "host" -> "127.0.0.1", "port" -> "33317", "schema" -> "fp_test",
        "expectedServerUuid" -> "11111111-1111-4111-8111-111111111111", "expectedBindingUuid" -> "22222222-2222-4222-8222-222222222222",
        "expectedEffectiveUser" -> "fp@localhost", "username" -> "fp", "pkcs12TrustStorePath" -> (absolute + "/trust.p12"),
        "credentialSourceRef" -> ("file:" + absolute + "/credential"), "trustSecretSourceRef" -> ("file:" + absolute + "/trust"),
        "connectTimeoutMillis" -> "3000", "socketTimeoutMillis" -> "10000", "activeLimit" -> active.toString,
        "innodbFlushLogAtTrxCommit" -> "1", "syncBinlog" -> "1", "approvedBaselinePath" -> (absolute + "/baseline"),
        "approvedBaselineSha256" -> pin, "approvedBaselineFormat" -> MySqlApprovedBaselineLoader.ApprovedFormat,
        "approvedSchemaFormat" -> "3", "approvedCoverage" -> MySqlAuditCoverage.Format3,
        "expectedServerVersion" -> "9.7.2", "expectedProvisioningUuid" -> "33333333-3333-4333-8333-333333333333",
        "approvedDdlSha256" -> ("D" * 64), "expectedAuditDefiner" -> "audit@localhost",
        "approvedSourceCaptureSha256" -> sourcePin, "format3RuntimeContractPath" -> (absolute + "/contract"),
        "format3RuntimeContractSha256" -> contractPin)
      MySqlRuntimeMetadata.fromBytes(fields.toVector.sortBy(_._1).map((k,v) => k + "=" + v).mkString("\n").getBytes(UTF_8))
    def a(b: String, r: String, k: String): String =
      MySqlManifestConfigurationIdentity.AnchorSha256.fromCompleteAnchorHex(
        WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(metadata(b, r, k)).configurationSha256()).hex
    val original = a("A" * 64, "B" * 64, "C" * 64)
    assertNotEquals(original, a("E" * 64, "B" * 64, "C" * 64))
    assertNotEquals(original, a("A" * 64, "E" * 64, "C" * 64))
    assertNotEquals(original, a("A" * 64, "B" * 64, "E" * 64))

    val budget = root()
    val source = "independent synthetic provenance DATA".getBytes(UTF_8)
    val sourceSha = MessageDigest.getInstance("SHA-256").digest(source)
    val observedConfiguration = MessageDigest.getInstance("SHA-256").digest("observed fixture configuration".getBytes(UTF_8))
    val policy = MySqlRuntimeAuditPolicy("NONE", "NONE", MySqlExpectedNoImport,
      MySqlExpectedPhysicalFormat.Format2, Duration.ofSeconds(1))
    def proposed(bpin: String, rpin: String, kpin: String, active: Int = 32): String =
      val m = metadata(bpin, rpin, kpin, active)
      val b = m.baseline()
      val identity = MySqlApprovedBaselineIdentity(b.format(), b.schemaFormat(), b.coverage(), m.expectedServerUuid(),
        b.serverVersion(), m.schema(), b.provisioningUuid(), m.expectedBindingUuid(), b.ddlSha256(), b.auditDefiner(), b.sourceCaptureSha256())
      val privileges = MySqlAuditCoverage.Format3PrivilegeResultSetIds.zip(MySqlSchemaAuditor.Format3PrivilegeLabels).zipWithIndex.map {
        case ((id, labels), index) =>
          val columns = labels.map(label => MySqlAuditColumn(label, Types.VARCHAR, "VARCHAR", 256, 0, 1, false))
          val rows = if index != 2 then Vector.empty else Vector("SELECT", "TRIGGER").map { privilege =>
            MySqlAuditRow(labels.map {
              case "GRANTEE" => MySqlAuditText("'audit'@'localhost'")
              case "TABLE_CATALOG" => MySqlAuditText("def")
              case "TABLE_SCHEMA" => MySqlAuditText("fp_test")
              case "PRIVILEGE_TYPE" => MySqlAuditText(privilege)
              case "IS_GRANTABLE" => MySqlAuditText("NO")
              case _ => throw new AssertionError("unexpected fixed privilege label")
            })
          }
          MySqlAuditResultSet(id, columns, rows)
      }
      // Constructed test DATA is not independently loaded/approved deployment material.
      val contract = new MySqlFormat3RuntimeContract(identity, bpin, "fp@localhost", "fp@localhost", "NONE",
        Vector("GRANT USAGE ON *.* TO 'fp'@'localhost'"), "NONE", MySqlExpectedPhysicalFormat.Format2,
        MySqlExpectedNoImport, Duration.ofSeconds(1), "FRESH_PLAN", source, sourceSha, privileges, budget.profile)
      val inputs = contract.auditInputs(observedConfiguration)
      val result = MySqlCurrentConfigurationProjection.proposeV2FromCurrent(m, contract, policy, inputs, budget)
      try result.candidate.hex finally result.close()
    try
      val base = proposed("A" * 64, "B" * 64, "C" * 64)
      assertEquals(base, proposed("E" * 64, "B" * 64, "C" * 64))
      assertEquals(base, proposed("A" * 64, "E" * 64, "C" * 64))
      assertEquals(base, proposed("A" * 64, "B" * 64, "E" * 64))
      assertNotEquals(base, proposed("A" * 64, "B" * 64, "C" * 64, 31))
      assertEquals(0L, budget.accountedMemoryBytes)
    finally budget.closeRootReadBudget()


  private def semanticPolicy = MySqlRuntimeAuditPolicy("NONE", "NONE", MySqlExpectedNoImport,
    MySqlExpectedPhysicalFormat.Format2, Duration.ofSeconds(1))

  private def semanticFixture(): JsonObject =
    val absolute=directory.toAbsolutePath.toString
    val fields = Map("backend" -> "mysql", "host" -> "127.0.0.1", "port" -> "33317", "schema" -> "fp_test",
      "expectedServerUuid" -> "11111111-1111-4111-8111-111111111111", "expectedBindingUuid" -> "22222222-2222-4222-8222-222222222222",
      "expectedEffectiveUser" -> "fp@localhost", "username" -> "fp", "pkcs12TrustStorePath" -> (absolute + "/trust.p12"),
      "credentialSourceRef" -> ("file:" + absolute + "/credential"), "trustSecretSourceRef" -> ("file:" + absolute + "/trust"),
      "connectTimeoutMillis" -> "3000", "socketTimeoutMillis" -> "10000", "activeLimit" -> "32",
      "innodbFlushLogAtTrxCommit" -> "1", "syncBinlog" -> "1", "approvedBaselinePath" -> (absolute + "/baseline"),
      "approvedBaselineSha256" -> ("A" * 64), "approvedBaselineFormat" -> MySqlApprovedBaselineLoader.ApprovedFormat,
      "approvedSchemaFormat" -> "3", "approvedCoverage" -> MySqlAuditCoverage.Format3,
      "expectedServerVersion" -> "9.7.2", "expectedProvisioningUuid" -> "33333333-3333-4333-8333-333333333333",
      "approvedDdlSha256" -> ("D" * 64), "expectedAuditDefiner" -> "audit@localhost",
      "approvedSourceCaptureSha256" -> ("B" * 64), "format3RuntimeContractPath" -> (absolute + "/contract"),
      "format3RuntimeContractSha256" -> ("C" * 64))

    val value=fixture();val m=value.getAsJsonObject("metadata");val c=value.getAsJsonObject("contract")
    fields.filter((k,_)=>metadataNames.contains(k)).foreach((k,v)=>m.addProperty(k,v))
    Vector("pkcs12TrustStorePath","approvedBaselinePath","format3RuntimeContractPath").foreach(k=>m.addProperty(k,Path.of(m.get(k).getAsString).toString))
    Vector("credentialSourceRef","trustSecretSourceRef").foreach(k=>m.addProperty(k,"file:"+Path.of(m.get(k).getAsString.substring(5)).toString))
    Map("serverUuid"->fields("expectedServerUuid"),"bindingUuid"->fields("expectedBindingUuid"),"provisioningUuid"->fields("expectedProvisioningUuid"),
      "schema"->"fp_test","coverage"->MySqlAuditCoverage.Format3,"serverVersion"->"9.7.2","effectiveUser"->"fp@localhost","connectedUser"->"fp@localhost",
      "callerRoles"->"NONE","auditDefinerRoles"->"NONE","ddlSha256"->("D"*64),"auditDefiner"->"audit@localhost","importState"->"NONE",
      "sourceProvenanceKind"->"FRESH_PLAN","sourceProvenanceHex"->hex("actual provenance DATA".getBytes(UTF_8)),
      "sourceProvenanceSha256"->hex(MessageDigest.getInstance("SHA-256").digest("actual provenance DATA".getBytes(UTF_8))),
      "limitProfileSha256"->hex(profile().sha256())).foreach((k,v)=>c.addProperty(k,v))
    val grants=new JsonArray;grants.add("GRANT USAGE ON *.* TO 'fp'@'localhost'");c.add("callerGrants",grants)
    val sets=new JsonArray
    MySqlAuditCoverage.Format3PrivilegeResultSetIds.zip(MySqlSchemaAuditor.Format3PrivilegeLabels).zipWithIndex.foreach { case ((id,labels),index)=>
      val set=new JsonObject;set.addProperty("id",id);val columns=new JsonArray
      labels.foreach { label=>
        val column=new JsonObject;column.addProperty("label",label);column.addProperty("jdbcType",Types.VARCHAR)
        column.addProperty("typeName","VARCHAR");column.addProperty("precision",256);column.addProperty("scale",0)
        column.addProperty("nullable",1);column.addProperty("signed",false);columns.add(column)
      }
      set.add("columns",columns);val rows=new JsonArray
      if index==2 then Vector("SELECT","TRIGGER").foreach { privilege=>
        val row=new JsonArray
        labels.foreach { label=>
          val cell=new JsonObject;cell.addProperty("tag","TEXT")
          cell.addProperty("value",label match
            case "GRANTEE"=>"'audit'@'localhost'"
            case "TABLE_CATALOG"=>"def"
            case "TABLE_SCHEMA"=>"fp_test"
            case "PRIVILEGE_TYPE"=>privilege
            case "IS_GRANTABLE"=>"NO"
            case _=>throw new AssertionError("unexpected privilege column"))
          row.add(cell)
        }
        rows.add(row)
      }
      set.add("rows",rows);sets.add(set)
    }
    c.add("definerPrivileges",sets);value

  @Test def semanticFactoryUsesIndependentGoldenAndExactCustody(): Unit =
    val budget=root()
    try
      val bytes=render(semanticFixture()).getBytes(UTF_8)
      val preimage=new ByteArrayOutputStream;val out=new DataOutputStream(preimage)
      val tag=MySqlManifestConfigurationIdentity.Encoding.getBytes(UTF_8)
      out.writeInt(tag.length);out.write(tag);out.writeInt(bytes.length);out.write(bytes)
      val result=MySqlConfigurationRecipeCodec.parseSemanticV2(bytes,semanticPolicy,budget)
      assertEquals(hex(MessageDigest.getInstance("SHA-256").digest(preimage.toByteArray)),result.hex)
      assertEquals(224L,budget.accountedMemoryBytes)
      result.close();assertEquals(0L,budget.accountedMemoryBytes)
      assertThrows(classOf[IllegalStateException],()=>result.hex)
    finally budget.closeRootReadBudget()

  @Test def semanticSelectorsIdentityCanonicalAndSecurityRejectWithoutDebt(): Unit =
    val changes=Vector[JsonObject=>Unit](
      v=>v.getAsJsonObject("metadata").addProperty("format3ManifestConfigurationEncoding","V1"),
      v=>v.getAsJsonObject("contract").addProperty("format3ManifestConfigurationEncoding","V1"),
      v=>v.getAsJsonObject("contract").addProperty("format","V1"),
      v=>v.getAsJsonObject("metadata").addProperty("expectedEffectiveUser","different@localhost"),
      v=>v.getAsJsonObject("metadata").addProperty("unexpected","x"),
      v=>v.getAsJsonObject("metadata").addProperty("port","033317"),
      v=>v.getAsJsonObject("metadata").addProperty("expectedServerUuid","ABCDEF11-1111-4111-8111-111111111111"),
      v=>v.getAsJsonObject("metadata").addProperty("approvedDdlSha256","d"*64),
      v=>v.getAsJsonObject("contract").addProperty("serverUuid","ABCDEF11-1111-4111-8111-111111111111"),
      v=>v.getAsJsonObject("contract").addProperty("ddlSha256","d"*64),
      v=>v.getAsJsonObject("contract").addProperty("sourceProvenanceSha256","A"*64),
      v=>v.getAsJsonObject("contract").addProperty("limitProfileSha256","A"*64),
      v=>v.getAsJsonObject("contract").addProperty("callerRoles","different"),
      v=>{v.getAsJsonObject("contract").getAsJsonArray("definerPrivileges").get(2).getAsJsonObject.getAsJsonArray("rows").remove(1);()},
      v=>v.getAsJsonObject("contract").getAsJsonArray("definerPrivileges").get(2).getAsJsonObject.getAsJsonArray("rows").get(0).getAsJsonArray.get(0).getAsJsonObject.addProperty("value","'intruder'@'localhost'"))
    changes.zipWithIndex.foreach { (change,index)=>
      val budget=root();val value=semanticFixture();change(value)
      try
        assertThrows(classOf[Exception],()=>{MySqlConfigurationRecipeCodec.parseSemanticV2(render(value).getBytes(UTF_8),semanticPolicy,budget);()},"mutation "+index)
        assertEquals(0L,budget.accountedMemoryBytes,"mutation "+index)
      finally budget.closeRootReadBudget()
    }

  @Test def semanticRootProfileBudgetAndLegacySelectorRemainStrict(): Unit =
    val value=semanticFixture();val bytes=render(value).getBytes(UTF_8)
    val wrong=root(16L*1024*1024)
    try
      assertThrows(classOf[Exception],()=>MySqlConfigurationRecipeCodec.parseSemanticV2(bytes,semanticPolicy,wrong))
      assertEquals(0L,wrong.accountedMemoryBytes)
    finally wrong.closeRootReadBudget()
    val small=root(1024)
    try assertThrows(classOf[Exception],()=>MySqlConfigurationRecipeCodec.parseSemanticV2(bytes,semanticPolicy,small))
    finally small.closeRootReadBudget()
    val closed=root();closed.closeRootReadBudget()
    assertThrows(classOf[Exception],()=>MySqlConfigurationRecipeCodec.parseSemanticV2(bytes,semanticPolicy,closed))
    import scala.jdk.CollectionConverters.*
    val m=value.getAsJsonObject("metadata")
    val properties=m.keySet().asScala.toVector.sorted.map(k=>k+"="+m.get(k).getAsString.replace('\\','/')).mkString("\n").getBytes(UTF_8)
    assertThrows(classOf[IllegalArgumentException],()=>MySqlRuntimeMetadata.fromBytes(properties))


  @Test def typedContractOwnerCannotCrossRootOrOutliveCloseAndFailedCloseIsExplicit(): Unit =
    import scala.jdk.CollectionConverters.*
    val value=semanticFixture();val m=value.getAsJsonObject("metadata")
    val deployment=MySqlRuntimeMetadata.parseManifestV2Projection(m.keySet().asScala.map(k=>k->m.get(k).getAsString).toMap.asJava)
    val owner=root();val other=root()
    val semantic=MySqlFormat3RuntimeContractLoader.parseManifestV2Projection(render(value.get("contract")),owner)
    try
      assertTrue(owner.accountedMemoryBytes>0)
      assertThrows(classOf[Exception],()=>MySqlManifestConfigurationIdentity.fromValidatedSemantics(deployment,semantic,semanticPolicy,other))
      assertEquals(0L,other.accountedMemoryBytes)
      semantic.close();semantic.close();assertEquals(0L,owner.accountedMemoryBytes)
      assertThrows(classOf[Exception],()=>MySqlManifestConfigurationIdentity.fromValidatedSemantics(deployment,semantic,semanticPolicy,owner))
      val scope=owner.newCanonicalScope()
      try assertThrows(classOf[Exception],()=>MySqlConfigurationRecipeCodec.parseSemanticV2(render(value).getBytes(UTF_8),semanticPolicy,scope))
      finally scope.closeScopedReadBudget()
    finally
      semantic.close();other.closeRootReadBudget();owner.closeRootReadBudget()
    val retired=root()
    val retained=MySqlFormat3RuntimeContractLoader.parseManifestV2Projection(render(value.get("contract")),retired)
    retired.closeRootReadBudget()
    assertThrows(classOf[Exception],()=>retained.fields)
    assertThrows(classOf[IllegalStateException],()=>retained.close())


  @Test def realLegacyKEntryRejectsV2AndPreservesStaticIdentityCause(): Unit =
    import scala.jdk.CollectionConverters.*
    val value=semanticFixture();val m=value.getAsJsonObject("metadata");val c=value.getAsJsonObject("contract")
    m.remove("format3ManifestConfigurationEncoding")
    m.addProperty("approvedBaselineSha256","A"*64);m.addProperty("approvedSourceCaptureSha256","B"*64)
    val identity=MySqlApprovedBaselineIdentity(MySqlApprovedBaselineLoader.ApprovedFormat,3,MySqlAuditCoverage.Format3,
      UUID.fromString(m.get("expectedServerUuid").getAsString),"9.7.2","fp_test",UUID.fromString(m.get("expectedProvisioningUuid").getAsString),
      UUID.fromString(m.get("expectedBindingUuid").getAsString),"D"*64,"audit@localhost","B"*64)
    def actualLoad(): MySqlFormat3RuntimeContract =
      val bytes=render(c).getBytes(UTF_8)
      java.nio.file.Files.write(Path.of(m.get("format3RuntimeContractPath").getAsString),bytes)
      m.addProperty("format3RuntimeContractSha256",hex(MessageDigest.getInstance("SHA-256").digest(bytes)))
      val properties=m.keySet().asScala.toVector.sorted.map(k=>k+"="+m.get(k).getAsString.replace('\\','/')).mkString("\n").getBytes(UTF_8)
      MySqlFormat3RuntimeContractLoader.loadExpected(MySqlRuntimeMetadata.fromBytes(properties),identity,"A"*64)
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>actualLoad())
    c.remove("format3ManifestConfigurationEncoding");c.addProperty("format",MySqlFormat3RuntimeContractLoader.Format)
    c.addProperty("approvedBaselineSha256","A"*64);c.addProperty("sourceCaptureSha256","B"*64)
    assertNotNull(actualLoad())
    c.addProperty("serverVersion","9.7.1")
    val observed=assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>actualLoad())
    assertEquals("format3 contract static identity is invalid",observed.getMessage)
    assertInstanceOf(classOf[IllegalArgumentException],observed.getCause)
    assertTrue(observed.getCause.getStackTrace.nonEmpty)

  @Test def v3IndependentCanonicalPrivilegesNormalizeQuotedMergedAndSplitObservations(): Unit =
    val budget=CallerPrivilegesV3TestData.root()
    try
      val v=CallerPrivilegesV3TestData.value
      MySqlRuntimeCallerPrivileges.requireObservation(v,CallerPrivilegesV3TestData.raw(),budget)
      MySqlRuntimeCallerPrivileges.requireObservation(v,CallerPrivilegesV3TestData.raw(false),budget)
      MySqlRuntimeCallerPrivileges.requireObservation(v,CallerPrivilegesV3TestData.merged,budget)
      MySqlRuntimeCallerPrivileges.requireObservation(v,CallerPrivilegesV3TestData.raw() ++ CallerPrivilegesV3TestData.raw(),budget)
      assertEquals(0L,budget.accountedMemoryBytes)
      val recipe=fixture()
      recipe.addProperty("format",MySqlManifestConfigurationIdentity.RecipeFormatV3)
      val c=recipe.getAsJsonObject("contract")
      c.addProperty("format",MySqlManifestConfigurationIdentity.ContractFormatV3)
      c.addProperty("format3ManifestConfigurationEncoding",MySqlManifestConfigurationIdentity.EncodingV3)
      c.remove("callerGrants");c.add("callerPrivileges",CallerPrivilegesV3TestData.json)
      recipe.getAsJsonObject("metadata").addProperty("format3ManifestConfigurationEncoding",MySqlManifestConfigurationIdentity.EncodingV3)
      val bytes=render(recipe).getBytes(UTF_8)
      val structure=MySqlConfigurationRecipeCodec.inspectV3(bytes,budget)
      try assertEquals(64,structure.candidate.hex.length) finally structure.close()
      assertThrows(classOf[java.sql.SQLDataException],()=>MySqlConfigurationRecipeCodec.inspect(bytes,budget))
      assertEquals(0L,budget.accountedMemoryBytes)
    finally budget.closeRootReadBudget()

  @Test def v3RejectsForbiddenUnknownMissingAndNoncanonicalIndependentPrivileges(): Unit =
    val budget=CallerPrivilegesV3TestData.root()
    try
      val value=CallerPrivilegesV3TestData.value
      val raw=CallerPrivilegesV3TestData.raw()
      val extras=Vector(
        "GRANT ALL PRIVILEGES ON *.* TO `fp_f3_runtime`@`localhost`",
        "GRANT SELECT ON `other`.* TO `fp_f3_runtime`@`localhost`",
        "GRANT SELECT ON `fp_test_20261007_format3_b2`.`unknown` TO `fp_f3_runtime`@`localhost`",
        "GRANT UPDATE (`unknown`) ON `fp_test_20261007_format3_b2`.`floating_pod_state` TO `fp_f3_runtime`@`localhost`",
        "GRANT EXECUTE ON PROCEDURE `fp_test_20261007_format3_b2`.`other` TO `fp_f3_runtime`@`localhost`",
        "GRANT PROXY ON 'x'@'localhost' TO 'fp_f3_runtime'@'localhost'",
        "GRANT `role`@`localhost` TO `fp_f3_runtime`@`localhost`",
        "REVOKE SELECT ON `other`.* FROM `fp_f3_runtime`@`localhost`",
        "GRANT SYSTEM_USER ON *.* TO `fp_f3_runtime`@`localhost`",
        raw.head+" WITH GRANT OPTION",raw.head+";",raw.head+" -- comment",
        raw.head.replace("`fp_f3_runtime`@`localhost`","fp_f3_runtime@localhost"),
        raw.head.replace("`fp_f3_runtime`@`localhost`","\"fp_f3_runtime\"@\"localhost\""),
        "GRANT USAGE ON `*`.`*` TO `fp_f3_runtime`@`localhost`",
        "GRANT USAGE ON *.* TO `fp_f3_runtime`@`local\\host`")
      extras.foreach(extra=>assertThrows(classOf[java.sql.SQLDataException],()=>MySqlRuntimeCallerPrivileges.requireObservation(value,raw :+ extra,budget)))
      assertThrows(classOf[java.sql.SQLDataException],()=>MySqlRuntimeCallerPrivileges.requireObservation(value,raw.tail,budget))
      val bad=CallerPrivilegesV3TestData.json
      bad.getAsJsonArray("grants").add(bad.getAsJsonArray("grants").get(0).deepCopy())
      val reader=new com.google.gson.stream.JsonReader(new java.io.StringReader(render(bad)))
      try assertThrows(classOf[java.sql.SQLDataException],()=>MySqlRuntimeCallerPrivileges.read(reader)) finally reader.close()
      assertEquals(0L,budget.accountedMemoryBytes)
    finally budget.closeRootReadBudget()

  @Test def v3SemanticCUsesNewDomainAndRejectsV2SelectorWithoutDebt(): Unit =
    val budget=root()
    try
      val recipe=semanticFixture()
      recipe.addProperty("format",MySqlManifestConfigurationIdentity.RecipeFormatV3)
      val m=recipe.getAsJsonObject("metadata");val c=recipe.getAsJsonObject("contract")
      m.addProperty("format3ManifestConfigurationEncoding",MySqlManifestConfigurationIdentity.EncodingV3)
      m.addProperty("schema",MySqlRuntimeCallerPrivileges.Schema)
      m.addProperty("expectedEffectiveUser","fp_f3_runtime@localhost");m.addProperty("username","fp_f3_runtime")
      c.addProperty("format",MySqlManifestConfigurationIdentity.ContractFormatV3)
      c.addProperty("format3ManifestConfigurationEncoding",MySqlManifestConfigurationIdentity.EncodingV3)
      c.addProperty("schema",MySqlRuntimeCallerPrivileges.Schema)
      c.addProperty("effectiveUser","fp_f3_runtime@localhost");c.addProperty("connectedUser","fp_f3_runtime@localhost")
      c.remove("callerGrants");c.add("callerPrivileges",CallerPrivilegesV3TestData.json)
      import scala.jdk.CollectionConverters.*
      c.getAsJsonArray("definerPrivileges").iterator().asScala.foreach { set=>
        set.getAsJsonObject.getAsJsonArray("rows").iterator().asScala.foreach { row=>
          row.getAsJsonArray.iterator().asScala.foreach { cell=>
            if cell.getAsJsonObject.get("value").getAsString == "fp_test" then
              cell.getAsJsonObject.addProperty("value",MySqlRuntimeCallerPrivileges.Schema)
          }
        }
      }
      val bytes=render(recipe).getBytes(UTF_8)
      val preimage=new ByteArrayOutputStream;val out=new DataOutputStream(preimage)
      val tag=MySqlManifestConfigurationIdentity.EncodingV3.getBytes(UTF_8)
      out.writeInt(tag.length);out.write(tag);out.writeInt(bytes.length);out.write(bytes)
      val result=MySqlConfigurationRecipeCodec.parseSemanticV3(bytes,semanticPolicy,budget)
      try assertEquals(hex(MessageDigest.getInstance("SHA-256").digest(preimage.toByteArray)),result.hex)
      finally result.close()
      assertThrows(classOf[java.sql.SQLDataException],()=>MySqlConfigurationRecipeCodec.parseSemanticV2(bytes,semanticPolicy,budget))
      assertEquals(0L,budget.accountedMemoryBytes)
    finally budget.closeRootReadBudget()

  @Test def v3ObservationPrechargeRejectsBeforeMalformedTextAndReturnsNoDebt(): Unit =
    val small=root(1024)
    try
      val cause=assertThrows(classOf[java.sql.SQLDataException],()=>
        MySqlRuntimeCallerPrivileges.requireObservation(CallerPrivilegesV3TestData.value,Vector("not grants"),small))
      assertEquals("V3 caller privilege workspace BEFORE allocation exceeds explicit retained-memory budget",cause.getMessage)
      assertEquals(0L,small.accountedMemoryBytes)
    finally small.closeRootReadBudget()


/** 固定已采纳58授权的独立测试材料；不是数据库观察或部署批准。 */
private[mysql] object CallerPrivilegesV3TestData:
  val canonical = """{"format":"FP_MYSQL_RUNTIME_CALLER_PRIVILEGES_BE_V1","grants":[{"columns":[],"grantOption":false,"object":"fp_audit_schema_format3","privilege":"EXECUTE","schema":"fp_test_20261007_format3_b2","scope":"PROCEDURE"},{"columns":[],"grantOption":false,"object":"","privilege":"LOCK TABLES","schema":"fp_test_20261007_format3_b2","scope":"SCHEMA"},{"columns":[],"grantOption":false,"object":"binding_bootstrap_intent","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"binding_bootstrap_intent","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["marker_ack_sha256","stage"],"grantOption":false,"object":"binding_bootstrap_intent","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"floating_pod_state","privilege":"DELETE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"floating_pod_state","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"floating_pod_state","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["dimension_id","is_active","last_mutation_operation_uuid","last_updated","operating_mode","owner_uuid","pitch","pos_x","pos_y","pos_z","state_version","world_name","yaw"],"grantOption":false,"object":"floating_pod_state","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"fp_lifecycle_completion","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"fp_lifecycle_completion","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["completion_revision","consumed_by_operation_uuid","last_mutation_operation_uuid"],"grantOption":false,"object":"fp_lifecycle_completion","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"fp_recovery_operation","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"fp_recovery_operation","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["commit_witness_bytes","commit_witness_sha256","operation_state","original_diagnostic_identity","original_diagnostic_sha256","result_at","settled_by_operation_uuid"],"grantOption":false,"object":"fp_recovery_operation","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"fp_recovery_reservation","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"fp_recovery_reservation","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["last_mutation_operation_uuid","operation_uuid","reservation_revision","reservation_state","workflow_uuid"],"grantOption":false,"object":"fp_recovery_reservation","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"fp_runtime_writer","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["last_commit_fingerprint","last_commit_operation_uuid","lease_until","owner_boot_uuid","writer_epoch"],"grantOption":false,"object":"fp_runtime_writer","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"fp_schema_manifest","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_footprint_transfer","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_footprint_transfer","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["cleanup_operation_uuid","cleanup_proof_bytes","cleanup_proof_sha256","commit_operation_uuid","last_mutation_operation_uuid","phase","transfer_revision"],"grantOption":false,"object":"physical_footprint_transfer","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_recovery_binding","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_recovery_binding","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["cutover_state"],"grantOption":false,"object":"physical_recovery_binding","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_recovery_footprint","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_recovery_footprint","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["seal_state"],"grantOption":false,"object":"physical_recovery_footprint","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_recovery_footprint_chunk","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_recovery_footprint_chunk","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_recovery_intent","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_recovery_intent","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["activation_operation_uuid","cleanup_operation_uuid","cleanup_proof_bytes","cleanup_proof_sha256","committed_at","committed_dimension_id","committed_domain_version","committed_pitch","committed_pos_x","committed_pos_y","committed_pos_z","committed_world_name","committed_yaw","intent_revision","last_mutation_operation_uuid","phase"],"grantOption":false,"object":"physical_recovery_intent","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_recovery_locator","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_recovery_locator","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["current_attempt_uuid","current_footprint_uuid","domain_version","last_mutation_operation_uuid","locator_revision","locator_state","reason_code","target_dimension_id"],"grantOption":false,"object":"physical_recovery_locator","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_recovery_obligation","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_recovery_obligation","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["attempt_retirement_operation_uuid","last_mutation_operation_uuid","obligation_revision","obligation_state","verification_proof_bytes","verification_proof_sha256","verified_operation_uuid"],"grantOption":false,"object":"physical_recovery_obligation","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_source_forest","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_source_forest","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["forest_state"],"grantOption":false,"object":"physical_source_forest","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_source_obligation","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_source_obligation","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["discharge_kind","discharge_proof_bytes","discharge_proof_sha256","discharged_operation_uuid","last_mutation_operation_uuid","obligation_revision","obligation_state"],"grantOption":false,"object":"physical_source_obligation","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_source_transfer","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_source_transfer","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["cutover_operation_uuid","last_mutation_operation_uuid","phase","retire_operation_uuid","retirement_proof_bytes","retirement_proof_sha256","target_receipts_bytes","target_receipts_sha256","transfer_revision"],"grantOption":false,"object":"physical_source_transfer","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_source_unit","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_source_unit","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["current_epoch","current_forest_uuid","last_mutation_operation_uuid","unit_revision","unit_state"],"grantOption":false,"object":"physical_source_unit","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_source_write","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"physical_source_write","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":["durable_receipt_bytes","durable_receipt_sha256","failure_diagnostic_identity","failure_diagnostic_sha256","last_mutation_operation_uuid","resolved_operation_uuid","write_revision","write_state"],"grantOption":false,"object":"physical_source_write","privilege":"UPDATE","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"player_entitlement","privilege":"INSERT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"},{"columns":[],"grantOption":false,"object":"player_entitlement","privilege":"SELECT","schema":"fp_test_20261007_format3_b2","scope":"TABLE"}],"principal":{"host":"localhost","user":"fp_f3_runtime"}}"""
  def json: JsonObject = com.google.gson.JsonParser.parseString(canonical).getAsJsonObject
  def value: MySqlRuntimeCallerPrivileges.Value =
    val reader=new com.google.gson.stream.JsonReader(new java.io.StringReader(canonical))
    reader.setStrictness(com.google.gson.Strictness.STRICT)
    try MySqlRuntimeCallerPrivileges.read(reader) finally reader.close()
  def root(): MySqlFormat3ReadBudget =
    val p=SourceLimitProfile.of(1024,2048,4096,32,100,8L*1024*1024,65536,16384)
    new MySqlFormat3ReadBudget(p,p.maxSqlPacketBytes())
  def raw(backtickAccount: Boolean = true): Vector[String] =
    import scala.jdk.CollectionConverters.*
    json.getAsJsonArray("grants").iterator().asScala.map { element =>
      val g=element.getAsJsonObject
      val cols=g.getAsJsonArray("columns").iterator().asScala.map(c=>"`"+c.getAsString+"`").mkString(", ")
      val permission=g.get("privilege").getAsString+(if cols.isEmpty then "" else " ("+cols+")")
      val obj=if g.get("scope").getAsString == "SCHEMA" then "*" else "`"+g.get("object").getAsString+"`"
      val prefix=if g.get("scope").getAsString == "PROCEDURE" then "PROCEDURE " else ""
      "GRANT "+permission+" ON "+prefix+"`fp_test_20261007_format3_b2`."+obj+" TO "+
        (if backtickAccount then "`fp_f3_runtime`@`localhost`" else "'fp_f3_runtime'@'localhost'")
    }.toVector
  def merged: Vector[String] =
    val grouped=scala.collection.mutable.LinkedHashMap.empty[String,Vector[String]]
    raw().foreach { line =>
      val on=line.indexOf(" ON ")
      val suffix=line.substring(on)
      grouped.update(suffix,grouped.getOrElse(suffix,Vector.empty) :+ line.substring(6,on))
    }
    grouped.iterator.map((suffix,permissions)=>"grant "+permissions.mkString(", ")+suffix).toVector.reverse
