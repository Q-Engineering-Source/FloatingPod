package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.{MySqlRuntimeMetadata, SourceLimitProfile}
import com.wi.floatingpod.bootstrap.{ProductionOrdinarySourceApproval, ProductionBootstrapControl, EarlyDependencyResolver}
import com.wi.floatingpod.server.io.{IoOperationTag,IoSubmission}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import com.wi.floatingpod.server.{WindowsWorldProvisioner, WindowsB1SourceAnchorStore}
import com.google.gson.{GsonBuilder, JsonArray, JsonObject}
import java.security.MessageDigest
import java.sql.Types
import java.util.{HexFormat, Locale}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.Assertions.*
import java.nio.file.{Files, Path}
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.UUID

class MySqlRuntimeInputsTest:
  @TempDir var temporary: Path = _
  private val packHash = "5d5626a4e3ed7b48ce4036788c9885b73f4fa92a9ec01424bbf7ce282029948e"
  private val binding = "e83a7eb6-c7fd-4094-9751-029bdef5c0cf"
  private val boot = UUID.fromString("00000000-0000-0000-0000-000000000123")
  private def policy = MySqlRuntimeAuditPolicy("NONE", "NONE", MySqlExpectedNoImport,
    MySqlExpectedPhysicalFormat.Format2, Duration.ofSeconds(20))
  private def metadata(change: String => String = identity): MySqlRuntimeMetadata =
    val input = getClass.getResourceAsStream("/com/wi/floatingpod/storage/mysql/approved-static-baseline-format2.json")
    assertNotNull(input)
    val pack = temporary.resolve("approved.json")
    try Files.copy(input, pack) finally input.close()
    val prefix = temporary.toAbsolutePath.toString.replace('\\', '/')
    val content = s"""backend=mysql
host=database.example.invalid
port=3307
schema=fp_test_20261006_sec06
expectedServerUuid=982937bc-bc46-11f1-b957-04421a0aa74d
expectedBindingUuid=$binding
expectedEffectiveUser=fixture@localhost
username=fixture
pkcs12TrustStorePath=$prefix/trust.p12
credentialSourceRef=file:$prefix/credential.txt
trustSecretSourceRef=file:$prefix/trust-secret.txt
connectTimeoutMillis=3000
socketTimeoutMillis=10000
activeLimit=17
innodbFlushLogAtTrxCommit=1
syncBinlog=1
approvedBaselinePath=$prefix/approved.json
approvedBaselineSha256=$packHash
approvedBaselineFormat=FP_MYSQL_APPROVED_STATIC_V1
approvedSchemaFormat=2
approvedCoverage=${MySqlAuditCoverage.Format2}
expectedServerVersion=9.7.2
expectedProvisioningUuid=16fb58b8-a867-43b7-8e8e-03554578bf14
approvedDdlSha256=d638b7d4f8a235b59787ff628f408d5ba2110371736c60c0db755c759e0a1e1f
expectedAuditDefiner=fp_sec06_audit@127.0.0.1
approvedSourceCaptureSha256=13E6930BABA77B47C71A68AE053CF79EB595AD5707D857926F18765DBF5BA83B
"""
    val path = temporary.resolve("runtime.properties")
    Files.writeString(path, change(content), StandardCharsets.UTF_8)
    MySqlRuntimeMetadata.load(path)

  @Test def format2StaticPackRemainsAvailableToTheOfflineLoader(): Unit =
    val m = metadata()
    val pins = m.baseline()
    val expected = MySqlApprovedBaselineIdentity(pins.format(), pins.schemaFormat(), pins.coverage(),
      m.expectedServerUuid(), pins.serverVersion(), m.schema(), pins.provisioningUuid(),
      m.expectedBindingUuid(), pins.ddlSha256(), pins.auditDefiner(), pins.sourceCaptureSha256())
    val loaded = MySqlApprovedBaselineLoader.load(pins.absolutePath(), pins.fileSha256(), expected)
    assertEquals(MySqlAuditCoverage.Format2, loaded.identity.coverage)
    assertEquals(packHash, loaded.fileSha256)

  @Test def formalAssemblyRejectsFormat2BeforeOpeningItsOfflinePack(): Unit =
    val m = metadata()
    Files.delete(m.baseline().absolutePath())
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlRuntimeInputs.assemble(m, Thread.currentThread(), boot, 7L, policy))

  @Test def wrongThreadFailsBeforeBaselineRead(): Unit =
    val m = metadata()
    Files.delete(m.baseline().absolutePath())
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlRuntimeInputs.assemble(m, new Thread(), boot, 7L, policy))

  @Test def schema3RuntimeMetadataRequiresIndependentContractPathAndRawHash(): Unit =
    assertThrows(classOf[IllegalArgumentException], () =>
      metadata(_.replace("approvedSchemaFormat=2", "approvedSchemaFormat=3")
        .replace(MySqlAuditCoverage.Format2, MySqlAuditCoverage.Format3)))

  @Test def legacyFormatOneMetadataIsRejectedBeforeBaselineRead(): Unit =
    val m = metadata(_.replace("approvedSchemaFormat=2", "approvedSchemaFormat=1")
      .replace(MySqlAuditCoverage.Format2, MySqlAuditCoverage.V4))
    Files.delete(m.baseline().absolutePath())
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlRuntimeInputs.assemble(m, Thread.currentThread(), boot, 7L, policy))

  @Test def callerMustSupplyBoundedAuditPolicyAndPositiveBootGeneration(): Unit =
    assertThrows(classOf[IllegalArgumentException], () => MySqlRuntimeAuditPolicy("NONE", "NONE",
      MySqlExpectedNoImport, MySqlExpectedPhysicalFormat.Format2, Duration.ofMinutes(31)))
    val m = metadata()
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlRuntimeInputs.assemble(m, Thread.currentThread(), boot, 0L, policy))

  private object V2Fixture:
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
  
    private val v2Profile=SourceLimitProfile.of(1024,2048,4096,32,100,8L*1024*1024,8192,16384)
    private def v2Root(): MySqlFormat3ReadBudget = new MySqlFormat3ReadBudget(v2Profile,v2Profile.maxSqlPacketBytes())
    private def realSyntheticB(): MySqlApprovedStaticBaseline =
      // 既有独立 synthetic DATA fixture 写入 pack，再调用真实 baseline loader。
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
  

    def actualPlan(b: MySqlApprovedStaticBaseline): WindowsWorldProvisioner.Plan =
      val i=b.identity
      new WindowsWorldProvisioner.Plan(plan.creationOperationUuid(),plan.bootstrapOperationUuid(),plan.prepareWriteUuid(),plan.certifyWriteUuid(),
        i.bindingUuid,2,i.serverUuid,i.schema,3,i.ddlSha256,i.provisioningUuid)
    def fixture(selector: String = "FP_MYSQL_FORMAT3_RUNTIME_CONTRACT_V2"): (MySqlRuntimeMetadata.FullV2,WindowsWorldProvisioner.Plan) =
      val b=realSyntheticB()
      val path=temporary.resolve("v2-baseline/approved-static-baseline-format3-ordinary-synthetic.json")
      assertEquals(386887L,Files.size(path))
      assertEquals("96F8E58D60A8B373FF9D4AC750B7FECD3E1A0FDD2E11E06B38EDA700413B9702",sha(Files.readAllBytes(path)))
      assertEquals("fp_ordinary_synthetic",b.identity.schema)
      assertEquals(10,b.objects.resultSets.size)
      val c=v2Contract(b);c.addProperty("format",selector);val p=actualPlan(b)
      val provenance=MySqlFormat3FreshPlanProvenance.canonicalBytes(p)
      c.addProperty("sourceProvenanceHex",hex(provenance));c.addProperty("sourceProvenanceSha256",sha(provenance))
      (v2Metadata(b,c),p)
    def root(): MySqlFormat3ReadBudget = v2Root()
    def auditPolicy: MySqlRuntimeAuditPolicy = MySqlRuntimeAuditPolicy("NONE","NONE",MySqlExpectedNoImport,
      MySqlExpectedPhysicalFormat.Format2,Duration.ofSeconds(10))
    def original(m: MySqlRuntimeMetadata.FullV2,p: WindowsWorldProvisioner.Plan): ProductionOrdinarySourceApproval.ApprovedOriginalCreation =
      val world=temporary.resolve("existing-world").toAbsolutePath;Files.createDirectories(world)
      val control=temporary.resolve("control.properties").toAbsolutePath
      val approval=temporary.resolve("approval.properties").toAbsolutePath
      val a=WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(m).configurationSha256()
      val metadataSha="D"*64
      val values=Map("approvalFormat"->"FP_ORDINARY_SOURCE_APPROVAL_V1","creationUuid"->p.creationOperationUuid().toString,
        "bootstrapUuid"->p.bootstrapOperationUuid().toString,"prepareUuid"->p.prepareWriteUuid().toString,"certifyUuid"->p.certifyWriteUuid().toString,
        "creationBootUuid"->UUID.randomUUID().toString,"creationBootGeneration"->"1","bindingUuid"->p.bindingUuid().toString,
        "physicalFormat"->"2","serverUuid"->p.mysqlServerUuid().toString,"schema"->p.schema(),"schemaFormat"->"3",
        "ddlSha256"->p.ddlSha256(),"provisioningUuid"->p.provisioningUuid().toString,"canonicalReceiptLength"->"123",
        "canonicalReceiptSha256"->("5"*64),"worldPath"->world.toString,"anchorPath"->temporary.resolve("source.anchor").toAbsolutePath.toString,
        "anchorSha256"->("6"*64),"configurationSha256"->a,"metadataSha256"->metadataSha,
        "baselineSha256"->m.baseline().fileSha256().toUpperCase(Locale.ROOT),"runtimeContractSha256"->m.format3RuntimeContract().fileSha256().toUpperCase(Locale.ROOT))++
        Vector("sourceWorldIdentity","guardFileIdentity","receiptFileIdentity","anchorFileIdentity").zipWithIndex.map((key,n)=>
          key->("FP_WIN32_FILE_ID_V1;volume=0000000000000001;id="+f"${n+1}%032x")).toMap
      val properties=new java.util.Properties;values.foreach((k,v)=>properties.setProperty(k,v))
      val writer=new java.io.StringWriter;properties.store(writer,null);Files.writeString(approval,writer.toString,StandardCharsets.UTF_8)
      ProductionOrdinarySourceApproval.loadPinnedV2(approval,sha(Files.readAllBytes(approval)),control,world,m,metadataSha,a,m.format3RuntimeContract().fileSha256())

    def sessionProfile: SourceLimitProfile = v2Profile
    def freshRequest(m: MySqlRuntimeMetadata.FullV2,p: WindowsWorldProvisioner.Plan,id: UUID,generation: Long): MySqlFreshBootRequest =
      MySqlFreshBootRequest(temporary.resolve("g4-new-world").toAbsolutePath,p,new WindowsWorldProvisioner.BootIdentity(id,generation),
        MySqlFreshControlledMetadata(temporary.resolve("v2-metadata.properties").toAbsolutePath,
          WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(m).configurationSha256()))
    def control(m: MySqlRuntimeMetadata.FullV2,p: WindowsWorldProvisioner.Plan,id: UUID,operation: UUID): ProductionBootstrapControl.StaticV3 =
      val b=m.baseline();val md=temporary.resolve("v2-metadata.properties").toAbsolutePath
      val values=Map("controlFormat"->"FP_PRODUCTION_BOOTSTRAP_CONTROL_V3","mode"->"FRESH","metadataPath"->md.toString,
        "metadataSha256"->sha(Files.readAllBytes(md)),"configurationSha256"->WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(m).configurationSha256(),
        "worldPath"->temporary.resolve("g4-new-world").toAbsolutePath.toString,"host"->m.host(),"port"->m.port().toString,
        "schema"->m.schema(),"effectiveUser"->m.expectedEffectiveUser(),"username"->m.username(),"serverUuid"->m.expectedServerUuid().toString,
        "bindingUuid"->m.expectedBindingUuid().toString,"activeLimit"->m.activeLimit().toString,
        "connectTimeoutMillis"->m.connectTimeout().toMillis.toString,"socketTimeoutMillis"->m.socketTimeout().toMillis.toString,
        "baselinePath"->b.absolutePath().toString,"baselineSha256"->b.fileSha256().toUpperCase(Locale.ROOT),"baselineFormat"->b.format(),
        "schemaFormat"->"3","coverage"->b.coverage(),"serverVersion"->b.serverVersion(),"provisioningUuid"->b.provisioningUuid().toString,
        "ddlSha256"->b.ddlSha256().toUpperCase(Locale.ROOT),"auditDefiner"->b.auditDefiner(),"sourceCaptureSha256"->b.sourceCaptureSha256().toUpperCase(Locale.ROOT),
        "runtimeContractSha256"->m.format3RuntimeContract().fileSha256().toUpperCase(Locale.ROOT),
        "bootUuid"->id.toString,"bootGeneration"->"7","connectionGeneration"->"1","submitOperationUuid"->operation.toString,
        "creationUuid"->p.creationOperationUuid().toString,"bootstrapUuid"->p.bootstrapOperationUuid().toString,
        "prepareUuid"->p.prepareWriteUuid().toString,"certifyUuid"->p.certifyWriteUuid().toString,"callerRoles"->"NONE","auditDefinerRoles"->"NONE",
        "importState"->"NONE","physicalFormat"->"2","receiptFreshnessMillis"->"10000","leaseMillis"->"20000",
        "preparationMillis"->"45000","claimMillis"->"10000","finalizationMillis"->"10000",
        "sqlOuterSha256"->EarlyDependencyResolver.SQL_OUTER,"scalarOuterSha256"->("1"*64),"scalarRuntimeVersion"->"3.8.4",
        "scalaLibrarySha256"->("2"*64),"scalaParserSha256"->("3"*64),"fugueOuterSha256"->("4"*64),"fugueRuntimeVersion"->"0.99.1",
        "sourceLimitProfileCanonicalHex"->hex(v2Profile.canonicalBytes()),"sourceLimitProfileSha256"->hex(v2Profile.sha256()))
      val properties=new java.util.Properties;values.foreach((k,v)=>properties.setProperty(k,v))
      val writer=new java.io.StringWriter;properties.store(writer,null)
      val path=temporary.resolve("g4-control.properties").toAbsolutePath;Files.writeString(path,writer.toString,StandardCharsets.UTF_8)
      ProductionBootstrapControl.loadV3(new ProductionBootstrapControl.LaunchContract(path,sha(Files.readAllBytes(path))))

  @Test def v2ActualFilesBuildCNormalFormAndRetainGraphUntilConsumerClose(): Unit =
    val (m,p)=V2Fixture.fixture();val root=V2Fixture.root()
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val inputs=MySqlRuntimeInputs.assembleV2(m,owner,boot,7L,V2Fixture.auditPolicy,p)
    assertSame(m,inputs.originalMetadataV2)
    assertThrows(classOf[IllegalStateException],()=>inputs.originalMetadata)
    assertEquals(10,inputs.approvedBaseline.objects.resultSets.size)
    assertEquals(m.expectedServerUuid(),inputs.auditExpectation.serverUuid)
    assertEquals(m.schema(),inputs.settings.expectedSchema)
    val a=WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(m).configurationSha256()
    val c=HexFormat.of().withUpperCase().formatHex(inputs.auditExpectation.format3Inputs.get.configurationSha256.bytes)
    assertNotEquals(a,c)
    val independent=V2Fixture.root()
    val k=MySqlFormat3RuntimeContractLoader.loadV2(m,inputs.approvedBaseline,independent)
    val expected=MySqlManifestConfigurationIdentity.fromValidatedSemantics(m.deploymentSemantics(),k.semanticFor(independent),V2Fixture.auditPolicy,independent)
    assertEquals(expected.hex,c)
    expected.close();k.close();independent.closeRootReadBudget()
    assertTrue(root.accountedMemoryBytes>512L)
    val lease=owner.borrow()
    assertThrows(classOf[IllegalStateException],()=>owner.close())
    assertSame(m,inputs.originalMetadataV2)
    lease.close();owner.close()
    assertEquals(0L,root.accountedMemoryBytes)
    assertFalse(owner.hasUnsettledResources)
    assertThrows(classOf[IllegalStateException],()=>inputs.settings)
    assertThrows(classOf[IllegalStateException],()=>inputs.originalMetadataV2)
    owner.close();root.closeRootReadBudget()

  @Test def v2OrdinaryPreservesCompleteAAndOriginalRawPins(): Unit =
    val (m,p)=V2Fixture.fixture();val original=V2Fixture.original(m,p);val root=V2Fixture.root()
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val inputs=MySqlRuntimeInputs.assembleCertifiedOrdinaryV2(m,owner,boot,7L,V2Fixture.auditPolicy,original)
    assertEquals(original.baselineSha256(),inputs.approvedBaseline.fileSha256.toUpperCase(Locale.ROOT))
    assertEquals(original.configurationSha256(),WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(inputs.originalMetadataV2).configurationSha256())
    owner.close();assertEquals(0L,root.accountedMemoryBytes);root.closeRootReadBudget()

  @Test def v2FailedAssemblyKeepsOriginalCauseOnCallerRetainedOwnerAndDropsPaidGraph(): Unit =
    val (m,p)=V2Fixture.fixture();Files.delete(m.format3RuntimeContract().absolutePath())
    val root=V2Fixture.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val failure=assertThrows(classOf[java.nio.file.NoSuchFileException],()=>MySqlRuntimeInputs.assembleV2(m,owner,boot,7L,V2Fixture.auditPolicy,p))
    assertSame(failure,owner.failure.get)
    assertFalse(owner.hasUnsettledResources);assertEquals(0L,root.accountedMemoryBytes)
    assertThrows(classOf[IllegalStateException],()=>MySqlRuntimeInputs.assembleV2(m,owner,boot,7L,V2Fixture.auditPolicy,p))
    owner.close();root.closeRootReadBudget()

  @Test def v2ExactThreadRootAndSingleOwnerAreRequiredBeforeFileIo(): Unit =
    val root=V2Fixture.root()
    assertThrows(classOf[IllegalArgumentException],()=>MySqlRuntimeInputs.beginV2(root,new Thread()))
    val scope=root.newCanonicalScope()
    assertThrows(classOf[IllegalArgumentException],()=>MySqlRuntimeInputs.beginV2(scope,Thread.currentThread()))
    scope.closeScopedReadBudget();root.closeRootReadBudget()
    assertThrows(classOf[IllegalArgumentException],()=>MySqlRuntimeInputs.beginV2(root,Thread.currentThread()))

  @Test def v2PolicyProvenanceAndRawPinsFailThroughTheRealAssembly(): Unit =
    val (m,p)=V2Fixture.fixture()
    val root=V2Fixture.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val failure=assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleV2(m,owner,boot,7L,policy,p))
    assertTrue(failure.getMessage.contains("runtime audit policy differs from loaded V2 contract"))
    assertSame(failure,owner.failure.get);assertEquals(0L,root.accountedMemoryBytes);root.closeRootReadBudget()
    val second=V2Fixture.root();val next=MySqlRuntimeInputs.beginV2(second,Thread.currentThread())
    val sourceFailure=assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleV2(m,next,boot,7L,V2Fixture.auditPolicy,null))
    assertTrue(sourceFailure.getMessage.contains("no admitted FreshPlan issuer"))
    assertEquals(0L,second.accountedMemoryBytes);second.closeRootReadBudget()

  @Test def v2CallerRootClosedFirstLeavesExplicitUnsettledCleanupAndNeverRetries(): Unit =
    val (m,p)=V2Fixture.fixture();val root=V2Fixture.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val inputs=MySqlRuntimeInputs.assembleV2(m,owner,boot,7L,V2Fixture.auditPolicy,p)
    root.closeRootReadBudget()
    val failure=assertThrows(classOf[IllegalStateException],()=>owner.close())
    assertNotNull(failure.getCause);assertTrue(owner.hasUnsettledResources);assertTrue(owner.failedCleanup.nonEmpty)
    assertThrows(classOf[IllegalStateException],()=>inputs.format3Contract)
    val first=owner.failedCleanup.get
    assertThrows(classOf[IllegalStateException],()=>owner.close())
    assertSame(first,owner.failedCleanup.get)

  @Test def v2RealCapRejectionsSettleCallerOwnerAtPreworkspaceSeedAndBaselineStages(): Unit =
    val (m,p)=V2Fixture.fixture()
    for cap <- Vector(512L,70000L,1000000L) do
      val profile=SourceLimitProfile.of(1024,2048,4096,32,100,cap,8192,16384)
      val root=new MySqlFormat3ReadBudget(profile,profile.maxSqlPacketBytes())
      val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
      val failure=assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleV2(m,owner,boot,7L,V2Fixture.auditPolicy,p))
      val stage=if cap==512L then "configuration codec workspace" else if cap==70000L then "FullV2 seed live retention" else "baseline decoded text"
      assertTrue(failure.getMessage.contains(stage),failure.getMessage)
      assertSame(failure,owner.failure.get)
      assertEquals(0L,root.accountedMemoryBytes)
      assertFalse(owner.hasUnsettledResources)
      owner.close();root.closeRootReadBudget()

  @Test def v2IndependentBaselineAndContractRawPinsRejectChangedRealFiles(): Unit =
    for baseline <- Vector(true,false); ordinary <- Vector(true,false) do
      val (m,p)=V2Fixture.fixture()
      val original=if ordinary then V2Fixture.original(m,p) else null
      val file=if baseline then m.baseline().absolutePath() else m.format3RuntimeContract().absolutePath()
      Files.write(file,Array[Byte](32),java.nio.file.StandardOpenOption.APPEND)
      val root=V2Fixture.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
      val failure=assertThrows(classOf[Exception],()=>
        if ordinary then MySqlRuntimeInputs.assembleCertifiedOrdinaryV2(m,owner,boot,7L,V2Fixture.auditPolicy,original)
        else MySqlRuntimeInputs.assembleV2(m,owner,boot,7L,V2Fixture.auditPolicy,p))
      val expected=if baseline then "approved static baseline file SHA-256 does not match" else "V2 K raw SHA differs"
      assertTrue(failure.getMessage.contains(expected),failure.getMessage)
      assertSame(failure,owner.failure.get);assertEquals(0L,root.accountedMemoryBytes)
      owner.close();root.closeRootReadBudget()

  @Test def v2ActualSelectorAndIndependentRootProfileAreBothRequired(): Unit =
    val (m,p)=V2Fixture.fixture("FP_MYSQL_FORMAT3_RUNTIME_CONTRACT_V1")
    val root=V2Fixture.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val selectorFailure=assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleV2(m,owner,boot,7L,V2Fixture.auditPolicy,p))
    assertTrue(selectorFailure.getMessage.contains("V2 contract requires exact format and actual selector"),selectorFailure.getMessage)
    assertEquals(0L,root.accountedMemoryBytes);root.closeRootReadBudget()
    val (valid,plan)=V2Fixture.fixture()
    val profile=SourceLimitProfile.of(2048,2048,4096,32,100,8L*1024*1024,8192,16384)
    val foreign=new MySqlFormat3ReadBudget(profile,profile.maxSqlPacketBytes())
    val next=MySqlRuntimeInputs.beginV2(foreign,Thread.currentThread())
    val profileFailure=assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleV2(valid,next,boot,7L,V2Fixture.auditPolicy,plan))
    assertTrue(profileFailure.getMessage.contains("V2 K full identity/B/R/policy profile differs"),profileFailure.getMessage)
    assertEquals(0L,foreign.accountedMemoryBytes);foreign.closeRootReadBudget()

  @Test def v2OrdinaryChangedCompleteARejectsAndSuccessfulOwnerCannotBeReassembled(): Unit =
    val (m,p)=V2Fixture.fixture();val original=V2Fixture.original(m,p)
    val file=temporary.resolve("v2-metadata.properties")
    val text=Files.readString(file,StandardCharsets.UTF_8).replace("activeLimit=32","activeLimit=33")
    assertNotEquals(Files.readString(file,StandardCharsets.UTF_8),text)
    val changed=MySqlRuntimeMetadata.fromBytesV2(text.getBytes(StandardCharsets.UTF_8))
    val root=V2Fixture.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val failure=assertThrows(classOf[IllegalArgumentException],()=>MySqlRuntimeInputs.assembleCertifiedOrdinaryV2(changed,owner,boot,7L,V2Fixture.auditPolicy,original))
    assertTrue(failure.getMessage.contains("complete current A"));assertEquals(0L,root.accountedMemoryBytes);root.closeRootReadBudget()
    val nextRoot=V2Fixture.root();val next=MySqlRuntimeInputs.beginV2(nextRoot,Thread.currentThread())
    val inputs=MySqlRuntimeInputs.assembleV2(m,next,boot,7L,V2Fixture.auditPolicy,p)
    assertThrows(classOf[IllegalStateException],()=>MySqlRuntimeInputs.assembleV2(m,next,boot,7L,V2Fixture.auditPolicy,p))
    assertSame(m,inputs.originalMetadataV2)
    assertTrue(next.failure.isEmpty)
    next.close();assertEquals(0L,nextRoot.accountedMemoryBytes);nextRoot.closeRootReadBudget()

  private def g4LongUncMetadata(): Array[Byte] =
    val properties=new java.util.Properties
    properties.load(new java.io.StringReader(Files.readString(temporary.resolve("v2-metadata.properties"),StandardCharsets.UTF_8)))
    properties.setProperty("pkcs12TrustStorePath","\\\\host\\"+("s"*30000)+"中\\leaf.p12")
    val writer=new java.io.StringWriter
    properties.store(writer,null)
    // 同一真实Properties输入同时包含非Latin1与Unicode转义；仅Path语法解析，不访问UNC网络。
    writer.toString.replace("中","\\u4E2D中").getBytes(StandardCharsets.UTF_8)

  private def g4WorkspaceNumbers(m: MySqlRuntimeMetadata.FullV2,root: MySqlFormat3ReadBudget): Unit =
    var chars=4096L
    val entries=m.deploymentSemantics().canonicalFields().entrySet().iterator()
    while entries.hasNext do
      val entry=entries.next();chars+=2L+entry.getKey.length+entry.getValue.length
    chars+=m.baseline().absolutePath().toString.length+m.format3RuntimeContract().absolutePath().toString.length
    val raw=21L*65536L+15L*(4096L+2L*65536L)+65536L
    val approved=16L*chars
    System.err.println("G4_WORKSPACE_DIAGNOSTIC retained="+root.accountedMemoryBytes+" cap="+V2Fixture.sessionProfile.maxRetainedBytes()+
      " rawPeak="+raw+" approved16C="+approved+" C="+chars+" totalRequired="+(root.accountedMemoryBytes+raw+approved))

  @Test def g4ExactLedgerAndPrepaidLeaseGuardRealSessionAllocation(): Unit =
    val (m,p)=V2Fixture.fixture();val ledger=new MySqlFormat3AllocationLedger(V2Fixture.sessionProfile.maxRetainedBytes())
    val root=MySqlFormat3ReadBudget.rootOnLedger(V2Fixture.sessionProfile,V2Fixture.sessionProfile.maxSqlPacketBytes(),ledger)
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val inputs=MySqlRuntimeInputs.assembleV2(m,owner,boot,7L,V2Fixture.auditPolicy,p)
    val foreign=new MySqlFormat3AllocationLedger(V2Fixture.sessionProfile.maxRetainedBytes())
    assertThrows(classOf[IllegalArgumentException],()=>owner.borrowForSession(foreign))
    val lease=owner.borrowForSession(ledger)
    val request=V2Fixture.freshRequest(m,p,boot,7L)
    assertThrows(classOf[IllegalStateException],()=>MySqlOwnedDatabaseSession.allocateFreshV2(inputs,1L,Duration.ofSeconds(20),request,lease,ledger))
    owner.reserveSessionGraph()
    assertThrows(classOf[IllegalArgumentException],()=>MySqlOwnedDatabaseSession.allocateFreshV2(inputs,1L,Duration.ofSeconds(20),request,lease,foreign))
    assertThrows(classOf[IllegalArgumentException],()=>MySqlOwnedDatabaseSession.allocateFreshV2(inputs,0L,Duration.ofSeconds(20),request,lease,ledger))
    assertThrows(classOf[IllegalStateException],()=>owner.close())
    lease.close();owner.close();assertEquals(0L,ledger.accountedBytes);root.closeRootReadBudget()

  @Test def g4ActualUninitializedFreshSessionAdoptsLeaseAndDropsRealFieldsBeforeOwnerRoot(): Unit =
    val (m,p)=V2Fixture.fixture();val ledger=new MySqlFormat3AllocationLedger(V2Fixture.sessionProfile.maxRetainedBytes())
    val root=MySqlFormat3ReadBudget.rootOnLedger(V2Fixture.sessionProfile,V2Fixture.sessionProfile.maxSqlPacketBytes(),ledger)
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val inputs=MySqlRuntimeInputs.assembleV2(m,owner,boot,7L,V2Fixture.auditPolicy,p)
    val lease=owner.borrowForSession(ledger);owner.reserveSessionGraph()
    g4WorkspaceNumbers(m,root)
    val request=V2Fixture.freshRequest(m,p,boot,7L)
    val session=MySqlOwnedDatabaseSession.allocateFreshV2(inputs,1L,Duration.ofSeconds(20),request,lease,ledger)
    assertThrows(classOf[IllegalStateException],()=>session.retireV2GraphForExit())
    assertThrows(classOf[IllegalStateException],()=>owner.close())
    val report=session.closeForWorkerExit(None)
    assertFalse(report.connectionOpened);assertFalse(report.unresolvedOwnedResponsibility)
    session.retireV2GraphForExit()
    owner.close();assertEquals(0L,ledger.accountedBytes);root.closeRootReadBudget()
    assertThrows(classOf[IllegalStateException],()=>inputs.auditExpectation)

  @Test def g4ActualUninitializedOrdinaryActorReleasesSeparateExpectationReference(): Unit =
    val (m,p)=V2Fixture.fixture();val original=V2Fixture.original(m,p)
    val ledger=new MySqlFormat3AllocationLedger(V2Fixture.sessionProfile.maxRetainedBytes())
    val root=MySqlFormat3ReadBudget.rootOnLedger(V2Fixture.sessionProfile,V2Fixture.sessionProfile.maxSqlPacketBytes(),ledger)
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val inputs=MySqlRuntimeInputs.assembleCertifiedOrdinaryV2(m,owner,boot,7L,V2Fixture.auditPolicy,original)
    val lease=owner.borrowForSession(ledger);owner.reserveSessionGraph()
    val identity=new ProductionBootstrapControl.OrdinaryIdentity(boot,7L,1L,UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID())
    val request=MySqlOrdinaryCertifiedBootRequest(original.worldPath(),original,new WindowsWorldProvisioner.BootIdentity(boot,7L),identity,
      MySqlFreshControlledMetadata(temporary.resolve("v2-metadata.properties").toAbsolutePath,original.configurationSha256()),original.runtimeContractSha256())
    val session=MySqlOwnedDatabaseSession.allocateCertifiedOrdinaryV2(inputs,1L,Duration.ofSeconds(20),request,lease,ledger)
    val report=session.closeForWorkerExit(None)
    assertFalse(report.connectionOpened);assertFalse(report.unresolvedOwnedResponsibility)
    session.retireV2GraphForExit();owner.close();assertEquals(0L,ledger.accountedBytes);root.closeRootReadBudget()

  @Test def g4RealWorkerStaticV3MissingBaselinePreservesFailureAndSettlesPreSessionCustody(): Unit =
    val (m,p)=V2Fixture.fixture();val id=UUID.randomUUID();val operation=UUID.randomUUID()
    val control=V2Fixture.control(m,p,id,operation)
    Files.delete(m.baseline().absolutePath())
    val identity=MySqlOwnedIoIdentity(id,7L,1L)
    val adapter=new MySqlOwnedIoAdapter(1,Thread.currentThread(),"g4-pure-missing-baseline",identity)
    val request=V2Fixture.freshRequest(m,p,id,7L)
    assertEquals(IoSubmission.Accepted,adapter.submit(IoOperationTag(id,7L,1L,operation),MySqlOwnedIoCommand.BootFreshV3(control,request)))
    adapter.quiesce()
    val failure=new AtomicReference[Throwable]()
    val waiter=new Thread(()=>
      try assertTrue(adapter.awaitExitOffMain(5L,TimeUnit.SECONDS))
      catch case f:Throwable => failure.set(f)
      ())
    waiter.start();waiter.join(6000L);assertFalse(waiter.isAlive)
    if failure.get()!=null then throw failure.get()
    val completed=adapter.drain(1);assertEquals(1,completed.size)
    val cause=completed.head.result.failed.get
    assertInstanceOf(classOf[java.sql.SQLNonTransientConnectionException],cause)
    assertTrue(cause.getMessage.contains("owned baseline file IO failed"),cause.getMessage)
    assertInstanceOf(classOf[java.nio.file.NoSuchFileException],cause.getCause)
    adapter.checkpoint.resources match
      case Some(MySqlOwnedIoResourceObservation.StaticV3AssemblyObserved(saved,unsettled)) =>
        assertSame(cause,saved.get);assertFalse(unsettled)
      case other => fail("actual worker did not retain static V3 assembly outcome: "+other)
    assertThrows(classOf[IllegalArgumentException],()=>control.consumeFreshIdentityOnce())

  @Test def g4RawUnknownAndKnownPropertiesUseIndependentPrepaidWorkspace(): Unit =
    val (m,p)=V2Fixture.fixture();val root=V2Fixture.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    MySqlRuntimeInputs.assembleV2(m,owner,boot,7L,V2Fixture.auditPolicy,p)
    owner.reserveSessionGraph()
    g4WorkspaceNumbers(m,root)
    val before=root.accountedMemoryBytes
    val unknown=(("x"*60000)+"\\u4E2D中=\\u4E01\n").getBytes(StandardCharsets.UTF_8)
    assertTrue(unknown.length<=65536)
    val unknownFailure=assertThrows(classOf[IllegalArgumentException],()=>owner.withConfigurationWorkspace {
      MySqlRuntimeMetadata.fromBytesV2(unknown)
    })
    assertTrue(unknownFailure.getMessage.contains("unknown full V2 metadata property"))
    assertEquals(before,root.accountedMemoryBytes)
    val longValue=("v"*55000)+"\\u4E2D中"
    val original=Files.readString(temporary.resolve("v2-metadata.properties"),StandardCharsets.UTF_8)
    val known=original.linesIterator.map(line=>if line.startsWith("username=") then "username="+longValue else line).mkString("\n").getBytes(StandardCharsets.UTF_8)
    assertTrue(known.length<=65536)
    owner.withConfigurationWorkspace {
      val parsed=MySqlRuntimeMetadata.fromBytesV2(known)
      assertTrue(parsed.username().endsWith("中中"))
      assertEquals(55002,parsed.username().length)
      val actual=WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(parsed)
      assertNotEquals(WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(m).configurationSha256(),actual.configurationSha256())
    }
    assertEquals(before,root.accountedMemoryBytes)
    val unc=g4LongUncMetadata();assertTrue(unc.length<=65536)
    owner.withConfigurationWorkspace {
      val parsed=MySqlRuntimeMetadata.fromBytesV2(unc)
      assertTrue(parsed.pkcs12TrustStorePath().isAbsolute)
      assertTrue(parsed.pkcs12TrustStorePath().getRoot.toString.length>30000)
      assertTrue(parsed.pkcs12TrustStorePath().toString.contains("中中"))
      assertNotNull(WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(parsed).configurationSha256())
    }
    assertEquals(before,root.accountedMemoryBytes)
    owner.close();assertEquals(0L,root.accountedMemoryBytes);root.closeRootReadBudget()

  @Test def g4RawPropertiesCapRejectsBeforeParserAndPreservesRealPathCause(): Unit =
    val (m,p)=V2Fixture.fixture();val root=V2Fixture.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    MySqlRuntimeInputs.assembleV2(m,owner,boot,7L,V2Fixture.auditPolicy,p)
    owner.reserveSessionGraph()
    g4WorkspaceNumbers(m,root)
    val before=root.accountedMemoryBytes
    val filler=V2Fixture.sessionProfile.maxRetainedBytes()-before-1024L
    assertTrue(filler>0L)
    root.reserveRetainedMemory(filler,"pure test actual competing live reservation")
    val malformed=g4LongUncMetadata()
    var enteredParser=false
    val capFailure=assertThrows(classOf[Exception],()=>owner.withConfigurationWorkspace {
      enteredParser=true
      MySqlRuntimeMetadata.fromBytesV2(malformed)
    })
    assertFalse(enteredParser)
    assertTrue(capFailure.getMessage.contains("configuration codec workspace"),capFailure.getMessage)
    assertEquals(before+filler,root.accountedMemoryBytes)
    root.releaseRetainedMemory(filler,"pure test competing reservation dropped")
    val original=Files.readString(temporary.resolve("v2-metadata.properties"),StandardCharsets.UTF_8)
    val badPath=original.linesIterator.map(line=>if line.startsWith("pkcs12TrustStorePath=") then "pkcs12TrustStorePath=C:\\\\bad\\u0000" else line).mkString("\n").getBytes(StandardCharsets.UTF_8)
    val pathFailure=assertThrows(classOf[IllegalArgumentException],()=>owner.withConfigurationWorkspace {
      MySqlRuntimeMetadata.fromBytesV2(badPath)
    })
    assertEquals("configuration file path is malformed",pathFailure.getMessage)
    assertInstanceOf(classOf[java.nio.file.InvalidPathException],pathFailure.getCause)
    assertTrue(pathFailure.getCause.getStackTrace.nonEmpty)
    assertEquals(before,root.accountedMemoryBytes)
    owner.close();assertEquals(0L,root.accountedMemoryBytes);root.closeRootReadBudget()

  /** 纯内存叶片使用真实未初始化Session；不发行admission或Fresh/native完成资格。 */
  @Test def freshUnusedIssuerLeafDropsActualReferencesAndRejectsClosedObservation(): Unit =
    val (m,p)=V2Fixture.fixture()
    val ledger=new MySqlFormat3AllocationLedger(V2Fixture.sessionProfile.maxRetainedBytes())
    val root=MySqlFormat3ReadBudget.rootOnLedger(V2Fixture.sessionProfile,V2Fixture.sessionProfile.maxSqlPacketBytes(),ledger)
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val inputs=MySqlRuntimeInputs.assembleV2(m,owner,boot,7L,V2Fixture.auditPolicy,p)
    val lease=owner.borrowForSession(ledger);owner.reserveSessionGraph()
    val session=MySqlOwnedDatabaseSession.allocateFreshV2(inputs,1L,Duration.ofSeconds(20),V2Fixture.freshRequest(m,p,boot,7L),lease,ledger)
    val holder=new java.util.concurrent.atomic.AtomicReference[MySqlOwnedRetainedPlanIssuer]()
    val mainFailure=new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val actualMain=Thread.ofPlatform().unstarted(()=>{
      try holder.get().sealUnusedForFreshCompletion()
      catch case cause: Throwable => mainFailure.set(cause)
    })
    val profile=inputs.format3Contract.limitProfile
    val issuer=new MySqlOwnedRetainedPlanIssuer(session,MySqlOwnedIoIdentity(boot,7L,1L),Thread.currentThread(),actualMain,profile,ledger,1)
    holder.set(issuer)
    val before=ledger.accountedBytes
    assertThrows(classOf[IllegalArgumentException],()=>issuer.detachUnusedFreshReferencesForExit(session,profile,ledger))
    val foreign=new MySqlFormat3AllocationLedger(V2Fixture.sessionProfile.maxRetainedBytes())
    assertThrows(classOf[IllegalArgumentException],()=>issuer.detachUnusedFreshReferencesForExit(session,profile,foreign))
    assertEquals(before,ledger.accountedBytes)
    actualMain.start();actualMain.join(5000L);assertFalse(actualMain.isAlive);assertNull(mainFailure.get())
    // 原seal不是综合门：真实未初始化Session必须继续拒绝，不能人为设置prepared/closeObserved。
    val composite=assertThrows(classOf[IllegalArgumentException],()=>session.detachUnusedFreshIssuerForExit(issuer))
    assertTrue(composite.getMessage.contains("actual Fresh completion preparation"))
    assertEquals(before,issuer.observation.accountedBytes)
    assertThrows(classOf[IllegalStateException],()=>owner.close())
    issuer.detachUnusedFreshReferencesForExit(session,profile,ledger)
    // 实际退休后的重复停止不得将合法清理变成 suppressed failure。
    issuer.stop()
    issuer.stop()
    assertEquals(before,ledger.accountedBytes,"撤引用不释放Session借用或另签账本结算")
    assertThrows(classOf[IllegalStateException],()=>issuer.observation)
    assertThrows(classOf[IllegalStateException],()=>issuer.accountedBytes)
    assertThrows(classOf[IllegalStateException],()=>issuer.requireSession(session,profile,ledger))
    assertThrows(classOf[IllegalStateException],()=>issuer.detachUnusedFreshReferencesForExit(session,profile,ledger))
    val report=session.closeForWorkerExit(None)
    assertFalse(report.connectionOpened);assertFalse(report.unresolvedOwnedResponsibility)
    session.retireV2GraphForExit();owner.close();assertEquals(0L,ledger.accountedBytes);root.closeRootReadBudget()

  @Test def freshUnusedIssuerObservationRaceIsCurrentOrExplicitlyRetiredNeverZero(): Unit =
    val (m,p)=V2Fixture.fixture()
    val ledger=new MySqlFormat3AllocationLedger(V2Fixture.sessionProfile.maxRetainedBytes())
    val root=MySqlFormat3ReadBudget.rootOnLedger(V2Fixture.sessionProfile,V2Fixture.sessionProfile.maxSqlPacketBytes(),ledger)
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val inputs=MySqlRuntimeInputs.assembleV2(m,owner,boot,7L,V2Fixture.auditPolicy,p)
    val lease=owner.borrowForSession(ledger);owner.reserveSessionGraph()
    val session=MySqlOwnedDatabaseSession.allocateFreshV2(inputs,1L,Duration.ofSeconds(20),V2Fixture.freshRequest(m,p,boot,7L),lease,ledger)
    val holder=new java.util.concurrent.atomic.AtomicReference[MySqlOwnedRetainedPlanIssuer]()
    val mainFailure=new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val actualMain=Thread.ofPlatform().unstarted(()=>{
      try holder.get().sealUnusedForFreshCompletion()
      catch case cause: Throwable => mainFailure.set(cause)
    })
    val profile=inputs.format3Contract.limitProfile
    val issuer=new MySqlOwnedRetainedPlanIssuer(session,MySqlOwnedIoIdentity(boot,7L,1L),Thread.currentThread(),actualMain,profile,ledger,1)
    holder.set(issuer);actualMain.start();actualMain.join(5000L);assertFalse(actualMain.isAlive);assertNull(mainFailure.get())
    val before=ledger.accountedBytes
    val sampled=new java.util.concurrent.CountDownLatch(1)
    val observerFailure=new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val observedRetired=new java.util.concurrent.CountDownLatch(1)
    val observer=Thread.ofPlatform().start(()=>{
      try
        var retired=false
        val deadline=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(5L)
        while !retired && System.nanoTime()<deadline do
          try
            val observation=issuer.observation
            assertEquals(before,observation.accountedBytes)
            sampled.countDown()
            Thread.`yield`()
          catch
            case closed: IllegalStateException if closed.getMessage=="retained issuer graph was retired" =>
              retired=true
              observedRetired.countDown()
        assertTrue(retired,"有界观察窗口内必须看到明确退役")
      catch case cause: Throwable => observerFailure.set(cause)
    })
    assertTrue(sampled.await(5,java.util.concurrent.TimeUnit.SECONDS))
    issuer.detachUnusedFreshReferencesForExit(session,profile,ledger)
    assertTrue(observedRetired.await(5,java.util.concurrent.TimeUnit.SECONDS))
    observer.join(5000L);assertFalse(observer.isAlive);assertNull(observerFailure.get())
    assertEquals(before,ledger.accountedBytes)
    val report=session.closeForWorkerExit(None);assertFalse(report.connectionOpened)
    session.retireV2GraphForExit();owner.close();assertEquals(0L,ledger.accountedBytes);root.closeRootReadBudget()

  @Test def ordinaryApplicationAliasesShareUnpublishedBackingAndOriginalFailure(): Unit =
    // 仅验证未发布的真实表示与失败别名；不伪造Session发行或Main退休资格。
    val backing = new MySqlOwnedIoAdapter.OrdinaryBootResultBacking(null, null, null, null)
    val original = new MySqlOwnedIoReply.OrdinaryCertifiedBooted(backing)
    val copied = original.copy() match
      case value: MySqlOwnedIoReply.OrdinaryCertifiedBooted => value
      case _ => throw new AssertionError("Ordinary case copy changed case")
    assertSame(backing, copied.owned)
    assertSame(backing, original.productElement(0))
    assertSame(backing, copied.productIterator.next())
    assertThrows(classOf[IllegalArgumentException], () => original.owned.databaseAdmission)
    val cause = new IllegalStateException("actual partial application result failure", new Exception("specified cause"))
    backing.retainFailure(cause)
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => copied.owned.databaseAdmission))
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => original.owned.fullTwentyRelationSnapshot))
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => copied.owned.ordinaryFacts))
    assertSame(cause, assertThrows(classOf[IllegalStateException], () => original.owned.actualSourceLimitProfile))
    assertThrows(classOf[IllegalArgumentException], () => backing.publish())

  @Test def ordinaryApplicationFailurePublicationIsVisibleThroughSharedMonitor(): Unit =
    val backing = new MySqlOwnedIoAdapter.OrdinaryBootResultBacking(null, null, null, null)
    val cause = new IllegalStateException("original shared backing failure")
    val observed = new AtomicReference[Throwable]()
    val writer = new Thread(() => backing.retainFailure(cause))
    writer.start()
    writer.join(5000L)
    assertFalse(writer.isAlive)
    val reader = new Thread(() =>
      try backing.ordinaryFacts
      catch case actual: Throwable => observed.set(actual)
      ()
    )
    reader.start()
    reader.join(5000L)
    assertFalse(reader.isAlive)
    assertSame(cause, observed.get())

  @Test def v3AssemblyRejectsV2KAndKeepsActualCallerOwnerCause(): Unit =
    val (original,plan)=V2Fixture.fixture()
    val file=temporary.resolve("v2-metadata.properties")
    val text=Files.readString(file,StandardCharsets.UTF_8).replace(MySqlRuntimeMetadata.MANIFEST_V2,MySqlRuntimeMetadata.MANIFEST_V3)
    val metadata=MySqlRuntimeMetadata.fromBytesV2(text.getBytes(StandardCharsets.UTF_8))
    val root=V2Fixture.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    try
      val cause=assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>
        MySqlRuntimeInputs.assembleV2(metadata,owner,boot,7L,V2Fixture.auditPolicy,plan))
      assertTrue(cause.getMessage.contains("contains an unsupported field"))
      assertSame(cause,owner.failure.get)
      assertFalse(owner.hasUnsettledResources)
      assertEquals(0L,root.accountedMemoryBytes)
    finally
      owner.close();root.closeRootReadBudget()

  /** 只重绑定独立 synthetic DATA；不修改共享 fixture，不形成部署批准或数据库观察。 */
  private def v3TemporaryPack(): (MySqlRuntimeMetadata.FullV2, WindowsWorldProvisioner.Plan, Array[Byte]) =
    import scala.jdk.CollectionConverters.*
    val (original, _) = V2Fixture.fixture()
    val directory = temporary.resolve("v3-positive-pack").toAbsolutePath
    Files.createDirectories(directory)
    val gson = new GsonBuilder().disableHtmlEscaping().create()
    val oldSchema = original.schema()
    val schema = "fp_test_20261007_format3_b2"
    def sha(bytes: Array[Byte]): String = HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
    def hex(bytes: Array[Byte]): String = HexFormat.of().withUpperCase().formatHex(bytes)
    def readJson(path: Path): JsonObject = com.google.gson.JsonParser.parseString(Files.readString(path, StandardCharsets.UTF_8)).getAsJsonObject
    val pack = readJson(original.baseline().absolutePath())
    val schemaColumns = Map(
      "RS02_TABLES" -> Set("TABLE_SCHEMA"),
      "RS03_COLUMNS" -> Set("TABLE_SCHEMA"),
      "RS04_STATISTICS" -> Set("TABLE_SCHEMA", "INDEX_SCHEMA"),
      "RS05_TABLE_CONSTRAINTS" -> Set("CONSTRAINT_SCHEMA", "TABLE_SCHEMA"),
      "RS06_KEY_COLUMN_USAGE" -> Set("CONSTRAINT_SCHEMA", "TABLE_SCHEMA", "REFERENCED_TABLE_SCHEMA"),
      "RS07_REFERENTIAL_CONSTRAINTS" -> Set("CONSTRAINT_SCHEMA", "UNIQUE_CONSTRAINT_SCHEMA"),
      "RS08_CHECK_CONSTRAINTS" -> Set("CONSTRAINT_SCHEMA"),
      "RS09_TRIGGERS" -> Set("TRIGGER_SCHEMA", "EVENT_OBJECT_SCHEMA"),
      "RS10_AUDITSELF" -> Set("ROUTINE_SCHEMA"),
      "RS11_AUDITSELF_PARAMETERS" -> Set("SPECIFIC_SCHEMA"))
    val sets = pack.getAsJsonArray("resultSets")
    assertEquals(MySqlOrdinaryCertifiedHandoffFixture.StaticResultSetIds,
      sets.iterator().asScala.map(_.getAsJsonObject.get("id").getAsString).toVector)
    sets.iterator().asScala.foreach { raw =>
      val set = raw.getAsJsonObject
      val id = set.get("id").getAsString
      val labels = set.getAsJsonArray("columns").iterator().asScala.map(_.getAsJsonObject.get("label").getAsString).toVector
      val rows = set.getAsJsonArray("rows")
      if id == "RS07_REFERENTIAL_CONSTRAINTS" || id == "RS11_AUDITSELF_PARAMETERS" then assertEquals(0, rows.size())
      rows.iterator().asScala.foreach { rawRow =>
        val row = rawRow.getAsJsonArray
        labels.zipWithIndex.foreach { (label, index) =>
          if schemaColumns(id).contains(label) then
            val cell = row.get(index).getAsJsonObject
            if cell.get("tag").getAsString != "SQL_NULL" then
              assertEquals("TEXT", cell.get("tag").getAsString)
              assertEquals(oldSchema, cell.get("value").getAsString)
              cell.addProperty("value", schema)
        }
        if id == "RS10_AUDITSELF" then
          val bodyCell = row.get(labels.indexOf("ROUTINE_DEFINITION")).getAsJsonObject
          val before = bodyCell.get("value").getAsString
          assertTrue(before.contains(oldSchema))
          val body = before.replace(oldSchema, schema)
          val bytes = body.getBytes(StandardCharsets.UTF_8)
          bodyCell.addProperty("value", body)
          row.get(labels.indexOf("definition_octets")).getAsJsonObject.addProperty("value", bytes.length.toString)
          row.get(labels.indexOf("definition_sha256")).getAsJsonObject.addProperty("hex", sha(bytes))
        else if id == "RS08_CHECK_CONSTRAINTS" || id == "RS09_TRIGGERS" then
          val bodyLabel = if id == "RS08_CHECK_CONSTRAINTS" then "CHECK_CLAUSE" else "ACTION_STATEMENT"
          assertFalse(row.get(labels.indexOf(bodyLabel)).getAsJsonObject.get("value").getAsString.contains(oldSchema))
      }
    }
    val resource = this.getClass.getResourceAsStream("/com/wi/floatingpod/storage/mysql/approved-static-baseline-format3-ordinary-fixture.json")
    assertNotNull(resource)
    val sourceBytes = try resource.readAllBytes() finally resource.close()
    assertEquals("34D10D2AEB0C73241C67E5A0D7EFD86752F7F7C43DA16F27765DA4A748BADF74", sha(sourceBytes))
    val reboundSource = new String(sourceBytes, StandardCharsets.UTF_8).replace(oldSchema, schema)
      .replace("fp_runtime@localhost", "fp_f3_runtime@localhost").getBytes(StandardCharsets.UTF_8)
    val sourceContract = com.google.gson.JsonParser.parseString(new String(reboundSource, StandardCharsets.UTF_8))
      .getAsJsonObject.getAsJsonObject("_ordinarySyntheticContract")
    assertEquals("SYNTHETIC_ORDINARY_TEST_ONLY", sourceContract.get("attribution").getAsString)
    assertEquals(schema, sourceContract.get("schema").getAsString)
    Files.write(directory.resolve("independent-source-fixture.json"), reboundSource)
    val ddlBytes = new String(MySqlOrdinaryCertifiedHandoffFixture.syntheticDdlBytes(), StandardCharsets.UTF_8)
      .replace(oldSchema, schema).getBytes(StandardCharsets.UTF_8)
    Files.write(directory.resolve("independent-ddl.sql"), ddlBytes)
    val expectedIdentity = MySqlApprovedBaselineIdentity(original.baseline().format(), 3, original.baseline().coverage(),
      original.expectedServerUuid(), original.baseline().serverVersion(), schema, original.baseline().provisioningUuid(),
      original.expectedBindingUuid(), sha(ddlBytes), original.baseline().auditDefiner(), sha(reboundSource))
    pack.addProperty("schema", schema)
    pack.addProperty("ddlSha256", expectedIdentity.ddlSha256)
    pack.addProperty("sourceCaptureSha256", expectedIdentity.sourceCaptureSha256)
    val baselinePath = directory.resolve("baseline.json")
    val baselineBytes = gson.toJson(pack).getBytes(StandardCharsets.UTF_8)
    Files.write(baselinePath, baselineBytes)
    val baseline = MySqlApprovedBaselineLoader.load(baselinePath, sha(baselineBytes), expectedIdentity)
    assertEquals(expectedIdentity, baseline.identity)
    assertEquals(10, baseline.objects.resultSets.size)
    val plan = V2Fixture.actualPlan(baseline)
    val contract = readJson(original.format3RuntimeContract().absolutePath())
    Map("format" -> "FP_MYSQL_FORMAT3_RUNTIME_CONTRACT_V3",
      "format3ManifestConfigurationEncoding" -> MySqlRuntimeMetadata.MANIFEST_V3,
      "schema" -> schema, "effectiveUser" -> "fp_f3_runtime@localhost", "connectedUser" -> "fp_f3_runtime@127.0.0.1",
      "approvedBaselineSha256" -> sha(baselineBytes), "ddlSha256" -> expectedIdentity.ddlSha256,
      "sourceCaptureSha256" -> expectedIdentity.sourceCaptureSha256).foreach((key, value) => contract.addProperty(key, value))
    contract.remove("callerGrants")
    contract.add("callerPrivileges", CallerPrivilegesV3TestData.json)
    contract.getAsJsonArray("definerPrivileges").iterator().asScala.foreach { raw =>
      val set = raw.getAsJsonObject
      val labels = set.getAsJsonArray("columns").iterator().asScala.map(_.getAsJsonObject.get("label").getAsString).toVector
      val index = labels.indexOf("TABLE_SCHEMA")
      if index >= 0 then set.getAsJsonArray("rows").iterator().asScala.foreach { row =>
        val cell = row.getAsJsonArray.get(index).getAsJsonObject
        assertEquals(oldSchema, cell.get("value").getAsString)
        cell.addProperty("value", schema)
      }
    }
    val provenance = MySqlFormat3FreshPlanProvenance.canonicalBytes(plan)
    contract.addProperty("sourceProvenanceHex", hex(provenance))
    contract.addProperty("sourceProvenanceSha256", sha(provenance))
    assertFalse(contract.has("configurationSha256"))
    val contractPath = directory.resolve("contract-v3.json")
    val contractBytes = gson.toJson(contract).getBytes(StandardCharsets.UTF_8)
    Files.write(contractPath, contractBytes)
    val properties = new java.util.Properties
    val reader = Files.newBufferedReader(temporary.resolve("v2-metadata.properties"), StandardCharsets.UTF_8)
    try properties.load(reader) finally reader.close()
    Map("schema" -> schema, "expectedEffectiveUser" -> "fp_f3_runtime@localhost", "username" -> "fp_f3_runtime",
      "approvedBaselinePath" -> baselinePath.toString, "approvedBaselineSha256" -> sha(baselineBytes),
      "approvedDdlSha256" -> expectedIdentity.ddlSha256, "approvedSourceCaptureSha256" -> expectedIdentity.sourceCaptureSha256,
      "format3RuntimeContractPath" -> contractPath.toString, "format3RuntimeContractSha256" -> sha(contractBytes),
      "format3ManifestConfigurationEncoding" -> MySqlRuntimeMetadata.MANIFEST_V3).foreach((key, value) => properties.setProperty(key, value))
    val writer = new java.io.StringWriter
    properties.store(writer, null)
    val metadataPath = directory.resolve("metadata-v3.properties")
    Files.writeString(metadataPath, writer.toString, StandardCharsets.UTF_8)
    val metadata = MySqlRuntimeMetadata.loadV2(metadataPath)
    val metadataKeys = "backend host port schema expectedServerUuid expectedBindingUuid expectedEffectiveUser username pkcs12TrustStorePath credentialSourceRef trustSecretSourceRef connectTimeoutMillis socketTimeoutMillis activeLimit innodbFlushLogAtTrxCommit syncBinlog approvedBaselinePath approvedBaselineFormat approvedSchemaFormat approvedCoverage expectedServerVersion expectedProvisioningUuid approvedDdlSha256 expectedAuditDefiner format3RuntimeContractPath format3ManifestConfigurationEncoding".split(" ").toVector
    val metadataProjection = new JsonObject
    metadataKeys.foreach { key =>
      val value = properties.getProperty(key)
      assertNotNull(value, key)
      metadataProjection.addProperty(key, value)
    }
    val contractProjection = contract.deepCopy()
    contractProjection.remove("approvedBaselineSha256")
    contractProjection.remove("sourceCaptureSha256")
    assertEquals(26, metadataProjection.size())
    assertEquals(26, contractProjection.size())
    val recipe = new JsonObject
    recipe.addProperty("format", "FP_MYSQL_FORMAT3_CONFIGURATION_RECIPE_V3")
    recipe.add("metadata", metadataProjection)
    recipe.add("contract", contractProjection)
    // 独立测试 renderer 与 JDK framing 形成 golden，不调用产品 canonical writer。
    def render(value: com.google.gson.JsonElement): String =
      if value.isJsonObject then
        val obj = value.getAsJsonObject
        obj.keySet().asScala.toVector.sorted.map(key => gson.toJson(key) + ":" + render(obj.get(key))).mkString("{", ",", "}")
      else if value.isJsonArray then value.getAsJsonArray.iterator().asScala.map(render).mkString("[", ",", "]")
      else gson.toJson(value)
    (metadata, plan, render(recipe).getBytes(StandardCharsets.UTF_8))

  @Test def v3ActualFilesBuildIndependentCAndKeepExactPrivilegeOwnerUntilConsumerClose(): Unit =
    val (metadata, plan, recipeBytes) = v3TemporaryPack()
    val root = V2Fixture.root()
    val owner = MySqlRuntimeInputs.beginV2(root, Thread.currentThread())
    var lease: AutoCloseable = null
    var primary: Throwable = null
    try
      val preimage = new java.io.ByteArrayOutputStream
      val framing = new java.io.DataOutputStream(preimage)
      val domain = "FP_MYSQL_FORMAT3_MANIFEST_CONFIGURATION_BE_V3".getBytes(StandardCharsets.UTF_8)
      framing.writeInt(domain.length); framing.write(domain)
      framing.writeInt(recipeBytes.length); framing.write(recipeBytes); framing.flush()
      val expectedC = HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("SHA-256").digest(preimage.toByteArray))
      val independentRoot = V2Fixture.root()
      var semantic: MySqlManifestConfigurationIdentity.ManifestSha256 = null
      var semanticFailure: Throwable = null
      try
        semantic = MySqlConfigurationRecipeCodec.parseSemanticV3(recipeBytes, V2Fixture.auditPolicy, independentRoot)
        assertEquals(expectedC, semantic.hex)
      catch
        case failure: Throwable => semanticFailure = failure; throw failure
      finally
        var semanticCleanup: Throwable = null
        try
          if semantic != null then semantic.close()
          assertEquals(0L, independentRoot.accountedMemoryBytes)
        catch case failure: Throwable => semanticCleanup = failure
        try independentRoot.closeRootReadBudget()
        catch case failure: Throwable =>
          if semanticCleanup == null then semanticCleanup = failure
          else if failure ne semanticCleanup then semanticCleanup.addSuppressed(failure)
        if semanticCleanup != null then
          if semanticFailure == null then throw semanticCleanup
          else if semanticCleanup ne semanticFailure then semanticFailure.addSuppressed(semanticCleanup)
      val inputs = MySqlRuntimeInputs.assembleV2(metadata, owner, boot, 7L, V2Fixture.auditPolicy, plan)
      assertSame(metadata, inputs.originalMetadataV2)
      assertEquals("fp_test_20261007_format3_b2", inputs.approvedBaseline.identity.schema)
      val audit = inputs.auditExpectation.format3Inputs.get
      assertSame(root, audit.privilegeRoot)
      assertNotNull(audit.callerPrivileges)
      assertEquals(58, audit.callerPrivileges.grants.size)
      assertEquals(expectedC, HexFormat.of().withUpperCase().formatHex(audit.configurationSha256.bytes))
      assertNotEquals(WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadataV2(metadata).configurationSha256(), expectedC)
      val caller = MySqlCallerScope(metadata.expectedServerUuid(), 17L, metadata.schema(), "fp_f3_runtime@localhost", "NONE",
        "fp_f3_runtime@127.0.0.1", Some(metadata.baseline().serverVersion()), CallerPrivilegesV3TestData.raw())
      audit.validateCapturedCallerGrants(caller.ownGrants)
      audit.validateSecurity(caller, caller, audit.definerPrivileges)
      assertTrue(root.accountedMemoryBytes > 512L)
      lease = owner.borrow()
      assertThrows(classOf[IllegalStateException], () => owner.close())
      assertSame(metadata, inputs.originalMetadataV2)
      lease.close(); lease = null
      owner.close()
      assertEquals(0L, root.accountedMemoryBytes)
      assertFalse(owner.hasUnsettledResources)
      assertThrows(classOf[IllegalStateException], () => inputs.settings)
      assertThrows(classOf[IllegalStateException], () => inputs.originalMetadataV2)
    catch
      case failure: Throwable => primary = failure; throw failure
    finally
      var cleanupFailure: Throwable = null
      try if lease != null then lease.close()
      catch case failure: Throwable => cleanupFailure = failure
      try owner.close()
      catch case failure: Throwable =>
        if cleanupFailure == null then cleanupFailure = failure
        else if failure ne cleanupFailure then cleanupFailure.addSuppressed(failure)
      try root.closeRootReadBudget()
      catch case failure: Throwable =>
        if cleanupFailure == null then cleanupFailure = failure
        else if failure ne cleanupFailure then cleanupFailure.addSuppressed(failure)
      if cleanupFailure != null then
        if primary == null then throw cleanupFailure
        else if cleanupFailure ne primary then primary.addSuppressed(cleanupFailure)

  @Test def schema5CallerClosedRootLeavesExplicitUnsettledCleanupWithoutFakeDropAck(): Unit =
    val f=DeploymentReceiptStaticTestData.fixture(temporary.resolve("schema5-closed-root").toAbsolutePath)
    val root=f.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    root.closeRootReadBudget()
    val failure=assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata,owner,DeploymentReceiptStaticTestData.policy))
    // start拒绝发生在装配责任范围外；调用方仍保留 holder，关闭失败不能假报资源已退。
    assertTrue(owner.hasUnsettledResources)
    assertThrows(classOf[Exception],()=>owner.close())
    assertTrue(owner.failedCleanup.nonEmpty)
    assertNotNull(failure)

  @Test def schema5StaticBudgetRejectionBeforeFileReadSettlesActualOwner(): Unit =
    val f=DeploymentReceiptStaticTestData.fixture(temporary.resolve("schema5-budget").toAbsolutePath)
    val limits=SourceLimitProfile.of(1024L,2048L,4096L,32L,100L,512L,8192L,16384L)
    val root=new MySqlFormat3ReadBudget(limits,limits.maxSqlPacketBytes())
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    Files.delete(f.metadata.baseline().absolutePath())
    val failure=assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata,owner,DeploymentReceiptStaticTestData.policy))
    assertSame(failure,owner.failure.get);assertFalse(owner.hasUnsettledResources)
    assertEquals(0L,root.accountedMemoryBytes);root.closeRootReadBudget()

  @Test def schema5SessionInputsBindExactAssemblyRootIdentityAndModeUntilSeparateConsumersRetire(): Unit =
    val f = DeploymentReceiptStaticTestData.fixture(temporary.resolve("schema5-session-inputs").toAbsolutePath)
    val root = MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(f.profile.sourceLimitProfile,
      f.profile.sourceLimitProfile.maxSqlPacketBytes(),new MySqlFormat3AllocationLedger(f.profile.sourceLimitProfile.maxRetainedBytes()))
    val otherRoot = f.root()
    val owner = MySqlRuntimeInputs.beginV2(root, Thread.currentThread())
    val identity = MySqlOwnedIoIdentity(boot, 7L, 1L)
    var inputs: MySqlDeploymentReceiptSessionInputs = null
    var independentAudit: MySqlDeploymentReceiptSchemaAuditInputs = null
    try
      val graph = MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata, owner, DeploymentReceiptStaticTestData.policy)
      val before = root.accountedMemoryBytes
      assertThrows(classOf[IllegalArgumentException], () => MySqlRuntimeInputs.assembleSessionInputs(
        graph, otherRoot, identity, MySqlDeploymentReceiptSessionInputs.Fresh))
      assertEquals(before, root.accountedMemoryBytes)
      inputs = MySqlRuntimeInputs.assembleSessionInputs(graph, root, identity, MySqlDeploymentReceiptSessionInputs.Fresh)
      inputs.requireForSession(root, identity, MySqlDeploymentReceiptSessionInputs.Fresh)
      assertThrows(classOf[IllegalArgumentException], () => inputs.requireForSession(root,
        identity.copy(), MySqlDeploymentReceiptSessionInputs.Fresh))
      assertThrows(classOf[IllegalArgumentException], () => inputs.requireForSession(root,
        identity, MySqlDeploymentReceiptSessionInputs.Ordinary))
      assertEquals(MySqlScriptSchemaDataLayout.DeploymentReceiptFull26, inputs.full26Inputs.dataLayout)
      assertEquals("fp_schema5_test", inputs.full26Inputs.schema)
      assertNotEquals(MySqlScriptSchemaProfile.Schema, inputs.full26Inputs.schema)
      assertEquals(MySqlSchemaAuditProfile.DeploymentReceipt1, inputs.auditExpectation.profile)
      assertSame(root.profile,inputs.sourceLimitProfile)
      val bound=inputs.full26Inputs
      root.requireScriptDataInputs(bound)
      assertEquals(graph.configurationSha256, inputs.configurationSha256)
      assertEquals(f.profile.sourceLimitProfile, inputs.sourceLimitProfile)
      assertEquals(13, new String(inputs.recipeBytes, StandardCharsets.UTF_8).split("\n", -1).length)
      assertThrows(classOf[IllegalStateException], () => MySqlRuntimeInputs.assembleSessionInputs(
        graph, root, identity, MySqlDeploymentReceiptSessionInputs.Ordinary))
      independentAudit = graph.openAuditInputs()
      inputs.close()
      assertThrows(classOf[IllegalStateException], () => inputs.settings)
      assertThrows(classOf[IllegalStateException], () => owner.close())
      assertEquals(f.profile.schema, independentAudit.schema)
      independentAudit.close(); independentAudit = null
      owner.close()
      root.requireScriptDataInputs(bound) // root仍持Full26/budgets，不能随Session/K提前退费。
      assertTrue(root.accountedMemoryBytes > 0L)
    finally
      if independentAudit != null then independentAudit.close()
      if inputs != null then inputs.close()
      owner.close(); root.closeRootReadBudget(); otherRoot.closeRootReadBudget()

  @Test def schema5CompleteAAndSemanticCAreDistinctAndRejectWrongPinsAtTheirOwnBoundaries(): Unit =
    val f=DeploymentReceiptStaticTestData.fixture(temporary.resolve("schema5-a-c-pins").toAbsolutePath)
    val root=MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(f.profile.sourceLimitProfile,
      f.profile.sourceLimitProfile.maxSqlPacketBytes(),new MySqlFormat3AllocationLedger(f.profile.sourceLimitProfile.maxRetainedBytes()))
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    var inputs: MySqlDeploymentReceiptSessionInputs=null
    try
      val graph=MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata,owner,DeploymentReceiptStaticTestData.policy)
      inputs=MySqlRuntimeInputs.assembleSessionInputs(graph,root,MySqlOwnedIoIdentity(boot,7L,1L),MySqlDeploymentReceiptSessionInputs.Fresh)
      val a=WindowsB1SourceAnchorStore.AnchorConfiguration.ofDeploymentReceiptMetadata(f.metadata).configurationSha256()
      val c=graph.configurationSha256
      assertNotEquals(a,c)
      val path=temporary.resolve("approved-schema5-metadata.properties").toAbsolutePath
      inputs.requireCompleteConfigurationPin(MySqlFreshControlledMetadata(path,a))
      assertThrows(classOf[IllegalArgumentException],()=>inputs.requireCompleteConfigurationPin(MySqlFreshControlledMetadata(path,c)))
      val audit=inputs.auditExpectation.deploymentReceiptInputs.get
      val profile=inputs.sourceLimitProfile
      val recipe=inputs.recipeBytes
      val cells: Vector[MySqlAuditCell]=Vector(audit.configurationSha256,MySqlAuditText("FRESH_PLAN"),MySqlAuditBinary(recipe),
        MySqlAuditBinary(MessageDigest.getInstance("SHA-256").digest(recipe)),MySqlAuditText(SourceLimitProfile.ENCODING),
        MySqlAuditBinary(profile.canonicalBytes()),MySqlAuditBinary(profile.sha256())) ++
        Vector(profile.maxRegionBytes(),profile.maxChunkBytes(),profile.maxDecompressedBytes(),profile.maxNbtDepth(),
          profile.maxMembers(),profile.maxRetainedBytes(),profile.maxSqlPacketBytes(),profile.maxNativeArtifactBytes()).map(n=>MySqlAuditText(n.toString))
      val columns=audit.manifestLabels.map(n=>MySqlAuditColumn(n,Types.VARCHAR,"VARCHAR",0,0,java.sql.ResultSetMetaData.columnNullable,false))
      val row=MySqlAuditRow(cells)
      val set=MySqlAuditResultSet("manifest",columns,Vector(row))
      audit.validateManifest(set,row)
      val wrong=MySqlAuditRow(cells.updated(0,MySqlAuditBinary(HexFormat.of().parseHex(a))))
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>audit.validateManifest(set.copy(rows=Vector(wrong)),wrong))
    finally
      if inputs != null then inputs.close()
      owner.close();root.closeRootReadBudget()

  @Test def schema5PendingRootBindsOnceAndLiveSnapshotBlocksRetirementWithoutRawBorrow(): Unit =
    val f=DeploymentReceiptStaticTestData.fixture(temporary.resolve("schema5-root-lifetime").toAbsolutePath)
    val profile=f.profile.sourceLimitProfile
    val root=MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(profile,profile.maxSqlPacketBytes(),new MySqlFormat3AllocationLedger(profile.maxRetainedBytes()))
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    var inputs: MySqlDeploymentReceiptSessionInputs=null
    var image: MySqlFormat3ReadBudget=null
    try
      val graph=MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata,owner,DeploymentReceiptStaticTestData.policy)
      inputs=MySqlRuntimeInputs.assembleSessionInputs(graph,root,MySqlOwnedIoIdentity(boot,7L,1L),MySqlDeploymentReceiptSessionInputs.Fresh)
      val exact=inputs.full26Inputs
      val equalProfile=SourceLimitProfile.decode(profile.canonicalBytes(),profile.sha256())
      assertEquals(profile,equalProfile)
      assertFalse(profile eq equalProfile)
      val foreign=MySqlScriptSchemaInputs.createDeploymentReceiptFull26(exact.schema,exact.ddlSha256,equalProfile,exact.budget,exact.deploymentBudget.get)
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>root.requireScriptDataInputs(foreign))
      assertThrows(classOf[IllegalStateException],()=>root.bindDeploymentAssembly(owner,graph,null,exact))
      root.requireScriptDataInputs(exact) // 被拒绝的第二次bind不能替换/破坏原绑定。
      val connection=MySqlScriptSchemaDataTestFixture.Connection.copy(serverUuid=DeploymentReceiptStaticTestData.server,catalog=exact.schema)
      image=root.issueDeploymentReceiptSnapshotImage(exact,exact.schema,connection,MySqlScriptSchemaDataTestFixture.Scope,exact.budget.packetBytes)
      assertTrue(image.isSnapshotReadChildOf(root));assertTrue(image.sharesAllocationLedger(root))
      assertFalse(image.hasActiveImmutableBorrows)
      assertThrows(classOf[IllegalStateException],()=>root.closeRootReadBudget())
      image.closeScopedReadBudget();image=null
      inputs.close();inputs=null
      owner.close()
      assertTrue(root.accountedMemoryBytes > 0L)
      root.closeRootReadBudget()
      assertEquals(0L,root.accountedMemoryBytes)
    finally
      if image != null then image.closeScopedReadBudget()
      if inputs != null then inputs.close()
      owner.close();root.closeRootReadBudget()

  @Test def schema5FailedAttemptIsStickyAndActualLeaseAuditFailuresKeepTheirSlots(): Unit =
    val f=DeploymentReceiptStaticTestData.fixture(temporary.resolve("schema5-attempt-slots").toAbsolutePath)
    val p=f.profile.sourceLimitProfile
    val root=MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(p,p.maxSqlPacketBytes(),new MySqlFormat3AllocationLedger(p.maxRetainedBytes()))
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata,owner,DeploymentReceiptStaticTestData.policy)
    val reserve=root.availableMemoryBytes-(64L+8L*14L+95L) // attempt可登记，真实96B lease reserve不足。
    root.reserveRetainedMemory(reserve,"synthetic actual ledger pressure")
    val identity=MySqlOwnedIoIdentity(boot,7L,1L)
    val failure=assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleSessionInputs(graph,root,identity,MySqlDeploymentReceiptSessionInputs.Fresh))
    val observed=owner.deploymentInputObservation.get
    assertSame(failure,observed.failure.get)
    assertFalse(observed.published);assertFalse(observed.sessionLeasePending)
    assertEquals(0L,observed.temporaryFee);assertEquals(0L,observed.inputFee)
    assertThrows(classOf[IllegalStateException],()=>MySqlRuntimeInputs.assembleSessionInputs(graph,root,identity,MySqlDeploymentReceiptSessionInputs.Fresh))
    root.releaseRetainedMemory(reserve,"synthetic pressure dropped")
    owner.close();root.closeRootReadBudget()

    val second=DeploymentReceiptStaticTestData.fixture(temporary.resolve("schema5-close-slots").toAbsolutePath)
    val r=MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(second.profile.sourceLimitProfile,second.profile.sourceLimitProfile.maxSqlPacketBytes(),
      new MySqlFormat3AllocationLedger(second.profile.sourceLimitProfile.maxRetainedBytes()))
    val o=MySqlRuntimeInputs.beginV2(r,Thread.currentThread())
    val g=MySqlRuntimeInputs.assembleDeploymentReceiptStatic(second.metadata,o,DeploymentReceiptStaticTestData.policy)
    val i=MySqlRuntimeInputs.assembleSessionInputs(g,r,identity,MySqlDeploymentReceiptSessionInputs.Fresh)
    val beforeWrongThread=i.cleanupObservation
    val wrongThreadFailure=new AtomicReference[Throwable]()
    val wrongThread=new Thread(() => {
      try i.close()
      catch case failure: Throwable => wrongThreadFailure.set(failure)
      ()
    },"schema5-input-close-wrong-owner")
    wrongThread.start()
    wrongThread.join(5000L)
    assertFalse(wrongThread.isAlive,"wrong-thread cleanup rejection must complete")
    assertNotNull(wrongThreadFailure.get())
    assertEquals("exact assembly IO owner thread required",wrongThreadFailure.get().getMessage)
    assertEquals(beforeWrongThread,i.cleanupObservation)
    assertTrue(i.cleanupObservation.failure.isEmpty)
    r.closeRootReadBudget() // 真实预算已被caller过早关闭；不注入/伪造lease或audit接口。
    val closeFailure=assertThrows(classOf[Exception],()=>i.close())
    assertEquals("V2 consumer borrow dropped is already closed",closeFailure.getMessage)
    assertEquals(1,closeFailure.getSuppressed.length)
    assertNotSame(closeFailure,closeFailure.getSuppressed.head)
    assertEquals("V2 consumer borrow dropped is already closed",closeFailure.getSuppressed.head.getMessage)
    val pending=i.cleanupObservation
    assertSame(closeFailure,pending.failure.get)
    assertFalse(pending.closed);assertTrue(pending.auditPending);assertTrue(pending.leasePending);assertTrue(pending.retainedFee > 0L)
    assertSame(closeFailure,assertThrows(classOf[Exception],()=>i.close()))
    assertThrows(classOf[IllegalStateException],()=>o.close())

  @Test def schema5WrongWorkerAndOwnerBindingAreStickyAndUnpublishedInputsSettleOnOriginalAttempt(): Unit =
    val f=DeploymentReceiptStaticTestData.fixture(temporary.resolve("schema5-bind-identity").toAbsolutePath)
    val p=f.profile.sourceLimitProfile
    def pending()=MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(p,p.maxSqlPacketBytes(),new MySqlFormat3AllocationLedger(p.maxRetainedBytes()))
    val root=pending()
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata,owner,DeploymentReceiptStaticTestData.policy)
    val wrongThread=new AtomicReference[Throwable]()
    val thread=new Thread(()=>
      try root.bindDeploymentAssembly(owner,graph,null,MySqlScriptSchemaDataTestFixture.Full26Inputs)
      catch case failure: Throwable=>wrongThread.set(failure))
    thread.start();thread.join(5000L);assertFalse(thread.isAlive,"pure bind rejection thread did not terminate")
    assertNotNull(wrongThread.get())
    val failure=assertThrows(classOf[IllegalStateException],()=>MySqlRuntimeInputs.assembleSessionInputs(graph,root,
      MySqlOwnedIoIdentity(boot,7L,1L),MySqlDeploymentReceiptSessionInputs.Fresh))
    assertSame(wrongThread.get(),failure)
    val observed=owner.deploymentInputObservation.get
    assertEquals("root-bind",observed.stage) // 实际audit/lease/inputs均已构造，原attempt在发布前接住失败。
    assertFalse(observed.published);assertFalse(observed.inputsPending)
    assertFalse(observed.auditPending);assertFalse(observed.auditLeasePending);assertFalse(observed.sessionLeasePending)
    assertEquals(0L,observed.temporaryFee);assertEquals(0L,observed.inputFee);assertEquals(0L,observed.bindingFee)
    owner.close();root.closeRootReadBudget()

    val a=pending();val b=pending()
    val first=MySqlRuntimeInputs.beginV2(a,Thread.currentThread())
    val second=MySqlRuntimeInputs.beginV2(b,Thread.currentThread())
    try
      assertThrows(classOf[IllegalStateException],()=>a.bindDeploymentAssembly(second,null,null,MySqlScriptSchemaDataTestFixture.Full26Inputs))
      assertThrows(classOf[java.sql.SQLNonTransientConnectionException],()=>a.newCanonicalDataScope())
    finally
      first.close();second.close();a.closeRootReadBudget();b.closeRootReadBudget()

  @Test def schema5Full26RejectsNoncanonicalSchemaAndAssemblyRejectsForeignSourceProfile(): Unit =
    val f = DeploymentReceiptStaticTestData.fixture(temporary.resolve("schema5-session-foreign-profile").toAbsolutePath)
    val p = f.profile
    Vector[String](null, "", "Uppercase", "1leading", "bad-name", "a" * 65).foreach { schema =>
      assertThrows(classOf[IllegalArgumentException], () => MySqlScriptSchemaInputs.createDeploymentReceiptFull26(
        schema, "ab" * 32, p.sourceLimitProfile, p.publicationBudget, p.deploymentBudget))
    }
    val foreign = SourceLimitProfile.of(1000000L,65536L,2000000L,64L,100001L,134217728L,1000000L,2000000L)
    val root = new MySqlFormat3ReadBudget(foreign, foreign.maxSqlPacketBytes())
    val owner = MySqlRuntimeInputs.beginV2(root, Thread.currentThread())
    try
      val failure = assertThrows(classOf[java.sql.SQLDataException], () =>
        MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata, owner, DeploymentReceiptStaticTestData.policy))
      assertSame(failure, owner.failure.get)
      assertFalse(owner.hasUnsettledResources)
      assertEquals(0L, root.accountedMemoryBytes)
    finally
      owner.close(); root.closeRootReadBudget()
