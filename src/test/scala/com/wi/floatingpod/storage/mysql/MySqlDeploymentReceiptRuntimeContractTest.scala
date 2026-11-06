package com.wi.floatingpod.storage.mysql

import com.google.gson.{GsonBuilder, JsonArray, JsonObject}
import com.wi.floatingpod.config.{MySqlRuntimeMetadata, SourceLimitProfile}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.sql.{ResultSetMetaData, Types}
import java.time.Duration
import java.util.{HexFormat, UUID}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.Assertions.*

/** 所有数值和文件均为显式 synthetic DATA；不连接数据库、不构造 native Plan。 */
private[mysql] object DeploymentReceiptStaticTestData:
  val server = UUID.fromString("982937bc-bc46-11f1-b957-04421a0aa74d")
  val provisioning = UUID.fromString("eea917d4-e945-4c54-b666-5ba68d666f6f")
  val binding = UUID.fromString("9ff22189-4730-41f6-80d8-3a3bdf971604")
  val policy = MySqlRuntimeAuditPolicy("NONE", "NONE", MySqlExpectedNoImport,
    MySqlExpectedPhysicalFormat.Format2, Duration.ofSeconds(20))
  val gson = new GsonBuilder().disableHtmlEscaping().create()
  def sha(bytes: Array[Byte]): String = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
  def defaultProfile(): MySqlDeploymentReceiptSchemaProfile.Inputs =
    import MySqlDeploymentReceiptSchemaProfile.*
    val limits = SourceLimitProfile.of(1000000L,65536L,2000000L,64L,100000L,134217728L,1000000L,2000000L)
    def a(role: String) = Account("fp_s5_" + role, "127.0.0.1")
    inputs("fp_schema5_test", Accounts(a("guard"),a("audit"),a("runtime"),a("provisioner"),a("importer"),a("quota")), limits,
      MySqlScriptSchemaProfile.Budget(16384L,16384L,16384L,4096L,65536L,142L,20000L,2000000L,1000000L),
      MySqlDeploymentReceiptSchemaDefinition.Budget(8192L,8192L),4096L,Vector.empty)

  def privileges(rendered: MySqlDeploymentReceiptSchemaProfile.Rendered): Vector[MySqlAuditResultSet] =
    MySqlDeploymentReceiptSchemaProfile.expectedPrivilegeRows(rendered).zipWithIndex.map { (rows, i) =>
      val labels = MySqlSchemaAuditor.Format3PrivilegeLabels(i)
      MySqlAuditResultSet(MySqlAuditCoverage.Format3PrivilegeResultSetIds(i), labels.map(l =>
        MySqlAuditColumn(l,Types.VARCHAR,"VARCHAR",512,0,ResultSetMetaData.columnNoNulls,false)),
        rows.map(row => MySqlAuditRow(row.map(MySqlAuditText.apply))))
    }
  def jsonSet(set: MySqlAuditResultSet): JsonObject =
    val o = new JsonObject(); o.addProperty("id",set.id)
    val cs = new JsonArray()
    set.columns.foreach { c =>
      val x = new JsonObject(); x.addProperty("label",c.label);x.addProperty("jdbcType",c.jdbcType)
      x.addProperty("typeName",c.typeName);x.addProperty("precision",c.precision);x.addProperty("scale",c.scale)
      x.addProperty("nullable",c.nullable);x.addProperty("signed",c.signed);cs.add(x)
    }
    o.add("columns",cs)
    val rs = new JsonArray()
    set.rows.foreach { r =>
      val a = new JsonArray()
      r.cells.foreach { c =>
        val x = new JsonObject()
        c match
          case MySqlAuditText(v) => x.addProperty("tag","TEXT");x.addProperty("value",v)
          case b: MySqlAuditBinary => x.addProperty("tag","BINARY");x.addProperty("hex",HexFormat.of().withUpperCase().formatHex(b.bytes))
          case MySqlAuditNull => x.addProperty("tag","SQL_NULL")
          case _ => throw new IllegalArgumentException("unsupported fixture cell")
        a.add(x)
      }
      rs.add(a)
    }
    o.add("rows",rs);o
  def profileJson(p: MySqlDeploymentReceiptSchemaProfile.Inputs): JsonObject =
    val o = new JsonObject();o.addProperty("schema",p.schema)
    val accounts = new JsonObject()
    Vector("guardDefiner","auditDefiner","runtime","provisioner","importer","operationQuotaUpdater")
      .zip(p.accounts.all).foreach { (key,a) =>
        val x = new JsonObject();x.addProperty("user",a.user);x.addProperty("host",a.host);accounts.add(key,x)
      }
    o.add("accounts",accounts)
    val l = new JsonObject();l.addProperty("encoding",SourceLimitProfile.ENCODING)
    l.addProperty("hex",HexFormat.of().formatHex(p.sourceLimitProfile.canonicalBytes()));l.addProperty("sha256",HexFormat.of().formatHex(p.sourceLimitProfile.sha256()))
    o.add("sourceLimitProfile",l)
    val pub = new JsonObject(); val b=p.publicationBudget
    Vector("evidenceBytes"->b.evidenceBytes,"diagnosticsBytes"->b.diagnosticsBytes,"configBytes"->b.configBytes,
      "requestBytes"->b.requestBytes,"snapshotBytes"->b.snapshotBytes,"receiptBytes"->b.receiptBytes,"rows"->b.rows,
      "retainedBytes"->b.retainedBytes,"packetBytes"->b.packetBytes).foreach((k,v)=>pub.addProperty(k,v))
    o.add("publicationBudget",pub)
    val dep=new JsonObject();dep.addProperty("evidenceBytes",p.deploymentBudget.evidenceBytes);dep.addProperty("diagnosticsBytes",p.deploymentBudget.diagnosticsBytes)
    o.add("deploymentBudget",dep);o.addProperty("quotaMaximumRecords",p.quotaMaximumRecords)
    val proxies=new JsonArray()
    p.trustedAdministrativeProxyRows.sortBy(_.tuple).foreach { r =>
      val x=new JsonObject();x.addProperty("user",r.user);x.addProperty("host",r.host)
      x.addProperty("proxiedUser",r.proxiedUser);x.addProperty("proxiedHost",r.proxiedHost);x.addProperty("withGrant",r.withGrant);proxies.add(x)
    }
    o.add("trustedAdministrativeProxyRows",proxies);o

  def recipe(p: MySqlDeploymentReceiptSchemaProfile.Inputs, ddl: String): Array[Byte] =
    val fields=Vector("creationOperationUuid"->"00000000-0000-0000-0000-000000000011",
      "bootstrapOperationUuid"->"00000000-0000-0000-0000-000000000012",
      "prepareWriteUuid"->"00000000-0000-0000-0000-000000000013",
      "certifyWriteUuid"->"00000000-0000-0000-0000-000000000014", "bindingUuid"->binding.toString,
      "physicalFormat"->"2","mysqlServerUuid"->server.toString,"schema"->p.schema,"schemaFormat"->"5",
      "ddlSha256"->ddl.toLowerCase(java.util.Locale.ROOT),"provisioningUuid"->provisioning.toString)
    (MySqlDeploymentReceiptRuntimeContract.RecipeEncoding+"\n"+fields.map((k,v)=>s"$k=$v\n").mkString).getBytes(StandardCharsets.UTF_8)

  final case class Fixture(metadata: MySqlRuntimeMetadata.DeploymentReceiptStatic, properties: Map[String,String],
      contract: JsonObject, profile: MySqlDeploymentReceiptSchemaProfile.Inputs, directory: Path):
    def root(): MySqlFormat3ReadBudget = new MySqlFormat3ReadBudget(profile.sourceLimitProfile,profile.sourceLimitProfile.maxSqlPacketBytes())
    def rebind(edits: Map[String,String]): MySqlRuntimeMetadata.DeploymentReceiptStatic = metadataFrom(properties ++ edits)
    def saveContract(value: JsonObject): MySqlRuntimeMetadata.DeploymentReceiptStatic =
      val bytes=gson.toJson(value).getBytes(StandardCharsets.UTF_8)
      Files.write(directory.resolve("contract.json"),bytes)
      rebind(Map("deploymentReceiptRuntimeContractSha256"->sha(bytes)))
  def metadataFrom(fields: Map[String,String]): MySqlRuntimeMetadata.DeploymentReceiptStatic =
    def escape(v: String): String = v.replace("\\","\\\\").replace("\n","\\n").replace("\r","\\r")
    MySqlRuntimeMetadata.fromDeploymentReceiptBytes(fields.toVector.sortBy(_._1).map((k,v)=>s"$k=${escape(v)}\n").mkString.getBytes(StandardCharsets.UTF_8))

  def fixture(directory: Path, p: MySqlDeploymentReceiptSchemaProfile.Inputs = defaultProfile(),
      security: Vector[MySqlAuditResultSet] = null): Fixture =
    Files.createDirectories(directory)
    val rendered=MySqlDeploymentReceiptSchemaProfile.render(p)
    val old=MySqlOrdinaryCertifiedHandoffFixture.loadIndependentStaticBaseline(directory.resolve("old").toAbsolutePath)
    // Synthetic static pack: real declared names/columns/guards and hash-consistent definitions; never a SQL observation.
    val oldSets=old.objects.resultSets
    def row(set: MySqlAuditResultSet, template: MySqlAuditRow, updates: Map[String,MySqlAuditCell]): MySqlAuditRow =
      MySqlAuditRow(set.columns.zip(template.cells).map((c,v)=>updates.getOrElse(c.label,v)))
    val tables=oldSets(0).copy(rows=rendered.tables.map(t=>row(oldSets(0),oldSets(0).rows.head,Map("TABLE_SCHEMA"->MySqlAuditText(p.schema),"TABLE_NAME"->MySqlAuditText(t.name)))))
    val columns=oldSets(1).copy(rows=rendered.tables.flatMap(t=>t.columns.zipWithIndex.map((c,i)=>row(oldSets(1),oldSets(1).rows.head,
      Map("TABLE_SCHEMA"->MySqlAuditText(p.schema),"TABLE_NAME"->MySqlAuditText(t.name),"COLUMN_NAME"->MySqlAuditText(c.name),"ORDINAL_POSITION"->MySqlAuditText((i+1).toString))))))
    val triggers=oldSets(7).copy(rows=rendered.guards.map(g=>row(oldSets(7),oldSets(7).rows.head,
      Map("TRIGGER_SCHEMA"->MySqlAuditText(p.schema),"TRIGGER_NAME"->MySqlAuditText(g.name)))))
    val self=oldSets(8).copy(rows=oldSets(8).rows.map(r=>row(oldSets(8),r,Map("ROUTINE_SCHEMA"->MySqlAuditText(p.schema),
      "ROUTINE_NAME"->MySqlAuditText(MySqlDeploymentReceiptSchemaProfile.AuditRoutine),"DEFINER"->MySqlAuditText(p.accounts.auditDefiner.identity)))))
    val sets=oldSets.updated(0,tables).updated(1,columns).updated(7,triggers).updated(8,self)
    MySqlApprovedObjectManifest.forDeploymentReceipt1(sets)
    val pack=new JsonObject()
    val sourceHash=sha("independent schema5 synthetic object fixture".getBytes(StandardCharsets.UTF_8))
    val identity=Map("format"->MySqlApprovedBaselineLoader.ApprovedFormat,"coverage"->MySqlDeploymentReceiptSchemaProfile.Coverage,
      "serverUuid"->server.toString,"serverVersion"->"9.7.2","schema"->p.schema,"provisioningUuid"->provisioning.toString,
      "bindingUuid"->binding.toString,"ddlSha256"->rendered.sha256,"auditDefiner"->p.accounts.auditDefiner.identity,"sourceCaptureSha256"->sourceHash)
    identity.foreach((k,v)=>pack.addProperty(k,v));pack.addProperty("schemaFormat",5)
    val rs=new JsonArray();sets.foreach(s=>rs.add(jsonSet(s)));pack.add("resultSets",rs)
    val baselineBytes=gson.toJson(pack).getBytes(StandardCharsets.UTF_8);Files.write(directory.resolve("baseline.json"),baselineBytes)
    val k=new JsonObject();identity.filterNot((key,_)=>key=="format").foreach((key,v)=>k.addProperty(key,v))
    k.addProperty("format",MySqlDeploymentReceiptRuntimeContract.Format);k.addProperty("schemaFormat",5)
    k.addProperty("approvedBaselineSha256",sha(baselineBytes));k.addProperty("effectiveUser",p.accounts.runtime.identity)
    k.addProperty("connectedUser",p.accounts.runtime.identity);k.addProperty("callerRoles","NONE");k.addProperty("auditDefinerRoles","NONE")
    val grants=new JsonArray();grants.add(s"GRANT USAGE ON *.* TO ${p.accounts.runtime.grantee}");k.add("callerGrants",grants)
    k.addProperty("physicalFormat",2);k.addProperty("importState","NONE");k.addProperty("maxReceiptAgeNanos",policy.maxReceiptAge.toNanos)
    val source=recipe(p,rendered.sha256);k.addProperty("sourceProvenanceKind","FRESH_PLAN")
    k.addProperty("sourceProvenanceHex",HexFormat.of().formatHex(source));k.addProperty("sourceProvenanceSha256",sha(source))
    k.addProperty("limitProfileEncoding",SourceLimitProfile.ENCODING);k.addProperty("limitProfileHex",HexFormat.of().formatHex(p.sourceLimitProfile.canonicalBytes()))
    k.addProperty("limitProfileSha256",HexFormat.of().formatHex(p.sourceLimitProfile.sha256()))
    val ps=new JsonArray();Option(security).getOrElse(privileges(rendered)).foreach(s=>ps.add(jsonSet(s)));k.add("definerPrivileges",ps)
    k.addProperty("deploymentReceiptManifestConfigurationEncoding",MySqlDeploymentReceiptRuntimeContract.Encoding);k.add("profile",profileJson(p))
    val kb=gson.toJson(k).getBytes(StandardCharsets.UTF_8);Files.write(directory.resolve("contract.json"),kb)
    val prefix=directory.toAbsolutePath.toString.replace('\\','/')
    val fields=Map("backend"->"mysql","host"->"database.example.invalid","port"->"3307","schema"->p.schema,
      "expectedServerUuid"->server.toString,"expectedBindingUuid"->binding.toString,"expectedEffectiveUser"->p.accounts.runtime.identity,
      "username"->p.accounts.runtime.user,"pkcs12TrustStorePath"->s"$prefix/trust.p12","credentialSourceRef"->s"file:$prefix/credential.txt",
      "trustSecretSourceRef"->s"file:$prefix/trust-secret.txt","connectTimeoutMillis"->"3000","socketTimeoutMillis"->"10000","activeLimit"->"17",
      "innodbFlushLogAtTrxCommit"->"1","syncBinlog"->"1","approvedBaselinePath"->s"$prefix/baseline.json","approvedBaselineSha256"->sha(baselineBytes),
      "approvedBaselineFormat"->MySqlApprovedBaselineLoader.ApprovedFormat,"approvedSchemaFormat"->"5","approvedCoverage"->MySqlDeploymentReceiptSchemaProfile.Coverage,
      "expectedServerVersion"->"9.7.2","expectedProvisioningUuid"->provisioning.toString,"approvedDdlSha256"->rendered.sha256,
      "expectedAuditDefiner"->p.accounts.auditDefiner.identity,"approvedSourceCaptureSha256"->sourceHash,
      "deploymentReceiptRuntimeContractPath"->s"$prefix/contract.json","deploymentReceiptRuntimeContractSha256"->sha(kb),
      "deploymentReceiptManifestConfigurationEncoding"->MySqlDeploymentReceiptRuntimeContract.Encoding)
    Fixture(metadataFrom(fields),fields,k,p,directory)

class MySqlDeploymentReceiptRuntimeContractTest:
  @TempDir var directory: Path = _
  @Test def recipeUsesExactElevenFieldsAndRejectsNoncanonicalBytes(): Unit =
    val p=DeploymentReceiptStaticTestData.defaultProfile()
    val bytes=DeploymentReceiptStaticTestData.recipe(p,"a"*64)
    assertArrayEquals(bytes,MySqlDeploymentReceiptRuntimeContract.decodeRecipe(bytes).canonicalBytes)
    assertEquals(12,new String(bytes,StandardCharsets.UTF_8).count(_=='\n'))
    for bad <- Vector(bytes.dropRight(1), bytes++Array[Byte](10), new String(bytes,StandardCharsets.UTF_8).replace("physicalFormat=2","physicalFormat=3").getBytes(StandardCharsets.UTF_8)) do
      assertThrows(classOf[java.sql.SQLDataException],()=>MySqlDeploymentReceiptRuntimeContract.decodeRecipe(bad))

  @Test def actualFilesAssembleOnlyClosedStaticGraphAndLeaseBlocksClose(): Unit =
    val f=DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
    val root=f.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata,owner,DeploymentReceiptStaticTestData.policy)
    val audit=graph.openAuditInputs()
    assertEquals(5,graph.approvedBaselineIdentity.schemaFormat)
    assertEquals(graph.configurationSha256,HexFormat.of().withUpperCase().formatHex(audit.configurationSha256.bytes))
    assertNotEquals(f.profile.profileIdentitySha256.toUpperCase(java.util.Locale.ROOT),graph.configurationSha256)
    val lease=graph.borrow()
    assertThrows(classOf[IllegalStateException],()=>owner.close())
    lease.close()
    assertThrows(classOf[IllegalStateException],()=>owner.close())
    audit.close();owner.close()
    assertThrows(classOf[IllegalStateException],()=>graph.configurationSha256)
    assertThrows(classOf[IllegalStateException],()=>audit.configurationSha256)
    assertFalse(owner.hasUnsettledResources);root.closeRootReadBudget()

  @Test def missingOrMutatedActualFilesFailWithoutIssuingGraph(): Unit =
    val f=DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
    Files.writeString(f.metadata.runtimeContract().absolutePath(),"{}")
    val root=f.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    assertThrows(classOf[java.sql.SQLDataException],()=>MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata,owner,DeploymentReceiptStaticTestData.policy))
    assertTrue(owner.failure.nonEmpty);assertFalse(owner.hasUnsettledResources);root.closeRootReadBudget()

  @Test def approvedKStillRejectsUnknownWrongTypedProfileSecurityAndRecipeInputs(): Unit =
    import DeploymentReceiptStaticTestData.*
    val changes=Vector[(com.google.gson.JsonObject=>Unit)](
      k=>k.addProperty("unexpected",true),
      k=>k.addProperty("configurationSha256", "00"*32),
      k=>k.getAsJsonObject("profile").addProperty("profileIdentitySha256", "00"*32),
      k=>k.addProperty("schemaFormat","5"),
      k=>k.addProperty("deploymentReceiptManifestConfigurationEncoding",MySqlRuntimeMetadata.MANIFEST_V3),
      k=>k.getAsJsonObject("profile").addProperty("quotaMaximumRecords",0),
      k=>k.getAsJsonObject("profile").getAsJsonObject("deploymentBudget").addProperty("evidenceBytes",1000001L),
      k=>k.addProperty("sourceProvenanceKind","FORMAT3_RESTORE"),
      k=>k.addProperty("sourceProvenanceSha256","00"*32),
      k=>k.getAsJsonArray("definerPrivileges").remove(0))
    changes.zipWithIndex.foreach { (change,index) =>
      val f=fixture(directory.resolve(index.toString).toAbsolutePath)
      val k=f.contract.deepCopy();change(k)
      val metadata=f.saveContract(k)
      val root=f.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
      val failure=assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleDeploymentReceiptStatic(metadata,owner,policy))
      assertSame(failure,owner.failure.get);assertFalse(owner.hasUnsettledResources)
      assertEquals(0L,root.accountedMemoryBytes);root.closeRootReadBudget()
    }

  @Test def duplicateJsonKeysAndCanonicalRecipeIdentityMismatchDoNotIssueStaticGraph(): Unit =
    import DeploymentReceiptStaticTestData.*
    val f=fixture(directory.toAbsolutePath)
    val text=gson.toJson(f.contract)
    val raw=("{\"format\":\"duplicate\","+text.drop(1)).getBytes(StandardCharsets.UTF_8)
    Files.write(f.metadata.runtimeContract().absolutePath(),raw)
    val metadata=f.rebind(Map("deploymentReceiptRuntimeContractSha256"->sha(raw)))
    val root=f.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleDeploymentReceiptStatic(metadata,owner,policy))
    assertEquals(0L,root.accountedMemoryBytes);root.closeRootReadBudget()
    val k=f.contract.deepCopy()
    val source=new String(recipe(f.profile,k.get("ddlSha256").getAsString),StandardCharsets.UTF_8)
      .replace(binding.toString,"00000000-0000-0000-0000-000000000099").getBytes(StandardCharsets.UTF_8)
    k.addProperty("sourceProvenanceHex",HexFormat.of().formatHex(source));k.addProperty("sourceProvenanceSha256",sha(source))
    val wrong=f.saveContract(k);val root2=f.root();val owner2=MySqlRuntimeInputs.beginV2(root2,Thread.currentThread())
    assertThrows(classOf[java.sql.SQLDataException],()=>MySqlRuntimeInputs.assembleDeploymentReceiptStatic(wrong,owner2,policy))
    assertEquals(0L,root2.accountedMemoryBytes);root2.closeRootReadBudget()

  @Test def twoActualAuditConsumersDetachOnlyOwnViewAndBlockOwnerUntilLastClose(): Unit =
    val f=DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
    val root=f.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata,owner,DeploymentReceiptStaticTestData.policy)
    val graphBytes=root.accountedMemoryBytes
    val first=graph.openAuditInputs();val second=graph.openAuditInputs()
    assertTrue(root.accountedMemoryBytes > graphBytes)
    val digest=second.configurationSha256
    first.close()
    assertThrows(classOf[IllegalStateException],()=>first.configurationSha256)
    assertEquals(digest,second.configurationSha256)
    assertTrue(root.accountedMemoryBytes > graphBytes)
    assertThrows(classOf[IllegalStateException],()=>owner.close())
    second.close()
    assertEquals(graphBytes,root.accountedMemoryBytes)
    owner.close()
    assertEquals(0L,root.accountedMemoryBytes);assertFalse(owner.hasUnsettledResources)
    root.closeRootReadBudget()

  @Test def differentActualRootProfileAndIndependentMetadataIdentityFailWithoutDebt(): Unit =
    import DeploymentReceiptStaticTestData.*
    val f=fixture(directory.toAbsolutePath)
    val other=SourceLimitProfile.of(1000000L,65536L,2000000L,64L,100001L,134217728L,1000000L,2000000L)
    val root=new MySqlFormat3ReadBudget(other,other.maxSqlPacketBytes())
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    assertThrows(classOf[java.sql.SQLDataException],()=>MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata,owner,policy))
    assertEquals(0L,root.accountedMemoryBytes);assertFalse(owner.hasUnsettledResources);root.closeRootReadBudget()
    val root2=f.root();val owner2=MySqlRuntimeInputs.beginV2(root2,Thread.currentThread())
    val metadata=f.rebind(Map("expectedBindingUuid"->"00000000-0000-0000-0000-000000000099"))
    assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleDeploymentReceiptStatic(metadata,owner2,policy))
    assertEquals(0L,root2.accountedMemoryBytes);assertFalse(owner2.hasUnsettledResources);root2.closeRootReadBudget()
