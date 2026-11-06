package com.wi.floatingpod.storage.mysql

import com.google.gson.{JsonArray, JsonObject, JsonParser}
import com.wi.floatingpod.config.MySqlRuntimeMetadata
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.HexFormat

/** 纯测试的显式文件/pin fixture；profile必须在任何B/K render前由caller提供。 */
private[mysql] object MySqlDbFirstHistoricalTestFixture:
  import DeploymentReceiptStaticTestData.*
  final case class StaticFixture(metadata: MySqlRuntimeMetadata.DbFirstStatic, fields: Map[String,String],
      contract: JsonObject, profile: MySqlDeploymentReceiptSchemaProfile.Inputs, directory: Path):
    def root(): MySqlFormat3ReadBudget = MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(profile.sourceLimitProfile, profile.sourceLimitProfile.maxSqlPacketBytes(),
      new MySqlFormat3AllocationLedger(profile.sourceLimitProfile.maxRetainedBytes()))
  def metadata(fields: Map[String,String]): MySqlRuntimeMetadata.DbFirstStatic =
    def escape(value: String): String = value.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r")
    MySqlRuntimeMetadata.fromDbFirstBytes(fields.toVector.sortBy(_._1).map((k,v) => s"$k=${escape(v)}\n").mkString.getBytes(StandardCharsets.UTF_8))
  def fixture(directory: Path, p: MySqlDeploymentReceiptSchemaProfile.Inputs): StaticFixture =
    val old = DeploymentReceiptStaticTestData.fixture(directory,p)
    val rendered = MySqlDbFirstSchemaProfile.render(p)
    val pack = JsonParser.parseString(Files.readString(directory.resolve("baseline.json"))).getAsJsonObject
    val oldSets = pack.getAsJsonArray("resultSets")
    def row(set: JsonObject, template: com.google.gson.JsonElement, updates: Map[String,String]): com.google.gson.JsonElement =
      import scala.jdk.CollectionConverters.*
      val result = template.deepCopy().getAsJsonArray
      set.getAsJsonArray("columns").asScala.zipWithIndex.foreach { (column,index) =>
        updates.get(column.getAsJsonObject.get("label").getAsString).foreach(v => result.get(index).getAsJsonObject.addProperty("value",v))
      }
      result
    def replaceRows(index: Int, values: Vector[Map[String,String]]): Unit =
      val set = oldSets.get(index).getAsJsonObject
      val template = set.getAsJsonArray("rows").get(0)
      val rows = new JsonArray(); values.foreach(v => rows.add(row(set,template,v))); set.add("rows",rows)
    replaceRows(0,rendered.tables.map(t => Map("TABLE_SCHEMA"->p.schema,"TABLE_NAME"->t.name)))
    replaceRows(1,rendered.tables.flatMap(t => t.columns.zipWithIndex.map((c,i) => Map(
      "TABLE_SCHEMA"->p.schema,"TABLE_NAME"->t.name,"COLUMN_NAME"->c.name,"ORDINAL_POSITION"->(i+1).toString))))
    replaceRows(7,rendered.guards.map(g => Map("TRIGGER_SCHEMA"->p.schema,"TRIGGER_NAME"->g.name)))
    replaceRows(8,Vector(Map("ROUTINE_SCHEMA"->p.schema,"ROUTINE_NAME"->MySqlDbFirstSchemaDefinition.AuditRoutine,
      "DEFINER"->p.accounts.auditDefiner.identity)))
    pack.addProperty("schemaFormat",6); pack.addProperty("coverage",MySqlDbFirstSchemaDefinition.Coverage)
    pack.addProperty("ddlSha256",rendered.sha256)
    val baselineBytes = gson.toJson(pack).getBytes(StandardCharsets.UTF_8)
    Files.write(directory.resolve("baseline.json"),baselineBytes)
    val contract = old.contract.deepCopy()
    contract.addProperty("format",MySqlDbFirstRuntimeContract.Format); contract.addProperty("schemaFormat",6)
    contract.addProperty("coverage",MySqlDbFirstSchemaDefinition.Coverage); contract.addProperty("ddlSha256",rendered.sha256)
    contract.addProperty("approvedBaselineSha256",sha(baselineBytes))
    contract.remove("deploymentReceiptManifestConfigurationEncoding")
    contract.addProperty("dbFirstManifestConfigurationEncoding",MySqlDbFirstRuntimeContract.Encoding)
    val history = new JsonObject(); history.addProperty("kind","NONE"); contract.add("historicalSchema5",history)
    val source = new String(recipe(p,rendered.sha256),StandardCharsets.UTF_8)
      .replace(MySqlDeploymentReceiptRuntimeContract.RecipeEncoding,MySqlDbFirstRuntimeContract.RecipeEncoding)
      .replace("schemaFormat=5\n","schemaFormat=6\n").getBytes(StandardCharsets.UTF_8)
    contract.addProperty("sourceProvenanceHex",HexFormat.of().formatHex(source)); contract.addProperty("sourceProvenanceSha256",sha(source))
    val rows = MySqlDbFirstSchemaProfile.expectedPrivilegeRows(rendered)
    val sets = new JsonArray()
    val oldPrivileges = privileges(MySqlDeploymentReceiptSchemaProfile.render(p))
    rows.zipWithIndex.foreach { (values,index) =>
      sets.add(jsonSet(oldPrivileges(index).copy(rows=values.map(v => MySqlAuditRow(v.map(MySqlAuditText.apply))))))
    }
    contract.add("definerPrivileges",sets)
    val contractBytes = gson.toJson(contract).getBytes(StandardCharsets.UTF_8)
    Files.write(directory.resolve("contract.json"),contractBytes)
    val fields = (old.properties - "deploymentReceiptRuntimeContractPath" - "deploymentReceiptRuntimeContractSha256" -
      "deploymentReceiptManifestConfigurationEncoding") ++ Map("approvedSchemaFormat"->"6",
      "approvedCoverage"->MySqlDbFirstSchemaDefinition.Coverage,"approvedDdlSha256"->rendered.sha256,
      "approvedBaselineSha256"->sha(baselineBytes),"dbFirstRuntimeContractPath"->directory.resolve("contract.json").toAbsolutePath.toString,
      "dbFirstRuntimeContractSha256"->sha(contractBytes),"dbFirstManifestConfigurationEncoding"->MySqlDbFirstRuntimeContract.Encoding)
    StaticFixture(metadata(fields),fields,contract,p,directory)

  /** 显式synthetic production文件；不创建秘密、连接或SQL资格。 */
  def productionControl(f:StaticFixture,io:MySqlOwnedIoIdentity,observe:java.util.UUID,
      acquire:java.util.UUID,expectedConfiguration:String):com.wi.floatingpod.bootstrap.ProductionBootstrapControl.DbFirstControl =
    import com.wi.floatingpod.bootstrap.{ProductionBootstrapControl,EarlyDependencyResolver}
    def bytes(fields:Map[String,String]):Array[Byte] = fields.toVector.sortBy(_._1).map { (k,v)=>
      k+"="+v.replace("\\","\\\\").replace("\n","\\n").replace("\r","\\r")+"\n"
    }.mkString.getBytes(StandardCharsets.UTF_8)
    val raw=bytes(f.fields);val metadataPath=f.directory.resolve("production-metadata.properties")
    Files.write(metadataPath,raw)
    val mapping=Map("host"->"host","port"->"port","schema"->"schema","effectiveUser"->"expectedEffectiveUser","username"->"username",
      "serverUuid"->"expectedServerUuid","bindingUuid"->"expectedBindingUuid","activeLimit"->"activeLimit",
      "connectTimeoutMillis"->"connectTimeoutMillis","socketTimeoutMillis"->"socketTimeoutMillis",
      "baselinePath"->"approvedBaselinePath","baselineSha256"->"approvedBaselineSha256","baselineFormat"->"approvedBaselineFormat",
      "schemaFormat"->"approvedSchemaFormat","coverage"->"approvedCoverage","serverVersion"->"expectedServerVersion",
      "provisioningUuid"->"expectedProvisioningUuid","ddlSha256"->"approvedDdlSha256","auditDefiner"->"expectedAuditDefiner",
      "sourceCaptureSha256"->"approvedSourceCaptureSha256","runtimeContractSha256"->"dbFirstRuntimeContractSha256")
    val fields=mapping.map((k,v)=>k->f.fields(v))++Map(
      "controlFormat"->ProductionBootstrapControl.DB_FIRST_FORMAT,"metadataPath"->metadataPath.toAbsolutePath.toString,
      "metadataSha256"->sha(raw),"configurationSha256"->expectedConfiguration,
      "worldPath"->f.directory.resolve("world").toAbsolutePath.toString,
      "sourceLimitProfileCanonicalHex"->HexFormat.of().formatHex(f.profile.sourceLimitProfile.canonicalBytes()),
      "sourceLimitProfileSha256"->HexFormat.of().withUpperCase().formatHex(f.profile.sourceLimitProfile.sha256()),
      "bootUuid"->io.bootId.toString,"bootGeneration"->io.bootGeneration.toString,"connectionGeneration"->io.connectionGeneration.toString,
      "observeOperationUuid"->observe.toString,"acquireOperationUuid"->acquire.toString,"barrierWaitSeconds"->"0",
      "callerRoles"->policy.callerRoles,"auditDefinerRoles"->policy.auditDefinerRoles,"receiptFreshnessMillis"->policy.maxReceiptAge.toMillis.toString,
      "leaseMillis"->"120000","importState"->"NONE","physicalFormat"->"2","preparationMillis"->"45000","claimMillis"->"10000","finalizationMillis"->"10000",
      "sqlOuterSha256"->EarlyDependencyResolver.SQL_OUTER,"scalarOuterSha256"->("1"*64),"scalarRuntimeVersion"->"3.8.4",
      "scalaLibrarySha256"->("2"*64),"scalaParserSha256"->("3"*64),"fugueOuterSha256"->("4"*64),"fugueRuntimeVersion"->"0.99.1")
    val controlPath=f.directory.resolve("production-control.properties");val control=bytes(fields.map((k,v)=>k->(if k.endsWith("Sha256") then v.toUpperCase(java.util.Locale.ROOT) else v)));Files.write(controlPath,control)
    ProductionBootstrapControl.loadDbFirst(new ProductionBootstrapControl.LaunchContract(controlPath.toAbsolutePath,sha(control).toUpperCase(java.util.Locale.ROOT)))

  def pinHistory(f: StaticFixture, old: DeploymentReceiptStaticTestData.Fixture): MySqlRuntimeMetadata.DbFirstStatic =
    def escape(value: String): String = value.replace("\\","\\\\").replace("\n","\\n").replace("\r","\\r")
    val bytes=old.properties.toVector.sortBy(_._1).map((k,v)=>s"$k=${escape(v)}\n").mkString.getBytes(StandardCharsets.UTF_8)
    val path=old.directory.resolve("historical.properties").toAbsolutePath
    Files.write(path,bytes)
    val source=new JsonObject();source.addProperty("kind","SCHEMA5")
    source.addProperty("metadataPath",path.toString);source.addProperty("metadataSha256",sha(bytes))
    val contract=f.contract.deepCopy();contract.add("historicalSchema5",source)
    val raw=gson.toJson(contract).getBytes(StandardCharsets.UTF_8)
    Files.write(f.directory.resolve("contract.json"),raw)
    metadata(f.fields.updated("dbFirstRuntimeContractSha256",sha(raw)))

  final class Owned private[MySqlDbFirstHistoricalTestFixture] (
      val inputs: MySqlDbFirstSessionInputs,
      val root: MySqlFormat3ReadBudget,
      val history: MySqlDbFirstHistoryInputs,
      val owner: MySqlRuntimeInputs.AssemblyOwner,
      val currentIdentity: MySqlApprovedBaselineIdentity,
      val historicalIdentity: MySqlApprovedBaselineIdentity,
      val ioIdentity: MySqlOwnedIoIdentity
  ) extends AutoCloseable:
    def schema: String = currentIdentity.schema
    def serverUuid: java.util.UUID = currentIdentity.serverUuid
    def bindingUuid: java.util.UUID = currentIdentity.bindingUuid
    def provisioningUuid: java.util.UUID = historicalIdentity.provisioningUuid
    override def close(): Unit =
      var failure: Throwable=null
      def settle(action: => Unit): Unit =
        try action
        catch case cause: Throwable =>
          if failure == null then failure=cause else MySqlOwnedSessionLifecycle.suppress(failure,cause)
      settle(inputs.close())
      settle(owner.close())
      settle(root.closeRootReadBudget())
      if failure != null then throw failure
      if root.accountedMemoryBytes != 0L || owner.hasUnsettledResources then
        throw new IllegalStateException("historical fixture cleanup left actual retained resources")

  /** 目录显式隔离current/history；不提供容量默认值，不启动SQL/native。 */
  def open(directory: Path, profile: MySqlDeploymentReceiptSchemaProfile.Inputs,
      ioIdentity: MySqlOwnedIoIdentity): Owned =
    require(directory != null && profile != null && ioIdentity != null,"explicit historical test fixture inputs required")
    val current=fixture(directory.resolve("current"),profile)
    val old=DeploymentReceiptStaticTestData.fixture(directory.resolve("history"),profile)
    val pinned=pinHistory(current,old)
    val root=current.root()
    var owner: MySqlRuntimeInputs.AssemblyOwner=null
    var inputs: MySqlDbFirstSessionInputs=null
    try
      owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
      val graph=MySqlRuntimeInputs.assembleDbFirstStatic(pinned,owner,policy)
      inputs=MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,ioIdentity)
      val history=inputs.historicalInputs.getOrElse(throw new IllegalStateException("actual pinned historical holder missing"))
      history.requireOpen()
      val m=old.metadata.deploymentSemantics().canonicalFields()
      val pin=old.metadata.baseline()
      val historicalIdentity=MySqlApprovedBaselineIdentity(pin.format(),pin.schemaFormat(),pin.coverage(),
        java.util.UUID.fromString(m.get("expectedServerUuid")),pin.serverVersion(),m.get("schema"),pin.provisioningUuid(),
        java.util.UUID.fromString(m.get("expectedBindingUuid")),pin.ddlSha256(),pin.auditDefiner(),pin.sourceCaptureSha256())
      new Owned(inputs,root,history,owner,graph.approvedBaselineIdentity,historicalIdentity,ioIdentity)
    catch
      case cause: Throwable =>
        def cleanup(action: => Unit): Unit =
          try action catch case failure: Throwable => MySqlOwnedSessionLifecycle.suppress(cause,failure)
        if inputs != null then cleanup(inputs.close())
        if owner != null then cleanup(owner.close())
        cleanup(root.closeRootReadBudget())
        throw cause
