package com.wi.floatingpod.storage.mysql

import com.google.gson.{JsonArray, JsonObject, JsonParser}
import com.wi.floatingpod.config.MySqlRuntimeMetadata
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.HexFormat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.Assertions.*

/** 显式 synthetic static inputs；实际文件/pin/renderer/security 装配，不签 SQL/native 资格。 */
class MySqlDbFirstRuntimeConfigurationTest:
  import DeploymentReceiptStaticTestData.*
  @TempDir var directory: Path = _
  private case class Fixture(metadata: MySqlRuntimeMetadata.DbFirstStatic, fields: Map[String,String],
      contract: JsonObject, profile: MySqlDeploymentReceiptSchemaProfile.Inputs):
    def root(): MySqlFormat3ReadBudget = new MySqlFormat3ReadBudget(profile.sourceLimitProfile, profile.sourceLimitProfile.maxSqlPacketBytes())
  private def metadata(fields: Map[String,String]): MySqlRuntimeMetadata.DbFirstStatic =
    def escape(value: String): String = value.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r")
    MySqlRuntimeMetadata.fromDbFirstBytes(fields.toVector.sortBy(_._1).map((k,v) => s"$k=${escape(v)}\n").mkString.getBytes(StandardCharsets.UTF_8))
  private def fixture(actualGrantRows: Boolean = false): Fixture =
    val old = DeploymentReceiptStaticTestData.fixture(directory)
    val p = old.profile
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
    if actualGrantRows then
      // 真实 SHOW GRANTS 行仅替换 fixture 的 schema/account；授权内容及排序差异保持。
      import scala.jdk.CollectionConverters.*
      val reader = new java.io.InputStreamReader(classOf[MySqlDbFirstRuntimeConfigurationTest].getResourceAsStream("/mysql/rbss-runtime-grants-20261105.json"),StandardCharsets.UTF_8)
      val grants = try JsonParser.parseReader(reader).getAsJsonObject.getAsJsonArray("grants").asScala.toVector.map(_.getAsString)
        finally reader.close()
      val adjusted = grants.map(_.replace("`floatingpodrbssv1`","`"+p.schema+"`")
        .replace("`fp_m_ad04712552860810e5c383d6_r`@`127.0.0.1`","`"+p.accounts.runtime.user+"`@`"+p.accounts.runtime.host+"`"))
      val encoded = new JsonArray(); adjusted.sorted.foreach(encoded.add)
      contract.add("callerGrants",encoded)
    val contractBytes = gson.toJson(contract).getBytes(StandardCharsets.UTF_8)
    Files.write(directory.resolve("contract.json"),contractBytes)
    val fields = (old.properties - "deploymentReceiptRuntimeContractPath" - "deploymentReceiptRuntimeContractSha256" -
      "deploymentReceiptManifestConfigurationEncoding") ++ Map("approvedSchemaFormat"->"6",
      "approvedCoverage"->MySqlDbFirstSchemaDefinition.Coverage,"approvedDdlSha256"->rendered.sha256,
      "approvedBaselineSha256"->sha(baselineBytes),"dbFirstRuntimeContractPath"->directory.resolve("contract.json").toAbsolutePath.toString,
      "dbFirstRuntimeContractSha256"->sha(contractBytes),"dbFirstManifestConfigurationEncoding"->MySqlDbFirstRuntimeContract.Encoding)
    Fixture(metadata(fields),fields,contract,p)

  @Test def schema6ActualPinnedGraphOwnsConfigurationAndAuditLeaseUntilClose(): Unit =
    val f = fixture(); val root = f.root()
    val before = root.accountedMemoryBytes
    val owner = MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph = MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,owner,policy)
    assertEquals(6,graph.approvedBaselineIdentity.schemaFormat)
    val consumer = graph.openAuditInputs()
    val expectation = graph.auditExpectation(consumer)
    assertEquals(MySqlSchemaAuditProfile.DbFirst1,expectation.profile)
    assertEquals(graph.configurationSha256.toLowerCase,HexFormat.of().formatHex(consumer.configurationSha256.bytes))
    assertThrows(classOf[IllegalStateException], () => owner.close())
    consumer.close(); owner.close()
    assertEquals(before,root.accountedMemoryBytes)
    assertThrows(classOf[IllegalStateException], () => graph.configurationSha256)
    assertThrows(classOf[IllegalStateException], () => consumer.limitProfile)
    assertThrows(classOf[IllegalStateException], () => expectation.validateCapturedCallerGrants(Vector.empty))

  @Test def schema6RejectsChangedFilePinAndKeepsOriginalFailureForExactCleanup(): Unit =
    val f = fixture(); val root = f.root(); val before = root.accountedMemoryBytes
    val owner = MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    Files.writeString(directory.resolve("contract.json"),gson.toJson(f.contract)+" ")
    val cause = assertThrows(classOf[java.sql.SQLDataException], () =>
      MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,owner,policy))
    assertSame(cause,owner.failure.get)
    owner.close(); assertEquals(before,root.accountedMemoryBytes)

  @Test def schema6ConsumerRejectsForeignOwnerAccountAndThreadWithoutChangingTheirFees(): Unit =
    import scala.jdk.CollectionConverters.*
    val f = fixture(actualGrantRows=true); val firstRoot = f.root(); val otherRoot = f.root()
    val firstOwner = MySqlRuntimeInputs.beginV2(firstRoot,Thread.currentThread())
    val otherOwner = MySqlRuntimeInputs.beginV2(otherRoot,Thread.currentThread())
    val first = MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,firstOwner,policy)
    val other = MySqlRuntimeInputs.assembleDbFirstStatic(f.metadata,otherOwner,policy)
    val consumer = first.openAuditInputs(); val foreign = other.openAuditInputs()
    assertThrows(classOf[IllegalArgumentException], () => first.auditExpectation(foreign))
    val expected = first.auditExpectation(consumer)
    val retained = firstRoot.accountedMemoryBytes
    assertSame(expected,first.auditExpectation(consumer))
    assertEquals(retained,firstRoot.accountedMemoryBytes)
    val grants = f.contract.getAsJsonArray("callerGrants").asScala.toVector.map(_.getAsString)
    assertEquals(30,grants.size)
    assertNotEquals(grants,grants.reverse)
    consumer.validateCapturedCallerGrants(grants)
    consumer.validateCapturedCallerGrants(grants.reverse)
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => consumer.validateCapturedCallerGrants(grants.tail))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => consumer.validateCapturedCallerGrants(grants :+ grants.head))
    assertThrows(classOf[java.sql.SQLNonTransientConnectionException], () => consumer.validateCapturedCallerGrants(grants.updated(0,grants.head + " altered")))
    val generated = MySqlDbFirstSchemaProfile.render(f.profile)
    val oldSets = privileges(MySqlDeploymentReceiptSchemaProfile.render(f.profile))
    val sets = MySqlDbFirstSchemaProfile.expectedPrivilegeRows(generated).zipWithIndex.map { (rows,index) =>
      oldSets(index).copy(rows=rows.map(row => MySqlAuditRow(row.map(MySqlAuditText.apply))))
    }
    val scope = new MySqlCallerScope(server,1L,f.profile.schema,consumer.effectiveUser,"NONE",consumer.connectedUser,
      Some("9.7.2"),grants)
    consumer.validateSecurity(scope,scope,sets)
    val permutedScope = scope.copy(ownGrants=grants.reverse)
    consumer.validateSecurity(permutedScope,permutedScope,sets)
    consumer.validateSecurity(scope,permutedScope,sets)
    assertEquals(retained,firstRoot.accountedMemoryBytes,"grant permutation comparisons release all scratch")
    val foreignBytes = otherRoot.accountedMemoryBytes
    assertThrows(classOf[IllegalArgumentException], () => consumer.validateSecurity(scope,scope,sets,otherRoot))
    assertEquals(foreignBytes,otherRoot.accountedMemoryBytes)
    val observed = new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val worker = new Thread(() =>
      try consumer.close()
      catch case cause: Throwable => observed.set(cause))
    worker.start(); worker.join(3000L)
    assertFalse(worker.isAlive); assertInstanceOf(classOf[IllegalStateException],observed.get())
    assertEquals(retained,firstRoot.accountedMemoryBytes)
    consumer.close(); foreign.close(); firstOwner.close(); otherOwner.close()
    assertEquals(0L,firstRoot.accountedMemoryBytes); assertEquals(0L,otherRoot.accountedMemoryBytes)

  @Test def schema6MissingCompleteSecurityInputsCannotProduceConfigurationOrExpectation(): Unit =
    val f = fixture(); val invalid = f.contract.deepCopy(); invalid.remove("definerPrivileges")
    val bytes = gson.toJson(invalid).getBytes(StandardCharsets.UTF_8)
    Files.write(directory.resolve("contract.json"),bytes)
    val pinned = metadata(f.fields.updated("dbFirstRuntimeContractSha256",sha(bytes)))
    val root = f.root(); val owner = MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val cause = assertThrows(classOf[java.sql.SQLDataException], () => MySqlRuntimeInputs.assembleDbFirstStatic(pinned,owner,policy))
    assertSame(cause,owner.failure.get)
    assertEquals(0L,root.accountedMemoryBytes)
    owner.close()

  @Test def schema6FreshRecipeHasIndependentWireAndCannotBeReadAsSchema5(): Unit =
    val f = fixture()
    val source = HexFormat.of().parseHex(f.contract.get("sourceProvenanceHex").getAsString)
    assertArrayEquals(source,MySqlDbFirstRuntimeContract.decodeRecipe(source).canonicalBytes)
    assertThrows(classOf[java.sql.SQLDataException], () => MySqlDeploymentReceiptRuntimeContract.decodeRecipe(source))

  @Test def historySourceClosedShapeAndCanonicalPinAreRequiredByActualK6Assembly(): Unit =
    Vector("missing","relative","uppercase","extra").foreach { variant =>
      val f=fixture();val contract=f.contract.deepCopy()
      if variant == "missing" then contract.remove("historicalSchema5")
      else
        val source=new JsonObject()
        source.addProperty("kind",if variant == "extra" then "NONE" else "SCHEMA5")
        source.addProperty("metadataPath",if variant == "relative" then "history.properties" else directory.resolve("history.properties").toAbsolutePath.toString)
        source.addProperty("metadataSha256",if variant == "uppercase" then "A"*64 else "a"*64)
        contract.add("historicalSchema5",source)
      val raw=gson.toJson(contract).getBytes(StandardCharsets.UTF_8)
      Files.write(directory.resolve("contract.json"),raw)
      val pinned=metadata(f.fields.updated("dbFirstRuntimeContractSha256",sha(raw)))
      val root=f.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
      val cause=assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleDbFirstStatic(pinned,owner,policy))
      assertSame(cause,owner.failure.get);owner.close();assertEquals(0L,root.accountedMemoryBytes)
    }
