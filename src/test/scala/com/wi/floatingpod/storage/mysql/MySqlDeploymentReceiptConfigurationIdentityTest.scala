package com.wi.floatingpod.storage.mysql

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.Assertions.*

class MySqlDeploymentReceiptConfigurationIdentityTest:
  @TempDir var directory: Path = _
  private def configuration(f: DeploymentReceiptStaticTestData.Fixture,
      metadata: com.wi.floatingpod.config.MySqlRuntimeMetadata.DeploymentReceiptStatic,
      policy: MySqlRuntimeAuditPolicy = DeploymentReceiptStaticTestData.policy): String =
    val root=f.root(); val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    try MySqlRuntimeInputs.assembleDeploymentReceiptStatic(metadata,owner,policy).configurationSha256
    finally
      try owner.close() finally root.closeRootReadBudget()

  @Test def schema5DomainAndBeFramingHaveIndependentGoldenDigest(): Unit =
    val root=new MySqlFormat3ReadBudget(DeploymentReceiptStaticTestData.defaultProfile().sourceLimitProfile,1000000L)
    val digest=MySqlManifestConfigurationIdentity.digest(root,
      out=>out.write("{}".getBytes(StandardCharsets.US_ASCII)),MySqlDeploymentReceiptRuntimeContract.Encoding)
    try assertEquals("B71EC2F36F2043A6839667CCD1F3886BE55795CFC4E8A887E6FC42FB7E3E49B6",digest.hex)
    finally
      digest.close();assertEquals(0L,root.accountedMemoryBytes);root.closeRootReadBudget()


    // 独立 oracle 只读实际批准 K 的输入文件；不调用 production projection、canonicalFields 或 canonical encoder。
    // TempDir 是语义路径的一部分，保留实际绝对规范路径；不剥离、替换或固定为全局目录。
    import com.google.gson.{JsonArray, JsonElement, JsonObject, JsonParser}
    import scala.jdk.CollectionConverters.*
    val f=DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
    val input=JsonParser.parseString(Files.readString(directory.resolve("contract.json"),StandardCharsets.UTF_8)).getAsJsonObject
    val base=directory.toAbsolutePath.normalize()
    val domain="FP_MYSQL_DEPLOYMENT_RECEIPT1_MANIFEST_CONFIGURATION_BE_V1"
    val coverage="script-publication1-deployment-receipt1:exact26-tables-exact78-guards-auditself-and-privileges"
    def text(key: String): String = input.get(key).getAsString
    def selected(from: JsonObject, keys: Vector[String]): JsonObject =
      val result=new JsonObject()
      keys.foreach(k=>result.add(k,from.get(k).deepCopy()))
      result
    val expectedContract=selected(input,Vector("serverUuid","serverVersion","schema","effectiveUser","connectedUser",
      "callerRoles","provisioningUuid","bindingUuid","ddlSha256","auditDefiner","auditDefinerRoles","callerGrants",
      "physicalFormat","importState","maxReceiptAgeNanos","sourceProvenanceKind","sourceProvenanceHex",
      "sourceProvenanceSha256","limitProfileEncoding","limitProfileHex","limitProfileSha256","definerPrivileges"))
    expectedContract.addProperty("format","FP_MYSQL_DEPLOYMENT_RECEIPT1_RUNTIME_CONTRACT_V1")
    expectedContract.addProperty("schemaFormat",5)
    expectedContract.addProperty("coverage",coverage)
    expectedContract.addProperty("deploymentReceiptManifestConfigurationEncoding",domain)
    val expectedProfile=selected(input.getAsJsonObject("profile"),Vector("schema","accounts","sourceLimitProfile",
      "publicationBudget","deploymentBudget","quotaMaximumRecords","trustedAdministrativeProxyRows"))
    expectedContract.add("profile",expectedProfile)
    // Hex lexical case and set-row order are not semantic distinctions. Explicitly normalize the input, not actual C output.
    Vector("ddlSha256","sourceProvenanceHex","sourceProvenanceSha256","limitProfileHex","limitProfileSha256").foreach { k =>
      expectedContract.addProperty(k,expectedContract.get(k).getAsString.toLowerCase(java.util.Locale.ROOT))
    }
    val sourceProfile=expectedProfile.getAsJsonObject("sourceLimitProfile")
    Vector("hex","sha256").foreach(k=>sourceProfile.addProperty(k,sourceProfile.get(k).getAsString.toLowerCase(java.util.Locale.ROOT)))
    val grants=new JsonArray()
    input.getAsJsonArray("callerGrants").asScala.map(_.getAsString).toVector.sorted.foreach(v=>grants.add(v))
    expectedContract.add("callerGrants",grants)
    expectedContract.getAsJsonArray("definerPrivileges").asScala.foreach { element =>
      val set=element.getAsJsonObject
      val rows=new JsonArray()
      set.getAsJsonArray("rows").asScala.toVector.sortBy(row=>
        row.getAsJsonArray.asScala.map(_.getAsJsonObject.get("value").getAsString).mkString("\u0000"))
        .foreach(row=>rows.add(row))
      set.add("rows",rows)
    }
    // This is the complete fixture metadata input specification, independently enumerated rather than filtering a production map.
    val expectedMetadata=new JsonObject()
    val metadata=Map("backend"->"mysql","host"->"database.example.invalid","port"->"3307","schema"->"fp_schema5_test",
      "expectedServerUuid"->"982937bc-bc46-11f1-b957-04421a0aa74d",
      "expectedBindingUuid"->"9ff22189-4730-41f6-80d8-3a3bdf971604",
      "expectedEffectiveUser"->"fp_s5_runtime@127.0.0.1","username"->"fp_s5_runtime",
      "pkcs12TrustStorePath"->base.resolve("trust.p12").toString,
      "credentialSourceRef"->("file:"+base.resolve("credential.txt")),
      "trustSecretSourceRef"->("file:"+base.resolve("trust-secret.txt")),
      "connectTimeoutMillis"->"3000","socketTimeoutMillis"->"10000","activeLimit"->"17",
      "innodbFlushLogAtTrxCommit"->"1","syncBinlog"->"1",
      "approvedBaselinePath"->base.resolve("baseline.json").toString,
      "approvedBaselineFormat"->"FP_MYSQL_APPROVED_STATIC_V1","approvedSchemaFormat"->"5","approvedCoverage"->coverage,
      "expectedServerVersion"->"9.7.2","expectedProvisioningUuid"->"eea917d4-e945-4c54-b666-5ba68d666f6f",
      "approvedDdlSha256"->text("ddlSha256").toUpperCase(java.util.Locale.ROOT),"expectedAuditDefiner"->"fp_s5_audit@127.0.0.1",
      "deploymentReceiptRuntimeContractPath"->base.resolve("contract.json").toString,
      "deploymentReceiptManifestConfigurationEncoding"->domain)
    metadata.foreach((k,v)=>expectedMetadata.addProperty(k,v))
    val expectedPolicy=new JsonObject()
    expectedPolicy.addProperty("auditDefinerRoles","NONE");expectedPolicy.addProperty("callerRoles","NONE")
    expectedPolicy.addProperty("importState","NONE");expectedPolicy.addProperty("maxReceiptAgeNanos",20000000000L)
    expectedPolicy.addProperty("physicalFormat",2)
    val projection=new JsonObject()
    projection.add("contract",expectedContract);projection.add("metadata",expectedMetadata);projection.add("policy",expectedPolicy)
    // Independent RFC8259 scalar escaping + sorted object members + ordered arrays; no Gson/production serialization.
    def quote(value: String): String =
      val out=new StringBuilder()
      out.append(34.toChar)
      value.foreach {
        case c if c == 34.toChar || c == 92.toChar => out.append(92.toChar).append(c)
        case c if c == 8.toChar => out.append(92.toChar).append('b')
        case c if c == 12.toChar => out.append(92.toChar).append('f')
        case c if c == 10.toChar => out.append(92.toChar).append('n')
        case c if c == 13.toChar => out.append(92.toChar).append('r')
        case c if c == 9.toChar => out.append(92.toChar).append('t')
        case c if c < 32.toChar || c.toInt == 0x2028 || c.toInt == 0x2029 =>
          out.append(92.toChar).append('u').append(f"${c.toInt}%04x")
        case c => out.append(c)
      }
      out.append(34.toChar).toString
    def canonical(value: JsonElement): String =
      if value.isJsonObject then
        val o=value.getAsJsonObject
        o.keySet().asScala.toVector.sorted.map(k=>quote(k)+":"+canonical(o.get(k))).mkString("{",",","}")
      else if value.isJsonArray then value.getAsJsonArray.asScala.map(canonical).mkString("[",",","]")
      else
        val primitive=value.getAsJsonPrimitive
        if primitive.isString then quote(primitive.getAsString)
        else if primitive.isBoolean then primitive.getAsBoolean.toString
        else primitive.getAsBigDecimal.toPlainString
    val preimage=canonical(projection).getBytes(StandardCharsets.UTF_8)
    val framed=new java.io.ByteArrayOutputStream()
    val be=new java.io.DataOutputStream(framed)
    val tag=domain.getBytes(StandardCharsets.US_ASCII)
    be.writeInt(tag.length);be.write(tag);be.writeInt(preimage.length);be.write(preimage);be.flush()
    val expected=java.util.HexFormat.of().withUpperCase().formatHex(
      java.security.MessageDigest.getInstance("SHA-256").digest(framed.toByteArray))
    // configuration invokes actual B/K file loading and the full owner-backed static assembly, not a proposal encoder.
    assertEquals(expected,configuration(f,f.metadata))

  @Test def sameRendererHAndDWithDifferentMetadataMustChangeCompleteC(): Unit =
    val f=DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
    val h=f.profile.profileIdentitySha256
    val first=configuration(f,f.metadata)
    val second=configuration(f,f.rebind(Map("host"->"other.example.invalid")))
    assertNotEquals(first,second)
    assertEquals(h,f.profile.profileIdentitySha256)
    assertEquals(f.contract.get("ddlSha256").getAsString,MySqlDeploymentReceiptSchemaProfile.render(f.profile).sha256)

  @Test def backwardRawPinsAreExcludedFromCButActualFilesAndCompleteAStillVerifyThem(): Unit =
    import DeploymentReceiptStaticTestData.*
    val f=fixture(directory.toAbsolutePath)
    val first=configuration(f,f.metadata)
    val path=f.metadata.baseline().absolutePath()
    val capture=sha("second independently approved synthetic capture".getBytes(StandardCharsets.UTF_8))
    val pack=com.google.gson.JsonParser.parseString(Files.readString(path)).getAsJsonObject
    pack.addProperty("sourceCaptureSha256",capture)
    val bytes=gson.toJson(pack).getBytes(StandardCharsets.UTF_8)++Array[Byte](10)
    Files.write(path,bytes)
    val root=f.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val failure=assertThrows(classOf[Exception],()=>MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata,owner,policy))
    assertSame(failure,owner.failure.get);assertEquals(0L,root.accountedMemoryBytes);root.closeRootReadBudget()
    val k=f.contract.deepCopy();k.addProperty("approvedBaselineSha256",sha(bytes));k.addProperty("sourceCaptureSha256",capture)
    val kb=gson.toJson(k).getBytes(StandardCharsets.UTF_8);Files.write(f.metadata.runtimeContract().absolutePath(),kb)
    val rebound=f.rebind(Map("approvedBaselineSha256"->sha(bytes),"approvedSourceCaptureSha256"->capture,"deploymentReceiptRuntimeContractSha256"->sha(kb)))
    assertEquals(first,configuration(f,rebound))
    assertNotEquals(f.metadata.completeCanonicalFields(),rebound.completeCanonicalFields())

  @Test def changedPolicyAndProfileCannotReuseApprovedK(): Unit =
    val f=DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
    val root=f.root();val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val other=DeploymentReceiptStaticTestData.policy.copy(maxReceiptAge=java.time.Duration.ofSeconds(19))
    val failure=assertThrows(classOf[java.sql.SQLDataException],()=>MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata,owner,other))
    assertSame(failure,owner.failure.get);assertFalse(owner.hasUnsettledResources)
    assertEquals(0L,root.accountedMemoryBytes);root.closeRootReadBudget()

  @Test def sameRendererHWithDifferentApprovedPolicyMustChangeCompleteC(): Unit =
    val f=DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
    val h=f.profile.profileIdentitySha256
    val first=configuration(f,f.metadata)
    val k=f.contract.deepCopy();k.addProperty("maxReceiptAgeNanos",19000000000L)
    val metadata=f.saveContract(k)
    val policy=DeploymentReceiptStaticTestData.policy.copy(maxReceiptAge=java.time.Duration.ofSeconds(19))
    assertNotEquals(first,configuration(f,metadata,policy))
    assertEquals(h,f.profile.profileIdentitySha256)
