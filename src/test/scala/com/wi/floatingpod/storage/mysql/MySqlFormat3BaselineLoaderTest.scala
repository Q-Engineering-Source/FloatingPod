package com.wi.floatingpod.storage.mysql

import com.google.gson.JsonParser
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.sql.SQLNonTransientConnectionException
import java.util.{HexFormat, UUID}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/** 旧真实 fixture 仅供拒绝改版本升格测试；不声明存在 approved format3 pack。 */
class MySqlFormat3BaselineLoaderTest:
  @TempDir var directory: Path = _
  @Test def realOldPackCannotBecomeFormat3ByChangingTwoDeclarations(): Unit =
    val stream = getClass.getResourceAsStream("/com/wi/floatingpod/storage/mysql/approved-static-baseline-format2.json")
    if stream == null then throw new IllegalStateException("existing pinned format2 regression resource is missing")
    val original = try stream.readAllBytes() finally stream.close()
    val json = JsonParser.parseString(new String(original, StandardCharsets.UTF_8)).getAsJsonObject
    json.addProperty("schemaFormat", 3); json.addProperty("coverage", MySqlAuditCoverage.Format3)
    def text(key: String) = json.get(key).getAsString
    val identity = MySqlApprovedBaselineIdentity(text("format"), 3, MySqlAuditCoverage.Format3,
      UUID.fromString(text("serverUuid")), text("serverVersion"), text("schema"),
      UUID.fromString(text("provisioningUuid")), UUID.fromString(text("bindingUuid")), text("ddlSha256"),
      text("auditDefiner"), text("sourceCaptureSha256"))
    val bytes = json.toString.getBytes(StandardCharsets.UTF_8)
    val path = directory.resolve("declaration-only-is-not-approved-format3.json")
    Files.write(path, bytes)
    val sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
    assertThrows(classOf[SQLNonTransientConnectionException], () => MySqlApprovedBaselineLoader.load(path, sha, identity))
    assertThrows(classOf[SQLNonTransientConnectionException], () => MySqlApprovedBaselineLoader.load(path, "00" * 32, identity))
  @Test def format3IdentityRequiresTheExactNewCoveragePair(): Unit =
    val id = UUID.fromString("01000000-0000-4000-8000-000000000001")
    assertThrows(classOf[IllegalArgumentException], () => MySqlApprovedBaselineIdentity("FP_MYSQL_APPROVED_STATIC_V1", 3,
      MySqlAuditCoverage.Format2, id, "9.7.2", "fp_fixture", id, id, "ab" * 32, "audit@localhost", "cd" * 32))
