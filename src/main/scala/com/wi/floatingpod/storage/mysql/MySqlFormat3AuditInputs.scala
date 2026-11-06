package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.util.{HexFormat, UUID}
import java.sql.SQLNonTransientConnectionException

/** 独立 control 的验证输入，不是审计证书、来源 issuer 或 Ready。 */
final class MySqlFormat3AuditInputs(
    val serverUuid: UUID, val serverVersion: String, val schema: String,
    val effectiveUser: String, val connectedUser: String, val callerRoles: String,
    val provisioningUuid: UUID, val bindingUuid: UUID, val ddlSha256: String,
    val auditDefiner: String, val callerGrants: Vector[String],
    val definerPrivileges: Vector[MySqlAuditResultSet],
    val configurationSha256: MySqlAuditBinary, val sourceProvenanceKind: String,
    val sourceProvenance: MySqlAuditBinary, val sourceProvenanceSha256: MySqlAuditBinary,
    val limitProfile: SourceLimitProfile,
    private[mysql] val callerPrivileges: MySqlRuntimeCallerPrivileges.Value,
    private[mysql] val privilegeRoot: MySqlFormat3ReadBudget
):
  def this(serverUuid: UUID, serverVersion: String, schema: String, effectiveUser: String,
      connectedUser: String, callerRoles: String, provisioningUuid: UUID, bindingUuid: UUID,
      ddlSha256: String, auditDefiner: String, callerGrants: Vector[String],
      definerPrivileges: Vector[MySqlAuditResultSet], configurationSha256: MySqlAuditBinary,
      sourceProvenanceKind: String, sourceProvenance: MySqlAuditBinary,
      sourceProvenanceSha256: MySqlAuditBinary, limitProfile: SourceLimitProfile) =
    this(serverUuid,serverVersion,schema,effectiveUser,connectedUser,callerRoles,provisioningUuid,
      bindingUuid,ddlSha256,auditDefiner,callerGrants,definerPrivileges,configurationSha256,
      sourceProvenanceKind,sourceProvenance,sourceProvenanceSha256,limitProfile,null,null)
  require(serverUuid != null && provisioningUuid != null && bindingUuid != null, "format3 independent identities are required")
  require(serverVersion != null && schema != null && effectiveUser != null && connectedUser != null && callerRoles != null,
    "format3 independent caller identity is required")
  require(ddlSha256 != null && MySqlSchemaAuditor.Sha256Hex.matches(ddlSha256), "format3 independent DDL pin is required")
  require(auditDefiner != null && callerGrants != null && (callerPrivileges != null || callerGrants.nonEmpty) && callerGrants.forall(g => g != null && g.nonEmpty),
    "independent complete caller grants are required; no empty default")
  require(callerGrants.distinct.size == callerGrants.size, "independent caller grants contain duplicates")
  require(definerPrivileges != null && definerPrivileges.map(_.id) == MySqlAuditCoverage.Format3PrivilegeResultSetIds,
    "independent four definer privilege sets are required")
  definerPrivileges.zip(MySqlSchemaAuditor.Format3PrivilegeLabels).foreach { (set, labels) =>
    require(set.columns != null && set.columns.map(_.label) == labels && set.rows != null, "independent definer privilege shape differs")
    require(set.rows.forall(r => r != null && r.cells != null && r.cells.size == labels.size && r.cells.forall(_.isInstanceOf[MySqlAuditText])),
      "independent privileges require full exact text rows")
  }
  MySqlSchemaAuditor.rejectDuplicateRows(definerPrivileges)
  if callerPrivileges != null then
    require(callerGrants.isEmpty,"V3 does not accept simultaneous legacy caller grants")
    require(privilegeRoot != null && !privilegeRoot.isClosed && privilegeRoot.profile == limitProfile,
      "V3 privileges require the actual open workflow root")
    require(effectiveUser == "fp_f3_runtime@localhost" && schema == MySqlRuntimeCallerPrivileges.Schema && callerRoles == "NONE",
      "V3 runtime principal/schema/roles differs")
  private val at = auditDefiner.lastIndexOf('@')
  require(at > 0 && at < auditDefiner.length - 1, "independent audit definer account is incomplete")
  private val grantee = "'" + auditDefiner.substring(0, at).replace("'", "''") + "'@'" + auditDefiner.substring(at + 1).replace("'", "''") + "'"
  definerPrivileges.zipWithIndex.foreach { (set, index) =>
    set.rows.foreach { row =>
      def text(label: String): String = row.cells(set.columns.indexWhere(_.label == label)).asInstanceOf[MySqlAuditText].value
      require(text("GRANTEE") == grantee && text("IS_GRANTABLE") == "NO", "audit definer identity/grant option differs from minimal profile")
      if index < 3 then require(text("TABLE_SCHEMA") == schema, "audit definer privileges escape the exact schema")
      val allowed = if index == 1 then Set("SELECT") else if index == 3 then Set("USAGE") else Set("SELECT", "TRIGGER")
      require(allowed.contains(text("PRIVILEGE_TYPE")), "audit definer has an unsupported privilege")
    }
  }
  private val schemaPrivileges = definerPrivileges(2)
  private val privilegeIndex = schemaPrivileges.columns.indexWhere(_.label == "PRIVILEGE_TYPE")
  require(schemaPrivileges.rows.map(_.cells(privilegeIndex).asInstanceOf[MySqlAuditText].value).toSet == Set("SELECT", "TRIGGER"),
    "format3 audit definer requires exact FP-only schema SELECT/TRIGGER metadata visibility")
  require(configurationSha256 != null && configurationSha256.length == 32 && sourceProvenance != null &&
    Set("FRESH_PLAN", "FORMAT3_RESTORE", "FORMAT2_IMPORT", "SQLITE_IMPORT").contains(sourceProvenanceKind) && sourceProvenanceSha256 != null && sourceProvenanceSha256.length == 32 && limitProfile != null,
    "complete independent format3 source/profile inputs are required")
  require(sourceProvenance.hasSha256(sourceProvenanceSha256),
    "independent source provenance bytes/SHA disagree")
  private def reject(reason: String): Nothing = throw new SQLNonTransientConnectionException(reason)
  private[mysql] def requireIdentity(server: UUID, version: String, targetSchema: String, principal: String,
      roles: String, provisioning: UUID, binding: UUID, ddl: String, definer: String): Unit =
    if server != serverUuid || version != serverVersion || targetSchema != schema || principal != effectiveUser || roles != callerRoles ||
      provisioning != provisioningUuid || binding != bindingUuid || !ddl.equalsIgnoreCase(ddlSha256) || definer != auditDefiner then
      reject("independent format3 inputs belong to another exact schema/identity/profile")
  private[mysql] def validateCapturedCallerGrants(grants: Vector[String]): Unit =
    if grants.isEmpty || (callerPrivileges == null && grants.distinct.size != grants.size) then
      reject("SHOW GRANTS is empty or duplicated")
  private[mysql] def validateSecurity(before: MySqlCallerScope, after: MySqlCallerScope, privileges: Vector[MySqlAuditResultSet]): Unit =
    if before != after || before.serverUuid != serverUuid || before.serverVersion != Some(serverVersion) || before.defaultSchema != schema ||
      before.effectivePrincipal != effectiveUser || before.connectedUser != connectedUser || before.effectiveRoles != callerRoles ||
      (callerPrivileges == null && before.ownGrants != callerGrants) || privileges != definerPrivileges then
      reject("format3 caller identity/full grants or complete definer privileges differ from independent inputs")
    if callerPrivileges != null then MySqlRuntimeCallerPrivileges.requireObservation(callerPrivileges,before.ownGrants,privilegeRoot)
  private[mysql] def validateRetainedObservation(sets: Vector[MySqlAuditResultSet], before: MySqlCallerScope, after: MySqlCallerScope): Unit =
    var retained = 0L
    def add(count: Long): Unit =
      retained = Math.addExact(retained, count)
      if retained > limitProfile.maxRetainedBytes() then reject("complete format3 observation exceeds explicit retained framing")
    def text(value: String): Unit =
      val count = MySqlSchemaAuditor.encodeUtf8(value).length.toLong
      if count > limitProfile.maxSqlPacketBytes() then reject("format3 text exceeds explicit SQL packet bound")
      add(4L + count)
    for caller <- Vector(before, after) do
      add(16L + 8L + 4L)
      Vector(caller.serverVersion.getOrElse(reject("format3 caller version is missing")), caller.defaultSchema,
        caller.effectivePrincipal, caller.effectiveRoles, caller.connectedUser).foreach(text)
      caller.ownGrants.foreach(text)
    add(4L)
    sets.foreach { set =>
      text(set.id); add(8L)
      set.columns.foreach { column => text(column.label); text(column.typeName); add(17L) }
      set.rows.foreach { row =>
        add(4L)
        row.cells.foreach { cell =>
          add(1L)
          cell match
            case MySqlAuditNull => ()
            case MySqlAuditText(value) => text(value)
            case binary: MySqlAuditBinary =>
              val length = binary.length
              if length.toLong > limitProfile.maxSqlPacketBytes() then reject("format3 binary exceeds explicit SQL packet bound")
              add(4L + length.toLong)
            case MySqlAuditTimestamp(value) => text(value.toString)
        }
      }
    }
  private[mysql] val manifestLabels = Vector("configuration_sha256", "source_provenance_kind", "source_provenance_bytes", "source_provenance_sha256",
    "limit_profile_encoding", "limit_profile_bytes", "limit_profile_sha256", "max_region_bytes", "max_chunk_bytes", "max_decompressed_bytes",
    "max_nbt_depth", "max_members", "max_retained_bytes", "max_sql_packet_bytes", "max_native_artifact_bytes")
  private[mysql] def expectedManifestCells: Vector[MySqlAuditCell] = Vector(configurationSha256, MySqlAuditText(sourceProvenanceKind),
    sourceProvenance, sourceProvenanceSha256, MySqlAuditText(SourceLimitProfile.ENCODING), MySqlAuditBinary(limitProfile.canonicalBytes()),
    MySqlAuditBinary(limitProfile.sha256())) ++ Vector(limitProfile.maxRegionBytes(), limitProfile.maxChunkBytes(), limitProfile.maxDecompressedBytes(),
    limitProfile.maxNbtDepth(), limitProfile.maxMembers(), limitProfile.maxRetainedBytes(), limitProfile.maxSqlPacketBytes(), limitProfile.maxNativeArtifactBytes())
    .map(v => MySqlAuditText(v.toString))
  private[mysql] def validateManifest(set: MySqlAuditResultSet, row: MySqlAuditRow): Unit =
    val actual = manifestLabels.map { label =>
      val index = set.columns.indexWhere(_.label == label)
      if index < 0 || row.cells.size != set.columns.size then reject("format3 full manifest shape is incomplete")
      row.cells(index)
    }
    if actual != expectedManifestCells then reject("format3 full source/profile/budget manifest differs from independent inputs")
    // 唯一 codec 再核实际完整字节；预算必须来自这些字节而不是 detached scalar。
    val profile = SourceLimitProfile.decode(actual(5).asInstanceOf[MySqlAuditBinary].bytes, actual(6).asInstanceOf[MySqlAuditBinary].bytes)
    if profile != limitProfile then reject("format3 decoded LimitProfile differs from independent canonical bytes")
  override def toString: String = "MySqlFormat3AuditInputs[redacted; independent validation inputs only]"
