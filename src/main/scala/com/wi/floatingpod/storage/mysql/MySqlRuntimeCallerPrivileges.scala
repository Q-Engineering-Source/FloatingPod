package com.wi.floatingpod.storage.mysql

import com.google.gson.stream.{JsonReader, JsonToken}
import java.io.OutputStream
import java.sql.SQLDataException
import scala.collection.mutable

/** 固定 runtime 账户的封闭权限配方；原 SHOW 文本不在这里改写。 */
private[mysql] object MySqlRuntimeCallerPrivileges:
  val Format = "FP_MYSQL_RUNTIME_CALLER_PRIVILEGES_BE_V1"
  val User = "fp_f3_runtime"
  val Host = "localhost"
  val Schema = "fp_test_20261007_format3_b2"
  private[mysql] final case class Grant private[mysql] (
      scope: String, schema: String, obj: String, privilege: String, columns: Vector[String])
  final class Value private[MySqlRuntimeCallerPrivileges] (val grants: Vector[Grant]):
    override def equals(other: Any): Boolean = other match
      case value: Value => grants == value.grants
      case _ => false
    override def hashCode(): Int = grants.hashCode()
  private def fail(message: String): Nothing = throw new SQLDataException(message)
  private def expectedColumns(scope: String, obj: String, privilege: String): String = (scope,obj,privilege) match
    case ("PROCEDURE","fp_audit_schema_format3","EXECUTE") => ""
    case ("SCHEMA","","LOCK TABLES") => ""
    case ("TABLE","binding_bootstrap_intent","INSERT") => ""
    case ("TABLE","binding_bootstrap_intent","SELECT") => ""
    case ("TABLE","binding_bootstrap_intent","UPDATE") => "marker_ack_sha256,stage"
    case ("TABLE","floating_pod_state","DELETE") => ""
    case ("TABLE","floating_pod_state","INSERT") => ""
    case ("TABLE","floating_pod_state","SELECT") => ""
    case ("TABLE","floating_pod_state","UPDATE") => "dimension_id,is_active,last_mutation_operation_uuid,last_updated,operating_mode,owner_uuid,pitch,pos_x,pos_y,pos_z,state_version,world_name,yaw"
    case ("TABLE","fp_lifecycle_completion","INSERT") => ""
    case ("TABLE","fp_lifecycle_completion","SELECT") => ""
    case ("TABLE","fp_lifecycle_completion","UPDATE") => "completion_revision,consumed_by_operation_uuid,last_mutation_operation_uuid"
    case ("TABLE","fp_recovery_operation","INSERT") => ""
    case ("TABLE","fp_recovery_operation","SELECT") => ""
    case ("TABLE","fp_recovery_operation","UPDATE") => "commit_witness_bytes,commit_witness_sha256,operation_state,original_diagnostic_identity,original_diagnostic_sha256,result_at,settled_by_operation_uuid"
    case ("TABLE","fp_recovery_reservation","INSERT") => ""
    case ("TABLE","fp_recovery_reservation","SELECT") => ""
    case ("TABLE","fp_recovery_reservation","UPDATE") => "last_mutation_operation_uuid,operation_uuid,reservation_revision,reservation_state,workflow_uuid"
    case ("TABLE","fp_runtime_writer","SELECT") => ""
    case ("TABLE","fp_runtime_writer","UPDATE") => "last_commit_fingerprint,last_commit_operation_uuid,lease_until,owner_boot_uuid,writer_epoch"
    case ("TABLE","fp_schema_manifest","SELECT") => ""
    case ("TABLE","physical_footprint_transfer","INSERT") => ""
    case ("TABLE","physical_footprint_transfer","SELECT") => ""
    case ("TABLE","physical_footprint_transfer","UPDATE") => "cleanup_operation_uuid,cleanup_proof_bytes,cleanup_proof_sha256,commit_operation_uuid,last_mutation_operation_uuid,phase,transfer_revision"
    case ("TABLE","physical_recovery_binding","INSERT") => ""
    case ("TABLE","physical_recovery_binding","SELECT") => ""
    case ("TABLE","physical_recovery_binding","UPDATE") => "cutover_state"
    case ("TABLE","physical_recovery_footprint","INSERT") => ""
    case ("TABLE","physical_recovery_footprint","SELECT") => ""
    case ("TABLE","physical_recovery_footprint","UPDATE") => "seal_state"
    case ("TABLE","physical_recovery_footprint_chunk","INSERT") => ""
    case ("TABLE","physical_recovery_footprint_chunk","SELECT") => ""
    case ("TABLE","physical_recovery_intent","INSERT") => ""
    case ("TABLE","physical_recovery_intent","SELECT") => ""
    case ("TABLE","physical_recovery_intent","UPDATE") => "activation_operation_uuid,cleanup_operation_uuid,cleanup_proof_bytes,cleanup_proof_sha256,committed_at,committed_dimension_id,committed_domain_version,committed_pitch,committed_pos_x,committed_pos_y,committed_pos_z,committed_world_name,committed_yaw,intent_revision,last_mutation_operation_uuid,phase"
    case ("TABLE","physical_recovery_locator","INSERT") => ""
    case ("TABLE","physical_recovery_locator","SELECT") => ""
    case ("TABLE","physical_recovery_locator","UPDATE") => "current_attempt_uuid,current_footprint_uuid,domain_version,last_mutation_operation_uuid,locator_revision,locator_state,reason_code,target_dimension_id"
    case ("TABLE","physical_recovery_obligation","INSERT") => ""
    case ("TABLE","physical_recovery_obligation","SELECT") => ""
    case ("TABLE","physical_recovery_obligation","UPDATE") => "attempt_retirement_operation_uuid,last_mutation_operation_uuid,obligation_revision,obligation_state,verification_proof_bytes,verification_proof_sha256,verified_operation_uuid"
    case ("TABLE","physical_source_forest","INSERT") => ""
    case ("TABLE","physical_source_forest","SELECT") => ""
    case ("TABLE","physical_source_forest","UPDATE") => "forest_state"
    case ("TABLE","physical_source_obligation","INSERT") => ""
    case ("TABLE","physical_source_obligation","SELECT") => ""
    case ("TABLE","physical_source_obligation","UPDATE") => "discharge_kind,discharge_proof_bytes,discharge_proof_sha256,discharged_operation_uuid,last_mutation_operation_uuid,obligation_revision,obligation_state"
    case ("TABLE","physical_source_transfer","INSERT") => ""
    case ("TABLE","physical_source_transfer","SELECT") => ""
    case ("TABLE","physical_source_transfer","UPDATE") => "cutover_operation_uuid,last_mutation_operation_uuid,phase,retire_operation_uuid,retirement_proof_bytes,retirement_proof_sha256,target_receipts_bytes,target_receipts_sha256,transfer_revision"
    case ("TABLE","physical_source_unit","INSERT") => ""
    case ("TABLE","physical_source_unit","SELECT") => ""
    case ("TABLE","physical_source_unit","UPDATE") => "current_epoch,current_forest_uuid,last_mutation_operation_uuid,unit_revision,unit_state"
    case ("TABLE","physical_source_write","INSERT") => ""
    case ("TABLE","physical_source_write","SELECT") => ""
    case ("TABLE","physical_source_write","UPDATE") => "durable_receipt_bytes,durable_receipt_sha256,failure_diagnostic_identity,failure_diagnostic_sha256,last_mutation_operation_uuid,resolved_operation_uuid,write_revision,write_state"
    case ("TABLE","player_entitlement","INSERT") => ""
    case ("TABLE","player_entitlement","SELECT") => ""
    case _ => fail("runtime caller privilege/object is not in the exact approved recipe")
  private def name(reader: JsonReader, expected: String): Unit =
    if !reader.hasNext || reader.nextName() != expected then fail("callerPrivileges missing/unknown/duplicate/out-of-order key")
  private def text(reader: JsonReader): String =
    if reader.peek() != JsonToken.STRING then fail("callerPrivileges text type required")
    val value=reader.nextString()
    if value.length > 256 then fail("callerPrivileges text exceeds fixed name bound")
    value
  private def end(reader: JsonReader): Unit =
    if reader.hasNext then fail("callerPrivileges extra key")
    reader.endObject()
  private def key(g: Grant): String = g.scope+"/"+g.schema+"/"+g.obj+"/"+g.privilege+"/"+g.columns.mkString(",")
  /** 调用方先付 typed graph/workspace；独立 P 必须已经是唯一 canonical 顺序。 */
  def read(reader: JsonReader): Value =
    reader.beginObject()
    name(reader,"format"); if text(reader) != Format then fail("caller privilege encoding differs")
    name(reader,"grants");reader.beginArray()
    val out=Vector.newBuilder[Grant]
    var previous=""
    var count=0
    while reader.hasNext do
      if count >= 58 then fail("caller privilege record count exceeds exact recipe")
      reader.beginObject();name(reader,"columns");reader.beginArray()
      val columns=Vector.newBuilder[String]
      var last=""
      var width=0
      while reader.hasNext do
        if width >= 16 then fail("caller privilege column count exceeds exact recipe")
        val value=text(reader)
        if value <= last then fail("caller privilege columns duplicate or unordered")
        last=value;columns+=value;width+=1
      reader.endArray()
      name(reader,"grantOption")
      if reader.peek() != JsonToken.BOOLEAN || reader.nextBoolean() then fail("caller grant option is forbidden")
      name(reader,"object");val obj=text(reader)
      name(reader,"privilege");val privilege=text(reader)
      name(reader,"schema");val schema=text(reader)
      name(reader,"scope");val scope=text(reader);end(reader)
      val cols=columns.result()
      if schema != Schema || cols.mkString(",") != expectedColumns(scope,obj,privilege) then fail("caller privilege scope/columns differ")
      val grant=Grant(scope,schema,obj,privilege,cols)
      val order=key(grant)
      if order <= previous then fail("caller privilege records duplicate or unordered")
      previous=order;out+=grant;count+=1
    reader.endArray()
    if count != 58 then fail("caller privilege recipe is incomplete")
    name(reader,"principal");reader.beginObject()
    name(reader,"host");if text(reader) != Host then fail("caller privilege host differs")
    name(reader,"user");if text(reader) != User then fail("caller privilege user differs")
    end(reader);end(reader)
    new Value(out.result())
  def write(output: OutputStream,value: Value): Unit =
    import MySqlConfigurationRecipeCodec.{ascii,quoted}
    if value == null then fail("caller privilege value required")
    ascii(output,"{\"format\":");quoted(output,Format);ascii(output,",\"grants\":[")
    var i=0
    while i < value.grants.size do
      val g=value.grants(i)
      if i != 0 then ascii(output,",")
      ascii(output,"{\"columns\":[")
      var j=0
      while j < g.columns.size do
        if j != 0 then ascii(output,",")
        quoted(output,g.columns(j));j+=1
      ascii(output,"],\"grantOption\":false,\"object\":");quoted(output,g.obj)
      ascii(output,",\"privilege\":");quoted(output,g.privilege)
      ascii(output,",\"schema\":");quoted(output,g.schema)
      ascii(output,",\"scope\":");quoted(output,g.scope);ascii(output,"}");i+=1
    ascii(output,"],\"principal\":{\"host\":");quoted(output,Host)
    ascii(output,",\"user\":");quoted(output,User);ascii(output,"}}")

  /** 工作区在分词/数组前从原 workflow root 预收；只保留固定58组累计位。 */
  def requireObservation(value: Value, raw: Vector[String], root: MySqlFormat3ReadBudget): Unit =
    if value == null || raw == null || raw.isEmpty || raw.size > MySqlSchemaAuditor.MaxRowsPerResultSet then fail("complete runtime caller grants required")
    var units=0L
    var at=0
    while at < raw.size do
      val line=raw(at)
      if line == null || line.length > MySqlSchemaAuditor.MaxCellBytes then fail("runtime grant text exceeds existing bound")
      units=Math.addExact(units,line.length.toLong);at+=1
    val fee=Math.addExact(65536L,Math.multiplyExact(units,128L))
    MySqlManifestConfigurationIdentity.requireRoot(root)
    root.reserveTemporaryMemory(fee,"V3 caller privilege workspace BEFORE allocation")
    var primary: Throwable=null
    try
      val seen=new Array[Long](58)
      var usage=false
      at=0
      while at < raw.size do
        val parser=new Observation(raw(at),value,seen,User,Host,Schema,null)
        if parser.read() then
          if usage then fail("duplicate global USAGE marker")
          usage=true
        at+=1
      var i=0
      while i < 58 do
        val width=value.grants(i).columns.size
        val required=if width == 0 then 1L else (1L << width)-1L
        if seen(i) != required then fail("runtime caller is missing an approved permission")
        i+=1
    catch
      case cause: Throwable => primary=cause;throw cause
    finally
      try root.releaseTemporaryMemory(fee,"V3 caller privilege workspace dropped")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup
        else if cleanup ne primary then primary.addSuppressed(cleanup)

  /** 使用格式3既有词法器，按显式发布账号/schema解析同连接 SHOW GRANTS；只比较有界 DATA 与渲染器集合，不签发运行凭据或资格。 */
  private[mysql] def requireScopedObservation(raw: Vector[String], user: String, host: String,
      schema: String, expected: Set[Grant], root: MySqlFormat3ReadBudget): Unit =
    if raw == null || raw.isEmpty || raw.size > MySqlSchemaAuditor.MaxRowsPerResultSet ||
      expected == null || expected.isEmpty || user == null || !user.matches("[A-Za-z0-9_]{1,32}") ||
      host == null || !host.matches("[A-Za-z0-9_.:%-]{1,255}") || schema == null ||
      !MySqlSchemaAuditor.SchemaIdentifier.matches(schema) then
      fail("explicit bounded runtime grant scope and renderer-derived permissions are required")
    var units=0L
    var at=0
    while at < raw.size do
      val line=raw(at)
      val bytes=MySqlSchemaAuditor.boundedUtf8ByteLength(line,MySqlSchemaAuditor.MaxCellBytes,"runtime SHOW GRANTS")
      units=Math.addExact(units,bytes.toLong);at+=1
    val fee=Math.addExact(65536L,Math.multiplyExact(units,128L))
    MySqlManifestConfigurationIdentity.requireRoot(root)
    root.reserveTemporaryMemory(fee,"managed scoped SHOW GRANTS workspace BEFORE allocation")
    var primary: Throwable=null
    try
      val observed=mutable.HashSet.empty[Grant]
      at=0
      while at < raw.size do
        new Observation(raw(at),null,null,user,host,schema,observed).read()
        at+=1
      if observed.toSet != expected then fail("managed runtime SHOW GRANTS differs from the exact release-rendered account/scope permissions")
    catch
      case cause: Throwable => primary=cause;throw cause
    finally
      try root.releaseTemporaryMemory(fee,"managed scoped SHOW GRANTS workspace dropped")
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup
        else if cleanup ne primary then primary.addSuppressed(cleanup)

  private final class Observation(input: String, expected: Value, seen: Array[Long],
      expectedUser: String, expectedHost: String, expectedSchema: String,
      scopedObserved: mutable.Set[Grant]):
    private var at=0
    private def ws(): Unit =
      while at < input.length && (input.charAt(at) == ' ' || input.charAt(at) == '\t' || input.charAt(at) == '\r' || input.charAt(at) == '\n' || input.charAt(at) == '\f' || input.charAt(at) == 11.toChar) do at+=1
    private def consume(c: Char): Boolean =
      ws()
      if at < input.length && input.charAt(at) == c then {at+=1;true} else false
    private def punctuation(c: Char): Unit = if !consume(c) then fail("unsupported SHOW GRANTS punctuation")
    private def word(): String =
      ws();val start=at
      while at < input.length && ((input.charAt(at) >= 'A' && input.charAt(at) <= 'Z') || (input.charAt(at) >= 'a' && input.charAt(at) <= 'z')) do at+=1
      if at == start then fail("unsupported SHOW GRANTS syntax")
      input.substring(start,at).toUpperCase(java.util.Locale.ROOT)
    private def keyword(expected: String): Unit = if word() != expected then fail("unsupported SHOW GRANTS keyword")
    private def quoted(account: Boolean): String =
      ws()
      if at >= input.length || (input.charAt(at) != '`' && (!account || input.charAt(at) != '\'')) then fail("SHOW GRANTS quoted token required")
      val quote=input.charAt(at);at+=1
      val out=new java.lang.StringBuilder()
      var ended=false
      while at < input.length && !ended do
        val c=input.charAt(at);at+=1
        if c == '\\' then fail("SHOW GRANTS backslash escapes are unsupported")
        if c == quote then
          if at < input.length && input.charAt(at) == quote then {out.append(quote);at+=1}
          else ended=true
        else out.append(c)
        if out.length() > 256 then fail("SHOW GRANTS identifier exceeds bound")
      if !ended then fail("unterminated SHOW GRANTS quoted token")
      out.toString
    private def record(scope: String,schema: String,obj: String,privilege: String,columns: Vector[String]): Unit =
      if schema != expectedSchema then fail("runtime privilege escapes the approved schema")
      if scopedObserved != null then
        if columns.nonEmpty && columns.distinct.size != columns.size then fail("runtime privilege repeats a column scope")
        val grants=if columns.isEmpty then Vector(Grant(scope,schema,obj,privilege,Vector.empty))
          else columns.map(column=>Grant(scope,schema,obj,privilege,Vector(column)))
        grants.foreach { grant =>
          if !scopedObserved.add(grant) then fail("runtime SHOW GRANTS repeats a privilege scope")
        }
        return
      var i=0
      while i < expected.grants.size && !(expected.grants(i).scope == scope && expected.grants(i).obj == obj && expected.grants(i).privilege == privilege) do i+=1
      if i == expected.grants.size then fail("extra runtime privilege/object")
      val approved=expected.grants(i).columns
      if columns.isEmpty then
        if approved.nonEmpty then fail("table-wide privilege exceeds approved columns")
        seen(i)|=1L
      else
        if approved.isEmpty then fail("column privilege cannot substitute table-wide permission")
        var j=0
        while j < columns.size do
          val index=approved.indexOf(columns(j))
          if index < 0 then fail("extra runtime privilege column")
          seen(i)|=(1L << index);j+=1
    def read(): Boolean =
      keyword("GRANT")
      val privileges=Vector.newBuilder[(String,Vector[String])]
      var more=true
      var count=0
      while more do
        if count >= input.length then fail("SHOW GRANTS privilege list exceeds text bound")
        var privilege=word()
        if privilege == "LOCK" then {keyword("TABLES");privilege="LOCK TABLES"}
        privilege match
          case "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "EXECUTE" | "LOCK TABLES" | "USAGE" => ()
          case _ => fail("forbidden runtime privilege/role/dynamic grant")
        val columns=Vector.newBuilder[String]
        if consume('(') then
          var width=0
          var next=true
          while next do
            if width >= input.length || (scopedObserved != null && width >= 256) then fail("SHOW GRANTS column list exceeds text bound")
            columns+=quoted(false);width+=1;next=consume(',')
          punctuation(')')
        privileges+=((privilege,columns.result()));count+=1;more=consume(',')
      keyword("ON")
      ws()
      var scope="TABLE"
      if at < input.length && input.charAt(at) != '`' && input.charAt(at) != '*' then
        keyword("PROCEDURE");scope="PROCEDURE"
      val schemaWildcard=consume('*')
      val schema=if schemaWildcard then "*" else quoted(false)
      punctuation('.')
      val objectWildcard=consume('*')
      val obj=if objectWildcard then "*" else quoted(false)
      keyword("TO");val user=quoted(true);punctuation('@');val host=quoted(true)
      if user != expectedUser || host != expectedHost then fail("SHOW GRANTS principal differs")
      ws();if at != input.length then fail("SHOW GRANTS trailing syntax/grant option is forbidden")
      val parsed=privileges.result()
      if schemaWildcard && objectWildcard then
        if scope != "TABLE" || parsed.size != 1 || parsed.head._1 != "USAGE" || parsed.head._2.nonEmpty then fail("global runtime privilege is forbidden")
        if scopedObserved != null && !scopedObserved.add(Grant("GLOBAL","*","*","USAGE",Vector.empty)) then
          fail("runtime SHOW GRANTS repeats its global USAGE scope")
        true
      else
        if schemaWildcard then fail("unsupported global object wildcard")
        if objectWildcard then
          if scope != "TABLE" then fail("unsupported routine wildcard")
          scope="SCHEMA"
        var i=0
        while i < parsed.size do
          record(scope,schema,if objectWildcard then "" else obj,parsed(i)._1,parsed(i)._2);i+=1
        false
