package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.entity.DbFirstPodSnapshot
import java.security.MessageDigest
import java.sql.SQLDataException

/** Full27 中最新快照与原始提交的关系校验；不把历史 Full26 投影冒充完整数据库。 */
private[mysql] object MySqlDbFirstSnapshotRows:
  def validate(relations: Vector[MySqlFormat3RelationSnapshot], inputs: MySqlScriptSchemaInputs,
      budget: MySqlFormat3ReadBudget): Unit =
    require(inputs != null && inputs.dataLayout == MySqlScriptSchemaDataLayout.DbFirstFull27 && budget != null,
      "exact Full27 inputs and allocation owner required")
    budget.requireScriptDataInputs(inputs)
    val snapshots = relation(relations, MySqlDbFirstSchemaDefinition.SnapshotTable)
    val domains = relation(relations, "floating_pod_state")
    val operations = relation(relations, "fp_recovery_operation")
    val intents = relation(relations, "physical_recovery_intent")
    val manifests = relation(relations, "fp_schema_manifest")
    val bindings = relation(relations, "physical_recovery_binding")
    val charge = Math.addExact(4096L, Math.multiplyExact(512L,
      Math.addExact(intents.rows.size.toLong, Math.addExact(snapshots.rows.size.toLong, Math.addExact(domains.rows.size.toLong, operations.rows.size.toLong)))))
    budget.reserveTemporaryMemory(charge, "Full27 snapshot identity indexes and digest workspace")
    var primary: Throwable = null
    try
      val domainIndex = domains.rows.map(r => binary(r, domains.table, "uuid", inputs) -> r).toMap
      val operationIndex = operations.rows.map(r =>
        (binary(r, operations.table, "binding_uuid", inputs), binary(r, operations.table, "operation_uuid", inputs)) -> r).toMap
      if domainIndex.size != domains.rows.size || operationIndex.size != operations.rows.size then fail("duplicate domain or operation identity")
      val snapshotIndex = snapshots.rows.map(r =>
        (binary(r, snapshots.table, "binding_uuid", inputs), binary(r, snapshots.table, "pod_uuid", inputs)) -> r).toMap
      if snapshotIndex.size != snapshots.rows.size then fail("duplicate latest snapshot identity")
      val latestSnapshotOperation = scala.collection.mutable.HashMap.empty[(MySqlFormat3Binary, MySqlFormat3Binary), Vector[MySqlFormat3Cell]]
      operations.rows.foreach { row =>
        val step=cell(row, operations.table, "step", inputs)
        if (step == MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallStep) ||
          step == MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.InitialActivationStep) ||
          step == MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.CheckpointStep)) &&
          cell(row, operations.table, "operation_state", inputs) == MySqlFormat3Cell.Text("COMMITTED") then
          val key = (uuid(row, operations.table, "binding_uuid", inputs), uuid(row, operations.table, "pod_uuid", inputs))
          val version = integer(row, operations.table, "expected_domain_version", inputs)
          if version < 0L || version == Long.MaxValue ||
            (step == MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.InitialActivationStep) && version != 0L) then
            fail("committed snapshot operation has invalid captured version")
          latestSnapshotOperation.get(key) match
            case Some(previous) =>
              val oldVersion = integer(previous, operations.table, "expected_domain_version", inputs)
              if version == oldVersion then fail("two committed snapshot operations claim the same captured domain version")
              if version > oldVersion then latestSnapshotOperation.update(key, row)
            case None => latestSnapshotOperation.update(key, row)
      }
      latestSnapshotOperation.foreach { (key, operation) =>
        val snapshot = snapshotIndex.getOrElse(key, fail("latest committed snapshot operation has no recovery snapshot"))
        if binary(snapshot, snapshots.table, "last_mutation_operation_uuid", inputs) !=
          binary(operation, operations.table, "operation_uuid", inputs) then fail("recovery snapshot predates the latest committed snapshot operation")
      }
      val profileBytes = inputs.sourceLimitProfile.canonicalBytes()
      val profileSha = inputs.sourceLimitProfile.sha256()
      snapshots.rows.foreach { row =>
        val table = snapshots.table
        val binding = uuid(row, table, "binding_uuid", inputs)
        val pod = uuid(row, table, "pod_uuid", inputs)
        val attempt = uuid(row, table, "attempt_uuid", inputs)
        val operation = uuid(row, table, "last_mutation_operation_uuid", inputs)
        val owner = cell(row, table, "owner_uuid", inputs)
        if owner != MySqlFormat3Cell.SqlNull then uuid(row, table, "owner_uuid", inputs)
        val captured = integer(row, table, "captured_state_version", inputs)
        val committed = integer(row, table, "committed_state_version", inputs)
        if captured < 0L || captured == Long.MaxValue || committed != captured + 1L then fail("snapshot version transition")
        if integer(row, table, "snapshot_format_version", inputs) != DbFirstPodSnapshot.FORMAT_VERSION then fail("snapshot payload encoding")
        val nbt = binary(row, table, "nbt_bytes", inputs)
        if nbt.length == 0 || nbt.length.toLong > MySqlDbFirstSchemaDefinition.maximumNbtBytes(inputs.sourceLimitProfile) then
          fail("snapshot NBT length exceeds its actual profile")
        checkDigest(nbt, binary(row, table, "nbt_sha256", inputs))
        if cell(row, table, "limit_profile_encoding", inputs) != MySqlFormat3Cell.Text(SourceLimitProfile.ENCODING) ||
          !binary(row, table, "limit_profile_bytes", inputs).equalsBytes(profileBytes) ||
          !binary(row, table, "limit_profile_sha256", inputs).equalsBytes(profileSha) then fail("snapshot profile identity")
        if manifests.rows.size != 1 || bindings.rows.size != 1 then fail("snapshot requires exact manifest and physical binding")
        val manifest = manifests.rows.head
        if integer(manifest, manifests.table, "format_version", inputs) != 6 ||
          binary(manifest, manifests.table, "expected_binding_uuid", inputs) != binding ||
          !binary(manifest, manifests.table, "limit_profile_bytes", inputs).equalsBytes(profileBytes) ||
          !binary(manifest, manifests.table, "limit_profile_sha256", inputs).equalsBytes(profileSha) ||
          binary(bindings.rows.head, bindings.table, "binding_uuid", inputs) != binding then fail("snapshot authority or manifest differs")
        val domain = domainIndex.getOrElse(pod, fail("snapshot has no domain"))
        val currentVersion = integer(domain, domains.table, "state_version", inputs)
        if binary(domain, domains.table, "binding_uuid", inputs) != binding || currentVersion < committed then fail("snapshot is ahead of its domain")
        val original = operationIndex.getOrElse((binding, operation), fail("snapshot has no original operation"))
        val step=cell(original, operations.table, "step", inputs)
        val initial=step == MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.InitialActivationStep)
        val checkpoint=step == MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.CheckpointStep)
        if (!initial && !checkpoint && step != MySqlFormat3Cell.Text(MySqlDbFirstSchemaDefinition.RecallStep)) ||
          cell(original, operations.table, "operation_state", inputs) != MySqlFormat3Cell.Text("COMMITTED") ||
          binary(original, operations.table, "pod_uuid", inputs) != pod ||
          binary(original, operations.table, "attempt_uuid", inputs) != attempt ||
          integer(original, operations.table, "expected_domain_version", inputs) != captured then fail("snapshot original commit identity differs")
        if initial then
          if captured != 0L || owner != MySqlFormat3Cell.SqlNull then fail("initial snapshot must capture ownerless prepared v0")
          val matching=intents.rows.filter(r=>
            cell(r,intents.table,"binding_uuid",inputs)==MySqlFormat3Cell.Binary(binding) &&
            cell(r,intents.table,"pod_uuid",inputs)==MySqlFormat3Cell.Binary(pod) &&
            cell(r,intents.table,"attempt_uuid",inputs)==MySqlFormat3Cell.Binary(attempt))
          if matching.size!=1 then fail("initial snapshot requires original activation intent")
          val intent=matching.head
          uuid(intent,intents.table,"create_operation_uuid",inputs)
          val prepare=uuid(intent,intents.table,"prepare_operation_uuid",inputs)
          val preparation=operationIndex.getOrElse((binding,prepare),fail("initial snapshot lacks original preparation commit"))
          if cell(intent,intents.table,"origin",inputs)!=MySqlFormat3Cell.Text("DEBUG_ITEM_CREATE") ||
            cell(intent,intents.table,"phase",inputs)!=MySqlFormat3Cell.Text("ACTIVATED") ||
            binary(intent,intents.table,"activation_operation_uuid",inputs)!=operation ||
            integer(intent,intents.table,"expected_domain_version",inputs)!=0L ||
            integer(intent,intents.table,"committed_domain_version",inputs)!=1L ||
            cell(intent,intents.table,"expected_owner_uuid",inputs)!=MySqlFormat3Cell.SqlNull ||
            cell(preparation,operations.table,"step",inputs)!=MySqlFormat3Cell.Text("REARM") ||
            cell(preparation,operations.table,"operation_state",inputs)!=MySqlFormat3Cell.Text("COMMITTED") ||
            binary(preparation,operations.table,"pod_uuid",inputs)!=pod ||
            binary(preparation,operations.table,"attempt_uuid",inputs)!=attempt then fail("initial snapshot preparation/activation lineage differs")
        // 较新domain可以保留旧快照；这不是当前完整内容的恢复许可。
        if currentVersion == committed && (integer(domain, domains.table, "is_active", inputs) != (if initial || checkpoint then 1L else 0L) ||
          cell(domain, domains.table, "owner_uuid", inputs) != owner ||
          binary(domain, domains.table, "last_mutation_operation_uuid", inputs) != operation ||
          (initial && cell(domain,domains.table,"operating_mode",inputs)!=MySqlFormat3Cell.Text("WORLD_IDLE"))) then
          fail("snapshot and original committed domain disagree")
      }
    catch
      case failure: Throwable => primary = failure; throw failure
    finally MySqlFormat3BudgetCleanup.run(primary, Vector(
      () => budget.releaseTemporaryMemory(charge, "Full27 snapshot validation workspace release")))

  private def relation(relations: Vector[MySqlFormat3RelationSnapshot], name: String): MySqlFormat3RelationSnapshot =
    relations.find(_.table == name).getOrElse(fail("missing relation " + name))
  private def cell(row: Vector[MySqlFormat3Cell], table: String, name: String, inputs: MySqlScriptSchemaInputs): MySqlFormat3Cell =
    val index = inputs.forTable(table).columns.indexWhere(_.name == name)
    if index < 0 || index >= row.size then fail("missing column " + table + "." + name)
    row(index)
  private def binary(row: Vector[MySqlFormat3Cell], table: String, name: String, inputs: MySqlScriptSchemaInputs): MySqlFormat3Binary =
    cell(row, table, name, inputs) match
      case MySqlFormat3Cell.Binary(value) => value
      case _ => fail("binary required for " + table + "." + name)
  private def uuid(row: Vector[MySqlFormat3Cell], table: String, name: String, inputs: MySqlScriptSchemaInputs): MySqlFormat3Binary =
    val value = binary(row, table, name, inputs)
    if value.length != 16 || (0 until 16).forall(i => value.byteAt(i) == 0) then fail("nonzero UUID required for " + name)
    value
  private def integer(row: Vector[MySqlFormat3Cell], table: String, name: String, inputs: MySqlScriptSchemaInputs): Long =
    cell(row, table, name, inputs) match
      case MySqlFormat3Cell.SignedInteger(value) => value
      case _ => fail("integer required for " + table + "." + name)
  private def checkDigest(payload: MySqlFormat3Binary, expected: MySqlFormat3Binary): Unit =
    if expected.length != 32 then fail("snapshot digest length")
    val digest = MessageDigest.getInstance("SHA-256")
    var i = 0
    while i < payload.length do
      digest.update(payload.byteAt(i)); i += 1
    if !expected.equalsBytes(digest.digest()) then fail("snapshot NBT SHA-256 differs")
  private def fail(message: String): Nothing = throw new SQLDataException("Full27 " + message)
