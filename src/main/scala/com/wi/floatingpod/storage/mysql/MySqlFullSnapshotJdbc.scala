package com.wi.floatingpod.storage.mysql

import java.sql.{Connection, ResultSet}
import java.util.UUID

/** 同一 caller-owned connection/事务上的完整九表读回；不打开、提交或关闭连接。 */
private[mysql] object MySqlFullSnapshotJdbc:
  private val PodColumns =
    "uuid,owner_uuid,state_version,world_name,pos_x,pos_y,pos_z,yaw,pitch,is_active,last_updated,dimension_id,operating_mode"
  private val IntentColumns =
    "pod_uuid,attempt_uuid,binding_uuid,origin,phase,source_kind,source_identity,source_identity_sha256,catalog_generation,create_operation_uuid,prepared_domain_version,target_world_name,target_pos_x,target_pos_y,target_pos_z,target_yaw,target_pitch,target_dimension_id,target_chunk_x,target_chunk_z,committed_domain_version,committed_at,committed_world_name,committed_pos_x,committed_pos_y,committed_pos_z,committed_yaw,committed_pitch,committed_dimension_id"
  private val BootstrapColumns =
    "singleton_fixed1,bootstrap_operation_uuid,prepare_write_uuid,certify_write_uuid,binding_uuid,binding_format_version,provisioning_uuid,source_world_identity,provision_receipt_sha256,stage,marker_ack_sha256"

  def read(connection: Connection, schema: String, lockRows: Boolean): MySqlFullRepositorySnapshot =
    require(connection != null, "caller-owned MySQL connection is required")
    require(schema != null && MySqlSchemaAuditor.SchemaIdentifier.matches(schema), "fixed schema identifier is unsafe")
    if lockRows && connection.getAutoCommit then
      throw new IllegalStateException("full snapshot FOR UPDATE requires the caller's active transaction")
    val owner = Thread.currentThread()
    val suffix = if lockRows then " FOR UPDATE" else ""
    val table = (name: String) => s"`$schema`.`$name`"
    def queryWithSuffix[A](sql: String, rowSuffix: String)(decode: ResultSet => A): Vector[A] =
      if Thread.currentThread() ne owner then throw new IllegalStateException("full snapshot read is confined to its caller-owned thread")
      MySqlBusinessRows.using(connection.prepareStatement(sql + rowSuffix)) { statement =>
        MySqlBusinessRows.using(statement.executeQuery()) { result =>
          val rows = Vector.newBuilder[A]
          while result.next() do rows += decode(result)
          rows.result()
        }
      }
    def query[A](sql: String)(decode: ResultSet => A): Vector[A] =
      queryWithSuffix(sql, suffix)(decode)

    val pods = query(s"SELECT $PodColumns FROM ${table("floating_pod_state")} ORDER BY uuid")(MySqlBusinessRows.readPod)
    val entitlementLockSuffix = if lockRows then " FOR SHARE" else ""
    val entitlements = queryWithSuffix(
      s"SELECT player_uuid,registration_limit,simultaneous_active_limit FROM ${table("player_entitlement")} ORDER BY player_uuid",
      entitlementLockSuffix
    )(MySqlBusinessRows.readEntitlement)
    val bindingRows = query(s"SELECT singleton_fixed1,binding_uuid,cutover_state,format_version FROM ${table("physical_recovery_binding")} ORDER BY singleton_fixed1")(MySqlRecoveryRows.readBinding)
    if bindingRows.size > 1 then throw new IllegalStateException("physical recovery binding is not a singleton")
    val locatorRows = query(s"SELECT pod_uuid,attempt_uuid,locator_state,reason_code,domain_version,target_dimension_id FROM ${table("physical_recovery_locator")} ORDER BY pod_uuid")(MySqlRecoveryRows.readLocator)
    val chunkRows = query(s"SELECT pod_uuid,dimension_id,chunk_x,chunk_z FROM ${table("physical_recovery_locator_chunk")} ORDER BY pod_uuid,dimension_id,chunk_x,chunk_z")(MySqlRecoveryRows.readChunk)
    val intentRows = query(s"SELECT $IntentColumns FROM ${table("physical_recovery_intent")} ORDER BY pod_uuid")(MySqlRecoveryRows.readIntent)
    val bootstrapRows = query(s"SELECT $BootstrapColumns FROM ${table("binding_bootstrap_intent")} ORDER BY singleton_fixed1")(MySqlBootstrapIntentRows.decode)
    val graph = MySqlRecoveryRows.assembleGraph(bindingRows.headOption, locatorRows, chunkRows, intentRows)
    val snapshot = MySqlFullRepositorySnapshot(pods, entitlements, graph, bootstrapRows)
    snapshot.validate()
    snapshot
