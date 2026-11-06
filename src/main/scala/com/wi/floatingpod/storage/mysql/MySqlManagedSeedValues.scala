package com.wi.floatingpod.storage.mysql

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.sql.{PreparedStatement, SQLDataException, SQLException}
import java.util.{HexFormat, UUID}

/** 两条 bootstrap seed 的封闭类型绑定；没有假 configuration hash 或初始业务状态。 */
private[mysql] object MySqlManagedSeedValues:
  /** 仅标记 executeUpdate 的 SQL 应答不确定性；只有原 statement 关闭确认后才允许核对实际效果。 */
  final class UncertainEffect private[MySqlManagedSeedValues](cause: SQLException)
      extends SQLException("Managed seed execution acknowledgement is uncertain", cause.getSQLState, cause.getErrorCode, cause):
    private var statementClosed = false
    def cleanupConfirmed: Boolean = statementClosed
    private[MySqlManagedSeedValues] def confirmStatementClosed(): Unit = statementClosed = true

  sealed trait Value:
    private[MySqlManagedSeedValues] def bind(statement: PreparedStatement, index: Int): Unit
  final case class Binary private[MySqlManagedSeedValues](private val value: Array[Byte]) extends Value:
    private[MySqlManagedSeedValues] def bind(statement: PreparedStatement, index: Int): Unit = statement.setBytes(index, value)
  final case class Text private[MySqlManagedSeedValues](private val value: String) extends Value:
    private[MySqlManagedSeedValues] def bind(statement: PreparedStatement, index: Int): Unit = statement.setString(index, value)
  final case class Number private[MySqlManagedSeedValues](private val value: Long) extends Value:
    private[MySqlManagedSeedValues] def bind(statement: PreparedStatement, index: Int): Unit = statement.setLong(index, value)

  final class Data private[MySqlManagedSeedValues](
      private val plan: MySqlManagedSchemaPlan.Plan, private val values: Map[String, Value]):
    /** 只对原 plan 中的 seed 语句做 placeholder 替换；SQL 不来自外部请求。 */
    private[mysql] def statement(step: MySqlManagedSchemaPlan.Step): (String, Vector[Value]) =
      if step == null || step.ordinal >= plan.steps.size || (plan.steps(step.ordinal) ne step) ||
          step.kind != MySqlManagedSchemaPlan.Kind.SeedInsert then
        fail("exact original managed seed plan step required")
      val pattern = "@fp_[a-z0-9_]+".r
      val variables = pattern.findAllIn(step.sql).toVector
      if variables.isEmpty || variables.exists(name => !values.contains(name)) then fail("managed seed contains an unknown typed variable")
      (pattern.replaceAllIn(step.sql, "?"), variables.map(values))

    def insert(owner: ManagedMySqlConnectionOwner, journal: ManagedInstallationJournal,
        journalIndex: Int, step: MySqlManagedSchemaPlan.Step): Unit =
      if owner == null || journal == null || journal.identity().recipeSha256() != plan.ddlSha256 then
        fail("exact managed initializer owner and persisted plan required before seed")
      val pending = journal.snapshot().pendingIntentIndex()
      if !pending.isPresent || pending.getAsInt != journalIndex then fail("managed seed requires its persisted original intent")
      owner.requireHeldInitializationLock()
      val (sql, bindings) = statement(step)
      val query = owner.connection().prepareStatement(sql)
      var primary: Throwable = null
      try
        query.setQueryTimeout(owner.queryTimeoutSeconds())
        bindings.zipWithIndex.foreach((value, index) => value.bind(query, index + 1))
        val changed = try query.executeUpdate()
          catch case cause: SQLException => throw new UncertainEffect(cause)
        if changed != 1 then fail("managed seed did not insert exactly one control row")
        owner.requireHeldInitializationLock()
      catch
        case cause: Throwable => primary = cause; throw cause
      finally
        try
          query.close()
          primary match
            case uncertain: UncertainEffect => uncertain.confirmStatementClosed()
            case _ => ()
        catch
          case cleanup: Throwable =>
            if primary == null then throw cleanup
            else if cleanup ne primary then primary.addSuppressed(cleanup)

  def create(record: ManagedInstallationIdentity.Record, rendered: MySqlDbFirstSchemaProfile.Rendered,
      plan: MySqlManagedSchemaPlan.Plan, recipeBytes: Array[Byte], configurationSha256: Array[Byte]): Data =
    if record == null || rendered == null || plan == null || recipeBytes == null ||
        configurationSha256 == null || configurationSha256.length != 32 || configurationSha256.forall(_ == 0.toByte) ||
        rendered.sha256 != plan.ddlSha256 || record.recipeSha256() != plan.ddlSha256 || record.schema() != plan.schema then
      fail("managed seed requires actual persisted identity, final recipe and non-placeholder configuration digest")
    val recipe = MySqlDeploymentReceiptRuntimeContract.decodeManagedDbFirstRecipe(recipeBytes)
    recipe.requireManagedIdentity(record.serverUuid(), record.schema(), record.provisioningUuid(), record.bindingUuid(),
      rendered.sha256, MySqlDeploymentReceiptRuntimeContract.DbFirstFamily)
    val profile = rendered.inputs.sourceLimitProfile
    val values: Map[String, Value] = Map(
      "@fp_ddl_sha256" -> Binary(HexFormat.of().parseHex(rendered.sha256)),
      "@fp_provisioning_uuid" -> Binary(uuid(record.provisioningUuid())),
      "@fp_expected_server_uuid" -> Binary(uuid(record.serverUuid())),
      "@fp_expected_binding_uuid" -> Binary(uuid(record.bindingUuid())),
      "@fp_configuration_sha256" -> Binary(configurationSha256.clone()),
      "@fp_source_provenance_kind" -> Text("MANAGED_INSTALL"),
      "@fp_source_provenance_bytes" -> Binary(recipeBytes.clone()),
      "@fp_source_provenance_sha256" -> Binary(MessageDigest.getInstance("SHA-256").digest(recipeBytes)),
      "@fp_limit_profile_bytes" -> Binary(profile.canonicalBytes()),
      "@fp_limit_profile_sha256" -> Binary(profile.sha256()),
      "@fp_max_region_bytes" -> Number(profile.maxRegionBytes()),
      "@fp_max_chunk_bytes" -> Number(profile.maxChunkBytes()),
      "@fp_max_decompressed_bytes" -> Number(profile.maxDecompressedBytes()),
      "@fp_max_nbt_depth" -> Number(profile.maxNbtDepth()),
      "@fp_max_members" -> Number(profile.maxMembers()),
      "@fp_max_retained_bytes" -> Number(profile.maxRetainedBytes()),
      "@fp_max_sql_packet_bytes" -> Number(profile.maxSqlPacketBytes()),
      "@fp_max_native_artifact_bytes" -> Number(profile.maxNativeArtifactBytes()))
    if plan.requiredSeedVariables.toSet != values.keySet then fail("managed seed variable closure differs from the exact release plan")
    new Data(plan, values)

  private def uuid(value: UUID): Array[Byte] = ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()
  private def fail(message: String): Nothing = throw new SQLDataException(message)
