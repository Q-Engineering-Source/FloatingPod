package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.bootstrap.{ManagedBootstrapPolicy, ManagedDatabaseConfiguration}
import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.{HexFormat, UUID}

/** 普通配置模式的发布策略。目标、来源地址与版本进入安装身份；没有数据库自批准。 */
private[mysql] object MySqlManagedReleasePolicy:
  private val MiB = 1024L * 1024L

  def profile(schema: String, server: UUID, clientHost: String): MySqlDeploymentReceiptSchemaProfile.Inputs =
    require(schema != null && schema.matches("[A-Za-z0-9_]{1,64}") && server != null &&
      server != new UUID(0L, 0L) && clientHost != null && clientHost.matches("[A-Za-z0-9_.:-]{1,255}"),
      "managed policy requires an exact existing target and connected client Host")
    val namespace = hash((server.toString + "\n" + schema + "\n").getBytes(StandardCharsets.US_ASCII)).take(24)
    import MySqlDeploymentReceiptSchemaProfile.*
    def account(role: String): Account = Account("fp_m_" + namespace + "_" + role, clientHost)
    inputs(schema, Accounts(account("g"), account("a"), account("r"), account("p"), account("i"), account("q")),
      ManagedBootstrapPolicy.sourceLimits(),
      MySqlScriptSchemaProfile.Budget(256 * 1024, 256 * 1024, MiB, 64 * 1024,
        8 * MiB, 64 * 1024, 65536, 32 * MiB, 16 * MiB),
      MySqlDeploymentReceiptSchemaDefinition.Budget(MiB, 256 * 1024), 65536,
      // 明示信任 MySQL 本机 root 的标准管理代理行；不根据现场行自动扩充白名单。
      Vector(TrustedProxyRow("root", "localhost", "", "", withGrant = true)))

  /** 排除秘密和installer密码；配置内容漂移不能静默选用另一份已持久身份。 */
  def configurationSha256(configuration: ManagedDatabaseConfiguration, server: UUID,
      profile: MySqlDeploymentReceiptSchemaProfile.Inputs): String =
    require(configuration != null && server != null && profile != null && configuration.database() == profile.schema,
      "exact managed configuration/target/profile required")
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    def field(value: String): Unit =
      val encoded = value.getBytes(StandardCharsets.UTF_8)
      out.writeInt(encoded.length)
      out.write(encoded)
    field(ManagedBootstrapPolicy.VERSION)
    field(configuration.host())
    out.writeInt(configuration.port())
    field(configuration.database())
    field(server.toString)
    field(profile.profileIdentitySha256)
    field(configuration.trustStore().orElseThrow().toString)
    field(configuration.trustStorePasswordFile().orElseThrow().toString)
    out.writeLong(configuration.connectTimeoutMillis())
    out.writeLong(configuration.socketTimeoutMillis())
    out.writeLong(configuration.initializationTimeoutMillis())
    out.writeLong(configuration.lockTimeoutMillis())
    out.flush()
    hash(bytes.toByteArray)

  private def hash(bytes: Array[Byte]): String =
    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
