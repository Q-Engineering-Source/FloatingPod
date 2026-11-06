package com.wi.floatingpod.storage.mysql

import com.wi.sqljdbcdriverlib.mysql.{MySqlCredentialSource, MySqlEndpoint, MySqlOpenRequest, MySqlSecretSource, MySqlTimeouts, MySqlTrust}

import java.nio.file.Path
import java.time.Duration
import java.util.UUID

/** 表示部署合同要求的 MySQL 持久性配置。 */
final case class MySqlDurabilityExpectation(
    innodbFlushLogAtTrxCommit: Int,
    syncBinlog: Long
):
  require(innodbFlushLogAtTrxCommit == 1, "innodb_flush_log_at_trx_commit must be 1")
  require(syncBinlog == 1L, "sync_binlog must be 1")

/**
 * 本地 MySQL 连接参数。凭据与信任库密码由调用方提供，既不复制进设置对象，也不进入诊断文本。
 */
final class MySqlConnectionSettings private (
    private[mysql] val endpoint: MySqlEndpoint,
    val expectedServerUuid: UUID,
    val expectedEffectiveUser: String,
    private[mysql] val credentialSource: MySqlCredentialSource,
    private[mysql] val trustStorePath: Path,
    private[mysql] val trustStorePasswordSource: MySqlSecretSource,
    val connectTimeout: Duration,
    val socketTimeout: Duration,
    val durabilityExpectation: MySqlDurabilityExpectation,
    private[mysql] val timeouts: MySqlTimeouts,
    private[mysql] val utf8mb4ResultsRequired: Boolean
):
  /** 受管安装的 B 使用 utf8mb4 结果编码；只改变服务器输出，不改变驱动的客户端编码。 */
  private[mysql] def withUtf8mb4Results: MySqlConnectionSettings =
    new MySqlConnectionSettings(endpoint, expectedServerUuid, expectedEffectiveUser, credentialSource,
      trustStorePath, trustStorePasswordSource, connectTimeout, socketTimeout, durabilityExpectation, timeouts, true)

  def expectedSchema: String = endpoint.database()

  private[mysql] def openRequest: MySqlOpenRequest =
    MySqlOpenRequest
      .builder(endpoint, credentialSource)
      .timeouts(timeouts)
      .trust(MySqlTrust.store(trustStorePath, MySqlTrust.StoreType.PKCS12, trustStorePasswordSource))
      .build()

  override def toString: String = "MySqlConnectionSettings[redacted]"

object MySqlConnectionSettings:
  def apply(
      host: String,
      port: Int,
      schema: String,
      expectedServerUuid: UUID,
      expectedEffectiveUser: String,
      credentialSource: MySqlCredentialSource,
      pkcs12TrustStore: Path,
      trustStorePasswordSource: MySqlSecretSource,
      connectTimeout: Duration,
      socketTimeout: Duration,
      durabilityExpectation: MySqlDurabilityExpectation
  ): MySqlConnectionSettings =
    require(expectedServerUuid != null, "expected MySQL server identity is required")
    require(expectedEffectiveUser != null && expectedEffectiveUser.nonEmpty, "expected MySQL effective account is required")
    require(credentialSource != null, "MySQL credential source is required")
    require(pkcs12TrustStore != null && pkcs12TrustStore.isAbsolute, "an absolute PKCS12 trust-store path is required")
    require(trustStorePasswordSource != null, "PKCS12 trust-store secret source is required")
    require(durabilityExpectation != null, "explicit MySQL durability expectations are required")

    val endpoint = new MySqlEndpoint(host, port, schema)
    val timeouts = new MySqlTimeouts(connectTimeout, socketTimeout)
    new MySqlConnectionSettings(
      endpoint,
      expectedServerUuid,
      expectedEffectiveUser,
      credentialSource,
      pkcs12TrustStore,
      trustStorePasswordSource,
      connectTimeout,
      socketTimeout,
      durabilityExpectation,
      timeouts,
      false
    )
