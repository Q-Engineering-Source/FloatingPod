package com.wi.floatingpod.storage.mysql

import com.mysql.cj.MysqlType
import com.mysql.cj.conf.DefaultPropertySet
import com.mysql.cj.result.DefaultColumnDefinition
import com.mysql.cj.protocol.a.result.ByteArrayRow
import com.mysql.cj.result.{BinaryStreamValueFactory, Field, StringValueFactory}
import java.io.InputStreamReader
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.charset.StandardCharsets
import java.sql.{ResultSet, SQLNonTransientConnectionException}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 离线 JDBC 适配器：ResultSetImpl 的构造要求真实连接；这里只把接口绑定到实际驱动 decoder。 */
class MySqlOwnedNumericColumnTest:
  private def result(value: String, kind: MysqlType, nullFlag: Option[Boolean] = None): ResultSet =
    val properties = new DefaultPropertySet()
    properties.initializeProperties(new java.util.Properties())
    val row = new ByteArrayRow(Array(if value == null then null else value.getBytes(StandardCharsets.UTF_8)), null)
    row.setMetadata(new DefaultColumnDefinition(Array(new Field("catalog", "n", 63, "UTF-8", kind, 65))))
    val strings = new StringValueFactory(properties)
    val streams = new BinaryStreamValueFactory(properties)
    Proxy.newProxyInstance(classOf[ResultSet].getClassLoader, Array(classOf[ResultSet]), new InvocationHandler:
      override def invoke(proxy: Object, method: Method, args: Array[Object]): Object = method.getName match
        case "getString" => row.getValue(0, strings)
        case "getCharacterStream" =>
          val stream = row.getValue(0, streams)
          if stream == null then null else new InputStreamReader(stream, StandardCharsets.UTF_8)
        case "wasNull" => java.lang.Boolean.valueOf(nullFlag.getOrElse(row.wasNull()))
        case other => throw new UnsupportedOperationException("unexpected JDBC test call: " + other)
    ).asInstanceOf[ResultSet]

  private def read(value: String, kind: MysqlType, nullFlag: Option[Boolean] = None): MySqlAuditCell =
    val profile = DeploymentReceiptStaticTestData.defaultProfile().sourceLimitProfile
    val root = MySqlFormat3ReadBudget.rootOnLedger(profile, profile.maxSqlPacketBytes(),
      new MySqlFormat3AllocationLedger(profile.maxRetainedBytes()))
    try
      val allocation = new MySqlSchemaAuditor.AuditAllocation(root):
        override protected def requireOwner(): Unit = assertFalse(root.isClosed)
        override protected def requireObservationOwner(value: UnverifiedMySqlSchemaObservation): Unit =
          throw new UnsupportedOperationException("numeric decode does not issue observations")
      val budget = new MySqlSchemaAuditor.ReadBudget(Some(profile), allocation)
      MySqlSchemaAuditor.readOwnedNumericColumn(result(value, kind, nullFlag), 1, "offline numeric", allocation, budget)
    finally root.closeRootReadBudget()

  @Test def actualConnectorNumericDecoderSupportsStringButRejectsCharacterStreams(): Unit =
    assertThrows(classOf[com.mysql.cj.exceptions.DataConversionException], () => result("1", MysqlType.INT).getCharacterStream(1))
    assertEquals(MySqlAuditText("1"), read("1", MysqlType.INT))

  @Test def retainsUnsignedBigintAndMaximumDecimalPrecisionWithoutFloatingPointConversion(): Unit =
    val integer = "18446744073709551615"
    val decimal = "-" + "9".repeat(35) + "." + "1".repeat(30)
    assertEquals(MySqlAuditText(integer), read(integer, MysqlType.BIGINT_UNSIGNED))
    assertEquals(MySqlAuditText(decimal), read(decimal, MysqlType.DECIMAL))

  @Test def sqlNullMustBeConfirmedAndInconsistentNullFlagsFail(): Unit =
    assertEquals(MySqlAuditNull, read(null, MysqlType.INT))
    assertThrows(classOf[SQLNonTransientConnectionException], () => read(null, MysqlType.INT, Some(false)))
    assertThrows(classOf[SQLNonTransientConnectionException], () => read("1", MysqlType.INT, Some(true)))

  @Test def numericRepresentationsBeyondThePinnedServerBoundFailExplicitly(): Unit =
    assertThrows(classOf[SQLNonTransientConnectionException], () => read("9".repeat(129), MysqlType.DECIMAL))
