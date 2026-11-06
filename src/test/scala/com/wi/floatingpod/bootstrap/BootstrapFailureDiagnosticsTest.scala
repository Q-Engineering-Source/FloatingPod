package com.wi.floatingpod.bootstrap

import com.wi.floatingpod.storage.mysql.*
import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.sql.Types
import java.util.IdentityHashMap
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.parallel.{ResourceLock, Resources}

class BootstrapFailureDiagnosticsTest:
  @Test @ResourceLock(Resources.SYSTEM_ERR)
  def typedStaticMismatchLogsOnlyLocationTypeAndDigest(): Unit =
    val secret = "private-static-body-must-not-enter-logs"
    val column = MySqlAuditColumn("controlled",Types.VARCHAR,"VARCHAR",64,0,1,false)
    val expected = Vector(MySqlAuditResultSet("RS02_TABLES",Vector(column),Vector(MySqlAuditRow(Vector(MySqlAuditText(secret))))))
    val actual = expected.updated(0,expected.head.copy(rows=Vector(MySqlAuditRow(Vector(MySqlAuditText(secret+" changed"))))))
    val failure = MySqlSchemaAuditor.staticManifestMismatch(MySqlSchemaAuditProfile.DbFirst1,expected,actual)
    val output = new ByteArrayOutputStream()
    val stream = new PrintStream(output,true,StandardCharsets.UTF_8)
    val original = System.err
    try
      System.setErr(stream)
      ProductionDedicatedBootstrapCallbacks.safeFailure(failure,new IdentityHashMap[Throwable,java.lang.Boolean](),"test")
      ProductionDedicatedBootstrapCallbacks.safeFailure(new java.sql.SQLException(secret),new IdentityHashMap[Throwable,java.lang.Boolean](),"untrusted")
    finally
      System.setErr(original)
      stream.close()
    val text = output.toString(StandardCharsets.UTF_8)
    assertTrue(text.contains("STATIC_MANIFEST_DIFFERENCE resultSet=RS02_TABLES row=1 column=1 field=cell"))
    assertTrue(text.contains("sha256="))
    assertFalse(text.contains(secret))
    assertTrue(text.contains("class=java.sql.SQLException"))
