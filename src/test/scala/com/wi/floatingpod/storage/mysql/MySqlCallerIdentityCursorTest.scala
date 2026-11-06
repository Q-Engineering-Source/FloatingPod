package com.wi.floatingpod.storage.mysql

import java.sql.{SQLException, SQLNonTransientConnectionException, Types}
import java.util.UUID
import javax.sql.rowset.{CachedRowSet, RowSetMetaDataImpl, RowSetProvider}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 使用实际 JDK cursor；未连接数据库，不伪签 scope、Held 或 qualification。 */
class MySqlCallerIdentityCursorTest:
  private val id = UUID.fromString("01000000-0000-4000-8000-000000000001")
  private val labels = Vector("server_uuid", "connection_id", "caller_default_schema", "caller_effective_principal",
    "caller_effective_roles", "connected_user", "caller_server_version")
  private def rows(count: Int, nullVersion: Boolean = false): CachedRowSet =
    val metadata = new RowSetMetaDataImpl()
    metadata.setColumnCount(7)
    labels.zipWithIndex.foreach { (label, index) =>
      metadata.setColumnName(index + 1, label); metadata.setColumnLabel(index + 1, label)
      metadata.setColumnType(index + 1, if index == 1 then Types.BIGINT else Types.VARCHAR)
    }
    val rs = RowSetProvider.newFactory().createCachedRowSet()
    rs.setMetaData(metadata)
    for _ <- 0 until count do
      rs.moveToInsertRow()
      rs.updateString(1, id.toString); rs.updateLong(2, 17L); rs.updateString(3, "fp_fixture")
      rs.updateString(4, "fixture@localhost"); rs.updateString(5, "NONE"); rs.updateString(6, "fixture@localhost")
      if nullVersion then rs.updateNull(7) else rs.updateString(7, "9.7.2")
      rs.insertRow(); rs.moveToCurrentRow()
    rs.beforeFirst(); rs
  private def use[A](rs: CachedRowSet)(body: CachedRowSet => A): A =
    var primary: Throwable = null
    try body(rs)
    catch
      case failure: Throwable => primary = failure; throw failure
    finally
      try rs.close()
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup else MySqlSchemaAuditor.addSuppressed(primary, cleanup)
  @Test def actualProductionDecoderReadsAllSevenBeforeUniquenessAdvance(): Unit =
    use(rows(1)) { rs =>
      val decoded = MySqlSchemaAuditor.decodeCallerIdentity(rs, MySqlSchemaAuditProfile.Format3)
      assertEquals(id, decoded.serverUuid); assertEquals(17L, decoded.connectionId)
      assertEquals("fp_fixture", decoded.defaultSchema); assertEquals("fixture@localhost", decoded.effectivePrincipal)
      assertEquals("NONE", decoded.effectiveRoles); assertEquals("fixture@localhost", decoded.connectedUser)
      assertEquals(Some("9.7.2"), decoded.serverVersion); assertEquals(Vector.empty, decoded.ownGrants)
      assertTrue(rs.isAfterLast)
    }
  @Test def emptyDuplicateAndSeventhNullAreExplicitlyRejected(): Unit =
    for (count, nullVersion) <- Vector((0, false), (2, false), (1, true)) do
      use(rows(count, nullVersion)) { cursor =>
        assertThrows(classOf[SQLNonTransientConnectionException], () => MySqlSchemaAuditor.decodeCallerIdentity(cursor, MySqlSchemaAuditProfile.Format3))
      }
  @Test def sixArgumentLegacyJvmEntryPointsDoNotForgeFormat3Identity(): Unit =
    val constructed = new MySqlCallerScope(id, 17L, "fp_fixture", "fixture@localhost", "NONE", "fixture@localhost")
    val applied = MySqlCallerScope(id, 17L, "fp_fixture", "fixture@localhost", "NONE", "fixture@localhost")
    assertEquals(constructed, applied); assertEquals(None, applied.serverVersion); assertEquals(Vector.empty, applied.ownGrants)
    val full = new MySqlCallerScope(id, 17L, "fp_fixture", "fixture@localhost", "NONE", "fixture@localhost", Some("9.7.2"), Vector("full grant"))
    val copied = full.copy(id, 18L, "fp_fixture", "fixture@localhost", "NONE", "fixture@localhost")
    assertEquals(18L, copied.connectionId); assertEquals(full.serverVersion, copied.serverVersion); assertEquals(full.ownGrants, copied.ownGrants)
    val named = full.copy(ownGrants = Vector("changed full grant"))
    assertEquals(Some("9.7.2"), named.serverVersion); assertEquals(Vector("changed full grant"), named.ownGrants)
