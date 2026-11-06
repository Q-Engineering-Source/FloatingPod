package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import java.io.{IOException,StringReader}
import java.sql.{ResultSetMetaData,SQLDataException,SQLNonTransientConnectionException,Types}
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 真ledger/stream/行单元/比较计费；不制造JDBC成功、qualification或UNKNOWN owner。 */
class MySqlDbFirstAuditAllocationTest:
  private final class OwnedAccount extends AutoCloseable:
    val profile=SourceLimitProfile.of(1048576L,1048576L,1048576L,64L,4096L,8388608L,1048576L,1048576L)
    val root=new MySqlFormat3ReadBudget(profile,profile.maxSqlPacketBytes())
    private val thread=Thread.currentThread()
    private var closed=false
    root.reserveRetainedMemory(1024L,"pure owned allocation fixed holder BEFORE allocation")
    val child=root.newCanonicalDataScope()
    child.reserveRetainedMemory(1024L,"pure allocation partial containers BEFORE allocation")
    private def requireOwner():Unit =
      if (Thread.currentThread() ne thread) || closed || root.isClosed || child.isClosed ||
        !child.isCanonicalChildOf(root) || !child.sharesAllocationLedger(root) then
        throw new IllegalStateException("actual pure allocation owner/thread/root required")
    val allocation:MySqlSchemaAuditor.AuditAllocation=new MySqlSchemaAuditor.AuditAllocation(child):
      override protected def requireOwner():Unit = OwnedAccount.this.requireOwner()
      override protected def requireObservationOwner(value:UnverifiedMySqlSchemaObservation):Unit =
        throw new IllegalStateException("pure allocation test cannot admit an audit observation")
    override def close():Unit =
      if Thread.currentThread() ne thread then throw new IllegalStateException("original pure allocation thread required")
      if !closed then
        child.closeScopedReadBudget()
        root.releaseRetainedMemory(1024L,"pure allocation holder detached")
        root.closeRootReadBudget()
        closed=true
  private def caller=MySqlCallerScope(UUID.fromString("12345678-1234-1234-1234-123456789abc"),1L,
    "fp_audit_test","reader@localhost","NONE","reader@localhost",Some("9.7.2"),Vector("GRANT SELECT ON fp_audit_test.* TO reader@localhost"))

  @Test def callerAndRowCellAdmissionsConsumeOriginalLedgerAndRefusePressureBeforeInsertion():Unit =
    val owned=new OwnedAccount
    try
      val before=owned.root.accountedMemoryBytes
      owned.allocation.retainCallerScope("before",caller)
      assertTrue(owned.root.accountedMemoryBytes > before)
      val budget=new MySqlSchemaAuditor.ReadBudget(Some(owned.profile),owned.allocation)
      val pressure=owned.root.availableMemoryBytes-95L
      owned.root.reserveRetainedMemory(pressure,"actual row/cell pressure")
      val retained=owned.root.accountedMemoryBytes
      assertThrows(classOf[SQLDataException],()=>budget.addCell())
      assertEquals(retained,owned.root.accountedMemoryBytes)
      assertThrows(classOf[SQLDataException],()=>budget.addRow(1))
      assertEquals(retained,owned.root.accountedMemoryBytes)
      assertThrows(classOf[SQLDataException],()=>owned.allocation.retainCallerScope("after",caller))
      assertEquals(retained,owned.root.accountedMemoryBytes)
      owned.root.releaseRetainedMemory(pressure,"pressure detached")
      budget.addRow(1);budget.addCell()
      assertTrue(owned.root.accountedMemoryBytes > before)
    finally owned.close()
    assertEquals(0L,owned.root.accountedMemoryBytes)

  @Test def realBoundedTextDecoderRetainsValueButReleasesFailedScratchAndRejectsOtherWorker():Unit =
    val owned=new OwnedAccount
    val before=owned.root.accountedMemoryBytes
    val value=MySqlSchemaAuditor.readBoundedTextOwned(new StringReader("完整audit caller text"),owned.allocation,"pure text cell")
    assertEquals("完整audit caller text",value)
    val retained=owned.root.accountedMemoryBytes
    assertTrue(retained > before)
    val observed=new AtomicReference[Throwable]()
    val worker=new Thread(()=>
      try owned.allocation.retain(1L,"foreign worker")
      catch case cause:Throwable=>observed.set(cause))
    worker.start();worker.join(5000L)
    assertFalse(worker.isAlive);assertInstanceOf(classOf[IllegalStateException],observed.get())
    assertEquals(retained,owned.root.accountedMemoryBytes)
    val cause=new IOException("actual decode failure")
    val broken=new java.io.Reader:
      private var returned=false
      override def read(buffer:Array[Char],offset:Int,length:Int):Int =
        if !returned then
          buffer(offset)='x'
          returned=true
          1
        else throw cause
      override def close():Unit = ()
    val actual=assertThrows(classOf[SQLNonTransientConnectionException],()=>
      MySqlSchemaAuditor.readBoundedTextOwned(broken,owned.allocation,"failed pure text cell"))
    assertSame(cause,actual.getCause)
    assertEquals(retained,owned.root.accountedMemoryBytes,"failed local decoder scratch is released; returned earlier value remains owned")
    owned.allocation.retainFailure(actual)
    assertSame(actual,owned.allocation.failureObservation.get)
    owned.close()
    assertEquals(0L,owned.root.accountedMemoryBytes)
    assertThrows(classOf[IllegalStateException],()=>owned.allocation.retain(1L,"retired account"))

  @Test def canonicalComparisonWorkspaceIsPrechargedAndRefundedOnSuccessAndOriginalFailure():Unit =
    val owned=new OwnedAccount
    try
      // 实际canonical hasher的封闭10-set输入；不表示对象审计/数据库资格已通过。
      val columns=Vector(MySqlAuditColumn("value",Types.VARCHAR,"VARCHAR",32,0,ResultSetMetaData.columnNoNulls,false))
      val sets=MySqlAuditCoverage.StaticResultSetIds.map(id=>MySqlAuditResultSet(id,columns,
        Vector(MySqlAuditRow(Vector(MySqlAuditText("actual canonical framing"))))))
      val fee=MySqlSchemaAuditor.canonicalHashScratch(sets)
      val before=owned.root.accountedMemoryBytes
      val pressure=owned.root.availableMemoryBytes-fee+1L
      owned.root.reserveRetainedMemory(pressure,"actual comparison pressure")
      var entered=false
      assertThrows(classOf[SQLDataException],()=>owned.allocation.withTemporary(fee,"canonical comparison") {
        entered=true
        MySqlSchemaAuditor.canonicalSha256(MySqlSchemaAuditProfile.DbFirst1,sets)
      })
      assertFalse(entered)
      owned.root.releaseRetainedMemory(pressure,"comparison pressure detached")
      val hash=owned.allocation.withTemporary(fee,"canonical comparison") {
        assertEquals(before+fee,owned.root.accountedMemoryBytes)
        MySqlSchemaAuditor.canonicalSha256(MySqlSchemaAuditProfile.DbFirst1,sets)
      }
      assertEquals(64,hash.length);assertEquals(before,owned.root.accountedMemoryBytes)
      val cause=new IOException("actual comparison body failure")
      val actual=assertThrows(classOf[IOException],()=>owned.allocation.withTemporary(fee,"failed comparison") { throw cause })
      assertSame(cause,actual);assertEquals(before,owned.root.accountedMemoryBytes)
      val five=MySqlSchemaAuditor.staticValidationScratch(sets,0,sets.size,MySqlSchemaAuditProfile.DeploymentReceipt1)
      val six=MySqlSchemaAuditor.staticValidationScratch(sets,0,sets.size,MySqlSchemaAuditProfile.DbFirst1)
      assertEquals(96L*(27L+480L+81L-26L-467L-78L),six-five)
    finally owned.close()
    assertEquals(0L,owned.root.accountedMemoryBytes)

  @Test def successfulGrowingTextAndBinaryReadersRetainOnlyReturnedValues():Unit =
    val owned=new OwnedAccount
    try
      val before=owned.root.accountedMemoryBytes
      val text="audit-完整-" * 2500
      assertEquals(text,MySqlSchemaAuditor.readBoundedTextOwned(new StringReader(text),owned.allocation,"growing text"))
      val bytes=Array.tabulate[Byte](50000)(i=>(i%251).toByte)
      assertArrayEquals(bytes,MySqlSchemaAuditor.readBoundedBytesOwned(new java.io.ByteArrayInputStream(bytes),owned.allocation,"growing binary"))
      assertTrue(owned.root.accountedMemoryBytes-before < 2L*1024L*1024L,
        "short completed values must not retain geometrically grown scratch buffers")
      assertTrue(owned.root.successfulReserveHighWaterBytes > owned.root.accountedMemoryBytes)
    finally owned.close()

  @Test def failedGrowingBinaryReaderReleasesScratchAndPreservesTheReadCause():Unit =
    val owned=new OwnedAccount
    try
      val before=owned.root.accountedMemoryBytes
      val cause=new IOException("actual binary read failure after growth")
      val stream=new java.io.ByteArrayInputStream(new Array[Byte](32768)):
        override def read(buffer:Array[Byte],offset:Int,length:Int):Int =
          if available()==0 then throw cause else super.read(buffer,offset,length)
      val failure=assertThrows(classOf[SQLNonTransientConnectionException],()=>
        MySqlSchemaAuditor.readBoundedBytesOwned(stream,owned.allocation,"failed growing binary"))
      assertSame(cause,failure.getCause)
      assertEquals(before,owned.root.accountedMemoryBytes)
      val noProgress=new java.io.InputStream:
        override def read():Int = 0
        override def read(buffer:Array[Byte],offset:Int,length:Int):Int = 0
      assertThrows(classOf[SQLNonTransientConnectionException],()=>
        MySqlSchemaAuditor.readBoundedBytesOwned(noProgress,owned.allocation,"no progress binary"))
      assertEquals(before,owned.root.accountedMemoryBytes)
    finally owned.close()
