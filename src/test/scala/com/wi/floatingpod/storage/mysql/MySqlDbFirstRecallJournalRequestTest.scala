package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.server.io.IoOperationTag
import java.nio.ByteBuffer
import java.sql.SQLDataException
import java.util.{HexFormat,UUID}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 直接验证生产codec和真实账本；golden由独立BE字段/标准SHA-256生成，不调用被测writer。 */
class MySqlDbFirstRecallJournalRequestTest:
  private def id(n:Long)=new UUID(0L,n)
  private def profile(members:Long=4096L)=SourceLimitProfile.of(
    1048576L,1048576L,1048576L,64L,members,8388608L,1048576L,1048576L)
  private def root()=new MySqlFormat3ReadBudget(profile(),1048576L)
  private val journal=IoOperationTag(id(1),11L,13L,id(2))
  private val original=IoOperationTag(id(1),11L,13L,id(3))
  private def fresh(owner:MySqlFormat3ReadBudget,client:Option[UUID]=Some(id(7)))=
    MySqlDbFirstRecallJournalRequest.fresh(journal,original,id(4),id(5),id(6),17L,client,owner)
  private def golden=HexFormat.of().parseHex(
    "0000002846505f44425f46495253545f524543414c4c5f4a4f55524e414c5f524551554553545f42455f563101010000"+
    "0000000000000000000000000001000000000000000b000000000000000d000000000000000000000000000000020000"+
    "0000000000000000000000000001000000000000000b000000000000000d000000000000000000000000000000030000"+
    "000000000000000000000000000400000000000000000000000000000005000000000000000000000000000000060000"+
    "0000000000110100000000000000000000000000000007000000610000001d46505f534f555243455f4c494d49545f50"+
    "524f46494c455f42455f5631000000000010000000000000001000000000000000100000000000000000004000000000"+
    "00001000000000000080000000000000001000000000000000100000000000200878b2baa4d9160e0211eaa8bca9c554"+
    "b5ecbcd40e603c8526b1e357da4e2093")

  @Test def fixedWireAndDecodedFieldsKeepJournalSeparateFromOriginalRequest():Unit =
    val owner=root()
    try
      val value=fresh(owner)
      assertEquals(352,value.canonical.length)
      assertArrayEquals(golden,value.canonical.copyBytes(owner,"golden journal request"))
      assertEquals("7E3CCEC78B7FB0EDFBA6DF39E416EF467F3FE646B76F3EB43544D45AA23EF083",value.canonical.hexSha256)
      val decoded=MySqlDbFirstRecallJournalRequest.decode(MySqlFormat3Binary.takeOwned(golden),owner)
      assertEquals(journal,decoded.journalTag)
      assertEquals(original,decoded.originalTag)
      assertNotEquals(decoded.journalTag.operationId,decoded.originalTag.operationId)
      assertEquals(id(4),decoded.bindingUuid)
      assertEquals(id(5),decoded.actorUuid)
      assertEquals(id(6),decoded.podUuid)
      assertEquals(17L,decoded.expectedStateVersion)
      assertEquals(Some(id(7)),decoded.clientRequestUuid)
      assertTrue(decoded.canonical.exactlyEquals(value.canonical))
    finally owner.closeRootReadBudget()
    assertEquals(0L,owner.accountedMemoryBytes)

  @Test def legacyUncorrelatedOptionHasDistinctCanonicalFraming():Unit =
    val owner=root()
    try
      val expected=golden.take(198)++Array(0.toByte)++golden.drop(215)
      val value=fresh(owner,None)
      assertEquals(336,value.canonical.length)
      assertArrayEquals(expected,value.canonical.copyBytes(owner,"legacy journal request"))
      val decoded=MySqlDbFirstRecallJournalRequest.decode(MySqlFormat3Binary.takeOwned(expected),owner)
      assertEquals(None,decoded.clientRequestUuid)
      assertTrue(decoded.canonical.exactlyEquals(value.canonical))
      assertNotEquals(fresh(owner).canonical.sha256,value.canonical.sha256)
    finally owner.closeRootReadBudget()

  @Test def malformedWireCannotIntroduceAnotherOriginScopeOrProfile():Unit =
    val owner=root()
    def reject(bytes:Array[Byte]):Unit =
      assertThrows(classOf[SQLDataException],()=>
        MySqlDbFirstRecallJournalRequest.decode(MySqlFormat3Binary.takeOwned(bytes),owner))
    def change(update:Array[Byte]=>Unit):Unit =
      val bytes=golden
      update(bytes)
      reject(bytes)
    try
      reject(golden.dropRight(1));reject(golden++Array(0.toByte))
      change(b=>ByteBuffer.wrap(b).putInt(0,-1))
      change(b=>b(4)='X'.toByte)
      change(b=>b(44)=2)
      change(b=>b(45)=2) // Fresh不能用来导入旧未应用请求。
      for offset<-Vector(46,78,94,126,142,158,174,199) do
        change(b=>java.util.Arrays.fill(b,offset,offset+16,0.toByte))
      change(b=>System.arraycopy(b,78,b,126,16))
      change(b=>b(109)=8) // original boot与J不一致。
      for offset<-Vector(62,70,110,118) do change(b=>ByteBuffer.wrap(b).putLong(offset,0L))
      change(b=>ByteBuffer.wrap(b).putLong(190,-1L))
      change(b=>ByteBuffer.wrap(b).putLong(190,Long.MaxValue))
      change(b=>b(198)=2)
      change(b=>ByteBuffer.wrap(b).putInt(215,Int.MaxValue))
      change(b=>b(220)=(b(220)^1).toByte)
      change(b=>ByteBuffer.wrap(b).putInt(316,31))
      change(b=>b(351)=(b(351)^1).toByte)
    finally owner.closeRootReadBudget()

  @Test def validRequestFieldChangesChangeIdentityWithoutPretendingDataIsAuthentication():Unit =
    val owner=root()
    try
      val originalHash=fresh(owner).canonical.sha256
      val alternatives=Vector(
        MySqlDbFirstRecallJournalRequest.fresh(journal,original,id(8),id(5),id(6),17L,Some(id(7)),owner),
        MySqlDbFirstRecallJournalRequest.fresh(journal,original,id(4),id(8),id(6),17L,Some(id(7)),owner),
        MySqlDbFirstRecallJournalRequest.fresh(journal,original,id(4),id(5),id(8),17L,Some(id(7)),owner),
        MySqlDbFirstRecallJournalRequest.fresh(journal,original,id(4),id(5),id(6),18L,Some(id(7)),owner),
        fresh(owner,Some(id(8))),
        MySqlDbFirstRecallJournalRequest.fresh(journal.copy(operationId=id(8)),original,id(4),id(5),id(6),17L,Some(id(7)),owner),
        MySqlDbFirstRecallJournalRequest.fresh(journal,original.copy(operationId=id(8)),id(4),id(5),id(6),17L,Some(id(7)),owner))
      alternatives.foreach(value=>assertNotEquals(originalHash,value.canonical.sha256))
      assertEquals(alternatives.size,alternatives.map(_.canonical.sha256).distinct.size)
    finally owner.closeRootReadBudget()

  @Test def invalidNewRequestCannotReuseJournalAsBusinessOperationOrLoseCorrelationShape():Unit =
    val owner=root()
    def reject(j:IoOperationTag=journal,c:IoOperationTag=original,binding:UUID=id(4),
        actor:UUID=id(5),pod:UUID=id(6),version:Long=17L,client:Option[UUID]=Some(id(7))):Unit =
      assertThrows(classOf[SQLDataException],()=>
        MySqlDbFirstRecallJournalRequest.fresh(j,c,binding,actor,pod,version,client,owner))
    try
      reject(j=null);reject(c=null)
      reject(c=original.copy(operationId=journal.operationId))
      reject(c=original.copy(bootId=id(8)))
      reject(c=original.copy(bootGeneration=12L))
      reject(c=original.copy(connectionGeneration=14L))
      reject(j=journal.copy(bootId=id(0)));reject(c=original.copy(operationId=id(0)))
      reject(binding=null);reject(actor=id(0));reject(pod=id(0))
      reject(version= -1L);reject(version=Long.MaxValue)
      reject(client=null);reject(client=Some(null));reject(client=Some(id(0)))
      assertEquals(0L,owner.accountedMemoryBytes,"invalid inputs rejected before constructing request")
    finally owner.closeRootReadBudget()

  @Test def decodeCopiesIntoSameFamilyAndRejectsForeignClosedOrWrongProfileInput():Unit =
    val owner=root()
    val other=root()
    val different=new MySqlFormat3ReadBudget(profile(4097L),1048576L)
    val child=owner.newCanonicalDataScope()
    try
      val input=fresh(child).canonical.borrowedBinary
      val decoded=MySqlDbFirstRecallJournalRequest.decode(input,owner)
      assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallJournalRequest.decode(input,other))
      assertThrows(classOf[SQLDataException],()=>
        MySqlDbFirstRecallJournalRequest.decode(MySqlFormat3Binary.takeOwned(golden),different))
      child.closeScopedReadBudget()
      assertArrayEquals(golden,decoded.canonical.copyBytes(owner,"decoded journal outlives original RAW child"))
      assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallJournalRequest.decode(input,owner))
    finally
      child.closeScopedReadBudget()
      owner.closeRootReadBudget();other.closeRootReadBudget();different.closeRootReadBudget()
    assertThrows(classOf[SQLDataException],()=>fresh(owner))

  @Test def budgetPressurePrecedesAllocationAndTemporaryWorkspaceIsActuallyReleased():Unit =
    val owner=root()
    try
      val pressure=owner.availableMemoryBytes-4095L
      owner.reserveRetainedMemory(pressure,"journal request allocation pressure")
      val before=owner.accountedMemoryBytes
      assertThrows(classOf[SQLDataException],()=>fresh(owner))
      assertThrows(classOf[SQLDataException],()=>
        MySqlDbFirstRecallJournalRequest.decode(MySqlFormat3Binary.takeOwned(golden),owner))
      assertEquals(before,owner.accountedMemoryBytes)
      owner.releaseRetainedMemory(pressure,"journal allocation pressure removed")
      fresh(owner)
      assertEquals(4096L+352L+640L,owner.accountedMemoryBytes,
        "only immutable holder/payload/digest remain; both actual temporary workspaces released")
    finally owner.closeRootReadBudget()
    assertEquals(0L,owner.accountedMemoryBytes)
    val partial=root()
    try
      val pressure=partial.availableMemoryBytes-7000L
      partial.reserveRetainedMemory(pressure,"journal workspace pressure")
      val before=partial.accountedMemoryBytes
      assertThrows(classOf[SQLDataException],()=>fresh(partial))
      assertEquals(before+4096L,partial.accountedMemoryBytes,"partial holder not refunded as a successful request")
    finally partial.closeRootReadBudget()

  private val settlement=IoOperationTag(id(9),21L,23L,original.operationId)
  private def refusalGolden(correlated:Boolean=true):Array[Byte] =
    // 独立BE布局；原J request digest/profile来自既有固定wire，不调用被测writer。
    val domain="FP_DB_FIRST_RECALL_REFUSED_REQUEST_BE_V1".getBytes(java.nio.charset.StandardCharsets.US_ASCII)
    val profileBytes=golden.slice(219,316)
    val profileHash=golden.takeRight(32)
    val journalDigest=if correlated then HexFormat.of().parseHex("7E3CCEC78B7FB0EDFBA6DF39E416EF467F3FE646B76F3EB43544D45AA23EF083")
      else java.security.MessageDigest.getInstance("SHA-256").digest(golden.take(198)++Array(0.toByte)++golden.drop(215))
    val b=ByteBuffer.allocate(283+domain.length+profileBytes.length+(if correlated then 16 else 0))
    def uuid(value:UUID):Unit={b.putLong(value.getMostSignificantBits);b.putLong(value.getLeastSignificantBits);()}
    def tag(value:IoOperationTag):Unit={uuid(value.bootId);b.putLong(value.bootGeneration);b.putLong(value.connectionGeneration);uuid(value.operationId)}
    b.putInt(domain.length);b.put(domain);b.put(1.toByte);b.put(1.toByte)
    tag(settlement);tag(original);tag(journal);uuid(id(4));uuid(id(5));uuid(id(6));b.putLong(17L)
    b.put((if correlated then 1 else 0).toByte)
    if correlated then uuid(id(7))
    b.putInt(32);b.put(journalDigest);b.putInt(profileBytes.length);b.put(profileBytes);b.putInt(32);b.put(profileHash)
    assertEquals(b.capacity(),b.position());b.array()

  @Test def refusalWireKeepsOriginalCAndJWhileSettlementUsesActualCurrentScope():Unit =
    val owner=root()
    try
      val j=fresh(owner)
      val request=MySqlDbFirstRecallRefusalRequest.notApplied(settlement,j,owner)
      assertEquals(original,request.originalTag);assertEquals(journal,request.journalTag)
      assertEquals(settlement,request.tag);assertNotEquals(request.originalTag.bootId,request.tag.bootId)
      assertEquals(request.originalTag.operationId,request.tag.operationId)
      assertArrayEquals(refusalGolden(),request.canonical.copyBytes(owner,"independent refusal golden"))
      assertArrayEquals(java.security.MessageDigest.getInstance("SHA-256").digest(refusalGolden()),request.canonical.sha256.toArray)
      val decoded=MySqlDbFirstRecallRefusalRequest.decode(MySqlFormat3Binary.takeOwned(refusalGolden()),owner)
      decoded.requireJournalIdentity(j)
      assertTrue(decoded.canonical.exactlyEquals(request.canonical))
      assertNotEquals(j.canonical.sha256,request.canonical.sha256)
      assertThrows(classOf[SQLDataException],()=>decoded.requireJournalIdentity(null))
    finally owner.closeRootReadBudget()
    assertEquals(0L,owner.accountedMemoryBytes)

  @Test def refusalStrictDecoderRejectsMalformedScopeResultOptionAndProfile():Unit =
    val owner=root();val start=6+MySqlDbFirstRecallRefusalRequest.Domain.length
    def reject(bytes:Array[Byte]):Unit=assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRefusalRequest.decode(MySqlFormat3Binary.takeOwned(bytes),owner))
    def change(edit:Array[Byte]=>Unit):Unit={val bytes=refusalGolden();edit(bytes);reject(bytes)}
    try
      reject(refusalGolden().dropRight(1));reject(refusalGolden()++Array(0.toByte))
      change(b=>ByteBuffer.wrap(b).putInt(0,Int.MaxValue));change(b=>b(4)='X'.toByte)
      change(b=>b(start-2)=2);change(b=>b(start-1)=2)
      for offset<-Vector(start,start+32,start+48,start+80,start+96,start+128,start+144,start+160,start+176,start+201) do
        change(b=>java.util.Arrays.fill(b,offset,offset+16,0.toByte))
      for offset<-Vector(start+16,start+24,start+64,start+72,start+112,start+120) do
        change(b=>ByteBuffer.wrap(b).putLong(offset,0L))
      change(b=>b(start+47)=8) // actual settlement不得改为另一个C。
      change(b=>b(start+143)=3) // J不能等于原C。
      change(b=>b(start+111)=8) // 原J与原C必须同scope。
      change(b=>ByteBuffer.wrap(b).putLong(start+192,-1L))
      change(b=>ByteBuffer.wrap(b).putLong(start+192,Long.MaxValue))
      change(b=>b(start+200)=2)
      change(b=>ByteBuffer.wrap(b).putInt(start+217,31))
      change(b=>ByteBuffer.wrap(b).putInt(start+253,Int.MaxValue))
      change(b=>b(start+258)=(b(start+258)^1).toByte)
      change(b=>b(b.length-1)=(b.last^1).toByte)
    finally owner.closeRootReadBudget()

  @Test def validRefusalDataChangesStillRequireTheExactOriginalJournalIdentity():Unit =
    val owner=root();val start=6+MySqlDbFirstRecallRefusalRequest.Domain.length
    try
      val j=fresh(owner)
      // 这些仍是可编码DATA，不凭解码成功声称与实际J相符。
      for offset<-Vector(start+159,start+175,start+191,start+199,start+216,start+221) do
        val bytes=refusalGolden();bytes(offset)=(bytes(offset)^1).toByte
        val changed=MySqlDbFirstRecallRefusalRequest.decode(MySqlFormat3Binary.takeOwned(bytes),owner)
        assertThrows(classOf[SQLDataException],()=>changed.requireJournalIdentity(j))
      val uncorrelated=MySqlDbFirstRecallRefusalRequest.notApplied(settlement,fresh(owner,None),owner)
      val decoded=MySqlDbFirstRecallRefusalRequest.decode(uncorrelated.canonical.borrowedBinary,owner)
      assertEquals(None,decoded.clientRequestUuid)
      assertArrayEquals(refusalGolden(false),uncorrelated.canonical.copyBytes(owner,"independent uncorrelated refusal golden"))
      decoded.requireJournalIdentity(fresh(owner,None))
      assertThrows(classOf[SQLDataException],()=>decoded.requireJournalIdentity(j))
      assertEquals(16,refusalGolden().length-uncorrelated.canonical.length)
    finally owner.closeRootReadBudget()

  @Test def refusalOwnsItsMetadataAndBytesAfterOriginalJournalScopeRetires():Unit =
    val owner=root();val child=owner.newCanonicalDataScope();val foreign=root()
    try
      val j=fresh(child)
      val refusal=MySqlDbFirstRecallRefusalRequest.notApplied(settlement,j,owner)
      val copyScope=owner.newCanonicalDataScope()
      val decoded=try
        val raw=MySqlDbFirstRecallRefusalRequest.notApplied(settlement,j,copyScope).canonical.borrowedBinary
        val result=MySqlDbFirstRecallRefusalRequest.decode(raw,owner)
        assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRefusalRequest.decode(raw,foreign))
        result
      finally copyScope.closeScopedReadBudget()
      assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRefusalRequest.notApplied(settlement,j,foreign))
      child.closeScopedReadBudget()
      assertArrayEquals(refusalGolden(),refusal.canonical.copyBytes(owner,"independent refusal outlives J"))
      assertTrue(decoded.canonical.exactlyEquals(refusal.canonical))
      assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRefusalRequest.notApplied(settlement,j,owner))
    finally
      child.closeScopedReadBudget();owner.closeRootReadBudget();foreign.closeRootReadBudget()
    assertEquals(0L,owner.accountedMemoryBytes)

  @Test def refusalBudgetAndWrongSettlementRejectBeforeMutationOrAllocationSuccess():Unit =
    val owner=root()
    try
      val j=fresh(owner);val before=owner.accountedMemoryBytes
      assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRefusalRequest.notApplied(null,j,owner))
      assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRefusalRequest.notApplied(settlement.copy(operationId=id(8)),j,owner))
      assertEquals(before,owner.accountedMemoryBytes)
      val pressure=owner.availableMemoryBytes-4095L
      owner.reserveRetainedMemory(pressure,"refusal preallocation pressure")
      val pressured=owner.accountedMemoryBytes
      assertThrows(classOf[SQLDataException],()=>MySqlDbFirstRecallRefusalRequest.notApplied(settlement,j,owner))
      assertEquals(pressured,owner.accountedMemoryBytes)
      owner.releaseRetainedMemory(pressure,"refusal pressure removed")
      val request=MySqlDbFirstRecallRefusalRequest.notApplied(settlement,j,owner)
      assertEquals(before+4096L+request.canonical.length+640L,owner.accountedMemoryBytes,
        "only independent refusal metadata/payload/digest remain; actual encoder workspace released")
    finally owner.closeRootReadBudget()
    assertEquals(0L,owner.accountedMemoryBytes)
