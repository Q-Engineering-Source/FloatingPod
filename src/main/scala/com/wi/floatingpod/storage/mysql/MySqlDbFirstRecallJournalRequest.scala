package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.io.IoOperationTag
import java.io.DataOutputStream
import java.sql.SQLDataException
import java.util.UUID

/** 耐久接纳前固定的请求DATA；J是内部收据，originalTag才是原业务/客户端回馈身份。
  * 本类型不证明已认证、已提交或可freeze，也不持有SQL/Main能力。
  */
private[mysql] final class MySqlDbFirstRecallJournalRequest private[MySqlDbFirstRecallJournalRequest] (
    val journalTag:IoOperationTag,val originalTag:IoOperationTag,
    val bindingUuid:UUID,val actorUuid:UUID,val podUuid:UUID,
    val expectedStateVersion:Long,val clientRequestUuid:Option[UUID],
    val canonical:MySqlRecoveryCanonicalBytes):
  override def toString:String =
    s"MySqlDbFirstRecallJournalRequest[DATA,sha256=${canonical.hexSha256}]"

/** 新请求的独立规范域；旧recall编码不改义。旧live未应用导入另有完整原事实，不能借用Fresh字节。 */
private[mysql] object MySqlDbFirstRecallJournalRequest:
  val Domain="FP_DB_FIRST_RECALL_JOURNAL_REQUEST_BE_V1"
  private val Fresh=1
  private val HolderBytes=4096L
  private val WorkspaceBytes=4096L
  private val ZeroUuid=new UUID(0L,0L)

  def fresh(journalTag:IoOperationTag,originalTag:IoOperationTag,bindingUuid:UUID,actorUuid:UUID,
      podUuid:UUID,expectedStateVersion:Long,clientRequestUuid:Option[UUID],
      owner:MySqlFormat3ReadBudget):MySqlDbFirstRecallJournalRequest =
    requireOwner(owner)
    validate(journalTag,originalTag,bindingUuid,actorUuid,podUuid,expectedStateVersion,clientRequestUuid)
    // 同一费用覆盖请求、tag/UUID/Option元数据和编码/解码游标；失败保留到真实owner退休。
    owner.reserveRetainedMemory(HolderBytes,"recall journal immutable request BEFORE allocation")
    owner.reserveTemporaryMemory(WorkspaceBytes,"recall journal profile/encoder workspace")
    var primary:Throwable=null
    try
      encode(journalTag,originalTag,bindingUuid,actorUuid,podUuid,expectedStateVersion,clientRequestUuid,
        owner.profile.canonicalBytes(),owner.profile.sha256(),owner)
    catch case cause:Throwable=>
      primary=cause
      throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(() =>
      owner.releaseTemporaryMemory(WorkspaceBytes,"recall journal profile/encoder workspace detached")))

  /** 有界逐字段读取并重编码到同一owner；不借入可在其owner退休后继续使用的RAW载荷。 */
  def decode(input:MySqlFormat3Binary,owner:MySqlFormat3ReadBudget):MySqlDbFirstRecallJournalRequest =
    requireOwner(owner)
    if input==null || (input.storageBudget!=null &&
        (input.storageBudget.isClosed || !input.storageBudget.sharesAllocationLedger(owner))) then
      fail("actual live journal bytes and matching allocation family required")
    owner.reserveRetainedMemory(HolderBytes,"recall journal decoded request/cursor BEFORE allocation")
    owner.reserveTemporaryMemory(WorkspaceBytes,"recall journal profile/decoder workspace")
    var primary:Throwable=null
    try
      val profileBytes=owner.profile.canonicalBytes()
      val profileHash=owner.profile.sha256()
      // 全部字段宽度已闭合；在创建游标/UUID前拒绝巨型、截断或尾随容器。
      val minimum=encodedLength(profileBytes.length,false)
      if input.length!=minimum && input.length!=minimum+16 then fail("journal request length differs")
      val reader=new Reader(input)
      reader.token(Domain)
      if reader.octet()!=1 || reader.octet()!=Fresh then fail("journal version/origin differs")
      val journalTag=reader.tag()
      val originalTag=reader.tag()
      val binding=reader.uuid()
      val actor=reader.uuid()
      val pod=reader.uuid()
      val version=reader.int64()
      val client=reader.octet() match
        case 0=>None
        case 1=>Some(reader.uuid())
        case _=>fail("journal client request option differs")
      reader.exactBytes(profileBytes)
      reader.exactBytes(profileHash)
      reader.end()
      validate(journalTag,originalTag,binding,actor,pod,version,client)
      encode(journalTag,originalTag,binding,actor,pod,version,client,profileBytes,profileHash,owner)
    catch case cause:Throwable=>
      primary=cause
      throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(() =>
      owner.releaseTemporaryMemory(WorkspaceBytes,"recall journal profile/decoder workspace detached")))

  private def requireOwner(owner:MySqlFormat3ReadBudget):Unit =
    if owner==null || owner.isClosed then fail("explicit open journal allocation owner required")

  private def nonzero(value:UUID):Boolean=value!=null && value!=ZeroUuid
  private[mysql] def validate(journal:IoOperationTag,original:IoOperationTag,binding:UUID,actor:UUID,pod:UUID,
      version:Long,client:Option[UUID]):Unit =
    if journal==null || original==null || !nonzero(journal.bootId) || !nonzero(original.bootId) ||
      !nonzero(journal.operationId) || !nonzero(original.operationId) ||
      journal.operationId==original.operationId || journal.bootId!=original.bootId ||
      journal.bootGeneration!=original.bootGeneration || journal.connectionGeneration!=original.connectionGeneration ||
      !nonzero(binding) || !nonzero(actor) || !nonzero(pod) || version<0L || version==Long.MaxValue then
      fail("fresh journal lost distinct J/original C or original request scope")
    client match
      case None=>()
      case Some(value) if nonzero(value)=>()
      case _=>fail("journal client request option is malformed")

  // token/version/origin、两个48B tag、三个UUID、version、Option和两段profile framing。
  private def encodedLength(profileLength:Int,correlated:Boolean):Int =
    Math.addExact(Math.addExact(199+Domain.length,profileLength),if correlated then 16 else 0)

  private def writeTag(out:DataOutputStream,tag:IoOperationTag):Unit =
    MySqlRecoveryCanonicalBytes.writeUuid(out,tag.bootId)
    out.writeLong(tag.bootGeneration);out.writeLong(tag.connectionGeneration)
    MySqlRecoveryCanonicalBytes.writeUuid(out,tag.operationId)

  private def encode(journal:IoOperationTag,original:IoOperationTag,binding:UUID,actor:UUID,pod:UUID,
      version:Long,client:Option[UUID],profileBytes:Array[Byte],profileHash:Array[Byte],
      owner:MySqlFormat3ReadBudget):MySqlDbFirstRecallJournalRequest =
    val bytes=MySqlRecoveryCanonicalBytes.withAllocationBudget(owner) {
      MySqlRecoveryCanonicalBytes.encodeExact(Domain,owner.profile,encodedLength(profileBytes.length,client.nonEmpty)) { out=>
        MySqlRecoveryCanonicalBytes.writeToken(out,Domain)
        out.writeByte(1);out.writeByte(Fresh)
        writeTag(out,journal);writeTag(out,original)
        MySqlRecoveryCanonicalBytes.writeUuid(out,binding)
        MySqlRecoveryCanonicalBytes.writeUuid(out,actor)
        MySqlRecoveryCanonicalBytes.writeUuid(out,pod)
        out.writeLong(version)
        MySqlRecoveryCanonicalBytes.writeOptionUuid(out,client)
        out.writeInt(profileBytes.length);out.write(profileBytes)
        out.writeInt(profileHash.length);out.write(profileHash)
      }
    }
    new MySqlDbFirstRecallJournalRequest(journal,original,binding,actor,pod,version,client,bytes)

  /** 不分配wire声明长度的数组或String；只有固定字段通过范围核验后才构造UUID/tag。 */
  private final class Reader(input:MySqlFormat3Binary):
    private var offset=0
    def octet():Int =
      if offset>=input.length then fail("truncated journal request")
      val value=input.byteAt(offset)&255
      offset+=1
      value
    def int32():Int=(octet()<<24)|(octet()<<16)|(octet()<<8)|octet()
    def int64():Long =
      var value=0L
      var i=0
      while i<8 do
        value=(value<<8)|octet().toLong
        i+=1
      value
    def uuid():UUID =
      val most=int64();val least=int64()
      if most==0L && least==0L then fail("zero journal UUID")
      new UUID(most,least)
    def tag():IoOperationTag =
      val boot=uuid()
      val bootGeneration=int64();val connectionGeneration=int64()
      if bootGeneration<=0L || connectionGeneration<=0L then fail("journal generations must be positive")
      val operation=uuid()
      IoOperationTag(boot,bootGeneration,connectionGeneration,operation)
    def token(expected:String):Unit =
      if int32()!=expected.length then fail("journal domain length differs")
      var i=0
      while i<expected.length do
        if octet()!=expected.charAt(i).toInt then fail("journal domain differs")
        i+=1
    def exactBytes(expected:Array[Byte]):Unit =
      if int32()!=expected.length then fail("journal profile frame differs")
      var i=0
      while i<expected.length do
        if octet()!=(expected(i)&255) then fail("journal profile differs")
        i+=1
    def end():Unit=if offset!=input.length then fail("trailing journal bytes")

  private def fail(message:String):Nothing=throw new SQLDataException(message)
