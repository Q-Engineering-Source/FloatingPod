package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.RecallRefusalReason
import com.wi.floatingpod.server.io.IoOperationTag
import java.io.DataOutputStream
import java.sql.SQLDataException
import java.util.UUID

/** 原C的未应用结果DATA；实际结算scope可跨boot，原J/C和认证关联仍不可改写。
  * 本类型不证明J已提交、C缺席、旧executor已退出或可执行拒绝SQL。
  */
private[mysql] final class MySqlDbFirstRecallRefusalRequest private[MySqlDbFirstRecallRefusalRequest] (
    val tag:IoOperationTag,val originalTag:IoOperationTag,val journalTag:IoOperationTag,
    val bindingUuid:UUID,val actorUuid:UUID,val podUuid:UUID,val expectedStateVersion:Long,
    val clientRequestUuid:Option[UUID],val journalRequestSha256:Vector[Byte],
    val refusalReason:Option[RecallRefusalReason],val canonical:MySqlRecoveryCanonicalBytes):
  /** 只核完整原请求DATA；调用者仍须独立证明实际J提交、C缺席和未应用资格。 */
  def requireJournalIdentity(journal:MySqlDbFirstRecallJournalRequest):Unit =
    if journal==null || originalTag!=journal.originalTag || journalTag!=journal.journalTag ||
        bindingUuid!=journal.bindingUuid || actorUuid!=journal.actorUuid || podUuid!=journal.podUuid ||
        expectedStateVersion!=journal.expectedStateVersion || clientRequestUuid!=journal.clientRequestUuid ||
        journalRequestSha256!=journal.canonical.sha256 then
      throw new SQLDataException("refusal original journal DATA identity differs")
  override def toString:String=s"MySqlDbFirstRecallRefusalRequest[DATA,sha256=${canonical.hexSha256}]"

/** 只定义原请求未应用这一内部结果，不复用CANCELLED或捏造原Recall行。
  * 旧live导入仍须独立证明proof→J delta；不能借此Fresh J身份代替该证明。
  */
private[mysql] object MySqlDbFirstRecallRefusalRequest:
  val Domain="FP_DB_FIRST_RECALL_REFUSED_REQUEST_BE_V1"
  val ReasonDomain="FP_DB_FIRST_RECALL_REFUSED_REQUEST_BE_V2"
  private val OriginalRecallNotApplied=1
  private val HolderBytes=4096L
  private val WorkspaceBytes=4096L
  private val ZeroUuid=new UUID(0L,0L)

  def notApplied(tag:IoOperationTag,journal:MySqlDbFirstRecallJournalRequest,
      owner:MySqlFormat3ReadBudget):MySqlDbFirstRecallRefusalRequest =
    notAppliedVersioned(tag,journal,None,owner)
  def notAppliedWithReason(tag:IoOperationTag,journal:MySqlDbFirstRecallJournalRequest,
      reason:RecallRefusalReason,owner:MySqlFormat3ReadBudget):MySqlDbFirstRecallRefusalRequest =
    require(reason!=null,"actual typed Main refusal reason required")
    notAppliedVersioned(tag,journal,Some(reason),owner)
  /** 原请求索引metadata的独立编码，仅DATA，不是已提交J或Main拒绝许可。
    * 调用者须先在完整读中严格匹配J；比较child完全退出后才可进入保留root编码。
    */
  def notAppliedRecoveredWithReason(tag:IoOperationTag,original:MySqlDbFirstRecallRequestRecord,
      reason:RecallRefusalReason,owner:MySqlFormat3ReadBudget):MySqlDbFirstRecallRefusalRequest =
    requireOwner(owner)
    require(original!=null && reason!=null && original.journalRequestSha256!=null &&
      original.journalRequestSha256.length==64 && original.journalRequestSha256.forall(c=>
        (c>='0' && c<='9') || (c>='A' && c<='F')),"complete original metadata and explicit typed reason required")
    owner.reserveRetainedMemory(HolderBytes,"recovered C refusal immutable metadata BEFORE allocation")
    owner.reserveTemporaryMemory(WorkspaceBytes,"recovered C refusal encoder workspace")
    var primary:Throwable=null
    try
      val digest=java.util.HexFormat.of().parseHex(original.journalRequestSha256).toVector
      validate(tag,original.originalTag,original.journalTag,original.bindingUuid,original.actorUuid,
        original.podUuid,original.expectedStateVersion,original.clientRequestUuid,digest)
      def copyUuid(value:UUID)=new UUID(value.getMostSignificantBits,value.getLeastSignificantBits)
      def copyTag(value:IoOperationTag)=IoOperationTag(copyUuid(value.bootId),value.bootGeneration,
        value.connectionGeneration,copyUuid(value.operationId))
      encode(copyTag(tag),copyTag(original.originalTag),copyTag(original.journalTag),copyUuid(original.bindingUuid),
        copyUuid(original.actorUuid),copyUuid(original.podUuid),original.expectedStateVersion,
        original.clientRequestUuid.map(copyUuid),digest,Some(reason),owner.profile.canonicalBytes(),owner.profile.sha256(),owner)
    catch case cause:Throwable=>
      primary=cause
      throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(() =>
      owner.releaseTemporaryMemory(WorkspaceBytes,"recovered C refusal encoder workspace detached")))

  private def notAppliedVersioned(tag:IoOperationTag,journal:MySqlDbFirstRecallJournalRequest,
      reason:Option[RecallRefusalReason],owner:MySqlFormat3ReadBudget):MySqlDbFirstRecallRefusalRequest =
    requireOwner(owner)
    if journal==null then fail("original journal request required")
    validate(tag,journal.originalTag,journal.journalTag,journal.bindingUuid,journal.actorUuid,
      journal.podUuid,journal.expectedStateVersion,journal.clientRequestUuid,journal.canonical.sha256)
    val source=journal.canonical.borrowedBinary.storageBudget
    if source==null || source.isClosed || !source.sharesAllocationLedger(owner) ||
        !(source.profile eq owner.profile) then fail("live original journal on exact profile/allocation family required")
    owner.reserveRetainedMemory(HolderBytes,"original C refusal immutable metadata BEFORE allocation")
    owner.reserveTemporaryMemory(WorkspaceBytes,"original C refusal encoder workspace")
    var primary:Throwable=null
    try
      def copyUuid(value:UUID)=new UUID(value.getMostSignificantBits,value.getLeastSignificantBits)
      def copyTag(value:IoOperationTag)=IoOperationTag(copyUuid(value.bootId),value.bootGeneration,
        value.connectionGeneration,copyUuid(value.operationId))
      // 不保留journal对象/RAW；旧J root退休后仍由新owner完整计费。
      encode(copyTag(tag),copyTag(journal.originalTag),copyTag(journal.journalTag),
        copyUuid(journal.bindingUuid),copyUuid(journal.actorUuid),copyUuid(journal.podUuid),
        journal.expectedStateVersion,journal.clientRequestUuid.map(copyUuid),
        Vector.tabulate(32)(journal.canonical.sha256(_)),reason,owner.profile.canonicalBytes(),owner.profile.sha256(),owner)
    catch case cause:Throwable=>
      primary=cause
      throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(() =>
      owner.releaseTemporaryMemory(WorkspaceBytes,"original C refusal encoder workspace detached")))

  def decode(input:MySqlFormat3Binary,owner:MySqlFormat3ReadBudget):MySqlDbFirstRecallRefusalRequest =
    requireOwner(owner)
    if input==null || (input.storageBudget!=null &&
        (input.storageBudget.isClosed || !input.storageBudget.sharesAllocationLedger(owner))) then
      fail("live refusal bytes on matching allocation family required")
    owner.reserveRetainedMemory(HolderBytes,"original C refusal decoded metadata BEFORE allocation")
    owner.reserveTemporaryMemory(WorkspaceBytes,"original C refusal decoder workspace")
    var primary:Throwable=null
    try
      val profile=owner.profile.canonicalBytes();val profileHash=owner.profile.sha256()
      val minimum=encodedLength(profile.length,false)
      if !Set(minimum,minimum+16,minimum+1,minimum+17).contains(input.length) then fail("refusal request length differs")
      val reader=new Reader(input)
      val wireVersion=reader.domainVersion()
      if reader.octet()!=wireVersion || reader.octet()!=OriginalRecallNotApplied then fail("refusal version/result differs")
      val tag=reader.tag();val original=reader.tag();val journal=reader.tag()
      val binding=reader.uuid();val actor=reader.uuid();val pod=reader.uuid();val stateVersion=reader.int64()
      val client=reader.octet() match
        case 0=>None
        case 1=>Some(reader.uuid())
        case _=>fail("refusal client request option differs")
      val digest=reader.digest()
      val reason=if wireVersion==1 then None else
        val id=reader.octet()
        Some(RecallRefusalReason.values.find(_.canonicalId==id).getOrElse(fail("unknown typed refusal reason")))
      if input.length!=encodedLength(profile.length,client.nonEmpty)+(if reason.nonEmpty then 1 else 0) then
        fail("refusal version/option length differs")
      reader.exactBytes(profile);reader.exactBytes(profileHash);reader.end()
      validate(tag,original,journal,binding,actor,pod,stateVersion,client,digest)
      encode(tag,original,journal,binding,actor,pod,stateVersion,client,digest,reason,profile,profileHash,owner)
    catch case cause:Throwable=>
      primary=cause
      throw cause
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(() =>
      owner.releaseTemporaryMemory(WorkspaceBytes,"original C refusal decoder workspace detached")))

  private def requireOwner(owner:MySqlFormat3ReadBudget):Unit =
    if owner==null || owner.isClosed then fail("explicit open refusal allocation owner required")
  private def validate(tag:IoOperationTag,original:IoOperationTag,journal:IoOperationTag,
      binding:UUID,actor:UUID,pod:UUID,version:Long,client:Option[UUID],digest:Vector[Byte]):Unit =
    MySqlDbFirstRecallJournalRequest.validate(journal,original,binding,actor,pod,version,client)
    if tag==null || tag.bootId==null || tag.bootId==ZeroUuid || tag.bootGeneration<=0L ||
        tag.connectionGeneration<=0L || tag.operationId!=original.operationId ||
        digest==null || digest.size!=32 then fail("refusal must settle the exact original C in an explicit current scope")
    // Fresh J原scope亦需完整有效；实际结算scope不必与它相同。
    if original.bootGeneration<=0L || original.connectionGeneration<=0L then fail("original journal generations differ")

  // token/version/result、三个48B tag、三个UUID、version、Option、J SHA和profile两段frame。
  private def encodedLength(profileLength:Int,correlated:Boolean):Int =
    Math.addExact(Math.addExact(283+Domain.length,profileLength),if correlated then 16 else 0)
  private def writeTag(out:DataOutputStream,tag:IoOperationTag):Unit =
    MySqlRecoveryCanonicalBytes.writeUuid(out,tag.bootId)
    out.writeLong(tag.bootGeneration);out.writeLong(tag.connectionGeneration)
    MySqlRecoveryCanonicalBytes.writeUuid(out,tag.operationId)
  private def encode(tag:IoOperationTag,original:IoOperationTag,journal:IoOperationTag,
      binding:UUID,actor:UUID,pod:UUID,version:Long,client:Option[UUID],digest:Vector[Byte],
      reason:Option[RecallRefusalReason],profile:Array[Byte],profileHash:Array[Byte],owner:MySqlFormat3ReadBudget):MySqlDbFirstRecallRefusalRequest =
    val domain=if reason.isEmpty then Domain else ReasonDomain
    val bytes=MySqlRecoveryCanonicalBytes.withAllocationBudget(owner) {
      MySqlRecoveryCanonicalBytes.encodeExact(domain,owner.profile,encodedLength(profile.length,client.nonEmpty)+(if reason.nonEmpty then 1 else 0)) { out=>
        MySqlRecoveryCanonicalBytes.writeToken(out,domain);out.writeByte(if reason.isEmpty then 1 else 2);out.writeByte(OriginalRecallNotApplied)
        writeTag(out,tag);writeTag(out,original);writeTag(out,journal)
        MySqlRecoveryCanonicalBytes.writeUuid(out,binding);MySqlRecoveryCanonicalBytes.writeUuid(out,actor)
        MySqlRecoveryCanonicalBytes.writeUuid(out,pod);out.writeLong(version)
        MySqlRecoveryCanonicalBytes.writeOptionUuid(out,client)
        out.writeInt(32);digest.foreach(value=>out.writeByte(value.toInt))
        reason.foreach(value=>out.writeByte(value.canonicalId))
        out.writeInt(profile.length);out.write(profile);out.writeInt(profileHash.length);out.write(profileHash)
      }
    }
    new MySqlDbFirstRecallRefusalRequest(tag,original,journal,binding,actor,pod,version,client,digest,reason,bytes)

  /** 固定字段与严格长度；不按wire声明分配任意数组/String。 */
  private final class Reader(input:MySqlFormat3Binary):
    private var offset=0
    def octet():Int =
      if offset>=input.length then fail("truncated refusal request")
      val value=input.byteAt(offset)&255;offset+=1;value
    def int32():Int=(octet()<<24)|(octet()<<16)|(octet()<<8)|octet()
    def int64():Long =
      var value=0L;var index=0
      while index<8 do
        value=(value<<8)|octet().toLong;index+=1
      value
    def uuid():UUID =
      val most=int64();val least=int64()
      if most==0L && least==0L then fail("zero refusal UUID")
      new UUID(most,least)
    def tag():IoOperationTag =
      val boot=uuid();val generation=int64();val connection=int64();val operation=uuid()
      if generation<=0L || connection<=0L then fail("refusal generations must be positive")
      IoOperationTag(boot,generation,connection,operation)
    def domainVersion():Int =
      if int32()!=Domain.length then fail("refusal domain length differs")
      Domain.dropRight(1).foreach(c=>if octet()!=c.toInt then fail("refusal domain differs"))
      octet() match
        case 49=>1
        case 50=>2
        case _=>fail("unknown refusal domain version")
    def digest():Vector[Byte] =
      if int32()!=32 then fail("refusal journal digest length differs")
      Vector.tabulate(32)(_=>octet().toByte)
    def exactBytes(expected:Array[Byte]):Unit =
      if int32()!=expected.length then fail("refusal profile frame differs")
      expected.foreach(b=>if octet()!=(b&255) then fail("refusal profile differs"))
    def end():Unit=if offset!=input.length then fail("trailing refusal bytes")
  private def fail(message:String):Nothing=throw new SQLDataException(message)
