package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.server.source.{SourceForestAllocationBudget, SourceForestMembershipCodec}
import java.io.IOException
import java.sql.SQLDataException
import java.util.UUID

/** 当前 Full27 全森林 DATA 消费；不保留解码图，不授予 absence、写入或隔离证明。 */
private[mysql] final class MySqlDbFirstForestRead(
    private val root: MySqlFormat3ReadBudget, private val inputs: MySqlScriptSchemaInputs,
    private val binding: UUID):
  private var child: MySqlFormat3ReadBudget=null
  private var rowFee=0L
  private var holderFee=MySqlDbFirstForestRead.HolderBytes
  private var started=false
  private var completed=false
  private var forests=0L
  private var identities=0L
  private var summary: MySqlDbFirstForestRead.Summary=null
  private var excludedPod: UUID=null

  /** 初建必须检查完整已验证membership；成功仅表示本次DATA扫描未碰撞，不发行absence资格。 */
  private[mysql] def validateFreshPod(snapshot: MySqlScriptSchemaSnapshot, proposedPod: UUID): Unit =
    require(!started && proposedPod != null && proposedPod != new UUID(0L,0L),
      "original fresh forest scan and non-zero proposed POD required")
    excludedPod=proposedPod
    validate(snapshot)

  private[mysql] def requireOwner(actualRoot: MySqlFormat3ReadBudget, actualInputs: MySqlScriptSchemaInputs,
      actualBinding: UUID): Unit =
    require(holderFee != 0L && (actualRoot eq root) && (actualInputs eq inputs) && binding == actualBinding &&
      binding != null && binding != new UUID(0L,0L),"exact active Full27 forest read holder required")
    root.requireScriptDataInputs(inputs)

  def validate(snapshot: MySqlScriptSchemaSnapshot): Unit =
    require(!started && holderFee != 0L,"Full27 forest DATA observation is one-shot")
    started=true
    MySqlFormat3SourceForestMembershipScan.consumeDbFirst(snapshot,inputs,root,binding,this)
    summary=MySqlDbFirstForestRead.Summary(forests,identities)
    completed=true

  def result: MySqlDbFirstForestRead.Summary =
    if !completed || holderFee == 0L || summary == null then
      throw new IllegalStateException("complete Full27 forest DATA observation unfinished or detached")
    summary

  private[mysql] def consumeRow(row: Vector[MySqlFormat3Cell]): Unit =
    requireOwner(root,inputs,binding)
    require(started && !completed && child == null && rowFee == 0L,"original forest row construction required")
    root.reserveTemporaryMemory(1024L,"Full27 forest row child/adapter/callback BEFORE allocation")
    rowFee=1024L
    var primary: Throwable=null
    try
      child=root.newCanonicalScope()
      val actualChild=child
      def binary(index: Int): MySqlFormat3Binary = row(index) match
        case MySqlFormat3Cell.Binary(bytes) if bytes != null => bytes
        case _ => throw new SQLDataException("Full27 forest mandatory binary payload absent or mistyped")
      def number(index: Int): Long = row(index) match
        case MySqlFormat3Cell.SignedInteger(value) => value
        case _ => throw new SQLDataException("Full27 forest scalar mistyped")
      def text(index: Int): String = row(index) match
        case MySqlFormat3Cell.Text(value) if value != null => value
        case _ => throw new SQLDataException("Full27 forest token mistyped")
      def uuid(index: Int): UUID =
        val bytes=binary(index)
        if bytes.length != 16 then throw new SQLDataException("Full27 forest UUID is not exact 16-byte DATA")
        actualChild.reserveTemporaryMemory(32L,"Full27 forest SQL expectation UUID BEFORE allocation")
        var most=0L;var least=0L;var offset=0
        while offset < 8 do
          most=(most << 8) | (bytes.byteAt(offset).toLong & 255L)
          least=(least << 8) | (bytes.byteAt(offset+8).toLong & 255L)
          offset+=1
        new UUID(most,least)
      if row == null || row.size != 15 || inputs.forTable("physical_source_forest").columns.size != 15 then
        throw new SQLDataException("Full27 forest complete row shape differs")
      if text(4) != "DRAFT" && text(4) != "SEALED" then throw new SQLDataException("Full27 forest state unsupported")
      val expected=new SourceForestMembershipCodec.ForestExpectation(uuid(0),uuid(1),uuid(2),number(3),uuid(12),
        text(5),number(8),number(9),number(10),number(11))
      if expected.binding() != binding then throw new SQLDataException("Full27 forest SQL binding differs from current image authority")
      val adapter=new SourceForestAllocationBudget:
        override def exactProfile(): SourceLimitProfile = root.profile
        override def reserveTemporary(bytes: Long, context: String): Unit =
          try
            requireOwner(root,inputs,binding)
            if actualChild.isClosed || !actualChild.isCanonicalComparisonChildOf(root) ||
              !actualChild.sharesAllocationLedger(root) then throw new SQLDataException("Full27 forest adapter lost original child")
            actualChild.reserveTemporaryMemory(bytes,context)
          catch case cause: java.sql.SQLException => throw new IOException("Full27 forest work reservation failed",cause)
        override def releaseTemporary(bytes: Long, context: String): Unit =
          try actualChild.releaseTemporaryMemory(bytes,context)
          catch case cause: java.sql.SQLException => throw new IOException("Full27 forest work release failed",cause)
      val forest=binary(6).copyBytes(actualChild,"Full27 complete forest SQL payload copy")
      val forestSha=binary(7).copyBytes(actualChild,"Full27 complete forest SQL digest copy")
      val index=binary(13).copyBytes(actualChild,"Full27 complete forest membership SQL payload copy")
      val indexSha=binary(14).copyBytes(actualChild,"Full27 complete forest membership SQL digest copy")
      SourceForestMembershipCodec.consumeVerifiedGraph(forest,forestSha,index,indexSha,root.profile,adapter,expected,
        (graph,membership) =>
          // 只在同步回调内提取已验证身份及其 unit/forest 关联，不向外保存图或 index 引用。
          val actual=graph.data().identity()
          val iterator=membership.identities().iterator()
          while iterator.hasNext do
            val pod=iterator.next()
            require(pod != null && actual.bindingId() == binding && actual.unitId() == expected.unit() &&
              actual.forestId() == expected.forest(),"verified forest membership association differs")
            if pod == excludedPod then
              throw new SQLDataException("Full27 initial preparation POD already occurs in verified source forest membership")
            identities=Math.addExact(identities,1L)
          forests=Math.addExact(forests,1L))
    catch case cause: Throwable =>
      primary=cause
      throw cause
    finally
      try closeChild()
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)

  private def closeChild(): Unit =
    if child != null then
      child.closeScopedReadBudget()
      child=null
    if rowFee != 0L then
      root.releaseTemporaryMemory(rowFee,"Full27 forest row child detached")
      rowFee=0L
  def close(): Unit =
    closeChild()
    if holderFee != 0L then
      summary=null
      root.releaseRetainedMemory(holderFee,"Full27 forest read holder detached")
      holderFee=0L

private[mysql] object MySqlDbFirstForestRead:
  val HolderBytes=1024L
  final case class Summary(verifiedForests: Long,verifiedMembershipAssociations: Long)
