package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.{Decoded, DeployRequest, MutationReceipt, Request, RetractRequest}
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.{Arrays, UUID}
import scala.util.control.NonFatal

/** 部署行的数据解码边界；不认证actor，不发行receipt/SQL/Ready能力。 */
private[mysql] object MySqlDeploymentOperationRows:
  enum ColumnValue:
    case Binary(value: Array[Byte])
    case Integer(value: Long)
    case SqlNull

  /** 实际SELECT显式列的顺序值；JDBC边界须用wasNull保留SQL NULL，不能把NULL当0。 */
  final case class ColumnValues(values: Vector[ColumnValue])

  val columnNames: Vector[String] = Vector(
    "namespace", "operation_uuid", "actor_uuid", "authority_binding_uuid",
    "request_encoding", "request_bytes", "request_sha256", "receipt_encoding", "receipt_bytes", "receipt_sha256",
    "row_revision", "created_boot_uuid", "created_boot_generation", "created_writer_epoch",
    "current_writer_boot_uuid", "current_writer_epoch", "register_step_uuid", "last_step_uuid", "last_step_code",
    "last_step_fingerprint", "enqueued_end_ordinal", "max_queue_age_end_ordinals",
    "enqueue_evidence_encoding", "enqueue_evidence_bytes", "enqueue_evidence_sha256",
    "step_evidence_encoding", "step_evidence_bytes", "step_evidence_sha256",
    "diagnostic_encoding", "diagnostic_bytes", "diagnostic_sha256")

  final class DecodeFailure private[MySqlDeploymentOperationRows](val column: String, cause: Throwable)
      extends IllegalArgumentException(s"invalid deployment operation column: $column", cause)

  private enum Frozen:
    case Binary(value: Array[Byte])
    case Integer(value: Long)
    case SqlNull

  /** 原列完整保留，所有字节入口/出口均复制；request和receipt是实际r03不可变record。 */
  final class Row private[MySqlDeploymentOperationRows](
      private val cells: Vector[Frozen],
      val actorUuid: UUID, val operationUuid: UUID, val request: Request, val receipt: MutationReceipt):
    def rowRevision: Long = integer("row_revision").get
    def sameOriginal(actor: UUID, original: Request): Boolean = actorUuid == actor && request == original

    def integer(column: String): Option[Long] = cells(index(column)) match
      case Frozen.Integer(value) => Some(value)
      case Frozen.SqlNull => None
      case _ => throw new IllegalArgumentException("column is not an integer")

    /** 导出复制也向真实接收方预算计费；本行不持有或关闭任何预算/会话。 */
    def copyBinary(column: String, budget: MySqlFormat3ReadBudget): Option[Array[Byte]] =
      requireBudget(budget)
      cells(index(column)) match
        case Frozen.SqlNull => None
        case Frozen.Binary(value) =>
          budget.reserveRetainedMemory(Math.addExact(32L, value.length.toLong), "deployment row exported binary")
          Some(value.clone())
        case _ => throw new IllegalArgumentException("column is not binary")

  private val uuidColumns = Set(1, 2, 3, 11, 14, 16, 17)
  private val hashColumns = Set(6, 9, 19, 24, 27, 30)
  private val numericColumns = Set(4, 7, 10, 12, 13, 15, 18, 20, 21, 22, 25, 28)
  private val encodings = Vector(4, 7, 22, 25)
  // 固定容器、UUID/record及数组头的保守表示收费；这是内存账成本，不是生产容量默认值。
  private val representationBytes = 4096L
  private val codecTemporaryBytes = 2048L

  /** lookup参数来自实际调用者捕获，UUID本身不表示认证；禁止跨actor/operation行逃逸。 */
  def decode(input: ColumnValues, actor: UUID, operation: UUID, budget: MySqlFormat3ReadBudget): Row =
    var column = "row"
    var retained = 0L
    var retainedReserved = false
    var temporary = false
    var completed = false
    var primary: Throwable = null
    try
      requireBudget(budget)
      require(input != null && input.values != null && input.values.size == columnNames.size,
        "exact deployment column vector required")
      require(actor != null && operation != null, "original actor-scoped lookup identity required")
      var total = 0L
      var i = 0
      while i < input.values.size do
        val cell = input.values(i)
        column = columnNames(i)
        require(cell != null, "NULL carrier is not SQL NULL")
        cell match
          case ColumnValue.SqlNull => require(i >= 28, "required SQL value is NULL")
          case ColumnValue.Integer(value) =>
            require(numericColumns(i), "binary column received integer")
            require(value >= 0L, "negative stored integer")
            total = Math.addExact(total, 8L)
          case ColumnValue.Binary(value) =>
            require(!numericColumns(i) && value != null, "invalid binary carrier")
            val expected = if uuidColumns(i) then 16 else if hashColumns(i) then 32
              else if i == 0 then 4 else if i == 5 then 134 else if i == 8 then 142 else -1
            require(if expected >= 0 then value.length == expected else value.nonEmpty, "wrong binary length")
            total = Math.addExact(total, value.length.toLong)
        i += 1
      column = "row_packet_length"
      require(total <= budget.profile.maxSqlPacketBytes(), "row exceeds explicit SQL packet profile")
      column = "row_budget"
      retained = Math.addExact(representationBytes, total)
      budget.reserveRetainedMemory(retained, "deployment row retained representation")
      retainedReserved = true
      budget.reserveTemporaryMemory(codecTemporaryBytes, "deployment row actual codec temporaries")
      temporary = true
      val cells = input.values.map {
        case ColumnValue.Binary(value) => Frozen.Binary(value.clone())
        case ColumnValue.Integer(value) => Frozen.Integer(value)
        case ColumnValue.SqlNull => Frozen.SqlNull
      }
      def bytes(i: Int): Array[Byte] = cells(i) match
        case Frozen.Binary(value) => value
        case _ => throw new IllegalArgumentException("required binary")
      def number(i: Int): Long = cells(i) match
        case Frozen.Integer(value) => value
        case _ => throw new IllegalArgumentException("required integer")
      def uuid(i: Int): UUID =
        val buffer = ByteBuffer.wrap(bytes(i))
        new UUID(buffer.getLong(), buffer.getLong())
      def at(i: Int)(check: => Boolean): Unit =
        column = columnNames(i)
        require(check, "stored value violates deployment row encoding")
      at(0)(Arrays.equals(bytes(0), Array[Byte](70, 80, 68, 67)))
      at(1)(uuid(1) == operation)
      at(2)(uuid(2) == actor)
      encodings.foreach(i => at(i)(number(i) == 1L))
      at(18)(number(18) >= 1L && number(18) <= 5L)
      at(15)(number(15) >= number(13))
      val nullDiagnostic = cells.drop(28).count(_ == Frozen.SqlNull)
      at(28)(nullDiagnostic == 0 || nullDiagnostic == 3)
      if nullDiagnostic == 0 then at(28)(number(28) == 1L)
      // 证据/诊断仅核列encoding、完整bytes/hash；未冻结的producer语义不在此认证。
      Vector((5, 6), (8, 9), (23, 24), (26, 27)).foreach { (payload, hash) =>
        at(hash)(MessageDigest.isEqual(MessageDigest.getInstance("SHA-256").digest(bytes(payload)), bytes(hash)))
      }
      if nullDiagnostic == 0 then at(30)(MessageDigest.isEqual(
        MessageDigest.getInstance("SHA-256").digest(bytes(29)), bytes(30)))
      column = "request_bytes"
      val request: Request = DeploymentCommandWireCodec.decodeRequest(bytes(5)) match
        case decoded: Decoded[?] => decoded.value() match
          case value: DeployRequest => value
          case value: RetractRequest => value
          case _ => throw new IllegalArgumentException("stored request is not a mutation")
        case _ => throw new IllegalArgumentException("actual r03 request decoder rejected frame")
      require(Arrays.equals(DeploymentCommandWireCodec.encodeRequest(request), bytes(5)), "noncanonical request")
      column = "receipt_bytes"
      val receipt = DeploymentCommandWireCodec.decodeResponse(bytes(8)) match
        case decoded: Decoded[?] => decoded.value() match
          case value: MutationReceipt => value
          case _ => throw new IllegalArgumentException("stored response is not MutationReceipt")
        case _ => throw new IllegalArgumentException("actual r03 response decoder rejected frame")
      require(Arrays.equals(DeploymentCommandWireCodec.encodeResponse(receipt), bytes(8)), "noncanonical receipt")
      val receiptRequest: Request = receipt.commandKind() match
        case DeploymentCommandWireCodec.MutationKind.DEPLOY => new DeployRequest(receipt.operationId(),
          receipt.podUuid(), receipt.moduleInstanceId(), receipt.moduleGeneration(), receipt.expected())
        case DeploymentCommandWireCodec.MutationKind.RETRACT => new RetractRequest(receipt.operationId(),
          receipt.podUuid(), receipt.moduleInstanceId(), receipt.moduleGeneration(), receipt.expected())
      require(request == receiptRequest && receipt.operationId() == operation, "receipt differs from original mutation identity")
      column = "row_revision"
      if number(10) == 0L then
        require(number(18) == 1L && uuid(16) == uuid(17) && uuid(11) == uuid(14) && number(13) == number(15),
          "initial registration metadata differs")
        require(receipt.phase() == DeploymentCommandWireCodec.OperationPhase.QUEUED, "initial row lacks QUEUED receipt")
      else require(number(18) != 1L, "REGISTER cannot replace an existing revision")
      val result = new Row(cells, actor, operation, request, receipt)
      completed = true
      result
    catch
      case NonFatal(cause) =>
        val failure = new DecodeFailure(column, cause)
        primary = failure
        throw failure
    finally
      def cleanup(action: => Unit): Unit =
        try action
        catch
          case NonFatal(cause) => if primary != null then primary.addSuppressed(cause) else throw cause
      if temporary then cleanup(budget.releaseTemporaryMemory(codecTemporaryBytes, "deployment row codec temporaries"))
      if !completed && retainedReserved then cleanup(budget.releaseRetainedMemory(retained, "rejected deployment row"))

  private def index(column: String): Int =
    val result = columnNames.indexOf(column)
    require(result >= 0, "unknown deployment column")
    result

  private def requireBudget(budget: MySqlFormat3ReadBudget): Unit =
    require(budget != null && !budget.isClosed, "actual open read budget required")
