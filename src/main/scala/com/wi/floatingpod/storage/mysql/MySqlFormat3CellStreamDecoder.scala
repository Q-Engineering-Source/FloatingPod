package com.wi.floatingpod.storage.mysql

import java.io.{InputStream, Reader}
import java.sql.SQLDataException

/** 在 JDBC 流读取边界逐步核对编码、数据包与同时存活的本地内存预算。 */
private[mysql] object MySqlFormat3CellStreamDecoder:
  private val StreamBufferBytes = 8192L
  private val StringBuilderObjectBudget = 64L
  private val StringBufferBytesPerCharacter = 2L
  private val BinaryArrayObjectBudget = 64L
  private val BinaryNodeObjectBudget = 64L

  def readText(reader: Reader, maximumCharacters: Option[Int], budget: MySqlFormat3ReadBudget, context: String,
      maximumUtf8Bytes: Option[Long] = None): String =
    require(reader != null && maximumCharacters != null && maximumUtf8Bytes != null &&
      maximumUtf8Bytes.forall(_ >= 0L) && budget != null && context != null, "text stream decoding requires a reader and explicit budgets")
    var accepted = false
    var retainedStringBytes = 0L
    var primary: Throwable = null
    try
      val result = MySqlBusinessRows.using(reader) { input =>
        budget.beginVariableCell(context)
        val charBufferBytes = Math.addExact(Math.multiplyExact(StreamBufferBytes, 2L), 64L)
        budget.reserveTemporaryMemory(charBufferBytes, s"$context UTF-16 stream buffer")
        var charBufferReserved = true
        var builderTemporaryBytes = 0L
        var value: StringBuilder = null
        var innerPrimary: Throwable = null
        try
          budget.reserveTemporaryMemory(StringBuilderObjectBudget, s"$context StringBuilder object")
          builderTemporaryBytes = StringBuilderObjectBudget
          value = new StringBuilder(0)
          val chars = new Array[Char](StreamBufferBytes.toInt)
          var codePoints = 0
          var utf8Bytes = 0L
          var pendingHighSurrogate = false
          var deferredHighSurrogate: Option[Char] = None

          def appendValidatedCharacter(current: Char): Unit =
            val added = appendTextChar(current, pendingHighSurrogate, codePoints, maximumCharacters,
              value, context, budget, utf8Bytes, builderTemporaryBytes, maximumUtf8Bytes)
            pendingHighSurrogate = added._2
            codePoints = added._3
            utf8Bytes = Math.addExact(utf8Bytes, added._1.toLong)
            builderTemporaryBytes = added._4

          def appendCharacter(current: Char): Unit =
            if maximumUtf8Bytes.nonEmpty && deferredHighSurrogate.nonEmpty then
              val high = deferredHighSurrogate.get
              if !Character.isLowSurrogate(current) then
                throw new SQLDataException(s"$context contains an unpaired UTF-16 high surrogate")
              // 先验证完整码点，再在任何 builder 追加前预核4字节；不以容量错误掩盖非法代理项。
              appendValidatedCharacter(high)
              appendValidatedCharacter(current)
              deferredHighSurrogate = None
            else if maximumUtf8Bytes.nonEmpty && Character.isHighSurrogate(current) then
              deferredHighSurrogate = Some(current)
            else appendValidatedCharacter(current)

          var done = false
          while !done do
            val count = input.read(chars, 0, chars.length)
            if count < 0 then done = true
            else if count == 0 then
              val single = input.read()
              if single < 0 then done = true else appendCharacter(single.toChar)
            else
              var index = 0
              while index < count do
                appendCharacter(chars(index))
                index += 1

          if pendingHighSurrogate || deferredHighSurrogate.nonEmpty then
            throw new SQLDataException(s"$context ends with an unpaired UTF-16 high surrogate")
          val stringLength = value.length.toLong
          val worstCaseStringArrayBytes = Math.multiplyExact(stringLength, StringBufferBytesPerCharacter)
          MySqlFormat3JvmArrayLimit.requireLength(worstCaseStringArrayBytes, s"$context UTF-16 String storage")
          val immutableStringCharge = Math.addExact(64L, worstCaseStringArrayBytes)
          budget.reserveRetainedMemory(immutableStringCharge, s"$context immutable String")
          retainedStringBytes = immutableStringCharge
          value.toString
        catch
          case failure: Throwable =>
            innerPrimary = failure
            throw failure
        finally
          val releaseActions = Vector.newBuilder[() => Unit]
          if builderTemporaryBytes > 0L then
            val released = builderTemporaryBytes
            releaseActions += (() => budget.releaseTemporaryMemory(released, s"$context StringBuilder backing and object"))
            builderTemporaryBytes = 0L
          if charBufferReserved then
            releaseActions += (() => budget.releaseTemporaryMemory(charBufferBytes, s"$context UTF-16 stream buffer"))
            charBufferReserved = false
          MySqlFormat3BudgetCleanup.run(innerPrimary, releaseActions.result())
      }
      accepted = true
      result
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      if !accepted && retainedStringBytes > 0L then
        MySqlFormat3BudgetCleanup.run(primary, Vector(() => budget.releaseRetainedMemory(retainedStringBytes, s"$context discarded String after read/close failure")))

  private def appendTextChar(
      current: Char,
      pendingHigh: Boolean,
      currentCodePoints: Int,
      maximumCharacters: Option[Int],
      value: StringBuilder,
      context: String,
      budget: MySqlFormat3ReadBudget,
      bytesAlreadyRead: Long,
      priorBuilderCharge: Long,
      maximumUtf8Bytes: Option[Long]
  ): (Int, Boolean, Int, Long) =
    val (byteCount, nextPending, completesCodePoint) =
      if pendingHigh then
        if !Character.isLowSurrogate(current) then throw new SQLDataException(s"$context contains an unpaired UTF-16 high surrogate")
        (4, false, true)
      else if Character.isHighSurrogate(current) then
        if maximumCharacters.exists(_ <= currentCodePoints) then throw new SQLDataException(s"$context exceeds its frozen character length")
        (0, true, false)
      else if Character.isLowSurrogate(current) then throw new SQLDataException(s"$context contains an unpaired UTF-16 low surrogate")
      else
        val encodedBytes = if current <= 0x7f then 1 else if current <= 0x7ff then 2 else 3
        (encodedBytes, false, true)

    val nextCodePoints = if completesCodePoint then Math.addExact(currentCodePoints, 1) else currentCodePoints
    if maximumCharacters.exists(_ < nextCodePoints) then throw new SQLDataException(s"$context exceeds its frozen character length")
    // 高代理项尚未追加/扩容前即为完整 supplementary codepoint 预核4字节；
    // low surrogate 即使来自下一 chunk，也沿用尚未计入 utf8Bytes 的相同4字节检查。
    val anticipatedBytes = if nextPending then 4L else byteCount.toLong
    if maximumUtf8Bytes.exists(_ < Math.addExact(bytesAlreadyRead, anticipatedBytes)) then
      throw new SQLDataException(s"$context exceeds its explicit UTF-8 byte length")
    if byteCount > 0 then
      val nextBytes = Math.addExact(bytesAlreadyRead, byteCount.toLong)
      MySqlFormat3JvmArrayLimit.requireLength(nextBytes, s"$context canonical UTF-8 text")
      budget.addVariablePayload(byteCount.toLong, context)

    val requiredCapacity = value.length.toLong + 1L
    var currentBuilderCharge = priorBuilderCharge
    if requiredCapacity > value.capacity.toLong then
      val doubled = Math.addExact(Math.multiplyExact(value.capacity.toLong, 2L), 2L)
      val nextCapacity = Math.max(requiredCapacity, doubled)
      val worstCaseBackingBytes = Math.multiplyExact(nextCapacity, StringBufferBytesPerCharacter)
      MySqlFormat3JvmArrayLimit.requireLength(worstCaseBackingBytes, s"$context StringBuilder UTF-16 storage")
      val nextCharge = Math.addExact(StringBuilderObjectBudget + 64L, worstCaseBackingBytes)
      budget.reserveTemporaryMemory(nextCharge, s"$context StringBuilder expansion")
      try value.ensureCapacity(nextCapacity.toInt)
      catch
        case failure: Throwable =>
          MySqlFormat3BudgetCleanup.run(failure, Vector(() => budget.releaseTemporaryMemory(nextCharge, s"$context failed StringBuilder expansion")))
          throw failure
      if currentBuilderCharge > 0L then budget.releaseTemporaryMemory(currentBuilderCharge, s"$context old StringBuilder backing array")
      currentBuilderCharge = nextCharge
    value.append(current)
    (byteCount, nextPending, nextCodePoints, currentBuilderCharge)

  def readBinary(input: InputStream, maximumBytes: Option[Int], budget: MySqlFormat3ReadBudget, context: String): Array[Byte] =
    require(input != null && maximumBytes != null && budget != null && context != null, "binary stream decoding requires an input and explicit budgets")
    var chunkHead: MySqlFormat3BinaryChunk = null
    var currentNode: MySqlFormat3BinaryChunk = null
    var currentOffset = 0
    var chunkStorageBytes = 0L
    var scratchReserved = false
    var retainedArrayBytes = 0L
    var accepted = false
    var primary: Throwable = null
    try
      val result = MySqlBusinessRows.using(input) { stream =>
        budget.beginVariableCell(context)
        val scratchBytes = StreamBufferBytes
        val scratchCharge = Math.addExact(scratchBytes, 64L)
        budget.reserveTemporaryMemory(scratchCharge, s"$context binary stream buffer")
        scratchReserved = true
        val scratch = new Array[Byte](scratchBytes.toInt)
        var totalBytes = 0L
        var complete = false

        def append(source: Array[Byte], offset: Int, length: Int, payloadAllowance: Long): Unit =
          var remaining = length
          var sourceOffset = offset
          while remaining > 0 do
            if currentNode == null || currentOffset == currentNode.bytes.length then
              val columnRemaining = maximumBytes.map(_.toLong - totalBytes).getOrElse(MySqlFormat3JvmArrayLimit.MaximumLength - totalBytes)
              val allowed = Math.min(payloadAllowance, Math.min(columnRemaining, MySqlFormat3JvmArrayLimit.MaximumLength - totalBytes))
              val memoryAvailable = budget.availableMemoryBytes - BinaryArrayObjectBudget - BinaryNodeObjectBudget
              val nextCapacity = Math.min(StreamBufferBytes, Math.min(allowed, memoryAvailable))
              if nextCapacity <= 0L then throw new SQLDataException(s"$context exceeds explicit retained-memory, byte, or JVM array budget")
              val chunkCharge = Math.addExact(nextCapacity, BinaryArrayObjectBudget + BinaryNodeObjectBudget)
              val nextChunkStorage = Math.addExact(chunkStorageBytes, chunkCharge)
              budget.reserveTemporaryMemory(chunkCharge, s"$context binary chunk and node allocation")
              try
                val nextBytes = new Array[Byte](nextCapacity.toInt)
                val nextNode = new MySqlFormat3BinaryChunk(nextBytes, chunkHead)
                chunkHead = nextNode
                currentNode = nextNode
                currentOffset = 0
                chunkStorageBytes = nextChunkStorage
              catch
                case failure: Throwable =>
                  MySqlFormat3BudgetCleanup.run(failure, Vector(() => budget.releaseTemporaryMemory(chunkCharge, s"$context failed binary chunk allocation")))
                  throw failure
            val copied = Math.min(remaining, currentNode.bytes.length - currentOffset)
            System.arraycopy(source, sourceOffset, currentNode.bytes, currentOffset, copied)
            sourceOffset += copied
            currentOffset += copied
            currentNode.used = currentOffset
            remaining -= copied
            totalBytes = Math.addExact(totalBytes, copied.toLong)

        while !complete do
          val remaining = budget.variablePayloadRemaining
          val columnRemaining = maximumBytes.map(limit => limit.toLong - totalBytes)
            .getOrElse(MySqlFormat3JvmArrayLimit.MaximumLength - totalBytes)
          val arrayRemaining = MySqlFormat3JvmArrayLimit.MaximumLength - totalBytes
          val allowable = Math.min(remaining, Math.min(columnRemaining, arrayRemaining))
          if allowable < 0L then throw new SQLDataException(s"$context exceeds its frozen binary length or explicit byte budget")
          if allowable == 0L then
            val next = stream.read()
            if next < 0 then complete = true
            else throw new SQLDataException(s"$context exceeds its frozen binary length or explicit byte budget")
          else
            val requested = Math.min(scratch.length.toLong, allowable).toInt
            val count = stream.read(scratch, 0, requested)
            if count < 0 then complete = true
            else if count == 0 then
              val next = stream.read()
              if next < 0 then complete = true
              else
                budget.addVariablePayload(1L, context)
                scratch(0) = next.toByte
                append(scratch, 0, 1, allowable)
            else
              budget.addVariablePayload(count.toLong, context)
              append(scratch, 0, count, allowable)

        val outputLength = MySqlFormat3JvmArrayLimit.requireLength(totalBytes, s"$context flat immutable byte array")
        budget.reserveRetainedMemory(totalBytes, s"$context final immutable byte array")
        retainedArrayBytes = totalBytes
        val output = new Array[Byte](outputLength)
        var destination = output.length
        var node = chunkHead
        while node != null do
          destination -= node.used
          if node.used > 0 then System.arraycopy(node.bytes, 0, output, destination, node.used)
          node = node.next
        if destination != 0 then throw new SQLDataException(s"$context binary stream ended with an inconsistent retained length")
        output
      }
      accepted = true
      result
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      val releaseActions = Vector.newBuilder[() => Unit]
      if chunkStorageBytes > 0L then
        chunkHead = null
        currentNode = null
        val released = chunkStorageBytes
        releaseActions += (() => budget.releaseTemporaryMemory(released, s"$context binary chunks and nodes"))
        chunkStorageBytes = 0L
      if scratchReserved then
        releaseActions += (() => budget.releaseTemporaryMemory(Math.addExact(StreamBufferBytes, 64L), s"$context binary stream buffer"))
        scratchReserved = false
      if !accepted && retainedArrayBytes > 0L then
        val released = retainedArrayBytes
        releaseActions += (() => budget.releaseRetainedMemory(released, s"$context discarded flat binary array"))
        retainedArrayBytes = 0L
      MySqlFormat3BudgetCleanup.run(primary, releaseActions.result())

private final class MySqlFormat3BinaryChunk(val bytes: Array[Byte], val next: MySqlFormat3BinaryChunk):
  var used: Int = 0
