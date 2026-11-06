package com.wi.floatingpod.storage.mysql

import java.io.DataOutputStream
import java.sql.SQLDataException

/** 仅固定七个历史 payload 可编码 descriptor；调用前仍验证整个原 RAW row。 */
private[mysql] object MySqlControlledDebugHistoricalRow:
  private val Table = "fp_recovery_operation"

  def requireValid(row: Vector[MySqlFormat3Cell], root: MySqlFormat3ReadBudget): Unit =
    if row == null || root == null || root.isClosed || root.isScopedReadBudget then
      fail("complete row and open original budget required")
    // 读回再生成处于原root的lexical comparison child；保持实际owner，不能从child倒切回root。
    val actual = MySqlRecoveryCanonicalBytes.allocationBudgetOrStandalone(root)
    if actual.isClosed || (actual.profile ne root.profile) ||
      ((actual ne root) && !actual.isCanonicalChildOf(root)) then
      fail("historical validation differs from original canonical allocation scope")
    MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(row, root.profile, actual)

  /** 返回 false 表示该列必须 ordinary 编码；不对未知 bytes 列推广 descriptor。 */
  def writeDescriptor(output: DataOutputStream, row: Vector[MySqlFormat3Cell], column: String): Boolean =
    if output == null || row == null || column == null then fail("descriptor writer inputs required")
    val digestColumn = column match
      case "request_identity_bytes" => "request_identity_sha256"
      case "original_context_bytes" => "original_context_sha256"
      case "scope_bytes" => "scope_sha256"
      case "preimage_bytes" => "preimage_sha256"
      case "target_bytes" => "target_sha256"
      case "commit_witness_bytes" => "commit_witness_sha256"
      case "original_diagnostic_identity" => "original_diagnostic_sha256"
      case _ => null
    if digestColumn == null then false
    else
      val value = MySqlModeOperationEffectCodec.value(row, Table, column)
      val digest = MySqlModeOperationEffectCodec.value(row, Table, digestColumn)
      value match
        case MySqlFormat3Cell.SqlNull =>
          if digest != MySqlFormat3Cell.SqlNull then fail("NULL historical payload requires NULL digest")
          false
        case MySqlFormat3Cell.Binary(bytes) =>
          digest match
            case MySqlFormat3Cell.Binary(hash) if hash.length == 32 =>
              output.writeByte(7)
              MySqlFormat3Utf8.write(column, output)
              MySqlFormat3Utf8.write("HISTORICAL_RAW_PAYLOAD_V1", output)
              output.writeLong(bytes.length.toLong)
              hash.writeTo(output)
              true
            case _ => fail("historical payload requires exact binary digest")
        case _ => fail("historical payload type")

  private def fail(reason: String): Nothing = throw new SQLDataException("controlled historical descriptor: " + reason)
