package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile

/** 完整 bytes + 完整 typed full20；只表达 DATA 一致性，不授当前 DB/Native/Ready/absence 资格。 */
private[mysql] final class MySqlFormat3CanonicalBundle private (
    val canonical: MySqlFormat3CanonicalPayload,
    val snapshot: MySqlFormat3FullSnapshot,
    private[mysql] val originalBudget: MySqlFormat3ReadBudget
):
  def byteLength: Int = canonical.byteLength
  def profile: SourceLimitProfile = snapshot.limitProfile
  // 没有 close/drop：bundle 可逃逸时原 root/child/bytes/typed refs 必须继续计费。
  // 真正责任结算后的 holder 撤销与原 root ACK 由后续 durable witness 接线负责。

private[mysql] object MySqlFormat3CanonicalBundle:
  def decode(payload: MySqlFormat3CanonicalPayload, profile: SourceLimitProfile,
      originalBudget: MySqlFormat3ReadBudget): MySqlFormat3CanonicalBundle =
    require(payload != null && profile != null && originalBudget != null &&
      (payload.originalBudget eq originalBudget) && (originalBudget.profile eq profile) && !originalBudget.isClosed,
      "exact original canonical DATA holder/profile required")
    originalBudget.reserveRetainedMemory(128L, "full20 complete DATA bundle holder")
    var decoded: MySqlFormat3FullSnapshot = null
    var complete = false
    var primary: Throwable = null
    try
      decoded = MySqlFormat3SnapshotCodec.decode(payload, profile, originalBudget)
      val result = new MySqlFormat3CanonicalBundle(payload, decoded, originalBudget)
      complete = true
      result
    catch
      case failure: Throwable =>
        primary = failure
        // Decoder 内失败已经释放局部 child；这里只处理 decode 成功但 bundle 构造失败。
        val abandonedBudget = if decoded == null then null else decoded.allocationBudget
        decoded = null
        if abandonedBudget != null then
          try abandonedBudget.abandonDecodedData()
          catch
            case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
        throw failure
    finally
      if !complete then
        MySqlFormat3BudgetCleanup.run(primary, Vector(
          () => originalBudget.releaseRetainedMemory(128L, "failed full20 DATA bundle holder")))
