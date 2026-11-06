package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile

/** 安装 DATA 与正式装配有不同能力生命周期，但必须共同受同一个内存上限约束。 */
private[mysql] final class MySqlManagedReadBudgets(profile: SourceLimitProfile, packetLimit: Long)
    extends AutoCloseable:
  private val ledger = new MySqlFormat3AllocationLedger(profile.maxRetainedBytes())
  val dataRoot: MySqlFormat3ReadBudget =
    MySqlFormat3ReadBudget.rootOnLedger(profile, packetLimit, ledger)
  val assemblyRoot: MySqlFormat3ReadBudget =
    MySqlFormat3ReadBudget.deploymentAssemblyRootOnLedger(profile, packetLimit, ledger)

  override def close(): Unit =
    var failure: Throwable = null
    try assemblyRoot.closeRootReadBudget()
    catch case cause: Throwable => failure = cause
    try dataRoot.closeRootReadBudget()
    catch
      case cause: Throwable =>
        if failure == null then failure = cause
        else if cause ne failure then failure.addSuppressed(cause)
    if failure != null then throw failure
