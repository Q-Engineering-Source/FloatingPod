package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.DbFirstWorldAdoptionPlanData

/** Schema6 Full27 PREPARE DATA facade; inputs cannot be widened to legacy Full26 plan proofs. */
private[mysql] object MySqlDbFirstPrepareComposition:
  def prepareTarget(before: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget, plan: DbFirstWorldAdoptionPlanData,
      sourceWorldIdentity: String, provisionReceiptSha256: Vector[Byte],
      beforeWriter: MySqlDbFirstBootstrapGraph.PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget,
      expectedQuotaMaximumRecords: Long): MySqlScriptSchemaSnapshot =
    MySqlDeploymentReceiptFreshPrepareComposition.prepareDbFirstTarget(before,inputs,originalRoot,plan,
      sourceWorldIdentity,provisionReceiptSha256,beforeWriter,targetWriter,expectedQuotaMaximumRecords)

  def requirePrepareTransition(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs, originalRoot: MySqlFormat3ReadBudget,
      plan: DbFirstWorldAdoptionPlanData, sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte], beforeWriter: MySqlDbFirstBootstrapGraph.PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget,
      expectedQuotaMaximumRecords: Long): Unit =
    MySqlDeploymentReceiptFreshPrepareComposition.requireDbFirstPrepareTransition(before,target,inputs,originalRoot,
      plan,sourceWorldIdentity,provisionReceiptSha256,beforeWriter,targetWriter,expectedQuotaMaximumRecords)

  def contextForPrepare(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget,
      plan: DbFirstWorldAdoptionPlanData, sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte], writer: MySqlFreshFormat3WriterTarget,
      expectedQuotaMaximumRecords: Long): MySqlOperationContext =
    MySqlDeploymentReceiptFreshPrepareComposition.contextForDbFirstPrepare(before,target,inputs,root,plan,
      sourceWorldIdentity,provisionReceiptSha256,writer,expectedQuotaMaximumRecords)
