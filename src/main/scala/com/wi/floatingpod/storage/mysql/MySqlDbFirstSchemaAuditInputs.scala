package com.wi.floatingpod.storage.mysql

/** 仅由完整 schema6 K/C、原 owner 与 live lease 构造；不复用 schema5 consumer 身份。 */
final class MySqlDbFirstSchemaAuditInputs private[mysql] (
    binding: MySqlManifestConfigurationIdentity.DbFirstBinding, lease: MySqlRuntimeInputs.ConsumerLease)
    extends MySqlStaticSchemaAuditInputs(binding, lease, 6, 512L):
  private var expectation: MySqlSchemaAuditExpectation = null
  override protected def detachAdditional(): Unit = expectation = null
  private[mysql] def expectationFor(actual: MySqlManifestConfigurationIdentity.DbFirstBinding,
      baseline: MySqlApprovedStaticBaseline, policy: MySqlRuntimeAuditPolicy): MySqlSchemaAuditExpectation =
    requireBinding(actual)
    if expectation == null then
      val id = baseline.identity
      expectation = MySqlSchemaAuditExpectation.forDbFirst1(id.serverUuid, id.serverVersion, id.schema, effectiveUser,
        callerRoles, id.provisioningUuid, id.bindingUuid, id.ddlSha256, id.auditDefiner, auditDefinerRoles,
        policy.expectedImport, baseline.objects, this)
    expectation
  override def toString: String = "MySqlDbFirstSchemaAuditInputs[redacted; schema6 security consumer]"
