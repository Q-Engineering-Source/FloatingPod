package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException

/** 固定 POD full-key 核对；hash仅索引，不决定 reservation 属于本请求。 */
private[mysql] object MySqlInactiveDomainResourceKey:
  def requireRow(row: Vector[MySqlFormat3Cell],request: MySqlInactiveDomainUpdateRequest,
      key: MySqlRecoveryResourceKey,originalRoot: MySqlFormat3ReadBudget): Unit =
    if row==null || request==null || key==null || originalRoot==null || originalRoot.isClosed ||
      (key.exactProfile ne originalRoot.profile) || key.resourceKind!="POD" || !key.podUuid.contains(request.podUuid) then
      fail("original fixed POD key and profile required")
    val table="fp_recovery_reservation"
    def value(name: String)=MySqlModeOperationEffectCodec.value(row,table,name)
    value("binding_uuid") match
      case MySqlFormat3Cell.Binary(bytes) if bytes.equalsUuid(request.expectedBinding.bindingUuid) => ()
      case _ => fail("binding differs")
    value("resource_kind") match
      case MySqlFormat3Cell.Text(kind) if kind=="POD" => ()
      case _ => fail("POD resource required")
    value("resource_pod_uuid") match
      case MySqlFormat3Cell.Binary(bytes) if bytes.equalsUuid(request.podUuid) => ()
      case _ => fail("resource pod differs")
    if value("resource_unit_uuid")!=MySqlFormat3Cell.SqlNull then fail("POD unit requires SQL NULL")
    value("resource_key_hash") match
      case MySqlFormat3Cell.Binary(bytes) if bytes.length==32 && bytes.equalsVector(key.keyHashSha256) => ()
      case _ => fail("resource index digest differs")
    MySqlRecoveryCanonicalBytes.withComparisonBudget(originalRoot) {
      val child=MySqlRecoveryCanonicalBytes.allocationBudgetFor(originalRoot.profile)
      var exactBytes: Array[Byte]=null
      try
        exactBytes=key.copyCanonicalBytes(child,"inactive fixed POD full-key comparison")
        value("resource_key") match
          case MySqlFormat3Cell.Binary(bytes) if bytes.equalsBytes(exactBytes) => ()
          case _ => fail("complete resource key differs")
      finally exactBytes=null
      // child scope owns and releases this fixed key copy only after its reference is dropped.
    }
  private def fail(reason: String): Nothing = throw new SQLDataException("inactive reservation key: " + reason)
