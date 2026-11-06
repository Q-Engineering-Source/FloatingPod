package com.wi.floatingpod.storage.mysql

import java.util.UUID

/** 只比较已验证完整DATA；调用方仍须证明实际owner、reader、scope与mutation来源。 */
private[mysql] object MySqlRetiredRecallWriterReadback:
  private val W="fp_runtime_writer"

  private def sameCell(a:MySqlFormat3Cell,b:MySqlFormat3Cell):Boolean = (a,b) match
    case (MySqlFormat3Cell.Binary(x),MySqlFormat3Cell.Binary(y))=>x!=null && y!=null && x.compareTo(y)==0
    case _=>a==b

  private def writerMatches(row:Vector[MySqlFormat3Cell],expected:MySqlWriterFenceRow,
      inputs:MySqlScriptSchemaInputs):Boolean =
    val shape=inputs.forTable(W)
    if row.size!=shape.columns.size then return false
    def v(name:String)=MySqlDbFirstRecallMaterializer.value(row,W,name,inputs)
    def uuid(name:String,value:Option[UUID]):Boolean=value match
      case Some(id)=>MySqlDbFirstRecallMaterializer.uuidEquals(v(name),id)
      case None=>v(name)==MySqlFormat3Cell.SqlNull
    val hash=expected.lastCommitFingerprint match
      case None=>v("last_commit_fingerprint")==MySqlFormat3Cell.SqlNull
      case Some(bytes)=>v("last_commit_fingerprint") match
        case MySqlFormat3Cell.Binary(actual) if actual!=null && actual.length==bytes.size=>
          bytes.indices.forall(i=>actual.byteAt(i)==bytes(i))
        case _=>false
    v("singleton_fixed1")==MySqlFormat3Cell.SignedInteger(1L) &&
      MySqlDbFirstRecallMaterializer.uuidEquals(v("binding_uuid"),expected.bindingUuid) &&
      v("writer_epoch")==MySqlFormat3Cell.SignedInteger(expected.epoch) &&
      uuid("owner_boot_uuid",expected.ownerBootUuid) &&
      v("lease_until")==MySqlFormat3Cell.DateTime(expected.leaseUntil) &&
      uuid("last_commit_operation_uuid",expected.lastCommitOperationUuid) && hash

  /** 不复制/排序relations，不构造替换W后的假snapshot；按已验证canonical PK顺序逐cell比较。 */
  def requireExactRelations(before:Vector[MySqlFormat3RelationSnapshot],
      after:Vector[MySqlFormat3RelationSnapshot],inputs:MySqlScriptSchemaInputs,
      lockedBefore:MySqlWriterFenceRow,confirmed:MySqlWriterFenceMutation):Unit =
    require(inputs!=null && inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27 &&
      before!=null && after!=null && before.size==27 && after.size==27 &&
      lockedBefore!=null && confirmed!=null && lockedBefore.ownerBootUuid.nonEmpty &&
      confirmed.row.ownerBootUuid.isEmpty &&
      confirmed.row==lockedBefore.copy(ownerBootUuid=None,leaseUntil=confirmed.row.leaseUntil) &&
      confirmed.row.leaseUntil.isBefore(confirmed.databaseNow),
      "exact owned-W retirement before/confirmed after images required")
    requireRows(before,after,inputs,lockedBefore,confirmed.row)

  /** acquired仅是actual owner从已确认退休行和真实token提取的预期列；本比较不发行writer资格。 */
  def requireExactAcquisitionRelations(before:Vector[MySqlFormat3RelationSnapshot],
      after:Vector[MySqlFormat3RelationSnapshot],inputs:MySqlScriptSchemaInputs,
      retired:MySqlWriterFenceRow,acquired:MySqlWriterFenceRow):Unit =
    require(retired!=null && acquired!=null && retired.ownerBootUuid.isEmpty &&
      acquired.ownerBootUuid.nonEmpty && acquired.epoch==Math.incrementExact(retired.epoch) &&
      acquired.leaseUntil.isAfter(retired.leaseUntil) &&
      acquired==retired.copy(epoch=acquired.epoch,ownerBootUuid=acquired.ownerBootUuid,leaseUntil=acquired.leaseUntil),
      "exact empty-W/new-epoch acquisition with unchanged binding/receipt slots required")
    requireRows(before,after,inputs,retired,acquired)

  private def requireRows(before:Vector[MySqlFormat3RelationSnapshot],
      after:Vector[MySqlFormat3RelationSnapshot],inputs:MySqlScriptSchemaInputs,
      expectedBefore:MySqlWriterFenceRow,expectedAfter:MySqlWriterFenceRow):Unit =
    require(inputs!=null && inputs.dataLayout==MySqlScriptSchemaDataLayout.DbFirstFull27 &&
      before!=null && after!=null && before.size==27 && after.size==27,
      "writer-transition comparison requires complete ordered Full27")
    val order=MySqlDbFirstSnapshotSql.canonicalOrder
    var relation=0
    while relation<order.size do
      val a=before(relation);val b=after(relation);val shape=order(relation)
      require(a.table==shape.table && b.table==shape.table && a.rows.size==b.rows.size,
        "writer-transition Full27 table order/row count changed: "+shape.table)
      if a.table==W then
        require(a.rows.size==1 && writerMatches(a.rows.head,expectedBefore,inputs) &&
          writerMatches(b.rows.head,expectedAfter,inputs),
          "writer-transition W differs from exact before/after columns")
      else
        var row=0
        while row<a.rows.size do
          val x=a.rows(row);val y=b.rows(row)
          require(x.size==shape.columns.size && y.size==x.size,
            "writer-transition Full27 column count changed: "+shape.table)
          var column=0
          while column<x.size do
            require(sameCell(x(column),y(column)),
              "writer-transition Full27 cell changed: "+shape.table+"."+shape.columns(column).name)
            column+=1
          row+=1
      relation+=1
