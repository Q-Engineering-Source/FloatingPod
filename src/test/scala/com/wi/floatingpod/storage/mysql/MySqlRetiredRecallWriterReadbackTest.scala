package com.wi.floatingpod.storage.mysql

import java.nio.ByteBuffer
import java.nio.file.Path
import java.time.LocalDateTime
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.Assertions.*

/** 真实比较逻辑的synthetic typed DATA用例；不签SQL、owner注册或运行时验收。 */
class MySqlRetiredRecallWriterReadbackTest:
  import DeploymentReceiptStaticTestData.*
  import MySqlFormat3Cell.*
  @TempDir var directory:Path = _
  private val W="fp_runtime_writer"
  private val now=LocalDateTime.of(2026,11,4,0,0)
  private val old=MySqlWriterFenceRow(UUID.fromString("11111111-1111-1111-1111-111111111111"),7L,
    Some(UUID.fromString("22222222-2222-2222-2222-222222222222")),now.plusSeconds(3),
    Some(UUID.fromString("33333333-3333-3333-3333-333333333333")),Some(Vector.fill(32)(5.toByte)))
  private val mutation=MySqlWriterFenceMutation(old.copy(ownerBootUuid=None,leaseUntil=now.minusNanos(1000)),now,1L)

  private def withInputs(f:MySqlScriptSchemaInputs=>Unit):Unit =
    val fixture=MySqlDbFirstHistoricalTestFixture.fixture(directory,defaultProfile())
    val root=fixture.root()
    val owner=MySqlRuntimeInputs.beginV2(root,Thread.currentThread())
    val graph=MySqlRuntimeInputs.assembleDbFirstStatic(fixture.metadata,owner,policy)
    val inputs=MySqlRuntimeInputs.assembleDbFirstSessionInputs(graph,root,MySqlOwnedIoIdentity(UUID.randomUUID(),7L,1L))
    try f(inputs.full27Inputs)
    finally { inputs.close();owner.close();root.closeRootReadBudget() }

  private def binary(bytes:Array[Byte])=Binary(MySqlFormat3Binary.takeOwned(bytes))
  private def uuid(id:UUID)=binary(ByteBuffer.allocate(16).putLong(id.getMostSignificantBits).putLong(id.getLeastSignificantBits).array())
  private def writer(row:MySqlWriterFenceRow,inputs:MySqlScriptSchemaInputs):Vector[MySqlFormat3Cell] =
    val fields:Map[String,MySqlFormat3Cell]=Map("singleton_fixed1"->SignedInteger(1L),
      "binding_uuid"->uuid(row.bindingUuid),"writer_epoch"->SignedInteger(row.epoch),
      "owner_boot_uuid"->row.ownerBootUuid.map(uuid).getOrElse(SqlNull),"lease_until"->DateTime(row.leaseUntil),
      "last_commit_operation_uuid"->row.lastCommitOperationUuid.map(uuid).getOrElse(SqlNull),
      "last_commit_fingerprint"->row.lastCommitFingerprint.map(x=>binary(x.toArray)).getOrElse(SqlNull))
    inputs.forTable(W).columns.map(c=>fields(c.name))
  private def relations(row:MySqlWriterFenceRow,inputs:MySqlScriptSchemaInputs)=
    MySqlDbFirstSnapshotSql.canonicalOrder.map { shape=>
      val cells=if shape.table==W then writer(row,inputs)
        else shape.columns.zipWithIndex.map((c,i)=>if i==0 then binary(Array[Byte](1,2,3)) else Text(c.name))
      MySqlFormat3RelationSnapshot(shape.table,Vector(cells))
    }
  private def verify(a:Vector[MySqlFormat3RelationSnapshot],b:Vector[MySqlFormat3RelationSnapshot],
      inputs:MySqlScriptSchemaInputs,m:MySqlWriterFenceMutation=mutation):Unit =
    MySqlRetiredRecallWriterReadback.requireExactRelations(a,b,inputs,old,m)

  @Test def acceptsOnlyExactRetirementWithIndependentEqualBinaryCells():Unit = withInputs { inputs=>
    val before=relations(old,inputs);val after=relations(mutation.row,inputs)
    assertNotSame(before.head.rows.head.head,after.head.rows.head.head)
    verify(before,after,inputs)
    val empty=before.map(r=>if r.table==W then r else r.copy(rows=Vector.empty))
    verify(empty,after.map(r=>if r.table==W then r else r.copy(rows=Vector.empty)),inputs)
  }

  @Test def rejectsChangedCellsInEveryOtherRelationIncludingBinaryContent():Unit = withInputs { inputs=>
    val before=relations(old,inputs);val after=relations(mutation.row,inputs)
    after.indices.filter(i=>after(i).table!=W).foreach { i=>
      after(i).rows.head.indices.foreach { column=>
        val changed=if column==0 then binary(Array[Byte](1,2,4)) else Text("changed")
        val row=after(i).rows.head.updated(column,changed)
        val altered=after.updated(i,after(i).copy(rows=Vector(row)))
        assertThrows(classOf[IllegalArgumentException],()=>verify(before,altered,inputs),after(i).table)
      }
    }
  }

  @Test def rejectsEveryWriterColumnChangeAndWrongLockedPreimage():Unit = withInputs { inputs=>
    val before=relations(old,inputs);val after=relations(mutation.row,inputs)
    val i=after.indexWhere(_.table==W)
    after(i).rows.head.indices.foreach { column=>
      val row=after(i).rows.head.updated(column,Text("foreign"))
      assertThrows(classOf[IllegalArgumentException],()=>verify(before,after.updated(i,after(i).copy(rows=Vector(row))),inputs))
    }
    assertThrows(classOf[IllegalArgumentException],()=>verify(relations(old.copy(epoch=8L),inputs),after,inputs))
    assertThrows(classOf[IllegalArgumentException],()=>verify(before,relations(mutation.row.copy(leaseUntil=now.minusSeconds(1)),inputs),inputs))
  }

  @Test def rejectsIncompleteReorderedExtraRowsAndNonRetirementMutation():Unit = withInputs { inputs=>
    val before=relations(old,inputs);val after=relations(mutation.row,inputs)
    assertThrows(classOf[IllegalArgumentException],()=>verify(before,after.dropRight(1),inputs))
    assertThrows(classOf[IllegalArgumentException],()=>verify(before,after.reverse,inputs))
    assertThrows(classOf[IllegalArgumentException],()=>verify(before,after.updated(0,after.head.copy(rows=after.head.rows++after.head.rows)),inputs))
    assertThrows(classOf[IllegalArgumentException],()=>verify(before,after,inputs,mutation.copy(row=mutation.row.copy(epoch=8L))))
    assertThrows(classOf[IllegalArgumentException],()=>verify(before,after,inputs,mutation.copy(row=old)))
    assertThrows(classOf[IllegalArgumentException],()=>verify(before,after,inputs,mutation.copy(databaseNow=mutation.row.leaseUntil)))
  }

  @Test def exactAcquiredColumnsPreserveAllOtherCellsAndRetiredReceiptSlots():Unit = withInputs { inputs=>
    val empty=mutation.row
    val acquired=empty.copy(epoch=8L,ownerBootUuid=old.ownerBootUuid,leaseUntil=now.plusSeconds(5))
    val before=relations(empty,inputs);val after=relations(acquired,inputs)
    def check(data:Vector[MySqlFormat3RelationSnapshot]):Unit =
      MySqlRetiredRecallWriterReadback.requireExactAcquisitionRelations(before,data,inputs,empty,acquired)
    check(after)
    after.indices.foreach { table=>
      after(table).rows.head.indices.foreach { column=>
        val row=after(table).rows.head.updated(column,Text("changed"))
        assertThrows(classOf[IllegalArgumentException],()=>check(after.updated(table,after(table).copy(rows=Vector(row)))))
      }
    }
    assertThrows(classOf[IllegalArgumentException],()=>check(relations(acquired.copy(leaseUntil=now.plusSeconds(6)),inputs)))
  }

  @Test def rejectsAcquisitionMetadataWithOccupiedSourceWrongEpochOrChangedReceipt():Unit = withInputs { inputs=>
    val empty=mutation.row
    val acquired=empty.copy(epoch=8L,ownerBootUuid=old.ownerBootUuid,leaseUntil=now.plusSeconds(5))
    val before=relations(empty,inputs);val after=relations(acquired,inputs)
    def check(source:MySqlWriterFenceRow,target:MySqlWriterFenceRow):Unit =
      MySqlRetiredRecallWriterReadback.requireExactAcquisitionRelations(before,after,inputs,source,target)
    assertThrows(classOf[IllegalArgumentException],()=>check(old,acquired))
    assertThrows(classOf[IllegalArgumentException],()=>check(empty,acquired.copy(epoch=7L)))
    assertThrows(classOf[IllegalArgumentException],()=>check(empty,acquired.copy(epoch=9L)))
    assertThrows(classOf[IllegalArgumentException],()=>check(empty,acquired.copy(ownerBootUuid=None)))
    assertThrows(classOf[IllegalArgumentException],()=>check(empty,acquired.copy(leaseUntil=empty.leaseUntil)))
    assertThrows(classOf[IllegalArgumentException],()=>check(empty,acquired.copy(bindingUuid=UUID.randomUUID())))
    assertThrows(classOf[IllegalArgumentException],()=>check(empty,acquired.copy(lastCommitOperationUuid=None,lastCommitFingerprint=None)))
    assertThrows(classOf[IllegalArgumentException],()=>check(empty,acquired.copy(lastCommitFingerprint=Some(Vector.fill(32)(6.toByte)))))
  }
