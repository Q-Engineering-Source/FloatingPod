package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.FloatingPodPose
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 直接 producer DATA 回归；不制造 session/admission/commit/native proof。 */
class MySqlControlledDebugRowsTest:
  import MySqlControlledDebugRows.*
  private def id(n: Long)=new UUID(0L,n)
  private val pose=FloatingPodPose("debug世界",1.25,2.5,3.75,1.25f,-0.0f,0)
  private val at=LocalDateTime.of(2026,10,9,0,0,0,123456000)
  private def budget(retained: Long=16_000_000L)=new MySqlFormat3ReadBudget(
    SourceLimitProfile.of(1_000_000L,1_000_000L,2_000_000L,64L,256L,retained,4_000_000L,2_000_000L),4_000_000L)
  private def intent(root: MySqlFormat3ReadBudget)=preparedIntent(id(1),id(2),id(3),id(4),id(5),id(6),root)

  @Test def rearmAbsentPreimageColumnsAreSqlNull(): Unit =
    val columns=expectedOperationColumns(false,budget())
    assertEquals(Null,columns("expected_domain_version"));assertEquals(Null,columns("expected_locator_revision"));assertEquals(Null,columns("expected_source_revision"))
  @Test def activatePreimageColumnsAreExactZero(): Unit =
    val columns=expectedOperationColumns(true,budget())
    assertEquals(signed(0),columns("expected_domain_version"));assertEquals(signed(0),columns("expected_locator_revision"));assertEquals(Null,columns("expected_source_revision"))
  @Test def freshPreparedDomainUsesUnclaimedIdleTargetZero(): Unit =
    val row=preparedDomain(id(1),id(2),pose,id(5),at,budget())
    assertEquals(Null,value(row,Domain,"owner_uuid"));assertEquals(text("WORLD_IDLE"),value(row,Domain,"operating_mode"))
    assertEquals(signed(0),value(row,Domain,"state_version"));assertEquals(signed(0),value(row,Domain,"is_active"))
    assertEquals(MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(-0.0)),value(row,Domain,"pitch"))
  @Test def preparedIntentHasEveryMandatoryFieldAndExplicitNullTopology(): Unit =
    val row=intent(budget())
    val shape=MySqlFormat3RelationShapes.forTable(Intent)
    assertEquals(shape.columns.size,row.size)
    shape.columns.zip(row).foreach { (column,cell) => if !column.nullable then assertNotEquals(Null,cell,column.name) }
    assertEquals(text("PREPARED"),value(row,Intent,"phase"));assertEquals(uuid(id(6)),value(row,Intent,"create_operation_uuid"))
    Vector("source_kind","source_identity","source_identity_sha256","catalog_generation","source_unit_uuid","source_forest_uuid",
      "activation_operation_uuid","committed_domain_version","committed_at","cleanup_operation_uuid").foreach(name => assertEquals(Null,value(row,Intent,name),name))
  @Test def emptyOrPartialTypedRowNeverGetsDefaultSuccess(): Unit =
    assertThrows(classOf[SQLDataException],() => row(Intent,Map.empty,budget()))
    assertThrows(classOf[SQLDataException],() => row(Intent,Map("binding_uuid"->uuid(id(1))),budget()))
  @Test def exactFootprintAnd25ChunksPreserveOriginalIdentity(): Unit =
    val root=budget();val footprint=preparedFootprint(id(1),id(2),id(3),id(4),pose,id(5),root)
    val chunks=preparedChunks(id(1),id(2),id(3),id(4),pose,root)
    assertEquals(text("SEALED"),value(footprint,Footprint,"seal_state"));assertEquals(25,chunks.size)
    assertEquals(25,chunks.map(r => (value(r,Chunk,"dimension_id"),value(r,Chunk,"chunk_x"),value(r,Chunk,"chunk_z"))).distinct.size)
    chunks.foreach(r => assertEquals(uuid(id(4)),value(r,Chunk,"footprint_uuid")))
  @Test def activationPreservesOriginalOwnerAndTargetWhileVersionChangesOnce(): Unit =
    val root=budget();val before=preparedDomain(id(1),id(2),pose,id(5),at,root)
    val after=activatedDomain(before,id(7),at.plusSeconds(1),root)
    assertEquals(signed(1),value(after,Domain,"state_version"));assertEquals(signed(1),value(after,Domain,"is_active"))
    Vector("owner_uuid","operating_mode","world_name","pos_x","pos_y","pos_z","yaw","pitch","dimension_id").foreach(name => assertEquals(value(before,Domain,name),value(after,Domain,name)))
  @Test def activatedIntentContainsAllTenCommittedFields(): Unit =
    val root=budget();val domain=activatedDomain(preparedDomain(id(1),id(2),pose,id(5),at,root),id(7),at.plusSeconds(1),root)
    val row=activatedIntent(intent(root),domain,id(7),root)
    Vector("activation_operation_uuid","committed_domain_version","committed_at","committed_world_name","committed_pos_x",
      "committed_pos_y","committed_pos_z","committed_yaw","committed_pitch","committed_dimension_id").foreach(name => assertNotEquals(Null,value(row,Intent,name),name))
    assertEquals(text("ACTIVATED"),value(row,Intent,"phase"));assertEquals(uuid(id(5)),value(row,Intent,"prepare_operation_uuid"));assertEquals(uuid(id(6)),value(row,Intent,"create_operation_uuid"))
  @Test def alreadyActiveCannotBeActivatedAgain(): Unit =
    val root=budget();val first=activatedDomain(preparedDomain(id(1),id(2),pose,id(5),at,root),id(7),at,root)
    assertThrows(classOf[SQLDataException],() => activatedDomain(first,id(8),at,root))
  @Test def locatorOriginalAttemptAndFootprintStayFixed(): Unit =
    val root=budget();val before=preparedLocator(id(1),id(2),id(3),id(4),0,id(5),root);val after=activatedLocator(before,id(7),root)
    assertEquals(signed(1),value(after,Locator,"locator_revision"));assertEquals(text("ROW_ACTIVE"),value(after,Locator,"locator_state"))
    Vector("binding_uuid","pod_uuid","current_attempt_uuid","current_footprint_uuid","target_dimension_id").foreach(name => assertEquals(value(before,Locator,name),value(after,Locator,name)))
  @Test def oneByteCapRefusesAtActualProducerBoundary(): Unit =
    val root=budget(1L)
    assertThrows(classOf[SQLDataException],() => expectedOperationColumns(false,root))
    assertEquals(0L,root.accountedMemoryBytes)
  @Test def malformedZeroUuidOrUnknownSealFailExplicitly(): Unit =
    assertThrows(classOf[SQLDataException],() => preparedDomain(id(0),id(2),pose,id(5),at,budget()))
    assertThrows(classOf[SQLDataException],() => preparedFootprint(id(1),id(2),id(3),id(4),pose,id(5),budget(),"BUILDING"))

  @Test def wrongTypedDomainCellsAreRejectedAtProductionUpdate(): Unit =
    val root=budget();val original=preparedDomain(id(1),id(2),pose,id(5),at,root)
    assertThrows(classOf[SQLDataException],()=>update(original,Domain,Map("state_version"->text("0")),root))
    assertThrows(classOf[SQLDataException],()=>update(original,Domain,Map("owner_uuid"->text("unclaimed")),root))
    assertThrows(classOf[SQLDataException],()=>update(original,Domain,Map("world_name"->Null),root))
  @Test def nonfiniteOrUnrepresentableDomainValuesCannotBecomeCommitTarget(): Unit =
    val root=budget();val original=preparedDomain(id(1),id(2),pose,id(5),at,root)
    assertThrows(classOf[SQLDataException],()=>update(original,Domain,
      Map("pos_x"->MySqlFormat3Cell.Floating(java.lang.Double.doubleToRawLongBits(Double.NaN))),root))
    assertThrows(classOf[SQLDataException],()=>update(original,Domain,Map("dimension_id"->signed(Long.MaxValue)),root))
