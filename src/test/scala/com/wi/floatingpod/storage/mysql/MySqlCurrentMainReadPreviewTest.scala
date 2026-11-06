package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.PlayerEntitlement
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class MySqlCurrentMainReadPreviewTest:
  @Test def currentMainObservationPreservesOwnershipVersionAndEntitlementSemantics():Unit =
    val state=MySqlPrepareModeGraphFixture.activeOwned._2
    val actor=state.ownerUuid.get
    def read(version:Long)=MySqlRequestReadPreview.decideCurrent(state.uuid,actor,version,Some(state),None,1)
    assertEquals(MySqlRequestOwnershipPreview.Matched(state.stateVersion,true),read(state.stateVersion).ownership)
    assertEquals(PlayerEntitlement.DefaultSimultaneousActiveLimit,read(state.stateVersion).simultaneousLimit)
    assertFalse(read(state.stateVersion).belowEntitlementLimit)
    assertEquals(MySqlRequestOwnershipPreview.VersionMismatch(state.stateVersion),read(state.stateVersion+1L).ownership)
    assertEquals(MySqlRequestOwnershipPreview.NotOwner,
      MySqlRequestReadPreview.decideCurrent(state.uuid,UUID.randomUUID(),state.stateVersion,Some(state),None,0).ownership)
    val increased=MySqlRequestReadPreview.decideCurrent(state.uuid,actor,state.stateVersion,Some(state),
      Some(PlayerEntitlement(actor,3,2)),1)
    assertTrue(increased.belowEntitlementLimit)
    assertEquals(MySqlRequestOwnershipPreview.Missing,
      MySqlRequestReadPreview.decideCurrent(state.uuid,actor,0L,None,None,0).ownership)
  @Test def foreignOrInvalidCurrentInputsAreRejectedInsteadOfScalarSuccess():Unit =
    val state=MySqlPrepareModeGraphFixture.activeOwned._2
    val actor=state.ownerUuid.get
    assertThrows(classOf[IllegalArgumentException],()=>MySqlRequestReadPreview.decideCurrent(
      UUID.randomUUID(),actor,state.stateVersion,Some(state),None,0))
    assertThrows(classOf[IllegalArgumentException],()=>MySqlRequestReadPreview.decideCurrent(
      state.uuid,actor,state.stateVersion,Some(state),Some(PlayerEntitlement(UUID.randomUUID())),0))
    assertThrows(classOf[IllegalArgumentException],()=>MySqlRequestReadPreview.decideCurrent(
      state.uuid,actor,state.stateVersion,Some(state),None,-1))
