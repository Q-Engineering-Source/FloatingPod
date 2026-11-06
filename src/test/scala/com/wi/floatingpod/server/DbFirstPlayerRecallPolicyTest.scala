package com.wi.floatingpod.server

import com.wi.floatingpod.network.RecallFailureReason
import com.wi.floatingpod.domain.{FloatingPodState,RecallRefusalReason}
import java.util.UUID
import com.wi.floatingpod.storage.mysql.{MySqlRequestOwnershipPreview, MySqlRequestReadDecision}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class DbFirstPlayerRecallPolicyTest:
  private val actor=UUID.randomUUID()
  private val pod=UUID.randomUUID()
  private val current=FloatingPodState(pod,Some(actor),7L,"world",0D,64D,0D,0F,0F,true,0L,Some(3))
  private def after(state:Option[FloatingPodState],ready:Boolean=true,mayBegin:Boolean=true,dimension:Option[Int]=Some(3)) =
    DbFirstPlayerRecallPolicy.afterJournal(actor,pod,7L,state,ready,mayBegin,dimension)

  @Test def durableJournalDoesNotHideCurrentOwnerVersionOrActiveChanges():Unit =
    assertEquals(Some(RecallFailureReason.UnknownPod),after(None))
    assertEquals(Some(RecallFailureReason.NotOwner),after(Some(current.copy(ownerUuid=Some(UUID.randomUUID())))))
    assertEquals(Some(RecallFailureReason.VersionMismatch),after(Some(current.copy(stateVersion=8L))))
    assertEquals(Some(RecallFailureReason.NotActive),after(Some(current.copy(isActive=false))))

  @Test def stopAndActualPublicationConflictRefuseBeforeCaptureTransfer():Unit =
    assertEquals(Some(RecallFailureReason.Busy),after(Some(current),ready=false))
    assertEquals(Some(RecallFailureReason.Busy),after(Some(current),mayBegin=false))
    assertTrue(after(Some(current)).isEmpty)

  @Test def connectedDimensionChangeRefusesWhileDisconnectedOriginalActorRemainsFixed():Unit =
    assertEquals(Some(RecallFailureReason.WrongDimension),after(Some(current),dimension=Some(4)))
    assertTrue(after(Some(current),dimension=None).isEmpty)
    assertEquals(Some(RecallFailureReason.NotOwner),after(Some(current.copy(ownerUuid=Some(UUID.randomUUID()))),dimension=None))

  @Test def foreignCurrentProjectionCannotBeUsedForOriginalJournalDecision():Unit =
    assertThrows(classOf[IllegalArgumentException],()=>after(Some(current.copy(uuid=UUID.randomUUID()))))
    assertThrows(classOf[IllegalArgumentException],()=>after(null))
  @Test def bootOriginalStateDecisionDoesNotInventReadyOrConnectedDimension():Unit =
    def boot(state:Option[FloatingPodState])=
      DbFirstPlayerRecallPolicy.originalStateRefusal(actor,pod,7L,state,None)
    assertTrue(boot(Some(current)).isEmpty,"Recovering is not by itself a Busy business refusal")
    assertEquals(Some(RecallFailureReason.UnknownPod),boot(None))
    assertEquals(Some(RecallFailureReason.NotOwner),boot(Some(current.copy(ownerUuid=Some(UUID.randomUUID())))))
    assertEquals(Some(RecallFailureReason.VersionMismatch),boot(Some(current.copy(stateVersion=8L))))
    assertEquals(Some(RecallFailureReason.NotActive),boot(Some(current.copy(isActive=false))))
    assertThrows(classOf[IllegalArgumentException],()=>boot(null))
    assertThrows(classOf[IllegalArgumentException],()=>boot(Some(current.copy(uuid=UUID.randomUUID()))))
    assertEquals(Some(RecallFailureReason.Busy),after(Some(current),ready=false),"live player Ready policy remains strict")

  private def refusal(ownership: MySqlRequestOwnershipPreview, count: Int = 0, limit: Int = 0) =
    DbFirstPlayerRecallPolicy.refusal(MySqlRequestReadDecision(ownership, count, limit))

  @Test def exactOwnershipRejectionsKeepProtocolReasons(): Unit =
    assertEquals(Some(RecallFailureReason.UnknownPod), refusal(MySqlRequestOwnershipPreview.Missing))
    assertEquals(Some(RecallFailureReason.NotOwner), refusal(MySqlRequestOwnershipPreview.NotOwner))
    assertEquals(Some(RecallFailureReason.VersionMismatch), refusal(MySqlRequestOwnershipPreview.VersionMismatch(9L)))

  @Test def recalledStateCannotBeRecalledAgain(): Unit =
    assertEquals(Some(RecallFailureReason.NotActive), refusal(MySqlRequestOwnershipPreview.Matched(9L, false)))

  @Test def recalledCapacityDoesNotGateRecallOfActualOwnedActiveState(): Unit =
    assertTrue(refusal(MySqlRequestOwnershipPreview.Matched(9L, true), 256, 0).isEmpty)

  @Test def missingDecisionIsNeverAuthority(): Unit =
    assertThrows(classOf[IllegalArgumentException], () => DbFirstPlayerRecallPolicy.refusal(null))

  @Test def durablePreCaptureReasonMappingIsExplicitAndNeverSerializesInfrastructureCause():Unit =
    val expected=Vector(RecallFailureReason.NotOwner->RecallRefusalReason.NotOwner,
      RecallFailureReason.VersionMismatch->RecallRefusalReason.VersionMismatch,
      RecallFailureReason.NotActive->RecallRefusalReason.NotActive,
      RecallFailureReason.WrongDimension->RecallRefusalReason.WrongDimension,
      RecallFailureReason.UnknownPod->RecallRefusalReason.UnknownPod,RecallFailureReason.Busy->RecallRefusalReason.Busy)
    expected.foreach((wire,durable)=>assertEquals(durable,DbFirstPlayerRecallPolicy.durableReason(wire)))
    assertThrows(classOf[IllegalArgumentException],()=>DbFirstPlayerRecallPolicy.durableReason(RecallFailureReason.InfrastructureFailure))
    assertThrows(classOf[IllegalArgumentException],()=>DbFirstPlayerRecallPolicy.durableReason(null))

  @Test def recoveredDurableReasonMapsBackToExactOriginalNetworkResult():Unit =
    val pairs=Vector(RecallRefusalReason.NotOwner->RecallFailureReason.NotOwner,
      RecallRefusalReason.VersionMismatch->RecallFailureReason.VersionMismatch,
      RecallRefusalReason.NotActive->RecallFailureReason.NotActive,
      RecallRefusalReason.WrongDimension->RecallFailureReason.WrongDimension,
      RecallRefusalReason.UnknownPod->RecallFailureReason.UnknownPod,RecallRefusalReason.Busy->RecallFailureReason.Busy)
    pairs.foreach((durable,wire)=>assertEquals(wire,DbFirstPlayerRecallPolicy.networkReason(durable)))
    assertThrows(classOf[IllegalArgumentException],()=>DbFirstPlayerRecallPolicy.networkReason(null))
