package com.wi.floatingpod.server

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.concurrent.atomic.AtomicReference

/** 实际词法读准入/排空；不模拟SQL、Main DATA或游戏Ready。 */
class MainDbFirstReadAdmissionTest:
  @Test def scopeObservationRequiresSealAndActualNestedFramesToReturn():Unit =
    val reads=new MainDbFirstReadAdmission(Thread.currentThread())
    assertThrows(classOf[IllegalArgumentException],()=>reads.preStopReadScopesPending)
    assertThrows(classOf[IllegalArgumentException],()=>reads.preStopReadFailureAfterScopesEnded)
    assertThrows(classOf[MainDbFirstReadAdmission.Sealed],()=>reads.withRead {
      reads.withRead {
        reads.sealNewRequestsForPreStop()
        assertTrue(reads.preStopReadScopesPending)
        assertThrows(classOf[IllegalArgumentException],()=>reads.preStopReadFailureAfterScopesEnded)
      }
    })
    assertFalse(reads.preStopReadScopesPending)
    assertTrue(reads.preStopReadFailureAfterScopesEnded.isEmpty)
    reads.requirePreStopReadDrainSettled()

  @Test def failedReadScopesEndButOriginalFailureNeverBecomesHealthyDrain():Unit =
    val reads=new MainDbFirstReadAdmission(Thread.currentThread())
    val cause=new java.io.IOException("actual failed lexical DATA read")
    assertSame(cause,assertThrows(classOf[java.io.IOException],()=>reads.withRead(throw cause)))
    reads.sealNewRequestsForPreStop()
    assertFalse(reads.preStopReadScopesPending)
    assertSame(cause,reads.preStopReadFailureAfterScopesEnded.get)
    assertSame(cause,assertThrows(classOf[java.io.IOException],()=>reads.preStopReadDrainPending))
    assertSame(cause,assertThrows(classOf[java.io.IOException],()=>reads.requirePreStopReadDrainSettled()))
    assertSame(cause,assertThrows(classOf[java.io.IOException],()=>reads.withRead("must not deliver")))

  @Test def originalRecallDrainCanRetainAckBeforeReadFailureRejectsStopFreeze():Unit =
    val reads=new MainDbFirstReadAdmission(Thread.currentThread())
    val cause=new java.io.IOException("actual original read failure")
    assertSame(cause,assertThrows(classOf[java.io.IOException],()=>reads.withRead(throw cause)))
    reads.sealNewRequestsForPreStop()
    val stop=new com.wi.floatingpod.network.DbFirstPreStopRecallDrain
    var pending=true
    var originalEnds=0
    var freezes=0
    var sourceSteps=0
    def end(n:Long):Boolean =
      assertFalse(reads.preStopReadScopesPending)
      val retained=reads.preStopReadFailureAfterScopesEnded
      stop.onEnd(n) { originalEnds+=1 } { !pending } {
        retained.foreach(throw _)
        reads.requirePreStopReadDrainSettled()
        freezes+=1
      } { sourceSteps+=1;true }
    assertFalse(end(1L))
    assertFalse(end(2L))
    pending=false // 仅调度输入，不伪造SQL/native或worker退休ACK。
    assertFalse(end(3L))
    assertEquals(3,originalEnds)
    assertSame(cause,assertThrows(classOf[java.io.IOException],()=>end(4L)))
    assertSame(cause,assertThrows(classOf[java.io.IOException],()=>end(5L)))
    assertEquals(3,originalEnds)
    assertEquals(0,freezes)
    assertEquals(0,sourceSteps)
    assertSame(cause,reads.preStopReadFailureAfterScopesEnded.get)

  @Test def fatalReadIsNotDeferredAsDiagnosticScopeResult():Unit =
    val reads=new MainDbFirstReadAdmission(Thread.currentThread())
    val fatal=new LinkageError("actual fatal read")
    assertSame(fatal,assertThrows(classOf[LinkageError],()=>reads.withRead(throw fatal)))
    reads.sealNewRequestsForPreStop()
    assertFalse(reads.preStopReadScopesPending)
    assertSame(fatal,assertThrows(classOf[LinkageError],()=>reads.preStopReadFailureAfterScopesEnded))
    assertSame(fatal,assertThrows(classOf[LinkageError],()=>reads.requirePreStopReadDrainSettled()))

  @Test def actualNestedScopesPreventEarlyDrainAndReturnNoValueAfterSeal():Unit =
    val original=new MainDbFirstReadAdmission(Thread.currentThread())
    assertThrows(classOf[IllegalArgumentException],()=>original.requirePreStopReadDrainSettled())
    var delivered=false
    assertThrows(classOf[MainDbFirstReadAdmission.Sealed],()=>original.withRead {
      original.withRead {
        original.sealNewRequestsForPreStop()
        assertTrue(original.preStopReadDrainPending)
        assertThrows(classOf[IllegalArgumentException],()=>original.requirePreStopReadDrainSettled())
        assertTrue(original.preStopReadScopesPending)
        assertThrows(classOf[IllegalArgumentException],()=>original.preStopReadFailureAfterScopesEnded)
        "scalar"
      }
      delivered=true
    })
    assertFalse(delivered)
    assertFalse(original.preStopReadDrainPending)
    original.requirePreStopReadDrainSettled()
    assertThrows(classOf[MainDbFirstReadAdmission.Sealed],()=>original.withRead(42))
  @Test def successfulReadReturnsOnlyItsValueAndSealedEmptyScopeActuallySettles():Unit =
    val original=new MainDbFirstReadAdmission(Thread.currentThread())
    assertEquals(7,original.withRead(original.withRead(7)))
    original.sealNewRequestsForPreStop()
    original.sealNewRequestsForPreStop()
    assertFalse(original.preStopReadDrainPending)
    original.requirePreStopReadDrainSettled()
  @Test def failedOriginalReadCannotBecomeEmptySuccessfulDrainOrPartialDelivery():Unit =
    val original=new MainDbFirstReadAdmission(Thread.currentThread())
    val first=new IllegalStateException("original current DATA failed")
    val second=new IllegalStateException("outer read failed")
    assertSame(second,assertThrows(classOf[IllegalStateException],()=>original.withRead {
      try original.withRead(throw first) catch case actual:IllegalStateException=>assertSame(first,actual)
      throw second
    }))
    assertEquals(Vector(second),first.getSuppressed.toVector)
    assertSame(first,assertThrows(classOf[IllegalStateException],()=>original.withRead(99)))
    original.sealNewRequestsForPreStop()
    assertSame(first,assertThrows(classOf[IllegalStateException],()=>original.requirePreStopReadDrainSettled()))
  @Test def actualPendingSuccessorTemporarilyRefusesThenReadAndDrainRecover():Unit =
    import com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission
    var confirmations=0
    val policy=new ManagedOrdinarySaveAdmission.OrdinaryProviderPolicy:
      override def decide(provider:net.minecraft.world.gen.ChunkProviderServer,world:net.minecraft.world.WorldServer,
          chunk:net.minecraft.world.chunk.Chunk,operation:ManagedOrdinarySaveAdmission.ProviderOperation):ManagedOrdinarySaveAdmission.ProviderDecision =
        throw new AssertionError("no game provider in pure successor test")
      override def retainTemporary(bytes:Long,purpose:String):AutoCloseable =
        throw new AssertionError("no provider allocation in pure successor test")
      override def requireConfirmedSuccessor():Unit = confirmations+=1
    val saved=new ManagedOrdinarySaveAdmission(Thread.currentThread(),4,policy)
    val reads=new MainDbFirstReadAdmission(Thread.currentThread())
    val mutation=saved.reserveMutation()
    saved.submitted(mutation,true)
    var delivered=false
    assertThrows(classOf[ManagedOrdinarySaveAdmission.MainReadPending],()=>reads.withRead {
      saved.requireMainReadAvailable()
      delivered=true
    })
    assertFalse(delivered)
    saved.resumeConfirmedSuccessor(policy)
    assertEquals(1,confirmations)
    assertEquals("current",reads.withRead { saved.requireMainReadAvailable();"current" })
    reads.sealNewRequestsForPreStop()
    assertFalse(reads.preStopReadDrainPending)
    reads.requirePreStopReadDrainSettled()

  @Test def foreignThreadCannotReadSealOrClaimDrain():Unit =
    val original=new MainDbFirstReadAdmission(Thread.currentThread())
    val error=new AtomicReference[Throwable]()
    val other=Thread.ofPlatform().start(()=>
      try
        assertThrows(classOf[IllegalArgumentException],()=>original.withRead(1))
        assertThrows(classOf[IllegalArgumentException],()=>original.sealNewRequestsForPreStop())
        assertThrows(classOf[IllegalArgumentException],()=>original.requirePreStopReadDrainSettled())
        assertThrows(classOf[IllegalArgumentException],()=>original.preStopReadScopesPending)
        assertThrows(classOf[IllegalArgumentException],()=>original.preStopReadFailureAfterScopesEnded)
      catch case cause:Throwable=>error.set(cause))
    other.join(5000L)
    assertFalse(other.isAlive)
    assertNull(error.get())
    assertEquals(2,original.withRead(2))
