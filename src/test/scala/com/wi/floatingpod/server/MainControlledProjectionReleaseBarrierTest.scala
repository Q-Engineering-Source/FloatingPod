package com.wi.floatingpod.server

import com.wi.floatingpod.server.io.IoOperationTag
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 直接生产双tag DATA收口；不自构 SQL-qualified F3 graph/worker issuer。 */
class MainControlledProjectionReleaseBarrierTest:
  private def tag(n: Long)=IoOperationTag(new UUID(0,1),2L,3L,new UUID(0,n))
  @Test def bothExactAcksRequiredInPrepareThenCommitOrder(): Unit =
    val state=new MainControlledProjectionReleaseBarrier(Thread.currentThread(),tag(4),tag(5))
    assertFalse(state.acknowledge(tag(4)));assertTrue(state.acknowledge(tag(5)))
  @Test def bothExactAcksRequiredInCommitThenPrepareOrder(): Unit =
    val state=new MainControlledProjectionReleaseBarrier(Thread.currentThread(),tag(4),tag(5))
    assertFalse(state.acknowledge(tag(5)));assertTrue(state.acknowledge(tag(4)))
  @Test def unknownAndDuplicateCannotCountAsSecondAck(): Unit =
    val state=new MainControlledProjectionReleaseBarrier(Thread.currentThread(),tag(4),tag(5))
    assertFalse(state.acknowledge(tag(4)))
    assertThrows(classOf[IllegalArgumentException],()=>state.acknowledge(tag(99)))
    assertThrows(classOf[IllegalArgumentException],()=>state.acknowledge(tag(4)))
    assertTrue(state.acknowledge(tag(5)))
  @Test def failureRetainsOriginalAndSecondaryAndForbidsCompaction(): Unit =
    val state=new MainControlledProjectionReleaseBarrier(Thread.currentThread(),tag(4),tag(5))
    val primary=new IllegalStateException("original");val secondary=new IllegalStateException("secondary")
    state.fail(tag(4),primary);state.fail(tag(5),secondary)
    assertSame(secondary,primary.getSuppressed.head)
    assertTrue(primary.getStackTrace.nonEmpty)
    assertThrows(classOf[IllegalArgumentException],()=>state.acknowledge(tag(4)))
    assertThrows(classOf[IllegalArgumentException],()=>state.acknowledge(tag(5)))
  @Test def foreignGenerationCannotConstructOriginalPair(): Unit =
    val foreign=IoOperationTag(tag(5).bootId,tag(5).bootGeneration,4L,tag(5).operationId)
    assertThrows(classOf[IllegalArgumentException],()=>new MainControlledProjectionReleaseBarrier(
      Thread.currentThread(),tag(4),foreign))
