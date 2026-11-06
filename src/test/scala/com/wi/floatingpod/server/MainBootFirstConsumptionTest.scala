package com.wi.floatingpod.server

import com.wi.floatingpod.storage.mysql.MainBootProjectionFixture.*
import com.wi.floatingpod.server.io.IoOperationTag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

/** 直接调用真实 consumer 的原 source/epoch 校验；DATA fixture 不能发行 claimed lease/SQL 权限。 */
class MainBootFirstConsumptionTest:
  @Test def mutationBeforeFirstConsumptionRejectsOldSourceAndKeepsOriginalDebtAndData(): Unit =
    val raw=snapshot(withCommittedOperationReferences(Map("floating_pod_state"->Vector(pod())),
      Set(new java.util.UUID(0x0101010101010101L,0x0101010101010101L))))
    val tag=IoOperationTag(raw.observationScope.bootUuid,1L,1L,id(81))
    val original=stampedBoot(raw,tag,17L)
    val data=original.requireBootReadData
    val charge=accountedMemoryBytes(raw)
    val failure=assertThrows(classOf[IllegalStateException], () =>
      MainBootReadLifetime.fromOriginalSource(Thread.currentThread(),original,tag,18L))
    val stack=failure.getStackTrace
    assertEquals("Boot read input retired by an accepted repository mutation",failure.getMessage)
    assertTrue(stack.nonEmpty)
    assertTrue(stack.sameElements(failure.getStackTrace))
    assertSame(data,original.requireBootReadData)
    assertSame(raw.limitProfile,original.limitProfile)
    assertEquals(17L,original.requireOriginalBootEpoch(tag))
    assertEquals(charge,accountedMemoryBytes(raw))
    assertEquals(20,original.relations.size)
    assertEquals(id(10),data.recoveryRows.head.uuid)
    assertEquals(raw.canonicalSha256Hex,original.canonicalSha256Hex)
  @Test def originalCapturePassesButValueEqualForeignTagCannotAdoptItsEpoch(): Unit =
    val raw=snapshot()
    val tag=IoOperationTag(raw.observationScope.bootUuid,1L,1L,id(82))
    val original=stampedBoot(raw,tag,17L)
    val core=MainBootReadLifetime.fromOriginalSource(Thread.currentThread(),original,tag,17L)
    core.requireCurrent(17L)
    assertEquals(None,core.retainedFailure)
    val copied=tag.copy()
    assertEquals(tag,copied); assertNotSame(tag,copied)
    assertThrows(classOf[IllegalArgumentException], () =>
      MainBootReadLifetime.fromOriginalSource(Thread.currentThread(),original,copied,17L))
    assertEquals(17L,original.requireOriginalBootEpoch(tag))
  @Test def dataOnlyFactoryCannotRemintCurrentEpochForConsumer(): Unit =
    val raw=snapshot()
    val tag=IoOperationTag(raw.observationScope.bootUuid,1L,1L,id(83))
    val dataOnly=unstampedBoot(raw)
    assertThrows(classOf[IllegalArgumentException], () =>
      MainBootReadLifetime.fromOriginalSource(Thread.currentThread(),dataOnly,tag,0L))
    assertSame(raw.limitProfile,dataOnly.limitProfile)
    assertEquals(20,dataOnly.relations.size)
