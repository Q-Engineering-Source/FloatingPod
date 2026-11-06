package com.wi.floatingpod.server

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertInstanceOf, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import com.wi.floatingpod.server.io.IoOperationTag
import scala.collection.mutable

class ForgePodRemovalObserverTest:
  private val uuid = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff")
  private val operation = new UUID(1L, 2L)
  private val boot = new UUID(3L, 4L)
  private val attempt = new UUID(5L, 6L)
  private val sources = Vector("loaded", "unloaded", "chunk", "id", "uuid", "tracker-map", "tracker-set")

  @Test
  def ioAcknowledgementRequiresBoundOperationRequestAndLaterActualAbsence(): Unit =
    val view = new MutableWorld()
    val observer = new ExactBodyRemovalObserver(view)
    val body = view.body()
    val tag = IoOperationTag(boot, 17L, 23L, operation)
    val pending = observer.captureForIo(body, tag, attempt, 41L, 10L)
    assertTrue(observer.acknowledgeForIo(pending, 11L).isEmpty)
    observer.request(pending, 20L)
    assertTrue(observer.acknowledgeForIo(pending, 20L).isEmpty)
    view.put("tracker-map", body)
    assertTrue(observer.acknowledgeForIo(pending, 21L).isEmpty)
    view.clearIndexes()
    val proof = observer.acknowledgeForIo(pending, 22L).get
    assertEquals(boot, proof.bootId())
    assertEquals(17L, proof.bootGeneration())
    assertEquals(23L, proof.connectionGeneration())
    assertEquals(operation, proof.operationId())
    assertEquals(attempt, proof.attemptId())
    assertEquals(uuid, proof.podId())
    assertEquals(41L, proof.expectedStateVersion())
    assertEquals(0, proof.dimensionId())
    assertEquals(42, proof.entityId())
    assertEquals(22L, proof.observedEndOrdinal())
    assertSame(proof, observer.acknowledgeForIo(pending, 23L).get)
    assertEquals(1, view.requests.size)

  @Test
  def ordinaryAcknowledgedStatusDoesNotUpgradeUnboundPendingToIoAuthority(): Unit =
    val view = new MutableWorld()
    val observer = new ExactBodyRemovalObserver(view)
    val pending = capture(observer, view.body())
    observer.request(pending, 10L)
    assertInstanceOf(classOf[ExactRemovalStatus.Acknowledged], observer.observeEnd(pending, 11L))
    assertThrows(classOf[IllegalStateException], () => { observer.acknowledgeForIo(pending, 11L); () })

  @Test
  def ioIssuanceRechecksForeignIdentityAndOriginalObservationFailure(): Unit =
    val view = new MutableWorld()
    val observer = new ExactBodyRemovalObserver(view)
    val pending = observer.captureForIo(view.body(), IoOperationTag(boot, 1L, 2L, operation), attempt, 3L, 10L)
    observer.request(pending, 10L)
    val primary = new IllegalStateException("actual snapshot unavailable")
    view.snapshotFailure = primary
    assertSame(primary, assertThrows(classOf[IllegalStateException], () => { observer.acknowledgeForIo(pending, 11L); () }))
    view.snapshotFailure = null
    val proof = observer.acknowledgeForIo(pending, 12L).get
    view.put("uuid", view.body())
    assertTrue(observer.acknowledgeForIo(pending, 13L).isEmpty)
    view.clearIndexes()
    assertTrue(observer.acknowledgeForIo(pending, 14L).isEmpty)
    assertEquals(12L, proof.observedEndOrdinal())

  @Test
  def ioIssuanceRejectsAnotherObserverAndOffMainThread(): Unit =
    val view = new MutableWorld()
    val observer = new ExactBodyRemovalObserver(view)
    val pending = observer.captureForIo(view.body(), IoOperationTag(boot, 1L, 2L, operation), attempt, 3L, 10L)
    observer.request(pending, 10L)
    val other = new ExactBodyRemovalObserver(view)
    assertThrows(classOf[IllegalArgumentException], () => { other.acknowledgeForIo(pending, 11L); () })
    val failure = new AtomicReference[Throwable]()
    val thread = new Thread(() =>
      try { observer.acknowledgeForIo(pending, 11L); () }
      catch case caught: Throwable => failure.set(caught)
    )
    thread.start()
    thread.join()
    assertInstanceOf(classOf[IllegalStateException], failure.get())
    assertTrue(pending.completion.isEmpty)

  @Test
  def removeReturnAndDeadFlagDoNotAcknowledgeAndRequestIsSingle(): Unit =
    val view = new MutableWorld()
    val body = view.body()
    view.loaded += body
    val observer = new ExactBodyRemovalObserver(view)
    val pending = capture(observer, body)
    assertInstanceOf(classOf[ExactRemovalStatus.Pending], observer.request(pending, 10L))
    assertTrue(body.dead)
    assertEquals(1, view.requests.size)
    observer.request(pending, 10L)
    assertEquals(1, view.requests.size)
    assertInstanceOf(classOf[ExactRemovalStatus.Pending], observer.observeEnd(pending, 10L))
    assertInstanceOf(classOf[ExactRemovalStatus.Pending], observer.observeEnd(pending, 11L))
    view.clearIndexes()
    val ack = assertInstanceOf(classOf[ExactRemovalStatus.Acknowledged], observer.observeEnd(pending, 12L))
    assertSame(body, ack.identity.body)
    assertSame(view.world, ack.identity.world)
    assertEquals(uuid, ack.identity.podUuid)
    assertEquals(0, ack.identity.dimensionId)
    assertEquals(operation, ack.identity.operationUuid)
    assertEquals(boot, ack.identity.bootUuid)
    assertEquals(attempt, ack.identity.attemptUuid)
    assertEquals(12L, ack.observedEndOrdinal)

  @Test
  def everyActualIndexRemnantIndependentlyRetainsObligation(): Unit =
    sources.foreach { source =>
      val view = new MutableWorld()
      val body = view.body()
      val observer = new ExactBodyRemovalObserver(view)
      val pending = capture(observer, body)
      observer.request(pending, 10L)
      view.put(source, body)
      assertInstanceOf(classOf[ExactRemovalStatus.Pending], observer.observeEnd(pending, 11L))
      view.clearIndexes()
      assertInstanceOf(classOf[ExactRemovalStatus.Acknowledged], observer.observeEnd(pending, 12L))
      assertEquals(1, view.requests.size)
    }

  @Test
  def foreignReplacementInEachIndexRejectsRequestWithoutRemovingForeign(): Unit =
    sources.foreach { source =>
      val view = new MutableWorld()
      val body = view.body()
      val foreign = view.body()
      val observer = new ExactBodyRemovalObserver(view)
      val pending = capture(observer, body)
      view.put(source, foreign)
      assertInstanceOf(classOf[ExactRemovalStatus.IdentityConflict], observer.request(pending, 10L))
      assertEquals(0, view.requests.size)
      assertFalse(foreign.dead)
      view.clearIndexes()
      assertInstanceOf(classOf[ExactRemovalStatus.IdentityConflict], observer.observeEnd(pending, 11L))
    }

  @Test
  def foreignTrackerCapturedSlotIsConflictEvenWhenBodyFieldsUseDifferentKeys(): Unit =
    val view = new MutableWorld()
    val body = view.body()
    val foreign = new Body(view.world, 999, new UUID(99L, 99L))
    val observer = new ExactBodyRemovalObserver(view)
    val pending = capture(observer, body)
    observer.request(pending, 10L)
    view.trackerMap(42) = new Entry(foreign)
    assertInstanceOf(classOf[ExactRemovalStatus.IdentityConflict], observer.observeEnd(pending, 11L))
    assertEquals(1, view.requests.size)
    assertFalse(foreign.dead)

  @Test
  def foreignSameUuidWithDifferentIdAndSameIdWithDifferentUuidBothConflict(): Unit =
    Vector((999, uuid), (42, new UUID(77L, 88L))).foreach { (id, podUuid) =>
      val view = new MutableWorld()
      val body = view.body()
      val foreign = new Body(view.world, id, podUuid)
      val observer = new ExactBodyRemovalObserver(view)
      val pending = capture(observer, body)
      observer.request(pending, 10L)
      view.trackerSet += new Entry(foreign)
      assertInstanceOf(classOf[ExactRemovalStatus.IdentityConflict], observer.observeEnd(pending, 11L))
    }

  @Test
  def exactBodyIdUuidOrWorldMutationIsConflict(): Unit =
    Vector[Body => Unit](_.id = 43, _.podUuid = new UUID(1L, 1L), _.world = new Object()).foreach { mutate =>
      val view = new MutableWorld()
      val body = view.body()
      val observer = new ExactBodyRemovalObserver(view)
      val pending = capture(observer, body)
      observer.request(pending, 10L)
      mutate(body)
      assertInstanceOf(classOf[ExactRemovalStatus.IdentityConflict], observer.observeEnd(pending, 11L))
    }

  @Test
  def unloadedCapturedWorldCannotAcknowledgeAbsence(): Unit =
    val view = new MutableWorld()
    val observer = new ExactBodyRemovalObserver(view)
    val pending = capture(observer, view.body())
    observer.request(pending, 10L)
    view.registered.clear()
    assertInstanceOf(classOf[ExactRemovalStatus.IdentityConflict], observer.observeEnd(pending, 11L))

  @Test
  def originalRequestFailureIsRetainedAndNeverRetried(): Unit =
    val view = new MutableWorld()
    val body = view.body()
    view.loaded += body
    val primary = new IllegalStateException("request failed", new IllegalArgumentException("cause"))
    view.removeFailure = primary
    val observer = new ExactBodyRemovalObserver(view)
    val pending = capture(observer, body)
    val thrown = assertThrows(classOf[IllegalStateException], () => observer.request(pending, 10L))
    assertSame(primary, thrown)
    assertSame(primary, pending.requestFailure.get)
    observer.request(pending, 10L)
    assertEquals(1, view.requests.size)
    assertInstanceOf(classOf[ExactRemovalStatus.Pending], observer.observeEnd(pending, 11L))
    view.clearIndexes()
    assertInstanceOf(classOf[ExactRemovalStatus.Acknowledged], observer.observeEnd(pending, 12L))
    assertSame(primary, pending.requestFailure.get)

  @Test
  def detachedOldTrackerEntryPlayersDoNotBlockServerAbsenceAck(): Unit =
    val view = new MutableWorld()
    val body = view.body()
    val oldEntry = new Entry(body)
    oldEntry.players += new Object()
    view.trackerSet += oldEntry
    view.trackerMap(42) = oldEntry
    val observer = new ExactBodyRemovalObserver(view)
    val pending = capture(observer, body)
    observer.request(pending, 10L)
    assertInstanceOf(classOf[ExactRemovalStatus.Pending], observer.observeEnd(pending, 11L))
    view.clearIndexes()
    assertTrue(oldEntry.players.nonEmpty)
    assertInstanceOf(classOf[ExactRemovalStatus.Acknowledged], observer.observeEnd(pending, 12L))

  @Test
  def missingRequestCannotAcknowledgeEvenWhenAllIndexesEmpty(): Unit =
    val view = new MutableWorld()
    val observer = new ExactBodyRemovalObserver(view)
    val pending = capture(observer, view.body())
    assertInstanceOf(classOf[ExactRemovalStatus.Pending], observer.observeEnd(pending, 11L))
    assertEquals(0, view.requests.size)

  @Test
  def offOwnerThreadFailsBeforeAnyRequest(): Unit =
    val view = new MutableWorld()
    val observer = new ExactBodyRemovalObserver(view)
    val pending = capture(observer, view.body())
    val failure = new AtomicReference[Throwable]()
    val thread = new Thread(() =>
      try
        observer.request(pending, 10L)
        ()
      catch case caught: Throwable => failure.set(caught)
    )
    thread.start()
    thread.join()
    assertInstanceOf(classOf[IllegalStateException], failure.get())
    assertEquals(0, view.requests.size)

  @Test
  def pendingCannotBeTransferredToAnotherObserver(): Unit =
    val view = new MutableWorld()
    val first = new ExactBodyRemovalObserver(view)
    val second = new ExactBodyRemovalObserver(view)
    val pending = capture(first, view.body())
    assertThrows(classOf[IllegalArgumentException], () => second.request(pending, 10L))
    assertEquals(0, view.requests.size)

  @Test
  def capturedWorldDimensionMutationCannotAcknowledge(): Unit =
    val view = new MutableWorld()
    val observer = new ExactBodyRemovalObserver(view)
    val pending = capture(observer, view.body())
    observer.request(pending, 10L)
    view.currentDimensionId = 1
    assertInstanceOf(classOf[ExactRemovalStatus.IdentityConflict], observer.observeEnd(pending, 11L))

  @Test
  def observationFailurePropagatesOriginalCauseInsteadOfAcknowledging(): Unit =
    val view = new MutableWorld()
    val observer = new ExactBodyRemovalObserver(view)
    val pending = capture(observer, view.body())
    observer.request(pending, 10L)
    val primary = new IllegalStateException("snapshot failed", new IllegalArgumentException("cause"))
    view.snapshotFailure = primary
    val thrown = assertThrows(classOf[IllegalStateException], () => observer.observeEnd(pending, 11L))
    assertSame(primary, thrown)
    assertEquals(1, view.requests.size)
    view.snapshotFailure = null
    assertInstanceOf(classOf[ExactRemovalStatus.Acknowledged], observer.observeEnd(pending, 12L))

  @Test
  def laterRequestUsesItsOwnEndClockAndCannotAckCurrentEnd(): Unit =
    val view = new MutableWorld()
    val observer = new ExactBodyRemovalObserver(view)
    val pending = capture(observer, view.body())
    observer.request(pending, 20L)
    assertInstanceOf(classOf[ExactRemovalStatus.Pending], observer.observeEnd(pending, 20L))
    assertInstanceOf(classOf[ExactRemovalStatus.Acknowledged], observer.observeEnd(pending, 21L))
    assertEquals(1, view.requests.size)

  private def capture(observer: ExactBodyRemovalObserver, body: Body): RemovalPending =
    observer.capture(body, operation, boot, attempt, 10L)

  private final class Body(var world: AnyRef, var id: Int, var podUuid: UUID):
    var dead = false
  private final class Entry(val body: Body):
    val players = mutable.Set.empty[AnyRef]

  /** 与生产 seam 相同的完整实体/索引视图；ACK 由共享 observer 从这些真实集合计算。 */
  private final class MutableWorld extends RemovalWorldView:
    val world: AnyRef = new Object()
    private val ownerThread = Thread.currentThread()
    var currentDimensionId = 0
    val registered = mutable.ArrayBuffer[AnyRef](world)
    val loaded = mutable.ArrayBuffer.empty[Body]
    val unloaded = mutable.ArrayBuffer.empty[Body]
    val chunks = mutable.ArrayBuffer.empty[Body]
    val idMap = mutable.Map.empty[Int, Body]
    val uuidMap = mutable.Map.empty[UUID, Body]
    val trackerMap = mutable.Map.empty[Int, Entry]
    val trackerSet = mutable.ArrayBuffer.empty[Entry]
    val requests = mutable.ArrayBuffer.empty[Body]
    var removeFailure: Throwable = null
    var snapshotFailure: Throwable = null
    def body(): Body = new Body(world, 42, uuid)
    override def worldRef: AnyRef = world
    override def dimensionId: Int = currentDimensionId
    override def assertMain(): Unit =
      if Thread.currentThread() ne ownerThread then throw new IllegalStateException("not owner Main")
    override def describeBody(body: AnyRef): RemovalEntityView =
      val entity = body.asInstanceOf[Body]
      RemovalEntityView(entity, entity.world, entity.id, entity.podUuid)
    override def requestRemove(body: AnyRef): Unit =
      val entity = body.asInstanceOf[Body]
      requests += entity
      entity.dead = true
      if removeFailure != null then throw removeFailure
    override def snapshot(body: AnyRef, entityId: Int, podUuid: UUID): RemovalWorldSnapshot =
      if snapshotFailure != null then throw snapshotFailure
      val slots = loaded.toVector.map(e => RemovalIndexSlot("loaded", describeBody(e))) ++
        unloaded.toVector.map(e => RemovalIndexSlot("unloaded", describeBody(e))) ++
        chunks.toVector.map(e => RemovalIndexSlot("chunk", describeBody(e))) ++
        idMap.get(entityId).toVector.map(e => RemovalIndexSlot("id", describeBody(e), idKey = Some(entityId))) ++
        uuidMap.get(podUuid).toVector.map(e => RemovalIndexSlot("uuid", describeBody(e), uuidKey = Some(podUuid))) ++
        trackerMap.get(entityId).toVector.map(e => RemovalIndexSlot("tracker-map", describeBody(e.body), idKey = Some(entityId))) ++
        trackerSet.toVector.map(e => RemovalIndexSlot("tracker-set", describeBody(e.body)))
      RemovalWorldSnapshot(currentDimensionId, registered.toVector, describeBody(body), slots)
    def put(source: String, body: Body): Unit = source match
      case "loaded" => loaded += body
      case "unloaded" => unloaded += body
      case "chunk" => chunks += body
      case "id" => idMap(42) = body
      case "uuid" => uuidMap(uuid) = body
      case "tracker-map" => trackerMap(42) = new Entry(body)
      case "tracker-set" => trackerSet += new Entry(body)
      case _ => throw new IllegalArgumentException("unknown test index")
    def clearIndexes(): Unit =
      loaded.clear()
      unloaded.clear()
      chunks.clear()
      idMap.clear()
      uuidMap.clear()
      trackerMap.clear()
      trackerSet.clear()
