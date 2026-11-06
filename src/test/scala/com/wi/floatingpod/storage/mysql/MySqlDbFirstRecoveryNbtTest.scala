package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.domain.{FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.entity.{PodArtifactIdentity, PodArtifactNbtCodec}
import java.io.{ByteArrayOutputStream, DataOutputStream, IOException}
import java.nio.ByteBuffer
import java.util.UUID
import net.minecraft.nbt.{CompressedStreamTools, NBTTagCompound, NBTTagList, NBTTagString}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 解码/版本门禁的纯行为证据；不伪造Session、SQL完成、Main transfer或游戏恢复资格。 */
class MySqlDbFirstRecoveryNbtTest:
  @Test def worldDiagnosticMetadataCannotBecomeActiveDatabaseRecovery():Unit =
    val diagnostic=fixtureNbt()
    new com.wi.floatingpod.entity.PodDiagnosticIdentity(new UUID(0,99),pod).write(diagnostic)
    val malformed=fixtureNbt();malformed.setString(com.wi.floatingpod.entity.PodDiagnosticIdentity.ROOT_KEY,"bad")
    for nbt<-Vector(diagnostic,malformed) do
      val fees=new Fees()
      assertThrows(classOf[IllegalArgumentException],()=>consume(Vector(row(encode(nbt))),fees)(_=>fail("diagnostic body gained active recovery")))
      assertEquals(0,fees.held)
  private val binding = new UUID(0, 11)
  private val pod = new UUID(0, 12)
  private val attempt = new UUID(0, 13)
  private val state = FloatingPodState(pod, None, 1, "world", 1, 64, 2, 3, 4, true, 1,
    Some(0), PodOperatingMode.WorldIdle)
  private val profile = SourceLimitProfile.of(1000000, 1000000, 1000000, 64, 10000, 4000000, 1000000, 65536)
  private val table = MySqlDbFirstSchemaDefinition.SnapshotTable
  private val columns = MySqlDbFirstRelationShapes.forTable(table).columns.map(_.name)
  private def id(value: UUID): MySqlOwnedFormat3Value = MySqlOwnedFormat3Value.Binary(
    MySqlOwnedFormat3Binary.takeOwned(ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()))
  private def fixtureNbt(): NBTTagCompound =
    val nbt = new NBTTagCompound()
    nbt.setUniqueId("UUID", pod)
    nbt.setString("id", "floatingpod:floating_pod")
    PodArtifactNbtCodec.write(nbt, new PodArtifactIdentity(binding, attempt, PodArtifactIdentity.Origin.DEBUG_ITEM_CREATE))
    val caps = new NBTTagCompound()
    caps.setByteArray("opaque", Array[Byte](0, 1, -1, 7))
    caps.setString("unknown", "x\u0000雪")
    nbt.setTag("ForgeCaps", caps)
    nbt.setLong("unknown-root", Long.MinValue)
    nbt
  private def encode(nbt: NBTTagCompound): Array[Byte] =
    val bytes = new ByteArrayOutputStream()
    CompressedStreamTools.write(nbt, new DataOutputStream(bytes))
    bytes.toByteArray
  private def row(payload: Array[Byte]): MySqlOwnedFormat3Row =
    val values = Map("binding_uuid" -> id(binding), "pod_uuid" -> id(pod), "attempt_uuid" -> id(attempt),
      "owner_uuid" -> MySqlOwnedFormat3Value.SqlNull,
      "snapshot_format_version" -> MySqlOwnedFormat3Value.SignedInteger(1),
      "captured_state_version" -> MySqlOwnedFormat3Value.SignedInteger(0),
      "committed_state_version" -> MySqlOwnedFormat3Value.SignedInteger(1),
      "nbt_bytes" -> MySqlOwnedFormat3Value.Binary(MySqlOwnedFormat3Binary.takeOwned(payload)))
    MySqlOwnedFormat3Row(columns.map(c => values.getOrElse(c, MySqlOwnedFormat3Value.SqlNull)))
  private def replace(source: MySqlOwnedFormat3Row, name: String, value: MySqlOwnedFormat3Value) =
    source.copy(cells = source.cells.updated(columns.indexOf(name), value))
  private final class Fees:
    var held = 0L
    var reservations = Vector.empty[Long]
    var denyAt = 0
    var closeFailure: Throwable = null
    def retain(bytes: Long, purpose: String): AutoCloseable =
      reservations :+= bytes
      if reservations.size == denyAt then throw new IOException("test allocation denied")
      held = Math.addExact(held, bytes)
      new AutoCloseable:
        private var closed = false
        def close(): Unit =
          assertFalse(closed)
          closed = true
          held -= bytes
          if closeFailure != null then throw closeFailure
  private def consume(rows: Vector[MySqlOwnedFormat3Row], fees: Fees)(use: NBTTagCompound => Unit): Unit =
    MySqlDbFirstRecoveryNbt.consume(MySqlOwnedFormat3Relation(table, rows), state, binding, attempt,
      profile, fees.retain, use)

  @Test def restoresOpaqueCapsAndUnknownFieldsWithAllChargesHeldUntilConsumerReturns(): Unit =
    val original = fixtureNbt()
    val bytes = encode(original)
    val fees = new Fees()
    var called = 0
    consume(Vector(row(bytes)), fees) { decoded =>
      called += 1
      assertEquals(original, decoded)
      assertEquals(3, fees.reservations.size)
      assertTrue(fees.held > bytes.length)
      assertNotSame(original, decoded)
    }
    assertEquals(1, called)
    assertEquals(0, fees.held)

  @Test def rejectsStaleVersionWrongAttemptBindingOwnerAndInactiveState(): Unit =
    val source = row(encode(fixtureNbt()))
    Vector("attempt_uuid" -> id(new UUID(0, 14)), "binding_uuid" -> id(new UUID(0, 14)),
      "pod_uuid" -> id(new UUID(0, 14)), "owner_uuid" -> id(new UUID(0, 14)),
      "snapshot_format_version" -> MySqlOwnedFormat3Value.SignedInteger(2),
      "committed_state_version" -> MySqlOwnedFormat3Value.SignedInteger(0),
      "captured_state_version" -> MySqlOwnedFormat3Value.SignedInteger(1)).foreach { (name, value) =>
      assertThrows(classOf[IllegalArgumentException], () =>
        MySqlDbFirstRecoveryNbt.requireCurrent(replace(source, name, value), state, binding, attempt))
    }
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlDbFirstRecoveryNbt.requireCurrent(source, state.copy(stateVersion = 2), binding, attempt))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlDbFirstRecoveryNbt.requireCurrent(source, state.copy(isActive = false), binding, attempt))

  @Test def noMissingOrDuplicateSnapshotCanInvokeConsumer(): Unit =
    val source = row(encode(fixtureNbt()))
    Vector(Vector.empty, Vector(source, source)).foreach { rows =>
      val fees = new Fees()
      assertThrows(classOf[IllegalArgumentException], () => consume(rows, fees)(_ => fail("unexpected recovery")))
      assertEquals(0, fees.held)
    }

  @Test def malformedInputAndWrongEntityOrArtifactCannotInvokeConsumer(): Unit =
    val wrongPod = fixtureNbt(); wrongPod.setUniqueId("UUID", new UUID(0, 99))
    val wrongArtifact = fixtureNbt()
    wrongArtifact.getCompoundTag(PodArtifactNbtCodec.ROOT_KEY).setString("Attempt", new UUID(0, 99).toString)
    Vector(encode(wrongPod), encode(wrongArtifact), Array[Byte](10, 0, 0, 7, 0, 0, 127, -1, -1, -1, 0)).foreach { payload =>
      val fees = new Fees()
      assertThrows(classOf[Exception], () => consume(Vector(row(payload)), fees)(_ => fail("unexpected recovery")))
      assertEquals(0, fees.held)
    }

  @Test def refusesPassengerTreeRatherThanSilentlyDroppingIt(): Unit =
    val passengers = new NBTTagList()
    passengers.appendTag(new NBTTagCompound())
    val typedWrong = new NBTTagList()
    typedWrong.appendTag(new NBTTagString("not-a-compound"))
    Vector(passengers, typedWrong, new NBTTagString("not-a-list")).foreach { payload =>
      val nbt = fixtureNbt(); nbt.setTag("Passengers", payload)
      val fees = new Fees()
      assertThrows(classOf[IllegalArgumentException], () => consume(Vector(row(encode(nbt))), fees)(_ => fail("unexpected recovery")))
      assertEquals(0, fees.held)
    }

  @Test def deniedTreeAllocationSettlesEarlierChargesBeforeAnyConsumer(): Unit =
    val fees = new Fees(); fees.denyAt = 3
    assertThrows(classOf[IOException], () => consume(Vector(row(encode(fixtureNbt()))), fees)(_ => fail("unexpected recovery")))
    assertEquals(0, fees.held)

  @Test def consumerFailurePreservesOriginalCauseAndSuppressedCleanup(): Unit =
    val fees = new Fees()
    val original = new IOException("actual consumer failed")
    val cleanup = new IOException("actual fee cleanup failed")
    fees.closeFailure = cleanup
    val failure = assertThrows(classOf[IOException], () => consume(Vector(row(encode(fixtureNbt()))), fees)(_ => throw original))
    assertSame(original, failure)
    assertTrue(failure.getSuppressed.exists(_ eq cleanup))
    assertEquals(0, fees.held)

  @Test def unresolvedOriginalOperationOrHeldPodReservationCannotRestoreFromAnOldMatchingSnapshot(): Unit =
    def relation(table: String, fields: Map[String, MySqlOwnedFormat3Value]) =
      val shape = MySqlDbFirstRelationShapes.forTable(table)
      MySqlOwnedFormat3Relation(table, Vector(MySqlOwnedFormat3Row(shape.columns.map(c =>
        fields.getOrElse(c.name, MySqlOwnedFormat3Value.SqlNull)))))
    val emptyOperations = MySqlOwnedFormat3Relation("fp_recovery_operation", Vector.empty)
    val emptyReservations = MySqlOwnedFormat3Relation("fp_recovery_reservation", Vector.empty)
    for status <- Vector("RESERVED", "CANCELLED"); scope <- Vector(id(pod), MySqlOwnedFormat3Value.SqlNull) do
      val pending = relation("fp_recovery_operation", Map("pod_uuid" -> scope,
        "operation_state" -> MySqlOwnedFormat3Value.Text(status)))
      assertThrows(classOf[IllegalArgumentException], () =>
        MySqlDbFirstRecoveryNbt.requireSettled(pending, emptyReservations, pod))
    val held = relation("fp_recovery_reservation", Map("resource_pod_uuid" -> id(pod),
      "resource_kind" -> MySqlOwnedFormat3Value.Text("POD"), "reservation_state" -> MySqlOwnedFormat3Value.Text("HELD")))
    assertThrows(classOf[IllegalArgumentException], () =>
      MySqlDbFirstRecoveryNbt.requireSettled(emptyOperations, held, pod))
    val otherPod = new UUID(0, 14)
    val otherPending = relation("fp_recovery_operation", Map("pod_uuid" -> id(otherPod),
      "operation_state" -> MySqlOwnedFormat3Value.Text("RESERVED")))
    MySqlDbFirstRecoveryNbt.requireSettled(otherPending, emptyReservations, pod)
    val settled = relation("fp_recovery_operation", Map("pod_uuid" -> id(pod),
      "operation_state" -> MySqlOwnedFormat3Value.Text("CANCELLED"), "settled_by_operation_uuid" -> id(otherPod)))
    MySqlDbFirstRecoveryNbt.requireSettled(settled, emptyReservations, pod)
