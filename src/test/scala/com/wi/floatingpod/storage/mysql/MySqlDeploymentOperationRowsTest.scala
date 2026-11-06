package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.*
import com.wi.floatingpod.storage.mysql.MySqlDeploymentOperationRows.*
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.sql.SQLDataException
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class MySqlDeploymentOperationRowsTest:
  private val actor = id(1)
  private val operation = id(2)

  @Test def actualCodecRoundTripAndAllColumnsAreRetained(): Unit = withBudget() { budget =>
    Vector(false, true).foreach { retract =>
      val input = fixture(retract = retract)
      val row = decode(input, actor, operation, budget)
      assertEquals(operation, row.operationUuid)
      assertEquals(actor, row.actorUuid)
      assertEquals(0L, row.rowRevision)
      assertTrue(row.sameOriginal(actor, request(retract)))
      assertFalse(row.sameOriginal(id(30), request(retract)))
      assertFalse(row.sameOriginal(actor, request(!retract)))
      assertEquals(request(retract).getClass, row.request.getClass)
      columnNames.zip(input.values).foreach { (name, cell) => cell match
        case ColumnValue.Binary(bytes) => assertArrayEquals(bytes, row.copyBinary(name, budget).get)
        case ColumnValue.Integer(value) => assertEquals(Some(value), row.integer(name))
        case ColumnValue.SqlNull => assertEquals(None, row.copyBinary(name, budget))
      }
      assertArrayEquals(DeploymentCommandWireCodec.encodeRequest(request(retract)), row.copyBinary("request_bytes", budget).get)
      assertEquals(receipt(retract), row.receipt)
    }
  }

  @Test def everyReceiptIdentityFieldMustMatchActualRequest(): Unit =
    // 修改合法receipt的每一个identity字段，重新计算hash，不能靠摘要相符放行。
    val original = DeploymentCommandWireCodec.encodeResponse(receipt())
    Vector(6, 22, 38, 54, 62, 63, 79, 87, 103, 111, 119, 127).foreach { offset =>
      val changed = original.clone()
      val position = offset + (if offset == 62 then 0 else if offset == 54 || offset == 79 || offset >= 103 then 7 else 15)
      changed(position) = (if offset == 62 then 2 else changed(position) ^ 1).toByte
      val data = replaceBytes(fixture(), "receipt_bytes", changed, "receipt_sha256")
      assertRejected(data)
    }

  @Test def actorScopedLookupCannotExposeForeignRows(): Unit = withBudget() { budget =>
    assertThrows(classOf[DecodeFailure], () => decode(fixture(), id(9), operation, budget))
    assertThrows(classOf[DecodeFailure], () => decode(fixture(), actor, id(9), budget))
    assertEquals(0L, budget.accountedMemoryBytes)
  }

  @Test def malformedFramesHashesAndReceiptAxesFailWithCause(): Unit =
    Vector("request_sha256", "receipt_sha256", "enqueue_evidence_sha256", "step_evidence_sha256").foreach { name =>
      assertRejected(set(fixture(), name, ColumnValue.Binary(new Array[Byte](32))))
    }
    Vector("request_bytes", "receipt_bytes").foreach { name =>
      val base = fixture().values(columnNames.indexOf(name)) match
        case ColumnValue.Binary(value) => value.clone()
        case _ => throw new AssertionError("binary fixture required")
      base(0) = 0
      assertRejected(replaceBytes(fixture(), name, base, if name == "request_bytes" then "request_sha256" else "receipt_sha256"))
    }
    val illegalAxes = DeploymentCommandWireCodec.encodeResponse(receipt())
    illegalAxes(138) = 127.toByte
    val failure = assertRejected(replaceBytes(fixture(), "receipt_bytes", illegalAxes, "receipt_sha256"))
    assertEquals("receipt_bytes", failure.column)
    assertNotNull(failure.getCause)
    assertTrue(failure.getCause.isInstanceOf[IllegalArgumentException])

  @Test def everyFixedLengthTypeAndRequiredNullIsChecked(): Unit =
    fixture().values.zipWithIndex.foreach { (cell, i) =>
      if i < 28 then assertRejected(ColumnValues(fixture().values.updated(i, ColumnValue.SqlNull)))
      cell match
        case ColumnValue.Binary(value) =>
          assertRejected(ColumnValues(fixture().values.updated(i, ColumnValue.Integer(1))))
          if value.length == 4 || value.length == 16 || value.length == 32 || value.length == 134 || value.length == 142 then
            assertRejected(ColumnValues(fixture().values.updated(i, ColumnValue.Binary(value.drop(1)))))
          assertRejected(ColumnValues(fixture().values.updated(i, ColumnValue.Binary(null))))
        case ColumnValue.Integer(_) =>
          assertRejected(ColumnValues(fixture().values.updated(i, ColumnValue.Binary(Array[Byte](1)))))
          assertRejected(ColumnValues(fixture().values.updated(i, ColumnValue.Integer(-1))))
        case _ => ()
    }
    assertRejected(ColumnValues(fixture().values.drop(1)))
    assertRejected(ColumnValues(fixture().values :+ ColumnValue.SqlNull))
    assertRejected(set(fixture(), "namespace", ColumnValue.Binary(Array[Byte](1, 2, 3, 4))))

  @Test def encodingsRevisionsAndRegistrationMetadataAreValidated(): Unit =
    Vector("request_encoding", "receipt_encoding", "enqueue_evidence_encoding", "step_evidence_encoding").foreach { name =>
      assertRejected(set(fixture(), name, ColumnValue.Integer(2)))
    }
    assertRejected(set(fixture(), "last_step_code", ColumnValue.Integer(6)))
    assertRejected(set(fixture(), "last_step_uuid", ColumnValue.Binary(uuid(id(99)))))
    assertRejected(set(fixture(), "current_writer_epoch", ColumnValue.Integer(0)))
    assertRejected(set(fixture(), "current_writer_boot_uuid", ColumnValue.Binary(uuid(id(99)))))
    assertRejected(set(fixture(), "row_revision", ColumnValue.Integer(1)))
    withBudget() { budget =>
      val transitioned = set(set(fixture(), "row_revision", ColumnValue.Integer(Long.MaxValue)), "last_step_code", ColumnValue.Integer(4))
      assertEquals(Long.MaxValue, decode(transitioned, actor, operation, budget).rowRevision)
    }

  @Test def diagnosticTriadIsCompleteHashedAndDefensivelyCopied(): Unit = withBudget() { budget =>
    val diagnostic = Array[Byte](10, 20, 30)
    val input = set(set(set(fixture(), "diagnostic_encoding", ColumnValue.Integer(1)),
      "diagnostic_bytes", ColumnValue.Binary(diagnostic)), "diagnostic_sha256", ColumnValue.Binary(hash(diagnostic)))
    val row = decode(input, actor, operation, budget)
    diagnostic(0) = 99
    val exported = row.copyBinary("diagnostic_bytes", budget).get
    assertArrayEquals(Array[Byte](10, 20, 30), exported)
    exported(1) = 99
    assertArrayEquals(Array[Byte](10, 20, 30), row.copyBinary("diagnostic_bytes", budget).get)
    Vector("diagnostic_encoding", "diagnostic_bytes", "diagnostic_sha256").foreach { name =>
      assertRejected(set(input, name, ColumnValue.SqlNull))
    }
    assertRejected(set(input, "diagnostic_encoding", ColumnValue.Integer(2)))
    assertRejected(set(input, "diagnostic_sha256", ColumnValue.Binary(new Array[Byte](32))))
  }

  @Test def allInputAndOutputBytesAreIsolated(): Unit = withBudget() { budget =>
    val input = fixture()
    val originals = input.values.map {
      case ColumnValue.Binary(value) => Some(value.clone())
      case _ => None
    }
    val row = decode(input, actor, operation, budget)
    input.values.foreach {
      case ColumnValue.Binary(value) => java.util.Arrays.fill(value, 99.toByte)
      case _ => ()
    }
    originals.zipWithIndex.foreach { (expected, i) => expected.foreach { bytes =>
      val exported = row.copyBinary(columnNames(i), budget).get
      assertArrayEquals(bytes, exported)
      java.util.Arrays.fill(exported, 0.toByte)
      assertArrayEquals(bytes, row.copyBinary(columnNames(i), budget).get)
    }}
    assertEquals(request(), row.request)
    assertEquals(receipt(), row.receipt)
  }

  @Test def actualSharedBudgetEnforcesPacketRetainedExportAndClosedLimits(): Unit =
    withBudget(retained = 128) { budget =>
      budget.reserveRetainedMemory(16, "prior real responsibility")
      val failure = assertThrows(classOf[DecodeFailure], () => decode(fixture(), actor, operation, budget))
      assertInstanceOf(classOf[SQLDataException], failure.getCause)
      assertEquals(16L, budget.accountedMemoryBytes)
    }
    withBudget(packet = 100) { budget =>
      val failure = assertThrows(classOf[DecodeFailure], () => decode(fixture(), actor, operation, budget))
      assertEquals("row_packet_length", failure.column)
      assertEquals(0L, budget.accountedMemoryBytes)
    }
    withBudget() { budget =>
      val row = decode(fixture(), actor, operation, budget)
      val before = budget.accountedMemoryBytes
      row.copyBinary("request_bytes", budget)
      assertEquals(before + 32L + 134L, budget.accountedMemoryBytes)
      budget.reserveRetainedMemory(budget.availableMemoryBytes, "other original retained work")
      assertThrows(classOf[SQLDataException], () => row.copyBinary("request_bytes", budget))
    }
    val closed = budgetFor(100000, 100000)
    closed.closeRootReadBudget()
    assertThrows(classOf[DecodeFailure], () => decode(fixture(), actor, operation, closed))

  private def assertRejected(input: ColumnValues): DecodeFailure = withBudget() { budget =>
    val failure = assertThrows(classOf[DecodeFailure], () => decode(input, actor, operation, budget))
    assertNotNull(failure.getCause)
    assertEquals(0L, budget.accountedMemoryBytes)
    failure
  }

  private def id(value: Long): UUID = new UUID(0, value)
  private def uuid(value: UUID): Array[Byte] = ByteBuffer.allocate(16).putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()
  private def hash(value: Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA-256").digest(value)
  private def request(retract: Boolean = false): Request =
    val expected = new ExpectedCommandVersions(id(5), 1, id(6), 2, 3, 4, 5)
    if retract then new RetractRequest(operation, id(3), id(4), 1, expected)
    else new DeployRequest(operation, id(3), id(4), 1, expected)
  private def receipt(retract: Boolean = false): MutationReceipt =
    val expected = new ExpectedCommandVersions(id(5), 1, id(6), 2, 3, 4, 5)
    new MutationReceipt(operation, id(3), id(4), 1, if retract then MutationKind.RETRACT else MutationKind.DEPLOY,
      expected, OperationPhase.QUEUED, ReceiptReason.NONE, ActualEffectStatus.NOT_STARTED,
      CriticalCommitStatus.NOT_ATTEMPTED, ReadinessStatus.NOT_EVALUATED, CleanupStatus.NOT_REQUIRED)

  /** 仅行值TEST_FIXTURE；证据字节不冒真实REGISTER/诊断producer。 */
  private def fixture(retract: Boolean = false): ColumnValues =
    import ColumnValue.*
    val req = DeploymentCommandWireCodec.encodeRequest(request(retract))
    val rec = DeploymentCommandWireCodec.encodeResponse(receipt(retract))
    val evidence = Array[Byte](1, 2, 3)
    ColumnValues(Vector(Binary(Array[Byte](70, 80, 68, 67)), Binary(uuid(operation)), Binary(uuid(actor)), Binary(uuid(id(6))),
      Integer(1), Binary(req), Binary(hash(req)), Integer(1), Binary(rec), Binary(hash(rec)), Integer(0),
      Binary(uuid(id(5))), Integer(1), Integer(1), Binary(uuid(id(5))), Integer(1), Binary(uuid(id(7))), Binary(uuid(id(7))),
      Integer(1), Binary(new Array[Byte](32)), Integer(10), Integer(20), Integer(1), Binary(evidence.clone()), Binary(hash(evidence)),
      Integer(1), Binary(evidence.clone()), Binary(hash(evidence)), SqlNull, SqlNull, SqlNull))

  private def set(input: ColumnValues, name: String, cell: ColumnValue): ColumnValues =
    ColumnValues(input.values.updated(columnNames.indexOf(name), cell))
  private def replaceBytes(input: ColumnValues, name: String, bytes: Array[Byte], hashName: String): ColumnValues =
    set(set(input, name, ColumnValue.Binary(bytes)), hashName, ColumnValue.Binary(hash(bytes)))
  private def budgetFor(retained: Long, packet: Long): MySqlFormat3ReadBudget =
    val profile = SourceLimitProfile.of(100000, 100000, 100000, 64, 100000, retained, packet, 100000)
    new MySqlFormat3ReadBudget(profile, packet)
  private def withBudget[A](retained: Long = 100000, packet: Long = 100000)(body: MySqlFormat3ReadBudget => A): A =
    val budget = budgetFor(retained, packet)
    try body(budget) finally budget.closeRootReadBudget()

object MySqlDeploymentOperationRowsTest:
  def main(args: Array[String]): Unit =
    val test = new MySqlDeploymentOperationRowsTest
    test.actualCodecRoundTripAndAllColumnsAreRetained()
    test.everyReceiptIdentityFieldMustMatchActualRequest()
    test.actorScopedLookupCannotExposeForeignRows()
    test.malformedFramesHashesAndReceiptAxesFailWithCause()
    test.everyFixedLengthTypeAndRequiredNullIsChecked()
    test.encodingsRevisionsAndRegistrationMetadataAreValidated()
    test.diagnosticTriadIsCompleteHashedAndDefensivelyCopied()
    test.allInputAndOutputBytesAreIsolated()
    test.actualSharedBudgetEnforcesPacketRetainedExportAndClosedLimits()
    println("MySqlDeploymentOperationRowsTest: PASS (9 directed behavior cases)")
