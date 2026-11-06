package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.util.UUID
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

final class MySqlRecoveryOperationHistoryValidationTest:
  import MySqlScriptSchemaDataTestFixture.*

  private val InputsForHistory = Full26Inputs
  private val RecoveryTable = "fp_recovery_operation"

  private def rowsWith(inputs: MySqlScriptSchemaInputs = InputsForHistory) =
    MySqlScriptSchemaDataTestFixture.full26OrdinaryRelations(inputs)

  private def recoveryRows(rows: Vector[MySqlFormat3RelationSnapshot]) =
    rows.find(_.table == RecoveryTable).get.rows

  private def binaryAt(row: Vector[MySqlFormat3Cell], table: String, column: String): MySqlFormat3Binary =
    val shape = MySqlFormat3RelationShapes.forTable(table)
    row(shape.columns.indexWhere(_.name == column)) match
      case MySqlFormat3Cell.Binary(value) => value
      case _ => throw new AssertionError(s"$table.$column must be binary")

  private def bytes(value: MySqlFormat3Binary): Array[Byte] =
    Array.tabulate[Byte](value.length)(value.byteAt)

  private def readInt(bytes: Array[Byte], offset: Int): Int = ByteBuffer.wrap(bytes, offset, 4).getInt

  private def effectEnd(bytes: Array[Byte], start: Int): Int =
    val identityLength = readInt(bytes, start)
    val fingerprintLengthOffset = Math.addExact(start, Math.addExact(4, identityLength + 8))
    val fingerprintLength = readInt(bytes, fingerprintLengthOffset)
    Math.addExact(fingerprintLengthOffset, Math.addExact(4, fingerprintLength))

  private def twoEffectsInReverseOrder(bytes: Array[Byte]): Array[Byte] =
    val domainLength = readInt(bytes, 0)
    val countOffset = Math.addExact(4, Math.addExact(domainLength, 16 + 8 + 16 + 16))
    require(readInt(bytes, countOffset) == 2, "history context must contain exactly two effects")
    val first = Math.addExact(countOffset, 4)
    val second = effectEnd(bytes, first)
    val end = effectEnd(bytes, second)
    require(end == bytes.length, "effect frame parser must reach the complete canonical context")
    val output = new ByteArrayOutputStream(bytes.length)
    val data = new DataOutputStream(output)
    data.write(bytes, 0, countOffset)
    data.writeInt(2)
    data.write(bytes, second, end - second)
    data.write(bytes, first, second - first)
    data.flush()
    output.toByteArray

  private def duplicatedFirstEffect(bytes: Array[Byte]): Array[Byte] =
    val domainLength = readInt(bytes, 0)
    val countOffset = Math.addExact(4, Math.addExact(domainLength, 16 + 8 + 16 + 16))
    require(readInt(bytes, countOffset) == 2, "history context must contain exactly two effects")
    val first = Math.addExact(countOffset, 4)
    val second = effectEnd(bytes, first)
    val output = new ByteArrayOutputStream(bytes.length)
    val data = new DataOutputStream(output)
    data.write(bytes, 0, countOffset)
    data.writeInt(2)
    data.write(bytes, first, second - first)
    data.write(bytes, first, second - first)
    data.flush()
    output.toByteArray

  private def expectRejected(inputs: MySqlScriptSchemaInputs,
      rows: Vector[MySqlFormat3RelationSnapshot], allocation: MySqlFormat3ReadBudget,
      expectedMessage: String): SQLDataException =
    val baseline = allocation.accountedMemoryBytes
    val failure = assertThrows(classOf[SQLDataException], () => {
      inputs.validateRelations(rows, inputs.budget.packetBytes, allocation)
      ()
    })
    assertTrue(failure.getMessage.contains(expectedMessage), failure.getMessage)
    assertTrue(failure.getStackTrace.nonEmpty)
    assertEquals(baseline, allocation.accountedMemoryBytes,
      "a rejected history image must release its temporary index/context scopes")
    failure

  @Test def full26RecoveryHistoryAcceptsCrossActorTerminalAndUnknownChains(): Unit =
    val rows = rowsWith()
    val allocation = root(InputsForHistory)
    val baseline = allocation.accountedMemoryBytes
    try
      assertTrue(InputsForHistory.validateRelations(rows, InputsForHistory.budget.packetBytes, allocation) >= 0L)
      assertEquals(baseline, allocation.accountedMemoryBytes,
        "valid history parse objects must die before temporary DATA charges are released")
      val history = recoveryRows(rows)
      assertEquals(7, history.size)
      assertEquals(Vector("DEPLOYMENT_REGISTER", "DEPLOYMENT_START", "DEPLOYMENT_FINALIZE",
        "DEPLOYMENT_REGISTER", "DEPLOYMENT_START", "DEPLOYMENT_FINALIZE", "DEPLOYMENT_RECONCILE"), history.map { row =>
          val index = MySqlFormat3RelationShapes.forTable(RecoveryTable).columns.indexWhere(_.name == "step")
          row(index).asInstanceOf[MySqlFormat3Cell.Text].value
        })
      assertNotEquals(binaryAt(history(1), RecoveryTable, "boot_uuid"),
        binaryAt(history(2), RecoveryTable, "boot_uuid"), "terminal history retains its writer handoff")
      assertNotEquals(binaryAt(history(5), RecoveryTable, "boot_uuid"),
        binaryAt(history(6), RecoveryTable, "boot_uuid"), "UNKNOWN history retains its cross-boot reconciliation")
      assertNotEquals(binaryAt(history(6), RecoveryTable, "original_context_sha256"),
        binaryAt(history(6), RecoveryTable, "operation_fingerprint"),
        "the phase fingerprint remains independent from the generic context digest")
      assertEquals(2, rows.find(_.table == MySqlDeploymentReceiptSchemaDefinition.OperationTable).get.rows.size)
      assertEquals(2L, rows.find(_.table == MySqlDeploymentReceiptSchemaDefinition.QuotaTable).get.rows.head(2)
        .asInstanceOf[MySqlFormat3Cell.SignedInteger].value)
    finally allocation.closeRootReadBudget()

  @Test def genericContextRejectsTrailingBytesAndDigestChangesWithoutLeakingTemporaryDebt(): Unit =
    val valid = rowsWith()
    val allocation = root(InputsForHistory)
    try
      val original = binaryAt(recoveryRows(valid)(6), RecoveryTable, "original_context_bytes")
      val trailing = bytes(original) ++ Array[Byte](0)
      val withTrailingBytes = replaceRowCell(valid, RecoveryTable, 6, "original_context_bytes", binary(trailing))
      val withTrailingDigest = replaceRowCell(withTrailingBytes, RecoveryTable, 6,
        "original_context_sha256", binary(digest(trailing)))
      expectRejected(InputsForHistory, withTrailingDigest, allocation, "original context has trailing bytes")

      val badDigest = replaceRowCell(valid, RecoveryTable, 6,
        "original_context_sha256", binary(Array.fill[Byte](32)(0x6d.toByte)))
      expectRejected(InputsForHistory, badDigest, allocation, "original context SHA-256 differs")

      val latest = recoveryRows(valid)(6)
      val contextShape = MySqlFormat3RelationShapes.forTable(RecoveryTable)
      def uuid(column: String): UUID =
        val buffer = ByteBuffer.wrap(bytes(binaryAt(latest, RecoveryTable, column)))
        new UUID(buffer.getLong(), buffer.getLong())
      val epochIndex = contextShape.columns.indexWhere(_.name == "fence_epoch")
      val sortedEffects = MySqlOperationContext(uuid("boot_uuid"),
        latest(epochIndex).asInstanceOf[MySqlFormat3Cell.SignedInteger].value,
        uuid("binding_uuid"), uuid("operation_uuid"), Vector(
          MySqlExpectedEffect("deployment/z", 2L, Vector.fill(32)(0x22.toByte)),
          MySqlExpectedEffect("deployment/a", 1L, Vector.fill(32)(0x11.toByte))))
      val canonicalMultiEffect = MySqlScriptSchemaDataTestFixture.encodeGenericContext(sortedEffects)
      val canonicalRows = replaceRowCell(valid, RecoveryTable, 6,
        "original_context_bytes", binary(canonicalMultiEffect))
      val canonicalRowsWithHash = replaceRowCell(canonicalRows, RecoveryTable, 6,
        "original_context_sha256", binary(digest(canonicalMultiEffect)))
      assertTrue(InputsForHistory.validateRelations(canonicalRowsWithHash,
        InputsForHistory.budget.packetBytes, allocation) >= 0L,
        "the actual generic context encoder's sorted multi-effect form remains valid")
      val reversed = twoEffectsInReverseOrder(canonicalMultiEffect)
      val reversedRows = replaceRowCell(canonicalRowsWithHash, RecoveryTable, 6,
        "original_context_bytes", binary(reversed))
      val reversedRowsWithHash = replaceRowCell(reversedRows, RecoveryTable, 6,
        "original_context_sha256", binary(digest(reversed)))
      expectRejected(InputsForHistory, reversedRowsWithHash, allocation,
        "original context bytes are not the canonical generic operation context encoding")

      val duplicated = duplicatedFirstEffect(canonicalMultiEffect)
      val duplicateRows = replaceRowCell(canonicalRowsWithHash, RecoveryTable, 6,
        "original_context_bytes", binary(duplicated))
      val duplicateRowsWithHash = replaceRowCell(duplicateRows, RecoveryTable, 6,
        "original_context_sha256", binary(digest(duplicated)))
      expectRejected(InputsForHistory, duplicateRowsWithHash, allocation,
        "generic operation context is empty, duplicated or noncanonical")
    finally allocation.closeRootReadBudget()

  @Test def deploymentHistoryBindsActorRequestAndFullStepEvidence(): Unit =
    val valid = rowsWith()
    val allocation = root(InputsForHistory)
    try
      val terminalIdentity = binaryAt(recoveryRows(valid)(0), RecoveryTable, "request_identity_bytes")
      val foreignIdentityBytes = bytes(terminalIdentity)
      val foreignIdentity = replaceRowCell(valid, RecoveryTable, 6, "request_identity_bytes", binary(foreignIdentityBytes))
      val foreignIdentityWithDigest = replaceRowCell(foreignIdentity, RecoveryTable, 6,
        "request_identity_sha256", binary(digest(foreignIdentityBytes)))
      expectRejected(InputsForHistory, foreignIdentityWithDigest, allocation,
        "recovery request identity differs from namespace/actor/wire request")

      val alteredTargetBytes = "different complete step evidence".getBytes(java.nio.charset.StandardCharsets.US_ASCII)
      val alteredTarget = replaceRowCell(valid, RecoveryTable, 6, "target_bytes", binary(alteredTargetBytes))
      val alteredTargetWithDigest = replaceRowCell(alteredTarget, RecoveryTable, 6,
        "target_sha256", binary(digest(alteredTargetBytes)))
      expectRejected(InputsForHistory, alteredTargetWithDigest, allocation,
        "last step target differs from full step evidence")
    finally allocation.closeRootReadBudget()

  @Test def deploymentHistoryRejectsMissingChainWrongCodeForeignBindingAndMalformedIdentity(): Unit =
    val valid = rowsWith()
    val allocation = root(InputsForHistory)
    try
      val emptyHistory = valid.map(relation =>
        if relation.table == RecoveryTable then relation.copy(rows = Vector.empty) else relation)
      expectRejected(InputsForHistory, emptyHistory, allocation, "deployment rows have no recovery operation history")

      val wrongCode = replaceRowCell(valid, MySqlDeploymentReceiptSchemaDefinition.OperationTable,
        1, "last_step_code", MySqlFormat3Cell.SignedInteger(5L))
      expectRejected(InputsForHistory, wrongCode, allocation,
        "last step UUID/code does not map to its exact recovery row")

      val foreignBinding = replaceRowCell(valid, RecoveryTable, 6, "binding_uuid",
        binary(uuidBytes(UUID.fromString("00000000-0000-0000-0000-000000009901"))))
      expectRejected(InputsForHistory, foreignBinding, allocation, "uses another database authority binding")

      val identity = binaryAt(recoveryRows(valid)(6), RecoveryTable, "request_identity_bytes")
      val shortIdentity = bytes(identity).dropRight(1)
      val malformedIdentity = replaceRowCell(valid, RecoveryTable, 6, "request_identity_bytes", binary(shortIdentity))
      val malformedIdentityWithDigest = replaceRowCell(malformedIdentity, RecoveryTable, 6,
        "request_identity_sha256", binary(digest(shortIdentity)))
      expectRejected(InputsForHistory, malformedIdentityWithDigest, allocation,
        "deployment request identity must be exactly 154 bytes")

      val shortFingerprint = replaceRowCell(valid, RecoveryTable, 6, "operation_fingerprint",
        binary(new Array[Byte](31)))
      expectRejected(InputsForHistory, shortFingerprint, allocation, "operation_fingerprint must be exactly 32 bytes")

      val oldReceipt = bytes(binaryAt(recoveryRows(valid)(6), RecoveryTable, "preimage_bytes"))
      oldReceipt(0) = (oldReceipt(0) ^ 1).toByte
      val badOldReceipt = replaceRowCell(valid, RecoveryTable, 6, "preimage_bytes", binary(oldReceipt))
      val badOldReceiptWithHash = replaceRowCell(badOldReceipt, RecoveryTable, 6,
        "preimage_sha256", binary(digest(oldReceipt)))
      expectRejected(InputsForHistory, badOldReceiptWithHash, allocation,
        "old preimage")

      val queuedBeforeStart = bytes(binaryAt(recoveryRows(valid)(1), RecoveryTable, "preimage_bytes"))
      val illegalFinalizeOld = replaceRowCell(valid, RecoveryTable, 2, "preimage_bytes", binary(queuedBeforeStart))
      val illegalFinalizeOldWithHash = replaceRowCell(illegalFinalizeOld, RecoveryTable, 2,
        "preimage_sha256", binary(digest(queuedBeforeStart)))
      expectRejected(InputsForHistory, illegalFinalizeOldWithHash, allocation,
        "violates its public QUEUED->APPLIED receipt transition")
    finally allocation.closeRootReadBudget()

  @Test def predecessorCyclesFailButReverseCancelSettlementReferencesRemainValid(): Unit =
    val valid = rowsWith()
    val allocation = root(InputsForHistory)
    try
      val firstId = UUID.fromString("00000000-0000-0000-0000-000000000831")
      val secondId = UUID.fromString("00000000-0000-0000-0000-000000000832")
      val cycleRows = appendRecoveryRows(valid, Vector(
        legacyHistoryRow(firstId, "SCRIPT_PUBLICATION", previous = Some(secondId)),
        legacyHistoryRow(secondId, "SCRIPT_PUBLICATION", previous = Some(firstId))))
      expectRejected(InputsForHistory, cycleRows, allocation, "forms a recovery predecessor cycle")

      val cancelledId = UUID.fromString("00000000-0000-0000-0000-000000000833")
      val cancelId = UUID.fromString("00000000-0000-0000-0000-000000000834")
      val reverseSettlement = appendRecoveryRows(valid, Vector(
        legacyHistoryRow(cancelledId, "SCRIPT_PUBLICATION", state = "CANCELLED", settledBy = Some(cancelId)),
        legacyHistoryRow(cancelId, "CANCEL", previous = Some(cancelledId))))
      val baseline = allocation.accountedMemoryBytes
      assertTrue(InputsForHistory.validateRelations(reverseSettlement,
        InputsForHistory.budget.packetBytes, allocation) >= 0L)
      assertEquals(baseline, allocation.accountedMemoryBytes,
        "settlement reverse references are not combined with predecessor edges into a false cycle")
    finally allocation.closeRootReadBudget()

  @Test def schema5HistoryKeepsScriptPublicationAndRecallPrepareButRejectsUnknownStep(): Unit =
    val valid = rowsWith()
    val allocation = root(InputsForHistory)
    try
      val publicationId = UUID.fromString("00000000-0000-0000-0000-000000000835")
      val withPublication = appendRecoveryRows(valid,
        Vector(legacyHistoryRow(publicationId, "SCRIPT_PUBLICATION")))
      assertTrue(InputsForHistory.validateRelations(withPublication,
        InputsForHistory.budget.packetBytes, allocation) >= 0L)
      val baseline = allocation.accountedMemoryBytes
      assertEquals(0L, baseline)

      val recalled = appendRecoveryRows(valid,
        Vector(legacyHistoryRow(UUID.fromString("00000000-0000-0000-0000-000000000836"), "RECALL_PREPARE")))
      assertTrue(InputsForHistory.validateRelations(recalled,
        InputsForHistory.budget.packetBytes,allocation)>=0L)
      assertEquals(baseline,allocation.accountedMemoryBytes)
      val unknown=appendRecoveryRows(valid,
        Vector(legacyHistoryRow(UUID.fromString("00000000-0000-0000-0000-000000000837"),"RECALL_PREPARE_UNKNOWN")))
      expectRejected(InputsForHistory,unknown,allocation,"has a recovery step outside its exact schema family")
    finally allocation.closeRootReadBudget()
