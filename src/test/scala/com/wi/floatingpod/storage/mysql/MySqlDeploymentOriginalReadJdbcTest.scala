package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.config.SourceLimitProfile
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec
import com.wi.floatingpod.deployment.command.DeploymentCommandWireCodec.*
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.storage.mysql.MySqlDeploymentOperationRows.{ColumnValue, ColumnValues}
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.sql.{ResultSetMetaData, SQLDataException, SQLNonTransientConnectionException, Types}
import java.util.UUID
import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import javax.sql.rowset.{CachedRowSet, RowSetMetaDataImpl, RowSetProvider}
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 标准内存 ResultSet 直接调用生产 decoder；实际 core/worker 只证明内存责任，不取得 SQL 资格。 */
final class MySqlDeploymentOriginalReadJdbcTest:
  private def id(n: Long): UUID = new UUID(0L, n)
  private val actor = id(1)
  private val original = id(2)
  private def lookup: MySqlRepositoryOperation.FindOriginalDeploymentOperation =
    MySqlRepositoryOperation.FindOriginalDeploymentOperation(actor, original)
  private def shape = MySqlDeploymentReceiptRelationShapes.operation

  @Test def emptyResultReturnsNoneWithoutChargingARow(): Unit = withRoot() { f =>
    val before = f.root.accountedMemoryBytes
    MySqlBusinessRows.using(rows(Vector.empty)) { result =>
      assertEquals(None, MySqlDeploymentOriginalReadJdbc.decodeResultSet(result, lookup, f.root))
    }
    assertEquals(before, f.root.accountedMemoryBytes)
    assertFalse(f.root.isClosed)
  }

  @Test def oneActualRowReturnsOriginalImmutableProjectionOnTheSameRoot(): Unit = withRoot() { f =>
    val before = f.shared.accountedBytes
    MySqlBusinessRows.using(rows(Vector(fixture()))) { result =>
      val value = MySqlDeploymentOriginalReadJdbc.decodeResultSet(result, lookup, f.root).get
      assertEquals(actor, value.actorUuid)
      assertEquals(original, value.originalOperationId)
      assertEquals(request, value.request)
      assertEquals(receipt, value.receipt)
      assertEquals(0L, value.rowRevision)
    }
    f.root.requireLedgerIdentity(f.shared)
    assertTrue(f.shared.accountedBytes > before)
    assertEquals(f.root.accountedMemoryBytes, f.shared.accountedBytes)
    assertFalse(f.root.isClosed)
  }

  @Test def secondRowIsRejectedWithoutBeginningAnotherBudgetedRow(): Unit = withRoot() { f =>
    MySqlBusinessRows.using(rows(Vector(fixture(), fixture()))) { result =>
      val failure = assertThrows(classOf[SQLDataException], () =>
        MySqlDeploymentOriginalReadJdbc.decodeResultSet(result, lookup, f.root))
      assertTrue(failure.getMessage.contains("more than one row"))
    }
    // 原 row 已在 finally 结束；再次 begin 只会触发 one-shot，而非遗留 activeRow。
    val replay = assertThrows(classOf[SQLDataException], () => f.root.beginRow(31, shape.table, 0L))
    assertTrue(replay.getMessage.contains("exactly row zero"))
  }

  @Test def metadataOrderJdbcTypeAndNullabilityAreCheckedBeforeRows(): Unit =
    val mutations: Vector[RowSetMetaDataImpl => Unit] = Vector(
      metadata => metadata.setColumnLabel(1, "wrong_column"),
      metadata => metadata.setColumnType(5, Types.BIGINT),
      metadata => metadata.setNullable(1, ResultSetMetaData.columnNullable))
    mutations.foreach { change => withRoot() { f =>
      val before = f.root.accountedMemoryBytes
      MySqlBusinessRows.using(rows(Vector(fixture()), change)) { result =>
        assertThrows(classOf[SQLDataException], () => MySqlDeploymentOriginalReadJdbc.decodeResultSet(result, lookup, f.root))
      }
      assertEquals(before, f.root.accountedMemoryBytes)
    }}

  @Test def requiredNullAndSignedSmallintOverflowDoNotPublishResults(): Unit =
    val invalid = Vector(
      ColumnValues(fixture().values.updated(4, ColumnValue.SqlNull)),
      ColumnValues(fixture().values.updated(4, ColumnValue.Integer(32768L))))
    invalid.foreach { input => withRoot() { f =>
      val overflow = input.values(4) == ColumnValue.Integer(32768L)
      MySqlBusinessRows.using(rows(Vector(input), stageWideSmallint = overflow)) { result =>
        if overflow then
          assertTrue(result.next())
          assertEquals(32768L, result.getLong(5))
          assertFalse(result.wasNull())
          assertEquals(Types.SMALLINT, result.getMetaData.getColumnType(5))
          result.beforeFirst()
        val failure = assertThrows(classOf[SQLDataException], () =>
          MySqlDeploymentOriginalReadJdbc.decodeResultSet(result, lookup, f.root))
        if overflow then assertTrue(failure.getMessage.contains("SMALLINT range"))
      }
    }}

  @Test def foreignActorOrOperationFailsInsteadOfReturningMissing(): Unit =
    Vector[MySqlRepositoryOperation.FindOriginalDeploymentOperation](MySqlRepositoryOperation.FindOriginalDeploymentOperation(id(99), original),
      MySqlRepositoryOperation.FindOriginalDeploymentOperation(actor, id(99))).foreach { operation =>
      withRoot(operation = operation) { f =>
        MySqlBusinessRows.using(rows(Vector(fixture()))) { result =>
          val failure = assertThrows(classOf[MySqlDeploymentOperationRows.DecodeFailure], () =>
            MySqlDeploymentOriginalReadJdbc.decodeResultSet(result, operation, f.root))
          assertNotNull(failure.getCause)
        }
      }
    }

  @Test def binaryColumnAndActualPacketLimitsFailAndLeaveTheRowReleasable(): Unit =
    withRoot() { f =>
      val oversized = ColumnValues(fixture().values.updated(0, ColumnValue.Binary(Array[Byte](70, 80, 68, 67, 0))))
      MySqlBusinessRows.using(rows(Vector(oversized))) { result =>
        assertThrows(classOf[SQLDataException], () => MySqlDeploymentOriginalReadJdbc.decodeResultSet(result, lookup, f.root))
      }
    }
    withRoot(packet = 512L) { f =>
      MySqlBusinessRows.using(rows(Vector(fixture()))) { result =>
        assertThrows(classOf[SQLDataException], () => MySqlDeploymentOriginalReadJdbc.decodeResultSet(result, lookup, f.root))
      }
    }

  @Test def actualRowsDecodeFailurePreservesCauseAndCannotEscapeAsAnEmptyResult(): Unit = withRoot() { f =>
    val corrupt = ColumnValues(fixture().values.updated(6, ColumnValue.Binary(new Array[Byte](32))))
    MySqlBusinessRows.using(rows(Vector(corrupt))) { result =>
      val failure = assertThrows(classOf[MySqlDeploymentOperationRows.DecodeFailure], () =>
        MySqlDeploymentOriginalReadJdbc.decodeResultSet(result, lookup, f.root))
      assertEquals("request_sha256", failure.column)
      assertNotNull(failure.getCause)
    }
  }

  @Test def uninitializedFull20AndForeignThreadCannotDecodeEvenAnEmptyResult(): Unit =
    for ordinary <- Vector(false, true) do
      MySqlBusinessRows.using(new Fixture(lookup, 65536L, ordinary)) { f =>
        f.onWorker {
          f.core.beginExecution(f.plan)
          MySqlBusinessRows.using(rows(Vector.empty)) { result =>
            assertThrows(classOf[SQLNonTransientConnectionException], () =>
              MySqlDeploymentOriginalReadJdbc.decodeResultSet(result, lookup, f.root))
          }
        }
      }
    MySqlBusinessRows.using(new Fixture(lookup, 65536L, false)) { f =>
      f.onWorker { f.initialize() }
      MySqlBusinessRows.using(rows(Vector.empty)) { result =>
        assertThrows(classOf[SQLNonTransientConnectionException], () =>
          MySqlDeploymentOriginalReadJdbc.decodeResultSet(result, lookup, f.root))
      }
    }

  private def request: DeployRequest =
    new DeployRequest(original, id(3), id(4), 1L, new ExpectedCommandVersions(id(5), 1L, id(6), 1L, 1L, 1L, 1L))

  private def receipt: MutationReceipt =
    val value = request
    new MutationReceipt(original, value.podUuid(), value.moduleInstanceId(), value.moduleGeneration(), MutationKind.DEPLOY,
      value.expected(), OperationPhase.QUEUED, ReceiptReason.NONE, ActualEffectStatus.NOT_STARTED,
      CriticalCommitStatus.NOT_ATTEMPTED, ReadinessStatus.NOT_EVALUATED, CleanupStatus.NOT_REQUIRED)

  private def fixture(): ColumnValues =
    import ColumnValue.*
    def bytes(uuid: UUID): Array[Byte] = ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits).putLong(uuid.getLeastSignificantBits).array()
    def hash(payload: Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA-256").digest(payload)
    val req = DeploymentCommandWireCodec.encodeRequest(request)
    val rec = DeploymentCommandWireCodec.encodeResponse(receipt)
    val evidence = Array[Byte](1, 2, 3)
    ColumnValues(Vector(Binary(Array[Byte](70, 80, 68, 67)), Binary(bytes(original)), Binary(bytes(actor)), Binary(bytes(id(6))),
      Integer(1L), Binary(req), Binary(hash(req)), Integer(1L), Binary(rec), Binary(hash(rec)), Integer(0L),
      Binary(bytes(id(5))), Integer(1L), Integer(1L), Binary(bytes(id(5))), Integer(1L), Binary(bytes(id(7))), Binary(bytes(id(7))),
      Integer(1L), Binary(new Array[Byte](32)), Integer(10L), Integer(20L), Integer(1L), Binary(evidence.clone()), Binary(hash(evidence)),
      Integer(1L), Binary(evidence.clone()), Binary(hash(evidence)), SqlNull, SqlNull, SqlNull))

  private def rows(input: Vector[ColumnValues], change: RowSetMetaDataImpl => Unit = _ => (),
      stageWideSmallint: Boolean = false): CachedRowSet =
    val metadata = new RowSetMetaDataImpl()
    metadata.setColumnCount(shape.columns.size)
    shape.columns.zipWithIndex.foreach { (column, offset) =>
      val index = offset + 1
      metadata.setColumnName(index, column.name)
      metadata.setColumnLabel(index, column.name)
      metadata.setColumnType(index, column.jdbcType)
      metadata.setNullable(index, if column.nullable then ResultSetMetaData.columnNullable else ResultSetMetaData.columnNoNulls)
    }
    change(metadata)
    // 标准 RowSet 的 updateLong 会按当前 metadata 先做窄化；先以 BIGINT 保存反例原值。
    if stageWideSmallint then metadata.setColumnType(5, Types.BIGINT)
    val result = RowSetProvider.newFactory().createCachedRowSet()
    var primary: Throwable = null
    try
      result.setMetaData(metadata)
      input.foreach { row =>
        result.moveToInsertRow()
        row.values.zipWithIndex.foreach { (value, offset) => value match
          case ColumnValue.Binary(bytes) => result.updateBytes(offset + 1, bytes)
          case ColumnValue.Integer(number) => result.updateLong(offset + 1, number)
          case ColumnValue.SqlNull => result.updateNull(offset + 1)
        }
        result.insertRow()
        result.moveToCurrentRow()
        result.afterLast()
      }
      // setMetaData 只更换列描述，不转换已保存的 Long；交给生产 decoder 的仍是 SMALLINT。
      if stageWideSmallint then
        metadata.setColumnType(5, Types.SMALLINT)
        result.setMetaData(metadata)
      result.beforeFirst()
      result
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      if primary != null then
        try result.close()
        catch case cleanup: Throwable => MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

  private def withRoot(
      operation: MySqlRepositoryOperation.FindOriginalDeploymentOperation = lookup,
      packet: Long = 65536L
  )(body: Fixture => Unit): Unit =
    MySqlBusinessRows.using(new Fixture(operation, packet, false)) { f =>
      f.onWorker { f.initialize(); body(f) }
    }

  /** 复用原 core 测试的单槽 worker 装配；None issuer 明确不代表实际 Session/SQL admission。 */
  private final class Fixture(operation: MySqlRepositoryOperation.FindOriginalDeploymentOperation, packet: Long, ordinary: Boolean)
      extends AutoCloseable:
    val profile = SourceLimitProfile.of(1048576L, 65536L, 1048576L, 32L, 256L, 100000L, 65536L, 65536L)
    val shared = new MySqlFormat3AllocationLedger(profile.maxRetainedBytes())
    private val actions = new ArrayBlockingQueue[Runnable](1)
    private val poison = new Runnable { override def run(): Unit = () }
    private val worker = new Thread(() =>
      var running = true
      while running do
        val action = actions.take()
        if action eq poison then running = false else action.run()
      (), "deployment-original-reader-core-worker")
    val core = new MySqlRetainedPlanReservationCore(MySqlOwnedIoIdentity(id(50), 1L, 1L), worker, Thread.currentThread(),
      profile, shared, 1, None)
    val plan = core.reserveSingle(IoOperationTag(id(50), 1L, 1L, id(51)), id(52), id(53),
      if ordinary then MySqlRepositoryOperation.FindAll else operation)
    private val handle = plan.originalRetainedHandle.get
    val root = core.validationBudget(handle)
    worker.start()

    def initialize(): Unit =
      assertSame(root, core.beginExecution(plan))
      root.claimDeploymentOriginalJdbcAttempt()
      root.initializeDeploymentOriginalRead(MySqlFull20CodecFixture.schema, MySqlFull20CodecFixture.observation,
        MySqlFull20CodecFixture.scope, packet)

    def onWorker(action: => Unit): Unit =
      val done = new CountDownLatch(1)
      val failure = new AtomicReference[Throwable]()
      val task = new Runnable:
        override def run(): Unit =
          try action
          catch case cause: Throwable => failure.set(cause)
          finally done.countDown()
      assertTrue(actions.offer(task, 3L, TimeUnit.SECONDS), "worker action slot unavailable")
      assertTrue(done.await(3L, TimeUnit.SECONDS), "worker action timed out; not passed")
      Option(failure.get()).foreach(cause => throw cause)

    override def close(): Unit =
      var primary: Throwable = null
      try
        val dropped = core.recordMainDrop(handle)
        onWorker { core.releaseDropped(dropped) }
        assertTrue(root.isClosed)
        assertEquals(0L, shared.accountedBytes)
      catch
        case failure: Throwable => primary = failure; throw failure
      finally
        try
          if !actions.offer(poison, 3L, TimeUnit.SECONDS) then worker.interrupt()
          worker.join(4000L)
          if worker.isAlive then
            worker.interrupt()
            worker.join(4000L)
          assertFalse(worker.isAlive, "actual worker leaked")
        catch
          case cleanup: Throwable =>
            if primary == null then throw cleanup
            else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)

object MySqlDeploymentOriginalReadJdbcTest:
  /** 仅此新 reader 的数据/内存接缝；bootstrap/replay 归原 root owner 的新用例。 */
  def main(args: Array[String]): Unit =
    val test = new MySqlDeploymentOriginalReadJdbcTest()
    test.emptyResultReturnsNoneWithoutChargingARow()
    test.oneActualRowReturnsOriginalImmutableProjectionOnTheSameRoot()
    test.secondRowIsRejectedWithoutBeginningAnotherBudgetedRow()
    test.metadataOrderJdbcTypeAndNullabilityAreCheckedBeforeRows()
    test.requiredNullAndSignedSmallintOverflowDoNotPublishResults()
    test.foreignActorOrOperationFailsInsteadOfReturningMissing()
    test.binaryColumnAndActualPacketLimitsFailAndLeaveTheRowReleasable()
    test.actualRowsDecodeFailurePreservesCauseAndCannotEscapeAsAnEmptyResult()
    test.uninitializedFull20AndForeignThreadCannotDecodeEvenAnEmptyResult()
    println("MySqlDeploymentOriginalReadJdbcTest: PASS (9 directed reader data behavior cases)")
