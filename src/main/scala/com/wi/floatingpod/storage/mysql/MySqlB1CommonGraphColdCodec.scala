package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.PlayerEntitlement
import com.wi.floatingpod.storage.*
import com.wi.floatingpod.server.B1ResumeRecord
import java.io.{ByteArrayInputStream, DataInputStream, EOFException}
import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.util.UUID

/** 旧common完整组件的DATA解码；不重新构造SQL plan或锁资格。 */
private[mysql] object MySqlB1CommonGraphColdCodec:
  final class Draft private[MySqlB1CommonGraphColdCodec] (
      val assertions: B1ResumeRecord.CommonGraphAssertions, val full20: MySqlFormat3FullSnapshot,
      val root: MySqlFormat3ReadBudget):
    var manifestBytes: Array[Byte] = null
    var bindingBytes: Array[Byte] = null
    var repositoryBytes: Array[Byte] = null
    var legacyBytes: Array[Byte] = null
    var manifest: MySqlLockedManifest = null
    var binding: Option[PhysicalRecoveryBinding] = null
    var repository: MySqlFullRepositorySnapshot = null
    var result: MySqlB1CommonGraph = null
    var failure: Throwable = null
    private[MySqlB1CommonGraphColdCodec] var started = false

  /** Distinct cold DATA path for an already decoded Full26 image; it does not mint B1 transition data. */
  final class DeploymentReceiptFull26Draft private[MySqlB1CommonGraphColdCodec] (
      val assertions: B1ResumeRecord.CommonGraphAssertions,
      val snapshot: MySqlScriptSchemaSnapshot,
      val inputs: MySqlScriptSchemaInputs,
      val root: MySqlFormat3ReadBudget):
    var manifestBytes: Array[Byte] = null
    var bindingBytes: Array[Byte] = null
    var repositoryBytes: Array[Byte] = null
    var result: MySqlB1CommonGraph = null
    var failure: Throwable = null
    private[MySqlB1CommonGraphColdCodec] var started = false

  def draft(assertions: B1ResumeRecord.CommonGraphAssertions, full20: MySqlFormat3FullSnapshot,
      root: MySqlFormat3ReadBudget): Draft =
    require(assertions != null && full20 != null && root != null && !root.isClosed &&
      (full20.limitProfile eq root.profile), "complete cold DATA and exact open original profile required")
    root.reserveRetainedMemory(512L, "cold common draft before allocation")
    new Draft(assertions, full20, root)

  def deploymentReceiptFull26Draft(assertions: B1ResumeRecord.CommonGraphAssertions,
      snapshot: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget): DeploymentReceiptFull26Draft =
    require(assertions != null && snapshot != null && inputs != null && root != null &&
      inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      (snapshot.inputs eq inputs) && !root.isClosed && !root.isScopedReadBudget &&
      (root.profile eq inputs.sourceLimitProfile),
      "actual decoded Full26 DATA snapshot, exact inputs and open original root are required")
    MySqlB1CommonGraph.requireFull26Owner(root, snapshot.allocationBudget, inputs)
    root.reserveRetainedMemory(512L, "cold Full26 common draft before allocation")
    new DeploymentReceiptFull26Draft(assertions, snapshot, inputs, root)

  def decode(d: Draft, inputs: MySqlFormat3AuditInputs): MySqlB1CommonGraph =
    require(d != null && !d.started && inputs != null && !d.root.isClosed, "cold common DATA decoder is once")
    d.started = true
    try
      require(d.assertions.manifestEncoding() == MySqlB1CommonGraph.ManifestEncoding &&
        d.assertions.bindingEncoding() == MySqlB1CommonGraph.BindingEncoding &&
        d.assertions.repositoryEncoding() == MySqlB1CommonGraph.Format3RepositoryEncoding,
        "exact existing format3 common component encodings required")
      // DTO accessors clone: reserve maximum bounded components before invoking them.
      d.root.reserveRetainedMemory(3L * 65600L, "three complete old common component copies before access")
      d.manifestBytes = d.assertions.manifestBytes()
      d.bindingBytes = d.assertions.bindingBytes()
      d.repositoryBytes = d.assertions.repositoryBytes()
      // A bounded old component cannot expand beyond one object per encoded byte; all
      // wrappers, text/Vector cells and original encoder roundtrip remain retained.
      d.root.reserveRetainedMemory(64L * (d.manifestBytes.length.toLong + d.bindingBytes.length +
        d.repositoryBytes.length) + 65536L, "cold common typed structures and unique encoder before decode")
      val m = new Reader(d.manifestBytes)
      m.domain(MySqlB1CommonGraph.ManifestEncoding)
      d.manifest = MySqlLockedManifest(m.int(), m.text(), m.int(), m.hash(), m.uuid(), m.text(),
        m.uuid(), m.uuid(), m.opt(m.hash()), m.text(), m.opt(m.uuid()), m.opt(m.hash()),
        m.opt(m.hash()), m.opt(m.hash()))
      m.end()
      val b = new Reader(d.bindingBytes)
      b.domain(MySqlB1CommonGraph.BindingEncoding)
      d.binding = b.opt {
        require(b.int() == 1, "binding singleton differs")
        PhysicalRecoveryBinding(b.uuid(), PhysicalRecoveryCutoverState.fromDatabase(b.text()), b.int())
      }
      b.end()
      val envelope = new Reader(d.repositoryBytes)
      envelope.domain(MySqlB1CommonGraph.Format3RepositoryEncoding)
      d.legacyBytes = envelope.blob()
      val fullLength = envelope.long()
      val fullSha = envelope.blob().toVector
      envelope.end()
      require(fullLength == d.full20.canonicalByteLength && fullSha == d.full20.canonicalSha256,
        "legacy common commitment differs from complete retained full20 DATA")
      d.repository = repository(new Reader(d.legacyBytes))
      d.repository.validate()
      d.result = MySqlB1CommonGraph.captureFormat3(d.manifest, d.binding, d.repository, d.full20, inputs)
      require(d.result.matchesResumeAssertions(d.assertions), "old common components are not canonical byte-for-byte")
      d.result
    catch
      case cause: Throwable =>
        d.failure = cause
        throw cause

  def decodeDeploymentReceiptFull26(d: DeploymentReceiptFull26Draft): MySqlB1CommonGraph =
    require(d != null && !d.started && !d.root.isClosed && (d.snapshot.inputs eq d.inputs),
      "cold Full26 common decoder is once and remains bound to its exact inputs/root")
    d.started = true
    try
      require(d.assertions.manifestEncoding() == MySqlB1CommonGraph.ManifestEncoding &&
        d.assertions.bindingEncoding() == MySqlB1CommonGraph.BindingEncoding &&
        d.assertions.repositoryEncoding() == MySqlB1CommonGraph.DeploymentReceiptFull26RepositoryEncoding,
        "exact deployment-receipt Full26 common component encodings required")
      // The Java DTO accessors clone; charge all three bounded copies before invoking any accessor.
      d.root.reserveRetainedMemory(3L * 65600L, "three complete Full26 common component copies before access")
      d.manifestBytes = d.assertions.manifestBytes()
      d.bindingBytes = d.assertions.bindingBytes()
      d.repositoryBytes = d.assertions.repositoryBytes()
      d.root.reserveRetainedMemory(64L * (d.manifestBytes.length.toLong + d.bindingBytes.length +
        d.repositoryBytes.length) + 65536L, "cold Full26 common structures and commitment reader before decode")
      requireFull26Commitment(d)
      d.result = MySqlB1CommonGraph.reconstructDeploymentReceiptFull26Cold(
        d.snapshot, d.inputs, d.root, d.assertions)
      d.result
    catch
      case cause: Throwable =>
        d.failure = cause
        throw cause

  private def requireFull26Commitment(d: DeploymentReceiptFull26Draft): Unit =
    val envelope = new Reader(d.repositoryBytes)
    envelope.domain(MySqlB1CommonGraph.DeploymentReceiptFull26RepositoryEncoding)
    val schemaFormat = envelope.int()
    val snapshotDomain = envelope.text()
    val schema = envelope.text()
    val ddlSha256 = envelope.text()
    val snapshotLength = envelope.long()
    val snapshotSha256 = envelope.hash()
    envelope.end()
    require(schemaFormat == d.inputs.schemaFormat && snapshotDomain == d.inputs.snapshotDomain &&
      schema == d.inputs.schema && ddlSha256 == d.inputs.ddlSha256,
      "Full26 common commitment domain/schema/DDL differs from exact DATA inputs")
    require(snapshotLength == d.snapshot.canonicalByteLength && snapshotSha256 == d.snapshot.canonicalSha256,
      "Full26 common commitment length or SHA256 differs from the actual decoded snapshot")

  private def repository(r: Reader): MySqlFullRepositorySnapshot =
    r.domain(MySqlB1CommonGraph.RepositoryEncoding)
    val pods = r.rows {
      MySqlStoredRowData(r.blob().toVector, r.opt(r.blob().toVector), r.long(), r.text(),
        r.dbl(), r.dbl(), r.dbl(), r.dbl(), r.dbl(), r.int(), r.long(), r.opt(r.int()), r.text())
    }
    val entitlements = r.rows { PlayerEntitlement(r.uuid(), r.int(), r.int()) }
    val binding = r.opt { PhysicalRecoveryBinding(r.uuid(), PhysicalRecoveryCutoverState.fromDatabase(r.text()), r.int()) }
    val locators = r.rows {
      val pod = r.uuid()
      val attempt = r.uuid()
      val state = PhysicalRecoveryLocatorState.fromDatabase(r.text())
      val reason = r.text()
      val version = r.opt(r.long())
      val dimension = r.opt(r.int())
      val chunks = r.rows { chunk(r) }.toList
      PhysicalRecoveryLocator(pod, attempt, state, reason, version, dimension, chunks)
    }
    val chunks = r.rows { MySqlRecoveryChunkRow(r.uuid(), chunk(r)) }
    val intents = r.rows { intent(r) }
    val bootstrap = r.rows {
      MySqlBootstrapIntentData(r.int(), r.uuid(), r.uuid(), r.uuid(), r.uuid(), r.int(), r.uuid(),
        r.text(), r.blob().toVector, MySqlBootstrapIntentStage.valueOf(r.text()), r.opt(r.blob().toVector))
    }
    r.end()
    val result = MySqlFullRepositorySnapshot(pods, entitlements, MySqlRecoveryGraph(binding, locators, chunks, intents), bootstrap)
    require(result.canonicalBytes == r.bytes.toVector, "legacy repository order/framing differs from unique original encoder")
    result

  private def chunk(r: Reader): PhysicalRecoveryChunk = PhysicalRecoveryChunk(r.int(), r.int(), r.int())
  private def intent(r: Reader): MySqlRecoveryIntentData =
    MySqlRecoveryIntentData(r.uuid(), r.uuid(), r.uuid(), UnboundIntentOrigin.valueOf(r.text()),
      MySqlRecoveryIntentPhase.valueOf(r.text()), r.opt(ObservedUnboundSourceKind.valueOf(r.text())),
      r.opt(r.text()), r.opt(r.blob().toVector), r.opt(r.uuid()), r.opt(r.uuid()), r.long(), r.text(),
      r.dbl(), r.dbl(), r.dbl(), r.dbl(), r.dbl(), r.int(), r.int(), r.int(),
      r.opt(r.long()), r.opt(r.long()), r.opt(r.text()), r.opt(r.dbl()), r.opt(r.dbl()),
      r.opt(r.dbl()), r.opt(r.dbl()), r.opt(r.dbl()), r.opt(r.int()))

  private final class Reader(val bytes: Array[Byte]):
    require(bytes != null && bytes.nonEmpty && bytes.length <= MySqlB1CommonGraph.MaxCanonicalBytes,
      "existing bounded complete common component required")
    private val in = new DataInputStream(new ByteArrayInputStream(bytes))
    def int(): Int = in.readInt()
    def long(): Long = in.readLong()
    def dbl(): Double = java.lang.Double.longBitsToDouble(long())
    def uuid(): UUID = new UUID(long(), long())
    def hash(): Vector[Byte] =
      if in.available() < 32 then throw new EOFException("truncated common SHA256")
      val a = new Array[Byte](32)
      in.readFully(a)
      a.toVector
    def blob(): Array[Byte] =
      val n = int()
      require(n >= 0 && n <= MySqlB1CommonGraph.MaxCanonicalBytes, "bounded common blob length required")
      if n > in.available() then throw new EOFException("truncated common blob")
      val a = new Array[Byte](n)
      in.readFully(a)
      a
    def text(): String =
      StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(blob())).toString
    def domain(expected: String): Unit = require(text() == expected, "common component domain differs")
    def opt[A](body: => A): Option[A] = in.readUnsignedByte() match
      case 0 => None
      case 1 => Some(body)
      case _ => throw new IllegalArgumentException("noncanonical common option tag")
    def rows[A](body: => A): Vector[A] =
      val n = int()
      require(n >= 0 && n <= bytes.length, "bounded common row count required")
      if n > in.available() then throw new EOFException("truncated common rows")
      Vector.tabulate(n)(_ => body)
    def end(): Unit = require(in.available() == 0, "trailing common component bytes")
