package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.B1ResumeRecord
import com.wi.floatingpod.storage.PhysicalRecoveryBinding
import com.wi.floatingpod.storage.PhysicalRecoveryCutoverState
import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.CharBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.security.MessageDigest
import java.util.{Arrays, HexFormat, UUID}

/** 完整 typed facts，不证明调用方曾持有 SQL 锁，不签发 plan/Fresh/settlement/Ready。 */
final class MySqlB1CommonGraph private (
    val manifest: MySqlLockedManifest,
    val binding: Option[PhysicalRecoveryBinding],
    private val repositoryValue: Option[MySqlFullRepositorySnapshot],
    val manifestBytes: Vector[Byte],
    val bindingBytes: Vector[Byte],
    val repositoryBytes: Vector[Byte],
    val canonicalBytes: Vector[Byte],
    val format3Snapshot: Option[MySqlFormat3FullSnapshot],
    private[mysql] val deploymentReceiptSnapshot: Option[MySqlScriptSchemaSnapshot],
    private[mysql] val originalBudget: Option[MySqlFormat3ReadBudget],
    private[mysql] val storageBudget: Option[MySqlFormat3ReadBudget]
):
  require(manifest != null && binding != null && binding.forall(_ != null) && repositoryValue != null &&
    manifestBytes != null && bindingBytes != null && repositoryBytes != null && canonicalBytes != null &&
    format3Snapshot != null && deploymentReceiptSnapshot != null && originalBudget != null && storageBudget != null &&
    !(format3Snapshot.nonEmpty && deploymentReceiptSnapshot.nonEmpty) &&
    (repositoryValue.nonEmpty == deploymentReceiptSnapshot.isEmpty) &&
    (deploymentReceiptSnapshot.nonEmpty == originalBudget.nonEmpty) &&
    (deploymentReceiptSnapshot.nonEmpty == storageBudget.nonEmpty),
    "common graph requires one closed legacy, full20, or Full26 image layout")

  /** Full26 has no legacy repository projection; callers must consume its retained full26 image. */
  def repository: MySqlFullRepositorySnapshot = repositoryValue.getOrElse(
    throw new IllegalStateException("Full26 common graph has no legacy full-repository projection"))

  val manifestSha256: Vector[Byte] = MySqlB1CommonGraph.sha256(manifestBytes)
  val bindingSha256: Vector[Byte] = MySqlB1CommonGraph.sha256(bindingBytes)
  // 直接复用 repository 的既有协议与摘要，不为同一图重建另一种 fingerprint。
  val repositorySha256: Vector[Byte] = format3Snapshot match
    case Some(_) => MySqlB1CommonGraph.sha256(repositoryBytes)
    case None => repositoryValue match
      case Some(value) => value.canonicalSha256
      case None => MySqlB1CommonGraph.sha256(repositoryBytes)
  val canonicalSha256: Vector[Byte] = MySqlB1CommonGraph.sha256(canonicalBytes)

  /** 摘要永远不能替代完整 typed/components 比较；repo按其既有规范化排序比较。 */
  def exactlyEquals(other: MySqlB1CommonGraph): Boolean =
    if other == null || manifest != other.manifest || binding != other.binding ||
      manifestBytes != other.manifestBytes || bindingBytes != other.bindingBytes ||
      repositoryBytes != other.repositoryBytes || canonicalBytes != other.canonicalBytes then false
    else
      val sameRepository = (repositoryValue, other.repositoryValue) match
        case (None, None) => true
        case (Some(left), Some(right)) => left.exactlyEquals(right)
        case _ => false
      val sameFull20 = (format3Snapshot, other.format3Snapshot) match
        case (None, None) => true
        case (Some(left), Some(right)) => left.exactlyEquals(right)
        case _ => false
      val sameFull26 = (deploymentReceiptSnapshot, other.deploymentReceiptSnapshot) match
        case (None, None) => true
        case (Some(left), Some(right)) => left.exactlyEquals(right)
        case _ => false
      sameRepository && sameFull20 && sameFull26

  /** 显式协议适配：三份完整 bytes，绝不只发送SHA；Java DTO自身再做防御性复制。 */
  def toResumeAssertions: B1ResumeRecord.CommonGraphAssertions =
    val repositoryEncoding = MySqlB1CommonGraph.repositoryEncoding(format3Snapshot, deploymentReceiptSnapshot)
    storageBudget match
      case None => new B1ResumeRecord.CommonGraphAssertions(MySqlB1CommonGraph.ManifestEncoding, manifestBytes.toArray,
        MySqlB1CommonGraph.BindingEncoding, bindingBytes.toArray, repositoryEncoding, repositoryBytes.toArray)
      case Some(owner) =>
        val root = originalBudget.getOrElse(throw new IllegalStateException("Full26 common graph lost its original root"))
        MySqlB1CommonGraph.requireFull26Owner(root, owner, deploymentReceiptSnapshot.get.inputs)
        val arrays = Math.addExact(manifestBytes.size.toLong,
          Math.addExact(bindingBytes.size.toLong, repositoryBytes.size.toLong))
        root.reserveTemporaryMemory(arrays, "Full26 resume assertion argument arrays before defensive cloning")
        var primary: Throwable = null
        var retained = false
        var complete = false
        try
          root.reserveRetainedMemory(Math.addExact(arrays, 256L), "Full26 resume assertion retained component clones before construction")
          retained = true
          val result = new B1ResumeRecord.CommonGraphAssertions(MySqlB1CommonGraph.ManifestEncoding, manifestBytes.toArray,
            MySqlB1CommonGraph.BindingEncoding, bindingBytes.toArray, repositoryEncoding, repositoryBytes.toArray)
          complete = true
          result
        catch
          case failure: Throwable =>
            primary = failure
            throw failure
        finally
          MySqlFormat3BudgetCleanup.run(primary, Vector(
            () => root.releaseTemporaryMemory(arrays, "Full26 resume assertion argument arrays"),
            () => if retained && !complete then root.releaseRetainedMemory(Math.addExact(arrays, 256L),
              "failed Full26 resume assertion retained component clones")))

  def matchesResumeAssertions(value: B1ResumeRecord.CommonGraphAssertions): Boolean =
    val repositoryEncoding = MySqlB1CommonGraph.repositoryEncoding(format3Snapshot, deploymentReceiptSnapshot)
    if value == null then false
    else storageBudget match
      case None => value.manifestEncoding() == MySqlB1CommonGraph.ManifestEncoding &&
        value.bindingEncoding() == MySqlB1CommonGraph.BindingEncoding && value.repositoryEncoding() == repositoryEncoding &&
        Arrays.equals(manifestBytes.toArray, value.manifestBytes()) &&
        Arrays.equals(bindingBytes.toArray, value.bindingBytes()) && Arrays.equals(repositoryBytes.toArray, value.repositoryBytes())
      case Some(owner) =>
        val root = originalBudget.getOrElse(throw new IllegalStateException("Full26 common graph lost its original root"))
        MySqlB1CommonGraph.requireFull26Owner(root, owner, deploymentReceiptSnapshot.get.inputs)
        val cloned = Math.addExact(64L, Math.addExact(manifestBytes.size.toLong,
          Math.addExact(bindingBytes.size.toLong, repositoryBytes.size.toLong)))
        root.reserveTemporaryMemory(cloned, "Full26 resume assertion getter clones before comparison")
        var primary: Throwable = null
        try
          value.manifestEncoding() == MySqlB1CommonGraph.ManifestEncoding &&
            value.bindingEncoding() == MySqlB1CommonGraph.BindingEncoding && value.repositoryEncoding() == repositoryEncoding &&
            MySqlB1CommonGraph.equalsBytes(manifestBytes, value.manifestBytes()) &&
            MySqlB1CommonGraph.equalsBytes(bindingBytes, value.bindingBytes()) &&
            MySqlB1CommonGraph.equalsBytes(repositoryBytes, value.repositoryBytes())
        catch
          case failure: Throwable =>
            primary = failure
            throw failure
        finally
          MySqlFormat3BudgetCleanup.run(primary, Vector(
            () => root.releaseTemporaryMemory(cloned, "Full26 resume assertion getter clones")))

  override def toString: String = "MySqlB1CommonGraph[complete facts; no lock or admission proof]"

object MySqlB1CommonGraph:
  val ManifestEncoding = "floatingpod.mysql.b1-manifest/v1"
  val BindingEncoding = "floatingpod.mysql.b1-binding/v1"
  val RepositoryEncoding = "floatingpod.mysql.full-repository-snapshot/v1"
  val Format3RepositoryEncoding = "floatingpod.mysql.format3-full20-repository-commitment/v1"
  val DeploymentReceiptFull26RepositoryEncoding =
    "floatingpod.mysql.script-publication1-deployment-receipt1-repository-commitment/v1"
  val CommonEncoding = "floatingpod.mysql.b1-common-graph/v1"
  val MaxCanonicalBytes = 65536

  private[mysql] enum DeploymentReceiptFull26Selection:
    case Before, Target

  def capture(manifest: MySqlLockedManifest, binding: Option[PhysicalRecoveryBinding],
      repository: MySqlFullRepositorySnapshot): MySqlB1CommonGraph =
    require(manifest != null && binding != null && binding.forall(_ != null) && repository != null,
      "complete non-null typed common graph required")
    require(repository.recovery.binding == binding, "independent binding differs from full repository binding")
    repository.validate()
    val m = encode { out => writeManifest(out, manifest) }
    val b = encode { out => writeBinding(out, binding) }
    val repo = repository.canonicalBytes
    bounded(repo)
    val all = encode { out =>
      text(out, CommonEncoding)
      data(out, m); data(out, b); text(out, RepositoryEncoding); data(out, repo)
    }
    new MySqlB1CommonGraph(manifest, binding, Some(repository), m, b, repo, all, None, None, None, None)

  /**
   * Format 3 retains and compares every typed row from all twenty relations. The durable B1
   * witness carries the canonical full20 digest and exact byte length alongside the legacy
   * typed transition projection; a digest never replaces in-memory full-row comparison.
   */
  def captureFormat3(manifest: MySqlLockedManifest, binding: Option[PhysicalRecoveryBinding],
      repository: MySqlFullRepositorySnapshot, snapshot: MySqlFormat3FullSnapshot,
      inputs: MySqlFormat3AuditInputs): MySqlB1CommonGraph =
    require(snapshot != null && inputs != null, "actual full format3 snapshot and independent inputs required")
    val legacy = capture(manifest, binding, repository)
    MySqlFreshFormat3Graph.requireManifestMatches(snapshot, manifest, inputs)
    MySqlFreshFormat3Graph.requireBindingMatches(snapshot, binding)
    MySqlFreshFormat3Graph.requireBootstrapIntentsMatch(snapshot, repository.bootstrapIntents)
    val snapshotHash = snapshot.canonicalSha256
    val snapshotLength = snapshot.canonicalByteLength
    val fullRepository = encode { out =>
      text(out, Format3RepositoryEncoding)
      data(out, repository.canonicalBytes)
      out.writeLong(snapshotLength)
      data(out, snapshotHash)
    }
    val common = encode { out =>
      text(out, CommonEncoding)
      data(out, legacy.manifestBytes); data(out, legacy.bindingBytes)
      text(out, Format3RepositoryEncoding); data(out, fullRepository)
    }
    new MySqlB1CommonGraph(manifest, binding, Some(repository), legacy.manifestBytes,
      legacy.bindingBytes, fullRepository, common, Some(snapshot), None, None, None)

  /** Live Full26 capture is bound to one sealed B1 DATA object and an explicit image choice. */
  private[mysql] def captureDeploymentReceiptFull26(data: MySqlScriptSchemaB1Graph.Data,
      selection: DeploymentReceiptFull26Selection): MySqlB1CommonGraph =
    require(data != null && selection != null, "actual Full26 B1 DATA and a closed before/target selection are required")
    val root = data.originalBudget
    val inputs = data.inputs
    requireFull26Root(root, inputs)
    require(inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      !data.storageBudget.isClosed && data.storageBudget.isCanonicalChildOf(root),
      "Full26 B1 DATA requires its open original root and retained canonical transition child")
    root.requireScriptDataInputs(inputs)
    data.storageBudget.requireScriptDataInputs(inputs)
    requireFull26SnapshotOwner(data.beforeSnapshot, inputs, root)
    requireFull26SnapshotOwner(data.targetSnapshot, inputs, root)
    data.kind match
      case MySqlScriptSchemaB1Graph.TransitionKind.FreshGenesis =>
        MySqlScriptSchemaB1Graph.requireDeploymentReceiptFull26FreshGenesisTransition(data.beforeSnapshot, data.targetSnapshot)
      case MySqlScriptSchemaB1Graph.TransitionKind.OrdinaryWriterHandoff =>
        MySqlScriptSchemaB1Graph.requireDeploymentReceiptFull26OnlyWriterChanged(data.beforeSnapshot, data.targetSnapshot)
    val snapshot = selection match
      case DeploymentReceiptFull26Selection.Before => data.beforeSnapshot
      case DeploymentReceiptFull26Selection.Target => data.targetSnapshot
    requireFull26SnapshotOwner(snapshot, inputs, root)
    captureDeploymentReceiptFull26Image(snapshot, inputs, root, None)

  /** 完整 Full26 image DATA；不替代 certificate-taking 入口的 phase 校验或发出 admission。 */
  private[mysql] def captureDeploymentReceiptFull26Data(snapshot: MySqlScriptSchemaSnapshot,
      expectedInputs: MySqlScriptSchemaInputs, originalBudget: MySqlFormat3ReadBudget): MySqlB1CommonGraph =
    requireFull26SnapshotOwner(snapshot, expectedInputs, originalBudget)
    captureDeploymentReceiptFull26Image(snapshot, expectedInputs, originalBudget, None)

  /** Cold DATA reconstruction owns only this decoded image; it does not recreate a live B1 certificate. */
  private[mysql] def reconstructDeploymentReceiptFull26Cold(snapshot: MySqlScriptSchemaSnapshot,
      expectedInputs: MySqlScriptSchemaInputs, originalBudget: MySqlFormat3ReadBudget,
      assertions: B1ResumeRecord.CommonGraphAssertions): MySqlB1CommonGraph =
    require(assertions != null, "bounded complete common assertions are required for cold Full26 DATA")
    requireFull26SnapshotOwner(snapshot, expectedInputs, originalBudget)
    originalBudget.reserveTemporaryMemory(32L, "Full26 cold assertion reference before local option allocation")
    var primary: Throwable = null
    try captureDeploymentReceiptFull26Image(snapshot, expectedInputs, originalBudget, Some(assertions))
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => originalBudget.releaseTemporaryMemory(32L, "Full26 cold assertion local option reference")))

  private def captureDeploymentReceiptFull26Image(snapshot: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget,
      expectedAssertions: Option[B1ResumeRecord.CommonGraphAssertions]): MySqlB1CommonGraph =
    requireFull26SnapshotOwner(snapshot, inputs, root)
    // lazy fingerprint 归 image owner 计费；先求值，再进入 graph 的兄弟 scope。
    val snapshotLength = snapshot.canonicalByteLength
    val snapshotSha256 = snapshot.canonicalSha256
    inDataScope(root) { child =>
      child.reserveRetainedMemory(16_384L, "Full26 common graph typed controls and holder structures before construction")
      val (manifest, binding) = deploymentReceiptFacts(snapshot, inputs)
      val manifestBytes = encodeData(child, "Full26 manifest component") { out => writeManifest(out, manifest) }
      val bindingBytes = encodeData(child, "Full26 physical binding component") { out => writeBinding(out, binding) }
      val repositoryBytes = encodeData(child, "Full26 repository commitment component") { out =>
        text(out, DeploymentReceiptFull26RepositoryEncoding)
        out.writeInt(inputs.schemaFormat)
        text(out, inputs.snapshotDomain)
        text(out, snapshot.schema)
        text(out, inputs.ddlSha256)
        out.writeLong(snapshotLength)
        hashNoClone(out, snapshotSha256)
      }
      val canonicalBytes = encodeData(child, "Full26 common graph component") { out =>
        text(out, CommonEncoding)
        dataNoClone(out, manifestBytes); dataNoClone(out, bindingBytes)
        text(out, DeploymentReceiptFull26RepositoryEncoding); dataNoClone(out, repositoryBytes)
      }
      // Four SHA result vectors plus the retained graph/Options are precharged before hashing or holder allocation.
      child.reserveRetainedMemory(4L * 640L + 1024L, "Full26 common graph digest results and final holder before allocation")
      val graph = new MySqlB1CommonGraph(manifest, binding, None, manifestBytes, bindingBytes, repositoryBytes,
        canonicalBytes, None, Some(snapshot), Some(root), Some(child))
      expectedAssertions.foreach { assertions =>
        if !graph.matchesResumeAssertions(assertions) then
          throw new IllegalArgumentException("Full26 common components are not canonical byte-for-byte for the actual cold DATA image")
      }
      graph
    }

  private def deploymentReceiptFacts(snapshot: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs): (MySqlLockedManifest, Option[PhysicalRecoveryBinding]) =
    val manifestRow = onlyRow(snapshot, "fp_schema_manifest")
    val expected = MySqlLockedManifest(
      intCell(snapshot, "fp_schema_manifest", manifestRow, "singleton_fixed1"),
      textCell(snapshot, "fp_schema_manifest", manifestRow, "backend"),
      intCell(snapshot, "fp_schema_manifest", manifestRow, "format_version"),
      hashCell(snapshot, "fp_schema_manifest", manifestRow, "ddl_sha256"),
      uuidCell(snapshot, "fp_schema_manifest", manifestRow, "provisioning_uuid"),
      textCell(snapshot, "fp_schema_manifest", manifestRow, "bootstrap_state"),
      uuidCell(snapshot, "fp_schema_manifest", manifestRow, "expected_server_uuid"),
      uuidCell(snapshot, "fp_schema_manifest", manifestRow, "expected_binding_uuid"),
      optionalHashCell(snapshot, "fp_schema_manifest", manifestRow, "import_manifest_sha256"),
      textCell(snapshot, "fp_schema_manifest", manifestRow, "import_state"),
      optionalUuidCell(snapshot, "fp_schema_manifest", manifestRow, "import_operation_uuid"),
      optionalHashCell(snapshot, "fp_schema_manifest", manifestRow, "import_snapshot_sha256"),
      optionalHashCell(snapshot, "fp_schema_manifest", manifestRow, "import_receipt_sha256"),
      optionalHashCell(snapshot, "fp_schema_manifest", manifestRow, "guard_manifest_sha256"))
    if expected.singleton != 1 || expected.backend != "MYSQL" || expected.formatVersion != inputs.schemaFormat ||
      expected.ddlSha256 != HexFormat.of().parseHex(inputs.ddlSha256).toVector ||
      expected.serverUuid != snapshot.connection.serverUuid || expected.bindingUuid == new UUID(0L, 0L) then
      throw new java.sql.SQLDataException("Full26 manifest does not correspond to its exact DATA schema/DDL/server controls")

    val bindingRows = snapshot.relation("physical_recovery_binding").rows
    val intentRows = snapshot.relation("binding_bootstrap_intent").rows
    val binding =
      if bindingRows.isEmpty && intentRows.isEmpty then None
      else
        if bindingRows.isEmpty || intentRows.isEmpty then
          throw new java.sql.SQLDataException("Full26 B/B1 must be absent together or both complete")
        val bindingRow = onlyRow(snapshot, "physical_recovery_binding")
        val binding = PhysicalRecoveryBinding(
          uuidCell(snapshot, "physical_recovery_binding", bindingRow, "binding_uuid"),
          PhysicalRecoveryCutoverState.fromDatabase(textCell(snapshot, "physical_recovery_binding", bindingRow, "cutover_state")),
          intCell(snapshot, "physical_recovery_binding", bindingRow, "format_version"))
        if intCell(snapshot, "physical_recovery_binding", bindingRow, "singleton_fixed1") != 1 ||
          binding.formatVersion != MySqlDeploymentReceiptSchemaDefinition.PhysicalBindingFormat ||
          binding.bindingUuid != expected.bindingUuid then
          throw new java.sql.SQLDataException("Full26 physical binding does not correspond to the manifest binding")

        val intent = onlyRow(snapshot, "binding_bootstrap_intent")
        if intCell(snapshot, "binding_bootstrap_intent", intent, "singleton_fixed1") != 1 ||
          intCell(snapshot, "binding_bootstrap_intent", intent, "binding_format_version") != binding.formatVersion ||
          uuidCell(snapshot, "binding_bootstrap_intent", intent, "binding_uuid") != binding.bindingUuid ||
          uuidCell(snapshot, "binding_bootstrap_intent", intent, "provisioning_uuid") != expected.provisioningUuid then
          throw new java.sql.SQLDataException("Full26 B1 bootstrap intent does not correspond to the manifest and binding")
        Some(binding)
    val writer = onlyRow(snapshot, "fp_runtime_writer")
    if intCell(snapshot, "fp_runtime_writer", writer, "singleton_fixed1") != 1 ||
      uuidCell(snapshot, "fp_runtime_writer", writer, "binding_uuid") != expected.bindingUuid then
      throw new java.sql.SQLDataException("Full26 runtime writer does not correspond to the manifest binding")
    expected -> binding

  private def onlyRow(snapshot: MySqlScriptSchemaSnapshot, table: String): Vector[MySqlFormat3Cell] =
    snapshot.relation(table).rows match
      case Vector(row) => row
      case _ => throw new java.sql.SQLDataException(s"Full26 common graph requires one typed $table control row")

  private def cell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): MySqlFormat3Cell =
    val index = snapshot.inputs.forTable(table).columns.indexWhere(_.name == column)
    if index < 0 || row.size != snapshot.inputs.forTable(table).columns.size then
      throw new java.sql.SQLDataException(s"Full26 common graph has an incomplete $table.$column control shape")
    row(index)

  private def intCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): Int = cell(snapshot, table, row, column) match
    case MySqlFormat3Cell.SignedInteger(value) => Math.toIntExact(value)
    case _ => throw new java.sql.SQLDataException(s"Full26 common graph requires integer $table.$column")

  private def textCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): String = cell(snapshot, table, row, column) match
    case MySqlFormat3Cell.Text(value) => value
    case _ => throw new java.sql.SQLDataException(s"Full26 common graph requires text $table.$column")

  private def binaryCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): MySqlFormat3Binary = cell(snapshot, table, row, column) match
    case MySqlFormat3Cell.Binary(value) => value
    case _ => throw new java.sql.SQLDataException(s"Full26 common graph requires binary $table.$column")

  private def hashCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): Vector[Byte] =
    val value = binaryCell(snapshot, table, row, column)
    if value.length != 32 then throw new java.sql.SQLDataException(s"Full26 common graph requires SHA256 $table.$column")
    Vector.tabulate(32)(value.byteAt)

  private def optionalHashCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): Option[Vector[Byte]] = cell(snapshot, table, row, column) match
    case MySqlFormat3Cell.SqlNull => None
    case MySqlFormat3Cell.Binary(value) if value.length == 32 => Some(Vector.tabulate(32)(value.byteAt))
    case _ => throw new java.sql.SQLDataException(s"Full26 common graph requires SQL NULL or SHA256 $table.$column")

  private def uuidCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): UUID =
    val value = binaryCell(snapshot, table, row, column)
    if value.length != 16 then throw new java.sql.SQLDataException(s"Full26 common graph requires UUID $table.$column")
    def word(offset: Int): Long =
      var result = 0L
      var index = 0
      while index < 8 do
        result = (result << 8) | (value.byteAt(offset + index).toLong & 0xffL)
        index += 1
      result
    new UUID(word(0), word(8))

  private def optionalUuidCell(snapshot: MySqlScriptSchemaSnapshot, table: String,
      row: Vector[MySqlFormat3Cell], column: String): Option[UUID] = cell(snapshot, table, row, column) match
    case MySqlFormat3Cell.SqlNull => None
    case MySqlFormat3Cell.Binary(value) if value.length == 16 =>
      def word(offset: Int): Long =
        var result = 0L
        var index = 0
        while index < 8 do
          result = (result << 8) | (value.byteAt(offset + index).toLong & 0xffL)
          index += 1
        result
      Some(new UUID(word(0), word(8)))
    case _ => throw new java.sql.SQLDataException(s"Full26 common graph requires SQL NULL or UUID $table.$column")

  private[mysql] def requireFull26Owner(root: MySqlFormat3ReadBudget,
      owner: MySqlFormat3ReadBudget, inputs: MySqlScriptSchemaInputs): Unit =
    requireFull26Root(root, inputs)
    require(owner != null && !owner.isClosed && owner.sharesAllocationLedger(root) &&
      ((owner eq root) || owner.isCanonicalChildOf(root)),
      "Full26 common graph requires its open exact original root or canonical DATA child")
    owner.requireScriptDataInputs(inputs)

  private def requireFull26Root(root: MySqlFormat3ReadBudget, inputs: MySqlScriptSchemaInputs): Unit =
    require(inputs != null && inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26 &&
      root != null && !root.isClosed && !root.isScopedReadBudget && (root.profile eq inputs.sourceLimitProfile),
      "Full26 common graph requires its exact open original DATA root and explicit inputs")
    root.requireScriptDataInputs(inputs)

  private def requireFull26SnapshotOwner(snapshot: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs, root: MySqlFormat3ReadBudget): Unit =
    requireFull26Root(root, inputs)
    require(snapshot != null && (snapshot.inputs eq inputs) && snapshot.schema == inputs.schema &&
      snapshot.inputs.dataLayout == MySqlScriptSchemaDataLayout.DeploymentReceiptFull26,
      "exact Full26 DATA snapshot and input identity are required")
    requireFull26Owner(root, snapshot.allocationBudget, inputs)
    require(snapshot.connection.catalog == inputs.schema && snapshot.connection.serverUuid != null &&
      snapshot.maxAllowedPacketBytes > 0L, "Full26 common graph requires the actual bounded snapshot observation")

  private def repositoryEncoding(full20: Option[MySqlFormat3FullSnapshot],
      full26: Option[MySqlScriptSchemaSnapshot]): String =
    if full26.nonEmpty then DeploymentReceiptFull26RepositoryEncoding
    else if full20.nonEmpty then Format3RepositoryEncoding
    else RepositoryEncoding

  private[mysql] def equalsBytes(expected: Vector[Byte], actual: Array[Byte]): Boolean =
    if expected == null || actual == null || expected.size != actual.length then false
    else
      var index = 0
      while index < actual.length do
        if expected(index) != actual(index) then return false
        index += 1
      true

  private def writeManifest(out: DataOutputStream, manifest: MySqlLockedManifest): Unit =
    text(out, ManifestEncoding)
    out.writeInt(manifest.singleton); text(out, manifest.backend); out.writeInt(manifest.formatVersion)
    hash(out, manifest.ddlSha256); uuid(out, manifest.provisioningUuid); text(out, manifest.bootstrapState)
    uuid(out, manifest.serverUuid); uuid(out, manifest.bindingUuid)
    optional(out, manifest.importManifestSha256)(hash(out, _)); text(out, manifest.importState)
    optional(out, manifest.importOperationUuid)(uuid(out, _))
    optional(out, manifest.importSnapshotSha256)(hash(out, _))
    optional(out, manifest.importReceiptSha256)(hash(out, _))
    optional(out, manifest.guardManifestSha256)(hash(out, _))

  private def writeBinding(out: DataOutputStream, binding: Option[PhysicalRecoveryBinding]): Unit =
    text(out, BindingEncoding)
    optional(out, binding) { row =>
      // PhysicalRecoveryBinding omits the fixed singleton SQL column; the complete component retains it.
      out.writeInt(1); uuid(out, row.bindingUuid); text(out, row.cutoverState.databaseValue); out.writeInt(row.formatVersion)
    }

  private def encodeData(root: MySqlFormat3ReadBudget, context: String)
      (body: DataOutputStream => Unit): Vector[Byte] =
    root.reserveTemporaryMemory(4096L, s"$context counting/UTF-8 writer workspace")
    var primary: Throwable = null
    try
      val counter = new MySqlFormat3CountingOutputStream
      val counted = new DataOutputStream(counter)
      body(counted); counted.flush()
      val length = counter.byteLength
      if length <= 0L || length > MaxCanonicalBytes then
        throw new java.sql.SQLDataException(s"$context exceeds the explicit bounded common component")
      val size = MySqlFormat3JvmArrayLimit.requireLength(length, s"$context exact byte array")
      root.reserveRetainedMemory(Math.addExact(Math.multiplyExact(length, 32L), 256L), s"$context final array/vector before allocation")
      val bytes = new Array[Byte](size)
      val sink = new CommonExactOutput(bytes)
      val output = new DataOutputStream(sink)
      body(output); output.flush()
      sink.finish().toVector
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      MySqlFormat3BudgetCleanup.run(primary, Vector(
        () => root.releaseTemporaryMemory(4096L, s"$context counting/UTF-8 writer workspace")))

  private def dataNoClone(out: DataOutputStream, bytes: Vector[Byte]): Unit =
    bounded(bytes); out.writeInt(bytes.size)
    var index = 0
    while index < bytes.size do
      out.writeByte(bytes(index)); index += 1

  private def hashNoClone(out: DataOutputStream, value: Vector[Byte]): Unit =
    require(value != null && value.size == 32, "exact manifest SHA256 required")
    var index = 0
    while index < value.size do
      out.writeByte(value(index)); index += 1

  private final class CommonExactOutput(bytes: Array[Byte]) extends java.io.OutputStream:
    private var offset = 0
    override def write(value: Int): Unit =
      if offset >= bytes.length then throw new java.sql.SQLDataException("Full26 common exact output overrun")
      bytes(offset) = value.toByte; offset += 1
    override def write(value: Array[Byte], start: Int, length: Int): Unit =
      java.util.Objects.checkFromIndexSize(start, length, value.length)
      if length > bytes.length - offset then throw new java.sql.SQLDataException("Full26 common exact output overrun")
      System.arraycopy(value, start, bytes, offset, length); offset += length
    def finish(): Array[Byte] =
      if offset != bytes.length then throw new java.sql.SQLDataException("Full26 common exact output underrun")
      bytes

  private def inDataScope[A](root: MySqlFormat3ReadBudget)(body: MySqlFormat3ReadBudget => A): A =
    MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      val child = root.newCanonicalDataScope()
      try MySqlRecoveryCanonicalBytes.withAllocationBudget(child) { body(child) }
      catch
        case failure: Throwable =>
          try child.closeScopedReadBudget()
          catch case cleanup: Throwable => if cleanup ne failure then failure.addSuppressed(cleanup)
          throw failure
    }

  private[mysql] def sha256(bytes: Vector[Byte]): Vector[Byte] =
    val digest = MessageDigest.getInstance("SHA-256")
    bytes.foreach(value => digest.update(value))
    digest.digest().toVector
  private def bounded(bytes: Vector[Byte]): Unit =
    require(bytes != null && bytes.nonEmpty && bytes.size <= MaxCanonicalBytes, "complete bounded canonical component required")
  private def encode(body: DataOutputStream => Unit): Vector[Byte] =
    val buffer = new ByteArrayOutputStream()
    val out = new DataOutputStream(buffer)
    try
      body(out); out.flush()
      val result = buffer.toByteArray.toVector
      bounded(result)
      result
    finally out.close() // memory-only stream; no JDBC/native ownership or durability acknowledgement
  private def data(out: DataOutputStream, bytes: Vector[Byte]): Unit =
    bounded(bytes); out.writeInt(bytes.size); out.write(bytes.toArray)
  private def text(out: DataOutputStream, value: String): Unit =
    require(value != null, "exact manifest text required")
    val encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(value))
    val bytes = new Array[Byte](encoded.remaining()); encoded.get(bytes)
    // 空字符串作为完整SQL scalar仍与NULL有区别；是否符合manifest语义由planner核验。
    require(bytes.length <= MaxCanonicalBytes, "canonical text bound exceeded")
    out.writeInt(bytes.length); out.write(bytes)
  private def uuid(out: DataOutputStream, value: UUID): Unit =
    require(value != null, "exact manifest UUID required")
    out.writeLong(value.getMostSignificantBits); out.writeLong(value.getLeastSignificantBits)
  private def hash(out: DataOutputStream, value: Vector[Byte]): Unit =
    require(value != null && value.size == 32, "exact manifest SHA256 required")
    out.write(value.toArray)
  private def optional[A](out: DataOutputStream, value: Option[A])(write: A => Unit): Unit =
    require(value != null && value.forall(_ != null), "explicit non-null SQL Option required")
    value match
      case None => out.writeByte(0)
      case Some(v) => out.writeByte(1); write(v)
