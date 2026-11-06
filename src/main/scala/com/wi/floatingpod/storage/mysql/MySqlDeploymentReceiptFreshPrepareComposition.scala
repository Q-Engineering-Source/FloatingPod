package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{DbFirstWorldAdoptionPlanData, FirstBindingPlanData, WindowsWorldProvisionReceiptDataCodec}
import java.io.{DataOutputStream, OutputStream}
import java.security.MessageDigest
import java.sql.SQLDataException
import java.util.UUID

/** 完整 PREPARE 最终图 DATA。中间三表效果只用于构造，不是额外 SQL 提交或资格。 */
private[mysql] object MySqlDeploymentReceiptFreshPrepareComposition:
  private val Domain = "floatingpod.mysql.script-publication1-deployment-receipt1-fresh-prepare-composition-effect/v1"
  private val DbFirstDomain = "floatingpod.mysql.db-first-full27-fresh-prepare-composition-effect/v1"
  private val Catalog = "fp_script_catalog"
  private val Writer = "fp_runtime_writer"
  private val Binding = "physical_recovery_binding"
  private val Intent = "binding_bootstrap_intent"

  /** 返回的Full26图自有所有行字节；caller 负责关闭其 allocationBudget，原 root/before 不由此关闭。 */
  def prepareTarget(before: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget, plan: WindowsWorldProvisionReceiptDataCodec.PlanData,
      sourceWorldIdentity: String, provisionReceiptSha256: Vector[Byte],
      beforeWriter: MySqlDeploymentReceiptBootstrapGraph.PrepareBeforeWriter, targetWriter: MySqlFreshFormat3WriterTarget): MySqlScriptSchemaSnapshot =
    prepareTargetKernel(before,inputs,originalRoot,plan,sourceWorldIdentity,provisionReceiptSha256,targetWriter,
      MySqlScriptSchemaDataLayout.DeploymentReceiptFull26,Domain,
      () => MySqlDeploymentReceiptBootstrapGraph.prepareBeforeEpoch(beforeWriter),
      () => MySqlDeploymentReceiptBootstrapGraph.prepareTarget(before,inputs,originalRoot,plan,
        sourceWorldIdentity,provisionReceiptSha256,beforeWriter,targetWriter),
      (oldImage,newImage) => MySqlScriptSchemaB1Graph.requireDeploymentReceiptFull26FreshGenesisTransition(oldImage,newImage))

  /** Schema6 Full27 PREPARE has its own plan type, bootstrap family, and effect domain. */
  def prepareDbFirstTarget(before: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget, plan: DbFirstWorldAdoptionPlanData,
      sourceWorldIdentity: String, provisionReceiptSha256: Vector[Byte],
      beforeWriter: MySqlDbFirstBootstrapGraph.PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget,
      expectedQuotaMaximumRecords: Long): MySqlScriptSchemaSnapshot =
    if expectedQuotaMaximumRecords <= 0L then reject("schema6 PREPARE requires the explicit positive quota maximum")
    prepareTargetKernel(before,inputs,originalRoot,plan,sourceWorldIdentity,provisionReceiptSha256,targetWriter,
      MySqlScriptSchemaDataLayout.DbFirstFull27,DbFirstDomain,
      () => MySqlDbFirstBootstrapGraph.prepareBeforeEpoch(beforeWriter),
      () => MySqlDbFirstBootstrapGraph.prepareTarget(before,inputs,originalRoot,plan,
        sourceWorldIdentity,provisionReceiptSha256,beforeWriter,targetWriter,expectedQuotaMaximumRecords),
      (oldImage,newImage) => MySqlScriptSchemaB1Graph.requireDbFirstFull27FreshGenesisTransition(
        oldImage,newImage,expectedQuotaMaximumRecords))

  private type GenesisValidator = (MySqlScriptSchemaSnapshot,MySqlScriptSchemaSnapshot) => Unit

  private def prepareTargetKernel(before: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      originalRoot: MySqlFormat3ReadBudget, plan: FirstBindingPlanData, sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte], targetWriter: MySqlFreshFormat3WriterTarget,
      layout: MySqlScriptSchemaDataLayout, domain: String, beforeEpoch: () => Long,
      bootstrapTarget: () => MySqlScriptSchemaSnapshot, validateCatalogGenesis: GenesisValidator): MySqlScriptSchemaSnapshot =
    requireOwner(before,inputs,originalRoot,layout)
    if before.relation(Catalog).rows.nonEmpty then reject("composite PREPARE requires its actual absent catalog")
    // 原 image 的 lazy canonical identity 必须在进入新 sibling child 前求值。
    val beforeLength = before.canonicalByteLength
    val beforeSha = before.canonicalSha256
    var intermediate: MySqlScriptSchemaSnapshot = null
    var child: MySqlFormat3ReadBudget = null
    var result: MySqlScriptSchemaSnapshot = null
    var complete = false
    var primary: Throwable = null
    originalRoot.reserveTemporaryMemory(65536L,"composite PREPARE registered control/validation workspace")
    try
      intermediate = bootstrapTarget()
      child = originalRoot.newCanonicalDataScope()
      child.bindCanonicalDataPacketLimit(before.maxAllowedPacketBytes)
      result = MySqlRecoveryCanonicalBytes.withAllocationBudget(child) {
        compose(intermediate,inputs,child,plan,sourceWorldIdentity,provisionReceiptSha256,targetWriter,
          domain,validateCatalogGenesis,beforeEpoch(),beforeLength,beforeSha)
      }
      complete = true
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      val intermediateBudget = if intermediate == null then null else intermediate.allocationBudget
      intermediate = null // 重图引用先丢弃，之后只持 cleanup budget。
      try
        MySqlFormat3BudgetCleanup.run(primary,Vector(
          () => if intermediateBudget != null then intermediateBudget.closeScopedReadBudget(),
          () => if !complete && child != null then child.closeScopedReadBudget(),
          () => originalRoot.releaseTemporaryMemory(65536L,"composite PREPARE control/validation workspace")))
      catch
        case cleanup: Throwable =>
          result = null
          if complete && child != null then
            try child.closeScopedReadBudget()
            catch case failedDrop: Throwable => if failedDrop ne cleanup then cleanup.addSuppressed(failedDrop)
          throw cleanup
    result

  /** 精确重建期望 Full26 DATA 后比较全图；不签发私有 plan、Fresh、SQL 或 Ready。 */
  def requirePrepareTransition(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs, originalRoot: MySqlFormat3ReadBudget,
      plan: WindowsWorldProvisionReceiptDataCodec.PlanData, sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte], beforeWriter: MySqlDeploymentReceiptBootstrapGraph.PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget): Unit =
    requirePrepareTransitionKernel(before,target,inputs,originalRoot,MySqlScriptSchemaDataLayout.DeploymentReceiptFull26,
      () => prepareTarget(before,inputs,originalRoot,plan,sourceWorldIdentity,provisionReceiptSha256,beforeWriter,targetWriter))

  def requireDbFirstPrepareTransition(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs, originalRoot: MySqlFormat3ReadBudget,
      plan: DbFirstWorldAdoptionPlanData, sourceWorldIdentity: String,
      provisionReceiptSha256: Vector[Byte], beforeWriter: MySqlDbFirstBootstrapGraph.PrepareBeforeWriter,
      targetWriter: MySqlFreshFormat3WriterTarget,
      expectedQuotaMaximumRecords: Long): Unit =
    requirePrepareTransitionKernel(before,target,inputs,originalRoot,MySqlScriptSchemaDataLayout.DbFirstFull27,
      () => prepareDbFirstTarget(before,inputs,originalRoot,plan,sourceWorldIdentity,provisionReceiptSha256,
        beforeWriter,targetWriter,expectedQuotaMaximumRecords))

  private def requirePrepareTransitionKernel(before: MySqlScriptSchemaSnapshot, target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs, originalRoot: MySqlFormat3ReadBudget,
      layout: MySqlScriptSchemaDataLayout, expectedTarget: () => MySqlScriptSchemaSnapshot): Unit =
    requireOwner(before,inputs,originalRoot,layout); requireOwner(target,inputs,originalRoot,layout)
    target.canonicalByteLength; target.canonicalSha256
    var expected: MySqlScriptSchemaSnapshot = null
    var primary: Throwable = null
    try
      expected = expectedTarget()
      if !expected.exactlyEquals(target) then reject("composite PREPARE differs from the complete final allowed relation changes and unchanged relation data")
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      val budget = if expected == null then null else expected.allocationBudget
      expected = null
      MySqlFormat3BudgetCleanup.run(primary,Vector(() => if budget != null then budget.closeScopedReadBudget()))

  private def compose(intermediate: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      child: MySqlFormat3ReadBudget, plan: FirstBindingPlanData,
      source: String, receipt: Vector[Byte], writer: MySqlFreshFormat3WriterTarget,
      domain: String, validateCatalogGenesis: GenesisValidator, beforeEpoch: Long,
      beforeLength: Long, beforeSha: Vector[Byte]): MySqlScriptSchemaSnapshot =
    child.reserveRetainedMemory(8192L,"composite PREPARE relation maps, holders and effect structures BEFORE allocation")
    val catalog = initialCatalog(inputs,plan.bindingUuid(),child)
    val relations = intermediate.relations.map { relation =>
      if relation.table == Catalog then MySqlFormat3RelationSnapshot(Catalog,Vector(catalog))
      else cloneRelation(relation,child)
    }
    val catalogStep = MySqlScriptSchemaSnapshot.decoded(intermediate.schema,intermediate.connection,intermediate.observationScope,
      intermediate.maxAllowedPacketBytes,inputs,relations,child)
    validateCatalogGenesis(intermediate,catalogStep)
    val rawEffect = effect(catalogStep,plan,source,receipt,writer,domain,beforeLength,beforeSha,child)
    val context = contextForEffect(rawEffect,beforeEpoch,writer,child,domain)
    val fingerprint = context.canonicalFingerprintSha256.toArray
    val shape = inputs.forTable(Writer)
    val index = shape.columns.indexWhere(_.name == "last_commit_fingerprint")
    if index < 0 then reject("actual writer fingerprint column missing")
    child.reserveRetainedMemory(1024L,"composite PREPARE final W row/relation and snapshot holders")
    val finalRelations = relations.map { relation =>
      if relation.table == Writer then
        if relation.rows.size != 1 then reject("composite PREPARE requires actual singleton W")
        relation.copy(rows = Vector(relation.rows.head.updated(index,MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(fingerprint,child)))))
      else relation
    }
    MySqlScriptSchemaSnapshot.decoded(intermediate.schema,intermediate.connection,intermediate.observationScope,
      intermediate.maxAllowedPacketBytes,inputs,finalRelations,child)

  /** 原effect只作为desiredEffect；actor直接消费同一DATA context，不另造operation摘要。 */
  def contextForPrepare(before: MySqlScriptSchemaSnapshot,target: MySqlScriptSchemaSnapshot,inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget,plan: WindowsWorldProvisionReceiptDataCodec.PlanData,source: String,
      receipt: Vector[Byte],writer: MySqlFreshFormat3WriterTarget): MySqlOperationContext =
    contextForPrepareKernel(before,target,inputs,root,plan,source,receipt,writer,
      MySqlScriptSchemaDataLayout.DeploymentReceiptFull26,Domain)

  def contextForDbFirstPrepare(before: MySqlScriptSchemaSnapshot,target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,root: MySqlFormat3ReadBudget,plan: DbFirstWorldAdoptionPlanData,
      source: String,receipt: Vector[Byte],writer: MySqlFreshFormat3WriterTarget,
      expectedQuotaMaximumRecords: Long): MySqlOperationContext =
    requireOwner(before,inputs,root,MySqlScriptSchemaDataLayout.DbFirstFull27)
    requireOwner(target,inputs,root,MySqlScriptSchemaDataLayout.DbFirstFull27)
    MySqlScriptSchemaB1Graph.requireDbFirstFull27QuotaConfiguration(before,expectedQuotaMaximumRecords)
    MySqlScriptSchemaB1Graph.requireDbFirstFull27QuotaConfiguration(target,expectedQuotaMaximumRecords)
    contextForPrepareKernel(before,target,inputs,root,plan,source,receipt,writer,
      MySqlScriptSchemaDataLayout.DbFirstFull27,DbFirstDomain)

  private def contextForPrepareKernel(before: MySqlScriptSchemaSnapshot,target: MySqlScriptSchemaSnapshot,
      inputs: MySqlScriptSchemaInputs,root: MySqlFormat3ReadBudget,plan: FirstBindingPlanData,
      source: String,receipt: Vector[Byte],writer: MySqlFreshFormat3WriterTarget,
      layout: MySqlScriptSchemaDataLayout,domain: String): MySqlOperationContext =
    requireOwner(before,inputs,root,layout);requireOwner(target,inputs,root,layout)
    val budget=target.allocationBudget
    val raw=effect(target,plan,source,receipt,writer,domain,before.canonicalByteLength,before.canonicalSha256,budget)
    val context=contextForEffect(raw,beforeWriterEpoch(before),writer,budget,domain)
    val index=inputs.forTable(Writer).columns.indexWhere(_.name=="last_commit_fingerprint")
    target.relation(Writer).rows.head(index) match
      case MySqlFormat3Cell.Binary(value) if value.equalsBytes(context.canonicalFingerprintSha256.toArray) => context
      case _ => reject("composite PREPARE target W differs from actual operation context")

  private def beforeWriterEpoch(image: MySqlScriptSchemaSnapshot): Long =
    val index=image.inputs.forTable(Writer).columns.indexWhere(_.name=="writer_epoch")
    image.relation(Writer).rows.head(index) match
      case MySqlFormat3Cell.SignedInteger(value) => value
      case _ => reject("composite PREPARE actual writer epoch required")

  private def contextForEffect(raw: Array[Byte],beforeEpoch: Long,writer: MySqlFreshFormat3WriterTarget,
      budget: MySqlFormat3ReadBudget,domain: String): MySqlOperationContext =
    budget.reserveRetainedMemory(2048L,"composite PREPARE actual effect/context/receipt before allocation")
    // intermediate已包含目标W，PREPARE的期望版本必须取原before epoch。
    MySqlOperationContext(writer.ownerBootUuid.getOrElse(reject("composite PREPARE actual owner required")),writer.writerEpoch,
      writer.bindingUuid,writer.operationUuid,Vector(MySqlExpectedEffect(s"${effectScope(domain)}/${writer.bindingUuid}",beforeEpoch,raw.toVector)))

  private def effectScope(domain: String): String =
    if domain == Domain then "deployment-receipt/composite-prepare"
    else if domain == DbFirstDomain then "db-first-full27/composite-prepare"
    else reject("unknown closed PREPARE composition domain")

  private def initialCatalog(inputs: MySqlScriptSchemaInputs, binding: UUID,
      child: MySqlFormat3ReadBudget): Vector[MySqlFormat3Cell] =
    val columns = inputs.forTable(Catalog).columns
    if columns.map(_.name) != Vector("binding_uuid","catalog_revision","snapshot_bytes","snapshot_sha256","next_version","last_mutation_operation_uuid") then
      reject("composite PREPARE requires the actual closed six-column catalog")
    child.reserveRetainedMemory(1024L,"composite PREPARE initial catalog wrappers/hash BEFORE allocation")
    columns.map(_.name).map {
      case "binding_uuid" =>
        child.reserveRetainedMemory(80L,"composite PREPARE catalog binding bytes")
        val bytes = new Array[Byte](16)
        java.nio.ByteBuffer.wrap(bytes).putLong(binding.getMostSignificantBits).putLong(binding.getLeastSignificantBits)
        MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes,child))
      case "catalog_revision" => MySqlFormat3Cell.SignedInteger(0L)
      case "snapshot_bytes" =>
        child.reserveRetainedMemory(64L,"composite PREPARE empty catalog owned binary")
        MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(new Array[Byte](0),child))
      case "snapshot_sha256" =>
        child.reserveRetainedMemory(96L,"composite PREPARE empty catalog SHA bytes")
        MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(MessageDigest.getInstance("SHA-256").digest(Array.emptyByteArray),child))
      case "next_version" => MySqlFormat3Cell.SignedInteger(1L)
      case "last_mutation_operation_uuid" => MySqlFormat3Cell.SqlNull
      case _ => reject("unknown initial catalog column")
    }

  private def cloneRelation(relation: MySqlFormat3RelationSnapshot, child: MySqlFormat3ReadBudget): MySqlFormat3RelationSnapshot =
    child.reserveRetainedMemory(Math.addExact(192L,Math.multiplyExact(relation.rows.size.toLong,32L)),"composite PREPARE owned relation/rows")
    val rows = relation.rows.map { row =>
      child.reserveRetainedMemory(Math.addExact(256L,Math.multiplyExact(row.size.toLong,64L)),"composite PREPARE owned row/cell wrappers")
      row.map {
        case MySqlFormat3Cell.Binary(value) =>
          MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(value.copyBytes(child,"composite PREPARE owned binary"),child))
        case MySqlFormat3Cell.Text(value) =>
          val fee = Math.addExact(64L,Math.multiplyExact(value.length.toLong,4L))
          child.reserveRetainedMemory(fee,"composite PREPARE owned text before char/string allocation")
          MySqlFormat3Cell.Text(new String(value.toCharArray))
        case MySqlFormat3Cell.SqlNull => MySqlFormat3Cell.SqlNull
        case MySqlFormat3Cell.SignedInteger(value) => MySqlFormat3Cell.SignedInteger(value)
        case MySqlFormat3Cell.Floating(value) => MySqlFormat3Cell.Floating(value)
        case MySqlFormat3Cell.DateTime(value) => MySqlFormat3Cell.DateTime(value)
      }
    }
    MySqlFormat3RelationSnapshot(relation.table,rows)

  /** BE domain/phase,11 recipe fields,source,receiptSHA,before length/SHA,named final B/B1/catalog,
    * normalized binding/epoch/owner/DATETIME6/operation W and one zero self-fingerprint tag.
    * Rows use the existing typed-cell writer, never a second table codec.
    */
  private def effect(target: MySqlScriptSchemaSnapshot, p: FirstBindingPlanData,
      source: String, receipt: Vector[Byte], w: MySqlFreshFormat3WriterTarget,
      domain: String, beforeLength: Long, beforeSha: Vector[Byte], child: MySqlFormat3ReadBudget): Array[Byte] =
    val temporaryFee = Math.addExact(4096L,Math.multiplyExact(source.length.toLong,8L))
    child.reserveTemporaryMemory(temporaryFee,"composite PREPARE streaming effect peak")
    var primary: Throwable = null
    try
      child.reserveRetainedMemory(96L,"composite PREPARE final owned fingerprint BEFORE digest")
      val digest = MessageDigest.getInstance("SHA-256")
      val sink = new BoundedDigest(digest,Math.min(target.inputs.budget.snapshotBytes,
        Math.min(target.inputs.budget.retainedBytes,target.inputs.sourceLimitProfile.maxRetainedBytes())))
      val out = new DataOutputStream(sink)
      def string(s: String): Unit = MySqlFormat3SnapshotCanonical.writeString(out,s)
      def uuid(u: UUID): Unit = MySqlFormat3SnapshotCanonical.writeUuid(out,u)
      string(domain); string("PREPARE")
      uuid(p.creationOperationUuid()); uuid(p.bootstrapOperationUuid()); uuid(p.prepareWriteUuid()); uuid(p.certifyWriteUuid()); uuid(p.bindingUuid())
      out.writeInt(p.physicalFormat()); uuid(p.mysqlServerUuid()); string(p.schema()); out.writeInt(p.schemaFormat())
      MySqlFormat3SnapshotCanonical.writeAscii(out,p.ddlSha256()); uuid(p.provisioningUuid())
      string(source); out.writeInt(receipt.size); receipt.foreach(b => out.writeByte(b.toInt))
      out.writeLong(beforeLength); out.writeInt(beforeSha.size); beforeSha.foreach(b => out.writeByte(b.toInt))
      Vector(Binding,Intent,Catalog).foreach { table =>
        string(table); val rows = target.relation(table).rows; out.writeInt(rows.size)
        rows.foreach { row => out.writeInt(row.size); row.foreach(MySqlFormat3SnapshotCanonical.writeCell(_,out)) }
      }
      uuid(w.bindingUuid); out.writeLong(w.writerEpoch)
      w.ownerBootUuid match
        case None => out.writeByte(0)
        case Some(boot) => out.writeByte(1); uuid(boot)
      val lease = w.leaseUntil
      out.writeInt(lease.getYear); out.writeByte(lease.getMonthValue); out.writeByte(lease.getDayOfMonth)
      out.writeByte(lease.getHour); out.writeByte(lease.getMinute); out.writeByte(lease.getSecond); out.writeInt(lease.getNano)
      uuid(w.operationUuid); out.writeByte(0)
      if domain == DbFirstDomain then
        // Full27's phase commitment includes every unchanged relation as exact target DATA too;
        // only W's self-referential fingerprint is represented by its normalized tuple above.
        target.relations.foreach { relation =>
          if relation.table != Binding && relation.table != Intent && relation.table != Catalog && relation.table != Writer then
            string(relation.table)
            out.writeInt(relation.rows.size)
            relation.rows.foreach { row =>
              out.writeInt(row.size)
              row.foreach(MySqlFormat3SnapshotCanonical.writeCell(_,out))
            }
        }
      out.flush()
      digest.digest()
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally MySqlFormat3BudgetCleanup.run(primary,Vector(() => child.releaseTemporaryMemory(temporaryFee,"composite PREPARE effect peak")))

  private final class BoundedDigest(digest: MessageDigest, limit: Long) extends OutputStream:
    private var length = 0L
    override def write(value: Int): Unit =
      length = Math.addExact(length,1L)
      if length > limit then reject("composite PREPARE effect exceeds explicit retained/snapshot bound")
      digest.update(value.toByte)
    override def write(bytes: Array[Byte], offset: Int, size: Int): Unit =
      java.util.Objects.checkFromIndexSize(offset,size,bytes.length)
      length = Math.addExact(length,size.toLong)
      if length > limit then reject("composite PREPARE effect exceeds explicit retained/snapshot bound")
      digest.update(bytes,offset,size)

  private def requireOwner(snapshot: MySqlScriptSchemaSnapshot, inputs: MySqlScriptSchemaInputs,
      root: MySqlFormat3ReadBudget, expectedLayout: MySqlScriptSchemaDataLayout): Unit =
    val family = if expectedLayout == null then "closed schema" else MySqlScriptSchemaInputs.familyName(expectedLayout)
    if inputs == null || root == null || root.isClosed || root.isScopedReadBudget ||
        expectedLayout == null || inputs.dataLayout != expectedLayout || (root.profile ne inputs.sourceLimitProfile) then
      reject(s"composite PREPARE requires exact open $family root/profile/inputs")
    root.requireScriptDataInputs(inputs)
    if snapshot == null || (snapshot.inputs ne inputs) || snapshot.allocationBudget.isClosed ||
        (!(snapshot.allocationBudget eq root) && !snapshot.allocationBudget.isCanonicalChildOf(root)) ||
        !snapshot.allocationBudget.sharesAllocationLedger(root) then reject("composite PREPARE image has foreign/closed/sibling owner")
    snapshot.allocationBudget.requireScriptDataInputs(inputs)

  private def reject(message: String): Nothing = throw new SQLDataException(message)
