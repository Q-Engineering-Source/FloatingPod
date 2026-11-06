package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.domain.{FloatingPodPose, FloatingPodState, PodOperatingMode}
import com.wi.floatingpod.storage.{PhysicalRecoveryBinding, PhysicalRecoveryCutoverState}
import java.io.{DataOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.sql.SQLDataException
import java.time.LocalDateTime
import java.util.UUID
import MySqlModeOperationEffectCodec.{Image,allocationBudget,limitProfile,schema,connection,observationScope,maxAllowedPacketBytes,relations,relation,rowCount,shapes}

/** SQL历史只重建DATA。真正来源/session/native资格始终由原Actor持有。 */
private[mysql] object MySqlHistoricalBusinessReceipt:
  private val O = "fp_recovery_operation"
  private val W = "fp_runtime_writer"
  private enum Kind:
    case Mode, Drift, Inactive, Debug
  private def cell(row: Vector[MySqlFormat3Cell], table: String, name: String): MySqlFormat3Cell =
    MySqlModeOperationEffectCodec.value(row, table, name)
  private def binary(row: Vector[MySqlFormat3Cell], name: String): MySqlFormat3Binary = cell(row, O, name) match
    case MySqlFormat3Cell.Binary(value) => value
    case _ => fail("original binary missing: " + name)
  private def token(row: Vector[MySqlFormat3Cell], name: String): String = cell(row, O, name) match
    case MySqlFormat3Cell.Text(value) => value
    case _ => fail("original token missing: " + name)
  private def sameId(row: Vector[MySqlFormat3Cell], table: String, name: String, id: UUID): Boolean =
    MySqlModeOperationEffectCodec.uuidEquals(cell(row, table, name), id)

  final class Verified private[MySqlHistoricalBusinessReceipt] (
      val row: Vector[MySqlFormat3Cell], val scope: MySqlNormalRecallCanonical.DecodedScope,
      val context: MySqlOperationContext, val receiptFingerprint: Vector[Byte],
      val before: Vector[MySqlFormat3RelationSnapshot], val target: Vector[MySqlFormat3RelationSnapshot])

  private final case class Inputs(kind: Kind, operation: MySqlRepositoryOperation,
      plan: MySqlOwnedRepositoryOperationPlan, context: MySqlRepositoryOperationContextIdentity,
      scope: MySqlNormalRecallCanonical.DecodedScope, pod: UUID, binding: UUID,
      debug: Option[MySqlControlledDebugPrepareRequest], inactive: Option[MySqlInactiveDomainUpdateRequest],
      activation: Boolean, activeLimit: Int, locatorRevision: Long,
      deploymentInputs:Option[MySqlScriptSchemaInputs]=None)

  private final class Reader(val input: MySqlFormat3Binary, budget: MySqlFormat3ReadBudget):
    var position = 0
    private def need(n: Long): Unit = require(n >= 0L && n <= input.length.toLong - position,
      "historical business frame is truncated or oversized")
    def byte(): Int = { need(1); val n = input.byteAt(position) & 255; position += 1; n }
    def bool(): Boolean = byte() match
      case 0 => false
      case 1 => true
      case _ => fail("noncanonical boolean")
    def int(): Int = (byte() << 24) | (byte() << 16) | (byte() << 8) | byte()
    def long(): Long = (int().toLong << 32) | (int().toLong & 0xffffffffL)
    def uuid(): UUID =
      val id = new UUID(long(), long())
      require(MySqlNormalRecallRequest.nonZero(id), "zero historical UUID")
      id
    def optionalUuid(): Option[UUID] = if bool() then Some(uuid()) else None
    def slice(begin: Int, end: Int): MySqlFormat3Binary =
      require(begin >= 0 && end >= begin && end <= input.length, "invalid historical slice")
      budget.reserveRetainedMemory(Math.addExact(end.toLong - begin, 64L), "historical owned frame")
      val bytes = new Array[Byte](MySqlFormat3JvmArrayLimit.requireLength(end.toLong - begin, "historical frame"))
      var i = 0
      while i < bytes.length do { bytes(i) = input.byteAt(begin + i); i += 1 }
      MySqlFormat3Binary.takeOwned(bytes)
    def bytes(n: Int): MySqlFormat3Binary =
      need(n.toLong); val begin = position; position += n; slice(begin, position)
    def frame(): MySqlFormat3Binary = bytes(int())
    private def temporary[A](amount: Long)(body: => A): A =
      budget.reserveTemporaryMemory(amount, "historical UTF8 scratch")
      var primary: Throwable = null
      try body
      catch
        case cause: Throwable => primary = cause; throw cause
      finally
        try budget.releaseTemporaryMemory(amount, "historical UTF8 scratch released")
        catch case cleanup: Throwable =>
          if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    def text(): String =
      val n = int(); need(n.toLong)
      budget.reserveRetainedMemory(Math.addExact(64L, Math.multiplyExact(n.toLong, 2L)), "historical decoded string")
      temporary(Math.addExact(512L, Math.multiplyExact(n.toLong, 3L))) {
        val data = new Array[Byte](MySqlFormat3JvmArrayLimit.requireLength(n.toLong, "historical UTF8"))
        var i = 0
        while i < n do { data(i) = input.byteAt(position + i); i += 1 }
        position += n
        StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(data)).toString
      }
    // 固定schema/domain只逐字比较原UTF8，不为每个receipt复制并永久计入常量文本。
    def expect(value: String): Unit = temporary(384L) {
      val sink = new OutputStream:
        override def write(expected: Int): Unit =
          require(byte() == (expected & 255), "historical domain/field differs: " + value)
      val output = new DataOutputStream(sink)
      MySqlFormat3Utf8.write(value, output)
      output.flush()
    }
    def count(expected: Int): Unit = require(int() == expected, "historical fixed count differs")
    def finish(): Unit = require(position == input.length, "historical trailing bytes")
    def date(micros: Boolean): LocalDateTime =
      val year = int(); val month = byte(); val day = byte(); val hour = byte(); val minute = byte(); val second = byte()
      val fraction = int()
      require(year >= 1000 && year <= 9999 && fraction >= 0 &&
        (if micros then fraction < 1000000 else fraction < 1000000000 && fraction % 1000 == 0), "historical DATETIME6")
      LocalDateTime.of(year, month, day, hour, minute, second, if micros then fraction * 1000 else fraction)
    def scalar(tag: Int): MySqlFormat3Cell = tag match
      case 0 => MySqlFormat3Cell.SqlNull
      case 1 => MySqlFormat3Cell.Text(text())
      case 2 => MySqlFormat3Cell.Binary(frame())
      case 3 => MySqlFormat3Cell.SignedInteger(long())
      case 4 =>
        val bits = long(); require(java.lang.Double.isFinite(java.lang.Double.longBitsToDouble(bits)), "nonfinite historical cell")
        MySqlFormat3Cell.Floating(bits)
      case 5 => MySqlFormat3Cell.DateTime(date(false))
      case _ => fail("unknown historical cell tag")
    def scalar(): MySqlFormat3Cell = scalar(byte())
    def pose(): FloatingPodPose = FloatingPodPose(text(), java.lang.Double.longBitsToDouble(long()),
      java.lang.Double.longBitsToDouble(long()), java.lang.Double.longBitsToDouble(long()),
      java.lang.Float.intBitsToFloat(int()), java.lang.Float.intBitsToFloat(int()), int())
    def binding(): PhysicalRecoveryBinding =
      val id = uuid(); expect("CERTIFIED"); count(2)
      PhysicalRecoveryBinding(id, PhysicalRecoveryCutoverState.Certified, 2)
    def state(): FloatingPodState =
      val id = uuid(); val owner = optionalUuid(); val version = long(); val world = text()
      val x = java.lang.Double.longBitsToDouble(long()); val y = java.lang.Double.longBitsToDouble(long())
      val z = java.lang.Double.longBitsToDouble(long()); val yaw = java.lang.Float.intBitsToFloat(int())
      val pitch = java.lang.Float.intBitsToFloat(int()); val active = bool(); val at = long()
      val dim = if bool() then Some(int()) else None
      FloatingPodState(id, owner, version, world, x, y, z, yaw, pitch, active, at, dim, PodOperatingMode.fromDatabase(text()))
    /** mode scope的共同字段没有长度前缀；按固定字段边界取出再用共同strict reader核。 */
    def inlineScope(): MySqlFormat3Binary =
      val begin = position
      uuid(); long(); long(); uuid(); uuid(); uuid(); uuid(); long(); text(); uuid(); long(); date(true)
      uuid(); text(); text(); text(); uuid(); int(); int(); text(); text(); bytes(32); text(); bytes(32); bytes(32)
      slice(begin, position)

  private def request(input: MySqlFormat3Binary, root: MySqlFormat3ReadBudget,
      dataBudget: MySqlFormat3ReadBudget): MySqlRepositoryOperation =
    val r = new Reader(input, dataBudget)
    r.expect("FP_MYSQL_REPOSITORY_REQUEST_BE_V1"); require(r.byte() == 1, "historical request version"); r.count(1)
    val operation = r.text() match
      case "SET_OPERATING_MODE_TRACKED_IF_OWNED" =>
        val binding = r.binding(); val pod = r.uuid(); val owner = r.uuid(); val version = r.long(); val attempt = r.uuid()
        val mode = PodOperatingMode.fromDatabase(r.text())
        MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(binding, pod, owner, version, attempt, mode, r.pose())
      case "COMMIT_TRACKED_ACTIVE_DRIFT" =>
        MySqlRepositoryOperation.CommitTrackedActiveDrift(r.binding(),r.state(),r.state(),r.uuid())
      case "UPDATE_INACTIVE_DOMAIN_F3" =>
        MySqlRepositoryOperation.UpdateInactiveDomain(MySqlInactiveDomainUpdateRequest(r.binding(), r.uuid(), r.optionalUuid(), r.long(), r.state()))
      case "PREPARE_CONTROLLED_DEBUG_CREATE_F3" =>
        MySqlRepositoryOperation.PrepareControlledDebugCreate(r.binding(), r.uuid(), r.uuid(), r.uuid(), r.pose(), r.uuid())
      case "COMMIT_CONTROLLED_DEBUG_CREATE_F3" =>
        MySqlRepositoryOperation.CommitControlledDebugCreate(r.binding(), r.uuid(), r.uuid(), r.uuid(), r.long(), r.pose(), r.uuid(), r.int())
      case _ => fail("unsupported historical business request")
    r.finish()
    MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
      require(MySqlRepositoryRequestIdentity.single(operation, root.profile).canonical.exactlyMatchesCell(MySqlFormat3Cell.Binary(input)),
        "historical request is not exact canonical")
    }
    operation

  private def inputs(row: Vector[MySqlFormat3Cell], root: MySqlFormat3ReadBudget,
      deploymentInputs: Option[MySqlScriptSchemaInputs], dataBudget: MySqlFormat3ReadBudget): Inputs =
    val operation = request(binary(row, "request_identity_bytes"), root, dataBudget)
    val r = new Reader(binary(row, "scope_bytes"), dataBudget)
    val kind = r.text() match
      case "FP_MYSQL_RECOVERY_OPERATION_SCOPE_BE_V1" => operation match
        case _:MySqlRepositoryOperation.CommitTrackedActiveDrift=>Kind.Drift
        case _=>Kind.Mode
      case "com.wi.floatingpod.mysql-inactive-domain-scope/v1" => Kind.Inactive
      case "com.wi.floatingpod.mysql-controlled-debug-scope/v1" => Kind.Debug
      case _ => fail("unsupported business scope")
    require(r.byte() == 1, "historical scope version")
    val scopeBytes=if kind==Kind.Mode || kind==Kind.Drift then r.inlineScope() else r.frame()
    val scope=MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      deploymentInputs match
        case None=>MySqlNormalRecallCanonical.decodeScopeFields(scopeBytes,root)
        case Some(full26)=>
          require(kind!=Kind.Inactive,"schema5 does not admit the legacy inactive mutation family")
          MySqlNormalRecallCanonical.decodeDeploymentReceiptScopeFields(scopeBytes,full26,root)
    }
    val s = scope.stableSession
    val plan = MySqlRecoveryCanonicalBytes.withAllocationBudget(dataBudget) {
      MySqlOwnedRepositoryOperationPlan.single(scope.tag, scope.workflow, scope.reserve, operation, root.profile)
    }
    def vector(bytes: MySqlFormat3Binary): Vector[Byte] =
      dataBudget.reserveRetainedMemory(Math.addExact(256L, bytes.length.toLong * 32L), "historical fixed hash vector")
      (0 until bytes.length).map(bytes.byteAt).toVector
    dataBudget.reserveRetainedMemory(128L, "historical exact profile digest vector")
    val context = new MySqlRepositoryOperationContextIdentity(scope.tag, scope.workflow, scope.reserve, s.barrier,
      scope.connectionId, s.namespace, scope.binding, scope.epoch, scope.lease, scope.server, s.serverVersion,
      scope.schema, s.effectiveUser, s.provisioning, s.schemaFormat, s.physicalFormat, s.ddl, s.manifest, vector(s.configuration),
      s.provenanceKind, vector(s.provenance), root.profile.sha256().toVector, "RUN", plan.requestIdentitySha256, root.profile)
    val result = operation match
      case MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(binding, pod, _, _, attempt, _, _) =>
        require(kind == Kind.Mode, "mode request/scope mismatch")
        r.count(1); r.expect("POD"); val key = r.frame(); val revision = r.long(); val savedAttempt = r.optionalUuid()
        require(revision >= 0 && savedAttempt.contains(attempt), "mode original resource revision/attempt")
        MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
          val expected = MySqlRecoveryCanonicalBytes.encode("FP_MYSQL_RECOVERY_RESOURCE_KEY_BE_V1", root.profile)(
            MySqlRecoveryResourceKey.pod(pod, root.profile).writeCanonicalTo)
          require(expected.exactlyMatchesCell(MySqlFormat3Cell.Binary(key)), "mode resource key differs")
        }
        Inputs(kind, operation, plan, context, scope, pod, binding.bindingUuid, None, None, false, 256, revision)
      case operation:MySqlRepositoryOperation.CommitTrackedActiveDrift =>
        require(kind==Kind.Drift,"drift request/scope mismatch")
        val drift=MySqlDriftOperationEffectCodec.driftRequest(operation)
        r.count(1);r.expect("POD");val key=r.frame();val revision=r.long();val attempt=r.optionalUuid()
        require(revision>=0 && attempt.contains(drift.attemptUuid),"drift original resource revision/attempt")
        MySqlRecoveryCanonicalBytes.withComparisonBudget(root) {
          val expected=MySqlRecoveryCanonicalBytes.encode("FP_MYSQL_RECOVERY_RESOURCE_KEY_BE_V1",root.profile)(
            MySqlRecoveryResourceKey.pod(drift.podUuid,root.profile).writeCanonicalTo)
          require(expected.exactlyMatchesCell(MySqlFormat3Cell.Binary(key)),"drift resource key differs")
        }
        Inputs(kind,operation,plan,context,scope,drift.podUuid,drift.binding.bindingUuid,None,None,false,256,revision)
      case MySqlRepositoryOperation.UpdateInactiveDomain(value) =>
        require(kind == Kind.Inactive, "inactive request/scope mismatch")
        r.expect("DOMAIN_MUTATION"); r.frame(); require(r.uuid() == value.podUuid && r.long() == value.expectedVersion &&
          r.optionalUuid() == value.expectedOwner, "inactive scope request differs")
        Inputs(kind, operation, plan, context, scope, value.podUuid, value.expectedBinding.bindingUuid, None, Some(value), false, 256, 0)
      case MySqlRepositoryOperation.PrepareControlledDebugCreate(binding, pod, attempt, footprint, pose, create) =>
        require(kind == Kind.Debug, "debug request/scope mismatch")
        readDebugScope(r, false, attempt, footprint, create)
        Inputs(kind, operation, plan, context, scope, pod, binding.bindingUuid,
          Some(MySqlControlledDebugPrepareRequest(binding, pod, attempt, footprint, pose, create)), None, false, 256, 0)
      case MySqlRepositoryOperation.CommitControlledDebugCreate(binding, pod, attempt, footprint, version, pose, create, limit) =>
        require(kind == Kind.Debug && version == 0L && limit > 0 && limit <= 256, "debug activation request/scope mismatch")
        readDebugScope(r, true, attempt, footprint, create)
        Inputs(kind, operation, plan, context, scope, pod, binding.bindingUuid,
          Some(MySqlControlledDebugPrepareRequest(binding, pod, attempt, footprint, pose, create)), None, true, limit, 0)
      case _ => fail("unsupported scope request")
    r.finish()
    require(scope.binding == result.binding, "historical binding differs")
    result.copy(deploymentInputs=deploymentInputs)

  private def readDebugScope(r: Reader, activation: Boolean, attempt: UUID, footprint: UUID, create: UUID): Unit =
    r.expect(if activation then "ACTIVATE" else "REARM"); r.frame()
    require(r.uuid() == attempt && r.uuid() == footprint && r.uuid() == create && r.bool() == !activation,
      "debug original scope identities")
    val expected = if activation then MySqlFormat3Cell.SignedInteger(0) else MySqlFormat3Cell.SqlNull
    require(r.scalar() == expected && r.scalar() == expected && r.scalar() == MySqlFormat3Cell.SqlNull, "debug original scope versions")

  private final case class Selected(schema: String, connection: MySqlFormat3ConnectionObservation,
      observation: MySqlFormat3ObservationScope, packet: Long, relations: Vector[MySqlFormat3RelationSnapshot])

  private def projection(raw: Vector[MySqlFormat3Cell], data: Inputs, phase: MySqlOperationEffectPhase,
      image: MySqlOperationEffectImage, inventory: Image, cursor: Image,
      root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): Selected =
    val r = new Reader(binary(raw, if image == MySqlOperationEffectImage.Before then "preimage_bytes" else "target_bytes"), dataBudget)
    val mode = data.kind == Kind.Mode || data.kind==Kind.Drift
    val modeVersion = if data.kind==Kind.Mode then Some(MySqlModeOperationEffectCodec.projectionVersion(r.input)) else None
    r.expect(data.kind match
      case Kind.Mode => modeVersion.get.domain
      case Kind.Drift => "com.wi.floatingpod.mysql-operation-effect-projection/v1"
      case Kind.Inactive => "com.wi.floatingpod.mysql-inactive-domain-effect/v1"
      case Kind.Debug => "com.wi.floatingpod.mysql-controlled-debug-effect/v2")
    require(r.byte() == (if data.kind==Kind.Mode then modeVersion.get.number else if data.kind==Kind.Drift then 1 else if data.kind == Kind.Debug then 2 else 1), "projection version differs")
    r.expect(if phase == MySqlOperationEffectPhase.Reserve then "RESERVE" else "EXECUTE")
    r.expect(if image == MySqlOperationEffectImage.Before then "BEFORE" else "TARGET")
    if mode then
      val (attempt,version,targetMode)=if data.kind==Kind.Mode then
        val request=MySqlModeOperationEffectCodec.modeRequest(data.operation)
        (request.attemptUuid,request.expectedStateVersion,request.targetMode)
      else
        val request=MySqlDriftOperationEffectCodec.driftRequest(data.operation)
        (request.attemptUuid,request.expectedStateVersion,request.targetMode)
      require(r.uuid()==data.binding && r.uuid()==data.pod && r.uuid()==attempt &&
        r.uuid()==data.plan.workflowUuid && r.uuid()==data.plan.reserveOperationUuid && r.uuid()==data.plan.executeOperationUuid &&
        r.long()==version && r.text()==targetMode.databaseValue && r.long()==data.locatorRevision,
        "mode/drift projection original identities")
    else
      require(r.frame() == binary(raw, "request_identity_bytes") && r.frame() == data.scope.bytes, "projection request/scope framing differs")
      r.expect(if data.kind == Kind.Inactive then "DOMAIN_MUTATION" else if data.activation then "ACTIVATE" else "REARM")
      require(r.uuid() == data.binding && r.uuid() == data.pod, "projection binding/pod")
      if data.kind == Kind.Inactive then
        require(r.long() == data.inactive.get.expectedVersion && r.optionalUuid() == data.inactive.get.expectedOwner, "inactive projection identity")
      else
        val d = data.debug.get
        require(r.uuid() == d.attemptUuid && r.uuid() == d.footprintUuid && r.uuid() == d.createOperationUuid, "debug projection identity")
      require(r.uuid() == data.plan.workflowUuid && r.uuid() == data.plan.reserveOperationUuid && r.uuid() == data.plan.executeOperationUuid,
        "projection workflow identity")
      if data.kind == Kind.Debug then
        require(r.bool() == !data.activation, "debug projection phase purpose")
        val expected = if data.activation then MySqlFormat3Cell.SignedInteger(0) else MySqlFormat3Cell.SqlNull
        require(r.scalar() == expected && r.scalar() == expected, "debug projection versions")
    val schema = if mode then data.scope.schema else r.text()
    if !mode then Vector(MySqlFormat3RelationShapes.SourceFoundationSha256, MySqlFormat3RelationShapes.SourceRelationDefinitionsSha256,
      MySqlFormat3RelationShapes.SourceLockedReadSha256).foreach(hash =>
        require(r.bytes(64).equalsBytes(hash.getBytes(StandardCharsets.US_ASCII)), "projection structural source differs"))
    val connection = if mode then cursor.connection else MySqlFormat3ConnectionObservation(r.uuid(), r.text(), r.text(), r.text(),
      r.text(), r.int(), r.long(), r.text(), r.text(), r.text(), r.text(), r.text(), r.text())
    val observation = if mode then MySqlFormat3ObservationScope(data.scope.tag.bootId, data.scope.tag.bootGeneration,
      data.scope.tag.connectionGeneration) else MySqlFormat3ObservationScope(r.uuid(), r.long(), r.long())
    val packet = if mode then cursor.maxAllowedPacketBytes else r.long()
    if !mode then require(r.frame().equalsBytes(root.profile.canonicalBytes()) && r.bytes(32).equalsBytes(root.profile.sha256()),
      "projection complete profile differs")
    require(schema == data.scope.schema && connection.serverUuid == data.scope.server && connection.serverVersion == data.scope.stableSession.serverVersion &&
      connection.effectiveUser == data.scope.stableSession.effectiveUser && observation.bootUuid == data.scope.tag.bootId &&
      observation.bootGeneration == data.scope.tag.bootGeneration && observation.connectionGeneration == data.scope.tag.connectionGeneration,
      "historical projection observation differs from original scope")
    val actualShapes=data.deploymentInputs.map(_.canonicalOrder).getOrElse(MySqlFormat3RelationShapes.canonicalOrder)
    r.count(actualShapes.size)
    var total = 0L
    val relations = actualShapes.map { shape =>
      r.expect(shape.table)
      if !mode then
        r.expect(shape.lockClause); r.count(shape.primaryKey.size); shape.primaryKey.foreach(r.expect)
      r.count(shape.columns.size)
      shape.columns.foreach { column =>
        r.expect(column.name); r.expect(column.sqlDeclaration)
        if !mode then
          require(r.byte() == column.kind.ordinal && r.bool() == column.nullable, "projection column kind/nullable")
          require(r.bool() == column.maximumLength.nonEmpty, "projection column bound presence")
          column.maximumLength.foreach(r.count)
      }
      val count = r.int(); require(count >= 0, "negative projection row count")
      total = Math.addExact(total, count.toLong)
      require(total <= root.profile.maxMembers(), "projection rows exceed original member bound")
      dataBudget.reserveRetainedMemory(Math.addExact(512L, Math.multiplyExact(count.toLong, 512L + shape.columns.size.toLong * 128L)),
        "historical selected rows and cells")
      val rows = Vector.tabulate(count) { _ =>
        if !mode then r.count(shape.columns.size)
        var values = Vector.empty[MySqlFormat3Cell]
        var hasDescriptor = false
        def original: Vector[MySqlFormat3Cell] =
          require(shape.table == O, "historical payload outside O")
          val binding = values(shape.columns.indexWhere(_.name == "binding_uuid"))
          val operation = values(shape.columns.indexWhere(_.name == "operation_uuid"))
          val found = inventory.relation(O).rows.filter(row => cell(row, O, "binding_uuid") == binding && cell(row, O, "operation_uuid") == operation)
          require(found.size == 1, "original historical payload row absent or ambiguous")
          found.head
        shape.columns.foreach { column =>
          val tag = r.byte()
          val value = if tag == 0x7f && mode then
            r.expect("FP_TYPED_MARKER_V1"); r.expect(if phase == MySqlOperationEffectPhase.Reserve then "RESERVE" else "EXECUTE")
            r.expect(shape.table); r.count(shape.primaryKey.size)
            shape.primaryKey.foreach(name => require(r.scalar() == values(shape.columns.indexWhere(_.name == name)), "marker key differs"))
            r.expect(column.name); r.expect(column.sqlDeclaration); require(r.byte() == 1, "marker version")
            derived(raw, data, phase, image, shape.table, column.name, values, if shape.table == O then Some(original) else None)
          else if tag == 6 && !mode then
            r.expect(column.name)
            val reason = r.text()
            val expected = if shape.table == W then
              if data.kind == Kind.Inactive then "CURRENT_INACTIVE_PHASE_WRITER_FINGERPRINT_V1" else "CURRENT_PHASE_WRITER_FINGERPRINT_V2"
            else if data.kind == Kind.Inactive then "CURRENT_INACTIVE_PLAN_OPERATION_CYCLE_V1" else "CURRENT_PLAN_OPERATION_CYCLE_V2"
            require(reason == expected, "projection omission reason differs")
            derived(raw, data, phase, image, shape.table, column.name, values, if shape.table == O then Some(original) else None)
          else if tag == 7 && (!mode || modeVersion.contains(MySqlModeProjectionVersion.V2)) then
            if mode then
              r.expect("FP_MODE_HISTORICAL_PAYLOAD_V1"); r.expect(column.name); r.expect(column.sqlDeclaration)
            else
              r.expect(column.name); r.expect("HISTORICAL_RAW_PAYLOAD_V1")
            val length = r.long(); val hash = r.bytes(32); val row = original
            val current = sameId(row, O, "operation_uuid", data.plan.executeOperationUuid) ||
              (phase == MySqlOperationEffectPhase.Reserve && sameId(row, O, "operation_uuid", data.plan.reserveOperationUuid))
            require(!current && Set("COMMITTED", "CANCELLED").contains(token(row, "operation_state")), "descriptor is not a terminal historical row")
            val hashColumn = if column.name == "original_diagnostic_identity" then "original_diagnostic_sha256"
              else column.name.stripSuffix("_bytes") + "_sha256"
            require(Set("request_identity_bytes", "original_context_bytes", "scope_bytes", "preimage_bytes", "target_bytes",
              "commit_witness_bytes", "original_diagnostic_identity").contains(column.name), "unsupported historical descriptor column")
            val payload = binary(row, column.name)
            require(payload.length.toLong == length && cell(row, O, hashColumn) == MySqlFormat3Cell.Binary(hash), "historical descriptor payload differs")
            hasDescriptor = true
            MySqlFormat3Cell.Binary(payload)
          else if tag == 5 && mode then MySqlFormat3Cell.DateTime(r.date(true))
          else r.scalar(tag)
          values = values :+ value
        }
        if hasDescriptor then require(values == original, "historical entire RAW differs")
        values
      }
      MySqlFormat3RelationSnapshot(shape.table, rows)
    }
    r.finish()
    data.deploymentInputs match
      case None=>MySqlFormat3SnapshotValidation.validateRelations(relations)
      case Some(_)=>MySqlFormat3SnapshotValidation.validateScriptPublication1DeploymentReceipt1Relations(relations)
    Selected(schema, connection, observation, packet, relations)

  private def derived(raw: Vector[MySqlFormat3Cell], data: Inputs, phase: MySqlOperationEffectPhase,
      image: MySqlOperationEffectImage, table: String, column: String, partial: Vector[MySqlFormat3Cell],
      original: Option[Vector[MySqlFormat3Cell]]): MySqlFormat3Cell =
    if table == W then
      require(image == MySqlOperationEffectImage.Target && column == "last_commit_fingerprint", "misplaced writer marker")
      cell(raw, O, if data.kind == Kind.Debug then "original_context_sha256" else "operation_fingerprint")
    else
      require(table == O && original.nonEmpty, "misplaced operation marker")
      val row = original.get
      val reserve = sameId(row, O, "operation_uuid", data.plan.reserveOperationUuid)
      val execute = sameId(row, O, "operation_uuid", data.plan.executeOperationUuid)
      require(sameId(row, O, "binding_uuid", data.binding) && sameId(row, O, "workflow_uuid", data.plan.workflowUuid) &&
        sameId(row, O, "pod_uuid", data.pod), "marker original identity differs")
      val common = Set("original_context_bytes", "original_context_sha256", "target_bytes", "target_sha256", "operation_fingerprint")
      val preimage = Set("preimage_bytes", "preimage_sha256")
      val witness = Set("commit_witness_bytes", "commit_witness_sha256")
      val allowed = if phase == MySqlOperationEffectPhase.Reserve then image == MySqlOperationEffectImage.Target &&
        ((reserve && (common ++ witness).contains(column)) || (execute && (common ++ preimage).contains(column)))
      else execute && (common.contains(column) ||
        ((data.kind == Kind.Mode || data.kind==Kind.Drift || image == MySqlOperationEffectImage.Before) && preimage.contains(column)) ||
        (image == MySqlOperationEffectImage.Target && witness.contains(column)))
      require(allowed, "misplaced operation cycle marker")
      val result = cell(row, O, column)
      require(result != MySqlFormat3Cell.SqlNull, "actual original derived payload missing")
      result

  private def select(snapshot: Image, data: Inputs): Vector[MySqlFormat3RelationSnapshot] = data.kind match
    case Kind.Mode => MySqlModeOperationEffectCodec.selectModeClosure(snapshot, data.binding, data.pod)
      .map((shape, rows) => MySqlFormat3RelationSnapshot(shape.table, rows))
    case Kind.Drift => MySqlDriftOperationEffectCodec.selectDriftClosure(snapshot,data.binding,data.pod)
      .map((shape,rows)=>MySqlFormat3RelationSnapshot(shape.table,rows))
    case _ => snapshot.relations.map(r => MySqlFormat3RelationSnapshot(r.table, r.rows.filter(row =>
      if data.kind == Kind.Inactive then MySqlInactiveDomainSelection.includes(r.table, row, data.inactive.get)
      else if data.deploymentInputs.nonEmpty && !MySqlFormat3RelationShapes.canonicalOrder.exists(_.table==r.table) then true
      else MySqlControlledDebugSelection.includes(r.table, row, data.debug.get))))

  private def snapshot(selected: Selected, relations: Vector[MySqlFormat3RelationSnapshot], dataBudget: MySqlFormat3ReadBudget,
      exactInputs:Option[MySqlScriptSchemaInputs]): Image =
    dataBudget.reserveRetainedMemory(512L, "historical complete image wrapper")
    exactInputs match
      case None=>new MySqlFormat3FullSnapshot(selected.schema,selected.connection,selected.observation,selected.packet,dataBudget.profile,relations,dataBudget)
      case Some(inputs)=>MySqlScriptSchemaSnapshot.decoded(selected.schema,selected.connection,selected.observation,selected.packet,inputs,relations,dataBudget)

  private def images(raw: Vector[MySqlFormat3Cell], data: Inputs, phase: MySqlOperationEffectPhase,
      inventory: Image, cursor: Image, root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget)
      : (Image, Image) =
    if data.kind == Kind.Mode then
      require(MySqlModeOperationEffectCodec.projectionVersion(binary(raw, "preimage_bytes")) ==
        MySqlModeOperationEffectCodec.projectionVersion(binary(raw, "target_bytes")), "mode phase projection versions differ")
    val before = projection(raw, data, phase, MySqlOperationEffectImage.Before, inventory, cursor, root, dataBudget)
    val target = projection(raw, data, phase, MySqlOperationEffectImage.Target, inventory, cursor, root, dataBudget)
    require(before.schema == target.schema && before.connection == target.connection && before.observation == target.observation &&
      before.packet == target.packet, "phase projection metadata differs")
    dataBudget.reserveRetainedMemory(Math.addExact(8192L, Math.multiplyExact(cursor.rowCount, 512L)), "historical full cursor overlay indexes")
    val selected = select(cursor, data)
    require(selected.filterNot(_.table == W) == target.relations.filterNot(_.table == W), "selected target differs from full cursor")
    // W生命周期由外层完整区段校验；这里保存原target，绝不把mask扩散到非W。
    val fullTarget = cursor.relations.map(r => if r.table == W then target.relations.find(_.table == W).get else r)
    val targetSnapshot = snapshot(target, fullTarget, dataBudget,data.deploymentInputs)
    require(select(targetSnapshot, data) == target.relations, "target closure differs")
    val fullBefore = fullTarget.map { relation =>
      val shape = data.deploymentInputs.map(_.forTable(relation.table)).getOrElse(MySqlFormat3RelationShapes.forTable(relation.table))
      def key(row: Vector[MySqlFormat3Cell]) = shape.primaryKey.map(name => row(shape.columns.indexWhere(_.name==name)))
      val targetKeys = target.relations.find(_.table == relation.table).get.rows.map(key).toSet
      val retained = relation.rows.filterNot(row => targetKeys.contains(key(row)))
      val previous = before.relations.find(_.table == relation.table).get.rows
      require(!retained.exists(row => previous.exists(p => key(p) == key(row))), "before overlay collides with unselected row")
      MySqlFormat3RelationSnapshot(relation.table, (retained ++ previous).sortWith((a, b) => MySqlControlledDebugDelta.compareKey(a, b, shape) < 0))
    }
    val beforeSnapshot = snapshot(before, fullBefore, dataBudget,data.deploymentInputs)
    require(select(beforeSnapshot, data) == before.relations, "before closure differs")
    Vector(beforeSnapshot,targetSnapshot).foreach {
      case legacy:MySqlFormat3FullSnapshot=>MySqlControlledDebugRelations.requireValidFullGraph(legacy,root)
      case deployment:MySqlScriptSchemaSnapshot=>MySqlControlledDebugRelations.requireValidDeploymentReceiptGraph(deployment,deployment.inputs,root)
    }
    (beforeSnapshot, targetSnapshot)

  def verify(raw: Vector[MySqlFormat3Cell], inventory: Image,
      cursor: Image, root: MySqlFormat3ReadBudget): Verified =
    verifyInternal(raw, inventory, cursor, root, root)

  /** H3 Full26 only: the retained authority remains root while fresh history DATA uses its registered plan child. */
  private[mysql] def verify(raw: Vector[MySqlFormat3Cell], inventory: Image,
      cursor: Image, root: MySqlFormat3ReadBudget,
      planDataBudget: MySqlFormat3ReadBudget): Verified =
    require(inventory.isInstanceOf[MySqlScriptSchemaSnapshot] && cursor.isInstanceOf[MySqlScriptSchemaSnapshot],
      "exact Full26 fresh-history source images required")
    requirePlanDataBudget(root, planDataBudget)
    verifyInternal(raw, inventory, cursor, root, planDataBudget)

  private def requirePlanDataBudget(root: MySqlFormat3ReadBudget,
      planDataBudget: MySqlFormat3ReadBudget): Unit =
    require(root != null && !root.isClosed && !root.isScopedReadBudget && planDataBudget != null &&
      !planDataBudget.isClosed && (planDataBudget.profile eq root.profile) &&
      planDataBudget.isCanonicalChildOf(root) && planDataBudget.sharesAllocationLedger(root),
      "exact same-ledger Full26 plan DATA child required")

  private def verifyInternal(raw: Vector[MySqlFormat3Cell], inventory: Image,
      cursor: Image, root: MySqlFormat3ReadBudget, dataBudget: MySqlFormat3ReadBudget): Verified =
    require(raw != null && inventory != null && cursor != null && root != null && !root.isClosed && !root.isScopedReadBudget &&
      (inventory.limitProfile eq root.profile) && (cursor.limitProfile eq root.profile) &&
      inventory.allocationBudget.sharesAllocationLedger(root) && cursor.allocationBudget.sharesAllocationLedger(root), "historical actual root/inventory required")
    require(inventory.relation(O).rows.contains(raw) && token(raw, "operation_state") == "COMMITTED", "actual committed business row required")
    require(dataBudget != null && ((dataBudget eq root) || ((dataBudget.profile eq root.profile) &&
      dataBudget.isCanonicalChildOf(root) && dataBudget.sharesAllocationLedger(root))),
      "historical DATA allocation must remain in the original same-ledger root")
    dataBudget.reserveRetainedMemory(16384L, "historical business parser and original DATA inputs")
    val exactInputs=(inventory,cursor) match
      case (a:MySqlScriptSchemaSnapshot,b:MySqlScriptSchemaSnapshot)=>
        require(a.inputs eq b.inputs,"historical Full26 inventory/cursor switched inputs")
        MySqlFormat3QueryProjection.requireDeploymentSource(a,a.inputs,root)
        MySqlFormat3QueryProjection.requireDeploymentSource(b,a.inputs,root)
        Some(a.inputs)
      case (_:MySqlFormat3FullSnapshot,_:MySqlFormat3FullSnapshot)=>None
      case _=>fail("historical inventory/cursor crossed complete-image families")
    MySqlRecoveryOperationRawEnvelopeCodec.requireValidRow(raw, root.profile, dataBudget, exactInputs)
    val data = inputs(raw, root,exactInputs,dataBudget)
    val phase = if token(raw, "step") == "RESERVE" then MySqlOperationEffectPhase.Reserve else MySqlOperationEffectPhase.Execute
    val (before, target) = images(raw, data, phase, inventory, cursor, root, dataBudget)
    val reserveRows = inventory.relation(O).rows.filter(r => sameId(r, O, "binding_uuid", data.binding) &&
      sameId(r, O, "operation_uuid", data.plan.reserveOperationUuid))
    require(reserveRows.size == 1 && token(reserveRows.head, "operation_state") == "COMMITTED", "actual original business reserve missing")
    val reserveRaw = reserveRows.head
    if data.kind == Kind.Mode then
      val version = MySqlModeOperationEffectCodec.projectionVersion(binary(raw, "preimage_bytes"))
      require(Vector("preimage_bytes", "target_bytes").forall(column =>
        MySqlModeOperationEffectCodec.projectionVersion(binary(reserveRaw, column)) == version), "original reserve projection version differs")
    val initial = if phase == MySqlOperationEffectPhase.Reserve then before else
      val reserveData = inputs(reserveRaw, root,exactInputs,dataBudget)
      require(reserveData.scope == data.scope && reserveData.operation == data.operation, "execute original reserve scope/request differs")
      val (initial, reserved) = images(reserveRaw, reserveData, MySqlOperationEffectPhase.Reserve, inventory, before, root, dataBudget)
      require(reserved.relations == before.relations, "original reserve target differs from execute before")
      initial
    val at = cell(raw, O, "planned_at") match
      case MySqlFormat3Cell.DateTime(value) => value
      case _ => fail("original business planned time missing")
    val receipt = cell(raw, O, if data.kind == Kind.Debug then "original_context_sha256" else "operation_fingerprint") match
      case MySqlFormat3Cell.Binary(bytes) if bytes.length == 32 =>
        dataBudget.reserveRetainedMemory(1152L, "historical receipt digest vector")
        (0 until 32).map(bytes.byteAt).toVector
      case _ => fail("original business receipt fingerprint missing")
    val context = if data.kind == Kind.Mode then
      dataBudget.reserveRetainedMemory(1024L, "historical root-owned mode context")
      val operationId = if phase == MySqlOperationEffectPhase.Reserve then data.plan.reserveOperationUuid else data.plan.executeOperationUuid
      val context = MySqlModeOperationEffectCodec.phaseOperationContext(data.context,
        MySqlModeOperationEffectCodec.modeRequest(data.operation), operationId, receipt, root.profile)
      if dataBudget eq root then
        MySqlModeWorkflowMaterializer.requireHistorical(data.plan, data.operation, data.context, at, initial,
          data.locatorRevision, MySqlModeOperationEffectCodec.projectionVersion(binary(raw, "preimage_bytes")),
          phase, before, target, reserveRaw, raw, context, root)
      else
        MySqlModeWorkflowMaterializer.requireHistorical(data.plan, data.operation, data.context, at, initial,
          data.locatorRevision, MySqlModeOperationEffectCodec.projectionVersion(binary(raw, "preimage_bytes")),
          phase, before, target, reserveRaw, raw, context, root, dataBudget)
      context
    else
      val (regeneratedReserve, regeneratedExecute, context) = data.kind match
        case Kind.Inactive =>
          val legacy=initial match
            case value:MySqlFormat3FullSnapshot=>value
            case _=>fail("inactive mutation does not admit Full26")
          val m = MySqlInactiveDomainWorkflowMaterializer.materialize(data.inactive.get, data.plan, data.context, at, legacy, root)
          (m.reserveTarget, m.executeTarget, (if phase == MySqlOperationEffectPhase.Reserve then m.reserve else m.execute).operationContext)
        case Kind.Mode => fail("mode must use bounded comparison")
        case Kind.Drift =>
          val m=initial match
            case legacy:MySqlFormat3FullSnapshot=>MySqlDriftWorkflowMaterializer.materialize(data.plan,data.operation,data.context,at,legacy,data.locatorRevision)
            case deployment:MySqlScriptSchemaSnapshot=>
              if dataBudget eq root then MySqlDriftWorkflowMaterializer.materializeDeploymentReceipt(
                data.plan,data.operation,data.context,at,deployment,data.locatorRevision,root)
              else MySqlDriftWorkflowMaterializer.materializeDeploymentReceipt(
                data.plan,data.operation,data.context,at,deployment,data.locatorRevision,root,dataBudget)
          (m.completeReserveTarget,m.completeExecuteTarget,(if phase==MySqlOperationEffectPhase.Reserve then m.effects(0) else m.effects(1)).exactOperationContext)
        case Kind.Debug =>
          val m = initial match
            case legacy:MySqlFormat3FullSnapshot=>MySqlControlledDebugWorkflowMaterializer.materialize(data.debug.get,data.plan,data.operation,data.context,
              at,legacy,data.activation,data.activeLimit,root)
            case deployment:MySqlScriptSchemaSnapshot=>
              if dataBudget eq root then MySqlControlledDebugWorkflowMaterializer.materializeDeploymentReceipt(
                data.debug.get,data.plan,data.operation,data.context,at,deployment,data.activation,data.activeLimit,root)
              else MySqlControlledDebugWorkflowMaterializer.materializeDeploymentReceipt(
                data.debug.get,data.plan,data.operation,data.context,at,deployment,data.activation,data.activeLimit,root,dataBudget)
          (m.completeReserveTarget,m.completeExecuteTarget,(if phase==MySqlOperationEffectPhase.Reserve then m.reserve else m.execute).operationContext)
      val rebuiltTarget = if phase == MySqlOperationEffectPhase.Reserve then regeneratedReserve else regeneratedExecute
      require(rebuiltTarget.relations == target.relations &&
        (phase == MySqlOperationEffectPhase.Reserve || regeneratedReserve.relations == before.relations),
        "historical complete target differs from original pure business algorithm")
      val original = rebuiltTarget.relation(O).rows.filter(r => sameId(r, O, "operation_uuid", context.operationUuid))
      require(original == Vector(raw), "actual complete business RAW/witness differs from original algorithm")
      require(regeneratedReserve.relation(O).rows.contains(reserveRaw), "actual original reserve RAW differs from original algorithm")
      context
    new Verified(raw, data.scope, context, receipt, before.relations, target.relations)

  private def fail(reason: String): Nothing = throw new SQLDataException("historical business receipt: " + reason)
