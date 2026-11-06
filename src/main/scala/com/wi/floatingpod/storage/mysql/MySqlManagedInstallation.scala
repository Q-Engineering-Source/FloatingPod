package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.DbFirstStartupRetirementCodec

import com.wi.floatingpod.bootstrap.{EarlyDependencyResolver, ManagedBootstrapPolicy, ManagedDatabaseConfiguration, ProductionBootstrapControl}
import com.wi.floatingpod.config.{MySqlRuntimeMetadata, OwnedIoSecretFiles, SourceLimitProfile}

import java.io.{IOException, OutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{FileAlreadyExistsException, Files, LinkOption, Path}
import java.security.MessageDigest
import java.sql.{Connection, SQLDataException, SQLException}
import java.time.Duration
import java.util.{Arrays, HexFormat, UUID}
import scala.jdk.CollectionConverters.*
import scala.util.Using

/**
 * 在受控 I/O 线程中执行仅限既有数据库的 schema6 安装。
 * 每项 SQL 副作用都绑定规范渲染顺序与强制落盘的 journal intent；恢复时先检查实际状态，再确认或继续执行。
 */
private[floatingpod] final class MySqlManagedInstallation(
    configuration: ManagedDatabaseConfiguration,
    worldPath: Path,
    bootIdentity: ProductionBootstrapControl.DbFirstIdentity,
    versionedPolicy: ProductionBootstrapControl.Policy,
    dependencyPins: EarlyDependencyResolver.ExpectedPins
):
  private val worker = Thread.currentThread()
  private val runtimeDirectory = configuration.instanceRoot()
  private var files: ManagedInstallationFiles = null
  private var owner: ManagedMySqlConnectionOwner = null
  private var budgets: MySqlManagedReadBudgets = null
  private def root: MySqlFormat3ReadBudget = if budgets == null then null else budgets.dataRoot
  private var journal: ManagedInstallationJournal = null
  private var catalogCapture: MySqlManagedCatalogCapture.Captured = null
  private var securityCapture: MySqlManagedCatalogCapture.Captured = null
  private var semantic: MySqlManagedSemanticProducer.Data = null
  private var semanticReparse: MySqlDeploymentReceiptRuntimeContract.ManagedSemantic = null
  private var metadataProjection: MySqlManagedMetadataProducer.Projection = null
  private var serializedMetadata: MySqlManagedMetadataProducer.SerializedMetadata = null
  private var runtimeOpen: MySqlConnectionSource.SourceUnknownInspectionOpen = null
  private var runtimeVerified: VerifiedMySqlConnection = null
  private var assembly: MySqlRuntimeInputs.AssemblyOwner = null
  private var byteStreams = Vector.empty[BudgetedByteArrayOutputStream]
  private var pendingPayloadBytes = Vector.empty[RetainedPayload]
  private var rendered: MySqlDbFirstSchemaProfile.Rendered = null
  private var plan: MySqlManagedSchemaPlan.Plan = null
  private var identity: ManagedInstallationIdentity.Record = null
  private var recipeBytes: Array[Byte] = null
  private var configurationDigest: Array[Byte] = null
  private var configurationDigestHex: String = null
  private var runtimeCredentialPath: Path = null
  private var policy: MySqlRuntimeAuditPolicy = null
  private var generated: MySqlDeploymentReceiptRuntimeContract.Generated = null
  private var seedValues: MySqlManagedSeedValues.Data = null
  private var disposed = false
  private var closeFailure: Throwable = null
  private var primaryFailure: Throwable = null

  require(configuration != null && worldPath != null && bootIdentity != null && versionedPolicy != null && dependencyPins != null,
    "complete managed installation identity and ordinary configuration required")
  require(worldPath.isAbsolute, "managed installation world path must be absolute")

  def prepareOnOwnedIoThread(): ProductionBootstrapControl.DbFirstControl =
    requireWorker()
    if disposed then throw new IllegalStateException("managed installation owner is closed")
    if files != null || owner != null then throw new IllegalStateException("managed installation prepare is one-shot")
    files = ManagedInstallationFiles.open(runtimeDirectory)
    val persisted = ManagedInstallationIdentity.readExisting(files)
    if persisted.isPresent then
      val active = ManagedInstallationBundle.load(files, persisted.get())
      if active.isPresent then
        return preparePublishedRestart(active.get(), persisted.get())
      rejectOrphanPublicationWithoutActive()
    else
      rejectOrphanFilesWithoutIdentity()

    owner = new ManagedMySqlConnectionOwner(configuration)
    val previousServer = if persisted.isPresent then persisted.get().serverUuid() else null
    val connectionIdentity = owner.open(previousServer)
    owner.acquireInitializationLock()
    owner.requireHeldInitializationLock()
    val clientHost = owner.connectedClientHost()
    val profile = MySqlManagedReleasePolicy.profile(configuration.database(), connectionIdentity.serverUuid, clientHost)
    rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(profile)
    plan = MySqlManagedSchemaPlan.build(rendered)
    generated = MySqlDeploymentReceiptRuntimeContract.generateManagedDbFirst(profile)
    policy = runtimeAuditPolicy()
    verifyRuntimeBootstrapPolicy()
    val actualConfigSha = MySqlManagedReleasePolicy.configurationSha256(configuration, connectionIdentity.serverUuid, profile)

    openBudget(connectionIdentity)
    MySqlManagedPrivilegePreflight.inspect(owner, rendered)

    val journalPath = files.directory().resolve(ManagedInstallationJournal.FILE_NAME)
    val journalExists = Files.exists(journalPath, LinkOption.NOFOLLOW_LINKS)
    if !journalExists then
      if persisted.isPresent then rejectUnjournaledFiles(profile)
      verifyEmptyInitialTarget(profile)

    identity = ManagedInstallationIdentity.openOrCreate(files, connectionIdentity.serverUuid, configuration.database(),
      actualConfigSha, rendered.sha256)
    recipeBytes = canonicalRecipe(identity)
    persistOrVerify("installation.recipe", recipeBytes)
    runtimeCredentialPath = files.ensureRuntimePassword(profile.accounts.runtime.user)
    val steps = journalSteps(plan)
    journal = ManagedInstallationJournal.openOrCreate(files.directory(), journalIdentity(identity), steps.asJava)
    if journal.snapshot().phase() == ManagedInstallationJournal.Phase.BUNDLE_PUBLISHED then
      throw new IOException("managed journal records a published bundle but its active pointer is absent")

    // 允许 identity 已落盘而 journal 尚未创建的恢复，前提是受锁检查仍证明目标完全未变更。
    ensureSecurityPrepared()
    processPlan()
    completeAndPublish()

  private def preparePublishedRestart(loaded: ManagedInstallationBundle.Loaded,
      persisted: ManagedInstallationIdentity.Record): ProductionBootstrapControl.DbFirstControl =
    requireWorker()
    identity = persisted
    val raw = files.readPrivateFile(ManagedInstallationBundle.RUNTIME_METADATA_FILE, MySqlRuntimeMetadata.MAX_METADATA_BYTES)
    try
      val metadata = MySqlRuntimeMetadata.fromDbFirstBytes(raw)
      val baseline = metadata.baseline()
      val audit = baseline.auditDefiner()
      val split = audit.lastIndexOf('@')
      if split <= 0 || split == audit.length - 1 then throw new IOException("managed audit account cannot recover the release host identity")
      val profile = MySqlManagedReleasePolicy.profile(persisted.schema(), persisted.serverUuid(), audit.substring(split + 1))
      rendered = MySqlDbFirstSchemaProfile.renderExistingDatabase(profile)
      plan = MySqlManagedSchemaPlan.build(rendered)
      generated = MySqlDeploymentReceiptRuntimeContract.generateManagedDbFirst(profile)
      policy = runtimeAuditPolicy()
      val expectedConfig = MySqlManagedReleasePolicy.configurationSha256(configuration, persisted.serverUuid(), profile)
      if persisted.configurationSha256() != expectedConfig ||
        persisted.recipeSha256() != rendered.sha256 || !baseline.ddlSha256().equalsIgnoreCase(rendered.sha256) then
        throw new IOException("managed restart configuration or release recipe differs from the persisted installation")
      verifyRuntimeBootstrapPolicy()
      val exactJournalSteps = journalSteps(plan)
      journal = ManagedInstallationJournal.openOrCreate(files.directory(), journalIdentity(persisted), exactJournalSteps.asJava)
      val publicationAckPending = journal.snapshot().phase() match
        case ManagedInstallationJournal.Phase.BUNDLE_PUBLISHED => false
        case ManagedInstallationJournal.Phase.SEALED => true
        case _ => throw new IOException("active managed bundle is not authorized by its exact durable journal phase")
      if !loaded.configurationDigestSha256().matches("[0-9a-f]{64}") then
        throw new IOException("active managed bundle has no canonical runtime configuration digest")
      configurationDigestHex = loaded.configurationDigestSha256()
      openRestartBudget(rendered.inputs.sourceLimitProfile)
      verifyNormalAssemblyC(raw)
      val startupIncident = readStartupRetirementRequest()
      cleanupCompletedDataOwners()
      budgets.close()
      budgets = null
      if publicationAckPending then acknowledgePublishedBundle(loaded)
      closeJournalForRestart()
      val control = ProductionBootstrapControl.createManaged(worldPath, loaded, files, bootIdentity, versionedPolicy,
        ManagedBootstrapPolicy.barrierWait(), dependencyPins, loaded.configurationDigestSha256(), startupIncident)
      configuration.close()
      clearPlanState()
      disposed = true
      control
    finally Arrays.fill(raw, 0.toByte)

  private def readStartupRetirementRequest(): DbFirstStartupRetirementCodec.RequestData =
    val fee = 65536L
    root.reserveTemporaryMemory(fee, "bounded optional startup incident input/codec BEFORE read")
    var raw: Array[Byte] = null
    var primary: Throwable = null
    try
      try raw = files.readPrivateFile(DbFirstStartupRetirementCodec.REQUEST_FILE,
        DbFirstStartupRetirementCodec.MAX_REQUEST_BYTES)
      catch case _: java.nio.file.NoSuchFileException => return null
      val request = DbFirstStartupRetirementCodec.decodeRequest(raw)
      val canonical = DbFirstStartupRetirementCodec.encodeRequest(request)
      try
        if !MessageDigest.isEqual(raw, canonical) then throw new IOException("noncanonical operator startup incident")
      finally Arrays.fill(canonical, 0.toByte)
      // 固定小型 DTO (<8 KiB) 随 control 移交，包含于既有 524288-byte actor/control holder。
      request
    catch
      case cause: Throwable => primary = cause; throw cause
    finally
      if raw != null then Arrays.fill(raw, 0.toByte)
      MySqlFormat3BudgetCleanup.run(primary, Vector(() => root.releaseTemporaryMemory(fee, "startup incident input retired")))

  private def acknowledgePublishedBundle(loaded: ManagedInstallationBundle.Loaded): Unit =
    val payloads = Array.ofDim[Array[Byte]](4)
    var primary: Throwable = null
    try
      payloads(0) = files.readPrivateFile(ManagedInstallationBundle.BASELINE_FILE, ManagedInstallationBundle.MAX_PAYLOAD_BYTES)
      payloads(1) = files.readPrivateFile(ManagedInstallationBundle.RUNTIME_CONTRACT_FILE, ManagedInstallationBundle.MAX_PAYLOAD_BYTES)
      payloads(2) = files.readPrivateFile(ManagedInstallationBundle.SOURCE_CAPTURE_FILE, ManagedInstallationBundle.MAX_PAYLOAD_BYTES)
      payloads(3) = files.readPrivateFile(ManagedInstallationBundle.RUNTIME_METADATA_FILE, ManagedInstallationBundle.MAX_PAYLOAD_BYTES)
      val acknowledged = ManagedInstallationBundle.publish(files, journal, identity,
        new ManagedInstallationBundle.Payload(payloads(0), loaded.baselineSha256()),
        new ManagedInstallationBundle.Payload(payloads(1), loaded.runtimeContractSha256()),
        new ManagedInstallationBundle.Payload(payloads(2), loaded.sourceCaptureSha256()),
        new ManagedInstallationBundle.Payload(payloads(3), loaded.metadataSha256()), loaded.configurationDigestSha256())
      if acknowledged != loaded || journal.snapshot().phase() != ManagedInstallationJournal.Phase.BUNDLE_PUBLISHED then
        throw new IOException("sealed active bundle publication acknowledgment failed exact journal readback")
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      var cleanupFailure: Throwable = null
      payloads.indices.foreach { index =>
        if payloads(index) != null then
          try Arrays.fill(payloads(index), 0.toByte)
          catch
            case cleanup: Throwable =>
              if cleanupFailure == null then cleanupFailure = cleanup
              else if cleanup ne cleanupFailure then cleanupFailure.addSuppressed(cleanup)
          payloads(index) = null
      }
      if cleanupFailure != null then
        if primary == null then throw cleanupFailure
        else if cleanupFailure ne primary then primary.addSuppressed(cleanupFailure)

  private def verifyRuntimeBootstrapPolicy(): Unit =
    val expected = ManagedBootstrapPolicy.policy(versionedPolicy.limits().preparation().toMillis())
    if versionedPolicy != expected then
      throw new IllegalArgumentException("managed bootstrap policy differs from the current versioned release policy")

  private def runtimeAuditPolicy(): MySqlRuntimeAuditPolicy =
    MySqlRuntimeAuditPolicy(versionedPolicy.callerRoles(), versionedPolicy.auditDefinerRoles(),
      MySqlExpectedNoImport, MySqlExpectedPhysicalFormat.Format2, versionedPolicy.receiptFreshness())

  private def openBudget(connectionIdentity: MySqlConnectionIdentity): Unit =
    owner.requireHeldInitializationLock()
    val packet = oneLong(owner.connection(), "SELECT @@GLOBAL.max_allowed_packet", owner.queryTimeoutSeconds())
    if rendered == null then throw new IllegalStateException("managed renderer must be fixed before the deployment root")
    val profile = rendered.inputs.sourceLimitProfile
    if packet < profile.maxSqlPacketBytes() then throw new SQLDataException("MySQL max_allowed_packet is below the pinned schema6 source limit")
    budgets = new MySqlManagedReadBudgets(profile, math.min(packet, profile.maxSqlPacketBytes()))
    MySqlManifestConfigurationIdentity.requireRoot(root)
    if connectionIdentity.serverVersion != MySqlConnectionSource.SupportedServerVersion ||
      connectionIdentity.catalog != configuration.database() then
      throw new SQLDataException("managed connection verification differs from the pinned server/schema")

  private def openRestartBudget(profile: SourceLimitProfile): Unit =
    if root != null || profile == null || (profile ne rendered.inputs.sourceLimitProfile) then
      throw new IllegalStateException("restart assembly requires the exact rendered source-limit profile")
    budgets = new MySqlManagedReadBudgets(profile, profile.maxSqlPacketBytes())
    MySqlManifestConfigurationIdentity.requireRoot(root)

  private def verifyEmptyInitialTarget(profile: MySqlDeploymentReceiptSchemaProfile.Inputs): Unit =
    owner.requireHeldInitializationLock()
    val capture = MySqlManagedCatalogCapture.captureCatalog(rendered, owner, root)
    catalogCapture = capture
    try
      if capture.resultSets.exists(_.rows.nonEmpty) then
        throw new SQLDataException("managed existing database is not empty of schema6 objects; no adoption is permitted")
    finally
      capture.close()
      catalogCapture = null
    if countSchemaRoutines() != 0L || countSchemaEvents() != 0L then
      throw new SQLDataException("managed existing database contains a routine or event outside the empty first-install inventory")
    verifyDatabaseCharacterSet()
    val accounts = profile.accounts.all
    val users = accounts.map(_.user)
    val absent = scalarLong(owner.connection(),
      "SELECT COUNT(*) FROM mysql.user WHERE " + users.map(_ => "BINARY User=BINARY ?").mkString("(", " OR ", ")"),
      users.map(_.asInstanceOf[AnyRef]))
    if absent != 0L then throw new SQLDataException("managed first installation found one or more pre-existing generated accounts")
    verifyNoAccountSecurityEdges(profile, expectRoutinePairs = 0)
    owner.requireHeldInitializationLock()

  private def verifyDatabaseCharacterSet(): Unit =
    owner.requireHeldInitializationLock()
    val connection = owner.connection()
    val statement = connection.prepareStatement(
      "SELECT DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE BINARY SCHEMA_NAME=BINARY ?")
    var primary: Throwable = null
    try
      statement.setQueryTimeout(owner.queryTimeoutSeconds())
      statement.setString(1, configuration.database())
      val rows = statement.executeQuery()
      try
        if !rows.next() then throw new SQLDataException("configured existing database schema is absent")
        val charset = rows.getString(1)
        val collation = rows.getString(2)
        if charset != "utf8mb4" || collation != "utf8mb4_0900_bin" || rows.next() then
          throw new SQLDataException("configured existing database charset/collation differs from the exact schema6 profile")
      finally rows.close()
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(statement, primary)

  private def ensureSecurityPrepared(): Unit =
    if owner == null || rendered == null || plan == null then throw new IllegalStateException("exact managed SQL owner and canonical plan required")
    owner.requireHeldInitializationLock()
    MySqlManagedPrivilegePreflight.inspect(owner, rendered)

  private def processPlan(): Unit =
    val steps = plan.steps
    val lastGrant = steps.lastIndexWhere(_.kind == MySqlManagedSchemaPlan.Kind.PrivilegeGrant)
    if lastGrant < 0 then throw new SQLDataException("closed managed recipe contains no privilege grants")
    var semanticPrepared = false
    steps.zipWithIndex.foreach { case (step, index) =>
      processStep(step, index)
      if index == lastGrant then
        processRuntimeUnlock(lastGrant + 1)
        prepareSemanticBeforeSeeds()
        semanticPrepared = true
      if step.kind == MySqlManagedSchemaPlan.Kind.SeedInsert && !semanticPrepared then
        throw new IllegalStateException("managed semantic C must be durably fixed before either control seed")
    }
    if !semanticPrepared then throw new SQLDataException("managed runtime security and semantic preparation did not reach the seed boundary")
    if semantic == null then
      throw new IllegalStateException("managed pre-seed semantic DATA must remain retained through final static validation")

  private def processStep(step: MySqlManagedSchemaPlan.Step, ordinal: Int): Unit =
    requireExactPlanStep(step, ordinal)
    val journalIndex = journalIndexForPlan(ordinal)
    owner.requireHeldInitializationLock()
    if step.kind == MySqlManagedSchemaPlan.Kind.SessionSetup || step.kind == MySqlManagedSchemaPlan.Kind.TargetDatabase then
      replaySessionStep(step, journalIndex)
      return
    if countSchemaEvents() != 0L then
      throw new SQLDataException("managed target schema contains an unapproved event during ordered installation")
    val snapshot = journal.snapshot()
    val pending = snapshot.pendingIntentIndex()
    if snapshot.nextStepIndex() > journalIndex then
      if !inspectEffect(step) then throw new SQLDataException("confirmed managed SQL effect is missing from its exact target")
      return
    if pending.isPresent && pending.getAsInt != journalIndex then
      throw new IOException("managed journal pending intent is not the canonical current plan step")
    if pending.isPresent then
      if inspectEffect(step) then
        journal.confirmEffect(journalIndex)
        return
      executeEffect(step, journalIndex)
      if !inspectEffect(step) then throw new SQLDataException("resumed managed SQL effect did not reach its exact release-defined state")
      journal.confirmEffect(journalIndex)
      return
    if snapshot.nextStepIndex() != journalIndex || snapshot.phase() != ManagedInstallationJournal.Phase.READY_FOR_STEP then
      throw new IOException("managed journal does not authorize the exact next ordered plan step")
    if inspectEffect(step) then
      throw new SQLDataException("managed SQL effect already exists without its same-plan durable intent; adoption is forbidden")
    journal.recordIntent(journalIndex)
    try
      executeEffect(step, journalIndex)
    catch
      case unknown: SqlEffectOutcomeUnknown if unknown.cleanupConfirmed =>
        confirmInspectedUnknownEffect(step, journalIndex, unknown)
        return
      case unknown: MySqlManagedSeedValues.UncertainEffect if unknown.cleanupConfirmed =>
        confirmInspectedUnknownEffect(step, journalIndex, unknown)
        return
    if !inspectEffect(step) then throw new SQLDataException("managed SQL effect failed exact post-execution inspection")
    journal.confirmEffect(journalIndex)

  private def confirmInspectedUnknownEffect(step: MySqlManagedSchemaPlan.Step, journalIndex: Int,
      unknown: SQLException): Unit =
    val matches = try inspectEffect(step)
    catch
      case inspectionFailure: Throwable =>
        if inspectionFailure ne unknown then unknown.addSuppressed(inspectionFailure)
        throw unknown
    if !matches then throw unknown
    try journal.confirmEffect(journalIndex)
    catch
      case confirmationFailure: Throwable =>
        if confirmationFailure ne unknown then confirmationFailure.addSuppressed(unknown)
        throw confirmationFailure

  private def replaySessionStep(step: MySqlManagedSchemaPlan.Step, journalIndex: Int): Unit =
    val snapshot = journal.snapshot()
    val pending = snapshot.pendingIntentIndex()
    if snapshot.nextStepIndex() > journalIndex then
      executeSessionSql(step)
      return
    if pending.isPresent && pending.getAsInt != journalIndex then
      throw new IOException("managed session step is not the pending canonical step")
    if !pending.isPresent then journal.recordIntent(journalIndex)
    executeSessionSql(step)
    journal.confirmEffect(journalIndex)

  private def executeSessionSql(step: MySqlManagedSchemaPlan.Step): Unit =
    owner.requireHeldInitializationLock()
    executeSql(step.sql)
    owner.requireHeldInitializationLock()

  private def executeEffect(step: MySqlManagedSchemaPlan.Step, ordinal: Int): Unit = step.kind match
    case MySqlManagedSchemaPlan.Kind.SessionSetup | MySqlManagedSchemaPlan.Kind.TargetDatabase => executeSessionSql(step)
    case MySqlManagedSchemaPlan.Kind.SeedInsert =>
      requireSemanticPrepared()
      seedValues = MySqlManagedSeedValues.create(identity, rendered, plan, recipeBytes, configurationDigest)
      seedValues.insert(owner, journal, ordinal, step)
    case MySqlManagedSchemaPlan.Kind.QuotaInitialization => executeSql(step.sql)
    case _ => executeSql(step.sql)

  private def inspectEffect(step: MySqlManagedSchemaPlan.Step): Boolean =
    owner.requireHeldInitializationLock()
    step.kind match
      case MySqlManagedSchemaPlan.Kind.ManagedAccount => inspectPlannedAccount(step.identity.stripPrefix("account:"))
      case MySqlManagedSchemaPlan.Kind.TableDefinition => inspectTable(step.identity.stripPrefix("table:"))
      case MySqlManagedSchemaPlan.Kind.TableConstraint =>
        val (table, name) = deferredConstraintIdentity(step)
        inspectTableOrConstraint(table, Some(name))
      case MySqlManagedSchemaPlan.Kind.QuotaInitialization => inspectQuota()
      case MySqlManagedSchemaPlan.Kind.SeedInsert =>
        requireSemanticPrepared()
        inspectSeed(step.identity.stripPrefix("seed:"))
      case MySqlManagedSchemaPlan.Kind.TriggerDefinition => inspectTrigger(step.identity.stripPrefix("trigger:"))
      case MySqlManagedSchemaPlan.Kind.RoutineDefinition => inspectRoutine()
      case MySqlManagedSchemaPlan.Kind.PrivilegeGrant => inspectGrant(step)
      case MySqlManagedSchemaPlan.Kind.SessionSetup | MySqlManagedSchemaPlan.Kind.TargetDatabase => true

  private def inspectPlannedAccount(identityText: String): Boolean =
    val runtimeIdentity = rendered.inputs.accounts.runtime.identity
    if identityText != runtimeIdentity then inspectAccount(identityText, locked = true)
    else
      val unlockIndex = plan.steps.lastIndexWhere(_.kind == MySqlManagedSchemaPlan.Kind.PrivilegeGrant) + 1
      val snapshot = journal.snapshot()
      val pending = snapshot.pendingIntentIndex()
      val authorizedUnlocked = snapshot.nextStepIndex() > unlockIndex ||
        (pending.isPresent && pending.getAsInt == unlockIndex)
      if authorizedUnlocked && inspectAccount(identityText, locked = false) then
        verifyRuntimeLogin()
        true
      else inspectAccount(identityText, locked = true)

  private def inspectTable(table: String): Boolean =
    inspectTableOrConstraint(table, None)

  private def inspectTableOrConstraint(table: String, constraint: Option[String]): Boolean =
    val captured = MySqlManagedCatalogCapture.captureCatalog(rendered, owner, root)
    catalogCapture = captured
    try
      assertPlannedTableInventory(captured.resultSets)
      val included = journalConstraintOrdinals(captured.resultSets)
      val present = MySqlManagedStaticValidation.verifyTableAt(rendered, table, captured.resultSets, root, included)
      if !present && constraint.nonEmpty then
        throw new SQLDataException("managed deferred foreign key target table is missing")
      present && constraint.forall(name => deferredConstraintPresent(captured.resultSets, table, name))
    finally
      captured.close()
      catalogCapture = null

  private def deferredConstraintIdentity(step: MySqlManagedSchemaPlan.Step): (String, String) =
    val parts = step.identity.split(":", -1)
    if step.kind != MySqlManagedSchemaPlan.Kind.TableConstraint || parts.length != 3 ||
        parts(0) != "constraint" || !plan.tables.contains(parts(1)) || !parts(2).matches("[A-Za-z0-9_]{1,64}") then
      throw new SQLDataException("managed deferred foreign key identity differs from the closed plan")
    (parts(1), parts(2))

  private def deferredConstraintPresent(sets: Vector[MySqlAuditResultSet], table: String, name: String): Boolean =
    val constraints = sets.find(_.id == "RS05_TABLE_CONSTRAINTS")
      .getOrElse(throw new SQLDataException("managed deferred foreign key catalog DATA is missing"))
    constraints.rows.exists { row =>
      fieldText(constraints, row, "TABLE_SCHEMA") == plan.schema &&
        fieldText(constraints, row, "TABLE_NAME") == table && fieldText(constraints, row, "CONSTRAINT_NAME") == name
    }

  // 已确认前缀固定包含；仅当前同计划 pending intent 可在两个编译状态间判别，随后仍逐字段验证完整表。
  // 最终 B 的静态门不使用此选择，始终要求三条外置外键全部存在。
  private def journalConstraintOrdinals(sets: Vector[MySqlAuditResultSet]): Set[Int] =
    val state = journal.snapshot()
    val pending = state.pendingIntentIndex()
    plan.deferredConstraintSteps.collect {
      case step if state.nextStepIndex() > journalIndexForPlan(step.ordinal) ||
          (pending.isPresent && pending.getAsInt == journalIndexForPlan(step.ordinal) && {
            val (table, name) = deferredConstraintIdentity(step)
            deferredConstraintPresent(sets, table, name)
          }) => step.ordinal
    }.toSet

  private def assertPlannedTableInventory(sets: Vector[MySqlAuditResultSet]): Unit =
    val set = sets.find(_.id == "RS02_TABLES").getOrElse(
      throw new SQLDataException("managed table catalog DATA is missing"))
    val expectedLabels = Vector("TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ENGINE", "VERSION", "ROW_FORMAT",
      "TABLE_COLLATION", "CREATE_OPTIONS", "TABLE_COMMENT")
    if set.columns.map(_.label) != expectedLabels then
      throw new SQLDataException("managed table inventory columns differ from the exact direct catalog query")
    val planned = plan.tables.toSet
    var observed = Set.empty[String]
    set.rows.foreach { row =>
      val schema = fieldText(set, row, "TABLE_SCHEMA")
      val name = fieldText(set, row, "TABLE_NAME")
      val kind = fieldText(set, row, "TABLE_TYPE")
      if schema != plan.schema || kind != "BASE TABLE" || !planned(name) || observed(name) then
        throw new SQLDataException("managed schema contains an unplanned, duplicate, or non-base table")
      observed += name
    }

  private def assertPlannedTriggerInventory(sets: Vector[MySqlAuditResultSet]): Unit =
    val set = sets.find(_.id == "RS09_TRIGGERS").getOrElse(
      throw new SQLDataException("managed trigger catalog DATA is missing"))
    if !set.columns.exists(_.label == "TRIGGER_SCHEMA") || !set.columns.exists(_.label == "TRIGGER_NAME") then
      throw new SQLDataException("managed trigger inventory lacks its exact schema/name columns")
    val planned = plan.steps.collect {
      case step if step.kind == MySqlManagedSchemaPlan.Kind.TriggerDefinition => step.identity.stripPrefix("trigger:")
    }.toSet
    var observed = Set.empty[String]
    set.rows.foreach { row =>
      val schema = fieldText(set, row, "TRIGGER_SCHEMA")
      val name = fieldText(set, row, "TRIGGER_NAME")
      if schema != plan.schema || !planned(name) || observed(name) then
        throw new SQLDataException("managed schema contains an unplanned or duplicate trigger")
      observed += name
    }

  private def inspectAccount(identityText: String, locked: Boolean): Boolean =
    val at = identityText.lastIndexOf('@')
    if at <= 0 || at == identityText.length - 1 then throw new SQLDataException("managed account step identity is malformed")
    val user = identityText.substring(0, at)
    val host = identityText.substring(at + 1)
    val connection = owner.connection()
    val query = connection.prepareStatement("SELECT User,Host,account_locked FROM mysql.user WHERE BINARY User=BINARY ? AND BINARY Host=BINARY ?")
    var primary: Throwable = null
    try
      query.setQueryTimeout(owner.queryTimeoutSeconds())
      query.setString(1, user); query.setString(2, host)
      val rows = query.executeQuery()
      try
        if !rows.next() then return false
        if rows.getString(1) != user || rows.getString(2) != host then throw new SQLDataException("managed account identity differs from the release plan")
        val accountLocked = rows.getString(3)
        if (accountLocked != "Y" && accountLocked != "N") || rows.next() then
          throw new SQLDataException("managed account lock state is malformed or its identity is duplicated")
        // 合法的另一锁定状态表示本次转换尚未生效；已确认步骤仍由调用方拒绝不匹配。
        accountLocked == (if locked then "Y" else "N")
      finally rows.close()
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(query, primary)

  private def inspectQuota(): Boolean =
    val table = s"`${plan.schema}`.`${MySqlDeploymentReceiptSchemaDefinition.QuotaTable}`"
    val total = scalarLong(owner.connection(), s"SELECT COUNT(*) FROM $table", Seq.empty)
    if total == 0L then return false
    if total != 1L then throw new SQLDataException("managed deployment quota table contains rows outside its single fixed namespace")
    val query = owner.connection().prepareStatement(s"SELECT namespace,max_records,used_records,quota_revision FROM $table")
    var primary: Throwable = null
    try
      query.setQueryTimeout(owner.queryTimeoutSeconds())
      val rows = query.executeQuery()
      try
        if !rows.next() then throw new SQLDataException("managed deployment quota row disappeared after its total-row census")
        val namespace = rows.getBytes(1)
        val maxRecords = rows.getLong(2); val maxWasNull = rows.wasNull()
        val usedRecords = rows.getLong(3); val usedWasNull = rows.wasNull()
        val revision = rows.getLong(4); val revisionWasNull = rows.wasNull()
        val ok = namespace != null && HexFormat.of().formatHex(namespace) == MySqlDeploymentReceiptSchemaDefinition.NamespaceHex.toLowerCase(java.util.Locale.ROOT) &&
          !maxWasNull && !usedWasNull && !revisionWasNull && maxRecords == rendered.inputs.quotaMaximumRecords && usedRecords == 0L && revision == 0L
        if !ok || rows.next() then throw new SQLDataException("managed deployment quota row conflicts with the exact release profile")
        true
      finally rows.close()
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(query, primary)

  private def inspectSeed(table: String): Boolean = table match
    case "fp_schema_manifest" => inspectManifestSeed()
    case "fp_runtime_writer" => inspectRuntimeWriterSeed()
    case _ => throw new SQLDataException("managed seed table is outside the two exact control rows")

  private def inspectManifestSeed(): Boolean =
    val profile = rendered.inputs.sourceLimitProfile
    val table = s"`${plan.schema}`.`fp_schema_manifest`"
    val total = scalarLong(owner.connection(), s"SELECT COUNT(*) FROM $table", Seq.empty)
    if total == 0L then return false
    if total != 1L then throw new SQLDataException("managed manifest table contains rows outside its fixed singleton control row")
    val journalState = journal.snapshot()
    val sealIndex = journalSteps(plan).size - 1
    val pending = journalState.pendingIntentIndex()
    val sealAuthorized = journalState.nextStepIndex() > sealIndex ||
      (pending.isPresent && pending.getAsInt == sealIndex) ||
      journalState.phase() == ManagedInstallationJournal.Phase.SEALED ||
      journalState.phase() == ManagedInstallationJournal.Phase.BUNDLE_PUBLISHED
    val readyPredicate = if sealAuthorized then "TRUE" else "FALSE"
    val sql = s"""SELECT COUNT(*) FROM `${plan.schema}`.`fp_schema_manifest`
      WHERE singleton_fixed1=1 AND backend='MYSQL' AND format_version=6 AND ddl_sha256=? AND provisioning_uuid=?
      AND ( (bootstrap_state='BOOTSTRAPPING' AND guard_manifest_sha256 IS NULL) OR
        (bootstrap_state='READY' AND guard_manifest_sha256 IS NOT NULL AND OCTET_LENGTH(guard_manifest_sha256)=32 AND $readyPredicate) )
      AND expected_server_uuid=? AND expected_binding_uuid=? AND import_state='NONE'
      AND import_operation_uuid IS NULL AND import_manifest_sha256 IS NULL AND import_snapshot_sha256 IS NULL AND import_receipt_sha256 IS NULL
      AND configuration_sha256=? AND source_provenance_kind='MANAGED_INSTALL'
      AND source_provenance_bytes=? AND source_provenance_sha256=? AND limit_profile_encoding=? AND limit_profile_bytes=?
      AND limit_profile_sha256=? AND max_region_bytes=? AND max_chunk_bytes=? AND max_decompressed_bytes=? AND max_nbt_depth=?
      AND max_members=? AND max_retained_bytes=? AND max_sql_packet_bytes=? AND max_native_artifact_bytes=?"""
    val query = owner.connection().prepareStatement(sql)
    var primary: Throwable = null
    try
      query.setQueryTimeout(owner.queryTimeoutSeconds())
      var i = 1
      query.setBytes(i, HexFormat.of().parseHex(rendered.sha256)); i += 1
      query.setBytes(i, uuidBytes(identity.provisioningUuid())); i += 1
      query.setBytes(i, uuidBytes(identity.serverUuid())); i += 1
      query.setBytes(i, uuidBytes(identity.bindingUuid())); i += 1
      query.setBytes(i, configurationDigest); i += 1
      query.setBytes(i, recipeBytes); i += 1
      query.setBytes(i, shaBytes(recipeBytes)); i += 1
      query.setString(i, SourceLimitProfile.ENCODING); i += 1
      query.setBytes(i, profile.canonicalBytes()); i += 1
      query.setBytes(i, profile.sha256()); i += 1
      Vector(profile.maxRegionBytes(), profile.maxChunkBytes(), profile.maxDecompressedBytes(), profile.maxNbtDepth().toLong,
        profile.maxMembers().toLong, profile.maxRetainedBytes(), profile.maxSqlPacketBytes(), profile.maxNativeArtifactBytes()).foreach { value => query.setLong(i, value); i += 1 }
      val rows = query.executeQuery()
      try
        if !rows.next() then throw new SQLDataException("managed manifest seed count query returned no row")
        val count = rows.getLong(1)
        if rows.wasNull() || rows.next() then throw new SQLDataException("managed manifest seed count query returned an invalid shape")
        count match
          case 0L => throw new SQLDataException("existing managed manifest singleton conflicts with the exact persisted seed identity or seal phase")
          case 1L => true
          case _ => throw new SQLDataException("managed manifest seed is duplicated")
      finally rows.close()
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(query, primary)

  private def inspectRuntimeWriterSeed(): Boolean =
    val table = s"`${plan.schema}`.`fp_runtime_writer`"
    val total = scalarLong(owner.connection(), s"SELECT COUNT(*) FROM $table", Seq.empty)
    if total == 0L then return false
    if total != 1L then throw new SQLDataException("managed runtime-writer table contains rows outside its fixed singleton control row")
    val query = owner.connection().prepareStatement(s"""SELECT COUNT(*) FROM `${plan.schema}`.`fp_runtime_writer`
      WHERE singleton_fixed1=1 AND binding_uuid=? AND writer_epoch=0 AND owner_boot_uuid IS NULL
      AND lease_until='1000-01-01 00:00:00.000000' AND last_commit_operation_uuid IS NULL AND last_commit_fingerprint IS NULL""")
    var primary: Throwable = null
    try
      query.setQueryTimeout(owner.queryTimeoutSeconds()); query.setBytes(1, uuidBytes(identity.bindingUuid()))
      val rows = query.executeQuery()
      try
        if !rows.next() then throw new SQLDataException("managed runtime-writer seed count query returned no row")
        val count = rows.getLong(1)
        if rows.wasNull() || rows.next() then throw new SQLDataException("managed runtime-writer seed count query returned an invalid shape")
        count match
          case 0L => throw new SQLDataException("existing managed runtime-writer singleton conflicts with the exact persisted seed identity")
          case 1L => true
          case _ => throw new SQLDataException("managed runtime-writer seed is duplicated")
      finally rows.close()
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(query, primary)

  private def inspectTrigger(name: String): Boolean =
    val captured = MySqlManagedCatalogCapture.captureCatalog(rendered, owner, root)
    catalogCapture = captured
    try
      assertPlannedTriggerInventory(captured.resultSets)
      val recipe = MySqlManagedStaticRecipe.fromPlan(plan)
      val expected = recipe.triggers.find(_.name == name).getOrElse(throw new SQLDataException("managed trigger is outside the exact SOURCE recipe"))
      val set = captured.resultSets.find(_.id == "RS09_TRIGGERS").getOrElse(throw new SQLDataException("managed trigger catalog DATA is missing"))
      val matches = set.rows.filter(row => fieldText(set, row, "TRIGGER_NAME") == name)
      if matches.isEmpty then false
      else if matches.size != 1 then throw new SQLDataException("managed trigger catalog contains duplicate exact name")
      else
        MySqlManagedRoutineValidation.validateTrigger(plan.schema, expected, set, matches.head, root)
        true
    finally
      captured.close()
      catalogCapture = null

  private def inspectRoutine(): Boolean =
    val routineCount = countSchemaRoutines()
    if routineCount == 0L then return false
    if routineCount != 1L then throw new SQLDataException("managed schema contains a routine outside its single fixed audit procedure")
    val captured = MySqlManagedCatalogCapture.captureCatalog(rendered, owner, root)
    catalogCapture = captured
    try
      assertPlannedTableInventory(captured.resultSets)
      assertPlannedTriggerInventory(captured.resultSets)
      val recipe = MySqlManagedStaticRecipe.fromPlan(plan)
      val routineSet = captured.resultSets.find(_.id == "RS10_AUDITSELF").getOrElse(throw new SQLDataException("managed routine catalog DATA is missing"))
      val parameterSet = captured.resultSets.find(_.id == "RS11_AUDITSELF_PARAMETERS").getOrElse(throw new SQLDataException("managed routine parameter DATA is missing"))
      val rows = routineSet.rows.filter(row => fieldText(routineSet, row, "ROUTINE_NAME") == recipe.auditRoutine.name)
      if rows.isEmpty then throw new SQLDataException("managed schema routine inventory is not the exact release audit procedure")
      else if rows.size != 1 then throw new SQLDataException("managed audit procedure catalog row is duplicated")
      else
        MySqlManagedRoutineValidation.validateRoutine(recipe, routineSet, parameterSet, root,
          captured.routineSource.getOrElse(throw new SQLDataException("managed audit procedure SHOW CREATE evidence missing")))
        true
    finally
      captured.close()
      catalogCapture = null

  private def inspectGrant(step: MySqlManagedSchemaPlan.Step): Boolean =
    val captured = MySqlManagedCatalogCapture.captureSecurity(owner, rendered, root)
    securityCapture = captured
    try
      val expected = MySqlDbFirstSchemaProfile.expectedPrivilegeRows(rendered)
      val sets = captured.resultSets
      if sets.map(_.id) != MySqlAuditCoverage.Format3PrivilegeResultSetIds then throw new SQLDataException("managed grant capture DATA shape differs")
      val exactRows = expected.map(_.map(_.map(MySqlAuditText.apply)).map(MySqlAuditRow.apply))
      sets.zip(exactRows).foreach { case (actual, rows) =>
        if actual.columns.map(_.label) != MySqlSchemaAuditor.Format3PrivilegeLabels(sets.indexOf(actual)) then
          throw new SQLDataException("managed grant capture columns differ from the fixed direct-privilege contract")
        if actual.rows.distinct.size != actual.rows.size || actual.rows.exists(row => !rows.contains(row)) then
          throw new SQLDataException("managed grant capture contains an unapproved or duplicate direct privilege")
      }
      val related = MySqlManagedGrantRows.forStep(step.sql, expected)
      if related.forall(_.isEmpty) then
        if !step.sql.startsWith("GRANT EXECUTE ON PROCEDURE ") then
          throw new SQLDataException("managed grant step has no exact direct privilege row mapping")
        inspectRoutineExecuteGrant(step.sql)
      else
        val direct = sets.zipWithIndex.exists { case (set, i) =>
          val wanted = related(i).map(_.map(MySqlAuditText.apply)).map(MySqlAuditRow.apply).toSet
          wanted.nonEmpty && wanted.subsetOf(set.rows.toSet)
        }
        if !direct then false else true
    finally
      captured.close()
      securityCapture = null

  private def inspectRoutineExecuteGrant(sql: String): Boolean =
    val pattern = "(?s)^GRANT EXECUTE ON PROCEDURE `[^`]+`\\.`[^`]+` TO '([A-Za-z0-9_]{1,32})'@'([A-Za-z0-9_.:%-]{1,255})'$".r
    val principal = sql match
      case pattern(user, host) => (user, host)
      case _ => throw new SQLDataException("managed procedure grant differs from its exact renderer syntax")
    routineGrantRows().contains(principal)

  private def processRuntimeUnlock(journalIndex: Int): Unit =
    owner.requireHeldInitializationLock()
    val keyIndex = plan.steps.lastIndexWhere(_.kind == MySqlManagedSchemaPlan.Kind.PrivilegeGrant)
    if keyIndex < 0 || journalIndex != keyIndex + 1 then throw new IllegalStateException("runtime credential effect is out of canonical plan order")
    val stepIndex = journalIndex
    val state = journal.snapshot()
    if state.nextStepIndex() > stepIndex then
      if !inspectAccount(rendered.inputs.accounts.runtime.identity, locked = false) then
        throw new SQLDataException("journal-confirmed runtime password installation is no longer unlocked")
      verifyRuntimeLogin()
      return
    val pending = state.pendingIntentIndex()
    if pending.isPresent && pending.getAsInt != stepIndex then throw new IOException("runtime password journal intent is out of order")
    if !pending.isPresent then journal.recordIntent(stepIndex)
    if !inspectAccount(rendered.inputs.accounts.runtime.identity, locked = false) then
      try executeRuntimePasswordUnlock()
      catch
        case unknown: SqlEffectOutcomeUnknown if unknown.cleanupConfirmed =>
          val applied = try
            val unlocked = inspectAccount(rendered.inputs.accounts.runtime.identity, locked = false)
            if unlocked then verifyRuntimeLogin()
            unlocked
          catch
            case inspectionFailure: Throwable =>
              if inspectionFailure ne unknown then unknown.addSuppressed(inspectionFailure)
              throw unknown
          if !applied then throw unknown
          try journal.confirmEffect(stepIndex)
          catch
            case confirmationFailure: Throwable =>
              if confirmationFailure ne unknown then confirmationFailure.addSuppressed(unknown)
              throw confirmationFailure
          return
      if !inspectAccount(rendered.inputs.accounts.runtime.identity, locked = false) then
        throw new SQLDataException("runtime password installation did not leave the exact runtime account unlocked")
    verifyRuntimeLogin()
    journal.confirmEffect(stepIndex)

  private def executeRuntimePasswordUnlock(): Unit =
    val account = rendered.inputs.accounts.runtime
    owner.requireHeldInitializationLock()
    val statement = owner.connection().prepareStatement(
      s"ALTER USER '${account.user}'@'${account.host}' IDENTIFIED BY ? ACCOUNT UNLOCK")
    var primary: Throwable = null
    try
      statement.setQueryTimeout(owner.queryTimeoutSeconds())
      OwnedIoSecretFiles.withSecret(worker, runtimeCredentialPath, secret =>
        // Connector/J 的 Reader 参数按二进制字面量编码；IDENTIFIED BY 只接受文本密码字面量。
        // JDBC 自身也会创建不可变字符串；此值只在受控回调内绑定，不进入日志或诊断。
        statement.setString(1, new String(secret))
        owner.requireHeldInitializationLock()
        try statement.execute()
        catch
          case failure: SQLException => throw new SqlEffectOutcomeUnknown(failure)
        owner.requireHeldInitializationLock())
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(statement, primary)

  private def verifyRuntimeLogin(): Unit =
    val connection = openRuntimeConnection()
    try
      val observed = readRuntimeIdentity(connection)
      val account = rendered.inputs.accounts.runtime
      if observed._1 != account.identity || observed._3 != "NONE" || !observed._2.startsWith(account.user + "@") then
        throw new SQLDataException("generated runtime credential does not authenticate the exact unlocked account with no active roles")
    finally closeRuntimeConnection()

  private def prepareSemanticBeforeSeeds(): Unit =
    if semantic != null then return
    if identity == null || recipeBytes == null || root == null || owner == null then
      throw new IllegalStateException("persisted identity, exact recipe, live root and installer are required before semantic K")
    owner.requireHeldInitializationLock()
    val connection = openRuntimeConnection()
    try
      val identityRow = readRuntimeIdentity(connection)
      if identityRow._1 != rendered.inputs.accounts.runtime.identity || identityRow._3 != "NONE" ||
        !identityRow._2.startsWith(rendered.inputs.accounts.runtime.user + "@") then
        throw new SQLDataException("runtime temporary session effective user, caller or roles differs from its exact release account")
      val grants = showRuntimeGrants(connection)
      owner.requireHeldInitializationLock()
      catalogCapture = MySqlManagedCatalogCapture.captureCatalog(rendered, owner, root)
      securityCapture = MySqlManagedCatalogCapture.captureSecurity(owner, rendered, root)
      verifyAccountSecurityGraph(expectRoutinePairs = 2)
      semantic = MySqlManagedSemanticProducer.create(root, rendered.inputs, identity.serverUuid(), identity.provisioningUuid(),
        identity.bindingUuid(), identity.creationOperationUuid(), identity.bootstrapOperationUuid(), identity.prepareWriteUuid(),
        identity.certifyWriteUuid(), identityRow._1, identityRow._2, policy, grants, securityCapture.resultSets)
      val metadata = MySqlRuntimeMetadata.parseDbFirstManifestProjection(metadataProjectionFields())
      val c = semantic.configurationDigest(metadata, rendered.inputs, generated, policy)
      try
        if c.length != 32 || c.forall(_ == 0.toByte) then throw new SQLDataException("actual managed semantic C must be a non-placeholder 32-byte digest")
        configurationDigest = c.clone()
        configurationDigestHex = HexFormat.of().formatHex(c)
      finally Arrays.fill(c, 0.toByte)
      val semanticOutput = newBudgetedOutput(MySqlDeploymentReceiptRuntimeContract.MaxBytes, "managed pre-seed semantic DATA")
      var semanticBytesValue: Array[Byte] = null
      try
        semantic.writeSemantic(semanticOutput)
        semanticBytesValue = semanticOutput.toByteArray
        persistOrVerify("pending-seed.semantic", semanticBytesValue)
      finally
        if semanticBytesValue != null then discardPayload(semanticBytesValue)
        releaseOutput(semanticOutput)
      val configBytes = newPayload(configurationDigestHex.length + 1, "managed pre-seed C text") { destination =>
        var index = 0
        while index < configurationDigestHex.length do
          destination(index) = configurationDigestHex.charAt(index).toByte
          index += 1
        destination(index) = '\n'.toByte
      }
      try persistOrVerify("pending-seed.configuration", configBytes)
      finally discardPayload(configBytes)
      closeCaptureOwners()
    finally closeRuntimeConnection()

  private def metadataProjectionFields(): java.util.Map[String, String] =
    if metadataProjection == null then
      metadataProjection = MySqlManagedMetadataProducer.create(root, configuration, identity, root.profile, rendered,
        runtimeCredentialPath, files.directory().resolve(ManagedInstallationBundle.BASELINE_FILE),
        files.directory().resolve(ManagedInstallationBundle.RUNTIME_CONTRACT_FILE))
    metadataProjection.projectionFields

  private def openRuntimeConnection(): VerifiedMySqlConnection =
    val account = rendered.inputs.accounts.runtime
    val credentialReference = new MySqlRuntimeMetadata.FileReference(runtimeCredentialPath)
    val trustReference = new MySqlRuntimeMetadata.FileReference(configuration.trustStorePasswordFile().orElseThrow())
    val credentials = MySqlFileSources.credentials(worker, account.user, credentialReference)
    val trust = MySqlFileSources.trustSecret(worker, trustReference)
    val settings = MySqlConnectionSettings(configuration.host(), configuration.port(), configuration.database(),
      identity.serverUuid(), account.identity, credentials, configuration.trustStore().orElseThrow(), trust,
      Duration.ofMillis(configuration.connectTimeoutMillis()), Duration.ofMillis(configuration.socketTimeoutMillis()),
      MySqlDurabilityExpectation(1, 1L))
    val source = new MySqlConnectionSource(settings)
    runtimeOpen = source.newOwnedSessionOpen()
    runtimeVerified = runtimeOpen.openOnce()
    runtimeVerified

  private def readRuntimeIdentity(connection: VerifiedMySqlConnection): (String, String, String) =
    val query = connection.connection().prepareStatement("SELECT CURRENT_USER(),USER(),CURRENT_ROLE()")
    var primary: Throwable = null
    try
      query.setQueryTimeout(owner.queryTimeoutSeconds())
      val rows = query.executeQuery()
      try
        if !rows.next() then throw new SQLDataException("runtime account identity query returned no row")
        val result = (rows.getString(1), rows.getString(2), rows.getString(3))
        if result._1 == null || result._2 == null || result._3 == null || rows.next() then
          throw new SQLDataException("runtime account identity query returned null or ambiguous fields")
        result
      finally rows.close()
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(query, primary)

  private def showRuntimeGrants(connection: VerifiedMySqlConnection): Vector[String] =
    val query = connection.connection().createStatement()
    var primary: Throwable = null
    try
      query.setQueryTimeout(owner.queryTimeoutSeconds())
      val rows = query.executeQuery("SHOW GRANTS")
      val result = Vector.newBuilder[String]
      try
        var count = 0
        while rows.next() do
          count += 1
          if count > 256 then throw new SQLDataException("runtime SHOW GRANTS exceeds the closed row bound")
          val value = rows.getString(1)
          if value == null || value.length > 8192 then throw new SQLDataException("runtime SHOW GRANTS contains a null or oversized row")
          result += value
        if count == 0 then throw new SQLDataException("runtime SHOW GRANTS is empty")
        result.result()
      finally rows.close()
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(query, primary)

  private def closeRuntimeConnection(): Unit =
    if runtimeVerified != null then
      val current = runtimeVerified
      current.close()
      runtimeVerified = null
      runtimeOpen = null

  private def newBudgetedOutput(maximum: Int, context: String): BudgetedByteArrayOutputStream =
    val stream = new BudgetedByteArrayOutputStream(root, maximum, context)
    byteStreams :+= stream
    stream.initialize()
    stream

  private def newPayload(length: Int, context: String)(fill: Array[Byte] => Unit): Array[Byte] =
    if root == null || length <= 0 || length > ManagedInstallationFiles.MAX_BUNDLE_BYTES || fill == null then
      throw new IllegalArgumentException("bounded managed payload allocation is required")
    val charge = Math.addExact(length.toLong, 64L)
    val retained = RetainedPayload(null, charge, context, charged = false)
    pendingPayloadBytes :+= retained
    var bytes: Array[Byte] = null
    try
      root.reserveRetainedMemory(charge, context + " before allocation")
      retained.charged = true
      bytes = new Array[Byte](length)
      retained.bytes = bytes
      fill(bytes)
      bytes
    catch
      case failure: Throwable =>
        if bytes != null then Arrays.fill(bytes, 0.toByte)
        retained.bytes = null
        if retained.charged then
          try
            root.releaseRetainedMemory(charge, context + " failed allocation detached")
            retained.charged = false
            pendingPayloadBytes = pendingPayloadBytes.filterNot(_ eq retained)
          catch
            case cleanup: Throwable =>
              if cleanup ne failure then failure.addSuppressed(cleanup)
        else pendingPayloadBytes = pendingPayloadBytes.filterNot(_ eq retained)
        throw failure

  private def discardPayload(bytes: Array[Byte]): Unit =
    val retained = pendingPayloadBytes.find(entry => entry.bytes eq bytes)
      .getOrElse(throw new IllegalStateException("managed payload bytes have no retained-memory owner"))
    discardPayload(retained)

  private def discardPayload(retained: RetainedPayload): Unit =
    if retained == null || !pendingPayloadBytes.exists(_ eq retained) then
      throw new IllegalStateException("managed payload owner is not registered")
    if retained.bytes != null then
      Arrays.fill(retained.bytes, 0.toByte)
      retained.bytes = null
    if retained.charged then
      root.releaseRetainedMemory(retained.charge, retained.context + " references cleared")
      retained.charged = false
    pendingPayloadBytes = pendingPayloadBytes.filterNot(_ eq retained)

  private def releaseOutput(stream: BudgetedByteArrayOutputStream): Unit =
    if stream == null || !byteStreams.exists(_ eq stream) then
      throw new IllegalStateException("managed DATA output stream has no retained-memory owner")
    stream.release()
    byteStreams = byteStreams.filterNot(_ eq stream)

  private def closeCaptureOwners(): Unit =
    var failure: Throwable = null
    def settle(action: => Unit): Unit =
      try action
      catch
        case cleanup: Throwable =>
          if failure == null then failure = cleanup
          else if cleanup ne failure then failure.addSuppressed(cleanup)
    if catalogCapture != null then
      val current = catalogCapture
      settle { current.close(); catalogCapture = null }
    if securityCapture != null then
      val current = securityCapture
      settle { current.close(); securityCapture = null }
    if failure != null then throw failure

  private def cleanupCompletedDataOwners(): Unit =
    var failure: Throwable = null
    def settle(action: => Unit): Unit =
      try action
      catch
        case cleanup: Throwable =>
          if failure == null then failure = cleanup
          else if cleanup ne failure then failure.addSuppressed(cleanup)
    settle { closeCaptureOwners() }
    if semantic != null then
      val current = semantic
      settle { current.close(); semantic = null }
    if semanticReparse != null then
      val current = semanticReparse
      settle { current.close(); semanticReparse = null }
    if serializedMetadata != null then
      val current = serializedMetadata
      settle { current.close(); serializedMetadata = null }
    if metadataProjection != null then
      val current = metadataProjection
      settle { current.close(); metadataProjection = null }
    if assembly != null then
      val current = assembly
      settle { current.close(); assembly = null }
    pendingPayloadBytes.toVector.foreach(entry => settle { discardPayload(entry) })
    byteStreams.toVector.foreach(stream => settle { releaseOutput(stream) })
    if recipeBytes != null then
      settle { Arrays.fill(recipeBytes, 0.toByte); recipeBytes = null }
    if configurationDigest != null then
      settle { Arrays.fill(configurationDigest, 0.toByte); configurationDigest = null }
    if failure != null then throw failure

  private def clearPlanState(): Unit =
    if owner != null && owner.retainsConnection then
      throw new IllegalStateException("managed SQL owner still retains its original connection")
    if runtimeOpen != null && runtimeOpen.hasRawHandle() && !runtimeOpen.closeReturned() then
      throw new IllegalStateException("managed verification open still retains an unclosed raw JDBC handle")
    rendered = null
    plan = null
    identity = null
    if recipeBytes != null then Arrays.fill(recipeBytes, 0.toByte)
    recipeBytes = null
    if configurationDigest != null then Arrays.fill(configurationDigest, 0.toByte)
    configurationDigest = null
    configurationDigestHex = null
    runtimeCredentialPath = null
    policy = null
    generated = null
    seedValues = null
    runtimeOpen = null
    owner = null

  private def completeAndPublish(): ProductionBootstrapControl.DbFirstControl =
    owner.requireHeldInitializationLock()
    catalogCapture = MySqlManagedCatalogCapture.captureCatalog(rendered, owner, root)
    securityCapture = MySqlManagedCatalogCapture.captureSecurity(owner, rendered, root)
    val finalCatalog = catalogCapture
    val finalSecurity = securityCapture
    var manifest: MySqlApprovedObjectManifest = null
    var manifestHash: String = null
    var captureSha: String = null
    var captureBytes: Array[Byte] = null
    var baselineBytes: Array[Byte] = null
    var contractBytes: Array[Byte] = null
    var metadataBytes: Array[Byte] = null
    try
      manifest = MySqlManagedStaticValidation.validate(rendered, finalCatalog.resultSets, root, finalCatalog.routineSource.getOrElse(throw new SQLDataException("managed final SHOW CREATE evidence missing")))
      manifestHash = manifest.canonicalSha256
      if !inspectRoutine() || countSchemaEvents() != 0L then
        throw new SQLDataException("managed final schema inventory contains an unapproved routine or event")
      verifyFinalSecurityMatches(finalSecurity.resultSets)
      verifyAccountSecurityGraph(expectRoutinePairs = 2)

      val captureOutput = newBudgetedOutput(ManagedInstallationFiles.MAX_BUNDLE_BYTES, "actual source-capture DATA")
      captureSha = MySqlManagedCaptureEncoder.write(identity, owner, finalCatalog, finalSecurity, captureOutput, root)
      captureBytes = captureOutput.toByteArray
      if captureSha != shaHex(captureBytes) then throw new SQLDataException("actual source-capture digest differs from its exact durable bytes")
      releaseOutput(captureOutput)

      val baselineIdentity = MySqlApprovedBaselineIdentity(MySqlApprovedBaselineLoader.ApprovedFormat,
        MySqlApprovedBaselineLoader.DbFirstSchemaFormat, MySqlDbFirstSchemaDefinition.Coverage,
        identity.serverUuid(), MySqlConnectionSource.SupportedServerVersion, identity.schema(), identity.provisioningUuid(),
        identity.bindingUuid(), rendered.sha256.toUpperCase(java.util.Locale.ROOT), rendered.inputs.accounts.auditDefiner.identity,
        captureSha.toUpperCase(java.util.Locale.ROOT))
      val baselineOutput = newBudgetedOutput(MySqlApprovedBaselineLoader.MaxApprovedFileBytes, "actual B baseline")
      val baselineSha = MySqlStaticBaselineEncoder.write(baselineIdentity, manifest, baselineOutput, root)
      baselineBytes = baselineOutput.toByteArray
      if baselineSha != shaHex(baselineBytes) then throw new SQLDataException("actual B digest differs from its exact durable bytes")
      releaseOutput(baselineOutput)
      manifest = null
      closeCaptureOwners()

      val contractOutput = newBudgetedOutput(MySqlDeploymentReceiptRuntimeContract.MaxBytes, "actual K runtime contract")
      semantic.writeFullK(contractOutput, baselineSha, captureSha)
      contractBytes = contractOutput.toByteArray
      releaseOutput(contractOutput)
      val contractSha = shaHex(contractBytes)
      if metadataProjection == null then throw new IllegalStateException("full managed metadata projection was not retained")
      serializedMetadata = metadataProjection.serializeFull(MySqlManagedMetadataProducer.Pins(baselineSha, captureSha, contractSha))
      val metadataOutput = newBudgetedOutput(MySqlRuntimeMetadata.MAX_METADATA_BYTES, "actual schema6 metadata")
      serializedMetadata.writeTo(metadataOutput)
      metadataBytes = metadataOutput.toByteArray
      releaseOutput(metadataOutput)

      persistOrVerify(ManagedInstallationBundle.SOURCE_CAPTURE_FILE, captureBytes)
      persistOrVerify(ManagedInstallationBundle.BASELINE_FILE, baselineBytes)
      persistOrVerify(ManagedInstallationBundle.RUNTIME_CONTRACT_FILE, contractBytes)
      persistOrVerify(ManagedInstallationBundle.RUNTIME_METADATA_FILE, metadataBytes)
      verifyNormalAssemblyC(metadataBytes)

      val sealIndex = journalSteps(plan).size - 1
      performReadySeal(manifestHash, sealIndex)
      if journal.snapshot().phase() == ManagedInstallationJournal.Phase.ALL_STEPS_CONFIRMED then journal.recordSealConfirmed()
      else if journal.snapshot().phase() != ManagedInstallationJournal.Phase.SEALED then
        throw new IOException("managed seal journal phase differs from the exact completed installation")
      val loaded = ManagedInstallationBundle.publish(files, journal, identity,
        new ManagedInstallationBundle.Payload(baselineBytes, shaHex(baselineBytes)),
        new ManagedInstallationBundle.Payload(contractBytes, shaHex(contractBytes)),
        new ManagedInstallationBundle.Payload(captureBytes, captureSha),
        new ManagedInstallationBundle.Payload(metadataBytes, shaHex(metadataBytes)), configurationDigestHex)
      val actual = ManagedInstallationBundle.load(files, identity).orElseThrow(() => new IOException("managed active bundle failed final re-read"))
      if actual != loaded then throw new IOException("managed active bundle changed between publication and final readback")
      owner.requireHeldInitializationLock()
      owner.releaseInitializationLock()
      if !owner.lockReleaseConfirmed() then throw new SQLException("managed initialization target lock release was not confirmed")
      owner.close()
      owner = null
      pendingPayloadBytes.foreach(discardPayload)
      captureBytes = null; baselineBytes = null; contractBytes = null; metadataBytes = null
      cleanupCompletedDataOwners()
      budgets.close()
      budgets = null
      closeJournal()
      configuration.close()
      val finalConfigurationDigest = configurationDigestHex
      clearPlanState()
      disposed = true
      ProductionBootstrapControl.createManaged(worldPath, actual, files, bootIdentity, versionedPolicy,
        ManagedBootstrapPolicy.barrierWait(), dependencyPins, finalConfigurationDigest)
    finally
      manifest = null
      manifestHash = null

  private def verifyFinalSecurityMatches(actualSecurity: Vector[MySqlAuditResultSet]): Unit =
    val fields = metadataProjectionFields()
    val deployment = MySqlRuntimeMetadata.parseDbFirstManifestProjection(fields)
    val semanticData = semantic
    val reprojected = semanticData.configurationDigest(deployment, rendered.inputs, generated, policy)
    try
      if !MessageDigest.isEqual(reprojected, configurationDigest) then
        throw new SQLDataException("managed semantic DATA configuration digest changed after the fixed seed boundary")
    finally Arrays.fill(reprojected, 0.toByte)
    val raw = newBudgetedOutput(MySqlDeploymentReceiptRuntimeContract.MaxBytes, "managed semantic reparse")
    var rawBytes: Array[Byte] = null
    var primary: Throwable = null
    try
      semantic.writeSemantic(raw)
      rawBytes = raw.toByteArray
      semanticReparse = MySqlDeploymentReceiptRuntimeContract.parseManagedDbFirstSemantic(rawBytes, root, rendered.inputs, generated)
      if !MySqlManagedGrantRows.sameSecurityData(semanticReparse.privileges, actualSecurity) then
        throw new SQLDataException("direct-grant JDBC DATA changed after the pre-seed K/C pins were fixed")
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally
      var cleanupFailure: Throwable = null
      def settle(action: => Unit): Unit =
        try action
        catch
          case cleanup: Throwable =>
            if cleanupFailure == null then cleanupFailure = cleanup
            else if cleanup ne cleanupFailure then cleanupFailure.addSuppressed(cleanup)
      if semanticReparse != null then
        val current = semanticReparse
        settle { current.close(); semanticReparse = null }
      if rawBytes != null then settle { discardPayload(rawBytes); rawBytes = null }
      settle { releaseOutput(raw) }
      if cleanupFailure != null then
        if primary == null then throw cleanupFailure
        else if cleanupFailure ne primary then primary.addSuppressed(cleanupFailure)

  private def verifyNormalAssemblyC(metadataBytes: Array[Byte]): Unit =
    val metadata = MySqlRuntimeMetadata.fromDbFirstBytes(metadataBytes)
    assembly = MySqlRuntimeInputs.beginV2(budgets.assemblyRoot, worker)
    val graph = MySqlRuntimeInputs.assembleDbFirstStatic(metadata, assembly, policy)
    if graph.configurationSha256 == null || !graph.configurationSha256.equalsIgnoreCase(configurationDigestHex) then
      throw new SQLDataException("normal runtime schema6 assembly C differs from the exact pre-seed C and seed value")
    assembly.close()
    assembly = null

  private def performReadySeal(manifestHash: String, journalIndex: Int): Unit =
    val state = journal.snapshot()
    val pending = state.pendingIntentIndex()
    if state.nextStepIndex() > journalIndex then
      verifyReadyRow(manifestHash, ready = true)
      return
    if pending.isPresent && pending.getAsInt != journalIndex then throw new IOException("schema-ready seal journal intent is out of order")
    if !pending.isPresent then journal.recordIntent(journalIndex)
    if !verifyReadyRow(manifestHash, ready = true) then
      verifyReadyRow(manifestHash, ready = false)
      val update = owner.connection().prepareStatement(s"""UPDATE `${plan.schema}`.`fp_schema_manifest`
        SET bootstrap_state='READY',guard_manifest_sha256=? WHERE singleton_fixed1=1 AND backend='MYSQL' AND format_version=6
        AND bootstrap_state='BOOTSTRAPPING' AND guard_manifest_sha256 IS NULL AND import_state='NONE'
        AND import_operation_uuid IS NULL AND import_manifest_sha256 IS NULL AND import_snapshot_sha256 IS NULL AND import_receipt_sha256 IS NULL
        AND configuration_sha256=? AND ddl_sha256=? AND expected_server_uuid=? AND expected_binding_uuid=? AND provisioning_uuid=?
        AND source_provenance_kind='MANAGED_INSTALL' AND source_provenance_bytes=? AND source_provenance_sha256=?
        AND limit_profile_encoding=? AND limit_profile_bytes=? AND limit_profile_sha256=?
        AND max_region_bytes=? AND max_chunk_bytes=? AND max_decompressed_bytes=? AND max_nbt_depth=? AND max_members=?
        AND max_retained_bytes=? AND max_sql_packet_bytes=? AND max_native_artifact_bytes=?""")
      var primary: Throwable = null
      try
        update.setQueryTimeout(owner.queryTimeoutSeconds()); update.setBytes(1, HexFormat.of().parseHex(manifestHash))
        update.setBytes(2, configurationDigest); update.setBytes(3, HexFormat.of().parseHex(rendered.sha256))
        update.setBytes(4, uuidBytes(identity.serverUuid())); update.setBytes(5, uuidBytes(identity.bindingUuid()))
        update.setBytes(6, uuidBytes(identity.provisioningUuid())); update.setBytes(7, recipeBytes)
        update.setBytes(8, shaBytes(recipeBytes))
        val profile = rendered.inputs.sourceLimitProfile
        update.setString(9, SourceLimitProfile.ENCODING); update.setBytes(10, profile.canonicalBytes())
        update.setBytes(11, profile.sha256())
        Vector(profile.maxRegionBytes(), profile.maxChunkBytes(), profile.maxDecompressedBytes(), profile.maxNbtDepth().toLong,
          profile.maxMembers().toLong, profile.maxRetainedBytes(), profile.maxSqlPacketBytes(), profile.maxNativeArtifactBytes())
          .zipWithIndex.foreach { case (value, index) => update.setLong(12 + index, value) }
        val changed = update.executeUpdate()
        if changed != 1 then throw new SQLDataException("managed schema-ready conditional seal did not update exactly one manifest row")
      catch
        case failure: Throwable =>
          primary = failure
          throw failure
      finally closePreserving(update, primary)
    if !verifyReadyRow(manifestHash, ready = true) then throw new SQLDataException("managed schema-ready seal failed exact database readback")
    journal.confirmEffect(journalIndex)

  private def verifyReadyRow(manifestHash: String, ready: Boolean): Boolean =
    val total = scalarLong(owner.connection(), s"SELECT COUNT(*) FROM `${plan.schema}`.`fp_schema_manifest`", Seq.empty)
    if total != 1L then throw new SQLDataException("managed manifest table is not its single fixed control row at the seal boundary")
    val state = if ready then "READY" else "BOOTSTRAPPING"
    val guardPredicate = if ready then "guard_manifest_sha256=?" else "guard_manifest_sha256 IS NULL"
    val query = owner.connection().prepareStatement(s"""SELECT COUNT(*) FROM `${plan.schema}`.`fp_schema_manifest`
      WHERE singleton_fixed1=1 AND backend='MYSQL' AND format_version=6 AND ddl_sha256=? AND provisioning_uuid=?
      AND bootstrap_state='$state' AND expected_server_uuid=? AND expected_binding_uuid=? AND import_state='NONE'
      AND import_operation_uuid IS NULL AND import_manifest_sha256 IS NULL AND import_snapshot_sha256 IS NULL AND import_receipt_sha256 IS NULL
      AND configuration_sha256=? AND $guardPredicate AND source_provenance_kind='MANAGED_INSTALL'
      AND source_provenance_bytes=? AND source_provenance_sha256=? AND limit_profile_encoding=? AND limit_profile_bytes=?
      AND limit_profile_sha256=? AND max_region_bytes=? AND max_chunk_bytes=? AND max_decompressed_bytes=? AND max_nbt_depth=?
      AND max_members=? AND max_retained_bytes=? AND max_sql_packet_bytes=? AND max_native_artifact_bytes=?""")
    var primary: Throwable = null
    try
      query.setQueryTimeout(owner.queryTimeoutSeconds()); query.setBytes(1, HexFormat.of().parseHex(rendered.sha256))
      query.setBytes(2, uuidBytes(identity.provisioningUuid())); query.setBytes(3, uuidBytes(identity.serverUuid()))
      query.setBytes(4, uuidBytes(identity.bindingUuid())); query.setBytes(5, configurationDigest)
      var n = 6
      if ready then { query.setBytes(n, HexFormat.of().parseHex(manifestHash)); n += 1 }
      query.setBytes(n, recipeBytes); n += 1; query.setBytes(n, shaBytes(recipeBytes)); n += 1
      val profile = rendered.inputs.sourceLimitProfile
      query.setString(n, SourceLimitProfile.ENCODING); n += 1; query.setBytes(n, profile.canonicalBytes()); n += 1
      query.setBytes(n, profile.sha256()); n += 1
      Vector(profile.maxRegionBytes(), profile.maxChunkBytes(), profile.maxDecompressedBytes(), profile.maxNbtDepth().toLong,
        profile.maxMembers().toLong, profile.maxRetainedBytes(), profile.maxSqlPacketBytes(), profile.maxNativeArtifactBytes()).foreach { value => query.setLong(n, value); n += 1 }
      val rows = query.executeQuery()
      try
        if !rows.next() then throw new SQLDataException("managed manifest seal readback returned no row")
        val count = rows.getLong(1)
        if rows.wasNull() || rows.next() then throw new SQLDataException("managed manifest seal readback returned an invalid shape")
        if count == 1L then true else if count == 0L then false else throw new SQLDataException("managed manifest seal identity is duplicated")
      finally rows.close()
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(query, primary)

  private def verifyAccountSecurityGraph(expectRoutinePairs: Int): Unit =
    val accounts = rendered.inputs.accounts
    accounts.all.foreach { account =>
      val locked = if account == accounts.runtime && expectRoutinePairs > 0 then false else true
      if !inspectAccount(account.identity, locked) then throw new SQLDataException("managed account security graph is missing a planned exact account")
    }
    val names = accounts.all.map(_.user)
    val userPrefix = managedUserPrefix(accounts.runtime.user)
    val predicate = names.map(_ => "BINARY User=BINARY ?").mkString("(", " OR ", ")")
    val lockQuery = owner.connection().prepareStatement("SELECT COUNT(*),COALESCE(SUM(account_locked='Y'),0) FROM mysql.user WHERE " + predicate)
    var primary: Throwable = null
    var result = (0L, 0L)
    try
      lockQuery.setQueryTimeout(owner.queryTimeoutSeconds()); names.zipWithIndex.foreach((name, i) => lockQuery.setString(i + 1, name))
      val rows = lockQuery.executeQuery()
      try
        if !rows.next() then throw new SQLDataException("managed helper lock census returned no row")
        result = (rows.getLong(1), rows.getLong(2))
        if rows.wasNull() || rows.next() then throw new SQLDataException("managed helper lock census is ambiguous")
      finally rows.close()
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(lockQuery, primary)
    if result != (6L, if expectRoutinePairs > 0 then 5L else 6L) then
      throw new SQLDataException("managed runtime/helper account lock graph differs from the exact six-account policy")
    val namespaceAccounts = scalarLong(owner.connection(),
      "SELECT COUNT(*) FROM mysql.user WHERE " + namespaceUserPredicate("User"), namespaceUserValues(userPrefix))
    if namespaceAccounts != 6L then throw new SQLDataException("managed generated account namespace contains an extra or missing account")

    val rolePredicate = names.map(_ => "BINARY TO_USER=BINARY ?").mkString("(", " OR ", ")") + " OR " +
      names.map(_ => "BINARY FROM_USER=BINARY ?").mkString("(", " OR ", ")") + " OR " +
      namespaceUserPredicate("TO_USER") + " OR " + namespaceUserPredicate("FROM_USER")
    val edgeCount = scalarLong(owner.connection(), "SELECT COUNT(*) FROM mysql.role_edges WHERE " + rolePredicate,
      names ++ names ++ namespaceUserValues(userPrefix) ++ namespaceUserValues(userPrefix))
    if edgeCount != 0L then throw new SQLDataException("managed account has an incoming or outgoing role edge")
    val defaultPredicate = names.map(_ => "BINARY USER=BINARY ?").mkString("(", " OR ", ")") + " OR " + namespaceUserPredicate("USER")
    val defaultCount = scalarLong(owner.connection(), "SELECT COUNT(*) FROM mysql.default_roles WHERE " + defaultPredicate,
      names ++ namespaceUserValues(userPrefix))
    if defaultCount != 0L then throw new SQLDataException("managed account has an enabled default role")
    val mandatory = oneText(owner.connection(), "SELECT COALESCE(@@GLOBAL.mandatory_roles,'')", owner.queryTimeoutSeconds())
    if mandatory.nonEmpty then throw new SQLDataException("MySQL mandatory_roles must be empty for the managed schema6 profile")
    verifyAdministrativeProxyRows(rendered.inputs, names, userPrefix)
    val procedureRows = routineGrantRows()
    if procedureRows.size != expectRoutinePairs then throw new SQLDataException("managed audit routine requires its exact zero-or-two EXECUTE grant phase")

  private def verifyNoAccountSecurityEdges(profile: MySqlDeploymentReceiptSchemaProfile.Inputs, expectRoutinePairs: Int): Unit =
    val accounts = profile.accounts
    val names = accounts.all.map(_.user)
    val userPrefix = managedUserPrefix(accounts.runtime.user)
    val target = names.map(_ => "BINARY User=BINARY ?").mkString("(", " OR ", ")") + " OR " + namespaceUserPredicate("User")
    val userCount = scalarLong(owner.connection(), "SELECT COUNT(*) FROM mysql.user WHERE " + target,
      names ++ namespaceUserValues(userPrefix))
    if userCount != 0L then throw new SQLDataException("first managed installation found a pre-existing generated MySQL account")
    val rolePredicate = names.map(_ => "BINARY TO_USER=BINARY ?").mkString("(", " OR ", ")") + " OR " +
      names.map(_ => "BINARY FROM_USER=BINARY ?").mkString("(", " OR ", ")") + " OR " +
      namespaceUserPredicate("TO_USER") + " OR " + namespaceUserPredicate("FROM_USER")
    val edges = scalarLong(owner.connection(), "SELECT COUNT(*) FROM mysql.role_edges WHERE " + rolePredicate,
      names ++ names ++ namespaceUserValues(userPrefix) ++ namespaceUserValues(userPrefix))
    val defaultPredicate = names.map(_ => "BINARY USER=BINARY ?").mkString("(", " OR ", ")") + " OR " + namespaceUserPredicate("USER")
    val defaults = scalarLong(owner.connection(), "SELECT COUNT(*) FROM mysql.default_roles WHERE " + defaultPredicate,
      names ++ namespaceUserValues(userPrefix))
    if edges != 0L || defaults != 0L then throw new SQLDataException("first managed installation found pre-existing role/default-role edges")
    if routineGrantRows().nonEmpty then throw new SQLDataException("first managed installation found pre-existing routine privileges")
    val mandatory = oneText(owner.connection(), "SELECT COALESCE(@@GLOBAL.mandatory_roles,'')", owner.queryTimeoutSeconds())
    if mandatory.nonEmpty then throw new SQLDataException("MySQL mandatory_roles must be empty for managed schema6")
    verifyAdministrativeProxyRows(profile, names, userPrefix)

  private def verifyAdministrativeProxyRows(profile: MySqlDeploymentReceiptSchemaProfile.Inputs,
      names: Vector[String], userPrefix: String): Unit =
    owner.requireHeldInitializationLock()
    // 候选集合仍包含匿名、通配、命名空间和畸形行；只在逐行精确验证后接受管理例外。
    val sql = "SELECT User,Host,Proxied_user,Proxied_host,With_grant FROM mysql.proxies_priv WHERE " +
      proxyAccountPredicate(names, userPrefix) + " LIMIT 65"
    Using.resource(owner.connection().prepareStatement(sql)) { statement =>
      statement.setQueryTimeout(owner.queryTimeoutSeconds())
      proxyAccountValues(names, userPrefix).zipWithIndex.foreach { (value, index) =>
        statement.setString(index + 1, value.asInstanceOf[String])
      }
      Using.resource(statement.executeQuery()) { rows =>
        var count = 0
        while rows.next() do
          count += 1
          if count > 64 then throw new SQLDataException("managed administrative proxy inspection exceeds its row bound")
          val user = rows.getString(1)
          val host = rows.getString(2)
          val proxiedUser = rows.getString(3)
          val proxiedHost = rows.getString(4)
          val grant = rows.getInt(5)
          val withGrant: java.lang.Integer = if rows.wasNull() then null else Integer.valueOf(grant)
          MySqlManagedPrivilegePreflight.requireApprovedAdministrativeProxy(
            profile, user, host, proxiedUser, proxiedHost, withGrant)
      }
    }
    owner.requireHeldInitializationLock()

  private def routineGrantRows(): Set[(String, String)] =
    val accounts = rendered.inputs.accounts
    val names = accounts.all.map(_.user)
    val userPrefix = managedUserPrefix(accounts.runtime.user)
    val predicate = names.map(_ => "BINARY User=BINARY ?").mkString("(", " OR ", ")") + " OR " + namespaceUserPredicate("User")
    val query = owner.connection().prepareStatement("SELECT User,Host,Db,Routine_name,Routine_type,Proc_priv FROM mysql.procs_priv WHERE " + predicate)
    var primary: Throwable = null
    try
      query.setQueryTimeout(owner.queryTimeoutSeconds())
      names.zipWithIndex.foreach((name, i) => query.setString(i + 1, name))
      namespaceUserValues(userPrefix).zipWithIndex.foreach((value, i) => query.setString(names.size + i + 1, value.asInstanceOf[String]))
      val rows = query.executeQuery(); val found = scala.collection.mutable.Set.empty[(String, String)]
      try
        while rows.next() do
          val user = rows.getString(1); val host = rows.getString(2)
          val exact = Vector(accounts.auditDefiner, accounts.runtime).exists(a => a.user == user && a.host == host) &&
            rows.getString(3) == plan.schema && rows.getString(4) == MySqlDbFirstSchemaDefinition.AuditRoutine &&
            rows.getString(5) == "PROCEDURE" && rows.getString(6) == "Execute"
          if !exact || !found.add((user, host)) then throw new SQLDataException("managed direct routine privilege row differs from the exact release plan")
      finally rows.close()
      found.toSet
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(query, primary)

  private def managedUserPrefix(runtimeUser: String): String =
    val separator = if runtimeUser == null then -1 else runtimeUser.lastIndexOf('_')
    if separator <= 0 then throw new SQLDataException("managed account namespace is malformed")
    runtimeUser.substring(0, separator + 1)

  private def namespaceUserPredicate(column: String): String =
    s"LEFT(BINARY $column,CHAR_LENGTH(BINARY ?))=BINARY ?"

  private def namespaceUserValues(prefix: String): Seq[AnyRef] = Seq(prefix, prefix)

  private def proxyAccountPredicate(names: Vector[String], prefix: String): String =
    "(User IS NULL OR Host IS NULL OR Proxied_user IS NULL OR Proxied_host IS NULL OR With_grant IS NULL OR User='' OR Proxied_user='' OR User IN (" +
      names.map(_ => "?").mkString(",") + ") OR Proxied_user IN (" + names.map(_ => "?").mkString(",") + ") OR " +
      namespaceUserPredicate("User") + " OR " + namespaceUserPredicate("Proxied_user") +
      " OR LOCATE('%',User)>0 OR LOCATE('_',User)>0 OR LOCATE(CHAR(92),User)>0 OR LOCATE('%',Proxied_user)>0 OR LOCATE('_',Proxied_user)>0 OR LOCATE(CHAR(92),Proxied_user)>0)"

  private def proxyAccountValues(names: Vector[String], prefix: String): Seq[AnyRef] =
    names ++ names ++ namespaceUserValues(prefix) ++ namespaceUserValues(prefix)

  private def executeSql(sql: String): Unit =
    if sql == null || sql.startsWith("CREATE DATABASE ") || sql.startsWith("DROP ") then
      throw new SQLDataException("managed schema executor rejected forbidden database creation or destructive SQL")
    owner.requireHeldInitializationLock()
    val statement = owner.connection().createStatement()
    var preFailure: Throwable = null
    try statement.setQueryTimeout(owner.queryTimeoutSeconds())
    catch
      case failure: Throwable =>
        preFailure = failure
    if preFailure != null then
      closePreserving(statement, preFailure)
      throw preFailure
    var executionFailure: Throwable = null
    try statement.execute(sql)
    catch
      case failure: Throwable =>
        executionFailure = failure
    if executionFailure != null then
      closePreserving(statement, executionFailure)
      executionFailure match
        case sqlFailure: SQLException => throw new SqlEffectOutcomeUnknown(sqlFailure)
        case other => throw other
    var postFailure: Throwable = null
    try owner.requireHeldInitializationLock()
    catch
      case failure: Throwable =>
        postFailure = failure
        throw failure
    finally closePreserving(statement, postFailure)

  private def requireExactPlanStep(step: MySqlManagedSchemaPlan.Step, ordinal: Int): Unit =
    if step == null || ordinal < 0 || ordinal >= plan.steps.size || (plan.steps(ordinal) ne step) ||
      step.ordinal != ordinal || shaHex(step.sql.getBytes(StandardCharsets.UTF_8)) != step.sha256 then
      throw new SQLDataException("managed executor must consume each exact original canonical plan step in ordinal order")

  private def persistOrVerify(name: String, bytes: Array[Byte]): Unit =
    if bytes == null || bytes.length == 0 || bytes.length > ManagedInstallationFiles.MAX_BUNDLE_BYTES then
      throw new IOException("managed pending payload is outside its fixed durable byte bound")
    try files.publishNewFile(name, bytes)
    catch
      case exists: FileAlreadyExistsException =>
        val current = files.readPrivateFile(name, ManagedInstallationFiles.MAX_BUNDLE_BYTES)
        try
          if !MessageDigest.isEqual(current, bytes) then throw new IOException("existing managed pending payload differs; no replacement was attempted", exists)
        finally Arrays.fill(current, 0.toByte)

  private def rejectOrphanPublicationWithoutActive(): Unit =
    val active = files.directory().resolve(ManagedInstallationBundle.ACTIVE_POINTER_FILE)
    if Files.exists(active, LinkOption.NOFOLLOW_LINKS) then throw new IOException("active managed pointer exists without its stable identity")

  private def rejectOrphanFilesWithoutIdentity(): Unit =
    val entries = Files.newDirectoryStream(files.directory())
    var primary: Throwable = null
    try
      val iterator = entries.iterator()
      if iterator.hasNext then
        throw new IOException("managed installation directory contains entries without a stable installation identity: " + iterator.next().getFileName)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(entries, primary)

  private def rejectUnjournaledFiles(profile: MySqlDeploymentReceiptSchemaProfile.Inputs): Unit =
    val allowed = Set(ManagedInstallationIdentity.FILE_NAME, "installation.recipe",
      "runtime-" + profile.accounts.runtime.user + ".secret")
    val entries = Files.newDirectoryStream(files.directory())
    var primary: Throwable = null
    try
      val iterator = entries.iterator()
      while iterator.hasNext do
        val name = iterator.next().getFileName.toString
        if !allowed.contains(name) then
          throw new IOException("unjournaled managed directory contains ambiguous state: " + name)
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(entries, primary)

  private def canonicalRecipe(record: ManagedInstallationIdentity.Record): Array[Byte] =
    val values = Vector(record.creationOperationUuid().toString, record.bootstrapOperationUuid().toString,
      record.prepareWriteUuid().toString, record.certifyWriteUuid().toString, record.bindingUuid().toString, "2",
      record.serverUuid().toString, record.schema(), "6", record.recipeSha256(), record.provisioningUuid().toString)
    val keys = Vector("creationOperationUuid", "bootstrapOperationUuid", "prepareWriteUuid", "certifyWriteUuid",
      "bindingUuid", "physicalFormat", "mysqlServerUuid", "schema", "schemaFormat", "ddlSha256", "provisioningUuid")
    (MySqlDeploymentReceiptRuntimeContract.ManagedDbFirstRecipeEncoding + "\n" + keys.zip(values).map((k, v) => s"$k=$v\n").mkString)
      .getBytes(StandardCharsets.UTF_8)

  private def journalSteps(value: MySqlManagedSchemaPlan.Plan): Vector[ManagedInstallationJournal.Step] =
    val result = Vector.newBuilder[ManagedInstallationJournal.Step]
    val lastGrant = value.steps.lastIndexWhere(_.kind == MySqlManagedSchemaPlan.Kind.PrivilegeGrant)
    if lastGrant < 0 then throw new SQLDataException("managed journal requires the exact renderer grant closure")
    value.steps.zipWithIndex.foreach { case (step, index) =>
      val keyDigest = shaHex((step.ordinal.toString + "\n" + step.identity + "\n").getBytes(StandardCharsets.UTF_8))
      result += new ManagedInstallationJournal.Step(f"step-$index%04d-$keyDigest", step.sha256)
      if index == lastGrant then result += new ManagedInstallationJournal.Step("runtime-account-password-unlock", shaHex("FP_MANAGED_RUNTIME_PASSWORD_UNLOCK_V1".getBytes(StandardCharsets.US_ASCII)))
    }
    result += new ManagedInstallationJournal.Step("schema-bootstrap-ready-seal", shaHex((value.schema + "\n" + value.ddlSha256 + "\nFP_MANAGED_SCHEMA_READY_V1\n").getBytes(StandardCharsets.UTF_8)))
    result.result()

  private def journalIndexForPlan(ordinal: Int): Int =
    if ordinal < 0 || ordinal >= plan.steps.size then throw new IllegalArgumentException("managed plan ordinal outside exact plan")
    val lastGrant = plan.steps.lastIndexWhere(_.kind == MySqlManagedSchemaPlan.Kind.PrivilegeGrant)
    ordinal + (if ordinal > lastGrant then 1 else 0)

  private def journalIdentity(record: ManagedInstallationIdentity.Record): ManagedInstallationJournal.Identity =
    new ManagedInstallationJournal.Identity(record.installationUuid(), record.serverUuid(), record.schema(),
      record.configurationSha256(), record.recipeSha256())

  private def closeJournalForRestart(): Unit =
    val current = journal
    current.close()
    journal = null

  private def closeJournal(): Unit =
    if journal != null then
      val current = journal
      current.close()
      journal = null

  def cleanupAfterFailure(primary: Throwable): Unit =
    requireWorker()
    primaryFailure = primary
    try closeOnOwnedIoThread()
    catch
      case cleanup: Throwable =>
        if cleanup ne primary then primary.addSuppressed(cleanup)

  def closeOnOwnedIoThread(): Unit =
    requireWorker()
    if disposed then
      if closeFailure != null then throw new IllegalStateException("managed installation cleanup remains unresolved", closeFailure)
      return
    val originalFailure = primaryFailure
    var cleanupFailure: Throwable = null
    def settle(label: String)(action: => Unit): Unit =
      try action
      catch
        case cleanup: Throwable =>
          if cleanupFailure == null then cleanupFailure = cleanup else if cleanup ne cleanupFailure then cleanupFailure.addSuppressed(cleanup)
          if originalFailure != null && (cleanup ne originalFailure) then originalFailure.addSuppressed(cleanup)
          if closeFailure == null then closeFailure = cleanup else if cleanup ne closeFailure then closeFailure.addSuppressed(cleanup)
    if owner != null && owner.ownsInitializationLock() && !owner.initializationLockReleaseAttempted() then
      settle("managed lock release") { owner.releaseInitializationLock() }
    if runtimeVerified != null then settle("runtime verified session") { closeRuntimeConnection() }
    if runtimeVerified == null && runtimeOpen != null && (!runtimeOpen.hasRawHandle() || runtimeOpen.closeReturned()) then runtimeOpen = null
    settle("managed captured and serialized DATA owners") { cleanupCompletedDataOwners() }
    if journal != null then settle("managed installation journal") { closeJournal() }
    if owner != null then settle("managed installer connection") { owner.close(); owner = null }
    if budgets != null then
      val current = budgets
      settle("managed DATA and deployment assembly roots") { current.close(); budgets = null }
    settle("managed configuration secret owner") { configuration.close() }
    if cleanupFailure != null then
      if originalFailure != null then throw originalFailure
      throw cleanupFailure
    clearPlanState()
    disposed = true
    closeFailure = null
    if originalFailure != null then throw originalFailure

  def resourcesRetained: Boolean =
    (owner != null && owner.retainsConnection) || (runtimeOpen != null && runtimeOpen.hasRawHandle() && !runtimeOpen.closeReturned()) ||
      runtimeVerified != null || budgets != null || journal != null || catalogCapture != null ||
      securityCapture != null || semantic != null || semanticReparse != null || metadataProjection != null ||
      serializedMetadata != null || assembly != null || byteStreams.nonEmpty || pendingPayloadBytes.nonEmpty

  private def requireWorker(): Unit =
    if Thread.currentThread() ne worker then throw new IllegalStateException("managed installation requires its exact owned-I/O worker")

  private def requireSemanticPrepared(): Unit =
    if semantic == null || configurationDigest == null || recipeBytes == null || journal == null then
      throw new IllegalStateException("durable exact managed semantic DATA/C, recipe and journal are required")

  private def fieldMap(set: MySqlAuditResultSet, row: MySqlAuditRow): Map[String, MySqlAuditCell] =
    if set.columns.size != row.cells.size || set.columns.map(_.label).distinct.size != set.columns.size then
      throw new SQLDataException("managed JDBC catalog row framing is not exact")
    set.columns.map(_.label).zip(row.cells).toMap

  private def fieldText(set: MySqlAuditResultSet, row: MySqlAuditRow, label: String): String =
    fieldMap(set, row).get(label) match
      case Some(MySqlAuditText(value)) => value
      case _ => throw new SQLDataException("managed JDBC catalog field is missing or not non-NULL text: " + label)

  private def scalarLong(connection: Connection, sql: String, values: Seq[AnyRef]): Long =
    val query = connection.prepareStatement(sql)
    var primary: Throwable = null
    try
      query.setQueryTimeout(owner.queryTimeoutSeconds())
      values.zipWithIndex.foreach { case (value, i) => query.setObject(i + 1, value) }
      val rows = query.executeQuery()
      try
        if !rows.next() then throw new SQLDataException("managed bounded scalar query returned no row")
        val value = rows.getLong(1)
        if rows.wasNull() || rows.next() then throw new SQLDataException("managed bounded scalar query returned a null or ambiguous result")
        value
      finally rows.close()
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(query, primary)

  private def countSchemaRoutines(): Long =
    scalarLong(owner.connection(),
      "SELECT COUNT(*) FROM INFORMATION_SCHEMA.ROUTINES WHERE BINARY ROUTINE_SCHEMA=BINARY ?",
      Seq(configuration.database()))

  private def countSchemaEvents(): Long =
    scalarLong(owner.connection(),
      "SELECT COUNT(*) FROM INFORMATION_SCHEMA.EVENTS WHERE BINARY EVENT_SCHEMA=BINARY ?",
      Seq(configuration.database()))

  private def oneLong(connection: Connection, sql: String, timeout: Int): Long =
    val query = connection.prepareStatement(sql)
    var primary: Throwable = null
    try
      query.setQueryTimeout(timeout)
      val rows = query.executeQuery()
      try
        if !rows.next() then throw new SQLDataException("managed server-limit query returned no row")
        val value = rows.getLong(1)
        if rows.wasNull() || rows.next() then throw new SQLDataException("managed server-limit query returned a null or ambiguous value")
        value
      finally rows.close()
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(query, primary)

  private def oneText(connection: Connection, sql: String, timeout: Int): String =
    val query = connection.prepareStatement(sql)
    var primary: Throwable = null
    try
      query.setQueryTimeout(timeout)
      val rows = query.executeQuery()
      try
        if !rows.next() then throw new SQLDataException("managed server security variable query returned no row")
        val value = rows.getString(1)
        if value == null || rows.next() then throw new SQLDataException("managed server security variable query returned null or ambiguous text")
        value
      finally rows.close()
    catch
      case failure: Throwable =>
        primary = failure
        throw failure
    finally closePreserving(query, primary)

  private def closePreserving[A <: AutoCloseable](resource: A, primary: Throwable): Unit =
    if resource != null then
      try resource.close()
      catch
        case cleanup: Throwable =>
          if primary == null then throw cleanup
          else if cleanup ne primary then primary.addSuppressed(cleanup)

  private def uuidBytes(value: UUID): Array[Byte] = java.nio.ByteBuffer.allocate(16)
    .putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array()
  private def shaBytes(value: Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA-256").digest(value)
  private def shaHex(value: Array[Byte]): String = HexFormat.of().formatHex(shaBytes(value))

  private final class SqlEffectOutcomeUnknown(cause: SQLException)
      extends SQLException("managed JDBC SQL effect returned an uncertain server outcome", cause):
    def cleanupConfirmed: Boolean = getSuppressed.isEmpty && cause.getSuppressed.isEmpty

  private final case class RetainedPayload(var bytes: Array[Byte], charge: Long, context: String, var charged: Boolean)

  private final class BudgetedByteArrayOutputStream(
      exactRoot: MySqlFormat3ReadBudget,
      maximum: Int,
      context: String
  ) extends OutputStream:
    if exactRoot == null || maximum <= 0 || maximum > ManagedInstallationFiles.MAX_BUNDLE_BYTES then
      throw new IllegalArgumentException("bounded actual managed DATA stream required")
    private val retained = Math.addExact(maximum.toLong, 128L)
    private var buffer: Array[Byte] = null
    private var size = 0
    private var charged = 0L
    private var initialized = false
    private var closed = false
    def initialize(): Unit =
      if initialized || closed then throw new IllegalStateException("managed DATA output stream initialization is one-shot")
      exactRoot.reserveRetainedMemory(retained, context + " before fixed buffer allocation")
      charged = retained
      buffer = new Array[Byte](maximum)
      initialized = true
    override def write(value: Int): Unit =
      if !initialized || closed || buffer == null || size >= maximum then throw new IOException("managed DATA stream exceeds its fixed byte bound")
      buffer(size) = value.toByte
      size += 1
    override def write(value: Array[Byte], offset: Int, length: Int): Unit =
      if !initialized || closed || buffer == null || value == null || offset < 0 || length < 0 || offset > value.length - length ||
        size.toLong + length.toLong > maximum then
        throw new IOException("managed DATA stream exceeds its fixed byte bound or has an invalid range")
      System.arraycopy(value, offset, buffer, size, length)
      size += length
    def toByteArray: Array[Byte] =
      if !initialized || closed || buffer == null || size == 0 then throw new IllegalStateException("managed DATA stream is closed or empty")
      newPayload(size, context + " output copy")(destination => System.arraycopy(buffer, 0, destination, 0, size))
    def release(): Unit =
      if !closed then
        if buffer != null then
          Arrays.fill(buffer, 0.toByte)
          buffer = null
        if charged > 0L then
          exactRoot.releaseRetainedMemory(charged, context + " buffer reference cleared")
          charged = 0L
        closed = true
    override def close(): Unit = release()

