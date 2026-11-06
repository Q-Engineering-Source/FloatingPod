package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.bootstrap.{ProductionBootstrapControl, ProductionDedicatedBootstrapCallbacks}
import com.wi.floatingpod.server.{WindowsB1SourceAnchorStore, WindowsWorldProvisioner}
import com.wi.floatingpod.config.MySqlRuntimeMetadata
import java.nio.charset.StandardCharsets
import com.wi.floatingpod.server.io.*
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 只测试真实pure批准值匹配、责任观察和actual worker actor guard；不是Fresh/SQL/native成功测试。 */
class MySqlFreshBootRulesTest:
  private val server = UUID.randomUUID()
  private val provision = UUID.randomUUID()
  private val binding = UUID.randomUUID()
  private val boot = UUID.randomUUID()
  private val baseline = MySqlApprovedBaselineIdentity(MySqlApprovedBaselineLoader.ApprovedFormat, 3,
    MySqlAuditCoverage.Format3, server, MySqlConnectionSource.SupportedServerVersion, "fresh_unit",
    provision, binding, "ab".repeat(32), "audit@host", "cd".repeat(32))
  private val plan = new WindowsWorldProvisioner.Plan(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
    UUID.randomUUID(), binding, 2, server, "fresh_unit", 3, "ab".repeat(32), provision)
  private val temporaryRoot = Path.of(System.getProperty("java.io.tmpdir")).toAbsolutePath()
  private def metadata(): MySqlRuntimeMetadata =
    val prefix = temporaryRoot.toString.replace('\\', '/')
    val content = s"""backend=mysql
host=database.example.invalid
port=3307
schema=fresh_unit
expectedServerUuid=$server
expectedBindingUuid=$binding
expectedEffectiveUser=fixture@localhost
username=fixture
pkcs12TrustStorePath=$prefix/never-opened-trust.p12
credentialSourceRef=file:$prefix/never-opened-credential.txt
trustSecretSourceRef=file:$prefix/never-opened-trust-secret.txt
connectTimeoutMillis=3000
socketTimeoutMillis=10000
activeLimit=17
innodbFlushLogAtTrxCommit=1
syncBinlog=1
approvedBaselinePath=$prefix/never-opened-approved.json
approvedBaselineSha256=${"ab".repeat(32)}
approvedBaselineFormat=${baseline.format}
approvedSchemaFormat=3
approvedCoverage=${MySqlAuditCoverage.Format3}
expectedServerVersion=${baseline.serverVersion}
expectedProvisioningUuid=$provision
approvedDdlSha256=${baseline.ddlSha256}
expectedAuditDefiner=${baseline.auditDefiner}
approvedSourceCaptureSha256=${baseline.sourceCaptureSha256}
format3RuntimeContractPath=$prefix/never-opened-format3-contract.json
format3RuntimeContractSha256=${"ef".repeat(32)}
"""
    MySqlRuntimeMetadata.fromBytes(content.getBytes(StandardCharsets.UTF_8))
  private def controlled(m: MySqlRuntimeMetadata = metadata()) = MySqlFreshControlledMetadata(
    temporaryRoot.resolve("never-opened-runtime.properties"),
    WindowsB1SourceAnchorStore.AnchorConfiguration.ofMetadata(m).configurationSha256())
  private def request() = MySqlFreshBootRequest(temporaryRoot
    .resolve("not-created-fresh-unit-" + UUID.randomUUID()), plan, new WindowsWorldProvisioner.BootIdentity(boot, 2L), controlled())
  private def validate(value: MySqlFreshBootRequest, expected: MySqlApprovedBaselineIdentity = baseline): Unit =
    MySqlFreshBootRules.validate(value, expected, MySqlExpectedNoImport, MySqlExpectedPhysicalFormat.Format2, boot, 2L, metadata())

  @Test def exactStaticPlanAndBootMatchWithoutGrantingNativeOrDatabaseQualification(): Unit =
    val value = request()
    validate(value)
    assertSame(plan, value.plan)
    assertEquals(plan.prepareWriteUuid(), value.plan.prepareWriteUuid())
    assertEquals(plan.certifyWriteUuid(), value.plan.certifyWriteUuid())
    assertEquals(plan.bootstrapOperationUuid(), value.plan.bootstrapOperationUuid())

  @Test def productionFreshPlanUsesSchemaFormat3AndPhysicalFormat2AndPassesFreshRules(): Unit =
    val value = metadata()
    val productionIdentity = ProductionBootstrapControl.FreshIdentity(boot, 2L, 1L, UUID.randomUUID(),
      UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
    val productionPlan = ProductionDedicatedBootstrapCallbacks.freshPlan(value, productionIdentity)
    assertEquals(3, productionPlan.schemaFormat())
    assertEquals(2, productionPlan.physicalFormat())
    assertEquals(server, productionPlan.mysqlServerUuid())
    assertEquals("fresh_unit", productionPlan.schema())
    assertEquals(provision, productionPlan.provisioningUuid())
    assertEquals(binding, productionPlan.bindingUuid())
    assertEquals(productionIdentity.prepare(), productionPlan.prepareWriteUuid())
    assertEquals(productionIdentity.certify(), productionPlan.certifyWriteUuid())
    val request = MySqlFreshBootRequest(temporaryRoot.resolve("production-fresh-plan-world"), productionPlan,
      new WindowsWorldProvisioner.BootIdentity(boot, 2L), controlled(value))
    MySqlFreshBootRules.validate(request, baseline, MySqlExpectedNoImport,
      MySqlExpectedPhysicalFormat.Format2, boot, 2L, value)
  @Test def independentApprovedTargetMismatchesAreRejectedWithoutRepairingCallerUuid(): Unit =
    val value = request()
    val mismatches = Vector(baseline.copy(serverUuid = UUID.randomUUID()), baseline.copy(schema = "other_unit"),
      baseline.copy(provisioningUuid = UUID.randomUUID()), baseline.copy(bindingUuid = UUID.randomUUID()),
      baseline.copy(ddlSha256 = "ef".repeat(32)))
    mismatches.foreach(expected => assertThrows(classOf[IllegalArgumentException], () => validate(value, expected)))
    validate(value)
    assertSame(plan, value.plan)

  @Test def foreignBootGenerationAndUnsupportedPhysicalImportProfileAreRejected(): Unit =
    val value = request()
    assertThrows(classOf[IllegalArgumentException], () => validate(value.copy(boot = new WindowsWorldProvisioner.BootIdentity(UUID.randomUUID(), 2L))))
    assertThrows(classOf[IllegalArgumentException], () => validate(value.copy(boot = new WindowsWorldProvisioner.BootIdentity(boot, 3L))))
    assertThrows(classOf[IllegalArgumentException], () => MySqlFreshBootRules.validate(value, baseline,
      MySqlExpectedNoImport, MySqlExpectedPhysicalFormat.Format1, boot, 2L, metadata()))
    assertThrows(classOf[IllegalArgumentException], () => MySqlFreshBootRules.validate(value, baseline,
      null, MySqlExpectedPhysicalFormat.Format2, boot, 2L, metadata()))
    assertThrows(classOf[IllegalArgumentException], () => validate(value,
      baseline.copy(schemaFormat = 1, coverage = MySqlAuditCoverage.V4)))

  @Test def relativeRootOrMissingPlanRequestFailsBeforeAnyResourceCanBeOpened(): Unit =
    assertThrows(classOf[IllegalArgumentException], () => MySqlFreshBootRequest(Path.of("relative-world"), plan,
      new WindowsWorldProvisioner.BootIdentity(boot, 2L), controlled()))
    val absolute = request().requestedWorldPath
    assertThrows(classOf[IllegalArgumentException], () => MySqlFreshBootRequest(absolute.getRoot, plan,
      new WindowsWorldProvisioner.BootIdentity(boot, 2L), controlled()))
    assertThrows(classOf[IllegalArgumentException], () => MySqlFreshBootRequest(absolute, null,
      new WindowsWorldProvisioner.BootIdentity(boot, 2L), controlled()))

  @Test def jdbcCloseObservationCannotDischargeProcessLifetimeOrFailedNativeResponsibility(): Unit =
    val value = request()
    val retained = MySqlFreshNativeObservation(MySqlFreshNativeRetention.ProcessLifetimeCustodyRetained,
      true, true, false, false, None, None)
    val record = MySqlOwnedSessionCloseObservation(boot, 2, 1, true, true,
      MySqlOwnedSessionRelease.Released, true, true, true, Some(true), None, None,
      Some(MySqlFreshBootstrapResponsibility(value, None, None, None, retained, true)))
    assertTrue(record.unresolvedOwnedResponsibility)
    assertTrue(retained.retainedResponsibility)
    val cause = new IllegalStateException("actual observed native cleanup cause fixture")
    val failed = retained.copy(retention = MySqlFreshNativeRetention.NativeCloseFailed(cause), closeAttempted = true)
    assertTrue(failed.retainedResponsibility)
    assertTrue(record.copy(freshResponsibility = record.freshResponsibility.map(_.copy(native = failed))).unresolvedOwnedResponsibility)
    val notOpened = retained.copy(retention = MySqlFreshNativeRetention.NotOpened,
      libraryConstructionAttempted = false, libraryConstructed = false)
    assertFalse(notOpened.retainedResponsibility)
    assertFalse(record.copy(freshResponsibility = record.freshResponsibility.map(_.copy(native = notOpened))).unresolvedOwnedResponsibility)
    val arenaReturnedAfterCreateAttempt = retained.copy(retention = MySqlFreshNativeRetention.NativeCloseReturned,
      closeAttempted = true, closeReturned = true, creationAttempted = true)
    assertTrue(arenaReturnedAfterCreateAttempt.retainedResponsibility,
      "closing native arena cannot erase possible directory/receipt side effects")

  @Test def nativeHolderIsAllocatedOnRealIoWorkerAndWrongActorFailsBeforeNativeConstruction(): Unit =
    val holder = new AtomicReference[MySqlFreshNativeHolder]()
    val worker = new AtomicReference[Thread]()
    val value = request()
    val io = new OwnedSerialIo[Int, MySqlFreshNativeObservation](1, Thread.currentThread(), "fresh-owner-guard-unit")(_ => {
      worker.set(Thread.currentThread())
      val owned = new MySqlFreshNativeHolder(Thread.currentThread(), value)
      holder.set(owned)
      owned.observation
    })
    val operation = IoOperationTag(boot, 2, 1, UUID.randomUUID())
    var primary: Throwable = null
    def exitOff(): Unit =
      val result = new AtomicReference[java.lang.Boolean]()
      val failure = new AtomicReference[Throwable]()
      val tail = new Thread(() => {
        try result.set(io.awaitExitOffMain(5, TimeUnit.SECONDS))
        catch case c: Throwable => failure.set(c)
        ()
      })
      var primary: Throwable = null
      try
        tail.start()
        tail.join(5000)
        assertFalse(tail.isAlive, "off-main tail timeout")
        if failure.get() != null then throw failure.get()
        assertEquals(java.lang.Boolean.TRUE, result.get())
      catch case c: Throwable => primary = c
      finally
        try
          tail.join(5000)
          assertFalse(tail.isAlive, "tail leaked")
        catch case c: Throwable =>
          if primary == null then primary = c else MySqlOwnedSessionLifecycle.suppress(primary, c)
      if primary != null then throw primary
    try
      io.submit(operation, 1)
      io.quiesce()
      exitOff()
      val completion = io.drain(1).head
      assertSame(operation, completion.tag)
      assertEquals(MySqlFreshNativeRetention.NotOpened, completion.result.get.retention)
      assertFalse(completion.result.get.libraryConstructionAttempted)
      assertThrows(classOf[IllegalArgumentException], () => holder.get().createFresh(metadata()))
      assertThrows(classOf[IllegalArgumentException], () => holder.get().settleForExit())
      assertThrows(classOf[IllegalArgumentException], () => holder.get().observation)
    catch case cause: Throwable => primary = cause
    finally
      try
        io.quiesce()
        exitOff()
        if worker.get() != null then
          worker.get().join(5000)
          assertFalse(worker.get().isAlive)
        assertTrue(io.checkpoint.stopped)
      catch
        case cleanup: Throwable =>
          if primary == null then primary = cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    if primary != null then throw primary

  @Test def mandatoryControlledMetadataRejectsMissingRelativeRootInsideWorldAndMalformedHash(): Unit =
    val value = request()
    assertThrows(classOf[IllegalArgumentException], () => MySqlFreshControlledMetadata(null, "ab".repeat(32)))
    assertThrows(classOf[IllegalArgumentException], () => MySqlFreshControlledMetadata(Path.of("relative.properties"), "ab".repeat(32)))
    assertThrows(classOf[IllegalArgumentException], () => MySqlFreshControlledMetadata(temporaryRoot.getRoot, "ab".repeat(32)))
    assertThrows(classOf[IllegalArgumentException], () => MySqlFreshControlledMetadata(temporaryRoot.resolve("metadata.properties"), "UNSET"))
    assertThrows(classOf[IllegalArgumentException], () => value.copy(controlledMetadata = null))
    assertThrows(classOf[IllegalArgumentException], () => value.copy(controlledMetadata =
      controlled().copy(metadataPath = value.requestedWorldPath.resolve("data/runtime.properties"))))
    assertEquals("MySqlFreshControlledMetadata[redacted; not ACL authority]", value.controlledMetadata.toString)

  @Test def configurationApprovalMismatchAndMissingOriginalMetadataFailBeforeNativeConstruction(): Unit =
    val value = request()
    assertThrows(classOf[IllegalArgumentException], () => validate(value.copy(controlledMetadata =
      value.controlledMetadata.copy(approvedConfigurationSha256 = "00".repeat(32)))))
    assertThrows(classOf[IllegalArgumentException], () => MySqlFreshBootRules.validate(value, baseline,
      MySqlExpectedNoImport, MySqlExpectedPhysicalFormat.Format2, boot, 2L, null))
    val owned = new MySqlFreshNativeHolder(Thread.currentThread(), value)
    assertThrows(classOf[IllegalArgumentException], () => owned.createFresh(null))
    assertFalse(owned.observation.libraryConstructionAttempted)
    val mismatch = new MySqlFreshNativeHolder(Thread.currentThread(), value.copy(controlledMetadata =
      value.controlledMetadata.copy(approvedConfigurationSha256 = "00".repeat(32))))
    assertThrows(classOf[IllegalArgumentException], () => mismatch.createFresh(metadata()))
    assertFalse(mismatch.observation.libraryConstructionAttempted)
    assertFalse(mismatch.observation.sourceCaptureAttempted)

  private def certifyObservation(phase: MySqlFreshCertifyPhase, failure: Option[Throwable] = None) =
    MySqlFreshCertifyObservation(phase, false, false, false, false, false, false, None, failure, None, None, None, Vector.empty)

  @Test def phaseFactCannotTurnCertifiedConfirmedOrFailureIntoAdmittedFact(): Unit =
    Vector(MySqlFreshCertifyPhase.New, MySqlFreshCertifyPhase.CertifiedConfirmed, MySqlFreshCertifyPhase.Frozen).foreach { phase =>
      assertThrows(classOf[IllegalArgumentException], () => MySqlFreshBootRules.requireAdmittedPhase(certifyObservation(phase)))
    }
    val failed = new IllegalStateException("actual retained failure fixture")
    assertThrows(classOf[IllegalArgumentException], () => MySqlFreshBootRules.requireAdmittedPhase(
      certifyObservation(MySqlFreshCertifyPhase.Admitted, Some(failed))))
    // 纯phase predicate仍不发行SQL/native/admission；实际session另核same coordinator admission。
    MySqlFreshBootRules.requireAdmittedPhase(certifyObservation(MySqlFreshCertifyPhase.Admitted))

  @Test def completeCertifyCommitButNativeFailureAndSourcePublicationRemainRetainedAfterJdbcClose(): Unit =
    val value = request()
    val cause = new IllegalStateException("controlled native confirmation failure observation")
    val certify = certifyObservation(MySqlFreshCertifyPhase.Frozen, Some(cause)).copy(
      executionAttempted = true, sqlCommitAcknowledged = true, fullReadbackConfirmed = true,
      failureStage = Some("native-confirmation"))
    val native = MySqlFreshNativeObservation(MySqlFreshNativeRetention.NativeCloseReturned,
      true, true, true, true, None, None, sourceCaptureAttempted = true, sourcePublicationAttempted = true)
    val responsibility = MySqlFreshBootstrapResponsibility(value, None, Some(certify), None, native, true)
    assertSame(cause, responsibility.certify.get.failure.get)
    assertTrue(responsibility.certify.get.sqlCommitAcknowledged)
    assertFalse(responsibility.certify.get.nativeConfirmationAcknowledged)
    assertTrue(responsibility.retainedResponsibility)
    val closed = MySqlOwnedSessionCloseObservation(boot, 2, 1, true, true,
      MySqlOwnedSessionRelease.Released, true, true, true, Some(true), None, None, Some(responsibility))
    assertTrue(closed.unresolvedOwnedResponsibility)
    assertTrue(responsibility.copy(native = native.copy(sourcePublicationAttempted = false)).retainedResponsibility,
      "full certify observation alone retains SQL/native obligation")
