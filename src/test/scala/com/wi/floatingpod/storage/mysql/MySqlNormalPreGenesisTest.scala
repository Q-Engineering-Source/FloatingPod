package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.bootstrap.{ProductionBootstrapControl, ProductionDedicatedBootstrapCallbacks}
import com.wi.floatingpod.server.{WindowsB1SourceAnchorStore, WindowsWorldProvisioner}
import com.wi.floatingpod.config.MySqlRuntimeMetadata
import java.nio.charset.StandardCharsets
import com.wi.floatingpod.server.io.*
import java.nio.file.Path
import java.util.UUID
import java.io.IOException
import java.sql.SQLException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 只测试真实pure批准值匹配、责任观察和actual worker actor guard；不是Fresh/SQL/native成功测试。 */
class MySqlNormalPreGenesisTest:
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
  private def request() = MySqlNormalBootRequest(temporaryRoot
    .resolve("not-created-fresh-unit-" + UUID.randomUUID()), plan, new WindowsWorldProvisioner.BootIdentity(boot, 2L), controlled())
  private def validate(value: MySqlNormalBootRequest, expected: MySqlApprovedBaselineIdentity = baseline): Unit =
    MySqlNormalBootRules.validate(value, expected, MySqlExpectedNoImport, MySqlExpectedPhysicalFormat.Format2, boot, 2L, metadata())

  @Test def exactStaticPlanAndBootMatchWithoutGrantingNativeOrDatabaseQualification(): Unit =
    val value = request()
    validate(value)
    assertSame(plan, value.plan)
    assertEquals(plan.prepareWriteUuid(), value.plan.prepareWriteUuid())
    assertEquals(plan.certifyWriteUuid(), value.plan.certifyWriteUuid())
    assertEquals(plan.bootstrapOperationUuid(), value.plan.bootstrapOperationUuid())

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
    assertThrows(classOf[IllegalArgumentException], () => MySqlNormalBootRules.validate(value, baseline,
      MySqlExpectedNoImport, MySqlExpectedPhysicalFormat.Format1, boot, 2L, metadata()))
    assertThrows(classOf[IllegalArgumentException], () => MySqlNormalBootRules.validate(value, baseline,
      null, MySqlExpectedPhysicalFormat.Format2, boot, 2L, metadata()))
    assertThrows(classOf[IllegalArgumentException], () => validate(value,
      baseline.copy(schemaFormat = 1, coverage = MySqlAuditCoverage.V4)))

  @Test def relativeRootOrMissingPlanRequestFailsBeforeAnyResourceCanBeOpened(): Unit =
    assertThrows(classOf[IllegalArgumentException], () => { MySqlNormalBootRequest(Path.of("relative-world"), plan,
      new WindowsWorldProvisioner.BootIdentity(boot, 2L), controlled()); () })
    val absolute = request().requestedWorldPath
    assertThrows(classOf[IllegalArgumentException], () => { MySqlNormalBootRequest(absolute.getRoot, plan,
      new WindowsWorldProvisioner.BootIdentity(boot, 2L), controlled()); () })
    assertThrows(classOf[IllegalArgumentException], () => { MySqlNormalBootRequest(absolute, null,
      new WindowsWorldProvisioner.BootIdentity(boot, 2L), controlled()); () })

  @Test def independentConfigurationMismatchFailsBeforeNormalNativeConstruction(): Unit =
    val value = request().copy(controlledMetadata = MySqlFreshControlledMetadata(
      temporaryRoot.resolve("not-opened-wrong-control.properties"), "00".repeat(32)))
    assertThrows(classOf[IllegalArgumentException], () => validate(value))
    val holder = new MySqlNormalNativeHolder(Thread.currentThread(), value)
    assertThrows(classOf[IllegalArgumentException], () => holder.beginInspection(metadata()))
    val observed = holder.observation
    assertFalse(observed.constructionAttempted)
    assertFalse(observed.libraryConstructed)
    assertFalse(observed.retainedResponsibility)

  @Test def actualWrongThreadCannotOpenInspectionOrNativeLibrary(): Unit =
    val holder = new MySqlNormalNativeHolder(Thread.currentThread(), request())
    val error = new AtomicReference[Throwable]()
    val wrong = new Thread(() => {
      try holder.beginInspection(metadata())
      catch case cause: Throwable => error.set(cause)
    })
    wrong.start(); wrong.join(5000L)
    assertFalse(wrong.isAlive)
    assertTrue(error.get().isInstanceOf[IllegalArgumentException])
    assertFalse(holder.observation.constructionAttempted)

  @Test def normalUnknownRetainsOriginalCauseSuppressedAndPhaseAndRejectsRetry(): Unit =
    val lifecycle = new MySqlFreshPrepareLifecycle(Thread.currentThread())
    lifecycle.beginPlanning(); lifecycle.planningCleaned(); lifecycle.beginGenesis()
    val cause = new SQLException("controlled unit response loss", "08006", 9101)
    val primary = new IOException("controlled unit owner publication failure", cause)
    val cleanup = new IOException("controlled unit cleanup")
    val stack = primary.getStackTrace.toVector
    lifecycle.fail("normal-pre-genesis-native-install", primary)
    lifecycle.fail("normal-cleanup", cleanup)
    val prepare = lifecycle.observe(None, None)
    assertSame(primary, prepare.failure.get)
    assertSame(cause, primary.getCause)
    assertEquals(Vector(cleanup), primary.getSuppressed.toVector)
    assertEquals(stack, primary.getStackTrace.toVector)
    assertEquals(Some("normal-pre-genesis-native-install"), prepare.failureStage)
    assertTrue(prepare.genesisPublicationAttempted)
    assertFalse(prepare.executionAttempted)
    assertSame(primary, assertThrows(classOf[IOException], () => lifecycle.beginPlanning()))

  @Test def closedJdbcCannotClearNormalUnknownNativeOrPrepareResponsibility(): Unit =
    val requestValue = request()
    val cause = new IOException("controlled retained native boundary")
    val native = MySqlNormalNativeObservation(true, true, None, None, None, None, None,
      true, false, Some(cause))
    val responsibility = MySqlNormalBootstrapResponsibility(requestValue, native, None, None, true)
    val close = MySqlOwnedSessionCloseObservation(boot, 2L, 1L, true, false,
      MySqlOwnedSessionRelease.NoAcquireAttempt, false, false, true, Some(true), None, Some(cause),
      normalResponsibility = Some(responsibility))
    assertTrue(close.unresolvedOwnedResponsibility)
    assertSame(cause, close.normalResponsibility.get.native.failure.get)
    val unopened = native.copy(constructionAttempted=false,libraryConstructed=false,
      closeAttempted=false,closeReturned=false,failure=None)
    assertFalse(close.copy(normalResponsibility=Some(responsibility.copy(native=unopened))).unresolvedOwnedResponsibility)
