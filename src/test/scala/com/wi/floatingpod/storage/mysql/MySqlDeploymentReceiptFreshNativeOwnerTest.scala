package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.{WindowsB1SourceAnchorStore, WindowsWorldProvisioner, WindowsWorldProvisionReceiptDataCodec}
import java.nio.file.Path
import java.sql.SQLDataException
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.Assertions.*

/** 仅真实静态文件/recipe/预算及 native 前拒绝；任何成功 native 创建都不在本类执行。 */
class MySqlDeploymentReceiptFreshNativeOwnerTest:
  @TempDir var directory: Path = _
  private val Boot = new WindowsWorldProvisioner.BootIdentity(UUID.fromString("b14d7eaa-436b-48b4-a022-a644cc67d424"),1L)
  private def operation(n: Int): UUID = UUID.fromString(f"00000000-0000-0000-0000-$n%012x")
  private def plan(f: DeploymentReceiptStaticTestData.Fixture, changed: Map[String,String] = Map.empty): WindowsWorldProvisionReceiptDataCodec.PlanData =
    val fields = Map(
      "creation" -> operation(17).toString,"bootstrap" -> operation(18).toString,
      "prepare" -> operation(19).toString,"certify" -> operation(20).toString,
      "binding" -> DeploymentReceiptStaticTestData.binding.toString,"physical" -> "2",
      "server" -> DeploymentReceiptStaticTestData.server.toString,"schema" -> f.profile.schema,
      "format" -> "5","ddl" -> f.metadata.baseline().ddlSha256(),
      "provisioning" -> DeploymentReceiptStaticTestData.provisioning.toString) ++ changed
    def uuid(key: String): UUID = UUID.fromString(fields(key))
    new WindowsWorldProvisionReceiptDataCodec.PlanData(uuid("creation"),uuid("bootstrap"),uuid("prepare"),uuid("certify"),
      uuid("binding"),fields("physical").toInt,uuid("server"),fields("schema"),fields("format").toInt,fields("ddl"),uuid("provisioning"))
  private def controlled(f: DeploymentReceiptStaticTestData.Fixture, hash: String = null): MySqlFreshControlledMetadata =
    val actual = WindowsB1SourceAnchorStore.AnchorConfiguration.ofDeploymentReceiptMetadata(f.metadata).configurationSha256()
    MySqlFreshControlledMetadata(f.directory.resolve("runtime.properties").toAbsolutePath,if hash == null then actual else hash)
  private def graph(f: DeploymentReceiptStaticTestData.Fixture, r: MySqlFormat3ReadBudget,
      owner: MySqlRuntimeInputs.AssemblyOwner): MySqlRuntimeInputs.DeploymentReceiptStaticGraph =
    MySqlRuntimeInputs.assembleDeploymentReceiptStatic(f.metadata,owner,DeploymentReceiptStaticTestData.policy)

  @Test def allElevenRecipeFieldsBindActualPlanWithoutIssuingNativeProof(): Unit =
    val f = DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
    val recipe = MySqlDeploymentReceiptRuntimeContract.decodeRecipe(DeploymentReceiptStaticTestData.recipe(f.profile,f.metadata.baseline().ddlSha256()))
    recipe.requirePlanData(plan(f))
    val changes = Vector("creation"->operation(33).toString,"bootstrap"->operation(34).toString,
      "prepare"->operation(35).toString,"certify"->operation(36).toString,
      "binding"->"ad227945-26df-46ef-bca8-770aee78c1cb","server"->operation(38).toString,
      "schema"->"other_schema5","format"->"3","ddl"->("e"*64),"provisioning"->operation(39).toString)
    changes.foreach { (key,value) =>
      assertThrows(classOf[SQLDataException],()=>recipe.requirePlanData(plan(f,Map(key->value))),key)
    }
    // physicalFormat 的不合法值由真实 PlanData 构造器先拒绝，不伪造绕过构造器的对象。
    assertThrows(classOf[IllegalArgumentException],()=>plan(f,Map("physical"->"1")))
    assertThrows(classOf[SQLDataException],()=>recipe.requirePlanData(null))

  @Test def nativeLeaseRequiresActualGraphRootAndRemainsOwnedUntilDetach(): Unit =
    val f = DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
    val r = f.root(); val assembly = MySqlRuntimeInputs.beginV2(r,Thread.currentThread())
    val g = graph(f,r,assembly)
    val lease = g.borrowForNative(r)
    try
      assertSame(f.metadata,g.nativeMetadata(lease,r))
      g.requireNativePlan(lease,r,plan(f))
      assertThrows(classOf[IllegalStateException],()=>assembly.close())
      val foreign = f.root()
      try
        assertThrows(classOf[IllegalArgumentException],()=>g.borrowForNative(foreign))
        assertThrows(classOf[IllegalArgumentException],()=>g.nativeMetadata(lease,foreign))
      finally foreign.closeRootReadBudget()
    finally lease.close()
    assertThrows(classOf[IllegalStateException],()=>g.nativeMetadata(lease,r))
    assembly.close()
    assertThrows(classOf[IllegalStateException],()=>g.borrowForNative(r))
    assertFalse(assembly.hasUnsettledResources)
    r.closeRootReadBudget()

  @Test def relativePathFailsBeforeNativeAndOneShotPreservesOriginalCause(): Unit =
    val f = DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
    val r = f.root(); val assembly = MySqlRuntimeInputs.beginV2(r,Thread.currentThread())
    val g = graph(f,r,assembly); val seed = r.accountedMemoryBytes
    val owner = MySqlDeploymentReceiptFreshNativeOwner.begin(r,g,Path.of("relative-world"),plan(f),Boot,controlled(f))
    val failure = assertThrows(classOf[IllegalArgumentException],()=>owner.createFresh())
    assertSame(failure,owner.failure.get)
    assertFalse(owner.attemptedNativeConstruction); assertFalse(owner.attemptedSourceCapture)
    assertFalse(owner.attemptedCreation); assertFalse(owner.attemptedPublication)
    assertTrue(owner.retainedWorld.isEmpty); assertTrue(owner.retainedAnchor.isEmpty)
    assertSame(failure,assertThrows(classOf[IllegalStateException],()=>owner.createFresh()).getCause)
    owner.close(); assertFalse(owner.hasUnsettledResources)
    assertEquals(seed,r.accountedMemoryBytes)
    assembly.close(); r.closeRootReadBudget()

  @Test def recipeMismatchRetainsRealBorrowUntilCallerClosesPartialOwner(): Unit =
    val f = DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
    val r = f.root(); val assembly = MySqlRuntimeInputs.beginV2(r,Thread.currentThread())
    val g = graph(f,r,assembly); val seed = r.accountedMemoryBytes
    val owner = MySqlDeploymentReceiptFreshNativeOwner.begin(r,g,directory.resolve("world").toAbsolutePath,
      plan(f,Map("prepare"->operation(37).toString)),Boot,controlled(f))
    val failure = assertThrows(classOf[SQLDataException],()=>owner.createFresh())
    assertSame(failure,owner.failure.get); assertFalse(owner.attemptedNativeConstruction)
    assertThrows(classOf[IllegalStateException],()=>assembly.close())
    assertTrue(owner.hasUnsettledResources)
    owner.close(); assertEquals(seed,r.accountedMemoryBytes)
    assembly.close(); r.closeRootReadBudget()

  @Test def completeAHashMismatchDropsPaidConfigurationBeforeRefundWithoutNative(): Unit =
    val f = DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
    val r = f.root(); val assembly = MySqlRuntimeInputs.beginV2(r,Thread.currentThread())
    val g = graph(f,r,assembly); val seed = r.accountedMemoryBytes
    val owner = MySqlDeploymentReceiptFreshNativeOwner.begin(r,g,directory.resolve("world").toAbsolutePath,
      plan(f),Boot,controlled(f,"0"*64))
    val failure = assertThrows(classOf[IllegalArgumentException],()=>owner.createFresh())
    assertSame(failure,owner.failure.get); assertFalse(owner.attemptedNativeConstruction)
    assertTrue(r.accountedMemoryBytes > seed+512L)
    assertThrows(classOf[IllegalStateException],()=>assembly.close())
    owner.close(); assertEquals(seed,r.accountedMemoryBytes)
    assertThrows(classOf[IllegalStateException],()=>owner.retainedWorld)
    assembly.close(); r.closeRootReadBudget()

  @Test def firstValidationCapacityFailureIsRecordedWithoutAttemptingNative(): Unit =
    val f = DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
    val r = f.root(); val assembly = MySqlRuntimeInputs.beginV2(r,Thread.currentThread())
    val g = graph(f,r,assembly); val seed = r.accountedMemoryBytes
    val owner = MySqlDeploymentReceiptFreshNativeOwner.begin(r,g,directory.resolve("world").toAbsolutePath,plan(f),Boot,controlled(f))
    val held = r.availableMemoryBytes-1L
    r.reserveTemporaryMemory(held,"test actual aggregate cap before native validation")
    val before = r.accountedMemoryBytes
    try
      val failure = assertThrows(classOf[SQLDataException],()=>owner.createFresh())
      assertSame(failure,owner.failure.get); assertFalse(owner.attemptedNativeConstruction)
      assertFalse(owner.attemptedSourceCapture); assertFalse(owner.attemptedCreation); assertFalse(owner.attemptedPublication)
      assertEquals(before,r.accountedMemoryBytes)
      assertSame(failure,assertThrows(classOf[IllegalStateException],()=>owner.createFresh()).getCause)
    finally r.releaseTemporaryMemory(held,"test aggregate cap released")
    owner.close(); assertEquals(seed,r.accountedMemoryBytes)
    assembly.close(); r.closeRootReadBudget()

  @Test def foreignThreadCannotEnterAndPrematureRootCloseRetainsCleanupFailure(): Unit =
    val f = DeploymentReceiptStaticTestData.fixture(directory.toAbsolutePath)
    val r = f.root(); val assembly = MySqlRuntimeInputs.beginV2(r,Thread.currentThread())
    val g = graph(f,r,assembly)
    val owner = MySqlDeploymentReceiptFreshNativeOwner.begin(r,g,directory.resolve("world").toAbsolutePath,
      plan(f,Map("prepare"->operation(37).toString)),Boot,controlled(f))
    val threadFailure = new AtomicReference[Throwable]()
    val worker = new Thread(() =>
      try owner.createFresh()
      catch case failure: Throwable => threadFailure.set(failure))
    worker.start(); worker.join()
    assertInstanceOf(classOf[IllegalStateException],threadFailure.get())
    assertTrue(owner.failure.isEmpty); assertFalse(owner.attemptedNativeConstruction)
    val primary = assertThrows(classOf[SQLDataException],()=>owner.createFresh())
    r.closeRootReadBudget()
    val cleanup = assertThrows(classOf[IllegalStateException],()=>owner.close())
    assertSame(primary,owner.failure.get); assertSame(cleanup,owner.failedCleanup.get)
    assertTrue(primary.getSuppressed.contains(cleanup)); assertTrue(owner.hasUnsettledResources)
    assertSame(cleanup,assertThrows(classOf[IllegalStateException],()=>owner.close()).getCause)
    assertFalse(owner.attemptedNativeConstruction)
    // 故意关闭原 root 的反例保留 caller/lease 未结状态；不伪造第二次 cleanup ACK。
