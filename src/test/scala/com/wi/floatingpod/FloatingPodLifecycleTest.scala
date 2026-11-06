package com.wi.floatingpod

import com.wi.floatingpod.network.{ServerRuntime, ServerRuntimeFactory, ServerRuntimeLifecycle, ServerRuntimeSettings}

import net.minecraftforge.fml.common.event.{FMLServerAboutToStartEvent, FMLServerStartingEvent, FMLServerStoppedEvent, FMLServerStoppingEvent}

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import java.nio.file.Paths
import scala.collection.mutable.ListBuffer

/** FML 入口只在可否决启动阶段传播异常，停服阶段必须保留清理机会。 */
class FloatingPodLifecycleTest:
  @Test
  def aboutAndStartingPropagateWhileNormalFourPhaseOrderReachesRuntime(): Unit =
    val events = ListBuffer.empty[String]
    val lifecycle = lifecycleFor(new EventRuntime(events))
    val mod = new FloatingPod(lifecycle)

    mod.onServerAboutToStart(null)
    mod.onServerStarting(null)
    mod.onServerStopping(null)
    mod.onServerStopped(null)

    assertEquals(List("about", "starting", "stopping", "stopped"), events.toList)

  @Test
  def aboutAndStartingFailuresPropagateButStoppingAndStoppedFailuresAreContained(): Unit =
    val aboutFailure = new IllegalStateException("about")
    val aboutMod = new FloatingPod(lifecycleFor(new EventRuntime(ListBuffer.empty, Some(aboutFailure), None, None, None)))
    assertThrows(classOf[IllegalStateException], () => aboutMod.onServerAboutToStart(null))

    val startingFailure = new IllegalStateException("starting")
    val startingMod = new FloatingPod(lifecycleFor(new EventRuntime(ListBuffer.empty, None, Some(startingFailure), None, None)))
    startingMod.onServerAboutToStart(null)
    assertThrows(classOf[IllegalStateException], () => startingMod.onServerStarting(null))

    val stoppingFailure = new IllegalStateException("stopping")
    val stoppedFailure = new IllegalStateException("stopped")
    val stoppingMod = new FloatingPod(lifecycleFor(new EventRuntime(ListBuffer.empty, None, None, Some(stoppingFailure), Some(stoppedFailure))))
    stoppingMod.onServerAboutToStart(null)
    stoppingMod.onServerStarting(null)
    stoppingMod.onServerStopping(null)
    stoppingMod.onServerStopped(null)

  private def lifecycleFor(runtime: ServerRuntime): ServerRuntimeLifecycle =
    val lifecycle = ServerRuntimeLifecycle.legacyForTests(new ServerRuntimeFactory:
      override def create(settings: ServerRuntimeSettings): ServerRuntime = runtime
    )
    lifecycle.configure(ServerRuntimeSettings.sqlite(Paths.get("C:/tmp/floatingpod-fml-handler.db"), 1))
    lifecycle

  private final class EventRuntime(
      events: ListBuffer[String],
      aboutFailure: Option[Throwable] = None,
      startingFailure: Option[Throwable] = None,
      stoppingFailure: Option[Throwable] = None,
      stoppedFailure: Option[Throwable] = None
  ) extends ServerRuntime:
    override def onServerAboutToStart(event: FMLServerAboutToStartEvent): Unit =
      events += "about"
      aboutFailure.foreach(throw _)
    override def onServerStarting(event: FMLServerStartingEvent): Unit =
      events += "starting"
      startingFailure.foreach(throw _)
    override def onServerStopping(event: FMLServerStoppingEvent): Unit =
      events += "stopping"
      stoppingFailure.foreach(throw _)
    override def onServerStopped(event: FMLServerStoppedEvent): Unit =
      events += "stopped"
      stoppedFailure.foreach(throw _)
