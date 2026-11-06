package com.wi.floatingpod.bootstrap

import com.wi.floatingpod.server.{DedicatedWorldContext, NormalRecallRecoverySourceCustody}
import com.wi.floatingpod.server.io.IoOperationTag
import com.wi.floatingpod.server.save.ManagedOrdinarySaveAdmission
import net.minecraft.server.MinecraftServer
import net.minecraft.world.WorldServer
import net.minecraft.world.chunk.Chunk
import net.minecraft.world.gen.ChunkProviderServer

/** 原Main的保存责任边界。实现必须保留真实source custody/判定，不以空owner代替尚未接入的恢复链。 */
private[bootstrap] trait ManagedBootSaveCallbacks extends DedicatedBootstrapBridge.Callbacks:
  def worldContext:DedicatedWorldContext
  def bootTag:IoOperationTag
  def verifyClosedBootSaveScopeIssuer(server:MinecraftServer):Unit
  def verifyClosedBootSaveScopeUse(server:MinecraftServer):Unit
  private[floatingpod] def retainedRecallSourceCustodyForClosedScope(server:MinecraftServer):Option[NormalRecallRecoverySourceCustody]
  def ordinarySaveAdmission():ManagedOrdinarySaveAdmission
  private[floatingpod] def decideManagedSourceProviderChunk(server:MinecraftServer,
      actualProvider:ChunkProviderServer,actualWorld:WorldServer,actualChunk:Chunk,
      operation:ManagedOrdinarySaveAdmission.ProviderOperation):ManagedOrdinarySaveAdmission.ProviderDecision
  def requireOrdinarySaveCurrent(server:MinecraftServer,dimension:Int,x:Int,z:Int,chunk:Chunk):Unit
