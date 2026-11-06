package com.wi.floatingpod.storage.mysql

import com.wi.floatingpod.server.source.{SourceForestCodec, SourceForestMembershipCodec, SourceForestSharedBudgetFixtures}
import java.sql.SQLDataException
import java.util.UUID
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

final class MySqlDbFirstForestReadTest:
  private val profile=SourceForestSharedBudgetFixtures.profile()
  private val binding=new UUID(0L,1L)
  private def id(n: Long)=new UUID(0L,n)
  private def binary(bytes: Array[Byte]): MySqlFormat3Cell=MySqlFormat3Cell.Binary(MySqlFormat3Binary.takeOwned(bytes))
  private def uuid(value: UUID): MySqlFormat3Cell=binary(java.nio.ByteBuffer.allocate(16)
    .putLong(value.getMostSignificantBits).putLong(value.getLeastSignificantBits).array())
  private def number(n: Long): MySqlFormat3Cell=MySqlFormat3Cell.SignedInteger(n)
  private def inputs: MySqlScriptSchemaInputs =
    val defaults=DeploymentReceiptStaticTestData.defaultProfile()
    val publication=MySqlScriptSchemaDataTestFixture.Inputs.budget.copy(rows=profile.maxMembers(),
      retainedBytes=profile.maxRetainedBytes(),packetBytes=profile.maxSqlPacketBytes())
    val configured=MySqlDeploymentReceiptSchemaProfile.inputs(defaults.schema,defaults.accounts,profile,publication,
      defaults.deploymentBudget,defaults.quotaMaximumRecords,Vector.empty)
    MySqlScriptSchemaInputs.createDbFirstFull27(MySqlDbFirstSchemaProfile.render(configured))
  private def row(sequence: Long, other: Boolean=false): Vector[MySqlFormat3Cell] =
    val source=SourceForestSharedBudgetFixtures.forest(1L,other,false,false)
    val old=source.identity()
    val identity=new SourceForestCodec.Identity(binding,id(1000L+sequence),id(2000L+sequence),old.sourceEpoch(),
      id(3000L+sequence),old.worldIdentity(),old.saveIdentity(),old.loaderProfile(),old.catalogGeneration(),old.readGeneration())
    val forest=new SourceForestCodec.Forest(identity,source.coverage(),source.sourceObservation(),source.rawRoots(),
      source.fixedRoots(),source.actualRoots(),source.carriers(),source.members())
    val encoded=SourceForestCodec.encode(forest,profile)
    val index=SourceForestMembershipCodec.derive(forest,profile)
    Vector(uuid(binding),uuid(identity.unitId()),uuid(identity.forestId()),number(identity.sourceEpoch()),
      MySqlFormat3Cell.Text("SEALED"),MySqlFormat3Cell.Text(SourceForestCodec.DOMAIN),binary(encoded.bytes().copy()),
      binary(encoded.sha256().copy()),number(encoded.memberCount()),number(encoded.rootCount()),
      number(encoded.carrierCount()),number(encoded.retainedBytes()),uuid(identity.createdOperationId()),
      binary(index.bytes().copy()),binary(index.sha256().copy()))
  private def snapshot(i: MySqlScriptSchemaInputs,root: MySqlFormat3ReadBudget,
      forests: Vector[Vector[MySqlFormat3Cell]]): MySqlScriptSchemaSnapshot =
    val old=MySqlScriptSchemaDataTestFixture.full26GenesisRelations(MySqlScriptSchemaInputs.createDeploymentReceiptFull26(
      i.schema,MySqlScriptSchemaDataTestFixture.Full26DdlSha256,profile,i.budget,i.deploymentBudget.get),false)
      .map(r=>r.table->r).toMap
    def put(row: Vector[MySqlFormat3Cell],table: String,name: String,cell: MySqlFormat3Cell)=
      row.updated(i.forTable(table).columns.indexWhere(_.name == name),cell)
    var manifest=old("fp_schema_manifest").rows.head
    Vector("format_version"->number(6L),"ddl_sha256"->binary(java.util.HexFormat.of().parseHex(i.ddlSha256)),
      "expected_binding_uuid"->uuid(binding),"limit_profile_bytes"->binary(profile.canonicalBytes()),
      "limit_profile_sha256"->binary(profile.sha256()),"max_region_bytes"->number(profile.maxRegionBytes()),
      "max_chunk_bytes"->number(profile.maxChunkBytes()),"max_decompressed_bytes"->number(profile.maxDecompressedBytes()),
      "max_nbt_depth"->number(profile.maxNbtDepth()),"max_members"->number(profile.maxMembers()),
      "max_retained_bytes"->number(profile.maxRetainedBytes()),"max_sql_packet_bytes"->number(profile.maxSqlPacketBytes()),
      "max_native_artifact_bytes"->number(profile.maxNativeArtifactBytes())).foreach { (name,cell) =>
      manifest=put(manifest,"fp_schema_manifest",name,cell)
    }
    val relationRows=Map("fp_schema_manifest"->Vector(manifest),"physical_recovery_binding"->Vector(
      put(old("physical_recovery_binding").rows.head,"physical_recovery_binding","binding_uuid",uuid(binding))),
      MySqlDeploymentReceiptSchemaDefinition.QuotaTable->old(MySqlDeploymentReceiptSchemaDefinition.QuotaTable).rows,
      "physical_source_forest"->forests)
    val relations=i.canonicalOrder.map(shape=>MySqlFormat3RelationSnapshot(shape.table,relationRows.getOrElse(shape.table,Vector.empty)))
    val connection=MySqlScriptSchemaDataTestFixture.Connection.copy(catalog=i.schema)
    MySqlScriptSchemaSnapshot.captureMaterialized(i.schema,connection,MySqlScriptSchemaDataTestFixture.Scope,
      profile.maxSqlPacketBytes(),i,relations,root)
  private def withRoot(body: (MySqlScriptSchemaInputs,MySqlFormat3ReadBudget) => Unit): Unit =
    val i=inputs
    val root=i.newAllocationRoot(profile.maxSqlPacketBytes(),new MySqlFormat3AllocationLedger(profile.maxRetainedBytes()))
    var primary: Throwable=null
    try body(i,root)
    catch case cause: Throwable =>
      primary=cause
      throw cause
    finally
      try root.closeRootReadBudget()
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)
  private def observe(image: MySqlScriptSchemaSnapshot,i: MySqlScriptSchemaInputs,root: MySqlFormat3ReadBudget):
      MySqlDbFirstForestRead.Summary =
    root.reserveRetainedMemory(MySqlDbFirstForestRead.HolderBytes,"forest test holder BEFORE allocation")
    val holder=new MySqlDbFirstForestRead(root,i,binding)
    var primary: Throwable=null
    try
      holder.validate(image)
      holder.result
    catch case cause: Throwable =>
      primary=cause
      throw cause
    finally
      try holder.close()
      catch case cleanup: Throwable =>
        if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary,cleanup)

  @Test def completeCurrentImageConsumesAllForestsWithoutTargetOrTransferFilter(): Unit = withRoot { (i,root) =>
    val image=snapshot(i,root,Vector(row(1L),row(2L,other=true)))
    val baseline=root.accountedMemoryBytes
    val summary=observe(image,i,root)
    assertEquals(2L,summary.verifiedForests)
    assertEquals(3L,summary.verifiedMembershipAssociations)
    assertEquals(baseline,root.accountedMemoryBytes)
  }
  @Test def freshPodChecksAllVerifiedStagesAndRetainsFailedObservationUntilClose(): Unit = withRoot { (i,root) =>
    val image=snapshot(i,root,Vector(row(1L,other=true),row(2L)))
    // 真NBT fixture的RAW/FIXED/ACTUAL身份分别为1/2/3；90仅为pig，不是Pod。
    Vector(1L,2L,3L,90L).foreach { proposed =>
      val baseline=root.accountedMemoryBytes
      root.reserveRetainedMemory(MySqlDbFirstForestRead.HolderBytes,"fresh forest holder BEFORE allocation")
      val holder=new MySqlDbFirstForestRead(root,i,binding)
      try
        if proposed != 90L then
          assertThrows(classOf[SQLDataException],()=>holder.validateFreshPod(image,id(proposed)))
          assertThrows(classOf[IllegalStateException],()=>holder.result)
          assertThrows(classOf[IllegalArgumentException],()=>holder.validateFreshPod(image,id(9999L)))
        else
          holder.validateFreshPod(image,id(proposed))
          assertEquals(2L,holder.result.verifiedForests)
          assertEquals(3L,holder.result.verifiedMembershipAssociations)
        assertEquals(baseline+MySqlDbFirstForestRead.HolderBytes,root.accountedMemoryBytes)
      finally holder.close()
      assertEquals(baseline,root.accountedMemoryBytes)
    }
  }
  @Test def unrelatedCorruptionNullIndexAndSqlIdentityMismatchFailAndRefund(): Unit = withRoot { (i,root) =>
    val original=row(2L,other=true)
    val candidates=Vector(original.updated(7,binary(new Array[Byte](32))),
      original.updated(14,binary(new Array[Byte](32))),original.updated(3,number(99L)),
      original.updated(1,uuid(id(9998L))),original.updated(2,uuid(id(9997L))),
      original.updated(8,number(99L)),original.updated(12,uuid(id(9999L))))
    candidates.foreach { damaged =>
      val image=snapshot(i,root,Vector(row(1L),damaged))
      val baseline=root.accountedMemoryBytes
      assertThrows(classOf[IllegalArgumentException],()=>observe(image,i,root))
      assertEquals(baseline,root.accountedMemoryBytes)
    }
    val absent=original.updated(4,MySqlFormat3Cell.Text("DRAFT"))
      .updated(13,MySqlFormat3Cell.SqlNull).updated(14,MySqlFormat3Cell.SqlNull)
    val image=snapshot(i,root,Vector(row(1L),absent))
    val baseline=root.accountedMemoryBytes
    assertThrows(classOf[SQLDataException],()=>observe(image,i,root))
    assertEquals(baseline,root.accountedMemoryBytes)
  }
  @Test def foreignRootAndClosedHolderCannotConsumeCurrentImage(): Unit = withRoot { (i,root) =>
    val image=snapshot(i,root,Vector(row(1L)))
    val foreign=i.newAllocationRoot(profile.maxSqlPacketBytes(),new MySqlFormat3AllocationLedger(profile.maxRetainedBytes()))
    try
      val baseline=foreign.accountedMemoryBytes
      assertThrows(classOf[SQLDataException],()=>observe(image,i,foreign))
      assertEquals(baseline,foreign.accountedMemoryBytes)
    finally foreign.closeRootReadBudget()
    val baseline=root.accountedMemoryBytes
    root.reserveRetainedMemory(MySqlDbFirstForestRead.HolderBytes,"closed forest test holder BEFORE allocation")
    val holder=new MySqlDbFirstForestRead(root,i,binding)
    holder.close()
    assertEquals(baseline,root.accountedMemoryBytes)
    assertThrows(classOf[IllegalArgumentException],()=>holder.validate(image))
  }
