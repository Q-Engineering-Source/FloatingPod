package com.wi.floatingpod.storage.mysql

import java.sql.SQLDataException
import java.util.concurrent.atomic.AtomicReference
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

/** 独立census DATA gate；不修改或替代原82selectors/business assertions。 */
final class MySqlModeEnvelopeV2CensusTest:
  @Test def completeMixedGraphMeasuresOriginalProfileRatherThanMinimalModeFixture(): Unit =
    val profile = com.wi.floatingpod.config.SourceLimitProfile.of(1000000L,1000000L,2000000L,64L,100000L,
      8000000L,4000000L,2000000L)
    val root = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    try
      // 已冻结的真实 Debug prepare/activation 后 owned DATA baseline；不是复制旧root对象。
      val path = java.nio.file.Path.of("D:/WI - Dev Workspace/FloatingPod-test/_evidence/FP-REPAIR-20261006/MAINLINE-EXCEPTION-BOUNDARY-20261021-v1/normal-stop-handoff/normal-recall-r1/crossboot-A/mode-unit-r5-review/cold-full-data/image0.bin")
      root.reserveRetainedMemory(java.nio.file.Files.size(path) + 64L, "mixed original complete input BEFORE allocation")
      val bytes = java.nio.file.Files.readAllBytes(path)
      assertEquals("15831D2CD18337CA75DBF88F61EAB1F531E1A712B701AC50B93196B01C0AEFA0",
        java.util.HexFormat.of().withUpperCase().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)))
      val payload = new MySqlFormat3CanonicalPayload(MySqlFormat3Binary.takeOwned(bytes), root, root)
      val decoded = MySqlFormat3SnapshotCodec.decode(payload, profile, root)
      root.reserveRetainedMemory(8192L, "mixed producer input DATA context and wrappers BEFORE allocation")
      val before = new MySqlFormat3FullSnapshot(decoded.schema, decoded.connection, decoded.observationScope,
        decoded.maxAllowedPacketBytes, profile, decoded.relations, root)
      def id(n: Long) = new java.util.UUID(0L, n)
      val at = java.time.LocalDateTime.of(2026,10,9,1,2,3,123456000)
      val pose = com.wi.floatingpod.domain.FloatingPodPose("world",1.25,64.0,2.5,1.25f,-0.0f,0)
      val binding = com.wi.floatingpod.storage.PhysicalRecoveryBinding(id(10),
        com.wi.floatingpod.storage.PhysicalRecoveryCutoverState.Certified,2)
      val operation = MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(binding,id(11),id(50),1L,id(13),
        com.wi.floatingpod.domain.PodOperatingMode.Normal,pose)
      val tag = com.wi.floatingpod.server.io.IoOperationTag(id(17),5L,9L,id(316))
      val plan = MySqlOwnedRepositoryOperationPlan.single(tag,id(314),id(315),operation,profile)
      val context = new MySqlRepositoryOperationContextIdentity(tag,id(314),id(315),id(19),123L,"fpb1:"+("A"*43),
        id(10),3L,at.plusHours(1),id(18),"9.7.2","fp_controlled_data_test","pure@localhost",id(20),3,2,"a"*64,"b"*64,
        Vector.fill(32)(1.toByte),"FRESH_PLAN",java.security.MessageDigest.getInstance("SHA-256").digest(
          "controlled-materializer-pure-data-v1".getBytes(java.nio.charset.StandardCharsets.UTF_8)).toVector,
        profile.sha256().toVector,"RUN",plan.requestIdentitySha256,profile)
      val draft = new MySqlModeOriginalWitnessDraft(plan,operation,context,root)
      val revision = MySqlModeOperationEffectCodec.value(before.relation("physical_recovery_locator").rows.head,
        "physical_recovery_locator","locator_revision").asInstanceOf[MySqlFormat3Cell.SignedInteger].value
      MySqlModeWorkflowMaterializer.materialize(plan,operation,context,at.plusSeconds(2),before,revision,draft)
      val partial = MySqlModeWitnessCodec.censusRecipes(ModeWitnessDataFixtures.source(root),draft)
      println(s"MODE_RECIPE_MIXED stage=${partial.stage} failure=${partial.failure} business=${partial.businessCount} facts=${partial.recipeFacts} metadata=${partial.retainedMetadataBytes}")
      val diagnostic = partial.diagnostics
      assertNotNull(diagnostic)
      println(s"MODE_CAPTURE_CHARGES literal=${diagnostic.literalCharge} span=${diagnostic.spanCharge} segment=${diagnostic.segmentCharge} page=${diagnostic.pageCharge} lookup=${diagnostic.lookupCharge} packedPage=${diagnostic.packedPageCharge} packedDirectory=${diagnostic.packedDirectoryCharge} body=${diagnostic.bodyCharge} digest=${diagnostic.digestCharge} sequence=${diagnostic.sequenceCharge} other=${diagnostic.otherCharge} sum=${diagnostic.classifiedCharge}")
      println(s"MODE_CAPTURE_BODIES attempted=${partial.attemptedBodyCount} unique=${diagnostic.successfulUnique} duplicate=${diagnostic.successfulDuplicate} uniqueLiterals=${diagnostic.uniqueLiterals} uniqueSpans=${diagnostic.uniqueSpans} duplicateLiterals=${diagnostic.duplicateLiterals} duplicateSpans=${diagnostic.duplicateSpans} currentKind=${diagnostic.currentKind} currentEncoding=${diagnostic.currentEncoding} currentAssociation=${diagnostic.currentAssociation} currentSegments=${diagnostic.currentSegments} currentLiterals=${diagnostic.currentLiterals} currentEmitted=${diagnostic.currentEmitted}")
      println(s"MODE_CAPTURE_WORK comparisonBytes=${diagnostic.comparisonBytes} utf8Characters=${diagnostic.utf8Characters} dagDescents=${diagnostic.dagDescents} expandedBytes=${diagnostic.expandedBytes} eventProbes=${diagnostic.eventProbes} sequentialProbes=${diagnostic.sequentialEventProbes} randomProbes=${diagnostic.eventProbes - diagnostic.sequentialEventProbes} capture=${diagnostic.captureWork} recipe=${diagnostic.recipeWork} other=${diagnostic.otherWork} rangeSeeks=${diagnostic.rangeSeeks} rangeBytes=${diagnostic.rangeBytes} rangeAdvances=${diagnostic.rangeAdvances} dagPushes=${diagnostic.dagPushes} dagPops=${diagnostic.dagPops}")
      if partial.failure != null then partial.failure.printStackTrace()
      assertEquals(partial.retainedMetadataBytes, diagnostic.classifiedCharge)
      assertNull(partial.failure,"complete mixed original-profile recipe stage="+partial.stage)
      assertTrue(partial.allRecipesExact)
      assertTrue(partial.facts.nativeFits,"complete mixed native capacity remains NG")
      assertTrue(partial.facts.retainedFits,"complete mixed retained capacity remains NG")
      val facts = partial.facts
      assertEquals(facts.baselineAccounted + facts.metadataBytes, root.accountedMemoryBytes)
      // 空间占用测量，不是V3编码产物或Actor/native资格；实际输入/partial继续存活。
      root.reserveRetainedMemory(202752L, "original Actor fixed obligation retained in capacity measurement")
      root.reserveRetainedMemory(facts.wireBytes + facts.holderBytes, "complete counted output workspace BEFORE allocation")
      val outputWorkspace = new Array[Byte](Math.toIntExact(facts.wireBytes))
      root.reserveTemporaryMemory(facts.scratchPeakBytes, "measured writer scratch coexists with actual output workspace")
      assertEquals(1671723, outputWorkspace.length)
      outputWorkspace(outputWorkspace.length - 1) = 1.toByte
      val need = facts.baselineAccounted + facts.metadataBytes + facts.wireBytes + facts.holderBytes +
        facts.scratchPeakBytes + 202752L
      assertEquals(need, root.accountedMemoryBytes)
      assertTrue(root.accountedMemoryBytes <= profile.maxRetainedBytes())
      println(s"MODE_MIXED_ACTUAL_WORKSPACE bytes=${outputWorkspace.length} accounted=${root.accountedMemoryBytes} highWater=${root.successfulReserveHighWaterBytes} remaining=${root.availableMemoryBytes} encodedArtifact=false")
      root.releaseTemporaryMemory(facts.scratchPeakBytes, "test scratch ended; retained original graph and output obligation remain")
    finally preserveLiveBorrowCustody(root)

  @Test def captureV3GuardShapeOnOriginalCompleteMixedGraph(): Unit =
    val profile = com.wi.floatingpod.config.SourceLimitProfile.of(1000000L,1000000L,2000000L,64L,100000L,
      8000000L,4000000L,2000000L)
    val root = new MySqlFormat3ReadBudget(profile, profile.maxSqlPacketBytes())
    try
      // 已冻结的真实 Debug prepare/activation 后 owned DATA baseline；不是复制旧root对象。
      val path = java.nio.file.Path.of("D:/WI - Dev Workspace/FloatingPod-test/_evidence/FP-REPAIR-20261006/MAINLINE-EXCEPTION-BOUNDARY-20261021-v1/normal-stop-handoff/normal-recall-r1/crossboot-A/mode-unit-r5-review/cold-full-data/image0.bin")
      root.reserveRetainedMemory(java.nio.file.Files.size(path) + 64L, "mixed original complete input BEFORE allocation")
      val bytes = java.nio.file.Files.readAllBytes(path)
      assertEquals("15831D2CD18337CA75DBF88F61EAB1F531E1A712B701AC50B93196B01C0AEFA0",
        java.util.HexFormat.of().withUpperCase().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)))
      val payload = new MySqlFormat3CanonicalPayload(MySqlFormat3Binary.takeOwned(bytes), root, root)
      val decoded = MySqlFormat3SnapshotCodec.decode(payload, profile, root)
      root.reserveRetainedMemory(8192L, "mixed producer input DATA context and wrappers BEFORE allocation")
      val before = new MySqlFormat3FullSnapshot(decoded.schema, decoded.connection, decoded.observationScope,
        decoded.maxAllowedPacketBytes, profile, decoded.relations, root)
      def id(n: Long) = new java.util.UUID(0L, n)
      val at = java.time.LocalDateTime.of(2026,10,9,1,2,3,123456000)
      val pose = com.wi.floatingpod.domain.FloatingPodPose("world",1.25,64.0,2.5,1.25f,-0.0f,0)
      val binding = com.wi.floatingpod.storage.PhysicalRecoveryBinding(id(10),
        com.wi.floatingpod.storage.PhysicalRecoveryCutoverState.Certified,2)
      val operation = MySqlRepositoryOperation.SetOperatingModeTrackedIfOwned(binding,id(11),id(50),1L,id(13),
        com.wi.floatingpod.domain.PodOperatingMode.Normal,pose)
      val tag = com.wi.floatingpod.server.io.IoOperationTag(id(17),5L,9L,id(316))
      val plan = MySqlOwnedRepositoryOperationPlan.single(tag,id(314),id(315),operation,profile)
      val context = new MySqlRepositoryOperationContextIdentity(tag,id(314),id(315),id(19),123L,"fpb1:"+("A"*43),
        id(10),3L,at.plusHours(1),id(18),"9.7.2","fp_controlled_data_test","pure@localhost",id(20),3,2,"a"*64,"b"*64,
        Vector.fill(32)(1.toByte),"FRESH_PLAN",java.security.MessageDigest.getInstance("SHA-256").digest(
          "controlled-materializer-pure-data-v1".getBytes(java.nio.charset.StandardCharsets.UTF_8)).toVector,
        profile.sha256().toVector,"RUN",plan.requestIdentitySha256,profile)
      val draft = new MySqlModeOriginalWitnessDraft(plan,operation,context,root)
      val revision = MySqlModeOperationEffectCodec.value(before.relation("physical_recovery_locator").rows.head,
        "physical_recovery_locator","locator_revision").asInstanceOf[MySqlFormat3Cell.SignedInteger].value
      MySqlModeWorkflowMaterializer.materialize(plan,operation,context,at.plusSeconds(2),before,revision,draft)
      val source = ModeWitnessDataFixtures.source(root)
      val partial = MySqlModeWitnessCodec.censusRecipes(source,draft)
      assertNull(partial.failure, "original complete census must finish before shape observation")
      assertEquals(159, partial.heldRecipeCount)
      assertEquals(37752L, partial.recipeFacts.segments)
      assertEquals(1671723L, partial.facts.wireBytes, "existing census grammar remains unchanged")
      val output = java.nio.file.Path.of("D:/WI - Dev Workspace/FloatingPod-test/_evidence/FP-REPAIR-20261006/MAINLINE-EXCEPTION-BOUNDARY-20261021-v1/normal-stop-handoff/normal-recall-r1/crossboot-A/capacity-v3-guard-shape-r1/SHAPE.jsonl")
      val diagnostic = partial.diagnostics
      def observedCounters: Vector[Long] = Vector(diagnostic.comparisonBytes, diagnostic.utf8Characters,
        diagnostic.dagDescents, diagnostic.expandedBytes, diagnostic.eventProbes, diagnostic.sequentialEventProbes,
        diagnostic.rangeSeeks, diagnostic.rangeBytes, diagnostic.rangeAdvances, diagnostic.dagPushes,
        diagnostic.dagPops, diagnostic.captureWork, diagnostic.recipeWork, diagnostic.otherWork,
        partial.retainedMetadataBytes, partial.rawReferenceCount.toLong)
      val originalAccounted = root.accountedMemoryBytes
      val originalCounters = observedCounters
      val originalStage = partial.stage
      // 独立测量I/O：8K char writer、8K byte encoder及有界逐行格式化；不作为产品容量结果。
      val observationCharge = 32768L
      root.reserveTemporaryMemory(observationCharge, "shape-only bounded output buffer and scalar formatting")
      var writer: java.io.BufferedWriter = null
      var primary: Throwable = null
      try
        writer = java.nio.file.Files.newBufferedWriter(output, java.nio.charset.StandardCharsets.UTF_8,
          java.nio.file.StandardOpenOption.CREATE_NEW, java.nio.file.StandardOpenOption.WRITE)
        def line(value: String): Unit =
          assertTrue(value.length <= 2048, "shape record must remain bounded")
          writer.write(value)
          writer.write('\n')
        val profileSha = java.util.HexFormat.of().withUpperCase().formatHex(profile.sha256())
        val sourceSha = java.util.HexFormat.of().withUpperCase().formatHex(source.predecessorSha.toArray)
        val anchorSha = java.util.HexFormat.of().withUpperCase().formatHex(source.anchorSha.toArray)
        line(s"""{"kind":"header","purpose":"shape-only","censusMajor":3,"censusRevision":1,"encodedArtifact":false,"inputSha256":"15831D2CD18337CA75DBF88F61EAB1F531E1A712B701AC50B93196B01C0AEFA0","profileSha256":"$profileSha","sourcePredecessorSha256":"$sourceSha","sourceAnchorSha256":"$anchorSha","sourceKind":${source.predecessorKind},"sourceSequence":${source.predecessorSequence},"maxRetained":${profile.maxRetainedBytes()},"maxNative":${profile.maxNativeArtifactBytes()},"observationOnlyBytes":$observationCharge}""")
        var events = 0L
        var zeroEvents = 0L
        var bodyReferences = 0L
        var candidateLeaves = 0L
        var maxEvents = 0L
        var maxUtf16 = 0L
        var expanded = 0L
        var segments = 0L
        var node = 0
        while node < partial.heldRecipeCount do
          val length = partial.guardNodeLength(node)
          val count = partial.guardNodeEventCount(node)
          val wire = partial.guardNodeWireSegmentCount(node)
          line(s"""{"kind":"node","id":$node,"length":$length,"events":$count,"wireSegments":$wire}""")
          expanded = Math.addExact(expanded, length)
          segments = Math.addExact(segments, wire.toLong)
          maxEvents = Math.max(maxEvents, count.toLong)
          var event = 0
          var total = 0L
          while event < count do
            val length = partial.guardEventLength(node, event)
            val offset = partial.guardEventOffset(node, event)
            val target = partial.guardEventTarget(node, event)
            val utf16 = partial.guardEventUtf16Length(node, event)
            assertTrue(length >= 0L && offset >= 0L)
            assertTrue(target >= -2 && target < node, "actual DAG reference must remain backward")
            if target >= 0 then
              assertEquals(length, partial.guardNodeLength(target))
              bodyReferences = Math.addExact(bodyReferences, 1L)
            if target != -2 then
              candidateLeaves = Math.addExact(candidateLeaves, Math.addExact(length, 4095L) / 4096L)
            if length == 0L then zeroEvents = Math.addExact(zeroEvents, 1L)
            total = Math.addExact(total, length)
            events = Math.addExact(events, 1L)
            maxUtf16 = Math.max(maxUtf16, utf16.toLong)
            line(s"""{"kind":"event","node":$node,"ordinal":$event,"target":$target,"length":$length,"offset":$offset,"rawUtf16Length":$utf16}""")
            event += 1
          assertEquals(length, total, "actual original events must cover each node")
          node += 1
        var sequence = 0
        var sequenceBytes = 0L
        while sequence < partial.retainedSequenceCount do
          val length = partial.guardSequenceLength(sequence)
          val utf16 = partial.guardSequenceUtf16Length(sequence)
          sequenceBytes = Math.addExact(sequenceBytes, length)
          maxUtf16 = Math.max(maxUtf16, utf16.toLong)
          line(s"""{"kind":"sequence","id":$sequence,"length":$length,"utf16Length":$utf16}""")
          sequence += 1
        var attempted = 0
        var attemptedEvents = 0L
        var attemptedZero = 0L
        while attempted < partial.attemptedBodyCount do
          val body = partial.attemptedBody(attempted)
          val zeros = partial.guardAttemptedBodyZeroEvents(attempted)
          attemptedEvents = Math.addExact(attemptedEvents, body.retainedEventCount.toLong)
          attemptedZero = Math.addExact(attemptedZero, zeros.toLong)
          line(s"""{"kind":"attemptedBody","id":$attempted,"bodyKind":${body.kind},"encoding":${body.encoding},"association":${body.association},"length":${body.length},"events":${body.retainedEventCount},"spans":${body.retainedSpanCount},"zeros":$zeros}""")
          attempted += 1
        val materialization = draft.value
        var image = 0
        var rowReferences = 0L
        var cellReferences = 0L
        var maxColumns = 0L
        while image < 3 do
          val snapshot = if image == 0 then materialization.preimage else if image == 1 then materialization.reserveTarget else materialization.executeTarget
          var relation = 0
          while relation < snapshot.relations.size do
            val table = snapshot.relations(relation)
            line(s"""{"kind":"relation","image":$image,"relation":$relation,"rows":${table.rows.size}}""")
            var rowIndex = 0
            while rowIndex < table.rows.size do
              val row = table.rows(rowIndex)
              var nulls = 0L; var texts = 0L; var binaries = 0L; var integers = 0L; var floats = 0L; var dates = 0L
              var textTotal = 0L; var textMax = 0L; var textZero = 0L
              var binaryTotal = 0L; var binaryMax = 0L; var binaryZero = 0L
              var column = 0
              while column < row.size do
                row(column) match
                  case MySqlFormat3Cell.SqlNull => nulls += 1L
                  case MySqlFormat3Cell.Text(value) =>
                    texts += 1L; textTotal = Math.addExact(textTotal, value.length.toLong)
                    textMax = Math.max(textMax, value.length.toLong)
                    if value.isEmpty then textZero += 1L
                  case MySqlFormat3Cell.Binary(value) =>
                    binaries += 1L; binaryTotal = Math.addExact(binaryTotal, value.length.toLong)
                    binaryMax = Math.max(binaryMax, value.length.toLong)
                    if value.length == 0 then binaryZero += 1L
                  case MySqlFormat3Cell.SignedInteger(_) => integers += 1L
                  case MySqlFormat3Cell.Floating(_) => floats += 1L
                  case MySqlFormat3Cell.DateTime(_) => dates += 1L
                column += 1
              rowReferences = Math.addExact(rowReferences, 1L)
              cellReferences = Math.addExact(cellReferences, row.size.toLong)
              maxColumns = Math.max(maxColumns, row.size.toLong)
              maxUtf16 = Math.max(maxUtf16, textMax)
              line(s"""{"kind":"row","image":$image,"relation":$relation,"ordinal":$rowIndex,"columns":${row.size},"nulls":$nulls,"texts":$texts,"binaries":$binaries,"integers":$integers,"floats":$floats,"dates":$dates,"textTotal":$textTotal,"textMax":$textMax,"textZero":$textZero,"binaryTotal":$binaryTotal,"binaryMax":$binaryMax,"binaryZero":$binaryZero}""")
              rowIndex += 1
            relation += 1
          image += 1
        assertEquals(partial.recipeFacts.segments, segments)
        assertEquals(partial.recipeFacts.checkedExpandedBytes, expanded)
        assertEquals(partial.recipeFacts.bodyReferences, bodyReferences)
        assertEquals(originalStage, partial.stage)
        assertEquals(originalCounters, observedCounters, "shape getters must not move diagnostic/cursor counters")
        assertEquals(originalAccounted + observationCharge, root.accountedMemoryBytes)
        line(s"""{"kind":"summary","N":${partial.heldRecipeCount},"S":$segments,"L":${partial.facts.b},"D":${partial.bodyCount},"A":${partial.facts.a},"R":${partial.facts.r},"H":${partial.attemptedBodyCount},"ERecipe":$events,"EAttempted":$attemptedEvents,"zeroRecipe":$zeroEvents,"zeroAttempted":$attemptedZero,"Q":${partial.retainedSequenceCount},"Tshape":$cellReferences,"imageRowReferences":$rowReferences,"FRecipe":$maxEvents,"Z":$maxUtf16,"P":$maxColumns,"CAllSpans":$candidateLeaves,"bodyReferences":$bodyReferences,"expanded":$expanded,"sequenceBytes":$sequenceBytes,"businessAccountedBefore":$originalAccounted,"businessAccountedAfter":${root.accountedMemoryBytes - observationCharge},"diagnosticsUnchanged":true}""")
        writer.flush()
        println(s"MODE_GUARD_SHAPE path=$output profile=$profileSha nodes=${partial.heldRecipeCount} events=$events zero=$zeroEvents rowReferences=$rowReferences encodedArtifact=false")
      catch
        case cause: Throwable => primary = cause; throw cause
      finally
        var closed = false
        try
          if writer != null then
            writer.close()
            writer = null
            closed = true
        catch
          case cleanup: Throwable =>
            if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
        finally
          if closed then
            try root.releaseTemporaryMemory(observationCharge, "shape-only output closed and references dropped")
            catch
              case cleanup: Throwable =>
                if primary == null then throw cleanup else MySqlOwnedSessionLifecycle.suppress(primary, cleanup)
    finally preserveLiveBorrowCustody(root)

  @Test def originalProfileMeasuresCompleteWriterRecipesWithoutActivatingNewCodec(): Unit =
    val draft = ModeWitnessDataFixtures.draft()
    val root = draft.originalRoot
    try
      val partial = MySqlModeWitnessCodec.censusRecipes(ModeWitnessDataFixtures.source(root), draft)
      println(s"MODE_RECIPE_CENSUS stage=${partial.stage} failure=${partial.failure} facts=${partial.recipeFacts} metadata=${partial.retainedMetadataBytes}")
      val diagnostic = partial.diagnostics
      println(s"MODE_SMALL_WORK comparisonBytes=${diagnostic.comparisonBytes} utf8Characters=${diagnostic.utf8Characters} eventProbes=${diagnostic.eventProbes} sequentialProbes=${diagnostic.sequentialEventProbes} randomProbes=${diagnostic.eventProbes - diagnostic.sequentialEventProbes} capture=${diagnostic.captureWork} recipe=${diagnostic.recipeWork} other=${diagnostic.otherWork} rangeSeeks=${diagnostic.rangeSeeks} rangeBytes=${diagnostic.rangeBytes} rangeAdvances=${diagnostic.rangeAdvances} dagPushes=${diagnostic.dagPushes} dagPops=${diagnostic.dagPops}")
      assertSame(partial, draft.retainedCensus)
      assertNotNull(partial.businessFailure)
      assertEquals(3128112L, partial.businessCount.total)
      assertNull(partial.failure, "recipe census stage=" + partial.stage)
      assertNotNull(partial.recipeFacts)
      assertEquals(partial.chunkCount.toLong * 64L, partial.recipeFacts.leafSpanMetadata,
        "Recipe charges Slice storage only for unique leaves")
      assertTrue(partial.allRecipesExact)
      val facts = partial.facts
      assertEquals(442081L, facts.wireBytes, "packed storage must preserve the complete recipe wire")
      assertEquals(facts.wireBytes, facts.headerWire + facts.bWire + facts.aWire + facts.rWire + facts.sourceWire +
        facts.planWire + facts.contextWire + facts.imagesWire + facts.phasesWire + facts.descriptorsWire + facts.footerWire)
      assertEquals(facts.wireBytes <= 2000000L, facts.nativeFits)
      assertEquals(facts.wireBytes + facts.metadataBytes + facts.holderBytes + facts.scratchPeakBytes <= facts.baselineRemaining,
        facts.retainedFits)
      assertNull(draft.retainedEncodingBinary)
      assertFalse(root.isClosed)
    finally preserveLiveBorrowCustody(root)

  private def registered(draft: MySqlModeOriginalWitnessDraft,
      method: MySqlModeEnvelopeV2Census.Method = MySqlModeEnvelopeV2Census.Method.FlatV2): MySqlModeEnvelopeV2Census.Partial =
    val root = draft.originalRoot
    root.reserveRetainedMemory(512L, "test census original partial BEFORE allocation")
    val partial = new MySqlModeEnvelopeV2Census.Partial(root, draft, method)
    draft.retainCensus(partial)
    partial

  @Test def packedPagesKeepLiteralBitsAndZeroLengthRecordsAcross63_64_65(): Unit =
    Vector(63,64,65).foreach { count =>
      val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
      try
        val partial = registered(draft, MySqlModeEnvelopeV2Census.Method.RecipeV3); partial.initialize()
        val empty = binary(root, 0)
        val body = partial.observeEventBoundary(count, empty)
        val bytes = new java.io.ByteArrayOutputStream
        val output = new java.io.DataOutputStream(bytes)
        (0 until count).foreach(i => if i % 2 == 0 then output.writeLong(Long.MinValue + i) else output.writeByte(i))
        output.writeByte(255); output.flush()
        val expected = bytes.toByteArray
        assertEquals(count + 194, body.retainedEventCount)
        assertEquals(body.retainedEventCount, body.sequentialEventCount,
          "selected nonempty record advances nextEvent before finish drains the 65 trailing zero records")
        assertEquals(body.retainedEventCount.toLong, partial.diagnostics.sequentialEventProbes)
        assertEquals(partial.diagnostics.sequentialEventProbes, partial.diagnostics.eventProbes)
        assertEquals(body.emitted, body.sequentialByteCount)
        assertEquals(expected.length.toLong, body.emitted)
        expected.indices.reverse.foreach(i => assertEquals(expected(i).toInt & 255, body.at(i.toLong)))
        assertEquals(body.retainedEventCount, body.sequentialEventCount)
        assertEquals(body.emitted, body.sequentialByteCount)
        assertEquals(body.retainedEventCount.toLong, partial.diagnostics.sequentialEventProbes)
        assertTrue(partial.diagnostics.eventProbes > partial.diagnostics.sequentialEventProbes)
        assertEquals(0L, partial.diagnostics.literalCharge)
        assertEquals(0L, partial.diagnostics.segmentCharge)
        assertEquals(0L, partial.diagnostics.spanCharge, "raw borrowed events allocate no wrapper")
        assertEquals(partial.retainedMetadataBytes, partial.diagnostics.classifiedCharge)
      finally preserveLiveBorrowCustody(root)
    }

  @Test def sequentialEmptyAndAllZeroWritersSettleEveryRecordWithoutInventingBytes(): Unit =
    Vector(false, true).foreach { records =>
      val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
      try
        val partial = registered(draft, MySqlModeEnvelopeV2Census.Method.RecipeV3); partial.initialize()
        val body = partial.observeEmptyEvents(binary(root, 0), records)
        assertEquals(if records then 65 else 0, body.retainedEventCount)
        assertEquals(body.retainedEventCount, body.sequentialEventCount)
        assertEquals(0L, body.emitted); assertEquals(0L, body.sequentialByteCount)
        assertEquals(body.retainedEventCount.toLong, partial.diagnostics.sequentialEventProbes)
        assertEquals(partial.diagnostics.sequentialEventProbes, partial.diagnostics.eventProbes)
        assertEquals(0L, partial.diagnostics.comparisonBytes)
        assertArrayEquals(java.security.MessageDigest.getInstance("SHA-256").digest(Array.emptyByteArray), body.digest)
        assertEquals(partial.retainedMetadataBytes, partial.diagnostics.classifiedCharge)
      finally preserveLiveBorrowCustody(root)
    }

  @Test def sequentialOriginalCanonicalMismatchKeepsTheFirstCauseAndPartial(): Unit =
    val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
    try
      val partial = registered(draft, MySqlModeEnvelopeV2Census.Method.RecipeV3); partial.initialize()
      val expected = MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
        MySqlRecoveryCanonicalBytes.encodeExact("FP_MYSQL_RECOVERY_OPERATION_RAW_ROW_BE_V1", root.profile, 1L) {
          output => output.writeByte(41)
        }
      }
      val cause = assertThrows(classOf[SQLDataException], () =>
        partial.compareOriginalCanonical(expected, binary(root, 1)))
      assertTrue(cause.getMessage.contains("differs from retained original canonical"))
      assertSame(cause, partial.failure); assertSame(partial, draft.retainedCensus)
      assertTrue(cause.getStackTrace.nonEmpty)
      assertEquals(1, partial.attemptedBodyCount)
      assertEquals(0L, partial.diagnostics.currentEmitted)
      assertEquals(1L, partial.diagnostics.sequentialEventProbes)
      assertEquals(partial.retainedMetadataBytes, partial.diagnostics.classifiedCharge)
    finally preserveLiveBorrowCustody(root)

  @Test def packedUtf8SliceBoundsAndFailedPageKeepTheOriginalCause(): Unit =
    val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
    try
      val partial = registered(draft, MySqlModeEnvelopeV2Census.Method.RecipeV3); partial.initialize()
      val value = "零\uD83D\uDE80尾"
      val body = partial.observeUtf8(value)
      val encoded = value.getBytes(java.nio.charset.StandardCharsets.UTF_8)
      assertEquals(encoded.length.toLong + 4L, body.emitted)
      encoded.indices.foreach(i => assertEquals(encoded(i).toInt & 255, body.at(i.toLong + 4L)))
      val data = binary(root, 3)
      val slice = partial.observeSlice(data, 1L, 2L)
      assertEquals(2L, slice.emitted); assertEquals(42, slice.at(1L))
      assertEquals(0L, partial.diagnostics.spanCharge)
      val cause = assertThrows(classOf[Exception], () => partial.observeSlice(data, Long.MaxValue, 1L))
      assertSame(cause, partial.failure)
    finally preserveLiveBorrowCustody(root)
    val failing = ModeWitnessDataFixtures.draft(); val budget = failing.originalRoot
    try
      val partial = registered(failing, MySqlModeEnvelopeV2Census.Method.RecipeV3); partial.initialize()
      val value = binary(budget, 1)
      budget.reserveRetainedMemory(budget.availableMemoryBytes - 3800L, "packed page refusal fixture")
      val cause = assertThrows(classOf[SQLDataException], () => partial.observeBinary(value, 11))
      assertSame(cause, partial.failure)
      assertSame(partial, failing.retainedCensus)
      assertEquals(1, partial.attemptedBodyCount)
      assertEquals(0, partial.diagnostics.currentSegments)
      assertTrue(cause.getMessage.contains("packed event page"), cause.getMessage)
      assertEquals(partial.retainedMetadataBytes, partial.diagnostics.classifiedCharge)
    finally preserveLiveBorrowCustody(budget)

  @Test def rawLeafKindsKeepEncodingAndFirstIdsWithoutDuplicateSliceAllocation(): Unit =
    val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
    try
      val partial = registered(draft, MySqlModeEnvelopeV2Census.Method.RecipeV3); partial.initialize()
      root.reserveRetainedMemory(4097L * 2L + 128L, "test original String BEFORE allocation")
      partial.observeLeafKinds(binary(root, 4097), "*" * 4097)
      assertEquals(0L, partial.diagnostics.spanCharge)
      partial.resolveObservedLeafKinds()
      assertEquals(4, partial.chunkCount)
      assertEquals(256L, partial.uniqueRecipeLeafCharge)
      for body <- 0 until 2; leaf <- 0 until 2 do
        assertEquals(body * 2 + leaf, partial.observedLeafId(body, 1, leaf))
        assertEquals(partial.observedLeafId(body, 1, leaf), partial.observedLeafId(body, 3, leaf))
      assertNotEquals(partial.observedLeafId(0, 1, 0), partial.observedLeafId(1, 1, 0))
      assertThrows(classOf[IllegalArgumentException], () => { partial.observedLeafId(0, 1, 2); () },
        "first span cannot read the next span even though both share a flat leaf array")
      assertTrue(partial.allRecipesExact)
      assertEquals(partial.retainedMetadataBytes, partial.diagnostics.classifiedCharge)
    finally preserveLiveBorrowCustody(root)

  @Test def firstUniqueLeafLookupFailureKeepsPendingSliceAndOriginalCause(): Unit =
    val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
    try
      val partial = registered(draft, MySqlModeEnvelopeV2Census.Method.RecipeV3); partial.initialize()
      root.reserveRetainedMemory(4097L * 2L + 128L, "test original String BEFORE allocation")
      partial.observeLeafKinds(binary(root, 4097), "*" * 4097)
      // W reader/cursor768 + M2:128 store+208 order+736 page+192 node+2*72 span arrays+80 leaf IDs+64 Slice+640 leaf page，余95拒96 lookup。
      root.reserveRetainedMemory(root.availableMemoryBytes - 3055L, "actual first unique leaf lookup refusal fixture")
      val cause = assertThrows(classOf[SQLDataException], () => partial.resolveObservedLeafKinds())
      assertTrue(cause.getMessage.contains("page lookup"), cause.getMessage)
      assertEquals(0, partial.chunkCount); assertEquals(1, partial.pendingChunkCount)
      assertEquals(64L, partial.uniqueRecipeLeafCharge)
      assertSame(cause, partial.failure); assertSame(partial, draft.retainedCensus)
      assertEquals(partial.retainedMetadataBytes, partial.diagnostics.classifiedCharge)
    finally preserveLiveBorrowCustody(root)

  @Test def sparsePayloadOrdinalCrossesIndependentEventAndSpanPages(): Unit =
    for count <- Vector(63,64,65); layout <- 0 to 3 do
      val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
      try
        val partial = registered(draft, MySqlModeEnvelopeV2Census.Method.RecipeV3); partial.initialize()
        val value = binary(root, 1)
        val body = partial.observeSparseEvents(count, layout, value)
        val outputBytes = new java.io.ByteArrayOutputStream
        val out = new java.io.DataOutputStream(outputBytes)
        if layout == 3 then (0 until 64).foreach(i => out.writeLong(Long.MinValue + i))
        (0 until count).foreach { i =>
          if layout == 0 || layout == 2 then out.writeLong(Long.MinValue + i)
          if layout != 0 then out.writeByte(42)
        }
        out.flush()
        val expected = outputBytes.toByteArray
        assertEquals(expected.length.toLong, body.length)
        expected.indices.reverse.foreach(i => assertEquals(expected(i).toInt & 255, body.at(i.toLong)))
        val spans = if layout == 0 then 0 else count
        assertEquals(spans, body.retainedSpanCount)
        assertEquals((spans + 63) / 64, body.retainedPayloadPages)
        assertEquals((body.retainedEventCount + 63) / 64, body.retainedEventPages)
        (0 until spans).foreach { ordinal =>
          val event = if layout == 2 then ordinal * 2 + 1 else if layout == 3 then ordinal + 64 else ordinal
          assertEquals(ordinal, body.eventSpanOrdinal(event))
          assertTrue(body.borrowedEventIs(event, value), "retain the exact original payload reference")
        }
        partial.resolveObservedSparseEvents()
        assertTrue(partial.allRecipesExact)
        assertEquals(if spans == 0 then 0 else 1, partial.chunkCount)
        assertEquals(if spans == 0 then 3 else 7, partial.recipeArrayMask(0))
        assertEquals(partial.retainedMetadataBytes, partial.diagnostics.classifiedCharge)
      finally preserveLiveBorrowCustody(root)

  @Test def sparsePageRefusalsKeepBothCustodyAndUncommittedOrdinal(): Unit =
    // capture base3360，event1280+directory96，再payload704+directory96；另一布局先有payload再拒第二event页。
    for (remaining, layout, message, events, spans, eventPages, payloadPages) <- Vector(
        (5439L, 1, "page payload", 0, 0, 1, 0),
        (5535L, 1, "directory payload", 0, 0, 1, 0),
        (6815L, 4, "page and two arrays", 64, 1, 1, 1)) do
      val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
      try
        val partial = registered(draft, MySqlModeEnvelopeV2Census.Method.RecipeV3); partial.initialize()
        val value = binary(root, 1)
        root.reserveRetainedMemory(root.availableMemoryBytes - remaining, "exact sparse page refusal fixture")
        val before = partial.retainedMetadataBytes
        val cause = assertThrows(classOf[SQLDataException], () => partial.observeSparseEvents(1, layout, value))
        assertTrue(cause.getMessage.contains(message), cause.getMessage)
        assertSame(cause, partial.failure); assertSame(partial, draft.retainedCensus)
        val body = partial.attemptedBody(0)
        assertEquals(events, body.retainedEventCount); assertEquals(spans, body.retainedSpanCount)
        assertEquals(eventPages, body.retainedEventPages); assertEquals(payloadPages, body.retainedPayloadPages)
        assertTrue(body.pendingRawIs(value))
        assertEquals(remaining - 2048L - (if layout == 4 then 1279L else if remaining == 5439L then 703L else 95L),
          partial.retainedMetadataBytes - before, "all successful fees remain; no credit for the refused allocation")
        if spans > 0 then assertTrue(body.borrowedEventIs(0, value))
        assertEquals(partial.retainedMetadataBytes, partial.diagnostics.classifiedCharge)
      finally preserveLiveBorrowCustody(root)

  @Test def recipeArrayFailuresRetainPublishedNodeAndRejectUnreadiness(): Unit =
    for (remaining, message, mask) <- Vector((2175L, "span starts", 1), (2255L, "flat leaf indices", 3)) do
      val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
      try
        val partial = registered(draft, MySqlModeEnvelopeV2Census.Method.RecipeV3); partial.initialize()
        root.reserveRetainedMemory(4097L * 2L + 128L, "test original String BEFORE allocation")
        partial.observeLeafKinds(binary(root, 4097), "*" * 4097)
        root.reserveRetainedMemory(root.availableMemoryBytes - remaining, "exact node array refusal fixture")
        val cause = assertThrows(classOf[SQLDataException], () => partial.resolveObservedLeafKinds())
        assertTrue(cause.getMessage.contains(message), cause.getMessage)
        assertSame(cause, partial.failure)
        assertEquals(1, partial.heldRecipeCount); assertEquals(mask, partial.recipeArrayMask(0))
        assertEquals(0, partial.chunkCount)
        val unreadiness = assertThrows(classOf[IllegalArgumentException], () => partial.allRecipesExact)
        assertTrue(unreadiness.getMessage.contains("not ready"))
        assertSame(cause, partial.failure, "readiness rejection must not overwrite the original allocation cause")
        assertEquals(partial.retainedMetadataBytes, partial.diagnostics.classifiedCharge)
      finally preserveLiveBorrowCustody(root)

  @Test def rawMalformedUtf16KeepsStrictFailureInOriginalPartial(): Unit =
    Vector(0xd800.toChar.toString, 0xdc00.toChar.toString).foreach { invalid =>
      val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
      try
        val partial = registered(draft, MySqlModeEnvelopeV2Census.Method.RecipeV3); partial.initialize()
        val cause = assertThrows(classOf[SQLDataException], () => partial.observeUtf8(invalid))
        assertTrue(cause.getMessage.contains("unpaired UTF-16 surrogate"))
        assertEquals(1L, partial.diagnostics.utf8Characters, "raw length validation counts its charAt before rejection")
        assertSame(cause, partial.failure); assertSame(partial, draft.retainedCensus)
        assertEquals(1, partial.attemptedBodyCount)
        assertEquals(0L, partial.diagnostics.currentEmitted)
        assertEquals(0L, partial.diagnostics.spanCharge)
      finally preserveLiveBorrowCustody(root)
    }
  @Test def sequentialRangesKeepUtf8InteriorBytesAndIndependentResets(): Unit =
    val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
    try
      val partial = registered(draft, MySqlModeEnvelopeV2Census.Method.RecipeV3); partial.initialize()
      val text = "a" * 4094 + "零\uD83D\uDE80尾" + "b" * 67
      root.reserveRetainedMemory(text.length.toLong * 2L + 128L, "original test text")
      val encoded = text.getBytes(java.nio.charset.StandardCharsets.UTF_8)
      root.reserveRetainedMemory(encoded.length.toLong + 64L, "original test binary")
      val value = MySqlFormat3Binary.takeOwned(encoded)
      partial.observeLeafKinds(value, text)
      partial.resolveObservedLeafKinds()
      assertTrue(partial.allRecipesExact)
      assertEquals(partial.diagnostics.dagPushes, partial.diagnostics.dagPops)
      // Body1 contains length/text/length/text: seek into both a multi-byte codepoint and the second span.
      for offset <- Vector(4099, encoded.length + 8 + 4096, 0, 4098) do
        val original = partial.attemptedBody(1)
        val expected = Array.tabulate[Byte](7)(i => original.at(offset.toLong + i).toByte)
        root.reserveRetainedMemory(71L, "test exact range bytes")
        val before = root.accountedMemoryBytes
        assertTrue(partial.observedRangeEquals(1, offset, MySqlFormat3Binary.takeOwned(expected)))
        assertEquals(before, root.accountedMemoryBytes)
        assertFalse(partial.hasReaderWorkspaceReferences)
      assertTrue(partial.diagnostics.rangeBytes > 0L)
      assertTrue(partial.diagnostics.rangeSeeks < partial.diagnostics.rangeBytes)
    finally preserveLiveBorrowCustody(root)

  @Test def sequentialFramesReplaySharedDeepChildrenAndCrossPageZeroSpans(): Unit =
    val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
    try
      val partial = registered(draft, MySqlModeEnvelopeV2Census.Method.RecipeV3); partial.initialize()
      var value = binary(root, 1)
      partial.observeBinary(value, 11)
      for depth <- 1 to 10 do
        partial.observeRepeatedBinary(value)
        val size = Math.addExact(Math.multiplyExact(value.length, 2), 1)
        root.reserveRetainedMemory(size.toLong + 64L, "actual shared-child test source")
        val next = new Array[Byte](size)
        var index = 0
        while index < value.length do
          next(index) = value.byteAt(index); next(index + value.length) = value.byteAt(index); index += 1
        next(size - 1) = 255.toByte
        value = MySqlFormat3Binary.takeOwned(next)
      val empty = binary(root, 0)
      partial.observeEventBoundary(65, empty)
      partial.observeEmptyEvents(empty, true)
      partial.resolveObservedRanges()
      assertTrue(partial.allRecipesExact)
      assertEquals(partial.diagnostics.dagPushes, partial.diagnostics.dagPops)
      assertTrue(partial.diagnostics.dagPushes > partial.heldRecipeCount * 10L,
        "each shared reference is replayed; a visited-node shortcut must not suppress bytes")
      assertEquals(0L, partial.heldReaderWorkspaceBytes)
      assertFalse(partial.hasReaderWorkspaceReferences)
    finally preserveLiveBorrowCustody(root)

  @Test def readerAndFrameReserveFailuresDropOnlyTemporaryWorkspace(): Unit =
    for remaining <- Vector(767L, 1371L) do
      val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
      try
        val partial = registered(draft, MySqlModeEnvelopeV2Census.Method.RecipeV3); partial.initialize()
        partial.observeBinary(binary(root, 1), 11); partial.resolveObservedRanges()
        assertEquals(1, partial.heldRecipeCount)
        root.reserveRetainedMemory(root.availableMemoryBytes - remaining, "exact sequential workspace refusal")
        val before = root.accountedMemoryBytes
        val cause = assertThrows(classOf[SQLDataException], () => { partial.allRecipesExact; () })
        assertTrue(cause.getMessage.contains(if remaining == 767L then "two independent" else "frame holder"))
        assertEquals(before, root.accountedMemoryBytes)
        assertEquals(0L, partial.heldReaderWorkspaceBytes)
        assertFalse(partial.hasReaderWorkspaceReferences)
        assertEquals(1, partial.heldRecipeCount)
      finally preserveLiveBorrowCustody(root)

  @Test def checkedReaderCleanupFailurePreservesPrimaryAfterDroppingReferences(): Unit =
    // 本用例只隔离reader cleanup：未materialize的真实draft没有A/B借用。
    val images = new MySqlModeOperationEffectCodecTest().modeWorkflowImages()
    val f = images.fixture
    val root = new MySqlFormat3ReadBudget(f.profile, f.profile.maxSqlPacketBytes())
    val draft = new MySqlModeOriginalWitnessDraft(f.plan, f.operation, f.context, root)
    val partial = registered(draft, MySqlModeEnvelopeV2Census.Method.RecipeV3); partial.initialize()
    partial.observeBinary(binary(root, 1), 11); partial.resolveObservedRanges()
    partial.openObservedReaderWorkspace()
    assertEquals(1372L, partial.heldReaderWorkspaceBytes)
    assertTrue(partial.hasReaderWorkspaceReferences)
    val primary = new SQLDataException("original range failure")
    root.closeRootReadBudget() // real checked-close refusal, no callback/serializer injection
    partial.closeObservedReaderWorkspace(primary)
    assertEquals(1, primary.getSuppressed.length)
    assertFalse(partial.hasReaderWorkspaceReferences)
    assertEquals(1372L, partial.heldReaderWorkspaceBytes, "failed cleanup must not claim a successful credit")

  private def preserveLiveBorrowCustody(root: MySqlFormat3ReadBudget): Unit =
    // Census未生成exact projection ACK；测试结束不能伪造drop/credit。
    val retained = root.accountedMemoryBytes
    assertTrue(root.hasActiveImmutableBorrows)
    assertThrows(classOf[IllegalArgumentException], () => root.closeRootReadBudget())
    assertFalse(root.isClosed)
    assertEquals(retained, root.accountedMemoryBytes)

  private def binary(root: MySqlFormat3ReadBudget, size: Int): MySqlFormat3Binary =
    root.reserveRetainedMemory(size.toLong + 64L, "test actual immutable chunk input BEFORE allocation")
    val bytes = new Array[Byte](size)
    java.util.Arrays.fill(bytes, 42.toByte)
    MySqlFormat3Binary.takeOwned(bytes)

  @Test def originalCaseSameRootMeasuresCompleteWireAndKeepsBusinessCapacityCause(): Unit =
    val draft = ModeWitnessDataFixtures.draft()
    val root = draft.originalRoot
    val partial = MySqlModeWitnessCodec.census(ModeWitnessDataFixtures.source(root), draft)
    assertSame(partial, draft.retainedCensus)
    assertSame(root, partial.root)
    assertNotNull(partial.businessFailure)
    assertSame(partial.businessFailure, draft.retainedFailure.get)
    assertEquals(3128112L, partial.businessCount.total)
    assertEquals(2000000L, partial.businessCount.nativeLimit)
    assertEquals(8000000L, partial.businessCount.retainedLimit)
    assertNull(partial.failure, "census NG stage=" + partial.stage + " B=" + partial.chunkCount +
      " bodies=" + partial.bodyCount + " metadata=" + partial.retainedMetadataBytes)
    assertNotNull(partial.facts)
    val facts = partial.facts
    println(s"MODE_V2_CENSUS business=${partial.businessCount} deduplicated=$facts")
    assertTrue(facts.b > 0 && facts.a > 0 && facts.r > 0 && facts.references > 0L)
    assertEquals(facts.wireBytes, facts.headerWire + facts.bWire + facts.aWire + facts.rWire + facts.sourceWire +
      facts.planWire + facts.contextWire + facts.imagesWire + facts.phasesWire + facts.descriptorsWire + facts.footerWire)
    assertEquals(facts.wireBytes <= 2000000L, facts.nativeFits)
    assertEquals(facts.wireBytes + facts.metadataBytes + facts.holderBytes + facts.scratchPeakBytes <= facts.baselineRemaining,
      facts.retainedFits)
    assertTrue(partial.sortedIdsAreBounded)
    assertTrue(partial.allRemappedSequencesExact)
    assertNull(draft.retainedEncodingBinary)
    assertFalse(root.isClosed)

  @Test def exact4096And4097ChunksKeepDifferentEncodingAndCompleteBodies(): Unit =
    val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
    val partial = registered(draft); partial.initialize()
    val first = binary(root, 4096); val second = binary(root, 4097)
    val a = partial.observeBinary(first, 11)
    assertEquals(4096L, a.emitted); assertEquals(1, partial.chunkCount)
    val b = partial.observeBinary(second, 11)
    assertEquals(4097L, b.emitted); assertEquals(2, partial.chunkCount)
    partial.observeBinary(second, 12)
    assertEquals(4, partial.chunkCount)
    assertEquals(3, partial.bodyCount)
    assertSame(partial, draft.retainedCensus)

  @Test def strictUtf8CrossChunkAndOriginalCompleteBodyMismatchAreExplicit(): Unit =
    val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
    val partial = registered(draft); partial.initialize()
    root.reserveRetainedMemory(2L * 4096L + 128L, "test original UTF16 String BEFORE allocation")
    val value = "a" * 4095 + "é"
    val body = partial.observeUtf8(value)
    assertEquals(4101L, body.emitted)
    val bytes = binary(root, 4097)
    val expected = MySqlRecoveryCanonicalBytes.withAllocationBudget(root) {
      MySqlRecoveryCanonicalBytes.encodeExact("FP_MYSQL_RECOVERY_OPERATION_RAW_ROW_BE_V1", root.profile, 4097L) {
        out => bytes.writeTo(out)
      }
    }
    val failure = assertThrows(classOf[SQLDataException], () =>
      partial.compareOriginalCanonical(expected, binary(root, 4096)))
    assertSame(failure, partial.failure)
    assertTrue(failure.getStackTrace.nonEmpty)
    assertSame(partial, draft.retainedCensus)
    assertFalse(root.isClosed)

  @Test def indexBudgetFailureRetainsOriginalPartialAndCauseBeforeArrays(): Unit =
    val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
    val partial = registered(draft)
    root.reserveRetainedMemory(root.availableMemoryBytes - 1L, "test real competing retained obligation")
    val failure = assertThrows(classOf[SQLDataException], () => partial.initialize())
    assertSame(failure, partial.failure)
    assertSame(partial, draft.retainedCensus)
    assertEquals(0, partial.chunkCount)
    assertEquals(0, partial.bodyCount)
    assertTrue(failure.getStackTrace.nonEmpty)
    assertFalse(root.isClosed)

  @Test def foreignRootAndForeignThreadRejectBeforeIndexAllocation(): Unit =
    val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
    // 故意foreign negative DATA root，不能给actual draft借第二cap。
    val foreign = new MySqlFormat3ReadBudget(root.profile, root.profile.maxSqlPacketBytes())
    foreign.reserveRetainedMemory(512L, "test foreign partial BEFORE allocation")
    val invalid = new MySqlModeEnvelopeV2Census.Partial(foreign, draft)
    val cause = assertThrows(classOf[IllegalArgumentException], () => invalid.initialize())
    assertSame(cause, invalid.failure); assertEquals(0, invalid.chunkCount)
    val partial = registered(draft)
    val observed = new AtomicReference[Throwable]()
    val thread = new Thread(() =>
      try partial.check()
      catch case failure: Throwable => observed.set(failure))
    thread.start(); thread.join(1000L)
    assertFalse(thread.isAlive, "foreign-thread owner probe must settle")
    assertTrue(observed.get().isInstanceOf[IllegalArgumentException])
    assertEquals(0, partial.chunkCount)
    assertSame(partial, draft.retainedCensus)

  @Test def every640Through735LookupFailureAllocatesNoUnheldPageOrBody(): Unit =
    var remaining = 640L
    while remaining <= 735L do
      val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
      val partial = registered(draft); partial.initialize()
      val input = binary(root, 1)
      root.reserveRetainedMemory(root.availableMemoryBytes - remaining, "test exact page/lookup reserve boundary")
      val cause = assertThrows(classOf[SQLDataException], () => partial.observeBinary(input, 11))
      assertSame(cause, partial.failure); assertTrue(cause.getStackTrace.nonEmpty)
      assertTrue(cause.getMessage.contains("page lookup array"))
      assertEquals("NG_CENSUS_BODY_INSERTION_PREPARE", partial.stage)
      assertEquals(0, partial.allocatedPageCount); assertEquals(0, partial.attemptedBodyCount)
      assertEquals(0, partial.retainedSequenceCount); assertEquals(remaining - 640L, root.availableMemoryBytes)
      assertSame(partial, draft.retainedCensus); assertFalse(root.isClosed)
      remaining += 1L

  @Test def bodyHeldBeforeActualStreamWorkspaceReserveFailure(): Unit =
    val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
    val partial = registered(draft); partial.initialize()
    val input = binary(root, 1)
    // page640 + first lookup96 + body256成功；余2047不能通过真实temporary2048。
    root.reserveRetainedMemory(root.availableMemoryBytes - 3039L, "test actual body workspace secondary reserve failure")
    val cause = assertThrows(classOf[SQLDataException], () => partial.observeBinary(input, 11))
    assertSame(cause, partial.failure); assertTrue(cause.getStackTrace.nonEmpty)
    assertEquals("NG_CENSUS_BODY_STREAM_WORKSPACE", partial.stage)
    assertEquals(1, partial.allocatedPageCount); assertEquals(1, partial.attemptedBodyCount)
    assertEquals(0, partial.bodyCount); assertEquals(2047L, root.availableMemoryBytes)
    assertSame(partial, draft.retainedCensus); assertFalse(root.isClosed)

  @Test def sequenceIdsAndPendingChunkHeldOnActualChunkLookupReserveFailure(): Unit =
    val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
    val partial = registered(draft); partial.initialize()
    val input = binary(root, 4096)
    // BinarySpan48 + sequence page736 + sequence/IDs116 + chunk64 + chunkpage640；lookup96失败。
    root.reserveRetainedMemory(root.availableMemoryBytes - 1604L, "test actual sequence/chunk insertion failure")
    val cause = assertThrows(classOf[SQLDataException], () => partial.observeSequence(input, 11))
    assertSame(cause, partial.failure); assertTrue(cause.getStackTrace.nonEmpty)
    assertTrue(cause.getMessage.contains("page lookup array"))
    assertEquals("NG_CENSUS_SEQUENCE_CHUNKS", partial.stage)
    assertEquals(1, partial.retainedSequenceCount); assertEquals(1, partial.retainedSequenceIdCount)
    assertEquals(1, partial.pendingChunkCount); assertEquals(1, partial.allocatedPageCount)
    assertEquals(0, partial.chunkCount); assertEquals(0L, root.availableMemoryBytes)
    assertSame(partial, draft.retainedCensus); assertFalse(root.isClosed)

  @Test def originalTwoPhasesOrderedRawReferencesKeepOwnAssociationAndExactData(): Unit =
    val draft = ModeWitnessDataFixtures.draft(); val root = draft.originalRoot
    val partial = MySqlModeWitnessCodec.census(ModeWitnessDataFixtures.source(root), draft)
    assertNull(partial.failure, "actual census must complete before ordered RAW DATA acceptance: " + partial.stage)
    assertNotNull(partial.facts)
    val materialization = draft.value
    var association = 0; var emitted = 0
    while association < 2 do
      val effect = materialization.effects(association)
      var list = 0
      while list < 2 do
        val values = if list == 0 then effect.preimageRawOperationEnvelopes else effect.rawOperationEnvelopes
        var ordinal = 0
        while ordinal < values.size do
          assertTrue(partial.rawReferenceMatches(emitted, association, list, ordinal, values(ordinal)),
            "actual emitted RAW must keep UUID/SHA/list order/full canonical bytes and own phase association")
          emitted += 1; ordinal += 1
        list += 1
      association += 1
    assertEquals(emitted, partial.rawReferenceCount)
    assertTrue(emitted > 0); assertTrue(partial.allRemappedSequencesExact)
    assertSame(partial.businessFailure, draft.retainedFailure.get)
    assertNull(draft.retainedEncodingBinary)
